//! Authenticated WebSocket `/streaming` (Phase 3, profile §2).
//!
//! Wire protocol (misskey-js `Stream` parity):
//! - Upgrade: token from `?i=` or `Authorization: Bearer`. No token means
//!   anonymous (non-credential channels only). Bad token → HTTP 401,
//!   suspended user → 403, token without `read:account` → 401.
//! - Client→server JSON `{type, body}`: `connect {channel,id,params?}`,
//!   `disconnect {id}`, `channel`/`ch` (only `readNotification` is
//!   interpreted), top-level `readNotification`, `subNote`/`s`/`sr` `{id}`,
//!   `unsubNote`/`un` `{id}`. Anything else is ignored.
//! - Server→client: `channel {id,type,body}` for channel events,
//!   top-level `noteUpdated {id,type,body}` for capture events,
//!   `connected {id}` when `connect` carried `pong: true`, and
//!   `error {code: UNSUPPORTED_CHANNEL, channel, id}` for unknown channels.
//! - Channels (upstream `stream/channels/*` parity): `main`,
//!   `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline`,
//!   `antenna` + `userList` (credential, owner-checked), `hashtag`
//!   (`q: string[][]`), `roleTimeline` (`roleId`), `drive` (credential),
//!   `serverStats` + `queueStats` (snapshot `stats` on connect, `statsLog`
//!   on `requestLog`), `admin` (credential + moderator), `clip`
//!   (`clipId`, `updated`/`deleted` with null bodies). `reversi`,
//!   `reversiGame`, `channel`, `chatUser`, `chatRoom` stay
//!   `UNSUPPORTED_CHANNEL` by decision (no channels/chat/Reversi model —
//!   the REST surfaces ship, the streams do not).
//! - Main events: every upstream `MainEventTypes` key is forwarded
//!   (`notification`, `mention`, `reply`, `renote`, `follow`, `followed`,
//!   `unfollow`, `meUpdated`, `readAllNotifications`,
//!   `notificationFlushed`, `unreadNotification`, `unreadAntenna`,
//!   `readAntenna`, `readAllAnnouncements`, `announcementCreated`,
//!   `myTokenRegenerated`, `signin`, `registryUpdated`, `driveFileCreated`,
//!   `urlUploadFinished`, `pageEvent`, `receiveFollowRequest`); producers
//!   live in the owning commands (see the WP4 report for the still-missing
//!   ones).
//! - DIVERGENCEs: `serverStats`/`queueStats` emit one snapshot on connect
//!   (+ `statsLog` on demand) instead of upstream's 1 s Xev broadcast
//!   (no in-process broadcast bus); `roleTimeline` accepts any `roleId`
//!   but no producer publishes to it (no role model); drive-folder bodies
//!   are `{id}` until the WP2 folder packer lands; `reacted.emoji` is
//!   `{name, url} | null` resolved from the emoji stores.
//! - Limits: 32 channels/connection, 1536 captured notes (LRU-evicts the
//!   oldest, like Misskey), 2 min idle reap with 60 s pings.
//!
//! Storage reads run on the blocking pool; the hub fan-out itself never
//! blocks. Timeline filtering mirrors the REST eligibility
//! (`note_eligible_for_channel`); capture uses the light gate
//! (`capture_gate`).

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::Value;
use tokio::time::interval;

use crate::api::misskey::auth::{authenticate, extract_raw_token};
use crate::api::misskey::endpoints::ApiState;
use crate::api::misskey::errors::ApiError;
use crate::api::misskey::packers::pack_me_detailed;
use crate::application::events::{MainEventKind, MainMessage};
use crate::application::queries::{TimelineChannel, TimelineChannelParams};
use crate::streaming::hub::HubMessage;

const MAX_CHANNELS_PER_CONNECTION: usize = 32;
const MAX_CAPTURED_NOTES: usize = 1536;
const IDLE_REAP_AFTER: Duration = Duration::from_secs(120);
const PING_EVERY: Duration = Duration::from_secs(60);
const MAX_WS_MESSAGE_BYTES: usize = 64 * 1024;

/// Owned caller snapshot for a streaming connection.
#[derive(Debug, Clone)]
struct CallerInfo {
    account_id: String,
    is_first_party: bool,
}

#[derive(Debug, Clone)]
enum ChannelKind {
    Main,
    Timeline(TimelineChannel),
    Antenna { antenna_id: String },
    UserList { list_id: String },
    /// Upstream `hashtag` (`q: string[][]`, AND-of-ORs over lowercased
    /// tags; no credential).
    Hashtag { q: Vec<Vec<String>> },
    /// Upstream `roleTimeline` (`roleId`; no credential). Pinetree has no
    /// role model, so no producer publishes `RoleTimelineNote` yet — the
    /// channel connects and stays silent (recorded DIVERGENCE, not an
    /// error: upstream would likewise refuse an unknown role id).
    RoleTimeline { role_id: String },
    /// Upstream `drive` (credential): per-user file/folder events.
    Drive,
    /// Upstream `serverStats` (no credential): snapshot `stats` on
    /// connect, `statsLog` on `requestLog`.
    ServerStats,
    /// Upstream `queueStats` (no credential): same shape, job counts.
    QueueStats,
    /// Upstream `admin` (credential + moderator): abuse-report events.
    Admin,
    /// Upstream `clip` (`clipId`; public clips allow anonymous, private
    /// clips need the owner): `updated`/`deleted` with null bodies.
    Clip { clip_id: String },
}

#[derive(Debug, Clone)]
struct ChannelSub {
    kind: ChannelKind,
    params: TimelineChannelParams,
}

fn parse_bool(value: Option<&Value>, default: bool) -> bool {
    match value {
        None | Some(Value::Null) => default,
        Some(Value::Bool(b)) => *b,
        _ => default,
    }
}

fn channel_params(kind: &ChannelKind, params: Option<&Value>) -> TimelineChannelParams {
    let obj = params.and_then(Value::as_object);
    let get = |key: &str| obj.and_then(|m| m.get(key));
    let mut out = TimelineChannelParams::default();
    out.with_renotes = parse_bool(get("withRenotes"), true);
    out.with_files = parse_bool(get("withFiles"), false);
    if matches!(
        kind,
        ChannelKind::Timeline(TimelineChannel::Local | TimelineChannel::Hybrid)
    ) {
        out.with_replies = parse_bool(get("withReplies"), false);
    }
    out
}

/// Upstream `userList` params parity (`withFiles` default false,
/// `withRenotes` default true; `withReplies` is per-membership, not a
/// channel param). Applied to the note fan-out in `dispatch_hub_message`.
fn user_list_params(params: Option<&Value>) -> TimelineChannelParams {
    let obj = params.and_then(Value::as_object);
    let get = |key: &str| obj.and_then(|m| m.get(key));
    TimelineChannelParams {
        with_files: parse_bool(get("withFiles"), false),
        with_renotes: parse_bool(get("withRenotes"), true),
        with_replies: false,
    }
}

/// Upstream `HashtagChannelService.init` parity: `q` must be an array of
/// non-empty string arrays (AND-of-ORs). Anything else (missing, wrong
/// shape, empty inner list, non-string member) is `None` → silent refusal.
fn parse_hashtag_q(q: Option<&Value>) -> Option<Vec<Vec<String>>> {
    let outer = q?.as_array()?;
    let mut out = Vec::with_capacity(outer.len());
    for group in outer {
        let inner = group.as_array()?;
        if inner.is_empty() {
            return None;
        }
        let mut tags = Vec::with_capacity(inner.len());
        for tag in inner {
            tags.push(tag.as_str()?.to_string());
        }
        out.push(tags);
    }
    Some(out)
}

/// `connected {id}` ack for `connect` with `pong: true` (Misskey parity).
async fn send_pong(
    body: Option<&Value>,
    id: &str,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) {
    let pong = body
        .and_then(|b| b.get("pong"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if pong {
        let frame = Message::Text(
            serde_json::json!({ "type": "connected", "body": { "id": id } })
                .to_string()
                .into(),
        );
        let _ = sender.send(frame).await;
    }
}

/// `serverStats` sample (mirrors the `server-info` endpoint shape:
/// machine/cpu/mem from stable Rust + `/proc`; `fs` totals are 0 without
/// `statvfs` — same DIVERGENCE, recorded on the endpoint).
fn server_stats_snapshot() -> Value {
    let machine = format!("{} {}", std::env::consts::OS, std::env::consts::ARCH);
    let cores = std::thread::available_parallelism()
        .map(|n| n.get() as u64)
        .unwrap_or(1);
    let mem_total = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                if key.trim() == "MemTotal" {
                    value
                        .trim()
                        .strip_suffix("kB")
                        .and_then(|n| n.trim().parse::<u64>().ok())
                        .map(|kb| kb * 1024)
                } else {
                    None
                }
            })
        })
        .unwrap_or(0);
    serde_json::json!({
        "machine": machine,
        "cpu": { "cores": cores },
        "mem": { "total": mem_total },
        "fs": { "total": 0, "used": 0 },
    })
}

/// `queueStats` sample: pending (claimable + delayed) jobs in the single
/// durable queue (DIVERGENCE: upstream reports per-queue
/// `{active, waiting, delayed}` for `deliver`/`inbox`; pinetree has one
/// queue and no active-job accounting, so `waiting` is the honest count).
fn queue_stats_snapshot(services: &crate::application::commands::AppServices) -> Value {
    let waiting = services.jobs.pending_count().unwrap_or(0);
    serde_json::json!({ "waiting": waiting })
}

/// `requestLog` on a `serverStats`/`queueStats` channel: answer one
/// `statsLog` sample (upstream replays the stored series; pinetree keeps
/// no series, so the current sample stands in — recorded DIVERGENCE).
async fn handle_stats_log_request(
    state: &ApiState,
    channels: &HashMap<String, ChannelSub>,
    channel_id: &str,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) {
    match channels.get(channel_id).map(|sub| &sub.kind) {
        Some(ChannelKind::ServerStats) => {
            let frame = channel_frame(channel_id, "statsLog", server_stats_snapshot());
            let _ = sender.send(frame).await;
        }
        Some(ChannelKind::QueueStats) => {
            let services = state.services.clone();
            let snapshot = tokio::task::spawn_blocking(move || queue_stats_snapshot(&services))
                .await
                .unwrap_or_else(|_| serde_json::json!({ "waiting": 0 }));
            let frame = channel_frame(channel_id, "statsLog", snapshot);
            let _ = sender.send(frame).await;
        }
        _ => {}
    }
}

/// `GET /streaming` upgrade handler.
pub async fn streaming_handler(
    State(state): State<ApiState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let raw = token_for_upgrade(&headers, &query);
    let caller = match raw {
        None => None,
        Some(token) => {
            let services = state.services.clone();
            let resolved =
                tokio::task::spawn_blocking(move || authenticate(&services, &token)).await;
            match resolved {
                Err(_) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "streaming auth task failed",
                    )
                        .into_response();
                }
                Ok(Err(err)) => return err.into_response(),
                Ok(Ok(authenticated)) => {
                    // Misskey answers 401 (not 403) when the app token may
                    // not use the streaming API at all.
                    if authenticated.require_permission("read:account").is_err() {
                        return ApiError::authentication_failed().into_response();
                    }
                    Some(CallerInfo {
                        account_id: authenticated.account.id,
                        is_first_party: authenticated.is_first_party,
                    })
                }
            }
        }
    };
    ws.max_message_size(MAX_WS_MESSAGE_BYTES)
        .on_upgrade(move |socket| run_connection(socket, state, caller))
}

fn channel_frame(channel_id: &str, event: &str, body: Value) -> Message {
    Message::Text(
        serde_json::json!({
            "type": "channel",
            "body": { "id": channel_id, "type": event, "body": body },
        })
        .to_string()
        .into(),
    )
}

fn note_updated_frame(note_id: &str, event: &str, body: Value) -> Message {
    Message::Text(
        serde_json::json!({
            "type": "noteUpdated",
            "body": { "id": note_id, "type": event, "body": body },
        })
        .to_string()
        .into(),
    )
}

async fn run_connection(socket: WebSocket, state: ApiState, caller: Option<CallerInfo>) {
    let viewer_id = caller.as_ref().map(|c| c.account_id.clone());
    let (mut sender, mut receiver) = socket.split();
    let mut hub_rx = state.hub.subscribe();
    let mut channels: HashMap<String, ChannelSub> = HashMap::new();
    let mut captures: HashMap<String, u32> = HashMap::new();
    let mut capture_order: VecDeque<String> = VecDeque::new();
    let mut last_activity = Instant::now();
    let mut pings = interval(PING_EVERY);
    // Batch P presence (`get-online-users-count`): an authenticated socket
    // marks its account present on connect, refreshes on every inbound
    // frame, and unmarks on close.
    if let Some(caller) = caller.as_ref() {
        state
            .hub
            .mark_present(&caller.account_id, state.services.clock.now_ms());
    }

    macro_rules! send {
        ($msg:expr) => {
            if sender.send($msg).await.is_err() {
                break;
            }
        };
    }

    loop {
        tokio::select! {
            biased;
            incoming = receiver.next() => {
                let Some(incoming) = incoming else { break };
                let Ok(msg) = incoming else { break };
                last_activity = Instant::now();
                if let Some(caller) = caller.as_ref() {
                    state
                        .hub
                        .mark_present(&caller.account_id, state.services.clock.now_ms());
                }
                match msg {
                    Message::Text(text) => {
                        handle_client_message(
                            &state, &caller, &mut channels,
                            &mut captures, &mut capture_order,
                            &text, &mut sender,
                        ).await;
                    }
                    Message::Close(_) => break,
                    Message::Pong(_) => {}
                    // Binary and Ping frames carry no protocol: Misskey
                    // ignores them (axum auto-answers Ping).
                    _ => {}
                }
            }
            hub_msg = hub_rx.recv() => {
                match hub_msg {
                    // WP3 stream close: a suspended account's open sockets
                    // drop (the durable `UserSuspended` job fans out through
                    // the hub). Other viewers — and anonymous sockets —
                    // ignore the event.
                    Ok(HubMessage::UserSuspended { account_id }) => {
                        if viewer_id.as_deref() == Some(account_id.as_str()) {
                            break;
                        }
                    }
                    Ok(msg) => {
                        dispatch_hub_message(
                            &state, &caller, viewer_id.as_deref(),
                            &channels, &captures, msg, &mut sender,
                        ).await;
                    }
                    // Lagged past the hub backlog: drop and resubscribe,
                    // like a reconnect (at-most-once live hints).
                    Err(_) => { hub_rx = state.hub.subscribe(); }
                }
            }
            _ = pings.tick() => {
                if last_activity.elapsed() > IDLE_REAP_AFTER {
                    break;
                }
                send!(Message::Ping(Vec::new().into()));
            }
        }
    }
    let _ = sender.close().await;
    if let Some(caller) = caller.as_ref() {
        state.hub.mark_gone(&caller.account_id);
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_client_message(
    state: &ApiState,
    caller: &Option<CallerInfo>,
    channels: &mut HashMap<String, ChannelSub>,
    captures: &mut HashMap<String, u32>,
    capture_order: &mut VecDeque<String>,
    text: &str,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) {
    let parsed: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };
    let msg_type = parsed.get("type").and_then(Value::as_str).unwrap_or("");
    let body = parsed.get("body");
    match msg_type {
        "connect" => {
            let Some(obj) = body.and_then(Value::as_object) else {
                return;
            };
            let (Some(id), Some(channel)) = (
                obj.get("id").and_then(Value::as_str),
                obj.get("channel").and_then(Value::as_str),
            ) else {
                return;
            };
            connect_channel(state, caller, channels, id, channel, body, sender).await;
        }
        "disconnect" => {
            if let Some(id) = body.and_then(|b| b.get("id")).and_then(Value::as_str) {
                channels.remove(id);
            }
        }
        "channel" | "ch" => {
            let Some(obj) = body.and_then(Value::as_object) else {
                return;
            };
            match obj.get("type").and_then(Value::as_str) {
                Some("readNotification") => {
                    read_all_notifications(state, caller).await;
                }
                // `serverStats`/`queueStats` log demand (upstream
                // `onMessage('requestLog', ...)` parity): `{id, type,
                // body?}` where `id` is the channel id.
                Some("requestLog") => {
                    let channel_id =
                        obj.get("id").and_then(Value::as_str).unwrap_or("");
                    if !channel_id.is_empty() {
                        handle_stats_log_request(state, channels, channel_id, sender).await;
                    }
                }
                _ => {}
            }
        }
        "readNotification" => {
            read_all_notifications(state, caller).await;
        }
        "subNote" | "s" | "sr" => {
            let Some(id) = body.and_then(|b| b.get("id")).and_then(Value::as_str) else {
                return;
            };
            let count = captures.get(id).copied().unwrap_or(0);
            if count == 0 {
                if captures.len() >= MAX_CAPTURED_NOTES {
                    if let Some(oldest) = capture_order.pop_front() {
                        captures.remove(&oldest);
                    }
                }
                capture_order.push_back(id.to_string());
            } else {
                // Refresh LRU position.
                capture_order.retain(|x| x != id);
                capture_order.push_back(id.to_string());
            }
            captures.insert(id.to_string(), count + 1);
        }
        "unsubNote" | "un" => {
            let Some(id) = body.and_then(|b| b.get("id")).and_then(Value::as_str) else {
                return;
            };
            match captures.get(id).copied() {
                None => {}
                Some(1) => {
                    captures.remove(id);
                    capture_order.retain(|x| x != id);
                }
                Some(n) => {
                    captures.insert(id.to_string(), n - 1);
                }
            }
        }
        _ => {}
    }
}

async fn connect_channel(
    state: &ApiState,
    caller: &Option<CallerInfo>,
    channels: &mut HashMap<String, ChannelSub>,
    id: &str,
    channel: &str,
    body: Option<&Value>,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) {
    // The `antenna`, `userList`, and `clip` channels need the owner check
    // up front (credential + owned id); every other channel resolves below.
    if channel == "antenna" || channel == "userList" || channel == "clip" {
        let (kind, owner_ok) = if channel == "clip" {
            let clip_id = body
                .and_then(|b| b.get("params"))
                .and_then(|p| p.get("clipId"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if clip_id.is_empty() {
                return;
            }
            let viewer = caller.as_ref().map(|c| c.account_id.clone());
            let services = state.services.clone();
            let clip_query = clip_id.clone();
            // Public clips allow anonymous subscribers (upstream parity);
            // private clips need the owner. Gone clips refuse silently.
            let allowed = tokio::task::spawn_blocking(move || {
                services
                    .clips
                    .get_clip(&clip_query)
                    .map(|row| {
                        row.is_some_and(|c| {
                            c.is_public || viewer.as_deref() == Some(c.owner_id.as_str())
                        })
                    })
                    .unwrap_or(false)
            })
            .await
            .unwrap_or(false);
            (ChannelKind::Clip { clip_id }, allowed)
        } else {
            let Some(caller) = caller else {
                return;
            };
            let param_key = if channel == "antenna" { "antennaId" } else { "listId" };
            let target_id = body
                .and_then(|b| b.get("params"))
                .and_then(|p| p.get(param_key))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if target_id.is_empty() {
                return;
            }
            let services = state.services.clone();
            let owner = caller.account_id.clone();
            let target_query = target_id.clone();
            let is_antenna = channel == "antenna";
            let owned = tokio::task::spawn_blocking(move || {
                if is_antenna {
                    services
                        .antennas
                        .get_antenna(&owner, &target_query)
                        .map(|row| row.is_some())
                        .unwrap_or(false)
                } else {
                    services
                        .lists
                        .get_user_list(&target_query)
                        .map(|row| row.is_some_and(|l| l.owner_id == owner))
                        .unwrap_or(false)
                }
            })
            .await
            .unwrap_or(false);
            (
                if is_antenna {
                    ChannelKind::Antenna { antenna_id: target_id }
                } else {
                    ChannelKind::UserList { list_id: target_id }
                },
                owned,
            )
        };
        if !owner_ok {
            // Unknown channel on connect answers UNSUPPORTED_CHANNEL, but a
            // foreign antenna/list or a private/gone clip is an auth refusal:
            // silently ignored (Misskey credential-channel parity — the
            // channel is never added).
            return;
        }
        if channels.contains_key(id) {
            channels.remove(id);
        }
        if channels.len() >= MAX_CHANNELS_PER_CONNECTION {
            return;
        }
        let params = if matches!(kind, ChannelKind::UserList { .. }) {
            user_list_params(body.and_then(|b| b.get("params")))
        } else {
            TimelineChannelParams::default()
        };
        channels.insert(id.to_string(), ChannelSub { kind, params });
        send_pong(body, id, sender).await;
        return;
    }
    let kind = match channel {
        "main" => ChannelKind::Main,
        "homeTimeline" => ChannelKind::Timeline(TimelineChannel::Home),
        "localTimeline" => ChannelKind::Timeline(TimelineChannel::Local),
        "hybridTimeline" => ChannelKind::Timeline(TimelineChannel::Hybrid),
        "globalTimeline" => ChannelKind::Timeline(TimelineChannel::Global),
        "hashtag" => {
            // Upstream `HashtagChannelService.init` parity: `q` must be an
            // array of non-empty string arrays; anything else refuses the
            // channel silently (like a foreign antenna/list — never an
            // `UNSUPPORTED_CHANNEL`, which is reserved for unknown names).
            let q = body
                .and_then(|b| b.get("params"))
                .and_then(|p| p.get("q"));
            match parse_hashtag_q(q) {
                Some(q) => ChannelKind::Hashtag { q },
                None => return,
            }
        }
        "roleTimeline" => {
            let role_id = body
                .and_then(|b| b.get("params"))
                .and_then(|p| p.get("roleId"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if role_id.is_empty() {
                return;
            }
            ChannelKind::RoleTimeline { role_id }
        }
        "drive" => ChannelKind::Drive,
        "serverStats" => ChannelKind::ServerStats,
        "queueStats" => ChannelKind::QueueStats,
        "admin" => {
            // Upstream `kind = 'read:admin:stream'` parity, mapped onto the
            // fixed admin/moderator flags (no custom roles): non-moderators
            // are refused silently, like foreign antennas/lists.
            let Some(caller) = caller else {
                return;
            };
            let services = state.services.clone();
            let viewer = caller.account_id.clone();
            let allowed = tokio::task::spawn_blocking(move || {
                services
                    .viewer_is_moderator(Some(&viewer))
                    .unwrap_or(false)
            })
            .await
            .unwrap_or(false);
            if !allowed {
                return;
            }
            ChannelKind::Admin
        }
        _ => {
            // `reversi`, `reversiGame`, `channel`, `chatUser`, `chatRoom`
            // are KNOWN upstream names answered `UNSUPPORTED_CHANNEL` here
            // by decision (documented in the module docs): pinetree ships
            // no channels/chat/Reversi model, so there is nothing to
            // subscribe to — the REST surfaces for those families ship
            // instead (gaps WP2) so clients stop 404ing.
            let frame = Message::Text(
                serde_json::json!({
                    "type": "error",
                    "body": {
                        "code": "UNSUPPORTED_CHANNEL",
                        "channel": channel,
                        "id": id,
                    },
                })
                .to_string()
                .into(),
            );
            let _ = sender.send(frame).await;
            return;
        }
    };
    // Credential channels need an authenticated caller (Misskey parity:
    // refused connects are silently ignored). (`admin` additionally needed
    // the moderator check above; `userList`/`antenna`/`clip` checked up
    // front.)
    if matches!(
        kind,
        ChannelKind::Main
            | ChannelKind::Timeline(TimelineChannel::Home | TimelineChannel::Hybrid)
            | ChannelKind::Drive
            | ChannelKind::Admin
    ) && caller.is_none()
    {
        return;
    }
    // `main` is shared: a second connect is a no-op (Misskey parity).
    if matches!(kind, ChannelKind::Main)
        && channels.values().any(|c| matches!(c.kind, ChannelKind::Main))
    {
        return;
    }
    if channels.contains_key(id) {
        channels.remove(id);
    }
    if channels.len() >= MAX_CHANNELS_PER_CONNECTION {
        return;
    }
    let params = body
        .and_then(|b| b.get("params"))
        .map(|p| channel_params(&kind, Some(p)))
        .unwrap_or_else(|| channel_params(&kind, None));
    channels.insert(
        id.to_string(),
        ChannelSub { kind: kind.clone(), params },
    );
    // Snapshot channels emit their current sample on connect (DIVERGENCE:
    // upstream broadcasts on a 1 s Xev tick; pinetree has no broadcast bus,
    // so clients get one snapshot now + `statsLog` on `requestLog`).
    match &kind {
        ChannelKind::ServerStats => {
            let frame = channel_frame(id, "stats", server_stats_snapshot());
            let _ = sender.send(frame).await;
        }
        ChannelKind::QueueStats => {
            let services = state.services.clone();
            let snapshot = tokio::task::spawn_blocking(move || queue_stats_snapshot(&services))
                .await
                .unwrap_or_else(|_| serde_json::json!({ "waiting": 0 }));
            let frame = channel_frame(id, "stats", snapshot);
            let _ = sender.send(frame).await;
        }
        _ => {}
    }
    send_pong(body, id, sender).await;
}

/// Shared read-time gate for the `hashtag`/`roleTimeline` note fan-out
/// (upstream `isNoteVisibleForMe` + `NoteStreamingHidingService` +
/// `requireSigninToViewContents` parity): visibility for the viewer, the
/// §0A/§12 mute/block/suspend/host/mute-instance/word-mute filter, and —
/// for anonymous viewers — the signin-to-view-contents trio (note author,
/// reply target author, renote target author). Runs on the blocking pool.
/// Fail-closed: any storage fault hides the note.
fn note_passes_stream_gate(
    services: &crate::application::commands::AppServices,
    viewer_id: Option<&str>,
    note: &crate::domain::note::Note,
) -> bool {
    use crate::domain::policy::{ViewerContext, is_visible};
    let ctx = match viewer_id {
        None => ViewerContext::anonymous(),
        Some(viewer) => {
            let follows_author = viewer == note.author_id
                || services
                    .follows
                    .is_following(viewer, &note.author_id)
                    .unwrap_or(false);
            ViewerContext::local_user(viewer, follows_author)
        }
    };
    if !is_visible(note, &ctx) {
        return false;
    }
    let filter = match services.feed_filter(viewer_id) {
        Ok(filter) => filter,
        Err(_) => return false,
    };
    let mut account_cache = std::collections::HashMap::new();
    let mut note_cache = std::collections::HashMap::new();
    if services
        .feed_filter_hides(&filter, note, &mut account_cache, &mut note_cache)
        .unwrap_or(true)
    {
        return false;
    }
    if viewer_id.is_none() {
        let mut author_ids = vec![note.author_id.clone()];
        for target_id in note.reply_id.iter().chain(note.renote_id.iter()) {
            if let Ok(Some(target)) = services.notes.get_note(target_id) {
                author_ids.push(target.author_id);
            }
        }
        for author_id in author_ids {
            match services.accounts.find_by_id(&author_id) {
                Ok(Some(account)) if account.require_signin_to_view_contents => {
                    return false;
                }
                Ok(_) => {}
                Err(_) => return false,
            }
        }
    }
    true
}

/// `reacted.emoji` detail (upstream `NoteEventTypes['reacted']` parity:
/// `{name, url} | null`). Unicode reactions carry no emoji (`null`);
/// `:shortcode:` resolves against the local emoji store, `:name@host:`
/// against the remote metadata (no fetch on the socket path). Unknown or
/// unreadable emoji fail open to `null` — the reaction event itself is
/// never dropped.
async fn resolve_reaction_emoji(state: &ApiState, reaction: &str) -> Value {
    let inner = match reaction
        .strip_prefix(':')
        .and_then(|s| s.strip_suffix(':'))
    {
        Some(inner) if !inner.is_empty() => inner.to_string(),
        _ => return Value::Null,
    };
    let services = state.services.clone();
    tokio::task::spawn_blocking(move || {
        let (name, host) = match inner.split_once('@') {
            Some((name, host)) => (name.to_string(), Some(host.to_string())),
            None => (inner, None),
        };
        if name.is_empty() {
            return Value::Null;
        }
        match host {
            Some(host) if !host.is_empty() => {
                match services.emojis.get_remote_emoji(&host, &name) {
                    Ok(Some(row)) => serde_json::json!({
                        "name": format!("{name}@{host}"),
                        "url": row.remote_url,
                    }),
                    _ => Value::Null,
                }
            }
            _ => match services.emojis.get_emoji_by_name(&name) {
                Ok(Some(emoji)) => {
                    let base = services.public_url.trim_end_matches('/');
                    let url = match emoji.media_id.as_deref() {
                        Some(media_id) => services
                            .media
                            .get(media_id)
                            .ok()
                            .flatten()
                            .map(|meta| meta.url)
                            .unwrap_or_else(|| {
                                format!("{base}/emojis/{}", emoji.shortcode)
                            }),
                        None => format!("{base}/emojis/{}", emoji.shortcode),
                    };
                    serde_json::json!({ "name": emoji.shortcode, "url": url })
                }
                _ => Value::Null,
            },
        }
    })
    .await
    .unwrap_or(Value::Null)
}
/// Streaming `readNotification`: mark everything read, then emit
/// `readAllNotifications` on the caller's `main` channels (Misskey
/// `NotificationService.readAllNotification` parity). Anonymous callers
/// have no notifications to read.
async fn read_all_notifications(state: &ApiState, caller: &Option<CallerInfo>) {
    let Some(caller) = caller else { return };
    let services = state.services.clone();
    let account_id = caller.account_id.clone();
    let marked = tokio::task::spawn_blocking(move || {
        services.notifications.mark_all_read(&account_id)
    })
    .await;
    if marked.is_ok() {
        let _ = state.services.streaming.push_main(MainMessage {
            user_id: caller.account_id.clone(),
            kind: MainEventKind::ReadAllNotifications,
            body: Value::Null,
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_hub_message(
    state: &ApiState,
    caller: &Option<CallerInfo>,
    viewer_id: Option<&str>,
    channels: &HashMap<String, ChannelSub>,
    captures: &HashMap<String, u32>,
    msg: HubMessage,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) {
    match msg {
        HubMessage::Note { note_id } => {
            let timeline: Vec<(String, ChannelKind, TimelineChannelParams)> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::Timeline(which) => {
                        Some((id.clone(), ChannelKind::Timeline(*which), sub.params))
                    }
                    ChannelKind::Main
                    | ChannelKind::Antenna { .. }
                    | ChannelKind::UserList { .. }
                    | ChannelKind::Hashtag { .. }
                    | ChannelKind::RoleTimeline { .. }
                    | ChannelKind::Drive
                    | ChannelKind::ServerStats
                    | ChannelKind::QueueStats
                    | ChannelKind::Admin
                    | ChannelKind::Clip { .. } => None,
                })
                .collect();
            let lists: Vec<(String, String, TimelineChannelParams)> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::UserList { list_id } => {
                        Some((id.clone(), list_id.clone(), sub.params))
                    }
                    _ => None,
                })
                .collect();
            let hashtags: Vec<(String, Vec<Vec<String>>)> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::Hashtag { q } => Some((id.clone(), q.clone())),
                    _ => None,
                })
                .collect();
            if timeline.is_empty() && lists.is_empty() && hashtags.is_empty() {
                return;
            }
            if !timeline.is_empty() {
            let services = state.services.clone();
            let viewer = viewer_id.map(|s| s.to_string());
            let note_id_timeline = note_id.clone();
            let packed = tokio::task::spawn_blocking(move || {
                let note = match services.notes.get_note(&note_id_timeline) {
                    Ok(Some(note)) => note,
                    _ => return None,
                };
                // Single eligibility pass: collect every eligible channel,
                // then pack once per viewer for all of them.
                let mut ids: Vec<String> = Vec::new();
                for (id, kind, params) in &timeline {
                    if let ChannelKind::Timeline(which) = kind {
                        if services
                            .note_eligible_for_channel(*which, viewer.as_deref(), &note, params)
                            .unwrap_or(false)
                        {
                            ids.push(id.clone());
                        }
                    }
                }
                if ids.is_empty() {
                    return None;
                }
                let packed =
                    crate::api::misskey::packers::pack_note(&note, viewer.as_deref(), &services, 0)
                        .ok()?;
                Some((ids, packed))
            })
            .await
            .ok()
            .flatten();
            if let Some((ids, packed)) = packed {
                for id in ids {
                    let frame = channel_frame(&id, "note", packed.clone());
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
            }
            }
            // Batch O WP16: fan the note out to this connection's `userList`
            // channels whose list contains the author. Membership is
            // re-read per event (no 5s poll); the read-time reply rule,
            // visibility, and mute/block filters apply (upstream `onNote`
            // parity, pull-model).
            if !lists.is_empty() {
                let services = state.services.clone();
                let viewer = viewer_id.map(|s| s.to_string());
                let note_id_owned = note_id.clone();
                let packed = tokio::task::spawn_blocking(move || {
                    let note = match services.notes.get_note(&note_id_owned) {
                        Ok(Some(note)) if !note.is_deleted => note,
                        _ => return None,
                    };
                    let viewer_ref = viewer.as_deref();
                    let filter = services.feed_filter(viewer_ref).ok()?;
                    let mut account_cache = std::collections::HashMap::new();
                    let mut note_cache = std::collections::HashMap::new();
                    let follows_cache = services.follows_cache(viewer_ref).ok()?;
                    let viewer_ctx =
                        services.viewer_ctx(viewer_ref, &note.author_id, &follows_cache);
                    if !crate::domain::policy::is_visible(&note, &viewer_ctx) {
                        return None;
                    }
                    if services
                        .feed_filter_hides(&filter, &note, &mut account_cache, &mut note_cache)
                        .unwrap_or(true)
                    {
                        return None;
                    }
                    let mut ids: Vec<String> = Vec::new();
                    for (id, list_id, params) in &lists {
                        // Upstream `userList` include flags (WP4): `withFiles`
                        // drops file-less notes, `withRenotes: false` drops
                        // pure renotes (quotes carry content and survive).
                        if params.with_files && note.file_ids.is_empty() {
                            continue;
                        }
                        if !params.with_renotes
                            && note.renote_id.is_some()
                            && note.text.is_none()
                            && note.cw.is_none()
                            && note.file_ids.is_empty()
                        {
                            continue;
                        }
                        let list = services.lists.get_user_list(list_id).ok()??;
                        let membership = services
                            .lists
                            .get_list_membership(list_id, &note.author_id)
                            .ok()??;
                        let reply_ok = match note.reply_id.as_deref() {
                            None => true,
                            Some(_) => {
                                let reply_to_user = note.reply_user_id.as_deref().unwrap_or("");
                                reply_to_user.is_empty()
                                    || reply_to_user == note.author_id
                                    || reply_to_user == list.owner_id
                                    || membership.with_replies
                            }
                        };
                        if reply_ok {
                            ids.push(id.clone());
                        }
                    }
                    if ids.is_empty() {
                        return None;
                    }
                    let packed =
                        crate::api::misskey::packers::pack_note(&note, viewer_ref, &services, 0)
                            .ok()?;
                    Some((ids, packed))
                })
                .await
                .ok()
                .flatten();
                if let Some((ids, packed)) = packed {
                    for id in ids {
                        let frame = channel_frame(&id, "note", packed.clone());
                        if sender.send(frame).await.is_err() {
                            return;
                        }
                    }
                }
            }
            // WP4 `hashtag` fan-out (upstream `HashtagChannelService.onNote`
            // parity): AND-of-OR tag match over the note's lowercased tags,
            // then the shared stream gate (visibility + mute/block + the
            // `requireSigninToViewContents` trio), packed once per viewer.
            if !hashtags.is_empty() {
                let services = state.services.clone();
                let viewer = viewer_id.map(|s| s.to_string());
                let note_id_owned = note_id.clone();
                let packed = tokio::task::spawn_blocking(move || {
                    let note = match services.notes.get_note(&note_id_owned) {
                        Ok(Some(note)) if !note.is_deleted => note,
                        _ => return None,
                    };
                    let tags: std::collections::HashSet<String> =
                        note.tags.iter().map(|t| t.to_lowercase()).collect();
                    let mut ids: Vec<String> = Vec::new();
                    for (id, q) in &hashtags {
                        if q.iter().any(|ands| {
                            ands.iter().all(|t| tags.contains(&t.to_lowercase()))
                        }) {
                            ids.push(id.clone());
                        }
                    }
                    if ids.is_empty() {
                        return None;
                    }
                    if !note_passes_stream_gate(&services, viewer.as_deref(), &note) {
                        return None;
                    }
                    let packed =
                        crate::api::misskey::packers::pack_note(&note, viewer.as_deref(), &services, 0)
                            .ok()?;
                    Some((ids, packed))
                })
                .await
                .ok()
                .flatten();
                if let Some((ids, packed)) = packed {
                    for id in ids {
                        let frame = channel_frame(&id, "note", packed.clone());
                        if sender.send(frame).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
        HubMessage::RoleTimelineNote { role_id, note_id } => {
            // WP4 `roleTimeline` fan-out (upstream
            // `RoleTimelineChannelService.onEvent` parity): only `public`
            // notes pass (the channel re-checks on every event), then the
            // shared stream gate; packed once per viewer.
            let targets: Vec<String> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::RoleTimeline { role_id: mine } if mine == &role_id => {
                        Some(id.clone())
                    }
                    _ => None,
                })
                .collect();
            if targets.is_empty() {
                return;
            }
            let services = state.services.clone();
            let viewer = viewer_id.map(|s| s.to_string());
            let packed = tokio::task::spawn_blocking(move || {
                let note = match services.notes.get_note(&note_id) {
                    Ok(Some(note)) if !note.is_deleted => note,
                    _ => return None,
                };
                if !matches!(
                    note.visibility,
                    crate::domain::note::Visibility::Public
                ) {
                    return None;
                }
                if !note_passes_stream_gate(&services, viewer.as_deref(), &note) {
                    return None;
                }
                crate::api::misskey::packers::pack_note(&note, viewer.as_deref(), &services, 0).ok()
            })
            .await
            .ok()
            .flatten();
            if let Some(packed) = packed {
                for id in targets {
                    let frame = channel_frame(&id, "note", packed.clone());
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        }
        HubMessage::DriveFile { user_id, event, file_id } => {
            // WP4 `drive` channel (upstream `DriveChannel.onData` parity):
            // per-user file events, packed at socket time from the file
            // row. Deletes carry only the id (the row is already gone).
            if viewer_id != Some(user_id.as_str()) {
                return;
            }
            let targets: Vec<String> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::Drive => Some(id.clone()),
                    _ => None,
                })
                .collect();
            if targets.is_empty() {
                return;
            }
            let name = event.wire_name();
            if matches!(
                event,
                crate::streaming::hub::DriveFileEvent::Deleted
            ) {
                // No content can leak in a deletion notice (the row is
                // gone), so subscribers get it unconditionally.
                for id in targets {
                    let frame =
                        channel_frame(&id, name, Value::String(file_id.clone()));
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
                return;
            }
            let services = state.services.clone();
            let packed = tokio::task::spawn_blocking(move || {
                let meta = services.media.get(&file_id).ok()??;
                Some(crate::api::misskey::packers::pack_drive_file(&meta, &services))
            })
            .await
            .ok()
            .flatten();
            if let Some(packed) = packed {
                for id in targets {
                    let frame = channel_frame(&id, name, packed.clone());
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        }
        HubMessage::DriveFolder { user_id, event, folder_id } => {
            // Folder events carry `{id}` until the WP2 folder packer lands
            // (recorded DIVERGENCE — upstream sends the packed folder).
            if viewer_id != Some(user_id.as_str()) {
                return;
            }
            let targets: Vec<String> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::Drive => Some(id.clone()),
                    _ => None,
                })
                .collect();
            if targets.is_empty() {
                return;
            }
            let body = serde_json::json!({ "id": folder_id });
            for id in targets {
                let frame = channel_frame(&id, event.wire_name(), body.clone());
                if sender.send(frame).await.is_err() {
                    return;
                }
            }
        }
        HubMessage::Clip { clip_id, deleted } => {
            // WP4 `clip` channel (upstream `ClipChannel.onEvent` parity):
            // `updated`/`deleted`, both with a null body. Connect-time
            // visibility was already checked; no per-event recheck.
            let targets: Vec<String> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::Clip { clip_id: mine } if mine == &clip_id => {
                        Some(id.clone())
                    }
                    _ => None,
                })
                .collect();
            if targets.is_empty() {
                return;
            }
            let event = if deleted { "deleted" } else { "updated" };
            for id in targets {
                let frame = channel_frame(&id, event, Value::Null);
                if sender.send(frame).await.is_err() {
                    return;
                }
            }
        }
        HubMessage::AdminReport { report_id } => {
            // WP4 `admin` channel (upstream `AdminChannel.onData` parity):
            // `newAbuseUserReport` with the report identifiers, packed at
            // socket time. Every connection reaching this arm passed the
            // moderator check at connect.
            let targets: Vec<String> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::Admin => Some(id.clone()),
                    _ => None,
                })
                .collect();
            if targets.is_empty() {
                return;
            }
            let services = state.services.clone();
            let packed = tokio::task::spawn_blocking(move || {
                let report = services.abuse_reports.get(&report_id).ok()??;
                Some(serde_json::json!({
                    "id": report.id,
                    "targetUserId": report.target_account,
                    "reporterId": report.reporter,
                    "comment": report.comment,
                }))
            })
            .await
            .ok()
            .flatten();
            if let Some(packed) = packed {
                for id in targets {
                    let frame = channel_frame(&id, "newAbuseUserReport", packed.clone());
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        }
        HubMessage::NoteReacted {
            note_id,
            user_id,
            reaction,
        } => {
            // WP4: `reacted` carries the `emoji` detail (upstream
            // `NoteEventTypes['reacted']` parity: `{reaction, emoji, userId}`
            // where `emoji` is `{name, url} | null`).
            let emoji = resolve_reaction_emoji(state, &reaction).await;
            push_note_updated(
                state, viewer_id, captures, &note_id, "reacted",
                serde_json::json!({ "reaction": reaction, "userId": user_id, "emoji": emoji }),
                sender,
            )
            .await;
        }
        HubMessage::NoteUnreacted {
            note_id,
            user_id,
            reaction,
        } => {
            push_note_updated(
                state, viewer_id, captures, &note_id, "unreacted",
                serde_json::json!({ "reaction": reaction, "userId": user_id }),
                sender,
            )
            .await;
        }
        HubMessage::NoteDeleted { note_id } => {
            if !captures.contains_key(&note_id) {
                return;
            }
            // No content can leak in a deletion notice, so subscribers get
            // it unconditionally (the note itself is already gone).
            let deleted_at = crate::api::misskey::packers::format_millis(
                state.services.clock.now_ms(),
            );
            let frame = note_updated_frame(
                &note_id,
                "deleted",
                serde_json::json!({ "deletedAt": deleted_at }),
            );
            let _ = sender.send(frame).await;
        }
        HubMessage::PollVoted {
            note_id,
            user_id,
            choice,
        } => {
            push_note_updated(
                state, viewer_id, captures, &note_id, "pollVoted",
                serde_json::json!({ "choice": choice, "userId": user_id }),
                sender,
            )
            .await;
        }
        HubMessage::PollEnded { note_id } => {
            push_note_updated(
                state,
                viewer_id,
                captures,
                &note_id,
                "pollEnded",
                serde_json::json!({}),
                sender,
            )
            .await;
        }
        HubMessage::NoteUpdated { note_id } => {
            // Sharkey `publishNoteStream(note.id, 'updated', ...)` parity:
            // the frame carries the current text/cw (a gone note loads as
            // no frame; `push_note_updated` skips uncaptured or invisible
            // notes anyway).
            let services = state.services.clone();
            let note_id_owned = note_id.clone();
            let loaded = tokio::task::spawn_blocking(move || {
                services.notes.get_note(&note_id_owned)
            })
            .await
            .unwrap_or(Ok(None))
            .unwrap_or(None);
            if let Some(note) = loaded {
                push_note_updated(
                    state,
                    viewer_id,
                    captures,
                    &note_id,
                    "updated",
                    serde_json::json!({
                        "cw": note.cw,
                        "text": note.text.unwrap_or_default(),
                    }),
                    sender,
                )
                .await;
            }
        }
        HubMessage::Main(msg) => {
            let me = match viewer_id {
                Some(v) => v,
                None => return,
            };
            if msg.user_id != me {
                return;
            }
            let mains: Vec<String> = channels
                .iter()
                .filter(|(_, sub)| matches!(sub.kind, ChannelKind::Main))
                .map(|(id, _)| id.clone())
                .collect();
            if mains.is_empty() {
                return;
            }
            let event = msg.kind.wire_name().to_string();
            let body = match msg.kind {
                MainEventKind::Mention | MainEventKind::Reply | MainEventKind::Renote => {
                    // Re-gate at socket time: the note may have been deleted
                    // or narrowed between enqueue and delivery (the timeline
                    // path re-checks eligibility; main events must too).
                    let note_id = msg
                        .body
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let services = state.services.clone();
                    let viewer = me.to_string();
                    let visible = tokio::task::spawn_blocking(move || {
                        services.notes_show(&note_id, Some(&viewer)).is_ok()
                    })
                    .await
                    .unwrap_or(false);
                    if !visible {
                        return;
                    }
                    msg.body
                }
                // Packed on receipt so `MeDetailed` honors this
                // connection's own token (first-party secrets stay scoped).
                MainEventKind::MeUpdated => {
                    let services = state.services.clone();
                    let account_id = msg.user_id.clone();
                    let first_party = caller.as_ref().map(|c| c.is_first_party).unwrap_or(false);
                    match tokio::task::spawn_blocking(move || {
                        let account = services.accounts.find_by_id(&account_id).ok()??;
                        pack_me_detailed(&account, first_party, &services).ok()
                    })
                    .await
                    {
                        Ok(Some(packed)) => packed,
                        _ => return,
                    }
                }
                _ => msg.body,
            };
            for id in mains {
                let frame = channel_frame(&id, &event, body.clone());
                if sender.send(frame).await.is_err() {
                    return;
                }
            }
        }
        // Filtered out before dispatch: `run_connection` closes the socket
        // when the suspended account is this connection's viewer. The arm
        // exists for exhaustiveness only.
        HubMessage::UserSuspended { .. } => {}
        // Custom-emoji broadcasts go to every connection with no channel
        // filter (Misskey `onBroadcastMessage` parity: `{type, body}` at
        // the top level, not wrapped in a channel frame).
        HubMessage::EmojiBroadcast { event, body } => {
            let frame = Message::Text(
                serde_json::json!({ "type": event, "body": body })
                    .to_string()
                    .into(),
            );
            let _ = sender.send(frame).await;
        }
        HubMessage::AntennaNote { antenna_id, note_id } => {
            // Batch N WS4: fan one matched note out to this connection's
            // `antenna` channels subscribed to that antenna. Read-time
            // visibility + mute/block filtering applies (upstream `onEvent`
            // parity); packing is the detail shape.
            let targets: Vec<String> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::Antenna { antenna_id: mine } if mine == &antenna_id => {
                        Some(id.clone())
                    }
                    _ => None,
                })
                .collect();
            if targets.is_empty() {
                return;
            }
            let services = state.services.clone();
            let viewer = viewer_id.map(|s| s.to_string());
            let packed = tokio::task::spawn_blocking(move || {
                let note = match services.notes.get_note(&note_id) {
                    Ok(Some(note)) if !note.is_deleted => note,
                    _ => return None,
                };
                let viewer_ref = viewer.as_deref();
                let filter = services.feed_filter(viewer_ref).ok()?;
                let mut account_cache = std::collections::HashMap::new();
                let mut note_cache = std::collections::HashMap::new();
                let follows_cache = services.follows_cache(viewer_ref).ok()?;
                let viewer_ctx = services.viewer_ctx(viewer_ref, &note.author_id, &follows_cache);
                if !crate::domain::policy::is_visible(&note, &viewer_ctx) {
                    return None;
                }
                if services
                    .feed_filter_hides(&filter, &note, &mut account_cache, &mut note_cache)
                    .unwrap_or(true)
                {
                    return None;
                }
                crate::api::misskey::packers::pack_note(&note, viewer_ref, &services, 0).ok()
            })
            .await
            .ok()
            .flatten();
            if let Some(packed) = packed {
                for id in targets {
                    let frame = channel_frame(&id, "note", packed.clone());
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        }
        HubMessage::UserListMember {
            list_id,
            member_id,
            added,
        } => {
            // Batch O WP16: `userAdded`/`userRemoved` with the packed
            // member (lite shape — no secrets cross the socket) to this
            // connection's `userList` channels for that list.
            let targets: Vec<String> = channels
                .iter()
                .filter_map(|(id, sub)| match &sub.kind {
                    ChannelKind::UserList { list_id: mine } if mine == &list_id => {
                        Some(id.clone())
                    }
                    _ => None,
                })
                .collect();
            if targets.is_empty() {
                return;
            }
            let services = state.services.clone();
            let member_id_owned = member_id.clone();
            let packed = tokio::task::spawn_blocking(move || {
                let member = services.accounts.find_by_id(&member_id_owned).ok()??;
                Some(crate::api::misskey::packers::pack_user_lite(
                    &member,
                    &services.public_url,
                    services.clock.now_ms(),
                ))
            })
            .await
            .ok()
            .flatten();
            if let Some(packed) = packed {
                let body = serde_json::to_value(&packed).unwrap_or(Value::Null);
                let event = if added { "userAdded" } else { "userRemoved" };
                for id in targets {
                    let frame = channel_frame(&id, event, body.clone());
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// Capture fan-out for `reacted`/`unreacted`: only subscribed notes pass,
/// and only through the light visibility gate.
async fn push_note_updated(
    state: &ApiState,
    viewer_id: Option<&str>,
    captures: &HashMap<String, u32>,
    note_id: &str,
    event: &str,
    body: Value,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) {
    if !captures.contains_key(note_id) {
        return;
    }
    let services = state.services.clone();
    let note_id_owned = note_id.to_string();
    let viewer = viewer_id.map(|s| s.to_string());
    let visible = tokio::task::spawn_blocking(move || {
        services.capture_gate(viewer.as_deref(), &note_id_owned)
    })
    .await
    .unwrap_or(Ok(false))
    .unwrap_or(false);
    if !visible {
        return;
    }
    let frame = note_updated_frame(note_id, event, body);
    let _ = sender.send(frame).await;
}

/// Extract the raw token for tests: `Authorization: Bearer` first, then the
/// `i` query parameter.
pub fn token_for_upgrade(headers: &HeaderMap, query: &HashMap<String, String>) -> Option<String> {
    extract_raw_token(
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
        None,
        None,
    )
    .or_else(|| query.get("i").cloned().filter(|t| !t.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_params_use_misskey_defaults() {
        let kind = ChannelKind::Timeline(TimelineChannel::Local);
        let params = channel_params(&kind, None);
        assert!(params.with_renotes);
        assert!(!params.with_files);
        assert!(!params.with_replies);
        let params = channel_params(
            &kind,
            Some(&serde_json::json!({ "withReplies": true, "withFiles": true })),
        );
        assert!(params.with_replies);
        assert!(params.with_files);
        // `withReplies` is not a global-timeline param: ignored there.
        let global = ChannelKind::Timeline(TimelineChannel::Global);
        let params = channel_params(
            &global,
            Some(&serde_json::json!({ "withReplies": true })),
        );
        assert!(!params.with_replies);
    }

    #[test]
    fn upgrade_token_prefers_bearer_over_query() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer header-token".parse().unwrap(),
        );
        let query: HashMap<String, String> =
            [("i".to_string(), "query-token".to_string())].into_iter().collect();
        assert_eq!(
            token_for_upgrade(&headers, &query),
            Some("header-token".to_string())
        );
        assert_eq!(
            token_for_upgrade(&HeaderMap::new(), &query),
            Some("query-token".to_string())
        );
        assert_eq!(token_for_upgrade(&HeaderMap::new(), &HashMap::new()), None);
    }

    #[test]
    fn channel_frame_shapes_match_misskey_js_demux() {
        let body = serde_json::json!({ "id": "n1" });
        let Message::Text(text) = channel_frame("chan-1", "note", body) else {
            panic!("expected text frame");
        };
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["type"], "channel");
        assert_eq!(parsed["body"]["id"], "chan-1");
        assert_eq!(parsed["body"]["type"], "note");
        assert_eq!(parsed["body"]["body"]["id"], "n1");
        let Message::Text(text) = note_updated_frame(
            "n1",
            "reacted",
            serde_json::json!({ "reaction": "❤", "userId": "u1" }),
        ) else {
            panic!("expected text frame");
        };
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["type"], "noteUpdated");
        assert_eq!(parsed["body"]["id"], "n1");
        assert_eq!(parsed["body"]["type"], "reacted");
    }
}
