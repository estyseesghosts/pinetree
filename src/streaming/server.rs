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
            let resolved = tokio::task::spawn_blocking(move || authenticate(&services, &token)).await;
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
            connect_channel(caller, channels, id, channel, body, sender).await;
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
            if obj.get("type").and_then(Value::as_str) == Some("readNotification") {
                read_all_notifications(state, caller).await;
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
    caller: &Option<CallerInfo>,
    channels: &mut HashMap<String, ChannelSub>,
    id: &str,
    channel: &str,
    body: Option<&Value>,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) {
    let kind = match channel {
        "main" => ChannelKind::Main,
        "homeTimeline" => ChannelKind::Timeline(TimelineChannel::Home),
        "localTimeline" => ChannelKind::Timeline(TimelineChannel::Local),
        "hybridTimeline" => ChannelKind::Timeline(TimelineChannel::Hybrid),
        "globalTimeline" => ChannelKind::Timeline(TimelineChannel::Global),
        _ => {
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
    // refused connects are silently ignored).
    if matches!(
        kind,
        ChannelKind::Main
            | ChannelKind::Timeline(TimelineChannel::Home | TimelineChannel::Hybrid)
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
        ChannelSub { kind, params },
    );
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
                    ChannelKind::Main => None,
                })
                .collect();
            if timeline.is_empty() {
                return;
            }
            let services = state.services.clone();
            let viewer = viewer_id.map(|s| s.to_string());
            let packed = tokio::task::spawn_blocking(move || {
                let note = match services.notes.get_note(&note_id) {
                    Ok(Some(note)) => note,
                    _ => return None,
                };
                let mut eligible = false;
                for (_, kind, params) in &timeline {
                    if let ChannelKind::Timeline(which) = kind {
                        if services
                            .note_eligible_for_channel(*which, viewer.as_deref(), &note, params)
                            .unwrap_or(false)
                        {
                            eligible = true;
                            break;
                        }
                    }
                }
                if !eligible {
                    return None;
                }
                // One pack per viewer serves every eligible channel.
                let packed =
                    crate::api::misskey::packers::pack_note(&note, viewer.as_deref(), &services, 0)
                        .ok()?;
                let ids: Vec<String> = timeline
                    .iter()
                    .filter_map(|(id, kind, params)| match kind {
                        ChannelKind::Timeline(which) => services
                            .note_eligible_for_channel(*which, viewer.as_deref(), &note, params)
                            .unwrap_or(false)
                            .then(|| id.clone()),
                        _ => None,
                    })
                    .collect();
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
        HubMessage::NoteReacted {
            note_id,
            user_id,
            reaction,
        } => {
            push_note_updated(
                state, viewer_id, captures, &note_id, "reacted",
                serde_json::json!({ "reaction": reaction, "userId": user_id }),
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
