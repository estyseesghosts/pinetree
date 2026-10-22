use super::connection::{channel_frame, send_pong};
use super::handshake::CallerInfo;
use crate::api::misskey::endpoints::ApiState;
use crate::application::queries::{TimelineChannel, TimelineChannelParams};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) enum ChannelKind {
    Main,
    Timeline(TimelineChannel),
    Antenna {
        antenna_id: String,
    },
    UserList {
        list_id: String,
    },
    /// Upstream `hashtag` (`q: string[][]`, AND-of-ORs over lowercased
    /// tags; no credential).
    Hashtag {
        q: Vec<Vec<String>>,
    },
    /// Upstream `roleTimeline` (`roleId`; no credential). Pinetree has no
    /// role model, so no producer publishes `RoleTimelineNote` yet — the
    /// channel connects and stays silent (recorded DIVERGENCE, not an
    /// error: upstream would likewise refuse an unknown role id).
    RoleTimeline {
        role_id: String,
    },
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
    Clip {
        clip_id: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ChannelSub {
    pub(crate) kind: ChannelKind,
    pub(crate) params: TimelineChannelParams,
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

pub(crate) async fn connect_channel(
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
            let param_key = if channel == "antenna" {
                "antennaId"
            } else {
                "listId"
            };
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
                    ChannelKind::Antenna {
                        antenna_id: target_id,
                    }
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
            let q = body.and_then(|b| b.get("params")).and_then(|p| p.get("q"));
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
                services.viewer_is_moderator(Some(&viewer)).unwrap_or(false)
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
        && channels
            .values()
            .any(|c| matches!(c.kind, ChannelKind::Main))
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
        ChannelSub {
            kind: kind.clone(),
            params,
        },
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
pub(crate) async fn handle_stats_log_request(
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

const MAX_CHANNELS_PER_CONNECTION: usize = 32;

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
        let params = channel_params(&global, Some(&serde_json::json!({ "withReplies": true })));
        assert!(!params.with_replies);
    }
}
