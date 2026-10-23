use super::super::handshake::CallerInfo;
use super::super::subscribe::{ChannelSub, connect_channel, handle_stats_log_request};
use super::dispatch::dispatch_hub_message;
use crate::api::misskey::endpoints::ApiState;
use crate::application::events::{MainEventKind, MainMessage};
use crate::streaming::hub::HubMessage;
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::time::interval;

const MAX_CAPTURED_NOTES: usize = 1536;

const IDLE_REAP_AFTER: Duration = Duration::from_secs(120);

const PING_EVERY: Duration = Duration::from_secs(60);

pub(crate) async fn run_connection(socket: WebSocket, state: ApiState, caller: Option<CallerInfo>) {
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
                    // like a reconnect (at-most-once live hints). Counted
                    // so the drop is not invisible; policy is unchanged.
                    Err(_) => {
                        crate::observability::record_error("streaming.lagged");
                        hub_rx = state.hub.subscribe();
                    }
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
                    let channel_id = obj.get("id").and_then(Value::as_str).unwrap_or("");
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

/// Streaming `readNotification`: mark everything read, then emit
/// `readAllNotifications` on the caller's `main` channels (Misskey
/// `NotificationService.readAllNotification` parity). Anonymous callers
/// have no notifications to read.
async fn read_all_notifications(state: &ApiState, caller: &Option<CallerInfo>) {
    let Some(caller) = caller else { return };
    let services = state.services.clone();
    let account_id = caller.account_id.clone();
    let marked =
        tokio::task::spawn_blocking(move || services.notifications.mark_all_read(&account_id))
            .await;
    if marked.is_ok() {
        let _ = state.services.streaming.push_main(MainMessage {
            user_id: caller.account_id.clone(),
            kind: MainEventKind::ReadAllNotifications,
            body: Value::Null,
        });
    }
}
