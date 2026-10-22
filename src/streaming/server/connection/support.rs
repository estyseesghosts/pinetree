use super::protocol::note_updated_frame;
use crate::api::misskey::endpoints::ApiState;
use axum::extract::ws::{Message, WebSocket};
use futures_util::SinkExt as _;
use serde_json::Value;
use std::collections::HashMap;

/// Shared read-time gate for the `hashtag`/`roleTimeline` note fan-out
/// (upstream `isNoteVisibleForMe` + `NoteStreamingHidingService` +
/// `requireSigninToViewContents` parity): visibility for the viewer, the
/// §0A/§12 mute/block/suspend/host/mute-instance/word-mute filter, and —
/// for anonymous viewers — the signin-to-view-contents trio (note author,
/// reply target author, renote target author). Runs on the blocking pool.
/// Fail-closed: any storage fault hides the note.
pub(crate) fn note_passes_stream_gate(
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
pub(crate) async fn resolve_reaction_emoji(state: &ApiState, reaction: &str) -> Value {
    let inner = match reaction.strip_prefix(':').and_then(|s| s.strip_suffix(':')) {
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
                            .unwrap_or_else(|| format!("{base}/emojis/{}", emoji.shortcode)),
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

/// Capture fan-out for `reacted`/`unreacted`: only subscribed notes pass,
/// and only through the light visibility gate.
pub(crate) async fn push_note_updated(
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
