use super::protocol::note_updated_frame;
use crate::api::misskey::endpoints::ApiState;
use crate::domain::id::PortError;
use axum::extract::ws::{Message, WebSocket};
use futures_util::SinkExt as _;
use serde_json::Value;
use std::collections::HashMap;

/// One diagnosed streaming skip (same shape as `log_me_updated_skip` in
/// `dispatch.rs`): a stable `streaming.skipped` warn line carrying the
/// error class only (never secrets, bodies, or DB strings) plus the
/// bounded error ledger.
///
/// Callers skip the frame: no retry (the next event re-delivers current
/// state) and never a partial pack. Gone (`Ok(None)`) and invisible
/// notes skip silently at the call site — only faults reach this helper.
pub(crate) fn log_stream_skip(scope: &str, stage: &str, err: &PortError) {
    let message = err.to_string();
    crate::observability::record_error("streaming.skipped");
    crate::observability::log_event(
        crate::observability::LogLevel::Warn,
        "streaming.skipped",
        &[
            ("scope", scope),
            ("stage", stage),
            ("error_class", crate::observability::error_class(&message)),
        ],
    );
}

/// A `spawn_blocking` join failure for one fan-out scope: same
/// `streaming.skipped` diagnosis (error class only), then skip the frame.
pub(crate) fn log_stream_join_skip(scope: &str, join_err: &tokio::task::JoinError) {
    let message = join_err.to_string();
    crate::observability::record_error("streaming.skipped");
    crate::observability::log_event(
        crate::observability::LogLevel::Warn,
        "streaming.skipped",
        &[
            ("scope", scope),
            ("stage", "dispatch-join"),
            ("error_class", crate::observability::error_class(&message)),
        ],
    );
}

/// Load one fan-out row: gone (`Ok(None)`) returns `None` silently; a
/// store fault is diagnosed via [`log_stream_skip`] and returns `None`
/// (skip the frame, no retry).
pub(crate) fn load_for_fanout<T>(
    scope: &str,
    stage: &str,
    loaded: Result<Option<T>, PortError>,
) -> Option<T> {
    match loaded {
        Ok(row) => row,
        Err(err) => {
            log_stream_skip(scope, stage, &err);
            None
        }
    }
}

/// Fallible fan-out step (packer, filter, cache): `Ok` passes through; a
/// fault is diagnosed via [`log_stream_skip`] and returns `None` (skip
/// the frame, no retry, no partial pack).
pub(crate) fn load_or_skip<T>(scope: &str, stage: &str, loaded: Result<T, PortError>) -> Option<T> {
    match loaded {
        Ok(value) => Some(value),
        Err(err) => {
            log_stream_skip(scope, stage, &err);
            None
        }
    }
}

/// Join one fan-out blocking task: the task's own `Option` already
/// diagnosed store/pack faults inside; a join failure is diagnosed here
/// (`dispatch-join`) and skips the frame.
pub(crate) fn join_fanout<T>(
    scope: &str,
    joined: Result<Option<T>, tokio::task::JoinError>,
) -> Option<T> {
    match joined {
        Ok(value) => value,
        Err(join_err) => {
            log_stream_join_skip(scope, &join_err);
            None
        }
    }
}

/// Count one at-most-once socket/hub drop. Delivery stays at-most-once by
/// design (no retry, no policy change); the counter keeps the drop
/// visible in the bounded error ledger.
pub(crate) fn count_send_drop() {
    crate::observability::record_error("streaming.send_dropped");
}

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
                        Some(media_id) => match services.media.get(media_id) {
                            Ok(row) => row
                                .map(|meta| meta.url)
                                .unwrap_or_else(|| format!("{base}/emojis/{}", emoji.shortcode)),
                            Err(err) => {
                                // Diagnosed, then the same static fallback
                                // URL (never a partial frame).
                                log_stream_skip("emoji", "media-load", &err);
                                format!("{base}/emojis/{}", emoji.shortcode)
                            }
                        },
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
    let gate = tokio::task::spawn_blocking(move || {
        services.capture_gate(viewer.as_deref(), &note_id_owned)
    })
    .await;
    // Join and store faults are diagnosed and skip the frame; a gate
    // verdict of invisible stays silent (nothing to send).
    let visible = match gate {
        Err(join_err) => {
            log_stream_join_skip("capture", &join_err);
            return;
        }
        Ok(loaded) => match loaded {
            Ok(visible) => visible,
            Err(err) => {
                log_stream_skip("capture", "capture-gate", &err);
                return;
            }
        },
    };
    if !visible {
        return;
    }
    let frame = note_updated_frame(note_id, event, body);
    if sender.send(frame).await.is_err() {
        // At-most-once by design: counted, not retried.
        count_send_drop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skipped_count() -> u64 {
        crate::observability::error_counts()
            .get("streaming.skipped")
            .copied()
            .unwrap_or(0)
    }

    #[test]
    fn fanout_load_fault_skips_and_counts() {
        let before = skipped_count();
        let out: Option<String> = load_for_fanout(
            "test",
            "note-load",
            Err(PortError::StorageUnavailable("test outage".to_string())),
        );
        assert!(out.is_none(), "store fault must skip the frame");
        assert_eq!(
            skipped_count(),
            before + 1,
            "store fault must be counted once"
        );
    }

    #[test]
    fn fanout_load_gone_is_silent() {
        let before = skipped_count();
        let out: Option<String> = load_for_fanout("test", "note-load", Ok(None));
        assert!(out.is_none(), "gone row must skip the frame");
        assert_eq!(
            skipped_count(),
            before,
            "gone rows stay silent: nothing to diagnose"
        );
    }

    #[test]
    fn fanout_pack_fault_skips_without_partial_value() {
        let before = skipped_count();
        let out: Option<String> = load_or_skip(
            "test",
            "pack",
            Err(PortError::StorageUnavailable(
                "test pack outage".to_string(),
            )),
        );
        assert!(out.is_none(), "pack fault must skip, not send partial");
        assert_eq!(skipped_count(), before + 1);
    }

    #[test]
    fn fanout_success_passes_through_uncounted() {
        let before = skipped_count();
        assert_eq!(
            load_for_fanout("test", "note-load", Ok(Some("n1".to_string()))),
            Some("n1".to_string())
        );
        assert_eq!(
            load_or_skip::<String>("test", "pack", Ok("packed".to_string())),
            Some("packed".to_string())
        );
        assert_eq!(skipped_count(), before, "success must not count");
    }
}
