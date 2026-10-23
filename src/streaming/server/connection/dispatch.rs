use super::super::handshake::CallerInfo;
use super::super::subscribe::{ChannelKind, ChannelSub};
use super::protocol::{channel_frame, note_updated_frame};
use super::support::{
    count_send_drop, join_fanout, load_for_fanout, load_or_skip, note_passes_stream_gate,
    push_note_updated, resolve_reaction_emoji,
};
use crate::api::misskey::endpoints::ApiState;
use crate::api::misskey::packers::pack_me_detailed;
use crate::application::events::MainEventKind;
use crate::application::queries::TimelineChannelParams;
use crate::domain::account::Account;
use crate::domain::id::PortError;
use crate::streaming::hub::HubMessage;
use axum::extract::ws::{Message, WebSocket};
use futures_util::SinkExt as _;
use serde_json::Value;
use std::collections::HashMap;

/// Resolve one `MeUpdated` frame body without ever emitting a partial
/// `MeDetailed`.
///
/// A transient store or packer `PortError` is logged (and counted) and
/// the frame is skipped; the next `MeUpdated` event carries the current
/// state, so there is no in-loop retry. A gone account (`Ok(None)`)
/// skips silently: the row is already gone, so there is nothing to
/// diagnose. `lookup` and `pack` are parameters (rather than
/// `AppServices`) so the skip logic is unit-testable without a socket.
pub(crate) fn me_updated_body(
    account_id: &str,
    lookup: Result<Option<Account>, PortError>,
    pack: impl FnOnce(&Account) -> Result<Value, PortError>,
) -> Option<Value> {
    let account = match lookup {
        Ok(Some(account)) => account,
        Ok(None) => return None,
        Err(err) => {
            log_me_updated_skip(account_id, "account-lookup", &err);
            return None;
        }
    };
    match pack(&account) {
        Ok(body) => Some(body),
        Err(err) => {
            log_me_updated_skip(account_id, "pack", &err);
            None
        }
    }
}

/// One diagnosed `MeUpdated` skip: a stable `streaming.me_updated_skipped`
/// line (error class only, never secrets or bodies) plus the bounded
/// error ledger.
fn log_me_updated_skip(account_id: &str, stage: &str, err: &PortError) {
    let message = err.to_string();
    crate::observability::record_error("streaming.me_updated");
    crate::observability::log_event(
        crate::observability::LogLevel::Warn,
        "streaming.me_updated_skipped",
        &[
            ("account_id", account_id),
            ("stage", stage),
            ("error_class", crate::observability::error_class(&message)),
        ],
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_hub_message(
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
                    let note = load_for_fanout(
                        "timeline",
                        "note-load",
                        services.notes.get_note(&note_id_timeline),
                    )?;
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
                    let packed = load_or_skip(
                        "timeline",
                        "pack",
                        crate::api::misskey::packers::pack_note(
                            &note,
                            viewer.as_deref(),
                            &services,
                            0,
                        ),
                    )?;
                    Some((ids, packed))
                })
                .await;
                let packed = join_fanout("timeline", packed);
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
                    let note = load_for_fanout(
                        "userList",
                        "note-load",
                        services.notes.get_note(&note_id_owned),
                    )?;
                    if note.is_deleted {
                        return None;
                    }
                    let viewer_ref = viewer.as_deref();
                    let filter =
                        load_or_skip("userList", "feed-filter", services.feed_filter(viewer_ref))?;
                    let mut account_cache = std::collections::HashMap::new();
                    let mut note_cache = std::collections::HashMap::new();
                    let follows_cache = load_or_skip(
                        "userList",
                        "follows-cache",
                        services.follows_cache(viewer_ref),
                    )?;
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
                        let list = load_for_fanout(
                            "userList",
                            "list-load",
                            services.lists.get_user_list(list_id),
                        )?;
                        let membership = load_for_fanout(
                            "userList",
                            "membership-load",
                            services.lists.get_list_membership(list_id, &note.author_id),
                        )?;
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
                    let packed = load_or_skip(
                        "userList",
                        "pack",
                        crate::api::misskey::packers::pack_note(&note, viewer_ref, &services, 0),
                    )?;
                    Some((ids, packed))
                })
                .await;
                let packed = join_fanout("userList", packed);
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
                    let note = load_for_fanout(
                        "hashtag",
                        "note-load",
                        services.notes.get_note(&note_id_owned),
                    )?;
                    if note.is_deleted {
                        return None;
                    }
                    let tags: std::collections::HashSet<String> =
                        note.tags.iter().map(|t| t.to_lowercase()).collect();
                    let mut ids: Vec<String> = Vec::new();
                    for (id, q) in &hashtags {
                        if q.iter()
                            .any(|ands| ands.iter().all(|t| tags.contains(&t.to_lowercase())))
                        {
                            ids.push(id.clone());
                        }
                    }
                    if ids.is_empty() {
                        return None;
                    }
                    if !note_passes_stream_gate(&services, viewer.as_deref(), &note) {
                        return None;
                    }
                    let packed = load_or_skip(
                        "hashtag",
                        "pack",
                        crate::api::misskey::packers::pack_note(
                            &note,
                            viewer.as_deref(),
                            &services,
                            0,
                        ),
                    )?;
                    Some((ids, packed))
                })
                .await;
                let packed = join_fanout("hashtag", packed);
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
                let note = load_for_fanout(
                    "roleTimeline",
                    "note-load",
                    services.notes.get_note(&note_id),
                )?;
                if note.is_deleted {
                    return None;
                }
                if !matches!(note.visibility, crate::domain::note::Visibility::Public) {
                    return None;
                }
                if !note_passes_stream_gate(&services, viewer.as_deref(), &note) {
                    return None;
                }
                load_or_skip(
                    "roleTimeline",
                    "pack",
                    crate::api::misskey::packers::pack_note(&note, viewer.as_deref(), &services, 0),
                )
            })
            .await;
            let packed = join_fanout("roleTimeline", packed);
            if let Some(packed) = packed {
                for id in targets {
                    let frame = channel_frame(&id, "note", packed.clone());
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        }
        HubMessage::DriveFile {
            user_id,
            event,
            file_id,
        } => {
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
            if matches!(event, crate::streaming::hub::DriveFileEvent::Deleted) {
                // No content can leak in a deletion notice (the row is
                // gone), so subscribers get it unconditionally.
                for id in targets {
                    let frame = channel_frame(&id, name, Value::String(file_id.clone()));
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
                return;
            }
            let services = state.services.clone();
            let packed = tokio::task::spawn_blocking(move || {
                let meta = load_for_fanout("drive", "file-load", services.media.get(&file_id))?;
                Some(crate::api::misskey::packers::pack_drive_file(
                    &meta, &services,
                ))
            })
            .await;
            let packed = join_fanout("drive", packed);
            if let Some(packed) = packed {
                for id in targets {
                    let frame = channel_frame(&id, name, packed.clone());
                    if sender.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        }
        HubMessage::DriveFolder {
            user_id,
            event,
            folder_id,
        } => {
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
                    ChannelKind::Clip { clip_id: mine } if mine == &clip_id => Some(id.clone()),
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
                let report = load_for_fanout(
                    "admin",
                    "report-load",
                    services.abuse_reports.get(&report_id),
                )?;
                Some(serde_json::json!({
                    "id": report.id,
                    "targetUserId": report.target_account,
                    "reporterId": report.reporter,
                    "comment": report.comment,
                }))
            })
            .await;
            let packed = join_fanout("admin", packed);
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
                state,
                viewer_id,
                captures,
                &note_id,
                "reacted",
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
                state,
                viewer_id,
                captures,
                &note_id,
                "unreacted",
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
            let deleted_at =
                crate::api::misskey::packers::format_millis(state.services.clock.now_ms());
            let frame = note_updated_frame(
                &note_id,
                "deleted",
                serde_json::json!({ "deletedAt": deleted_at }),
            );
            if sender.send(frame).await.is_err() {
                // At-most-once by design: counted, not retried.
                count_send_drop();
            }
        }
        HubMessage::PollVoted {
            note_id,
            user_id,
            choice,
        } => {
            push_note_updated(
                state,
                viewer_id,
                captures,
                &note_id,
                "pollVoted",
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
                load_for_fanout(
                    "noteUpdated",
                    "note-load",
                    services.notes.get_note(&note_id_owned),
                )
            })
            .await;
            let loaded = join_fanout("noteUpdated", loaded);
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
                        me_updated_body(
                            &account_id,
                            services.accounts.find_by_id(&account_id),
                            |account| pack_me_detailed(account, first_party, &services),
                        )
                    })
                    .await
                    {
                        Ok(Some(packed)) => packed,
                        // Store/pack failures already logged inside
                        // `me_updated_body`; a gone account skips silently.
                        Ok(None) => return,
                        Err(join_err) => {
                            crate::observability::record_error("streaming.me_updated");
                            crate::observability::log_event(
                                crate::observability::LogLevel::Warn,
                                "streaming.me_updated_skipped",
                                &[("stage", "dispatch-join"), ("cause", &join_err.to_string())],
                            );
                            return;
                        }
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
            if sender.send(frame).await.is_err() {
                // At-most-once by design: counted, not retried.
                count_send_drop();
            }
        }
        HubMessage::AntennaNote {
            antenna_id,
            note_id,
        } => {
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
                let note =
                    load_for_fanout("antenna", "note-load", services.notes.get_note(&note_id))?;
                if note.is_deleted {
                    return None;
                }
                let viewer_ref = viewer.as_deref();
                let filter =
                    load_or_skip("antenna", "feed-filter", services.feed_filter(viewer_ref))?;
                let mut account_cache = std::collections::HashMap::new();
                let mut note_cache = std::collections::HashMap::new();
                let follows_cache = load_or_skip(
                    "antenna",
                    "follows-cache",
                    services.follows_cache(viewer_ref),
                )?;
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
                load_or_skip(
                    "antenna",
                    "pack",
                    crate::api::misskey::packers::pack_note(&note, viewer_ref, &services, 0),
                )
            })
            .await;
            let packed = join_fanout("antenna", packed);
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
                    ChannelKind::UserList { list_id: mine } if mine == &list_id => Some(id.clone()),
                    _ => None,
                })
                .collect();
            if targets.is_empty() {
                return;
            }
            let services = state.services.clone();
            let member_id_owned = member_id.clone();
            let packed = tokio::task::spawn_blocking(move || {
                let member = load_for_fanout(
                    "userList",
                    "member-load",
                    services.accounts.find_by_id(&member_id_owned),
                )?;
                Some(crate::api::misskey::packers::pack_user_lite(
                    &member,
                    &services.public_url,
                    services.clock.now_ms(),
                ))
            })
            .await;
            let packed = join_fanout("userList", packed);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_account() -> Account {
        Account::new_local("a1".to_string(), "alice".to_string()).unwrap()
    }

    #[test]
    fn me_updated_lookup_error_skips_without_packing() {
        let mut packed = false;
        let body = me_updated_body(
            "a1",
            Err(PortError::StorageUnavailable("test outage".to_string())),
            |_| {
                packed = true;
                Ok(Value::Null)
            },
        );
        assert!(body.is_none(), "lookup error must skip the frame");
        assert!(
            !packed,
            "packer must not run after a failed lookup (no partial MeDetailed)"
        );
    }

    #[test]
    fn me_updated_gone_account_skips_without_packing() {
        let mut packed = false;
        let body = me_updated_body("a1", Ok(None), |_| {
            packed = true;
            Ok(Value::Null)
        });
        assert!(body.is_none(), "gone account must skip the frame");
        assert!(!packed, "packer must not run without an account");
    }

    #[test]
    fn me_updated_pack_error_skips_frame() {
        let body = me_updated_body("a1", Ok(Some(test_account())), |_| {
            Err(PortError::StorageUnavailable(
                "test pack outage".to_string(),
            ))
        });
        assert!(
            body.is_none(),
            "pack error must skip the frame, not send partial"
        );
    }

    #[test]
    fn me_updated_success_forwards_packed_body() {
        let expected = serde_json::json!({ "id": "a1" });
        let body = me_updated_body("a1", Ok(Some(test_account())), |_| Ok(expected.clone()));
        assert_eq!(body, Some(expected));
    }
}
