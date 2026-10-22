//! Pack note (split from `packers.rs`). Pure move.
//!
#![allow(unused_imports)]
//! Misskey response packers (Slice D4).
//!
//! Render domain types into Misskey 2026.10 wire shapes
//! (`misskey/packages/backend/src/models/json-schema/`:
//! `note.ts`, `user.ts`, `drive-file.ts`, `meta.ts`). Field sets are
//! pinned by snapshot tests at the bottom of this file that read the
//! actual checkout schemas from disk. The tests skip (pass vacuously) when
//! the git-ignored `./misskey` checkout is absent, e.g. in clean clones.

use serde_json::{Map, Value};

use crate::activitypub::format_millis;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::{Account, FollowVisibility};
use crate::domain::blocking::Blocking;
use crate::domain::draft::NoteDraft;
use crate::domain::drive_folder::DriveFolder;
use crate::domain::follow::Follow;
use crate::domain::instance::InviteCode;
use crate::domain::media::MediaMeta;
use crate::domain::moderation::{AbuseReport, ModerationLogEntry};
use crate::domain::muting::{Muting, RenoteMuting};
use crate::domain::note::{Note, Visibility};
use crate::domain::notification::{Notification, NotificationKind};
use crate::domain::reaction::Reaction;
use crate::domain::visibility::{ViewerContext, is_visible};
use crate::domain::webhook::SystemWebhook;
use crate::domain::webhook::UserWebhook;

/// Misskey server version we claim compatibility with.

/// Millisecond timestamps as ISO-8601 UTC strings
/// (`"YYYY-MM-DDTHH:MM:SS.sssZ"`).
use super::common::note_time_ms;
use super::drive_file::pack_drive_file;
use super::emoji::emoji_maps_for_viewer;
use super::user::pack_user_lite;

pub(crate) fn viewer_context_for(
    services: &AppServices,
    viewer_id: Option<&str>,
    author_id: &str,
) -> ViewerContext {
    match viewer_id {
        None => ViewerContext::anonymous(),
        Some(viewer) => {
            let follows_author = if viewer == author_id {
                true
            } else {
                services
                    .follows
                    .is_following(viewer, author_id)
                    .unwrap_or(false)
            };
            ViewerContext::local_user(viewer, follows_author)
        }
    }
}
pub(crate) fn pack_nested_note(
    target_id: Option<&str>,
    viewer_id: Option<&str>,
    services: &AppServices,
    depth: u8,
) -> Result<Value, PortError> {
    let Some(target_id) = target_id else {
        return Ok(Value::Null);
    };
    if depth > 0 {
        return Ok(Value::Null);
    }
    let target = match services.notes.get_note(target_id)? {
        None => return Ok(Value::Null),
        Some(note) => note,
    };
    let viewer = viewer_context_for(services, viewer_id, &target.author_id);
    if !is_visible(&target, &viewer) {
        return Ok(Value::Null);
    }
    pack_note(&target, viewer_id, services, depth + 1)
}
/// `Note` (`note.ts` `packedNoteSchema`): `user` packed inline, `files`
/// Batch N WS8 soft-mute flag: true when the viewer's `mutedWords` set
/// transitively matches the note (self → reply → renote, first match wins;
/// the viewer's own notes never match). Callers additionally require the
/// note to carry no CW of its own (the Keystone badge rule).
pub(crate) fn soft_muted_for_viewer(
    services: &AppServices,
    viewer: &str,
    note: &Note,
) -> Result<bool, PortError> {
    let Some(account) = services.accounts.find_by_id(viewer)? else {
        return Ok(false);
    };
    if account.muted_words.is_empty() {
        return Ok(false);
    }
    let compiled = crate::domain::word_mute::CompiledWordMute::compile(&account.muted_words);
    if compiled.is_empty() {
        return Ok(false);
    }
    if compiled.matches_note(
        note.cw.as_deref(),
        note.text.as_deref(),
        &note.author_id,
        viewer,
    ) {
        return Ok(true);
    }
    if let Some(reply_id) = note.reply_id.as_deref() {
        if let Some(reply) = services.notes.get_note(reply_id)? {
            if !reply.is_deleted
                && compiled.matches_note(
                    reply.cw.as_deref(),
                    reply.text.as_deref(),
                    &reply.author_id,
                    viewer,
                )
            {
                return Ok(true);
            }
        }
    }
    if let Some(renote_id) = note.renote_id.as_deref() {
        if let Some(target) = services.notes.get_note(renote_id)? {
            if !target.is_deleted
                && compiled.matches_note(
                    target.cw.as_deref(),
                    target.text.as_deref(),
                    &target.author_id,
                    viewer,
                )
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
/// from `file_ids`, `myReaction` for `viewer_id`, `visibleUserIds` for
/// specified notes, and one level of nested `reply`/`renote` when
/// `depth == 0` and visible to the viewer.
pub fn pack_note(
    note: &Note,
    viewer_id: Option<&str>,
    services: &AppServices,
    depth: u8,
) -> Result<Value, PortError> {
    let author = services
        .accounts
        .find_by_id(&note.author_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let user = pack_user_lite(&author, &services.public_url, services.clock.now_ms());
    let reaction_count: u64 = note.reactions.values().sum();
    let mut files = Vec::new();
    for fid in &note.file_ids {
        if let Some(meta) = services.media.get(fid)? {
            files.push(pack_drive_file(&meta, services));
        }
    }
    let my_reaction = match viewer_id {
        Some(viewer) => services
            .reactions
            .get_reaction(viewer, &note.id)?
            .map(|r| Value::String(r.reaction))
            .unwrap_or(Value::Null),
        None => Value::Null,
    };
    let mut obj = Map::new();
    obj.insert("id".to_string(), Value::String(note.id.clone()));
    obj.insert(
        "createdAt".to_string(),
        Value::String(format_millis(note_time_ms(note, services))),
    );
    obj.insert("userId".to_string(), Value::String(note.author_id.clone()));
    obj.insert("user".to_string(), user);
    // §8: last edit time (Sharkey `updatedAt` parity: ISO string or null).
    obj.insert(
        "updatedAt".to_string(),
        note.updated_at_ms
            .map(crate::activitypub::format_millis)
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    // §3: edit/strip diagnostics (Sharkey `processErrors` parity: array or
    // null; upstream emits it only in detail packs — pinetree packs a single
    // shape, so it is always present).
    obj.insert(
        "processErrors".to_string(),
        note.process_errors
            .clone()
            .map(|errors| Value::Array(errors.into_iter().map(Value::String).collect()))
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "text".to_string(),
        note.text.clone().map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert(
        "cw".to_string(),
        note.cw.clone().map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert(
        "visibility".to_string(),
        serde_json::to_value(&note.visibility).unwrap_or(Value::String("public".to_string())),
    );
    obj.insert("localOnly".to_string(), Value::Bool(note.local_only));
    obj.insert(
        "reactionAcceptance".to_string(),
        note.reaction_acceptance
            .as_ref()
            .and_then(|a| serde_json::to_value(a).ok())
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "renoteCount".to_string(),
        Value::Number(note.renote_count.into()),
    );
    obj.insert(
        "repliesCount".to_string(),
        Value::Number(note.replies_count.into()),
    );
    obj.insert(
        "reactionCount".to_string(),
        Value::Number(reaction_count.into()),
    );
    obj.insert(
        "reactions".to_string(),
        serde_json::to_value(&note.reactions).unwrap_or(Value::Object(Map::new())),
    );
    // Batch N WS1 + WP8b: registry maps (bare shortcode → URL locally,
    // `name@host` remotes only for opted-in viewers).
    let show_remote = match viewer_id {
        Some(viewer) => services
            .accounts
            .find_by_id(viewer)
            .ok()
            .flatten()
            .map(|a| a.show_remote_emojis)
            .unwrap_or(false),
        None => false,
    };
    let (emojis_map, reaction_emojis_map) = emoji_maps_for_viewer(services, show_remote);
    obj.insert("reactionEmojis".to_string(), reaction_emojis_map);
    obj.insert(
        "fileIds".to_string(),
        Value::Array(
            note.file_ids
                .iter()
                .map(|id| Value::String(id.clone()))
                .collect(),
        ),
    );
    obj.insert("files".to_string(), Value::Array(files));
    obj.insert(
        "replyId".to_string(),
        note.reply_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "renoteId".to_string(),
        note.renote_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "reply".to_string(),
        pack_nested_note(note.reply_id.as_deref(), viewer_id, services, depth)?,
    );
    obj.insert(
        "renote".to_string(),
        pack_nested_note(note.renote_id.as_deref(), viewer_id, services, depth)?,
    );
    // Structured-quote extensions (Pinetree informational, non-standard).
    // Present on quotes only — never on ordinary pure renotes. Visibility
    // stays mandatory and separate from authorization: rejected/unapproved
    // quotes still render their relation subject to the normal visibility
    // rules below.
    if let Some(quote) = note.quote.as_ref() {
        obj.insert(
            "quoteAuthorizationStatus".to_string(),
            Value::String(quote.authorization.api_status().to_string()),
        );
        let quoting_public = matches!(note.visibility, Visibility::Public | Visibility::Home);
        match quote.target_note_id.as_deref() {
            Some(tid) => {
                let visible = match services.notes.get_note(tid)? {
                    Some(target) => {
                        let viewer = viewer_context_for(services, viewer_id, &target.author_id);
                        crate::domain::visibility::is_visible(&target, &viewer)
                    }
                    // Deleted or missing target: stable unavailable fallback.
                    None => false,
                };
                if !visible {
                    // Never serialize the protected object or leak its URI;
                    // the `renote` slot is already null via `pack_nested_note`.
                    obj.insert("quoteTargetUnavailable".to_string(), Value::Bool(true));
                }
            }
            // Unresolved target: preserve the relationship with its URI when
            // safe to disclose (public quoting posts), otherwise a
            // controlled unavailable marker. Never fabricate a `renoteId`.
            None => {
                if quoting_public && !note.local_only {
                    obj.insert(
                        "quoteTargetUri".to_string(),
                        Value::String(quote.target_uri.clone()),
                    );
                } else {
                    obj.insert("quoteTargetUnavailable".to_string(), Value::Bool(true));
                }
            }
        }
    }
    obj.insert(
        "mentions".to_string(),
        Value::Array(
            note.mentions
                .iter()
                .map(|id| Value::String(id.clone()))
                .collect(),
        ),
    );
    if note.visibility == Visibility::Specified {
        obj.insert(
            "visibleUserIds".to_string(),
            Value::Array(
                note.visible_user_ids
                    .iter()
                    .map(|id| Value::String(id.clone()))
                    .collect(),
            ),
        );
    }
    obj.insert(
        "tags".to_string(),
        Value::Array(
            note.tags
                .iter()
                .map(|tag| Value::String(tag.clone()))
                .collect(),
        ),
    );
    obj.insert("emojis".to_string(), emojis_map);
    obj.insert("myReaction".to_string(), my_reaction);
    // Batch N WS2: `isFavorited` for the authenticated caller (upstream
    // `NoteEntityService` packs it; anonymous callers see no field — Misskey
    // omits it, but a null keeps the shape stable; use bool always).
    let is_favorited = match viewer_id {
        Some(viewer) => services
            .favorites
            .is_favorited(viewer, &note.id)
            .unwrap_or(false),
        None => false,
    };
    obj.insert("isFavorited".to_string(), Value::Bool(is_favorited));
    // Batch N WS8: viewer-specific soft word-mute flag (Keystone badge
    // rule): true when the viewer's soft set transitively matches AND the
    // note has no CW of its own. Hard mutes drop the note upstream of the
    // packer (`FeedFilter`); notifications are never gated.
    let is_soft_muted = match viewer_id {
        Some(viewer) => {
            let no_cw = note.cw.as_deref().map(str::is_empty).unwrap_or(true);
            no_cw && soft_muted_for_viewer(services, viewer, note).unwrap_or(false)
        }
        None => false,
    };
    obj.insert("isSoftMuted".to_string(), Value::Bool(is_soft_muted));
    // §4 polls (Misskey `note.ts` `poll` + `hasPoll` parity): the poll with
    // per-requester `isVoted` flags (Misskey `populatePoll` parity), or
    // `poll: null` + `hasPoll: false` when the note carries none.
    let (has_poll, poll_value) = match services.polls.get_poll(&note.id)? {
        Some(poll) => {
            let my_choices: std::collections::HashSet<usize> = match viewer_id {
                Some(viewer) => services
                    .polls
                    .votes_by_user_for_note(viewer, &note.id)?
                    .into_iter()
                    .map(|v| v.choice)
                    .collect(),
                None => std::collections::HashSet::new(),
            };
            let choices: Vec<Value> = poll
                .choices
                .iter()
                .enumerate()
                .map(|(i, text)| {
                    serde_json::json!({
                        "isVoted": my_choices.contains(&i),
                        "text": text,
                        "votes": poll.votes.get(i).copied().unwrap_or(0),
                    })
                })
                .collect();
            (
                true,
                serde_json::json!({
                    "multiple": poll.multiple,
                    "expiresAt": poll.expires_at_ms.map(format_millis).map(Value::String).unwrap_or(Value::Null),
                    "choices": choices,
                }),
            )
        }
        _ => (false, Value::Null),
    };
    obj.insert("hasPoll".to_string(), Value::Bool(has_poll));
    obj.insert("poll".to_string(), poll_value);
    // Optional AP identity fields (Misskey `note.ts` `uri`/`url`); `channel`
    // is always null — channels do not exist here.
    obj.insert(
        "uri".to_string(),
        note.uri.clone().map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert(
        "url".to_string(),
        note.url.clone().map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert("channel".to_string(), Value::Null);
    Ok(Value::Object(obj))
}
