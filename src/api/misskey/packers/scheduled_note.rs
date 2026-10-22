//! Pack scheduled_note (split from `packers.rs`). Pure move.
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
use super::drive_file::pack_drive_file;
use super::note::pack_note;
use super::user::pack_user_lite;

/// Sharkey scheduled-note pack (Batch N scheduling shim; upstream
/// `notes/schedule/list.ts` shape): `{id, userId, scheduledAt: ISO, note:
/// {text (default `''`), cw, fileIds (+ packed `files`), visibility
/// (default `public`), visibleUsers (packed UserLite),
/// reactionAcceptance (default null), user (packed caller), createdAt:
/// scheduledAt-ISO, isSchedule: true, id, renote/reply (packed or null),
/// renoteId/replyId, poll?}}`. Missing reply/renote targets or files pack
/// as null / are skipped (never an error).
pub fn pack_scheduled_note(
    draft: &NoteDraft,
    viewer: &Account,
    services: &AppServices,
) -> Result<Value, PortError> {
    let scheduled_at_ms = draft
        .scheduled_at_ms
        .unwrap_or_else(|| services.clock.now_ms());
    let scheduled_at = format_millis(scheduled_at_ms);
    let mut files = Vec::new();
    let mut file_ids = Vec::new();
    for fid in &draft.file_ids {
        if let Some(meta) = services.media.get(fid)? {
            file_ids.push(Value::String(meta.id.clone()));
            files.push(pack_drive_file(&meta, services));
        }
    }
    let mut visible_users = Vec::new();
    for id in &draft.visible_user_ids {
        if let Some(account) = services.accounts.find_by_id(id)? {
            visible_users.push(pack_user_lite(
                &account,
                &services.public_url,
                services.clock.now_ms(),
            ));
        }
    }
    let pack_target = |id: Option<&String>| -> Result<Value, PortError> {
        match id {
            Some(id) => match services.notes.get_note(id)? {
                Some(note) if !note.is_deleted => pack_note(&note, Some(&viewer.id), services, 0),
                _ => Ok(Value::Null),
            },
            None => Ok(Value::Null),
        }
    };
    let renote = pack_target(draft.renote_id.as_ref())?;
    let reply = pack_target(draft.reply_id.as_ref())?;
    let poll = match services.polls.get_poll(&draft.id)? {
        Some(poll) => {
            let choices: Vec<Value> = poll
                .choices
                .iter()
                .map(|text| serde_json::json!({ "isVoted": false, "text": text, "votes": 0 }))
                .collect();
            serde_json::json!({
                "multiple": poll.multiple,
                "expiresAt": poll.expires_at_ms.map(format_millis).map(Value::String).unwrap_or(Value::Null),
                "choices": choices,
            })
        }
        // Draft poll columns (the poll row is created at post time).
        None if draft.has_poll => serde_json::json!({
            "multiple": draft.poll_multiple,
            "expiresAt": draft.poll_expires_at_ms.map(format_millis).map(Value::String).unwrap_or(Value::Null),
            "choices": draft.poll_choices.iter().map(|text| {
                serde_json::json!({ "isVoted": false, "text": text, "votes": 0 })
            }).collect::<Vec<_>>(),
        }),
        None => Value::Null,
    };
    let mut note = Map::new();
    note.insert(
        "text".to_string(),
        draft
            .text
            .clone()
            .map(Value::String)
            .unwrap_or_else(|| Value::String(String::new())),
    );
    note.insert(
        "cw".to_string(),
        draft.cw.clone().map(Value::String).unwrap_or(Value::Null),
    );
    note.insert("fileIds".to_string(), Value::Array(file_ids));
    note.insert("files".to_string(), Value::Array(files));
    note.insert(
        "visibility".to_string(),
        serde_json::to_value(&draft.visibility).unwrap_or(Value::String("public".to_string())),
    );
    note.insert("visibleUsers".to_string(), Value::Array(visible_users));
    note.insert(
        "user".to_string(),
        pack_user_lite(viewer, &services.public_url, services.clock.now_ms()),
    );
    note.insert(
        "reactionAcceptance".to_string(),
        draft
            .reaction_acceptance
            .as_ref()
            .and_then(|a| serde_json::to_value(a).ok())
            .unwrap_or(Value::Null),
    );
    note.insert("createdAt".to_string(), Value::String(scheduled_at.clone()));
    note.insert("isSchedule".to_string(), Value::Bool(true));
    note.insert("id".to_string(), Value::String(draft.id.clone()));
    note.insert("renote".to_string(), renote);
    note.insert("reply".to_string(), reply);
    note.insert(
        "renoteId".to_string(),
        draft
            .renote_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    note.insert(
        "replyId".to_string(),
        draft
            .reply_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    if !poll.is_null() {
        note.insert("poll".to_string(), poll);
    }
    Ok(serde_json::json!({
        "id": draft.id,
        "userId": draft.user_id,
        "scheduledAt": scheduled_at,
        "note": note,
    }))
}
