//! Pack note_draft (split from `packers.rs`). Pure move.
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

/// `NoteDraft` (Misskey `note-draft.ts` `packedNoteDraftSchema` parity):
/// every draft column; `user` packed inline as `UserLite`; `files` from
/// `file_ids`; `reply`/`renote` packed one level when resolvable (else
/// `null`); `channel`/`channelId` always null (channels do not exist);
/// `poll` is `{choices, multiple, expiresAt (ISO|null), expiredAfter}` or
/// `null`; `scheduledAt` is epoch millis or `null`.
pub fn pack_note_draft(draft: &NoteDraft, services: &AppServices) -> Value {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&draft.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let user = match services.accounts.find_by_id(&draft.user_id) {
        Ok(Some(account)) => {
            pack_user_lite(&account, &services.public_url, services.clock.now_ms())
        }
        _ => Value::Null,
    };
    let mut files = Vec::new();
    for fid in &draft.file_ids {
        if let Ok(Some(meta)) = services.media.get(fid) {
            files.push(pack_drive_file(&meta, services));
        }
    }
    let viewer = Some(draft.user_id.as_str());
    let reply = match draft.reply_id.as_deref() {
        Some(id) => match services.notes.get_note(id) {
            Ok(Some(note)) => pack_note(&note, viewer, services, 1).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        None => Value::Null,
    };
    let renote = match draft.renote_id.as_deref() {
        Some(id) => match services.notes.get_note(id) {
            Ok(Some(note)) => pack_note(&note, viewer, services, 1).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        None => Value::Null,
    };
    let poll = if draft.has_poll {
        serde_json::json!({
            "choices": draft.poll_choices,
            "multiple": draft.poll_multiple,
            "expiresAt": draft.poll_expires_at_ms.map(format_millis).map(Value::String).unwrap_or(Value::Null),
            "expiredAfter": draft.poll_expired_after_ms.map(|v| Value::Number(v.into())).unwrap_or(Value::Null),
        })
    } else {
        Value::Null
    };
    serde_json::json!({
        "id": draft.id,
        "createdAt": created_at,
        "text": draft.text.clone().map(Value::String).unwrap_or(Value::Null),
        "cw": draft.cw.clone().map(Value::String).unwrap_or(Value::Null),
        "userId": draft.user_id,
        "user": user,
        "replyId": draft.reply_id.clone().map(Value::String).unwrap_or(Value::Null),
        "renoteId": draft.renote_id.clone().map(Value::String).unwrap_or(Value::Null),
        "reply": reply,
        "renote": renote,
        "visibility": serde_json::to_value(&draft.visibility).unwrap_or(Value::String("public".to_string())),
        "visibleUserIds": draft.visible_user_ids,
        "fileIds": draft.file_ids,
        "files": files,
        "hashtag": draft.hashtag.clone().map(Value::String).unwrap_or(Value::Null),
        "poll": poll,
        "channelId": Value::Null,
        "channel": Value::Null,
        "localOnly": draft.local_only,
        "reactionAcceptance": draft.reaction_acceptance.as_ref().and_then(|a| serde_json::to_value(a).ok()).unwrap_or(Value::Null),
        "scheduledAt": draft.scheduled_at_ms.map(|v| Value::Number(v.into())).unwrap_or(Value::Null),
        "isActuallyScheduled": draft.is_actually_scheduled,
    })
}
