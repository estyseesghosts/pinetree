//! Pack note_reaction (split from `packers.rs`). Pure move.
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
use super::note::pack_note;
use super::user::pack_user_lite;

/// `NoteReaction` + `Note` (`note-reaction.ts`
/// `packedNoteReactionWithNoteSchema`):
/// `{id, createdAt, user: UserLite, type, note: Note}`. `createdAt`
/// derives from the reaction-row id timestamp (aidx ids are time-ordered).
/// `type` is the stored reaction string verbatim (legacy-emoji
/// normalization rides with §6, which owns the reaction path).
pub fn pack_note_reaction_with_note(
    reaction: &Reaction,
    author: &Account,
    note: &Note,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&reaction.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let user = pack_user_lite(author, &services.public_url, services.clock.now_ms());
    let packed_note = pack_note(note, viewer_id, services, 0)?;
    Ok(serde_json::json!({
        "id": reaction.id,
        "createdAt": created_at,
        "user": user,
        "type": reaction.reaction,
        "note": packed_note,
    }))
}
/// `NoteReaction` (`note-reaction.ts` `packedNoteReactionSchema`):
/// `{id, createdAt, user: UserLite, type}`.
pub fn pack_note_reaction(reaction: &Reaction, reactor: &Account, services: &AppServices) -> Value {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&reaction.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    serde_json::json!({
        "id": reaction.id,
        "createdAt": created_at,
        "user": pack_user_lite(reactor, &services.public_url, services.clock.now_ms()),
        "type": reaction.reaction,
    })
}
