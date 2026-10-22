//! Pack moderation (split from `packers.rs`). Pure move.
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
use super::user::pack_user_detailed_not_me;

/// `ModerationLog` (upstream `ModerationLogEntityService.pack`):
/// `{id, createdAt, type, info, userId, user}` with `user` as
/// `UserDetailedNotMe`. DIVERGENCE: operator (CLI) rows have no acting
/// account, so `userId`/`user` are `Null` there (upstream rows always
/// carry a user).
pub fn pack_moderation_log(
    entry: &ModerationLogEntry,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let created_at = format_millis(entry.created_at_ms);
    let (user_id, user) = match &entry.actor_account {
        Some(actor) => {
            let packed = match services.accounts.find_by_id(actor)? {
                Some(account) => pack_user_detailed_not_me(&account, viewer_id, services)?,
                None => Value::Null,
            };
            (Value::String(actor.clone()), packed)
        }
        None => (Value::Null, Value::Null),
    };
    Ok(serde_json::json!({
        "id": entry.id,
        "createdAt": created_at,
        "type": entry.action.as_str(),
        "info": entry.details,
        "userId": user_id,
        "user": user,
    }))
}
