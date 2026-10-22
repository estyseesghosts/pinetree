//! Pack blocking (split from `packers.rs`). Pure move.
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

/// `Blocking` (`blocking.ts` `packedBlockingSchema`):
/// `{id, createdAt, blockeeId, blockee}` with the blockee packed as
/// `UserDetailedNotMe` (`Null` when the account row is gone).
pub fn pack_blocking(
    blocking: &Blocking,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&blocking.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let blockee = match services.accounts.find_by_id(&blocking.blockee_id)? {
        Some(account) => pack_user_detailed_not_me(&account, viewer_id, services)?,
        None => Value::Null,
    };
    Ok(serde_json::json!({
        "id": blocking.id,
        "createdAt": created_at,
        "blockeeId": blocking.blockee_id,
        "blockee": blockee,
    }))
}
