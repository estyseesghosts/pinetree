//! Pack muting (split from `packers.rs`). Pure move.
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

/// `Muting` (`mute.ts` `packedMutingSchema`):
/// `{id, createdAt, expiresAt, muteeId, mutee}`. `expiresAt` is an ISO-8601
/// timestamp or `null` for indefinite mutes.
pub fn pack_muting(
    muting: &Muting,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&muting.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let mutee = match services.accounts.find_by_id(&muting.mutee_id)? {
        Some(account) => pack_user_detailed_not_me(&account, viewer_id, services)?,
        None => Value::Null,
    };
    Ok(serde_json::json!({
        "id": muting.id,
        "createdAt": created_at,
        "expiresAt": muting.expires_at_ms.map(format_millis),
        "muteeId": muting.mutee_id,
        "mutee": mutee,
    }))
}
/// `RenoteMuting` (`renote-mute.ts`): `{id, createdAt, muteeId, mutee}`.
pub fn pack_renote_muting(
    row: &RenoteMuting,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&row.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let mutee = match services.accounts.find_by_id(&row.mutee_id)? {
        Some(account) => pack_user_detailed_not_me(&account, viewer_id, services)?,
        None => Value::Null,
    };
    Ok(serde_json::json!({
        "id": row.id,
        "createdAt": created_at,
        "muteeId": row.mutee_id,
        "mutee": mutee,
    }))
}
