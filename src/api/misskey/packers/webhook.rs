//! Pack webhook (split from `packers.rs`). Pure move.
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

/// Pack one system webhook (admin view; the secret is included because the
/// arm is admin-gated, mirroring the owner-view webhook pack below).
pub fn pack_system_webhook(row: &SystemWebhook) -> Value {
    serde_json::json!({
        "id": row.id,
        "name": row.name,
        "url": row.url,
        "secret": row.secret,
        "trigger": row.trigger,
        "isActive": row.is_active,
        "createdAt": format_millis(row.created_at_ms),
        "updatedAt": format_millis(row.updated_at_ms),
    })
}
/// Pack one user webhook (owner view; includes the HMAC secret the owner
/// needs to verify deliveries).
pub fn pack_user_webhook(row: &UserWebhook) -> Value {
    serde_json::json!({
        "id": row.id,
        "userId": row.owner_id,
        "name": row.name,
        "url": row.url,
        "secret": row.secret,
        "trigger": row.trigger,
        "isActive": row.is_active,
        "createdAt": format_millis(row.created_at_ms),
        "updatedAt": format_millis(row.updated_at_ms),
    })
}
