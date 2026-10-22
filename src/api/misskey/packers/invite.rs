//! Pack invite (split from `packers.rs`). Pure move.
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
use super::user::pack_user_lite;

/// Packed `InviteCode` (upstream `InviteCodeEntityService.pack`):
/// `{id, code, expiresAt, createdAt, createdBy, usedBy, usedAt, used}`.
/// `createdBy`/`usedBy` are `UserLite` or `null`.
pub fn pack_invite_code(
    services: &AppServices,
    invite: &InviteCode,
    created_by: Option<&Account>,
    used_by: Option<&Account>,
) -> Value {
    let clock_ms = services.clock.now_ms();
    serde_json::json!({
        "id": invite.id,
        "code": invite.code,
        "expiresAt": invite.expires_at_ms.map(format_millis),
        "createdAt": format_millis(invite.created_at_ms),
        "createdBy": created_by.map(|account| pack_user_lite(account, &services.public_url, clock_ms)),
        "usedBy": used_by.map(|account| pack_user_lite(account, &services.public_url, clock_ms)),
        "usedAt": invite.used_at_ms.map(format_millis),
        "used": invite.is_used(),
    })
}
