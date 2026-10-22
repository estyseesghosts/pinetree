//! Pack drive_folder (split from `packers.rs`). Pure move.
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

/// Pack one drive folder (gaps WP2a; upstream `MiDriveFolder` shape: the
/// folder rows carry no timestamps, so no `createdAt`).
pub fn pack_drive_folder(folder: &DriveFolder) -> Value {
    serde_json::json!({
        "id": folder.id,
        "name": folder.name,
        "parentId": folder.parent_id,
        "userId": folder.owner_id,
    })
}
