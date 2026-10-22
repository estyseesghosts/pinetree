//! Pack drive_file (split from `packers.rs`). Pure move.
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

/// `DriveFile` (`drive-file.ts` `packedDriveFileSchema`).
pub fn pack_drive_file(media: &MediaMeta, services: &AppServices) -> Value {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&media.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let mut properties = Map::new();
    if let Some(width) = media.properties.width {
        properties.insert("width".to_string(), Value::Number(width.into()));
    }
    if let Some(height) = media.properties.height {
        properties.insert("height".to_string(), Value::Number(height.into()));
    }
    if let Some(orientation) = media.properties.orientation {
        properties.insert("orientation".to_string(), Value::Number(orientation.into()));
    }
    if let Some(avg_color) = &media.properties.avg_color {
        properties.insert("avgColor".to_string(), Value::String(avg_color.clone()));
    }
    serde_json::json!({
        "id": media.id,
        "createdAt": created_at,
        "name": media.name,
        "type": media.mime,
        "md5": media.hash,
        "size": media.size_bytes,
        "isSensitive": media.is_sensitive,
        "blurhash": media.blurhash,
        "properties": properties,
        "url": media.url,
        "thumbnailUrl": media.thumbnail_url,
        "comment": media.comment,
        "folderId": drive_file_folder_id(media, services),
        "folder": null,
        "userId": media.owner_id,
        "user": null,
    })
}
/// Resolve a file's packed `folderId` through the durable folder mapping
/// (gaps WP2a final wiring; `None`/unmapped/explicit-root all pack as
/// null, upstream parity). Fail-open to null: the pack path is infallible
/// and the arm already validated placement.
pub(crate) fn drive_file_folder_id(media: &MediaMeta, services: &AppServices) -> Value {
    match services.folders.file_folder_id(&media.id) {
        Ok(placement) => match placement.flatten() {
            Some(folder_id) => Value::String(folder_id),
            None => Value::Null,
        },
        Err(_) => Value::Null,
    }
}
