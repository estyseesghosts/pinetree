//! Pack page (split from `packers.rs`). Pure move.
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
use super::user::pack_user_lite;

/// Collect `fileId`s from `image`-type page blocks (recursive over
/// `section.children`). Referenced *notes* in `note`-blocks are not
/// tracked or packed (pinetree renders quote links at federation time).
pub(crate) fn collect_page_file_ids(content: &Value, out: &mut Vec<String>) {
    let Some(arr) = content.as_array() else {
        return;
    };
    for block in arr {
        let Some(obj) = block.as_object() else {
            continue;
        };
        match obj.get("type").and_then(Value::as_str) {
            Some("image") => {
                if let Some(id) = obj.get("fileId").and_then(Value::as_str) {
                    out.push(id.to_string());
                }
            }
            Some("section") => {
                if let Some(children) = obj.get("children") {
                    collect_page_file_ids(children, out);
                }
            }
            _ => {}
        }
    }
}
/// Page pack (federated clips + pages handoff, Phase 3 local; upstream
/// `PageEntityService.pack` + `packedPageSchema` parity): `{id, createdAt,
/// updatedAt, userId, user: UserLite, content, variables, title, name,
/// summary?, hideTitleWhenPinned, alignCenter, font, script,
/// eyeCatchingImageId, eyeCatchingImage?, attachedFiles, likedCount,
/// isLiked? (only when authed)}`. Legacy `textInput`/`numberInput` block
/// migration is a no-op: pinetree never wrote the legacy shapes.
pub fn pack_page(
    page: &crate::domain::page::Page,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let owner = services
        .accounts
        .find_by_id(&page.owner_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let now_ms = services.clock.now_ms();
    let created_at = format_millis(services.ids.parse_time(&page.id).unwrap_or(now_ms));
    let mut file_ids = Vec::new();
    collect_page_file_ids(&page.content, &mut file_ids);
    let mut attached = Vec::new();
    for fid in &file_ids {
        if let Some(meta) = services.media.get(fid)? {
            attached.push(pack_drive_file(&meta, services));
        }
    }
    let mut obj = Map::new();
    obj.insert("id".to_string(), Value::String(page.id.clone()));
    obj.insert("createdAt".to_string(), Value::String(created_at));
    obj.insert(
        "updatedAt".to_string(),
        Value::String(format_millis(page.updated_at_ms)),
    );
    obj.insert("userId".to_string(), Value::String(page.owner_id.clone()));
    obj.insert(
        "user".to_string(),
        pack_user_lite(&owner, &services.public_url, now_ms),
    );
    obj.insert("content".to_string(), page.content.clone());
    obj.insert("variables".to_string(), page.variables.clone());
    obj.insert("title".to_string(), Value::String(page.title.clone()));
    obj.insert("name".to_string(), Value::String(page.name.clone()));
    if let Some(summary) = &page.summary {
        obj.insert("summary".to_string(), Value::String(summary.clone()));
    }
    obj.insert(
        "hideTitleWhenPinned".to_string(),
        Value::Bool(page.hide_title_when_pinned),
    );
    obj.insert("alignCenter".to_string(), Value::Bool(page.align_center));
    obj.insert(
        "font".to_string(),
        Value::String(page.font.as_str().to_string()),
    );
    obj.insert("script".to_string(), Value::String(page.script.clone()));
    obj.insert(
        "eyeCatchingImageId".to_string(),
        page.eye_catching_image_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    let mut eye_image = Value::Null;
    if let Some(id) = &page.eye_catching_image_id {
        if let Some(meta) = services.media.get(id)? {
            eye_image = pack_drive_file(&meta, services);
        }
    }
    obj.insert("eyeCatchingImage".to_string(), eye_image);
    obj.insert("attachedFiles".to_string(), Value::Array(attached));
    obj.insert(
        "likedCount".to_string(),
        Value::from(page.liked_count as u64),
    );
    if let Some(viewer) = viewer_id {
        obj.insert(
            "isLiked".to_string(),
            Value::Bool(services.pages.is_page_liked(viewer, &page.id)?),
        );
    }
    Ok(Value::Object(obj))
}
