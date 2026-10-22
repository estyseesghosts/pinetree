//! Pack clip (split from `packers.rs`). Pure move.
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

/// Clip pack (federated clips + pages handoff, Phase 1; upstream
/// `ClipEntityService.pack` parity): `{id, createdAt, lastClippedAt|null,
/// userId, user: UserLite, name, description|null, isPublic,
/// favoritedCount, isFavorited? (only when authed), notesCount? (only
/// when the viewer owns the clip)}`. Federation columns (`tier`,
/// `federate`, approvers) are internal-only, never packed.
pub fn pack_clip(
    clip: &crate::domain::clip::Clip,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let owner = services
        .accounts
        .find_by_id(&clip.owner_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let created_at = format_millis(
        services
            .ids
            .parse_time(&clip.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let mut obj = Map::new();
    obj.insert("id".to_string(), Value::String(clip.id.clone()));
    obj.insert("createdAt".to_string(), Value::String(created_at));
    obj.insert(
        "lastClippedAt".to_string(),
        clip.last_clipped_at_ms
            .map(format_millis)
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert("userId".to_string(), Value::String(clip.owner_id.clone()));
    obj.insert(
        "user".to_string(),
        pack_user_lite(&owner, &services.public_url, services.clock.now_ms()),
    );
    obj.insert("name".to_string(), Value::String(clip.name.clone()));
    obj.insert(
        "description".to_string(),
        clip.description
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert("isPublic".to_string(), Value::Bool(clip.is_public));
    obj.insert(
        "favoritedCount".to_string(),
        Value::from(services.clips.clip_favorite_count(&clip.id)?),
    );
    if let Some(viewer) = viewer_id {
        obj.insert(
            "isFavorited".to_string(),
            Value::Bool(services.clips.is_clip_favorited(viewer, &clip.id)?),
        );
        obj.insert(
            "isFollowing".to_string(),
            Value::Bool(services.clips.is_following_clip(viewer, &clip.id)?),
        );
        if viewer == clip.owner_id.as_str() {
            obj.insert(
                "notesCount".to_string(),
                Value::from(services.clips.count_clip_notes(&clip.id)?),
            );
        }
    }
    Ok(Value::Object(obj))
}
