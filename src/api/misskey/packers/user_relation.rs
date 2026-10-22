//! Pack user_relation (split from `packers.rs`). Pure move.
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

/// One `users/relation` entry (Misskey `UserEntityService.getRelation`
/// parity): follow edges + pending requests in both directions + the §0A
/// block/mute flags.
pub fn pack_user_relation(
    me_id: &str,
    target_id: &str,
    services: &AppServices,
) -> Result<Value, PortError> {
    Ok(serde_json::json!({
        "id": target_id,
        "isFollowing": services.follows.is_following(me_id, target_id).unwrap_or(false),
        "hasPendingFollowRequestFromYou": services
            .follows
            .get_follow_request(me_id, target_id)
            .map(|r| r.is_some())
            .unwrap_or(false),
        "hasPendingFollowRequestToYou": services
            .follows
            .get_follow_request(target_id, me_id)
            .map(|r| r.is_some())
            .unwrap_or(false),
        "isFollowed": services.follows.is_following(target_id, me_id).unwrap_or(false),
        "isBlocking": services.blocks.is_blocking(me_id, target_id).unwrap_or(false),
        "isBlocked": services.blocks.is_blocking(target_id, me_id).unwrap_or(false),
        "isMuted": services.mutes.is_muting(me_id, target_id).unwrap_or(false),
        "isRenoteMuted": services
            .mutes
            .is_renote_muting(me_id, target_id)
            .unwrap_or(false),
    }))
}
