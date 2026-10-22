//! Pack following (split from `packers.rs`). Pure move.
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

/// `Following` (`following.ts` `packedFollowingSchema`):
/// `{id, createdAt, followerId, followeeId, follower, followee}` with both
/// sides packed as `UserDetailedNotMe` (`Null` when the account row is
/// gone). `createdAt` derives from the follow-record id timestamp (aidx ids
/// are time-ordered; profile §3).
pub fn pack_following(
    follow: &Follow,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&follow.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let follower = match services.accounts.find_by_id(&follow.follower_id)? {
        Some(account) => pack_user_detailed_not_me(&account, viewer_id, services)?,
        None => Value::Null,
    };
    let followee = match services.accounts.find_by_id(&follow.followee_id)? {
        Some(account) => pack_user_detailed_not_me(&account, viewer_id, services)?,
        None => Value::Null,
    };
    Ok(serde_json::json!({
        "id": follow.id,
        "createdAt": created_at,
        "followerId": follow.follower_id,
        "followeeId": follow.followee_id,
        "follower": follower,
        "followee": followee,
    }))
}
