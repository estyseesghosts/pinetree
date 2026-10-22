//! Pack antenna (split from `packers.rs`). Pure move.
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

/// Antenna pack (Batch N WS4; upstream `AntennaEntityService.pack`
/// parity): `{id, createdAt, name, keywords, excludeKeywords, src,
/// userListId, users, caseSensitive, localOnly, excludeBots, withReplies,
/// withFile, excludeNotesInSensitiveChannel, isActive, hasUnreadNote:
/// false (upstream TODO), notify: false (back-compat)}`.
pub fn pack_antenna(antenna: &crate::domain::antenna::Antenna, services: &AppServices) -> Value {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&antenna.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    serde_json::json!({
        "id": antenna.id,
        "createdAt": created_at,
        "name": antenna.name,
        "keywords": antenna.keywords,
        "excludeKeywords": antenna.exclude_keywords,
        "src": antenna.src.as_str(),
        "userListId": antenna.user_list_id,
        "users": antenna.users,
        "caseSensitive": antenna.case_sensitive,
        "localOnly": antenna.local_only,
        "excludeBots": antenna.exclude_bots,
        "withReplies": antenna.with_replies,
        "withFile": antenna.with_file,
        "excludeNotesInSensitiveChannel": antenna.exclude_notes_in_sensitive_channel,
        "isActive": antenna.is_active,
        "hasUnreadNote": false,
        "notify": false,
    })
}
