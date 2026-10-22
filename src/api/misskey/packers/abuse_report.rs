//! Pack abuse_report (split from `packers.rs`). Pure move.
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

/// `AbuseUserReport` (upstream `AbuseUserReportEntityService.pack`):
/// `{id, createdAt, comment, resolved, reporterId, targetUserId,
/// assigneeId, reporter, targetUser, assignee, forwarded, resolvedAs,
/// moderationNote}` with the three users packed as `UserDetailedNotMe`
/// (`Null` when the account row is gone; assignee is `Null` when unset).
pub fn pack_abuse_report(
    report: &AbuseReport,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&report.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let pack_user_opt = |id: &str| -> Result<Value, PortError> {
        match services.accounts.find_by_id(id)? {
            Some(account) => pack_user_detailed_not_me(&account, viewer_id, services),
            None => Ok(Value::Null),
        }
    };
    Ok(serde_json::json!({
        "id": report.id,
        "createdAt": created_at,
        "comment": report.comment,
        "resolved": report.resolved,
        "reporterId": report.reporter,
        "targetUserId": report.target_account,
        "assigneeId": report.assignee,
        "reporter": pack_user_opt(&report.reporter)?,
        "targetUser": pack_user_opt(&report.target_account)?,
        "assignee": match report.assignee.as_deref() {
            Some(id) => pack_user_opt(id)?,
            None => Value::Null,
        },
        "forwarded": report.forwarded,
        "resolvedAs": report.resolved_as.map(|r| r.as_str()),
        "moderationNote": report.moderation_note,
    }))
}
