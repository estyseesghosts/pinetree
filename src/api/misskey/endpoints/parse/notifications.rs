//! Shared request parsing, validation and packing helpers (notifications).
//!
//! Split from `endpoints/parse.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::files_http::identicon;
use super::opt_bool;
use super::parse_cursor;
use super::str_list;
use crate::activitypub::federated_collections as ap_collections;
use crate::activitypub::{actor as ap_actor, objects as ap_objects, webfinger as ap_webfinger};
use crate::api::misskey::auth::{
    AuthenticatedCaller, SESSION_COOKIE_NAME, authenticate, extract_raw_token,
    generate_random_token, hash_password_async, hash_token, is_safe_callback_url, needs_rehash,
    render_miauth_page, require_admin, require_moderator, verify_password_async,
};
use crate::api::misskey::errors::ApiError;
use crate::api::misskey::packers::{
    pack_abuse_report, pack_antenna, pack_blocking, pack_clip, pack_drive_file, pack_drive_folder,
    pack_emoji_detailed, pack_emoji_simple, pack_following, pack_invite_code, pack_me_detailed,
    pack_meta, pack_moderation_log, pack_muting, pack_note, pack_note_draft,
    pack_note_reaction_with_note, pack_notification, pack_page, pack_remote_emoji_detailed,
    pack_renote_muting, pack_scheduled_note, pack_system_webhook, pack_user_detailed_not_me,
    pack_user_list, pack_user_lite, pack_user_relation, pack_user_webhook,
};
use crate::api::misskey::request::check_allowed_fields;
use crate::api::misskey::response::{RateLimiter, UploadSlots};
use crate::application::commands::AppServices;
use crate::application::commands::announcements::MemAnnouncementStore;
use crate::application::commands::gallery::FlashVisibility;
use crate::application::commands::gallery::IdCursor;
use crate::application::commands::gallery::MemGalleryStore;
use crate::application::commands::invites::SignupRejection;
use crate::application::commands::misc_endpoints::EmojisV2Page;
use crate::application::commands::misc_endpoints::ping;
use crate::application::commands::pins::FeaturedCache;
use crate::application::commands::webhooks::CreateSystemWebhookInput;
use crate::application::commands::webhooks::CreateUserWebhookInput;
use crate::application::commands::webhooks::UpdateSystemWebhookInput;
use crate::application::commands::webhooks::UpdateUserWebhookInput;
use crate::application::commands::webhooks::create_system_webhook;
use crate::application::commands::webhooks::create_user_webhook;
use crate::application::commands::webhooks::delete_system_webhook;
use crate::application::commands::webhooks::delete_user_webhook;
use crate::application::commands::webhooks::show_system_webhook;
use crate::application::commands::webhooks::show_user_webhook;
use crate::application::commands::webhooks::update_system_webhook;
use crate::application::commands::webhooks::update_user_webhook;
use crate::application::ports::{
    AbuseReportFilter, ApiTokenRecord, BlobStore, CreateDraftInput, CreateNoteInput, EditNoteInput,
    ExportEntity, ImportEntity, InviteListFilter, InviteSort, InviteTypeFilter, JobKind,
    LegacyAuthSessionRecord, MiAuthSessionRecord, ModerationLogFilter, NewJob,
    PasswordResetRequest, PortError, RegistryItem, SigninRecord, TimelineCursor,
    UpdateAccountPatch, UpdateDraftPatch,
};
use crate::domain::account::{
    Account, AvatarDecoration, ChatScope, FollowVisibility, MutedWord, NotificationRecieveConfig,
    ProfileField,
};
use crate::domain::follow::FollowNotify;
use crate::domain::instance::{InviteCode, UgcVisibilityForVisitor};
use crate::domain::moderation::{AbuseReportResolution, ModerationAction};
use crate::domain::note::{MAX_CW_API_CHARS, MAX_NOTE_TEXT_CHARS, ReactionAcceptance, Visibility};
use crate::domain::notification::NotificationKind;
use crate::federation::delivery::{
    fetch_and_upsert_remote_actor, fetch_remote_note, webfinger_discover_actor_uri,
};
use crate::federation::inbox as fed_inbox;
use crate::jobs::worker::JobWorker;
use crate::media::transcode::TranscodeConfig;
use crate::services::email::EmailError;
use crate::services::email::EmailMessage;
use crate::services::email::EmailSendPayload;
use crate::services::email::EmailSender;
use crate::services::email::SmtpConfig;
use crate::services::email::SmtpEmailSender;
use crate::services::email::password_reset_email;
use crate::services::email::verification_email;
use crate::streaming::hub::{Hub, ONLINE_THRESHOLD_MS};
use crate::streaming::server::streaming_handler;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, FromRequest, Multipart, Path, Query, Request, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode,
        header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE},
    },
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use http_body_util::{BodyExt, Limited};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Misskey notification types (misskey `types.ts` `notificationTypes`) that
/// are valid `i/notifications` `includeTypes`/`excludeTypes` entries but can
/// never match a local row. The `reaction:grouped` / `renote:grouped`
/// pseudo-types are valid entries too (§10): like upstream (whose DB query
/// only matches stored rows), including only a grouped type matches nothing
/// and excluding one excludes nothing. (`exportCompleted` left this list in
/// §13: exports now emit it.)
pub(crate) fn is_unemitted_notification_type(name: &str) -> bool {
    matches!(
        name,
        "note"
            | "pollEnded"
            | "reaction:grouped"
            | "renote:grouped"
            | "roleAssigned"
            | "chatRoomInvitationReceived"
            | "achievementEarned"
            | "login"
            | "createToken"
            | "app"
    )
}

/// Misskey `notificationTypes` parity: `test` (Batch P
/// `notifications/test-notification`) is a real emittable kind, not an
/// unemitted alias — it was wrongly listed above until Batch P.
/// Misskey obsolete notification types (`types.ts`
/// `obsoleteNotificationTypes`): accepted for backwards compatibility and
/// silently dropped from filters.
pub(crate) fn is_obsolete_notification_type(name: &str) -> bool {
    matches!(name, "pollVote" | "groupInvited")
}

/// Map one `includeTypes`/`excludeTypes` entry to the local kind.
/// `Ok(None)` = valid but locally unemitted (matches nothing); obsolete
/// types never reach here (the caller drops them first).
pub(crate) fn notification_kind_for(
    field: &str,
    name: &str,
) -> Result<Option<NotificationKind>, ApiError> {
    match name {
        "follow" => Ok(Some(NotificationKind::Follow)),
        "mention" => Ok(Some(NotificationKind::Mention)),
        "reply" => Ok(Some(NotificationKind::Reply)),
        // Quotes notify as `Renote` locally (no separate quote kind), so
        // both Misskey types select renote notifications.
        "renote" | "quote" => Ok(Some(NotificationKind::Renote)),
        // Sharkey `edited` notification (a `notes/edit` addition —
        // Misskey `notificationTypes` has no such entry).
        "edited" => Ok(Some(NotificationKind::Edited)),
        "reaction" => Ok(Some(NotificationKind::Reaction)),
        "receiveFollowRequest" => Ok(Some(NotificationKind::ReceiveFollowRequest)),
        // Batch N WS3: locked-account accepts now emit this.
        "followRequestAccepted" => Ok(Some(NotificationKind::FollowRequestAccepted)),
        // §5: scheduling notifications are now emitted locally.
        "scheduledNotePosted" => Ok(Some(NotificationKind::ScheduledNotePosted)),
        "scheduledNotePostFailed" => Ok(Some(NotificationKind::ScheduledNotePostFailed)),
        // §13: export completions are now emitted locally.
        "exportCompleted" => Ok(Some(NotificationKind::ExportCompleted)),
        // Batch P: test notifications are now emitted locally.
        "test" => Ok(Some(NotificationKind::Test)),
        // Phase 2c: pinetree-only clip/page notifications.
        "clipUpdated" => Ok(Some(NotificationKind::ClipUpdated)),
        "pageLiked" => Ok(Some(NotificationKind::PageLiked)),
        _ if is_unemitted_notification_type(name) => Ok(None),
        _ => Err(ApiError::invalid_param(
            field,
            &format!("unknown notification type: {name}"),
        )),
    }
}

/// Every [`NotificationKind`] the local store can emit, for the
/// exclude-everything early return (`i/notifications` Misskey parity).
pub(crate) const ALL_NOTIFICATION_KINDS: [NotificationKind; 14] = [
    NotificationKind::Follow,
    NotificationKind::ReceiveFollowRequest,
    NotificationKind::FollowRequestAccepted,
    NotificationKind::Mention,
    NotificationKind::Reply,
    NotificationKind::Renote,
    NotificationKind::Reaction,
    NotificationKind::ScheduledNotePosted,
    NotificationKind::ScheduledNotePostFailed,
    NotificationKind::Edited,
    NotificationKind::ExportCompleted,
    NotificationKind::Test,
    NotificationKind::ClipUpdated,
    NotificationKind::PageLiked,
];

/// Shared query params of `i/notifications` and `i/notifications-grouped`
/// (§10; upstream `paramDef` is identical on both endpoints).
pub(crate) struct NotificationQuery {
    pub(crate) cursor: TimelineCursor,
    pub(crate) mark_as_read: bool,
    pub(crate) include: Vec<NotificationKind>,
    pub(crate) exclude: Vec<NotificationKind>,
    pub(crate) matches_nothing: bool,
}

/// Parse the shared notification-list params: cursor + `markAsRead` +
/// kind allow/deny lists (Misskey `notifications.ts` parity — presence,
/// not emptiness, of `includeTypes` decides filtering; an explicitly
/// empty or never-matching allow-list, or denying every emittable kind,
/// matches nothing without touching the store).
pub(crate) fn parse_notification_query(
    obj: &Map<String, Value>,
) -> Result<NotificationQuery, ApiError> {
    let cursor = parse_cursor(obj)?;
    let mark_as_read = opt_bool(obj, "markAsRead", true)?;
    let include_requested = obj.contains_key("includeTypes");
    let mut include: Vec<NotificationKind> = Vec::new();
    for name in str_list(obj, "includeTypes")? {
        if is_obsolete_notification_type(&name) {
            continue;
        }
        if let Some(kind) = notification_kind_for("includeTypes", &name)? {
            if !include.contains(&kind) {
                include.push(kind);
            }
        }
    }
    let mut exclude: Vec<NotificationKind> = Vec::new();
    for name in str_list(obj, "excludeTypes")? {
        if is_obsolete_notification_type(&name) {
            continue;
        }
        if let Some(kind) = notification_kind_for("excludeTypes", &name)? {
            if !exclude.contains(&kind) {
                exclude.push(kind);
            }
        }
    }
    let matches_nothing = (include_requested && include.is_empty())
        || (!exclude.is_empty()
            && ALL_NOTIFICATION_KINDS
                .iter()
                .all(|kind| exclude.contains(kind)));
    Ok(NotificationQuery {
        cursor,
        mark_as_read,
        include,
        exclude,
        matches_nothing,
    })
}

/// Pack `account` for `viewer`: the caller's own account packs as
/// `MeDetailed`, everyone else as `UserDetailedNotMe`.
pub(crate) fn pack_account_for(
    account: &Account,
    viewer: Option<&AuthenticatedCaller>,
    services: &AppServices,
) -> Result<Value, ApiError> {
    if let Some(caller) = viewer {
        if caller.account.id == account.id {
            return pack_me_detailed(account, caller.is_first_party, services)
                .map_err(ApiError::from_port_error);
        }
    }
    let viewer_id = viewer.map(|caller| caller.account.id.as_str());
    pack_user_detailed_not_me(account, viewer_id, services).map_err(ApiError::from_port_error)
}
