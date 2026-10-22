//! Shared request parsing, validation and packing helpers (query).
//!
//! Split from `endpoints/parse.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::files_http::identicon;
use super::get_limit_with_default;
use super::opt_i64;
use super::opt_string;
use super::opt_usize;
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

/// Process-global announcement store for the `announcements*` +
/// `i/read-announcement` + `promo/read` arms (same interim posture as
/// [`gallery_store`]).
// TODO(final): swap to redb.
pub(crate) static ANNOUNCEMENT_STORE: OnceLock<MemAnnouncementStore> = OnceLock::new();

pub(crate) fn announcement_store() -> &'static MemAnnouncementStore {
    ANNOUNCEMENT_STORE.get_or_init(MemAnnouncementStore::new)
}

/// Parse an `IdCursor` (`sinceId`/`untilId`/`limit`, Misskey gallery
/// parity: default 10, range 1–100) for the gallery/flash list endpoints.
pub(crate) fn parse_id_cursor(obj: &Map<String, Value>) -> Result<IdCursor, ApiError> {
    Ok(IdCursor {
        since_id: opt_string(obj, "sinceId")?,
        until_id: opt_string(obj, "untilId")?,
        limit: get_limit_with_default(obj, 10)?,
    })
}

/// Parse the optional `notify` tri-state for `following/update`
/// (`normal` enables, `none`/absent clears).
pub(crate) fn parse_follow_notify(
    obj: &Map<String, Value>,
) -> Result<Option<FollowNotify>, ApiError> {
    match obj.get("notify") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s == "normal" => Ok(Some(FollowNotify::Normal)),
        Some(Value::String(s)) if s == "none" => Ok(None),
        Some(_) => Err(ApiError::invalid_param("notify", "must be normal or none")),
    }
}

/// Required integer param (Misskey ad/role numeric fields).
pub(crate) fn required_i64(obj: &Map<String, Value>, key: &str) -> Result<i64, ApiError> {
    match obj.get(key) {
        Some(Value::Number(n)) => n
            .as_i64()
            .ok_or_else(|| ApiError::invalid_param(key, "must be an integer")),
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

/// Required float param (`admin/ad` `ratio`).
pub(crate) fn parse_ad_ratio(obj: &Map<String, Value>) -> Result<f64, ApiError> {
    match obj.get("ratio") {
        Some(Value::Number(n)) => n
            .as_f64()
            .ok_or_else(|| ApiError::invalid_param("ratio", "must be a number")),
        _ => Err(ApiError::invalid_param("ratio", "this field is required")),
    }
}

/// Optional boolean param (`None` when absent or null — the tri-state
/// federation override flags).
pub(crate) fn parse_opt_bool(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<bool>, ApiError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(ApiError::invalid_param(key, "must be a boolean")),
    }
}

/// Cursor paging for the block/mute list endpoints (Misskey parity: `limit`
/// defaults to 30, range 1–100).
pub(crate) fn parse_list_cursor(obj: &Map<String, Value>) -> Result<TimelineCursor, ApiError> {
    let limit = match opt_usize(obj, "limit")? {
        None => 30,
        Some(limit) if (1..=100).contains(&limit) => limit,
        Some(_) => {
            return Err(ApiError::invalid_param(
                "limit",
                "must be an integer between 1 and 100",
            ));
        }
    };
    Ok(TimelineCursor {
        since_id: opt_string(obj, "sinceId")?,
        until_id: opt_string(obj, "untilId")?,
        since_date: opt_i64(obj, "sinceDate")?,
        until_date: opt_i64(obj, "untilDate")?,
        limit,
    })
}
