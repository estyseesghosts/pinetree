//! Admin dispatcher arms (relays_ads).
//!
//! Split from `endpoints/dispatch_admin.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::admin_status::{STATUS_BACKUP_LIST_CAP, build_admin_status, pack_backup_status};
use super::super::auth_http::{EMAIL_SENDER, client_ip, require_caller};
use super::super::middleware::{read_backup_context, read_ops_config};
use super::super::parse::{
    email_shape_valid, get_limit, get_limit_with_default, opt_bool, opt_i64, opt_string, opt_usize,
    pack_account_for, pack_invite_codes, parse_ad_ratio, parse_cursor, parse_invite_expires_at,
    parse_opt_bool, required_i64, required_str, update_bool_field,
};
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

/// Pack one relay row (admin `relays/*` shape).
pub(crate) fn pack_relay_record(record: &crate::domain::admin::RelayRecord) -> Value {
    serde_json::json!({
        "inbox": record.inbox,
        "status": record.status,
    })
}

/// `POST /api/admin/relays/add` (split from `api_dispatch_blocking`).
pub(crate) fn admin_relays_add(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["inbox"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:relays")?;
    require_admin(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/relays/add")?;
    let inbox = required_str(&obj, "inbox")?;
    let record = state
        .services
        .relay_add(&inbox)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_relay_record(&record)).into_response())
}

/// `POST /api/admin/relays/list` (split from `api_dispatch_blocking`).
pub(crate) fn admin_relays_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:relays")?;
    require_moderator(&caller)?;
    let rows = state
        .services
        .relay_list()
        .map_err(ApiError::from_port_error)?;
    let out: Vec<Value> = rows.iter().map(pack_relay_record).collect();
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/admin/relays/remove` (split from `api_dispatch_blocking`).
pub(crate) fn admin_relays_remove(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["inbox"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:relays")?;
    require_admin(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/relays/remove")?;
    let inbox = required_str(&obj, "inbox")?;
    state
        .services
        .relay_remove(&inbox)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Pack one ad row (admin `ad/*` shape; millisecond bounds as ISO-8601).
pub(crate) fn pack_ad_record(record: &crate::domain::admin::AdRecord) -> Value {
    serde_json::json!({
        "id": record.id,
        "url": record.url,
        "memo": record.memo,
        "place": record.place,
        "ratio": record.ratio,
        "priority": record.priority,
        "expiresAt": crate::activitypub::format_millis(record.expires_at_ms),
        "startsAt": crate::activitypub::format_millis(record.starts_at_ms),
        "imageUrl": record.image_url,
        "dayOfWeek": record.day_of_week,
    })
}

/// `POST /api/admin/ad/create` (split from `api_dispatch_blocking`).
pub(crate) fn admin_ad_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &[
            "url",
            "memo",
            "place",
            "ratio",
            "priority",
            "expiresAt",
            "startsAt",
            "imageUrl",
            "dayOfWeek",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:ad")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/ad/create")?;
    let record = state
        .services
        .ad_create(
            &required_str(&obj, "url")?,
            &required_str(&obj, "memo")?,
            &required_str(&obj, "place")?,
            parse_ad_ratio(&obj)?,
            &required_str(&obj, "priority")?,
            required_i64(&obj, "expiresAt")?,
            required_i64(&obj, "startsAt")?,
            &required_str(&obj, "imageUrl")?,
            required_i64(&obj, "dayOfWeek")?,
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_ad_record(&record)).into_response())
}

/// `POST /api/admin/ad/list` (split from `api_dispatch_blocking`).
pub(crate) fn admin_ad_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:ad")?;
    require_moderator(&caller)?;
    let rows = state
        .services
        .ad_list()
        .map_err(ApiError::from_port_error)?;
    let out: Vec<Value> = rows.iter().map(pack_ad_record).collect();
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/admin/ad/update` (split from `api_dispatch_blocking`).
pub(crate) fn admin_ad_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &[
            "id",
            "url",
            "memo",
            "place",
            "ratio",
            "priority",
            "expiresAt",
            "startsAt",
            "imageUrl",
            "dayOfWeek",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:ad")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/ad/update")?;
    let record = state
        .services
        .ad_update(
            &required_str(&obj, "id")?,
            &required_str(&obj, "url")?,
            &required_str(&obj, "memo")?,
            &required_str(&obj, "place")?,
            parse_ad_ratio(&obj)?,
            &required_str(&obj, "priority")?,
            required_i64(&obj, "expiresAt")?,
            required_i64(&obj, "startsAt")?,
            &required_str(&obj, "imageUrl")?,
            required_i64(&obj, "dayOfWeek")?,
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_ad_record(&record)).into_response())
}

/// `POST /api/admin/ad/delete` (split from `api_dispatch_blocking`).
pub(crate) fn admin_ad_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["id"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:ad")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/ad/delete")?;
    let id = required_str(&obj, "id")?;
    state
        .services
        .ad_delete(&id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/ad/refresh` (split from `api_dispatch_blocking`).
pub(crate) fn admin_ad_refresh(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:ad")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/ad/refresh")?;
    state
        .services
        .ad_refresh()
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
