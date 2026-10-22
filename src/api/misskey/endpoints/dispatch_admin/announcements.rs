//! Admin dispatcher arms (announcements).
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

/// Pack one admin announcement row (admin `announcements/*` shape).
pub(crate) fn pack_admin_announcement_record(
    record: &crate::domain::admin::AnnouncementRecord,
) -> Value {
    serde_json::json!({
        "id": record.id,
        "title": record.title,
        "text": record.text,
        "imageUrl": record.image_url,
        "createdAt": crate::activitypub::format_millis(record.created_at_ms),
        "updatedAt": crate::activitypub::format_millis(record.updated_at_ms),
    })
}

/// `POST /api/admin/announcements/create` (split from `api_dispatch_blocking`).
pub(crate) fn admin_announcements_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["title", "text", "imageUrl"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:announcements")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/announcements/create")?;
    let title = required_str(&obj, "title")?;
    let text = required_str(&obj, "text")?;
    let image_url = opt_string(&obj, "imageUrl")?;
    let record = state
        .services
        .admin_announcement_create(&title, &text, image_url)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_admin_announcement_record(&record)).into_response())
}

/// `POST /api/admin/announcements/list` (split from `api_dispatch_blocking`).
pub(crate) fn admin_announcements_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:announcements")?;
    require_moderator(&caller)?;
    let rows = state
        .services
        .admin_announcement_list()
        .map_err(ApiError::from_port_error)?;
    let out: Vec<Value> = rows.iter().map(pack_admin_announcement_record).collect();
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/admin/announcements/update` (split from `api_dispatch_blocking`).
pub(crate) fn admin_announcements_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["id", "title", "text", "imageUrl"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:announcements")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/announcements/update")?;
    let id = required_str(&obj, "id")?;
    let title = required_str(&obj, "title")?;
    let text = required_str(&obj, "text")?;
    let image_url = opt_string(&obj, "imageUrl")?;
    let record = state
        .services
        .admin_announcement_update(&id, &title, &text, image_url)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_admin_announcement_record(&record)).into_response())
}

/// `POST /api/admin/announcements/delete` (split from `api_dispatch_blocking`).
pub(crate) fn admin_announcements_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["id"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:announcements")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/announcements/delete")?;
    let id = required_str(&obj, "id")?;
    state
        .services
        .admin_announcement_delete(&id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/stats` (split from `api_dispatch_blocking`).
pub(crate) fn admin_stats(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:meta")?;
    require_moderator(&caller)?;
    let stats = state
        .services
        .admin_stats_overview()
        .map_err(ApiError::from_port_error)?;
    Ok(Json(stats).into_response())
}

/// `POST /api/admin/server-info` (split from `api_dispatch_blocking`).
pub(crate) fn admin_server_info(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:meta")?;
    require_moderator(&caller)?;
    Ok(Json(state.services.admin_server_info()).into_response())
}

/// `POST /api/admin/update-proxy-account` (split from `api_dispatch_blocking`).
pub(crate) fn admin_update_proxy_account(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:proxy-account")?;
    require_admin(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/update-proxy-account")?;
    let user_id = opt_string(&obj, "userId")?;
    state
        .services
        .proxy_account_set(user_id.as_deref())
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/proxy-account` (split from `api_dispatch_blocking`).
pub(crate) fn admin_proxy_account(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:proxy-account")?;
    require_moderator(&caller)?;
    let user_id = state
        .services
        .proxy_account_get()
        .map_err(ApiError::from_port_error)?;
    Ok(Json(serde_json::json!({ "userId": user_id })).into_response())
}
