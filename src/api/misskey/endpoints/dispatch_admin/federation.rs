//! Admin dispatcher arms (federation).
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

/// Pack one federation override row (admin/federation `update-instance`
/// shape).
pub(crate) fn pack_federation_override(
    record: &crate::domain::admin::FederationInstanceOverride,
) -> Value {
    serde_json::json!({
        "host": record.host,
        "isSuspended": record.is_suspended,
        "isBlocked": record.is_blocked,
        "isSilenced": record.is_silenced,
        "moderationNote": record.moderation_note,
        "updatedAt": crate::activitypub::format_millis(record.updated_at_ms),
    })
}

/// `POST /api/admin/queue/stats` (split from `api_dispatch_blocking`).
pub(crate) fn admin_queue_stats(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:queue")?;
    require_moderator(&caller)?;
    let stats = state
        .services
        .queue_stats()
        .map_err(ApiError::from_port_error)?;
    Ok(Json(stats).into_response())
}

// `admin/queue/deliver-delayed`, `admin/queue/inbox-delayed`,
// `admin/queue/clear`: loud 400s - no enumeration/clear ports exist
// for the unified queue (proposed in the subagent report).
/// Shared body for `admin/queue/deliver-delayed`, `admin/queue/inbox-delayed`, `admin/queue/clear` (split from `api_dispatch_blocking`).
fn admin_queue_delayed_inner(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:queue")?;
    require_admin(&caller)?;
    state
        .services
        .queue_delayed_unsupported()
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/queue/deliver-delayed` (split from `api_dispatch_blocking`).
pub(crate) fn admin_queue_deliver_delayed(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_queue_delayed_inner(state, obj, caller, rate_key, client_ip, headers)
}

/// `POST /api/admin/queue/inbox-delayed` (split from `api_dispatch_blocking`).
pub(crate) fn admin_queue_inbox_delayed(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_queue_delayed_inner(state, obj, caller, rate_key, client_ip, headers)
}

/// `POST /api/admin/queue/clear` (split from `api_dispatch_blocking`).
pub(crate) fn admin_queue_clear(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_queue_delayed_inner(state, obj, caller, rate_key, client_ip, headers)
}

// `admin/federation/list` + `federation/instances`: observed remote
// hosts merged with moderator overrides.
/// Shared body for `admin/federation/list`, `federation/instances` (split from `api_dispatch_blocking`).
fn admin_federation_list_inner(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:federation")?;
    require_moderator(&caller)?;
    let rows = state
        .services
        .federation_instances()
        .map_err(ApiError::from_port_error)?;
    Ok(Json(Value::Array(rows)).into_response())
}

/// `POST /api/admin/federation/list` (split from `api_dispatch_blocking`).
pub(crate) fn admin_federation_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_federation_list_inner(state, obj, caller, rate_key, client_ip, headers)
}

/// `POST /api/federation/instances` (split from `api_dispatch_blocking`).
pub(crate) fn federation_instances(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_federation_list_inner(state, obj, caller, rate_key, client_ip, headers)
}

// `admin/federation/show-instance` + `federation/show-instance
// {host!}`: one observed host merged with its override, or null
// when unknown.
/// Shared body for `admin/federation/show-instance`, `federation/show-instance` (split from `api_dispatch_blocking`).
fn admin_federation_show_instance_inner(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["host"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:federation")?;
    require_moderator(&caller)?;
    let host = required_str(&obj, "host")?;
    let row = state
        .services
        .federation_show_instance(&host)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(row.unwrap_or(Value::Null)).into_response())
}

/// `POST /api/admin/federation/show-instance` (split from `api_dispatch_blocking`).
pub(crate) fn admin_federation_show_instance(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_federation_show_instance_inner(state, obj, caller, rate_key, client_ip, headers)
}

/// `POST /api/federation/show-instance` (split from `api_dispatch_blocking`).
pub(crate) fn federation_show_instance(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_federation_show_instance_inner(state, obj, caller, rate_key, client_ip, headers)
}

// `admin/federation/update-instance` +
// `federation/update-instance`: store the moderator override.
/// Shared body for `admin/federation/update-instance`, `federation/update-instance` (split from `api_dispatch_blocking`).
fn admin_federation_update_instance_inner(
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
            "host",
            "isSuspended",
            "isBlocked",
            "isSilenced",
            "moderationNote",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:federation")?;
    require_admin(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/federation/update-instance")?;
    let host = required_str(&obj, "host")?;
    let record = state
        .services
        .federation_update_instance(
            &host,
            parse_opt_bool(&obj, "isSuspended")?,
            parse_opt_bool(&obj, "isBlocked")?,
            parse_opt_bool(&obj, "isSilenced")?,
            opt_string(&obj, "moderationNote")?,
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_federation_override(&record)).into_response())
}

/// `POST /api/admin/federation/update-instance` (split from `api_dispatch_blocking`).
pub(crate) fn admin_federation_update_instance(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_federation_update_instance_inner(state, obj, caller, rate_key, client_ip, headers)
}

/// `POST /api/federation/update-instance` (split from `api_dispatch_blocking`).
pub(crate) fn federation_update_instance(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_federation_update_instance_inner(state, obj, caller, rate_key, client_ip, headers)
}

// `admin/federation/refresh-remote-instance-metadata` +
// `federation/refresh-remote-instance-metadata {host!}`: touch the
// override so caches re-read it (no live metadata refetch).
/// Shared body for `admin/federation/refresh-remote-instance-metadata`, `federation/refresh-remote-instance-metadata` (split from `api_dispatch_blocking`).
fn admin_federation_refresh_inner(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["host"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:federation")?;
    require_admin(&caller)?;
    state.rate_limiter.check(
        &rate_key,
        "admin/federation/refresh-remote-instance-metadata",
    )?;
    let host = required_str(&obj, "host")?;
    state
        .services
        .federation_refresh_metadata(&host)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/federation/refresh-remote-instance-metadata` (split from `api_dispatch_blocking`).
pub(crate) fn admin_federation_refresh_remote_instance_metadata(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_federation_refresh_inner(state, obj, caller, rate_key, client_ip, headers)
}

/// `POST /api/federation/refresh-remote-instance-metadata` (split from `api_dispatch_blocking`).
pub(crate) fn federation_refresh_remote_instance_metadata(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_federation_refresh_inner(state, obj, caller, rate_key, client_ip, headers)
}
