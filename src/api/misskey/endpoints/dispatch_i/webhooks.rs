//! I/notifications/registry/webhook dispatcher arms (webhooks).
//!
//! Split from `endpoints/dispatch_i.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{
    auth_ip_key, client_ip, require_caller, send_email_now, set_email_sender,
    verify_and_upgrade_password,
};
use super::super::parse::{
    email_available, email_shape_valid, is_oldest_admin, map_import_file_error, map_webhook_error,
    migrate_followers_to, opt_bool, opt_string, opt_usize, parse_acct, parse_cursor,
    parse_notification_query, parse_registry_scope, random_code, registry_domain, required_str,
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

/// `POST /api/system-webhook/create` (split from `api_dispatch_blocking`).
pub(crate) fn system_webhook_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["name", "url", "secret", "trigger"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:system-webhook")?;
    require_admin(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "system-webhook/create")?;
    let input = CreateSystemWebhookInput {
        name: required_str(&obj, "name")?,
        url: required_str(&obj, "url")?,
        secret: opt_string(&obj, "secret")?,
        trigger: required_str(&obj, "trigger")?,
    };
    let row = create_system_webhook(
        state.services.system_webhooks.as_ref(),
        state.services.ids.as_ref(),
        state.services.clock.as_ref(),
        caller.account.is_admin,
        input,
    )
    .map_err(|err| map_webhook_error(err, "system-webhook/create"))?;
    Ok(Json(pack_system_webhook(&row)).into_response())
}

/// `POST /api/system-webhook/list` (split from `api_dispatch_blocking`).
pub(crate) fn system_webhook_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:system-webhook")?;
    require_admin(&caller)?;
    state.rate_limiter.check(&rate_key, "system-webhook/list")?;
    let rows = state
        .services
        .system_webhooks
        .list_system_webhooks()
        .map_err(ApiError::from_port_error)?;
    let packed: Vec<Value> = rows.iter().map(pack_system_webhook).collect();
    Ok(Json(Value::Array(packed)).into_response())
}

/// `POST /api/system-webhook/show` (split from `api_dispatch_blocking`).
pub(crate) fn system_webhook_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["webhookId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:system-webhook")?;
    require_admin(&caller)?;
    state.rate_limiter.check(&rate_key, "system-webhook/show")?;
    let webhook_id = required_str(&obj, "webhookId")?;
    let row = show_system_webhook(
        state.services.system_webhooks.as_ref(),
        caller.account.is_admin,
        &webhook_id,
    )
    .map_err(|err| map_webhook_error(err, "system-webhook/show"))?;
    Ok(Json(pack_system_webhook(&row)).into_response())
}

/// `POST /api/system-webhook/update` (split from `api_dispatch_blocking`).
pub(crate) fn system_webhook_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["webhookId", "name", "url", "secret", "trigger", "isActive"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:system-webhook")?;
    require_admin(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "system-webhook/update")?;
    let webhook_id = required_str(&obj, "webhookId")?;
    let patch = UpdateSystemWebhookInput {
        name: opt_string(&obj, "name")?,
        url: opt_string(&obj, "url")?,
        secret: opt_string(&obj, "secret")?,
        trigger: opt_string(&obj, "trigger")?,
        is_active: match obj.get("isActive") {
            None | Some(Value::Null) => None,
            Some(Value::Bool(active)) => Some(*active),
            Some(_) => {
                return Err(ApiError::invalid_param("isActive", "must be a boolean"));
            }
        },
    };
    let row = update_system_webhook(
        state.services.system_webhooks.as_ref(),
        state.services.clock.as_ref(),
        caller.account.is_admin,
        &webhook_id,
        patch,
    )
    .map_err(|err| map_webhook_error(err, "system-webhook/update"))?;
    Ok(Json(pack_system_webhook(&row)).into_response())
}

/// `POST /api/system-webhook/delete` (split from `api_dispatch_blocking`).
pub(crate) fn system_webhook_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["webhookId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:system-webhook")?;
    require_admin(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "system-webhook/delete")?;
    let webhook_id = required_str(&obj, "webhookId")?;
    delete_system_webhook(
        state.services.system_webhooks.as_ref(),
        caller.account.is_admin,
        &webhook_id,
    )
    .map_err(|err| map_webhook_error(err, "system-webhook/delete"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/webhooks/create` (split from `api_dispatch_blocking`).
pub(crate) fn webhooks_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["name", "url", "secret", "trigger"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "webhooks/create")?;
    let input = CreateUserWebhookInput {
        name: required_str(&obj, "name")?,
        url: required_str(&obj, "url")?,
        secret: opt_string(&obj, "secret")?,
        trigger: required_str(&obj, "trigger")?,
    };
    let row = create_user_webhook(
        state.services.user_webhooks.as_ref(),
        state.services.ids.as_ref(),
        state.services.clock.as_ref(),
        &caller.account.id,
        crate::domain::webhook::DEFAULT_WEBHOOK_LIMIT,
        input,
    )
    .map_err(|err| map_webhook_error(err, "webhooks/create"))?;
    Ok(Json(pack_user_webhook(&row)).into_response())
}

/// `POST /api/webhooks/list` (split from `api_dispatch_blocking`).
pub(crate) fn webhooks_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    state.rate_limiter.check(&rate_key, "webhooks/list")?;
    let rows = state
        .services
        .user_webhooks
        .list_user_webhooks(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    let packed: Vec<Value> = rows.iter().map(pack_user_webhook).collect();
    Ok(Json(Value::Array(packed)).into_response())
}

/// `POST /api/webhooks/show` (split from `api_dispatch_blocking`).
pub(crate) fn webhooks_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["webhookId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    state.rate_limiter.check(&rate_key, "webhooks/show")?;
    let webhook_id = required_str(&obj, "webhookId")?;
    let row = show_user_webhook(
        state.services.user_webhooks.as_ref(),
        &caller.account.id,
        &webhook_id,
    )
    .map_err(|err| map_webhook_error(err, "webhooks/show"))?;
    Ok(Json(pack_user_webhook(&row)).into_response())
}

/// `POST /api/webhooks/update` (split from `api_dispatch_blocking`).
pub(crate) fn webhooks_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["webhookId", "name", "url", "secret", "trigger", "isActive"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "webhooks/update")?;
    let webhook_id = required_str(&obj, "webhookId")?;
    let patch = UpdateUserWebhookInput {
        name: opt_string(&obj, "name")?,
        url: opt_string(&obj, "url")?,
        secret: opt_string(&obj, "secret")?,
        trigger: opt_string(&obj, "trigger")?,
        is_active: match obj.get("isActive") {
            None | Some(Value::Null) => None,
            Some(Value::Bool(active)) => Some(*active),
            Some(_) => {
                return Err(ApiError::invalid_param("isActive", "must be a boolean"));
            }
        },
    };
    let row = update_user_webhook(
        state.services.user_webhooks.as_ref(),
        state.services.clock.as_ref(),
        &caller.account.id,
        &webhook_id,
        patch,
    )
    .map_err(|err| map_webhook_error(err, "webhooks/update"))?;
    Ok(Json(pack_user_webhook(&row)).into_response())
}

/// `POST /api/webhooks/delete` (split from `api_dispatch_blocking`).
pub(crate) fn webhooks_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["webhookId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "webhooks/delete")?;
    let webhook_id = required_str(&obj, "webhookId")?;
    delete_user_webhook(
        state.services.user_webhooks.as_ref(),
        &caller.account.id,
        &webhook_id,
    )
    .map_err(|err| map_webhook_error(err, "webhooks/delete"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
