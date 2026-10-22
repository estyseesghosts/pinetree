//! I/notifications/registry/webhook dispatcher arms (registry).
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

/// `POST /api/i/registry/get` (split from `api_dispatch_blocking`).
pub(crate) fn i_registry_get(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["key", "scope", "domain"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let key = required_str(&obj, "key")?;
    let scope = parse_registry_scope(&obj)?;
    let domain = registry_domain(&state.services, &caller, &obj)?;
    let item = state
        .services
        .registry
        .registry_get(&caller.account.id, domain.as_deref(), &scope, &key)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(ApiError::registry_no_such_key)?;
    Ok(Json(item.value).into_response())
}

/// `POST /api/i/registry/set` (split from `api_dispatch_blocking`).
pub(crate) fn i_registry_set(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["key", "value", "scope", "domain"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let key = required_str(&obj, "key")?;
    if key.is_empty() {
        return Err(ApiError::invalid_param("key", "must be a non-empty string"));
    }
    let value = obj
        .get("value")
        .cloned()
        .ok_or_else(|| ApiError::invalid_param("value", "this field is required"))?;
    let scope = parse_registry_scope(&obj)?;
    let domain = registry_domain(&state.services, &caller, &obj)?;
    state
        .services
        .registry
        .registry_set(RegistryItem {
            id: state.services.ids.generate(),
            user_id: caller.account.id.clone(),
            domain,
            scope,
            key,
            value,
            updated_at_ms: state.services.clock.now_ms(),
        })
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/registry/get-all` (split from `api_dispatch_blocking`).
pub(crate) fn i_registry_get_all(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["scope", "domain"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let scope = parse_registry_scope(&obj)?;
    let domain = registry_domain(&state.services, &caller, &obj)?;
    let items = state
        .services
        .registry
        .registry_list(&caller.account.id, domain.as_deref(), &scope)
        .map_err(ApiError::from_port_error)?;
    let mut out = Map::new();
    for item in items {
        out.insert(item.key, item.value);
    }
    Ok(Json(Value::Object(out)).into_response())
}

/// `POST /api/i/registry/get-detail` (split from `api_dispatch_blocking`).
pub(crate) fn i_registry_get_detail(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["key", "scope", "domain"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let key = required_str(&obj, "key")?;
    let scope = parse_registry_scope(&obj)?;
    let domain = registry_domain(&state.services, &caller, &obj)?;
    let item = state
        .services
        .registry
        .registry_get(&caller.account.id, domain.as_deref(), &scope, &key)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(ApiError::registry_no_such_key)?;
    Ok(Json(serde_json::json!({
        "value": item.value,
        "updatedAt": crate::activitypub::format_millis(item.updated_at_ms),
    }))
    .into_response())
}

/// `POST /api/i/registry/keys` (split from `api_dispatch_blocking`).
pub(crate) fn i_registry_keys(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["scope", "domain"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let scope = parse_registry_scope(&obj)?;
    let domain = registry_domain(&state.services, &caller, &obj)?;
    let items = state
        .services
        .registry
        .registry_list(&caller.account.id, domain.as_deref(), &scope)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(Value::Array(
        items
            .into_iter()
            .map(|item| Value::String(item.key))
            .collect(),
    ))
    .into_response())
}

/// `POST /api/i/registry/keys-with-type` (split from `api_dispatch_blocking`).
pub(crate) fn i_registry_keys_with_type(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["scope", "domain"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let scope = parse_registry_scope(&obj)?;
    let domain = registry_domain(&state.services, &caller, &obj)?;
    let items = state
        .services
        .registry
        .registry_list(&caller.account.id, domain.as_deref(), &scope)
        .map_err(ApiError::from_port_error)?;
    let mut out = Map::new();
    for item in items {
        // `typeof value` parity: JSON null/array/object all read
        // `object`, like upstream `typeof`.
        let kind = match &item.value {
            Value::String(_) => "string",
            Value::Number(_) => "number",
            Value::Bool(_) => "boolean",
            _ => "object",
        };
        out.insert(item.key, Value::String(kind.to_string()));
    }
    Ok(Json(Value::Object(out)).into_response())
}

/// `POST /api/i/registry/remove` (split from `api_dispatch_blocking`).
pub(crate) fn i_registry_remove(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["key", "scope", "domain"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let key = required_str(&obj, "key")?;
    let scope = parse_registry_scope(&obj)?;
    let domain = registry_domain(&state.services, &caller, &obj)?;
    state
        .services
        .registry
        .registry_remove(&caller.account.id, domain.as_deref(), &scope, &key)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/registry/scopes-with-domain` (split from `api_dispatch_blocking`).
pub(crate) fn i_registry_scopes_with_domain(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let pairs = state
        .services
        .registry
        .registry_scopes(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    let out: Vec<Value> = pairs
        .into_iter()
        .map(|(domain, scopes)| {
            serde_json::json!({
                "domain": domain,
                "scopes": scopes,
            })
        })
        .collect();
    Ok(Json(Value::Array(out)).into_response())
}
