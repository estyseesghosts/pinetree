//! Remaining dispatcher arms (gallery/flash/hashtags/announcements etc.) (moderation).
//!
//! Split from `endpoints/dispatch_rest.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{client_ip, require_caller};
use super::super::parse::{
    announcement_store, fetch_rss_feed, get_limit_with_default, is_fetchable_http_url,
    map_gallery_error, opt_bool, opt_i64, opt_string, pack_account_for, parse_id_cursor,
    parse_list_cursor, read_cpu_model, read_mem_total, required_str, str_list,
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

/// `POST /api/blocking/create` (split from `api_dispatch_blocking`).
pub(crate) fn blocking_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:blocks")?;
    state.rate_limiter.check(&rate_key, "blocking/create")?;
    let user_id = required_str(&obj, "userId")?;
    // Upstream order: self-check, then user existence, then dup.
    if caller.account.id == user_id {
        return Err(ApiError::from_port_error(PortError::Validation(
            "BLOCKEE_IS_YOURSELF".to_string(),
        )));
    }
    state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    state
        .services
        .block(&caller.account.id, &user_id)
        .map_err(ApiError::from_port_error)?;
    let blockee = state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(
        pack_user_detailed_not_me(&blockee, Some(&caller.account.id), &state.services)
            .map_err(ApiError::from_port_error)?,
    )
    .into_response())
}

/// `POST /api/blocking/delete` (split from `api_dispatch_blocking`).
pub(crate) fn blocking_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:blocks")?;
    state.rate_limiter.check(&rate_key, "blocking/delete")?;
    let user_id = required_str(&obj, "userId")?;
    if caller.account.id == user_id {
        return Err(ApiError::from_port_error(PortError::Validation(
            "BLOCKEE_IS_YOURSELF".to_string(),
        )));
    }
    state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    state
        .services
        .unblock(&caller.account.id, &user_id)
        .map_err(ApiError::from_port_error)?;
    let blockee = state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(
        pack_user_detailed_not_me(&blockee, Some(&caller.account.id), &state.services)
            .map_err(ApiError::from_port_error)?,
    )
    .into_response())
}

/// `POST /api/blocking/list` (split from `api_dispatch_blocking`).
pub(crate) fn blocking_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:blocks")?;
    let cursor = parse_list_cursor(&obj)?;
    let rows = state
        .services
        .blocks
        .list_blockings(&caller.account.id, &cursor)
        .map_err(ApiError::from_port_error)?;
    let viewer_id = Some(caller.account.id.as_str());
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(
            pack_blocking(row, viewer_id, &state.services).map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/mute/create` (split from `api_dispatch_blocking`).
pub(crate) fn mute_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "expiresAt"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:mutes")?;
    state.rate_limiter.check(&rate_key, "mute/create")?;
    let user_id = required_str(&obj, "userId")?;
    if caller.account.id == user_id {
        return Err(ApiError::from_port_error(PortError::Validation(
            "MUTEE_IS_YOURSELF".to_string(),
        )));
    }
    state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    let expires_at = opt_i64(&obj, "expiresAt")?;
    state
        .services
        .mute(&caller.account.id, &user_id, expires_at)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/mute/delete` (split from `api_dispatch_blocking`).
pub(crate) fn mute_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:mutes")?;
    let user_id = required_str(&obj, "userId")?;
    if caller.account.id == user_id {
        return Err(ApiError::from_port_error(PortError::Validation(
            "MUTEE_IS_YOURSELF".to_string(),
        )));
    }
    state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    state
        .services
        .unmute(&caller.account.id, &user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/mute/list` (split from `api_dispatch_blocking`).
pub(crate) fn mute_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:mutes")?;
    let cursor = parse_list_cursor(&obj)?;
    let rows = state
        .services
        .mutes
        .list_mutings(&caller.account.id, &cursor)
        .map_err(ApiError::from_port_error)?;
    let viewer_id = Some(caller.account.id.as_str());
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(pack_muting(row, viewer_id, &state.services).map_err(ApiError::from_port_error)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/renote-mute/create` (split from `api_dispatch_blocking`).
pub(crate) fn renote_mute_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:mutes")?;
    state.rate_limiter.check(&rate_key, "renote-mute/create")?;
    let user_id = required_str(&obj, "userId")?;
    if caller.account.id == user_id {
        return Err(ApiError::from_port_error(PortError::Validation(
            "MUTEE_IS_YOURSELF".to_string(),
        )));
    }
    state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    state
        .services
        .renote_mute(&caller.account.id, &user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/renote-mute/delete` (split from `api_dispatch_blocking`).
pub(crate) fn renote_mute_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:mutes")?;
    let user_id = required_str(&obj, "userId")?;
    if caller.account.id == user_id {
        return Err(ApiError::from_port_error(PortError::Validation(
            "MUTEE_IS_YOURSELF".to_string(),
        )));
    }
    state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    state
        .services
        .renote_unmute(&caller.account.id, &user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/renote-mute/list` (split from `api_dispatch_blocking`).
pub(crate) fn renote_mute_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:mutes")?;
    let cursor = parse_list_cursor(&obj)?;
    let rows = state
        .services
        .mutes
        .list_renote_mutings(&caller.account.id, &cursor)
        .map_err(ApiError::from_port_error)?;
    let viewer_id = Some(caller.account.id.as_str());
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(
            pack_renote_muting(row, viewer_id, &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}
