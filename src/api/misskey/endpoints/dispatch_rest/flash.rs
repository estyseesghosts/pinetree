//! Remaining dispatcher arms (gallery/flash/hashtags/announcements etc.) (flash).
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
use super::gallery_store;
use super::pack_gallery_post;
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

/// Pack one [`Flash`](crate::application::commands::gallery::Flash) (same
/// `isLiked` omission as [`pack_gallery_post`]).
pub(crate) fn pack_flash(
    state: &ApiState,
    flash: &crate::application::commands::gallery::Flash,
) -> Result<Value, ApiError> {
    let owner = state
        .services
        .accounts
        .find_by_id(&flash.owner_id)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_USER".to_string()))
        })?;
    let services = &state.services;
    let created_at = services
        .ids
        .parse_time(&flash.id)
        .map(crate::activitypub::format_millis)
        .unwrap_or_else(|_| crate::activitypub::format_millis(flash.created_at_ms));
    let visibility = match flash.visibility {
        FlashVisibility::Public => "public",
        FlashVisibility::Home => "home",
    };
    Ok(serde_json::json!({
        "id": flash.id,
        "createdAt": created_at,
        "updatedAt": crate::activitypub::format_millis(flash.updated_at_ms),
        "userId": flash.owner_id,
        "user": pack_user_lite(&owner, &services.public_url, services.clock.now_ms()),
        "title": flash.title,
        "summary": flash.summary,
        "script": flash.script,
        "visibility": visibility,
        "likedCount": flash.liked_count,
    }))
}

/// `POST /api/flash/create` (split from `api_dispatch_blocking`).
pub(crate) fn flash_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["title", "summary", "script", "visibility"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "flash/create")?;
    let title = required_str(&obj, "title")?;
    let summary = opt_string(&obj, "summary")?;
    let script = required_str(&obj, "script")?;
    let visibility = opt_string(&obj, "visibility")?.unwrap_or_else(|| "public".to_string());
    let flash = state
        .services
        .flash_create(
            gallery_store(),
            &caller.account.id,
            &title,
            summary.as_deref(),
            &script,
            &visibility,
        )
        .map_err(map_gallery_error)?;
    Ok(Json(pack_flash(&state, &flash)?).into_response())
}

/// `POST /api/flash/show` (split from `api_dispatch_blocking`).
pub(crate) fn flash_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["flashId"])?;
    let flash_id = required_str(&obj, "flashId")?;
    let flash = state
        .services
        .flash_show(gallery_store(), &flash_id)
        .map_err(map_gallery_error)?;
    Ok(Json(pack_flash(&state, &flash)?).into_response())
}

/// `POST /api/flash/update` (split from `api_dispatch_blocking`).
pub(crate) fn flash_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["flashId", "title", "summary", "script", "visibility"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "flash/update")?;
    let flash_id = required_str(&obj, "flashId")?;
    let title = opt_string(&obj, "title")?;
    let summary = match obj.get("summary") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(s)) => Some(Some(s.clone())),
        Some(_) => {
            return Err(ApiError::invalid_param("summary", "must be a string"));
        }
    };
    let script = opt_string(&obj, "script")?;
    let visibility = opt_string(&obj, "visibility")?;
    let flash = state
        .services
        .flash_update(
            gallery_store(),
            &caller.account.id,
            &flash_id,
            title.as_deref(),
            summary.as_ref().map(|inner| inner.as_deref()),
            script.as_deref(),
            visibility.as_deref(),
        )
        .map_err(map_gallery_error)?;
    Ok(Json(pack_flash(&state, &flash)?).into_response())
}

/// `POST /api/flash/delete` (split from `api_dispatch_blocking`).
pub(crate) fn flash_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["flashId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "flash/delete")?;
    let flash_id = required_str(&obj, "flashId")?;
    state
        .services
        .flash_delete(gallery_store(), &caller.account.id, &flash_id)
        .map_err(map_gallery_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/flash/like` (split from `api_dispatch_blocking`).
pub(crate) fn flash_like(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["flashId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "flash/like")?;
    let flash_id = required_str(&obj, "flashId")?;
    state
        .services
        .flash_like(gallery_store(), &caller.account.id, &flash_id)
        .map_err(map_gallery_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/flash/unlike` (split from `api_dispatch_blocking`).
pub(crate) fn flash_unlike(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["flashId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "flash/unlike")?;
    let flash_id = required_str(&obj, "flashId")?;
    state
        .services
        .flash_unlike(gallery_store(), &caller.account.id, &flash_id)
        .map_err(map_gallery_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/flash/featured` (split from `api_dispatch_blocking`).
pub(crate) fn flash_featured(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
    let cursor = parse_id_cursor(&obj)?;
    let flashes = state
        .services
        .flash_featured(gallery_store(), &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(flashes.len());
    for flash in &flashes {
        out.push(pack_flash(&state, flash)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/flash/my` (split from `api_dispatch_blocking`).
pub(crate) fn flash_my(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let cursor = parse_id_cursor(&obj)?;
    let flashes = state
        .services
        .flash_my(gallery_store(), &caller.account.id, &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(flashes.len());
    for flash in &flashes {
        out.push(pack_flash(&state, flash)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/flash/my-likes` (split from `api_dispatch_blocking`).
pub(crate) fn flash_my_likes(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let cursor = parse_id_cursor(&obj)?;
    let flashes = state
        .services
        .flash_my_likes(gallery_store(), &caller.account.id, &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(flashes.len());
    for flash in &flashes {
        out.push(pack_flash(&state, flash)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}
