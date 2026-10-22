//! Remaining dispatcher arms (gallery/flash/hashtags/announcements etc.) (gallery).
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

/// Process-global gallery store for the `gallery/*` + `flash/*` arms.
// TODO(final): swap to redb. The `GalleryStore` port has no durable
// implementation yet (sibling-owned `ports.rs`/`redb`), so the arms run
// against the command-layer in-memory store. Single-process, non-durable,
// lost on restart — the arm comments carry the same marker.
pub(crate) static GALLERY_STORE: OnceLock<MemGalleryStore> = OnceLock::new();

pub(crate) fn gallery_store() -> &'static MemGalleryStore {
    GALLERY_STORE.get_or_init(MemGalleryStore::new)
}

/// Pack one [`GalleryPost`](crate::application::commands::gallery::GalleryPost)
/// (upstream `GalleryPost` shape subset: no `isLiked` — the store seam has
/// no per-viewer like query; the field is omitted rather than lied about).
pub(crate) fn pack_gallery_post(
    state: &ApiState,
    post: &crate::application::commands::gallery::GalleryPost,
) -> Result<Value, ApiError> {
    let owner = state
        .services
        .accounts
        .find_by_id(&post.owner_id)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_USER".to_string()))
        })?;
    let services = &state.services;
    let created_at = services
        .ids
        .parse_time(&post.id)
        .map(crate::activitypub::format_millis)
        .unwrap_or_else(|_| crate::activitypub::format_millis(post.created_at_ms));
    let mut files = Vec::new();
    for file_id in &post.file_ids {
        if let Some(meta) = services
            .media
            .get(file_id)
            .map_err(ApiError::from_port_error)?
        {
            files.push(pack_drive_file(&meta, services));
        }
    }
    Ok(serde_json::json!({
        "id": post.id,
        "createdAt": created_at,
        "updatedAt": crate::activitypub::format_millis(post.updated_at_ms),
        "userId": post.owner_id,
        "user": pack_user_lite(&owner, &services.public_url, services.clock.now_ms()),
        "title": post.title,
        "description": post.description,
        "fileIds": post.file_ids,
        "files": files,
        "isSensitive": post.is_sensitive,
        "likedCount": post.liked_count,    }))
}

/// `POST /api/gallery/posts/create` (split from `api_dispatch_blocking`).
pub(crate) fn gallery_posts_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["title", "description", "fileIds", "isSensitive"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state
        .rate_limiter
        .check(&rate_key, "gallery/posts/create")?;
    let title = opt_string(&obj, "title")?;
    let description = opt_string(&obj, "description")?;
    let file_ids = str_list(&obj, "fileIds")?;
    for file_id in &file_ids {
        state
            .services
            .get_media_for(&caller.account.id, file_id)
            .map_err(ApiError::from_port_error)?;
    }
    let is_sensitive = opt_bool(&obj, "isSensitive", false)?;
    let post = state
        .services
        .gallery_post_create(
            gallery_store(),
            &caller.account.id,
            title.as_deref(),
            description.as_deref(),
            file_ids,
            is_sensitive,
        )
        .map_err(map_gallery_error)?;
    Ok(Json(pack_gallery_post(&state, &post)?).into_response())
}

/// `POST /api/gallery/posts/show` (split from `api_dispatch_blocking`).
pub(crate) fn gallery_posts_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["postId"])?;
    let post_id = required_str(&obj, "postId")?;
    let post = state
        .services
        .gallery_post_show(gallery_store(), &post_id)
        .map_err(map_gallery_error)?;
    Ok(Json(pack_gallery_post(&state, &post)?).into_response())
}

/// `POST /api/gallery/posts/update` (split from `api_dispatch_blocking`).
pub(crate) fn gallery_posts_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["postId", "title", "description", "fileIds", "isSensitive"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state
        .rate_limiter
        .check(&rate_key, "gallery/posts/update")?;
    let post_id = required_str(&obj, "postId")?;
    let title = match obj.get("title") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(s)) => Some(Some(s.as_str())),
        Some(_) => {
            return Err(ApiError::invalid_param("title", "must be a string"));
        }
    };
    let description = match obj.get("description") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(s)) => Some(Some(s.as_str())),
        Some(_) => {
            return Err(ApiError::invalid_param("description", "must be a string"));
        }
    };
    let file_ids = match obj.get("fileIds") {
        None | Some(Value::Null) => None,
        Some(Value::Array(_)) => {
            let ids = str_list(&obj, "fileIds")?;
            for file_id in &ids {
                state
                    .services
                    .get_media_for(&caller.account.id, file_id)
                    .map_err(ApiError::from_port_error)?;
            }
            Some(ids)
        }
        Some(_) => {
            return Err(ApiError::invalid_param(
                "fileIds",
                "must be an array of strings",
            ));
        }
    };
    let is_sensitive = match obj.get("isSensitive") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => {
            return Err(ApiError::invalid_param("isSensitive", "must be a boolean"));
        }
    };
    let post = state
        .services
        .gallery_post_update(
            gallery_store(),
            &caller.account.id,
            &post_id,
            title,
            description,
            file_ids,
            is_sensitive,
        )
        .map_err(map_gallery_error)?;
    Ok(Json(pack_gallery_post(&state, &post)?).into_response())
}

/// `POST /api/gallery/posts/delete` (split from `api_dispatch_blocking`).
pub(crate) fn gallery_posts_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["postId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state
        .rate_limiter
        .check(&rate_key, "gallery/posts/delete")?;
    let post_id = required_str(&obj, "postId")?;
    state
        .services
        .gallery_post_delete(gallery_store(), &caller.account.id, &post_id)
        .map_err(map_gallery_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/gallery/posts/like` (split from `api_dispatch_blocking`).
pub(crate) fn gallery_posts_like(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["postId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "gallery/posts/like")?;
    let post_id = required_str(&obj, "postId")?;
    state
        .services
        .gallery_post_like(gallery_store(), &caller.account.id, &post_id)
        .map_err(map_gallery_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/gallery/posts/unlike` (split from `api_dispatch_blocking`).
pub(crate) fn gallery_posts_unlike(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["postId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state
        .rate_limiter
        .check(&rate_key, "gallery/posts/unlike")?;
    let post_id = required_str(&obj, "postId")?;
    state
        .services
        .gallery_post_unlike(gallery_store(), &caller.account.id, &post_id)
        .map_err(map_gallery_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/gallery/posts` (split from `api_dispatch_blocking`).
pub(crate) fn gallery_posts(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
    let cursor = parse_id_cursor(&obj)?;
    let posts = state
        .services
        .gallery_posts_list(gallery_store(), &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(posts.len());
    for post in &posts {
        out.push(pack_gallery_post(&state, post)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/gallery/featured` (split from `api_dispatch_blocking`).
pub(crate) fn gallery_featured(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
    let cursor = parse_id_cursor(&obj)?;
    let posts = state
        .services
        .gallery_featured(gallery_store(), &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(posts.len());
    for post in &posts {
        out.push(pack_gallery_post(&state, post)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/gallery/popular` (split from `api_dispatch_blocking`).
pub(crate) fn gallery_popular(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
    let cursor = parse_id_cursor(&obj)?;
    let posts = state
        .services
        .gallery_popular(gallery_store(), &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(posts.len());
    for post in &posts {
        out.push(pack_gallery_post(&state, post)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}
