//! Drive dispatcher arms (files).
//!
//! Split from `endpoints/dispatch_drive.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::admin_status::walk_media_for_url;
use super::super::auth_http::{client_ip, require_caller};
use super::super::parse::{
    get_limit_with_default, map_drive_extra_error, map_drive_folder_error, opt_bool, opt_i64,
    opt_nullable_string, opt_string, pack_note_list, parse_cursor, required_str, str_list,
    update_string_field,
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

/// `POST /api/drive/files` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &[
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
            "folderId",
            "type",
            "sort",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    // Folder-aware listing (gaps WP2a final wiring): a non-null
    // `folderId` scopes the page to that folder (missing/foreign
    // folders match nothing → `[]`); absent/null lists everything.
    let folder_id = opt_string(&obj, "folderId")?;
    let type_prefix: Option<String> = match opt_string(&obj, "type")? {
        None => None,
        Some(raw) => {
            if raw.is_empty()
                || !raw
                    .chars()
                    .all(|c| c.is_ascii_alphabetic() || c == '/' || c == '-' || c == '*')
            {
                return Err(ApiError::invalid_param(
                    "type",
                    "must match /^[a-zA-Z/\\-*]+$/",
                ));
            }
            Some(raw.strip_suffix('*').unwrap_or(&raw).to_string())
        }
    };
    match opt_string(&obj, "sort")? {
        None => {}
        Some(sort)
            if [
                "+createdAt",
                "-createdAt",
                "+name",
                "-name",
                "+size",
                "-size",
            ]
            .contains(&sort.as_str()) => {}
        Some(_) => {
            return Err(ApiError::invalid_param(
                "sort",
                "must be one of +createdAt, -createdAt, +name, -name, +size, -size",
            ));
        }
    }
    let sort = opt_string(&obj, "sort")?;
    let cursor = parse_cursor(&obj)?;
    let files = match folder_id.as_deref() {
        None => state
            .services
            .list_drive_files(
                &caller.account.id,
                &cursor,
                type_prefix.as_deref(),
                sort.as_deref(),
            )
            .map_err(ApiError::from_port_error)?,
        Some(_) => state
            .services
            .list_drive_files_in(
                state.services.folders.as_ref(),
                &caller.account.id,
                folder_id.as_deref(),
                &cursor,
                type_prefix.as_deref(),
                sort.as_deref(),
            )
            .map_err(ApiError::from_port_error)?,
    };
    let packed: Vec<Value> = files
        .iter()
        .map(|meta| pack_drive_file(meta, &state.services))
        .collect();
    Ok(Json(Value::Array(packed)).into_response())
}

/// `POST /api/drive/files/show` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    let file_id = required_str(&obj, "fileId")?;
    let meta = state
        .services
        .get_media_for(&caller.account.id, &file_id)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_drive_file(&meta, &state.services)).into_response())
}

/// `POST /api/drive/files/upload-from-url` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files_upload_from_url(
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
            "folderId",
            "isSensitive",
            "comment",
            "marker",
            "force",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:drive")?;
    state
        .rate_limiter
        .check(&rate_key, "drive/files/upload-from-url")?;
    let url = required_str(&obj, "url")?;
    if let Some(folder) = obj.get("folderId") {
        if !folder.is_null() {
            return Err(ApiError::invalid_param(
                "folderId",
                "folders are unsupported; folderId must be null",
            ));
        }
    }
    let comment = match obj.get("comment") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.chars().count() > 512 {
                return Err(ApiError::invalid_param(
                    "comment",
                    "must be at most 512 chars",
                ));
            }
            Some(s.clone())
        }
        Some(_) => {
            return Err(ApiError::invalid_param("comment", "must be a string"));
        }
    };
    let is_sensitive = opt_bool(&obj, "isSensitive", false)?;
    let force = opt_bool(&obj, "force", false)?;
    let meta = state
        .services
        .upload_media_from_url(&caller.account.id, &url, comment, is_sensitive, force)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_drive_file(&meta, &state.services)).into_response())
}

/// `POST /api/drive/files/find-by-hash` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files_find_by_hash(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["md5"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    state
        .rate_limiter
        .check(&rate_key, "drive/files/find-by-hash")?;
    let md5 = required_str(&obj, "md5")?;
    let meta = state
        .services
        .media
        .find_by_owner_and_hash(&caller.account.id, &md5)
        .map_err(ApiError::from_port_error)?;
    match meta {
        Some(meta) => Ok(Json(pack_drive_file(&meta, &state.services)).into_response()),
        None => Err(ApiError::from_port_error(PortError::NotFound(
            "NO_SUCH_FILE".to_string(),
        ))),
    }
}

/// `POST /api/drive/files/update` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["fileId", "isSensitive", "comment", "name", "folderId"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:drive")?;
    // Same cost class as uploads; no dedicated bucket exists.
    state.rate_limiter.check(&rate_key, "drive/files/create")?;
    let file_id = required_str(&obj, "fileId")?;
    // Folder leg (gaps WP2a final wiring): a present `folderId`
    // (even null = move to root) re-files the note before the
    // metadata update below. `NO_SUCH_FILE` on miss,
    // `ACCESS_DENIED` for non-moderator foreign files,
    // `NO_SUCH_FOLDER` on a missing/foreign target.
    if obj.contains_key("folderId") {
        let folder_id = opt_nullable_string(&obj, "folderId")?.flatten();
        state
            .services
            .set_drive_file_folder(
                state.services.folders.as_ref(),
                &caller.account.id,
                &file_id,
                folder_id.as_deref(),
                caller.account.has_moderator_powers(),
            )
            .map_err(|err| map_drive_folder_error(err, "drive/files/update"))?;
    }
    let name = match obj.get("name") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            return Err(ApiError::invalid_param("name", "must be a string"));
        }
    };
    let is_sensitive = match obj.get("isSensitive") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => {
            return Err(ApiError::invalid_param("isSensitive", "must be a boolean"));
        }
    };
    let comment = update_string_field(&obj, "comment")?;
    if let Some(Some(text)) = comment.as_ref() {
        if text.chars().count() > 512 {
            return Err(ApiError::invalid_param(
                "comment",
                "must be at most 512 chars",
            ));
        }
    }
    let meta = state
        .services
        .update_media(&caller.account.id, &file_id, name, is_sensitive, comment)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_drive_file(&meta, &state.services)).into_response())
}

/// `POST /api/drive/files/delete` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:drive")?;
    // Same cost class as uploads; no dedicated bucket exists.
    state.rate_limiter.check(&rate_key, "drive/files/create")?;
    let file_id = required_str(&obj, "fileId")?;
    state
        .services
        .delete_media(&caller.account.id, &file_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/drive/cache/stats` (split from `api_dispatch_blocking`).
pub(crate) fn drive_cache_stats(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    let stats = state
        .services
        .media
        .cache_stats()
        .map_err(ApiError::from_port_error)?;
    let value = serde_json::to_value(&stats).map_err(|_| ApiError::internal())?;
    Ok(Json(value).into_response())
}

/// `POST /api/drive/files/check-existence` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files_check_existence(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["md5"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    state
        .rate_limiter
        .check(&rate_key, "drive/files/check-existence")?;
    let md5 = required_str(&obj, "md5")?;
    let exists = state
        .services
        .drive_file_exists(&caller.account.id, &md5)
        .map_err(ApiError::from_port_error)?;
    if !exists {
        return Err(ApiError::drive_no_such_file("drive/files/check-existence"));
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/drive/files/attached-notes` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files_attached_notes(
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
            "fileId",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    state
        .rate_limiter
        .check(&rate_key, "drive/files/attached-notes")?;
    let file_id = required_str(&obj, "fileId")?;
    let cursor = parse_cursor(&obj)?;
    let notes = state
        .services
        .attached_notes(
            &caller.account.id,
            &file_id,
            caller.account.has_moderator_powers(),
            &cursor,
        )
        .map_err(|err| map_drive_extra_error(err, "drive/files/attached-notes"))?;
    pack_note_list(&state.services, &notes, Some(&caller.account.id))
}

/// `POST /api/drive/stream` (split from `api_dispatch_blocking`).
pub(crate) fn drive_stream(
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
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
            "type",
            "sort",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    state.rate_limiter.check(&rate_key, "drive/stream")?;
    let type_prefix: Option<String> = match opt_string(&obj, "type")? {
        None => None,
        Some(raw) => {
            if raw.is_empty()
                || !raw
                    .chars()
                    .all(|c| c.is_ascii_alphabetic() || c == '/' || c == '-' || c == '*')
            {
                return Err(ApiError::invalid_param(
                    "type",
                    "must match /^[a-zA-Z/\\-*]+$/",
                ));
            }
            Some(raw.strip_suffix('*').unwrap_or(&raw).to_string())
        }
    };
    match opt_string(&obj, "sort")? {
        None => {}
        Some(sort)
            if [
                "+createdAt",
                "-createdAt",
                "+name",
                "-name",
                "+size",
                "-size",
            ]
            .contains(&sort.as_str()) => {}
        Some(_) => {
            return Err(ApiError::invalid_param(
                "sort",
                "must be one of +createdAt, -createdAt, +name, -name, +size, -size",
            ));
        }
    }
    let sort = opt_string(&obj, "sort")?;
    let cursor = parse_cursor(&obj)?;
    let files = state
        .services
        .stream_drive_files(
            &caller.account.id,
            &cursor,
            type_prefix.as_deref(),
            sort.as_deref(),
        )
        .map_err(ApiError::from_port_error)?;
    let packed: Vec<Value> = files
        .iter()
        .map(|meta| pack_drive_file(meta, &state.services))
        .collect();
    Ok(Json(Value::Array(packed)).into_response())
}
