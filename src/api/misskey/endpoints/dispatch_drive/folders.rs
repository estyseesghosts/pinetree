//! Drive dispatcher arms (folders).
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

/// `POST /api/drive/folders/create` (split from `api_dispatch_blocking`).
pub(crate) fn drive_folders_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["name", "parentId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:drive")?;
    state
        .rate_limiter
        .check(&rate_key, "drive/folders/create")?;
    let name = opt_string(&obj, "name")?;
    let parent_id = opt_string(&obj, "parentId")?;
    let folder = state
        .services
        .create_drive_folder(
            state.services.folders.as_ref(),
            &caller.account.id,
            name,
            parent_id,
        )
        .map_err(|err| map_drive_folder_error(err, "drive/folders/create"))?;
    Ok(Json(pack_drive_folder(&folder)).into_response())
}

/// `POST /api/drive/folders/show` (split from `api_dispatch_blocking`).
pub(crate) fn drive_folders_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["folderId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    state.rate_limiter.check(&rate_key, "drive/folders/show")?;
    let folder_id = required_str(&obj, "folderId")?;
    let folder = state
        .services
        .show_drive_folder(
            state.services.folders.as_ref(),
            &caller.account.id,
            &folder_id,
        )
        .map_err(|err| map_drive_folder_error(err, "drive/folders/show"))?;
    Ok(Json(pack_drive_folder(&folder)).into_response())
}

/// `POST /api/drive/folders/find` (split from `api_dispatch_blocking`).
pub(crate) fn drive_folders_find(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["name", "parentId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    state.rate_limiter.check(&rate_key, "drive/folders/find")?;
    let name = required_str(&obj, "name")?;
    let parent_id = opt_string(&obj, "parentId")?;
    let folders = state
        .services
        .find_drive_folders(
            state.services.folders.as_ref(),
            &caller.account.id,
            &name,
            parent_id.as_deref(),
        )
        .map_err(ApiError::from_port_error)?;
    let packed: Vec<Value> = folders.iter().map(pack_drive_folder).collect();
    Ok(Json(Value::Array(packed)).into_response())
}

/// `POST /api/drive/folders/update` (split from `api_dispatch_blocking`).
pub(crate) fn drive_folders_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["folderId", "name", "parentId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:drive")?;
    state
        .rate_limiter
        .check(&rate_key, "drive/folders/update")?;
    let folder_id = required_str(&obj, "folderId")?;
    let name = opt_string(&obj, "name")?;
    let parent_id = opt_nullable_string(&obj, "parentId")?;
    let folder = state
        .services
        .update_drive_folder(
            state.services.folders.as_ref(),
            &caller.account.id,
            &folder_id,
            name,
            parent_id,
        )
        .map_err(|err| map_drive_folder_error(err, "drive/folders/update"))?;
    Ok(Json(pack_drive_folder(&folder)).into_response())
}

/// `POST /api/drive/folders/delete` (split from `api_dispatch_blocking`).
pub(crate) fn drive_folders_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["folderId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:drive")?;
    state
        .rate_limiter
        .check(&rate_key, "drive/folders/delete")?;
    let folder_id = required_str(&obj, "folderId")?;
    state
        .services
        .delete_drive_folder(
            state.services.folders.as_ref(),
            &caller.account.id,
            &folder_id,
        )
        .map_err(|err| map_drive_folder_error(err, "drive/folders/delete"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/drive/files/find` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files_find(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["name", "folderId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:drive")?;
    state.rate_limiter.check(&rate_key, "drive/files/find")?;
    let name = required_str(&obj, "name")?;
    let folder_id = opt_string(&obj, "folderId")?;
    let files = state
        .services
        .find_drive_files(
            state.services.folders.as_ref(),
            &caller.account.id,
            &name,
            folder_id.as_deref(),
        )
        .map_err(ApiError::from_port_error)?;
    let packed: Vec<Value> = files
        .iter()
        .map(|meta| pack_drive_file(meta, &state.services))
        .collect();
    Ok(Json(Value::Array(packed)).into_response())
}

/// `POST /api/drive/files/move-bulk` (split from `api_dispatch_blocking`).
pub(crate) fn drive_files_move_bulk(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileIds", "folderId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:drive")?;
    state
        .rate_limiter
        .check(&rate_key, "drive/files/move-bulk")?;
    let file_ids = str_list(&obj, "fileIds")?;
    if file_ids.is_empty() || file_ids.len() > 100 {
        return Err(ApiError::invalid_param(
            "fileIds",
            "must be an array of 1..=100 file ids",
        ));
    }
    let folder_id = opt_string(&obj, "folderId")?;
    state
        .services
        .move_drive_files(
            state.services.folders.as_ref(),
            &caller.account.id,
            &file_ids,
            folder_id.as_deref(),
        )
        .map_err(|err| map_drive_folder_error(err, "drive/files/move-bulk"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
