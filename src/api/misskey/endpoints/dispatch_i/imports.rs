//! I/notifications/registry/webhook dispatcher arms (imports).
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

/// `POST /api/i/import-following` (split from `api_dispatch_blocking`).
pub(crate) fn i_import_following(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId", "withReplies"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "i/import-following")?;
    let file_id = required_str(&obj, "fileId")?;
    // Misskey `i/import-following.ts:56` declares `withReplies`
    // with no default; the import processor treats an omitted
    // value as falsy ("without replies" unless a CSV row
    // overrides), so the job-level default is false — matching
    // `following/create` above.
    let with_replies = opt_bool(&obj, "withReplies", false)?;
    if let Err(err) = state
        .services
        .check_import_file(&caller.account.id, &file_id)
    {
        return Err(map_import_file_error(err, "i/import-following"));
    }
    state
        .services
        .enqueue_import(
            &caller.account.id,
            ImportEntity::Following,
            &file_id,
            Some(with_replies),
        )
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/import-blocking` (split from `api_dispatch_blocking`).
pub(crate) fn i_import_blocking(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "i/import-blocking")?;
    let file_id = required_str(&obj, "fileId")?;
    if let Err(err) = state
        .services
        .check_import_file(&caller.account.id, &file_id)
    {
        return Err(map_import_file_error(err, "i/import-blocking"));
    }
    state
        .services
        .enqueue_import(&caller.account.id, ImportEntity::Blocking, &file_id, None)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/import-muting` (split from `api_dispatch_blocking`).
pub(crate) fn i_import_muting(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "i/import-muting")?;
    let file_id = required_str(&obj, "fileId")?;
    if let Err(err) = state
        .services
        .check_import_file(&caller.account.id, &file_id)
    {
        return Err(map_import_file_error(err, "i/import-muting"));
    }
    state
        .services
        .enqueue_import(&caller.account.id, ImportEntity::Muting, &file_id, None)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/import-user-lists` (split from `api_dispatch_blocking`).
pub(crate) fn i_import_user_lists(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "i/import-user-lists")?;
    let file_id = required_str(&obj, "fileId")?;
    if let Err(err) = state
        .services
        .check_import_file(&caller.account.id, &file_id)
    {
        return Err(map_import_file_error(err, "i/import-user-lists"));
    }
    state
        .services
        .enqueue_import(&caller.account.id, ImportEntity::UserLists, &file_id, None)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/import-clips` (split from `api_dispatch_blocking`).
pub(crate) fn i_import_clips(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "i/import-clips")?;
    let file_id = required_str(&obj, "fileId")?;
    if let Err(err) = state
        .services
        .check_import_file(&caller.account.id, &file_id)
    {
        return Err(map_import_file_error(err, "i/import-clips"));
    }
    state
        .services
        .enqueue_import(&caller.account.id, ImportEntity::Clips, &file_id, None)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/import-antennas` (split from `api_dispatch_blocking`).
pub(crate) fn i_import_antennas(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "i/import-antennas")?;
    let file_id = required_str(&obj, "fileId")?;
    // Upstream (`import-antennas.ts:77-84`) has no size cap and
    // reads + parses the file in the endpoint, so this arm does
    // not use `check_import_file`: missing/foreign → `noSuchFile`,
    // empty → `emptyFile` (verbatim UUIDs).
    let meta = match state
        .services
        .media
        .get(&file_id)
        .map_err(ApiError::from_port_error)?
    {
        Some(meta) if meta.owner_id.as_deref() == Some(caller.account.id.as_str()) => meta,
        _ => return Err(ApiError::import_no_such_file("i/import-antennas")),
    };
    if meta.size_bytes == 0 {
        return Err(ApiError::import_empty_file("i/import-antennas"));
    }
    // Upstream (`import-antennas.ts:77-84`) has no size cap and
    // reads + parses the file in the endpoint, so this arm does
    // not use `check_import_file`: missing/foreign → `noSuchFile`,
    // empty → `emptyFile` (verbatim UUIDs). DIVERGENCE: upstream
    // `JSON.parse`s here (a malformed file throws → HTTP 500);
    // pinetree answers malformed JSON with `INVALID_PARAM`.
    let bytes = match state
        .services
        .blobs
        .get(&meta.blob_key)
        .map_err(ApiError::from_port_error)?
    {
        Some(bytes) => bytes,
        None => return Err(ApiError::import_no_such_file("i/import-antennas")),
    };
    let parsed: Value = serde_json::from_slice(&bytes).map_err(|_| {
        ApiError::invalid_param("fileId", "file must contain a JSON array of antennas")
    })?;
    let batch_len = match parsed.as_array() {
        Some(entries) => entries.len(),
        None => {
            return Err(ApiError::invalid_param(
                "fileId",
                "file must contain a JSON array of antennas",
            ));
        }
    };
    // Upstream pre-check (`import-antennas.ts:81-84`): current count
    // + batch length at the limit refuses with `TOO_MANY_ANTENNAS`
    // (import-endpoint UUID `600917d4-…`).
    let current = state
        .services
        .antennas
        .antennas_by_owner(&caller.account.id)
        .map_err(ApiError::from_port_error)?
        .len();
    if current + batch_len >= crate::domain::antenna::DEFAULT_ANTENNA_LIMIT {
        return Err(ApiError::import_too_many_antennas());
    }
    state
        .services
        .enqueue_import(&caller.account.id, ImportEntity::Antennas, &file_id, None)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
