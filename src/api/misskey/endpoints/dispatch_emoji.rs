//! Emoji dispatcher arms.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]
//! Misskey HTTP API surface (Slice D3).
//!
//! [`ApiState`] carries the application services, the in-process job
//! worker, and the rate limiter. [`build_router`] wires every Phase 1
//! route: `/healthz`, media/identicon file serving, the MiAuth web flow
//! (`GET`/`POST /miauth/{session}` plus the one-shot `check`
//! endpoints), web `signup`/`signin`, `drive/files/create` (multipart and
//! JSON), and the `POST /api/{endpoint}` JSON dispatcher.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

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

use crate::activitypub::federated_collections as ap_collections;
use crate::activitypub::{actor as ap_actor, objects as ap_objects, webfinger as ap_webfinger};
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
use crate::streaming::hub::{Hub, ONLINE_THRESHOLD_MS};
use crate::streaming::server::streaming_handler;

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
use crate::services::email::EmailError;
use crate::services::email::EmailMessage;
use crate::services::email::EmailSendPayload;
use crate::services::email::EmailSender;
use crate::services::email::SmtpConfig;
use crate::services::email::SmtpEmailSender;
use crate::services::email::password_reset_email;
use crate::services::email::verification_email;

use super::ApiState;
use super::auth_http::{client_ip, require_caller};
/// Shared API state: application services, the durable-job worker, the
/// Misskey rate limiter, the concurrent-upload slots, the live-streaming
/// broadcast hub, and the reverse proxies whose `X-Forwarded-For` is
/// honored ([`client_ip`]).
use super::parse::{
    get_limit_with_default, map_import_file_error, opt_i64, opt_string, parse_string_array,
    required_str, required_str_array, update_bool_field,
};

/// `POST /api/emojis` (split from `api_dispatch_blocking`).
pub(crate) fn emojis(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let rows = state
        .services
        .emojis
        .list_emojis(
            None,
            &TimelineCursor {
                limit: 10_000,
                ..Default::default()
            },
        )
        .map_err(ApiError::from_port_error)?;
    // Upstream packs `EmojiSimple` ordered by category, name.
    let mut rows = rows;
    rows.sort_by(|a, b| {
        a.category
            .cmp(&b.category)
            .then_with(|| a.shortcode.cmp(&b.shortcode))
    });
    let packed: Vec<Value> = rows
        .iter()
        .map(|e| pack_emoji_simple(e, &state.services))
        .collect();
    Ok(Json(serde_json::json!({ "emojis": packed })).into_response())
}

/// `POST /api/emoji` (split from `api_dispatch_blocking`).
pub(crate) fn emoji(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["name"])?;
    let name = required_str(&obj, "name")?;
    match state.services.emojis.get_emoji_by_name(&name) {
        Ok(Some(emoji)) => Ok(Json(pack_emoji_detailed(&emoji, &state.services)).into_response()),
        Ok(None) => Err(ApiError::from_port_error(PortError::NotFound(
            "NO_SUCH_EMOJI".to_string(),
        ))),
        Err(e) => Err(ApiError::from_port_error(e)),
    }
}

/// `POST /api/admin/emoji/add` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_add(
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
            "name",
            "fileId",
            "category",
            "aliases",
            "license",
            "isSensitive",
            "localOnly",
            "roleIdsThatCanBeUsedThisEmojiAsReaction",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/emoji/add")?;
    let name = required_str(&obj, "name")?;
    if name.is_empty()
        || name.chars().count() > 128
        || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(ApiError::invalid_param(
            "name",
            "must match ^[a-zA-Z0-9_]+$",
        ));
    }
    let file_id = required_str(&obj, "fileId")?;
    let emoji = state
        .services
        .create_emoji(
            &caller.account.id,
            &name,
            &file_id,
            opt_string(&obj, "category")?.filter(|s| !s.is_empty()),
            parse_string_array(&obj, "aliases", Some(32))?.unwrap_or_default(),
            opt_string(&obj, "license")?.filter(|s| !s.is_empty()),
            update_bool_field(&obj, "isSensitive")?.unwrap_or(false),
            update_bool_field(&obj, "localOnly")?.unwrap_or(false),
            parse_string_array(&obj, "roleIdsThatCanBeUsedThisEmojiAsReaction", None)?
                .unwrap_or_default(),
        )
        .map_err(|e| match e {
            PortError::NotFound(code) if code == "NO_SUCH_FILE" => ApiError::emoji_no_such_file(),
            other => ApiError::from_port_error(other),
        })?;
    Ok(Json(pack_emoji_detailed(&emoji, &state.services)).into_response())
}

/// `POST /api/admin/emoji/update` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_update(
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
            "id",
            "name",
            "fileId",
            "category",
            "aliases",
            "license",
            "isSensitive",
            "localOnly",
            "roleIdsThatCanBeUsedThisEmojiAsReaction",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/emoji/update")?;
    // Upstream `anyOf`: at least one of `id` / `name` addresses the
    // row; both may be given, in which case a different `name`
    // renames (`doNameUpdate` in `CustomEmojiService.update`).
    let (id, name) = match (obj.get("id"), obj.get("name")) {
        (None, None) => {
            return Err(ApiError::invalid_param(
                "id",
                "either id or name is required",
            ));
        }
        (id, name) => (
            id.and_then(Value::as_str).map(str::to_string),
            name.and_then(Value::as_str).map(str::to_string),
        ),
    };
    // Explicit null resets `category`/`license` (upstream parity);
    // absent leaves them unchanged.
    let category = match obj.get("category") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(s)) => Some(Some(s.clone())),
        Some(_) => {
            return Err(ApiError::invalid_param(
                "category",
                "must be a string or null",
            ));
        }
    };
    let license = match obj.get("license") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(s)) => Some(Some(s.clone())),
        Some(_) => {
            return Err(ApiError::invalid_param(
                "license",
                "must be a string or null",
            ));
        }
    };
    let emoji = state
        .services
        .update_emoji(
            &caller.account.id,
            id.as_deref(),
            name.as_deref(),
            opt_string(&obj, "fileId")?.as_deref(),
            category,
            parse_string_array(&obj, "aliases", Some(32))?,
            license,
            update_bool_field(&obj, "isSensitive")?,
            update_bool_field(&obj, "localOnly")?,
            parse_string_array(&obj, "roleIdsThatCanBeUsedThisEmojiAsReaction", None)?,
        )
        .map_err(|e| match e {
            PortError::NotFound(code) if code == "NO_SUCH_FILE" => ApiError::emoji_no_such_file(),
            other => ApiError::from_port_error(other),
        })?;
    Ok(Json(pack_emoji_detailed(&emoji, &state.services)).into_response())
}

/// `POST /api/admin/emoji/delete` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["id"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/emoji/delete")?;
    let id = required_str(&obj, "id")?;
    state
        .services
        .delete_emoji(&caller.account.id, &id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/emoji/list` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_list(
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
            "query",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:emoji")?;
    require_moderator(&caller)?;
    let cursor = TimelineCursor {
        since_id: opt_string(&obj, "sinceId")?,
        until_id: opt_string(&obj, "untilId")?,
        since_date: opt_i64(&obj, "sinceDate")?,
        until_date: opt_i64(&obj, "untilDate")?,
        limit: get_limit_with_default(&obj, 10)?,
    };
    let rows = state
        .services
        .emojis
        .list_emojis(opt_string(&obj, "query")?.as_deref(), &cursor)
        .map_err(ApiError::from_port_error)?;
    let packed: Vec<Value> = rows
        .iter()
        .map(|e| pack_emoji_detailed(e, &state.services))
        .collect();
    Ok(Json(Value::Array(packed)).into_response())
}

/// `POST /api/admin/emoji/add-aliases-bulk` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_add_aliases_bulk(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["ids", "aliases"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/emoji/add-aliases-bulk")?;
    let ids = required_str_array(&obj, "ids")?;
    let aliases = required_str_array(&obj, "aliases")?;
    if aliases.len() > 32 {
        return Err(ApiError::invalid_param(
            "aliases",
            "must have at most 32 entries",
        ));
    }
    state
        .services
        .add_emoji_aliases_bulk(&caller.account.id, &ids, &aliases)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/emoji/remove-aliases-bulk` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_remove_aliases_bulk(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["ids", "aliases"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/emoji/remove-aliases-bulk")?;
    let ids = required_str_array(&obj, "ids")?;
    let aliases = required_str_array(&obj, "aliases")?;
    if aliases.len() > 32 {
        return Err(ApiError::invalid_param(
            "aliases",
            "must have at most 32 entries",
        ));
    }
    state
        .services
        .remove_emoji_aliases_bulk(&caller.account.id, &ids, &aliases)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/emoji/set-aliases-bulk` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_set_aliases_bulk(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["ids", "aliases"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/emoji/set-aliases-bulk")?;
    let ids = required_str_array(&obj, "ids")?;
    let aliases = required_str_array(&obj, "aliases")?;
    if aliases.len() > 32 {
        return Err(ApiError::invalid_param(
            "aliases",
            "must have at most 32 entries",
        ));
    }
    state
        .services
        .set_emoji_aliases_bulk(&caller.account.id, &ids, &aliases)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/emoji/set-category-bulk` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_set_category_bulk(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["ids", "category"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/emoji/set-category-bulk")?;
    let ids = required_str_array(&obj, "ids")?;
    // Absent resets to null (upstream `ps.category ?? null` parity).
    let category = match obj.get("category") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            return Err(ApiError::invalid_param(
                "category",
                "must be a string or null",
            ));
        }
    };
    state
        .services
        .set_emoji_category_bulk(&caller.account.id, &ids, category)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/emoji/set-license-bulk` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_set_license_bulk(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["ids", "license"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/emoji/set-license-bulk")?;
    let ids = required_str_array(&obj, "ids")?;
    // Absent resets to null (upstream `ps.license ?? null` parity).
    let license = match obj.get("license") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            return Err(ApiError::invalid_param(
                "license",
                "must be a string or null",
            ));
        }
    };
    state
        .services
        .set_emoji_license_bulk(&caller.account.id, &ids, license)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/emoji/delete-bulk` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_delete_bulk(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["ids"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/emoji/delete-bulk")?;
    let ids = required_str_array(&obj, "ids")?;
    state
        .services
        .delete_emojis_bulk(&caller.account.id, &ids)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/emoji/copy` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_copy(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    // DIVERGENCE: upstream takes `{emojiId}` against the shared
    // local+remote table; pinetree remote rows have no global ids, so this
    // takes `{host, name}` (see `AppServices::copy_remote_emoji`).
    check_allowed_fields(&obj, &["host", "name"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/emoji/copy")?;
    let host = required_str(&obj, "host")?;
    let name = required_str(&obj, "name")?;
    let emoji = state
        .services
        .copy_remote_emoji(&caller.account.id, &host, &name)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_emoji_detailed(&emoji, &state.services)).into_response())
}

/// `POST /api/admin/emoji/list-remote` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_list_remote(
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
            "query",
            "host",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:emoji")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/emoji/list-remote")?;
    let cursor = TimelineCursor {
        since_id: opt_string(&obj, "sinceId")?,
        until_id: opt_string(&obj, "untilId")?,
        since_date: opt_i64(&obj, "sinceDate")?,
        until_date: opt_i64(&obj, "untilDate")?,
        limit: get_limit_with_default(&obj, 10)?,
    };
    // `host` null/absent = all remote rows; otherwise that host
    // (case-insensitive). `sinceId`/`untilId` accepted but ignored.
    let host = opt_string(&obj, "host")?;
    let rows = state
        .services
        .list_remote_emojis(
            opt_string(&obj, "query")?.as_deref(),
            host.as_deref(),
            &cursor,
        )
        .map_err(ApiError::from_port_error)?;
    let packed: Vec<Value> = rows
        .iter()
        .map(|e| pack_remote_emoji_detailed(e, &state.services))
        .collect();
    Ok(Json(Value::Array(packed)).into_response())
}

/// `POST /api/admin/emoji/import-zip` (split from `api_dispatch_blocking`).
pub(crate) fn admin_emoji_import_zip(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:emoji")?;
    require_moderator(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/emoji/import-zip")?;
    let file_id = required_str(&obj, "fileId")?;
    // Zips legitimately exceed the §13 64 KiB endpoint cap, so the
    // transfer guard (32 MiB) applies instead of `check_import_file`.
    if let Err(err) = state
        .services
        .check_custom_emoji_import_file(&caller.account.id, &file_id)
    {
        return Err(map_import_file_error(err, "admin/emoji/import-zip"));
    }
    state
        .services
        .enqueue_custom_emoji_import(&caller.account.id, &file_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/export-custom-emojis` (split from `api_dispatch_blocking`).
pub(crate) fn export_custom_emojis(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    // Misskey `export-custom-emojis.ts` parity: any authenticated
    // caller (self-service, like the `i/export-*` family — NOT
    // moderator-gated), 1/hr bucket (see `response.rs`).
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state
        .rate_limiter
        .check(&rate_key, "export-custom-emojis")?;
    state
        .services
        .enqueue_custom_emoji_export(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/v2/admin/emoji/list` (split from `api_dispatch_blocking`).
pub(crate) fn v2_admin_emoji_list(
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
            "query",
            "host",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:emoji")?;
    require_moderator(&caller)?;
    let query = opt_string(&obj, "query")?;
    let host = opt_string(&obj, "host")?;
    let cursor = TimelineCursor {
        since_id: opt_string(&obj, "sinceId")?,
        until_id: opt_string(&obj, "untilId")?,
        since_date: opt_i64(&obj, "sinceDate")?,
        until_date: opt_i64(&obj, "untilDate")?,
        limit: get_limit_with_default(&obj, 10)?,
    };
    let page = state
        .services
        .list_emojis_v2(query.as_deref(), host.as_deref(), &cursor)
        .map_err(ApiError::from_port_error)?;
    match page {
        EmojisV2Page::Local(_) => {
            let rows = state
                .services
                .emojis
                .list_emojis(query.as_deref(), &cursor)
                .map_err(ApiError::from_port_error)?;
            let packed: Vec<Value> = rows
                .iter()
                .map(|emoji| pack_emoji_detailed(emoji, &state.services))
                .collect();
            Ok(Json(Value::Array(packed)).into_response())
        }
        EmojisV2Page::Remote(_) => {
            let rows = state
                .services
                .emojis
                .list_remote_emojis(query.as_deref(), host.as_deref(), &cursor)
                .map_err(ApiError::from_port_error)?;
            let packed: Vec<Value> = rows
                .iter()
                .map(|emoji| pack_remote_emoji_detailed(emoji, &state.services))
                .collect();
            Ok(Json(Value::Array(packed)).into_response())
        }
    }
}
