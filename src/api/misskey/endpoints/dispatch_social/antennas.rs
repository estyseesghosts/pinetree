//! Following/clips/pages/antennas/invites dispatcher arms (antennas).
//!
//! Split from `endpoints/dispatch_social.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{client_ip, require_caller};
use super::super::parse::{
    get_limit_with_default, map_clip_error, map_page_error, opt_antenna_src, opt_bool,
    opt_bool_opt, opt_bounded_string, opt_i64, opt_nullable_string, opt_str_array,
    opt_str_array_array, opt_string, pack_invite_codes, parse_cursor, parse_follow_notify,
    parse_page_body, required_antenna_src, required_bool, required_str, required_str_array,
    required_str_array_array, required_string, update_string_field,
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

/// `POST /api/antennas/list` (split from `api_dispatch_blocking`).
pub(crate) fn antennas_list(
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
    let rows = state
        .services
        .antennas
        .antennas_by_owner(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    let packed: Vec<Value> = rows
        .iter()
        .map(|a| pack_antenna(a, &state.services))
        .collect();
    Ok(Json(Value::Array(packed)).into_response())
}

/// `POST /api/antennas/show` (split from `api_dispatch_blocking`).
pub(crate) fn antennas_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["antennaId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let antenna_id = required_str(&obj, "antennaId")?;
    match state
        .services
        .antennas
        .get_antenna(&caller.account.id, &antenna_id)
    {
        Ok(Some(a)) => Ok(Json(pack_antenna(&a, &state.services)).into_response()),
        Ok(None) => Err(ApiError::from_port_error(PortError::NotFound(
            "NO_SUCH_ANTENNA".to_string(),
        ))),
        Err(e) => Err(ApiError::from_port_error(e)),
    }
}

/// `POST /api/antennas/create` (split from `api_dispatch_blocking`).
pub(crate) fn antennas_create(
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
            "src",
            "userListId",
            "keywords",
            "excludeKeywords",
            "users",
            "caseSensitive",
            "localOnly",
            "excludeBots",
            "withReplies",
            "withFile",
            "excludeNotesInSensitiveChannel",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "antennas/create")?;
    let antenna = state
        .services
        .create_antenna(
            &caller.account.id,
            required_string(&obj, "name", 1, 100)?,
            required_antenna_src(&obj)?,
            opt_string(&obj, "userListId")?,
            required_str_array_array(&obj, "keywords")?,
            required_str_array_array(&obj, "excludeKeywords")?,
            required_str_array(&obj, "users")?,
            required_bool(&obj, "caseSensitive")?,
            opt_bool(&obj, "localOnly", false)?,
            opt_bool(&obj, "excludeBots", false)?,
            required_bool(&obj, "withReplies")?,
            required_bool(&obj, "withFile")?,
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_antenna(&antenna, &state.services)).into_response())
}

/// `POST /api/antennas/update` (split from `api_dispatch_blocking`).
pub(crate) fn antennas_update(
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
            "antennaId",
            "name",
            "src",
            "userListId",
            "keywords",
            "excludeKeywords",
            "users",
            "caseSensitive",
            "localOnly",
            "excludeBots",
            "withReplies",
            "withFile",
            "excludeNotesInSensitiveChannel",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "antennas/update")?;
    let antenna = state
        .services
        .update_antenna(
            &caller.account.id,
            &required_str(&obj, "antennaId")?,
            opt_bounded_string(&obj, "name", 1, 100)?,
            opt_antenna_src(&obj)?,
            opt_nullable_string(&obj, "userListId")?,
            opt_str_array_array(&obj, "keywords")?,
            opt_str_array_array(&obj, "excludeKeywords")?,
            opt_str_array(&obj, "users")?,
            opt_bool_opt(&obj, "caseSensitive")?,
            opt_bool_opt(&obj, "localOnly")?,
            opt_bool_opt(&obj, "excludeBots")?,
            opt_bool_opt(&obj, "withReplies")?,
            opt_bool_opt(&obj, "withFile")?,
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_antenna(&antenna, &state.services)).into_response())
}

/// `POST /api/antennas/delete` (split from `api_dispatch_blocking`).
pub(crate) fn antennas_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["antennaId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "antennas/delete")?;
    let antenna_id = required_str(&obj, "antennaId")?;
    state
        .services
        .delete_antenna(&caller.account.id, &antenna_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/antennas/notes` (split from `api_dispatch_blocking`).
pub(crate) fn antennas_notes(
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
            "antennaId",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let antenna_id = required_str(&obj, "antennaId")?;
    let cursor = TimelineCursor {
        since_id: opt_string(&obj, "sinceId")?,
        until_id: opt_string(&obj, "untilId")?,
        since_date: opt_i64(&obj, "sinceDate")?,
        until_date: opt_i64(&obj, "untilDate")?,
        limit: get_limit_with_default(&obj, 10)?,
    };
    let notes = state
        .services
        .antenna_notes_for(&caller.account.id, &antenna_id, &cursor)
        .map_err(ApiError::from_port_error)?;
    let viewer = Some(caller.account.id.as_str());
    let mut out = Vec::with_capacity(notes.len());
    for note in &notes {
        out.push(pack_note(note, viewer, &state.services, 0).map_err(ApiError::from_port_error)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/antennas/remove-note` (split from `api_dispatch_blocking`).
pub(crate) fn antennas_remove_note(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["antennaId", "noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let antenna_id = required_str(&obj, "antennaId")?;
    let note_id = required_str(&obj, "noteId")?;
    // Owner-scoped like every other antenna read; unknown rows are
    // `NO_SUCH_ANTENNA` (upstream parity).
    if state
        .services
        .antennas
        .get_antenna(&caller.account.id, &antenna_id)
        .map_err(ApiError::from_port_error)?
        .is_none()
    {
        return Err(ApiError::from_port_error(PortError::NotFound(
            "NO_SUCH_ANTENNA".to_string(),
        )));
    }
    state
        .services
        .antenna_notes
        .remove_antenna_note(&antenna_id, &note_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
