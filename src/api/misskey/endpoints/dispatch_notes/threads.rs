//! Notes dispatcher arms (threads).
//!
//! Split from `endpoints/dispatch_notes.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{client_ip, require_caller};
use super::super::parse::{
    draft_file_ids_for_key, draft_visible_user_ids, get_limit, get_limit_with_default, opt_bool,
    opt_draft_cw, opt_draft_text, opt_i64, opt_string, opt_usize, pack_note_list, parse_cursor,
    parse_draft_create, parse_draft_poll_param, parse_draft_update, parse_list_cursor,
    parse_poll_param, parse_reaction_acceptance, parse_visibility, required_str, str_list,
};
use super::notes_show;
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

/// `POST /api/notes/replies` (split from `api_dispatch_blocking`).
pub(crate) fn notes_replies(
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
            "noteId",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let note_id = required_str(&obj, "noteId")?;
    let cursor = parse_cursor(&obj)?;
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    let notes = state
        .services
        .note_replies(&note_id, viewer_id, &cursor)
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, viewer_id)
}

/// `POST /api/notes/renotes` (split from `api_dispatch_blocking`).
pub(crate) fn notes_renotes(
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
            "noteId",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let note_id = required_str(&obj, "noteId")?;
    let cursor = parse_cursor(&obj)?;
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    let notes = state
        .services
        .note_renotes(&note_id, viewer_id, &cursor)
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, viewer_id)
}

/// `POST /api/notes/children` (split from `api_dispatch_blocking`).
pub(crate) fn notes_children(
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
            "noteId",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let note_id = required_str(&obj, "noteId")?;
    let cursor = parse_cursor(&obj)?;
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    let notes = state
        .services
        .note_children(&note_id, viewer_id, &cursor)
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, viewer_id)
}

/// `POST /api/notes/conversation` (split from `api_dispatch_blocking`).
pub(crate) fn notes_conversation(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId", "limit", "offset"])?;
    let note_id = required_str(&obj, "noteId")?;
    let limit = get_limit(&obj)?;
    let offset = opt_usize(&obj, "offset")?.unwrap_or(0);
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    // Upstream `notes/conversation.ts:62`: anonymous + `none` → `[]`.
    if viewer_id.is_none()
        && state
            .services
            .instance_settings
            .get()
            .map(|s| s.ugc_visibility_for_visitor)
            .unwrap_or_default()
            == UgcVisibilityForVisitor::None
    {
        return Ok(Json(Value::Array(Vec::new())).into_response());
    }
    let notes = state
        .services
        .note_conversation(&note_id, viewer_id, limit, offset)
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, viewer_id)
}

/// `POST /api/notes/unrenote` (split from `api_dispatch_blocking`).
pub(crate) fn notes_unrenote(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:notes")?;
    // Same cost class as note deletion; no dedicated bucket exists.
    state.rate_limiter.check(&rate_key, "notes/delete")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .unrenote(&caller.account.id, &note_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/notes/versions` (split from `api_dispatch_blocking`).
pub(crate) fn notes_versions(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    state.rate_limiter.check(&rate_key, "notes/versions")?;
    let note_id = required_str(&obj, "noteId")?;
    let viewer_id = caller.as_ref().map(|c| c.account.id.as_str());
    let note = state
        .services
        .notes
        .get_note(&note_id)
        .map_err(ApiError::from_port_error)?
        .filter(|n| !n.is_deleted)
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_NOTE".to_string()))
        })?;
    // Upstream order: visibility query first (miss → NO_SUCH_NOTE),
    // then the signin gate.
    state
        .services
        .notes_show(&note_id, viewer_id)
        .map_err(|_| ApiError::from_port_error(PortError::NotFound("NO_SUCH_NOTE".to_string())))?;
    if viewer_id.is_none() {
        let author = state
            .services
            .accounts
            .find_by_id(&note.author_id)
            .map_err(ApiError::from_port_error)?;
        if author.is_some_and(|a| a.require_signin_to_view_contents) {
            return Err(ApiError::versions_signin_required());
        }
    }
    let mut edits = state
        .services
        .notes
        .note_edits(&note_id)
        .map_err(ApiError::from_port_error)?;
    edits.sort_by(|a, b| b.old_date_ms.cmp(&a.old_date_ms));
    let out: Vec<Value> = edits
        .into_iter()
        .map(|edit| {
            serde_json::json!({
                "oldDate": crate::activitypub::format_millis(edit.old_date_ms),
                "updatedAt": crate::activitypub::format_millis(edit.updated_at_ms),
                "text": edit.old_text.or(edit.new_text),
            })
        })
        .collect();
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/notes/translate` (split from `api_dispatch_blocking`).
pub(crate) fn notes_translate(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId", "targetLang"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let note_id = required_str(&obj, "noteId")?;
    let target_lang = required_str(&obj, "targetLang")?;
    let note = state
        .services
        .notes
        .get_note(&note_id)
        .map_err(ApiError::from_port_error)?
        .filter(|n| !n.is_deleted)
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_NOTE".to_string()))
        })?;
    // Visibility first (miss/invisible →
    // `CANNOT_TRANSLATE_INVISIBLE_NOTE`, upstream parity).
    if state
        .services
        .notes_show(&note_id, Some(&caller.account.id))
        .is_err()
    {
        return Err(ApiError::translate_invisible_note());
    }
    let mut text = note.text.clone().unwrap_or_default();
    if let Some(cw) = note.cw.as_deref() {
        text = format!("{cw}\n-----\n{text}");
    }
    if text.trim().is_empty() {
        return Ok(Json(Value::Null).into_response());
    }
    // Upstream truncates `targetLang` at `-` (parity, before use).
    let _target = target_lang.split('-').next().unwrap_or(&target_lang);
    // No backend configured → loud failure (never proxy arbitrary
    // fetch — translate path only, dormant until wired).
    Err(ApiError::translate_not_configured())
}

/// `POST /api/notes/featured` (split from `api_dispatch_blocking`).
pub(crate) fn notes_featured(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "untilId", "channelId"])?;
    // No channels exist: any channel ranking is empty (upstream
    // serves an empty ranking set for unknown channels).
    if let Some(channel) = obj.get("channelId") {
        if !channel.is_null() {
            return Ok(Json(Value::Array(Vec::new())).into_response());
        }
    }
    let limit = get_limit(&obj)?;
    let until_id = opt_string(&obj, "untilId")?;
    let now = state.services.clock.now_ms();
    let ranked = match state.featured_cache.get("global", now) {
        Some(ids) => ids,
        None => {
            let ids = state
                .services
                .rank_featured_global()
                .map_err(ApiError::from_port_error)?;
            state.featured_cache.put("global", ids.clone(), now);
            ids
        }
    };
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    let notes = state
        .services
        .featured_notes_visible(&ranked, viewer_id, until_id.as_deref(), limit)
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, viewer_id)
}

/// `POST /api/users/featured-notes` (split from `api_dispatch_blocking`).
pub(crate) fn users_featured_notes(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "untilId", "userId"])?;
    let user_id = required_str(&obj, "userId")?;
    let limit = get_limit(&obj)?;
    let until_id = opt_string(&obj, "untilId")?;
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    // Early return when the target blocks the viewer (Misskey
    // `featured-notes.ts` parity).
    if let Some(viewer) = viewer_id {
        if state
            .services
            .blocks
            .is_blocking(&user_id, viewer)
            .map_err(ApiError::from_port_error)?
        {
            return Ok(Json(Value::Array(Vec::new())).into_response());
        }
    }
    let now = state.services.clock.now_ms();
    let key = format!("user:{user_id}");
    let ranked = match state.featured_cache.get(&key, now) {
        Some(ids) => ids,
        None => {
            let ids = state
                .services
                .rank_featured_for_user(&user_id)
                .map_err(ApiError::from_port_error)?;
            state.featured_cache.put(&key, ids.clone(), now);
            ids
        }
    };
    let notes = state
        .services
        .featured_notes_visible(&ranked, viewer_id, until_id.as_deref(), limit)
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, viewer_id)
}

/// `POST /api/notes/thread-muting/create` (split from `api_dispatch_blocking`).
pub(crate) fn notes_thread_muting_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state
        .rate_limiter
        .check(&rate_key, "notes/thread-muting/create")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .mute_note_thread(&caller.account.id, &note_id)
        .map_err(|err| match &err {
            PortError::NotFound(msg) if msg == "NO_SUCH_NOTE" => {
                ApiError::thread_mute_no_such_note("notes/thread-muting/create")
            }
            _ => ApiError::from_port_error(err),
        })?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/notes/thread-muting/delete` (split from `api_dispatch_blocking`).
pub(crate) fn notes_thread_muting_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state
        .rate_limiter
        .check(&rate_key, "notes/thread-muting/delete")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .unmute_note_thread(&caller.account.id, &note_id)
        .map_err(|err| match &err {
            PortError::NotFound(msg) if msg == "NO_SUCH_NOTE" => {
                ApiError::thread_mute_no_such_note("notes/thread-muting/delete")
            }
            _ => ApiError::from_port_error(err),
        })?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
