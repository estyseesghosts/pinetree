//! Notes dispatcher arms (reactions).
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

/// `POST /api/notes/reactions` (split from `api_dispatch_blocking`).
pub(crate) fn notes_reactions(
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
            "type",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let note_id = required_str(&obj, "noteId")?;
    let reaction_type = opt_string(&obj, "type")?;
    let cursor = parse_cursor(&obj)?;
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    let rows = state
        .services
        .note_reactions(&note_id, viewer_id, reaction_type.as_deref(), &cursor)
        .map_err(ApiError::from_port_error)?;
    let out: Vec<Value> = rows
        .iter()
        .map(|(reaction, reactor)| {
            crate::api::misskey::packers::pack_note_reaction(reaction, reactor, &state.services)
        })
        .collect();
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/notes/like` (split from `api_dispatch_blocking`).
pub(crate) fn notes_like(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId", "override"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:reactions")?;
    state.rate_limiter.check(&rate_key, "notes/like")?;
    let note_id = required_str(&obj, "noteId")?;
    let override_emoji = opt_string(&obj, "override")?;
    if matches!(override_emoji.as_deref(), Some("")) {
        return Err(ApiError::invalid_param(
            "override",
            "this field is required",
        ));
    }
    state
        .services
        .like_note(&caller.account.id, &note_id, override_emoji.as_deref())
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/notes/reactions/create` (split from `api_dispatch_blocking`).
pub(crate) fn notes_reactions_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId", "reaction"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:reactions")?;
    let note_id = required_str(&obj, "noteId")?;
    // §6 DIVERGENCE (decided, `docs/misskey-parity-plan.md` §6):
    // upstream requires `reaction`; pinetree leaves it optional and
    // resolves an absent/null param server-side to the reactor's
    // `defaultReaction ?? FALLBACK` (bare ❤).
    let reaction = opt_string(&obj, "reaction")?;
    if matches!(reaction.as_deref(), Some("")) {
        return Err(ApiError::invalid_param(
            "reaction",
            "this field is required",
        ));
    }
    // Misskey rejects a duplicate reaction on the same note with
    // ALREADY_REACTED, so this endpoint must not upsert.
    state
        .services
        .react_with_default(&caller.account.id, &note_id, reaction.as_deref(), false)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/notes/reactions/delete` (split from `api_dispatch_blocking`).
pub(crate) fn notes_reactions_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:reactions")?;
    state
        .rate_limiter
        .check(&rate_key, "notes/reactions/delete")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .unreact(&caller.account.id, &note_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/notes/favorites/create` (split from `api_dispatch_blocking`).
pub(crate) fn notes_favorites_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:favorites")?;
    state
        .rate_limiter
        .check(&rate_key, "notes/favorites/create")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .favorite_note(&caller.account.id, &note_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/notes/favorites/delete` (split from `api_dispatch_blocking`).
pub(crate) fn notes_favorites_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:favorites")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .unfavorite_note(&caller.account.id, &note_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/favorites` (split from `api_dispatch_blocking`).
pub(crate) fn i_favorites(
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
    caller.require_permission("read:favorites")?;
    let cursor = parse_list_cursor(&obj)?;
    let favs = state
        .services
        .favorites_for_user(&caller.account.id, &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(favs.len());
    for fav in &favs {
        match state.services.notes.get_note(&fav.note_id) {
            Ok(Some(note)) if !note.is_deleted => {
                out.push(
                    pack_note(&note, Some(&caller.account.id), &state.services, 0)
                        .map_err(ApiError::from_port_error)?,
                );
            }
            // Gone notes are skipped (upstream `leftJoin`s; invisible
            // on the wire).
            _ => continue,
        }
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/users/reactions` (split from `api_dispatch_blocking`).
pub(crate) fn users_reactions(
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
            "userId",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let user_id = required_str(&obj, "userId")?;
    let cursor = parse_cursor(&obj)?;
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    let rows = state
        .services
        .user_reactions(&user_id, viewer_id, &cursor)
        .map_err(ApiError::from_port_error)?;
    let target = state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(
            pack_note_reaction_with_note(
                &row.reaction,
                &target,
                &row.note,
                viewer_id,
                &state.services,
            )
            .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/notes/state` (split from `api_dispatch_blocking`).
pub(crate) fn notes_state(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let note_id = required_str(&obj, "noteId")?;
    // Unknown or invisible notes are indistinguishable (D8): reuse
    // the favorite-create visibility gate for the existence check.
    let note = match state.services.notes.get_note(&note_id) {
        Ok(Some(n)) if !n.is_deleted => n,
        _ => {
            return Err(ApiError::from_port_error(PortError::NotFound(
                "NO_SUCH_NOTE".to_string(),
            )));
        }
    };
    {
        use crate::domain::policy::{ViewerContext, is_visible};
        let viewer_id = &caller.account.id;
        let follows_author = viewer_id == &note.author_id
            || state
                .services
                .follows
                .is_following(viewer_id, &note.author_id)
                .map_err(ApiError::from_port_error)?;
        let viewer = ViewerContext::local_user(viewer_id, follows_author);
        if !is_visible(&note, &viewer) {
            return Err(ApiError::from_port_error(PortError::NotFound(
                "NO_SUCH_NOTE".to_string(),
            )));
        }
    }
    let is_favorited = state
        .services
        .favorites
        .is_favorited(&caller.account.id, &note_id)
        .map_err(ApiError::from_port_error)?;
    // Thread-muting stays out (guide §8): always false.
    Ok(Json(serde_json::json!({
        "isFavorited": is_favorited,
        "isMutedThread": false,
    }))
    .into_response())
}

/// `POST /api/notes/polls/vote` (split from `api_dispatch_blocking`).
pub(crate) fn notes_polls_vote(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId", "choice"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:votes")?;
    state.rate_limiter.check(&rate_key, "notes/polls/vote")?;
    let note_id = required_str(&obj, "noteId")?;
    let choice = match obj.get("choice") {
        Some(Value::Number(n)) => n
            .as_u64()
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(|| ApiError::invalid_param("choice", "must be an integer"))?,
        _ => return Err(ApiError::invalid_param("choice", "this field is required")),
    };
    state
        .services
        .vote_on_poll(&caller.account.id, &note_id, choice)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
