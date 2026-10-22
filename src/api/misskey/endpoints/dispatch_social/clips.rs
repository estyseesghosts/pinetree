//! Following/clips/pages/antennas/invites dispatcher arms (clips).
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

/// `POST /api/clips/create` (split from `api_dispatch_blocking`).
pub(crate) fn clips_create(
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
            "name",
            "isPublic",
            "description",
            "visibility",
            "federate",
            "approvedActorUris",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let name = required_string(&obj, "name", 1, 100)?;
    let is_public = opt_bool(&obj, "isPublic", false)?;
    let description = match obj.get("description") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.chars().count() > 2048 {
                return Err(ApiError::invalid_param(
                    "description",
                    "must be <=2048 chars",
                ));
            }
            Some(s.clone())
        }
        Some(_) => {
            return Err(ApiError::invalid_param("description", "must be a string"));
        }
    };
    let visibility = opt_string(&obj, "visibility")?;
    let federate = opt_bool(&obj, "federate", false)?;
    let approved = opt_str_array(&obj, "approvedActorUris")?.unwrap_or_default();
    let clip = state
        .services
        .create_clip(
            &caller.account.id,
            name,
            is_public,
            description,
            visibility,
            federate,
            approved,
        )
        .map_err(|e| map_clip_error(e, "clips/create"))?;
    let packed = pack_clip(&clip, Some(&caller.account.id), &state.services)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/clips/update` (split from `api_dispatch_blocking`).
pub(crate) fn clips_update(
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
            "clipId",
            "name",
            "isPublic",
            "description",
            "visibility",
            "federate",
            "approvedActorUris",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let clip_id = required_str(&obj, "clipId")?;
    let name = opt_bounded_string(&obj, "name", 1, 100)?;
    let is_public = match obj.get("isPublic") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => {
            return Err(ApiError::invalid_param("isPublic", "must be a boolean"));
        }
    };
    let description = update_string_field(&obj, "description")?;
    if let Some(Some(d)) = &description {
        if d.chars().count() > 2048 {
            return Err(ApiError::invalid_param(
                "description",
                "must be <=2048 chars",
            ));
        }
    }
    let visibility = opt_string(&obj, "visibility")?;
    let federate = match obj.get("federate") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => {
            return Err(ApiError::invalid_param("federate", "must be a boolean"));
        }
    };
    let approved = opt_str_array(&obj, "approvedActorUris")?;
    let clip = state
        .services
        .update_clip(
            &caller.account.id,
            &clip_id,
            name,
            is_public,
            description,
            visibility,
            federate,
            approved,
        )
        .map_err(|e| map_clip_error(e, "clips/update"))?;
    let packed = pack_clip(&clip, Some(&caller.account.id), &state.services)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/clips/delete` (split from `api_dispatch_blocking`).
pub(crate) fn clips_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["clipId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let clip_id = required_str(&obj, "clipId")?;
    state
        .services
        .delete_clip(&caller.account.id, &clip_id)
        .map_err(|e| map_clip_error(e, "clips/delete"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/clips/list` (split from `api_dispatch_blocking`).
pub(crate) fn clips_list(
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
    caller.require_permission("read:account")?;
    let cursor = parse_cursor(&obj)?;
    let clips = state
        .services
        .list_clips(
            &caller.account.id,
            cursor.limit,
            cursor.since_id.as_deref(),
            cursor.until_id.as_deref(),
            cursor.since_date,
            cursor.until_date,
        )
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(clips.len());
    for clip in &clips {
        out.push(
            pack_clip(clip, Some(&caller.account.id), &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/clips/show` (split from `api_dispatch_blocking`).
pub(crate) fn clips_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["clipId"])?;
    if let Some(c) = &caller {
        c.require_permission("read:account")?;
    }
    let clip_id = required_str(&obj, "clipId")?;
    let viewer_id = caller.as_ref().map(|c| c.account.id.as_str());
    let clip = state
        .services
        .visible_clip(&clip_id, viewer_id)
        .map_err(|e| map_clip_error(e, "clips/show"))?;
    let packed = pack_clip(&clip, viewer_id, &state.services).map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/clips/notes` (split from `api_dispatch_blocking`).
pub(crate) fn clips_notes(
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
            "clipId",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
            "search",
        ],
    )?;
    if let Some(c) = &caller {
        c.require_permission("read:account")?;
    }
    let clip_id = required_str(&obj, "clipId")?;
    let cursor = parse_cursor(&obj)?;
    let search = opt_string(&obj, "search")?;
    if let Some(s) = &search {
        let n = s.chars().count();
        if n == 0 || n > 100 {
            return Err(ApiError::invalid_param("search", "must be 1..=100 chars"));
        }
    }
    let viewer_id = caller.as_ref().map(|c| c.account.id.as_str());
    let ids = state
        .services
        .clip_member_note_ids(
            &clip_id,
            viewer_id,
            cursor.limit,
            cursor.since_id.as_deref(),
            cursor.until_id.as_deref(),
            cursor.since_date,
            cursor.until_date,
        )
        .map_err(|e| map_clip_error(e, "clips/notes"))?;
    let words: Vec<String> = search
        .as_deref()
        .map(|s| s.split_whitespace().map(|w| w.to_lowercase()).collect())
        .unwrap_or_default();
    let mut out = Vec::new();
    for id in &ids {
        let Some(note) = state
            .services
            .notes
            .get_note(id)
            .map_err(ApiError::from_port_error)?
        else {
            continue;
        };
        if note.is_deleted {
            continue;
        }
        // Visibility gate (private clips already gated the reader;
        // per-note visibility still applies).
        {
            use crate::domain::policy::{ViewerContext, is_visible};
            let (vid, follows) = match &caller {
                Some(c) => (
                    Some(c.account.id.as_str()),
                    note.author_id == c.account.id
                        || state
                            .services
                            .follows
                            .is_following(&c.account.id, &note.author_id)
                            .map_err(ApiError::from_port_error)?,
                ),
                None => (None, false),
            };
            let viewer = match vid {
                Some(v) => ViewerContext::local_user(v, follows),
                None => ViewerContext::anonymous(),
            };
            if !is_visible(&note, &viewer) {
                continue;
            }
        }
        // Mute/block filter (both directions hide, like timelines).
        if let Some(c) = &caller {
            if c.account.id != note.author_id {
                if state
                    .services
                    .mutes
                    .is_muting(&c.account.id, &note.author_id)
                    .map_err(ApiError::from_port_error)?
                {
                    continue;
                }
                if state
                    .services
                    .blocks
                    .is_blocking(&c.account.id, &note.author_id)
                    .map_err(ApiError::from_port_error)?
                    || state
                        .services
                        .blocks
                        .is_blocking(&note.author_id, &c.account.id)
                        .map_err(ApiError::from_port_error)?
                {
                    continue;
                }
            }
        }
        // Per-word text/cw substring search (upstream ILIKE parity).
        if !words.is_empty() {
            let haystack = format!(
                "{} {}",
                note.text.as_deref().unwrap_or(""),
                note.cw.as_deref().unwrap_or("")
            )
            .to_lowercase();
            if !words.iter().all(|w| haystack.contains(w)) {
                continue;
            }
        }
        out.push(
            pack_note(&note, viewer_id, &state.services, 0).map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/clips/add-note` (split from `api_dispatch_blocking`).
pub(crate) fn clips_add_note(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["clipId", "noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "clips/add-note")?;
    let clip_id = required_str(&obj, "clipId")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .add_clip_note(&caller.account.id, &clip_id, &note_id)
        .map_err(|e| map_clip_error(e, "clips/add-note"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/clips/remove-note` (split from `api_dispatch_blocking`).
pub(crate) fn clips_remove_note(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["clipId", "noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let clip_id = required_str(&obj, "clipId")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .remove_clip_note(&caller.account.id, &clip_id, &note_id)
        .map_err(|e| map_clip_error(e, "clips/remove-note"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/clips/favorite` (split from `api_dispatch_blocking`).
pub(crate) fn clips_favorite(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["clipId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:clip-favorite")?;
    let clip_id = required_str(&obj, "clipId")?;
    state
        .services
        .favorite_clip(&caller.account.id, &clip_id)
        .map_err(|e| map_clip_error(e, "clips/favorite"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/clips/unfavorite` (split from `api_dispatch_blocking`).
pub(crate) fn clips_unfavorite(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["clipId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:clip-favorite")?;
    let clip_id = required_str(&obj, "clipId")?;
    state
        .services
        .unfavorite_clip(&caller.account.id, &clip_id)
        .map_err(|e| map_clip_error(e, "clips/unfavorite"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/clips/my-favorites` (split from `api_dispatch_blocking`).
pub(crate) fn clips_my_favorites(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:clip-favorite")?;
    let clips = state
        .services
        .clip_favorites(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(clips.len());
    for clip in &clips {
        out.push(
            pack_clip(clip, Some(&caller.account.id), &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/clips/follow` (split from `api_dispatch_blocking`).
pub(crate) fn clips_follow(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["clipId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:clip-favorite")?;
    let clip_id = required_str(&obj, "clipId")?;
    state
        .services
        .follow_clip(&caller.account.id, &clip_id)
        .map_err(|e| map_clip_error(e, "clips/follow"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/clips/unfollow` (split from `api_dispatch_blocking`).
pub(crate) fn clips_unfollow(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["clipId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:clip-favorite")?;
    let clip_id = required_str(&obj, "clipId")?;
    state
        .services
        .unfollow_clip(&caller.account.id, &clip_id)
        .map_err(|e| map_clip_error(e, "clips/unfollow"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/clips/followers` (split from `api_dispatch_blocking`).
pub(crate) fn clips_followers(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["clipId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let clip_id = required_str(&obj, "clipId")?;
    let clip = state
        .services
        .visible_clip(&clip_id, Some(&caller.account.id))
        .map_err(|e| map_clip_error(e, "clips/followers"))?;
    if clip.owner_id != caller.account.id {
        return Err(ApiError::clip_followers_forbidden());
    }
    let follows = state
        .services
        .clips
        .followers_of_clip(&clip_id, 100)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(follows.len());
    for f in &follows {
        if let Some(acc) = state
            .services
            .accounts
            .find_by_id(&f.user_id)
            .map_err(ApiError::from_port_error)?
        {
            out.push(crate::api::misskey::packers::pack_user_lite(
                &acc,
                &state.services.public_url,
                state.services.clock.now_ms(),
            ));
        }
    }
    Ok(Json(Value::Array(out)).into_response())
}
