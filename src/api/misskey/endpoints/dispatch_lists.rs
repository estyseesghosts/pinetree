//! User-list dispatcher arms.
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
use super::parse::{map_list_error, opt_bool, pack_account_for, parse_list_cursor, required_str};

/// `POST /api/users/lists/create` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["name"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let name = required_str(&obj, "name")?;
    let list = state
        .services
        .create_user_list(&caller.account.id, name)
        .map_err(|e| map_list_error(e, "users/lists/create"))?;
    Ok(Json(pack_user_list(&list, &[], &state.services)).into_response())
}

/// `POST /api/users/lists/update` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["listId", "name", "isPublic"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let list_id = required_str(&obj, "listId")?;
    let name = match obj.get("name") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            return Err(ApiError::invalid_param("name", "must be a string"));
        }
    };
    let is_public = match obj.get("isPublic") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => {
            return Err(ApiError::invalid_param("isPublic", "must be a boolean"));
        }
    };
    let list = state
        .services
        .update_user_list(&caller.account.id, &list_id, name, is_public)
        .map_err(|e| map_list_error(e, "users/lists/update"))?;
    let members = state
        .services
        .lists
        .members_of_list(&list.id)
        .map_err(ApiError::from_port_error)?;
    let ids = members
        .iter()
        .map(|m| m.member_id.clone())
        .collect::<Vec<_>>();
    Ok(Json(pack_user_list(&list, &ids, &state.services)).into_response())
}

/// `POST /api/users/lists/delete` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["listId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let list_id = required_str(&obj, "listId")?;
    state
        .services
        .delete_user_list(&caller.account.id, &list_id)
        .map_err(|e| map_list_error(e, "users/lists/delete"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/users/lists/list` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    if let Some(c) = &caller {
        c.require_permission("read:account")?;
    }
    let lists = match obj.get("userId").and_then(Value::as_str) {
        Some(user_id) => {
            let user = state
                .services
                .users_show_by_id(user_id)
                .map_err(|e| map_list_error(e, "users/lists/list"))?;
            if !user.is_local() {
                return Err(ApiError::user_list_remote_not_allowed());
            }
            state
                .services
                .lists
                .public_lists_by_owner(&user.id)
                .map_err(ApiError::from_port_error)?
        }
        None => {
            let caller = require_caller(caller)?;
            state
                .services
                .lists
                .lists_by_owner(&caller.account.id)
                .map_err(ApiError::from_port_error)?
        }
    };
    let mut out = Vec::with_capacity(lists.len());
    for list in &lists {
        let members = state
            .services
            .lists
            .members_of_list(&list.id)
            .map_err(ApiError::from_port_error)?;
        let ids = members
            .iter()
            .map(|m| m.member_id.clone())
            .collect::<Vec<_>>();
        out.push(pack_user_list(list, &ids, &state.services));
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/users/lists/show` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["listId", "forPublic"])?;
    if let Some(c) = &caller {
        c.require_permission("read:account")?;
    }
    let list_id = required_str(&obj, "listId")?;
    let for_public = opt_bool(&obj, "forPublic", false)?;
    let list = match state.services.lists.get_user_list(&list_id) {
        Ok(Some(list)) => {
            // Upstream lookup parity (`show.ts` / `get-memberships.ts`):
            // owner view (`{id, userId}`) when `forPublic` is off and a
            // caller is present, public-only view (`{id, isPublic}`)
            // otherwise — anonymous callers see public lists either way.
            let allowed = match (for_public, caller.as_ref()) {
                (false, Some(present)) => list.owner_id == present.account.id,
                _ => list.is_public,
            };
            if !allowed {
                return Err(ApiError::user_list_no_such_list("users/lists/show"));
            }
            list
        }
        Ok(None) => return Err(ApiError::user_list_no_such_list("users/lists/show")),
        Err(err) => return Err(ApiError::from_port_error(err)),
    };
    let members = state
        .services
        .lists
        .members_of_list(&list.id)
        .map_err(ApiError::from_port_error)?;
    let ids = members
        .iter()
        .map(|m| m.member_id.clone())
        .collect::<Vec<_>>();
    let mut packed = pack_user_list(&list, &ids, &state.services);
    if for_public && list.is_public {
        let liked_count = state
            .services
            .lists
            .list_favorite_count(&list.id)
            .map_err(ApiError::from_port_error)?;
        let is_liked = match &caller {
            Some(c) => state
                .services
                .lists
                .is_list_favorited(&c.account.id, &list.id)
                .map_err(ApiError::from_port_error)?,
            None => false,
        };
        if let Some(map) = packed.as_object_mut() {
            map.insert("likedCount".to_string(), serde_json::json!(liked_count));
            map.insert("isLiked".to_string(), Value::Bool(is_liked));
        }
    }
    Ok(Json(packed).into_response())
}

/// `POST /api/users/lists/push` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_push(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["listId", "userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "users/lists/push")?;
    let list_id = required_str(&obj, "listId")?;
    let user_id = required_str(&obj, "userId")?;
    state
        .services
        .push_list_member(&caller.account.id, &list_id, &user_id)
        .map_err(|e| map_list_error(e, "users/lists/push"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/users/lists/pull` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_pull(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["listId", "userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let list_id = required_str(&obj, "listId")?;
    let user_id = required_str(&obj, "userId")?;
    state
        .services
        .pull_list_member(&caller.account.id, &list_id, &user_id)
        .map_err(|e| map_list_error(e, "users/lists/pull"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/users/lists/get-memberships` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_get_memberships(
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
            "listId",
            "forPublic",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    if let Some(c) = &caller {
        c.require_permission("read:account")?;
    }
    let list_id = required_str(&obj, "listId")?;
    let for_public = opt_bool(&obj, "forPublic", false)?;
    let cursor = parse_list_cursor(&obj)?;
    let list = match state.services.lists.get_user_list(&list_id) {
        Ok(Some(list)) => {
            // Upstream lookup parity (`show.ts` / `get-memberships.ts`):
            // owner view (`{id, userId}`) when `forPublic` is off and a
            // caller is present, public-only view (`{id, isPublic}`)
            // otherwise — anonymous callers see public lists either way.
            let allowed = match (for_public, caller.as_ref()) {
                (false, Some(present)) => list.owner_id == present.account.id,
                _ => list.is_public,
            };
            if !allowed {
                return Err(ApiError::user_list_no_such_list(
                    "users/lists/get-memberships",
                ));
            }
            list
        }
        Ok(None) => {
            return Err(ApiError::user_list_no_such_list(
                "users/lists/get-memberships",
            ));
        }
        Err(err) => return Err(ApiError::from_port_error(err)),
    };
    let mut members = state
        .services
        .lists
        .members_of_list(&list.id)
        .map_err(ApiError::from_port_error)?;
    members.sort_by(|a, b| b.member_id.cmp(&a.member_id));
    let mut out = Vec::new();
    for m in members {
        if out.len() >= cursor.limit {
            break;
        }
        if let Some(since) = cursor.since_id.as_deref() {
            if m.member_id.as_str() <= since {
                continue;
            }
        }
        if let Some(until) = cursor.until_id.as_deref() {
            if m.member_id.as_str() >= until {
                continue;
            }
        }
        let Some(member) = state
            .services
            .accounts
            .find_by_id(&m.member_id)
            .map_err(ApiError::from_port_error)?
        else {
            continue;
        };
        let created_at = state
            .services
            .ids
            .parse_time(&m.id)
            .map(crate::activitypub::format_millis)
            .unwrap_or_else(|_| crate::activitypub::format_millis(m.created_at_ms));
        out.push(serde_json::json!({
            "id": m.id,
            "createdAt": created_at,
            "userId": member.id,
            "user": pack_account_for(&member, caller.as_ref(), &state.services)?,
            "withReplies": m.with_replies,
        }));
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/users/lists/update-membership` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_update_membership(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["listId", "userId", "withReplies"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let list_id = required_str(&obj, "listId")?;
    let user_id = required_str(&obj, "userId")?;
    let with_replies = opt_bool(&obj, "withReplies", false)?;
    state
        .services
        .update_list_membership(&caller.account.id, &list_id, &user_id, with_replies)
        .map_err(|e| map_list_error(e, "users/lists/update-membership"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/users/lists/create-from-public` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_create_from_public(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["name", "listId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state
        .rate_limiter
        .check(&rate_key, "users/lists/create-from-public")?;
    let name = required_str(&obj, "name")?;
    if name.chars().count() > 100 {
        return Err(ApiError::invalid_param("name", "must be at most 100 chars"));
    }
    let list_id = required_str(&obj, "listId")?;
    let list = state
        .services
        .create_list_from_public(&caller.account.id, name, &list_id)
        .map_err(|err| map_list_error(err, "users/lists/create-from-public"))?;
    // The source members were just copied in; pack the member ids
    // for the `UserList` shape (same pattern as `users/lists/show`).
    let members = state
        .services
        .lists
        .members_of_list(&list.id)
        .map_err(ApiError::from_port_error)?;
    let ids = members
        .iter()
        .map(|member| member.member_id.clone())
        .collect::<Vec<_>>();
    Ok(Json(pack_user_list(&list, &ids, &state.services)).into_response())
}

// `users/lists/favorite|unfavorite {listId!}` -> 204. Public lists
// only; dup/missing map to the verbatim favorite errors (note the
// unfavorite-miss quirk: wire code `ALREADY_FAVORITED`).
/// Shared body for `users/lists/favorite`, `users/lists/unfavorite` (split from `api_dispatch_blocking`).
fn users_lists_favorite_inner(
    state: &ApiState,
    endpoint: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["listId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let list_id = required_str(&obj, "listId")?;
    let list = match state.services.lists.get_user_list(&list_id) {
        Ok(Some(list)) if list.is_public => list,
        Ok(_) => {
            return Err(ApiError::user_list_no_such_user_list(endpoint));
        }
        Err(err) => return Err(ApiError::from_port_error(err)),
    };
    if endpoint == "users/lists/favorite" {
        let added = state
            .services
            .lists
            .favorite_list(&caller.account.id, &list.id)
            .map_err(ApiError::from_port_error)?;
        if !added {
            return Err(ApiError::user_list_already_favorited());
        }
    } else {
        let removed = state
            .services
            .lists
            .unfavorite_list(&caller.account.id, &list.id)
            .map_err(ApiError::from_port_error)?;
        if !removed {
            return Err(ApiError::user_list_not_favorited());
        }
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/users/lists/favorite` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_favorite(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    users_lists_favorite_inner(
        state,
        "users/lists/favorite",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}

/// `POST /api/users/lists/unfavorite` (split from `api_dispatch_blocking`).
pub(crate) fn users_lists_unfavorite(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    users_lists_favorite_inner(
        state,
        "users/lists/unfavorite",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}
