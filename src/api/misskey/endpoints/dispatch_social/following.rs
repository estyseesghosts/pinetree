//! Following/clips/pages/antennas/invites dispatcher arms (following).
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

/// Pack one follow request row (`following/requests/*` shape: id + lite
/// follower/followee).
pub(crate) fn pack_follow_request(
    state: &ApiState,
    id: &str,
    follower_id: &str,
    followee_id: &str,
) -> Result<Value, ApiError> {
    let now = state.services.clock.now_ms();
    let public_url = &state.services.public_url;
    let follower = state
        .services
        .accounts
        .find_by_id(follower_id)
        .map_err(ApiError::from_port_error)?;
    let followee = state
        .services
        .accounts
        .find_by_id(followee_id)
        .map_err(ApiError::from_port_error)?;
    Ok(serde_json::json!({
        "id": id,
        "follower": follower.map(|account| pack_user_lite(&account, public_url, now)),
        "followee": followee.map(|account| pack_user_lite(&account, public_url, now)),
    }))
}

/// `POST /api/following/create` (split from `api_dispatch_blocking`).
pub(crate) fn following_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "withReplies"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:following")?;
    state.rate_limiter.check(&rate_key, "following/create")?;
    let user_id = required_str(&obj, "userId")?;
    // Misskey `following/create.ts` declares no `withReplies`
    // default; the value flows into the relationship row whose
    // column default is false (`Following.ts:49-52`), so an
    // omitted `withReplies` means false upstream too.
    let with_replies = opt_bool(&obj, "withReplies", false)?;
    state
        .services
        .follow(&caller.account.id, &user_id, with_replies)
        .map_err(ApiError::from_port_error)?;
    let followee = state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_user_lite(
        &followee,
        &state.services.public_url,
        state.services.clock.now_ms(),
    ))
    .into_response())
}

/// `POST /api/following/delete` (split from `api_dispatch_blocking`).
pub(crate) fn following_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:following")?;
    state.rate_limiter.check(&rate_key, "following/delete")?;
    let user_id = required_str(&obj, "userId")?;
    state
        .services
        .unfollow(&caller.account.id, &user_id)
        .map_err(ApiError::from_port_error)?;
    let followee = state
        .services
        .users_show_by_id(&user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_user_lite(
        &followee,
        &state.services.public_url,
        state.services.clock.now_ms(),
    ))
    .into_response())
}

/// `POST /api/following/invalidate` (split from `api_dispatch_blocking`).
pub(crate) fn following_invalidate(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:following")?;
    state
        .rate_limiter
        .check(&rate_key, "following/invalidate")?;
    let follower_id = required_str(&obj, "userId")?;
    if follower_id == caller.account.id {
        return Err(ApiError::follower_is_yourself());
    }
    // Pinetree ids for unknown users differ from the invalidate
    // endpoint UUID upstream, so map explicitly.
    match state.services.users_show_by_id(&follower_id) {
        Ok(_) => {}
        Err(PortError::NotFound(_)) => return Err(ApiError::invalidate_no_such_user()),
        Err(err) => return Err(ApiError::from_port_error(err)),
    }
    match state
        .services
        .remove_follower(&caller.account.id, &follower_id)
    {
        Ok(()) => {}
        Err(PortError::NotFound(msg)) if msg.starts_with("NOT_FOLLOWING") => {
            return Err(ApiError::invalidate_not_following());
        }
        Err(err) => return Err(ApiError::from_port_error(err)),
    }
    let follower = state
        .services
        .users_show_by_id(&follower_id)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_user_lite(
        &follower,
        &state.services.public_url,
        state.services.clock.now_ms(),
    ))
    .into_response())
}

/// `POST /api/following/requests/list` (split from `api_dispatch_blocking`).
pub(crate) fn following_requests_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
    let caller = require_caller(caller)?;
    // Misskey `following/requests/list.ts:18` kind is
    // `read:following` (not `read:account`).
    caller.require_permission("read:following")?;
    let reqs = state
        .services
        .follows
        .follow_requests_to(&caller.account.id, None, 100)
        .map_err(ApiError::from_port_error)?;
    let now = state.services.clock.now_ms();
    let mut out = Vec::new();
    for r in reqs {
        let (Some(f), Some(t)) = (
            state
                .services
                .accounts
                .find_by_id(&r.follower_id)
                .map_err(ApiError::from_port_error)?,
            state
                .services
                .accounts
                .find_by_id(&r.followee_id)
                .map_err(ApiError::from_port_error)?,
        ) else {
            continue;
        };
        out.push(serde_json::json!({
            "id": r.id,
            "follower": pack_user_lite(&f, &state.services.public_url, now),
            "followee": pack_user_lite(&t, &state.services.public_url, now),
        }));
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/following/update` (split from `api_dispatch_blocking`).
pub(crate) fn following_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "withReplies", "notify"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:following")?;
    state.rate_limiter.check(&rate_key, "following/update")?;
    let user_id = required_str(&obj, "userId")?;
    if obj.get("withReplies").is_none() && obj.get("notify").is_none() {
        return Err(ApiError::invalid_param(
            "withReplies",
            "at least one of withReplies, notify is required",
        ));
    }
    let edge = state
        .services
        .follows
        .get_follow(&caller.account.id, &user_id)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NOT_FOLLOWING".to_string()))
        })?;
    let with_replies = opt_bool(&obj, "withReplies", edge.with_replies)?;
    let notify = match obj.get("notify") {
        None | Some(Value::Null) => edge.notify.clone(),
        _ => parse_follow_notify(&obj)?,
    };
    let updated = state
        .services
        .follows
        .update_follow_options(
            &caller.account.id,
            &user_id,
            with_replies,
            notify,
            Vec::new(),
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(
        pack_following(&updated, Some(&caller.account.id), &state.services)
            .map_err(ApiError::from_port_error)?,
    )
    .into_response())
}

/// `POST /api/following/update-all` (split from `api_dispatch_blocking`).
pub(crate) fn following_update_all(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["withReplies", "notify"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:following")?;
    state
        .rate_limiter
        .check(&rate_key, "following/update-all")?;
    if obj.get("withReplies").is_none() && obj.get("notify").is_none() {
        return Err(ApiError::invalid_param(
            "withReplies",
            "at least one of withReplies, notify is required",
        ));
    }
    let with_replies = match obj.get("withReplies") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(_)) => Some(opt_bool(&obj, "withReplies", false)?),
        Some(_) => {
            return Err(ApiError::invalid_param("withReplies", "must be a boolean"));
        }
    };
    let notify = match obj.get("notify") {
        None | Some(Value::Null) => None,
        _ => Some(parse_follow_notify(&obj)?),
    };
    for edge in state
        .services
        .follows
        .following_of(&caller.account.id, None, 10_000)
        .map_err(ApiError::from_port_error)?
    {
        state
            .services
            .follows
            .update_follow_options(
                &caller.account.id,
                &edge.followee_id,
                with_replies.unwrap_or(edge.with_replies),
                notify.clone().unwrap_or(edge.notify.clone()),
                Vec::new(),
            )
            .map_err(ApiError::from_port_error)?;
    }
    Ok(Json(Value::Object(Map::new())).into_response())
}

/// `POST /api/following/list` (split from `api_dispatch_blocking`).
pub(crate) fn following_list(
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
            "notification",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:following")?;
    let notify_only = opt_bool(&obj, "notification", false)?;
    let cursor = parse_cursor(&obj)?;
    let edges = state
        .services
        .following_list(&caller.account.id, notify_only, &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(edges.len());
    for edge in &edges {
        out.push(
            pack_following(edge, Some(&caller.account.id), &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/following/requests/sent` (split from `api_dispatch_blocking`).
pub(crate) fn following_requests_sent(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:following")?;
    let limit = get_limit_with_default(&obj, 10)?;
    let reqs = state
        .services
        .follows
        .follow_requests_from(&caller.account.id, None, limit)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::new();
    for request in reqs.iter().take(limit) {
        out.push(pack_follow_request(
            &state,
            &request.id,
            &request.follower_id,
            &request.followee_id,
        )?);
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// Shared body for `following/requests/accept`, `following/requests/reject`, `following/requests/cancel` (split from `api_dispatch_blocking`).
fn following_requests_inner(
    state: &ApiState,
    endpoint: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:following")?;
    state.rate_limiter.check(&rate_key, endpoint)?;
    let user_id = required_str(&obj, "userId")?;
    let me = caller.account.id.clone();
    match endpoint {
        "following/requests/accept" => state
            .services
            .accept_follow_request(&me, &user_id)
            .map(|_| ()),
        "following/requests/reject" => state.services.reject_follow_request(&me, &user_id),
        _ => state.services.cancel_follow_request(&me, &user_id),
    }
    .map_err(ApiError::from_port_error)?;
    if endpoint == "following/requests/cancel" {
        let followee = state
            .services
            .users_show_by_id(&user_id)
            .map_err(ApiError::from_port_error)?;
        return Ok(Json(pack_user_lite(
            &followee,
            &state.services.public_url,
            state.services.clock.now_ms(),
        ))
        .into_response());
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/following/requests/accept` (split from `api_dispatch_blocking`).
pub(crate) fn following_requests_accept(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    following_requests_inner(
        state,
        "following/requests/accept",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}

/// `POST /api/following/requests/reject` (split from `api_dispatch_blocking`).
pub(crate) fn following_requests_reject(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    following_requests_inner(
        state,
        "following/requests/reject",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}

/// `POST /api/following/requests/cancel` (split from `api_dispatch_blocking`).
pub(crate) fn following_requests_cancel(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    following_requests_inner(
        state,
        "following/requests/cancel",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}
