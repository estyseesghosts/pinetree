//! Following/clips/pages/antennas/invites dispatcher arms (pages).
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

/// `POST /api/pages/create` (split from `api_dispatch_blocking`).
pub(crate) fn pages_create(
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
            "title",
            "name",
            "summary",
            "content",
            "variables",
            "script",
            "eyeCatchingImageId",
            "font",
            "alignCenter",
            "hideTitleWhenPinned",
            "visibility",
            "pageVisibility",
            "visibleUserIds",
            "federate",
            "approvedActorUris",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:pages")?;
    state.rate_limiter.check(&rate_key, "pages/create")?;
    let body = parse_page_body(&obj, true)?;
    let page = state
        .services
        .create_page(&caller.account.id, body)
        .map_err(|e| map_page_error(e, "pages/create"))?;
    let packed = pack_page(&page, Some(&caller.account.id), &state.services)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/pages/update` (split from `api_dispatch_blocking`).
pub(crate) fn pages_update(
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
            "pageId",
            "title",
            "name",
            "summary",
            "content",
            "variables",
            "script",
            "eyeCatchingImageId",
            "font",
            "alignCenter",
            "hideTitleWhenPinned",
            "visibility",
            "pageVisibility",
            "visibleUserIds",
            "federate",
            "approvedActorUris",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:pages")?;
    state.rate_limiter.check(&rate_key, "pages/update")?;
    let page_id = required_str(&obj, "pageId")?;
    let body = parse_page_body(&obj, false)?;
    state
        .services
        .update_page(&caller.account.id, &page_id, body)
        .map_err(|e| map_page_error(e, "pages/update"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/pages/delete` (split from `api_dispatch_blocking`).
pub(crate) fn pages_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["pageId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:pages")?;
    let page_id = required_str(&obj, "pageId")?;
    state
        .services
        .delete_page(&caller.account.id, &page_id)
        .map_err(|e| map_page_error(e, "pages/delete"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/pages/show` (split from `api_dispatch_blocking`).
pub(crate) fn pages_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["pageId", "name", "username"])?;
    let viewer_id = caller.as_ref().map(|c| c.account.id.as_str());
    let page = match (obj.get("pageId"), obj.get("name"), obj.get("username")) {
        (Some(Value::String(id)), _, _) if !id.is_empty() => state
            .services
            .show_page_by_id(id, viewer_id)
            .map_err(|e| map_page_error(e, "pages/show"))?,
        (_, Some(Value::String(name)), Some(Value::String(username))) => state
            .services
            .show_page_by_name(name, username, viewer_id)
            .map_err(|e| map_page_error(e, "pages/show"))?,
        _ => {
            return Err(ApiError::invalid_param(
                "pageId",
                "either pageId or name+username is required",
            ));
        }
    };
    let packed = pack_page(&page, viewer_id, &state.services).map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/pages/like` (split from `api_dispatch_blocking`).
pub(crate) fn pages_like(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["pageId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:page-likes")?;
    let page_id = required_str(&obj, "pageId")?;
    state
        .services
        .like_page(&caller.account.id, &page_id)
        .map_err(|e| map_page_error(e, "pages/like"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/pages/unlike` (split from `api_dispatch_blocking`).
pub(crate) fn pages_unlike(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["pageId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:page-likes")?;
    let page_id = required_str(&obj, "pageId")?;
    state
        .services
        .unlike_page(&caller.account.id, &page_id)
        .map_err(|e| map_page_error(e, "pages/unlike"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/pages/featured` (split from `api_dispatch_blocking`).
pub(crate) fn pages_featured(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let viewer_id = caller.as_ref().map(|c| c.account.id.as_str());
    let pages = state
        .services
        .pages
        .featured_pages()
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(pages.len());
    for page in &pages {
        // `pages/featured` stays a pure query but serves public
        // pages only (Phase 1: gated pages never surface here —
        // upstream parity is a top-10-by-likedCount query, and the
        // gate is read-side).
        if state.services.check_page_visible(page, viewer_id).is_err() {
            continue;
        }
        // Double-guard: never feature non-public rows even if the
        // gate above ever admits Home.
        if page.visibility != crate::domain::note::Visibility::Public {
            continue;
        }
        out.push(pack_page(page, viewer_id, &state.services).map_err(ApiError::from_port_error)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}
