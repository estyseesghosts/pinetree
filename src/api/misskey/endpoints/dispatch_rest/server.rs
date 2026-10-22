//! Remaining dispatcher arms (gallery/flash/hashtags/announcements etc.) (server).
//!
//! Split from `endpoints/dispatch_rest.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{client_ip, require_caller};
use super::super::parse::{
    announcement_store, fetch_rss_feed, get_limit_with_default, is_fetchable_http_url,
    map_gallery_error, opt_bool, opt_i64, opt_string, pack_account_for, parse_id_cursor,
    parse_list_cursor, read_cpu_model, read_mem_total, required_str, str_list,
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

/// `server-info` (upstream `server-info.ts` shape): machine/cpu/mem from
/// what stable Rust + `/proc` can report; `fs` totals are unavailable
/// without `statvfs` (no libc/sysinfo dep) and answer 0 (DIVERGENCE,
/// recorded — the drive is local-filesystem single-node).
pub(crate) fn pack_server_info() -> Value {
    let machine = format!("{} {}", std::env::consts::OS, std::env::consts::ARCH);
    let cores = std::thread::available_parallelism()
        .map(|n| n.get() as u64)
        .unwrap_or(1);
    let cpu_model = read_cpu_model().unwrap_or_else(|| std::env::consts::ARCH.to_string());
    let mem_total = read_mem_total().unwrap_or(0);
    serde_json::json!({
        "machine": machine,
        "cpu": { "model": cpu_model, "cores": cores },
        "mem": { "total": mem_total },
        "fs": { "total": 0, "used": 0 },
    })
}

/// `POST /api/i/delete-account` (split from `api_dispatch_blocking`).
pub(crate) fn i_delete_account(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["password"])?;
    let caller = require_caller(caller)?;
    let password = required_str(&obj, "password")?;
    state
        .services
        .delete_account(
            Some(&caller.account.id),
            &caller.account.id,
            Some(&password),
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(serde_json::json!({})).into_response())
}

/// `POST /api/get-online-users-count` (split from `api_dispatch_blocking`).
pub(crate) fn get_online_users_count(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    // "Online" is defined by OUR streaming hub presence (live
    // authenticated sockets seen within 10 min), not Redis
    // heartbeats — we have no Redis (DIVERGENCE, hub docs).
    let count = state
        .hub
        .online_count(state.services.clock.now_ms(), ONLINE_THRESHOLD_MS);
    Ok(Json(serde_json::json!({ "count": count })).into_response())
}

/// `POST /api/pinned-users` (split from `api_dispatch_blocking`).
pub(crate) fn pinned_users(
    _state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    // Upstream reads `meta.pinnedUsers` (admin-set accts). Pinetree
    // has no admin surface for pinning users, so the configured set
    // is empty — shape-compatible `[]`, recorded in the manifest.
    Ok(Json(Value::Array(Vec::new())).into_response())
}

/// `POST /api/fetch-external-resources` (split from `api_dispatch_blocking`).
pub(crate) fn fetch_external_resources(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["url", "hash"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state
        .rate_limiter
        .check(&rate_key, "fetch-external-resources")?;
    let url = required_str(&obj, "url")?;
    let hash = required_str(&obj, "hash")?;
    // SSRF-guarded fetch (same client as federation input); 1 MiB
    // cap. Upstream `getJson` parses any body as JSON.
    let resp = state
        .services
        .federation_http
        .send("GET", &url, &[], None, 1_048_576)
        .map_err(|e| ApiError::invalid_param("url", &e.to_string()))?;
    if !(200..300).contains(&resp.status) {
        return Err(ApiError::invalid_param(
            "url",
            &format!("fetch returned HTTP {}", resp.status),
        ));
    }
    let body: Value =
        serde_json::from_slice(&resp.body).map_err(|_| ApiError::ext_resource_invalid_schema())?;
    let (kind, data) = match (&body.get("type"), &body.get("data")) {
        (Some(Value::String(kind)), Some(Value::String(data))) => (kind.clone(), data.clone()),
        _ => return Err(ApiError::ext_resource_invalid_schema()),
    };
    // Upstream parity: sha512 hex of the data with CRLF normalized
    // to LF must equal `hash`.
    let normalized = data.replace("\r\n", "\n");
    let digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha512::digest(normalized.as_bytes()).as_slice())
    };
    if digest != hash.to_lowercase() {
        return Err(ApiError::ext_resource_hash_mismatch());
    }
    Ok(Json(serde_json::json!({ "type": kind, "data": data })).into_response())
}

/// `POST /api/fetch-rss` (split from `api_dispatch_blocking`).
pub(crate) fn fetch_rss(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["url"])?;
    state.rate_limiter.check(&rate_key, "fetch-rss")?;
    let url = required_str(&obj, "url")?;
    if !is_fetchable_http_url(&url) {
        return Err(ApiError::fetch_rss_invalid_url());
    }
    let feed = fetch_rss_feed(&state.services, &url)?;
    Ok(Json(feed).into_response())
}

/// `POST /api/server-info` (split from `api_dispatch_blocking`).
pub(crate) fn server_info(
    _state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    Ok(Json(pack_server_info()).into_response())
}

/// `POST /api/stats` (split from `api_dispatch_blocking`).
pub(crate) fn stats(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let (notes, local_notes, users, local_users, reactions, instances, drive_local, drive_remote) =
        state
            .services
            .instance_stats()
            .map_err(ApiError::from_port_error)?;
    Ok(Json(serde_json::json!({
        "notesCount": notes,
        "originalNotesCount": local_notes,
        "usersCount": users,
        "originalUsersCount": local_users,
        "reactionsCount": reactions,
        "instances": instances,
        "driveUsageLocal": drive_local,
        "driveUsageRemote": drive_remote,
    }))
    .into_response())
}

/// `POST /api/url-preview` (split from `api_dispatch_blocking`).
pub(crate) fn url_preview(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["url"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    state.rate_limiter.check(&rate_key, "url-preview")?;
    let url = required_str(&obj, "url")?;
    match crate::api::misskey::preview::fetch_preview(&state.services.federation_http, &url) {
        Ok(value) => Ok(Json(value).into_response()),
        Err(detail) => Err(ApiError::invalid_param("url", &detail)),
    }
}
