//! Remaining dispatcher arms (gallery/flash/hashtags/announcements etc.) (announcements).
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
use super::endpoint;
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

/// Pack one announcement view (user `announcements*` shape, with the
/// per-viewer `isRead` flag).
pub(crate) fn pack_announcement_view(
    view: &crate::application::commands::announcements::AnnouncementView,
) -> Value {
    let announcement = &view.announcement;
    let created_at = announcement.created_at_ms;
    serde_json::json!({
        "id": announcement.id,
        "createdAt": crate::activitypub::format_millis(created_at),
        "updatedAt": announcement.updated_at_ms.map(crate::activitypub::format_millis),
        "text": announcement.text,
        "title": announcement.title,
        "imageUrl": announcement.image_url,
        "isRead": view.is_read,
        "showPopup": announcement.show_popup,
        "isGoodNews": announcement.is_good_news,
    })
}

/// `POST /api/announcements` (split from `api_dispatch_blocking`).
pub(crate) fn announcements(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let viewer = caller.as_ref().map(|caller| caller.account.id.clone());
    let views = state
        .services
        .announcements_list(announcement_store(), viewer.as_deref())
        .map_err(ApiError::from_port_error)?;
    let out: Vec<Value> = views.iter().map(pack_announcement_view).collect();
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/announcements/show` (split from `api_dispatch_blocking`).
pub(crate) fn announcements_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["announcementId"])?;
    let viewer = caller.as_ref().map(|caller| caller.account.id.clone());
    let announcement_id = required_str(&obj, "announcementId")?;
    let view = state
        .services
        .announcement_show(announcement_store(), &announcement_id, viewer.as_deref())
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_announcement_view(&view)).into_response())
}

/// `POST /api/i/read-announcement` (split from `api_dispatch_blocking`).
pub(crate) fn i_read_announcement(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["announcementId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "i/read-announcement")?;
    let announcement_id = required_str(&obj, "announcementId")?;
    state
        .services
        .announcement_read(announcement_store(), &caller.account.id, &announcement_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/promo/read` (split from `api_dispatch_blocking`).
pub(crate) fn promo_read(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "promo/read")?;
    state
        .services
        .promo_read(announcement_store(), &caller.account.id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

// `ap/show` (+ the admin-debug alias `ap/get`, upstream `ap/get.ts`:
// admin-only 30/h): same packed-output shape, never the raw-object
// return, credential always required.
/// Shared body for `ap/show`, `ap/get` (split from `api_dispatch_blocking`).
fn ap_show_inner(
    state: &ApiState,
    endpoint: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["uri"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    // `ap/get` is the admin-debug alias (upstream `ap/get.ts` is
    // admin-only 30/h): same packed-output shape as `ap/show`, never
    // the raw-object return, credential always required.
    state.rate_limiter.check(&rate_key, endpoint)?;
    let uri = required_str(&obj, "uri")?;
    let viewer_id = Some(caller.account.id.as_str());
    let base = state.services.public_url.trim_end_matches('/').to_string();

    // Local note first: direct URI lookup, then `{base}/notes/{id}`.
    let mut local_note = state
        .services
        .notes
        .find_by_uri(&uri)
        .map_err(ApiError::from_port_error)?;
    if local_note.is_none() {
        if let Some(rest) = uri.strip_prefix(&format!("{base}/notes/")) {
            let id = rest.split(['/', '?', '#']).next().unwrap_or("").trim();
            if !id.is_empty() {
                local_note = state
                    .services
                    .notes
                    .get_note(id)
                    .map_err(ApiError::from_port_error)?;
            }
        }
    }
    if let Some(cached) = local_note {
        let note = state
            .services
            .notes_show(&cached.id, viewer_id)
            .map_err(ApiError::from_port_error)?;
        let packed =
            pack_note(&note, viewer_id, &state.services, 0).map_err(ApiError::from_port_error)?;
        return Ok(Json(serde_json::json!({ "type": "Note", "object": packed })).into_response());
    }

    // Local user: direct URI lookup, then `{base}/users/{id}`.
    let mut local_account = state
        .services
        .accounts
        .find_by_uri(&uri)
        .map_err(ApiError::from_port_error)?;
    if local_account.is_none() {
        if let Some(rest) = uri.strip_prefix(&format!("{base}/users/")) {
            let id = rest.split(['/', '?', '#']).next().unwrap_or("").trim();
            if !id.is_empty() {
                local_account = state
                    .services
                    .accounts
                    .find_by_id(id)
                    .map_err(ApiError::from_port_error)?;
            }
        }
    }
    if let Some(cached) = local_account {
        let account = state
            .services
            .users_show_by_id(&cached.id)
            .map_err(ApiError::from_port_error)?;
        let packed = pack_account_for(&account, Some(&caller), &state.services)?;
        return Ok(Json(serde_json::json!({ "type": "User", "object": packed })).into_response());
    }

    // Remote: try the actor first, then the note.
    if let Ok((account, _)) =
        fetch_and_upsert_remote_actor(&state.services, &state.services.federation_http, uri.trim())
    {
        let packed = pack_account_for(&account, Some(&caller), &state.services)?;
        return Ok(Json(serde_json::json!({ "type": "User", "object": packed })).into_response());
    }
    let no_such_object =
        || ApiError::from_port_error(PortError::NotFound("NO_SUCH_OBJECT".to_string()));
    match fetch_remote_note(&state.services, &state.services.federation_http, uri.trim()) {
        Ok(parsed) => {
            let author = match state
                .services
                .accounts
                .find_by_uri(&parsed.attributed_to)
                .map_err(ApiError::from_port_error)?
            {
                Some(account) => account,
                None => match fetch_and_upsert_remote_actor(
                    &state.services,
                    &state.services.federation_http,
                    &parsed.attributed_to,
                ) {
                    Ok((account, _)) => account,
                    Err(_) => return Err(no_such_object()),
                },
            };
            // Same ingest as the federated inbox (one pipeline, no
            // adapter-local copy): reply link, media job, quotes.
            match crate::federation::inbox::ingest_fetched_remote_note(
                &state.services,
                &parsed,
                &author,
            ) {
                Ok(Some(note)) => {
                    let packed = pack_note(&note, viewer_id, &state.services, 0)
                        .map_err(ApiError::from_port_error)?;
                    Ok(
                        Json(serde_json::json!({ "type": "Note", "object": packed }))
                            .into_response(),
                    )
                }
                Ok(None) => {
                    // Replay: the note is already cached; re-read it.
                    match state
                        .services
                        .notes
                        .find_by_uri(&parsed.uri)
                        .map_err(ApiError::from_port_error)?
                    {
                        Some(note) => {
                            let packed = pack_note(&note, viewer_id, &state.services, 0)
                                .map_err(ApiError::from_port_error)?;
                            Ok(
                                Json(serde_json::json!({ "type": "Note", "object": packed }))
                                    .into_response(),
                            )
                        }
                        None => Err(no_such_object()),
                    }
                }
                Err(_) => Err(no_such_object()),
            }
        }
        Err(_) => Err(no_such_object()),
    }
}

/// `POST /api/ap/show` (split from `api_dispatch_blocking`).
pub(crate) fn ap_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    ap_show_inner(state, "ap/show", obj, caller, rate_key, client_ip, headers)
}

/// `POST /api/ap/get` (split from `api_dispatch_blocking`).
pub(crate) fn ap_get(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    ap_show_inner(state, "ap/get", obj, caller, rate_key, client_ip, headers)
}
