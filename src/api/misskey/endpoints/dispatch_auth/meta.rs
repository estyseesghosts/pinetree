//! Auth/session/meta/i dispatcher arms (meta).
//!
//! Split from `endpoints/dispatch_auth.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{
    SIGNUP_FIELDS, SigninFlowError, SignupError, auth_ip_key, client_ip, do_signin, do_signin_flow,
    do_signup, record_2fa_auth_failure, record_signin_attempt, require_caller, send_email_now,
    set_email_sender, verify_and_upgrade_password,
};
use super::super::dispatch_rest::pack_announcement_view;
use super::super::parse::{
    MIAUTH_APPROVAL_WINDOW_MS, announcement_store, opt_bool, opt_string, parse_permissions,
    parse_profile_patch, random_code, required_str, required_str_array,
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

/// Pack one developer-app row. The secret is only included for the
/// creator-facing endpoints (`app/create`, `app/show`); `my/apps` omits it.
pub(crate) fn pack_app_record(
    record: &crate::domain::admin::AppRecord,
    include_secret: bool,
) -> Value {
    let mut obj = serde_json::json!({
        "id": record.id,
        "name": record.name,
        "callbackUrl": record.callback_url,
        "permission": record.permission,
        "description": record.description,
        "createdAt": crate::activitypub::format_millis(record.created_at_ms),
    });
    if include_secret {
        if let Some(map) = obj.as_object_mut() {
            map.insert("secret".to_string(), Value::String(record.secret.clone()));
        }
    }
    obj
}

/// `POST /api/meta` (split from `api_dispatch_blocking`).
pub(crate) fn meta(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["detail"])?;
    let detail = opt_bool(&obj, "detail", true)?;
    let meta = pack_meta(&state.services, detail).map_err(ApiError::from_port_error)?;
    Ok(Json(meta).into_response())
}

/// `POST /api/miauth/gen-token` (split from `api_dispatch_blocking`).
pub(crate) fn miauth_gen_token(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["session", "permission", "name", "description", "iconUrl"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    // Upstream `gen-token.ts` requires both params; pinetree used
    // to fill defaults (random session, empty permissions). Batch
    // N WS7 aligns to upstream (missing → INVALID_PARAM).
    let session = match obj.get("session") {
        Some(Value::String(s)) if !s.trim().is_empty() => s.clone(),
        _ => return Err(ApiError::invalid_param("session", "this field is required")),
    };
    if !matches!(obj.get("permission"), Some(Value::Array(_))) {
        return Err(ApiError::invalid_param(
            "permission",
            "this field is required",
        ));
    }
    let permissions = parse_permissions(obj.get("permission"));
    let raw_token = generate_random_token();
    let now_ms = state.services.clock.now_ms();
    // Batch N WS7: approval window (DIVERGENCE — upstream has no
    // expiry). Expired sessions read as missing (deleted on read).
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&raw_token),
            account_id: caller.account.id.clone(),
            permissions: permissions.clone(),
            is_first_party: false,
            name: obj
                .get("name")
                .and_then(Value::as_str)
                .map(|s| s.to_string()),
            description: obj
                .get("description")
                .and_then(Value::as_str)
                .map(|s| s.to_string()),
            icon_url: obj
                .get("iconUrl")
                .and_then(Value::as_str)
                .map(|s| s.to_string()),
            created_at_ms: state.services.clock.now_ms(),
        })
        .map_err(ApiError::from_port_error)?;
    state
        .services
        .auth
        .save_miauth_session(MiAuthSessionRecord {
            session: session.clone(),
            name: obj
                .get("name")
                .and_then(Value::as_str)
                .map(|s| s.to_string()),
            description: obj
                .get("description")
                .and_then(Value::as_str)
                .map(|s| s.to_string()),
            icon_url: obj
                .get("iconUrl")
                .and_then(Value::as_str)
                .map(|s| s.to_string()),
            callback: None,
            permissions,
            account_id: Some(caller.account.id.clone()),
            token: Some(raw_token.clone()),
            fetched: false,
            created_at_ms: now_ms,
            expires_at_ms: Some(now_ms + MIAUTH_APPROVAL_WINDOW_MS),
        })
        .map_err(ApiError::from_port_error)?;
    Ok(Json(serde_json::json!({ "token": raw_token })).into_response())
}

/// `POST /api/auth/session/generate` (split from `api_dispatch_blocking`).
pub(crate) fn auth_session_generate(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["appSecret"])?;
    let app_secret = obj
        .get("appSecret")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let token = generate_random_token();
    state
        .services
        .auth
        .save_legacy_session(LegacyAuthSessionRecord {
            id: state.services.ids.generate(),
            token: token.clone(),
            app_secret,
            app_name: "Legacy app".to_string(),
            permissions: Vec::new(),
            account_id: None,
            access_token: None,
            created_at_ms: state.services.clock.now_ms(),
            // Batch N WS7: same approval window as MiAuth
            // (DIVERGENCE — upstream has no expiry). DIVERGENCE
            // (kept): pinetree has no app registry, so any
            // `appSecret` is accepted here and verified at
            // `userkey` time (`NO_SUCH_APP` has nothing to check
            // against). The approve URL stays the MiAuth page,
            // which mints a fresh unrelated row — legacy approval
            // runs through `auth/accept`, not the URL.
            expires_at_ms: Some(state.services.clock.now_ms() + MIAUTH_APPROVAL_WINDOW_MS),
        })
        .map_err(ApiError::from_port_error)?;
    let url = format!(
        "{}/miauth/{}",
        state.services.public_url.trim_end_matches('/'),
        token
    );
    Ok(Json(serde_json::json!({ "token": token, "url": url })).into_response())
}

/// `POST /api/auth/session/show` (split from `api_dispatch_blocking`).
pub(crate) fn auth_session_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["token"])?;
    let token = required_str(&obj, "token")?;
    let sess = state
        .services
        .auth
        .get_legacy_session(&token)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_SESSION".to_string()))
        })?;
    Ok(Json(serde_json::json!({
        "id": sess.id,
        "app": {
            "id": sess.id,
            "name": sess.app_name,
            "callbackUrl": null,
            "permission": sess.permissions,
        },
        "token": sess.token,
    }))
    .into_response())
}

/// `POST /api/auth/accept` (split from `api_dispatch_blocking`).
pub(crate) fn auth_accept(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["token"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    let token = required_str(&obj, "token")?;
    let mut sess = state
        .services
        .auth
        .get_legacy_session(&token)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_SESSION".to_string()))
        })?;
    let access_token = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&access_token),
            account_id: caller.account.id.clone(),
            permissions: sess.permissions.clone(),
            is_first_party: false,
            name: Some(sess.app_name.clone()),
            description: None,
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .map_err(ApiError::from_port_error)?;
    sess.account_id = Some(caller.account.id.clone());
    sess.access_token = Some(access_token);
    state
        .services
        .auth
        .save_legacy_session(sess)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/auth/session/userkey` (split from `api_dispatch_blocking`).
pub(crate) fn auth_session_userkey(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["appSecret", "token"])?;
    let token = required_str(&obj, "token")?;
    let app_secret = obj
        .get("appSecret")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let sess = state
        .services
        .auth
        .take_legacy_session(&token)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_SESSION".to_string()))
        })?;
    if sess.app_secret != app_secret {
        return Err(ApiError::authentication_failed());
    }
    match (sess.account_id, sess.access_token) {
        (Some(account_id), Some(access_token)) => {
            let account = state
                .services
                .accounts
                .find_by_id(&account_id)
                .map_err(ApiError::from_port_error)?
                .ok_or_else(|| {
                    ApiError::from_port_error(PortError::NotFound("NO_SUCH_USER".to_string()))
                })?;
            let user = pack_user_detailed_not_me(&account, None, &state.services)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(serde_json::json!({
                "accessToken": access_token,
                "user": user,
            }))
            .into_response())
        }
        _ => Err(ApiError::from_port_error(PortError::Validation(
            "PENDING_SESSION".to_string(),
        ))),
    }
}
