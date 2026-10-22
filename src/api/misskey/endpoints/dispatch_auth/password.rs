//! Auth/session/meta/i dispatcher arms (password).
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
use super::i;
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

/// `POST /api/signup` (split from `api_dispatch_blocking`).
pub(crate) fn signup(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, SIGNUP_FIELDS)?;
    let (account, raw_token) = match tokio::runtime::Handle::current()
        .block_on(do_signup(&state, &obj, &client_ip, &headers))
    {
        Ok(ok) => ok,
        // Registration-gate rejection: upstream's plain empty 400.
        Err(SignupError::Rejected) => return Ok(StatusCode::BAD_REQUEST.into_response()),
        Err(SignupError::Api(err)) => return Err(err),
    };
    let mut packed =
        pack_me_detailed(&account, true, &state.services).map_err(ApiError::from_port_error)?;
    if let Some(map) = packed.as_object_mut() {
        map.insert("token".to_string(), Value::String(raw_token));
    }
    Ok(Json(packed).into_response())
}

/// `POST /api/signin` (split from `api_dispatch_blocking`).
pub(crate) fn signin(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["username", "password"])?;
    let (account, raw_token) = tokio::runtime::Handle::current()
        .block_on(do_signin(&state, &obj, &client_ip, &headers))?;
    let user =
        pack_me_detailed(&account, true, &state.services).map_err(ApiError::from_port_error)?;
    Ok(Json(serde_json::json!({
        "id": account.id,
        "token": raw_token,
        "i": raw_token,
        "user": user,
    }))
    .into_response())
}

/// `POST /api/signin-flow` (split from `api_dispatch_blocking`).
pub(crate) fn signin_flow(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    match tokio::runtime::Handle::current()
        .block_on(do_signin_flow(&state, &obj, &client_ip, &headers))
    {
        Ok(value) => Ok(Json(value).into_response()),
        Err(SigninFlowError::BadRequest) => Ok(StatusCode::BAD_REQUEST.into_response()),
        Err(SigninFlowError::Api(err)) => Err(err),
    }
}

/// `POST /api/signin-with-passkey` (split from `api_dispatch_blocking`).
pub(crate) fn signin_with_passkey(
    _state: &ApiState,
    _obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    Err(ApiError::invalid_param(
        "credential",
        "passkey sign-in is not supported by this server",
    ))
}

/// `POST /api/request-reset-password` (split from `api_dispatch_blocking`).
pub(crate) fn request_reset_password(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["username", "email"])?;
    state
        .rate_limiter
        .check(&rate_key, "request-reset-password")?;
    let username = required_str(&obj, "username")?;
    let email = required_str(&obj, "email")?;
    // Information-leak posture (upstream: unknown user, email
    // mismatch, and unverified address are all silently ignored).
    // The transport errors UNIFORMLY below, so no oracle
    // distinguishes the cases (DIVERGENCE: without SMTP the
    // endpoint cannot operate, so it fails loudly instead of
    // silently dropping — uniformly, preserving the posture).
    let candidate = state
        .services
        .accounts
        .find_by_username_and_host(&username.to_lowercase(), None)
        .map_err(ApiError::from_port_error)?
        .filter(|acc| acc.is_local());
    let mailable = candidate
        .as_ref()
        .is_some_and(|acc| acc.email.as_deref() == Some(email.as_str()) && acc.email_verified);
    if mailable {
        let acc = candidate.expect("filtered Some");
        let token = random_code(64);
        state
            .services
            .password_resets
            .create_reset_request(PasswordResetRequest {
                id: state.services.ids.generate(),
                user_id: acc.id.clone(),
                token: token.clone(),
            })
            .map_err(ApiError::from_port_error)?;
        // Live send through the configured sender (the sibling-owned
        // `main.rs` wiring publishes it via `set_email_sender`).
        // Unconfigured answers `EMAIL_NOT_CONFIGURED` — uniformly,
        // like the non-mailable path below, so no oracle
        // distinguishes the cases.
        let message = password_reset_email(&email, &state.services.public_url, &token);
        send_email_now(&message)?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    // The row above keeps the loop code-complete and testable
    // (white-box token read); the user-visible send needs a
    // configured sender.
    Err(ApiError::email_not_configured())
}

/// `POST /api/reset-password` (split from `api_dispatch_blocking`).
pub(crate) fn reset_password(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["token", "password"])?;
    let token = required_str(&obj, "token")?;
    let password = required_str(&obj, "password")?;
    // Upstream parity: unknown tokens (`findOneByOrFail`) and
    // expired rows (bare `throw`, 30-minute TTL) both answer 500.
    let req = state
        .services
        .password_resets
        .find_reset_request(&token)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(ApiError::internal)?;
    let issued = state
        .services
        .ids
        .parse_time(&req.id)
        .map_err(|_| ApiError::internal())?;
    if state.services.clock.now_ms() - issued > 30 * 60 * 1000 {
        let _ = state.services.password_resets.delete_reset_request(&req.id);
        return Err(ApiError::internal());
    }
    if password.is_empty() {
        return Err(ApiError::invalid_param(
            "password",
            "must be a non-empty string",
        ));
    }
    let hash = tokio::runtime::Handle::current().block_on(hash_password_async(password))?;
    state
        .services
        .auth
        .set_password_hash(&req.user_id, &hash)
        .map_err(ApiError::from_port_error)?;
    state
        .services
        .password_resets
        .delete_reset_request(&req.id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/verify-email` (split from `api_dispatch_blocking`).
pub(crate) fn verify_email(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["code"])?;
    let code = required_str(&obj, "code")?;
    let account = state
        .services
        .accounts
        .find_by_email_verify_code(&code)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(ApiError::verify_email_no_such_code)?;
    state
        .services
        .accounts
        .update_account(
            &account.id,
            UpdateAccountPatch {
                email_verified: Some(true),
                email_verify_code: Some(None),
                ..Default::default()
            },
            Vec::new(),
        )
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
