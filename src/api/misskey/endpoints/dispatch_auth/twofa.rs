//! Auth/session/meta/i dispatcher arms (twofa).
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

/// `POST /api/i/2fa/register` (split from `api_dispatch_blocking`).
pub(crate) fn i_2fa_register(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["password"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/2fa/register")?;
    let password = required_str(&obj, "password")?;
    let acct_key = format!("acctid:{}", caller.account.id);
    let ip_key = auth_ip_key(&client_ip);
    state
        .rate_limiter
        .check_auth_failures("auth/2fa", &acct_key, &ip_key)?;
    match state.services.register_2fa(&caller.account.id, &password) {
        Ok(enrol) => Ok(Json(serde_json::json!({
            "qr": enrol.qr,
            "url": enrol.url,
            "secret": enrol.secret,
            "label": enrol.label,
        }))
        .into_response()),
        Err(err) => {
            if matches!(&err, PortError::Validation(msg) if msg == "INCORRECT_PASSWORD") {
                record_2fa_auth_failure(&state, &caller.account.id, &client_ip);
            }
            Err(ApiError::from_port_error(err))
        }
    }
}

/// `POST /api/i/2fa/key-done` (split from `api_dispatch_blocking`).
pub(crate) fn i_2fa_key_done(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["password", "token"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/2fa/key-done")?;
    let password = required_str(&obj, "password")?;
    let token = required_str(&obj, "token")?;
    let acct_key = format!("acctid:{}", caller.account.id);
    let ip_key = auth_ip_key(&client_ip);
    state
        .rate_limiter
        .check_auth_failures("auth/2fa", &acct_key, &ip_key)?;
    match state
        .services
        .confirm_2fa_key(&caller.account.id, &password, &token)
    {
        Ok(()) => Ok(StatusCode::NO_CONTENT.into_response()),
        Err(err) => {
            if matches!(&err, PortError::Validation(msg) if msg == "INCORRECT_PASSWORD") {
                record_2fa_auth_failure(&state, &caller.account.id, &client_ip);
            }
            Err(ApiError::from_port_error(err))
        }
    }
}

/// `POST /api/i/2fa/done` (split from `api_dispatch_blocking`).
pub(crate) fn i_2fa_done(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["token"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/2fa/done")?;
    let token = required_str(&obj, "token")?;
    state
        .services
        .done_2fa(&caller.account.id, &token)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/2fa/password-less` (split from `api_dispatch_blocking`).
pub(crate) fn i_2fa_password_less(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["value"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/2fa/password-less")?;
    let value = match obj.get("value") {
        Some(Value::Bool(b)) => *b,
        _ => {
            return Err(ApiError::invalid_param("value", "must be a boolean"));
        }
    };
    state
        .services
        .set_2fa_passwordless(&caller.account.id, value)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/2fa/unregister` (split from `api_dispatch_blocking`).
pub(crate) fn i_2fa_unregister(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["password"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/2fa/unregister")?;
    let password = required_str(&obj, "password")?;
    let acct_key = format!("acctid:{}", caller.account.id);
    let ip_key = auth_ip_key(&client_ip);
    state
        .rate_limiter
        .check_auth_failures("auth/2fa", &acct_key, &ip_key)?;
    match state.services.unregister_2fa(&caller.account.id, &password) {
        Ok(()) => Ok(StatusCode::NO_CONTENT.into_response()),
        Err(err) => {
            if matches!(&err, PortError::Validation(msg) if msg == "INCORRECT_PASSWORD") {
                record_2fa_auth_failure(&state, &caller.account.id, &client_ip);
            }
            Err(ApiError::from_port_error(err))
        }
    }
}

/// `POST /api/i/2fa/update-key` (split from `api_dispatch_blocking`).
pub(crate) fn i_2fa_update_key(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["password"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/2fa/update-key")?;
    let password = required_str(&obj, "password")?;
    let acct_key = format!("acctid:{}", caller.account.id);
    let ip_key = auth_ip_key(&client_ip);
    state
        .rate_limiter
        .check_auth_failures("auth/2fa", &acct_key, &ip_key)?;
    match state.services.rotate_2fa_key(&caller.account.id, &password) {
        Ok(enrol) => Ok(Json(serde_json::json!({
            "qr": enrol.qr,
            "url": enrol.url,
            "secret": enrol.secret,
            "label": enrol.label,
        }))
        .into_response()),
        Err(err) => {
            if matches!(&err, PortError::Validation(msg) if msg == "INCORRECT_PASSWORD") {
                record_2fa_auth_failure(&state, &caller.account.id, &client_ip);
            }
            Err(ApiError::from_port_error(err))
        }
    }
}

/// `POST /api/i/2fa/remove-key` (split from `api_dispatch_blocking`).
pub(crate) fn i_2fa_remove_key(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["credentialId"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    let credential_id = opt_string(&obj, "credentialId")?.unwrap_or_default();
    state
        .services
        .remove_2fa_security_key(&caller.account.id, &credential_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/2fa/authenticate` (split from `api_dispatch_blocking`).
///
/// Second step of the signin-flow login: `challenge` is the one-time
/// value the password step returned for this account. Bare
/// `userId + token` is rejected: without the challenge binding, a TOTP
/// code alone must never mint a session.
pub(crate) fn i_2fa_authenticate(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "token", "challenge"])?;
    state.rate_limiter.check(&rate_key, "i/2fa/authenticate")?;
    let user_id = required_str(&obj, "userId")?;
    let token = required_str(&obj, "token")?;
    let challenge = required_str(&obj, "challenge")?;
    let acct_key = format!("acctid:{user_id}");
    let ip_key = auth_ip_key(&client_ip);
    state
        .rate_limiter
        .check_auth_failures("auth/2fa-authenticate", &acct_key, &ip_key)?;
    let authed = state
        .services
        .authenticate_2fa(&user_id, &challenge, &token);
    if let Err(err) = authed {
        state
            .rate_limiter
            .record_auth_failure("auth/2fa-authenticate", &acct_key, &ip_key);
        record_signin_attempt(&state.services, &user_id, &client_ip, &headers, false);
        return Err(ApiError::from_port_error(err));
    }
    let raw_token = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&raw_token),
            account_id: user_id.clone(),
            permissions: Vec::new(),
            is_first_party: true,
            name: None,
            description: None,
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .map_err(ApiError::from_port_error)?;
    record_signin_attempt(&state.services, &user_id, &client_ip, &headers, true);
    Ok(Json(serde_json::json!({ "token": raw_token })).into_response())
}
