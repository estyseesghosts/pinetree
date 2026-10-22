//! Signup/signin, MiAuth web flow and caller resolution (signup).
//!
//! Split from `endpoints/auth_http.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::parse::{MIAUTH_APPROVAL_WINDOW_MS, parse_json_body};
use super::auth_account_key;
use super::auth_ip_key;
use super::client_ip;
use super::record_signin_attempt;
use super::session_cookie_value;
use super::verify_and_upgrade_password;
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

/// Signup failure. `Rejected` answers plain HTTP 400 with an empty body —
/// the upstream shape for registration-gate rejections (`SignupApiService.ts`
/// around line 136: `reply.code(400); return;`); everything else keeps the
/// standard `{ error: … }` envelope.
pub(crate) enum SignupError {
    Rejected,
    Api(ApiError),
}

pub(crate) async fn do_signup(
    state: &ApiState,
    obj: &Map<String, Value>,
    ip: &str,
    headers: &HeaderMap,
) -> Result<(Account, String), SignupError> {
    let username = match obj.get("username").and_then(Value::as_str) {
        Some(username) if !username.is_empty() => username,
        _ => {
            return Err(SignupError::Api(ApiError::invalid_param(
                "username",
                "username is required",
            )));
        }
    };
    let password = match obj.get("password").and_then(Value::as_str) {
        Some(password) if !password.is_empty() => password,
        _ => {
            return Err(SignupError::Api(ApiError::invalid_param(
                "password",
                "password is required",
            )));
        }
    };
    if let Some(host) = obj.get("host") {
        if !host.is_null() {
            return Err(SignupError::Api(ApiError::invalid_param(
                "host",
                "remote accounts are unsupported in Phase 1",
            )));
        }
    }
    // Slice 9 (H4): every signup attempt from one IP counts.
    state
        .rate_limiter
        .check(&auth_ip_key(ip), "auth/signup")
        .map_err(SignupError::Api)?;
    let hash = hash_password_async(password.to_string())
        .await
        .map_err(SignupError::Api)?;
    // WP2: `AppServices::signup` enforces `InstanceSettings.registration`
    // and, when closed, claims `invitationCode` in the account-creation
    // transaction (conformance I1/I2). Gate rejections are the plain 400.
    let account = state
        .services
        .signup(
            username,
            Some(&hash),
            obj.get("invitationCode").and_then(Value::as_str),
            state.services.clock.now_ms(),
        )
        .map_err(|err| match err {
            SignupRejection::InviteOnly => SignupError::Rejected,
            SignupRejection::Port(err) => SignupError::Api(ApiError::from_port_error(err)),
        })?;
    let raw_token = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&raw_token),
            account_id: account.id.clone(),
            permissions: Vec::new(),
            is_first_party: true,
            name: None,
            description: None,
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .map_err(|err| SignupError::Api(ApiError::from_port_error(err)))?;
    // Batch O WP9: signup counts as a successful signin (upstream funnels
    // signup through `SigninService.signin`).
    record_signin_attempt(&state.services, &account.id, ip, headers, true);
    Ok((account, raw_token))
}

pub(crate) async fn do_signin(
    state: &ApiState,
    obj: &Map<String, Value>,
    ip: &str,
    headers: &HeaderMap,
) -> Result<(Account, String), ApiError> {
    let username = match obj.get("username").and_then(Value::as_str) {
        Some(username) if !username.is_empty() => username,
        _ => {
            return Err(ApiError::invalid_param("username", "username is required"));
        }
    };
    let password = match obj.get("password").and_then(Value::as_str) {
        Some(password) if !password.is_empty() => password,
        _ => {
            return Err(ApiError::invalid_param("password", "password is required"));
        }
    };
    // Slice 9 (H4): throttle before any lookup or hash check. Failures are
    // counted for unknown usernames too, so responses do not reveal them.
    let acct_key = auth_account_key(username);
    let ip_key = auth_ip_key(ip);
    state
        .rate_limiter
        .check_auth_failures("auth/signin", &acct_key, &ip_key)?;
    let found = state
        .services
        .accounts
        .find_by_username_and_host(username, None)
        .map_err(ApiError::from_port_error)?;
    let verified = match &found {
        Some(account) => {
            let stored = state
                .services
                .auth
                .get_password_hash(&account.id)
                .map_err(ApiError::from_port_error)?;
            verify_and_upgrade_password(state, &account.id, password, stored).await?
        }
        None => false,
    };
    let account = match found {
        Some(account) if verified => account,
        _ => {
            state
                .rate_limiter
                .record_auth_failure("auth/signin", &acct_key, &ip_key);
            // Batch O WP9: known-account failures append a `success: false`
            // row (upstream `fail()`); unknown usernames record nothing, so
            // the history never reveals whether a name exists.
            if let Some(known) = &found {
                record_signin_attempt(&state.services, &known.id, ip, headers, false);
            }
            return Err(ApiError::authentication_failed());
        }
    };
    // Batch M WP3: suspended accounts cannot sign in (upstream
    // `SigninApiService.ts:138` answers 403). Checked AFTER the password
    // verifies so sign-in keeps pinetree's existence-hiding ordering (an
    // unknown and a suspended username answer alike to a caller without
    // the password) — DIVERGENCE from upstream, which reveals the account
    // state before the password check. Upstream's per-endpoint error id
    // (`e03a5f46-…`) is not copied: one UUID per code keeps pinetree's
    // `USER_SUSPENDED` id (§16 convention).
    if account.is_suspended {
        return Err(ApiError::user_suspended());
    }
    if account.is_deleted {
        // Existence-hiding order, like the wrong-password path above: a
        // deleted handle answers exactly like an unknown one.
        return Err(ApiError::authentication_failed());
    }
    // Phase 2 (P0): a verified TOTP secret ends the password-only path.
    // The legacy endpoint has no challenge step in its contract, so it
    // refuses outright and points at `signin-flow`; it never mints a
    // session for a TOTP-enabled account. `usePasswordLessLogin` does not
    // change this: without WebAuthn credentials there is no safe
    // passwordless path, so the second factor stays mandatory.
    if state
        .services
        .totp_flags(&account.id)
        .map_err(ApiError::from_port_error)?
        .0
    {
        return Err(ApiError::invalid_param(
            "password",
            "two-factor authentication is enabled for this account; finish sign-in with the signin-flow challenge",
        ));
    }
    let raw_token = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&raw_token),
            account_id: account.id.clone(),
            permissions: Vec::new(),
            is_first_party: true,
            name: None,
            description: None,
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .map_err(ApiError::from_port_error)?;
    // Batch O WP9: successful password signin appends a `success: true` row.
    record_signin_attempt(&state.services, &account.id, ip, headers, true);
    Ok((account, raw_token))
}

pub(crate) const SIGNUP_FIELDS: &[&str] = &[
    "username",
    "password",
    "setupPassword",
    "host",
    "invitationCode",
    "emailAddress",
];

pub(crate) async fn signup(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let obj = parse_json_body(&body)?;
    check_allowed_fields(&obj, SIGNUP_FIELDS)?;
    let ip = client_ip(peer, &headers, &state.trusted_proxies);
    let (account, raw_token) = match do_signup(&state, &obj, &ip, &headers).await {
        Ok(ok) => ok,
        // Registration-gate rejection: upstream's plain empty 400.
        Err(SignupError::Rejected) => return Ok(StatusCode::BAD_REQUEST.into_response()),
        Err(SignupError::Api(err)) => return Err(err),
    };
    let mut packed =
        pack_me_detailed(&account, true, &state.services).map_err(ApiError::from_port_error)?;
    if let Some(obj) = packed.as_object_mut() {
        obj.insert("token".to_string(), Value::String(raw_token.clone()));
        obj.insert("i".to_string(), Value::String(raw_token.clone()));
    }
    let mut response = Json(packed).into_response();
    if let Ok(cookie) = session_cookie_value(&raw_token).parse::<HeaderValue>() {
        response.headers_mut().insert(SET_COOKIE, cookie);
    }
    Ok(response)
}

pub(crate) async fn signin(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let obj = parse_json_body(&body)?;
    check_allowed_fields(&obj, &["username", "password"])?;
    let ip = client_ip(peer, &headers, &state.trusted_proxies);
    let (account, raw_token) = do_signin(&state, &obj, &ip, &headers).await?;
    let user =
        pack_me_detailed(&account, true, &state.services).map_err(ApiError::from_port_error)?;
    let mut response = Json(serde_json::json!({
        "id": account.id,
        "token": raw_token,
        "i": raw_token,
        "user": user,
    }))
    .into_response();
    if let Ok(cookie) = session_cookie_value(&raw_token).parse::<HeaderValue>() {
        response.headers_mut().insert(SET_COOKIE, cookie);
    }
    Ok(response)
}
