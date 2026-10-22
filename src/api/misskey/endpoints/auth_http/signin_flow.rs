//! Signup/signin, MiAuth web flow and caller resolution (signin_flow).
//!
//! Split from `endpoints/auth_http.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::parse::{MIAUTH_APPROVAL_WINDOW_MS, parse_json_body};
use super::auth_account_key;
use super::auth_ip_key;
use super::client_ip;
use super::do_signin;
use super::is_truthy;
use super::record_signin_attempt;
use super::signin;
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

/// Misskey `signin-flow` step failure: plain empty 400 (upstream
/// `reply.code(400); return;` for a missing/misshaped `username`,
/// `password`, or `token`) versus a JSON error envelope.
pub(crate) enum SigninFlowError {
    BadRequest,
    Api(ApiError),
}

impl From<ApiError> for SigninFlowError {
    fn from(err: ApiError) -> Self {
        SigninFlowError::Api(err)
    }
}

/// Misskey `signin-flow` (upstream `SigninApiService.signin`): the
/// multi-step password login the web frontend (`MkSignin.vue`) drives:
///
/// 1. `{ username }` → `200 { finished: false, next: 'captcha' }`
///    (upstream answers `'password'` when the account has 2FA; pinetree
///    accounts never do, so the hint is always `'captcha'` — both hints
///    render the same password page, the flag only toggles captcha).
/// 2. `{ username, password, [captcha responses] }` →
///    `200 { finished: true, id, i }` on success, `403` incorrect-password
///    otherwise. Unknown usernames are `404`, suspended accounts `403`,
///    both before any password check (upstream order).
///
/// `token` (totp) and `credential` (passkey) fields are accepted and
/// ignored: no account can have 2FA/security keys yet (packers hardcode
/// `twoFactorEnabled: false`), so upstream's `!twoFactorEnabled` branch
/// — which ignores both and checks only the password — is the whole
/// reachable surface. Captcha verification is skipped the same way
/// upstream skips it when no captcha provider is configured.
///
/// DIVERGENCES from upstream: no `2fa`/`passkey` `next` hints (nothing to
/// enroll them with); throttling reuses the `auth/signin` failure-counted
/// bucket (10/account + 30/IP per minute) instead of upstream's IP-only
/// 10/hour + 1s min-interval, answering the verbatim upstream
/// `TOO_MANY_AUTHENTICATION_FAILURES` id on 429; unknown fields are
/// ignored rather than `UNSUPPORTED_FIELD`-rejected (upstream performs no
/// field check on this route); the success response sets no session
/// cookie (upstream `SigninService.signin` sets none either).
pub(crate) async fn do_signin_flow(
    state: &ApiState,
    obj: &Map<String, Value>,
    ip: &str,
    headers: &HeaderMap,
) -> Result<Value, SigninFlowError> {
    // Upstream only rejects a non-string `username` (`typeof username !==
    // 'string'` → 400); an empty string proceeds to the lookup and answers
    // 404 like any unknown name.
    let username = match obj.get("username").and_then(Value::as_str) {
        Some(username) => username.to_string(),
        _ => return Err(SigninFlowError::BadRequest),
    };
    if let Some(token) = obj.get("token") {
        if !token.is_null() && token.as_str().is_none() {
            return Err(SigninFlowError::BadRequest);
        }
    }
    // Throttle before any lookup or hash check (same failure-counted
    // bucket as the legacy `signin` arm).
    let acct_key = auth_account_key(&username);
    let ip_key = auth_ip_key(ip);
    if let Err(throttled) =
        state
            .rate_limiter
            .check_auth_failures("auth/signin", &acct_key, &ip_key)
    {
        return Err(SigninFlowError::Api(ApiError::signin_too_many_failures(
            throttled.retry_after_secs.unwrap_or(60),
        )));
    }
    let found = state
        .services
        .accounts
        .find_by_username_and_host(&username, None)
        .map_err(ApiError::from_port_error)?;
    let Some(account) = found else {
        // Unknown usernames record nothing, so the history never reveals
        // whether a name exists (same rule as `do_signin`).
        return Err(SigninFlowError::Api(ApiError::signin_no_such_user()));
    };
    // Suspended accounts are refused before the password verifies
    // (upstream `SigninApiService.ts` order; no history row, like
    // upstream's direct `error()` return).
    if account.is_suspended {
        return Err(SigninFlowError::Api(ApiError::signin_suspended()));
    }
    let password = match obj.get("password") {
        None | Some(Value::Null) => None,
        Some(Value::String(password)) => Some(password.clone()),
        Some(_) => return Err(SigninFlowError::BadRequest),
    };
    let Some(password) = password else {
        return Ok(serde_json::json!({ "finished": false, "next": "captcha" }));
    };
    let stored = state
        .services
        .auth
        .get_password_hash(&account.id)
        .map_err(ApiError::from_port_error)?;
    if !verify_and_upgrade_password(state, &account.id, &password, stored).await? {
        state
            .rate_limiter
            .record_auth_failure("auth/signin", &acct_key, &ip_key);
        record_signin_attempt(&state.services, &account.id, ip, headers, false);
        return Err(SigninFlowError::Api(ApiError::signin_incorrect_password()));
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
    record_signin_attempt(&state.services, &account.id, ip, headers, true);
    Ok(serde_json::json!({
        "finished": true,
        "id": account.id,
        "i": raw_token,
    }))
}

pub(crate) async fn signin_flow(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let obj = parse_json_body(&body)?;
    let ip = client_ip(peer, &headers, &state.trusted_proxies);
    match do_signin_flow(&state, &obj, &ip, &headers).await {
        Ok(value) => Ok(Json(value).into_response()),
        Err(SigninFlowError::BadRequest) => Ok(StatusCode::BAD_REQUEST.into_response()),
        Err(SigninFlowError::Api(err)) => Err(err),
    }
}

// --- Drive upload ----------------------------------------------------------
pub(crate) fn decode_base64(input: &str) -> Option<Vec<u8>> {
    let compact: Vec<u8> = input.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if compact.is_empty() || compact.len() % 4 != 0 {
        return None;
    }
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            b'=' => Some(0),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(compact.len() / 4 * 3);
    for (index, chunk) in compact.chunks(4).enumerate() {
        let last = index == compact.len() / 4 - 1;
        let mut values = [0u8; 4];
        let mut pad = 0u8;
        for (j, byte) in chunk.iter().enumerate() {
            values[j] = digit(*byte)?;
            if *byte == b'=' {
                pad += 1;
            }
        }
        if pad > 0 && !last {
            return None;
        }
        if pad > 2 {
            return None;
        }
        out.push(values[0] << 2 | values[1] >> 4);
        if pad < 2 {
            out.push(values[1] << 4 | values[2] >> 2);
        }
        if pad < 1 {
            out.push(values[2] << 6 | values[3]);
        }
    }
    Some(out)
}

pub(crate) fn is_truthy_value(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => is_truthy(Some(s.as_str())),
        Some(Value::Number(n)) => n.as_u64().is_some_and(|v| v != 0),
        _ => false,
    }
}
