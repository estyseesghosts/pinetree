//! Signup/signin, MiAuth web flow and caller resolution (session).
//!
//! Split from `endpoints/auth_http.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::parse::{MIAUTH_APPROVAL_WINDOW_MS, parse_json_body};
use super::signin;
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

pub(crate) fn session_cookie_value(raw_token: &str) -> String {
    format!("{SESSION_COOKIE_NAME}={raw_token}; HttpOnly; Path=/")
}

/// Client IP for rate limits and upload slots (slice 9, H4). The TCP peer is
/// the client unless it is in `trusted_proxies`. Only then is
/// `X-Forwarded-For` consulted: walk it right to left and take the first
/// address that is not a trusted proxy, so a client cannot prepend a fake
/// hop. An all-trusted chain uses its leftmost entry, and an unparseable
/// entry falls back to the peer. `X-Real-IP` and headers from untrusted
/// peers are ignored.
pub(crate) fn client_ip(
    peer: SocketAddr,
    headers: &HeaderMap,
    trusted_proxies: &[IpAddr],
) -> String {
    let peer_ip = peer.ip();
    if !trusted_proxies.contains(&peer_ip) {
        return peer_ip.to_string();
    }
    let mut hops: Vec<IpAddr> = Vec::new();
    for value in headers.get_all("x-forwarded-for") {
        let Ok(text) = value.to_str() else {
            return peer_ip.to_string();
        };
        for part in text.split(',') {
            match part.trim().parse::<IpAddr>() {
                Ok(ip) => hops.push(ip),
                Err(_) => return peer_ip.to_string(),
            }
        }
    }
    hops.iter()
        .rev()
        .find(|ip| !trusted_proxies.contains(ip))
        .or_else(|| hops.first())
        .unwrap_or(&peer_ip)
        .to_string()
}

/// Resolve the caller from `Authorization: Bearer`, the JSON body `"i"`
/// parameter, or the session cookie. `Ok(None)` when no credential was
/// presented; a bad token is `401 AUTHENTICATION_FAILED`.
pub(crate) fn resolve_caller(
    services: &AppServices,
    headers: &HeaderMap,
    body: Option<&Value>,
) -> Result<Option<AuthenticatedCaller>, ApiError> {
    let raw = extract_raw_token(
        headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()),
        body,
        headers.get(COOKIE).and_then(|v| v.to_str().ok()),
    );
    match raw {
        Some(token) => Ok(Some(authenticate(services, &token)?)),
        None => Ok(None),
    }
}

pub(crate) fn require_caller(
    caller: Option<AuthenticatedCaller>,
) -> Result<AuthenticatedCaller, ApiError> {
    caller.ok_or_else(ApiError::credential_required)
}

/// Rate-limit key: the caller's account id when authenticated, else the
/// client IP.
pub(crate) fn rate_key_for(caller: Option<&AuthenticatedCaller>, client_ip: &str) -> String {
    match caller {
        Some(caller) => caller.account.id.clone(),
        None => client_ip.to_string(),
    }
}

/// Slice 9 (H4): auth-throttle keys. Account keys use the lowercased
/// username as typed, so a failure is counted whether or not the account
/// exists. IP keys are the resolved client IP.
pub(crate) fn auth_account_key(username: &str) -> String {
    format!("acct:{}", username.to_lowercase())
}

pub(crate) fn auth_ip_key(client_ip: &str) -> String {
    format!("ip:{client_ip}")
}

/// Batch O WP9: append one `i/signin-history` row. Fire-and-forget like
/// upstream's `setImmediate` insert: a storage failure is logged, never
/// surfaced, so a history write can neither block nor fail a login.
pub(crate) fn record_signin_attempt(
    services: &AppServices,
    account_id: &str,
    ip: &str,
    headers: &HeaderMap,
    success: bool,
) {
    let user_agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // DIVERGENCE: upstream stores the full request headers jsonb; pinetree
    // keeps only the user agent (privacy + size; the JSON-RPC dispatch path
    // is the only other caller and shares this helper).
    let headers_json = serde_json::json!({ "user-agent": user_agent }).to_string();
    let record = SigninRecord {
        id: services.ids.generate(),
        account_id: account_id.to_string(),
        ip: ip.to_string(),
        headers: headers_json,
        success,
        created_at_ms: services.clock.now_ms(),
    };
    if let Err(e) = services.signin.record_signin(record) {
        eprintln!("signin history append for {account_id} failed: {e}");
    }
}

/// Slice 9 (H3): verify `password` against the stored hash off the async
/// executor. On success, a legacy or outdated hash is replaced with a fresh
/// Argon2id hash through the existing password-update path.
pub(crate) async fn verify_and_upgrade_password(
    state: &ApiState,
    account_id: &str,
    password: &str,
    stored: Option<String>,
) -> Result<bool, ApiError> {
    let Some(hash) = stored else {
        return Ok(false);
    };
    if !verify_password_async(password.to_string(), hash.clone()).await? {
        return Ok(false);
    }
    if needs_rehash(&hash) {
        let fresh = hash_password_async(password.to_string()).await?;
        state
            .services
            .auth
            .set_password_hash(account_id, &fresh)
            .map_err(ApiError::from_port_error)?;
    }
    Ok(true)
}

/// Record a 2fa password-check failure against the shared `auth/2fa`
/// throttle bucket (mirrors the `auth/change-password` pattern).
pub(crate) fn record_2fa_auth_failure(state: &ApiState, account_id: &str, client_ip: &str) {
    state.rate_limiter.record_auth_failure(
        "auth/2fa",
        &format!("acctid:{account_id}"),
        &auth_ip_key(client_ip),
    );
}
