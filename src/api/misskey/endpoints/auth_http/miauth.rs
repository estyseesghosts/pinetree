//! Signup/signin, MiAuth web flow and caller resolution (miauth).
//!
//! Split from `endpoints/auth_http.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::parse::{MIAUTH_APPROVAL_WINDOW_MS, parse_json_body};
use super::auth_account_key;
use super::auth_ip_key;
use super::client_ip;
use super::session_cookie_value;
use super::signup;
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

pub(crate) async fn miauth_page(
    State(_state): State<ApiState>,
    Path(session): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let name = query.get("name").cloned();
    let icon = query.get("icon").cloned();
    let callback = query.get("callback").cloned();
    let permission = query.get("permission").cloned();
    // Batch N WS7: the GET never writes. Upstream serves a static client
    // (no write); pinetree used to upsert a row for any attacker-chosen
    // session id on every page view. The page renders from the query
    // params; `miauth_submit` persists on approve.
    Ok(Html(render_miauth_page(
        &session,
        name.as_deref(),
        icon.as_deref(),
        callback.as_deref(),
        permission.as_deref(),
        None,
    ))
    .into_response())
}

pub(crate) fn hex_val(byte: Option<&u8>) -> Option<u8> {
    match byte {
        Some(b'0'..=b'9') => Some(byte.expect("matched") - b'0'),
        Some(b'a'..=b'f') => Some(byte.expect("matched") - b'a' + 10),
        Some(b'A'..=b'F') => Some(byte.expect("matched") - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn pct_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => match (hex_val(bytes.get(i + 1)), hex_val(bytes.get(i + 2))) {
                (Some(hi), Some(lo)) => {
                    out.push(hi << 4 | lo);
                    i += 3;
                }
                _ => {
                    out.push(b'%');
                    i += 1;
                }
            },
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub(crate) fn parse_form(body: &str) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    for pair in body.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        fields.insert(pct_decode(key), pct_decode(value));
    }
    fields
}

pub(crate) fn json_to_string_map(value: &Value) -> HashMap<String, String> {
    match value.as_object() {
        Some(obj) => obj
            .iter()
            .filter_map(|(k, v)| match v {
                Value::String(s) => Some((k.clone(), s.clone())),
                Value::Bool(b) => Some((k.clone(), b.to_string())),
                Value::Number(n) => Some((k.clone(), n.to_string())),
                Value::Null => None,
                _ => None,
            })
            .collect(),
        None => HashMap::new(),
    }
}

pub(crate) fn is_truthy(value: Option<&str>) -> bool {
    matches!(value, Some("true") | Some("1") | Some("on") | Some("yes"))
}

pub(crate) fn miauth_error_page(
    session: &str,
    fields: &HashMap<String, String>,
    message: &str,
) -> Response {
    Html(render_miauth_page(
        session,
        fields.get("name").map(String::as_str),
        fields.get("icon").map(String::as_str),
        fields.get("callback").map(String::as_str),
        fields.get("permission").map(String::as_str),
        Some(message),
    ))
    .into_response()
}

pub(crate) async fn miauth_submit(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(session): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let ip = client_ip(peer, &headers, &state.trusted_proxies);
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let fields: HashMap<String, String> = if content_type.contains("application/json") {
        let value: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        json_to_string_map(&value)
    } else {
        parse_form(&String::from_utf8_lossy(&body))
    };

    let username = fields.get("username").cloned().unwrap_or_default();
    let password = fields.get("password").cloned().unwrap_or_default();
    if username.is_empty() || password.is_empty() {
        return Ok(miauth_error_page(
            &session,
            &fields,
            "Username and password are required.",
        ));
    }

    let account = if is_truthy(fields.get("signup").map(String::as_str)) {
        // Slice 9 (H4): every signup attempt from one IP counts.
        state.rate_limiter.check(&auth_ip_key(&ip), "auth/signup")?;
        let hash = hash_password_async(password.clone()).await?;
        match state.services.create_account(&username, Some(&hash)) {
            Ok(account) => account,
            Err(err) => {
                return Ok(miauth_error_page(
                    &session,
                    &fields,
                    &ApiError::from_port_error(err).message,
                ));
            }
        }
    } else {
        // Slice 9 (H4): throttle before any lookup or hash check.
        let acct_key = auth_account_key(&username);
        let ip_key = auth_ip_key(&ip);
        state
            .rate_limiter
            .check_auth_failures("auth/miauth", &acct_key, &ip_key)?;
        let found = state
            .services
            .accounts
            .find_by_username_and_host(&username, None)
            .map_err(ApiError::from_port_error)?;
        let stored = match &found {
            Some(account) => state
                .services
                .auth
                .get_password_hash(&account.id)
                .map_err(ApiError::from_port_error)?,
            None => None,
        };
        let verified = match &found {
            Some(account) => {
                verify_and_upgrade_password(&state, &account.id, &password, stored).await?
            }
            None => false,
        };
        match found {
            Some(account) if verified => {
                // Batch N WS7: suspended accounts cannot approve (their
                // tokens would die at authenticate anyway; refuse here so
                // no session row is minted for them).
                if account.is_suspended {
                    state
                        .rate_limiter
                        .record_auth_failure("auth/miauth", &acct_key, &ip_key);
                    return Ok(miauth_error_page(
                        &session,
                        &fields,
                        "This account has been suspended.",
                    ));
                }
                // Deleted handles read as unknown (same page as a wrong
                // password; no oracle).
                if account.is_deleted {
                    state
                        .rate_limiter
                        .record_auth_failure("auth/miauth", &acct_key, &ip_key);
                    return Ok(miauth_error_page(
                        &session,
                        &fields,
                        "Invalid username or password.",
                    ));
                }
                // Phase 2 (P0): the approval form has no TOTP input, so a
                // TOTP-enabled account cannot approve here. Fail closed:
                // mint nothing and say so, instead of issuing a usable
                // session on the password alone.
                if state
                    .services
                    .totp_flags(&account.id)
                    .map_err(ApiError::from_port_error)?
                    .0
                {
                    return Ok(miauth_error_page(
                        &session,
                        &fields,
                        "This account has two-factor authentication enabled. Approve this sign-in from the web sign-in flow instead.",
                    ));
                }
                account
            }
            _ => {
                state
                    .rate_limiter
                    .record_auth_failure("auth/miauth", &acct_key, &ip_key);
                return Ok(miauth_error_page(
                    &session,
                    &fields,
                    "Invalid username or password.",
                ));
            }
        }
    };

    let permissions = fields
        .get("permission")
        .map(|p| {
            p.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let raw_token = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&raw_token),
            account_id: account.id.clone(),
            permissions: permissions.clone(),
            is_first_party: false,
            name: fields.get("name").cloned(),
            description: None,
            icon_url: fields.get("icon").cloned(),
            created_at_ms: state.services.clock.now_ms(),
        })
        .map_err(ApiError::from_port_error)?;

    let now_ms = state.services.clock.now_ms();
    let existing = state
        .services
        .auth
        .get_miauth_session(&session)
        .map_err(ApiError::from_port_error)?;
    let record = match existing {
        Some(mut sess) => {
            if fields.contains_key("name") {
                sess.name = fields.get("name").cloned();
            }
            if fields.contains_key("icon") {
                sess.icon_url = fields.get("icon").cloned();
            }
            if fields.contains_key("callback") {
                sess.callback = fields.get("callback").cloned();
            }
            if fields.contains_key("permission") {
                sess.permissions = permissions.clone();
            }
            sess.account_id = Some(account.id.clone());
            sess.token = Some(raw_token.clone());
            sess.fetched = false;
            // Re-arm the approval window on every (re-)approve.
            sess.expires_at_ms = Some(now_ms + MIAUTH_APPROVAL_WINDOW_MS);
            sess
        }
        None => MiAuthSessionRecord {
            session: session.clone(),
            name: fields.get("name").cloned(),
            description: None,
            icon_url: fields.get("icon").cloned(),
            callback: fields.get("callback").cloned(),
            permissions: permissions.clone(),
            account_id: Some(account.id.clone()),
            token: Some(raw_token.clone()),
            fetched: false,
            created_at_ms: now_ms,
            expires_at_ms: Some(now_ms + MIAUTH_APPROVAL_WINDOW_MS),
        },
    };
    state
        .services
        .auth
        .save_miauth_session(record)
        .map_err(ApiError::from_port_error)?;

    if let Some(callback) = fields.get("callback") {
        if is_safe_callback_url(callback) {
            let separator = if callback.contains('?') { '&' } else { '?' };
            let location = format!("{callback}{separator}session={session}");
            if let Ok(location) = location.parse::<HeaderValue>() {
                return Ok((
                    StatusCode::FOUND,
                    [(LOCATION, location)],
                    "Redirecting to the application…",
                )
                    .into_response());
            }
        }
    }

    // No (safe) callback: mint a first-party browser session and confirm.
    let browser_token = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&browser_token),
            account_id: account.id.clone(),
            permissions: Vec::new(),
            is_first_party: true,
            name: None,
            description: None,
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .map_err(ApiError::from_port_error)?;
    let mut response = Html(format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\" /><title>Authorized</title></head>\
        <body><h1>Authorization successful</h1>\
        <p>You have authorized this application. You can close this window and return to the app.</p></body></html>"
    ))
    .into_response();
    if let Ok(cookie) = session_cookie_value(&browser_token).parse::<HeaderValue>() {
        response.headers_mut().insert(SET_COOKIE, cookie);
    }
    Ok(response)
}

/// One-shot MiAuth session check (Misskey `ApiServerService.ts:157-177`):
/// the first call with an approved session returns the token + user,
/// every other call returns `{ "ok": false }`.
pub(crate) async fn miauth_check(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(session): Path<String>,
) -> Result<Response, ApiError> {
    // Slice 9 (H4): every check counts per client IP. Pending polls
    // (`ok:false`) are normal client behaviour, so they are not failures.
    let ip = client_ip(peer, &headers, &state.trusted_proxies);
    state
        .rate_limiter
        .check(&auth_ip_key(&ip), "auth/miauth-check")?;
    let consumed = state
        .services
        .auth
        .consume_miauth_session(&session)
        .map_err(ApiError::from_port_error)?;
    let approved = match consumed {
        Some(sess) => match (sess.token, sess.account_id) {
            (Some(token), Some(account_id)) => Some((token, account_id)),
            _ => None,
        },
        None => None,
    };
    match approved {
        Some((token, account_id)) => {
            let account = state
                .services
                .accounts
                .find_by_id(&account_id)
                .map_err(ApiError::from_port_error)?
                .ok_or_else(ApiError::authentication_failed)?;
            let user = pack_user_detailed_not_me(&account, None, &state.services)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(serde_json::json!({
                "ok": true,
                "token": token,
                "user": user,
            }))
            .into_response())
        }
        None => Ok(Json(serde_json::json!({ "ok": false })).into_response()),
    }
}
