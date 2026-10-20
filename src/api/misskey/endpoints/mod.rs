//! Misskey HTTP API surface (Slice D3).
//!
//! [`ApiState`] carries the application services, the in-process job
//! worker, and the rate limiter. [`build_router`] wires every Phase 1
//! route: `/healthz`, media/identicon file serving, the MiAuth web flow
//! (`GET`/`POST /miauth/{session}` plus the one-shot `check`
//! endpoints), web `signup`/`signin`, `drive/files/create` (multipart and
//! JSON), and the `POST /api/{endpoint}` JSON dispatcher.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::{
    body::{Body, Bytes},
    extract::{ConnectInfo, FromRequest, Multipart, Path, Query, Request, State},
    http::{
        header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE},
        HeaderMap, HeaderName, HeaderValue, StatusCode,
    },
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use http_body_util::{BodyExt, Limited};
use serde_json::{Map, Value};

use crate::activitypub::{actor as ap_actor, objects as ap_objects, webfinger as ap_webfinger};
use crate::application::commands::invites::SignupRejection;
use crate::application::commands::pins::FeaturedCache;
use crate::application::commands::AppServices;
use crate::application::ports::{
    AbuseReportFilter, ApiTokenRecord, CreateDraftInput, CreateNoteInput, EditNoteInput, ExportEntity, ImportEntity,
    InviteListFilter, InviteSort, InviteTypeFilter, LegacyAuthSessionRecord, MiAuthSessionRecord,
    ModerationLogFilter, PortError, TimelineCursor,
    UpdateAccountPatch, UpdateDraftPatch,
};
use crate::domain::account::{
    Account, AvatarDecoration, ChatScope, FollowVisibility, MutedWord,
    NotificationRecieveConfig, ProfileField,
};
use crate::domain::instance::InviteCode;
use crate::domain::moderation::{AbuseReportResolution, ModerationAction};
use crate::domain::note::{
    ReactionAcceptance, Visibility, MAX_CW_API_CHARS, MAX_NOTE_TEXT_CHARS,
};
use crate::domain::notification::NotificationKind;
use crate::federation::delivery::{
    fetch_and_upsert_remote_actor, fetch_remote_note, webfinger_discover_actor_uri,
};
use crate::federation::inbox as fed_inbox;
use crate::jobs::worker::JobWorker;
use crate::streaming::hub::Hub;
use crate::streaming::server::streaming_handler;

use super::auth::{
    authenticate, extract_raw_token, generate_random_token, hash_password_async, hash_token,
    is_safe_callback_url, needs_rehash, render_miauth_page, require_admin, require_moderator,
    verify_password_async, AuthenticatedCaller, SESSION_COOKIE_NAME,
};
use super::errors::ApiError;
use super::packers::{
    pack_abuse_report, pack_antenna, pack_blocking, pack_drive_file, pack_emoji_detailed, pack_emoji_simple,
    pack_following, pack_invite_code, pack_me_detailed,
    pack_meta, pack_moderation_log, pack_muting, pack_note, pack_note_draft,
    pack_note_reaction_with_note, pack_scheduled_note,
    pack_notification, pack_renote_muting, pack_user_detailed_not_me, pack_user_lite,
    pack_user_relation,
};
use super::request::check_allowed_fields;
use super::response::{RateLimiter, UploadSlots};

/// Shared API state: application services, the durable-job worker, the
/// Misskey rate limiter, the concurrent-upload slots, the live-streaming
/// broadcast hub, and the reverse proxies whose `X-Forwarded-For` is
/// honored ([`client_ip`]).
#[derive(Clone)]
pub struct ApiState {
    pub services: Arc<AppServices>,
    pub worker: Arc<JobWorker>,
    pub rate_limiter: Arc<RateLimiter>,
    pub upload_slots: Arc<UploadSlots>,
    pub hub: Hub,
    pub trusted_proxies: Arc<Vec<IpAddr>>,
    /// Timed featured-ranking cache (§7; 3600s TTL, one entry per scope
    /// key). Per-state so tests get isolated caches.
    pub featured_cache: Arc<FeaturedCache>,
}

/// Build the full Phase 1 Axum router.
///
/// Misskey client routes run with the full [`ApiState`]; the ActivityPub /
/// federation routes (WebFinger, actors, notes, inboxes) run with just
/// `Arc<AppServices>` so those adapters stay decoupled from the Misskey
/// adapter.
pub fn build_router(state: ApiState) -> Router {
    let services = state.services.clone();
    let federation_routes = Router::new()
        .route("/.well-known/webfinger", get(ap_webfinger::webfinger))
        .route("/.well-known/host-meta", get(ap_webfinger::host_meta_xrd))
        .route(
            "/.well-known/host-meta.json",
            get(ap_webfinger::host_meta_json),
        )
        .route(
            "/.well-known/nodeinfo",
            get(ap_webfinger::well_known_nodeinfo),
        )
        .route("/nodeinfo/2.0", get(ap_webfinger::nodeinfo_2_0))
        .route("/nodeinfo/2.1", get(ap_webfinger::nodeinfo_2_1))
        .route("/users/{id}", get(ap_actor::get_actor))
        .route(
            "/users/{id}/publickey",
            get(ap_actor::get_actor_public_key),
        )
        .route("/users/{id}/outbox", get(ap_actor::get_actor_outbox))
        .route(
            "/users/{id}/followers",
            get(ap_actor::get_actor_followers),
        )
        .route(
            "/users/{id}/following",
            get(ap_actor::get_actor_following),
        )
        .route(
            "/users/{id}/collections/featured",
            get(ap_actor::get_actor_featured),
        )
        .route("/@{acct}", get(ap_actor::get_at_profile))
        .route("/notes/{id}", get(ap_objects::get_note_ap))
        .route(
            "/notes/{id}/activity",
            get(ap_objects::get_note_activity_ap),
        )
        .route("/users/{id}/stamps/{stamp}", get(ap_objects::get_quote_stamp))
        .route("/inbox", post(fed_inbox::inbox_handler))
        .route("/users/{id}/inbox", post(fed_inbox::user_inbox_handler))
        .with_state(services);
    Router::new()
        .route("/healthz", get(healthz))
        .route("/files/{id}", get(serve_file))
        .route("/files/{id}/{name}", get(serve_file_named))
        .route("/identicon/{id}", get(identicon))
        .route("/miauth/{session}", get(miauth_page).post(miauth_submit))
        .route("/miauth/{session}/check", post(miauth_check))
        .route("/api/miauth/{session}/check", post(miauth_check))
        .route("/signup", post(signup))
        .route("/api/signup", post(signup))
        .route("/signin", post(signin))
        .route("/api/signin", post(signin))
        .route("/api/drive/files/create", post(drive_files_create))
        .route("/api/{*endpoint}", post(api_dispatch))
        .route("/streaming", get(streaming_handler))
        // No `DefaultBodyLimit::disable()`: the Misskey JSON dispatcher
        // enforces its own 1 MiB cap in `api_dispatch_blocking` (testable
        // without router layers), `drive/files/create` enforces its own
        // streaming cap in the handler, and everything else keeps axum's
        // 2 MiB default extraction limit.
        .with_state(state)
        .merge(federation_routes)
        // Slice 7 (H2): every response carries `nosniff`, including error
        // bodies and redirects.
        .layer(axum::middleware::from_fn(nosniff_middleware))
}

/// Slice 7 (H2): global `X-Content-Type-Options: nosniff` for all
/// responses, so a stored file can never be sniffed into active content.
async fn nosniff_middleware(req: Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(req).await;
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response
}

// --- Small helpers ---------------------------------------------------------

async fn healthz() -> &'static str {
    "ok"
}

fn session_cookie_value(raw_token: &str) -> String {
    format!("{SESSION_COOKIE_NAME}={raw_token}; HttpOnly; Path=/")
}

/// Client IP for rate limits and upload slots (slice 9, H4). The TCP peer is
/// the client unless it is in `trusted_proxies`. Only then is
/// `X-Forwarded-For` consulted: walk it right to left and take the first
/// address that is not a trusted proxy, so a client cannot prepend a fake
/// hop. An all-trusted chain uses its leftmost entry, and an unparseable
/// entry falls back to the peer. `X-Real-IP` and headers from untrusted
/// peers are ignored.
fn client_ip(peer: SocketAddr, headers: &HeaderMap, trusted_proxies: &[IpAddr]) -> String {
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

/// Parse a request body as a JSON object. An empty body defaults to `{}`.
/// Anything that is not a JSON object is `400 INVALID_PARAM`.
fn parse_json_body(body: &[u8]) -> Result<Map<String, Value>, ApiError> {
    if body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(Map::new());
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(ApiError::invalid_param(
            "body",
            "request body must be a JSON object",
        )),
    }
}

/// Resolve the caller from `Authorization: Bearer`, the JSON body `"i"`
/// parameter, or the session cookie. `Ok(None)` when no credential was
/// presented; a bad token is `401 AUTHENTICATION_FAILED`.
fn resolve_caller(
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

fn require_caller(caller: Option<AuthenticatedCaller>) -> Result<AuthenticatedCaller, ApiError> {
    caller.ok_or_else(ApiError::credential_required)
}

/// Rate-limit key: the caller's account id when authenticated, else the
/// client IP.
fn rate_key_for(caller: Option<&AuthenticatedCaller>, client_ip: &str) -> String {
    match caller {
        Some(caller) => caller.account.id.clone(),
        None => client_ip.to_string(),
    }
}

/// Slice 9 (H4): auth-throttle keys. Account keys use the lowercased
/// username as typed, so a failure is counted whether or not the account
/// exists. IP keys are the resolved client IP.
fn auth_account_key(username: &str) -> String {
    format!("acct:{}", username.to_lowercase())
}

fn auth_ip_key(client_ip: &str) -> String {
    format!("ip:{client_ip}")
}

/// Slice 9 (H3): verify `password` against the stored hash off the async
/// executor. On success, a legacy or outdated hash is replaced with a fresh
/// Argon2id hash through the existing password-update path.
async fn verify_and_upgrade_password(
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

/// Slice 6 (H1): maximum accepted Misskey JSON API body (1 MiB), enforced
/// in `api_dispatch_blocking` so oversized JSON answers 413 without
/// router layers. Multipart uploads are exempt: `drive/files/create`
/// enforces its own streaming cap instead.
pub const MAX_JSON_BODY_BYTES: usize = 1_048_576;

/// Multipart/JSON framing overhead allowed on top of `max_file_size` for
/// `drive/files/create` (1 MiB).
pub const UPLOAD_OVERHEAD_BYTES: usize = 1_048_576;

/// MiAuth + legacy auth approval window (Batch N WS7 hardening,
/// DIVERGENCE — upstream has no expiry on either session type). Sessions
/// older than this read as missing (deleted on read); 30 minutes covers
/// the approve-and-poll loop with margin.
pub const MIAUTH_APPROVAL_WINDOW_MS: i64 = 30 * 60 * 1000;

/// Upload body cap: `max_file_size` + multipart overhead.
fn upload_cap_bytes(services: &AppServices) -> usize {
    usize::try_from(services.max_file_size)
        .unwrap_or(usize::MAX)
        .saturating_add(UPLOAD_OVERHEAD_BYTES)
}

/// Read the whole request body with a streaming running cap: abort with
/// 413 `MAX_FILE_SIZE_EXCEEDED` as soon as the cap is crossed, without
/// ever holding more than ~cap bytes.
async fn read_capped_body(body: Body, cap: usize) -> Result<Bytes, ApiError> {
    Limited::new(body, cap)
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .map_err(|_| ApiError::max_file_size_exceeded())
}

fn required_str(obj: &Map<String, Value>, key: &str) -> Result<String, ApiError> {
    match obj.get(key) {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

fn opt_string(obj: &Map<String, Value>, key: &str) -> Result<Option<String>, ApiError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

fn opt_bool(
    obj: &Map<String, Value>,
    key: &str,
    default: bool,
) -> Result<bool, ApiError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => Err(ApiError::invalid_param(key, "must be a boolean")),
    }
}

fn opt_i64(obj: &Map<String, Value>, key: &str) -> Result<Option<i64>, ApiError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_i64()
            .map(Some)
            .ok_or_else(|| ApiError::invalid_param(key, "must be an integer")),
        Some(_) => Err(ApiError::invalid_param(key, "must be an integer")),
    }
}

fn opt_usize(obj: &Map<String, Value>, key: &str) -> Result<Option<usize>, ApiError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(|v| v as usize)
            .map(Some)
            .ok_or_else(|| ApiError::invalid_param(key, "must be an integer")),
        Some(_) => Err(ApiError::invalid_param(key, "must be an integer")),
    }
}

fn get_limit(obj: &Map<String, Value>) -> Result<usize, ApiError> {
    match opt_usize(obj, "limit")? {
        None => Ok(10),
        Some(limit) if (1..=100).contains(&limit) => Ok(limit),
        Some(_) => Err(ApiError::invalid_param(
            "limit",
            "must be an integer between 1 and 100",
        )),
    }
}

fn str_list(obj: &Map<String, Value>, key: &str) -> Result<Vec<String>, ApiError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    _ => return Err(ApiError::invalid_param(key, "must be an array of strings")),
                }
            }
            Ok(out)
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be an array of strings")),
    }
}

/// Parse a `notes/create` `poll` param (Misskey `notes/create.ts:168-184`
/// parity): `{ choices[2..10] unique 1..50, multiple?, expiresAt?,
/// expiredAfter? }`. `null`/absent → `None`. Choice-shape violations are
/// `INVALID_PARAM`; an already-past expiry is the exact
/// `CANNOT_CREATE_ALREADY_EXPIRED_POLL` ident (surfaced through
/// `ApiError::from_port_error` so the UUID matches `create.ts`).
fn parse_poll_param(
    obj: &Map<String, Value>,
    key: &str,
    now_ms: i64,
) -> Result<Option<crate::domain::poll::PollInput>, ApiError> {
    let raw = match obj.get(key) {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::Object(map)) => map,
        Some(_) => return Err(ApiError::invalid_param(key, "must be an object")),
    };
    for field in raw.keys() {
        if !matches!(field.as_str(), "choices" | "multiple" | "expiresAt" | "expiredAfter") {
            return Err(ApiError::invalid_param(
                key,
                &format!("unknown field: {field}"),
            ));
        }
    }
    let choices = match raw.get("choices") {
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    _ => {
                        return Err(ApiError::invalid_param(
                            key,
                            "choices must be an array of strings",
                        ));
                    }
                }
            }
            out
        }
        _ => {
            return Err(ApiError::invalid_param(
                key,
                "choices is required and must be an array of strings",
            ));
        }
    };
    let multiple = match raw.get("multiple") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => return Err(ApiError::invalid_param(key, "multiple must be a boolean")),
    };
    let expires_at_ms = match raw.get("expiresAt") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) => Some(
            n.as_i64()
                .ok_or_else(|| ApiError::invalid_param(key, "expiresAt must be an integer"))?,
        ),
        Some(_) => return Err(ApiError::invalid_param(key, "expiresAt must be an integer")),
    };
    let expired_after_ms = match raw.get("expiredAfter") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| ApiError::invalid_param(key, "expiredAfter must be an integer"))?;
            if v < 1 {
                return Err(ApiError::invalid_param(
                    key,
                    "expiredAfter must be at least 1",
                ));
            }
            Some(v)
        }
        Some(_) => return Err(ApiError::invalid_param(key, "expiredAfter must be an integer")),
    };
    // `expiredAfter` wins over `expiresAt` (Misskey `create.ts:230` parity).
    let expires_at = match (expired_after_ms, expires_at_ms) {
        (Some(after), _) => Some(now_ms.saturating_add(after)),
        (None, at) => at,
    };
    let input = crate::domain::poll::PollInput {
        choices,
        multiple,
        expires_at_ms: expires_at,
    };
    input.validate(now_ms).map_err(|e| match e {
        crate::domain::id::DomainError::Validation(msg)
            if msg == "CANNOT_CREATE_ALREADY_EXPIRED_POLL" =>
        {
            ApiError::from_port_error(crate::application::ports::PortError::Validation(msg))
        }
        crate::domain::id::DomainError::Validation(msg) => {
            ApiError::invalid_param(key, &msg)
        }
        other => ApiError::invalid_param(key, &format!("{other:?}")),
    })?;
    Ok(Some(input))
}

/// Parsed draft `poll` columns (§5; Misskey `notes/drafts/create|update.ts`
/// `paramDef` parity): choices may be empty (relaxed draft rules —
/// `minItems: 0`), each 1..=50 chars, at most 10, unique. `null`/absent →
///
/// `None` (no poll content in the patch). Shape violations are
/// `INVALID_PARAM`; expiry-vs-now is service-level (the exact
/// `CANNOT_CREATE_ALREADY_EXPIRED_POLL` ident) so both endpoints share it.
struct DraftPollParam {
    choices: Vec<String>,
    multiple: Option<bool>,
    expires_at_ms: Option<Option<i64>>,
    expired_after_ms: Option<Option<i64>>,
}

fn parse_draft_poll_param(obj: &Map<String, Value>) -> Result<Option<DraftPollParam>, ApiError> {
    let raw = match obj.get("poll") {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::Object(map)) => map,
        Some(_) => return Err(ApiError::invalid_param("poll", "must be an object")),
    };
    for field in raw.keys() {
        if !matches!(field.as_str(), "choices" | "multiple" | "expiresAt" | "expiredAfter") {
            return Err(ApiError::invalid_param(
                "poll",
                &format!("unknown field: {field}"),
            ));
        }
    }
    let choices = match raw.get("choices") {
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    _ => {
                        return Err(ApiError::invalid_param(
                            "poll",
                            "choices must be an array of strings",
                        ));
                    }
                }
            }
            out
        }
        _ => {
            return Err(ApiError::invalid_param(
                "poll",
                "choices is required and must be an array of strings",
            ));
        }
    };
    if choices.len() > 10 {
        return Err(ApiError::invalid_param("poll", "choices must have at most 10 items"));
    }
    {
        let mut seen = std::collections::HashSet::new();
        if !choices.iter().all(|c| seen.insert(c)) {
            return Err(ApiError::invalid_param("poll", "choices must be unique"));
        }
    }
    for choice in &choices {
        let n = choice.chars().count();
        if n == 0 || n > 50 {
            return Err(ApiError::invalid_param(
                "poll",
                "choices must be 1..=50 chars",
            ));
        }
    }
    let multiple = match raw.get("multiple") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => return Err(ApiError::invalid_param("poll", "multiple must be a boolean")),
    };
    let expires_at_ms = match raw.get("expiresAt") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::Number(n)) => Some(Some(
            n.as_i64()
                .ok_or_else(|| ApiError::invalid_param("poll", "expiresAt must be an integer"))?,
        )),
        Some(_) => return Err(ApiError::invalid_param("poll", "expiresAt must be an integer")),
    };
    let expired_after_ms = match raw.get("expiredAfter") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::Number(n)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| ApiError::invalid_param("poll", "expiredAfter must be an integer"))?;
            if v < 1 {
                return Err(ApiError::invalid_param(
                    "poll",
                    "expiredAfter must be at least 1",
                ));
            }
            Some(Some(v))
        }
        Some(_) => {
            return Err(ApiError::invalid_param("poll", "expiredAfter must be an integer"));
        }
    };
    Ok(Some(DraftPollParam {
        choices,
        multiple,
        expires_at_ms,
        expired_after_ms,
    }))
}

/// Draft text (`minLength: 0, maxLength: 3000, nullable` parity): empty is
/// allowed, overlong is `INVALID_PARAM`.
fn opt_draft_text(obj: &Map<String, Value>, key: &str) -> Result<Option<String>, ApiError> {
    match opt_string(obj, key)? {
        Some(text) if text.chars().count() > 3000 => Err(ApiError::invalid_param(
            key,
            "must be at most 3000 chars",
        )),
        text => Ok(text),
    }
}

/// Draft cw (`minLength: 1, maxLength: 100, nullable` parity).
fn opt_draft_cw(obj: &Map<String, Value>) -> Result<Option<String>, ApiError> {
    match opt_string(obj, "cw")? {
        Some(cw) => {
            let n = cw.chars().count();
            if n == 0 || n > 100 {
                return Err(ApiError::invalid_param("cw", "must be 1..=100 chars"));
            }
            Ok(Some(cw))
        }
        None => Ok(None),
    }
}

/// Draft hashtag (`maxLength: 200, nullable` parity).
fn opt_draft_hashtag(obj: &Map<String, Value>) -> Result<Option<String>, ApiError> {
    match opt_string(obj, "hashtag")? {
        Some(tag) if tag.chars().count() > 200 => Err(ApiError::invalid_param(
            "hashtag",
            "must be at most 200 chars",
        )),
        tag => Ok(tag),
    }
}

/// Draft `fileIds` (`minItems: 0, maxItems: 16, uniqueItems` parity).
/// The `key`-parameterized form serves the Sharkey schedule shim, whose
/// `mediaIds` key carries the same shape (`fileIds` wins when both are
/// present — upstream `fileIds ?? mediaIds` parity).
fn draft_file_ids_for_key(obj: &Map<String, Value>, key: &str) -> Result<Vec<String>, ApiError> {
    let ids = str_list(obj, key)?;
    if ids.len() > 16 {
        return Err(ApiError::invalid_param(key, "must have at most 16 items"));
    }
    let mut seen = std::collections::HashSet::new();
    if !ids.iter().all(|id| seen.insert(id)) {
        return Err(ApiError::invalid_param(key, "must be unique"));
    }
    Ok(ids)
}

fn draft_file_ids(obj: &Map<String, Value>) -> Result<Vec<String>, ApiError> {
    draft_file_ids_for_key(obj, "fileIds")
}

/// Draft `visibleUserIds` (`uniqueItems` parity).
fn draft_visible_user_ids(obj: &Map<String, Value>) -> Result<Vec<String>, ApiError> {
    let ids = str_list(obj, "visibleUserIds")?;
    let mut seen = std::collections::HashSet::new();
    if !ids.iter().all(|id| seen.insert(id)) {
        return Err(ApiError::invalid_param(
            "visibleUserIds",
            "must be unique",
        ));
    }
    Ok(ids)
}

/// Build a [`CreateDraftInput`] from a `notes/drafts/create` body.
/// `channelId` must be null (channels do not exist; a non-null channel is
/// `NO_SUCH_CHANNEL`, the upstream existence-check outcome for an unknown
/// channel).
fn parse_draft_create(obj: &Map<String, Value>) -> Result<CreateDraftInput, ApiError> {
    if let Some(channel) = obj.get("channelId") {
        if !channel.is_null() {
            return Err(ApiError::from_port_error(PortError::NotFound(
                "NO_SUCH_CHANNEL".to_string(),
            )));
        }
    }
    let poll = parse_draft_poll_param(obj)?;
    Ok(CreateDraftInput {
        text: opt_draft_text(obj, "text")?,
        cw: opt_draft_cw(obj)?,
        visibility: parse_visibility(obj.get("visibility"))?,
        visible_users: draft_visible_user_ids(obj)?,
        local_only: opt_bool(obj, "localOnly", false)?,
        reply_to: opt_string(obj, "replyId")?,
        renote_of: opt_string(obj, "renoteId")?,
        file_ids: draft_file_ids(obj)?,
        hashtag: opt_draft_hashtag(obj)?,
        reaction_acceptance: parse_reaction_acceptance(obj.get("reactionAcceptance"))?,
        has_poll: poll.is_some(),
        poll_choices: poll.as_ref().map(|p| p.choices.clone()).unwrap_or_default(),
        poll_multiple: poll.as_ref().and_then(|p| p.multiple).unwrap_or(false),
        poll_expires_at_ms: poll.as_ref().and_then(|p| p.expires_at_ms).flatten(),
        poll_expired_after_ms: poll.as_ref().and_then(|p| p.expired_after_ms).flatten(),
        scheduled_at_ms: opt_i64(obj, "scheduledAt")?,
        is_actually_scheduled: opt_bool(obj, "isActuallyScheduled", false)?,
    })
}

/// Nullable-string patch field: absent → `None` (unchanged), `null` →
/// `Some(None)` (clear), string → `Some(Some(_))`.
fn patch_nullable_string(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Option<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(s)) => Ok(Some(Some(s.clone()))),
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

/// Nullable-bool patch field.
fn patch_nullable_bool(obj: &Map<String, Value>, key: &str) -> Result<Option<bool>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(ApiError::invalid_param(key, "must be a boolean")),
    }
}

/// Nullable-integer patch field: absent → `None`, `null` → `Some(None)`,
/// integer → `Some(Some(_))`.
fn patch_nullable_i64(obj: &Map<String, Value>, key: &str) -> Result<Option<Option<i64>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::Number(n)) => Ok(Some(Some(
            n.as_i64()
                .ok_or_else(|| ApiError::invalid_param(key, "must be an integer"))?,
        ))),
        Some(_) => Err(ApiError::invalid_param(key, "must be an integer")),
    }
}

/// Build an [`UpdateDraftPatch`] from a `notes/drafts/update` body.
/// Every field is optional; `poll: null`/absent leaves poll columns
/// untouched (upstream `ps.poll?.choices` parity), while a `poll` object
/// replaces the poll columns wholesale (absent sub-fields clear, mirroring
/// the upstream `.set(data)` with `null` defaults).
fn parse_draft_update(obj: &Map<String, Value>) -> Result<UpdateDraftPatch, ApiError> {
    if let Some(channel) = obj.get("channelId") {
        if !channel.is_null() {
            return Err(ApiError::from_port_error(PortError::NotFound(
                "NO_SUCH_CHANNEL".to_string(),
            )));
        }
    }
    let poll = parse_draft_poll_param(obj)?;
    let text = match patch_nullable_string(obj, "text")? {
        Some(Some(text)) if text.chars().count() > 3000 => {
            return Err(ApiError::invalid_param("text", "must be at most 3000 chars"));
        }
        text => text,
    };
    let cw = match patch_nullable_string(obj, "cw")? {
        Some(Some(cw)) => {
            let n = cw.chars().count();
            if n == 0 || n > 100 {
                return Err(ApiError::invalid_param("cw", "must be 1..=100 chars"));
            }
            Some(Some(cw))
        }
        other => other,
    };
    let hashtag = match patch_nullable_string(obj, "hashtag")? {
        Some(Some(tag)) if tag.chars().count() > 200 => {
            return Err(ApiError::invalid_param("hashtag", "must be at most 200 chars"));
        }
        tag => tag,
    };
    let file_ids = match obj.get("fileIds") {
        None | Some(Value::Null) => None,
        Some(Value::Array(_)) => Some(draft_file_ids(obj)?),
        Some(_) => return Err(ApiError::invalid_param("fileIds", "must be an array of strings")),
    };
    let visible_users = match obj.get("visibleUserIds") {
        None | Some(Value::Null) => None,
        Some(Value::Array(_)) => Some(draft_visible_user_ids(obj)?),
        Some(_) => {
            return Err(ApiError::invalid_param(
                "visibleUserIds",
                "must be an array of strings",
            ));
        }
    };
    let visibility = match obj.get("visibility") {
        None | Some(Value::Null) => None,
        Some(_) => Some(parse_visibility(obj.get("visibility"))?),
    };
    let reaction_acceptance = match obj.get("reactionAcceptance") {
        None => None,
        Some(_) => Some(parse_reaction_acceptance(obj.get("reactionAcceptance"))?),
    };
    Ok(UpdateDraftPatch {
        text,
        cw,
        visibility,
        visible_users,
        local_only: patch_nullable_bool(obj, "localOnly")?,
        reply_to: patch_nullable_string(obj, "replyId")?,
        renote_of: patch_nullable_string(obj, "renoteId")?,
        file_ids,
        hashtag,
        reaction_acceptance,
        poll_choices: poll.as_ref().map(|p| p.choices.clone()),
        poll_multiple: poll.as_ref().and_then(|p| p.multiple),
        poll_expires_at_ms: poll.as_ref().and_then(|p| p.expires_at_ms),
        poll_expired_after_ms: poll.as_ref().and_then(|p| p.expired_after_ms),
        scheduled_at_ms: patch_nullable_i64(obj, "scheduledAt")?,
        is_actually_scheduled: patch_nullable_bool(obj, "isActuallyScheduled")?,
    })
}

/// Split a permission parameter (array or comma-separated string) into a
/// permission list.
fn parse_permissions(value: Option<&Value>) -> Vec<String> {
    match value {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(s)) => s
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(|p| p.to_string())
            .collect(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(|p| p.to_string())
            .collect(),
        Some(_) => Vec::new(),
    }
}

fn parse_visibility(value: Option<&Value>) -> Result<Visibility, ApiError> {
    match value {
        None | Some(Value::Null) => Ok(Visibility::Public),
        Some(Value::String(s)) => match s.as_str() {
            "public" => Ok(Visibility::Public),
            "home" => Ok(Visibility::Home),
            "followers" => Ok(Visibility::Followers),
            "specified" => Ok(Visibility::Specified),
            _ => Err(ApiError::invalid_param(
                "visibility",
                "must be public, home, followers, or specified",
            )),
        },
        Some(_) => Err(ApiError::invalid_param(
            "visibility",
            "must be public, home, followers, or specified",
        )),
    }
}

fn parse_reaction_acceptance(
    value: Option<&Value>,
) -> Result<Option<ReactionAcceptance>, ApiError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(_)) => serde_json::from_value::<ReactionAcceptance>(
            value.expect("matched Some(Value::String)").clone(),
        )
        .map(Some)
        .map_err(|_| {
            ApiError::invalid_param(
                "reactionAcceptance",
                "must be likeOnly, likeOnlyForRemote, nonSensitiveOnly, or nonSensitiveOnlyForLocalLikeOnlyForRemote",
            )
        }),
        Some(_) => Err(ApiError::invalid_param(
            "reactionAcceptance",
            "must be a string",
        )),
    }
}

fn parse_cursor(obj: &Map<String, Value>) -> Result<TimelineCursor, ApiError> {
    Ok(TimelineCursor {
        since_id: opt_string(obj, "sinceId")?,
        until_id: opt_string(obj, "untilId")?,
        since_date: opt_i64(obj, "sinceDate")?,
        until_date: opt_i64(obj, "untilDate")?,
        limit: get_limit(obj)?,
    })
}

/// Map an `AppServices::check_import_file` failure to the caller's
/// per-endpoint verbatim upstream error (`i/import-*.ts` parity: each
/// endpoint mints its own UUID per code).
fn map_import_file_error(err: PortError, endpoint: &str) -> ApiError {
    match err {
        PortError::NotFound(_) => ApiError::import_no_such_file(endpoint),
        PortError::Validation(code) if code == "EMPTY_FILE" => {
            ApiError::import_empty_file(endpoint)
        }
        PortError::Validation(code) if code == "TOO_BIG_FILE" => {
            ApiError::import_too_big_file(endpoint)
        }
        other => ApiError::from_port_error(other),
    }
}

/// Cursor paging for the block/mute list endpoints (Misskey parity: `limit`
/// defaults to 30, range 1–100).
fn parse_list_cursor(obj: &Map<String, Value>) -> Result<TimelineCursor, ApiError> {
    let limit = match opt_usize(obj, "limit")? {
        None => 30,
        Some(limit) if (1..=100).contains(&limit) => limit,
        Some(_) => {
            return Err(ApiError::invalid_param(
                "limit",
                "must be an integer between 1 and 100",
            ));
        }
    };
    Ok(TimelineCursor {
        since_id: opt_string(obj, "sinceId")?,
        until_id: opt_string(obj, "untilId")?,
        since_date: opt_i64(obj, "sinceDate")?,
        until_date: opt_i64(obj, "untilDate")?,
        limit,
    })
}

/// Misskey notification types (misskey `types.ts` `notificationTypes`) that
/// are valid `i/notifications` `includeTypes`/`excludeTypes` entries but can
/// never match a local row. The `reaction:grouped` / `renote:grouped`
/// pseudo-types are valid entries too (§10): like upstream (whose DB query
/// only matches stored rows), including only a grouped type matches nothing
/// and excluding one excludes nothing. (`exportCompleted` left this list in
/// §13: exports now emit it.)
fn is_unemitted_notification_type(name: &str) -> bool {
    matches!(
        name,
        "note"
            | "pollEnded"
            | "reaction:grouped"
            | "renote:grouped"
            | "roleAssigned"
            | "chatRoomInvitationReceived"
            | "achievementEarned"
            | "login"
            | "createToken"
            | "app"
            | "test"
    )
}

/// Misskey obsolete notification types (`types.ts`
/// `obsoleteNotificationTypes`): accepted for backwards compatibility and
/// silently dropped from filters.
fn is_obsolete_notification_type(name: &str) -> bool {
    matches!(name, "pollVote" | "groupInvited")
}

/// Map one `includeTypes`/`excludeTypes` entry to the local kind.
/// `Ok(None)` = valid but locally unemitted (matches nothing); obsolete
/// types never reach here (the caller drops them first).
fn notification_kind_for(field: &str, name: &str) -> Result<Option<NotificationKind>, ApiError> {
    match name {
        "follow" => Ok(Some(NotificationKind::Follow)),
        "mention" => Ok(Some(NotificationKind::Mention)),
        "reply" => Ok(Some(NotificationKind::Reply)),
        // Quotes notify as `Renote` locally (no separate quote kind), so
        // both Misskey types select renote notifications.
        "renote" | "quote" => Ok(Some(NotificationKind::Renote)),
        // Sharkey `edited` notification (a `notes/edit` addition —
        // Misskey `notificationTypes` has no such entry).
        "edited" => Ok(Some(NotificationKind::Edited)),
        "reaction" => Ok(Some(NotificationKind::Reaction)),
        "receiveFollowRequest" => Ok(Some(NotificationKind::ReceiveFollowRequest)),
        // Batch N WS3: locked-account accepts now emit this.
        "followRequestAccepted" => Ok(Some(NotificationKind::FollowRequestAccepted)),
        // §5: scheduling notifications are now emitted locally.
        "scheduledNotePosted" => Ok(Some(NotificationKind::ScheduledNotePosted)),
        "scheduledNotePostFailed" => Ok(Some(NotificationKind::ScheduledNotePostFailed)),
        // §13: export completions are now emitted locally.
        "exportCompleted" => Ok(Some(NotificationKind::ExportCompleted)),
        _ if is_unemitted_notification_type(name) => Ok(None),
        _ => Err(ApiError::invalid_param(
            field,
            &format!("unknown notification type: {name}"),
        )),
    }
}

/// Every [`NotificationKind`] the local store can emit, for the
/// exclude-everything early return (`i/notifications` Misskey parity).
const ALL_NOTIFICATION_KINDS: [NotificationKind; 11] = [
    NotificationKind::Follow,
    NotificationKind::ReceiveFollowRequest,
    NotificationKind::FollowRequestAccepted,
    NotificationKind::Mention,
    NotificationKind::Reply,
    NotificationKind::Renote,
    NotificationKind::Reaction,
    NotificationKind::ScheduledNotePosted,
    NotificationKind::ScheduledNotePostFailed,
    NotificationKind::Edited,
    NotificationKind::ExportCompleted,
];

/// Shared query params of `i/notifications` and `i/notifications-grouped`
/// (§10; upstream `paramDef` is identical on both endpoints).
struct NotificationQuery {
    cursor: TimelineCursor,
    mark_as_read: bool,
    include: Vec<NotificationKind>,
    exclude: Vec<NotificationKind>,
    matches_nothing: bool,
}

/// Parse the shared notification-list params: cursor + `markAsRead` +
/// kind allow/deny lists (Misskey `notifications.ts` parity — presence,
/// not emptiness, of `includeTypes` decides filtering; an explicitly
/// empty or never-matching allow-list, or denying every emittable kind,
/// matches nothing without touching the store).
fn parse_notification_query(obj: &Map<String, Value>) -> Result<NotificationQuery, ApiError> {
    let cursor = parse_cursor(obj)?;
    let mark_as_read = opt_bool(obj, "markAsRead", true)?;
    let include_requested = obj.contains_key("includeTypes");
    let mut include: Vec<NotificationKind> = Vec::new();
    for name in str_list(obj, "includeTypes")? {
        if is_obsolete_notification_type(&name) {
            continue;
        }
        if let Some(kind) = notification_kind_for("includeTypes", &name)? {
            if !include.contains(&kind) {
                include.push(kind);
            }
        }
    }
    let mut exclude: Vec<NotificationKind> = Vec::new();
    for name in str_list(obj, "excludeTypes")? {
        if is_obsolete_notification_type(&name) {
            continue;
        }
        if let Some(kind) = notification_kind_for("excludeTypes", &name)? {
            if !exclude.contains(&kind) {
                exclude.push(kind);
            }
        }
    }
    let matches_nothing = (include_requested && include.is_empty())
        || (!exclude.is_empty()
            && ALL_NOTIFICATION_KINDS
                .iter()
                .all(|kind| exclude.contains(kind)));
    Ok(NotificationQuery {
        cursor,
        mark_as_read,
        include,
        exclude,
        matches_nothing,
    })
}

/// Pack `account` for `viewer`: the caller's own account packs as
/// `MeDetailed`, everyone else as `UserDetailedNotMe`.
fn pack_account_for(
    account: &Account,
    viewer: Option<&AuthenticatedCaller>,
    services: &AppServices,
) -> Result<Value, ApiError> {
    if let Some(caller) = viewer {
        if caller.account.id == account.id {
            return pack_me_detailed(account, caller.is_first_party, services)
                .map_err(ApiError::from_port_error);
        }
    }
    let viewer_id = viewer.map(|caller| caller.account.id.as_str());
    pack_user_detailed_not_me(account, viewer_id, services).map_err(ApiError::from_port_error)
}

// --- Media / identicon -----------------------------------------------------

async fn serve_file(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let meta = state
        .services
        .media
        .get(&id)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_FILE".to_string()))
        })?;
    let bytes = state
        .services
        .blobs
        .get(&meta.blob_key)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(ApiError::internal)?;
    let mut response = bytes.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        meta.mime
            .parse::<HeaderValue>()
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    apply_media_security_headers(&mut response, &meta.mime);
    Ok(response)
}

/// Inline-safe MIME set (slice 7, H2): sniffed `image/*` (except SVG —
/// the sniffer never emits it, and legacy `image/svg+xml` rows must not
/// render inline), `audio/*` and `video/*` render inline. Everything else —
/// `application/octet-stream`, `text/plain`, and any legacy row outside the
/// allowlist (e.g. stored `application/pdf`) — is served as an attachment
/// so it can never become active content in the site origin.
fn is_inline_safe_mime(mime: &str) -> bool {
    (mime.starts_with("image/") && mime != "image/svg+xml" && !mime.ends_with("+xml"))
        || mime.starts_with("audio/")
        || mime.starts_with("video/")
}

/// Slice 7 (H2): every served file carries `nosniff` plus a
/// `default-src 'none'; sandbox` CSP, and non-inline-safe files carry
/// `Content-Disposition: attachment`.
fn apply_media_security_headers(response: &mut Response, mime: &str) {
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'; sandbox"),
    );
    if !is_inline_safe_mime(mime) {
        response.headers_mut().insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment"),
        );
    }
}

/// Misskey `FileServerService.ts:93-99` redirects `/files/{id}/{name}` to
/// `/files/{id}`.
async fn serve_file_named(Path((id, _name)): Path<(String, String)>) -> Response {
    (
        StatusCode::MOVED_PERMANENTLY,
        [(LOCATION, format!("/files/{id}"))],
        "",
    )
        .into_response()
}

/// Tiny deterministic SVG identicon so avatar URLs always resolve. The
/// bytes are fully server-generated (SHA-256-derived hex colors and
/// coordinates; no user input, no script elements), and the response carries
/// the same sandboxed CSP as served files, so even a direct navigation
/// cannot execute script.
async fn identicon(Path(id): Path<String>) -> Response {
    let mut response = identicon_svg(&id).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("image/svg+xml"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'; sandbox"),
    );
    response
}

fn identicon_svg(id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(id.as_bytes());
    let color = format!(
        "#{:02x}{:02x}{:02x}",
        digest[0], digest[1], digest[2]
    );
    let cell = 20;
    let mut rects = String::new();
    for row in 0..5usize {
        for col in 0..3usize {
            if (digest[3 + row] >> col) & 1 == 1 {
                let left = col * cell;
                let right = (4 - col) * cell;
                rects.push_str(&format!(
                    "<rect x=\"{left}\" y=\"{y}\" width=\"{cell}\" height=\"{cell}\" fill=\"{color}\"/>",
                    y = row * cell,
                ));
                if right != left {
                    rects.push_str(&format!(
                        "<rect x=\"{right}\" y=\"{y}\" width=\"{cell}\" height=\"{cell}\" fill=\"{color}\"/>",
                        y = row * cell,
                    ));
                }
            }
        }
    }
    format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 100 100\"><rect width=\"100\" height=\"100\" fill=\"#f0f0f0\"/>{rects}</svg>"
    )
}

// --- MiAuth web flow -------------------------------------------------------

async fn miauth_page(
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

fn hex_val(byte: Option<&u8>) -> Option<u8> {
    match byte {
        Some(b'0'..=b'9') => Some(byte.expect("matched") - b'0'),
        Some(b'a'..=b'f') => Some(byte.expect("matched") - b'a' + 10),
        Some(b'A'..=b'F') => Some(byte.expect("matched") - b'A' + 10),
        _ => None,
    }
}

fn pct_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                match (hex_val(bytes.get(i + 1)), hex_val(bytes.get(i + 2))) {
                    (Some(hi), Some(lo)) => {
                        out.push(hi << 4 | lo);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_form(body: &str) -> HashMap<String, String> {
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

fn json_to_string_map(value: &Value) -> HashMap<String, String> {
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

fn is_truthy(value: Option<&str>) -> bool {
    matches!(value, Some("true") | Some("1") | Some("on") | Some("yes"))
}

fn miauth_error_page(
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

async fn miauth_submit(
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
        state
            .rate_limiter
            .check(&auth_ip_key(&ip), "auth/signup")?;
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
        response
            .headers_mut()
            .insert(SET_COOKIE, cookie);
    }
    Ok(response)
}

/// One-shot MiAuth session check (Misskey `ApiServerService.ts:157-177`):
/// the first call with an approved session returns the token + user,
/// every other call returns `{ "ok": false }`.
async fn miauth_check(
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
        None => {
            Ok(Json(serde_json::json!({ "ok": false })).into_response())
        }
    }
}

/// `limit` with an upstream default other than the 10-row moderation-log
/// default (the invite listings default to 30 upstream).
/// Batch N WS4: antenna param helpers (upstream `antennas/*` `paramDef`
/// parity: required-with-bounds strings, required string arrays, the `src`
/// enum, optional-with-bounds strings, optional arrays, optional bools, and
/// nullable strings). Missing/wrong-typed params answer `INVALID_PARAM`
/// naming the field.
fn required_string(obj: &Map<String, Value>, key: &str, min: usize, max: usize) -> Result<String, ApiError> {
    match obj.get(key) {
        Some(Value::String(s)) => {
            let n = s.chars().count();
            if n < min || n > max {
                return Err(ApiError::invalid_param(key, &format!("must be {min}..={max} chars")));
            }
            Ok(s.clone())
        }
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

fn opt_bounded_string(obj: &Map<String, Value>, key: &str, min: usize, max: usize) -> Result<Option<String>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => {
            let n = s.chars().count();
            if n < min || n > max {
                return Err(ApiError::invalid_param(key, &format!("must be {min}..={max} chars")));
            }
            Ok(Some(s.clone()))
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

fn opt_nullable_string(obj: &Map<String, Value>, key: &str) -> Result<Option<Option<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(s)) => Ok(Some(Some(s.clone()))),
        Some(_) => Err(ApiError::invalid_param(key, "must be a string or null")),
    }
}

fn required_antenna_src(obj: &Map<String, Value>) -> Result<crate::domain::antenna::AntennaSrc, ApiError> {
    match obj.get("src").and_then(Value::as_str) {
        Some(s) => crate::domain::antenna::AntennaSrc::parse(s)
            .ok_or_else(|| ApiError::invalid_param("src", "must be one of home, all, users, list, users_blacklist")),
        None => Err(ApiError::invalid_param("src", "this field is required")),
    }
}

fn opt_antenna_src(obj: &Map<String, Value>) -> Result<Option<crate::domain::antenna::AntennaSrc>, ApiError> {
    match obj.get("src") {
        None => Ok(None),
        Some(Value::String(s)) => crate::domain::antenna::AntennaSrc::parse(s)
            .map(Some)
            .ok_or_else(|| ApiError::invalid_param("src", "must be one of home, all, users, list, users_blacklist")),
        Some(_) => Err(ApiError::invalid_param("src", "must be a string")),
    }
}

fn required_str_array(obj: &Map<String, Value>, key: &str) -> Result<Vec<String>, ApiError> {
    match obj.get(key) {
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    _ => return Err(ApiError::invalid_param(key, "must be an array of strings")),
                }
            }
            Ok(out)
        }
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

fn opt_str_array(obj: &Map<String, Value>, key: &str) -> Result<Option<Vec<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    _ => return Err(ApiError::invalid_param(key, "must be an array of strings")),
                }
            }
            Ok(Some(out))
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be an array of strings")),
    }
}

fn required_str_array_array(obj: &Map<String, Value>, key: &str) -> Result<Vec<Vec<String>>, ApiError> {
    match obj.get(key) {
        Some(Value::Array(groups)) => {
            let mut out = Vec::with_capacity(groups.len());
            for group in groups {
                match group {
                    Value::Array(items) => {
                        let mut inner = Vec::with_capacity(items.len());
                        for item in items {
                            match item {
                                Value::String(s) => inner.push(s.clone()),
                                _ => return Err(ApiError::invalid_param(key, "must be an array of string arrays")),
                            }
                        }
                        out.push(inner);
                    }
                    _ => return Err(ApiError::invalid_param(key, "must be an array of string arrays")),
                }
            }
            Ok(out)
        }
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

fn opt_str_array_array(obj: &Map<String, Value>, key: &str) -> Result<Option<Vec<Vec<String>>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(_) => required_str_array_array(obj, key).map(Some),
    }
}

fn required_bool(obj: &Map<String, Value>, key: &str) -> Result<bool, ApiError> {
    match obj.get(key) {
        Some(Value::Bool(b)) => Ok(*b),
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

fn opt_bool_opt(obj: &Map<String, Value>, key: &str) -> Result<Option<bool>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(ApiError::invalid_param(key, "must be a boolean")),
    }
}

fn get_limit_with_default(
    obj: &Map<String, Value>,
    default: usize,
) -> Result<usize, ApiError> {
    match opt_usize(obj, "limit")? {
        None => Ok(default),
        Some(limit) if (1..=100).contains(&limit) => Ok(limit),
        Some(_) => Err(ApiError::invalid_param(
            "limit",
            "must be an integer between 1 and 100",
        )),
    }
}

/// Packed `InviteCode[]` for the invite listings (resolves `createdBy`/
/// `usedBy` through the account store, upstream `InviteCodeEntityService.
/// packMany`).
fn pack_invite_codes(state: &ApiState, invites: &[InviteCode]) -> Result<Value, ApiError> {
    let mut packed = Vec::with_capacity(invites.len());
    for invite in invites {
        let (created_by, used_by) = state.services.invite_fields(invite);
        packed.push(pack_invite_code(
            &state.services,
            invite,
            created_by.as_ref(),
            used_by.as_ref(),
        ));
    }
    Ok(Value::Array(packed))
}

/// `admin/invite/create` `expiresAt` (upstream parses any `Date.parse`-
/// acceptable string and answers `INVALID_DATE_TIME` when it does not).
fn parse_invite_expires_at(raw: &str) -> Result<i64, ApiError> {
    let invalid = || {
        ApiError::from_port_error(PortError::Validation(
            "INVALID_DATE_TIME".to_string(),
        ))
    };
    let trimmed = raw.trim();
    chrono::DateTime::parse_from_rfc3339(trimmed)
        .map(|dt| dt.timestamp_millis())
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f")
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f")
                })
                .map(|dt| dt.and_utc().timestamp_millis())
        })
        .map_err(|_| invalid())
}

// --- Web signup / signin ---------------------------------------------------

/// Signup failure. `Rejected` answers plain HTTP 400 with an empty body —
/// the upstream shape for registration-gate rejections (`SignupApiService.ts`
/// around line 136: `reply.code(400); return;`); everything else keeps the
/// standard `{ error: … }` envelope.
enum SignupError {
    Rejected,
    Api(ApiError),
}

async fn do_signup(
    state: &ApiState,
    obj: &Map<String, Value>,
    ip: &str,
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
    Ok((account, raw_token))
}

async fn do_signin(
    state: &ApiState,
    obj: &Map<String, Value>,
    ip: &str,
) -> Result<(Account, String), ApiError> {
    let username = match obj.get("username").and_then(Value::as_str) {
        Some(username) if !username.is_empty() => username,
        _ => {
            return Err(ApiError::invalid_param(
                "username",
                "username is required",
            ));
        }
    };
    let password = match obj.get("password").and_then(Value::as_str) {
        Some(password) if !password.is_empty() => password,
        _ => {
            return Err(ApiError::invalid_param(
                "password",
                "password is required",
            ));
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
    let raw_token = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
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
    Ok((account, raw_token))
}

const SIGNUP_FIELDS: &[&str] = &[
    "username",
    "password",
    "setupPassword",
    "host",
    "invitationCode",
    "emailAddress",
];

async fn signup(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let obj = parse_json_body(&body)?;
    check_allowed_fields(&obj, SIGNUP_FIELDS)?;
    let ip = client_ip(peer, &headers, &state.trusted_proxies);
    let (account, raw_token) = match do_signup(&state, &obj, &ip).await {
        Ok(ok) => ok,
        // Registration-gate rejection: upstream's plain empty 400.
        Err(SignupError::Rejected) => return Ok(StatusCode::BAD_REQUEST.into_response()),
        Err(SignupError::Api(err)) => return Err(err),
    };
    let mut packed = pack_me_detailed(&account, true, &state.services)
        .map_err(ApiError::from_port_error)?;
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

async fn signin(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let obj = parse_json_body(&body)?;
    check_allowed_fields(&obj, &["username", "password"])?;
    let ip = client_ip(peer, &headers, &state.trusted_proxies);
    let (account, raw_token) = do_signin(&state, &obj, &ip).await?;
    let user = pack_me_detailed(&account, true, &state.services)
        .map_err(ApiError::from_port_error)?;
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

// --- Drive upload ----------------------------------------------------------

fn decode_base64(input: &str) -> Option<Vec<u8>> {
    let compact: Vec<u8> = input
        .bytes()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
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

fn is_truthy_value(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => is_truthy(Some(s.as_str())),
        Some(Value::Number(n)) => n.as_u64().is_some_and(|v| v != 0),
        _ => false,
    }
}

#[allow(clippy::too_many_lines)]
async fn drive_files_create(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
) -> Result<Response, ApiError> {
    let (parts, body) = req.into_parts();
    let headers = parts.headers.clone();
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    // Slice 6 (H1): when the credential travels in the `Authorization`
    // header, authenticate, check `write:drive`, and rate-limit BEFORE
    // reading a single body byte.
    let header_token = extract_raw_token(
        headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()),
        None,
        None,
    );
    let early_caller = match header_token {
        Some(raw) => {
            let caller = authenticate(&state.services, &raw)?;
            caller.require_permission("write:drive")?;
            state
                .rate_limiter
                .check(&caller.account.id, "drive/files/create")?;
            Some(caller)
        }
        None => None,
    };
    // Slice 6 (H1, decision D-c): bound concurrent uploads per client IP
    // (plus a global cap). Held for the whole handler.
    let _upload_permit = state
        .upload_slots
        .acquire(&client_ip(peer, &headers, &state.trusted_proxies))?;
    // Slice 6 (H1): streaming running cap — 413 as soon as the cap is
    // crossed, without ever holding more than ~cap bytes. When the token
    // is the multipart `i` field (as misskey-js sends it), this bounded
    // read is the guard; auth still happens below before any write.
    let raw = read_capped_body(body, upload_cap_bytes(&state.services)).await?;

    // Collected upload fields, from multipart or JSON.
    // (No content-type is collected: the stored MIME comes from the shared
    // sniffer only — slice 7, H2 — so a caller MIME can never be stored.)
    let mut name: Option<String> = None;
    let mut filename: Option<String> = None;
    let mut data: Option<Vec<u8>> = None;
    let mut comment: Option<String> = None;
    let mut is_sensitive = false;
    let mut force = false;
    let mut body_token: Option<String> = None;

    if content_type.contains("multipart/form-data") {
        let request = Request::from_parts(parts, Body::from(raw));
        let mut multipart = Multipart::from_request(request, &state)
            .await
            .map_err(|err| ApiError::invalid_param("file", &err.to_string()))?;
        while let Some(field) = multipart
            .next_field()
            .await
            .map_err(|err| ApiError::invalid_param("file", &err.to_string()))?
        {
            let field_name = field.name().unwrap_or("").to_string();
            let field_filename = field.file_name().map(|s| s.to_string());
            let field_bytes = field
                .bytes()
                .await
                .map_err(|err| ApiError::invalid_param("file", &err.to_string()))?;
            match field_name.as_str() {
                "file" => {
                    filename = field_filename;
                    data = Some(field_bytes.to_vec());
                }
                "name" => name = Some(String::from_utf8_lossy(&field_bytes).into_owned()),
                "comment" => {
                    comment = Some(String::from_utf8_lossy(&field_bytes).into_owned());
                }
                "isSensitive" => {
                    is_sensitive = is_truthy(Some(
                        String::from_utf8_lossy(&field_bytes).trim(),
                    ));
                }
                "force" => {
                    force = is_truthy(Some(String::from_utf8_lossy(&field_bytes).trim()));
                }
                "i" => body_token = Some(String::from_utf8_lossy(&field_bytes).into_owned()),
                _ => {}
            }
        }
    } else {
        let obj = parse_json_body(&raw)?;
        check_allowed_fields(
            &obj,
            &[
                "name",
                "content",
                "bytes",
                "comment",
                "isSensitive",
                "force",
                "folderId",
            ],
        )?;
        name = obj.get("name").and_then(Value::as_str).map(|s| s.to_string());
        if let Some(text) = obj
            .get("content")
            .or_else(|| obj.get("bytes"))
            .and_then(Value::as_str)
        {
            data = decode_base64(text).or_else(|| Some(text.as_bytes().to_vec()));
        }
        comment = obj
            .get("comment")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        is_sensitive = is_truthy_value(obj.get("isSensitive"));
        force = is_truthy_value(obj.get("force"));
        body_token = obj.get("i").and_then(Value::as_str).map(|s| s.to_string());
    }

    let body_value = body_token
        .as_deref()
        .map(|token| serde_json::json!({ "i": token }));
    // The header path already authenticated above; otherwise resolve the
    // multipart/JSON `i` field (or cookie) exactly as before.
    let caller = match early_caller {
        Some(caller) => caller,
        None => {
            let raw_token = extract_raw_token(
                headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()),
                body_value.as_ref(),
                headers.get(COOKIE).and_then(|v| v.to_str().ok()),
            )
            .ok_or_else(ApiError::credential_required)?;
            let caller = authenticate(&state.services, &raw_token)?;
            caller.require_permission("write:drive")?;
            state
                .rate_limiter
                .check(&caller.account.id, "drive/files/create")?;
            caller
        }
    };

    let bytes = data
        .ok_or_else(|| ApiError::invalid_param("file", "an upload file is required"))?;
    let file_name = name
        .or(filename)
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| "untitled".to_string());
    let meta = state
        .services
        .upload_media(
            &caller.account.id,
            file_name,
            &bytes,
            comment,
            is_sensitive,
            force,
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_drive_file(&meta, &state.services)).into_response())
}

// --- JSON dispatcher -------------------------------------------------------

/// Endpoints may perform blocking storage and federation I/O, so the whole
/// dispatch runs on the blocking pool rather than an async worker thread.
async fn api_dispatch(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(endpoint): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let ip = client_ip(peer, &headers, &state.trusted_proxies);
    tokio::task::spawn_blocking(move || api_dispatch_blocking(state, endpoint, headers, body, ip))
        .await
        .map_err(|_| ApiError::from_port_error(PortError::StorageUnavailable("dispatch task failed".into())))?
}

/// `client_ip` is the resolved client address (see [`client_ip`]). The
/// signin and signup arms block on the async password helpers; this runs on
/// the blocking pool, so `block_on` is safe here.
#[allow(clippy::too_many_lines)]
fn api_dispatch_blocking(
    state: ApiState,
    endpoint: String,
    headers: HeaderMap,
    body: Bytes,
    client_ip: String,
) -> Result<Response, ApiError> {
    // Slice 6 (H1): 1 MiB default JSON body limit, enforced here (not in a
    // router layer) so direct dispatch calls are covered too. Oversized
    // bodies answer 413 `MAX_FILE_SIZE_EXCEEDED`, mirroring the inbox
    // `payload_too_large()` pattern.
    if body.len() > MAX_JSON_BODY_BYTES {
        return Err(ApiError::max_file_size_exceeded());
    }
    let obj = parse_json_body(&body)?;
    let body_value = Value::Object(obj.clone());
    let caller = resolve_caller(&state.services, &headers, Some(&body_value))?;
    let rate_key = rate_key_for(caller.as_ref(), &client_ip);

    match endpoint.as_str() {
        "meta" => {
            check_allowed_fields(&obj, &["detail"])?;
            let detail = opt_bool(&obj, "detail", true)?;
            let meta = pack_meta(&state.services, detail).map_err(ApiError::from_port_error)?;
            Ok(Json(meta).into_response())
        }

        "miauth/gen-token" => {
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
                return Err(ApiError::invalid_param("permission", "this field is required"));
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
                    token_hash: hash_token(&raw_token),
                    account_id: caller.account.id.clone(),
                    permissions: permissions.clone(),
                    is_first_party: false,
                    name: obj.get("name").and_then(Value::as_str).map(|s| s.to_string()),
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
                    name: obj.get("name").and_then(Value::as_str).map(|s| s.to_string()),
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

        "auth/session/generate" => {
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
                    expires_at_ms: Some(
                        state.services.clock.now_ms() + MIAUTH_APPROVAL_WINDOW_MS,
                    ),
                })
                .map_err(ApiError::from_port_error)?;
            let url = format!(
                "{}/miauth/{}",
                state.services.public_url.trim_end_matches('/'),
                token
            );
            Ok(Json(serde_json::json!({ "token": token, "url": url })).into_response())
        }

        "auth/session/show" => {
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

        "auth/accept" => {
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

        "auth/session/userkey" => {
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
                            ApiError::from_port_error(PortError::NotFound(
                                "NO_SUCH_USER".to_string(),
                            ))
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

        "signup" => {
            check_allowed_fields(&obj, SIGNUP_FIELDS)?;
            let (account, raw_token) = match tokio::runtime::Handle::current()
                .block_on(do_signup(&state, &obj, &client_ip))
            {
                Ok(ok) => ok,
                // Registration-gate rejection: upstream's plain empty 400.
                Err(SignupError::Rejected) => return Ok(StatusCode::BAD_REQUEST.into_response()),
                Err(SignupError::Api(err)) => return Err(err),
            };
            let mut packed = pack_me_detailed(&account, true, &state.services)
                .map_err(ApiError::from_port_error)?;
            if let Some(map) = packed.as_object_mut() {
                map.insert("token".to_string(), Value::String(raw_token));
            }
            Ok(Json(packed).into_response())
        }

        // WP2: `admin/accounts/create` (upstream `admin/accounts/create.ts`)
        // is the ADMIN-only operator path and deliberately bypasses the
        // registration gate (upstream calls `SignupService` there, not the
        // gated `SignupApiService`). DIVERGENCES: pinetree always requires
        // an authenticated admin — upstream's first-setup bootstrap
        // (`setupPassword`, `INCORRECT_INITIAL_PASSWORD`) has no equivalent
        // here (CLI `create-account` bootstraps), so `setupPassword` is
        // accepted but never consulted, exactly as upstream ignores it for
        // an authenticated admin; and the action logs `createAccount`
        // (upstream writes no moderation-log row).
        "admin/accounts/create" => {
            check_allowed_fields(&obj, &["username", "password", "setupPassword"])?;
            let caller = require_caller(caller)?;
            require_admin(&caller)?;
            let username = required_str(&obj, "username")?;
            let password = required_str(&obj, "password")?;
            let hash = tokio::runtime::Handle::current().block_on(hash_password_async(password))?;
            let account = state
                .services
                .create_account_admin(&username, Some(&hash), &caller.account.id)
                .map_err(ApiError::from_port_error)?;
            let raw_token = generate_random_token();
            state
                .services
                .auth
                .store_token(ApiTokenRecord {
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
            let mut packed = pack_me_detailed(&account, true, &state.services)
                .map_err(ApiError::from_port_error)?;
            if let Some(map) = packed.as_object_mut() {
                map.insert("token".to_string(), Value::String(raw_token));
            }
            Ok(Json(packed).into_response())
        }

        "signin" => {
            check_allowed_fields(&obj, &["username", "password"])?;
            let (account, raw_token) =
                tokio::runtime::Handle::current().block_on(do_signin(&state, &obj, &client_ip))?;
            let user = pack_me_detailed(&account, true, &state.services)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(serde_json::json!({
                "id": account.id,
                "token": raw_token,
                "i": raw_token,
                "user": user,
            }))
            .into_response())
        }

        "i" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let packed =
                pack_me_detailed(&caller.account, caller.is_first_party, &state.services)
                    .map_err(ApiError::from_port_error)?;
            Ok(Json(packed).into_response())
        }

        "i/update" => {
            // Full profile store (§0B): all 39 `i/update.ts` params plus the
            // DIVERGENCE `rejectQuotes` (§3) and `defaultReaction` (§6)
            // params. Business rules (prohibited words, media checks, mute
            // limits, alias resolution, unlock-accept) live in
            // `AppServices::update_profile`; this arm only parses shapes.
            check_allowed_fields(
                &obj,
                &[
                    "name",
                    "description",
                    "followedMessage",
                    "location",
                    "birthday",
                    "lang",
                    "avatarId",
                    "avatarDecorations",
                    "bannerId",
                    "fields",
                    "isLocked",
                    "isExplorable",
                    "hideOnlineStatus",
                    "publicReactions",
                    "carefulBot",
                    "autoAcceptFollowed",
                    "followApprovalLocalSeconds",
                    "followApprovalRemoteSeconds",
                    "noCrawle",
                    "preventAiLearning",
                    "requireSigninToViewContents",
                    "makeNotesFollowersOnlyBefore",
                    "makeNotesHiddenBefore",
                    "isBot",
                    "isCat",
                    "injectFeaturedNote",
                    "receiveAnnouncementEmail",
                    "alwaysMarkNsfw",
                    "autoSensitive",
                    "followingVisibility",
                    "followersVisibility",
                    "chatScope",
                    "pinnedPageId",
                    "mutedWords",
                    "hardMutedWords",
                    "mutedInstances",
                    "notificationRecieveConfig",
                    "emailNotificationTypes",
                    "alsoKnownAs",
                    "rejectQuotes",
                    "defaultReaction",
                    // DIVERGENCE (Batch N WP8b): per-user remote-emoji opt-in.
                    "showRemoteEmojis",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/update")?;
            let patch = parse_profile_patch(&obj)?;
            let show_remote_emojis_on = patch.show_remote_emojis == Some(true);
            let account = state
                .services
                .update_profile(&caller.account.id, patch)
                .map_err(ApiError::from_port_error)?;
            // WP8b lazy fetch: turning the opt-in on enqueues one bounded
            // fetch job per known remote emoji (metadata rows only; no fetch
            // happened at ingest). Off-request, crash-safe, retryable.
            if show_remote_emojis_on {
                if let Err(e) = state.services.enqueue_remote_emoji_fetches() {
                    eprintln!("remote emoji fetch enqueue failed: {e}");
                }
            }
            let packed = pack_me_detailed(&account, caller.is_first_party, &state.services)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(packed).into_response())
        }

        "users/show" => {
            check_allowed_fields(&obj, &["userId", "userIds", "username", "host"])?;
            if let Some(Value::Array(ids)) = obj.get("userIds") {
                let mut out = Vec::with_capacity(ids.len());
                for id in ids {
                    let id = id.as_str().ok_or_else(|| {
                        ApiError::invalid_param("userIds", "must be an array of strings")
                    })?;
                    let account = state
                        .services
                        .users_show_by_id(id)
                        .map_err(ApiError::from_port_error)?;
                    out.push(pack_account_for(&account, caller.as_ref(), &state.services)?);
                }
                return Ok(Json(Value::Array(out)).into_response());
            }
            if obj.get("userIds").is_some() {
                return Err(ApiError::invalid_param(
                    "userIds",
                    "must be an array of strings",
                ));
            }
            let account = if let Some(user_id) = obj.get("userId").and_then(Value::as_str) {
                state
                    .services
                    .users_show_by_id(user_id)
                    .map_err(ApiError::from_port_error)?
            } else if let Some(username) = obj.get("username").and_then(Value::as_str) {
                let host = obj.get("host").and_then(Value::as_str);
                match state.services.users_show_by_username(username, host) {
                    Ok(account) => account,
                    Err(original @ PortError::NotFound(_)) => match host {
                        Some(remote_host) => {
                            let wf = webfinger_discover_actor_uri(
                                &state.services.federation_http,
                                username,
                                remote_host,
                                None,
                            );
                            if let Err(e) = &wf {
                                eprintln!("remote user resolve {username}@{remote_host}: webfinger failed: {e}");
                            }
                            let resolved = wf
                            .ok()
                            .and_then(|actor_uri| {
                                let r = fetch_and_upsert_remote_actor(
                                    &state.services,
                                    &state.services.federation_http,
                                    &actor_uri,
                                );
                                if let Err(e) = &r {
                                    eprintln!("remote user resolve {actor_uri}: actor fetch failed: {e}");
                                }
                                r.ok()
                            })
                            .map(|(account, _)| account);
                            match resolved {
                                Some(account) => account,
                                None => return Err(ApiError::from_port_error(original)),
                            }
                        }
                        None => return Err(ApiError::from_port_error(original)),
                    },
                    Err(err) => return Err(ApiError::from_port_error(err)),
                }
            } else {
                return Err(ApiError::invalid_param(
                    "userId",
                    "one of userId, userIds, or username is required",
                ));
            };
            Ok(Json(pack_account_for(
                &account,
                caller.as_ref(),
                &state.services,
            )?)
            .into_response())
        }

        "users/search" => {
            check_allowed_fields(&obj, &["query", "offset", "limit", "origin", "detail"])?;
            let query = obj
                .get("query")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let offset = opt_usize(&obj, "offset")?.unwrap_or(0);
            let limit = get_limit(&obj)?;
            let accounts = state
                .services
                .accounts
                .search_accounts(&query, offset, limit)
                .map_err(ApiError::from_port_error)?;
            let mut out = Vec::with_capacity(accounts.len());
            for account in &accounts {
                out.push(pack_account_for(account, caller.as_ref(), &state.services)?);
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "username/available" => {
            check_allowed_fields(&obj, &["username"])?;
            let username = required_str(&obj, "username")?;
            let valid = !username.is_empty()
                && username.len() <= 20
                && username
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_');
            if !valid {
                return Err(ApiError::invalid_param(
                    "username",
                    "must match ^[A-Za-z0-9_]{1,20}$",
                ));
            }
            let available = state
                .services
                .accounts
                .is_username_available(&username, &state.services.preserved_usernames)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(serde_json::json!({ "available": available })).into_response())
        }

        "notes/create" => {
            check_allowed_fields(
                &obj,
                &[
                    "visibility",
                    "visibleUserIds",
                    "text",
                    "cw",
                    "localOnly",
                    "reactionAcceptance",
                    "noExtractMentions",
                    "noExtractHashtags",
                    "noExtractEmojis",
                    "replyId",
                    "renoteId",
                    "channelId",
                    "fileIds",
                    "mediaIds",
                    "poll",
                ],
            )?;
            if let Some(channel) = obj.get("channelId") {
                if !channel.is_null() {
                    return Err(ApiError::unsupported_field("channelId"));
                }
            }
            // §4 polls (Misskey `notes/create.ts:168-184` parity).
            let poll_input = parse_poll_param(&obj, "poll", state.services.clock.now_ms())?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:notes")?;
            state.rate_limiter.check(&rate_key, "notes/create")?;
            let mut file_ids = str_list(&obj, "fileIds")?;
            for id in str_list(&obj, "mediaIds")? {
                if !file_ids.contains(&id) {
                    file_ids.push(id);
                }
            }
            let input = CreateNoteInput {
                author_id: caller.account.id.clone(),
                text: opt_string(&obj, "text")?,
                cw: opt_string(&obj, "cw")?,
                visibility: parse_visibility(obj.get("visibility"))?,
                local_only: opt_bool(&obj, "localOnly", false)?,
                reply_to: opt_string(&obj, "replyId")?,
                reply_parent_uri: None,
                renote_of: opt_string(&obj, "renoteId")?,
                file_ids,
                visible_users: str_list(&obj, "visibleUserIds")?,
                reaction_acceptance: parse_reaction_acceptance(obj.get("reactionAcceptance"))?,
                no_extract_mentions: opt_bool(&obj, "noExtractMentions", false)?,
                no_extract_hashtags: opt_bool(&obj, "noExtractHashtags", false)?,
                no_extract_emojis: opt_bool(&obj, "noExtractEmojis", false)?,
                has_poll: poll_input.is_some(),
                poll: poll_input,
                poll_initial_votes: None,
                has_channel: false,
                prohibited_words: Vec::new(),
                sensitive_words: Vec::new(),
                uri: None,
                uri_base: String::new(),
                url: None,
                extra_mentions: Vec::new(),
                extra_tags: Vec::new(),
                quote: None,
            };
            let note = state
                .services
                .create_note(input)
                .map_err(ApiError::from_port_error)?;
            let packed = pack_note(&note, Some(&caller.account.id), &state.services, 0)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(serde_json::json!({ "createdNote": packed })).into_response())
        }

        "notes/show" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let note_id = required_str(&obj, "noteId")?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let note = state
                .services
                .notes_show(&note_id, viewer_id)
                .map_err(ApiError::from_port_error)?;
            let packed = pack_note(&note, viewer_id, &state.services, 0)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(packed).into_response())
        }

        "notes/delete" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:notes")?;
            state.rate_limiter.check(&rate_key, "notes/delete")?;
            let note_id = required_str(&obj, "noteId")?;
            state
                .services
                .delete_note(&caller.account.id, &note_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        // §8 `notes/edit` (Sharkey `notes/edit.ts` 484-line parity, backend
        // only): `requireCredential`, `write:notes`, 300/h, res
        // `{createdNote: Note}`. `replyId` is validated when supplied but
        // the original parent is always kept; `visibility`/`localOnly` are
        // parsed (for the specified-reply check) then forced back to the
        // original note's values by the service. Pinetree has no
        // moved-account machinery, so `prohibitMoved` has nothing to hook
        // (same as every other write endpoint).
        "notes/edit" => {
            check_allowed_fields(
                &obj,
                &[
                    "editId",
                    "visibility",
                    "visibleUserIds",
                    "text",
                    "cw",
                    "localOnly",
                    "reactionAcceptance",
                    "noExtractMentions",
                    "noExtractHashtags",
                    "noExtractEmojis",
                    "replyId",
                    "renoteId",
                    "channelId",
                    "fileIds",
                    "mediaIds",
                    "poll",
                ],
            )?;
            if let Some(channel) = obj.get("channelId") {
                if !channel.is_null() {
                    // No channels exist: same `UNSUPPORTED_FIELD` rule as
                    // `notes/create` (the `NO_SUCH_CHANNEL` below is
                    // unreachable while channels are absent).
                    return Err(ApiError::unsupported_field("channelId"));
                }
            }
            // Missing/empty `editId` is `NEEDS_EDIT_ID`, not
            // `INVALID_PARAM` (Sharkey error-set parity).
            let edit_id = match obj.get("editId") {
                Some(Value::String(s)) if !s.is_empty() => s.clone(),
                _ => {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "NEEDS_EDIT_ID".to_string(),
                    )));
                }
            };
            // Length pre-gates (Sharkey `edit.ts` parity — the endpoint
            // rejects, the service trims/truncates for the inbound path).
            // Counts are Rust chars (upstream counts UTF-16 code units).
            if let Some(Value::String(text)) = obj.get("text") {
                if text.chars().count() > MAX_NOTE_TEXT_CHARS {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "MAX_LENGTH".to_string(),
                    )));
                }
            }
            if let Some(Value::String(cw)) = obj.get("cw") {
                if cw.chars().count() > MAX_CW_API_CHARS {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "MAX_CW_LENGTH".to_string(),
                    )));
                }
            }
            let caller = require_caller(caller)?;
            caller.require_permission("write:notes")?;
            state.rate_limiter.check(&rate_key, "notes/edit")?;
            let requested_visibility = parse_visibility(obj.get("visibility"))?;
            // §8 poll params (same `parse_poll_param` as `notes/create`:
            // `expiredAfter` wins, past expiry is the exact
            // `CANNOT_CREATE_ALREADY_EXPIRED_POLL` ident).
            let poll_input =
                parse_poll_param(&obj, "poll", state.services.clock.now_ms())?;
            // The edit target must exist here so the renote/reply checks
            // below run against the original note (the service re-checks
            // authorship atomically; a deleted target is `NO_SUCH_NOTE`).
            let original = match state.services.notes.get_note(&edit_id).map_err(ApiError::from_port_error)? {
                Some(note) => note,
                None => {
                    return Err(ApiError::from_port_error(PortError::NotFound(
                        "NO_SUCH_NOTE".to_string(),
                    )));
                }
            };
            if original.author_id != caller.account.id {
                return Err(ApiError::from_port_error(PortError::Validation(
                    "YOU_ARE_NOT_THE_AUTHOR".to_string(),
                )));
            }
            // Renote checks (Sharkey `edit.ts` endpoint order: self-quote,
            // existence, pure-renote target, quote-of-edited-note,
            // blocking, visibility).
            let mut renote_of: Option<String> = None;
            if let Some(renote_id) = opt_string(&obj, "renoteId")? {
                if renote_id == edit_id {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "CANNOT_QUOTE_THE_CURRENT_NOTE".to_string(),
                    )));
                }
                let target = match state.services.notes.get_note(&renote_id).map_err(ApiError::from_port_error)? {
                    Some(note) => note,
                    None => {
                        return Err(ApiError::from_port_error(PortError::NotFound(
                            "NO_SUCH_RENOTE_TARGET".to_string(),
                        )));
                    }
                };
                if target.renote_id.is_some() && target.quote.is_none() {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "CANNOT_RENOTE_TO_A_PURE_RENOTE".to_string(),
                    )));
                }
                if target.renote_id.as_deref() == Some(edit_id.as_str()) {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE".to_string(),
                    )));
                }
                if target.author_id != caller.account.id
                    && state
                        .services
                        .blocks
                        .is_blocking(&target.author_id, &caller.account.id)
                        .map_err(ApiError::from_port_error)?
                {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "YOU_HAVE_BEEN_BLOCKED".to_string(),
                    )));
                }
                if target.visibility == Visibility::Specified
                    || (target.visibility == Visibility::Followers
                        && target.author_id != caller.account.id)
                {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "CANNOT_RENOTE_DUE_TO_VISIBILITY".to_string(),
                    )));
                }
                // Renote-in-channel narrowing (`NO_SUCH_CHANNEL` /
                // `CANNOT_RENOTE_OUTSIDE_OF_CHANNEL`) is unreachable: stored
                // notes never carry a channel (creation rejects them).
                renote_of = Some(renote_id);
            }
            // Reply checks (Sharkey `edit.ts` endpoint order). The supplied
            // reply is validated but then discarded: the service always keeps
            // the original parent (`replyId` immutable).
            if let Some(reply_id) = opt_string(&obj, "replyId")? {
                let reply = match state.services.notes.get_note(&reply_id).map_err(ApiError::from_port_error)? {
                    Some(note) => note,
                    None => {
                        return Err(ApiError::from_port_error(PortError::NotFound(
                            "NO_SUCH_REPLY_TARGET".to_string(),
                        )));
                    }
                };
                if reply.renote_id.is_some() && reply.quote.is_none() {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "CANNOT_REPLY_TO_A_PURE_RENOTE".to_string(),
                    )));
                }
                let follows_author = reply.author_id == caller.account.id
                    || state
                        .services
                        .follows
                        .is_following(&caller.account.id, &reply.author_id)
                        .map_err(ApiError::from_port_error)?;
                let viewer = crate::domain::policy::ViewerContext::local_user(
                    &caller.account.id,
                    follows_author,
                );
                if !crate::domain::visibility::is_visible(&reply, &viewer) {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "CANNOT_REPLY_TO_AN_INVISIBLE_NOTE".to_string(),
                    )));
                }
                if reply.visibility == Visibility::Specified
                    && requested_visibility != Visibility::Specified
                {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY"
                            .to_string(),
                    )));
                }
                if reply.author_id != caller.account.id
                    && state
                        .services
                        .blocks
                        .is_blocking(&reply.author_id, &caller.account.id)
                        .map_err(ApiError::from_port_error)?
                {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "YOU_HAVE_BEEN_BLOCKED".to_string(),
                    )));
                }
            }
            let mut file_ids = str_list(&obj, "fileIds")?;
            for id in str_list(&obj, "mediaIds")? {
                if !file_ids.contains(&id) {
                    file_ids.push(id);
                }
            }
            let input = EditNoteInput {
                editor_id: caller.account.id.clone(),
                note_id: edit_id,
                text: opt_string(&obj, "text")?,
                cw: opt_string(&obj, "cw")?,
                renote_of,
                file_ids,
                poll: poll_input,
                reaction_acceptance: parse_reaction_acceptance(
                    obj.get("reactionAcceptance"),
                )?,
                visible_users: str_list(&obj, "visibleUserIds")?,
                // `noExtract*` params are accepted and ignored: pinetree
                // extracts mentions/tags unconditionally (same as the
                // `notes/create` arm, which reads them for validation only).
                prohibited_words: Vec::new(),
                sensitive_words: Vec::new(),
                uri_base: String::new(),
            };
            let note = state
                .services
                .edit_note(input)
                .map_err(ApiError::from_port_error)?;
            let packed = pack_note(&note, Some(&caller.account.id), &state.services, 0)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(serde_json::json!({ "createdNote": packed })).into_response())
        }

        // §3 `admin/reject-quotes` (Sharkey `admin/reject-quotes.ts`
        // parity): moderation toggle for one user's `rejectQuotes` flag.
        // `credential` + `write:admin:reject-quotes` + the moderator role
        // (Batch M WP1: scopes alone are not authorization). The
        // moderator-target refusal (WP3 refusal rule) and the Sharkey
        // `acceptQuotesUser`/`rejectQuotesUser` moderation-log row live in
        // the application command; the log row and the `MeUpdated`
        // streaming job commit atomically (the `localUserUpdated`
        // equivalent). Unlike `i/update` a moderation toggle never
        // federates `Update(Person)`.
        "admin/reject-quotes" => {
            check_allowed_fields(&obj, &["userId", "rejectQuotes"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:reject-quotes")?;
            require_moderator(&caller)?;
            let user_id = required_str(&obj, "userId")?;
            let reject = match obj.get("rejectQuotes") {
                Some(Value::Bool(b)) => *b,
                _ => {
                    return Err(ApiError::invalid_param(
                        "rejectQuotes",
                        "must be a boolean",
                    ));
                }
            };
            let account = state
                .services
                .set_reject_quotes(&caller.account.id, &user_id, reject)
                .map_err(ApiError::from_port_error)?;
            let packed = pack_me_detailed(&account, caller.is_first_party, &state.services)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(packed).into_response())
        }

        // Batch M WP6 `admin/show-user` (upstream `admin/show-user.ts`):
        // moderator only. Returns account details including isSuspended,
        // isAdmin, isModerator, and report counts.
        "admin/show-user" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            require_moderator(&caller)?;
            let user_id = required_str(&obj, "userId")?;
            let account = state
                .services
                .accounts
                .find_by_id(&user_id)
                .map_err(ApiError::from_port_error)?
                .ok_or_else(|| ApiError::from_port_error(PortError::NotFound("NO_SUCH_USER".to_string())))?;
            let report_count = state
                .services
                .abuse_reports
                .count_for_target(&user_id)
                .map_err(ApiError::from_port_error)?;
            let mut packed = if account.id == caller.account.id {
                pack_me_detailed(&account, caller.is_first_party, &state.services)
                    .map_err(ApiError::from_port_error)?
            } else {
                pack_user_detailed_not_me(&account, Some(&caller.account.id), &state.services)
                    .map_err(ApiError::from_port_error)?
            };
            if let Some(o) = packed.as_object_mut() {
                o.insert("reportCount".to_string(), serde_json::json!(report_count));
                o.insert("moderationNote".to_string(), serde_json::json!(account.moderator_note));
            }
            Ok(Json(packed).into_response())
        }

        // Batch M WP6 `admin/show-users` (upstream `admin/show-users.ts`):
        // moderator only. Subset of upstream params.
        "admin/show-users" => {
            check_allowed_fields(&obj, &["limit", "offset", "state", "origin", "username", "hostname", "sort"])?;
            let caller = require_caller(caller)?;
            require_moderator(&caller)?;
            let limit = get_limit(&obj)?;
            let offset = opt_usize(&obj, "offset")?.unwrap_or(0);
            let state_filter = opt_string(&obj, "state")?.unwrap_or_else(|| "all".to_string());
            let origin = opt_string(&obj, "origin")?.unwrap_or_else(|| "combined".to_string());
            let username_filter = opt_string(&obj, "username")?;
            let hostname_filter = opt_string(&obj, "hostname")?;
            // Enumerate accounts bounded (all, including suspended/deleted).
            let mut accounts = Vec::new();
            let mut cursor = None;
            loop {
                let (page, next) = state.services.accounts.all_accounts(cursor.as_deref(), 1000).map_err(ApiError::from_port_error)?;
                accounts.extend(page);
                match next {
                    Some(c) => cursor = Some(c),
                    None => break,
                }
                if accounts.len() >= 10_000 { break; }
            }
            // Filter.
            let mut filtered: Vec<_> = accounts.into_iter().filter(|a| {
                if a.is_deleted { return false; }
                match state_filter.as_str() {
                    "available" => !a.is_suspended,
                    "suspended" => a.is_suspended,
                    "admin" => a.is_admin,
                    "moderator" => a.is_moderator && !a.is_admin,
                    "adminOrModerator" => a.is_admin || a.is_moderator,
                    _ => true,
                }
            }).filter(|a| match origin.as_str() {
                "local" => a.host.is_none(),
                "remote" => a.host.is_some(),
                _ => true,
            }).filter(|a| {
                if let Some(u) = &username_filter {
                    a.username_lower.starts_with(&u.to_lowercase())
                } else { true }
            }).filter(|a| {
                if let Some(h) = &hostname_filter {
                    a.host.as_deref() == Some(h.as_str())
                } else { true }
            }).collect();
            // sort by id asc default
            filtered.sort_by(|a, b| a.id.cmp(&b.id));
            let page: Vec<_> = filtered.into_iter().skip(offset).take(limit).collect();
            let mut out = Vec::new();
            for a in page {
                let rc = state.services.abuse_reports.count_for_target(&a.id).map_err(ApiError::from_port_error)?;
                let mut packed = pack_user_detailed_not_me(&a, Some(&caller.account.id), &state.services).map_err(ApiError::from_port_error)?;
                if let Some(o) = packed.as_object_mut() {
                    o.insert("reportCount".to_string(), serde_json::json!(rc));
                    o.insert("moderationNote".to_string(), serde_json::json!(a.moderator_note));
                }
                out.push(packed);
            }
            Ok(Json(serde_json::json!(out)).into_response())
        }

        // Batch M WP6 `admin/update-user-note` (upstream `admin/update-user-note.ts`).
        "admin/update-user-note" => {
            check_allowed_fields(&obj, &["userId", "text"])?;
            let caller = require_caller(caller)?;
            require_moderator(&caller)?;
            let user_id = required_str(&obj, "userId")?;
            let text = required_str(&obj, "text")?;
            let account = state
                .services
                .update_user_note(&caller.account.id, &user_id, &text)
                .map_err(ApiError::from_port_error)?;
            let packed = pack_user_detailed_not_me(&account, Some(&caller.account.id), &state.services).map_err(ApiError::from_port_error)?;
            Ok(Json(packed).into_response())
        }

        // Batch M WP6 `admin/delete-account` (upstream `admin/delete-account.ts`): admin only.
        "admin/delete-account" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            require_admin(&caller)?;
            let user_id = required_str(&obj, "userId")?;
            state
                .services
                .delete_account(Some(&caller.account.id), &user_id, None)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(serde_json::json!({})).into_response())
        }

        // Batch M WP6 `i/delete-account` (upstream `i/delete-account.ts`): self-service, password required.
        "i/delete-account" => {
            check_allowed_fields(&obj, &["password"])?;
            let caller = require_caller(caller)?;
            let password = required_str(&obj, "password")?;
            state
                .services
                .delete_account(Some(&caller.account.id), &caller.account.id, Some(&password))
                .map_err(ApiError::from_port_error)?;
            Ok(Json(serde_json::json!({})).into_response())
        }

        // --- Batch N WS6: moderation/admin remainder -----------------------
        // `admin/unset-user-avatar|banner` (moderator + scope; refusal rule;
        // unset log rows), `admin/delete-all-files-of-a-user` (admin),
        // `admin/drive/files` + `admin/drive/show-file` (moderator,
        // read-only). Everything else in `admin/*` stays out — see the
        // manifest §30 audit table for the per-endpoint verdict + reason.
        "admin/unset-user-avatar" | "admin/unset-user-banner" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            let avatar = endpoint == "admin/unset-user-avatar";
            caller.require_permission(if avatar {
                "write:admin:unset-user-avatar"
            } else {
                "write:admin:unset-user-banner"
            })?;
            require_moderator(&caller)?;
            let user_id = required_str(&obj, "userId")?;
            state
                .services
                .unset_user_media(&caller.account.id, &user_id, avatar)
                .map_err(|e| match e {
                    // Guide §3.1 blanket rule (DIVERGENCE: upstream checks
                    // the role only): moderators cannot act on admins or
                    // other moderators.
                    PortError::Validation(code) if code == "cannot moderate moderator account" => {
                        ApiError::invalid_param("userId", &code)
                    }
                    other => ApiError::from_port_error(other),
                })?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "admin/delete-all-files-of-a-user" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:delete-all-files-of-a-user")?;
            require_admin(&caller)?;
            let user_id = required_str(&obj, "userId")?;
            state
                .services
                .delete_all_files_of_a_user(&caller.account.id, &user_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "admin/drive/files" => {
            check_allowed_fields(
                &obj,
                &["limit", "sinceId", "untilId", "sinceDate", "untilDate", "userId", "type", "origin", "hostname"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:admin:drive")?;
            require_moderator(&caller)?;
            let cursor = TimelineCursor {
                since_id: opt_string(&obj, "sinceId")?,
                until_id: opt_string(&obj, "untilId")?,
                since_date: opt_i64(&obj, "sinceDate")?,
                until_date: opt_i64(&obj, "untilDate")?,
                limit: get_limit_with_default(&obj, 10)?,
            };
            let user_id = opt_string(&obj, "userId")?;
            let type_prefix = match opt_string(&obj, "type")? {
                None => None,
                // Upstream `type` is a MIME pattern (`image/*`); honor a
                // trailing `*` as a prefix match, anything else exact.
                Some(pattern) => Some(match pattern.strip_suffix('*') {
                    Some(prefix) => (prefix.to_string(), true),
                    None => (pattern, false),
                }),
            };
            let origin = opt_string(&obj, "origin")?.unwrap_or_else(|| "local".to_string());
            if !["combined", "local", "remote"].contains(&origin.as_str()) {
                return Err(ApiError::invalid_param("origin", "must be one of combined, local, remote"));
            }
            let hostname = opt_string(&obj, "hostname")?;
            let rows = state
                .services
                .media
                .list_all_media(&cursor)
                .map_err(ApiError::from_port_error)?;
            let mut out = Vec::new();
            for meta in &rows {
                if let Some(user_id) = user_id.as_deref() {
                    if meta.owner_id.as_deref() != Some(user_id) {
                        continue;
                    }
                }
                if let Some((pattern, prefix)) = type_prefix.as_ref() {
                    if *prefix {
                        if !meta.mime.starts_with(pattern.as_str()) {
                            continue;
                        }
                    } else if meta.mime != *pattern {
                        continue;
                    }
                }
                // Resolve the owner's host once per row (small pages; the
                // admin list defaults to 10 rows).
                let host = match meta.owner_id.as_deref() {
                    Some(owner) => state
                        .services
                        .accounts
                        .find_by_id(owner)
                        .map_err(ApiError::from_port_error)?
                        .and_then(|account| account.host),
                    None => None,
                };
                match origin.as_str() {
                    "local" if host.is_some() => continue,
                    "remote" if host.is_none() => continue,
                    _ => {}
                }
                if let Some(hostname) = hostname.as_deref() {
                    // Upstream: the local host is represented with `null` —
                    // a hostname filter never matches local files.
                    match host.as_deref() {
                        Some(h) if h == hostname => {}
                        _ => continue,
                    }
                }
                out.push(pack_drive_file(meta, &state.services));
                if out.len() >= cursor.limit {
                    break;
                }
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "admin/drive/show-file" => {
            check_allowed_fields(&obj, &["fileId", "url"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:admin:drive")?;
            require_moderator(&caller)?;
            let (file_id, url) = match (obj.get("fileId"), obj.get("url")) {
                (Some(_), Some(_)) => {
                    return Err(ApiError::invalid_param("url", "specify either fileId or url, not both"));
                }
                (None, None) => {
                    return Err(ApiError::invalid_param("fileId", "either fileId or url is required"));
                }
                (id, url) => (
                    id.and_then(Value::as_str).map(str::to_string),
                    url.and_then(Value::as_str).map(str::to_string),
                ),
            };
            let meta = if let Some(file_id) = file_id {
                state
                    .services
                    .media
                    .get(&file_id)
                    .map_err(ApiError::from_port_error)?
            } else {
                let url = url.unwrap_or_default();
                let rows = state
                    .services
                    .media
                    .list_all_media(&TimelineCursor { limit: 10_000, ..Default::default() })
                    .map_err(ApiError::from_port_error)?;
                rows.into_iter()
                    .find(|m| m.url == url || m.source_url.as_deref() == Some(url.as_str()))
            };
            match meta {
                Some(meta) => Ok(Json(pack_drive_file(&meta, &state.services)).into_response()),
                None => Err(ApiError::drive_show_file_no_such_file()),
            }
        }

        // Batch M §3.2 `admin/show-moderation-logs` (upstream file of the
        // same name): cursor-paged moderation log with `type`/`userId`/
        // `search` filters. DIVERGENCE: upstream requires admin
        // (`requireAdmin`); pinetree lets moderators read the log (guide
        // §3.2).
        "admin/show-moderation-logs" => {
            check_allowed_fields(
                &obj,
                &[
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "type",
                    "userId",
                    "search",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:admin:show-moderation-log")?;
            require_moderator(&caller)?;
            let cursor = TimelineCursor {
                since_id: opt_string(&obj, "sinceId")?,
                until_id: opt_string(&obj, "untilId")?,
                since_date: opt_i64(&obj, "sinceDate")?,
                until_date: opt_i64(&obj, "untilDate")?,
                limit: get_limit(&obj)?,
            };
            // An unknown `type` name matches no rows upstream
            // (`log.type = :type`); answer `[]` the same way.
            let action = match obj.get("type") {
                None | Some(Value::Null) => None,
                Some(Value::String(name)) => match ModerationAction::parse(name) {
                    Some(action) => Some(action),
                    None => return Ok(Json(Value::Array(Vec::new())).into_response()),
                },
                Some(_) => return Err(ApiError::invalid_param("type", "must be a string")),
            };
            let filter = ModerationLogFilter {
                action,
                actor: opt_string(&obj, "userId")?,
                target: None,
                search: opt_string(&obj, "search")?,
            };
            let rows = state
                .services
                .list_moderation_logs(filter, &cursor)
                .map_err(ApiError::from_port_error)?;
            let mut packed = Vec::with_capacity(rows.len());
            for row in &rows {
                packed.push(
                    pack_moderation_log(row, Some(&caller.account.id), &state.services)
                        .map_err(ApiError::from_port_error)?,
                );
            }
            Ok(Json(Value::Array(packed)).into_response())
        }

        // --- Batch M WP3: suspension ---------------------------------
        // (upstream `admin/suspend-user.ts` / `admin/unsuspend-user.ts`:
        // `credential` + `write:admin:suspend-user` / `write:admin:unsuspend-user`
        // + the moderator role; `res` is void → 204.) The moderator-target
        // refusal (upstream's own `cannot suspend moderator account`,
        // `suspend-user.ts:46`), the `NO_SUCH_USER` miss, the `suspend`/
        // `unsuspend` log row, and the bounded sweep jobs live in the
        // application command — one transaction (§2.3). DIVERGENCE: the
        // refusal is applied to `unsuspend-user` too (guide §3.1 blanket
        // rule); upstream `unsuspend-user.ts` has no check there, and both
        // upstream refusals throw untyped `Error`s (`500 INTERNAL_ERROR`
        // via `ApiCallService.ts:#onExecError`) while pinetree answers
        // `400 INVALID_PARAM` carrying the same message.
        "admin/suspend-user" | "admin/unsuspend-user" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            let suspend = endpoint == "admin/suspend-user";
            caller.require_permission(if suspend {
                "write:admin:suspend-user"
            } else {
                "write:admin:unsuspend-user"
            })?;
            require_moderator(&caller)?;
            let user_id = required_str(&obj, "userId")?;
            let result = if suspend {
                state
                    .services
                    .suspend_user(&caller.account.id, &user_id)
            } else {
                state
                    .services
                    .unsuspend_user(&caller.account.id, &user_id)
            };
            result.map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        // --- Batch N WS5: abuse reports (Batch M WP4 contract, §24) --------
        // Upstream: `users/report-abuse` (`write:report-abuse`) + the four
        // `admin/*` endpoints (moderator role on top of the scope check).
        // `report_abuse` enforces self/admin refusal in the op; resolve and
        // forward commit state + log row + jobs in one transaction; the
        // double-forward and local-target refusals live in the op too.
        "users/report-abuse" => {
            check_allowed_fields(&obj, &["userId", "comment"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:report-abuse")?;
            state.rate_limiter.check(&rate_key, "users/report-abuse")?;
            let user_id = required_str(&obj, "userId")?;
            let comment = required_str(&obj, "comment")?;
            let report = state
                .services
                .report_abuse(&caller.account.id, &user_id, &comment)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(
                pack_abuse_report(&report, Some(&caller.account.id), &state.services)
                    .map_err(ApiError::from_port_error)?,
            )
            .into_response())
        }

        "admin/abuse-user-reports" => {
            check_allowed_fields(
                &obj,
                &[
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "state",
                    "reporterOrigin",
                    "targetUserOrigin",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:admin:abuse-user-reports")?;
            require_moderator(&caller)?;
            let cursor = parse_cursor(&obj)?;
            let filter = AbuseReportFilter {
                state: opt_string(&obj, "state")?,
                reporter_origin: opt_string(&obj, "reporterOrigin")?
                    .unwrap_or_else(|| "combined".to_string()),
                target_origin: opt_string(&obj, "targetUserOrigin")?
                    .unwrap_or_else(|| "combined".to_string()),
            };
            let rows = state
                .services
                .list_abuse_reports(filter, &cursor)
                .map_err(ApiError::from_port_error)?;
            let viewer_id = Some(caller.account.id.as_str());
            let mut out = Vec::with_capacity(rows.len());
            for row in &rows {
                out.push(
                    pack_abuse_report(row, viewer_id, &state.services)
                        .map_err(ApiError::from_port_error)?,
                );
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "admin/resolve-abuse-user-report" => {
            check_allowed_fields(&obj, &["reportId", "resolvedAs"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:resolve-abuse-user-report")?;
            require_moderator(&caller)?;
            let report_id = required_str(&obj, "reportId")?;
            let resolved_as = match opt_string(&obj, "resolvedAs")? {
                None => None,
                Some(s) => match AbuseReportResolution::parse(&s) {
                    Some(r) => Some(r),
                    None => return Err(ApiError::invalid_param("resolvedAs", "must be accept, reject, or null")),
                },
            };
            state
                .services
                .resolve_abuse_report(&caller.account.id, &report_id, resolved_as)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "admin/update-abuse-user-report" => {
            check_allowed_fields(&obj, &["reportId", "moderationNote"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:resolve-abuse-user-report")?;
            require_moderator(&caller)?;
            let report_id = required_str(&obj, "reportId")?;
            // Upstream: `moderationNote` optional (absent clears to null;
            // pinetree stores `""`).
            let note = opt_string(&obj, "moderationNote")?.unwrap_or_default();
            state
                .services
                .update_abuse_report_note(&caller.account.id, &report_id, note)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "admin/forward-abuse-user-report" => {
            check_allowed_fields(&obj, &["reportId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:resolve-abuse-user-report")?;
            require_moderator(&caller)?;
            let report_id = required_str(&obj, "reportId")?;
            state
                .services
                .forward_abuse_report(&caller.account.id, &report_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        // --- Batch N WS1: custom emoji (listing + admin management) --------
        // Upstream `emojis` (public, `{}`), `emoji {name}`, `admin/emoji/
        // add|update|delete|list` (`canManageCustomEmojis` + kind
        // `write:admin:emoji` / `read:admin:emoji`). Pinetree has no
        // custom-role policies, so the role gate is the moderator role
        // (documented). Every mutation commits state + moderation-log row in
        // one transaction; bulk ops / import-zip / copy / list-remote stay
        // out (guide §8).
        "emojis" => {
            check_allowed_fields(&obj, &[])?;
            let rows = state
                .services
                .emojis
                .list_emojis(None, &TimelineCursor { limit: 10_000, ..Default::default() })
                .map_err(ApiError::from_port_error)?;
            // Upstream packs `EmojiSimple` ordered by category, name.
            let mut rows = rows;
            rows.sort_by(|a, b| {
                a.category.cmp(&b.category).then_with(|| a.shortcode.cmp(&b.shortcode))
            });
            let packed: Vec<Value> = rows
                .iter()
                .map(|e| pack_emoji_simple(e, &state.services))
                .collect();
            Ok(Json(serde_json::json!({ "emojis": packed })).into_response())
        }

        "emoji" => {
            check_allowed_fields(&obj, &["name"])?;
            let name = required_str(&obj, "name")?;
            match state.services.emojis.get_emoji_by_name(&name) {
                Ok(Some(emoji)) => Ok(Json(pack_emoji_detailed(&emoji, &state.services)).into_response()),
                Ok(None) => Err(ApiError::from_port_error(PortError::NotFound("NO_SUCH_EMOJI".to_string()))),
                Err(e) => Err(ApiError::from_port_error(e)),
            }
        }

        "admin/emoji/add" => {
            check_allowed_fields(
                &obj,
                &[
                    "name",
                    "fileId",
                    "category",
                    "aliases",
                    "license",
                    "isSensitive",
                    "localOnly",
                    "roleIdsThatCanBeUsedThisEmojiAsReaction",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:emoji")?;
            require_moderator(&caller)?;
            state.rate_limiter.check(&rate_key, "admin/emoji/add")?;
            let name = required_str(&obj, "name")?;
            if name.is_empty()
                || name.chars().count() > 128
                || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(ApiError::invalid_param("name", "must match ^[a-zA-Z0-9_]+$"));
            }
            let file_id = required_str(&obj, "fileId")?;
            let emoji = state
                .services
                .create_emoji(
                    &caller.account.id,
                    &name,
                    &file_id,
                    opt_string(&obj, "category")?.filter(|s| !s.is_empty()),
                    parse_string_array(&obj, "aliases", Some(32))?.unwrap_or_default(),
                    opt_string(&obj, "license")?.filter(|s| !s.is_empty()),
                    update_bool_field(&obj, "isSensitive")?.unwrap_or(false),
                    update_bool_field(&obj, "localOnly")?.unwrap_or(false),
                    parse_string_array(&obj, "roleIdsThatCanBeUsedThisEmojiAsReaction", None)?.unwrap_or_default(),
                )
                .map_err(|e| match e {
                    PortError::NotFound(code) if code == "NO_SUCH_FILE" => ApiError::emoji_no_such_file(),
                    other => ApiError::from_port_error(other),
                })?;
            Ok(Json(pack_emoji_detailed(&emoji, &state.services)).into_response())
        }

        "admin/emoji/update" => {
            check_allowed_fields(
                &obj,
                &[
                    "id",
                    "name",
                    "fileId",
                    "category",
                    "aliases",
                    "license",
                    "isSensitive",
                    "localOnly",
                    "roleIdsThatCanBeUsedThisEmojiAsReaction",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:emoji")?;
            require_moderator(&caller)?;
            state.rate_limiter.check(&rate_key, "admin/emoji/update")?;
            // Upstream `anyOf`: at least one of `id` / `name` addresses the
            // row; both may be given, in which case a different `name`
            // renames (`doNameUpdate` in `CustomEmojiService.update`).
            let (id, name) = match (obj.get("id"), obj.get("name")) {
                (None, None) => {
                    return Err(ApiError::invalid_param("id", "either id or name is required"));
                }
                (id, name) => (
                    id.and_then(Value::as_str).map(str::to_string),
                    name.and_then(Value::as_str).map(str::to_string),
                ),
            };
            // Explicit null resets `category`/`license` (upstream parity);
            // absent leaves them unchanged.
            let category = match obj.get("category") {
                None => None,
                Some(Value::Null) => Some(None),
                Some(Value::String(s)) => Some(Some(s.clone())),
                Some(_) => return Err(ApiError::invalid_param("category", "must be a string or null")),
            };
            let license = match obj.get("license") {
                None => None,
                Some(Value::Null) => Some(None),
                Some(Value::String(s)) => Some(Some(s.clone())),
                Some(_) => return Err(ApiError::invalid_param("license", "must be a string or null")),
            };
            let emoji = state
                .services
                .update_emoji(
                    &caller.account.id,
                    id.as_deref(),
                    name.as_deref(),
                    opt_string(&obj, "fileId")?.as_deref(),
                    category,
                    parse_string_array(&obj, "aliases", Some(32))?,
                    license,
                    update_bool_field(&obj, "isSensitive")?,
                    update_bool_field(&obj, "localOnly")?,
                    parse_string_array(&obj, "roleIdsThatCanBeUsedThisEmojiAsReaction", None)?,
                )
                .map_err(|e| match e {
                    PortError::NotFound(code) if code == "NO_SUCH_FILE" => ApiError::emoji_no_such_file(),
                    other => ApiError::from_port_error(other),
                })?;
            Ok(Json(pack_emoji_detailed(&emoji, &state.services)).into_response())
        }

        "admin/emoji/delete" => {
            check_allowed_fields(&obj, &["id"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:emoji")?;
            require_moderator(&caller)?;
            state.rate_limiter.check(&rate_key, "admin/emoji/delete")?;
            let id = required_str(&obj, "id")?;
            state
                .services
                .delete_emoji(&caller.account.id, &id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "admin/emoji/list" => {
            check_allowed_fields(
                &obj,
                &["query", "limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:admin:emoji")?;
            require_moderator(&caller)?;
            let cursor = TimelineCursor {
                since_id: opt_string(&obj, "sinceId")?,
                until_id: opt_string(&obj, "untilId")?,
                since_date: opt_i64(&obj, "sinceDate")?,
                until_date: opt_i64(&obj, "untilDate")?,
                limit: get_limit_with_default(&obj, 10)?,
            };
            let rows = state
                .services
                .emojis
                .list_emojis(opt_string(&obj, "query")?.as_deref(), &cursor)
                .map_err(ApiError::from_port_error)?;
            let packed: Vec<Value> = rows
                .iter()
                .map(|e| pack_emoji_detailed(e, &state.services))
                .collect();
            Ok(Json(Value::Array(packed)).into_response())
        }

        // --- Batch N WS4: antennas (guide WP15) ---------------------------
        // `antennas/list|show|create|update|delete|notes|remove-note` +
        // real `i/export-antennas` / `i/import-antennas` + the `antenna`
        // stream channel. DIVERGENCEs (manifest §29): `home` = own +
        // followed notes; no auto-deactivation (instance cap 5); `src=list`
        // refused (WP16 pending — `NO_SUCH_USER_LIST`, no lists exist).
        "antennas/list" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let rows = state
                .services
                .antennas
                .antennas_by_owner(&caller.account.id)
                .map_err(ApiError::from_port_error)?;
            let packed: Vec<Value> = rows.iter().map(|a| pack_antenna(a, &state.services)).collect();
            Ok(Json(Value::Array(packed)).into_response())
        }

        "antennas/show" => {
            check_allowed_fields(&obj, &["antennaId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let antenna_id = required_str(&obj, "antennaId")?;
            match state.services.antennas.get_antenna(&caller.account.id, &antenna_id) {
                Ok(Some(a)) => Ok(Json(pack_antenna(&a, &state.services)).into_response()),
                Ok(None) => Err(ApiError::from_port_error(PortError::NotFound("NO_SUCH_ANTENNA".to_string()))),
                Err(e) => Err(ApiError::from_port_error(e)),
            }
        }

        "antennas/create" => {
            check_allowed_fields(
                &obj,
                &[
                    "name",
                    "src",
                    "userListId",
                    "keywords",
                    "excludeKeywords",
                    "users",
                    "caseSensitive",
                    "localOnly",
                    "excludeBots",
                    "withReplies",
                    "withFile",
                    "excludeNotesInSensitiveChannel",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "antennas/create")?;
            let antenna = state
                .services
                .create_antenna(
                    &caller.account.id,
                    required_string(&obj, "name", 1, 100)?,
                    required_antenna_src(&obj)?,
                    opt_string(&obj, "userListId")?,
                    required_str_array_array(&obj, "keywords")?,
                    required_str_array_array(&obj, "excludeKeywords")?,
                    required_str_array(&obj, "users")?,
                    required_bool(&obj, "caseSensitive")?,
                    opt_bool(&obj, "localOnly", false)?,
                    opt_bool(&obj, "excludeBots", false)?,
                    required_bool(&obj, "withReplies")?,
                    required_bool(&obj, "withFile")?,
                )
                .map_err(ApiError::from_port_error)?;
            Ok(Json(pack_antenna(&antenna, &state.services)).into_response())
        }

        "antennas/update" => {
            check_allowed_fields(
                &obj,
                &[
                    "antennaId",
                    "name",
                    "src",
                    "userListId",
                    "keywords",
                    "excludeKeywords",
                    "users",
                    "caseSensitive",
                    "localOnly",
                    "excludeBots",
                    "withReplies",
                    "withFile",
                    "excludeNotesInSensitiveChannel",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "antennas/update")?;
            let antenna = state
                .services
                .update_antenna(
                    &caller.account.id,
                    &required_str(&obj, "antennaId")?,
                    opt_bounded_string(&obj, "name", 1, 100)?,
                    opt_antenna_src(&obj)?,
                    opt_nullable_string(&obj, "userListId")?,
                    opt_str_array_array(&obj, "keywords")?,
                    opt_str_array_array(&obj, "excludeKeywords")?,
                    opt_str_array(&obj, "users")?,
                    opt_bool_opt(&obj, "caseSensitive")?,
                    opt_bool_opt(&obj, "localOnly")?,
                    opt_bool_opt(&obj, "excludeBots")?,
                    opt_bool_opt(&obj, "withReplies")?,
                    opt_bool_opt(&obj, "withFile")?,
                )
                .map_err(ApiError::from_port_error)?;
            Ok(Json(pack_antenna(&antenna, &state.services)).into_response())
        }

        "antennas/delete" => {
            check_allowed_fields(&obj, &["antennaId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "antennas/delete")?;
            let antenna_id = required_str(&obj, "antennaId")?;
            state
                .services
                .delete_antenna(&caller.account.id, &antenna_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "antennas/notes" => {
            check_allowed_fields(
                &obj,
                &["antennaId", "limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let antenna_id = required_str(&obj, "antennaId")?;
            let cursor = TimelineCursor {
                since_id: opt_string(&obj, "sinceId")?,
                until_id: opt_string(&obj, "untilId")?,
                since_date: opt_i64(&obj, "sinceDate")?,
                until_date: opt_i64(&obj, "untilDate")?,
                limit: get_limit_with_default(&obj, 10)?,
            };
            let notes = state
                .services
                .antenna_notes_for(&caller.account.id, &antenna_id, &cursor)
                .map_err(ApiError::from_port_error)?;
            let viewer = Some(caller.account.id.as_str());
            let mut out = Vec::with_capacity(notes.len());
            for note in &notes {
                out.push(
                    pack_note(note, viewer, &state.services, 0)
                        .map_err(ApiError::from_port_error)?,
                );
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "antennas/remove-note" => {
            check_allowed_fields(&obj, &["antennaId", "noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            let antenna_id = required_str(&obj, "antennaId")?;
            let note_id = required_str(&obj, "noteId")?;
            // Owner-scoped like every other antenna read; unknown rows are
            // `NO_SUCH_ANTENNA` (upstream parity).
            if state.services.antennas.get_antenna(&caller.account.id, &antenna_id)
                .map_err(ApiError::from_port_error)?.is_none()
            {
                return Err(ApiError::from_port_error(PortError::NotFound("NO_SUCH_ANTENNA".to_string())));
            }
            state
                .services
                .antenna_notes
                .remove_antenna_note(&antenna_id, &note_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        // --- Batch M WP2: registration policy + invite codes -------------
        // (upstream `invite/*` + `admin/invite/*`; §9.1 default policy:
        // moderator-only creation — upstream `canInvite`, default false).
        // `admin/invite/create` is upstream's moderator endpoint and logs
        // `createInvitation`; `invite/create` is silent there upstream too.

        "invite/create" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:invite-codes")?;
            require_moderator(&caller)?;
            let codes = state
                .services
                .create_invites(
                    &caller.account.id,
                    1,
                    None,
                    state.services.clock.now_ms(),
                    false,
                )
                .map_err(ApiError::from_port_error)?;
            let packed = pack_invite_codes(&state, &codes)?;
            // Single object upstream (`res: ref: InviteCode`).
            let first = packed
                .as_array()
                .and_then(|rows| rows.first())
                .cloned()
                .unwrap_or(Value::Null);
            Ok(Json(first).into_response())
        }

        "invite/list" => {
            check_allowed_fields(
                &obj,
                &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:invite-codes")?;
            require_moderator(&caller)?;
            let filter = InviteListFilter {
                since_id: opt_string(&obj, "sinceId")?,
                until_id: opt_string(&obj, "untilId")?,
                since_date_ms: opt_i64(&obj, "sinceDate")?,
                until_date_ms: opt_i64(&obj, "untilDate")?,
                limit: get_limit_with_default(&obj, 30)?,
                ..Default::default()
            };
            let invites = state
                .services
                .list_my_invites(&caller.account.id, filter)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(pack_invite_codes(&state, &invites)?).into_response())
        }

        "invite/delete" => {
            check_allowed_fields(&obj, &["inviteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:invite-codes")?;
            require_moderator(&caller)?;
            let invite_id = required_str(&obj, "inviteId")?;
            state
                .services
                .delete_invite(
                    &invite_id,
                    &caller.account.id,
                    caller.account.has_moderator_powers(),
                )
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "invite/limit" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:invite-codes")?;
            require_moderator(&caller)?;
            let remaining = state.services.invite_limit(&caller.account.id);
            Ok(Json(serde_json::json!({ "remaining": remaining })).into_response())
        }

        // `admin/invite/create` (upstream `requireModerator`): `count`
        // codes with an optional `expiresAt`, logged as one
        // `createInvitation` row in the create transaction.
        "admin/invite/create" => {
            check_allowed_fields(&obj, &["count", "expiresAt"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:invite-codes")?;
            require_moderator(&caller)?;
            let count = opt_usize(&obj, "count")?.unwrap_or(1);
            if count == 0 || count > 100 {
                return Err(ApiError::invalid_param(
                    "count",
                    "must be an integer between 1 and 100",
                ));
            }
            let expires_at_ms = match obj.get("expiresAt") {
                None | Some(Value::Null) => None,
                Some(Value::String(raw)) => Some(parse_invite_expires_at(raw)?),
                Some(_) => {
                    return Err(ApiError::invalid_param("expiresAt", "must be a string"));
                }
            };
            let codes = state
                .services
                .create_invites(
                    &caller.account.id,
                    count,
                    expires_at_ms,
                    state.services.clock.now_ms(),
                    true,
                )
                .map_err(ApiError::from_port_error)?;
            Ok(Json(pack_invite_codes(&state, &codes)?).into_response())
        }

        // `admin/invite/list` (upstream `requireModerator`): `type` filter
        // (`all`/`unused`/`used`/`expired`), `sort`, `limit`, `offset`.
        "admin/invite/list" => {
            check_allowed_fields(&obj, &["limit", "offset", "type", "sort"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:admin:invite-codes")?;
            require_moderator(&caller)?;
            let type_filter = match opt_string(&obj, "type")?.as_deref() {
                None | Some("all") => InviteTypeFilter::All,
                Some("unused") => InviteTypeFilter::Unused,
                Some("used") => InviteTypeFilter::Used,
                Some("expired") => InviteTypeFilter::Expired,
                Some(_) => {
                    return Err(ApiError::invalid_param(
                        "type",
                        "must be one of unused, used, expired, all",
                    ));
                }
            };
            let sort = match opt_string(&obj, "sort")?.as_deref() {
                None | Some("+createdAt") => InviteSort::CreatedDesc,
                Some("-createdAt") => InviteSort::CreatedAsc,
                Some("+usedAt") => InviteSort::UsedDesc,
                Some("-usedAt") => InviteSort::UsedAsc,
                Some(_) => {
                    return Err(ApiError::invalid_param(
                        "sort",
                        "must be one of +createdAt, -createdAt, +usedAt, -usedAt",
                    ));
                }
            };
            let filter = InviteListFilter {
                filter: type_filter,
                sort,
                limit: get_limit_with_default(&obj, 30)?,
                offset: opt_usize(&obj, "offset")?.unwrap_or(0),
                ..Default::default()
            };
            let invites = state
                .services
                .list_all_invites(filter)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(pack_invite_codes(&state, &invites)?).into_response())
        }

        // WP2 `admin/meta` + `admin/update-meta` (upstream files of the
        // same names, both `requireAdmin`). WP2 ships the stored-settings
        // subset (`disableRegistration`, `blockedHosts`); WP6 completes
        // the admin read view. `blockedHosts` is STORED from here and
        // enforced from WP5.
        "admin/meta" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:admin:meta")?;
            require_admin(&caller)?;
            let mut meta = pack_meta(&state.services, true).map_err(ApiError::from_port_error)?;
            let settings = state
                .services
                .instance_settings
                .get()
                .map_err(ApiError::from_port_error)?;
            if let Some(map) = meta.as_object_mut() {
                map.insert(
                    "blockedHosts".to_string(),
                    serde_json::json!(settings.blocked_hosts),
                );
            }
            Ok(Json(meta).into_response())
        }

        "admin/update-meta" => {
            check_allowed_fields(&obj, &["disableRegistration", "blockedHosts"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:admin:meta")?;
            require_admin(&caller)?;
            let disable_registration = match obj.get("disableRegistration") {
                None | Some(Value::Null) => None,
                Some(Value::Bool(b)) => Some(*b),
                Some(_) => {
                    return Err(ApiError::invalid_param(
                        "disableRegistration",
                        "must be a boolean",
                    ));
                }
            };
            let blocked_hosts = match obj.get("blockedHosts") {
                None | Some(Value::Null) => None,
                Some(Value::Array(items)) => {
                    let mut hosts = Vec::with_capacity(items.len());
                    for item in items {
                        match item {
                            Value::String(host) => hosts.push(host.clone()),
                            _ => {
                                return Err(ApiError::invalid_param(
                                    "blockedHosts",
                                    "must be an array of strings",
                                ));
                            }
                        }
                    }
                    Some(hosts)
                }
                Some(_) => {
                    return Err(ApiError::invalid_param(
                        "blockedHosts",
                        "must be an array of strings",
                    ));
                }
            };
            state
                .services
                .update_instance_settings(
                    &caller.account.id,
                    disable_registration,
                    blocked_hosts,
                    state.services.clock.now_ms(),
                )
                .map_err(ApiError::from_port_error)?;
            // Upstream has a void res (204), like the export endpoints.
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        // §13 profile import/export (Misskey `i/export-*` + `i/import-*`
        // parity, backend only — see the endpoint manifest §20 for payload
        // shapes and DIVERGENCEs). Every endpoint takes a credential
        // (`secure:true` parity: pinetree has no secure mode, so credential
        // + its own 1/hr bucket) and answers `204 No Content` immediately
        // (upstream returns void); the `Export`/`Import` job does the work
        // off-request and exports report back via `exportCompleted`.
        // `requiredRolePolicy` (`canImport*`) and `prohibitMoved` have no
        // pinetree equivalent (no roles/moved infra) and are not gated.
        "i/export-following" => {
            check_allowed_fields(&obj, &["excludeMuting", "excludeInactive"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/export-following")?;
            let exclude_muting = opt_bool(&obj, "excludeMuting", false)?;
            let exclude_inactive = opt_bool(&obj, "excludeInactive", false)?;
            state
                .services
                .enqueue_export(
                    &caller.account.id,
                    ExportEntity::Following,
                    exclude_muting,
                    exclude_inactive,
                )
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/export-blocking" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/export-blocking")?;
            state
                .services
                .enqueue_export(&caller.account.id, ExportEntity::Blocking, false, false)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/export-mute" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/export-mute")?;
            state
                .services
                .enqueue_export(&caller.account.id, ExportEntity::Muting, false, false)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/export-notes" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/export-notes")?;
            state
                .services
                .enqueue_export(&caller.account.id, ExportEntity::Notes, false, false)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/export-user-lists" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/export-user-lists")?;
            state
                .services
                .enqueue_export(&caller.account.id, ExportEntity::UserLists, false, false)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/export-antennas" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/export-antennas")?;
            state
                .services
                .enqueue_export(&caller.account.id, ExportEntity::Antennas, false, false)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/export-clips" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/export-clips")?;
            state
                .services
                .enqueue_export(&caller.account.id, ExportEntity::Clips, false, false)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/export-favorites" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/export-favorites")?;
            state
                .services
                .enqueue_export(&caller.account.id, ExportEntity::Favorites, false, false)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/import-following" => {
            check_allowed_fields(&obj, &["fileId", "withReplies"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/import-following")?;
            let file_id = required_str(&obj, "fileId")?;
            // DIVERGENCE: upstream `withReplies` is optional with no
            // default (undefined flows into the relationship job);
            // pinetree booleans need a default — `true` matches the
            // `following/create` default. Row-level `withReplies=` still
            // overrides per row.
            let with_replies = opt_bool(&obj, "withReplies", true)?;
            if let Err(err) = state.services.check_import_file(&caller.account.id, &file_id) {
                return Err(map_import_file_error(err, "i/import-following"));
            }
            state
                .services
                .enqueue_import(
                    &caller.account.id,
                    ImportEntity::Following,
                    &file_id,
                    Some(with_replies),
                )
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/import-blocking" => {
            check_allowed_fields(&obj, &["fileId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/import-blocking")?;
            let file_id = required_str(&obj, "fileId")?;
            if let Err(err) = state.services.check_import_file(&caller.account.id, &file_id) {
                return Err(map_import_file_error(err, "i/import-blocking"));
            }
            state
                .services
                .enqueue_import(&caller.account.id, ImportEntity::Blocking, &file_id, None)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/import-muting" => {
            check_allowed_fields(&obj, &["fileId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/import-muting")?;
            let file_id = required_str(&obj, "fileId")?;
            if let Err(err) = state.services.check_import_file(&caller.account.id, &file_id) {
                return Err(map_import_file_error(err, "i/import-muting"));
            }
            state
                .services
                .enqueue_import(&caller.account.id, ImportEntity::Muting, &file_id, None)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/import-user-lists" => {
            check_allowed_fields(&obj, &["fileId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/import-user-lists")?;
            let file_id = required_str(&obj, "fileId")?;
            if let Err(err) = state.services.check_import_file(&caller.account.id, &file_id) {
                return Err(map_import_file_error(err, "i/import-user-lists"));
            }
            state
                .services
                .enqueue_import(&caller.account.id, ImportEntity::UserLists, &file_id, None)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/import-antennas" => {
            check_allowed_fields(&obj, &["fileId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/import-antennas")?;
            let file_id = required_str(&obj, "fileId")?;
            // Upstream (`import-antennas.ts:77-84`) has no size cap and
            // reads + parses the file in the endpoint, so this arm does
            // not use `check_import_file`: missing/foreign → `noSuchFile`,
            // empty → `emptyFile` (verbatim UUIDs).
            let meta = match state
                .services
                .media
                .get(&file_id)
                .map_err(ApiError::from_port_error)?
            {
                Some(meta) if meta.owner_id.as_deref() == Some(caller.account.id.as_str()) => meta,
                _ => return Err(ApiError::import_no_such_file("i/import-antennas")),
            };
            if meta.size_bytes == 0 {
                return Err(ApiError::import_empty_file("i/import-antennas"));
            }
            // Upstream (`import-antennas.ts:77-84`) has no size cap and
            // reads + parses the file in the endpoint, so this arm does
            // not use `check_import_file`: missing/foreign → `noSuchFile`,
            // empty → `emptyFile` (verbatim UUIDs). DIVERGENCE: upstream
            // `JSON.parse`s here (a malformed file throws → HTTP 500);
            // pinetree answers malformed JSON with `INVALID_PARAM`.
            let bytes = match state
                .services
                .blobs
                .get(&meta.blob_key)
                .map_err(ApiError::from_port_error)?
            {
                Some(bytes) => bytes,
                None => return Err(ApiError::import_no_such_file("i/import-antennas")),
            };
            let parsed: Value = serde_json::from_slice(&bytes).map_err(|_| {
                ApiError::invalid_param("fileId", "file must contain a JSON array of antennas")
            })?;
            let batch_len = match parsed.as_array() {
                Some(entries) => entries.len(),
                None => {
                    return Err(ApiError::invalid_param(
                        "fileId",
                        "file must contain a JSON array of antennas",
                    ));
                }
            };
            // Upstream pre-check (`import-antennas.ts:81-84`): current count
            // + batch length at the limit refuses with `TOO_MANY_ANTENNAS`
            // (import-endpoint UUID `600917d4-…`).
            let current = state
                .services
                .antennas
                .antennas_by_owner(&caller.account.id)
                .map_err(ApiError::from_port_error)?
                .len();
            if current + batch_len >= crate::domain::antenna::DEFAULT_ANTENNA_LIMIT {
                return Err(ApiError::import_too_many_antennas());
            }
            state
                .services
                .enqueue_import(&caller.account.id, ImportEntity::Antennas, &file_id, None)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "following/create" => {
            check_allowed_fields(&obj, &["userId", "withReplies"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:following")?;
            state.rate_limiter.check(&rate_key, "following/create")?;
            let user_id = required_str(&obj, "userId")?;
            let with_replies = opt_bool(&obj, "withReplies", true)?;
            state
                .services
                .follow(&caller.account.id, &user_id, with_replies)
                .map_err(ApiError::from_port_error)?;
            let followee = state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(pack_user_lite(
                &followee,
                &state.services.public_url,
                state.services.clock.now_ms(),
            ))
            .into_response())
        }

        "following/delete" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:following")?;
            state.rate_limiter.check(&rate_key, "following/delete")?;
            let user_id = required_str(&obj, "userId")?;
            state
                .services
                .unfollow(&caller.account.id, &user_id)
                .map_err(ApiError::from_port_error)?;
            let followee = state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(pack_user_lite(
                &followee,
                &state.services.public_url,
                state.services.clock.now_ms(),
            ))
            .into_response())
        }

        "following/requests/list" => {
            check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let reqs = state
                .services
                .follows
                .follow_requests_to(&caller.account.id, None, 100)
                .map_err(ApiError::from_port_error)?;
            let now = state.services.clock.now_ms();
            let mut out = Vec::new();
            for r in reqs {
                let (Some(f), Some(t)) = (
                    state.services.accounts.find_by_id(&r.follower_id).map_err(ApiError::from_port_error)?,
                    state.services.accounts.find_by_id(&r.followee_id).map_err(ApiError::from_port_error)?,
                ) else {
                    continue;
                };
                out.push(serde_json::json!({
                    "id": r.id,
                    "follower": pack_user_lite(&f, &state.services.public_url, now),
                    "followee": pack_user_lite(&t, &state.services.public_url, now),
                }));
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "following/requests/accept" | "following/requests/reject" | "following/requests/cancel" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:following")?;
            state.rate_limiter.check(&rate_key, endpoint.as_str())?;
            let user_id = required_str(&obj, "userId")?;
            let me = caller.account.id.clone();
            match endpoint.as_str() {
                "following/requests/accept" => state.services.accept_follow_request(&me, &user_id).map(|_| ()),
                "following/requests/reject" => state.services.reject_follow_request(&me, &user_id),
                _ => state.services.cancel_follow_request(&me, &user_id),
            }
            .map_err(ApiError::from_port_error)?;
            if endpoint.as_str() == "following/requests/cancel" {
                let followee = state
                    .services
                    .users_show_by_id(&user_id)
                    .map_err(ApiError::from_port_error)?;
                return Ok(Json(pack_user_lite(
                    &followee,
                    &state.services.public_url,
                    state.services.clock.now_ms(),
                ))
                .into_response());
            }
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "notes/timeline" => {
            check_allowed_fields(
                &obj,
                &[
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "allowPartial",
                    "includeMyRenotes",
                    "includeRenotedMyNotes",
                    "includeLocalRenotes",
                    "withFiles",
                    "withRenotes",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let cursor = parse_cursor(&obj)?;
            let with_files = opt_bool(&obj, "withFiles", false)?;
            let with_renotes = opt_bool(&obj, "withRenotes", true)?;
            let notes = state
                .services
                .home_timeline(&caller.account.id, &cursor, with_files, with_renotes)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, Some(&caller.account.id))
        }

        "notes/local-timeline" => {
            check_allowed_fields(
                &obj,
                &[
                    "withFiles",
                    "withRenotes",
                    "withReplies",
                    "limit",
                    "sinceId",
                    "untilId",
                    "allowPartial",
                    "sinceDate",
                    "untilDate",
                ],
            )?;
            let cursor = parse_cursor(&obj)?;
            let with_files = opt_bool(&obj, "withFiles", false)?;
            let with_renotes = opt_bool(&obj, "withRenotes", true)?;
            let with_replies = opt_bool(&obj, "withReplies", false)?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let notes = state
                .services
                .local_timeline(viewer_id, &cursor, with_files, with_renotes, with_replies)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "notes/global-timeline" => {
            // Misskey `notes/global-timeline` paramDef: withFiles,
            // withRenotes, limit, sinceId, untilId, sinceDate, untilDate —
            // no auth, no withReplies. `allowPartial` is accepted and
            // ignored like on the other timeline endpoints (Misskey's ajv
            // validation ignores unknown params; ours rejects them, so
            // list the harmless one explicitly).
            check_allowed_fields(
                &obj,
                &[
                    "withFiles",
                    "withRenotes",
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "allowPartial",
                ],
            )?;
            let cursor = parse_cursor(&obj)?;
            let with_files = opt_bool(&obj, "withFiles", false)?;
            let with_renotes = opt_bool(&obj, "withRenotes", true)?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let notes = state
                .services
                .global_timeline(viewer_id, &cursor, with_files, with_renotes)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "notes/hybrid-timeline" => {
            check_allowed_fields(
                &obj,
                &[
                    "withFiles",
                    "withRenotes",
                    "withReplies",
                    "limit",
                    "sinceId",
                    "untilId",
                    "allowPartial",
                    "sinceDate",
                    "untilDate",
                    "includeMyRenotes",
                    "includeRenotedMyNotes",
                    "includeLocalRenotes",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let cursor = parse_cursor(&obj)?;
            let with_files = opt_bool(&obj, "withFiles", false)?;
            let with_renotes = opt_bool(&obj, "withRenotes", true)?;
            let with_replies = opt_bool(&obj, "withReplies", false)?;
            let notes = state
                .services
                .hybrid_timeline(
                    &caller.account.id,
                    &cursor,
                    with_files,
                    with_renotes,
                    with_replies,
                )
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, Some(&caller.account.id))
        }

        "notes/reactions" => {
            check_allowed_fields(
                &obj,
                &[
                    "noteId",
                    "type",
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                ],
            )?;
            let note_id = required_str(&obj, "noteId")?;
            let reaction_type = opt_string(&obj, "type")?;
            let cursor = parse_cursor(&obj)?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let rows = state
                .services
                .note_reactions(&note_id, viewer_id, reaction_type.as_deref(), &cursor)
                .map_err(ApiError::from_port_error)?;
            let out: Vec<Value> = rows
                .iter()
                .map(|(reaction, reactor)| {
                    crate::api::misskey::packers::pack_note_reaction(
                        reaction,
                        reactor,
                        &state.services,
                    )
                })
                .collect();
            Ok(Json(Value::Array(out)).into_response())
        }

        "notes/reactions/create" => {
            check_allowed_fields(&obj, &["noteId", "reaction"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:reactions")?;
            let note_id = required_str(&obj, "noteId")?;
            // §6 DIVERGENCE (decided, `docs/misskey-parity-plan.md` §6):
            // upstream requires `reaction`; pinetree leaves it optional and
            // resolves an absent/null param server-side to the reactor's
            // `defaultReaction ?? FALLBACK` (bare ❤).
            let reaction = opt_string(&obj, "reaction")?;
            if matches!(reaction.as_deref(), Some("")) {
                return Err(ApiError::invalid_param("reaction", "this field is required"));
            }
            // Misskey rejects a duplicate reaction on the same note with
            // ALREADY_REACTED, so this endpoint must not upsert.
            state
                .services
                .react_with_default(&caller.account.id, &note_id, reaction.as_deref(), false)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "notes/reactions/delete" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:reactions")?;
            state.rate_limiter.check(&rate_key, "notes/reactions/delete")?;
            let note_id = required_str(&obj, "noteId")?;
            state
                .services
                .unreact(&caller.account.id, &note_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        // Batch N WS2: favorites + note state (Misskey
        // `notes/favorites/create|delete.ts`, `i/favorites.ts`,
        // `notes/state.ts` parity).
        "notes/favorites/create" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:favorites")?;
            state.rate_limiter.check(&rate_key, "notes/favorites/create")?;
            let note_id = required_str(&obj, "noteId")?;
            state
                .services
                .favorite_note(&caller.account.id, &note_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "notes/favorites/delete" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:favorites")?;
            let note_id = required_str(&obj, "noteId")?;
            state
                .services
                .unfavorite_note(&caller.account.id, &note_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/favorites" => {
            check_allowed_fields(
                &obj,
                &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:favorites")?;
            let cursor = parse_list_cursor(&obj)?;
            let favs = state
                .services
                .favorites_for_user(&caller.account.id, &cursor)
                .map_err(ApiError::from_port_error)?;
            let mut out = Vec::with_capacity(favs.len());
            for fav in &favs {
                match state.services.notes.get_note(&fav.note_id) {
                    Ok(Some(note)) if !note.is_deleted => {
                        out.push(pack_note(&note, Some(&caller.account.id), &state.services, 0)
                            .map_err(ApiError::from_port_error)?);
                    }
                    // Gone notes are skipped (upstream `leftJoin`s; invisible
                    // on the wire).
                    _ => continue,
                }
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "notes/state" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let note_id = required_str(&obj, "noteId")?;
            // Unknown or invisible notes are indistinguishable (D8): reuse
            // the favorite-create visibility gate for the existence check.
            let note = match state.services.notes.get_note(&note_id) {
                Ok(Some(n)) if !n.is_deleted => n,
                _ => return Err(ApiError::from_port_error(PortError::NotFound("NO_SUCH_NOTE".to_string()))),
            };
            {
                use crate::domain::policy::{ViewerContext, is_visible};
                let viewer_id = &caller.account.id;
                let follows_author = viewer_id == &note.author_id
                    || state.services.follows.is_following(viewer_id, &note.author_id)
                        .map_err(ApiError::from_port_error)?;
                let viewer = ViewerContext::local_user(viewer_id, follows_author);
                if !is_visible(&note, &viewer) {
                    return Err(ApiError::from_port_error(PortError::NotFound("NO_SUCH_NOTE".to_string())));
                }
            }
            let is_favorited = state
                .services
                .favorites
                .is_favorited(&caller.account.id, &note_id)
                .map_err(ApiError::from_port_error)?;
            // Thread-muting stays out (guide §8): always false.
            Ok(Json(serde_json::json!({
                "isFavorited": is_favorited,
                "isMutedThread": false,
            }))
            .into_response())
        }

        // §4 polls (Misskey `notes/polls/vote.ts` parity:
        // `requireCredential`, kind `write:votes`). Pinetree has no
        // moved-account machinery, so `prohibitMoved` has nothing to hook
        // (same as every other write endpoint).
        "notes/polls/vote" => {
            check_allowed_fields(&obj, &["noteId", "choice"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:votes")?;
            state.rate_limiter.check(&rate_key, "notes/polls/vote")?;
            let note_id = required_str(&obj, "noteId")?;
            let choice = match obj.get("choice") {
                Some(Value::Number(n)) => n.as_u64().and_then(|v| usize::try_from(v).ok()).ok_or_else(
                    || ApiError::invalid_param("choice", "must be an integer"),
                )?,
                _ => return Err(ApiError::invalid_param("choice", "this field is required")),
            };
            state
                .services
                .vote_on_poll(&caller.account.id, &note_id, choice)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/notifications" => {
            check_allowed_fields(
                &obj,
                &[
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "markAsRead",
                    "includeTypes",
                    "excludeTypes",
                ],
            )?;
            let caller = require_caller(caller)?;
            // Misskey kind is `read:notifications` (not `read:account`).
            caller.require_permission("read:notifications")?;
            let query = parse_notification_query(&obj)?;
            if query.matches_nothing {
                if query.mark_as_read {
                    state
                        .services
                        .mark_all_notifications_read(&caller.account.id)
                        .map_err(ApiError::from_port_error)?;
                }
                return Ok(Json(Value::Array(Vec::new())).into_response());
            }
            let notifications = state
                .services
                .notifications
                .list_notifications_filtered(
                    &caller.account.id,
                    &query.cursor,
                    &query.include,
                    &query.exclude,
                )
                .map_err(ApiError::from_port_error)?;
            let packed: Vec<Value> = notifications
                .iter()
                .map(|note| pack_notification(note, &state.services))
                .collect();
            if query.mark_as_read {
                state
                    .services
                    .mark_all_notifications_read(&caller.account.id)
                    .map_err(ApiError::from_port_error)?;
            }
            Ok(Json(Value::Array(packed)).into_response())
        }

        "notifications/mark-all-as-read" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:notifications")?;
            state
                .services
                .mark_all_notifications_read(&caller.account.id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/revoke-token" => {
            check_allowed_fields(&obj, &["token", "tokenId"])?;
            let caller = require_caller(caller)?;
            // Any credential may revoke (Misskey parity); third-party tokens
            // are restricted to their own token below. No rate limit: token
            // revocation is rare and must never lock a user out.
            if obj.get("tokenId").is_some_and(|v| !v.is_null()) {
                // Pinetree tokens are hash-addressed and no token ids are
                // ever issued, so no `tokenId` can resolve: Misskey-parity
                // no-op success for an unknown target (profile §3 deviation,
                // logged in the phase-3 endpoints state log).
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
            let raw = match obj.get("token") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) => Some(s.clone()),
                Some(_) => {
                    return Err(ApiError::invalid_param("token", "must be a string"));
                }
            };
            match raw {
                // Both absent: revoke the caller's own token (profile §3).
                None => {
                    state
                        .services
                        .auth
                        .delete_token_by_hash(&hash_token(&caller.raw_token))
                        .map_err(ApiError::from_port_error)?;
                    Ok(StatusCode::NO_CONTENT.into_response())
                }
                // Misskey parity: an empty `token` is a no-op success.
                Some(token) if token.is_empty() => Ok(StatusCode::NO_CONTENT.into_response()),
                Some(token) => {
                    let hash = hash_token(&token);
                    let record = state
                        .services
                        .auth
                        .get_token_by_hash(&hash)
                        .map_err(ApiError::from_port_error)?;
                    let Some(record) = record else {
                        return Ok(StatusCode::NO_CONTENT.into_response());
                    };
                    if record.account_id != caller.account.id {
                        return Ok(StatusCode::NO_CONTENT.into_response());
                    }
                    if !caller.is_first_party && hash != hash_token(&caller.raw_token) {
                        return Err(ApiError::permission_denied());
                    }
                    state
                        .services
                        .auth
                        .delete_token_by_hash(&hash)
                        .map_err(ApiError::from_port_error)?;
                    Ok(StatusCode::NO_CONTENT.into_response())
                }
            }
        }

        "users/notes" => {
            check_allowed_fields(
                &obj,
                &[
                    "userId",
                    "withReplies",
                    "withRenotes",
                    "withChannelNotes",
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "allowPartial",
                    "withFiles",
                ],
            )?;
            let user_id = required_str(&obj, "userId")?;
            let cursor = parse_cursor(&obj)?;
            let with_replies = opt_bool(&obj, "withReplies", false)?;
            let with_renotes = opt_bool(&obj, "withRenotes", true)?;
            // No channels exist: `withChannelNotes` is accepted and ignored,
            // as is `allowPartial`.
            let with_files = opt_bool(&obj, "withFiles", false)?;
            // Misskey parity: the combination is rejected, not applied.
            if with_replies && with_files {
                return Err(ApiError::from_port_error(PortError::Validation(
                    "BOTH_WITH_REPLIES_AND_WITH_FILES".to_string(),
                )));
            }
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let notes = state
                .services
                .user_notes(
                    &user_id,
                    viewer_id,
                    &cursor,
                    with_replies,
                    with_renotes,
                    with_files,
                )
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "notes/replies" => {
            check_allowed_fields(
                &obj,
                &["noteId", "limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let note_id = required_str(&obj, "noteId")?;
            let cursor = parse_cursor(&obj)?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let notes = state
                .services
                .note_replies(&note_id, viewer_id, &cursor)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "notes/renotes" => {
            check_allowed_fields(
                &obj,
                &["noteId", "limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let note_id = required_str(&obj, "noteId")?;
            let cursor = parse_cursor(&obj)?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let notes = state
                .services
                .note_renotes(&note_id, viewer_id, &cursor)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "notes/children" => {
            check_allowed_fields(
                &obj,
                &["noteId", "limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let note_id = required_str(&obj, "noteId")?;
            let cursor = parse_cursor(&obj)?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let notes = state
                .services
                .note_children(&note_id, viewer_id, &cursor)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "notes/conversation" => {
            check_allowed_fields(&obj, &["noteId", "limit", "offset"])?;
            let note_id = required_str(&obj, "noteId")?;
            let limit = get_limit(&obj)?;
            let offset = opt_usize(&obj, "offset")?.unwrap_or(0);
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let notes = state
                .services
                .note_conversation(&note_id, viewer_id, limit, offset)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "notes/unrenote" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:notes")?;
            // Same cost class as note deletion; no dedicated bucket exists.
            state.rate_limiter.check(&rate_key, "notes/delete")?;
            let note_id = required_str(&obj, "noteId")?;
            state
                .services
                .unrenote(&caller.account.id, &note_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "users/followers" | "users/following" => {
            check_allowed_fields(
                &obj,
                &[
                    "userId",
                    "username",
                    "host",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "limit",
                ],
            )?;
            let account = if let Some(user_id) = obj.get("userId").and_then(Value::as_str) {
                state
                    .services
                    .users_show_by_id(user_id)
                    .map_err(ApiError::from_port_error)?
            } else if let Some(username) = obj.get("username").and_then(Value::as_str) {
                let host = match obj.get("host") {
                    None => {
                        return Err(ApiError::invalid_param(
                            "host",
                            "host is required with username",
                        ));
                    }
                    Some(Value::Null) => None,
                    Some(Value::String(h)) => Some(h.as_str()),
                    Some(_) => {
                        return Err(ApiError::invalid_param(
                            "host",
                            "must be a string or null",
                        ));
                    }
                };
                state
                    .services
                    .users_show_by_username(username, host)
                    .map_err(ApiError::from_port_error)?
            } else {
                return Err(ApiError::invalid_param(
                    "userId",
                    "one of userId or username is required",
                ));
            };
            let cursor = parse_cursor(&obj)?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let follows = if endpoint == "users/followers" {
                state
                    .services
                    .user_followers(&account.id, viewer_id, &cursor)
                    .map_err(ApiError::from_port_error)?
            } else {
                state
                    .services
                    .user_following(&account.id, viewer_id, &cursor)
                    .map_err(ApiError::from_port_error)?
            };
            let mut out = Vec::with_capacity(follows.len());
            for follow in &follows {
                out.push(
                    pack_following(follow, viewer_id, &state.services)
                        .map_err(ApiError::from_port_error)?,
                );
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "users/reactions" => {
            check_allowed_fields(
                &obj,
                &[
                    "userId",
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                ],
            )?;
            let user_id = required_str(&obj, "userId")?;
            let cursor = parse_cursor(&obj)?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let rows = state
                .services
                .user_reactions(&user_id, viewer_id, &cursor)
                .map_err(ApiError::from_port_error)?;
            let target = state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            let mut out = Vec::with_capacity(rows.len());
            for row in &rows {
                out.push(
                    pack_note_reaction_with_note(
                        &row.reaction,
                        &target,
                        &row.note,
                        viewer_id,
                        &state.services,
                    )
                    .map_err(ApiError::from_port_error)?,
                );
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "drive/files" => {
            check_allowed_fields(
                &obj,
                &[
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "folderId",
                    "type",
                    "sort",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:drive")?;
            if let Some(folder) = obj.get("folderId") {
                if !folder.is_null() {
                    return Err(ApiError::invalid_param(
                        "folderId",
                        "folders are unsupported; folderId must be null",
                    ));
                }
            }
            let type_prefix: Option<String> = match opt_string(&obj, "type")? {
                None => None,
                Some(raw) => {
                    if raw.is_empty()
                        || !raw
                            .chars()
                            .all(|c| c.is_ascii_alphabetic() || c == '/' || c == '-' || c == '*')
                    {
                        return Err(ApiError::invalid_param(
                            "type",
                            "must match /^[a-zA-Z/\\-*]+$/",
                        ));
                    }
                    Some(raw.strip_suffix('*').unwrap_or(&raw).to_string())
                }
            };
            match opt_string(&obj, "sort")? {
                None => {}
                Some(sort)
                    if [
                        "+createdAt",
                        "-createdAt",
                        "+name",
                        "-name",
                        "+size",
                        "-size",
                    ]
                    .contains(&sort.as_str()) => {}
                Some(_) => {
                    return Err(ApiError::invalid_param(
                        "sort",
                        "must be one of +createdAt, -createdAt, +name, -name, +size, -size",
                    ));
                }
            }
            let sort = opt_string(&obj, "sort")?;
            let cursor = parse_cursor(&obj)?;
            let files = state
                .services
                .list_drive_files(
                    &caller.account.id,
                    &cursor,
                    type_prefix.as_deref(),
                    sort.as_deref(),
                )
                .map_err(ApiError::from_port_error)?;
            let packed: Vec<Value> = files
                .iter()
                .map(|meta| pack_drive_file(meta, &state.services))
                .collect();
            Ok(Json(Value::Array(packed)).into_response())
        }

        "drive/files/show" => {
            check_allowed_fields(&obj, &["fileId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:drive")?;
            let file_id = required_str(&obj, "fileId")?;
            let meta = state
                .services
                .get_media_for(&caller.account.id, &file_id)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(pack_drive_file(&meta, &state.services)).into_response())
        }

        "drive/files/update" => {
            check_allowed_fields(&obj, &["fileId", "isSensitive", "comment", "name"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:drive")?;
            // Same cost class as uploads; no dedicated bucket exists.
            state.rate_limiter.check(&rate_key, "drive/files/create")?;
            let file_id = required_str(&obj, "fileId")?;
            let name = match obj.get("name") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) => Some(s.clone()),
                Some(_) => {
                    return Err(ApiError::invalid_param("name", "must be a string"));
                }
            };
            let is_sensitive = match obj.get("isSensitive") {
                None | Some(Value::Null) => None,
                Some(Value::Bool(b)) => Some(*b),
                Some(_) => {
                    return Err(ApiError::invalid_param(
                        "isSensitive",
                        "must be a boolean",
                    ));
                }
            };
            let comment = update_string_field(&obj, "comment")?;
            if let Some(Some(text)) = comment.as_ref() {
                if text.chars().count() > 512 {
                    return Err(ApiError::invalid_param(
                        "comment",
                        "must be at most 512 chars",
                    ));
                }
            }
            let meta = state
                .services
                .update_media(
                    &caller.account.id,
                    &file_id,
                    name,
                    is_sensitive,
                    comment,
                )
                .map_err(ApiError::from_port_error)?;
            Ok(Json(pack_drive_file(&meta, &state.services)).into_response())
        }

        "drive/files/delete" => {
            check_allowed_fields(&obj, &["fileId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:drive")?;
            // Same cost class as uploads; no dedicated bucket exists.
            state.rate_limiter.check(&rate_key, "drive/files/create")?;
            let file_id = required_str(&obj, "fileId")?;
            state
                .services
                .delete_media(&caller.account.id, &file_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "notes/search" => {
            check_allowed_fields(
                &obj,
                &[
                    "query",
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "userId",
                    "host",
                    "offset",
                    "rangeStartAt",
                    "rangeEndAt",
                ],
            )?;
            // Misskey parity: public endpoint (`requireCredential: false`).
            // Anonymous viewers only see what `is_visible` allows.
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let query = required_str(&obj, "query")?;
            let mut cursor = parse_cursor(&obj)?;
            // `rangeStartAt/EndAt` are ms timestamps bounding note creation,
            // like `sinceDate/untilDate` (Misskey maps them through id gen).
            if cursor.since_date.is_none() {
                cursor.since_date = opt_i64(&obj, "rangeStartAt")?;
            }
            if cursor.until_date.is_none() {
                cursor.until_date = opt_i64(&obj, "rangeEndAt")?;
            }
            // `offset` applies before `limit` (Misskey parity): widen the
            // window, then skip.
            let offset = opt_usize(&obj, "offset")?.unwrap_or(0);
            cursor.limit = cursor.limit.saturating_add(offset);
            let user_id = opt_string(&obj, "userId")?;
            let host = opt_string(&obj, "host")?;
            let mut notes = state
                .services
                .search_notes(
                    &query,
                    viewer_id,
                    &cursor,
                    user_id.as_deref(),
                    host.as_deref(),
                )
                .map_err(ApiError::from_port_error)?;
            if offset > 0 {
                notes.drain(..offset.min(notes.len()));
            }
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "notes/search-by-tag" => {
            check_allowed_fields(
                &obj,
                &["tag", "limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            // Misskey parity: public endpoint (no `requireCredential`).
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let tag = required_str(&obj, "tag")?;
            let cursor = parse_cursor(&obj)?;
            let notes = state
                .services
                .notes_with_tag(&tag, viewer_id, &cursor)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "ap/show" => {
            check_allowed_fields(&obj, &["uri"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
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
                    pack_note(&note, viewer_id, &state.services, 0)
                        .map_err(ApiError::from_port_error)?;
                return Ok(
                    Json(serde_json::json!({ "type": "Note", "object": packed }))
                        .into_response(),
                );
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
                return Ok(
                    Json(serde_json::json!({ "type": "User", "object": packed }))
                        .into_response(),
                );
            }

            // Remote: try the actor first, then the note.
            if let Ok((account, _)) = fetch_and_upsert_remote_actor(
                &state.services,
                &state.services.federation_http,
                uri.trim(),
            ) {
                let packed = pack_account_for(&account, Some(&caller), &state.services)?;
                return Ok(
                    Json(serde_json::json!({ "type": "User", "object": packed }))
                        .into_response(),
                );
            }
            let no_such_object =
                || ApiError::from_port_error(PortError::NotFound("NO_SUCH_OBJECT".to_string()));
            match fetch_remote_note(
                &state.services,
                &state.services.federation_http,
                uri.trim(),
            ) {
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
                    )
                    {
                        Ok(Some(note)) => {
                            let packed = pack_note(&note, viewer_id, &state.services, 0)
                                .map_err(ApiError::from_port_error)?;
                            Ok(Json(
                                serde_json::json!({ "type": "Note", "object": packed }),
                            )
                            .into_response())
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
                                    let packed =
                                        pack_note(&note, viewer_id, &state.services, 0)
                                            .map_err(ApiError::from_port_error)?;
                                    Ok(Json(
                                        serde_json::json!({ "type": "Note", "object": packed }),
                                    )
                                    .into_response())
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

        "blocking/create" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:blocks")?;
            state.rate_limiter.check(&rate_key, "blocking/create")?;
            let user_id = required_str(&obj, "userId")?;
            // Upstream order: self-check, then user existence, then dup.
            if caller.account.id == user_id {
                return Err(ApiError::from_port_error(PortError::Validation(
                    "BLOCKEE_IS_YOURSELF".to_string(),
                )));
            }
            state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            state
                .services
                .block(&caller.account.id, &user_id)
                .map_err(ApiError::from_port_error)?;
            let blockee = state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(
                pack_user_detailed_not_me(&blockee, Some(&caller.account.id), &state.services)
                    .map_err(ApiError::from_port_error)?,
            )
            .into_response())
        }

        "blocking/delete" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:blocks")?;
            state.rate_limiter.check(&rate_key, "blocking/delete")?;
            let user_id = required_str(&obj, "userId")?;
            if caller.account.id == user_id {
                return Err(ApiError::from_port_error(PortError::Validation(
                    "BLOCKEE_IS_YOURSELF".to_string(),
                )));
            }
            state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            state
                .services
                .unblock(&caller.account.id, &user_id)
                .map_err(ApiError::from_port_error)?;
            let blockee = state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(
                pack_user_detailed_not_me(&blockee, Some(&caller.account.id), &state.services)
                    .map_err(ApiError::from_port_error)?,
            )
            .into_response())
        }

        "blocking/list" => {
            check_allowed_fields(
                &obj,
                &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:blocks")?;
            let cursor = parse_list_cursor(&obj)?;
            let rows = state
                .services
                .blocks
                .list_blockings(&caller.account.id, &cursor)
                .map_err(ApiError::from_port_error)?;
            let viewer_id = Some(caller.account.id.as_str());
            let mut out = Vec::with_capacity(rows.len());
            for row in &rows {
                out.push(
                    pack_blocking(row, viewer_id, &state.services)
                        .map_err(ApiError::from_port_error)?,
                );
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "mute/create" => {
            check_allowed_fields(&obj, &["userId", "expiresAt"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:mutes")?;
            state.rate_limiter.check(&rate_key, "mute/create")?;
            let user_id = required_str(&obj, "userId")?;
            if caller.account.id == user_id {
                return Err(ApiError::from_port_error(PortError::Validation(
                    "MUTEE_IS_YOURSELF".to_string(),
                )));
            }
            state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            let expires_at = opt_i64(&obj, "expiresAt")?;
            state
                .services
                .mute(&caller.account.id, &user_id, expires_at)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "mute/delete" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:mutes")?;
            let user_id = required_str(&obj, "userId")?;
            if caller.account.id == user_id {
                return Err(ApiError::from_port_error(PortError::Validation(
                    "MUTEE_IS_YOURSELF".to_string(),
                )));
            }
            state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            state
                .services
                .unmute(&caller.account.id, &user_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "mute/list" => {
            check_allowed_fields(
                &obj,
                &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:mutes")?;
            let cursor = parse_list_cursor(&obj)?;
            let rows = state
                .services
                .mutes
                .list_mutings(&caller.account.id, &cursor)
                .map_err(ApiError::from_port_error)?;
            let viewer_id = Some(caller.account.id.as_str());
            let mut out = Vec::with_capacity(rows.len());
            for row in &rows {
                out.push(
                    pack_muting(row, viewer_id, &state.services)
                        .map_err(ApiError::from_port_error)?,
                );
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "renote-mute/create" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:mutes")?;
            state.rate_limiter.check(&rate_key, "renote-mute/create")?;
            let user_id = required_str(&obj, "userId")?;
            if caller.account.id == user_id {
                return Err(ApiError::from_port_error(PortError::Validation(
                    "MUTEE_IS_YOURSELF".to_string(),
                )));
            }
            state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            state
                .services
                .renote_mute(&caller.account.id, &user_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "renote-mute/delete" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:mutes")?;
            let user_id = required_str(&obj, "userId")?;
            if caller.account.id == user_id {
                return Err(ApiError::from_port_error(PortError::Validation(
                    "MUTEE_IS_YOURSELF".to_string(),
                )));
            }
            state
                .services
                .users_show_by_id(&user_id)
                .map_err(ApiError::from_port_error)?;
            state
                .services
                .renote_unmute(&caller.account.id, &user_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "renote-mute/list" => {
            check_allowed_fields(
                &obj,
                &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:mutes")?;
            let cursor = parse_list_cursor(&obj)?;
            let rows = state
                .services
                .mutes
                .list_renote_mutings(&caller.account.id, &cursor)
                .map_err(ApiError::from_port_error)?;
            let viewer_id = Some(caller.account.id.as_str());
            let mut out = Vec::with_capacity(rows.len());
            for row in &rows {
                out.push(
                    pack_renote_muting(row, viewer_id, &state.services)
                        .map_err(ApiError::from_port_error)?,
                );
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "users/relation" => {
            check_allowed_fields(&obj, &["userId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let me = caller.account.id.clone();
            // Misskey parity: always an array (a single `userId` yields a
            // one-element array), with no existence check on the targets.
            let ids: Vec<String> = match obj.get("userId") {
                Some(Value::String(id)) => vec![id.clone()],
                Some(Value::Array(ids)) => {
                    let mut out = Vec::with_capacity(ids.len());
                    for id in ids {
                        out.push(
                            id.as_str()
                                .ok_or_else(|| {
                                    ApiError::invalid_param(
                                        "userId",
                                        "must be a string or an array of strings",
                                    )
                                })?
                                .to_string(),
                        );
                    }
                    out
                }
                _ => {
                    return Err(ApiError::invalid_param(
                        "userId",
                        "this field is required",
                    ));
                }
            };
            let mut out = Vec::with_capacity(ids.len());
            for id in &ids {
                out.push(
                    pack_user_relation(&me, id, &state.services)
                        .map_err(ApiError::from_port_error)?,
                );
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "notes/drafts/create" => {
            check_allowed_fields(
                &obj,
                &[
                    "visibility",
                    "visibleUserIds",
                    "cw",
                    "hashtag",
                    "localOnly",
                    "reactionAcceptance",
                    "replyId",
                    "renoteId",
                    "channelId",
                    "text",
                    "fileIds",
                    "poll",
                    "scheduledAt",
                    "isActuallyScheduled",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "notes/drafts/create")?;
            let input = parse_draft_create(&obj)?;
            let draft = state
                .services
                .create_draft(&caller.account.id, input)
                .map_err(ApiError::from_port_error)?;
            let packed = pack_note_draft(&draft, &state.services);
            Ok(Json(serde_json::json!({ "createdDraft": packed })).into_response())
        }

        "notes/drafts/update" => {
            check_allowed_fields(
                &obj,
                &[
                    "draftId",
                    "visibility",
                    "visibleUserIds",
                    "cw",
                    "hashtag",
                    "localOnly",
                    "reactionAcceptance",
                    "replyId",
                    "renoteId",
                    "channelId",
                    "text",
                    "fileIds",
                    "poll",
                    "scheduledAt",
                    "isActuallyScheduled",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "notes/drafts/update")?;
            let draft_id = required_str(&obj, "draftId")?;
            let patch = parse_draft_update(&obj)?;
            let draft = state
                .services
                .update_draft(&caller.account.id, &draft_id, patch)
                .map_err(ApiError::from_port_error)?;
            let packed = pack_note_draft(&draft, &state.services);
            Ok(Json(serde_json::json!({ "updatedDraft": packed })).into_response())
        }

        "notes/drafts/delete" => {
            check_allowed_fields(&obj, &["draftId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "notes/drafts/delete")?;
            let draft_id = required_str(&obj, "draftId")?;
            // The endpoint distinguishes foreign drafts (`ACCESS_DENIED`)
            // from missing ones (`NO_SUCH_NOTE_DRAFT`); the application op
            // filters by owner first, so the ownership check below decides
            // the error before delegating.
            match state
                .services
                .notes
                .get_draft(&draft_id)
                .map_err(ApiError::from_port_error)?
            {
                Some(draft) if draft.user_id != caller.account.id => {
                    return Err(ApiError::from_port_error(PortError::Validation(
                        "ACCESS_DENIED".to_string(),
                    )));
                }
                _ => {}
            }
            state
                .services
                .delete_draft(&caller.account.id, &draft_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "notes/drafts/list" => {
            check_allowed_fields(
                &obj,
                &["limit", "sinceId", "untilId", "sinceDate", "untilDate", "scheduled"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let limit = match opt_usize(&obj, "limit")? {
                None => 30,
                Some(limit) if (1..=100).contains(&limit) => limit,
                Some(_) => {
                    return Err(ApiError::invalid_param(
                        "limit",
                        "must be an integer between 1 and 100",
                    ));
                }
            };
            let cursor = TimelineCursor {
                since_id: opt_string(&obj, "sinceId")?,
                until_id: opt_string(&obj, "untilId")?,
                since_date: opt_i64(&obj, "sinceDate")?,
                until_date: opt_i64(&obj, "untilDate")?,
                limit,
            };
            let scheduled = match obj.get("scheduled") {
                None | Some(Value::Null) => None,
                Some(Value::Bool(b)) => Some(*b),
                Some(_) => {
                    return Err(ApiError::invalid_param("scheduled", "must be a boolean"));
                }
            };
            let drafts = state
                .services
                .list_drafts(&caller.account.id, &cursor, scheduled)
                .map_err(ApiError::from_port_error)?;
            let mut out = Vec::with_capacity(drafts.len());
            for draft in &drafts {
                out.push(pack_note_draft(draft, &state.services));
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "notes/drafts/count" => {
            check_allowed_fields(&obj, &[])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let count = state
                .services
                .count_drafts(&caller.account.id)
                .map_err(ApiError::from_port_error)?;
            Ok(Json(Value::Number(count.into())).into_response())
        }

        // --- Sharkey scheduling shim (Batch N WS-batch tail) ----------------
        // `notes/schedule/create|list|delete` as a thin alias over the
        // Misskey-model drafts (`scheduledAt` + `isActuallyScheduled` →
        // delayed publish job). No new storage, tables, job kinds, or
        // ports; the publish path is untouched. Auth/permission/rate-limit:
        // credential required; the `notes/drafts/create` 300/hr bucket is
        // reused (DIVERGENCE — upstream kind is `write:notes-schedule`; no
        // second rate-kind for an alias). No `notes/schedule/update`
        // upstream — edits go through `notes/drafts/update` on the
        // underlying draft.
        "notes/schedule/create" => {
            check_allowed_fields(
                &obj,
                &[
                    "visibility",
                    "visibleUserIds",
                    "cw",
                    "reactionAcceptance",
                    "noExtractMentions",
                    "noExtractHashtags",
                    "noExtractEmojis",
                    "replyId",
                    "renoteId",
                    "text",
                    "fileIds",
                    "mediaIds",
                    "poll",
                    "scheduleNote",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:notes-schedule")?;
            // DIVERGENCE (documented): the drafts 300/hr bucket is reused
            // (upstream kind is `write:notes-schedule`; no second
            // rate-kind for an alias).
            state.rate_limiter.check(&rate_key, "notes/drafts/create")?;
            // Upstream `anyOf`: one of text/renoteId/fileIds/mediaIds/poll.
            // Blank text counts as absent (upstream `minLength: 1`); a
            // present-but-blank text is rejected like upstream schema.
            if matches!(obj.get("text"), Some(Value::String(s)) if s.is_empty()) {
                return Err(ApiError::invalid_param("text", "must be 1..=3000 chars"));
            }
            let has_text = obj.get("text").and_then(Value::as_str).map(|s| !s.is_empty()).unwrap_or(false);
            let has_content = has_text
                || obj.get("renoteId").and_then(Value::as_str).map(|s| !s.is_empty()).unwrap_or(false)
                || obj.get("fileIds").and_then(Value::as_array).map(|a| !a.is_empty()).unwrap_or(false)
                || obj.get("mediaIds").and_then(Value::as_array).map(|a| !a.is_empty()).unwrap_or(false)
                || matches!(obj.get("poll"), Some(Value::Object(_)));
            if !has_content {
                return Err(ApiError::invalid_param("text", "one of text, renoteId, fileIds, mediaIds, poll is required"));
            }
            // `scheduleNote: {scheduledAt!}` (ms epoch — confirmed against
            // the tail of upstream `create.ts`: `new Date(schedNoteLocalTime)`
            // where the local time derives from `moment.utc(...).valueOf()`).
            let scheduled_at_ms = match obj.get("scheduleNote").and_then(|v| v.get("scheduledAt")) {
                Some(Value::Number(n)) => n.as_i64().ok_or_else(|| ApiError::invalid_param("scheduleNote", "scheduledAt must be an integer"))?,
                _ => return Err(ApiError::invalid_param("scheduleNote", "scheduleNote.scheduledAt is required")),
            };
            // Upstream cap check against the role policy; pinetree's
            // `scheduled_note_limit` stands in (default 1, instance
            // configurable — upstream default 5). Overflow answers
            // Sharkey's `SCHEDULE_NOTE_MAX`, not our `TOO_MANY_SCHEDULED_NOTES`.
            if state.services.notes.count_scheduled_drafts(&caller.account.id).map_err(ApiError::from_port_error)? >= state.services.scheduled_note_limit {
                return Err(ApiError::schedule_note_max());
            }
            // `fileIds ?? mediaIds` (upstream parity: prefer `fileIds`).
            let mut file_ids = draft_file_ids_for_key(&obj, "fileIds")?;
            if file_ids.is_empty() {
                file_ids = draft_file_ids_for_key(&obj, "mediaIds")?;
            }
            // Poll: `expiredAfter` converts against the *scheduled* time
            // (upstream parity); `expiresAt` wins when present.
            let poll = parse_draft_poll_param(&obj)?;
            let (poll_choices, poll_multiple, poll_expires_at_ms, has_poll) = match poll {
                None => (Vec::new(), false, None, false),
                Some(p) => {
                    let expires_at = match (p.expires_at_ms, p.expired_after_ms) {
                        (Some(Some(expires)), _) => Some(expires),
                        (_, Some(Some(after))) => Some(scheduled_at_ms.saturating_add(after)),
                        _ => None,
                    };
                    (p.choices, p.multiple.unwrap_or(false), expires_at, true)
                }
            };
            let input = CreateDraftInput {
                text: opt_draft_text(&obj, "text")?,
                cw: opt_draft_cw(&obj)?,
                visibility: parse_visibility(obj.get("visibility"))?,
                visible_users: draft_visible_user_ids(&obj)?,
                local_only: false,
                reply_to: opt_string(&obj, "replyId")?,
                renote_of: opt_string(&obj, "renoteId")?,
                file_ids,
                hashtag: None,
                reaction_acceptance: parse_reaction_acceptance(obj.get("reactionAcceptance"))?,
                has_poll,
                poll_choices,
                poll_multiple,
                poll_expires_at_ms,
                poll_expired_after_ms: None,
                scheduled_at_ms: Some(scheduled_at_ms),
                is_actually_scheduled: true,
            };
            let draft = state
                .services
                .create_scheduled_note(&caller.account.id, input, scheduled_at_ms)
                .map_err(|e| match e {
                    // The draft op's own scheduled cap (race with the
                    // pre-check above) surfaces as Sharkey's code too.
                    PortError::Validation(code) if code == "TOO_MANY_SCHEDULED_NOTES" => {
                        ApiError::schedule_note_max()
                    }
                    PortError::Validation(code) if code == "SHARKEY_CANNOT_RERENOTE" => {
                        ApiError::sharkey_cannot_rerenote()
                    }
                    other => ApiError::from_port_error(other),
                })?;
            Ok(Json(pack_scheduled_note(&draft, &caller.account, &state.services)
                .map_err(ApiError::from_port_error)?)
            .into_response())
        }

        "notes/schedule/list" => {
            check_allowed_fields(&obj, &["sinceId", "untilId", "limit"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:notes-schedule")?;
            let cursor = TimelineCursor {
                since_id: opt_string(&obj, "sinceId")?,
                until_id: opt_string(&obj, "untilId")?,
                since_date: None,
                until_date: None,
                limit: get_limit_with_default(&obj, 10)?,
            };
            let drafts = state
                .services
                .list_drafts(&caller.account.id, &cursor, Some(true))
                .map_err(ApiError::from_port_error)?;
            let mut out = Vec::with_capacity(drafts.len());
            for draft in &drafts {
                out.push(pack_scheduled_note(draft, &caller.account, &state.services)
                    .map_err(ApiError::from_port_error)?);
            }
            Ok(Json(Value::Array(out)).into_response())
        }

        "notes/schedule/delete" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:notes-schedule")?;
            let note_id = required_str(&obj, "noteId")?;
            // The schedule id IS the draft id. Unknown → Sharkey's
            // `NO_SUCH_NOTE` (`a58056ba-…`); foreign → 403
            // `PERMISSION_DENIED` (`c0da2fed-…`).
            let stored = state
                .services
                .notes
                .get_draft(&note_id)
                .map_err(ApiError::from_port_error)?;
            match stored {
                None => return Err(ApiError::schedule_no_such_note()),
                Some(draft) if draft.user_id != caller.account.id => {
                    return Err(ApiError::schedule_permission_denied())
                }
                _ => {}
            }
            // Delete via the draft path so the publish job is cleared
            // exactly once (no double-publish, no orphan job).
            state
                .services
                .delete_draft(&caller.account.id, &note_id)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/pin" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            let note_id = required_str(&obj, "noteId")?;
            state
                .services
                .pin_note(&caller.account.id, &note_id)
                .map_err(ApiError::from_port_error)?;
            let packed =
                pack_me_detailed(&caller.account, caller.is_first_party, &state.services)
                    .map_err(ApiError::from_port_error)?;
            Ok(Json(packed).into_response())
        }

        "i/unpin" => {
            check_allowed_fields(&obj, &["noteId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            let note_id = required_str(&obj, "noteId")?;
            state
                .services
                .unpin_note(&caller.account.id, &note_id)
                .map_err(ApiError::from_port_error)?;
            let packed =
                pack_me_detailed(&caller.account, caller.is_first_party, &state.services)
                    .map_err(ApiError::from_port_error)?;
            Ok(Json(packed).into_response())
        }

        "notes/featured" => {
            check_allowed_fields(&obj, &["limit", "untilId", "channelId"])?;
            // No channels exist: any channel ranking is empty (upstream
            // serves an empty ranking set for unknown channels).
            if let Some(channel) = obj.get("channelId") {
                if !channel.is_null() {
                    return Ok(Json(Value::Array(Vec::new())).into_response());
                }
            }
            let limit = get_limit(&obj)?;
            let until_id = opt_string(&obj, "untilId")?;
            let now = state.services.clock.now_ms();
            let ranked = match state.featured_cache.get("global", now) {
                Some(ids) => ids,
                None => {
                    let ids = state
                        .services
                        .rank_featured_global()
                        .map_err(ApiError::from_port_error)?;
                    state.featured_cache.put("global", ids.clone(), now);
                    ids
                }
            };
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            let notes = state
                .services
                .featured_notes_visible(&ranked, viewer_id, until_id.as_deref(), limit)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "users/featured-notes" => {
            check_allowed_fields(&obj, &["limit", "untilId", "userId"])?;
            let user_id = required_str(&obj, "userId")?;
            let limit = get_limit(&obj)?;
            let until_id = opt_string(&obj, "untilId")?;
            let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
            // Early return when the target blocks the viewer (Misskey
            // `featured-notes.ts` parity).
            if let Some(viewer) = viewer_id {
                if state
                    .services
                    .blocks
                    .is_blocking(&user_id, viewer)
                    .map_err(ApiError::from_port_error)?
                {
                    return Ok(Json(Value::Array(Vec::new())).into_response());
                }
            }
            let now = state.services.clock.now_ms();
            let key = format!("user:{user_id}");
            let ranked = match state.featured_cache.get(&key, now) {
                Some(ids) => ids,
                None => {
                    let ids = state
                        .services
                        .rank_featured_for_user(&user_id)
                        .map_err(ApiError::from_port_error)?;
                    state.featured_cache.put(&key, ids.clone(), now);
                    ids
                }
            };
            let notes = state
                .services
                .featured_notes_visible(&ranked, viewer_id, until_id.as_deref(), limit)
                .map_err(ApiError::from_port_error)?;
            pack_note_list(&state.services, &notes, viewer_id)
        }

        "i/notifications-grouped" => {
            check_allowed_fields(
                &obj,
                &[
                    "limit",
                    "sinceId",
                    "untilId",
                    "sinceDate",
                    "untilDate",
                    "markAsRead",
                    "includeTypes",
                    "excludeTypes",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:notifications")?;
            let query = parse_notification_query(&obj)?;
            if query.matches_nothing {
                if query.mark_as_read {
                    state
                        .services
                        .mark_all_notifications_read(&caller.account.id)
                        .map_err(ApiError::from_port_error)?;
                }
                return Ok(Json(Value::Array(Vec::new())).into_response());
            }
            let notifications = state
                .services
                .notifications
                .list_notifications_filtered(
                    &caller.account.id,
                    &query.cursor,
                    &query.include,
                    &query.exclude,
                )
                .map_err(ApiError::from_port_error)?;
            if notifications.is_empty() {
                return Ok(Json(Value::Array(Vec::new())).into_response());
            }
            if query.mark_as_read {
                state
                    .services
                    .mark_all_notifications_read(&caller.account.id)
                    .map_err(ApiError::from_port_error)?;
            }
            let mut packed = crate::api::misskey::packers::pack_grouped_notifications(
                &notifications,
                &caller.account.id,
                &state.services,
            );
            packed.truncate(query.cursor.limit.max(1));
            Ok(Json(Value::Array(packed)).into_response())
        }

        _ => Err(ApiError::unknown_endpoint()),
    }
}

fn pack_note_list(
    services: &AppServices,
    notes: &[crate::domain::note::Note],
    viewer_id: Option<&str>,
) -> Result<Response, ApiError> {
    let mut out = Vec::with_capacity(notes.len());
    for note in notes {
        out.push(
            pack_note(note, viewer_id, services, 0).map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

fn update_string_field(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Option<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(s)) => Ok(Some(Some(s.clone()))),
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

fn update_bool_field(obj: &Map<String, Value>, key: &str) -> Result<Option<bool>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(ApiError::invalid_param(key, "must be a boolean")),
    }
}

/// Parse the full `i/update` body into an [`UpdateAccountPatch`]
/// (`i/update.ts` `paramDef` parity: all 39 params plus the DIVERGENCE
/// `rejectQuotes`/`defaultReaction`). Length/enum shapes are enforced here
/// as `INVALID_PARAM`; role-gated and cross-entity rules (prohibited words,
/// media checks, mute limits, alias resolution) live in
/// `AppServices::update_profile` with their exact Misskey error codes.
fn parse_profile_patch(obj: &Map<String, Value>) -> Result<UpdateAccountPatch, ApiError> {
    // `name`: raw value validated at 1..=50 chars (`nameSchema`); the
    // trim / `""` → null step runs in `update_profile`.
    if let Some(raw) = obj.get("name") {
        match raw {
            Value::Null => {}
            Value::String(s) => {
                let n = s.chars().count();
                if n == 0 || n > 50 {
                    return Err(ApiError::invalid_param(
                        "name",
                        "must be 1..=50 chars",
                    ));
                }
            }
            _ => return Err(ApiError::invalid_param("name", "must be a string")),
        }
    }
    Ok(UpdateAccountPatch {
        display_name: update_string_field(obj, "name")?,
        description: bounded_nullable_string(obj, "description", 1500)?,
        followed_message: bounded_nullable_string(obj, "followedMessage", 256)?,
        location: bounded_nullable_string(obj, "location", 50)?,
        birthday: update_string_field(obj, "birthday")?,
        // Upstream restricts `lang` to its langmap enum; pinetree has no
        // langmap yet, so any 1..=32-char tag is accepted (TODO(§0B)).
        lang: bounded_nullable_string(obj, "lang", 32)?,
        avatar_id: profile_file_id(obj, "avatarId")?,
        avatar_decorations: parse_avatar_decorations(obj)?,
        banner_id: profile_file_id(obj, "bannerId")?,
        fields: parse_profile_fields(obj)?,
        is_locked: update_bool_field(obj, "isLocked")?,
        is_bot: update_bool_field(obj, "isBot")?,
        is_cat: update_bool_field(obj, "isCat")?,
        is_explorable: update_bool_field(obj, "isExplorable")?,
        hide_online_status: update_bool_field(obj, "hideOnlineStatus")?,
        public_reactions: update_bool_field(obj, "publicReactions")?,
        careful_bot: update_bool_field(obj, "carefulBot")?,
        auto_accept_followed: update_bool_field(obj, "autoAcceptFollowed")?,
        follow_approval_local_seconds: bounded_nullable_seconds(
            obj,
            "followApprovalLocalSeconds",
        )?,
        follow_approval_remote_seconds: bounded_nullable_seconds(
            obj,
            "followApprovalRemoteSeconds",
        )?,
        no_crawle: update_bool_field(obj, "noCrawle")?,
        prevent_ai_learning: update_bool_field(obj, "preventAiLearning")?,
        require_signin_to_view_contents: update_bool_field(
            obj,
            "requireSigninToViewContents",
        )?,
        make_notes_followers_only_before: nullable_integer(obj, "makeNotesFollowersOnlyBefore")?,
        make_notes_hidden_before: nullable_integer(obj, "makeNotesHiddenBefore")?,
        inject_featured_note: update_bool_field(obj, "injectFeaturedNote")?,
        receive_announcement_email: update_bool_field(obj, "receiveAnnouncementEmail")?,
        always_mark_nsfw: update_bool_field(obj, "alwaysMarkNsfw")?,
        auto_sensitive: update_bool_field(obj, "autoSensitive")?,
        following_visibility: parse_follow_visibility(obj, "followingVisibility")?,
        followers_visibility: parse_follow_visibility(obj, "followersVisibility")?,
        chat_scope: parse_chat_scope(obj)?,
        pinned_page_id: profile_file_id(obj, "pinnedPageId")?,
        muted_words: parse_muted_words(obj, "mutedWords")?,
        hard_muted_words: parse_muted_words(obj, "hardMutedWords")?,
        muted_instances: parse_string_array(obj, "mutedInstances", None)?,
        notification_recieve_config: parse_notification_recieve_config(obj)?,
        email_notification_types: parse_string_array(obj, "emailNotificationTypes", None)?,
        also_known_as: parse_string_array(obj, "alsoKnownAs", Some(10))?.map(Some),
        verified_links: None,
        reject_quotes: update_bool_field(obj, "rejectQuotes")?,
        default_reaction: bounded_nullable_string(obj, "defaultReaction", 260)?,
        // DIVERGENCE (Batch N WP8b): plain bool, absent = unchanged.
        show_remote_emojis: update_bool_field(obj, "showRemoteEmojis")?,
        moderator_note: None,
    })
}

/// Nullable string with a 1..=max char bound (empty strings rejected like
/// upstream `minLength: 1` schemas).
fn bounded_nullable_string(
    obj: &Map<String, Value>,
    key: &str,
    max_chars: usize,
) -> Result<Option<Option<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(s)) => {
            let n = s.chars().count();
            if n == 0 || n > max_chars {
                return Err(ApiError::invalid_param(
                    key,
                    &format!("must be 1..={max_chars} chars"),
                ));
            }
            Ok(Some(Some(s.clone())))
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

/// Nullable file/page id: a non-empty string or explicit null.
fn profile_file_id(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Option<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(s)) => {
            if s.is_empty() {
                return Err(ApiError::invalid_param(key, "must be a non-empty id"));
            }
            Ok(Some(Some(s.clone())))
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

/// Nullable integer (`followApproval*Seconds` are range-checked again in
/// domain validation; `makeNotes*Before` accept any integer).
fn nullable_integer(obj: &Map<String, Value>, key: &str) -> Result<Option<Option<i64>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::Number(n)) => match n.as_i64() {
            Some(v) => Ok(Some(Some(v))),
            None => Err(ApiError::invalid_param(key, "must be an integer")),
        },
        Some(_) => Err(ApiError::invalid_param(key, "must be an integer")),
    }
}

fn bounded_nullable_seconds(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Option<i64>>, ApiError> {
    match nullable_integer(obj, key)? {
        None => Ok(None),
        Some(None) => Ok(Some(None)),
        Some(Some(v)) => {
            if v < 0 || v > crate::domain::account::FOLLOW_APPROVAL_MAX_SECONDS {
                return Err(ApiError::invalid_param(
                    key,
                    "must be 0..=2592000 (30 days)",
                ));
            }
            Ok(Some(Some(v)))
        }
    }
}

fn parse_follow_visibility(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<FollowVisibility>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => FollowVisibility::parse(s).map(Some).ok_or_else(|| {
            ApiError::invalid_param(key, "must be public, followers, or private")
        }),
        Some(_) => Err(ApiError::invalid_param(
            key,
            "must be public, followers, or private",
        )),
    }
}

fn parse_chat_scope(obj: &Map<String, Value>) -> Result<Option<ChatScope>, ApiError> {
    match obj.get("chatScope") {
        None => Ok(None),
        Some(Value::String(s)) => ChatScope::parse(s).map(Some).ok_or_else(|| {
            ApiError::invalid_param(
                "chatScope",
                "must be everyone, followers, following, mutual, or none",
            )
        }),
        Some(_) => Err(ApiError::invalid_param(
            "chatScope",
            "must be everyone, followers, following, mutual, or none",
        )),
    }
}

/// `fields`: max 16 `{name, value}` entries; blank-after-trim rows are
/// dropped (`update.ts` fields filter parity).
fn parse_profile_fields(
    obj: &Map<String, Value>,
) -> Result<Option<Vec<ProfileField>>, ApiError> {
    let Some(value) = obj.get("fields") else {
        return Ok(None);
    };
    let Value::Array(items) = value else {
        return Err(ApiError::invalid_param("fields", "must be an array"));
    };
    if items.len() > 16 {
        return Err(ApiError::invalid_param("fields", "must have at most 16 entries"));
    }
    let mut out = Vec::new();
    for item in items {
        let entry = item.as_object().ok_or_else(|| {
            ApiError::invalid_param("fields", "each entry must be {name, value}")
        })?;
        let name = entry.get("name").and_then(Value::as_str).ok_or_else(|| {
            ApiError::invalid_param("fields", "each entry must be {name, value}")
        })?;
        let val = entry.get("value").and_then(Value::as_str).ok_or_else(|| {
            ApiError::invalid_param("fields", "each entry must be {name, value}")
        })?;
        let (name, val) = (name.trim(), val.trim());
        if name.is_empty() || val.is_empty() {
            continue;
        }
        out.push(ProfileField {
            name: name.to_string(),
            value: val.to_string(),
        });
    }
    Ok(Some(out))
}

/// `avatarDecorations`: max 16 entries with `update.ts` numeric ranges.
fn parse_avatar_decorations(
    obj: &Map<String, Value>,
) -> Result<Option<Vec<AvatarDecoration>>, ApiError> {
    let Some(value) = obj.get("avatarDecorations") else {
        return Ok(None);
    };
    let Value::Array(items) = value else {
        return Err(ApiError::invalid_param(
            "avatarDecorations",
            "must be an array",
        ));
    };
    if items.len() > 16 {
        return Err(ApiError::invalid_param(
            "avatarDecorations",
            "must have at most 16 entries",
        ));
    }
    let mut out = Vec::new();
    for item in items {
        let entry = item.as_object().ok_or_else(|| {
            ApiError::invalid_param("avatarDecorations", "each entry must be an object")
        })?;
        let id = entry.get("id").and_then(Value::as_str).ok_or_else(|| {
            ApiError::invalid_param("avatarDecorations", "each entry requires id")
        })?;
        if id.is_empty() {
            return Err(ApiError::invalid_param(
                "avatarDecorations",
                "each entry requires id",
            ));
        }
        let opt_num = |key: &str, min: f64, max: f64| -> Result<Option<f64>, ApiError> {
            match entry.get(key) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::Number(n)) => {
                    let v = n.as_f64().ok_or_else(|| {
                        ApiError::invalid_param("avatarDecorations", "bad numeric value")
                    })?;
                    if !v.is_finite() || v < min || v > max {
                        return Err(ApiError::invalid_param(
                            "avatarDecorations",
                            "numeric value out of range",
                        ));
                    }
                    Ok(Some(v))
                }
                Some(_) => Err(ApiError::invalid_param(
                    "avatarDecorations",
                    "bad numeric value",
                )),
            }
        };
        let flip_h = match entry.get("flipH") {
            None | Some(Value::Null) => None,
            Some(Value::Bool(b)) => Some(*b),
            Some(_) => {
                return Err(ApiError::invalid_param(
                    "avatarDecorations",
                    "flipH must be a boolean",
                ));
            }
        };
        out.push(AvatarDecoration {
            id: id.to_string(),
            angle: opt_num("angle", -0.5, 0.5)?,
            flip_h,
            offset_x: opt_num("offsetX", -0.25, 0.25)?,
            offset_y: opt_num("offsetY", -0.25, 0.25)?,
        });
    }
    Ok(Some(out))
}

/// `mutedWords`/`hardMutedWords`: each entry a string or string array.
/// Count and regex rules run in `update_profile`.
fn parse_muted_words(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<MutedWord>>, ApiError> {
    let Some(value) = obj.get(key) else {
        return Ok(None);
    };
    let Value::Array(items) = value else {
        return Err(ApiError::invalid_param(key, "must be an array"));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::String(s) => out.push(MutedWord::Single(s.clone())),
            Value::Array(words) => {
                let mut and = Vec::with_capacity(words.len());
                for word in words {
                    match word {
                        Value::String(s) => and.push(s.clone()),
                        _ => {
                            return Err(ApiError::invalid_param(
                                key,
                                "entries must be strings or string arrays",
                            ));
                        }
                    }
                }
                out.push(MutedWord::And(and));
            }
            _ => {
                return Err(ApiError::invalid_param(
                    key,
                    "entries must be strings or string arrays",
                ));
            }
        }
    }
    Ok(Some(out))
}

/// Plain string arrays (`mutedInstances`, `emailNotificationTypes`,
/// `alsoKnownAs` with `maxItems: 10` + `uniqueItems` parity).
fn parse_string_array(
    obj: &Map<String, Value>,
    key: &str,
    max_items: Option<usize>,
) -> Result<Option<Vec<String>>, ApiError> {
    let Some(value) = obj.get(key) else {
        return Ok(None);
    };
    let Value::Array(items) = value else {
        return Err(ApiError::invalid_param(key, "must be an array"));
    };
    if let Some(max) = max_items {
        if items.len() > max {
            return Err(ApiError::invalid_param(
                key,
                &format!("must have at most {max} entries"),
            ));
        }
    }
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::String(s) => out.push(s.clone()),
            _ => return Err(ApiError::invalid_param(key, "must be an array of strings")),
        }
    }
    if key == "alsoKnownAs" {
        let mut seen = std::collections::HashSet::new();
        for entry in &out {
            if !seen.insert(entry) {
                return Err(ApiError::invalid_param(key, "entries must be unique"));
            }
        }
    }
    Ok(Some(out))
}

/// `notificationRecieveConfig` (upstream misspelling kept): an object whose
/// values are `{type}` or `{type: list, userListId}` shapes.
fn parse_notification_recieve_config(
    obj: &Map<String, Value>,
) -> Result<
    Option<std::collections::BTreeMap<String, NotificationRecieveConfig>>,
    ApiError,
> {
    let Some(value) = obj.get("notificationRecieveConfig") else {
        return Ok(None);
    };
    let Value::Object(map) = value else {
        return Err(ApiError::invalid_param(
            "notificationRecieveConfig",
            "must be an object",
        ));
    };
    let mut out = std::collections::BTreeMap::new();
    for (key, entry) in map {
        let entry_obj = entry.as_object().ok_or_else(|| {
            ApiError::invalid_param("notificationRecieveConfig", "each value must be an object")
        })?;
        let kind = entry_obj
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::invalid_param(
                    "notificationRecieveConfig",
                    "each value requires type",
                )
            })?;
        let user_list_id = match entry_obj.get("userListId") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => {
                return Err(ApiError::invalid_param(
                    "notificationRecieveConfig",
                    "userListId must be a string",
                ));
            }
        };
        let config = NotificationRecieveConfig {
            kind: kind.to_string(),
            user_list_id,
        };
        config
            .validate()
            .map_err(|_| ApiError::invalid_param("notificationRecieveConfig", "bad entry shape"))?;
        out.insert(key.clone(), config);
    }
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use crate::api::misskey::auth::verify_password;

    use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE};

    use crate::application::ports::{
        ActivityDedup, BlobStore, NoopStreamingSink, ReactionStore, StreamingSink,
    };
    use crate::domain::id::{AidxGenerator, Clock, ManualClock};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::federation::retry::DestinationTracker;
    use crate::persistence::redb::RedbStore;

    use super::*;

    #[derive(Default)]
    struct MemBlobs {
        inner: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl BlobStore for MemBlobs {
        fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
            self.inner
                .lock()
                .unwrap()
                .insert(hash.to_string(), bytes.to_vec());
            Ok(())
        }

        fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
            Ok(self.inner.lock().unwrap().get(hash).cloned())
        }

        fn delete(&self, hash: &str) -> Result<(), PortError> {
            self.inner.lock().unwrap().remove(hash);
            Ok(())
        }
    }

    fn test_state() -> (ApiState, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(1_700_000_000_000));
        let ids: Arc<dyn crate::application::ports::IdGenerator> =
            Arc::new(AidxGenerator::with_node_id(clock.clone(), "test", 0));
        let dir = std::env::temp_dir().join(format!("pinetree-api-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = RedbStore::open(dir.join("t.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(MemBlobs::default());
        let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
        let services = Arc::new(AppServices {
            clock: clock.clone(),
            ids: ids.clone(),
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            favorites: store.clone(),
            emojis: store.clone(),
            antennas: store.clone(),
            antenna_notes: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: streaming.clone(),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://example.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        });
        let worker = Arc::new(JobWorker {
            clock: clock.clone(),
            ids: ids.clone(),
            jobs: store.clone(),
            notifications: store.clone(),
            streaming,
            services: services.clone(),
            delivery_tracker: Arc::new(DestinationTracker::new()),
            lease_duration_ms: 30_000,
            base_backoff_ms: 1_000,
        });
        let rate_limiter = Arc::new(RateLimiter::with_misskey_defaults(clock.clone()));
        let upload_slots = Arc::new(UploadSlots::with_defaults());
        (
            ApiState {
                services,
                worker,
                rate_limiter,
                upload_slots,
                hub: Hub::new(1024),
                trusted_proxies: Arc::new(Vec::new()),
                featured_cache: Arc::new(
                    crate::application::commands::pins::FeaturedCache::default(),
                ),
            },
            clock,
        )
    }

    async fn read_json(response: Response) -> (StatusCode, Value, HeaderMap) {
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, value, headers)
    }

    /// Direct-call peer used by every handler test unless a test says otherwise.
    fn test_peer() -> ConnectInfo<SocketAddr> {
        ConnectInfo("127.0.0.1:9".parse().unwrap())
    }

    fn peer_at(ip: &str) -> ConnectInfo<SocketAddr> {
        ConnectInfo(format!("{ip}:9").parse().unwrap())
    }

    async fn call(
        state: &ApiState,
        endpoint: &str,
        body: Value,
        token: Option<&str>,
    ) -> (StatusCode, Value) {
        call_from(state, endpoint, body, token, test_peer()).await
    }

    async fn call_from(
        state: &ApiState,
        endpoint: &str,
        body: Value,
        token: Option<&str>,
        peer: ConnectInfo<SocketAddr>,
    ) -> (StatusCode, Value) {
        let mut headers = HeaderMap::new();
        if let Some(token) = token {
            headers.insert(
                AUTHORIZATION,
                format!("Bearer {token}").parse::<HeaderValue>().unwrap(),
            );
        }
        let bytes = Bytes::from(serde_json::to_vec(&body).unwrap());
        let response = match api_dispatch(
            State(state.clone()),
            peer,
            Path(endpoint.to_string()),
            headers,
            bytes,
        )
        .await
        {
            Ok(response) => response,
            Err(err) => err.into_response(),
        };
        let (status, value, _) = read_json(response).await;
        (status, value)
    }

    async fn signup_user(state: &ApiState, username: &str) -> (String, String) {
        let (status, value) = call(
            state,
            "signup",
            serde_json::json!({ "username": username, "password": "s3cret-pw" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "signup {username}: {value}");
        let token = value["token"].as_str().unwrap().to_string();
        let id = value["id"].as_str().unwrap().to_string();
        (id, token)
    }

    /// Slice 10: API writes do not wait for peers. The follower's inbox
    /// accepts the connection, reads the request and then stalls; the worker
    /// is busy with it in the background while `notes/create` returns fast.
    #[tokio::test]
    async fn slice10_api_latency_independent_of_slow_peer() {
        use std::io::Read;
        use std::net::TcpListener;
        use std::time::{Duration, Instant};

        use crate::application::ports::{AccountStore, CreateAccountInput, FollowStore};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind slow peer");
        let addr = listener.local_addr().expect("slow peer addr");
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                std::thread::sleep(Duration::from_secs(3));
            }
        });

        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let remote = state
            .services
            .accounts
            .create_account(CreateAccountInput {
                handle: "slowpoke".to_string(),
                host: Some("slow.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://slow.example/users/slowpoke".to_string()),
                inbox: Some(format!("http://{addr}/inbox")),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .expect("remote follower");
        state
            .services
            .follows
            .follow(&remote.id, &alice_id, false, None, Vec::new())
            .expect("remote account follows alice");

        // Production runs the worker on a background loop; do the same here.
        let _worker = state.worker.clone().spawn(Duration::from_millis(10));

        let started = Instant::now();
        let (status, value) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "hello slow peers" }),
            Some(&alice_token),
        )
        .await;
        let elapsed = started.elapsed();
        assert_eq!(status, StatusCode::OK, "{value}");
        assert!(
            elapsed < Duration::from_millis(500),
            "notes/create waited on a federation peer: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        assert_eq!(healthz().await, "ok");
    }

    #[tokio::test]
    async fn meta_reports_version() {
        let (state, _) = test_state();
        let (status, value) = call(&state, "meta", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["name"], "Pinetree");
        assert!(value["version"].as_str().unwrap().starts_with("2026"));
        let (status, lite) = call(
            &state,
            "meta",
            serde_json::json!({ "detail": false }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(lite.get("features").is_none());
    }

    #[tokio::test]
    async fn signup_signin_and_i_roundtrip() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;

        let (status, me) = call(&state, "i", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["id"], alice_id);
        assert_eq!(me["username"], "alice");

        // No credential -> 401.
        let (status, value) = call(&state, "i", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(value["code"], "CREDENTIAL_REQUIRED");

        // Bad token -> 401.
        let (status, _) = call(&state, "i", serde_json::json!({}), Some("nope")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Web signin route returns id/token/i/user + cookie.
        let response = signin(
            State(state.clone()),
            test_peer(),
            HeaderMap::new(),
            Bytes::from(serde_json::to_vec(&serde_json::json!({
                "username": "alice", "password": "s3cret-pw",
            })).unwrap()),
        )
        .await
        .unwrap();
        let (status, value, headers) = read_json(response).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["id"], alice_id);
        assert!(value["token"].as_str().is_some());
        assert_eq!(value["i"], value["token"]);
        assert_eq!(value["user"]["username"], "alice");
        assert!(headers.get(SET_COOKIE).is_some());

        // Wrong password -> 401.
        let (status, _) = call(
            &state,
            "signin",
            serde_json::json!({ "username": "alice", "password": "wrong" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    /// Pre-slice-9 `salt:sha256` hash, built the way the legacy hasher did.
    fn legacy_password_hash(salt_hex: &str, password: &str) -> String {
        let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
        sha2::Digest::update(&mut hasher, salt_hex.as_bytes());
        sha2::Digest::update(&mut hasher, b":");
        sha2::Digest::update(&mut hasher, password.as_bytes());
        format!("{salt_hex}:{}", hex::encode(sha2::Digest::finalize(hasher)))
    }

    #[tokio::test]
    async fn slice9_legacy_hash_verifies_then_upgrades_to_argon2id_on_signin() {
        let (state, _) = test_state();
        let legacy = legacy_password_hash("00112233445566778899aabbccddeeff", "legacy-pw");
        let account = state
            .services
            .create_account("legacy_user", Some(&legacy))
            .expect("create legacy account");
        let response = signin(
            State(state.clone()),
            test_peer(),
            HeaderMap::new(),
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "username": "legacy_user", "password": "legacy-pw",
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let (status, value, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["id"], account.id);
        let stored = state
            .services
            .auth
            .get_password_hash(&account.id)
            .unwrap()
            .expect("stored hash");
        assert!(stored.starts_with("$argon2id$"), "{stored}");
        assert!(verify_password("legacy-pw", &stored));
        assert!(!verify_password("wrong", &stored));
    }

    #[tokio::test]
    async fn slice9_signup_stores_argon2id_and_rejects_wrong_password() {
        let (state, _) = test_state();
        let (account_id, _) = signup_user(&state, "slice9_new").await;
        let stored = state
            .services
            .auth
            .get_password_hash(&account_id)
            .unwrap()
            .expect("stored hash");
        assert!(stored.starts_with("$argon2id$"), "{stored}");
        let (status, _) = call(
            &state,
            "signin",
            serde_json::json!({ "username": "slice9_new", "password": "wrong" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn slice9_signin_brute_force_returns_rate_limit_exceeded() {
        let (mut state, clock) = test_state();
        signup_user(&state, "victim").await;
        // Three failures per account trip the cap; the per-IP cap is generous here.
        state.rate_limiter = Arc::new(
            RateLimiter::new(clock.clone()).with_auth_limit("auth/signin", 3, 30, 60_000),
        );
        let bad = serde_json::json!({ "username": "victim", "password": "guess" });
        let good = serde_json::json!({ "username": "victim", "password": "s3cret-pw" });

        let (status, _) = call(&state, "signin", bad.clone(), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // Right after a failure the progressive delay refuses every attempt.
        let (status, value) = call(&state, "signin", good.clone(), None).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
        assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");

        clock.advance_ms(1_000);
        let (status, _) = call(&state, "signin", bad.clone(), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        clock.advance_ms(2_000);
        let (status, _) = call(&state, "signin", bad, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // Account cap reached: even the right password waits for the window.
        let (status, value) = call(&state, "signin", good.clone(), None).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
        clock.advance_ms(60_000);
        let (status, value) = call(&state, "signin", good, None).await;
        assert_eq!(status, StatusCode::OK, "{value}");
    }

    #[tokio::test]
    async fn slice9_signin_per_ip_bucket_trips_across_accounts() {
        let (mut state, clock) = test_state();
        signup_user(&state, "ipa").await;
        signup_user(&state, "ipb").await;
        // One failure per IP is enough; the per-account cap is generous.
        state.rate_limiter = Arc::new(
            RateLimiter::new(clock.clone()).with_auth_limit("auth/signin", 100, 1, 60_000),
        );
        let (status, _) = call_from(
            &state,
            "signin",
            serde_json::json!({ "username": "ipa", "password": "x" }),
            None,
            peer_at("198.51.100.20"),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // A different account from the same IP is refused by the IP bucket.
        let (status, value) = call_from(
            &state,
            "signin",
            serde_json::json!({ "username": "ipb", "password": "x" }),
            None,
            peer_at("198.51.100.20"),
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
        assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");
        // Another IP still signs in.
        let (status, value) = call_from(
            &state,
            "signin",
            serde_json::json!({ "username": "ipb", "password": "s3cret-pw" }),
            None,
            peer_at("198.51.100.21"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
    }

    #[test]
    fn slice9_spoofed_xff_from_untrusted_peer_is_ignored() {
        let peer: SocketAddr = "203.0.113.9:4000".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("192.0.2.77"));
        headers.insert("x-real-ip", HeaderValue::from_static("192.0.2.78"));
        assert_eq!(client_ip(peer, &headers, &[]), "203.0.113.9");
        // Another proxy being trusted does not make this untrusted peer's header count.
        let trusted = ["10.0.0.1".parse::<IpAddr>().unwrap()];
        assert_eq!(client_ip(peer, &headers, &trusted), "203.0.113.9");
    }

    #[test]
    fn slice9_xff_honored_only_from_trusted_proxy() {
        let proxy: SocketAddr = "10.0.0.1:4000".parse().unwrap();
        let trusted = ["10.0.0.1".parse::<IpAddr>().unwrap()];
        let mut headers = HeaderMap::new();
        // The client prepended a fake hop; the trusted proxy appended the real client.
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.1, 192.0.2.77"),
        );
        assert_eq!(client_ip(proxy, &headers, &trusted), "192.0.2.77");
        // A trusted hop on the right is skipped.
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("192.0.2.77, 10.0.0.1"),
        );
        assert_eq!(client_ip(proxy, &headers, &trusted), "192.0.2.77");
        // All hops trusted: the leftmost entry is used.
        let both = ["10.0.0.1".parse::<IpAddr>().unwrap(), "10.0.0.2".parse().unwrap()];
        let second: SocketAddr = "10.0.0.2:4000".parse().unwrap();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("10.0.0.1, 10.0.0.2"),
        );
        assert_eq!(client_ip(second, &headers, &both), "10.0.0.1");
        // An unparseable hop makes the header untrustworthy: use the peer.
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("203.0.113.5, garbage"),
        );
        assert_eq!(client_ip(second, &headers, &both), "10.0.0.2");
    }

    #[tokio::test]
    async fn i_update_and_unsupported_field() {
        let (mut state, clock) = test_state();
        let (_, token) = signup_user(&state, "carol").await;

        let (status, me) = call(
            &state,
            "i/update",
            serde_json::json!({ "name": "Carol", "isCat": true }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["name"], "Carol");
        assert_eq!(me["isCat"], true);

        let (status, value) = call(
            &state,
            "i/update",
            serde_json::json!({ "name": "X", "channelId": "abc" }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["code"], "UNSUPPORTED_FIELD");
        assert_eq!(value["error"]["info"]["param"], "channelId");

        // Rate limit bucket trips on the second quick update.
        state.rate_limiter = Arc::new(
            RateLimiter::new(clock.clone()).with_limit("i/update", 1, 3_600_000, None),
        );
        let (status, _) = call(
            &state,
            "i/update",
            serde_json::json!({ "name": "Again" }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, value) = call(
            &state,
            "i/update",
            serde_json::json!({ "name": "Third" }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
        assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");
    }

    #[tokio::test]
    async fn i_update_full_profile_round_trip_and_defaults() {
        let (state, _) = test_state();
        let (_, token) = signup_user(&state, "prof").await;

        // Fresh accounts pack the upstream column defaults.
        let (status, me) = call(&state, "i", serde_json::json!({}), Some(&token)).await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["preventAiLearning"], true);
        assert_eq!(me["injectFeaturedNote"], true);
        assert_eq!(me["receiveAnnouncementEmail"], true);
        assert_eq!(me["chatScope"], "mutual");
        assert_eq!(me["publicReactions"], true);
        assert_eq!(me["followingVisibility"], "public");
        assert_eq!(me["followersVisibility"], "public");
        assert_eq!(
            me["emailNotificationTypes"],
            serde_json::json!(["follow", "receiveFollowRequest"])
        );
        assert_eq!(me["mutedWords"], serde_json::json!([]));
        assert_eq!(me["notificationRecieveConfig"], serde_json::json!({}));
        assert_eq!(me["rejectQuotes"], false);
        assert_eq!(me["defaultReaction"], Value::Null);

        // Full round-trip across the new params.
        let (status, me) = call(
            &state,
            "i/update",
            serde_json::json!({
                "name": "  Prof  ",
                "description": "bio",
                "followedMessage": "thanks for following",
                "location": "nowhere",
                "birthday": "1990-02-28",
                "lang": "en",
                "fields": [
                    {"name": " site ", "value": " https://example.local "},
                    {"name": "  ", "value": "blank"},
                ],
                "isLocked": true,
                "hideOnlineStatus": true,
                "publicReactions": false,
                "carefulBot": true,
                "autoAcceptFollowed": true,
                "followApprovalLocalSeconds": 3600,
                "followApprovalRemoteSeconds": null,
                "noCrawle": true,
                "preventAiLearning": false,
                "requireSigninToViewContents": true,
                "makeNotesFollowersOnlyBefore": 123,
                "makeNotesHiddenBefore": null,
                "injectFeaturedNote": false,
                "receiveAnnouncementEmail": false,
                "alwaysMarkNsfw": true,
                "autoSensitive": true,
                "followingVisibility": "followers",
                "followersVisibility": "private",
                "chatScope": "followers",
                "mutedWords": [["foo", "bar"], "/^spam/i"],
                "hardMutedWords": [],
                "mutedInstances": ["spam.example"],
                "notificationRecieveConfig": {
                    "mention": {"type": "following"},
                    "follow": {"type": "list", "userListId": "lst1"},
                },
                "emailNotificationTypes": ["mention"],
                "isBot": true,
                "isCat": true,
                "isExplorable": false,
                "avatarDecorations": [{"id": "deco1", "angle": 0.1, "flipH": true}],
                "rejectQuotes": true,
                "defaultReaction": "❤",
            }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["name"], "Prof");
        assert_eq!(me["followedMessage"], "thanks for following");
        assert_eq!(me["lang"], "en");
        assert_eq!(
            me["fields"],
            serde_json::json!([{"name": "site", "value": "https://example.local"}])
        );
        assert_eq!(me["isLocked"], true);
        assert_eq!(me["hideOnlineStatus"], true);
        assert_eq!(me["onlineStatus"], "unknown");
        assert_eq!(me["requireSigninToViewContents"], true);
        assert_eq!(me["makeNotesFollowersOnlyBefore"], 123);
        assert_eq!(me["publicReactions"], false);
        assert_eq!(me["followApprovalLocalSeconds"], 3600);
        assert_eq!(me["followApprovalRemoteSeconds"], Value::Null);
        assert_eq!(me["preventAiLearning"], false);
        assert_eq!(me["chatScope"], "followers");
        assert_eq!(me["followingVisibility"], "followers");
        assert_eq!(me["followersVisibility"], "private");
        assert_eq!(me["mutedWords"], serde_json::json!([["foo", "bar"], "/^spam/i"]));
        assert_eq!(me["mutedInstances"], serde_json::json!(["spam.example"]));
        assert_eq!(me["notificationRecieveConfig"]["mention"]["type"], "following");
        assert_eq!(me["notificationRecieveConfig"]["follow"]["userListId"], "lst1");
        assert_eq!(me["emailNotificationTypes"], serde_json::json!(["mention"]));
        assert_eq!(
            me["avatarDecorations"],
            serde_json::json!([{"id": "deco1", "angle": 0.1, "flipH": true}])
        );
        assert_eq!(me["rejectQuotes"], true);
        assert_eq!(me["defaultReaction"], "❤");
        assert_eq!(me["verifiedLinks"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn i_update_profile_validation_errors() {
        use crate::application::ports::{InsertMediaInput, MediaStore};

        let (state, _) = test_state();
        let (alice_id, token) = signup_user(&state, "vald").await;

        async fn expect_code(
            state: &ApiState,
            token: &str,
            body: Value,
            code: &str,
        ) -> Value {
            let (status, value) = call(state, "i/update", body, Some(token)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
            assert_eq!(value["code"], code, "{value}");
            value
        }

        expect_code(
            &state,
            &token,
            serde_json::json!({"chatScope": "everyone!"}),
            "INVALID_PARAM",
        )
        .await;
        expect_code(
            &state,
            &token,
            serde_json::json!({"name": "x".repeat(51)}),
            "INVALID_PARAM",
        )
        .await;
        expect_code(
            &state,
            &token,
            serde_json::json!({"avatarDecorations": [{"id": "d", "angle": 0.9}]}),
            "INVALID_PARAM",
        )
        .await;
        // Plain (non-`/regex/`) single words are rejected like upstream.
        expect_code(
            &state,
            &token,
            serde_json::json!({"mutedWords": ["plain"]}),
            "INVALID_REGEXP",
        )
        .await;
        expect_code(
            &state,
            &token,
            serde_json::json!({"mutedWords": ["/(/"]}),
            "INVALID_REGEXP",
        )
        .await;
        // 201 mute chars exceed the default wordMuteLimit of 200.
        expect_code(
            &state,
            &token,
            serde_json::json!({"mutedWords": [format!("/{}/", "a".repeat(199))]}),
            "TOO_MANY_MUTED_WORDS",
        )
        .await;
        // Own username resolves locally to self.
        expect_code(
            &state,
            &token,
            serde_json::json!({"alsoKnownAs": ["vald"]}),
            "FORBIDDEN_TO_SET_YOURSELF",
        )
        .await;
        // `uniqueItems` parity.
        expect_code(
            &state,
            &token,
            serde_json::json!({"alsoKnownAs": ["someone@example.local", "someone@example.local"]}),
            "INVALID_PARAM",
        )
        .await;
        // No pages store yet: any non-null page is missing.
        expect_code(
            &state,
            &token,
            serde_json::json!({"pinnedPageId": "pag00000000000001"}),
            "NO_SUCH_PAGE",
        )
        .await;
        expect_code(
            &state,
            &token,
            serde_json::json!({"avatarId": "mid00000000000009"}),
            "NO_SUCH_AVATAR",
        )
        .await;
        expect_code(
            &state,
            &token,
            serde_json::json!({"notificationRecieveConfig": {"mention": {"type": "sometimes"}}}),
            "INVALID_PARAM",
        )
        .await;
        // Misskey spells it `name`; camelCase guesses stay unsupported.
        expect_code(
            &state,
            &token,
            serde_json::json!({"displayName": "x"}),
            "UNSUPPORTED_FIELD",
        )
        .await;

        // A non-image file cannot be an avatar; an image can (and clears).
        let text_file = state
            .services
            .media
            .insert_metadata(InsertMediaInput {
                id: None,
                owner_id: Some(alice_id.clone()),
                name: "note.txt".to_string(),
                mime: "text/plain".to_string(),
                size_bytes: 8,
                hash: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
                blob_key: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_string(),
                blurhash: None,
                properties: crate::domain::media::MediaProperties::default(),
                is_sensitive: false,
                is_link: false,
                url: "https://example.local/files/note".to_string(),
                thumbnail_url: None,
                comment: None,
                source_url: None,
                max_file_size: 10_000_000,
            })
            .unwrap();
        expect_code(
            &state,
            &token,
            serde_json::json!({"avatarId": text_file.id}),
            "AVATAR_NOT_AN_IMAGE",
        )
        .await;
        let image_file = state
            .services
            .media
            .insert_metadata(InsertMediaInput {
                id: None,
                owner_id: Some(alice_id.clone()),
                name: "photo.png".to_string(),
                mime: "image/png".to_string(),
                size_bytes: 512,
                hash: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
                blob_key: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_string(),
                blurhash: None,
                properties: crate::domain::media::MediaProperties::default(),
                is_sensitive: false,
                is_link: false,
                url: "https://example.local/files/photo".to_string(),
                thumbnail_url: None,
                comment: None,
                source_url: None,
                max_file_size: 10_000_000,
            })
            .unwrap();
        let (status, me) = call(
            &state,
            "i/update",
            serde_json::json!({"avatarId": image_file.id}),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["avatarId"], image_file.id);
        let (status, me) = call(
            &state,
            "i/update",
            serde_json::json!({"avatarId": null}),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["avatarId"], Value::Null);
    }

    #[tokio::test]
    async fn miauth_gen_token_and_check_are_one_shot() {
        let (state, _) = test_state();
        let (_, token) = signup_user(&state, "dave").await;

        let (status, value) = call(
            &state,
            "miauth/gen-token",
            serde_json::json!({
                "session": "sess-001",
                "permission": ["read:account"],
                "name": "Test App",
            }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let app_token = value["token"].as_str().unwrap().to_string();
        assert_eq!(app_token.len(), 32);

        // The minted token carries only the granted permission.
        let (status, _) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "should fail" }),
            Some(&app_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // First check consumes the session.
        let response = miauth_check(
            State(state.clone()),
            test_peer(),
            HeaderMap::new(),
            Path("sess-001".to_string()),
        )
        .await
        .unwrap();
        let (status, value, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["ok"], true);
        assert_eq!(value["token"], app_token);
        assert_eq!(value["user"]["username"], "dave");

        // Second check is spent.
        let response = miauth_check(
            State(state.clone()),
            test_peer(),
            HeaderMap::new(),
            Path("sess-001".to_string()),
        )
            .await
            .unwrap();
        let (status, value, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["ok"], false);
    }

    #[tokio::test]
    async fn miauth_web_flow_with_callback_redirects() {
        let (state, _) = test_state();
        signup_user(&state, "erin").await;

        // Approval page renders.
        let mut query = HashMap::new();
        query.insert("name".to_string(), "Web App".to_string());
        query.insert(
            "callback".to_string(),
            "https://app.example/cb".to_string(),
        );
        let response = miauth_page(
            State(state.clone()),
            Path("websess".to_string()),
            Query(query),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Approve without a callback -> confirmation page + cookie.
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let response = miauth_submit(
            State(state.clone()),
            test_peer(),
            Path("websess".to_string()),
            headers,
            Bytes::from("username=erin&password=s3cret-pw"),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cookie = response.headers()[SET_COOKIE].to_str().unwrap().to_string();
        assert!(cookie.starts_with("pinetree_session="));

        // Session cookie authenticates API calls.
        let mut cookie_headers = HeaderMap::new();
        cookie_headers.insert(
            COOKIE,
            cookie
                .split(';')
                .next()
                .unwrap()
                .parse::<HeaderValue>()
                .unwrap(),
        );
        let response = match api_dispatch(
            State(state.clone()),
            test_peer(),
            Path("i".to_string()),
            cookie_headers,
            Bytes::from("{}"),
        )
        .await
        {
            Ok(response) => response,
            Err(err) => err.into_response(),
        };
        let (status, value, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["username"], "erin");

        // Approve with a safe callback -> 302 to callback + session.
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let response = miauth_submit(
            State(state.clone()),
            test_peer(),
            Path("cbsess".to_string()),
            headers,
            Bytes::from(
                "username=erin&password=s3cret-pw&callback=https%3A%2F%2Fapp.example%2Fcb",
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers()[LOCATION],
            "https://app.example/cb?session=cbsess"
        );

        // The approved session checks out exactly once.
        let response = miauth_check(
            State(state.clone()),
            test_peer(),
            HeaderMap::new(),
            Path("cbsess".to_string()),
        )
            .await
            .unwrap();
        let (status, value, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["ok"], true);
    }

    #[tokio::test]
    async fn legacy_auth_flow() {
        let (state, _) = test_state();
        let (_, token) = signup_user(&state, "frank").await;

        let (status, value) = call(
            &state,
            "auth/session/generate",
            serde_json::json!({ "appSecret": "s3cret" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let session_token = value["token"].as_str().unwrap().to_string();
        assert!(value["url"].as_str().unwrap().contains(&session_token));

        let (status, shown) = call(
            &state,
            "auth/session/show",
            serde_json::json!({ "token": session_token }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["token"], session_token);

        // userkey before accept is pending.
        let (status, value) = call(
            &state,
            "auth/session/userkey",
            serde_json::json!({ "appSecret": "s3cret", "token": session_token }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "PENDING_SESSION");

        // Unknown session -> NO_SUCH_SESSION.
        let (status, value) = call(
            &state,
            "auth/session/show",
            serde_json::json!({ "token": "missing" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
        assert_eq!(value["code"], "NO_SUCH_SESSION");

        // Accept authorizes the session…
        let (status, _) = call(
            &state,
            "auth/accept",
            serde_json::json!({ "token": session_token }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // …and userkey now yields an access token + user.
        let (status, value) = call(
            &state,
            "auth/session/userkey",
            serde_json::json!({ "appSecret": "s3cret", "token": session_token }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert!(value["accessToken"].as_str().is_some());
        assert_eq!(value["user"]["username"], "frank");
    }

    #[tokio::test]
    async fn miauth_hardening_expiry_params_suspension() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_, _bob_token) = signup_user(&state, "bob").await;

        // The approval page renders without persisting anything (Batch N
        // WS7: the GET never writes, even for attacker-chosen ids).
        let mut query = HashMap::new();
        query.insert("name".to_string(), "Probe".to_string());
        let response = miauth_page(
            State(state.clone()),
            Path("probe-sess".to_string()),
            Query(query),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            state.services.auth.get_miauth_session("probe-sess").unwrap().is_none(),
            "GET minted no row"
        );

        // gen-token requires both params (upstream parity).
        let (status, value) = call(
            &state,
            "miauth/gen-token",
            serde_json::json!({ "permission": ["read:account"] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        let (status, value) = call(
            &state,
            "miauth/gen-token",
            serde_json::json!({ "session": "s1" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");

        // Mint, then let the approval window lapse: check reads ok:false
        // and the row is gone (deleted on read).
        let (status, value) = call(
            &state,
            "miauth/gen-token",
            serde_json::json!({ "session": "expiry-sess", "permission": ["read:account"] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        clock.advance_ms(31 * 60 * 1000);
        let response = miauth_check(
            State(state.clone()),
            test_peer(),
            HeaderMap::new(),
            Path("expiry-sess".to_string()),
        )
        .await
        .unwrap();
        let (status, value, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["ok"], false);
        assert!(
            state.services.auth.get_miauth_session("expiry-sess").unwrap().is_none(),
            "expired row deleted on read"
        );

        // Fresh session still approves after unrelated expiry.
        let (status, value) = call(
            &state,
            "miauth/gen-token",
            serde_json::json!({ "session": "fresh-sess", "permission": ["read:account"] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");

        // Fresh session still approves after unrelated expiry.
        let response = miauth_check(
            State(state.clone()),
            test_peer(),
            HeaderMap::new(),
            Path("fresh-sess".to_string()),
        )
        .await
        .unwrap();
        let (status, value, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["ok"], true);

        // Suspended accounts cannot approve: submit renders the error page
        // and mints no session row.
        let (carol_id, _) = signup_user(&state, "carol").await;
        state.services.set_role(None, &alice_id, false, true).expect("moderator");
        state.services.suspend_user(&alice_id, &carol_id).expect("suspend");
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let response = miauth_submit(
            State(state.clone()),
            test_peer(),
            Path("susp-sess".to_string()),
            headers,
            Bytes::from("username=carol&password=s3cret-pw"),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("suspended"), "{body:?}");
        assert!(
            state.services.auth.get_miauth_session("susp-sess").unwrap().is_none(),
            "no session row minted for a suspended approver"
        );

        // Legacy sessions expire the same way: userkey on an expired token
        // reads as missing (NO_SUCH_SESSION, never PENDING).
        let (status, value) = call(
            &state,
            "auth/session/generate",
            serde_json::json!({ "appSecret": "s3cret" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let legacy_token = value["token"].as_str().unwrap().to_string();
        clock.advance_ms(31 * 60 * 1000);
        let (status, value) = call(
            &state,
            "auth/session/userkey",
            serde_json::json!({ "appSecret": "s3cret", "token": legacy_token }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
        assert_eq!(value["code"], "NO_SUCH_SESSION");

        // Wrong appSecret on userkey fails closed (late, like upstream).
        let (status, value) = call(
            &state,
            "auth/session/generate",
            serde_json::json!({ "appSecret": "s3cret" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let legacy_token = value["token"].as_str().unwrap().to_string();
        let (status, value) = call(
            &state,
            "auth/session/userkey",
            serde_json::json!({ "appSecret": "wrong", "token": legacy_token }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{value}");
    }

    #[tokio::test]
    async fn notes_lifecycle() {
        let (state, _) = test_state();
        let (_, token) = signup_user(&state, "gina").await;

        let (status, value) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "hello world" }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let note_id = value["createdNote"]["id"].as_str().unwrap().to_string();
        assert_eq!(value["createdNote"]["text"], "hello world");

        // channelId / poll are rejected per the Phase 1 surface.
        let (status, value) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "x", "channelId": "c" }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["code"], "UNSUPPORTED_FIELD");
        let (status, value) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "x", "poll": {} }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["code"], "INVALID_PARAM");

        let (status, shown) = call(
            &state,
            "notes/show",
            serde_json::json!({ "noteId": note_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["id"], note_id);

        let (status, _) = call(
            &state,
            "notes/delete",
            serde_json::json!({ "noteId": note_id }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, value) = call(
            &state,
            "notes/show",
            serde_json::json!({ "noteId": note_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
    }

    #[tokio::test]
    async fn follows_and_notifications() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        let (status, lite) = call(
            &state,
            "following/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{lite}");
        assert_eq!(lite["id"], bob_id);

        // Bob sees a follow notification. The endpoint no longer drains the
        // worker: delivery and notify jobs run in the background, so this
        // test drains explicitly to stay deterministic.
        state
            .worker
            .drain(64)
            .expect("drain follow notification job");
        let (status, notifications) = call(
            &state,
            "i/notifications",
            serde_json::json!({}),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notifications}");
        assert_eq!(notifications[0]["type"], "follow");
        assert_eq!(notifications[0]["userId"], alice_id);

        let (status, lite) = call(
            &state,
            "following/delete",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{lite}");
        assert_eq!(lite["id"], bob_id);
    }

    #[tokio::test]
    async fn timelines_page_with_cursors() {
        let (state, clock) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        for text in ["one", "two", "three"] {
            let (status, _) = call(
                &state,
                "notes/create",
                serde_json::json!({ "text": text }),
                Some(&alice_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            clock.advance_ms(1_000);
        }

        let (status, home) = call(
            &state,
            "notes/timeline",
            serde_json::json!({ "limit": 10 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{home}");
        // Bob follows nobody: empty home timeline.
        assert_eq!(home.as_array().unwrap().len(), 0);

        let (status, local) = call(
            &state,
            "notes/local-timeline",
            serde_json::json!({ "limit": 10 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{local}");
        assert_eq!(local.as_array().unwrap().len(), 3);

        let newest = local[0]["id"].as_str().unwrap().to_string();
        let oldest = local[2]["id"].as_str().unwrap().to_string();
        let (status, page) = call(
            &state,
            "notes/local-timeline",
            serde_json::json!({ "untilId": newest }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page.as_array().unwrap().len(), 2);
        let (status, page) = call(
            &state,
            "notes/local-timeline",
            serde_json::json!({ "sinceId": oldest }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page.as_array().unwrap().len(), 2);

        // Hybrid requires auth and merges home + local.
        let (status, _) = call(
            &state,
            "notes/hybrid-timeline",
            serde_json::json!({}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, hybrid) = call(
            &state,
            "notes/hybrid-timeline",
            serde_json::json!({ "limit": 10 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{hybrid}");
        assert_eq!(hybrid.as_array().unwrap().len(), 3);

        // Bad limits are rejected.
        let (status, value) = call(
            &state,
            "notes/local-timeline",
            serde_json::json!({ "limit": 500 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["code"], "INVALID_PARAM");
    }

    #[tokio::test]
    async fn global_timeline_carries_remote_public_notes() {
        use crate::application::ports::{
            AccountStore, CreateAccountInput, CreateNoteInput,
        };
        use crate::domain::note::Visibility;

        let (state, clock) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        // Remote actor, as cached from federation.
        let remote = state
            .services
            .accounts
            .create_account(CreateAccountInput {
                handle: "rina".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/rina".to_string()),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .expect("remote account");

        async fn post(
            state: &ApiState,
            body: Value,
            token: Option<&str>,
        ) -> String {
            let (status, value) = call(state, "notes/create", body, token).await;
            assert_eq!(status, StatusCode::OK, "{value}");
            value["createdNote"]["id"].as_str().unwrap().to_string()
        }

        let alice_public = post(
            &state,
            serde_json::json!({ "text": "alice public" }),
            Some(&alice_token),
        )
        .await;
        clock.advance_ms(1_000);
        post(
            &state,
            serde_json::json!({ "text": "alice home", "visibility": "home" }),
            Some(&alice_token),
        )
        .await;
        clock.advance_ms(1_000);
        post(
            &state,
            serde_json::json!({ "text": "alice local-only", "localOnly": true }),
            Some(&alice_token),
        )
        .await;
        clock.advance_ms(1_000);
        post(
            &state,
            serde_json::json!({ "text": "alice followers", "visibility": "followers" }),
            Some(&alice_token),
        )
        .await;
        clock.advance_ms(1_000);
        // A self-reply: excluded from local-timeline by default, but the
        // global timeline has no reply filter (Misskey has no `withReplies`
        // on `notes/global-timeline`).
        let alice_reply = post(
            &state,
            serde_json::json!({ "text": "alice self-reply", "replyId": alice_public }),
            Some(&alice_token),
        )
        .await;
        clock.advance_ms(1_000);

        // Remote notes arrive through the same inbound op the inbox uses.
        let mut inbound = |activity: &str, uri: &str, text: &str, visibility: Visibility| {
            let mut input = CreateNoteInput::simple_public(remote.id.clone(), text);
            input.visibility = visibility;
            input.uri = Some(uri.to_string());
            state
                .services
                .handle_inbound_create_note(activity, input, Vec::new())
                .expect("inbound note")
                .expect("first delivery commits")
                .id
        };
        let remote_public = inbound(
            "https://remote.example/activities/1",
            "https://remote.example/notes/1",
            "remote public",
            Visibility::Public,
        );
        clock.advance_ms(1_000);
        inbound(
            "https://remote.example/activities/2",
            "https://remote.example/notes/2",
            "remote followers",
            Visibility::Followers,
        );
        clock.advance_ms(1_000);

        // Anonymous global timeline: local + remote public notes (plus the
        // self-reply, which is public), newest first. Home, local-only and
        // followers notes never appear.
        let (status, global) = call(
            &state,
            "notes/global-timeline",
            serde_json::json!({ "limit": 10 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{global}");
        let ids: Vec<&str> = global
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap())
            .collect();
        assert_eq!(
            ids,
            vec![
                remote_public.as_str(),
                alice_reply.as_str(),
                alice_public.as_str()
            ],
            "{global}"
        );

        // `withReplies` is not a global-timeline param (Misskey parity).
        let (status, value) = call(
            &state,
            "notes/global-timeline",
            serde_json::json!({ "withReplies": true }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "UNSUPPORTED_FIELD");

        // Authenticated reads see the same public surface.
        let (status, authed) = call(
            &state,
            "notes/global-timeline",
            serde_json::json!({ "limit": 10 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{authed}");
        assert_eq!(authed, global);

        // Local timeline still excludes the remote note.
        let (status, local) = call(
            &state,
            "notes/local-timeline",
            serde_json::json!({ "limit": 10 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{local}");
        assert!(
            local
                .as_array()
                .unwrap()
                .iter()
                .all(|n| n["id"].as_str().unwrap() != remote_public),
            "{local}"
        );

        // `withRenotes=false` drops pure renotes; `withFiles=true` keeps
        // only notes with attachments (none here).
        let alice_renote = post(
            &state,
            serde_json::json!({ "renoteId": alice_public }),
            Some(&alice_token),
        )
        .await;
        clock.advance_ms(1_000);
        let (status, with_renotes) = call(
            &state,
            "notes/global-timeline",
            serde_json::json!({ "limit": 10 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{with_renotes}");
        assert_eq!(with_renotes.as_array().unwrap().len(), ids.len() + 1);
        assert_eq!(
            with_renotes[0]["id"].as_str().unwrap(),
            alice_renote.as_str()
        );
        let (status, no_renotes) = call(
            &state,
            "notes/global-timeline",
            serde_json::json!({ "withRenotes": false }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{no_renotes}");
        assert_eq!(no_renotes.as_array().unwrap().len(), ids.len());
        let (status, with_files) = call(
            &state,
            "notes/global-timeline",
            serde_json::json!({ "withFiles": true }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{with_files}");
        assert_eq!(with_files.as_array().unwrap().len(), 0);

        // Cursor pagination over the global timeline.
        let (status, page) = call(
            &state,
            "notes/global-timeline",
            serde_json::json!({ "limit": 1 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page.as_array().unwrap().len(), 1);
        assert_eq!(page[0]["id"].as_str().unwrap(), alice_renote.as_str());
        let (status, rest) = call(
            &state,
            "notes/global-timeline",
            serde_json::json!({ "untilId": alice_renote }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rest}");
        assert_eq!(rest.as_array().unwrap().len(), ids.len());
    }

    #[tokio::test]
    async fn reactions_create_rejects_duplicates() {
        let (state, clock) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;

        let (status, value) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "react to me" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let note_id = value["createdNote"]["id"].as_str().unwrap().to_string();

        let (status, _) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": "❤" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, value) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": "❤" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "ALREADY_REACTED");

        clock.advance_ms(5_000);
        let (status, _) = call(
            &state,
            "notes/reactions/delete",
            serde_json::json!({ "noteId": note_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn users_endpoints() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        signup_user(&state, "bob").await;

        let (status, shown) = call(
            &state,
            "users/show",
            serde_json::json!({ "userId": alice_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        // Own account packs as MeDetailed.
        assert!(shown.get("policies").is_some());

        let (status, shown) = call(
            &state,
            "users/show",
            serde_json::json!({ "username": "bob" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["username"], "bob");

        let (status, shown) = call(
            &state,
            "users/show",
            serde_json::json!({ "userIds": [alice_id] }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown.as_array().unwrap().len(), 1);

        let (status, results) = call(
            &state,
            "users/search",
            serde_json::json!({ "query": "ali", "limit": 10 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{results}");
        assert!(results.as_array().unwrap().iter().any(|u| u["username"] == "alice"));

        let (status, value) = call(
            &state,
            "username/available",
            serde_json::json!({ "username": "alice" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["available"], false);
        let (status, value) = call(
            &state,
            "username/available",
            serde_json::json!({ "username": "fresh" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["available"], true);
        let (status, value) = call(
            &state,
            "username/available",
            serde_json::json!({ "username": "has space" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["code"], "INVALID_PARAM");
    }

    #[tokio::test]
    async fn drive_upload_json_and_file_serving() {
        let (state, _) = test_state();
        let (_, token) = signup_user(&state, "heidi").await;

        // Multipart upload.
        let boundary = "BOUND123";
        let multipart_body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"i\"\r\n\r\n{token}\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"hi.txt\"\r\n\
             Content-Type: text/plain\r\n\r\nhello multipart\r\n--{boundary}--\r\n"
        );
        let request = Request::builder()
            .header(
                CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(multipart_body))
            .unwrap();
        let response = drive_files_create(State(state.clone()), test_peer(), request)
            .await
            .unwrap();
        let (status, mp_file, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK, "{mp_file}");
        assert_eq!(mp_file["name"], "hi.txt");

        // JSON upload (base64 content).
        let (status, file) = call(
            &state,
            "drive/files/create",
            serde_json::json!({
                "i": token,
                "name": "hello.txt",
                "content": "aGVsbG8gd29ybGQ=",
            }),
            None,
        )
        .await;
        // Endpoint is behind the wildcard dispatcher; drive uploads have a
        // dedicated route, so this must 404 as an unknown endpoint.
        assert_eq!(status, StatusCode::NOT_FOUND, "{file}");

        let request = Request::builder()
            .header(CONTENT_TYPE, "application/json")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "name": "hello.txt",
                    "content": "aGVsbG8gd29ybGQ=",
                }))
                .unwrap(),
            ))
            .unwrap();
        let response = drive_files_create(State(state.clone()), test_peer(), request)
            .await
            .unwrap();
        let (status, file, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK, "{file}");
        let file_id = file["id"].as_str().unwrap().to_string();
        assert_eq!(file["name"], "hello.txt");

        // Served back with the stored content type and bytes.
        let response = serve_file(State(state.clone()), Path(file_id.clone()))
            .await
            .unwrap();
        assert_eq!(response.headers()[CONTENT_TYPE], "text/plain");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"hello world");
        let _ = file_id;
    }

    /// Streaming chunk source that counts every byte the server pulls.
    /// Slice 6 (H1): proves an oversized upload aborts early instead of
    /// buffering the whole body.
    struct CountingChunks {
        remaining: usize,
        chunk: usize,
        yielded: Arc<AtomicUsize>,
    }

    impl futures_core::Stream for CountingChunks {
        type Item = Result<Bytes, std::io::Error>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            if self.remaining == 0 {
                return std::task::Poll::Ready(None);
            }
            let n = self.chunk.min(self.remaining);
            self.remaining -= n;
            self.yielded
                .fetch_add(n, std::sync::atomic::Ordering::SeqCst);
            std::task::Poll::Ready(Some(Ok(Bytes::from(vec![0x78; n]))))
        }
    }

    #[tokio::test]
    async fn oversized_upload_rejected_without_buffering() {
        // Slice 6 (H1): a streaming body larger than max_file_size + 1 MiB
        // must fail 413 while pulling only ~cap bytes from the source.
        let (state, _) = test_state();
        let (_, token) = signup_user(&state, "uploader_cap").await;
        let cap = upload_cap_bytes(&state.services);
        let chunk = 64 * 1024;
        let total = cap + 5 * 1024 * 1024;
        let yielded = Arc::new(AtomicUsize::new(0));
        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/form-data; boundary=BOUND123")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::from_stream(CountingChunks {
                remaining: total,
                chunk,
                yielded: yielded.clone(),
            }))
            .unwrap();
        let err = drive_files_create(State(state.clone()), test_peer(), request)
            .await
            .unwrap_err();
        assert_eq!(err.code, "MAX_FILE_SIZE_EXCEEDED");
        assert_eq!(err.http_status, 413);
        let pulled = yielded.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            pulled < total,
            "must abort before reading the whole body: pulled {pulled} of {total}"
        );
        assert!(
            pulled <= cap + chunk,
            "must stay bounded by the cap: pulled {pulled}, cap {cap}"
        );
    }

    #[tokio::test]
    async fn drive_upload_header_auth_checked_before_body_read() {
        // Slice 6 (H1): a bad `Authorization: Bearer` token fails 401
        // without pulling a single body byte.
        let (state, _) = test_state();
        let yielded = Arc::new(AtomicUsize::new(0));
        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/form-data; boundary=BOUND123")
            .header(AUTHORIZATION, "Bearer wrong-token")
            .body(Body::from_stream(CountingChunks {
                remaining: 4 * 1024 * 1024,
                chunk: 64 * 1024,
                yielded: yielded.clone(),
            }))
            .unwrap();
        let err = drive_files_create(State(state.clone()), test_peer(), request)
            .await
            .unwrap_err();
        assert_eq!(err.http_status, 401);
        assert_eq!(
            yielded.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "no body byte may be read before header auth fails"
        );
    }

    #[tokio::test]
    async fn oversized_json_body_returns_413() {
        // Slice 6 (H1): a JSON body over 1 MiB is rejected 413 in the
        // Misskey dispatch path even when called directly.
        let (state, _) = test_state();
        let (_, token) = signup_user(&state, "json_cap").await;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {token}").parse::<HeaderValue>().unwrap(),
        );
        let big = "x".repeat(MAX_JSON_BODY_BYTES + 1);
        let raw = format!("{{\"text\":{}}}", serde_json::to_string(&big).unwrap());
        let body = Bytes::from(raw.into_bytes());
        assert!(body.len() > MAX_JSON_BODY_BYTES);
        let err =
            api_dispatch_blocking(
                state.clone(),
                "notes/create".to_string(),
                headers,
                body,
                "127.0.0.1".to_string(),
            )
            .unwrap_err();
        assert_eq!(err.code, "MAX_FILE_SIZE_EXCEEDED");
        assert_eq!(err.http_status, 413);
    }

    #[tokio::test]
    async fn drive_upload_concurrency_limit_returns_429() {
        // Slice 6 (H1, decision D-c): with both per-IP upload slots held,
        // another upload from the same IP fails Misskey-shaped 429.
        let (state, _) = test_state();
        let (_, token) = signup_user(&state, "uploader_busy").await;
        let _first = state.upload_slots.acquire("127.0.0.1").expect("slot 1");
        let _second = state.upload_slots.acquire("127.0.0.1").expect("slot 2");
        let boundary = "BUSY123";
        let multipart_body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"i\"\r\n\r\n{token}\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"tiny.txt\"\r\n\
             Content-Type: text/plain\r\n\r\ntiny\r\n--{boundary}--\r\n"
        );
        let request = Request::builder()
            .header(
                CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(multipart_body))
            .unwrap();
        let err = drive_files_create(State(state.clone()), test_peer(), request)
            .await
            .unwrap_err();
        assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
        assert_eq!(err.http_status, 429);
    }

    #[tokio::test]
    async fn unknown_endpoint_is_404() {
        let (state, _) = test_state();
        let (status, value) = call(&state, "nope/not-real", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(value["code"], "UNKNOWN_API_ENDPOINT");
    }

    #[tokio::test]
    async fn serve_file_sets_security_headers() {
        // Slice 7 (H2, written before the fix): every served file carries
        // nosniff + CSP; octet-stream/text/plain (and anything outside the
        // inline-safe set) serve as attachments; sniffed images stay inline.
        let (state, _) = test_state();
        let (account_id, _) = signup_user(&state, "serena").await;
        let mut png = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
        png.extend_from_slice(&[1, 2, 3, 4]);
        let png_id = state
            .services
            .upload_media(&account_id, "pic.png".to_string(), &png, None, false, true)
            .unwrap()
            .id;
        let txt_id = state
            .services
            .upload_media(
                &account_id,
                "hi.txt".to_string(),
                b"hello world",
                None,
                false,
                true,
            )
            .unwrap()
            .id;
        // Non-UTF-8 bytes named `.html`: stored (and served) as
        // octet-stream, never as HTML.
        let bin_id = state
            .services
            .upload_media(
                &account_id,
                "evil.html".to_string(),
                &[0xFF, 0xFE, 0x00, 0x01],
                None,
                false,
                true,
            )
            .unwrap()
            .id;

        let response = serve_file(State(state.clone()), Path(png_id))
            .await
            .unwrap();
        assert_eq!(response.headers()[CONTENT_TYPE], "image/png");
        assert_eq!(
            response.headers()["x-content-type-options"],
            "nosniff"
        );
        assert_eq!(
            response.headers()["content-security-policy"],
            "default-src 'none'; sandbox"
        );
        assert!(
            response.headers().get(CONTENT_DISPOSITION).is_none(),
            "sniffed images stay inline"
        );

        let response = serve_file(State(state.clone()), Path(txt_id))
            .await
            .unwrap();
        assert_eq!(response.headers()[CONTENT_TYPE], "text/plain");
        assert_eq!(
            response.headers()["x-content-type-options"],
            "nosniff"
        );
        assert_eq!(
            response.headers()["content-security-policy"],
            "default-src 'none'; sandbox"
        );
        assert_eq!(
            response.headers()[CONTENT_DISPOSITION],
            "attachment"
        );

        let response = serve_file(State(state.clone()), Path(bin_id))
            .await
            .unwrap();
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            "application/octet-stream"
        );
        assert_eq!(
            response.headers()[CONTENT_DISPOSITION],
            "attachment"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), &[0xFF, 0xFE, 0x00, 0x01]);
    }

    #[test]
    fn inline_safe_mime_set() {
        assert!(is_inline_safe_mime("image/png"));
        assert!(is_inline_safe_mime("audio/mpeg"));
        assert!(is_inline_safe_mime("video/webm"));
        assert!(!is_inline_safe_mime("application/octet-stream"));
        assert!(!is_inline_safe_mime("text/plain"));
        assert!(!is_inline_safe_mime("application/pdf"));
        assert!(!is_inline_safe_mime("image/svg+xml"));
    }

    #[tokio::test]
    async fn file_named_redirects_and_identicon_serves_svg() {
        let response = serve_file_named(Path(("abc".to_string(), "hi.png".to_string()))).await;
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(response.headers()[LOCATION], "/files/abc");

        let response = identicon(Path("alice".to_string())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "image/svg+xml");
    }

    // --- Phase 3 REST profile ------------------------------------------------

    async fn create_note(state: &ApiState, body: Value, token: &str) -> String {
        let (status, value) = call(state, "notes/create", body, Some(token)).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        value["createdNote"]["id"].as_str().unwrap().to_string()
    }

    fn note_ids(value: &Value) -> Vec<String> {
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn blocking_mute_endpoints_and_relation() {
        let (state, _clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // blocking/create packs the blockee as UserDetailedNotMe.
        let (status, packed) = call(
            &state,
            "blocking/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{packed}");
        assert_eq!(packed["id"], bob_id);

        // Duplicate → ALREADY_BLOCKING (exact Misskey code).
        let (status, err) = call(
            &state,
            "blocking/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
        assert_eq!(err["code"], "ALREADY_BLOCKING");
        assert_eq!(err["error"]["code"], "ALREADY_BLOCKING");

        // Self-block → BLOCKEE_IS_YOURSELF.
        let (status, err) = call(
            &state,
            "blocking/create",
            serde_json::json!({ "userId": alice_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
        assert_eq!(err["code"], "BLOCKEE_IS_YOURSELF");

        // Unknown user → NO_SUCH_USER.
        let (status, err) = call(
            &state,
            "blocking/create",
            serde_json::json!({ "userId": "aid00000000009999" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
        assert_eq!(err["code"], "NO_SUCH_USER");

        // blocking/list carries the row with the packed blockee.
        let (status, list) = call(
            &state,
            "blocking/list",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list.as_array().unwrap().len(), 1);
        assert_eq!(list[0]["blockeeId"], bob_id);
        assert_eq!(list[0]["blockee"]["id"], bob_id);

        // users/relation reflects the block both ways (single → 1-array).
        let (status, rel) = call(
            &state,
            "users/relation",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rel}");
        assert_eq!(rel.as_array().unwrap().len(), 1);
        assert_eq!(rel[0]["isBlocking"], true);
        assert_eq!(rel[0]["isBlocked"], false);
        let (status, rel) = call(
            &state,
            "users/relation",
            serde_json::json!({ "userId": [bob_id.clone(), alice_id.clone()] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rel}");
        assert_eq!(rel[0]["id"], bob_id);
        assert_eq!(rel[1]["isBlocked"], true);
        assert_eq!(rel[1]["isBlocking"], false);
        assert_eq!(rel[1]["id"], alice_id);

        // Blocked follow → BLOCKING / BLOCKED through the HTTP envelope.
        let (status, err) = call(
            &state,
            "following/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
        assert_eq!(err["code"], "BLOCKING");
        let (status, err) = call(
            &state,
            "following/create",
            serde_json::json!({ "userId": alice_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
        assert_eq!(err["code"], "BLOCKED");

        // blocking/delete removes the row; repeat → NOT_BLOCKING.
        let (status, packed) = call(
            &state,
            "blocking/delete",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{packed}");
        assert_eq!(packed["id"], bob_id);
        let (status, err) = call(
            &state,
            "blocking/delete",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
        assert_eq!(err["code"], "NOT_BLOCKING");

        // mute/create → 204; duplicate → ALREADY_MUTING.
        let (status, _) = call(
            &state,
            "mute/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, err) = call(
            &state,
            "mute/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
        assert_eq!(err["code"], "ALREADY_MUTING");
        // mute/list carries the row; expiresAt is null for indefinite mutes.
        let (status, list) = call(
            &state,
            "mute/list",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list.as_array().unwrap().len(), 1);
        assert_eq!(list[0]["muteeId"], bob_id);
        assert!(list[0]["expiresAt"].is_null());
        // mute/delete → 204; repeat → NOT_MUTING.
        let (status, _) = call(
            &state,
            "mute/delete",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, err) = call(
            &state,
            "mute/delete",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
        assert_eq!(err["code"], "NOT_MUTING");

        // renote-mute endpoints mirror mute (no expiresAt).
        let (status, _) = call(
            &state,
            "renote-mute/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, err) = call(
            &state,
            "renote-mute/create",
            serde_json::json!({ "userId": alice_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
        assert_eq!(err["code"], "MUTEE_IS_YOURSELF");
        let (status, list) = call(
            &state,
            "renote-mute/list",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list.as_array().unwrap().len(), 1);
        assert_eq!(list[0]["muteeId"], bob_id);
        let (status, _) = call(
            &state,
            "renote-mute/delete",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn phase3_users_notes_filters_and_pagination() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        let root = create_note(
            &state,
            serde_json::json!({ "text": "root note" }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);
        let reply = create_note(
            &state,
            serde_json::json!({ "text": "a reply", "replyId": root }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);
        let pure = create_note(
            &state,
            serde_json::json!({ "renoteId": root }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);
        let quote = create_note(
            &state,
            serde_json::json!({ "renoteId": root, "text": "quoted" }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);

        // Defaults: no replies, renotes included.
        let (status, notes) = call(
            &state,
            "users/notes",
            serde_json::json!({ "userId": alice_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        assert_eq!(note_ids(&notes), vec![quote.clone(), pure.clone(), root.clone()]);

        // With replies.
        let (status, notes) = call(
            &state,
            "users/notes",
            serde_json::json!({ "userId": alice_id, "withReplies": true }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        assert_eq!(
            note_ids(&notes),
            vec![quote.clone(), pure.clone(), reply.clone(), root.clone()]
        );

        // Without renotes: the pure renote drops out, the quote stays.
        let (status, notes) = call(
            &state,
            "users/notes",
            serde_json::json!({ "userId": alice_id, "withRenotes": false }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        assert_eq!(note_ids(&notes), vec![quote.clone(), root.clone()]);

        // Cursor pagination.
        let (status, page) = call(
            &state,
            "users/notes",
            serde_json::json!({ "userId": alice_id, "limit": 1 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(note_ids(&page), vec![quote.clone()]);
        let (status, page) = call(
            &state,
            "users/notes",
            serde_json::json!({ "userId": alice_id, "untilId": quote }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page.as_array().unwrap().len(), 2);

        // Unknown author is NO_SUCH_USER (Misskey parity).
        let (status, value) = call(
            &state,
            "users/notes",
            serde_json::json!({ "userId": "aid00000000000099" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_USER");

        // Param validation.
        let (status, value) = call(
            &state,
            "users/notes",
            serde_json::json!({}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        // Misskey parity: withReplies + withFiles is rejected, not applied.
        let (status, value) = call(
            &state,
            "users/notes",
            serde_json::json!({
                "userId": "aid00000000000001",
                "withReplies": true,
                "withFiles": true,
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "BOTH_WITH_REPLIES_AND_WITH_FILES");
        let (status, value) = call(
            &state,
            "users/notes",
            serde_json::json!({ "userId": alice_id, "limit": 500 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        let (status, value) = call(
            &state,
            "users/notes",
            serde_json::json!({ "userId": alice_id, "channelId": "x" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "UNSUPPORTED_FIELD");
    }

    #[tokio::test]
    async fn phase3_note_relations() {
        let (state, clock) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        let root = create_note(
            &state,
            serde_json::json!({ "text": "root" }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);
        let reply1 = create_note(
            &state,
            serde_json::json!({ "text": "reply one", "replyId": root }),
            &bob_token,
        )
        .await;
        clock.advance_ms(1_000);
        let reply2 = create_note(
            &state,
            serde_json::json!({ "text": "reply two", "replyId": root }),
            &bob_token,
        )
        .await;
        clock.advance_ms(1_000);
        let pure = create_note(
            &state,
            serde_json::json!({ "renoteId": root }),
            &bob_token,
        )
        .await;
        clock.advance_ms(1_000);
        let quote = create_note(
            &state,
            serde_json::json!({ "renoteId": root, "text": "quoting" }),
            &bob_token,
        )
        .await;
        clock.advance_ms(1_000);

        let (status, replies) = call(
            &state,
            "notes/replies",
            serde_json::json!({ "noteId": root }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{replies}");
        assert_eq!(note_ids(&replies), vec![reply2.clone(), reply1.clone()]);

        let (status, renotes) = call(
            &state,
            "notes/renotes",
            serde_json::json!({ "noteId": root }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{renotes}");
        assert_eq!(note_ids(&renotes), vec![quote.clone(), pure.clone()]);

        // Children: replies plus the quote, never the pure renote.
        let (status, children) = call(
            &state,
            "notes/children",
            serde_json::json!({ "noteId": root }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{children}");
        assert_eq!(
            note_ids(&children),
            vec![quote.clone(), reply2.clone(), reply1.clone()]
        );

        // Pagination over children.
        let (status, page) = call(
            &state,
            "notes/children",
            serde_json::json!({ "noteId": root, "limit": 1 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(note_ids(&page), vec![quote.clone()]);
        let (status, page) = call(
            &state,
            "notes/children",
            serde_json::json!({ "noteId": root, "untilId": quote }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(note_ids(&page), vec![reply2.clone(), reply1.clone()]);

        // Unknown targets: renotes 404s (Misskey parity), replies/children
        // answer [].
        let (status, value) = call(
            &state,
            "notes/renotes",
            serde_json::json!({ "noteId": "nid00000000000099" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
        for endpoint in ["notes/replies", "notes/children"] {
            let (status, list) = call(
                &state,
                endpoint,
                serde_json::json!({ "noteId": "nid00000000000099" }),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{endpoint}: {list}");
            assert_eq!(list.as_array().unwrap().len(), 0);
        }

        let (status, value) = call(
            &state,
            "notes/replies",
            serde_json::json!({}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
    }

    #[tokio::test]
    async fn phase3_note_conversation() {
        let (state, clock) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        let a = create_note(
            &state,
            serde_json::json!({ "text": "first" }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);
        let b = create_note(
            &state,
            serde_json::json!({ "text": "second", "replyId": a }),
            &bob_token,
        )
        .await;
        clock.advance_ms(1_000);
        let c = create_note(
            &state,
            serde_json::json!({ "text": "third", "replyId": b }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);

        // Oldest-first ancestor chain.
        let (status, conv) = call(
            &state,
            "notes/conversation",
            serde_json::json!({ "noteId": c }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{conv}");
        assert_eq!(note_ids(&conv), vec![a.clone(), b.clone()]);

        let (status, conv) = call(
            &state,
            "notes/conversation",
            serde_json::json!({ "noteId": b }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{conv}");
        assert_eq!(note_ids(&conv), vec![a.clone()]);

        let (status, conv) = call(
            &state,
            "notes/conversation",
            serde_json::json!({ "noteId": a }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{conv}");
        assert_eq!(conv.as_array().unwrap().len(), 0);

        // Offset skips from the oldest side; limit caps.
        let (status, conv) = call(
            &state,
            "notes/conversation",
            serde_json::json!({ "noteId": c, "offset": 1 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{conv}");
        assert_eq!(note_ids(&conv), vec![b.clone()]);
        let (status, conv) = call(
            &state,
            "notes/conversation",
            serde_json::json!({ "noteId": c, "limit": 1 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{conv}");
        assert_eq!(note_ids(&conv), vec![a.clone()]);

        let (status, value) = call(
            &state,
            "notes/conversation",
            serde_json::json!({ "noteId": "nid00000000000099" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
    }

    #[tokio::test]
    async fn phase3_unrenote() {
        let (state, clock) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        let root = create_note(
            &state,
            serde_json::json!({ "text": "renote me" }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);
        let pure = create_note(
            &state,
            serde_json::json!({ "renoteId": root }),
            &bob_token,
        )
        .await;
        clock.advance_ms(1_000);
        let quote = create_note(
            &state,
            serde_json::json!({ "renoteId": root, "text": "quoting" }),
            &bob_token,
        )
        .await;
        clock.advance_ms(1_000);

        // Unauthenticated callers are rejected before any state change.
        let (status, value) = call(
            &state,
            "notes/unrenote",
            serde_json::json!({ "noteId": root }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{value}");

        let (status, _) = call(
            &state,
            "notes/unrenote",
            serde_json::json!({ "noteId": root }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Both of Bob's renotes are gone; Alice's root survives.
        for gone in [pure, quote] {
            let (status, value) = call(
                &state,
                "notes/show",
                serde_json::json!({ "noteId": gone }),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
            assert_eq!(value["code"], "NO_SUCH_NOTE");
        }
        let (status, _) = call(
            &state,
            "notes/show",
            serde_json::json!({ "noteId": root }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // Idempotent: nothing left to delete is still success. (The shared
        // notes/delete bucket enforces a 1 s minimum interval, so the clock
        // moves past it first.)
        clock.advance_ms(1_000);
        let (status, _) = call(
            &state,
            "notes/unrenote",
            serde_json::json!({ "noteId": root }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        clock.advance_ms(1_000);
        let (status, value) = call(
            &state,
            "notes/unrenote",
            serde_json::json!({ "noteId": "nid00000000000099" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
    }

    #[tokio::test]
    async fn phase3_follow_lists() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, _) = signup_user(&state, "bob").await;
        let (carol_id, carol_token) = signup_user(&state, "carol").await;

        let (status, _) = call(
            &state,
            "following/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        clock.advance_ms(1_000);
        let (status, _) = call(
            &state,
            "following/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&carol_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        clock.advance_ms(1_000);

        // Followers of Bob: Following[] with the follower side populated.
        let (status, followers) = call(
            &state,
            "users/followers",
            serde_json::json!({ "userId": bob_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{followers}");
        let followers = followers.as_array().unwrap();
        assert_eq!(followers.len(), 2);
        // Newest first: Carol followed after Alice.
        assert_eq!(followers[0]["follower"]["id"], carol_id);
        assert_eq!(followers[1]["follower"]["id"], alice_id);
        for entry in followers {
            assert_eq!(entry["followeeId"], bob_id);
            assert_eq!(entry["followee"]["id"], bob_id);
            assert!(entry["id"].as_str().is_some());
            assert!(entry["createdAt"].as_str().is_some());
            assert_eq!(entry["followerId"], entry["follower"]["id"]);
        }

        // Following of Alice: the followee side populated.
        let (status, following) = call(
            &state,
            "users/following",
            serde_json::json!({ "userId": alice_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{following}");
        let following = following.as_array().unwrap();
        assert_eq!(following.len(), 1);
        assert_eq!(following[0]["followee"]["id"], bob_id);
        assert_eq!(following[0]["follower"]["id"], alice_id);

        // username+host resolution (null host = local).
        let (status, via_name) = call(
            &state,
            "users/followers",
            serde_json::json!({ "username": "bob", "host": null }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{via_name}");
        assert_eq!(via_name.as_array().unwrap().len(), 2);
        assert_eq!(via_name[0]["follower"]["id"], carol_id);

        // Cursor pagination.
        let (status, page) = call(
            &state,
            "users/followers",
            serde_json::json!({ "userId": bob_id, "limit": 1 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page.as_array().unwrap().len(), 1);
        assert_eq!(page[0]["follower"]["id"], carol_id);

        // Errors.
        let (status, value) = call(
            &state,
            "users/followers",
            serde_json::json!({ "userId": "aid00000000000099" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_USER");
        let (status, value) = call(
            &state,
            "users/following",
            serde_json::json!({}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        let (status, value) = call(
            &state,
            "users/followers",
            serde_json::json!({ "username": "bob" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
    }

    async fn set_ff_visibility(state: &ApiState, token: &str, followers: &str, following: &str) {
        let (status, value) = call(
            state,
            "i/update",
            serde_json::json!({
                "followersVisibility": followers,
                "followingVisibility": following,
            }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "i/update visibility: {value}");
    }

    async fn ff_lists(
        state: &ApiState,
        target: &str,
        token: Option<&str>,
    ) -> ((StatusCode, Value), (StatusCode, Value)) {
        let followers = call(
            state,
            "users/followers",
            serde_json::json!({ "userId": target }),
            token,
        )
        .await;
        let following = call(
            state,
            "users/following",
            serde_json::json!({ "userId": target }),
            token,
        )
        .await;
        (followers, following)
    }

    fn assert_forbidden(status: StatusCode, value: &Value) {
        // Upstream `FORBIDDEN` is `kind: client` → HTTP 400, not 403.
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "FORBIDDEN");
        assert_eq!(value["error"]["code"], "FORBIDDEN");
        assert_eq!(value["error"]["kind"], "client");
        assert_eq!(value["message"], "Forbidden.");
    }

    #[tokio::test]
    async fn ff_visibility_gates_and_count_masking() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;
        let (carol_id, carol_token) = signup_user(&state, "carol").await;

        // Bob follows Alice; Alice follows Carol.
        for (token, target) in [(&alice_token, carol_id.as_str()), (&bob_token, alice_id.as_str())] {
            let (status, value) = call(
                &state,
                "following/create",
                serde_json::json!({ "userId": target }),
                Some(token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{value}");
        }

        // Public (default): everyone reads both directions.
        let ((status, followers), (status2, following)) = ff_lists(&state, &alice_id, None).await;
        assert_eq!(status, StatusCode::OK, "{followers}");
        assert_eq!(followers.as_array().unwrap().len(), 1);
        assert_eq!(status2, StatusCode::OK, "{following}");
        assert_eq!(following.as_array().unwrap().len(), 1);
        let (status, counts) = call(
            &state,
            "users/show",
            serde_json::json!({ "userId": alice_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{counts}");
        assert_eq!(counts["followersCount"], 1);
        assert_eq!(counts["followingCount"], 1);

        // `followers` mode: owner + follower read; strangers denied.
        set_ff_visibility(&state, &alice_token, "followers", "followers").await;
        let ((status, value), (status2, value2)) = ff_lists(&state, &alice_id, None).await;
        assert_forbidden(status, &value);
        assert_forbidden(status2, &value2);
        let ((status, value), (status2, value2)) =
            ff_lists(&state, &alice_id, Some(&carol_token)).await;
        assert_forbidden(status, &value);
        assert_forbidden(status2, &value2);
        // Bob follows Alice: both directions read fine.
        let ((status, followers), (status2, following)) =
            ff_lists(&state, &alice_id, Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{followers}");
        assert_eq!(followers.as_array().unwrap().len(), 1);
        assert_eq!(status2, StatusCode::OK, "{following}");
        assert_eq!(following[0]["followee"]["id"], carol_id);
        // Self reads fine.
        let ((status, _), (status2, _)) = ff_lists(&state, &alice_id, Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(status2, StatusCode::OK);

        // Count masking in `followers` mode: follower + self see counts,
        // strangers see null.
        let (status, masked) = call(
            &state,
            "users/show",
            serde_json::json!({ "userId": alice_id }),
            Some(&carol_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{masked}");
        assert_eq!(masked["followersCount"], Value::Null);
        assert_eq!(masked["followingCount"], Value::Null);
        let (status, shown) = call(
            &state,
            "users/show",
            serde_json::json!({ "userId": alice_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["followersCount"], 1);
        assert_eq!(shown["followingCount"], 1);
        let (status, me) = call(
            &state,
            "users/show",
            serde_json::json!({ "userId": alice_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["followersCount"], 1);
        assert_eq!(me["followingCount"], 1);

        // `private` mode: only self reads; follower denied; counts masked
        // for everyone but self.
        set_ff_visibility(&state, &alice_token, "private", "private").await;
        let ((status, value), (status2, value2)) =
            ff_lists(&state, &alice_id, Some(&bob_token)).await;
        assert_forbidden(status, &value);
        assert_forbidden(status2, &value2);
        let ((status, _), (status2, _)) = ff_lists(&state, &alice_id, Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(status2, StatusCode::OK);
        let (status, masked) = call(
            &state,
            "users/show",
            serde_json::json!({ "userId": alice_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{masked}");
        assert_eq!(masked["followersCount"], Value::Null);
        assert_eq!(masked["followingCount"], Value::Null);

        // Unknown users still report NO_SUCH_USER (not FORBIDDEN).
        let (status, value) = call(
            &state,
            "users/followers",
            serde_json::json!({ "userId": "aid00000000000099" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_USER");
        let _ = bob_id;
    }

    #[tokio::test]
    async fn notes_reactions_lists_reactors() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;
        let (_, carol_token) = signup_user(&state, "carol").await;
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "hello" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        for (token, reaction) in [(&alice_token, "👍"), (&bob_token, "❤")] {
            let (status, v) = call(
                &state,
                "notes/reactions/create",
                serde_json::json!({ "noteId": note_id, "reaction": reaction }),
                Some(token),
            )
            .await;
            assert_eq!(status, StatusCode::NO_CONTENT, "{v}");
        }

        // Anonymous read of a public note: newest first, upstream shape.
        let (status, rows) = call(
            &state,
            "notes/reactions",
            serde_json::json!({ "noteId": note_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        let rows = rows.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["user"]["id"], bob_id);
        assert_eq!(rows[0]["type"], "❤");
        assert_eq!(rows[1]["user"]["id"], alice_id);
        assert!(rows[1]["createdAt"].is_string() && rows[1].get("note").is_none());

        // `type` filter and paging.
        let (_, only) = call(
            &state,
            "notes/reactions",
            serde_json::json!({ "noteId": note_id, "type": "👍" }),
            None,
        )
        .await;
        assert_eq!(only.as_array().unwrap().len(), 1);
        let (_, page) = call(
            &state,
            "notes/reactions",
            serde_json::json!({ "noteId": note_id, "limit": 1, "untilId": rows[0]["id"] }),
            None,
        )
        .await;
        assert_eq!(page.as_array().unwrap().len(), 1);
        assert_eq!(page[0]["user"]["id"], alice_id);

        // Unknown note, and a followers-only note hidden from a stranger.
        let (status, v) = call(
            &state,
            "notes/reactions",
            serde_json::json!({ "noteId": "nosuchnote0000000" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
        let (_, private) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "fo", "visibility": "followers" }),
            Some(&bob_token),
        )
        .await;
        let pid = private["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, v) = call(
            &state,
            "notes/reactions",
            serde_json::json!({ "noteId": pid }),
            Some(&carol_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
    }

    #[tokio::test]
    async fn users_reactions_gates_and_filters() {
        use crate::application::ports::{AccountStore, CreateAccountInput};

        async fn post(state: &ApiState, body: Value, token: Option<&str>) -> String {
            let (status, value) = call(state, "notes/create", body, token).await;
            assert_eq!(status, StatusCode::OK, "{value}");
            value["createdNote"]["id"].as_str().unwrap().to_string()
        }

        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;
        let (carol_id, carol_token) = signup_user(&state, "carol").await;
        let (dave_id, dave_token) = signup_user(&state, "dave").await;

        // Bob's public note; Alice reacts to it.
        let note_id = post(
            &state,
            serde_json::json!({ "text": "bob note" }),
            Some(&bob_token),
        )
        .await;
        let (status, value) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": "👍" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");

        // Public target: anonymous readers see the reaction with its note.
        let (status, reactions) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": alice_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reactions}");
        let reactions = reactions.as_array().unwrap();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0]["user"]["id"], alice_id);
        assert_eq!(reactions[0]["type"], "👍");
        assert_eq!(reactions[0]["note"]["id"], note_id);
        assert!(reactions[0]["createdAt"].as_str().is_some());

        // Unknown target → NO_SUCH_USER; missing userId → INVALID_PARAM.
        let (status, value) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": "aid00000000000099" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_USER");
        let (status, value) = call(&state, "users/reactions", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");

        // Remote target → IS_REMOTE_USER (exact upstream strings).
        let remote = state
            .services
            .accounts
            .create_account(CreateAccountInput {
                handle: "rem".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/rem".to_string()),
                inbox: Some("https://remote.example/users/rem/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .expect("remote account");
        let (status, value) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": remote.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "IS_REMOTE_USER");
        assert_eq!(value["error"]["kind"], "client");
        assert_eq!(
            value["message"],
            "Currently unavailable to display reactions of remote users. See https://github.com/misskey-dev/misskey/issues/12964"
        );

        // Private reactions: strangers denied, self allowed.
        let (status, value) = call(
            &state,
            "i/update",
            serde_json::json!({ "publicReactions": false }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        for token in [None, Some(bob_token.as_str()), Some(carol_token.as_str())] {
            let (status, value) = call(
                &state,
                "users/reactions",
                serde_json::json!({ "userId": alice_id }),
                token,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
            assert_eq!(value["code"], "REACTIONS_NOT_PUBLIC");
            assert_eq!(value["error"]["kind"], "client");
            assert_eq!(value["message"], "Reactions of the user is not public.");
        }
        let (status, own) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": alice_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{own}");
        assert_eq!(own.as_array().unwrap().len(), 1);

        // Blocked-by-target reads as silent `[]` (re-open reactions first:
        // the gate order is remote → private → blocked).
        let (status, value) = call(
            &state,
            "i/update",
            serde_json::json!({ "publicReactions": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let (status, value) = call(
            &state,
            "blocking/create",
            serde_json::json!({ "userId": carol_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let (status, reactions) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": alice_id }),
            Some(&carol_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reactions}");
        assert_eq!(reactions.as_array().unwrap().len(), 0);
        // Non-blocked strangers still read.
        let (status, reactions) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": alice_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reactions}");
        assert_eq!(reactions.as_array().unwrap().len(), 1);
        // Unblock so later filter checks exercise visibility, not the gate.
        let (status, value) = call(
            &state,
            "blocking/delete",
            serde_json::json!({ "userId": carol_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");

        // Visibility filter: Alice reacts to Dave's followers-only note
        // (Dave follows Alice? no — Alice cannot see it either unless she
        // follows Dave or authored it). Setup: Dave posts followers-only;
        // Alice follows Dave so her reaction path is valid, then Carol
        // (stranger) must not see the reaction while Alice still does.
        let (status, value) = call(
            &state,
            "following/create",
            serde_json::json!({ "userId": dave_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let followers_note = post(
            &state,
            serde_json::json!({ "text": "dave followers", "visibility": "followers" }),
            Some(&dave_token),
        )
        .await;
        let (status, value) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": followers_note, "reaction": "❤" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        // Carol cannot see Dave's followers-only note, so that reaction is
        // filtered from her view while Bob's public-note reaction stays;
        // Alice (follower) still sees both.
        let (status, reactions) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": alice_id }),
            Some(&carol_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reactions}");
        let reactions = reactions.as_array().unwrap();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0]["note"]["id"], note_id);
        let (status, reactions) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": alice_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reactions}");
        assert_eq!(reactions.as_array().unwrap().len(), 2);

        // Mute filter: Carol mutes Bob (note author). Dave reacts to Bob's
        // public note and to his own public note; Carol's view of Dave's
        // reactions drops the Bob-authored one while the reaction to Dave's
        // own note stays (the target-author exemption).
        let own_note = post(
            &state,
            serde_json::json!({ "text": "dave public" }),
            Some(&dave_token),
        )
        .await;
        let (status, value) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": own_note, "reaction": "🎉" }),
            Some(&dave_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        let (status, value) = call(
            &state,
            "mute/create",
            serde_json::json!({ "userId": bob_id }),
            Some(&carol_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        let (status, value) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": "👀" }),
            Some(&dave_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        let (status, reactions) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": dave_id }),
            Some(&carol_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reactions}");
        let reactions = reactions.as_array().unwrap();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0]["note"]["id"], own_note);
        // Unmuted viewers still see both of Dave's reactions.
        let (status, reactions) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": dave_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reactions}");
        assert_eq!(reactions.as_array().unwrap().len(), 2);
    }

    /// §9 full follower list sweep: 250 follows page through completely in
    /// both directions with no duplicates or gaps across cursor pages.
    #[tokio::test]
    async fn follow_list_sweep_250_both_directions() {
        use crate::application::ports::{AccountStore, CreateAccountInput, FollowStore};

        let (state, _) = test_state();
        let owner = state
            .services
            .accounts
            .create_account(CreateAccountInput::local("owner"), None)
            .expect("owner account");
        let mut fans: Vec<String> = Vec::with_capacity(250);
        for i in 0..250 {
            let fan = state
                .services
                .accounts
                .create_account(CreateAccountInput::local(format!("fan{i:03}")), None)
                .expect("fan account");
            // Both directions: every fan follows the owner and vice versa.
            state
                .services
                .follows
                .follow(&fan.id, &owner.id, false, None, Vec::new())
                .expect("fan follows owner");
            state
                .services
                .follows
                .follow(&owner.id, &fan.id, false, None, Vec::new())
                .expect("owner follows fan");
            fans.push(fan.id);
        }

        // Limit validation: range 1–100, default 10.
        let (status, value) = call(
            &state,
            "users/followers",
            serde_json::json!({ "userId": owner.id, "limit": 101 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        let (status, page) = call(
            &state,
            "users/followers",
            serde_json::json!({ "userId": owner.id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page.as_array().unwrap().len(), 10);

        // Walk every page (limit 100 → 100 + 100 + 50) in both directions.
        async fn walk(state: &ApiState, endpoint: &str, target: &str, side: &str) -> Vec<String> {
            let mut seen: Vec<String> = Vec::new();
            let mut last_entry_id: Option<String> = None;
            loop {
                let mut body = serde_json::json!({ "userId": target, "limit": 100 });
                if let Some(until) = &last_entry_id {
                    body["untilId"] = Value::String(until.clone());
                }
                let (status, page) = call(state, endpoint, body, None).await;
                assert_eq!(status, StatusCode::OK, "{page}");
                let page = page.as_array().unwrap();
                if page.is_empty() {
                    break;
                }
                assert!(page.len() <= 100);
                for entry in page {
                    // Newest-first: entry ids strictly descend across pages.
                    let entry_id = entry["id"].as_str().unwrap().to_string();
                    if let Some(prev) = &last_entry_id {
                        assert!(prev.as_str() > entry_id.as_str(), "cursor order broke");
                    }
                    last_entry_id = Some(entry_id);
                    let side_id = entry[side].as_str().unwrap().to_string();
                    assert!(!seen.contains(&side_id), "duplicate {side_id}");
                    seen.push(side_id);
                }
                if page.len() < 100 {
                    break;
                }
            }
            seen
        }

        let mut newest_first = fans.clone();
        newest_first.reverse();
        let followers = walk(&state, "users/followers", &owner.id, "followerId").await;
        assert_eq!(followers.len(), 250, "no gaps in followers sweep");
        assert_eq!(followers, newest_first, "followers newest-first, no dupes");
        let following = walk(&state, "users/following", &owner.id, "followeeId").await;
        assert_eq!(following.len(), 250, "no gaps in following sweep");
        assert_eq!(following, newest_first, "following newest-first, no dupes");
    }

    #[tokio::test]
    async fn phase3_drive_files() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        let mut png = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
        png.extend_from_slice(&[9, 9, 9]);
        let png_id = state
            .services
            .upload_media(&alice_id, "pic.png".to_string(), &png, None, false, true)
            .unwrap()
            .id;
        clock.advance_ms(1_000);
        let txt_id = state
            .services
            .upload_media(
                &alice_id,
                "hi.txt".to_string(),
                b"hello world",
                None,
                false,
                true,
            )
            .unwrap()
            .id;
        clock.advance_ms(1_000);

        // Newest first.
        let (status, files) = call(
            &state,
            "drive/files",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{files}");
        assert_eq!(
            files
                .as_array()
                .unwrap()
                .iter()
                .map(|f| f["id"].as_str().unwrap().to_string())
                .collect::<Vec<_>>(),
            vec![txt_id.clone(), png_id.clone()]
        );

        // MIME prefix filtering.
        let (status, images) = call(
            &state,
            "drive/files",
            serde_json::json!({ "type": "image/*" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{images}");
        assert_eq!(images.as_array().unwrap().len(), 1);
        assert_eq!(images[0]["id"], png_id);
        let (status, exact) = call(
            &state,
            "drive/files",
            serde_json::json!({ "type": "text/plain" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{exact}");
        assert_eq!(exact.as_array().unwrap().len(), 1);
        assert_eq!(exact[0]["id"], txt_id);

        // Pagination.
        let (status, page) = call(
            &state,
            "drive/files",
            serde_json::json!({ "limit": 1, "untilId": txt_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page.as_array().unwrap().len(), 1);
        assert_eq!(page[0]["id"], png_id);

        // Only the caller's own files.
        let (status, bobs) = call(
            &state,
            "drive/files",
            serde_json::json!({}),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{bobs}");
        assert_eq!(bobs.as_array().unwrap().len(), 0);

        // Param validation.
        for (body, param) in [
            (serde_json::json!({ "sort": "newest" }), "sort"),
            (serde_json::json!({ "folderId": "did00000000000001" }), "folderId"),
            (serde_json::json!({ "type": "not a mime!!" }), "type"),
            (serde_json::json!({ "limit": 0 }), "limit"),
        ] {
            let (status, value) = call(&state, "drive/files", body, Some(&alice_token)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
            assert_eq!(value["code"], "INVALID_PARAM", "param {param}");
        }
        // All six Misskey sort variants are honored (inverted mapping).
        for sort in ["+createdAt", "-createdAt", "+name", "-name", "+size", "-size"] {
            let (status, value) = call(
                &state,
                "drive/files",
                serde_json::json!({ "sort": sort }),
                Some(&alice_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "sort {sort}: {value}");
        }
        let (status, value) = call(
            &state,
            "drive/files",
            serde_json::json!({ "sort": "-createdAt" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let (status, value) = call(
            &state,
            "drive/files",
            serde_json::json!({ "bogus": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "UNSUPPORTED_FIELD");

        // Auth: anonymous callers are rejected.
        let (status, _) = call(&state, "drive/files", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn phase3_drive_file_show_update_delete() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;
        let file_id = state
            .services
            .upload_media(
                &alice_id,
                "doc.txt".to_string(),
                b"file bytes",
                Some("original".to_string()),
                false,
                true,
            )
            .unwrap()
            .id;
        clock.advance_ms(1_000);

        let (status, shown) = call(
            &state,
            "drive/files/show",
            serde_json::json!({ "fileId": file_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["name"], "doc.txt");
        assert_eq!(shown["comment"], "original");

        // Non-owners (and unknown ids) see NO_SUCH_FILE, never the row.
        for (token, fid) in [
            (bob_token.clone(), file_id.clone()),
            (alice_token.clone(), "mid00000000000099".to_string()),
        ] {
            let (status, value) = call(
                &state,
                "drive/files/show",
                serde_json::json!({ "fileId": fid }),
                Some(&token),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
            assert_eq!(value["code"], "NO_SUCH_FILE");
        }

        // Update metadata.
        let (status, updated) = call(
            &state,
            "drive/files/update",
            serde_json::json!({
                "fileId": file_id,
                "name": "renamed.txt",
                "isSensitive": true,
                "comment": "edited",
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{updated}");
        assert_eq!(updated["name"], "renamed.txt");
        assert_eq!(updated["isSensitive"], true);
        assert_eq!(updated["comment"], "edited");

        // Invalid names are INVALID_FILE_NAME; overlong comments INVALID_PARAM.
        let long_name = "n".repeat(201);
        for bad in ["", "a/b", "a\\b", "..", long_name.as_str()] {
            let (status, value) = call(
                &state,
                "drive/files/update",
                serde_json::json!({ "fileId": file_id, "name": bad }),
                Some(&alice_token),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {value}");
            assert_eq!(value["code"], "INVALID_FILE_NAME");
        }
        let (status, value) = call(
            &state,
            "drive/files/update",
            serde_json::json!({ "fileId": file_id, "comment": "c".repeat(513) }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");

        // Bob cannot update or delete Alice's file.
        for endpoint in ["drive/files/update", "drive/files/delete"] {
            let (status, value) = call(
                &state,
                endpoint,
                serde_json::json!({ "fileId": file_id }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{endpoint}: {value}");
            assert_eq!(value["code"], "NO_SUCH_FILE");
        }

        // Delete removes metadata; the row is gone afterwards.
        let (status, _) = call(
            &state,
            "drive/files/delete",
            serde_json::json!({ "fileId": file_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, value) = call(
            &state,
            "drive/files/show",
            serde_json::json!({ "fileId": file_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_FILE");
        // Deleting again is NO_SUCH_FILE (the row is already gone).
        let (status, value) = call(
            &state,
            "drive/files/delete",
            serde_json::json!({ "fileId": file_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_FILE");
    }

    #[tokio::test]
    async fn phase3_notifications_list_and_mark_all() {
        use crate::domain::notification::{Notification, NotificationKind};

        let (state, clock) = test_state();
        let (alice_id, _) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // Twelve notifications for Bob (direct store inserts, deterministic
        // ids): even indexes are follows, odd are mentions.
        for n in 0..12 {
            let kind = if n % 2 == 0 {
                NotificationKind::Follow
            } else {
                NotificationKind::Mention
            };
            state
                .services
                .notifications
                .insert_notification(Notification {
                    id: state.services.ids.generate(),
                    notifiee_id: bob_id.clone(),
                    notifier_id: alice_id.clone(),
                    kind,
                    note_id: None,
                    reaction: None,
                    draft_id: None,
                    file_id: None,
                    exported_entity: None,
                    is_read: false,
                })
                .unwrap();
            clock.advance_ms(1_000);
        }

        // Default limit is 10 (Misskey-aligned), newest first.
        let (status, first) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{first}");
        let first = first.as_array().unwrap();
        assert_eq!(first.len(), 10);
        assert_eq!(first[0]["isRead"], false);

        // Cursor pagination reaches the remainder without overlap.
        let oldest_seen = first[9]["id"].as_str().unwrap().to_string();
        let (status, second) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "untilId": oldest_seen }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{second}");
        assert_eq!(second.as_array().unwrap().len(), 2);

        // includeTypes filters by kind.
        let (status, follows) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "includeTypes": ["follow"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{follows}");
        assert_eq!(follows.as_array().unwrap().len(), 6);
        assert!(
            follows
                .as_array()
                .unwrap()
                .iter()
                .all(|n| n["type"] == "follow")
        );
        // Excluding everything matches nothing.
        let (status, none) = call(
            &state,
            "i/notifications",
            serde_json::json!({
                "markAsRead": false,
                "excludeTypes": ["follow", "mention", "reply", "renote", "reaction", "receiveFollowRequest", "scheduledNotePosted", "scheduledNotePostFailed"],
            }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{none}");
        assert_eq!(none.as_array().unwrap().len(), 0);
        // Explicitly empty includeTypes matches nothing (Misskey parity).
        let (status, none) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "includeTypes": [] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{none}");
        assert_eq!(none.as_array().unwrap().len(), 0);
        // Unknown type strings are INVALID_PARAM; obsolete ones are dropped.
        let (status, value) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "includeTypes": ["bogus"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        let (status, none) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "includeTypes": ["pollVote"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{none}");
        assert_eq!(none.as_array().unwrap().len(), 0);

        // Default markAsRead=true flips everything to read.
        let (status, _) = call(
            &state,
            "i/notifications",
            serde_json::json!({}),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, after) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{after}");
        assert!(
            after.as_array().unwrap().iter().all(|n| n["isRead"] == true),
            "{after}"
        );

        // notifications/mark-all-as-read marks unread rows read.
        state
            .services
            .notifications
            .insert_notification(Notification {
                id: state.services.ids.generate(),
                notifiee_id: bob_id.clone(),
                notifier_id: alice_id.clone(),
                kind: NotificationKind::Reaction,
                note_id: None,
                reaction: Some("❤".to_string()),
                draft_id: None,
                file_id: None,
                exported_entity: None,
                is_read: false,
            })
            .unwrap();
        let (status, _) = call(
            &state,
            "notifications/mark-all-as-read",
            serde_json::json!({}),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, after) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "limit": 1 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{after}");
        assert_eq!(after[0]["isRead"], true);

        // Auth + param validation.
        let (status, _) = call(&state, "i/notifications", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = call(
            &state,
            "notifications/mark-all-as-read",
            serde_json::json!({}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, value) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "limit": 101 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
    }

    #[tokio::test]
    async fn drafts_crud_count_and_error_codes() {
        use crate::domain::notification::NotificationKind;

        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        // Auth: no credential → 401.
        let (status, value) = call(
            &state,
            "notes/drafts/create",
            serde_json::json!({ "text": "hi" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Create a plain draft.
        let (status, created) = call(
            &state,
            "notes/drafts/create",
            serde_json::json!({ "text": "hello drafts", "visibility": "public" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let draft_id = created["createdDraft"]["id"].as_str().unwrap().to_string();
        assert_eq!(created["createdDraft"]["text"], "hello drafts");
        assert_eq!(created["createdDraft"]["scheduledAt"], serde_json::Value::Null);
        assert_eq!(created["createdDraft"]["isActuallyScheduled"], false);
        assert_eq!(created["createdDraft"]["userId"], alice_id);

        // List + count.
        let (status, list) = call(
            &state,
            "notes/drafts/list",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list.as_array().unwrap().len(), 1);
        let (status, count) = call(
            &state,
            "notes/drafts/count",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{count}");
        assert_eq!(count, 1);

        // Update merges.
        let (status, updated) = call(
            &state,
            "notes/drafts/update",
            serde_json::json!({ "draftId": draft_id, "text": "edited" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{updated}");
        assert_eq!(updated["updatedDraft"]["text"], "edited");
        // Unknown draft → NO_SUCH_NOTE_DRAFT.
        let (status, value) = call(
            &state,
            "notes/drafts/update",
            serde_json::json!({ "draftId": draft_id, "text": "x" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE_DRAFT");

        // Schedule gates carry the exact upstream codes.
        let (status, value) = call(
            &state,
            "notes/drafts/create",
            serde_json::json!({ "text": "sched", "isActuallyScheduled": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "SCHEDULED_AT_REQUIRED");
        let (status, value) = call(
            &state,
            "notes/drafts/create",
            serde_json::json!({
                "text": "sched",
                "scheduledAt": clock.now_ms(),
                "isActuallyScheduled": true,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "SCHEDULED_AT_MUST_BE_IN_FUTURE");

        // One scheduled draft fits the default cap of 1...
        let (status, scheduled) = call(
            &state,
            "notes/drafts/create",
            serde_json::json!({
                "text": "future",
                "scheduledAt": clock.now_ms() + 60_000,
                "isActuallyScheduled": true,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{scheduled}");
        let scheduled_id = scheduled["createdDraft"]["id"].as_str().unwrap().to_string();
        // ...the second is rejected with the exact upstream code.
        let (status, value) = call(
            &state,
            "notes/drafts/create",
            serde_json::json!({
                "text": "future 2",
                "scheduledAt": clock.now_ms() + 120_000,
                "isActuallyScheduled": true,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "TOO_MANY_SCHEDULED_NOTES");

        // Delete: foreign drafts are ACCESS_DENIED, missing ones are
        // NO_SUCH_NOTE_DRAFT, success is 204.
        let (status, value) = call(
            &state,
            "notes/drafts/delete",
            serde_json::json!({ "draftId": draft_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
        assert_eq!(value["code"], "ACCESS_DENIED");
        let (status, value) = call(
            &state,
            "notes/drafts/delete",
            serde_json::json!({ "draftId": "did00000000000001" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE_DRAFT");
        let (status, _) = call(
            &state,
            "notes/drafts/delete",
            serde_json::json!({ "draftId": draft_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // The scheduled draft posts through the worker once due.
        clock.advance_ms(61_000);
        state.worker.drain(100).unwrap();
        let (status, list) = call(
            &state,
            "notes/drafts/list",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert!(list.as_array().unwrap().is_empty());
        let (status, count) = call(
            &state,
            "notes/drafts/count",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{count}");
        assert_eq!(count, 0);
        // The posted note is visible on the author's timeline.
        let (status, notes) = call(
            &state,
            "users/notes",
            serde_json::json!({ "userId": alice_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        assert_eq!(notes.as_array().unwrap().len(), 1);
        assert_eq!(notes[0]["text"], "future");
        // ...and the scheduling notification exists (findable by kind).
        let (status, notifs) = call(
            &state,
            "i/notifications",
            serde_json::json!({
                "markAsRead": false,
                "includeTypes": ["scheduledNotePosted"],
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notifs}");
        assert_eq!(notifs.as_array().unwrap().len(), 1);
        assert_eq!(notifs[0]["type"], "scheduledNotePosted");
        assert!(notifs[0].get("note").is_some());
        let _ = (scheduled_id, NotificationKind::ScheduledNotePosted);
    }

    #[tokio::test]
    async fn pin_unpin_flows_and_errors() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // Auth: no credential → 401.
        let (status, _) = call(
            &state,
            "i/pin",
            serde_json::json!({ "noteId": "x" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Alice posts a note; Bob posts one too.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "pin me" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "bob note" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let bob_note = created["createdNote"]["id"].as_str().unwrap().to_string();

        // Pinning a foreign note is NO_SUCH_NOTE (must own the note).
        let (status, value) = call(
            &state,
            "i/pin",
            serde_json::json!({ "noteId": bob_note }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");

        // Pin works and packs into MeDetailed.
        let (status, me) = call(
            &state,
            "i/pin",
            serde_json::json!({ "noteId": note_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["pinnedNoteIds"][0], note_id);
        assert_eq!(me["pinnedNotes"][0]["id"], note_id);
        assert_eq!(me["pinnedNotes"][0]["text"], "pin me");

        // Duplicate pins conflict with the exact upstream code.
        let (status, value) = call(
            &state,
            "i/pin",
            serde_json::json!({ "noteId": note_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "ALREADY_PINNED");

        // Fill to the pin limit of 5, then exceed it.
        let mut pinned = vec![note_id.clone()];
        for n in 0..4 {
            let (status, created) = call(
                &state,
                "notes/create",
                serde_json::json!({ "text": format!("note {n}") }),
                Some(&alice_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{created}");
            let id = created["createdNote"]["id"].as_str().unwrap().to_string();
            let (status, _) = call(
                &state,
                "i/pin",
                serde_json::json!({ "noteId": id }),
                Some(&alice_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            pinned.push(id);
        }
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "one too many" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let sixth = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, value) = call(
            &state,
            "i/pin",
            serde_json::json!({ "noteId": sixth }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "PIN_LIMIT_EXCEEDED");

        // Unpin removes the id; unpinning again is a no-op success.
        let (status, me) = call(
            &state,
            "i/unpin",
            serde_json::json!({ "noteId": pinned[0] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["pinnedNoteIds"].as_array().unwrap().len(), 4);
        let (status, _) = call(
            &state,
            "i/unpin",
            serde_json::json!({ "noteId": pinned[0] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // Unpinning a foreign note is NO_SUCH_NOTE.
        let (status, value) = call(
            &state,
            "i/unpin",
            serde_json::json!({ "noteId": bob_note }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
        let _ = (alice_id, bob_id);
    }

    #[tokio::test]
    async fn featured_endpoints_return_ranked_notes() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // Alice posts two notes; Bob reacts to both and renotes one.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "first note" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let first = created["createdNote"]["id"].as_str().unwrap().to_string();
        clock.advance_ms(1_000);
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "second note" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let second = created["createdNote"]["id"].as_str().unwrap().to_string();
        for target in [&first, &second] {
            let (status, _) = call(
                &state,
                "notes/reactions/create",
                serde_json::json!({ "noteId": target, "reaction": "❤" }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::NO_CONTENT);
        }
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "renoteId": second }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let rt = created["createdNote"]["id"].as_str().unwrap().to_string();

        // Global featured returns all three notes newest-first (the pure
        // renote is itself a recent public note with no sensitivity flags,
        // so it ranks too).
        let (status, featured) = call(
            &state,
            "notes/featured",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{featured}");
        let ids: Vec<&str> = featured
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&first.as_str()) && ids.contains(&second.as_str()));
        assert_eq!(featured[0]["id"], rt, "newest first");
        assert_eq!(featured[1]["id"], second);
        assert_eq!(featured[2]["id"], first);
        // Anonymous callers are served too; unknown params are rejected.
        let (status, anon) = call(&state, "notes/featured", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::OK, "{anon}");
        assert_eq!(anon.as_array().unwrap().len(), ids.len());
        let (status, value) = call(
            &state,
            "notes/featured",
            serde_json::json!({ "bogus": 1 }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "UNSUPPORTED_FIELD");
        // A non-null channelId ranks empty (no channels exist).
        let (status, empty) = call(
            &state,
            "notes/featured",
            serde_json::json!({ "channelId": "ch1" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{empty}");
        assert!(empty.as_array().unwrap().is_empty());
        // `untilId` pages newest-first.
        let (status, page) = call(
            &state,
            "notes/featured",
            serde_json::json!({ "untilId": second }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page.as_array().unwrap().len(), 1);
        assert_eq!(page[0]["id"], first);

        // Per-user featured scopes to the author; unknown users rank empty.
        let (status, user_featured) = call(
            &state,
            "users/featured-notes",
            serde_json::json!({ "userId": alice_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{user_featured}");
        assert_eq!(user_featured.as_array().unwrap().len(), 2);
        let (status, bob_featured) = call(
            &state,
            "users/featured-notes",
            serde_json::json!({ "userId": bob_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{bob_featured}");
        assert_eq!(bob_featured.as_array().unwrap().len(), 1);
        let (status, ghost) = call(
            &state,
            "users/featured-notes",
            serde_json::json!({ "userId": "aid00000000000099" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{ghost}");
        assert!(ghost.as_array().unwrap().is_empty());
        // Missing userId is INVALID_PARAM.
        let (status, value) = call(
            &state,
            "users/featured-notes",
            serde_json::json!({}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
    }

    #[tokio::test]
    async fn notifications_grouped_shapes() {
        use crate::domain::notification::{Notification, NotificationKind};

        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;
        let (carol_id, _) = signup_user(&state, "carol").await;
        let (dave_id, _) = signup_user(&state, "dave").await;

        // Two notes by Alice; Bob renotes the first twice (two renote notes).
        let n1 = state
            .services
            .create_note(crate::application::ports::CreateNoteInput::simple_public(
                &alice_id, "n1",
            ))
            .unwrap();
        clock.advance_ms(1_000);
        let n2 = state
            .services
            .create_note(crate::application::ports::CreateNoteInput::simple_public(
                &alice_id, "n2",
            ))
            .unwrap();
        clock.advance_ms(1_000);
        let r1 = state
            .services
            .create_note(crate::application::ports::CreateNoteInput {
                renote_of: Some(n1.id.clone()),
                ..crate::application::ports::CreateNoteInput::simple_public(&bob_id, "rt1")
            })
            .unwrap();
        clock.advance_ms(1_000);
        let r2 = state
            .services
            .create_note(crate::application::ports::CreateNoteInput {
                renote_of: Some(n1.id.clone()),
                ..crate::application::ports::CreateNoteInput::simple_public(&carol_id, "rt2")
            })
            .unwrap();

        // Flat notifications for Bob, oldest first (clock advances make ids
        // time-ordered): follow, react(n1, bob), react(n1, carol),
        // react(n2, dave), renote(r1), renote(r2), mention(n2).
        let mut insert = |kind: NotificationKind,
                          notifier: &str,
                          note: Option<String>,
                          reaction: Option<String>| {
            let id = state.services.ids.generate();
            state
                .services
                .notifications
                .insert_notification(Notification {
                    id,
                    notifiee_id: bob_id.clone(),
                    notifier_id: notifier.to_string(),
                    kind,
                    note_id: note,
                    reaction,
                    draft_id: None,
                    file_id: None,
                    exported_entity: None,
                    is_read: false,
                })
                .unwrap();
            clock.advance_ms(1_000);
        };
        insert(NotificationKind::Follow, &alice_id, None, None);
        insert(
            NotificationKind::Reaction,
            &bob_id,
            Some(n1.id.clone()),
            Some("❤".to_string()),
        );
        insert(
            NotificationKind::Reaction,
            &carol_id,
            Some(n1.id.clone()),
            Some("👍".to_string()),
        );
        insert(
            NotificationKind::Reaction,
            &dave_id,
            Some(n2.id.clone()),
            Some("❤".to_string()),
        );
        insert(NotificationKind::Renote, &bob_id, Some(r1.id.clone()), None);
        insert(NotificationKind::Renote, &carol_id, Some(r2.id.clone()), None);
        insert(
            NotificationKind::Mention,
            &alice_id,
            Some(n2.id.clone()),
            None,
        );

        // Grouped fetch: newest-first =
        // mention | renote:grouped(r2+r1) | reaction(n2) |
        // reaction:grouped(n1+n1) | follow.
        let (status, grouped) = call(
            &state,
            "i/notifications-grouped",
            serde_json::json!({ "markAsRead": false }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{grouped}");
        let grouped = grouped.as_array().unwrap();
        assert_eq!(grouped.len(), 5);
        assert_eq!(grouped[0]["type"], "mention");
        assert!(grouped[0].get("userId").is_some(), "plain packs keep userId");
        assert_eq!(grouped[1]["type"], "renote:grouped");
        assert_eq!(grouped[1]["note"]["id"], r2.id, "group carries the newest renote");
        assert_eq!(
            grouped[1]["users"].as_array().unwrap().len(),
            2,
            "both renoters merge: {}",
            grouped[1]
        );
        assert!(grouped[1].get("userId").is_none(), "groups drop top-level userId");
        assert_eq!(grouped[2]["type"], "reaction", "lone reactions pass through");
        assert_eq!(grouped[2]["note"]["id"], n2.id);
        assert_eq!(grouped[2]["reaction"], "❤");
        assert_eq!(grouped[3]["type"], "reaction:grouped");
        assert_eq!(grouped[3]["note"]["id"], n1.id);
        let reactions = grouped[3]["reactions"].as_array().unwrap();
        assert_eq!(reactions.len(), 2);
        assert_eq!(reactions[0]["reaction"], "👍", "newest member first");
        assert_eq!(reactions[1]["reaction"], "❤");
        assert_eq!(grouped[4]["type"], "follow");

        // Same params as `i/notifications` are accepted (full param set).
        let (status, full) = call(
            &state,
            "i/notifications-grouped",
            serde_json::json!({
                "limit": 100,
                "sinceId": grouped[4]["id"],
                "untilId": grouped[0]["id"],
                "sinceDate": 1,
                "untilDate": 9_999_999_999_999i64,
                "markAsRead": false,
                "includeTypes": ["reaction", "renote", "follow", "mention"],
                "excludeTypes": [],
            }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{full}");
        // includeTypes filters before grouping (mention + follow dropped).
        assert_eq!(full.as_array().unwrap().len(), 3);

        // markAsRead defaults to true and flips everything read.
        let (status, _) = call(
            &state,
            "i/notifications-grouped",
            serde_json::json!({}),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, after) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "limit": 100 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{after}");
        assert!(after.as_array().unwrap().iter().all(|n| n["isRead"] == true));

        // Grouped pseudo-types are valid filter entries (and match nothing
        // alone, upstream parity).
        let (status, none) = call(
            &state,
            "i/notifications-grouped",
            serde_json::json!({ "markAsRead": false, "includeTypes": ["reaction:grouped"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{none}");
        assert!(none.as_array().unwrap().is_empty());
        // Auth + unknown-type validation mirror `i/notifications`.
        let (status, _) = call(
            &state,
            "i/notifications-grouped",
            serde_json::json!({}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, value) = call(
            &state,
            "i/notifications-grouped",
            serde_json::json!({ "includeTypes": ["bogus"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        let _ = alice_token;
    }

    /// §11 read-time parity: `includeTypes`/`excludeTypes` accept the same
    /// 20 upstream type names on `i/notifications` and
    /// `i/notifications-grouped`, with identical matching. Covers the
    /// remaining name mappings not exercised by the per-endpoint tests:
    /// `quote` selects renote rows, unemitted `pollEnded` matches nothing,
    /// obsolete `pollVote`/`groupInvited` are silently dropped (so an
    /// include-list of only obsolete types matches nothing), and the
    /// grouped pseudo-types match nothing alone while excluding nothing.
    #[tokio::test]
    async fn notifications_type_filters_agree_on_both_endpoints() {
        use crate::domain::notification::{Notification, NotificationKind};

        let (state, clock) = test_state();
        let (alice_id, _) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // One notification per emittable kind (direct inserts, oldest first).
        let kinds = [
            NotificationKind::Follow,
            NotificationKind::ReceiveFollowRequest,
            NotificationKind::Mention,
            NotificationKind::Reply,
            NotificationKind::Renote,
            NotificationKind::Reaction,
            NotificationKind::ScheduledNotePosted,
            NotificationKind::ScheduledNotePostFailed,
        ];
        for kind in kinds {
            state
                .services
                .notifications
                .insert_notification(Notification {
                    id: state.services.ids.generate(),
                    notifiee_id: bob_id.clone(),
                    notifier_id: alice_id.clone(),
                    kind,
                    note_id: None,
                    reaction: None,
                    draft_id: None,
                    file_id: None,
                    exported_entity: None,
                    is_read: false,
                })
                .unwrap();
            clock.advance_ms(1_000);
        }

        // `quote` selects the renote row on both endpoints (local quotes
        // notify as `Renote`).
        for endpoint in ["i/notifications", "i/notifications-grouped"] {
            let (status, rows) = call(
                &state,
                endpoint,
                serde_json::json!({ "markAsRead": false, "includeTypes": ["quote"] }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{endpoint}: {rows}");
            let rows = rows.as_array().unwrap();
            assert_eq!(rows.len(), 1, "{endpoint}: quote selects the renote row");
            assert!(rows[0]["type"] == "renote" || rows[0]["type"] == "renote:grouped");
        }
        // Unemitted `pollEnded` matches nothing on both endpoints.
        for endpoint in ["i/notifications", "i/notifications-grouped"] {
            let (status, rows) = call(
                &state,
                endpoint,
                serde_json::json!({ "markAsRead": false, "includeTypes": ["pollEnded"] }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{endpoint}: {rows}");
            assert!(rows.as_array().unwrap().is_empty(), "{endpoint}");
        }
        // Obsolete types are dropped: an include-list of only obsolete
        // types matches nothing (presence of `includeTypes` still filters).
        for endpoint in ["i/notifications", "i/notifications-grouped"] {
            let (status, rows) = call(
                &state,
                endpoint,
                serde_json::json!({ "markAsRead": false, "includeTypes": ["pollVote", "groupInvited"] }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{endpoint}: {rows}");
            assert!(rows.as_array().unwrap().is_empty(), "{endpoint}");
        }
        // Grouped pseudo-types match nothing alone and exclude nothing, on
        // both endpoints identically.
        for endpoint in ["i/notifications", "i/notifications-grouped"] {
            let (status, none) = call(
                &state,
                endpoint,
                serde_json::json!({ "markAsRead": false, "includeTypes": ["reaction:grouped", "renote:grouped"] }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{endpoint}: {none}");
            assert!(none.as_array().unwrap().is_empty(), "{endpoint}");
            let (status, all) = call(
                &state,
                endpoint,
                serde_json::json!({ "markAsRead": false, "excludeTypes": ["reaction:grouped", "renote:grouped"] }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{endpoint}: {all}");
            assert_eq!(all.as_array().unwrap().len(), 8, "{endpoint}");
        }
        // Excluding every emittable kind matches nothing on both endpoints.
        for endpoint in ["i/notifications", "i/notifications-grouped"] {
            let (status, none) = call(
                &state,
                endpoint,
                serde_json::json!({
                    "markAsRead": false,
                    "excludeTypes": ["follow", "receiveFollowRequest", "mention", "reply", "renote", "reaction", "scheduledNotePosted", "scheduledNotePostFailed"],
                }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{endpoint}: {none}");
            assert!(none.as_array().unwrap().is_empty(), "{endpoint}");
        }
        // Unknown names are INVALID_PARAM on both endpoints.
        for endpoint in ["i/notifications", "i/notifications-grouped"] {
            let (status, value) = call(
                &state,
                endpoint,
                serde_json::json!({ "includeTypes": ["bogus"] }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{endpoint}");
            assert_eq!(value["code"], "INVALID_PARAM");
        }
    }

    #[tokio::test]
    async fn phase3_revoke_token() {
        let (state, _) = test_state();
        let (_, token_a) = signup_user(&state, "alice").await;

        // Baseline: the token authenticates.
        let (status, _) = call(&state, "i", serde_json::json!({}), Some(&token_a)).await;
        assert_eq!(status, StatusCode::OK);

        // No params revokes the caller's own token.
        let (status, _) = call(
            &state,
            "i/revoke-token",
            serde_json::json!({}),
            Some(&token_a),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = call(&state, "i", serde_json::json!({}), Some(&token_a)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Explicit token revocation.
        let (_, token_b) = signup_user(&state, "bob").await;
        let (status, _) = call(
            &state,
            "i/revoke-token",
            serde_json::json!({ "token": token_b }),
            Some(&token_b),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = call(&state, "i", serde_json::json!({}), Some(&token_b)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Unknown tokens and tokenIds are no-op successes.
        let (_, token_c) = signup_user(&state, "carol").await;
        let (status, _) = call(
            &state,
            "i/revoke-token",
            serde_json::json!({ "token": "deadbeefdeadbeefdeadbeefdeadbeef" }),
            Some(&token_c),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = call(
            &state,
            "i/revoke-token",
            serde_json::json!({ "tokenId": "tid00000000000001" }),
            Some(&token_c),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        // Carol's own token still works.
        let (status, _) = call(&state, "i", serde_json::json!({}), Some(&token_c)).await;
        assert_eq!(status, StatusCode::OK);

        // A third-party token cannot revoke a different token.
        let third_party = generate_random_token();
        state
            .services
            .auth
            .store_token(ApiTokenRecord {
                token_hash: hash_token(&third_party),
                account_id: {
                    let (status, me) =
                        call(&state, "i", serde_json::json!({}), Some(&token_c)).await;
                    assert_eq!(status, StatusCode::OK, "{me}");
                    me["id"].as_str().unwrap().to_string()
                },
                permissions: vec!["read:account".to_string()],
                is_first_party: false,
                name: None,
                description: None,
                icon_url: None,
                created_at_ms: state.services.clock.now_ms(),
            })
            .unwrap();
        let (status, value) = call(
            &state,
            "i/revoke-token",
            serde_json::json!({ "token": token_c }),
            Some(&third_party),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
        assert_eq!(value["code"], "PERMISSION_DENIED");
        // ...but it can revoke itself.
        let (status, _) = call(
            &state,
            "i/revoke-token",
            serde_json::json!({ "token": third_party }),
            Some(&third_party),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // No credential at all is rejected.
        let (status, _) = call(&state, "i/revoke-token", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn phase3_notes_search() {
        let (state, clock) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        create_note(
            &state,
            serde_json::json!({ "text": "hello rust world #rust" }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);
        create_note(
            &state,
            serde_json::json!({ "text": "goodbye moon" }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);
        let bob_rust = create_note(
            &state,
            serde_json::json!({ "text": "hello rust again" }),
            &bob_token,
        )
        .await;
        clock.advance_ms(1_000);
        create_note(
            &state,
            serde_json::json!({ "text": "secret rust", "visibility": "followers" }),
            &alice_token,
        )
        .await;
        clock.advance_ms(1_000);

        // Substring match over local notes; Bob cannot see Alice's
        // followers-only note.
        let (status, hits) = call(
            &state,
            "notes/search",
            serde_json::json!({ "query": "rust" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{hits}");
        assert_eq!(hits.as_array().unwrap().len(), 2);

        // Alice sees her own followers-only note too.
        let (status, hits) = call(
            &state,
            "notes/search",
            serde_json::json!({ "query": "rust" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{hits}");
        assert_eq!(hits.as_array().unwrap().len(), 3);

        // userId + host filters.
        let (status, hits) = call(
            &state,
            "notes/search",
            serde_json::json!({ "query": "rust", "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{hits}");
        assert_eq!(note_ids(&hits), vec![bob_rust.clone()]);
        let (status, hits) = call(
            &state,
            "notes/search",
            serde_json::json!({ "query": "rust", "host": "." }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{hits}");
        assert_eq!(hits.as_array().unwrap().len(), 3);
        let (status, hits) = call(
            &state,
            "notes/search",
            serde_json::json!({ "query": "rust", "host": "remote.example" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{hits}");
        assert_eq!(hits.as_array().unwrap().len(), 0);

        // search-by-tag: case-insensitive.
        let (status, hits) = call(
            &state,
            "notes/search-by-tag",
            serde_json::json!({ "tag": "RUST" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{hits}");
        assert_eq!(hits.as_array().unwrap().len(), 1);

        // Misskey parity: public endpoint — anonymous search works and
        // sees exactly the public notes (not the followers-only one).
        let (status, hits) = call(
            &state,
            "notes/search",
            serde_json::json!({ "query": "rust" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{hits}");
        assert_eq!(hits.as_array().unwrap().len(), 2);
        let (status, value) = call(
            &state,
            "notes/search",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        // `offset` is accepted (applies before `limit`); `channelId` and
        // the `query[][]` tag form stay rejected (no channels here; the
        // profile is `tag`-only).
        let (status, _) = call(
            &state,
            "notes/search",
            serde_json::json!({ "query": "x", "offset": 0 }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        for (endpoint, body) in [
            ("notes/search", serde_json::json!({ "query": "x", "channelId": "y" })),
            ("notes/search-by-tag", serde_json::json!({ "tag": "rust", "query": [] })),
        ] {
            let (status, value) = call(&state, endpoint, body, Some(&alice_token)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{endpoint}: {value}");
            assert_eq!(value["code"], "UNSUPPORTED_FIELD");
        }
    }

    /// §4 polls + §6 wire compat, endpoint level: `notes/create` poll
    /// params, `notes/polls/vote` flows and exact Misskey error codes, and
    /// reaction normalization on `notes/reactions/create`.
    #[tokio::test]
    async fn notes_polls_vote_flows_and_error_codes() {
        let (state, clock) = test_state();
        let (_alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_bob_id, bob_token) = signup_user(&state, "bob").await;
        let now = clock.now_ms();

        // Poll choices validation: too few, dupes, overlong.
        for poll in [
            serde_json::json!({ "choices": ["only"] }),
            serde_json::json!({ "choices": ["a", "a"] }),
            serde_json::json!({ "choices": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"] }),
            serde_json::json!({ "choices": ["", "b"] }),
        ] {
            let (status, value) = call(
                &state,
                "notes/create",
                serde_json::json!({ "text": "t", "poll": poll }),
                Some(&alice_token),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{poll}: {value}");
            assert_eq!(value["code"], "INVALID_PARAM");
        }
        // Already-expired poll → exact Misskey ident + UUID.
        let (status, value) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "t", "poll": { "choices": ["a", "b"], "expiresAt": now - 1 } }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "CANNOT_CREATE_ALREADY_EXPIRED_POLL");
        assert_eq!(value["error"]["id"], "04da457d-b083-4055-9082-955525eda5a5");
        assert_eq!(value["message"], "Poll is already expired.");

        // A live poll creates with the Misskey pack shape.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({
                "text": "pick one",
                "poll": { "choices": ["a", "b"], "multiple": false, "expiredAfter": 3_600_000 },
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        assert_eq!(created["createdNote"]["hasPoll"], serde_json::json!(true));
        assert_eq!(created["createdNote"]["poll"]["choices"][0]["text"], serde_json::json!("a"));

        // A poll stands in for text (Misskey `notes/create.ts` parity).
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "poll": { "choices": ["a", "b"] } }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        assert_eq!(created["createdNote"]["hasPoll"], serde_json::json!(true));

        // Vote: 204, then duplicate → ALREADY_VOTED (exact UUID).
        let (status, _) = call(
            &state,
            "notes/polls/vote",
            serde_json::json!({ "noteId": note_id, "choice": 0 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, value) = call(
            &state,
            "notes/polls/vote",
            serde_json::json!({ "noteId": note_id, "choice": 0 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "ALREADY_VOTED");
        assert_eq!(value["error"]["id"], "0963fc77-efac-419b-9424-b391608dc6d8");

        // Tallies + isVoted visible on show.
        let (status, shown) = call(
            &state,
            "notes/show",
            serde_json::json!({ "noteId": note_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["poll"]["choices"][0]["votes"], serde_json::json!(1));
        assert_eq!(shown["poll"]["choices"][0]["isVoted"], serde_json::json!(true));
        assert_eq!(shown["poll"]["choices"][1]["isVoted"], serde_json::json!(false));

        // Full error set with exact codes.
        let (status, value) = call(
            &state,
            "notes/polls/vote",
            serde_json::json!({ "noteId": "aid00000000000099", "choice": 0 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
        let (status, value) = call(
            &state,
            "notes/polls/vote",
            serde_json::json!({ "noteId": note_id, "choice": 9 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_CHOICE");
        assert_eq!(value["error"]["id"], "e0cc9a04-f2e8-41e4-a5f1-4127293260cc");

        // NO_POLL on a plain note.
        let (status, plain) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "plain" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{plain}");
        let plain_id = plain["createdNote"]["id"].as_str().unwrap();
        assert_eq!(plain["createdNote"]["hasPoll"], serde_json::json!(false));
        assert!(plain["createdNote"]["poll"].is_null());
        let (status, value) = call(
            &state,
            "notes/polls/vote",
            serde_json::json!({ "noteId": plain_id, "choice": 0 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_POLL");
        assert_eq!(value["error"]["id"], "5f979967-52d9-4314-a911-1c673727f92f");

        // Blocked voter → YOU_HAVE_BEEN_BLOCKED (author blocked the voter).
        let (status, _) = call(
            &state,
            "blocking/create",
            serde_json::json!({ "userId": _bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "alice blocks bob");
        let (status, value) = call(
            &state,
            "notes/polls/vote",
            serde_json::json!({ "noteId": note_id, "choice": 1 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "YOU_HAVE_BEEN_BLOCKED");
    }

    /// §6 wire compat, endpoint level: legacy names, VS16 fallback, FE0F
    /// stripping, and `likeOnly` coercion on `notes/reactions/create`.
    #[tokio::test]
    async fn reactions_normalize_like_misskey() {
        use crate::application::ports::ReactionStore;

        let (state, _clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_bob_id, bob_token) = signup_user(&state, "bob").await;

        // A likeOnly note coerces everything to the bare heart.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "like only", "reactionAcceptance": "likeOnly" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, _) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": "👍" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let stored = state
            .services
            .reactions
            .get_reaction(&_bob_id, &note_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.reaction, "❤");

        // Legacy names map; VS16 hearts normalize to the bare fallback.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "open" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let open_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, _) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": open_id, "reaction": "like" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let stored = state
            .services
            .reactions
            .get_reaction(&_bob_id, &open_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.reaction, "👍");

        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "open 2" }),
            Some(&alice_token),
        )
        .await;
        // Alice reacts with the VS16 heart on her own note (first-party
        // token path exercises the same normalization).
        let open2_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        assert_eq!(status, StatusCode::OK, "{created}");
        let (status, _) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": open2_id, "reaction": "❤️" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let stored = state
            .services
            .reactions
            .get_reaction(&alice_id, &open2_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.reaction, "❤");
    }

    #[tokio::test]
    async fn parity_notes_edit_endpoint_gates() {
        let (state, _) = test_state();
        let (_alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_bob_id, bob_token) = signup_user(&state, "bob").await;

        // Missing and empty `editId` are NEEDS_EDIT_ID (exact UUID), not
        // INVALID_PARAM (Sharkey error-set parity).
        for body in [
            serde_json::json!({ "text": "x" }),
            serde_json::json!({ "editId": "", "text": "x" }),
        ] {
            let (status, value) = call(&state, "notes/edit", body, Some(&alice_token)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
            assert_eq!(value["code"], "NEEDS_EDIT_ID");
            assert_eq!(value["error"]["id"], "d697edc8-8c73-4de8-bded-35fd198b79e5");
        }
        // Unknown id → NO_SUCH_NOTE.
        let (status, value) = call(
            &state,
            "notes/edit",
            serde_json::json!({ "editId": "aid00000000000099", "text": "x" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
        assert_eq!(value["error"]["id"], "24fcbfc6-2e37-42b6-8388-c29b3861a08d");

        // Alice's note: bob is not the author.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "original" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, value) = call(
            &state,
            "notes/edit",
            serde_json::json!({ "editId": note_id, "text": "hijack" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "YOU_ARE_NOT_THE_AUTHOR");
        assert_eq!(value["error"]["id"], "c6e61685-411d-43d0-b90a-a448d2539001");

        // Unknown fields are UNSUPPORTED_FIELD (same `check_allowed_fields`
        // rule as `notes/create`).
        let (status, value) = call(
            &state,
            "notes/edit",
            serde_json::json!({ "editId": note_id, "bogus": 1 }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "UNSUPPORTED_FIELD");

        // Successful edit returns `{createdNote}` with the new text and a
        // non-null `updatedAt`; `notes/show` reflects it.
        let (status, edited) = call(
            &state,
            "notes/edit",
            serde_json::json!({ "editId": note_id, "text": "revised" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{edited}");
        assert_eq!(
            edited["createdNote"]["id"].as_str().unwrap(),
            note_id.as_str()
        );
        assert_eq!(edited["createdNote"]["text"], serde_json::json!("revised"));
        assert!(edited["createdNote"]["updatedAt"].is_string(), "{edited}");
        let (status, shown) = call(
            &state,
            "notes/show",
            serde_json::json!({ "noteId": note_id }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["text"], serde_json::json!("revised"));
        assert!(shown["updatedAt"].is_string(), "{shown}");
    }

    #[tokio::test]
    async fn parity_notes_edit_noop_and_history() {
        let (state, _) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "v1" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        assert!(created["createdNote"]["updatedAt"].is_null());

        // Identical-text edit: `updatedAt` stays null, no history row, and
        // the endpoint's side-effect jobs are dropped (pending unchanged).
        let before = state.services.jobs.pending_count().unwrap();
        let (status, noop) = call(
            &state,
            "notes/edit",
            serde_json::json!({ "editId": note_id, "text": "v1" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{noop}");
        assert!(noop["createdNote"]["updatedAt"].is_null(), "{noop}");
        assert!(
            state.services.notes.note_edits(&note_id).unwrap().is_empty(),
            "no history row on no-op"
        );
        assert_eq!(
            state.services.jobs.pending_count().unwrap(),
            before,
            "no jobs enqueued on no-op"
        );

        // Two effective edits chain two history rows through the endpoint path.
        for text in ["v2", "v3"] {
            let (status, edited) = call(
                &state,
                "notes/edit",
                serde_json::json!({ "editId": note_id, "text": text }),
                Some(&alice_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{edited}");
            assert!(edited["createdNote"]["updatedAt"].is_string(), "{edited}");
        }
        let rows = state.services.notes.note_edits(&note_id).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].new_text.as_deref(), Some("v2"));
        assert_eq!(rows[1].old_text.as_deref(), Some("v2"));
        assert_eq!(rows[1].new_text.as_deref(), Some("v3"));
        assert_eq!(rows[1].old_date_ms, rows[0].updated_at_ms);
    }

    #[tokio::test]
    async fn parity_notes_edit_reply_immutable_visibility_forced() {
        let (state, _) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        // Bob posts the parent; alice replies (public, non-local-only).
        let (status, parent) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "parent" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{parent}");
        let parent_id = parent["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, reply) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "reply v1", "replyId": parent_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        let reply_id = reply["createdNote"]["id"].as_str().unwrap().to_string();

        // A second valid note to offer as the (discarded) new parent.
        let (status, other) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "other" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{other}");
        let other_id = other["createdNote"]["id"].as_str().unwrap().to_string();

        // The edit succeeds but keeps the original parent, visibility, and
        // localOnly (Sharkey forced-values parity).
        let (status, edited) = call(
            &state,
            "notes/edit",
            serde_json::json!({
                "editId": reply_id,
                "replyId": other_id,
                "text": "reply v2",
                "visibility": "followers",
                "localOnly": true,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{edited}");
        assert_eq!(
            edited["createdNote"]["text"],
            serde_json::json!("reply v2")
        );
        let stored = state
            .services
            .notes
            .get_note(&reply_id)
            .unwrap()
            .expect("reply stored");
        assert_eq!(
            stored.reply_id.as_deref(),
            Some(parent_id.as_str()),
            "reply parent is immutable"
        );
        assert_eq!(
            stored.visibility,
            Visibility::Public,
            "visibility forced back to the original"
        );
        assert!(!stored.local_only, "localOnly forced back to the original");
    }

    #[tokio::test]
    async fn parity_notes_edit_poll_resets_votes() {
        let (state, _) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        // Alice posts a note with a live poll; bob votes choice 0.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({
                "text": "pick one",
                "poll": { "choices": ["a", "b"], "expiredAfter": 3_600_000 },
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, _) = call(
            &state,
            "notes/polls/vote",
            serde_json::json!({ "noteId": note_id, "choice": 0 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Alice replaces the poll: tallies zero out for the new choices.
        let (status, edited) = call(
            &state,
            "notes/edit",
            serde_json::json!({
                "editId": note_id,
                "text": "pick one",
                "poll": { "choices": ["x", "y", "z"], "expiredAfter": 7_200_000 },
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{edited}");
        let (status, shown) = call(
            &state,
            "notes/show",
            serde_json::json!({ "noteId": note_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        for (i, choice) in shown["poll"]["choices"].as_array().unwrap().iter().enumerate() {
            assert_eq!(choice["votes"], serde_json::json!(0), "choice {i} zeroed");
            assert_eq!(choice["isVoted"], serde_json::json!(false));
        }

        // The old vote no longer blocks: bob can vote again...
        let (status, _) = call(
            &state,
            "notes/polls/vote",
            serde_json::json!({ "noteId": note_id, "choice": 0 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        // ...but a removed choice index is INVALID_CHOICE (exact UUID).
        let (status, value) = call(
            &state,
            "notes/polls/vote",
            serde_json::json!({ "noteId": note_id, "choice": 9 }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_CHOICE");
        assert_eq!(value["error"]["id"], "e0cc9a04-f2e8-41e4-a5f1-4127293260cc");

        // An identical-poll edit preserves the new tally (no reset).
        let (status, edited) = call(
            &state,
            "notes/edit",
            serde_json::json!({
                "editId": note_id,
                "text": "pick one",
                "poll": { "choices": ["x", "y", "z"], "expiredAfter": 7_200_000 },
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{edited}");
        let (status, shown) = call(
            &state,
            "notes/show",
            serde_json::json!({ "noteId": note_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["poll"]["choices"][0]["votes"], serde_json::json!(1));
    }

    #[tokio::test]
    async fn parity_notes_edit_edited_notification() {
        let (state, _) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        // Bob posts; alice replies; alice edits her reply → bob is notified.
        let (status, parent) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "parent" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{parent}");
        let parent_id = parent["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, reply) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "reply v1", "replyId": parent_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        let reply_id = reply["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, _) = call(
            &state,
            "notes/edit",
            serde_json::json!({ "editId": reply_id, "text": "reply v2" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        state.worker.drain(100).unwrap();
        let (status, notifs) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "includeTypes": ["edited"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notifs}");
        let notifs = notifs.as_array().unwrap();
        assert_eq!(notifs.len(), 1, "{notifs:?}");
        assert_eq!(notifs[0]["type"], serde_json::json!("edited"));
        assert_eq!(notifs[0]["note"]["id"].as_str().unwrap(), reply_id.as_str());

        // Alice edits her own non-reply note → no `edited` notification.
        let (status, own) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "solo" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{own}");
        let own_id = own["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, _) = call(
            &state,
            "notes/edit",
            serde_json::json!({ "editId": own_id, "text": "solo v2" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // Self-reply edit: alice replies to her own note and edits it →
        // still no notification (Sharkey `postNoteEdited` parity: the
        // `Notify` worker gate drops self-notifies).
        let (status, self_reply) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "self reply", "replyId": own_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{self_reply}");
        let self_reply_id = self_reply["createdNote"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let (status, _) = call(
            &state,
            "notes/edit",
            serde_json::json!({ "editId": self_reply_id, "text": "self reply v2" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        state.worker.drain(100).unwrap();
        let (status, bob_notifs) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "includeTypes": ["edited"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{bob_notifs}");
        assert_eq!(
            bob_notifs.as_array().unwrap().len(),
            1,
            "bob keeps only the first edit notification: {bob_notifs}"
        );
        let (status, alice_notifs) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "includeTypes": ["edited"] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{alice_notifs}");
        assert!(
            alice_notifs.as_array().unwrap().is_empty(),
            "alice has no edited notifications: {alice_notifs}"
        );
    }

    #[tokio::test]
    async fn parity_reject_quotes_end_to_end() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;
        let (carol_id, carol_token) = signup_user(&state, "carol").await;

        // Alice posts a note for bob to quote; bob posts the target for
        // alice's and carol's quotes.
        let (status, alice_note) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "alice note" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{alice_note}");
        let alice_note_id = alice_note["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, target) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "quotable" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{target}");
        let target_id = target["createdNote"]["id"].as_str().unwrap().to_string();

        // (a) Self-service toggle: alice opts out, quotes fail with the
        // exact Sharkey code; opting back in restores quoting.
        let (status, me) = call(
            &state,
            "i/update",
            serde_json::json!({ "rejectQuotes": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["rejectQuotes"], serde_json::json!(true));
        let (status, me) = call(&state, "i", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["rejectQuotes"], serde_json::json!(true));
        let (status, value) = call(
            &state,
            "notes/create",
            serde_json::json!({ "renoteId": target_id, "text": "quoting" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "QUOTE_DISABLED_FOR_USER");
        assert_eq!(value["error"]["id"], "1c0ea108-d1e3-4e8e-aa3f-4d2487626153");
        let (status, me) = call(
            &state,
            "i/update",
            serde_json::json!({ "rejectQuotes": false }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["rejectQuotes"], serde_json::json!(false));
        let (status, quote) = call(
            &state,
            "notes/create",
            serde_json::json!({ "renoteId": target_id, "text": "quoting" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{quote}");

        // (b) Moderation toggle: bob quotes alice's note BEFORE the flag is
        // set. Batch M WP1: a plain user cannot call `admin/reject-quotes`
        // (403, no state change), a moderator can (and the action writes a
        // Sharkey-shaped moderation-log row in the same commit), and a
        // moderator cannot act on another moderator (WP3 refusal rule).
        let (status, bob_quote) = call(
            &state,
            "notes/create",
            serde_json::json!({ "renoteId": alice_note_id, "text": "bob quoting" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{bob_quote}");
        let bob_quote_id = bob_quote["createdNote"]["id"].as_str().unwrap().to_string();

        // Plain user: refused, nothing changes.
        let (status, value) = call(
            &state,
            "admin/reject-quotes",
            serde_json::json!({ "userId": bob_id, "rejectQuotes": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
        assert_eq!(value["code"], "PERMISSION_DENIED");
        assert!(
            !state
                .services
                .accounts
                .find_by_id(&bob_id)
                .unwrap()
                .unwrap()
                .reject_quotes,
            "a refused call must not change state"
        );

        // Make alice a moderator (the CLI `set-role` path) and retry.
        state
            .services
            .set_role(None, &alice_id, false, true)
            .expect("set role");
        let (status, packed) = call(
            &state,
            "admin/reject-quotes",
            serde_json::json!({ "userId": bob_id, "rejectQuotes": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{packed}");
        assert_eq!(packed["rejectQuotes"], serde_json::json!(true));
        // The moderation-log row is in the same commit: it is readable
        // through `admin/show-moderation-logs` immediately.
        let (status, logs) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({ "type": "rejectQuotesUser" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        let rows = logs.as_array().unwrap();
        assert_eq!(rows.len(), 1, "one log row: {logs}");
        assert_eq!(rows[0]["type"], "rejectQuotesUser");
        assert_eq!(rows[0]["userId"], serde_json::json!(alice_id));
        assert_eq!(rows[0]["user"]["username"], "alice");
        assert_eq!(rows[0]["info"]["userId"], serde_json::json!(bob_id));
        assert_eq!(rows[0]["info"]["userUsername"], "bob");

        // WP3 refusal rule: a moderator cannot act on a moderator.
        state
            .services
            .set_role(None, &carol_id, false, true)
            .expect("set role");
        let (status, value) = call(
            &state,
            "admin/reject-quotes",
            serde_json::json!({ "userId": carol_id, "rejectQuotes": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert!(
            !state
                .services
                .accounts
                .find_by_id(&carol_id)
                .unwrap()
                .unwrap()
                .reject_quotes,
            "a refused call must not change state"
        );

        // New quotes fail...
        let (status, value) = call(
            &state,
            "notes/create",
            serde_json::json!({ "renoteId": alice_note_id, "text": "again" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "QUOTE_DISABLED_FOR_USER");
        assert_eq!(value["error"]["id"], "1c0ea108-d1e3-4e8e-aa3f-4d2487626153");
        // ...and so does editing the pre-flag quote while keeping its
        // quote shape (`notes/edit.ts` §3 wiring: the effective shape is
        // still a quote, so the quoter's flag rejects it).
        let (status, value) = call(
            &state,
            "notes/edit",
            serde_json::json!({
                "editId": bob_quote_id,
                "renoteId": alice_note_id,
                "text": "bob quoting v2",
            }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "QUOTE_DISABLED_FOR_USER");
        assert_eq!(value["error"]["id"], "1c0ea108-d1e3-4e8e-aa3f-4d2487626153");

        // (c) Missing or non-boolean `rejectQuotes` is INVALID_PARAM.
        for body in [
            serde_json::json!({ "userId": bob_id }),
            serde_json::json!({ "userId": bob_id, "rejectQuotes": "yes" }),
        ] {
            let (status, value) =
                call(&state, "admin/reject-quotes", body, Some(&alice_token)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
            assert_eq!(value["code"], "INVALID_PARAM");
        }

        // (d) Control: carol is never flagged, so her quotes work throughout.
        let (status, quote) = call(
            &state,
            "notes/create",
            serde_json::json!({ "renoteId": target_id, "text": "carol quoting" }),
            Some(&carol_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{quote}");
    }

    /// Batch M §3.2 `admin/show-moderation-logs`: moderator-only, upstream
    /// packing, cursor paging, and the upstream `type`-miss behavior.
    #[tokio::test]
    async fn admin_show_moderation_logs_moderator_only() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // Plain users are refused the log itself.
        let (status, value) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({}),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
        assert_eq!(value["code"], "PERMISSION_DENIED");

        // Two moderator actions generate two rows (the `setRole` row and
        // the `rejectQuotesUser` row).
        state
            .services
            .set_role(None, &alice_id, false, true)
            .expect("set role");
        let (status, _) = call(
            &state,
            "admin/reject-quotes",
            serde_json::json!({ "userId": bob_id, "rejectQuotes": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, logs) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        let rows = logs.as_array().unwrap();
        assert_eq!(rows.len(), 2, "newest first: {logs}");
        assert_eq!(rows[0]["type"], "rejectQuotesUser");
        assert_eq!(rows[1]["type"], "setRole");
        assert_eq!(rows[1]["info"]["isModerator"], serde_json::json!(true));
        assert_eq!(rows[1]["user"], serde_json::json!(null)); // operator (CLI) row
        for row in rows {
            for key in ["id", "createdAt", "type", "info", "userId", "user"] {
                assert!(row.get(key).is_some(), "row has {key}: {row}");
            }
        }

        // `userId` filters on the acting moderator; an unknown `type`
        // matches no rows (upstream `log.type = :type`).
        let (status, logs) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({ "userId": alice_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        assert_eq!(logs.as_array().unwrap().len(), 1, "{logs}");
        let (status, logs) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({ "type": "noSuchAction" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        assert!(logs.as_array().unwrap().is_empty(), "{logs}");

        // Cursor paging: `untilId` at the newest row returns the older one.
        let newest_id = rows[0]["id"].as_str().unwrap();
        let (status, logs) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({ "untilId": newest_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        let page = logs.as_array().unwrap();
        assert_eq!(page.len(), 1, "{logs}");
        assert_eq!(page[0]["type"], "setRole");
    }

    /// §6 per-user `defaultReaction` heart-click (`docs/misskey-parity-plan.md`
    /// §6 DIVERGENCE: `notes/reactions/create` leaves `reaction` optional and
    /// resolves an absent param server-side to `defaultReaction ?? FALLBACK`).
    // --- Batch M WP2: registration policy + invite codes -----------------

    #[tokio::test]
    async fn wp2_signup_policy_gate_and_invite_claim() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;

        // Non-admin: 403 on admin/update-meta.
        let (status, value) = call(
            &state,
            "admin/update-meta",
            serde_json::json!({ "disableRegistration": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");

        // Admin closes registration; meta reflects the setting.
        state
            .services
            .set_role(None, &alice_id, true, false)
            .expect("set role");
        let (status, value) = call(
            &state,
            "admin/update-meta",
            serde_json::json!({ "disableRegistration": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        let (status, meta) = call(&state, "meta", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::OK, "{meta}");
        assert_eq!(meta["disableRegistration"], serde_json::json!(true));

        // Closed with no invite: plain empty 400 (upstream
        // `SignupApiService` gate), no account created.
        let (status, value) = call(
            &state,
            "signup",
            serde_json::json!({ "username": "nosuchluck", "password": "s3cret-pw" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert!(
            value.get("error").is_none(),
            "the registration gate answers a plain 400: {value}"
        );
        assert!(
            state
                .services
                .accounts
                .find_by_handle("nosuchluck")
                .unwrap()
                .is_none(),
            "a refused signup creates nothing"
        );

        // A bad code is refused the same way.
        let (status, value) = call(
            &state,
            "signup",
            serde_json::json!({
                "username": "nosuchluck2",
                "password": "s3cret-pw",
                "invitationCode": "NOTACODE0AAAA",
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");

        // Moderator issues one code; exactly one signup consumes it.
        let (status, codes) = call(
            &state,
            "admin/invite/create",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{codes}");
        let code = codes.as_array().unwrap()[0]["code"]
            .as_str()
            .unwrap()
            .to_string();
        let (status, value) = call(
            &state,
            "signup",
            serde_json::json!({
                "username": "newbie",
                "password": "s3cret-pw",
                "invitationCode": code,
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["username"], serde_json::json!("newbie"));
        let (status, value) = call(
            &state,
            "signup",
            serde_json::json!({
                "username": "latecomer",
                "password": "s3cret-pw",
                "invitationCode": code,
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert!(
            state
                .services
                .accounts
                .find_by_handle("latecomer")
                .unwrap()
                .is_none(),
            "the second consume creates nothing"
        );

        // Reopened: signup needs no code again.
        let (status, _) = call(
            &state,
            "admin/update-meta",
            serde_json::json!({ "disableRegistration": false }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, value) = call(
            &state,
            "signup",
            serde_json::json!({ "username": "freedom", "password": "s3cret-pw" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
    }

    #[tokio::test]
    async fn wp2_admin_accounts_create_admin_only_bypasses_gate() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        // Non-admin: 403 (upstream ACCESS_DENIED).
        let (status, value) = call(
            &state,
            "admin/accounts/create",
            serde_json::json!({ "username": "made1", "password": "s3cret-pw" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");

        // Close registration: the public path is shut…
        state
            .services
            .set_role(None, &alice_id, true, false)
            .expect("set role");
        let (status, _) = call(
            &state,
            "admin/update-meta",
            serde_json::json!({ "disableRegistration": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // …but admin/accounts/create creates accounts anyway (its
        // purpose: it bypasses the gate).
        let (status, value) = call(
            &state,
            "admin/accounts/create",
            serde_json::json!({ "username": "made2", "password": "s3cret-pw" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["username"], serde_json::json!("made2"));
        assert!(value["token"].is_string(), "{value}");

        // The `createAccount` row committed with the account.
        let (status, logs) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({ "type": "createAccount" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        let rows = logs.as_array().unwrap();
        assert_eq!(rows.len(), 1, "{logs}");
        assert_eq!(rows[0]["type"], "createAccount");
        assert_eq!(rows[0]["info"]["userUsername"], serde_json::json!("made2"));
    }

    #[tokio::test]
    async fn wp2_invite_lifecycle_endpoints() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_bob_id, bob_token) = signup_user(&state, "bob").await;
        state
            .services
            .set_role(None, &alice_id, false, true)
            .expect("set role");

        // §9.1 default: plain users cannot create codes (upstream
        // `canInvite`, default false).
        let (status, value) = call(
            &state,
            "invite/create",
            serde_json::json!({}),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");

        // Moderator creates one code (single object, upstream shape).
        let (status, code1) = call(
            &state,
            "invite/create",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{code1}");
        assert!(code1["code"].is_string(), "{code1}");
        assert!(code1["id"].is_string(), "{code1}");
        assert_eq!(code1["used"], serde_json::json!(false));
        assert_eq!(code1["createdBy"]["username"], serde_json::json!("alice"));

        // invite/list is scoped to the caller.
        let (status, mine) = call(
            &state,
            "invite/list",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{mine}");
        assert_eq!(mine.as_array().unwrap().len(), 1, "{mine}");
        let (status, value) = call(&state, "invite/list", serde_json::json!({}), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");

        // invite/limit: `remaining` is null with the default (uncapped) policy.
        let (status, limit) = call(
            &state,
            "invite/limit",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{limit}");
        assert_eq!(limit["remaining"], serde_json::json!(null));

        // admin/invite/create: `count` + `expiresAt`; unparsable datetime
        // is upstream's INVALID_DATE_TIME.
        let (status, value) = call(
            &state,
            "admin/invite/create",
            serde_json::json!({ "count": 3, "expiresAt": "not-a-date" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_DATE_TIME");
        assert_eq!(value["id"], "f1380b15-3760-4c6c-a1db-5c3aaf1cbd49");
        let (status, batch) = call(
            &state,
            "admin/invite/create",
            serde_json::json!({ "count": 3, "expiresAt": "2030-01-02T03:04:05.000Z" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{batch}");
        let rows = batch.as_array().unwrap();
        assert_eq!(rows.len(), 3, "{batch}");
        for row in rows {
            assert_eq!(
                row["expiresAt"],
                serde_json::json!("2030-01-02T03:04:05.000Z"),
                "{row}"
            );
        }

        // admin/invite/list: every creator's codes, `type` filter.
        let (status, all) = call(
            &state,
            "admin/invite/list",
            serde_json::json!({ "type": "unused" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{all}");
        assert_eq!(all.as_array().unwrap().len(), 4, "{all}");
        let (status, used) = call(
            &state,
            "admin/invite/list",
            serde_json::json!({ "type": "used" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{used}");
        assert_eq!(used.as_array().unwrap().len(), 0, "{used}");

        // invite/delete: own code goes (204); unknown id is upstream's
        // NO_SUCH_INVITE_CODE (400).
        let code1_id = code1["id"].as_str().unwrap().to_string();
        let (status, value) = call(
            &state,
            "invite/delete",
            serde_json::json!({ "inviteId": code1_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        let (status, after) = call(
            &state,
            "admin/invite/list",
            serde_json::json!({ "type": "unused" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(after.as_array().unwrap().len(), 3, "{after}");
        let (status, value) = call(
            &state,
            "invite/delete",
            serde_json::json!({ "inviteId": "missing-invite-id" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_INVITE_CODE");
        assert_eq!(value["id"], "cd4f9ae4-7854-4e3e-8df9-c296f051e634");
    }

    #[tokio::test]
    async fn wp2_admin_meta_and_blocked_hosts() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;

        // Both admin endpoints are admin-only.
        for endpoint in ["admin/meta", "admin/update-meta"] {
            let (status, value) = call(&state, endpoint, serde_json::json!({}), Some(&alice_token)).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{endpoint}: {value}");
        }

        state
            .services
            .set_role(None, &alice_id, true, false)
            .expect("set role");
        // blockedHosts: lowercased, deduped, empties dropped (upstream
        // `admin/update-meta.ts` normalization).
        let (status, value) = call(
            &state,
            "admin/update-meta",
            serde_json::json!({
                "blockedHosts": ["Example.COM", "", " example.com ", "sub.example.com"],
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        let (status, meta) = call(&state, "admin/meta", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{meta}");
        assert_eq!(
            meta["blockedHosts"],
            serde_json::json!(["example.com", "sub.example.com"])
        );
        assert_eq!(meta["disableRegistration"], serde_json::json!(false));

        // Public meta carries the setting too (WP2 acceptance).
        let (status, public_meta) = call(&state, "meta", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::OK, "{public_meta}");
        assert_eq!(public_meta["disableRegistration"], serde_json::json!(false));

        // Fields outside the WP2 subset are refused explicitly (WP6
        // completes the parameter set) — never silently ignored.
        let (status, value) = call(
            &state,
            "admin/update-meta",
            serde_json::json!({ "name": "Nope" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    }

    /// Stub inbox on 127.0.0.1 that records each POST body verbatim (WP3
    /// fan-out assertions). Answers `202 Accepted`, serves `max_requests`.
    fn capture_inbox(max_requests: usize) -> (String, Arc<Mutex<Vec<String>>>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind capture inbox");
        let addr = listener.local_addr().expect("capture inbox addr");
        let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = bodies.clone();
        std::thread::spawn(move || {
            for _ in 0..max_requests {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf: Vec<u8> = Vec::new();
                let mut chunk = [0u8; 4096];
                let mut body_start: Option<usize> = None;
                let mut content_len = 0usize;
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            if body_start.is_none() {
                                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                    let head =
                                        String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
                                    content_len = head
                                        .lines()
                                        .filter_map(|l| l.split_once(':'))
                                        .find(|(k, _)| k.trim() == "content-length")
                                        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
                                        .unwrap_or(0);
                                    body_start = Some(pos + 4);
                                }
                            }
                            if let Some(start) = body_start {
                                if buf.len() >= start + content_len {
                                    break;
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
                if let Some(start) = body_start {
                    let end = (start + content_len).min(buf.len());
                    sink.lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&buf[start..end]).to_string());
                }
                let _ = stream.write_all(
                    b"HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                );
                let _ = stream.flush();
            }
        });
        (format!("http://{addr}/inbox"), bodies)
    }

    #[tokio::test]
    async fn wp3_suspend_unsuspend_endpoints() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;
        let (carol_id, _carol_token) = signup_user(&state, "carol").await;
        let (dan_id, _dan_token) = signup_user(&state, "dan").await;

        async fn actor_status(state: &ApiState, id: &str) -> StatusCode {
            ap_actor::get_actor(State(state.services.clone()), Path(id.to_string()), HeaderMap::new())
                .await
                .status()
        }

        fn is_suspended(state: &ApiState, id: &str) -> bool {
            state
                .services
                .accounts
                .find_by_id(id)
                .unwrap()
                .unwrap()
                .is_suspended
        }

        // Plain users are refused both admin actions (moderator role in the
        // handler; scopes alone are not authorization).
        for endpoint in ["admin/suspend-user", "admin/unsuspend-user"] {
            let (status, value) = call(
                &state,
                endpoint,
                serde_json::json!({ "userId": bob_id }),
                Some(&bob_token),
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{endpoint}: {value}");
            assert_eq!(value["code"], "PERMISSION_DENIED");
        }

        state
            .services
            .set_role(None, &alice_id, false, true)
            .expect("set role");

        // Missing target: typed `NO_SUCH_USER` (upstream throws an untyped
        // `user not found` here — its API layer answers 500; manifest §23).
        let (status, value) = call(
            &state,
            "admin/suspend-user",
            serde_json::json!({ "userId": "aidmissingmissingmi" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_USER");

        // WP3 refusal rule (applied to both endpoints, guide §3.1): a
        // moderator cannot act on an admin or another moderator. Upstream's
        // own suspend message is carried verbatim.
        state
            .services
            .set_role(None, &carol_id, false, true)
            .expect("set role");
        state
            .services
            .set_role(None, &dan_id, true, false)
            .expect("set role");
        for (endpoint, target, message) in [
            (
                "admin/suspend-user",
                &carol_id,
                "cannot suspend moderator account",
            ),
            (
                "admin/suspend-user",
                &dan_id,
                "cannot suspend moderator account",
            ),
            (
                "admin/unsuspend-user",
                &carol_id,
                "cannot unsuspend moderator account",
            ),
        ] {
            let (status, value) = call(
                &state,
                endpoint,
                serde_json::json!({ "userId": target }),
                Some(&alice_token),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{endpoint}: {value}");
            assert_eq!(value["code"], "INVALID_PARAM");
            assert_eq!(value["error"]["message"], message);
            assert!(!is_suspended(&state, target), "refusal must change nothing");
        }

        // Suspend bob: 204, and the enforcement surface flips together.
        let (status, value) = call(
            &state,
            "admin/suspend-user",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        assert!(is_suspended(&state, &bob_id));
        // Existing token: 403 USER_SUSPENDED.
        let (status, value) = call(&state, "i", serde_json::json!({}), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
        assert_eq!(value["code"], "USER_SUSPENDED");
        // Sign-in refused (after the password verifies).
        let (status, value) = call(
            &state,
            "signin",
            serde_json::json!({ "username": "bob", "password": "s3cret-pw" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
        assert_eq!(value["code"], "USER_SUSPENDED");
        // AP actor fetch not served.
        assert_eq!(actor_status(&state, &bob_id).await, StatusCode::NOT_FOUND);

        // One log row per action, in the same commit (the refused calls
        // wrote none).
        let (status, logs) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({ "type": "suspend" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        let rows = logs.as_array().unwrap();
        assert_eq!(rows.len(), 1, "one suspend row: {logs}");
        assert_eq!(rows[0]["type"], "suspend");
        assert_eq!(rows[0]["userId"], serde_json::json!(alice_id));
        assert_eq!(rows[0]["user"]["username"], "alice");
        assert_eq!(rows[0]["info"]["userId"], serde_json::json!(bob_id));
        assert_eq!(rows[0]["info"]["userUsername"], "bob");
        assert_eq!(rows[0]["info"]["userHost"], Value::Null);

        // Unsuspend restores the enforcement surface.
        let (status, value) = call(
            &state,
            "admin/unsuspend-user",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        assert!(!is_suspended(&state, &bob_id));
        let (status, _) = call(&state, "i", serde_json::json!({}), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK);
        let (status, value) = call(
            &state,
            "signin",
            serde_json::json!({ "username": "bob", "password": "s3cret-pw" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(actor_status(&state, &bob_id).await, StatusCode::OK);
        let (status, logs) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({ "type": "unsuspend" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        let rows = logs.as_array().unwrap();
        assert_eq!(rows.len(), 1, "one unsuspend row: {logs}");
        assert_eq!(rows[0]["type"], "unsuspend");
        assert_eq!(rows[0]["info"]["userId"], serde_json::json!(bob_id));
    }

    /// WP3 bounded fan-out: the suspend transaction queues ONE sweep job
    /// per outbound concern and the worker expands them at run time —
    /// `Delete(Person)` to every knowable shared inbox (upstream
    /// `postSuspend`) and a silent `Undo(Follow)` per outgoing follow
    /// (upstream `unFollowAll`). The removed follow requests and outgoing
    /// follows stay removed after unsuspend (upstream parity).
    #[tokio::test]
    async fn wp3_suspend_fanout_sweep_and_follow_teardown() {
        use std::time::{Duration, Instant};

        use crate::application::ports::{AccountStore, CreateAccountInput, FollowStore};

        let (inbox, bodies) = capture_inbox(16);
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, _bob_token) = signup_user(&state, "bob").await;
        let (carol_id, _carol_token) = signup_user(&state, "carol").await;
        state
            .services
            .set_role(None, &alice_id, false, true)
            .expect("set role");

        // One remote account whose shared inbox is the stub: it is both the
        // Delete(Person) target (known shared inbox) and the Undo(Follow)
        // target (bob's outgoing follow to it).
        let remote = state
            .services
            .accounts
            .create_account(
                CreateAccountInput {
                    handle: "faraway".to_string(),
                    host: Some("remote.example".to_string()),
                    display_name: None,
                    description: None,
                    location: None,
                    birthday: None,
                    uri: Some("https://remote.example/users/faraway".to_string()),
                    inbox: Some(inbox.clone()),
                    shared_inbox: Some(inbox.clone()),
                    is_bot: false,
                    is_cat: false,
                    is_locked: false,
                    preserved_usernames: Vec::new(),
                },
                None,
            )
            .expect("remote account");

        // Outgoing follows (removed by the sweep) and one incoming follow
        // (kept — upstream `unFollowAll` is outgoing-only).
        state
            .services
            .follows
            .follow(&bob_id, &remote.id, false, None, Vec::new())
            .expect("bob -> remote");
        state
            .services
            .follows
            .follow(&bob_id, &carol_id, false, None, Vec::new())
            .expect("bob -> carol");
        state
            .services
            .follows
            .follow(&remote.id, &bob_id, false, None, Vec::new())
            .expect("remote -> bob");
        // Pending requests in both directions (torn down at suspend).
        state
            .services
            .follows
            .request_follow(&alice_id, &bob_id, false, None, Vec::new())
            .expect("alice -> bob request");
        state
            .services
            .follows
            .request_follow(&bob_id, &alice_id, false, None, Vec::new())
            .expect("bob -> alice request");

        // Production runs the worker on a background loop; do the same here.
        let _worker = state.worker.clone().spawn(Duration::from_millis(10));

        let (status, value) = call(
            &state,
            "admin/suspend-user",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        // Follow requests are gone in both directions in the same commit.
        assert!(
            state
                .services
                .follows
                .get_follow_request(&alice_id, &bob_id)
                .unwrap()
                .is_none()
        );
        assert!(
            state
                .services
                .follows
                .get_follow_request(&bob_id, &alice_id)
                .unwrap()
                .is_none()
        );

        // Poll until the sweep has fanned out: one Delete(Person) for the
        // actor and one Undo(Follow) for the remote edge.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            {
                let captured = bodies.lock().unwrap();
                let has_delete = captured.iter().any(|b| {
                    b.contains("\"type\":\"Delete\"") && b.contains(&format!("users/{bob_id}"))
                });
                let has_undo_follow = captured
                    .iter()
                    .any(|b| b.contains("\"type\":\"Undo\"") && b.contains("\"type\":\"Follow\""));
                if has_delete && has_undo_follow {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "suspend sweeps did not fan out: {:?}",
                bodies.lock().unwrap()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Outgoing follows removed (local and remote), incoming kept.
        assert!(!state.services.follows.is_following(&bob_id, &remote.id).unwrap());
        assert!(!state.services.follows.is_following(&bob_id, &carol_id).unwrap());
        assert!(state.services.follows.is_following(&remote.id, &bob_id).unwrap());

        // Unsuspend: the same knowable shared inboxes get
        // Undo(Delete(Person)); removed follows and requests stay removed.
        let (status, value) = call(
            &state,
            "admin/unsuspend-user",
            serde_json::json!({ "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            {
                let captured = bodies.lock().unwrap();
                let has_undo_delete = captured.iter().any(|b| {
                    b.contains("\"type\":\"Undo\"")
                        && b.contains("\"type\":\"Delete\"")
                        && b.contains(&format!("users/{bob_id}"))
                });
                if has_undo_delete {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "unsuspend broadcast did not fan out: {:?}",
                bodies.lock().unwrap()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!state.services.follows.is_following(&bob_id, &remote.id).unwrap());
        assert!(!state.services.follows.is_following(&bob_id, &carol_id).unwrap());
        assert!(
            state
                .services
                .follows
                .get_follow_request(&alice_id, &bob_id)
                .unwrap()
                .is_none(),
            "removed follow requests stay removed"
        );
    }

    #[tokio::test]
    async fn parity_default_reaction_heart_click() {
        let (state, _) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        async fn alice_note(state: &ApiState, alice_token: &str, body: Value) -> String {
            let (status, note) = call(state, "notes/create", body, Some(alice_token)).await;
            assert_eq!(status, StatusCode::OK, "{note}");
            note["createdNote"]["id"].as_str().unwrap().to_string()
        }

        async fn heart_click(
            state: &ApiState,
            bob_token: &str,
            note_id: &str,
            reaction: Value,
        ) {
            let mut body = serde_json::json!({ "noteId": note_id });
            if !reaction.is_null() {
                body["reaction"] = reaction;
            }
            let (status, value) =
                call(state, "notes/reactions/create", body, Some(bob_token)).await;
            assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
        }

        fn stored(state: &ApiState, user_id: &str, note_id: &str) -> String {
            state
                .services
                .reactions
                .get_reaction(user_id, note_id)
                .unwrap()
                .unwrap()
                .reaction
        }

        // No default set: heart-click with no `reaction` param → FALLBACK.
        let note_id = alice_note(&state, &alice_token, serde_json::json!({ "text": "n1" })).await;
        heart_click(&state, &bob_token, &note_id, Value::Null).await;
        assert_eq!(stored(&state, &bob_id, &note_id), "❤");

        // Explicit empty `reaction` is still rejected (upstream parity).
        let (status, value) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": "" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");

        // Bob sets a per-user default; heart-click resolves to it.
        let (status, me) = call(
            &state,
            "i/update",
            serde_json::json!({ "defaultReaction": "🎉" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        let note_id = alice_note(&state, &alice_token, serde_json::json!({ "text": "n2" })).await;
        heart_click(&state, &bob_token, &note_id, Value::Null).await;
        assert_eq!(stored(&state, &bob_id, &note_id), "🎉");

        // Explicit reaction wins over the stored default.
        let note_id = alice_note(&state, &alice_token, serde_json::json!({ "text": "n3" })).await;
        heart_click(
            &state,
            &bob_token,
            &note_id,
            serde_json::json!("👍"),
        )
        .await;
        assert_eq!(stored(&state, &bob_id, &note_id), "👍");

        // A VS16-form default normalizes through the standard pipeline.
        let (status, me) = call(
            &state,
            "i/update",
            serde_json::json!({ "defaultReaction": "❤️" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{me}");
        let note_id = alice_note(&state, &alice_token, serde_json::json!({ "text": "n4" })).await;
        heart_click(&state, &bob_token, &note_id, Value::Null).await;
        assert_eq!(stored(&state, &bob_id, &note_id), "❤");

        // `likeOnly` coercion still wins over the default.
        let note_id = alice_note(
            &state,
            &alice_token,
            serde_json::json!({ "text": "n5", "reactionAcceptance": "likeOnly" }),
        )
        .await;
        heart_click(&state, &bob_token, &note_id, Value::Null).await;
        assert_eq!(stored(&state, &bob_id, &note_id), "❤");
    }

    /// Read one export result file's bytes through the notification's
    /// `fileId` (media metadata + blob store, like the import worker does).
    async fn read_export_bytes(state: &ApiState, file_id: &str) -> Vec<u8> {
        let meta = state
            .services
            .media
            .get(file_id)
            .unwrap()
            .expect("export file metadata");
        state
            .services
            .blobs
            .get(&meta.blob_key)
            .unwrap()
            .expect("export file bytes")
    }

    /// The newest `exportCompleted` notifications for `token`
    /// (`markAsRead: false` so repeated reads in one test stay stable).
    async fn export_completed_notifications(state: &ApiState, token: &str) -> Value {
        let (status, notes) = call(
            state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false, "includeTypes": ["exportCompleted"] }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        notes
    }

    /// Create a remote account in the store (§13 import/export fixture).
    fn remote_account(services: &AppServices, username: &str, host: &str) -> String {
        use crate::application::ports::CreateAccountInput;
        services
            .accounts
            .create_account(CreateAccountInput {
                handle: username.to_string(),
                host: Some(host.to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://{host}/users/{username}")),
                inbox: Some(format!("https://{host}/inbox")),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .unwrap()
            .id
    }

    #[tokio::test]
    async fn parity_export_following_csv_exact_and_notification() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, _) = signup_user(&state, "bob").await;
        let (carol_id, _) = signup_user(&state, "carol").await;
        let eve_id = remote_account(&state.services, "eve", "remote.example");
        state.services.follow(&alice_id, &bob_id, true).unwrap();
        clock.advance_ms(1_000);
        state.services.follow(&alice_id, &carol_id, false).unwrap();
        clock.advance_ms(1_000);
        state.services.follow(&alice_id, &eve_id, true).unwrap();

        let (status, _) = call(
            &state,
            "i/export-following",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();

        // `exportCompleted` carries the drive file id + entity tag.
        let notes = export_completed_notifications(&state, &alice_token).await;
        let notes = notes.as_array().unwrap();
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert_eq!(notes[0]["type"], "exportCompleted");
        assert_eq!(notes[0]["exportedEntity"], "following");
        let file_id = notes[0]["fileId"].as_str().unwrap().to_string();

        // Exact upstream row shape: `{acct},withReplies={bool}`, one per
        // line, follow-row-id order, no header row. Locals are bare
        // usernames (DIVERGENCE), remotes are `user@host`.
        let bytes = read_export_bytes(&state, &file_id).await;
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "bob,withReplies=true\ncarol,withReplies=false\neve@remote.example,withReplies=true\n",
        );
    }

    #[tokio::test]
    async fn parity_export_following_exclude_muting_and_inactive() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, _) = signup_user(&state, "bob").await;
        let (carol_id, _) = signup_user(&state, "carol").await;
        state.services.follow(&alice_id, &bob_id, true).unwrap();
        clock.advance_ms(1_000);
        state.services.follow(&alice_id, &carol_id, true).unwrap();
        state.services.mute(&alice_id, &carol_id, None).unwrap();

        // Default export keeps muted followees.
        let (status, _) = call(
            &state,
            "i/export-following",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();
        let notes = export_completed_notifications(&state, &alice_token).await;
        let file_id = notes[0]["fileId"].as_str().unwrap().to_string();
        let bytes = read_export_bytes(&state, &file_id).await;
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "bob,withReplies=true\ncarol,withReplies=true\n",
        );

        // `excludeMuting` drops them (second call needs a fresh hour).
        clock.advance_ms(3_600_000);
        let (status, _) = call(
            &state,
            "i/export-following",
            serde_json::json!({ "excludeMuting": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();
        let notes = export_completed_notifications(&state, &alice_token).await;
        let file_id = notes[0]["fileId"].as_str().unwrap().to_string();
        let bytes = read_export_bytes(&state, &file_id).await;
        assert_eq!(String::from_utf8(bytes).unwrap(), "bob,withReplies=true\n");

        // `excludeInactive` is accepted and ignored (DIVERGENCE: no
        // last-active tracking): same bytes as the default export.
        clock.advance_ms(3_600_000);
        let (status, _) = call(
            &state,
            "i/export-following",
            serde_json::json!({ "excludeInactive": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();
        let notes = export_completed_notifications(&state, &alice_token).await;
        let file_id = notes[0]["fileId"].as_str().unwrap().to_string();
        let bytes = read_export_bytes(&state, &file_id).await;
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "bob,withReplies=true\ncarol,withReplies=true\n",
        );
    }

    #[tokio::test]
    async fn parity_export_blocking_and_mute_csv() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, _) = signup_user(&state, "bob").await;
        let (carol_id, _) = signup_user(&state, "carol").await;
        let (dave_id, _) = signup_user(&state, "dave").await;
        state.services.block(&alice_id, &bob_id).unwrap();
        state.services.mute(&alice_id, &carol_id, None).unwrap();
        // Expiring mutes are NOT exported (upstream `IsNull()` parity).
        state
            .services
            .mute(
                &alice_id,
                &dave_id,
                Some(state.services.clock.now_ms() + 100_000),
            )
            .unwrap();

        let (status, _) = call(
            &state,
            "i/export-blocking",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = call(
            &state,
            "i/export-mute",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();

        let notes = export_completed_notifications(&state, &alice_token).await;
        let notes = notes.as_array().unwrap();
        assert_eq!(notes.len(), 2, "{notes:?}");
        let mut seen = std::collections::HashMap::new();
        for note in notes {
            let entity = note["exportedEntity"].as_str().unwrap().to_string();
            let file_id = note["fileId"].as_str().unwrap().to_string();
            let bytes = read_export_bytes(&state, &file_id).await;
            seen.insert(entity, String::from_utf8(bytes).unwrap());
        }
        assert_eq!(seen["blocking"], "bob\n");
        assert_eq!(seen["muting"], "carol\n");
    }

    #[tokio::test]
    async fn parity_export_notes_json_roundtrip() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let first = state
            .services
            .create_note(crate::application::ports::CreateNoteInput::simple_public(
                &alice_id,
                "first note",
            ))
            .unwrap();
        clock.advance_ms(1_000);
        let second = state
            .services
            .create_note(crate::application::ports::CreateNoteInput::simple_public(
                &alice_id,
                "second note",
            ))
            .unwrap();

        let (status, _) = call(
            &state,
            "i/export-notes",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();

        let notes = export_completed_notifications(&state, &alice_token).await;
        assert_eq!(notes[0]["exportedEntity"], "note");
        let file_id = notes[0]["fileId"].as_str().unwrap().to_string();
        let bytes = read_export_bytes(&state, &file_id).await;
        let items: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
        // Newest-first, upstream DTO keys.
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["id"], second.id);
        assert_eq!(items[0]["text"], "second note");
        assert_eq!(items[1]["id"], first.id);
        for item in &items {
            for key in [
                "id",
                "text",
                "createdAt",
                "fileIds",
                "files",
                "replyId",
                "renoteId",
                "poll",
                "cw",
                "visibility",
                "visibleUserIds",
                "localOnly",
                "reactionAcceptance",
            ] {
                assert!(item.get(key).is_some(), "missing {key} in {item}");
            }
            assert_eq!(item["visibility"], "public");
            assert_eq!(item["poll"], Value::Null);
        }
    }

    #[tokio::test]
    async fn parity_import_blocking_guards_use_upstream_errors() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, _) = signup_user(&state, "bob").await;

        // Unknown id → NO_SUCH_FILE with the import-blocking UUID.
        let (status, value) = call(
            &state,
            "i/import-blocking",
            serde_json::json!({ "fileId": "mid00000000000999" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_FILE");
        assert_eq!(
            value["error"]["id"],
            "ebb53e5f-6574-9c0c-0b92-7ca6def56d7e"
        );

        // Foreign (bob's) file → same NO_SUCH_FILE (ownership is hidden).
        clock.advance_ms(3_600_000);
        let foreign = state
            .services
            .upload_media(&bob_id, "list.csv".to_string(), b"alice\n", None, false, true)
            .unwrap();
        let (status, value) = call(
            &state,
            "i/import-blocking",
            serde_json::json!({ "fileId": foreign.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_FILE");
        assert_eq!(
            value["error"]["id"],
            "ebb53e5f-6574-9c0c-0b92-7ca6def56d7e"
        );

        // Empty file → EMPTY_FILE.
        clock.advance_ms(3_600_000);
        let empty = state
            .services
            .upload_media(&alice_id, "empty.csv".to_string(), b"", None, false, true)
            .unwrap();
        let (status, value) = call(
            &state,
            "i/import-blocking",
            serde_json::json!({ "fileId": empty.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "EMPTY_FILE");
        assert_eq!(
            value["error"]["id"],
            "6f3a4dcc-f060-a707-4950-806fbdbe60d6"
        );

        // Over 64KiB → TOO_BIG_FILE.
        clock.advance_ms(3_600_000);
        let big = vec![b'a'; 65_537];
        let oversized = state
            .services
            .upload_media(&alice_id, "big.csv".to_string(), &big, None, false, true)
            .unwrap();
        let (status, value) = call(
            &state,
            "i/import-blocking",
            serde_json::json!({ "fileId": oversized.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "TOO_BIG_FILE");
        assert_eq!(
            value["error"]["id"],
            "b7fbf0b1-aeef-3b21-29ef-fadd4cb72ccf"
        );
    }

    #[tokio::test]
    async fn parity_import_blocking_rows_and_replay() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, _) = signup_user(&state, "bob").await;
        let file = state
            .services
            .upload_media(
                &alice_id,
                "blocking.csv".to_string(),
                b"bob\nno-such-user-xyz\n",
                None,
                false,
                true,
            )
            .unwrap();

        let (status, _) = call(
            &state,
            "i/import-blocking",
            serde_json::json!({ "fileId": file.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();

        // Valid rows applied, unresolvable rows skipped, batch intact.
        assert!(state.services.blocks.is_blocking(&alice_id, &bob_id).unwrap());
        let list = || {
            state
                .services
                .blocks
                .list_blockings(
                    &alice_id,
                    &TimelineCursor {
                        limit: 100,
                        ..Default::default()
                    },
                )
                .unwrap()
        };
        assert_eq!(list().len(), 1);

        // A replay of the same file converges (conflicts skip, no dupes).
        clock.advance_ms(3_600_000);
        let (status, _) = call(
            &state,
            "i/import-blocking",
            serde_json::json!({ "fileId": file.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();
        assert_eq!(list().len(), 1);
    }

    #[tokio::test]
    async fn parity_import_following_acct_and_legacy_rows() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, _) = signup_user(&state, "bob").await;
        let (carol_id, _) = signup_user(&state, "carol").await;
        let (dave_id, _) = signup_user(&state, "dave").await;
        let file = state
            .services
            .upload_media(
                &alice_id,
                "following.csv".to_string(),
                b"bob\ncarol,withReplies=false\ndeadbeef,dave,example.local\nghost@unknown.invalid\n",
                None,
                false,
                true,
            )
            .unwrap();

        let (status, _) = call(
            &state,
            "i/import-following",
            serde_json::json!({ "fileId": file.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();

        // Bare acct, acct + row `withReplies=`, and the legacy 3-column
        // (`id,username,host`) variant all resolve; the unresolvable remote
        // row skips without failing the batch.
        let bob = state
            .services
            .follows
            .get_follow(&alice_id, &bob_id)
            .unwrap()
            .unwrap();
        assert!(bob.with_replies, "endpoint default withReplies is true");
        let carol = state
            .services
            .follows
            .get_follow(&alice_id, &carol_id)
            .unwrap()
            .unwrap();
        assert!(!carol.with_replies, "row withReplies=false wins");
        assert!(
            state
                .services
                .follows
                .is_following(&alice_id, &dave_id)
                .unwrap()
        );
        assert_eq!(
            state
                .services
                .follows
                .following_of(&alice_id, None, 100)
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn parity_import_muting_applies_rows() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, _) = signup_user(&state, "bob").await;
        let file = state
            .services
            .upload_media(&alice_id, "mute.csv".to_string(), b"bob\n", None, false, true)
            .unwrap();

        let (status, _) = call(
            &state,
            "i/import-muting",
            serde_json::json!({ "fileId": file.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();
        assert!(
            state
                .services
                .mutes
                .is_muting(&alice_id, &bob_id)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn batch_n_favorites_crud_state_pack_and_errors() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        // Alice posts; Bob favorites it.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "fav target" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();

        let (status, _) = call(
            &state,
            "notes/favorites/create",
            serde_json::json!({ "noteId": note_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Duplicate → ALREADY_FAVORITED with the upstream UUID.
        let (status, value) = call(
            &state,
            "notes/favorites/create",
            serde_json::json!({ "noteId": note_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "ALREADY_FAVORITED");
        assert_eq!(value["error"]["id"], "a402c12b-34dd-41d2-9d8d-4d2ffd96a1a6");

        // notes/show packs isFavorited for the favoriter only.
        let (status, shown) = call(&state, "notes/show", serde_json::json!({ "noteId": note_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["isFavorited"], true);
        let (status, shown) = call(&state, "notes/show", serde_json::json!({ "noteId": note_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["isFavorited"], false);

        // notes/state shape.
        let (status, st) = call(&state, "notes/state", serde_json::json!({ "noteId": note_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{st}");
        assert_eq!(st, serde_json::json!({ "isFavorited": true, "isMutedThread": false }));

        // i/favorites lists the note.
        let (status, favs) = call(&state, "i/favorites", serde_json::json!({}), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{favs}");
        assert!(favs.as_array().unwrap().iter().any(|n| n["id"] == note_id), "{favs:?}");

        // Unfavorite; second delete → NOT_FAVORITED with the upstream UUID.
        let (status, _) = call(&state, "notes/favorites/delete", serde_json::json!({ "noteId": note_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, value) = call(&state, "notes/favorites/delete", serde_json::json!({ "noteId": note_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NOT_FAVORITED");
        assert_eq!(value["error"]["id"], "b625fc69-635e-4e89-baa6-dbefbef35af5");

        // Unknown note → NO_SUCH_NOTE on both paths.
        let (status, value) = call(&state, "notes/favorites/create", serde_json::json!({ "noteId": "aaaaaaaaaaaaaaaa" }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
        let (status, value) = call(&state, "notes/state", serde_json::json!({ "noteId": "aaaaaaaaaaaaaaaa" }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");

        // Invisible (followers-only, strangers) note → NO_SUCH_NOTE on create.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "locked", "visibility": "followers" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let priv_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, value) = call(&state, "notes/favorites/create", serde_json::json!({ "noteId": priv_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
        let _ = alice_id;
    }

    #[tokio::test]
    async fn batch_n_follow_request_accepted_emitted_and_filterable() {
        let (state, _) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // Bob locks his account; Alice's follow becomes a pending request.
        let (status, _) = call(&state, "i/update", serde_json::json!({ "isLocked": true }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&state, "following/create", serde_json::json!({ "userId": bob_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK);

        // Bob accepts; the worker emits followRequestAccepted to Alice.
        let (alice_id, _) = {
            let (status, me) = call(&state, "i", serde_json::json!({}), Some(&alice_token)).await;
            assert_eq!(status, StatusCode::OK);
            (me["id"].as_str().unwrap().to_string(), ())
        };
        let (status, _) = call(&state, "following/requests/accept", serde_json::json!({ "userId": alice_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.worker.drain(100).unwrap();
        let (status, notifs) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "limit": 20, "markAsRead": false, "includeTypes": ["followRequestAccepted"] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notifs}");
        assert!(
            notifs.as_array().unwrap().iter().any(|n| n["type"] == "followRequestAccepted"),
            "{notifs:?}"
        );
        // Excluding it hides it.
        let (status, notifs) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "limit": 20, "markAsRead": false, "excludeTypes": ["followRequestAccepted"] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{notifs}");
        assert!(
            notifs.as_array().unwrap().iter().all(|n| n["type"] != "followRequestAccepted"),
            "{notifs:?}"
        );
    }

    #[tokio::test]
    async fn batch_n_admin_remainder_unset_media_files() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;
        let (carol_id, carol_token) = signup_user(&state, "carol").await;
        state.services.set_role(None, &alice_id, false, true).expect("moderator");
        state.services.set_role(None, &carol_id, true, false).expect("admin");

        // Non-moderator cannot unset.
        let (status, _) = call(&state, "admin/unset-user-avatar", serde_json::json!({ "userId": bob_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // Bob sets an avatar first.
        let png = [
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
            0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78,
            0x9C, 0x62, 0x60, 0x01, 0x00, 0x00, 0xFF, 0xFF, 0x03, 0x00, 0x00, 0x06, 0x00, 0x05,
            0x72, 0x76, 0x01, 0x82, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42,
            0x60, 0x82,
        ];
        let file = state.services.upload_media(&bob_id, "a.png".to_string(), &png, None, false, true).expect("upload");
        let (status, _) = call(&state, "i/update", serde_json::json!({ "avatarId": file.id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK);

        // Moderator unsets → 204, avatar cleared, log row written.
        let (status, _) = call(&state, "admin/unset-user-avatar", serde_json::json!({ "userId": bob_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, shown) = call(&state, "users/show", serde_json::json!({ "userId": bob_id }), None).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert!(shown.get("avatarId").is_none() || shown["avatarId"].is_null(), "{shown:?}");
        let (status, logs) = call(&state, "admin/show-moderation-logs", serde_json::json!({ "type": "unsetUserAvatar" }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        assert_eq!(logs.as_array().unwrap().len(), 1, "{logs:?}");

        // Second unset is a no-op (no second row).
        let (status, _) = call(&state, "admin/unset-user-avatar", serde_json::json!({ "userId": bob_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, logs) = call(&state, "admin/show-moderation-logs", serde_json::json!({ "type": "unsetUserAvatar" }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        assert_eq!(logs.as_array().unwrap().len(), 1, "{logs:?}");

        // Refusal rule on admins (DIVERGENCE from upstream's role-only gate).
        let (status, value) = call(&state, "admin/unset-user-avatar", serde_json::json!({ "userId": carol_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        // Unknown target → NO_SUCH_USER.
        let (status, value) = call(&state, "admin/unset-user-banner", serde_json::json!({ "userId": "aaaaaaaaaaaaaaaa" }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_USER");

        // delete-all-files: moderator (non-admin) refused; admin clears.
        let (status, _) = call(&state, "admin/delete-all-files-of-a-user", serde_json::json!({ "userId": bob_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, files) = call(&state, "drive/files", serde_json::json!({}), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{files}");
        assert!(!files.as_array().unwrap().is_empty(), "bob has files");
        let (status, _) = call(&state, "admin/delete-all-files-of-a-user", serde_json::json!({ "userId": bob_id }), Some(&carol_token)).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, files) = call(&state, "drive/files", serde_json::json!({}), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{files}");
        assert!(files.as_array().unwrap().is_empty(), "{files:?}");

        // admin/drive/files: moderator can list with filters.
        let (status, rows) = call(&state, "admin/drive/files", serde_json::json!({ "limit": 10 }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        let (status, rows) = call(&state, "admin/drive/files", serde_json::json!({ "userId": bob_id, "origin": "combined" }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert!(rows.as_array().unwrap().is_empty(), "{rows:?}");
        // Non-moderator refused.
        let (status, _) = call(&state, "admin/drive/files", serde_json::json!({}), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn batch_n_admin_drive_show_file() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;
        state.services.set_role(None, &alice_id, false, true).expect("moderator");
        let file = state.services.upload_media(&alice_id, "s.png".to_string(), b"fakepng-bytes", None, false, true).expect("upload");

        // By fileId (any owner — admin view).
        let (status, shown) = call(&state, "admin/drive/show-file", serde_json::json!({ "fileId": file.id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["id"], file.id);
        // By served url.
        let (status, shown) = call(&state, "admin/drive/show-file", serde_json::json!({ "url": shown["url"] }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["id"], file.id);
        // Unknown → verbatim NO_SUCH_FILE.
        let (status, value) = call(&state, "admin/drive/show-file", serde_json::json!({ "fileId": "aaaaaaaaaaaaaaaa" }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_FILE");
        assert_eq!(value["error"]["id"], "caf3ca38-c6e5-472e-a30c-b05377dcc240");
        // Both/neither → INVALID_PARAM.
        let (status, _) = call(&state, "admin/drive/show-file", serde_json::json!({ "fileId": file.id, "url": "x" }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = call(&state, "admin/drive/show-file", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // Non-moderator refused.
        let (status, _) = call(&state, "admin/drive/show-file", serde_json::json!({ "fileId": file.id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn batch_n_abuse_reports_wire_up() {
        use crate::application::ports::CreateAccountInput;

        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // Self-report → CANNOT_REPORT_YOURSELF (verbatim upstream id).
        let (status, value) = call(
            &state,
            "users/report-abuse",
            serde_json::json!({ "userId": alice_id, "comment": "me" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "CANNOT_REPORT_YOURSELF");
        assert_eq!(value["error"]["id"], "1e13149e-b1e8-43cf-902e-c01dbfcb202f");

        // Make Bob an admin (CLI set-role path); reporting him →
        // CANNOT_REPORT_THE_ADMIN.
        state.services.set_role(None, &bob_id, true, false).expect("set role");
        let (status, value) = call(
            &state,
            "users/report-abuse",
            serde_json::json!({ "userId": bob_id, "comment": "him" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "CANNOT_REPORT_THE_ADMIN");

        // Alice reports Carol (plain user).
        let (carol_id, _) = signup_user(&state, "carol").await;
        let (status, report) = call(
            &state,
            "users/report-abuse",
            serde_json::json!({ "userId": carol_id, "comment": "spam" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{report}");
        let report_id = report["id"].as_str().unwrap().to_string();
        assert_eq!(report["comment"], "spam");
        assert_eq!(report["resolved"], false);
        assert_eq!(report["reporterId"], serde_json::json!(alice_id));

        // Non-moderator cannot list.
        let (status, _) = call(&state, "admin/abuse-user-reports", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // Promote Alice to moderator; list shows the report.
        state.services.set_role(None, &alice_id, false, true).expect("set role");
        let (status, rows) = call(&state, "admin/abuse-user-reports", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert!(rows.as_array().unwrap().iter().any(|r| r["id"] == report_id), "{rows:?}");

        // Resolve as accept → resolved + assignee set.
        let (status, _) = call(
            &state,
            "admin/resolve-abuse-user-report",
            serde_json::json!({ "reportId": report_id, "resolvedAs": "accept" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, rows) = call(
            &state,
            "admin/abuse-user-reports",
            serde_json::json!({ "state": "resolved" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        let row = rows.as_array().unwrap().iter().find(|r| r["id"] == report_id).cloned().unwrap();
        assert_eq!(row["resolved"], true);
        assert_eq!(row["resolvedAs"], "accept");
        assert_eq!(row["assigneeId"], serde_json::json!(alice_id));

        // Moderation-note update round-trips.
        let (status, _) = call(
            &state,
            "admin/update-abuse-user-report",
            serde_json::json!({ "reportId": report_id, "moderationNote": "handled" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, rows) = call(&state, "admin/abuse-user-reports", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        let row = rows.as_array().unwrap().iter().find(|r| r["id"] == report_id).cloned().unwrap();
        assert_eq!(row["moderationNote"], "handled");

        // Unknown report → NO_SUCH_ABUSE_REPORT.
        let (status, value) = call(
            &state,
            "admin/resolve-abuse-user-report",
            serde_json::json!({ "reportId": "aaaaaaaaaaaaaaaa", "resolvedAs": "reject" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
        assert_eq!(value["code"], "NO_SUCH_ABUSE_REPORT");

        // Forward on a local target is refused.
        let (status, value) = call(
            &state,
            "admin/forward-abuse-user-report",
            serde_json::json!({ "reportId": report_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");

        // Remote target: report → forward enqueues the Flag job and marks
        // the report forwarded; a second forward is refused.
        let remote = state
            .services
            .accounts
            .create_account(CreateAccountInput {
                handle: "rina".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/rina".to_string()),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .expect("remote account");
        let (status, report) = call(
            &state,
            "users/report-abuse",
            serde_json::json!({ "userId": remote.id, "comment": "remote spam" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{report}");
        let remote_report = report["id"].as_str().unwrap().to_string();
        let before = state.services.jobs.pending_count().expect("pending");
        let (status, _) = call(
            &state,
            "admin/forward-abuse-user-report",
            serde_json::json!({ "reportId": remote_report }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(
            state.services.jobs.pending_count().expect("pending") > before,
            "the Flag delivery job is enqueued"
        );
        let (status, value) = call(
            &state,
            "admin/forward-abuse-user-report",
            serde_json::json!({ "reportId": remote_report }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    }

    #[tokio::test]
    async fn batch_n_emoji_admin_pack_and_opt_in() {
        use crate::domain::moderation::ModerationAction;

        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        // Public listing starts empty in the Misskey `{emojis: [...]}` shape.
        let (status, body) = call(&state, "emojis", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, serde_json::json!({ "emojis": [] }));

        // Non-moderator add is refused with no state change.
        let (status, _) = call(
            &state,
            "admin/emoji/add",
            serde_json::json!({ "name": "blobcat", "fileId": "aaaaaaaaaaaaaaaa" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // Moderator: unknown file → upstream add-UUID NO_SUCH_FILE.
        state.services.set_role(None, &alice_id, false, true).expect("set role");
        let (status, value) = call(
            &state,
            "admin/emoji/add",
            serde_json::json!({ "name": "blobcat", "fileId": "aaaaaaaaaaaaaaaa" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_FILE");
        assert_eq!(value["error"]["id"], "fc46b5a4-6b92-4c33-ac66-b806659bb5cf");

        // Bad shortcode → INVALID_PARAM naming name.
        let file = state
            .services
            .upload_media(&alice_id, "e.png".to_string(), &[
                0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
                0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
                0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78,
                0x9C, 0x62, 0x60, 0x01, 0x00, 0x00, 0xFF, 0xFF, 0x03, 0x00, 0x00, 0x06, 0x00, 0x05,
                0x72, 0x76, 0x01, 0x82, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42,
                0x60, 0x82,
            ], None, false, true)
            .expect("upload png");
        let (status, value) = call(
            &state,
            "admin/emoji/add",
            serde_json::json!({ "name": "has-dash", "fileId": file.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");

        // Non-image file → UNSUPPORTED_FILE_TYPE.
        let text = state
            .services
            .upload_media(&alice_id, "t.txt".to_string(), b"hello", None, false, true)
            .expect("upload text");
        let (status, value) = call(
            &state,
            "admin/emoji/add",
            serde_json::json!({ "name": "blobcat", "fileId": text.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "UNSUPPORTED_FILE_TYPE");
        assert_eq!(value["error"]["id"], "f7599d96-8750-af68-1633-9575d625c1a7");

        // Happy-path add packs `EmojiDetailed`.
        let (status, packed) = call(
            &state,
            "admin/emoji/add",
            serde_json::json!({ "name": "blobcat", "fileId": file.id, "category": "c", "aliases": ["bc"] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{packed}");
        let emoji_id = packed["id"].as_str().unwrap().to_string();
        assert_eq!(packed["name"], "blobcat");
        assert!(packed["url"].as_str().unwrap().ends_with(&format!("/files/{}", file.id)));

        // Duplicate (case-insensitive) → DUPLICATE_NAME.
        let (status, value) = call(
            &state,
            "admin/emoji/add",
            serde_json::json!({ "name": "BlobCat", "fileId": file.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "DUPLICATE_NAME");

        // Moderation log row committed with the add.
        let (status, logs) = call(
            &state,
            "admin/show-moderation-logs",
            serde_json::json!({ "type": "addCustomEmoji" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{logs}");
        assert_eq!(logs.as_array().unwrap().len(), 1, "{logs:?}");

        // Public `emojis` + single `emoji` serve it.
        let (status, body) = call(&state, "emojis", serde_json::json!({}), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let names: Vec<&str> = body["emojis"].as_array().unwrap().iter().map(|e| e["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["blobcat"]);
        let (status, single) = call(&state, "emoji", serde_json::json!({ "name": "blobcat" }), None).await;
        assert_eq!(status, StatusCode::OK, "{single}");
        assert_eq!(single["id"], serde_json::json!(emoji_id));
        let (status, value) = call(&state, "emoji", serde_json::json!({ "name": "nope" }), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_EMOJI");

        // Update by name: category reset via explicit null.
        let (status, updated) = call(
            &state,
            "admin/emoji/update",
            serde_json::json!({ "name": "blobcat", "category": null, "isSensitive": true }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{updated}");
        assert!(updated.get("category").is_none() || updated["category"].is_null());
        assert_eq!(updated["isSensitive"], true);

        // Update on a missing row → NO_SUCH_EMOJI.
        let (status, value) = call(
            &state,
            "admin/emoji/update",
            serde_json::json!({ "id": "aaaaaaaaaaaaaaaa" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_EMOJI");

        // A second emoji, then rename onto the taken name → SAME_NAME_EMOJI_EXISTS.
        let (status, second) = call(
            &state,
            "admin/emoji/add",
            serde_json::json!({ "name": "free", "fileId": file.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{second}");
        let second_id = second["id"].as_str().unwrap().to_string();
        let (status, value) = call(
            &state,
            "admin/emoji/update",
            serde_json::json!({ "id": second_id, "name": "BLOBCAT" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "SAME_NAME_EMOJI_EXISTS");
        assert_eq!(value["error"]["id"], "7180fe9d-1ee3-bff9-647d-fe9896d2ffb8");
        // Rename to a free name succeeds.
        let (status, renamed) = call(
            &state,
            "admin/emoji/update",
            serde_json::json!({ "id": second_id, "name": "freed" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{renamed}");
        assert_eq!(renamed["name"], "freed");

        // Admin list with a query filter.
        let (status, rows) = call(
            &state,
            "admin/emoji/list",
            serde_json::json!({ "query": "blob" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert_eq!(rows.as_array().unwrap().len(), 1, "{rows:?}");

        // Delete → NO_CONTENT; the name resolves empty afterwards.
        let (status, _) = call(
            &state,
            "admin/emoji/delete",
            serde_json::json!({ "id": emoji_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, value) = call(&state, "emoji", serde_json::json!({ "name": "blobcat" }), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_EMOJI");

        // WP8b opt-in: `i/update {showRemoteEmojis}` round-trips on `i`.
        let (status, _) = call(&state, "i/update", serde_json::json!({ "showRemoteEmojis": true }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK);
        let (status, me) = call(&state, "i", serde_json::json!({}), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["showRemoteEmojis"], true);

        // Seed one remote-emoji row directly (ingest path is covered by the
        // federation unit tests); a note packs its URL for the opted-in
        // viewer only.
        state
            .services
            .emojis
            .upsert_remote_emoji(crate::domain::emoji::RemoteEmoji::new(
                "remote.example", "party", "https://remote.example/e/party.png", 1_700_000_000_000,
            ).unwrap())
            .expect("seed remote emoji");
        let (status, created) = call(&state, "notes/create", serde_json::json!({ "text": "hi" }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, shown) = call(&state, "notes/show", serde_json::json!({ "noteId": note_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["emojis"]["party@remote.example"], "https://remote.example/e/party.png");
        // Opted-out viewers see local-only maps.
        let (status, shown) = call(&state, "notes/show", serde_json::json!({ "noteId": note_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert!(shown["emojis"].get("party@remote.example").is_none(), "{shown:?}");
        // Anonymous viewers likewise.
        let (status, shown) = call(&state, "notes/show", serde_json::json!({ "noteId": note_id }), None).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert!(shown["emojis"].get("party@remote.example").is_none(), "{shown:?}");

        // Moderator-only gate on list/update/delete (bob is a plain user).
        let (status, _) = call(&state, "admin/emoji/list", serde_json::json!({}), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let _ = ModerationAction::AddCustomEmoji;
    }

    #[tokio::test]
    async fn parity_empty_exports_are_valid() {
        let (state, _) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;

        for endpoint in [
            "i/export-user-lists",
            "i/export-antennas",
            "i/export-clips",
            "i/export-favorites",
        ] {
            let (status, _) =
                call(&state, endpoint, serde_json::json!({}), Some(&alice_token)).await;
            assert_eq!(status, StatusCode::NO_CONTENT, "{endpoint}");
        }
        state.worker.drain(100).unwrap();

        let notes = export_completed_notifications(&state, &alice_token).await;
        let notes = notes.as_array().unwrap();
        assert_eq!(notes.len(), 4, "{notes:?}");
        let mut seen = std::collections::HashMap::new();
        for note in notes {
            let entity = note["exportedEntity"].as_str().unwrap().to_string();
            let file_id = note["fileId"].as_str().unwrap().to_string();
            let bytes = read_export_bytes(&state, &file_id).await;
            seen.insert(entity, String::from_utf8(bytes).unwrap());
        }
        // Zero user lists export as zero CSV rows; antennas/clips/favorites
        // export as empty JSON arrays (what upstream writes when empty —
        // antennas/favorites now have subsystems, so this is the empty case
        // of the real exporters).
        assert_eq!(seen["userList"], "");
        assert_eq!(seen["antenna"], "[]");
        assert_eq!(seen["clip"], "[]");
        assert_eq!(seen["favorite"], "[]");
    }

    #[tokio::test]
    async fn parity_export_rate_limit_trips_and_recovers() {
        let (state, clock) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;

        let (status, _) = call(
            &state,
            "i/export-notes",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        // Second call inside the hour trips the 1/hr bucket.
        let (status, value) = call(
            &state,
            "i/export-notes",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
        assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");
        // A fresh hour admits again.
        clock.advance_ms(3_600_000);
        let (status, _) = call(
            &state,
            "i/export-notes",
            serde_json::json!({}),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn batch_n_antennas_crud_notes_and_errors() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // Bad src → INVALID_PARAM.
        let (status, value) = call(
            &state,
            "antennas/create",
            serde_json::json!({
                "name": "x", "src": "nope", "keywords": [["x"]],
                "excludeKeywords": [], "users": [],
                "caseSensitive": false, "withReplies": true, "withFile": false,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");

        // Empty keywords → EMPTY_KEYWORD (verbatim id).
        let (status, value) = call(
            &state,
            "antennas/create",
            serde_json::json!({
                "name": "x", "src": "all", "keywords": [[" "]], "excludeKeywords": [],
                "users": [], "caseSensitive": false, "withReplies": true, "withFile": false,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "EMPTY_KEYWORD");
        assert_eq!(value["error"]["id"], "53ee222e-1ddd-4f9a-92e5-9fb82ddb463a");

        // src=list is refused (WP16 pending): NO_SUCH_USER_LIST.
        let (status, value) = call(
            &state,
            "antennas/create",
            serde_json::json!({
                "name": "x", "src": "list", "userListId": "aaaaaaaaaaaaaaaa",
                "keywords": [["x"]], "excludeKeywords": [], "users": [],
                "caseSensitive": false, "withReplies": true, "withFile": false,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_USER_LIST");

        // Happy path: keyword antenna over everything.
        let (status, packed) = call(
            &state,
            "antennas/create",
            serde_json::json!({
                "name": "kw", "src": "all", "keywords": [["hello"]],
                "excludeKeywords": [["spam"]], "users": [],
                "caseSensitive": false, "withReplies": false, "withFile": false,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{packed}");
        let antenna_id = packed["id"].as_str().unwrap().to_string();
        assert_eq!(packed["src"], "all");
        assert_eq!(packed["hasUnreadNote"], false);
        assert_eq!(packed["isActive"], true);

        // Cap: 4 more succeed, the 6th refuses with TOO_MANY_ANTENNAS.
        for i in 0..4 {
            let (status, _) = call(
                &state,
                "antennas/create",
                serde_json::json!({
                    "name": format!("extra{i}"), "src": "all", "keywords": [["z"]],
                    "excludeKeywords": [], "users": [],
                    "caseSensitive": false, "withReplies": true, "withFile": false,
                }),
                Some(&alice_token),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{i}");
        }
        let (status, value) = call(
            &state,
            "antennas/create",
            serde_json::json!({
                "name": "toomany", "src": "all", "keywords": [["z"]],
                "excludeKeywords": [], "users": [],
                "caseSensitive": false, "withReplies": true, "withFile": false,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "TOO_MANY_ANTENNAS");
        assert_eq!(value["error"]["id"], "faf47050-e8b5-438c-913c-db2b1576fde4");

        // Show + list.
        let (status, shown) = call(&state, "antennas/show", serde_json::json!({ "antennaId": antenna_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["name"], "kw");
        let (status, rows) = call(&state, "antennas/list", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert_eq!(rows.as_array().unwrap().len(), 5, "{rows:?}");
        // Foreign viewers see nothing (owner-scoped).
        let (status, value) = call(&state, "antennas/show", serde_json::json!({ "antennaId": antenna_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_ANTENNA");
        assert_eq!(value["error"]["id"], "10c673ac-8852-48eb-aa1f-f5b67f069290");

        // Update: rename + narrow to users-source.
        let (status, updated) = call(
            &state,
            "antennas/update",
            serde_json::json!({ "antennaId": antenna_id, "name": "kw2", "src": "users", "users": ["bob"] }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{updated}");
        assert_eq!(updated["name"], "kw2");
        assert_eq!(updated["src"], "users");

        // Notes flow is covered below against a fresh src=all antenna; first
        // reset this one and check the empty feed + remove-note on empty.
        let (status, notes) = call(&state, "antennas/notes", serde_json::json!({ "antennaId": antenna_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        assert_eq!(notes.as_array().unwrap().len(), 0, "{notes:?}");
        let (status, _) = call(
            &state,
            "antennas/remove-note",
            serde_json::json!({ "antennaId": antenna_id, "noteId": "aaaaaaaaaaaaaaaa" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Feed: a fresh match-all antenna collects a matching note through
        // the worker (note-create transaction enqueues the match job).
        // (An empty keyword list is EMPTY_KEYWORD at the API — like
        // upstream — so the antenna carries a real keyword.)
        let (status, packed) = call(
            &state,
            "antennas/create",
            serde_json::json!({
                "name": "all2", "src": "all", "keywords": [["hello"]],
                "excludeKeywords": [], "users": [],
                "caseSensitive": false, "withReplies": true, "withFile": false,
            }),
            Some(&bob_token),
        )
        .await;
        // Bob already has 0 antennas; this is fine — but Alice is at cap, so
        // use Bob's account for the feed antenna.
        assert_eq!(status, StatusCode::OK, "{packed}");
        let feed_id = packed["id"].as_str().unwrap().to_string();
        let (status, created) = call(&state, "notes/create", serde_json::json!({ "text": "hello world" }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        state.worker.drain(200).unwrap();
        let (status, notes) = call(&state, "antennas/notes", serde_json::json!({ "antennaId": feed_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        assert!(notes.as_array().unwrap().iter().any(|n| n["id"] == note_id), "{notes:?}");
        // Read-time visibility filtering: a followers-only note from a
        // stranger stays out of Bob's feed.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "locked hello", "visibility": "followers" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let locked_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        state.worker.drain(200).unwrap();
        let (status, notes) = call(&state, "antennas/notes", serde_json::json!({ "antennaId": feed_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        assert!(!notes.as_array().unwrap().iter().any(|n| n["id"] == locked_id), "{notes:?}");
        // remove-note drops the visible one.
        let (status, _) = call(
            &state,
            "antennas/remove-note",
            serde_json::json!({ "antennaId": feed_id, "noteId": note_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, notes) = call(&state, "antennas/notes", serde_json::json!({ "antennaId": feed_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        assert!(!notes.as_array().unwrap().iter().any(|n| n["id"] == note_id), "{notes:?}");

        // Delete cascades; the feed is gone afterwards.
        let (status, _) = call(&state, "antennas/delete", serde_json::json!({ "antennaId": feed_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, value) = call(&state, "antennas/notes", serde_json::json!({ "antennaId": feed_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_ANTENNA");
        let _ = (alice_id, bob_id);
    }

    #[tokio::test]
    async fn batch_n_word_mutes_soft_flag_packed_hard_dropped() {
        let (state, _) = test_state();
        let (_, alice_token) = signup_user(&state, "alice").await;
        let (_, bob_token) = signup_user(&state, "bob").await;

        // Alice: soft /spoiler/i, hard /secret/i (i/update transport is the
        // Misskey `/pattern/flags` string form).
        let (status, _) = call(
            &state,
            "i/update",
            serde_json::json!({
                "mutedWords": ["/spoiler/i"],
                "hardMutedWords": ["/secret/i"],
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // Bob posts a soft hit, a hard hit, and a plain note.
        let (status, soft) = call(&state, "notes/create", serde_json::json!({ "text": "a spoiler here" }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{soft}");
        let soft_id = soft["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, hard) = call(&state, "notes/create", serde_json::json!({ "text": "a secret here" }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{hard}");
        let hard_id = hard["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, plain) = call(&state, "notes/create", serde_json::json!({ "text": "just a note" }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{plain}");
        let plain_id = plain["createdNote"]["id"].as_str().unwrap().to_string();

        // Soft hit packs the viewer flag; hard hit reads as missing.
        let (status, shown) = call(&state, "notes/show", serde_json::json!({ "noteId": soft_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["isSoftMuted"], true);
        let (status, shown) = call(&state, "notes/show", serde_json::json!({ "noteId": plain_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["isSoftMuted"], false);
        let (status, value) = call(&state, "notes/show", serde_json::json!({ "noteId": hard_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");

        // Bob (no mutes) sees everything unflagged.
        let (status, shown) = call(&state, "notes/show", serde_json::json!({ "noteId": soft_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::OK, "{shown}");
        assert_eq!(shown["isSoftMuted"], false);
    }

    #[tokio::test]
    async fn sharkey_schedule_shim_crud_and_errors() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (bob_id, bob_token) = signup_user(&state, "bob").await;

        // scheduleNote.scheduledAt is required and must be in the future.
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "text": "x" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        let past = clock.now_ms() - 1_000;
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "text": "x", "scheduleNote": { "scheduledAt": past } }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "CANNOT_CREATE_ALREADY_EXPIRED_SCHEDULE");
        assert_eq!(value["error"]["id"], "8a9bfb90-fc7e-4878-a3e8-d97faaf5fb07");

        // anyOf content rule.
        let future = clock.now_ms() + 3_600_000;
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "scheduleNote": { "scheduledAt": future } }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");

        // Up-front target errors carry verbatim Sharkey ids.
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "renoteId": "aaaaaaaaaaaaaaaa", "scheduleNote": { "scheduledAt": future } }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_RENOTE_TARGET");
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "replyId": "aaaaaaaaaaaaaaaa", "text": "x", "scheduleNote": { "scheduledAt": future } }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_REPLY_TARGET");
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "renoteId": "aaaaaaaaaaaaaaaa", "scheduleNote": { "scheduledAt": future } }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_RENOTE_TARGET");

        // Pure-renote gates.
        let (status, created) = call(&state, "notes/create", serde_json::json!({ "text": "target" }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let target_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, created) = call(&state, "notes/create", serde_json::json!({ "renoteId": target_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let pure_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "renoteId": pure_id, "scheduleNote": { "scheduledAt": future } }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "CANNOT_RENOTE_TO_A_PURE_RENOTE");
        assert_eq!(value["error"]["id"], "fd4cc33e-2a37-48dd-99cc-9b806eb2031a");
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "replyId": pure_id, "text": "x", "scheduleNote": { "scheduledAt": future } }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "CANNOT_REPLY_TO_A_PURE_RENOTE");
        assert_eq!(value["error"]["id"], "3ac74a84-8fd5-4bb0-870f-01804f82ce15");

        // Renote visibility gate.
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "followers only", "visibility": "followers" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let followers_id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "renoteId": followers_id, "scheduleNote": { "scheduledAt": future } }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "CANNOT_RENOTE_DUE_TO_VISIBILITY");

        // Poll expiry is judged against the scheduled time, not now.
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({
                "text": "x",
                "poll": { "choices": ["a", "b"], "expiresAt": future - 1 },
                "scheduleNote": { "scheduledAt": future },
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "CANNOT_CREATE_ALREADY_EXPIRED_POLL");

        // Happy path: full shape + defaults, packed schedule-note shape.
        // `mediaIds` is accepted as the file-key form (upstream parity;
        // `fileIds` wins when both are present).
        let media = state.services.upload_media(&alice_id, "sched.png".to_string(), b"fakepng", None, false, true).expect("upload");
        let (status, packed) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({
                "text": "sched hello",
                "visibility": "public",
                "mediaIds": [media.id],
                "scheduleNote": { "scheduledAt": future },
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{packed}");
        assert_eq!(packed["userId"], alice_id);
        assert!(packed["scheduledAt"].as_str().unwrap().ends_with('Z'), "{packed:?}");
        assert_eq!(packed["note"]["text"], "sched hello");
        assert_eq!(packed["note"]["isSchedule"], true);
        assert_eq!(packed["note"]["visibility"], "public");
        assert_eq!(packed["note"]["reactionAcceptance"], Value::Null);
        assert!(packed["note"]["visibleUsers"].as_array().unwrap().is_empty());
        assert_eq!(packed["note"]["fileIds"].as_array().unwrap().len(), 1, "{packed:?}");
        let schedule_id = packed["id"].as_str().unwrap().to_string();

        // Second scheduled entry overflows the default limit of 1.
        let (status, value) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "text": "y", "scheduleNote": { "scheduledAt": future } }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "SCHEDULE_NOTE_MAX");
        assert_eq!(value["error"]["id"], "168707c3-e7da-4031-989e-f42aa3a274b2");

        // List shows the entry in the upstream shape.
        let (status, rows) = call(&state, "notes/schedule/list", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert_eq!(rows.as_array().unwrap().len(), 1, "{rows:?}");
        assert_eq!(rows[0]["id"], schedule_id);

        // Delete: unknown id → NO_SUCH_NOTE (a58056ba); foreign → 403.
        let (status, value) = call(&state, "notes/schedule/delete", serde_json::json!({ "noteId": "aaaaaaaaaaaaaaaa" }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
        assert_eq!(value["error"]["id"], "a58056ba-8ba1-4323-8ebf-e0b585bc244f");
        let (status, value) = call(&state, "notes/schedule/delete", serde_json::json!({ "noteId": schedule_id }), Some(&bob_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
        assert_eq!(value["code"], "PERMISSION_DENIED");
        assert_eq!(value["error"]["id"], "c0da2fed-8f61-4c47-a41d-431992607b5c");

        // Delete then advance past the time: nothing posts (job cleared).
        let (status, _) = call(&state, "notes/schedule/delete", serde_json::json!({ "noteId": schedule_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        clock.advance_ms(3_601_000);
        state.worker.drain(100).unwrap();
        let (status, rows) = call(&state, "notes/schedule/list", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert!(rows.as_array().unwrap().is_empty(), "{rows:?}");

        // End to end: a shim-created entry publishes through the normal path.
        let fire_at = clock.now_ms() + 60_000;
        let (status, packed) = call(
            &state,
            "notes/schedule/create",
            serde_json::json!({ "text": "fires", "scheduleNote": { "scheduledAt": fire_at } }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{packed}");
        state.worker.drain(100).unwrap();
        let (status, mine) = call(&state, "users/notes", serde_json::json!({ "userId": alice_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{mine}");
        assert!(!mine.as_array().unwrap().iter().any(|n| n["text"] == "fires"), "not yet due");
        clock.advance_ms(61_000);
        state.worker.drain(100).unwrap();
        let (status, mine) = call(&state, "users/notes", serde_json::json!({ "userId": alice_id }), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{mine}");
        assert!(mine.as_array().unwrap().iter().any(|n| n["text"] == "fires"), "{mine:?}");
        let _ = bob_id;
    }

    #[tokio::test]
    async fn batch_n_import_antennas_applies_valid_rows() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let valid = serde_json::json!([
            {
                "name": "a",
                "src": "all",
                "keywords": [["x"]],
                "excludeKeywords": [],
                "users": [],
                "userListAccts": null,
                "caseSensitive": false,
                "localOnly": false,
                "excludeBots": false,
                "withReplies": true,
                "withFile": false,
            },
            // Empty-keyword rows skip (upstream verbatim rule).
            {
                "name": "empty",
                "src": "all",
                "keywords": [[""]],
                "excludeKeywords": [],
                "users": [],
                "caseSensitive": false,
                "localOnly": false,
                "excludeBots": false,
                "withReplies": true,
                "withFile": false,
            },
            // `src=list` + accts rewrites to `users` (upstream parity).
            {
                "name": "listed",
                "src": "list",
                "keywords": [["y"]],
                "excludeKeywords": [],
                "users": [],
                "userListAccts": ["bob@example.com"],
                "caseSensitive": false,
                "localOnly": false,
                "excludeBots": false,
                "withReplies": true,
                "withFile": false,
            },
            // Schema-invalid rows skip.
            { "name": "broken" },
        ]);
        let file = state
            .services
            .upload_media(
                &alice_id,
                "antennas.json".to_string(),
                &serde_json::to_vec(&valid).unwrap(),
                None,
                false,
                true,
            )
            .unwrap();
        let (status, _) = call(
            &state,
            "i/import-antennas",
            serde_json::json!({ "fileId": file.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        // The worker applies 2 rows (a, listed), skips 2 (empty, broken);
        // imports never notify.
        state.worker.drain(100).unwrap();
        let (status, rows) = call(&state, "antennas/list", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        let names: Vec<&str> = rows.as_array().unwrap().iter().map(|r| r["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"a"), "{rows:?}");
        assert!(names.contains(&"listed"), "{rows:?}");
        assert_eq!(rows.as_array().unwrap().len(), 2, "{rows:?}");
        let listed = rows.as_array().unwrap().iter().find(|r| r["name"] == "listed").unwrap();
        assert_eq!(listed["src"], "users");
        assert_eq!(listed["users"], serde_json::json!(["bob@example.com"]));

        // Malformed JSON is INVALID_PARAM (DIVERGENCE: upstream throws 500).
        clock.advance_ms(3_600_000);
        let bad = state
            .services
            .upload_media(&alice_id, "bad.json".to_string(), b"not json", None, false, true)
            .unwrap();
        let (status, value) = call(
            &state,
            "i/import-antennas",
            serde_json::json!({ "fileId": bad.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
    }

    #[tokio::test]
    async fn parity_import_user_lists_validates_but_applies_nothing() {
        let (state, _) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let (_bob_id, _bob_token) = signup_user(&state, "bob").await;
        let file = state
            .services
            .upload_media(
                &alice_id,
                "lists.csv".to_string(),
                b"Friends,bob\nnot-a-valid-row\n",
                None,
                false,
                true,
            )
            .unwrap();
        let (status, _) = call(
            &state,
            "i/import-user-lists",
            serde_json::json!({ "fileId": file.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        // No user-list subsystem: validates, applies zero, completes.
        state.worker.drain(100).unwrap();
    }
}
