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
use crate::application::commands::pins::FeaturedCache;
use crate::application::commands::AppServices;
use crate::application::ports::{
    ApiTokenRecord, CreateDraftInput, CreateNoteInput, EditNoteInput, ExportEntity, ImportEntity,
    LegacyAuthSessionRecord, MiAuthSessionRecord, ModerationLogFilter, PortError, TimelineCursor,
    UpdateAccountPatch, UpdateDraftPatch,
};
use crate::domain::account::{
    Account, AvatarDecoration, ChatScope, FollowVisibility, MutedWord,
    NotificationRecieveConfig, ProfileField,
};
use crate::domain::moderation::ModerationAction;
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
    is_safe_callback_url, needs_rehash, render_miauth_page, require_moderator,
    verify_password_async, AuthenticatedCaller, SESSION_COOKIE_NAME,
};
use super::errors::ApiError;
use super::packers::{
    pack_blocking, pack_drive_file, pack_following, pack_me_detailed,
    pack_meta, pack_moderation_log, pack_muting, pack_note, pack_note_draft,
    pack_note_reaction_with_note,
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
fn draft_file_ids(obj: &Map<String, Value>) -> Result<Vec<String>, ApiError> {
    let ids = str_list(obj, "fileIds")?;
    if ids.len() > 16 {
        return Err(ApiError::invalid_param("fileIds", "must have at most 16 items"));
    }
    let mut seen = std::collections::HashSet::new();
    if !ids.iter().all(|id| seen.insert(id)) {
        return Err(ApiError::invalid_param("fileIds", "must be unique"));
    }
    Ok(ids)
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
            | "followRequestAccepted"
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
const ALL_NOTIFICATION_KINDS: [NotificationKind; 10] = [
    NotificationKind::Follow,
    NotificationKind::ReceiveFollowRequest,
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
    State(state): State<ApiState>,
    Path(session): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let name = query.get("name").cloned();
    let icon = query.get("icon").cloned();
    let callback = query.get("callback").cloned();
    let permission = query.get("permission").cloned();
    let permission_value = permission.clone().map(Value::String);
    let permissions = parse_permissions(permission_value.as_ref());
    let existing = state
        .services
        .auth
        .get_miauth_session(&session)
        .map_err(ApiError::from_port_error)?;
    let record = match existing {
        Some(mut sess) => {
            if name.is_some() {
                sess.name = name.clone();
            }
            if icon.is_some() {
                sess.icon_url = icon.clone();
            }
            if callback.is_some() {
                sess.callback = callback.clone();
            }
            if query.contains_key("permission") {
                sess.permissions = permissions.clone();
            }
            sess
        }
        None => MiAuthSessionRecord {
            session: session.clone(),
            name: name.clone(),
            description: None,
            icon_url: icon.clone(),
            callback: callback.clone(),
            permissions: permissions.clone(),
            account_id: None,
            token: None,
            fetched: false,
            created_at_ms: state.services.clock.now_ms(),
        },
    };
    state
        .services
        .auth
        .save_miauth_session(record)
        .map_err(ApiError::from_port_error)?;
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
            Some(account) if verified => account,
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
            created_at_ms: state.services.clock.now_ms(),
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

// --- Web signup / signin ---------------------------------------------------

async fn do_signup(
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
    if let Some(host) = obj.get("host") {
        if !host.is_null() {
            return Err(ApiError::invalid_param(
                "host",
                "remote accounts are unsupported in Phase 1",
            ));
        }
    }
    // Slice 9 (H4): every signup attempt from one IP counts.
    state.rate_limiter.check(&auth_ip_key(ip), "auth/signup")?;
    let hash = hash_password_async(password.to_string()).await?;
    let account = state
        .services
        .create_account(username, Some(&hash))
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
    let (account, raw_token) = do_signup(&state, &obj, &ip).await?;
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
            Ok(Json(pack_meta(&state.services, detail)).into_response())
        }

        "miauth/gen-token" => {
            check_allowed_fields(
                &obj,
                &["session", "permission", "name", "description", "iconUrl"],
            )?;
            let caller = require_caller(caller)?;
            caller.require_secure()?;
            let session = obj
                .get("session")
                .and_then(Value::as_str)
                .map(|s| s.to_string())
                .unwrap_or_else(generate_random_token);
            let permissions = parse_permissions(obj.get("permission"));
            let raw_token = generate_random_token();
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
                    created_at_ms: state.services.clock.now_ms(),
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

        "signup" | "admin/accounts/create" => {
            check_allowed_fields(&obj, SIGNUP_FIELDS)?;
            let (account, raw_token) =
                tokio::runtime::Handle::current().block_on(do_signup(&state, &obj, &client_ip))?;
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
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/update")?;
            let patch = parse_profile_patch(&obj)?;
            let account = state
                .services
                .update_profile(&caller.account.id, patch)
                .map_err(ApiError::from_port_error)?;
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
            // DIVERGENCE: upstream `JSON.parse`s here (a malformed file
            // throws → HTTP 500) and enforces the `antennaLimit` role
            // policy (`TOO_MANY_ANTENNAS`). Pinetree answers malformed JSON
            // with `INVALID_PARAM` and skips the limit check (no antenna
            // subsystem or role limits exist); the worker re-validates rows
            // and applies zero of them.
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
            if !parsed.is_array() {
                return Err(ApiError::invalid_param(
                    "fileId",
                    "file must contain a JSON array of antennas",
                ));
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
            })
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
            })
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
            })
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
            .create_account(CreateAccountInput::local("owner"))
            .expect("owner account");
        let mut fans: Vec<String> = Vec::with_capacity(250);
        for i in 0..250 {
            let fan = state
                .services
                .accounts
                .create_account(CreateAccountInput::local(format!("fan{i:03}")))
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
            })
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
    async fn parity_no_subsystem_exports_are_empty_but_valid() {
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
        // export as empty JSON arrays (what upstream writes when empty).
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
    async fn parity_import_antennas_validates_but_applies_nothing() {
        let (state, clock) = test_state();
        let (alice_id, alice_token) = signup_user(&state, "alice").await;
        let valid = serde_json::json!([{
            "name": "a",
            "src": "home",
            "keywords": [["x"]],
            "excludeKeywords": [],
            "users": [],
            "userListAccts": null,
            "caseSensitive": false,
            "localOnly": false,
            "excludeBots": false,
            "withReplies": false,
            "withFile": false,
            "excludeNotesInSensitiveChannel": false,
        }]);
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
        // No antenna subsystem exists: the job validates rows, applies zero,
        // and still completes (imports never notify).
        state.worker.drain(100).unwrap();

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
