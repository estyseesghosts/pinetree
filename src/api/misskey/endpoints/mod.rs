//! Misskey HTTP API surface (Slice D3).
//!
//! [`ApiState`] carries the application services, the in-process job
//! worker, and the rate limiter. [`build_router`] wires every Phase 1
//! route: `/healthz`, media/identicon file serving, the MiAuth web flow
//! (`GET`/`POST /miauth/{session}` plus the one-shot `check`
//! endpoints), web `signup`/`signin`, `drive/files/create` (multipart and
//! JSON), and the `POST /api/{endpoint}` JSON dispatcher.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    body::{Body, Bytes},
    extract::{FromRequest, Multipart, Path, Query, Request, State},
    http::{
        header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use http_body_util::{BodyExt, Limited};
use serde_json::{Map, Value};

use crate::activitypub::{actor as ap_actor, objects as ap_objects, webfinger as ap_webfinger};
use crate::application::commands::AppServices;
use crate::application::ports::{
    ApiTokenRecord, CreateNoteInput, LegacyAuthSessionRecord, MiAuthSessionRecord, PortError,
    TimelineCursor, UpdateAccountPatch,
};
use crate::domain::account::Account;
use crate::domain::note::{ReactionAcceptance, Visibility};
use crate::domain::notification::Notification;
use crate::federation::delivery::{
    fetch_and_upsert_remote_actor, fetch_remote_note, webfinger_discover_actor_uri,
};
use crate::federation::inbox as fed_inbox;
use crate::jobs::worker::JobWorker;

use super::auth::{
    authenticate, extract_raw_token, generate_random_token, hash_password, hash_token,
    is_safe_callback_url, render_miauth_page, verify_password, AuthenticatedCaller,
    SESSION_COOKIE_NAME,
};
use super::errors::ApiError;
use super::packers::{
    format_millis, pack_drive_file, pack_me_detailed, pack_meta, pack_note,
    pack_user_detailed_not_me, pack_user_lite,
};
use super::request::check_allowed_fields;
use super::response::{RateLimiter, UploadSlots};

/// Shared API state: application services, the durable-job worker, the
/// Misskey rate limiter, and the concurrent-upload slots.
#[derive(Clone)]
pub struct ApiState {
    pub services: Arc<AppServices>,
    pub worker: Arc<JobWorker>,
    pub rate_limiter: Arc<RateLimiter>,
    pub upload_slots: Arc<UploadSlots>,
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
        // No `DefaultBodyLimit::disable()`: the Misskey JSON dispatcher
        // enforces its own 1 MiB cap in `api_dispatch_blocking` (testable
        // without router layers), `drive/files/create` enforces its own
        // streaming cap in the handler, and everything else keeps axum's
        // 2 MiB default extraction limit.
        .with_state(state)
        .merge(federation_routes)
}

// --- Small helpers ---------------------------------------------------------

async fn healthz() -> &'static str {
    "ok"
}

fn session_cookie_value(raw_token: &str) -> String {
    format!("{SESSION_COOKIE_NAME}={raw_token}; HttpOnly; Path=/")
}

fn client_ip(headers: &HeaderMap) -> String {
    if let Some(forwarded) = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(first) = forwarded.split(',').next().map(str::trim) {
            if !first.is_empty() {
                return first.to_string();
            }
        }
    }
    if let Some(real_ip) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
        if !real_ip.trim().is_empty() {
            return real_ip.trim().to_string();
        }
    }
    "unknown".to_string()
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
fn rate_key_for(caller: Option<&AuthenticatedCaller>, headers: &HeaderMap) -> String {
    match caller {
        Some(caller) => caller.account.id.clone(),
        None => client_ip(headers),
    }
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

fn pack_notification(note: &Notification, services: &AppServices) -> Value {
    let created_at = services
        .ids
        .parse_time(&note.id)
        .map(format_millis)
        .unwrap_or_else(|_| format_millis(services.clock.now_ms()));
    let kind = serde_json::to_value(&note.kind)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "follow".to_string());
    let user = match services.accounts.find_by_id(&note.notifier_id) {
        Ok(Some(account)) => {
            pack_user_lite(&account, &services.public_url, services.clock.now_ms())
        }
        _ => Value::Null,
    };
    let packed_note = match note.note_id.as_deref() {
        Some(note_id) => match services.notes.get_note(note_id) {
            Ok(Some(target)) => {
                pack_note(&target, Some(&note.notifiee_id), services, 0).unwrap_or(Value::Null)
            }
            _ => Value::Null,
        },
        None => Value::Null,
    };
    serde_json::json!({
        "id": note.id,
        "createdAt": created_at,
        "type": kind,
        "isRead": note.is_read,
        "userId": note.notifier_id,
        "user": user,
        "note": packed_note,
        "reaction": note.reaction.clone().unwrap_or_default(),
    })
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
        .get(&meta.hash)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(ApiError::internal)?;
    let mut response = bytes.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        meta.mime
            .parse::<HeaderValue>()
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    Ok(response)
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

/// Tiny deterministic SVG identicon so avatar URLs always resolve.
async fn identicon(Path(id): Path<String>) -> Response {
    let mut response = identicon_svg(&id).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("image/svg+xml"),
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
    Path(session): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
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
        match state
            .services
            .create_account(&username, Some(&hash_password(&password)))
        {
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
        let found = state
            .services
            .accounts
            .find_by_username_and_host(&username, None)
            .map_err(ApiError::from_port_error)?;
        let found = match found {
            Some(account) => account,
            None => {
                return Ok(miauth_error_page(
                    &session,
                    &fields,
                    "Invalid username or password.",
                ));
            }
        };
        let stored = state
            .services
            .auth
            .get_password_hash(&found.id)
            .map_err(ApiError::from_port_error)?;
        match stored {
            Some(hash) if verify_password(&password, &hash) => found,
            _ => {
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
    Path(session): Path<String>,
) -> Result<Response, ApiError> {
    let consumed = state
        .services
        .auth
        .consume_miauth_session(&session)
        .map_err(ApiError::from_port_error)?;
    match consumed {
        Some(sess) => match (sess.token, sess.account_id) {
            (Some(token), Some(account_id)) => {
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
            _ => Ok(Json(serde_json::json!({ "ok": false })).into_response()),
        },
        None => Ok(Json(serde_json::json!({ "ok": false })).into_response()),
    }
}

// --- Web signup / signin ---------------------------------------------------

fn do_signup(state: &ApiState, obj: &Map<String, Value>) -> Result<(Account, String), ApiError> {
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
    let account = state
        .services
        .create_account(username, Some(&hash_password(password)))
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

fn do_signin(state: &ApiState, obj: &Map<String, Value>) -> Result<(Account, String), ApiError> {
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
    let account = state
        .services
        .accounts
        .find_by_username_and_host(username, None)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(ApiError::authentication_failed)?;
    let stored = state
        .services
        .auth
        .get_password_hash(&account.id)
        .map_err(ApiError::from_port_error)?;
    match stored {
        Some(hash) if verify_password(password, &hash) => {}
        _ => return Err(ApiError::authentication_failed()),
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
    _headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let obj = parse_json_body(&body)?;
    check_allowed_fields(&obj, SIGNUP_FIELDS)?;
    let (account, raw_token) = do_signup(&state, &obj)?;
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
    _headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let obj = parse_json_body(&body)?;
    check_allowed_fields(&obj, &["username", "password"])?;
    let (account, raw_token) = do_signin(&state, &obj)?;
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
async fn drive_files_create(State(state): State<ApiState>, req: Request) -> Result<Response, ApiError> {
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
    let _upload_permit = state.upload_slots.acquire(&client_ip(&headers))?;
    // Slice 6 (H1): streaming running cap — 413 as soon as the cap is
    // crossed, without ever holding more than ~cap bytes. When the token
    // is the multipart `i` field (as misskey-js sends it), this bounded
    // read is the guard; auth still happens below before any write.
    let raw = read_capped_body(body, upload_cap_bytes(&state.services)).await?;

    // Collected upload fields, from multipart or JSON.
    let mut name: Option<String> = None;
    let mut filename: Option<String> = None;
    let mut mime: Option<String> = None;
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
            let field_mime = field.content_type().map(|s| s.to_string());
            let field_bytes = field
                .bytes()
                .await
                .map_err(|err| ApiError::invalid_param("file", &err.to_string()))?;
            match field_name.as_str() {
                "file" => {
                    filename = field_filename;
                    mime = field_mime;
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
            mime,
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
    Path(endpoint): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    tokio::task::spawn_blocking(move || api_dispatch_blocking(state, endpoint, headers, body))
        .await
        .map_err(|_| ApiError::from_port_error(PortError::StorageUnavailable("dispatch task failed".into())))?
}

#[allow(clippy::too_many_lines)]
fn api_dispatch_blocking(
    state: ApiState,
    endpoint: String,
    headers: HeaderMap,
    body: Bytes,
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
    let rate_key = rate_key_for(caller.as_ref(), &headers);

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
            let (account, raw_token) = do_signup(&state, &obj)?;
            let mut packed = pack_me_detailed(&account, true, &state.services)
                .map_err(ApiError::from_port_error)?;
            if let Some(map) = packed.as_object_mut() {
                map.insert("token".to_string(), Value::String(raw_token));
            }
            Ok(Json(packed).into_response())
        }

        "signin" => {
            check_allowed_fields(&obj, &["username", "password"])?;
            let (account, raw_token) = do_signin(&state, &obj)?;
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
            check_allowed_fields(
                &obj,
                &[
                    "name",
                    "description",
                    "location",
                    "birthday",
                    "avatarId",
                    "bannerId",
                    "isLocked",
                    "isBot",
                    "isCat",
                    "isExplorable",
                ],
            )?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:account")?;
            state.rate_limiter.check(&rate_key, "i/update")?;
            let patch = UpdateAccountPatch {
                display_name: update_string_field(&obj, "name")?,
                description: update_string_field(&obj, "description")?,
                location: update_string_field(&obj, "location")?,
                birthday: update_string_field(&obj, "birthday")?,
                avatar_id: update_string_field(&obj, "avatarId")?,
                banner_id: update_string_field(&obj, "bannerId")?,
                is_locked: update_bool_field(&obj, "isLocked")?,
                is_bot: update_bool_field(&obj, "isBot")?,
                is_cat: update_bool_field(&obj, "isCat")?,
                is_explorable: update_bool_field(&obj, "isExplorable")?,
            };
            let account = state
                .services
                .update_profile(&caller.account.id, patch)
                .map_err(ApiError::from_port_error)?;
            state
                .worker
                .drain(64)
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
            if let Some(poll) = obj.get("poll") {
                if !poll.is_null() {
                    return Err(ApiError::invalid_param(
                        "poll",
                        "polls are unsupported in Phase 1",
                    ));
                }
            }
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
                renote_of: opt_string(&obj, "renoteId")?,
                file_ids,
                visible_users: str_list(&obj, "visibleUserIds")?,
                reaction_acceptance: parse_reaction_acceptance(obj.get("reactionAcceptance"))?,
                no_extract_mentions: opt_bool(&obj, "noExtractMentions", false)?,
                no_extract_hashtags: opt_bool(&obj, "noExtractHashtags", false)?,
                no_extract_emojis: opt_bool(&obj, "noExtractEmojis", false)?,
                has_poll: false,
                has_channel: false,
                prohibited_words: Vec::new(),
                sensitive_words: Vec::new(),
                uri: None,
                url: None,
                extra_mentions: Vec::new(),
                extra_tags: Vec::new(),
            };
            let note = state
                .services
                .create_note(input)
                .map_err(ApiError::from_port_error)?;
            state
                .worker
                .drain(64)
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
            state
                .worker
                .drain(64)
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
            state
                .worker
                .drain(64)
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
            state
                .worker
                .drain(64)
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
            state.worker.drain(64).map_err(ApiError::from_port_error)?;
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

        "notes/reactions/create" => {
            check_allowed_fields(&obj, &["noteId", "reaction"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("write:reactions")?;
            let note_id = required_str(&obj, "noteId")?;
            let reaction = required_str(&obj, "reaction")?;
            // Misskey rejects a duplicate reaction on the same note with
            // ALREADY_REACTED, so this endpoint must not upsert.
            state
                .services
                .react(&caller.account.id, &note_id, &reaction, false)
                .map_err(ApiError::from_port_error)?;
            state
                .worker
                .drain(64)
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
            state
                .worker
                .drain(64)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }

        "i/notifications" => {
            check_allowed_fields(&obj, &["limit", "sinceId", "untilId"])?;
            let caller = require_caller(caller)?;
            caller.require_permission("read:account")?;
            let limit = match opt_usize(&obj, "limit")? {
                None => 20,
                Some(limit) if (1..=100).contains(&limit) => limit,
                Some(_) => {
                    return Err(ApiError::invalid_param(
                        "limit",
                        "must be an integer between 1 and 100",
                    ));
                }
            };
            let notifications = state
                .services
                .notifications
                .list_notifications(&caller.account.id, limit)
                .map_err(ApiError::from_port_error)?;
            let packed: Vec<Value> = notifications
                .iter()
                .map(|note| pack_notification(note, &state.services))
                .collect();
            Ok(Json(Value::Array(packed)).into_response())
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
                    let author_uri = author.actor_url(&state.services.public_url);
                    let (visibility, _) = ap_objects::parse_note_audience(
                        &parsed.to,
                        &parsed.cc,
                        Some(&format!("{author_uri}/followers")),
                    );
                    let input = CreateNoteInput {
                        author_id: author.id.clone(),
                        text: parsed.text.clone(),
                        cw: parsed.cw.clone(),
                        visibility,
                        local_only: false,
                        reply_to: None,
                        renote_of: None,
                        file_ids: Vec::new(),
                        visible_users: Vec::new(),
                        reaction_acceptance: None,
                        no_extract_mentions: false,
                        no_extract_hashtags: false,
                        no_extract_emojis: false,
                        has_poll: false,
                        has_channel: false,
                        prohibited_words: Vec::new(),
                        sensitive_words: Vec::new(),
                        uri: Some(parsed.uri.clone()),
                        url: parsed.url.clone(),
                        extra_mentions: Vec::new(),
                        extra_tags: Vec::new(),
                    };
                    // Cache under the note URI as the dedup key so concurrent
                    // fetches converge.
                    match state
                        .services
                        .handle_inbound_create_note(&parsed.uri, input)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE};

    use crate::application::ports::{
        ActivityDedup, BlobStore, NoopStreamingSink, StreamingSink,
    };
    use crate::domain::id::{AidxGenerator, ManualClock};
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
            reactions: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: streaming.clone(),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://example.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
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

    async fn call(
        state: &ApiState,
        endpoint: &str,
        body: Value,
        token: Option<&str>,
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
        let response = miauth_check(State(state.clone()), Path("sess-001".to_string()))
            .await
            .unwrap();
        let (status, value, _) = read_json(response).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["ok"], true);
        assert_eq!(value["token"], app_token);
        assert_eq!(value["user"]["username"], "dave");

        // Second check is spent.
        let response = miauth_check(State(state.clone()), Path("sess-001".to_string()))
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
        let response = miauth_check(State(state.clone()), Path("cbsess".to_string()))
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

        // Bob sees a follow notification (worker drained by the endpoint).
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
        let response = drive_files_create(State(state.clone()), request)
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
        let response = drive_files_create(State(state.clone()), request)
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
        let err = drive_files_create(State(state.clone()), request)
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
        let err = drive_files_create(State(state.clone()), request)
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
            api_dispatch_blocking(state.clone(), "notes/create".to_string(), headers, body)
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
        let _first = state.upload_slots.acquire("unknown").expect("slot 1");
        let _second = state.upload_slots.acquire("unknown").expect("slot 2");
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
        let err = drive_files_create(State(state.clone()), request)
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
    async fn file_named_redirects_and_identicon_serves_svg() {
        let response = serve_file_named(Path(("abc".to_string(), "hi.png".to_string()))).await;
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(response.headers()[LOCATION], "/files/abc");

        let response = identicon(Path("alice".to_string())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "image/svg+xml");
    }
}
