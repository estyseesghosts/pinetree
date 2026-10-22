//! Axum middleware, health/ready probes and ops slots.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]
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
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

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

use crate::activitypub::federated_collections as ap_collections;
use crate::activitypub::{actor as ap_actor, objects as ap_objects, webfinger as ap_webfinger};
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
use crate::streaming::hub::{Hub, ONLINE_THRESHOLD_MS};
use crate::streaming::server::streaming_handler;

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
use crate::services::email::EmailError;
use crate::services::email::EmailMessage;
use crate::services::email::EmailSendPayload;
use crate::services::email::EmailSender;
use crate::services::email::SmtpConfig;
use crate::services::email::SmtpEmailSender;
use crate::services::email::password_reset_email;
use crate::services::email::verification_email;

use super::ApiState;
/// Shared API state: application services, the durable-job worker, the
/// Misskey rate limiter, the concurrent-upload slots, the live-streaming
/// broadcast hub, and the reverse proxies whose `X-Forwarded-For` is
/// honored ([`client_ip`]).

/// Slice 7 (H2): global `X-Content-Type-Options: nosniff` for all
/// responses, so a stored file can never be sniffed into active content.
pub(crate) async fn nosniff_middleware(req: Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(req).await;
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response
}
/// CORS simple-response support (Misskey `@fastify/cors` `origin: '*'`
/// parity, `HANDOFF_auth-signin.md` §4). Every response carries
/// `Access-Control-Allow-Origin: *`. Upstream's per-route
/// `Access-Control-Allow-Origin: <config.url>` + `Allow-Credentials` on
/// the signin routes is not copied: the frontend sends
/// `credentials: 'omit'` (`misskey-api.ts`), so `*` without credentials
/// is the working subset for split deployments.
pub(crate) async fn cors_middleware(req: Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(req).await;
    response.headers_mut().insert(
        HeaderName::from_static("access-control-allow-origin"),
        HeaderValue::from_static("*"),
    );
    response
}
/// Batch H request correlation (H3): one uuid per request, echoed in
/// `x-request-id` and in the `http.request.start/done` structured lines
/// (method + path only — never query, headers, or bodies). The timer
/// covers the inner stack; status + duration make slow endpoints visible
/// without touching handler code.
pub(crate) async fn request_id_middleware(req: Request, next: axum::middleware::Next) -> Response {
    // One structured line per request at info level (id + duration); the
    // debug `http.request.start` helper exists for deep dives but stays
    // off the normal path so steady-state volume is one line per request.
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let request_id = crate::observability::new_request_id();
    let started = std::time::Instant::now();
    let mut response = next.run(req).await;
    // The request id is a locally generated uuid (alphanumeric plus `-`),
    // so header construction cannot fail; the `unknown` fallback only
    // exists so request correlation never panics, even in theory.
    response.headers_mut().insert(
        HeaderName::from_static("x-request-id"),
        HeaderValue::from_str(&request_id).unwrap_or_else(|_| HeaderValue::from_static("unknown")),
    );
    crate::observability::log_request_done(
        &request_id,
        &method,
        &path,
        response.status().as_u16(),
        started.elapsed().as_millis() as u64,
    );
    response
}
/// Answer a CORS preflight: 204 with `*` origin and the methods/headers
/// the JSON API + web auth routes accept.
pub(crate) async fn preflight() -> Response {
    (
        StatusCode::NO_CONTENT,
        [
            (
                HeaderName::from_static("access-control-allow-origin"),
                HeaderValue::from_static("*"),
            ),
            (
                HeaderName::from_static("access-control-allow-methods"),
                HeaderValue::from_static("GET, POST, OPTIONS"),
            ),
            (
                HeaderName::from_static("access-control-allow-headers"),
                HeaderValue::from_static("Content-Type, Authorization"),
            ),
        ],
    )
        .into_response()
}

// --- Small helpers ---------------------------------------------------------
pub(crate) async fn healthz() -> &'static str {
    "ok"
}
/// Readiness probe: the durable store can serve a read AND the critical
/// storage roots are present.
///
/// The liveness probe (`/healthz`) answers as long as the process is up.
/// Readiness additionally proves storage is readable, so an orchestrator
/// stops routing traffic to a node whose database has gone bad instead of
/// serving 500s. Checks are bounded and cheap: one single-row redb read
/// (`instance_settings`, the same read path the API uses) plus
/// existence/is-dir checks on the durable + cache pool roots captured at
/// boot. No table walks, no integrity checks, no network probes. Bodies
/// stay tiny (`ready` / `not ready`); detail lives behind the admin
/// status surface.
pub(crate) async fn ready(State(state): State<ApiState>) -> (StatusCode, &'static str) {
    match readiness_check(&state) {
        Ok(()) => {
            record_readiness(true, "ok");
            (StatusCode::OK, "ready")
        }
        Err(class) => {
            record_readiness(false, class);
            crate::observability::record_error("readiness.probe");
            crate::observability::log_event(
                crate::observability::LogLevel::Warn,
                "readiness.probe_failed",
                &[("error_class", class)],
            );
            (StatusCode::SERVICE_UNAVAILABLE, "not ready")
        }
    }
}
/// One bounded readiness evaluation. Returns `Ok` only when the redb read
/// path works and every configured pool root is a directory. Error classes
/// are stable strings (safe for status surfaces and logs).
pub(crate) fn readiness_check(state: &ApiState) -> Result<(), &'static str> {
    if state.services.instance_settings.get().is_err() {
        return Err("storage-unavailable");
    }
    let ops = read_ops_config();
    for root in [&ops.durable_dir, &ops.cache_dir] {
        if root.as_os_str().is_empty() {
            continue;
        }
        if !root.is_dir() {
            return Err("storage-root-missing");
        }
    }
    Ok(())
}

// --- Batch H ops state ------------------------------------------------------
// Process-wide operations wiring (`prod-ready.md` §9 H1/H4/H5), published
// once by `main` at boot and read by the probes + admin status arms.
//
// `ApiState` is intentionally untouched: integration tests construct it
// literally, so new required fields would break every test harness.
// These `Mutex` cells default to safe, honest `unknown` values, which is
// exactly what hermetic tests assert until `main` (or a test) publishes
// real roots.
/// Boot-published operations context for probes + admin status. All fields
/// are non-secret by construction (booleans, counts, host class, version
/// strings); raw paths stay inside `main`, only presence booleans leave.
#[derive(Debug, Clone)]
pub struct OpsConfig {
    /// `diagnostics.detailed_admin_status` file value.
    pub detailed_admin_status: bool,
    /// Durable pool root (presence-checked by `/ready`).
    pub durable_dir: PathBuf,
    /// Cache pool root (presence-checked by `/ready`).
    pub cache_dir: PathBuf,
    /// Backup bundle + status directory (admin backup routes read it).
    pub backup_dir: PathBuf,
    /// `transcode.enabled` file value.
    pub transcode_enabled: bool,
    /// Validated toolchain line (`ffmpeg version ...`), `"disabled"`, or
    /// `"unavailable: <redacted cause>"` — never a binary dump.
    pub ffmpeg_status: String,
    /// [`crate::config::Config::config_summary`] snapshot (no secrets).
    pub config_fingerprint: serde_json::Value,
    /// Boot wall-clock millis (uptime base).
    pub started_ms: i64,
}
impl Default for OpsConfig {
    fn default() -> Self {
        Self {
            detailed_admin_status: false,
            durable_dir: PathBuf::new(),
            cache_dir: PathBuf::new(),
            backup_dir: PathBuf::new(),
            transcode_enabled: false,
            ffmpeg_status: "unknown".to_string(),
            config_fingerprint: serde_json::Value::Null,
            started_ms: 0,
        }
    }
}

pub(crate) fn ops_state() -> &'static Mutex<OpsConfig> {
    static OPS: OnceLock<Mutex<OpsConfig>> = OnceLock::new();
    OPS.get_or_init(|| Mutex::new(OpsConfig::default()))
}
/// Publish the boot operations context. Called once by `main` before
/// serving (and by hermetic tests to stage roots/flags).
pub fn set_ops_config(config: OpsConfig) {
    *ops_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = config;
}
pub(crate) fn read_ops_config() -> OpsConfig {
    ops_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}
/// Boot wall-clock millis for uptime math.
pub fn ops_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
pub(crate) fn backup_slot() -> &'static Mutex<Option<crate::backup::BackupContext>> {
    static SLOT: OnceLock<Mutex<Option<crate::backup::BackupContext>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}
/// Publish the live backup context (store handle + pool roots + backup
/// dir). Called once by `main`; unset means backup routes answer 503.
pub fn set_backup_context(context: crate::backup::BackupContext) {
    *backup_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(context);
}
pub(crate) fn read_backup_context() -> Option<crate::backup::BackupContext> {
    backup_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}
/// Last readiness outcome for the admin status surface: `(ok, error_class,
/// checked_ms)`. Updated by every `/ready` evaluation.
#[derive(Debug, Clone)]
pub(crate) struct ReadinessProbe {
    ok: bool,
    error_class: String,
    checked_ms: u64,
}
pub(crate) fn readiness_slot() -> &'static Mutex<ReadinessProbe> {
    static SLOT: OnceLock<Mutex<ReadinessProbe>> = OnceLock::new();
    SLOT.get_or_init(|| {
        Mutex::new(ReadinessProbe {
            ok: true,
            error_class: "ok".to_string(),
            checked_ms: 0,
        })
    })
}
pub(crate) fn record_readiness(ok: bool, error_class: &str) {
    *readiness_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = ReadinessProbe {
        ok,
        error_class: error_class.to_string(),
        checked_ms: ops_now_ms(),
    };
}
/// Last readiness outcome (`ok`, stable error class). Starts optimistic;
/// every `/ready` evaluation overwrites it.
pub fn last_readiness() -> (bool, String) {
    let probe = readiness_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    (probe.ok, probe.error_class.clone())
}

// --- Batch H admin status builders -------------------------------------------
// Authenticated admin surface (`prod-ready.md` §9 H4/H5). Every value comes
// from a maintained counter or a single bounded read — never a full table
// walk, `check_integrity`, recursive hash, or network fetch per request.
// Unavailable values are explicit `null` (stale/unknown is data, not an
// error here); lists are capped (response size limit); expensive detail
// stays behind `diagnostics.detailed_admin_status` (time limit by
// construction: nothing here can run for minutes).
