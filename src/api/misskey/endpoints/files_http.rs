//! File serving, identicon, proxy and drive upload.
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
use std::sync::atomic::{AtomicU64, Ordering};
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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
use super::auth_http::{client_ip, decode_base64, is_truthy, is_truthy_value};
/// Shared API state: application services, the durable-job worker, the
/// Misskey rate limiter, the concurrent-upload slots, the live-streaming
/// broadcast hub, and the reverse proxies whose `X-Forwarded-For` is
/// honored ([`client_ip`]).
use super::parse::parse_json_body;

/// Multipart/JSON framing overhead allowed on top of `max_file_size` for
/// `drive/files/create` (1 MiB).
pub const UPLOAD_OVERHEAD_BYTES: usize = 1_048_576;
/// Upload body cap: `max_file_size` + multipart overhead.
pub(crate) fn upload_cap_bytes(services: &AppServices) -> usize {
    usize::try_from(services.max_file_size)
        .unwrap_or(usize::MAX)
        .saturating_add(UPLOAD_OVERHEAD_BYTES)
}
/// Read the whole request body with a streaming running cap: abort with
/// 413 `MAX_FILE_SIZE_EXCEEDED` as soon as the cap is crossed, without
/// ever holding more than ~cap bytes.
pub(crate) async fn read_capped_body(body: Body, cap: usize) -> Result<Bytes, ApiError> {
    Limited::new(body, cap)
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .map_err(|_| ApiError::max_file_size_exceeded())
}
pub(crate) async fn serve_file(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    // Wave 7 Location/id hardening: ids are Misskey `aidx` (alnum, <=32).
    // Anything else (CR/LF, `/`, `%0D`, …) is not a file id — plain 404,
    // never a redirect or a DB lookup with attacker bytes.
    if !is_valid_file_id(&id) {
        return Ok(StatusCode::NOT_FOUND.into_response());
    }
    // Wave 7 hit-path pool: bound separately from the 8 cold slots so a hit
    // flood cannot fill the default blocking pool. Non-blocking 429.
    let _hit_slot = acquire_file_hit_slot()?;
    let id_for_prepare = id.clone();
    // Blocking metadata lookup + optional cold refetch run on the
    // blocking pool; a slow origin never occupies a runtime worker.
    let prepared = {
        let state = state.clone();
        tokio::task::spawn_blocking(move || serve_file_prepare(&state, &id_for_prepare))
            .await
            .map_err(|_| ApiError::internal())?
    };
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(e) if crate::media::remote_cache::is_media_fetch_slot_busy(&e) => {
            return Err(ApiError::rate_limit_exceeded(
                FILE_FETCH_BUSY_RETRY_AFTER_SECS,
            ));
        }
        // Wave 7 cold-GET parity with proxy: origin validation (dead
        // origin, SSRF, oversize, disabled, budget) is a plain 404, not a
        // 500. Unexpected store faults stay 500 via the fallthrough.
        Err(PortError::Validation(_)) => {
            return Ok(StatusCode::NOT_FOUND.into_response());
        }
        Err(PortError::NotFound(msg)) => {
            return Err(ApiError::from_port_error(PortError::NotFound(msg)));
        }
        Err(e) => return Err(ApiError::from_port_error(e)),
    };
    // The blob open also blocks (stat + sidecar check); the response then
    // streams without ever buffering the whole file.
    let blob_key_first = prepared.blob_key.clone();
    let cached_first = prepared.cached;
    let mime_first = prepared.mime.clone();
    let open_once = {
        let state = state.clone();
        tokio::task::spawn_blocking(move || {
            open_served_blob(&state.services, &blob_key_first, cached_first)
        })
        .await
        .map_err(|_| ApiError::internal())?
    };
    let (mime, blob) = match open_once {
        Ok(blob) => (mime_first, blob),
        // Wave 7 hit-to-open TOCTOU: eviction between `byte_len` and
        // `open_reader` is a routine race, never a 500. Cached rows
        // re-enter the refetch path once; durable rows and a second miss
        // are a plain 404.
        Err(PortError::NotFound(_)) if prepared.cached => {
            let id_for_retry = id.clone();
            let retried = {
                let state = state.clone();
                tokio::task::spawn_blocking(move || serve_file_prepare(&state, &id_for_retry))
                    .await
                    .map_err(|_| ApiError::internal())?
            };
            let retried = match retried {
                Ok(retried) => retried,
                Err(e) if crate::media::remote_cache::is_media_fetch_slot_busy(&e) => {
                    return Err(ApiError::rate_limit_exceeded(
                        FILE_FETCH_BUSY_RETRY_AFTER_SECS,
                    ));
                }
                Err(PortError::Validation(_)) => {
                    return Ok(StatusCode::NOT_FOUND.into_response());
                }
                Err(PortError::NotFound(msg)) => {
                    return Err(ApiError::from_port_error(PortError::NotFound(msg)));
                }
                Err(e) => return Err(ApiError::from_port_error(e)),
            };
            let blob_key_second = retried.blob_key.clone();
            let cached_second = retried.cached;
            let mime_second = retried.mime.clone();
            let reopened = {
                let state = state.clone();
                tokio::task::spawn_blocking(move || {
                    open_served_blob(&state.services, &blob_key_second, cached_second)
                })
                .await
                .map_err(|_| ApiError::internal())?
            };
            match reopened {
                Ok(blob) => (mime_second, blob),
                // Second miss after a refetch: the bytes vanished again
                // (racing evict/delete). Plain 404, never 500.
                Err(PortError::NotFound(_)) => {
                    return Ok(StatusCode::NOT_FOUND.into_response());
                }
                Err(e) if crate::media::remote_cache::is_media_fetch_slot_busy(&e) => {
                    return Err(ApiError::rate_limit_exceeded(
                        FILE_FETCH_BUSY_RETRY_AFTER_SECS,
                    ));
                }
                Err(PortError::Validation(_)) => {
                    return Ok(StatusCode::NOT_FOUND.into_response());
                }
                Err(e) => return Err(ApiError::from_port_error(e)),
            }
        }
        // Durable TOCTOU (local upload deleted between prepare and open):
        // plain 404, never 500.
        Err(PortError::NotFound(_)) => {
            return Ok(StatusCode::NOT_FOUND.into_response());
        }
        Err(e) if crate::media::remote_cache::is_media_fetch_slot_busy(&e) => {
            return Err(ApiError::rate_limit_exceeded(
                FILE_FETCH_BUSY_RETRY_AFTER_SECS,
            ));
        }
        Err(PortError::Validation(_)) => {
            return Ok(StatusCode::NOT_FOUND.into_response());
        }
        Err(e) => return Err(ApiError::from_port_error(e)),
    };
    Ok(file_response(&mime, blob))
}
/// Canonical cold-fetch bound lives in
/// `crate::media::remote_cache` (Wave 7: cold `GET /files`, `GET /proxy`,
/// and background job fetches share one semaphore). This alias keeps the
/// adversarial-2 name stable for callers/docs.
pub const MAX_CONCURRENT_FILE_FETCHES: usize =
    crate::media::remote_cache::MAX_CONCURRENT_MEDIA_FETCHES;
pub const FILE_FETCH_BUSY_RETRY_AFTER_SECS: u64 = 5;

/// Wave 7 hit-path pool: cache/durable hits still enqueue blocking tasks,
/// so bound them separately from the 8 cold slots. A hit is a stat + open
/// (no network), hence the larger bound; exhaustion is still non-blocking
/// 429 with `Retry-After` so a hit flood sheds load instead of filling the
/// default blocking pool (512 threads).
pub const MAX_CONCURRENT_FILE_HIT_OPENS: usize = 32;
pub const FILE_HIT_BUSY_RETRY_AFTER_SECS: u64 = 5;

fn file_hit_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_FILE_HIT_OPENS)))
}

/// Non-blocking take of one hit-open slot. `Err` is retryable 429.
pub(crate) fn acquire_file_hit_slot() -> Result<OwnedSemaphorePermit, ApiError> {
    match file_hit_slots().clone().try_acquire_owned() {
        Ok(permit) => Ok(permit),
        Err(_) => {
            eprintln!(
                "file hit slots exhausted (all {MAX_CONCURRENT_FILE_HIT_OPENS} held); failing closed 429"
            );
            Err(ApiError::rate_limit_exceeded(
                FILE_HIT_BUSY_RETRY_AFTER_SECS,
            ))
        }
    }
}

/// Bound concurrent cold-cache refetches and proxy origin fetches
/// (adversarial-2 slice 3, findings 3 + D; Wave 7 canonical semaphore in
/// `crate::media::remote_cache::fetch`): a slow origin must neither occupy
/// a Tokio worker (see `serve_file`/`proxy_media`) nor stack unbounded
/// blocking fetches. The acquire is non-blocking: exhaustion answers
/// Misskey-shaped 429 `RATE_LIMIT_EXCEEDED` with a short `Retry-After`
/// hint so clients back off instead of hot-looping. `/files` refetch and
/// `/proxy` share the one limit so neither surface can starve the other.
/// Background `fetch_and_index_remote_media` calls hold the same semaphore
/// inside the fetch core (hits return before acquiring), so a mention flood
/// defers/retries instead of stacking blocking fetches.
///
/// Slot pinning: a parked cold fetch holds its slot plus a full
/// `max_file_size` in-flight reservation for the whole origin timeout
/// (plain-HTTP `total_timeout`). Eight stalled origins pin
/// `8 * max_file_size` phantom plus eight blocking threads; the fetch core
/// logs exhaustion (`media fetch slots exhausted`) rather than queueing.
pub(crate) fn acquire_file_fetch_slot() -> Result<OwnedSemaphorePermit, ApiError> {
    crate::media::remote_cache::try_acquire_media_fetch_slot()
        .map_err(|_| ApiError::rate_limit_exceeded(FILE_FETCH_BUSY_RETRY_AFTER_SECS))
}

/// Wave 7 id gate: Misskey file ids are `aidx` (lowercase alnum, 16 chars;
/// accepted here as Misskey-id alnum `<=32` for forward compat). Rejects
/// CR, LF, and every non-id character (including `/`, `?`, `#`, `%`,
/// whitespace, controls) before any DB lookup or `Location` interpolation.
fn is_valid_file_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 32 {
        return false;
    }
    if id.bytes().any(|b| b == b'\r' || b == b'\n') {
        return false;
    }
    crate::domain::id::is_valid_misskey_id(id)
}

/// Blocking half of `serve_file`: metadata lookup, LRU touch on hits, and
/// the cold refetch (slot-bounded inside the fetch core). Runs on the
/// blocking pool, never on a Tokio worker. Returns `PortError` so the
/// async wrapper can map `Validation` (dead origin, SSRF, oversize,
/// disabled, budget) to plain 404 while unexpected store faults stay 500.
struct PreparedFile {
    mime: String,
    blob_key: String,
    cached: bool,
}

fn serve_file_prepare(state: &ApiState, id: &str) -> Result<PreparedFile, PortError> {
    let meta = state
        .services
        .media
        .get(id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_FILE".to_string()))?;
    // Durable pool for local uploads, cache pool for remote rows. A remote
    // row whose bytes are absent (evicted) re-fetches on first request when
    // the cache mode allows it; metadata + source_url are always retained.
    let cached = meta.owner_id.is_none();
    let pool: &Arc<dyn BlobStore> = if cached {
        &state.services.cache_blobs
    } else {
        &state.services.blobs
    };
    if pool.byte_len(&meta.blob_key)?.is_some() {
        // Hit: advance the LRU + hit diagnostics before serving (finding
        // 5). No touch on a miss that failed (see the fallthrough below).
        // Wave 7: a failed touch must not fail the hit — serve the bytes
        // and log the touch error.
        if cached {
            if let Some(url) = meta.source_url.clone() {
                if let Err(e) = state.services.media.remote_cache_touch(
                    &url,
                    &meta.variant,
                    state.services.clock.now_ms(),
                ) {
                    eprintln!("remote_cache_touch failed for hit {id}: {e:?}; serving anyway");
                }
            }
        }
        return Ok(PreparedFile {
            mime: meta.mime,
            blob_key: meta.blob_key,
            cached,
        });
    }
    if cached {
        if let Some(url) = meta.source_url.clone() {
            if state.services.remote_cache_mode == crate::config::RemoteCacheMode::Disabled {
                return Err(PortError::Validation("REMOTE_CACHE_DISABLED".to_string()));
            }
            // No outer slot here: the fetch core holds the shared
            // cold-fetch slot for the whole miss path (fail-closed 429
            // before any reservation exists, so nothing leaks).
            match crate::media::remote_cache::fetch_on_first_request(
                &state.services,
                &url,
                &meta.variant,
            ) {
                Ok(refreshed) => {
                    // Refreshed metadata wins: `Content-Type` and the
                    // security headers come from the new bytes, not
                    // the pre-fetch row (finding 3).
                    return Ok(PreparedFile {
                        mime: refreshed.mime,
                        blob_key: refreshed.blob_key,
                        cached: true,
                    });
                }
                Err(e) => return Err(e),
            }
        }
    }
    Err(PortError::NotFound("NO_SUCH_FILE".to_string()))
}

/// Blob bytes opened off the worker: a streaming handle when the pool
/// offers one, otherwise (in-memory doubles) bytes read off the worker.
enum ServedBlob {
    Stream(std::fs::File),
    Buffered(Vec<u8>),
}

fn open_served_blob(
    services: &AppServices,
    blob_key: &str,
    cached: bool,
) -> Result<ServedBlob, PortError> {
    let pool: &Arc<dyn BlobStore> = if cached {
        &services.cache_blobs
    } else {
        &services.blobs
    };
    match pool.open_reader(blob_key)? {
        Some(file) => Ok(ServedBlob::Stream(file)),
        None => match pool.get(blob_key)? {
            Some(bytes) => Ok(ServedBlob::Buffered(bytes)),
            // Wave 7 TOCTOU: bytes vanished between `byte_len` and open
            // (racing evict/delete). Plain 404 via the caller, never 500.
            None => Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        },
    }
}

/// Response over already-resolved bytes: streaming chunks when the pool
/// opened a handle, so a large file never sits whole in memory; headers
/// from the resolved (post-refetch) MIME.
fn file_response(mime: &str, blob: ServedBlob) -> Response {
    let body = match blob {
        ServedBlob::Buffered(bytes) => Body::from(bytes),
        ServedBlob::Stream(std_file) => {
            // Channel-backed stream: a reader task pulls 64 KiB chunks
            // through the Tokio file (never blocking a worker) with a
            // 4-chunk backpressure window. A mid-stream read error aborts
            // the body rather than truncating silently.
            let (mut sender, receiver) =
                http_body_util::channel::Channel::<Bytes, std::io::Error>::new(4);
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt as _;
                let mut file = tokio::fs::File::from_std(std_file);
                loop {
                    let mut chunk = vec![0u8; 65536];
                    match file.read(&mut chunk).await {
                        Ok(0) => break,
                        Ok(n) => {
                            chunk.truncate(n);
                            if sender.send_data(Bytes::from(chunk)).await.is_err() {
                                break;
                            }
                        }
                        Err(error) => {
                            sender.abort(error);
                            break;
                        }
                    }
                }
            });
            Body::new(receiver)
        }
    };
    let mut response = body.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        mime.parse::<HeaderValue>()
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    // Wave 7: short anonymous cache so repeat image loads do not cost a
    // server hit (and its per-hit write txn) every time. Touch/LRU semantics
    // are unchanged: every server hit still touches + bumps generation
    // (`file_hit_touches_cache_record`); this only lets clients/CDNs avoid
    // re-requesting for 60 s. No Range support (plan §6: full re-download).
    response.headers_mut().insert(
        HeaderName::from_static("cache-control"),
        HeaderValue::from_static("public, max-age=60"),
    );
    apply_media_security_headers(&mut response, mime);
    response
}
/// Inline-safe MIME set (slice 7, H2): sniffed `image/*` (except SVG —
/// the sniffer never emits it, and legacy `image/svg+xml` rows must not
/// render inline), `audio/*` and `video/*` render inline. Everything else —
/// `application/octet-stream`, `text/plain`, and any legacy row outside the
/// allowlist (e.g. stored `application/pdf`) — is served as an attachment
/// so it can never become active content in the site origin.
pub(crate) fn is_inline_safe_mime(mime: &str) -> bool {
    (mime.starts_with("image/") && mime != "image/svg+xml" && !mime.ends_with("+xml"))
        || mime.starts_with("audio/")
        || mime.starts_with("video/")
}
/// Slice 7 (H2): every served file carries `nosniff` plus a
/// `default-src 'none'; sandbox` CSP, and non-inline-safe files carry
/// `Content-Disposition: attachment`.
pub(crate) fn apply_media_security_headers(response: &mut Response, mime: &str) {
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'; sandbox"),
    );
    if !is_inline_safe_mime(mime) {
        response
            .headers_mut()
            .insert(CONTENT_DISPOSITION, HeaderValue::from_static("attachment"));
    }
}
/// Misskey `FileServerService.ts:93-99` redirects `/files/{id}/{name}` to
/// `/files/{id}`. Wave 7: `id` is validated before reflecting into
/// `Location` — CR, LF, and non-id characters are a plain 404, never a
/// header split. Valid ids are alnum only, so no percent-encoding is needed.
pub(crate) async fn serve_file_named(Path((id, _name)): Path<(String, String)>) -> Response {
    if !is_valid_file_id(&id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    (
        StatusCode::MOVED_PERMANENTLY,
        [(LOCATION, format!("/files/{id}"))],
        "",
    )
        .into_response()
}
/// Proxy origin body cap (unchanged): 8 MiB.
const PROXY_BODY_CAP_BYTES: usize = 8_388_608;
/// Wave 7: proxy URLs are capped at 1024 chars, same as the fetch core
/// (`validate_remote_media_url`). Longer URLs are a plain 404 collapsed
/// with every other proxy refusal (oracle §2.9), never a distinct status.
const PROXY_URL_MAX_CHARS: usize = 1024;

/// Blocking proxy-fetch outcome: slot exhaustion fails closed retryable,
/// origin failure and wrong-type collapse to the same plain 404 (oracle
/// fix, Wave 7), local disk failure stays 500.
enum ProxyFetchError {
    Busy,
    Origin,
    Server,
}

static PROXY_TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Spill a fetched proxy body to a temp file (called on the blocking pool,
/// under the cold-fetch slot, with the 8 MiB cap already enforced by the
/// fetch). The caller streams the file and deletes it afterwards, so the
/// 8 MiB never sits in the response for a slow client.
fn proxy_spill_body(body: &[u8]) -> std::io::Result<PathBuf> {
    let dir = std::env::temp_dir().join("pinetree-proxy");
    std::fs::create_dir_all(&dir)?;
    let seq = PROXY_TMP_SEQ.fetch_add(1, Ordering::SeqCst);
    let path = dir.join(format!("proxy.{}.{seq}.tmp", std::process::id()));
    std::fs::write(&path, body)?;
    Ok(path)
}

/// Stream a spilled proxy file with the same chunk/backpressure shape as
/// `file_response`. Deletes the temp file when streaming ends (EOF, error,
/// or client disconnect).
fn proxy_file_response(mime: &str, tmp_path: PathBuf) -> Response {
    let std_file = match std::fs::File::open(&tmp_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("proxy temp open failed: {e:?}");
            let _ = std::fs::remove_file(&tmp_path);
            return ApiError::internal().into_response();
        }
    };
    let (mut sender, receiver) = http_body_util::channel::Channel::<Bytes, std::io::Error>::new(4);
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt as _;
        let mut file = tokio::fs::File::from_std(std_file);
        loop {
            let mut chunk = vec![0u8; 65536];
            match file.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => {
                    chunk.truncate(n);
                    if sender.send_data(Bytes::from(chunk)).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    sender.abort(error);
                    break;
                }
            }
        }
        let _ = tokio::fs::remove_file(&tmp_path).await;
    });
    let mut response = Body::new(receiver).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        mime.parse::<HeaderValue>()
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    response.headers_mut().insert(
        HeaderName::from_static("cache-control"),
        HeaderValue::from_static("max-age=31536000, immutable"),
    );
    apply_media_security_headers(&mut response, mime);
    response
}

/// Batch Q Phase 1 (item 9): `GET /proxy`, the route `meta.mediaProxy`
/// (`{uri}/proxy`) advertises. Upstream shape is `/proxy/:url*` with a
/// `?url=` override (`FileServerService.ts:106`,
/// `FileServerProxyHandler.ts`); the cosmetic path segment is accepted and
/// ignored, `?url=` wins. No auth: clients embed proxied URLs in `<img>`
/// tags. The abuse surface is bounded by the SSRF-guarded federation HTTP
/// client (private/reserved IPs refused), an 8 MiB body cap, and an
/// inline-safe media allowlist.
/// DIVERGENCE: no thumbnailing/transcoding and no `emoji`/`avatar`/`static`
/// variants yet (media pipeline workstream); fetch failure, non-2xx, and
/// wrong-type all collapse to the same plain 404 (no `dummy.png` fallback
/// asset; Wave 7 oracle fix — SSRF-blocked and wrong-type are
/// indistinguishable); SVG and non-media are refused rather than proxied.
/// No Range support (same as `/files`).
pub(crate) async fn proxy_media(
    State(state): State<ApiState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    // Upstream `FileServerProxyHandler.ts` requires a User-Agent and
    // refuses to proxy another proxy (loop protection).
    let user_agent = headers
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if user_agent.is_empty() {
        return ApiError::invalid_param("url", "User-Agent is required").into_response();
    }
    if user_agent.to_ascii_lowercase().contains("misskey/") {
        return (
            StatusCode::FORBIDDEN,
            "refusing to proxy a request from another proxy",
        )
            .into_response();
    }
    let url = match params.get("url") {
        Some(url) => url,
        None => {
            return ApiError::invalid_param("url", "url query parameter is required")
                .into_response();
        }
    };
    // Wave 7 URL cap (same 1024 chars as the fetch core). Longer URLs are a
    // plain 404 collapsed with every other refusal, so length is not an
    // oracle either.
    if url.chars().count() > PROXY_URL_MAX_CHARS {
        return StatusCode::NOT_FOUND.into_response();
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return ApiError::invalid_param("url", "only http(s) URLs can be proxied").into_response();
    }
    // Origin fetch + MIME gate + spill to disk, all off the worker under
    // the shared cold-fetch slot (finding D): the 8 MiB cap stays,
    // concurrent proxy fetches share the slot limit with `/files`
    // refetches and background job fetches, and the body never sits in
    // the response for a slow client. Residual: `SafeHttpClient` still
    // buffers up to the cap (chunked holds encoded + decoded transiently,
    // ~16 MiB worst case in `framing.rs`); end-to-end streaming decode
    // needs a client transport change (federation-owned, out of scope).
    let proxy_url = url.clone();
    let proxy_client = state.services.federation_http.clone();
    let fetched = match tokio::task::spawn_blocking(move || {
        let _slot = acquire_file_fetch_slot().map_err(|_| ProxyFetchError::Busy)?;
        let resp = proxy_client
            .send("GET", &proxy_url, &[], None, PROXY_BODY_CAP_BYTES)
            .map_err(|_| ProxyFetchError::Origin)?;
        if !(200..300).contains(&resp.status) {
            return Err(ProxyFetchError::Origin);
        }
        let mime = resp
            .header("content-type")
            .unwrap_or("application/octet-stream")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        // Inline-safe media only (same set as served files, so the
        // response can never become active content in the site origin).
        // Wave 7 oracle: wrong-type is the same plain 404 as a dead or
        // SSRF-blocked origin.
        if !is_inline_safe_mime(&mime) {
            return Err(ProxyFetchError::Origin);
        }
        let tmp_path = proxy_spill_body(&resp.body).map_err(|_| ProxyFetchError::Server)?;
        Ok((mime, tmp_path))
    })
    .await
    {
        Err(_) => return ApiError::internal().into_response(),
        Ok(Err(ProxyFetchError::Busy)) => {
            return ApiError::rate_limit_exceeded(FILE_FETCH_BUSY_RETRY_AFTER_SECS).into_response();
        }
        Ok(Err(ProxyFetchError::Origin)) => return StatusCode::NOT_FOUND.into_response(),
        Ok(Err(ProxyFetchError::Server)) => return ApiError::internal().into_response(),
        Ok(Ok((mime, tmp_path))) => (mime, tmp_path),
    };
    proxy_file_response(&fetched.0, fetched.1)
}
/// Tiny deterministic SVG identicon so avatar URLs always resolve. The
/// bytes are fully server-generated (SHA-256-derived hex colors and
/// coordinates; no user input, no script elements), and the response carries
/// the same sandboxed CSP as served files, so even a direct navigation
/// cannot execute script.
pub(crate) async fn identicon(Path(id): Path<String>) -> Response {
    let mut response = identicon_svg(&id).into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("image/svg+xml"));
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
pub(crate) fn identicon_svg(id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(id.as_bytes());
    let color = format!("#{:02x}{:02x}{:02x}", digest[0], digest[1], digest[2]);
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

#[cfg(test)]
mod wave7_id_tests {
    use super::*;

    #[test]
    fn wave7_file_id_gate_accepts_aidx_and_rejects_injection() {
        // Valid lowercase aidx passes through to the DB lookup.
        assert!(is_valid_file_id("0123456789abcdef"));
        // Uppercase alnum is accepted (forward-compat Misskey ids).
        assert!(is_valid_file_id("ABCDEF123456"));
        // CR/LF, separators, traversal, whitespace, and overlong ids are
        // rejected before any lookup or `Location` interpolation.
        for bad in [
            "",
            "abc\r\n",
            "abc\n",
            "abc\r",
            "ab!cd",
            "ab/cd",
            "ab?cd",
            "ab#cd",
            "ab%cd",
            "ab cd",
            "..",
            "../etc",
            "a+b",
            "0123456789abcdef0123456789abcdefX",
        ] {
            assert!(!is_valid_file_id(bad), "must reject {bad:?}");
        }
    }
}

// --- MiAuth web flow -------------------------------------------------------
#[allow(clippy::too_many_lines)]
pub(crate) async fn drive_files_create(
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
    let _upload_permit =
        state
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
                    is_sensitive = is_truthy(Some(String::from_utf8_lossy(&field_bytes).trim()));
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
        name = obj
            .get("name")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
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

    let bytes =
        data.ok_or_else(|| ApiError::invalid_param("file", "an upload file is required"))?;
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
