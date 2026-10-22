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
    let meta = state
        .services
        .media
        .get(&id)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_FILE".to_string()))
        })?;
    // Durable pool for local uploads, cache pool for remote rows. A remote
    // row whose bytes are absent (evicted) re-fetches on first request when
    // the cache mode allows it; metadata + source_url are always retained.
    let cached = meta.owner_id.is_none();
    let pool: &Arc<dyn BlobStore> = if cached {
        &state.services.cache_blobs
    } else {
        &state.services.blobs
    };
    let mut bytes = pool
        .get(&meta.blob_key)
        .map_err(ApiError::from_port_error)?;
    if bytes.is_none() && cached {
        if let Some(url) = meta.source_url.clone() {
            if state.services.remote_cache_mode != crate::config::RemoteCacheMode::Disabled {
                match crate::media::remote_cache::fetch_on_first_request(
                    &state.services,
                    &url,
                    &meta.variant,
                ) {
                    Ok(refreshed) => {
                        bytes = state
                            .services
                            .cache_blobs
                            .get(&refreshed.blob_key)
                            .map_err(ApiError::from_port_error)?;
                    }
                    Err(PortError::Validation(_)) => {}
                    Err(e) => return Err(ApiError::from_port_error(e)),
                }
            }
        }
    }
    let bytes = bytes.ok_or_else(ApiError::internal)?;
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
/// `/files/{id}`.
pub(crate) async fn serve_file_named(Path((id, _name)): Path<(String, String)>) -> Response {
    (
        StatusCode::MOVED_PERMANENTLY,
        [(LOCATION, format!("/files/{id}"))],
        "",
    )
        .into_response()
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
/// variants yet (media pipeline workstream); fetch failure or non-2xx is a
/// plain 404 (no `dummy.png` fallback asset); SVG and non-media are refused
/// rather than proxied.
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
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return ApiError::invalid_param("url", "only http(s) URLs can be proxied").into_response();
    }
    let fetched = match state
        .services
        .federation_http
        .send("GET", url, &[], None, 8_388_608)
    {
        Ok(resp) => resp,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    if !(200..300).contains(&fetched.status) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let mime = fetched
        .header("content-type")
        .unwrap_or("application/octet-stream")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    // Inline-safe media only (same set as served files, so the response
    // can never become active content in the site origin).
    if !is_inline_safe_mime(&mime) {
        return ApiError::invalid_param("url", "only image, audio, and video media can be proxied")
            .into_response();
    }
    let mut response = fetched.body.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        mime.parse::<HeaderValue>()
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    response.headers_mut().insert(
        HeaderName::from_static("cache-control"),
        HeaderValue::from_static("max-age=31536000, immutable"),
    );
    apply_media_security_headers(&mut response, &mime);
    response
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
