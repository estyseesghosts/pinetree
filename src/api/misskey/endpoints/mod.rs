//! Misskey HTTP API surface (Slice D3).
//!
//! [`ApiState`] carries the application services, the in-process job
//! worker, and the rate limiter. [`build_router`] wires every Phase 1
//! route: `/healthz`, media/identicon file serving, the MiAuth web flow
//! (`GET`/`POST /miauth/{session}` plus the one-shot `check`
//! endpoints), web `signup`/`signin`, `drive/files/create` (multipart and
//! JSON), and the `POST /api/{endpoint}` JSON dispatcher.

#![allow(unused_imports)]

mod admin_status;
mod auth_http;
mod dispatch_admin;
mod dispatch_auth;
mod dispatch_drive;
mod dispatch_emoji;
mod dispatch_i;
mod dispatch_lists;
mod dispatch_notes;
mod dispatch_rest;
mod dispatch_social;
mod dispatch_users;
mod files_http;
mod middleware;
mod parse;
#[cfg(test)]
mod tests_admin;
#[cfg(test)]
mod tests_auth;
#[cfg(test)]
mod tests_batch_n;
#[cfg(test)]
mod tests_batch_o;
#[cfg(test)]
mod tests_batch_pq;
#[cfg(test)]
mod tests_common;
#[cfg(test)]
mod tests_drive;
#[cfg(test)]
mod tests_exports;
#[cfg(test)]
mod tests_notes;
#[cfg(test)]
mod tests_users;

pub(crate) use admin_status::{build_admin_status, pack_backup_status, pack_pool_presence};
pub use auth_http::{configure_smtp, set_email_sender};
pub use files_http::UPLOAD_OVERHEAD_BYTES;
pub use middleware::{OpsConfig, last_readiness, ops_now_ms, set_backup_context, set_ops_config};
pub use parse::{MAX_JSON_BODY_BYTES, MIAUTH_APPROVAL_WINDOW_MS};

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

use super::auth::{
    AuthenticatedCaller, SESSION_COOKIE_NAME, authenticate, extract_raw_token,
    generate_random_token, hash_password_async, hash_token, is_safe_callback_url, needs_rehash,
    render_miauth_page, require_admin, require_moderator, verify_password_async,
};
use super::errors::ApiError;
use super::packers::{
    pack_abuse_report, pack_antenna, pack_blocking, pack_clip, pack_drive_file, pack_drive_folder,
    pack_emoji_detailed, pack_emoji_simple, pack_following, pack_invite_code, pack_me_detailed,
    pack_meta, pack_moderation_log, pack_muting, pack_note, pack_note_draft,
    pack_note_reaction_with_note, pack_notification, pack_page, pack_remote_emoji_detailed,
    pack_renote_muting, pack_scheduled_note, pack_system_webhook, pack_user_detailed_not_me,
    pack_user_list, pack_user_lite, pack_user_relation, pack_user_webhook,
};
use super::request::check_allowed_fields;
use super::response::{RateLimiter, UploadSlots};
use crate::services::email::EmailError;
use crate::services::email::EmailMessage;
use crate::services::email::EmailSendPayload;
use crate::services::email::EmailSender;
use crate::services::email::SmtpConfig;
use crate::services::email::SmtpEmailSender;
use crate::services::email::password_reset_email;
use crate::services::email::verification_email;

/// Shared API state: application services, the durable-job worker, the
/// Misskey rate limiter, the concurrent-upload slots, the live-streaming
/// broadcast hub, and the reverse proxies whose `X-Forwarded-For` is
/// honored ([`client_ip`]).
use self::auth_http::{
    client_ip, miauth_check, miauth_page, miauth_submit, rate_key_for, require_caller,
    resolve_caller, signin, signin_flow, signup,
};
use self::files_http::{drive_files_create, identicon, proxy_media, serve_file, serve_file_named};
use self::middleware::{
    cors_middleware, healthz, nosniff_middleware, preflight, ready, request_id_middleware,
};
use self::parse::parse_json_body;

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
        .route("/users/{id}/publickey", get(ap_actor::get_actor_public_key))
        .route("/users/{id}/outbox", get(ap_actor::get_actor_outbox))
        .route("/users/{id}/followers", get(ap_actor::get_actor_followers))
        .route("/users/{id}/following", get(ap_actor::get_actor_following))
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
        .route(
            "/users/{id}/stamps/{stamp}",
            get(ap_objects::get_quote_stamp),
        )
        // Federated clips + pages (pinetree-first DIVERGENCE): fetch-gated
        // `OrderedCollection` clips, the advertised clips index, and
        // `Article` pages. Global switches off ⇒ 404 on all three.
        .route("/clips/{id}", get(ap_collections::get_clip_collection))
        .route("/users/{id}/clips", get(ap_collections::get_clips_index))
        .route("/pages/{id}", get(ap_collections::get_page_article))
        // Batch G: the 64 KiB inbox business cap is enforced at
        // body-extraction time (per-route `DefaultBodyLimit`, owned by
        // `fed_inbox::inbox_body_limit`), so oversized bodies answer 413
        // before the handler buffers anything.
        .route(
            "/inbox",
            post(fed_inbox::inbox_handler).layer(fed_inbox::inbox_body_limit()),
        )
        .route(
            "/users/{id}/inbox",
            post(fed_inbox::user_inbox_handler).layer(fed_inbox::inbox_body_limit()),
        )
        .with_state(services);
    Router::new()
        .route("/healthz", get(healthz))
        .route("/ready", get(ready))
        .route("/files/{id}", get(serve_file))
        .route("/files/{id}/{name}", get(serve_file_named))
        .route("/identicon/{id}", get(identicon))
        .route(
            "/miauth/{session}",
            get(miauth_page).post(miauth_submit).options(preflight),
        )
        .route(
            "/miauth/{session}/check",
            post(miauth_check).options(preflight),
        )
        .route(
            "/api/miauth/{session}/check",
            post(miauth_check).options(preflight),
        )
        .route("/signup", post(signup).options(preflight))
        .route("/api/signup", post(signup).options(preflight))
        .route("/signin", post(signin).options(preflight))
        .route("/api/signin", post(signin).options(preflight))
        // Misskey `signin-flow` (the web password login; `ApiServerService`
        // mounts the whole API plugin at `/api`, so upstream serves it at
        // `/api/signin-flow` — the dispatcher arm below covers that path.
        // The bare `/signin-flow` alias matches no upstream route; it exists
        // so direct callers work the same with or without the prefix).
        .route("/signin-flow", post(signin_flow).options(preflight))
        .route("/api/signin-flow", post(signin_flow).options(preflight))
        .route(
            "/api/drive/files/create",
            post(drive_files_create).options(preflight),
        )
        // Batch Q Phase 1 (item 9): the route `meta.mediaProxy`
        // (`{uri}/proxy`) advertises. Upstream shape is `/proxy/:url*`
        // with a `?url=` override, so both forms route here.
        .route("/proxy", get(proxy_media))
        .route("/proxy/{*rest}", get(proxy_media))
        .route("/api/{*endpoint}", post(api_dispatch).options(preflight))
        .route("/streaming", get(streaming_handler))
        // No `DefaultBodyLimit::disable()`: the Misskey JSON dispatcher
        // enforces its own 1 MiB cap in `api_dispatch_blocking` (testable
        // without router layers), `drive/files/create` enforces its own
        // streaming cap in the handler, the inbox routes carry a per-route
        // 64 KiB `DefaultBodyLimit` (see `fed_inbox::inbox_body_limit`),
        // and everything else keeps axum's 2 MiB default extraction limit.
        .with_state(state)
        .merge(federation_routes)
        // Slice 7 (H2): every response carries `nosniff`, including error
        // bodies and redirects. CORS (`*`, Misskey `@fastify/cors`
        // `origin: '*'` parity — the frontend sends `credentials: 'omit'`,
        // so no allow-credentials is needed) rides a second layer.
        // Batch H: request correlation + durations ride outermost so the
        // timer covers the whole inner stack.
        .layer(axum::middleware::from_fn(nosniff_middleware))
        .layer(axum::middleware::from_fn(cors_middleware))
        .layer(axum::middleware::from_fn(request_id_middleware))
}

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
        .map_err(|_| {
            ApiError::from_port_error(PortError::StorageUnavailable("dispatch task failed".into()))
        })?
}

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
        "meta" => dispatch_auth::meta(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "miauth/gen-token" => {
            dispatch_auth::miauth_gen_token(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "auth/session/generate" => dispatch_auth::auth_session_generate(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "auth/session/show" => {
            dispatch_auth::auth_session_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "auth/accept" => {
            dispatch_auth::auth_accept(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "auth/session/userkey" => dispatch_auth::auth_session_userkey(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "signup" => dispatch_auth::signup(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "admin/accounts/create" => dispatch_admin::admin_accounts_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "signin" => dispatch_auth::signin(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "signin-flow" => {
            dispatch_auth::signin_flow(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "signin-with-passkey" => dispatch_auth::signin_with_passkey(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i" => dispatch_auth::i(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "i/update" => {
            dispatch_auth::i_update(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/show" => {
            dispatch_users::users_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/search-by-username-and-host" => dispatch_users::users_search_by_username_and_host(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/search" => {
            dispatch_users::users_search(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/lists/create" => dispatch_lists::users_lists_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/lists/update" => dispatch_lists::users_lists_update(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/lists/delete" => dispatch_lists::users_lists_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/lists/list" => {
            dispatch_lists::users_lists_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/lists/show" => {
            dispatch_lists::users_lists_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/lists/push" => {
            dispatch_lists::users_lists_push(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/lists/pull" => {
            dispatch_lists::users_lists_pull(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/lists/get-memberships" => dispatch_lists::users_lists_get_memberships(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/lists/update-membership" => dispatch_lists::users_lists_update_membership(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "clips/create" => {
            dispatch_social::clips_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/update" => {
            dispatch_social::clips_update(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/delete" => {
            dispatch_social::clips_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/list" => {
            dispatch_social::clips_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/show" => {
            dispatch_social::clips_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/notes" => {
            dispatch_social::clips_notes(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/add-note" => {
            dispatch_social::clips_add_note(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/remove-note" => dispatch_social::clips_remove_note(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "clips/favorite" => {
            dispatch_social::clips_favorite(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/unfavorite" => {
            dispatch_social::clips_unfavorite(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/my-favorites" => dispatch_social::clips_my_favorites(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "clips/follow" => {
            dispatch_social::clips_follow(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/unfollow" => {
            dispatch_social::clips_unfollow(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "clips/followers" => {
            dispatch_social::clips_followers(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "pages/create" => {
            dispatch_social::pages_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "pages/update" => {
            dispatch_social::pages_update(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "pages/delete" => {
            dispatch_social::pages_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "pages/show" => {
            dispatch_social::pages_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "pages/like" => {
            dispatch_social::pages_like(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "pages/unlike" => {
            dispatch_social::pages_unlike(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "pages/featured" => {
            dispatch_social::pages_featured(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/user-list-timeline" => dispatch_notes::notes_user_list_timeline(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "username/available" => dispatch_users::username_available(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/create" => {
            dispatch_notes::notes_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/show" => {
            dispatch_notes::notes_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/delete" => {
            dispatch_notes::notes_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/edit" => {
            dispatch_notes::notes_edit(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/reject-quotes" => dispatch_admin::admin_reject_quotes(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/show-user" => {
            dispatch_admin::admin_show_user(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/show-users" => {
            dispatch_admin::admin_show_users(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/update-user-note" => dispatch_admin::admin_update_user_note(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/delete-account" => dispatch_admin::admin_delete_account(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i/delete-account" => {
            dispatch_rest::i_delete_account(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/delete-all-files-of-a-user" => dispatch_admin::admin_delete_all_files_of_a_user(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/drive/files" => {
            dispatch_drive::admin_drive_files(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/drive/show-file" => dispatch_drive::admin_drive_show_file(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/show-moderation-logs" => dispatch_admin::admin_show_moderation_logs(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/report-abuse" => dispatch_users::users_report_abuse(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/abuse-user-reports" => dispatch_admin::admin_abuse_user_reports(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/resolve-abuse-user-report" => dispatch_admin::admin_resolve_abuse_user_report(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/update-abuse-user-report" => dispatch_admin::admin_update_abuse_user_report(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/forward-abuse-user-report" => dispatch_admin::admin_forward_abuse_user_report(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "emojis" => dispatch_emoji::emojis(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "emoji" => dispatch_emoji::emoji(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "admin/emoji/add" => {
            dispatch_emoji::admin_emoji_add(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/emoji/update" => dispatch_emoji::admin_emoji_update(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/emoji/delete" => dispatch_emoji::admin_emoji_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/emoji/list" => {
            dispatch_emoji::admin_emoji_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/emoji/add-aliases-bulk" => dispatch_emoji::admin_emoji_add_aliases_bulk(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/emoji/remove-aliases-bulk" => dispatch_emoji::admin_emoji_remove_aliases_bulk(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/emoji/set-aliases-bulk" => dispatch_emoji::admin_emoji_set_aliases_bulk(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/emoji/set-category-bulk" => dispatch_emoji::admin_emoji_set_category_bulk(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/emoji/set-license-bulk" => dispatch_emoji::admin_emoji_set_license_bulk(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/emoji/delete-bulk" => dispatch_emoji::admin_emoji_delete_bulk(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/emoji/copy" => {
            dispatch_emoji::admin_emoji_copy(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/emoji/list-remote" => dispatch_emoji::admin_emoji_list_remote(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/emoji/import-zip" => dispatch_emoji::admin_emoji_import_zip(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "antennas/list" => {
            dispatch_social::antennas_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "antennas/show" => {
            dispatch_social::antennas_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "antennas/create" => {
            dispatch_social::antennas_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "antennas/update" => {
            dispatch_social::antennas_update(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "antennas/delete" => {
            dispatch_social::antennas_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "antennas/notes" => {
            dispatch_social::antennas_notes(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "antennas/remove-note" => dispatch_social::antennas_remove_note(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "invite/create" => {
            dispatch_social::invite_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "invite/list" => {
            dispatch_social::invite_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "invite/delete" => {
            dispatch_social::invite_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "invite/limit" => {
            dispatch_social::invite_limit(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/invite/create" => dispatch_admin::admin_invite_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/invite/list" => {
            dispatch_admin::admin_invite_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/meta" => {
            dispatch_admin::admin_meta(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/update-meta" => {
            dispatch_admin::admin_update_meta(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/status" => {
            dispatch_admin::admin_status(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/backup" => {
            dispatch_admin::admin_backup(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/backups" => {
            dispatch_admin::admin_backups(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/backup-status" => dispatch_admin::admin_backup_status(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i/export-following" => {
            dispatch_i::i_export_following(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/export-blocking" => {
            dispatch_i::i_export_blocking(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/export-mute" => {
            dispatch_i::i_export_mute(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/export-notes" => {
            dispatch_i::i_export_notes(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/export-user-lists" => {
            dispatch_i::i_export_user_lists(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/export-antennas" => {
            dispatch_i::i_export_antennas(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/export-clips" => {
            dispatch_i::i_export_clips(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/export-favorites" => {
            dispatch_i::i_export_favorites(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "export-custom-emojis" => dispatch_emoji::export_custom_emojis(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i/import-following" => {
            dispatch_i::i_import_following(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/import-blocking" => {
            dispatch_i::i_import_blocking(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/import-muting" => {
            dispatch_i::i_import_muting(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/import-user-lists" => {
            dispatch_i::i_import_user_lists(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/import-clips" => {
            dispatch_i::i_import_clips(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/import-antennas" => {
            dispatch_i::i_import_antennas(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "following/create" => {
            dispatch_social::following_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "following/delete" => {
            dispatch_social::following_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "following/invalidate" => dispatch_social::following_invalidate(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "following/requests/list" => dispatch_social::following_requests_list(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/timeline" => {
            dispatch_notes::notes_timeline(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/local-timeline" => dispatch_notes::notes_local_timeline(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/global-timeline" => dispatch_notes::notes_global_timeline(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/hybrid-timeline" => dispatch_notes::notes_hybrid_timeline(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/reactions" => {
            dispatch_notes::notes_reactions(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/like" => {
            dispatch_notes::notes_like(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/reactions/create" => dispatch_notes::notes_reactions_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/reactions/delete" => dispatch_notes::notes_reactions_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/favorites/create" => dispatch_notes::notes_favorites_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/favorites/delete" => dispatch_notes::notes_favorites_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i/favorites" => {
            dispatch_notes::i_favorites(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/state" => {
            dispatch_notes::notes_state(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/polls/vote" => {
            dispatch_notes::notes_polls_vote(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/notifications" => {
            dispatch_i::i_notifications(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notifications/mark-all-as-read" => dispatch_i::notifications_mark_all_as_read(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i/change-password" => {
            dispatch_auth::i_change_password(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/regenerate-token" => {
            dispatch_auth::i_regenerate_token(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/apps" => dispatch_i::i_apps(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "i/authorized-apps" => {
            dispatch_i::i_authorized_apps(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/signin-history" => {
            dispatch_i::i_signin_history(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/revoke-token" => {
            dispatch_auth::i_revoke_token(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/notes" => {
            dispatch_users::users_notes(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/replies" => {
            dispatch_notes::notes_replies(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/renotes" => {
            dispatch_notes::notes_renotes(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/children" => {
            dispatch_notes::notes_children(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/conversation" => dispatch_notes::notes_conversation(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/unrenote" => {
            dispatch_notes::notes_unrenote(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/reactions" => {
            dispatch_notes::users_reactions(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "drive/files" => {
            dispatch_drive::drive_files(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "drive/files/show" => {
            dispatch_drive::drive_files_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/update-email" => {
            dispatch_i::i_update_email(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "request-reset-password" => dispatch_auth::request_reset_password(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "reset-password" => {
            dispatch_auth::reset_password(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "verify-email" => {
            dispatch_auth::verify_email(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/move" => dispatch_i::i_move(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "i/registry/get" => {
            dispatch_i::i_registry_get(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/registry/set" => {
            dispatch_i::i_registry_set(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/registry/get-all" => {
            dispatch_i::i_registry_get_all(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/registry/get-detail" => {
            dispatch_i::i_registry_get_detail(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/registry/keys" => {
            dispatch_i::i_registry_keys(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/registry/keys-with-type" => dispatch_i::i_registry_keys_with_type(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i/registry/remove" => {
            dispatch_i::i_registry_remove(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/registry/scopes-with-domain" => dispatch_i::i_registry_scopes_with_domain(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "get-online-users-count" => dispatch_rest::get_online_users_count(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "pinned-users" => {
            dispatch_rest::pinned_users(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "fetch-external-resources" => dispatch_rest::fetch_external_resources(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "fetch-rss" => {
            dispatch_rest::fetch_rss(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "server-info" => {
            dispatch_rest::server_info(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "stats" => dispatch_rest::stats(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "notes/mentions" => {
            dispatch_notes::notes_mentions(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notifications/test-notification" => dispatch_i::notifications_test_notification(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notifications/flush" => {
            dispatch_i::notifications_flush(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/recommendation" => dispatch_users::users_recommendation(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/recommendations" => dispatch_notes::notes_recommendations(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/versions" => {
            dispatch_notes::notes_versions(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/translate" => {
            dispatch_notes::notes_translate(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "drive/files/upload-from-url" => dispatch_drive::drive_files_upload_from_url(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/files/find-by-hash" => dispatch_drive::drive_files_find_by_hash(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/files/update" => dispatch_drive::drive_files_update(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/files/delete" => dispatch_drive::drive_files_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/cache/stats" => {
            dispatch_drive::drive_cache_stats(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/search" => {
            dispatch_notes::notes_search(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/search-by-tag" => dispatch_notes::notes_search_by_tag(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "url-preview" => {
            dispatch_rest::url_preview(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "blocking/create" => {
            dispatch_rest::blocking_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "blocking/delete" => {
            dispatch_rest::blocking_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "blocking/list" => {
            dispatch_rest::blocking_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "mute/create" => {
            dispatch_rest::mute_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "mute/delete" => {
            dispatch_rest::mute_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "mute/list" => {
            dispatch_rest::mute_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "renote-mute/create" => {
            dispatch_rest::renote_mute_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "renote-mute/delete" => {
            dispatch_rest::renote_mute_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "renote-mute/list" => {
            dispatch_rest::renote_mute_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/relation" => {
            dispatch_users::users_relation(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/drafts/create" => dispatch_notes::notes_drafts_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/drafts/update" => dispatch_notes::notes_drafts_update(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/drafts/delete" => dispatch_notes::notes_drafts_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/drafts/list" => {
            dispatch_notes::notes_drafts_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "notes/drafts/count" => dispatch_notes::notes_drafts_count(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/schedule/create" => dispatch_notes::notes_schedule_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/schedule/list" => dispatch_notes::notes_schedule_list(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/schedule/delete" => dispatch_notes::notes_schedule_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i/pin" => dispatch_notes::i_pin(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "i/unpin" => dispatch_notes::i_unpin(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "notes/featured" => {
            dispatch_notes::notes_featured(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/featured-notes" => dispatch_notes::users_featured_notes(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i/notifications-grouped" => dispatch_i::i_notifications_grouped(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/roles/create" => dispatch_admin::admin_roles_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/roles/list" => {
            dispatch_admin::admin_roles_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/roles/show" => {
            dispatch_admin::admin_roles_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/roles/delete" => dispatch_admin::admin_roles_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/roles/update" => dispatch_admin::admin_roles_update(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/roles/assign" => dispatch_admin::admin_roles_assign(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/roles/unassign" => dispatch_admin::admin_roles_unassign(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/roles/users" => {
            dispatch_admin::admin_roles_users(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/queue/stats" => {
            dispatch_admin::admin_queue_stats(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/relays/add" => {
            dispatch_admin::admin_relays_add(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/relays/list" => {
            dispatch_admin::admin_relays_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/relays/remove" => dispatch_admin::admin_relays_remove(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/ad/create" => {
            dispatch_admin::admin_ad_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/ad/list" => {
            dispatch_admin::admin_ad_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/ad/update" => {
            dispatch_admin::admin_ad_update(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/ad/delete" => {
            dispatch_admin::admin_ad_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/ad/refresh" => {
            dispatch_admin::admin_ad_refresh(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/announcements/create" => dispatch_admin::admin_announcements_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/announcements/list" => dispatch_admin::admin_announcements_list(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/announcements/update" => dispatch_admin::admin_announcements_update(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/announcements/delete" => dispatch_admin::admin_announcements_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/stats" => {
            dispatch_admin::admin_stats(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/server-info" => {
            dispatch_admin::admin_server_info(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/update-proxy-account" => dispatch_admin::admin_update_proxy_account(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/proxy-account" => dispatch_admin::admin_proxy_account(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/unset-mfa" => {
            dispatch_admin::admin_unset_mfa(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/reset-password" => dispatch_admin::admin_reset_password(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "queue/stats" => {
            dispatch_rest::queue_stats(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "queue/deliver-delayed" => dispatch_rest::queue_deliver_delayed(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "abuse-report-notification-recipient/create" => {
            dispatch_i::abuse_report_notification_recipient_create(
                &state, &obj, caller, &rate_key, &client_ip, &headers,
            )
        }
        "abuse-report-notification-recipient/list" => {
            dispatch_i::abuse_report_notification_recipient_list(
                &state, &obj, caller, &rate_key, &client_ip, &headers,
            )
        }
        "abuse-report-notification-recipient/show" => {
            dispatch_i::abuse_report_notification_recipient_show(
                &state, &obj, caller, &rate_key, &client_ip, &headers,
            )
        }
        "abuse-report-notification-recipient/update" => {
            dispatch_i::abuse_report_notification_recipient_update(
                &state, &obj, caller, &rate_key, &client_ip, &headers,
            )
        }
        "abuse-report-notification-recipient/delete" => {
            dispatch_i::abuse_report_notification_recipient_delete(
                &state, &obj, caller, &rate_key, &client_ip, &headers,
            )
        }
        "i/2fa/register" => {
            dispatch_auth::i_2fa_register(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/2fa/key-done" => {
            dispatch_auth::i_2fa_key_done(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/2fa/done" => {
            dispatch_auth::i_2fa_done(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/2fa/password-less" => dispatch_auth::i_2fa_password_less(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "i/2fa/unregister" => {
            dispatch_auth::i_2fa_unregister(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/2fa/update-key" => {
            dispatch_auth::i_2fa_update_key(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/2fa/remove-key" => {
            dispatch_auth::i_2fa_remove_key(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/2fa/authenticate" => {
            dispatch_auth::i_2fa_authenticate(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "my/apps" => dispatch_auth::my_apps(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "app/create" => {
            dispatch_auth::app_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "app/show" => {
            dispatch_auth::app_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "ping" => dispatch_auth::ping(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "retention" => {
            dispatch_rest::retention(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "reset-db" => {
            dispatch_rest::reset_db(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "sw/register" => {
            dispatch_i::sw_register(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "sw/show-registration" => {
            dispatch_i::sw_show_registration(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "sw/unregister" => {
            dispatch_i::sw_unregister(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "sw/update-registration" => dispatch_i::sw_update_registration(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "v2/admin/emoji/list" => dispatch_emoji::v2_admin_emoji_list(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "endpoint" => {
            dispatch_rest::endpoint(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "endpoints" => {
            dispatch_rest::endpoints(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "gallery/posts/create" => dispatch_rest::gallery_posts_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "gallery/posts/show" => {
            dispatch_rest::gallery_posts_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "gallery/posts/update" => dispatch_rest::gallery_posts_update(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "gallery/posts/delete" => dispatch_rest::gallery_posts_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "gallery/posts/like" => {
            dispatch_rest::gallery_posts_like(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "gallery/posts/unlike" => dispatch_rest::gallery_posts_unlike(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "gallery/posts" => {
            dispatch_rest::gallery_posts(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "gallery/featured" => {
            dispatch_rest::gallery_featured(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "gallery/popular" => {
            dispatch_rest::gallery_popular(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "flash/create" => {
            dispatch_rest::flash_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "flash/show" => {
            dispatch_rest::flash_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "flash/update" => {
            dispatch_rest::flash_update(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "flash/delete" => {
            dispatch_rest::flash_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "flash/like" => {
            dispatch_rest::flash_like(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "flash/unlike" => {
            dispatch_rest::flash_unlike(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "flash/featured" => {
            dispatch_rest::flash_featured(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "flash/my" => {
            dispatch_rest::flash_my(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "flash/my-likes" => {
            dispatch_rest::flash_my_likes(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "hashtags/list" => {
            dispatch_rest::hashtags_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "hashtags/search" => {
            dispatch_rest::hashtags_search(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "hashtags/show" => {
            dispatch_rest::hashtags_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "hashtags/trend" => {
            dispatch_rest::hashtags_trend(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "hashtags/users" => {
            dispatch_rest::hashtags_users(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "announcements" => {
            dispatch_rest::announcements(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "announcements/show" => {
            dispatch_rest::announcements_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "i/read-announcement" => dispatch_rest::i_read_announcement(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "promo/read" => {
            dispatch_rest::promo_read(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "following/update" => {
            dispatch_social::following_update(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "following/update-all" => dispatch_social::following_update_all(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "following/list" => {
            dispatch_social::following_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "following/requests/sent" => dispatch_social::following_requests_sent(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/files/check-existence" => dispatch_drive::drive_files_check_existence(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/files/attached-notes" => dispatch_drive::drive_files_attached_notes(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/stream" => {
            dispatch_drive::drive_stream(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "drive/folders/create" => dispatch_drive::drive_folders_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/folders/show" => dispatch_drive::drive_folders_show(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/folders/find" => dispatch_drive::drive_folders_find(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/folders/update" => dispatch_drive::drive_folders_update(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/folders/delete" => dispatch_drive::drive_folders_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "drive/files/find" => {
            dispatch_drive::drive_files_find(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "drive/files/move-bulk" => dispatch_drive::drive_files_move_bulk(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/thread-muting/create" => dispatch_notes::notes_thread_muting_create(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "notes/thread-muting/delete" => dispatch_notes::notes_thread_muting_delete(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/update-memo" => {
            dispatch_users::users_update_memo(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "system-webhook/create" => {
            dispatch_i::system_webhook_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "system-webhook/list" => {
            dispatch_i::system_webhook_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "system-webhook/show" => {
            dispatch_i::system_webhook_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "system-webhook/update" => {
            dispatch_i::system_webhook_update(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "system-webhook/delete" => {
            dispatch_i::system_webhook_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "webhooks/create" => {
            dispatch_i::webhooks_create(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "webhooks/list" => {
            dispatch_i::webhooks_list(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "webhooks/show" => {
            dispatch_i::webhooks_show(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "webhooks/update" => {
            dispatch_i::webhooks_update(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "webhooks/delete" => {
            dispatch_i::webhooks_delete(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/send-email" => {
            dispatch_admin::admin_send_email(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/pages" => {
            dispatch_users::users_pages(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/lists/create-from-public" => dispatch_lists::users_lists_create_from_public(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/lists/favorite" => dispatch_lists::users_lists_favorite(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/lists/unfavorite" => dispatch_lists::users_lists_unfavorite(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/suspend-user" => dispatch_admin::admin_suspend_user(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/unsuspend-user" => dispatch_admin::admin_unsuspend_user(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/unset-user-avatar" => dispatch_admin::admin_unset_user_avatar(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/unset-user-banner" => dispatch_admin::admin_unset_user_banner(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "following/requests/accept" => dispatch_social::following_requests_accept(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "following/requests/reject" => dispatch_social::following_requests_reject(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "following/requests/cancel" => dispatch_social::following_requests_cancel(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "users/followers" => {
            dispatch_users::users_followers(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "users/following" => {
            dispatch_users::users_following(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "ap/show" => dispatch_rest::ap_show(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "ap/get" => dispatch_rest::ap_get(&state, &obj, caller, &rate_key, &client_ip, &headers),
        "admin/queue/deliver-delayed" => dispatch_admin::admin_queue_deliver_delayed(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/queue/inbox-delayed" => dispatch_admin::admin_queue_inbox_delayed(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/queue/clear" => {
            dispatch_admin::admin_queue_clear(&state, &obj, caller, &rate_key, &client_ip, &headers)
        }
        "admin/federation/list" => dispatch_admin::admin_federation_list(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "federation/instances" => dispatch_admin::federation_instances(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/federation/show-instance" => dispatch_admin::admin_federation_show_instance(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "federation/show-instance" => dispatch_admin::federation_show_instance(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/federation/update-instance" => dispatch_admin::admin_federation_update_instance(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "federation/update-instance" => dispatch_admin::federation_update_instance(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/federation/refresh-remote-instance-metadata" => {
            dispatch_admin::admin_federation_refresh_remote_instance_metadata(
                &state, &obj, caller, &rate_key, &client_ip, &headers,
            )
        }
        "federation/refresh-remote-instance-metadata" => {
            dispatch_admin::federation_refresh_remote_instance_metadata(
                &state, &obj, caller, &rate_key, &client_ip, &headers,
            )
        }
        "admin/get-user-ips" => dispatch_admin::admin_get_user_ips(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "federation/get-user-ips" => dispatch_admin::federation_get_user_ips(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "admin/accounts/find-by-email" => dispatch_admin::admin_accounts_find_by_email(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        "federation/find-by-email" => dispatch_admin::federation_find_by_email(
            &state, &obj, caller, &rate_key, &client_ip, &headers,
        ),
        _ => Err(ApiError::unknown_endpoint()),
    }
}
