//! Notes dispatcher arms (timeline).
//!
//! Split from `endpoints/dispatch_notes.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{client_ip, require_caller};
use super::super::parse::{
    draft_file_ids_for_key, draft_visible_user_ids, get_limit, get_limit_with_default, opt_bool,
    opt_draft_cw, opt_draft_text, opt_i64, opt_string, opt_usize, pack_note_list, parse_cursor,
    parse_draft_create, parse_draft_poll_param, parse_draft_update, parse_list_cursor,
    parse_poll_param, parse_reaction_acceptance, parse_visibility, required_str, str_list,
};
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

/// `POST /api/notes/user-list-timeline` (split from `api_dispatch_blocking`).
pub(crate) fn notes_user_list_timeline(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &[
            "listId",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
            "allowPartial",
            "includeMyRenotes",
            "includeRenotedMyNotes",
            "includeLocalRenotes",
            "withRenotes",
            "withFiles",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let list_id = required_str(&obj, "listId")?;
    let cursor = parse_cursor(&obj)?;
    let include_my_renotes = opt_bool(&obj, "includeMyRenotes", true)?;
    let include_renoted_my_notes = opt_bool(&obj, "includeRenotedMyNotes", true)?;
    let include_local_renotes = opt_bool(&obj, "includeLocalRenotes", true)?;
    let with_renotes = opt_bool(&obj, "withRenotes", true)?;
    let with_files = opt_bool(&obj, "withFiles", false)?;
    let notes = state
        .services
        .user_list_timeline(
            &list_id,
            &caller.account.id,
            &cursor,
            include_my_renotes,
            include_renoted_my_notes,
            include_local_renotes,
            with_renotes,
            with_files,
        )
        .map_err(|e| match e {
            PortError::NotFound(msg) if msg == "NO_SUCH_LIST" => {
                ApiError::user_list_no_such_list("notes/user-list-timeline")
            }
            other => ApiError::from_port_error(other),
        })?;
    pack_note_list(&state.services, &notes, Some(&caller.account.id))
}

/// `POST /api/notes/timeline` (split from `api_dispatch_blocking`).
pub(crate) fn notes_timeline(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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
    // Batch Q Phase 1 item 7: the `include*` trio is honored, not
    // just accepted (upstream `notes/timeline.ts:48-50` defaults,
    // all true).
    let include_my_renotes = opt_bool(&obj, "includeMyRenotes", true)?;
    let include_renoted_my_notes = opt_bool(&obj, "includeRenotedMyNotes", true)?;
    let include_local_renotes = opt_bool(&obj, "includeLocalRenotes", true)?;
    let notes = state
        .services
        .home_timeline_with_includes(
            &caller.account.id,
            &cursor,
            with_files,
            with_renotes,
            include_my_renotes,
            include_renoted_my_notes,
            include_local_renotes,
        )
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, Some(&caller.account.id))
}

/// `POST /api/notes/local-timeline` (split from `api_dispatch_blocking`).
pub(crate) fn notes_local_timeline(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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

/// `POST /api/notes/global-timeline` (split from `api_dispatch_blocking`).
pub(crate) fn notes_global_timeline(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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

/// `POST /api/notes/hybrid-timeline` (split from `api_dispatch_blocking`).
pub(crate) fn notes_hybrid_timeline(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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
    // Batch Q Phase 1 item 7: same `include*` trio as home (flags
    // apply to the merged home ∪ local-public set, upstream
    // `hybrid-timeline.ts getFromDb` parity).
    let include_my_renotes = opt_bool(&obj, "includeMyRenotes", true)?;
    let include_renoted_my_notes = opt_bool(&obj, "includeRenotedMyNotes", true)?;
    let include_local_renotes = opt_bool(&obj, "includeLocalRenotes", true)?;
    let notes = state
        .services
        .hybrid_timeline_with_includes(
            &caller.account.id,
            &cursor,
            with_files,
            with_renotes,
            with_replies,
            include_my_renotes,
            include_renoted_my_notes,
            include_local_renotes,
        )
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, Some(&caller.account.id))
}

/// `POST /api/notes/mentions` (split from `api_dispatch_blocking`).
pub(crate) fn notes_mentions(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &[
            "following",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
            "visibility",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let following = opt_bool(&obj, "following", false)?;
    let cursor = parse_cursor(&obj)?;
    // Upstream `visibility` is a free string equality filter on the
    // note's visibility (no validation — unknown values match
    // nothing). Mirror: parse strictly, unknown → empty page.
    let visibility = match obj.get("visibility") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => match s.as_str() {
            "public" => Some(Visibility::Public),
            "home" => Some(Visibility::Home),
            "followers" => Some(Visibility::Followers),
            "specified" => Some(Visibility::Specified),
            _ => {
                return Ok(Json(Value::Array(Vec::new())).into_response());
            }
        },
        Some(_) => return Err(ApiError::invalid_param("visibility", "must be a string")),
    };
    let notes = state
        .services
        .mentions_timeline(&caller.account.id, following, visibility, &cursor)
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, Some(&caller.account.id))
}

/// `POST /api/notes/search` (split from `api_dispatch_blocking`).
pub(crate) fn notes_search(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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

/// `POST /api/notes/search-by-tag` (split from `api_dispatch_blocking`).
pub(crate) fn notes_search_by_tag(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &[
            "tag",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
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

/// `POST /api/notes/recommendations` (split from `api_dispatch_blocking`).
pub(crate) fn notes_recommendations(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &[
            "limit",
            "offset",
            "refresh",
            "lang",
            "withSensitive",
            "withRenotes",
        ],
    )?;
    let viewer_id = caller.as_ref().map(|c| c.account.id.as_str());
    let limit = get_limit(&obj)?.clamp(1, 30);
    let offset = opt_i64(&obj, "offset")?.unwrap_or(0).max(0) as usize;
    // `refresh` (re-rank) and `lang` (language lanes) are
    // accepted-and-ignored: no algorithmic-feed infrastructure
    // (non-goal, manifest §35).
    let with_sensitive = opt_bool(&obj, "withSensitive", false)?;
    let with_renotes = opt_bool(&obj, "withRenotes", true)?;
    let notes = state
        .services
        .recommended_notes(viewer_id, limit, offset, with_renotes, with_sensitive)
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, viewer_id)
}
