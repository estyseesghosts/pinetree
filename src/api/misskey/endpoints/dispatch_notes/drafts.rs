//! Notes dispatcher arms (drafts).
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

/// `POST /api/notes/drafts/create` (split from `api_dispatch_blocking`).
pub(crate) fn notes_drafts_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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

/// `POST /api/notes/drafts/update` (split from `api_dispatch_blocking`).
pub(crate) fn notes_drafts_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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

/// `POST /api/notes/drafts/delete` (split from `api_dispatch_blocking`).
pub(crate) fn notes_drafts_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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

/// `POST /api/notes/drafts/list` (split from `api_dispatch_blocking`).
pub(crate) fn notes_drafts_list(
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
            "scheduled",
        ],
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

/// `POST /api/notes/drafts/count` (split from `api_dispatch_blocking`).
pub(crate) fn notes_drafts_count(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let count = state
        .services
        .count_drafts(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(Value::Number(count.into())).into_response())
}

/// `POST /api/notes/schedule/create` (split from `api_dispatch_blocking`).
pub(crate) fn notes_schedule_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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
    let has_text = obj
        .get("text")
        .and_then(Value::as_str)
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    let has_content = has_text
        || obj
            .get("renoteId")
            .and_then(Value::as_str)
            .map(|s| !s.is_empty())
            .unwrap_or(false)
        || obj
            .get("fileIds")
            .and_then(Value::as_array)
            .map(|a| !a.is_empty())
            .unwrap_or(false)
        || obj
            .get("mediaIds")
            .and_then(Value::as_array)
            .map(|a| !a.is_empty())
            .unwrap_or(false)
        || matches!(obj.get("poll"), Some(Value::Object(_)));
    if !has_content {
        return Err(ApiError::invalid_param(
            "text",
            "one of text, renoteId, fileIds, mediaIds, poll is required",
        ));
    }
    // `scheduleNote: {scheduledAt!}` (ms epoch — confirmed against
    // the tail of upstream `create.ts`: `new Date(schedNoteLocalTime)`
    // where the local time derives from `moment.utc(...).valueOf()`).
    let scheduled_at_ms = match obj.get("scheduleNote").and_then(|v| v.get("scheduledAt")) {
        Some(Value::Number(n)) => n.as_i64().ok_or_else(|| {
            ApiError::invalid_param("scheduleNote", "scheduledAt must be an integer")
        })?,
        _ => {
            return Err(ApiError::invalid_param(
                "scheduleNote",
                "scheduleNote.scheduledAt is required",
            ));
        }
    };
    // Upstream cap check against the role policy; pinetree's
    // `scheduled_note_limit` stands in (default 1, instance
    // configurable — upstream default 5). Overflow answers
    // Sharkey's `SCHEDULE_NOTE_MAX`, not our `TOO_MANY_SCHEDULED_NOTES`.
    if state
        .services
        .notes
        .count_scheduled_drafts(&caller.account.id)
        .map_err(ApiError::from_port_error)?
        >= state.services.scheduled_note_limit
    {
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
    Ok(Json(
        pack_scheduled_note(&draft, &caller.account, &state.services)
            .map_err(ApiError::from_port_error)?,
    )
    .into_response())
}

/// `POST /api/notes/schedule/list` (split from `api_dispatch_blocking`).
pub(crate) fn notes_schedule_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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
        out.push(
            pack_scheduled_note(draft, &caller.account, &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/notes/schedule/delete` (split from `api_dispatch_blocking`).
pub(crate) fn notes_schedule_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
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
            return Err(ApiError::schedule_permission_denied());
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

/// `POST /api/i/pin` (split from `api_dispatch_blocking`).
pub(crate) fn i_pin(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .pin_note(&caller.account.id, &note_id)
        .map_err(ApiError::from_port_error)?;
    let packed = pack_me_detailed(&caller.account, caller.is_first_party, &state.services)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/i/unpin` (split from `api_dispatch_blocking`).
pub(crate) fn i_unpin(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .unpin_note(&caller.account.id, &note_id)
        .map_err(ApiError::from_port_error)?;
    let packed = pack_me_detailed(&caller.account, caller.is_first_party, &state.services)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}
