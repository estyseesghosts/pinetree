//! Shared request parsing, validation and packing helpers (poll).
//!
//! Split from `endpoints/parse.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::files_http::identicon;
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

/// Parse a `notes/create` `poll` param (Misskey `notes/create.ts:168-184`
/// parity): `{ choices[2..10] unique 1..50, multiple?, expiresAt?,
/// expiredAfter? }`. `null`/absent → `None`. Choice-shape violations are
/// `INVALID_PARAM`; an already-past expiry is the exact
/// `CANNOT_CREATE_ALREADY_EXPIRED_POLL` ident (surfaced through
/// `ApiError::from_port_error` so the UUID matches `create.ts`).
pub(crate) fn parse_poll_param(
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
        if !matches!(
            field.as_str(),
            "choices" | "multiple" | "expiresAt" | "expiredAfter"
        ) {
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
        Some(_) => {
            return Err(ApiError::invalid_param(
                key,
                "expiredAfter must be an integer",
            ));
        }
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
        crate::domain::id::DomainError::Validation(msg) => ApiError::invalid_param(key, &msg),
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
pub(crate) struct DraftPollParam {
    pub(crate) choices: Vec<String>,
    pub(crate) multiple: Option<bool>,
    pub(crate) expires_at_ms: Option<Option<i64>>,
    pub(crate) expired_after_ms: Option<Option<i64>>,
}

pub(crate) fn parse_draft_poll_param(
    obj: &Map<String, Value>,
) -> Result<Option<DraftPollParam>, ApiError> {
    let raw = match obj.get("poll") {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::Object(map)) => map,
        Some(_) => return Err(ApiError::invalid_param("poll", "must be an object")),
    };
    for field in raw.keys() {
        if !matches!(
            field.as_str(),
            "choices" | "multiple" | "expiresAt" | "expiredAfter"
        ) {
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
        return Err(ApiError::invalid_param(
            "poll",
            "choices must have at most 10 items",
        ));
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
        Some(_) => {
            return Err(ApiError::invalid_param(
                "poll",
                "multiple must be a boolean",
            ));
        }
    };
    let expires_at_ms = match raw.get("expiresAt") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::Number(n)) => {
            Some(Some(n.as_i64().ok_or_else(|| {
                ApiError::invalid_param("poll", "expiresAt must be an integer")
            })?))
        }
        Some(_) => {
            return Err(ApiError::invalid_param(
                "poll",
                "expiresAt must be an integer",
            ));
        }
    };
    let expired_after_ms = match raw.get("expiredAfter") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::Number(n)) => {
            let v = n.as_i64().ok_or_else(|| {
                ApiError::invalid_param("poll", "expiredAfter must be an integer")
            })?;
            if v < 1 {
                return Err(ApiError::invalid_param(
                    "poll",
                    "expiredAfter must be at least 1",
                ));
            }
            Some(Some(v))
        }
        Some(_) => {
            return Err(ApiError::invalid_param(
                "poll",
                "expiredAfter must be an integer",
            ));
        }
    };
    Ok(Some(DraftPollParam {
        choices,
        multiple,
        expires_at_ms,
        expired_after_ms,
    }))
}
