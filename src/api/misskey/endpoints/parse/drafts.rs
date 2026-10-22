//! Shared request parsing, validation and packing helpers (drafts).
//!
//! Split from `endpoints/parse.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::files_http::identicon;
use super::opt_bool;
use super::opt_i64;
use super::opt_string;
use super::parse_draft_poll_param;
use super::parse_reaction_acceptance;
use super::parse_visibility;
use super::str_list;
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

/// Draft text (`minLength: 0, maxLength: 3000, nullable` parity): empty is
/// allowed, overlong is `INVALID_PARAM`.
pub(crate) fn opt_draft_text(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<String>, ApiError> {
    match opt_string(obj, key)? {
        Some(text) if text.chars().count() > 3000 => {
            Err(ApiError::invalid_param(key, "must be at most 3000 chars"))
        }
        text => Ok(text),
    }
}

/// Draft cw (`minLength: 1, maxLength: 100, nullable` parity).
pub(crate) fn opt_draft_cw(obj: &Map<String, Value>) -> Result<Option<String>, ApiError> {
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
pub(crate) fn opt_draft_hashtag(obj: &Map<String, Value>) -> Result<Option<String>, ApiError> {
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
pub(crate) fn draft_file_ids_for_key(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Vec<String>, ApiError> {
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

pub(crate) fn draft_file_ids(obj: &Map<String, Value>) -> Result<Vec<String>, ApiError> {
    draft_file_ids_for_key(obj, "fileIds")
}

/// Draft `visibleUserIds` (`uniqueItems` parity).
pub(crate) fn draft_visible_user_ids(obj: &Map<String, Value>) -> Result<Vec<String>, ApiError> {
    let ids = str_list(obj, "visibleUserIds")?;
    let mut seen = std::collections::HashSet::new();
    if !ids.iter().all(|id| seen.insert(id)) {
        return Err(ApiError::invalid_param("visibleUserIds", "must be unique"));
    }
    Ok(ids)
}

/// Build a [`CreateDraftInput`] from a `notes/drafts/create` body.
/// `channelId` must be null (channels do not exist; a non-null channel is
/// `NO_SUCH_CHANNEL`, the upstream existence-check outcome for an unknown
/// channel).
pub(crate) fn parse_draft_create(obj: &Map<String, Value>) -> Result<CreateDraftInput, ApiError> {
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
pub(crate) fn patch_nullable_string(
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
pub(crate) fn patch_nullable_bool(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<bool>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(ApiError::invalid_param(key, "must be a boolean")),
    }
}

/// Nullable-integer patch field: absent → `None`, `null` → `Some(None)`,
/// integer → `Some(Some(_))`.
pub(crate) fn patch_nullable_i64(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Option<i64>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::Number(n)) => {
            Ok(Some(Some(n.as_i64().ok_or_else(|| {
                ApiError::invalid_param(key, "must be an integer")
            })?)))
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be an integer")),
    }
}

/// Build an [`UpdateDraftPatch`] from a `notes/drafts/update` body.
/// Every field is optional; `poll: null`/absent leaves poll columns
/// untouched (upstream `ps.poll?.choices` parity), while a `poll` object
/// replaces the poll columns wholesale (absent sub-fields clear, mirroring
/// the upstream `.set(data)` with `null` defaults).
pub(crate) fn parse_draft_update(obj: &Map<String, Value>) -> Result<UpdateDraftPatch, ApiError> {
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
            return Err(ApiError::invalid_param(
                "text",
                "must be at most 3000 chars",
            ));
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
            return Err(ApiError::invalid_param(
                "hashtag",
                "must be at most 200 chars",
            ));
        }
        tag => tag,
    };
    let file_ids = match obj.get("fileIds") {
        None | Some(Value::Null) => None,
        Some(Value::Array(_)) => Some(draft_file_ids(obj)?),
        Some(_) => {
            return Err(ApiError::invalid_param(
                "fileIds",
                "must be an array of strings",
            ));
        }
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
