//! Shared request parsing, validation and packing helpers (json).
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

/// Parse a request body as a JSON object. An empty body defaults to `{}`.
/// Anything that is not a JSON object is `400 INVALID_PARAM`.
pub(crate) fn parse_json_body(body: &[u8]) -> Result<Map<String, Value>, ApiError> {
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

/// Slice 6 (H1): maximum accepted Misskey JSON API body (1 MiB), enforced
/// in `api_dispatch_blocking` so oversized JSON answers 413 without
/// router layers. Multipart uploads are exempt: `drive/files/create`
/// enforces its own streaming cap instead.
pub const MAX_JSON_BODY_BYTES: usize = 1_048_576;

/// MiAuth + legacy auth approval window (Batch N WS7 hardening,
/// DIVERGENCE — upstream has no expiry on either session type). Sessions
/// older than this read as missing (deleted on read); 30 minutes covers
/// the approve-and-poll loop with margin.
pub const MIAUTH_APPROVAL_WINDOW_MS: i64 = 30 * 60 * 1000;

pub(crate) fn required_str(obj: &Map<String, Value>, key: &str) -> Result<String, ApiError> {
    match obj.get(key) {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

pub(crate) fn opt_string(obj: &Map<String, Value>, key: &str) -> Result<Option<String>, ApiError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

pub(crate) fn opt_bool(
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

pub(crate) fn opt_i64(obj: &Map<String, Value>, key: &str) -> Result<Option<i64>, ApiError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_i64()
            .map(Some)
            .ok_or_else(|| ApiError::invalid_param(key, "must be an integer")),
        Some(_) => Err(ApiError::invalid_param(key, "must be an integer")),
    }
}

pub(crate) fn opt_usize(obj: &Map<String, Value>, key: &str) -> Result<Option<usize>, ApiError> {
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

pub(crate) fn get_limit(obj: &Map<String, Value>) -> Result<usize, ApiError> {
    match opt_usize(obj, "limit")? {
        None => Ok(10),
        Some(limit) if (1..=100).contains(&limit) => Ok(limit),
        Some(_) => Err(ApiError::invalid_param(
            "limit",
            "must be an integer between 1 and 100",
        )),
    }
}

pub(crate) fn str_list(obj: &Map<String, Value>, key: &str) -> Result<Vec<String>, ApiError> {
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
