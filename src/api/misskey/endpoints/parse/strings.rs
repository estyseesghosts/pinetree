//! Shared request parsing, validation and packing helpers (strings).
//!
//! Split from `endpoints/parse.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::files_http::identicon;
use super::opt_usize;
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

// --- Media / identicon -----------------------------------------------------
/// `limit` with an upstream default other than the 10-row moderation-log
/// default (the invite listings default to 30 upstream).
/// Batch N WS4: antenna param helpers (upstream `antennas/*` `paramDef`
/// parity: required-with-bounds strings, required string arrays, the `src`
/// enum, optional-with-bounds strings, optional arrays, optional bools, and
/// nullable strings). Missing/wrong-typed params answer `INVALID_PARAM`
/// naming the field.
pub(crate) fn required_string(
    obj: &Map<String, Value>,
    key: &str,
    min: usize,
    max: usize,
) -> Result<String, ApiError> {
    match obj.get(key) {
        Some(Value::String(s)) => {
            let n = s.chars().count();
            if n < min || n > max {
                return Err(ApiError::invalid_param(
                    key,
                    &format!("must be {min}..={max} chars"),
                ));
            }
            Ok(s.clone())
        }
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

pub(crate) fn opt_bounded_string(
    obj: &Map<String, Value>,
    key: &str,
    min: usize,
    max: usize,
) -> Result<Option<String>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => {
            let n = s.chars().count();
            if n < min || n > max {
                return Err(ApiError::invalid_param(
                    key,
                    &format!("must be {min}..={max} chars"),
                ));
            }
            Ok(Some(s.clone()))
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

pub(crate) fn opt_nullable_string(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Option<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(s)) => Ok(Some(Some(s.clone()))),
        Some(_) => Err(ApiError::invalid_param(key, "must be a string or null")),
    }
}

pub(crate) fn required_antenna_src(
    obj: &Map<String, Value>,
) -> Result<crate::domain::antenna::AntennaSrc, ApiError> {
    match obj.get("src").and_then(Value::as_str) {
        Some(s) => crate::domain::antenna::AntennaSrc::parse(s).ok_or_else(|| {
            ApiError::invalid_param(
                "src",
                "must be one of home, all, users, list, users_blacklist",
            )
        }),
        None => Err(ApiError::invalid_param("src", "this field is required")),
    }
}

pub(crate) fn opt_antenna_src(
    obj: &Map<String, Value>,
) -> Result<Option<crate::domain::antenna::AntennaSrc>, ApiError> {
    match obj.get("src") {
        None => Ok(None),
        Some(Value::String(s)) => crate::domain::antenna::AntennaSrc::parse(s)
            .map(Some)
            .ok_or_else(|| {
                ApiError::invalid_param(
                    "src",
                    "must be one of home, all, users, list, users_blacklist",
                )
            }),
        Some(_) => Err(ApiError::invalid_param("src", "must be a string")),
    }
}

pub(crate) fn required_str_array(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Vec<String>, ApiError> {
    match obj.get(key) {
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
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

pub(crate) fn opt_str_array(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    _ => return Err(ApiError::invalid_param(key, "must be an array of strings")),
                }
            }
            Ok(Some(out))
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be an array of strings")),
    }
}

pub(crate) fn required_str_array_array(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Vec<Vec<String>>, ApiError> {
    match obj.get(key) {
        Some(Value::Array(groups)) => {
            let mut out = Vec::with_capacity(groups.len());
            for group in groups {
                match group {
                    Value::Array(items) => {
                        let mut inner = Vec::with_capacity(items.len());
                        for item in items {
                            match item {
                                Value::String(s) => inner.push(s.clone()),
                                _ => {
                                    return Err(ApiError::invalid_param(
                                        key,
                                        "must be an array of string arrays",
                                    ));
                                }
                            }
                        }
                        out.push(inner);
                    }
                    _ => {
                        return Err(ApiError::invalid_param(
                            key,
                            "must be an array of string arrays",
                        ));
                    }
                }
            }
            Ok(out)
        }
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

pub(crate) fn opt_str_array_array(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<Vec<String>>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(_) => required_str_array_array(obj, key).map(Some),
    }
}

pub(crate) fn required_bool(obj: &Map<String, Value>, key: &str) -> Result<bool, ApiError> {
    match obj.get(key) {
        Some(Value::Bool(b)) => Ok(*b),
        _ => Err(ApiError::invalid_param(key, "this field is required")),
    }
}

pub(crate) fn opt_bool_opt(obj: &Map<String, Value>, key: &str) -> Result<Option<bool>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(ApiError::invalid_param(key, "must be a boolean")),
    }
}

pub(crate) fn get_limit_with_default(
    obj: &Map<String, Value>,
    default: usize,
) -> Result<usize, ApiError> {
    match opt_usize(obj, "limit")? {
        None => Ok(default),
        Some(limit) if (1..=100).contains(&limit) => Ok(limit),
        Some(_) => Err(ApiError::invalid_param(
            "limit",
            "must be an integer between 1 and 100",
        )),
    }
}
