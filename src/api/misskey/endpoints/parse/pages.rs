//! Shared request parsing, validation and packing helpers (pages).
//!
//! Split from `endpoints/parse.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::files_http::identicon;
use super::opt_str_array;
use super::opt_string;
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

pub(crate) fn update_string_field(
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

pub(crate) fn update_bool_field(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<bool>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(ApiError::invalid_param(key, "must be a boolean")),
    }
}

/// Parse a `pages/create` (full body, `require_all = true`) or
/// `pages/update` (partial, `require_all = false`) body into a
/// [`PageBody`](crate::application::commands::pages::PageBody).
/// Upstream `create.ts`/`update.ts` param shapes: `title`, `name`
/// (1–256 + slug rules), `summary?` (nullable ≤256), `content`
/// (array of `{type, ...}` blocks), `variables` (array), `script`
/// (string, empty allowed), `eyeCatchingImageId?` (nullable),
/// `font` (`serif`|`sans-serif`), `alignCenter`, `hideTitleWhenPinned`
/// (booleans) — plus the pinetree federation extension `visibility?`,
/// `federate?`, `approvedActorUris?`.
pub(crate) fn parse_page_body(
    obj: &Map<String, Value>,
    require_all: bool,
) -> Result<crate::application::commands::pages::PageBody, ApiError> {
    use crate::application::commands::pages::PageBody;
    let get_req = |key: &str| -> Result<Option<Value>, ApiError> {
        match obj.get(key) {
            None | Some(Value::Null) if !require_all => Ok(None),
            None if require_all => Err(ApiError::invalid_param(key, "this field is required")),
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    };
    // `title`: required string 1–256 (create); optional on update.
    let title = match get_req("title")? {
        None => None,
        Some(Value::String(s)) => {
            let n = s.chars().count();
            if n == 0 || n > 256 {
                return Err(ApiError::invalid_param("title", "must be 1..=256 chars"));
            }
            Some(s)
        }
        Some(_) => return Err(ApiError::invalid_param("title", "must be a string")),
    };
    // `name`: required slug 1–256 + reserved-char rules (create).
    let name = match get_req("name")? {
        None => None,
        Some(Value::String(s)) => {
            crate::domain::page::validate_page_name(&s).map_err(|_| {
                ApiError::invalid_param("name", "must be 1..=256 chars without reserved characters")
            })?;
            Some(s)
        }
        Some(_) => return Err(ApiError::invalid_param("name", "must be a string")),
    };
    let summary = match obj.get("summary") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(s)) => {
            if s.chars().count() > 256 {
                return Err(ApiError::invalid_param("summary", "must be <=256 chars"));
            }
            Some(Some(s.clone()))
        }
        Some(_) => {
            return Err(ApiError::invalid_param(
                "summary",
                "must be a string or null",
            ));
        }
    };
    // `content`: required array of `{type: string, ...}` blocks (create).
    let content = match get_req("content")? {
        None => None,
        Some(Value::Array(blocks)) => {
            for block in &blocks {
                let ok = block
                    .as_object()
                    .is_some_and(|o| o.get("type").and_then(Value::as_str).is_some());
                if !ok {
                    return Err(ApiError::invalid_param(
                        "content",
                        "blocks must be objects with a string type",
                    ));
                }
            }
            Some(Value::Array(blocks))
        }
        Some(_) => return Err(ApiError::invalid_param("content", "must be an array")),
    };
    // `variables`: required array (create).
    let variables = match get_req("variables")? {
        None => None,
        Some(Value::Array(vars)) => Some(Value::Array(vars)),
        Some(_) => return Err(ApiError::invalid_param("variables", "must be an array")),
    };
    // `script`: required string, empty allowed (create).
    let script = match get_req("script")? {
        None => None,
        Some(Value::String(s)) => {
            if s.chars().count() > crate::domain::page::PAGE_SCRIPT_MAX {
                return Err(ApiError::invalid_param("script", "must be <=16384 chars"));
            }
            Some(s)
        }
        Some(_) => return Err(ApiError::invalid_param("script", "must be a string")),
    };
    let eye_catching_image_id = match obj.get("eyeCatchingImageId") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(s)) => Some(Some(s.clone())),
        Some(_) => {
            return Err(ApiError::invalid_param(
                "eyeCatchingImageId",
                "must be a string or null",
            ));
        }
    };
    let font = match opt_string(obj, "font")? {
        None => None,
        Some(f) if f == "serif" || f == "sans-serif" => Some(f),
        Some(_) => {
            return Err(ApiError::invalid_param(
                "font",
                "must be serif or sans-serif",
            ));
        }
    };
    let visibility = opt_string(obj, "visibility")?;
    if let Some(v) = &visibility {
        crate::domain::collection::validate_tier_name(v)
            .map_err(|_| ApiError::invalid_param("visibility", "unknown tier"))?;
    }
    let page_visibility = opt_string(obj, "pageVisibility")?;
    if let Some(v) = &page_visibility {
        if !matches!(v.as_str(), "public" | "home" | "followers" | "specified") {
            return Err(ApiError::invalid_param(
                "pageVisibility",
                "must be public|home|followers|specified",
            ));
        }
    }
    let visible_user_ids = opt_str_array(obj, "visibleUserIds")?;
    Ok(PageBody {
        title,
        name,
        summary,
        content,
        variables,
        script,
        eye_catching_image_id,
        font,
        align_center: update_bool_field(obj, "alignCenter")?,
        hide_title_when_pinned: update_bool_field(obj, "hideTitleWhenPinned")?,
        visibility,
        page_visibility,
        visible_user_ids,
        federate: update_bool_field(obj, "federate")?,
        approved_actor_uris: opt_str_array(obj, "approvedActorUris")?,
    })
}
