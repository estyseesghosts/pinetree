//! Shared request parsing, validation and packing helpers (errors).
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

/// Map a list-op [`PortError`] to the caller's per-endpoint verbatim
/// upstream error (Batch O WP16: one UUID per endpoint per code).
pub(crate) fn map_list_error(err: PortError, endpoint: &str) -> ApiError {
    match &err {
        PortError::NotFound(msg) if msg == "NO_SUCH_LIST" => {
            ApiError::user_list_no_such_list(endpoint)
        }
        PortError::NotFound(msg) if msg == "NO_SUCH_USER" => {
            ApiError::user_list_no_such_user(endpoint)
        }
        PortError::Validation(msg) if msg == "YOU_HAVE_BEEN_BLOCKED" => {
            ApiError::user_list_blocked()
        }
        PortError::Conflict(msg) if msg == "ALREADY_ADDED" => ApiError::user_list_already_added(),
        PortError::Conflict(msg) if msg == "TOO_MANY_USERS" => ApiError::user_list_too_many_users(),
        PortError::Conflict(msg) if msg == "TOO_MANY_USERLISTS" => ApiError::user_list_too_many(),
        _ => ApiError::from_port_error(err),
    }
}

/// Map a clip-op [`PortError`] to the caller's per-endpoint verbatim
/// upstream error (federated clips + pages handoff, Phase 1: one UUID per
/// endpoint per code).
pub(crate) fn map_clip_error(err: PortError, endpoint: &str) -> ApiError {
    match &err {
        PortError::NotFound(msg) if msg == "NO_SUCH_CLIP" => ApiError::clip_no_such_clip(endpoint),
        PortError::NotFound(msg) if msg == "NO_SUCH_NOTE" => ApiError::clip_no_such_note(endpoint),
        PortError::Conflict(msg) if msg == "TOO_MANY_CLIPS" => ApiError::clip_too_many_clips(),
        PortError::Conflict(msg) if msg == "ALREADY_CLIPPED" => ApiError::clip_already_clipped(),
        PortError::Conflict(msg) if msg == "TOO_MANY_CLIP_NOTES" => {
            ApiError::clip_too_many_clip_notes()
        }
        PortError::Conflict(msg) if msg == "ALREADY_FAVORITED" => {
            ApiError::clip_already_favorited()
        }
        PortError::Conflict(msg) if msg == "NOT_FAVORITED" => ApiError::clip_not_favorited(),
        _ => ApiError::from_port_error(err),
    }
}

/// Map a page-op [`PortError`] to the caller's per-endpoint verbatim
/// upstream error (federated clips + pages handoff, Phase 3 local: one
/// UUID per endpoint per code). `NO_SUCH_FILE`/`NAME_ALREADY_EXISTS` take
/// the create/update arm by endpoint; `ACCESS_DENIED` takes update/delete.
pub(crate) fn map_page_error(err: PortError, endpoint: &str) -> ApiError {
    match &err {
        PortError::NotFound(msg) if msg == "NO_SUCH_PAGE" => ApiError::page_no_such_page(endpoint),
        PortError::NotFound(msg) if msg == "NO_SUCH_FILE" => ApiError::page_no_such_file(endpoint),
        PortError::Conflict(msg) if msg == "NAME_ALREADY_EXISTS" => {
            ApiError::page_name_already_exists(endpoint)
        }
        PortError::Conflict(msg) if msg == "ALREADY_LIKED" => ApiError::page_already_liked(),
        PortError::Conflict(msg) if msg == "NOT_LIKED" => ApiError::page_not_liked(),
        PortError::Validation(msg) if msg == "YOUR_PAGE" => ApiError::page_your_page(),
        PortError::Validation(msg) if msg == "ACCESS_DENIED" => {
            ApiError::page_access_denied(endpoint)
        }
        _ => ApiError::from_port_error(err),
    }
}

/// Map an `AppServices::check_import_file` failure to the caller's
/// per-endpoint verbatim upstream error (`i/import-*.ts` parity: each
/// endpoint mints its own UUID per code).
pub(crate) fn map_import_file_error(err: PortError, endpoint: &str) -> ApiError {
    match err {
        PortError::NotFound(_) => ApiError::import_no_such_file(endpoint),
        PortError::Validation(code) if code == "EMPTY_FILE" => {
            ApiError::import_empty_file(endpoint)
        }
        PortError::Validation(code) if code == "TOO_BIG_FILE" => {
            ApiError::import_too_big_file(endpoint)
        }
        other => ApiError::from_port_error(other),
    }
}

/// Map a gallery/flash op [`PortError`] to the caller's verbatim upstream
/// error. `NO_SUCH_POST` / `NO_SUCH_FLASH` / `ACCESS_DENIED` flow through
/// [`ApiError::from_port_error`]; the like twins stay out of the shared
/// lookup (so the pages `ALREADY_LIKED` UUID is untouched) via the
/// `gallery_*` constructors.
pub(crate) fn map_gallery_error(err: PortError) -> ApiError {
    match &err {
        PortError::Conflict(msg) if msg == "ALREADY_LIKED" => ApiError::gallery_already_liked(),
        PortError::Conflict(msg) if msg == "NOT_LIKED" => ApiError::gallery_not_liked(),
        _ => ApiError::from_port_error(err),
    }
}

/// Map a drive file-extra op [`PortError`] to the caller's per-endpoint
/// verbatim upstream error (`NO_SUCH_FILE` carries one UUID per endpoint).
pub(crate) fn map_drive_extra_error(err: PortError, endpoint: &str) -> ApiError {
    match &err {
        PortError::NotFound(msg) if msg == "NO_SUCH_FILE" => ApiError::drive_no_such_file(endpoint),
        _ => ApiError::from_port_error(err),
    }
}

/// Map drive-folder store errors to the per-endpoint Misskey envelopes
/// (gaps WP2a final wiring). Verbatim codes; unknown shapes fall through
/// to the generic lookup.
pub(crate) fn map_drive_folder_error(err: PortError, endpoint: &str) -> ApiError {
    match &err {
        PortError::NotFound(msg) if msg == "NO_SUCH_FOLDER" => {
            ApiError::folder_no_such_folder(endpoint)
        }
        PortError::NotFound(msg) if msg == "NO_SUCH_PARENT_FOLDER" => {
            ApiError::folder_update_parent_error("NO_SUCH_PARENT_FOLDER")
        }
        PortError::Validation(msg) if msg == "RECURSIVE_NESTING" => {
            ApiError::folder_update_parent_error("RECURSIVE_NESTING")
        }
        PortError::Conflict(msg) if msg == "HAS_CHILD_FILES_OR_FOLDERS" => {
            ApiError::folder_delete_has_child()
        }
        _ => ApiError::from_port_error(err),
    }
}

/// Map webhook store errors to the per-endpoint Misskey envelopes (gaps
/// WP3 final wiring). The store layer reports lowercase
/// `no such webhook`; the limit conflict maps to the pinetree-defined
/// `TOO_MANY_WEBHOOKS`.
pub(crate) fn map_webhook_error(err: PortError, endpoint: &str) -> ApiError {
    match &err {
        PortError::NotFound(msg) if msg == "no such webhook" => {
            ApiError::webhook_no_such_webhook(endpoint)
        }
        PortError::Conflict(msg) if msg.starts_with("too many webhooks") => {
            ApiError::webhook_too_many()
        }
        _ => ApiError::from_port_error(err),
    }
}
