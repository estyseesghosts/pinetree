//! Following/clips/pages/antennas/invites dispatcher arms (invites).
//!
//! Split from `endpoints/dispatch_social.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{client_ip, require_caller};
use super::super::parse::{
    get_limit_with_default, map_clip_error, map_page_error, opt_antenna_src, opt_bool,
    opt_bool_opt, opt_bounded_string, opt_i64, opt_nullable_string, opt_str_array,
    opt_str_array_array, opt_string, pack_invite_codes, parse_cursor, parse_follow_notify,
    parse_page_body, required_antenna_src, required_bool, required_str, required_str_array,
    required_str_array_array, required_string, update_string_field,
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

/// `POST /api/invite/create` (split from `api_dispatch_blocking`).
pub(crate) fn invite_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:invite-codes")?;
    require_moderator(&caller)?;
    let codes = state
        .services
        .create_invites(
            &caller.account.id,
            1,
            None,
            state.services.clock.now_ms(),
            false,
        )
        .map_err(ApiError::from_port_error)?;
    let packed = pack_invite_codes(&state, &codes)?;
    // Single object upstream (`res: ref: InviteCode`).
    let first = packed
        .as_array()
        .and_then(|rows| rows.first())
        .cloned()
        .unwrap_or(Value::Null);
    Ok(Json(first).into_response())
}

/// `POST /api/invite/list` (split from `api_dispatch_blocking`).
pub(crate) fn invite_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:invite-codes")?;
    require_moderator(&caller)?;
    let filter = InviteListFilter {
        since_id: opt_string(&obj, "sinceId")?,
        until_id: opt_string(&obj, "untilId")?,
        since_date_ms: opt_i64(&obj, "sinceDate")?,
        until_date_ms: opt_i64(&obj, "untilDate")?,
        limit: get_limit_with_default(&obj, 30)?,
        ..Default::default()
    };
    let invites = state
        .services
        .list_my_invites(&caller.account.id, filter)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_invite_codes(&state, &invites)?).into_response())
}

/// `POST /api/invite/delete` (split from `api_dispatch_blocking`).
pub(crate) fn invite_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["inviteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:invite-codes")?;
    require_moderator(&caller)?;
    let invite_id = required_str(&obj, "inviteId")?;
    state
        .services
        .delete_invite(
            &invite_id,
            &caller.account.id,
            caller.account.has_moderator_powers(),
        )
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/invite/limit` (split from `api_dispatch_blocking`).
pub(crate) fn invite_limit(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:invite-codes")?;
    require_moderator(&caller)?;
    let remaining = state.services.invite_limit(&caller.account.id);
    Ok(Json(serde_json::json!({ "remaining": remaining })).into_response())
}
