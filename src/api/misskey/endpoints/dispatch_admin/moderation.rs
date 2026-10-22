//! Admin dispatcher arms (moderation).
//!
//! Split from `endpoints/dispatch_admin.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::admin_status::{STATUS_BACKUP_LIST_CAP, build_admin_status, pack_backup_status};
use super::super::auth_http::{EMAIL_SENDER, client_ip, require_caller};
use super::super::middleware::{read_backup_context, read_ops_config};
use super::super::parse::{
    email_shape_valid, get_limit, get_limit_with_default, opt_bool, opt_i64, opt_string, opt_usize,
    pack_account_for, pack_invite_codes, parse_ad_ratio, parse_cursor, parse_invite_expires_at,
    parse_opt_bool, required_i64, required_str, update_bool_field,
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

/// `POST /api/admin/reject-quotes` (split from `api_dispatch_blocking`).
pub(crate) fn admin_reject_quotes(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "rejectQuotes"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:reject-quotes")?;
    require_moderator(&caller)?;
    let user_id = required_str(&obj, "userId")?;
    let reject = match obj.get("rejectQuotes") {
        Some(Value::Bool(b)) => *b,
        _ => {
            return Err(ApiError::invalid_param("rejectQuotes", "must be a boolean"));
        }
    };
    let account = state
        .services
        .set_reject_quotes(&caller.account.id, &user_id, reject)
        .map_err(ApiError::from_port_error)?;
    let packed = pack_me_detailed(&account, caller.is_first_party, &state.services)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/admin/show-moderation-logs` (split from `api_dispatch_blocking`).
pub(crate) fn admin_show_moderation_logs(
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
            "type",
            "userId",
            "search",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:show-moderation-log")?;
    require_moderator(&caller)?;
    let cursor = TimelineCursor {
        since_id: opt_string(&obj, "sinceId")?,
        until_id: opt_string(&obj, "untilId")?,
        since_date: opt_i64(&obj, "sinceDate")?,
        until_date: opt_i64(&obj, "untilDate")?,
        limit: get_limit(&obj)?,
    };
    // An unknown `type` name matches no rows upstream
    // (`log.type = :type`); answer `[]` the same way.
    let action = match obj.get("type") {
        None | Some(Value::Null) => None,
        Some(Value::String(name)) => match ModerationAction::parse(name) {
            Some(action) => Some(action),
            None => return Ok(Json(Value::Array(Vec::new())).into_response()),
        },
        Some(_) => return Err(ApiError::invalid_param("type", "must be a string")),
    };
    let filter = ModerationLogFilter {
        action,
        actor: opt_string(&obj, "userId")?,
        target: None,
        search: opt_string(&obj, "search")?,
    };
    let rows = state
        .services
        .list_moderation_logs(filter, &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut packed = Vec::with_capacity(rows.len());
    for row in &rows {
        packed.push(
            pack_moderation_log(row, Some(&caller.account.id), &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(packed)).into_response())
}

/// `POST /api/admin/abuse-user-reports` (split from `api_dispatch_blocking`).
pub(crate) fn admin_abuse_user_reports(
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
            "state",
            "reporterOrigin",
            "targetUserOrigin",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:abuse-user-reports")?;
    require_moderator(&caller)?;
    let cursor = parse_cursor(&obj)?;
    let filter = AbuseReportFilter {
        state: opt_string(&obj, "state")?,
        reporter_origin: opt_string(&obj, "reporterOrigin")?
            .unwrap_or_else(|| "combined".to_string()),
        target_origin: opt_string(&obj, "targetUserOrigin")?
            .unwrap_or_else(|| "combined".to_string()),
    };
    let rows = state
        .services
        .list_abuse_reports(filter, &cursor)
        .map_err(ApiError::from_port_error)?;
    let viewer_id = Some(caller.account.id.as_str());
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(
            pack_abuse_report(row, viewer_id, &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/admin/resolve-abuse-user-report` (split from `api_dispatch_blocking`).
pub(crate) fn admin_resolve_abuse_user_report(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["reportId", "resolvedAs"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:resolve-abuse-user-report")?;
    require_moderator(&caller)?;
    let report_id = required_str(&obj, "reportId")?;
    let resolved_as = match opt_string(&obj, "resolvedAs")? {
        None => None,
        Some(s) => match AbuseReportResolution::parse(&s) {
            Some(r) => Some(r),
            None => {
                return Err(ApiError::invalid_param(
                    "resolvedAs",
                    "must be accept, reject, or null",
                ));
            }
        },
    };
    state
        .services
        .resolve_abuse_report(&caller.account.id, &report_id, resolved_as)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/update-abuse-user-report` (split from `api_dispatch_blocking`).
pub(crate) fn admin_update_abuse_user_report(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["reportId", "moderationNote"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:resolve-abuse-user-report")?;
    require_moderator(&caller)?;
    let report_id = required_str(&obj, "reportId")?;
    // Upstream: `moderationNote` optional (absent clears to null;
    // pinetree stores `""`).
    let note = opt_string(&obj, "moderationNote")?.unwrap_or_default();
    state
        .services
        .update_abuse_report_note(&caller.account.id, &report_id, note)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/forward-abuse-user-report` (split from `api_dispatch_blocking`).
pub(crate) fn admin_forward_abuse_user_report(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["reportId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:resolve-abuse-user-report")?;
    require_moderator(&caller)?;
    let report_id = required_str(&obj, "reportId")?;
    state
        .services
        .forward_abuse_report(&caller.account.id, &report_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
