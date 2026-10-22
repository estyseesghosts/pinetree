//! Admin dispatcher arms (roles).
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

/// Pack one role row (admin `roles/*` shape).
pub(crate) fn pack_role_record(record: &crate::domain::admin::RoleRecord) -> Value {
    serde_json::json!({
        "id": record.id,
        "name": record.name,
        "description": record.description,
        "color": record.color,
        "iconUrl": record.icon_url,
        "target": record.target,
        "condFormula": record.cond_formula,
        "isPublic": record.is_public,
        "asBadge": record.as_badge,
        "canEditMembersByModerator": record.can_edit_members_by_moderator,
        "displayOrder": record.display_order,
        "policies": record.policies,
    })
}

/// `POST /api/admin/roles/create` (split from `api_dispatch_blocking`).
pub(crate) fn admin_roles_create(
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
            "name",
            "description",
            "color",
            "iconUrl",
            "target",
            "condFormula",
            "isPublic",
            "asBadge",
            "canEditMembersByModerator",
            "displayOrder",
            "policies",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:roles")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/roles/create")?;
    let name = required_str(&obj, "name")?;
    let record = state
        .services
        .role_create(
            &name,
            opt_string(&obj, "description")?.as_deref().unwrap_or(""),
            opt_string(&obj, "color")?,
            opt_string(&obj, "iconUrl")?,
            opt_string(&obj, "target")?.as_deref().unwrap_or("manual"),
            obj.get("condFormula")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
            opt_bool(&obj, "isPublic", false)?,
            opt_bool(&obj, "asBadge", false)?,
            opt_bool(&obj, "canEditMembersByModerator", false)?,
            opt_i64(&obj, "displayOrder")?.unwrap_or(0),
            obj.get("policies")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_role_record(&record)).into_response())
}

/// `POST /api/admin/roles/list` (split from `api_dispatch_blocking`).
pub(crate) fn admin_roles_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:roles")?;
    require_moderator(&caller)?;
    let rows = state
        .services
        .role_list()
        .map_err(ApiError::from_port_error)?;
    let out: Vec<Value> = rows.iter().map(pack_role_record).collect();
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/admin/roles/show` (split from `api_dispatch_blocking`).
pub(crate) fn admin_roles_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["roleId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:roles")?;
    require_moderator(&caller)?;
    let role_id = required_str(&obj, "roleId")?;
    let record = state
        .services
        .role_show(&role_id)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_role_record(&record)).into_response())
}

/// `POST /api/admin/roles/delete` (split from `api_dispatch_blocking`).
pub(crate) fn admin_roles_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["roleId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:roles")?;
    require_admin(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/roles/delete")?;
    let role_id = required_str(&obj, "roleId")?;
    state
        .services
        .role_delete(&role_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/roles/update` (split from `api_dispatch_blocking`).
pub(crate) fn admin_roles_update(
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
            "roleId",
            "name",
            "description",
            "color",
            "iconUrl",
            "target",
            "condFormula",
            "isPublic",
            "asBadge",
            "canEditMembersByModerator",
            "displayOrder",
            "policies",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:roles")?;
    require_moderator(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/roles/update")?;
    let role_id = required_str(&obj, "roleId")?;
    let name = required_str(&obj, "name")?;
    let record = state
        .services
        .role_update(
            &role_id,
            &name,
            opt_string(&obj, "description")?.as_deref().unwrap_or(""),
            opt_string(&obj, "color")?,
            opt_string(&obj, "iconUrl")?,
            opt_string(&obj, "target")?.as_deref().unwrap_or("manual"),
            obj.get("condFormula")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
            opt_bool(&obj, "isPublic", false)?,
            opt_bool(&obj, "asBadge", false)?,
            opt_bool(&obj, "canEditMembersByModerator", false)?,
            opt_i64(&obj, "displayOrder")?.unwrap_or(0),
            obj.get("policies")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_role_record(&record)).into_response())
}

/// `POST /api/admin/roles/assign` (split from `api_dispatch_blocking`).
pub(crate) fn admin_roles_assign(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["roleId", "userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:roles")?;
    require_admin(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/roles/assign")?;
    let role_id = required_str(&obj, "roleId")?;
    let user_id = required_str(&obj, "userId")?;
    state
        .services
        .role_assign(&role_id, &user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/roles/unassign` (split from `api_dispatch_blocking`).
pub(crate) fn admin_roles_unassign(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["roleId", "userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:roles")?;
    require_admin(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/roles/unassign")?;
    let role_id = required_str(&obj, "roleId")?;
    let user_id = required_str(&obj, "userId")?;
    state
        .services
        .role_unassign(&role_id, &user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/roles/users` (split from `api_dispatch_blocking`).
pub(crate) fn admin_roles_users(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["roleId", "limit", "offset"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:roles")?;
    require_moderator(&caller)?;
    let role_id = required_str(&obj, "roleId")?;
    let limit = get_limit_with_default(&obj, 10)?;
    let offset = opt_usize(&obj, "offset")?.unwrap_or(0);
    let accounts = state
        .services
        .role_users(&role_id, limit, offset)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(accounts.len());
    for account in &accounts {
        out.push(pack_account_for(account, Some(&caller), &state.services)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}
