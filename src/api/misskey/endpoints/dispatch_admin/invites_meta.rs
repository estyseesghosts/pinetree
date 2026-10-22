//! Admin dispatcher arms (invites_meta).
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

/// `POST /api/admin/invite/create` (split from `api_dispatch_blocking`).
pub(crate) fn admin_invite_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["count", "expiresAt"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:invite-codes")?;
    require_moderator(&caller)?;
    let count = opt_usize(&obj, "count")?.unwrap_or(1);
    if count == 0 || count > 100 {
        return Err(ApiError::invalid_param(
            "count",
            "must be an integer between 1 and 100",
        ));
    }
    let expires_at_ms = match obj.get("expiresAt") {
        None | Some(Value::Null) => None,
        Some(Value::String(raw)) => Some(parse_invite_expires_at(raw)?),
        Some(_) => {
            return Err(ApiError::invalid_param("expiresAt", "must be a string"));
        }
    };
    let codes = state
        .services
        .create_invites(
            &caller.account.id,
            count,
            expires_at_ms,
            state.services.clock.now_ms(),
            true,
        )
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_invite_codes(&state, &codes)?).into_response())
}

/// `POST /api/admin/invite/list` (split from `api_dispatch_blocking`).
pub(crate) fn admin_invite_list(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "offset", "type", "sort"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:invite-codes")?;
    require_moderator(&caller)?;
    let type_filter = match opt_string(&obj, "type")?.as_deref() {
        None | Some("all") => InviteTypeFilter::All,
        Some("unused") => InviteTypeFilter::Unused,
        Some("used") => InviteTypeFilter::Used,
        Some("expired") => InviteTypeFilter::Expired,
        Some(_) => {
            return Err(ApiError::invalid_param(
                "type",
                "must be one of unused, used, expired, all",
            ));
        }
    };
    let sort = match opt_string(&obj, "sort")?.as_deref() {
        None | Some("+createdAt") => InviteSort::CreatedDesc,
        Some("-createdAt") => InviteSort::CreatedAsc,
        Some("+usedAt") => InviteSort::UsedDesc,
        Some("-usedAt") => InviteSort::UsedAsc,
        Some(_) => {
            return Err(ApiError::invalid_param(
                "sort",
                "must be one of +createdAt, -createdAt, +usedAt, -usedAt",
            ));
        }
    };
    let filter = InviteListFilter {
        filter: type_filter,
        sort,
        limit: get_limit_with_default(&obj, 30)?,
        offset: opt_usize(&obj, "offset")?.unwrap_or(0),
        ..Default::default()
    };
    let invites = state
        .services
        .list_all_invites(filter)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(pack_invite_codes(&state, &invites)?).into_response())
}

/// `POST /api/admin/meta` (split from `api_dispatch_blocking`).
pub(crate) fn admin_meta(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:meta")?;
    require_admin(&caller)?;
    let mut meta = pack_meta(&state.services, true).map_err(ApiError::from_port_error)?;
    let settings = state
        .services
        .instance_settings
        .get()
        .map_err(ApiError::from_port_error)?;
    if let Some(map) = meta.as_object_mut() {
        map.insert(
            "blockedHosts".to_string(),
            serde_json::json!(settings.blocked_hosts),
        );
        // Federated clips + pages handoff: admin federation
        // kill-switches (default OFF).
        map.insert(
            "federateClips".to_string(),
            serde_json::json!(settings.federate_clips),
        );
        map.insert(
            "federatePages".to_string(),
            serde_json::json!(settings.federate_pages),
        );
        // Batch Q Phase 1 item 3: anonymous-visitor UGC visibility
        // (upstream `admin/meta.ts:590-594`).
        map.insert(
            "ugcVisibilityForVisitor".to_string(),
            serde_json::json!(settings.ugc_visibility_for_visitor.as_wire()),
        );
    }
    Ok(Json(meta).into_response())
}

/// `POST /api/admin/update-meta` (split from `api_dispatch_blocking`).
pub(crate) fn admin_update_meta(
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
            "disableRegistration",
            "blockedHosts",
            "federateClips",
            "federatePages",
            "ugcVisibilityForVisitor",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:meta")?;
    require_admin(&caller)?;
    let disable_registration = match obj.get("disableRegistration") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => {
            return Err(ApiError::invalid_param(
                "disableRegistration",
                "must be a boolean",
            ));
        }
    };
    let blocked_hosts = match obj.get("blockedHosts") {
        None | Some(Value::Null) => None,
        Some(Value::Array(items)) => {
            let mut hosts = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(host) => hosts.push(host.clone()),
                    _ => {
                        return Err(ApiError::invalid_param(
                            "blockedHosts",
                            "must be an array of strings",
                        ));
                    }
                }
            }
            Some(hosts)
        }
        Some(_) => {
            return Err(ApiError::invalid_param(
                "blockedHosts",
                "must be an array of strings",
            ));
        }
    };
    state
        .services
        .update_instance_settings(
            &caller.account.id,
            disable_registration,
            blocked_hosts,
            update_bool_field(&obj, "federateClips")?,
            update_bool_field(&obj, "federatePages")?,
            match obj.get("ugcVisibilityForVisitor") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) => {
                    Some(UgcVisibilityForVisitor::parse(s).ok_or_else(|| {
                        ApiError::invalid_param(
                            "ugcVisibilityForVisitor",
                            "must be one of all, local, none",
                        )
                    })?)
                }
                Some(_) => {
                    return Err(ApiError::invalid_param(
                        "ugcVisibilityForVisitor",
                        "must be one of all, local, none",
                    ));
                }
            },
            state.services.clock.now_ms(),
        )
        .map_err(ApiError::from_port_error)?;
    // Upstream has a void res (204), like the export endpoints.
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/status` (split from `api_dispatch_blocking`).
pub(crate) fn admin_status(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["detailed"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:meta")?;
    require_admin(&caller)?;
    let detailed = opt_bool(&obj, "detailed", false)?;
    if detailed && !read_ops_config().detailed_admin_status {
        // Flag off hides the expensive/private fields (H2): same
        // 403 as any other unauthorized admin read.
        return Err(ApiError::permission_denied());
    }
    crate::observability::log_event(
        crate::observability::LogLevel::Info,
        "admin.status",
        &[("detailed", if detailed { "true" } else { "false" })],
    );
    Ok(Json(build_admin_status(&state, detailed)).into_response())
}

/// `POST /api/admin/backup` (split from `api_dispatch_blocking`).
pub(crate) fn admin_backup(
    _state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:meta")?;
    require_admin(&caller)?;
    let context = read_backup_context().ok_or_else(ApiError::internal)?;
    let job_id = crate::backup::request_backup(&context)
        .map_err(|err| ApiError::invalid_param("backup", &err.to_string()))?;
    crate::observability::log_event(
        crate::observability::LogLevel::Info,
        "backup.requested",
        &[("job_id", job_id.as_str())],
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "jobId": job_id })),
    )
        .into_response())
}

/// `POST /api/admin/backups` (split from `api_dispatch_blocking`).
pub(crate) fn admin_backups(
    _state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:meta")?;
    require_admin(&caller)?;
    let context = read_backup_context().ok_or_else(ApiError::internal)?;
    let mut statuses = crate::backup::list_backup_statuses(&context.backup_dir);
    statuses.truncate(STATUS_BACKUP_LIST_CAP * 4);
    let recent: Vec<Value> = statuses
        .iter()
        .rev()
        .take(STATUS_BACKUP_LIST_CAP)
        .map(pack_backup_status)
        .collect();
    Ok(Json(Value::Array(recent)).into_response())
}

/// `POST /api/admin/backup-status` (split from `api_dispatch_blocking`).
pub(crate) fn admin_backup_status(
    _state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["jobId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:meta")?;
    require_admin(&caller)?;
    let context = read_backup_context().ok_or_else(ApiError::internal)?;
    let job_id = required_str(&obj, "jobId")?;
    match crate::backup::backup_status(&context, &job_id) {
        Some(status) => Ok(Json(pack_backup_status(&status)).into_response()),
        None => Err(ApiError::invalid_param("jobId", "unknown backup job")),
    }
}
