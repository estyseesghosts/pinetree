//! Admin dispatcher arms (accounts).
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

/// `POST /api/admin/accounts/create` (split from `api_dispatch_blocking`).
pub(crate) fn admin_accounts_create(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["username", "password", "setupPassword"])?;
    let caller = require_caller(caller)?;
    require_admin(&caller)?;
    let username = required_str(&obj, "username")?;
    let password = required_str(&obj, "password")?;
    let hash = tokio::runtime::Handle::current().block_on(hash_password_async(password))?;
    let account = state
        .services
        .create_account_admin(&username, Some(&hash), &caller.account.id)
        .map_err(ApiError::from_port_error)?;
    let raw_token = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&raw_token),
            account_id: account.id.clone(),
            permissions: Vec::new(),
            is_first_party: true,
            name: None,
            description: None,
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .map_err(ApiError::from_port_error)?;
    let mut packed =
        pack_me_detailed(&account, true, &state.services).map_err(ApiError::from_port_error)?;
    if let Some(map) = packed.as_object_mut() {
        map.insert("token".to_string(), Value::String(raw_token));
    }
    Ok(Json(packed).into_response())
}

/// `POST /api/admin/show-user` (split from `api_dispatch_blocking`).
pub(crate) fn admin_show_user(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    require_moderator(&caller)?;
    let user_id = required_str(&obj, "userId")?;
    let account = state
        .services
        .accounts
        .find_by_id(&user_id)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(|| {
            ApiError::from_port_error(PortError::NotFound("NO_SUCH_USER".to_string()))
        })?;
    let report_count = state
        .services
        .abuse_reports
        .count_for_target(&user_id)
        .map_err(ApiError::from_port_error)?;
    let mut packed = if account.id == caller.account.id {
        pack_me_detailed(&account, caller.is_first_party, &state.services)
            .map_err(ApiError::from_port_error)?
    } else {
        pack_user_detailed_not_me(&account, Some(&caller.account.id), &state.services)
            .map_err(ApiError::from_port_error)?
    };
    if let Some(o) = packed.as_object_mut() {
        o.insert("reportCount".to_string(), serde_json::json!(report_count));
        o.insert(
            "moderationNote".to_string(),
            serde_json::json!(account.moderator_note),
        );
    }
    Ok(Json(packed).into_response())
}

/// `POST /api/admin/show-users` (split from `api_dispatch_blocking`).
pub(crate) fn admin_show_users(
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
            "limit", "offset", "state", "origin", "username", "hostname", "sort",
        ],
    )?;
    let caller = require_caller(caller)?;
    require_moderator(&caller)?;
    let limit = get_limit(&obj)?;
    let offset = opt_usize(&obj, "offset")?.unwrap_or(0);
    let state_filter = opt_string(&obj, "state")?.unwrap_or_else(|| "all".to_string());
    let origin = opt_string(&obj, "origin")?.unwrap_or_else(|| "combined".to_string());
    let username_filter = opt_string(&obj, "username")?;
    let hostname_filter = opt_string(&obj, "hostname")?;
    // Enumerate accounts bounded (all, including suspended/deleted).
    let mut accounts = Vec::new();
    let mut cursor = None;
    loop {
        let (page, next) = state
            .services
            .accounts
            .all_accounts(cursor.as_deref(), 1000)
            .map_err(ApiError::from_port_error)?;
        accounts.extend(page);
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
        if accounts.len() >= 10_000 {
            break;
        }
    }
    // Filter.
    let mut filtered: Vec<_> = accounts
        .into_iter()
        .filter(|a| {
            if a.is_deleted {
                return false;
            }
            match state_filter.as_str() {
                "available" => !a.is_suspended,
                "suspended" => a.is_suspended,
                "admin" => a.is_admin,
                "moderator" => a.is_moderator && !a.is_admin,
                "adminOrModerator" => a.is_admin || a.is_moderator,
                _ => true,
            }
        })
        .filter(|a| match origin.as_str() {
            "local" => a.host.is_none(),
            "remote" => a.host.is_some(),
            _ => true,
        })
        .filter(|a| {
            if let Some(u) = &username_filter {
                a.username_lower.starts_with(&u.to_lowercase())
            } else {
                true
            }
        })
        .filter(|a| {
            if let Some(h) = &hostname_filter {
                a.host.as_deref() == Some(h.as_str())
            } else {
                true
            }
        })
        .collect();
    // sort by id asc default
    filtered.sort_by(|a, b| a.id.cmp(&b.id));
    let page: Vec<_> = filtered.into_iter().skip(offset).take(limit).collect();
    let mut out = Vec::new();
    for a in page {
        let rc = state
            .services
            .abuse_reports
            .count_for_target(&a.id)
            .map_err(ApiError::from_port_error)?;
        let mut packed = pack_user_detailed_not_me(&a, Some(&caller.account.id), &state.services)
            .map_err(ApiError::from_port_error)?;
        if let Some(o) = packed.as_object_mut() {
            o.insert("reportCount".to_string(), serde_json::json!(rc));
            o.insert(
                "moderationNote".to_string(),
                serde_json::json!(a.moderator_note),
            );
        }
        out.push(packed);
    }
    Ok(Json(serde_json::json!(out)).into_response())
}

/// `POST /api/admin/update-user-note` (split from `api_dispatch_blocking`).
pub(crate) fn admin_update_user_note(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "text"])?;
    let caller = require_caller(caller)?;
    require_moderator(&caller)?;
    let user_id = required_str(&obj, "userId")?;
    let text = required_str(&obj, "text")?;
    let account = state
        .services
        .update_user_note(&caller.account.id, &user_id, &text)
        .map_err(ApiError::from_port_error)?;
    let packed = pack_user_detailed_not_me(&account, Some(&caller.account.id), &state.services)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/admin/delete-account` (split from `api_dispatch_blocking`).
pub(crate) fn admin_delete_account(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    require_admin(&caller)?;
    let user_id = required_str(&obj, "userId")?;
    state
        .services
        .delete_account(Some(&caller.account.id), &user_id, None)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(serde_json::json!({})).into_response())
}

/// `POST /api/admin/delete-all-files-of-a-user` (split from `api_dispatch_blocking`).
pub(crate) fn admin_delete_all_files_of_a_user(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:delete-all-files-of-a-user")?;
    require_admin(&caller)?;
    let user_id = required_str(&obj, "userId")?;
    state
        .services
        .delete_all_files_of_a_user(&caller.account.id, &user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

// --- Batch M WP3: suspension ---------------------------------
// (upstream `admin/suspend-user.ts` / `admin/unsuspend-user.ts`:
// `credential` + `write:admin:suspend-user` / `write:admin:unsuspend-user`
// + the moderator role; `res` is void -> 204.) The moderator-target
// refusal (upstream's own `cannot suspend moderator account`,
// `suspend-user.ts:46`), the `NO_SUCH_USER` miss, the `suspend`/
// `unsuspend` log row, and the bounded sweep jobs live in the
// application command - one transaction (S2.3). DIVERGENCE: the
// refusal is applied to `unsuspend-user` too (guide S3.1 blanket
// rule); upstream `unsuspend-user.ts` has no check there, and both
// upstream refusals throw untyped `Error`s (`500 INTERNAL_ERROR`
// via `ApiCallService.ts:#onExecError`) while pinetree answers
// `400 INVALID_PARAM` carrying the same message.
/// Shared body for `admin/suspend-user`, `admin/unsuspend-user` (split from `api_dispatch_blocking`).
fn admin_suspend_user_inner(
    state: &ApiState,
    endpoint: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    let suspend = endpoint == "admin/suspend-user";
    caller.require_permission(if suspend {
        "write:admin:suspend-user"
    } else {
        "write:admin:unsuspend-user"
    })?;
    require_moderator(&caller)?;
    let user_id = required_str(&obj, "userId")?;
    let result = if suspend {
        state.services.suspend_user(&caller.account.id, &user_id)
    } else {
        state.services.unsuspend_user(&caller.account.id, &user_id)
    };
    result.map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/suspend-user` (split from `api_dispatch_blocking`).
pub(crate) fn admin_suspend_user(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_suspend_user_inner(
        state,
        "admin/suspend-user",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}

/// `POST /api/admin/unsuspend-user` (split from `api_dispatch_blocking`).
pub(crate) fn admin_unsuspend_user(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_suspend_user_inner(
        state,
        "admin/unsuspend-user",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}

// --- Batch N WS6: moderation/admin remainder -----------------------
// `admin/unset-user-avatar|banner` (moderator + scope; refusal rule;
// unset log rows). Everything else in `admin/*` stays out - see the
// manifest S30 audit table for the per-endpoint verdict + reason.
/// Shared body for `admin/unset-user-avatar`, `admin/unset-user-banner` (split from `api_dispatch_blocking`).
fn admin_unset_user_avatar_inner(
    state: &ApiState,
    endpoint: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    let avatar = endpoint == "admin/unset-user-avatar";
    caller.require_permission(if avatar {
        "write:admin:unset-user-avatar"
    } else {
        "write:admin:unset-user-banner"
    })?;
    require_moderator(&caller)?;
    let user_id = required_str(&obj, "userId")?;
    state
        .services
        .unset_user_media(&caller.account.id, &user_id, avatar)
        .map_err(|e| match e {
            // Guide §3.1 blanket rule (DIVERGENCE: upstream checks
            // the role only): moderators cannot act on admins or
            // other moderators.
            PortError::Validation(code) if code == "cannot moderate moderator account" => {
                ApiError::invalid_param("userId", &code)
            }
            other => ApiError::from_port_error(other),
        })?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/unset-user-avatar` (split from `api_dispatch_blocking`).
pub(crate) fn admin_unset_user_avatar(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_unset_user_avatar_inner(
        state,
        "admin/unset-user-avatar",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}

/// `POST /api/admin/unset-user-banner` (split from `api_dispatch_blocking`).
pub(crate) fn admin_unset_user_banner(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_unset_user_avatar_inner(
        state,
        "admin/unset-user-banner",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}

/// `POST /api/admin/reset-password` (split from `api_dispatch_blocking`).
pub(crate) fn admin_reset_password(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "password"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:users")?;
    require_admin(&caller)?;
    state
        .rate_limiter
        .check(&rate_key, "admin/reset-password")?;
    let user_id = required_str(&obj, "userId")?;
    let password = required_str(&obj, "password")?;
    if password.is_empty() {
        return Err(ApiError::invalid_param(
            "password",
            "must be a non-empty string",
        ));
    }
    let hash = tokio::runtime::Handle::current().block_on(hash_password_async(password))?;
    state
        .services
        .admin_reset_password(&user_id, &hash)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/unset-mfa` (split from `api_dispatch_blocking`).
pub(crate) fn admin_unset_mfa(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:users")?;
    require_admin(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/unset-mfa")?;
    let user_id = required_str(&obj, "userId")?;
    state
        .services
        .admin_unset_2fa(&user_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/admin/send-email` (split from `api_dispatch_blocking`).
pub(crate) fn admin_send_email(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["to", "subject", "text"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:admin:email")?;
    require_admin(&caller)?;
    state.rate_limiter.check(&rate_key, "admin/send-email")?;
    if EMAIL_SENDER.get().is_none() {
        return Err(ApiError::email_not_configured());
    }
    let to = required_str(&obj, "to")?;
    if !email_shape_valid(&to) {
        return Err(ApiError::invalid_param("to", "must be an email address"));
    }
    let subject = required_str(&obj, "subject")?;
    let text = required_str(&obj, "text")?;
    let now = state.services.clock.now_ms();
    let payload = EmailSendPayload {
        to: vec![to.clone()],
        subject,
        body: text,
        dedup_key: format!("admin-send:{}:{now}", caller.account.id),
    };
    let job = NewJob::new_json(
        JobKind::EmailSend,
        &payload,
        format!("email:admin-send:{}:{now}", caller.account.id),
        now,
    )
    .map_err(ApiError::from_port_error)?;
    state
        .services
        .jobs
        .enqueue_jobs(vec![job])
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

// `admin/get-user-ips` + `federation/get-user-ips {userId!,
// limit?}`: recent signin IPs for one account, newest-first.
/// Shared body for `admin/get-user-ips`, `federation/get-user-ips` (split from `api_dispatch_blocking`).
fn admin_get_user_ips_inner(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "limit"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:users")?;
    require_moderator(&caller)?;
    let user_id = required_str(&obj, "userId")?;
    let limit = get_limit_with_default(&obj, 10)?;
    let rows = state
        .services
        .get_user_ips(&user_id, limit)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(Value::Array(rows)).into_response())
}

/// `POST /api/admin/get-user-ips` (split from `api_dispatch_blocking`).
pub(crate) fn admin_get_user_ips(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_get_user_ips_inner(state, obj, caller, rate_key, client_ip, headers)
}

/// `POST /api/federation/get-user-ips` (split from `api_dispatch_blocking`).
pub(crate) fn federation_get_user_ips(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_get_user_ips_inner(state, obj, caller, rate_key, client_ip, headers)
}

// `admin/accounts/find-by-email` + `federation/find-by-email
// {email}`: exact (case-insensitive) match, or null.
/// Shared body for `admin/accounts/find-by-email`, `federation/find-by-email` (split from `api_dispatch_blocking`).
fn admin_accounts_find_by_email_inner(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["email"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:users")?;
    require_moderator(&caller)?;
    let email = required_str(&obj, "email")?;
    let found = state
        .services
        .find_account_by_email(&email)
        .map_err(ApiError::from_port_error)?;
    match found {
        Some(account) => {
            Ok(Json(pack_account_for(&account, Some(&caller), &state.services)?).into_response())
        }
        None => Ok(Json(Value::Null).into_response()),
    }
}

/// `POST /api/admin/accounts/find-by-email` (split from `api_dispatch_blocking`).
pub(crate) fn admin_accounts_find_by_email(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_accounts_find_by_email_inner(state, obj, caller, rate_key, client_ip, headers)
}

/// `POST /api/federation/find-by-email` (split from `api_dispatch_blocking`).
pub(crate) fn federation_find_by_email(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    admin_accounts_find_by_email_inner(state, obj, caller, rate_key, client_ip, headers)
}
