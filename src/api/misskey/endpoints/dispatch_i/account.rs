//! I/notifications/registry/webhook dispatcher arms (account).
//!
//! Split from `endpoints/dispatch_i.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{
    auth_ip_key, client_ip, require_caller, send_email_now, set_email_sender,
    verify_and_upgrade_password,
};
use super::super::parse::{
    email_available, email_shape_valid, is_oldest_admin, map_import_file_error, map_webhook_error,
    migrate_followers_to, opt_bool, opt_string, opt_usize, parse_acct, parse_cursor,
    parse_notification_query, parse_registry_scope, random_code, registry_domain, required_str,
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

/// `POST /api/i/apps` (split from `api_dispatch_blocking`).
pub(crate) fn i_apps(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["sort"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/apps")?;
    let sort = match obj.get("sort").and_then(Value::as_str) {
        None => "default",
        Some("+createdAt") | Some("-createdAt") | Some("+lastUsedAt") | Some("-lastUsedAt") => {
            obj.get("sort").and_then(Value::as_str).unwrap()
        }
        Some(_) => {
            return Err(ApiError::invalid_param(
                "sort",
                "must be one of +createdAt, -createdAt, +lastUsedAt, -lastUsedAt",
            ));
        }
    };
    let mut grants = state
        .services
        .auth
        .tokens_for_account(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    grants.retain(|g| !g.is_first_party);
    match sort {
        "-createdAt" | "+lastUsedAt" => {
            grants.sort_by(|a, b| {
                b.created_at_ms
                    .cmp(&a.created_at_ms)
                    .then_with(|| b.id.cmp(&a.id))
            });
        }
        _ => {
            grants.sort_by(|a, b| {
                a.created_at_ms
                    .cmp(&b.created_at_ms)
                    .then_with(|| a.id.cmp(&b.id))
            });
        }
    }
    let out: Vec<Value> = grants
        .iter()
        .map(|g| {
            serde_json::json!({
                "id": g.id,
                "name": g.name,
                "createdAt": crate::activitypub::format_millis(g.created_at_ms),
                "lastUsedAt": null,
                "permission": g.permissions,
                "iconUrl": g.icon_url,
                "description": g.description,
            })
        })
        .collect();
    Ok(Json(out).into_response())
}

/// `POST /api/i/authorized-apps` (split from `api_dispatch_blocking`).
pub(crate) fn i_authorized_apps(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "offset", "sort"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/authorized-apps")?;
    let limit = match opt_usize(&obj, "limit")? {
        None => 10,
        Some(limit) if (1..=100).contains(&limit) => limit,
        Some(_) => {
            return Err(ApiError::invalid_param(
                "limit",
                "must be an integer between 1 and 100",
            ));
        }
    };
    let offset = opt_usize(&obj, "offset")?.unwrap_or(0);
    let desc = match obj.get("sort").and_then(Value::as_str) {
        None | Some("desc") => true,
        Some("asc") => false,
        Some(_) => {
            return Err(ApiError::invalid_param("sort", "must be one of desc, asc"));
        }
    };
    let mut grants = state
        .services
        .auth
        .tokens_for_account(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    grants.retain(|g| !g.is_first_party);
    grants.sort_by(|a, b| {
        if desc {
            b.created_at_ms
                .cmp(&a.created_at_ms)
                .then_with(|| b.id.cmp(&a.id))
        } else {
            a.created_at_ms
                .cmp(&b.created_at_ms)
                .then_with(|| a.id.cmp(&b.id))
        }
    });
    let out: Vec<Value> = grants
        .iter()
        .skip(offset)
        .take(limit)
        .map(|g| {
            serde_json::json!({
                "id": g.id,
                "name": g.name,
                "callbackUrl": null,
                "permission": g.permissions,
                "isAuthorized": true,
            })
        })
        .collect();
    Ok(Json(out).into_response())
}

/// `POST /api/i/signin-history` (split from `api_dispatch_blocking`).
pub(crate) fn i_signin_history(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &["limit", "sinceId", "untilId", "sinceDate", "untilDate"],
    )?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/signin-history")?;
    let cursor = parse_cursor(&obj)?;
    let rows = state
        .services
        .signin
        .list_signins(
            &caller.account.id,
            cursor.limit,
            cursor.since_id.as_deref(),
            cursor.until_id.as_deref(),
            cursor.since_date,
            cursor.until_date,
        )
        .map_err(ApiError::from_port_error)?;
    let out: Vec<Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id,
                "createdAt": crate::activitypub::format_millis(r.created_at_ms),
                "ip": r.ip,
                "headers": serde_json::from_str::<Value>(&r.headers).unwrap_or(Value::Null),
                "success": r.success,
            })
        })
        .collect();
    Ok(Json(out).into_response())
}

/// `POST /api/i/update-email` (split from `api_dispatch_blocking`).
pub(crate) fn i_update_email(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["password", "email", "token"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/update-email")?;
    let password = required_str(&obj, "password")?;
    // 2FA `token`: accepted-and-ignored (no 2FA exists — same
    // treatment as `i/change-password`).
    let acct_key = format!("acctid:{}", caller.account.id);
    let ip_key = auth_ip_key(&client_ip);
    state
        .rate_limiter
        .check_auth_failures("auth/update-email", &acct_key, &ip_key)?;
    let stored = state
        .services
        .auth
        .get_password_hash(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    let ok = tokio::runtime::Handle::current().block_on(verify_and_upgrade_password(
        &state,
        &caller.account.id,
        &password,
        stored,
    ))?;
    if !ok {
        state
            .rate_limiter
            .record_auth_failure("auth/update-email", &acct_key, &ip_key);
        return Err(ApiError::update_email_incorrect_password());
    }
    let email = match obj.get("email") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            return Err(ApiError::invalid_param("email", "must be a string"));
        }
    };
    if let Some(addr) = email.as_deref() {
        if !email_shape_valid(addr) || !email_available(&state.services, &caller.account.id, addr)?
        {
            return Err(ApiError::update_email_unavailable());
        }
    }
    // `emailRequiredForSignup` is always false here (see
    // `pack_meta`), so clearing needs no `EMAIL_REQUIRED` branch —
    // the constructor stays for parity coverage.
    let code = email.as_deref().map(|_| random_code(16));
    let updated = state
        .services
        .accounts
        .update_account(
            &caller.account.id,
            UpdateAccountPatch {
                email: Some(email.clone()),
                email_verified: Some(false),
                email_verify_code: Some(code.clone()),
                ..Default::default()
            },
            Vec::new(),
        )
        .map_err(ApiError::from_port_error)?;
    // Live send through the configured sender (published by the
    // sibling-owned `main.rs` wiring via `set_email_sender`); no
    // sender keeps the previous dormant posture — mint and store
    // the code, log loudly, succeed. A configured sender that
    // fails at transport level is a loud 500 (never silently
    // dropped); a configured-but-unconfigured sender degrades to
    // the dormant posture.
    if let Some(addr) = email.as_deref() {
        let code_value = code.as_deref().unwrap_or("");
        let message = verification_email(addr, &state.services.public_url, code_value);
        match send_email_now(&message) {
            Ok(()) => {}
            Err(err) if err.code == "EMAIL_NOT_CONFIGURED" => {
                eprintln!(
                    "email dormant: verification code for {} not sent (no SMTP configured)",
                    caller.account.id
                );
            }
            Err(err) => return Err(err),
        }
    }
    Ok(Json(
        pack_me_detailed(&updated, caller.is_first_party, &state.services)
            .map_err(ApiError::from_port_error)?,
    )
    .into_response())
}

/// `POST /api/i/move` (split from `api_dispatch_blocking`).
pub(crate) fn i_move(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["moveToAccount"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/move")?;
    let target_acct = required_str(&obj, "moveToAccount")?;
    if target_acct.trim().is_empty() {
        return Err(ApiError::move_no_such_user());
    }
    // Root refusal: pinetree has no `rootUserId` column — the
    // instance's initial administrator (lowest account id among
    // admins, DIVERGENCE) cannot migrate.
    if caller.account.is_admin && is_oldest_admin(&state.services, &caller.account.id)? {
        return Err(ApiError::move_root_forbidden());
    }
    if caller.account.moved_to_uri.is_some() {
        return Err(ApiError::move_already_moved());
    }
    let (username, host) = parse_acct(&target_acct);
    if username.is_empty() {
        return Err(ApiError::move_no_such_user());
    }
    // Local-first resolution, then remote WebFinger + fetch
    // (upstream `remoteUserResolveService.resolveUser` parity).
    let destination = match state
        .services
        .users_show_by_username(&username, host.as_deref())
    {
        Ok(account) => account,
        Err(PortError::NotFound(_)) => match host.as_deref() {
            Some(remote_host) if !remote_host.is_empty() => webfinger_discover_actor_uri(
                &state.services.federation_http,
                &username,
                remote_host,
                None,
            )
            .ok()
            .and_then(|actor_uri| {
                fetch_and_upsert_remote_actor(
                    &state.services,
                    &state.services.federation_http,
                    &actor_uri,
                )
                .ok()
            })
            .map(|(account, _)| account)
            .ok_or_else(ApiError::move_no_such_user)?,
            _ => return Err(ApiError::move_no_such_user()),
        },
        Err(err) => return Err(ApiError::from_port_error(err)),
    };
    let new_uri = if destination.is_local() {
        destination.actor_url(&state.services.public_url)
    } else {
        destination
            .uri
            .clone()
            .filter(|u| !u.is_empty())
            .ok_or_else(ApiError::move_uri_null)?
    };
    // Alias handshake: the destination must list the old account URI
    // in `alsoKnownAs` (upstream substring `includes` parity), and
    // must not itself have moved.
    let from_url = format!(
        "{}/users/{}",
        state.services.public_url.trim_end_matches('/'),
        caller.account.id
    );
    let allowed = destination
        .also_known_as
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|known| known.contains(&from_url));
    if !allowed || destination.moved_to_uri.is_some() {
        return Err(ApiError::move_destination_forbids());
    }
    let now_ms = state.services.clock.now_ms();
    let mut also_known = caller.account.also_known_as.clone().unwrap_or_default();
    if !also_known.iter().any(|u| u == &new_uri) {
        also_known.push(new_uri.clone());
    }
    let activity_id = format!(
        "{}/moves/{}/{}",
        state.services.public_url.trim_end_matches('/'),
        caller.account.id,
        destination.id
    );
    let move_job = crate::federation::outbox::FederateDeliverPayload::MoveDelivered {
        activity_id: activity_id.clone(),
        account_id: caller.account.id.clone(),
        new_actor_uri: new_uri.clone(),
        new_account_id: destination.id.clone(),
        inbox_override: None,
    }
    .into_job(
        format!("federate:move:{}:{activity_id}", caller.account.id),
        now_ms,
    )
    .map_err(ApiError::from_port_error)?;
    let update_job = crate::federation::outbox::FederateDeliverPayload::ActorUpdated {
        update_id: state.services.ids.generate(),
        account_id: caller.account.id.clone(),
        inbox_override: None,
    }
    .into_job(
        format!("federate:move-update:{}:{now_ms}", caller.account.id),
        now_ms,
    )
    .map_err(ApiError::from_port_error)?;
    let updated = state
        .services
        .accounts
        .update_account(
            &caller.account.id,
            UpdateAccountPatch {
                moved_to_uri: Some(new_uri.clone()),
                moved_at_ms: Some(now_ms),
                also_known_as: Some(Some(also_known)),
                ..Default::default()
            },
            vec![update_job, move_job],
        )
        .map_err(ApiError::from_port_error)?;
    // `postMoveProcess` parity (inline, paged): local followers
    // unfollow-old + follow-new. Remote followers learn via the
    // delivered `Move`.
    migrate_followers_to(&state.services, &caller.account.id, &destination.id)?;
    Ok(Json(
        pack_me_detailed(&updated, caller.is_first_party, &state.services)
            .map_err(ApiError::from_port_error)?,
    )
    .into_response())
}
