//! Users dispatcher arms.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]
//! Misskey HTTP API surface (Slice D3).
//!
//! [`ApiState`] carries the application services, the in-process job
//! worker, and the rate limiter. [`build_router`] wires every Phase 1
//! route: `/healthz`, media/identicon file serving, the MiAuth web flow
//! (`GET`/`POST /miauth/{session}` plus the one-shot `check`
//! endpoints), web `signup`/`signin`, `drive/files/create` (multipart and
//! JSON), and the `POST /api/{endpoint}` JSON dispatcher.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

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

use crate::activitypub::federated_collections as ap_collections;
use crate::activitypub::{actor as ap_actor, objects as ap_objects, webfinger as ap_webfinger};
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
use crate::streaming::hub::{Hub, ONLINE_THRESHOLD_MS};
use crate::streaming::server::streaming_handler;

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
use crate::services::email::EmailError;
use crate::services::email::EmailMessage;
use crate::services::email::EmailSendPayload;
use crate::services::email::EmailSender;
use crate::services::email::SmtpConfig;
use crate::services::email::SmtpEmailSender;
use crate::services::email::password_reset_email;
use crate::services::email::verification_email;

use super::ApiState;
use super::auth_http::{client_ip, require_caller};
/// Shared API state: application services, the durable-job worker, the
/// Misskey rate limiter, the concurrent-upload slots, the live-streaming
/// broadcast hub, and the reverse proxies whose `X-Forwarded-For` is
/// honored ([`client_ip`]).
use super::parse::{
    get_limit, opt_bool, opt_i64, opt_string, opt_usize, pack_account_for, pack_note_list,
    parse_cursor, required_str,
};

/// `POST /api/users/show` (split from `api_dispatch_blocking`).
pub(crate) fn users_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "userIds", "username", "host"])?;
    // Batch Q Phase 1 item 3 (Misskey `users/show.ts` parity):
    // suspension + ugc gates, with moderator bypass.
    let viewer_id = caller.as_ref().map(|c| c.account.id.as_str());
    if let Some(Value::Array(ids)) = obj.get("userIds") {
        if ids.is_empty() {
            return Ok(Json(Value::Array(Vec::new())).into_response());
        }
        // Upstream batch omits missing AND suspended rows for
        // non-moderators (moderators see suspended rows).
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let id = id
                .as_str()
                .ok_or_else(|| ApiError::invalid_param("userIds", "must be an array of strings"))?;
            match state.services.users_show_by_id_as(id, viewer_id) {
                Ok(account) => out.push(pack_account_for(
                    &account,
                    caller.as_ref(),
                    &state.services,
                )?),
                Err(PortError::NotFound(_)) => {}
                Err(err) => return Err(ApiError::from_port_error(err)),
            }
        }
        return Ok(Json(Value::Array(out)).into_response());
    }
    if obj.get("userIds").is_some() {
        return Err(ApiError::invalid_param(
            "userIds",
            "must be an array of strings",
        ));
    }
    let ugc = state
        .services
        .instance_settings
        .get()
        .map(|s| s.ugc_visibility_for_visitor)
        .unwrap_or_default();
    let account = if let Some(user_id) = obj.get("userId").and_then(Value::as_str) {
        match state.services.users_show_by_id_as(user_id, viewer_id) {
            Ok(account) => {
                // Upstream `show.ts:178`: anonymous + `local` +
                // remote user → `noSuchUser`.
                if viewer_id.is_none()
                    && ugc == UgcVisibilityForVisitor::Local
                    && account.host.is_some()
                {
                    return Err(ApiError::users_show_no_such_user());
                }
                account
            }
            Err(PortError::NotFound(_)) => {
                return Err(ApiError::users_show_no_such_user());
            }
            Err(err) => return Err(ApiError::from_port_error(err)),
        }
    } else if let Some(username) = obj.get("username").and_then(Value::as_str) {
        let host = obj.get("host").and_then(Value::as_str);
        // Upstream `show.ts:158-160`: anonymous + `local` + remote
        // handle lookup → `noSuchUser` without resolving.
        if viewer_id.is_none() && ugc == UgcVisibilityForVisitor::Local && host.is_some() {
            return Err(ApiError::users_show_no_such_user());
        }
        match state
            .services
            .users_show_by_username_as(username, host, viewer_id)
        {
            Ok(account) => {
                if viewer_id.is_none()
                    && ugc == UgcVisibilityForVisitor::Local
                    && account.host.is_some()
                {
                    return Err(ApiError::users_show_no_such_user());
                }
                account
            }
            Err(PortError::NotFound(_)) => match host {
                Some(remote_host) => {
                    let wf = webfinger_discover_actor_uri(
                        &state.services.federation_http,
                        username,
                        remote_host,
                        None,
                    );
                    if let Err(e) = &wf {
                        eprintln!(
                            "remote user resolve {username}@{remote_host}: webfinger failed: {e}"
                        );
                    }
                    let resolved = wf
                        .ok()
                        .and_then(|actor_uri| {
                            let r = fetch_and_upsert_remote_actor(
                                &state.services,
                                &state.services.federation_http,
                                &actor_uri,
                            );
                            if let Err(e) = &r {
                                eprintln!(
                                    "remote user resolve {actor_uri}: actor fetch failed: {e}"
                                );
                            }
                            r.ok()
                        })
                        .map(|(account, _)| account);
                    match resolved {
                        Some(account) => {
                            // A remotely resolved suspended account
                            // stays hidden from non-moderators
                            // (upstream re-checks after resolve).
                            if account.is_suspended
                                && !state
                                    .services
                                    .viewer_is_moderator(viewer_id)
                                    .map_err(ApiError::from_port_error)?
                            {
                                return Err(ApiError::users_show_no_such_user());
                            }
                            account
                        }
                        None => return Err(ApiError::users_show_no_such_user()),
                    }
                }
                None => return Err(ApiError::users_show_no_such_user()),
            },
            Err(err) => return Err(ApiError::from_port_error(err)),
        }
    } else {
        return Err(ApiError::invalid_param(
            "userId",
            "one of userId, userIds, or username is required",
        ));
    };
    Ok(Json(pack_account_for(
        &account,
        caller.as_ref(),
        &state.services,
    )?)
    .into_response())
}

/// `POST /api/users/search-by-username-and-host` (split from `api_dispatch_blocking`).
pub(crate) fn users_search_by_username_and_host(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["username", "host", "limit", "detail"])?;
    let username = obj.get("username").and_then(Value::as_str);
    let host = obj.get("host").and_then(Value::as_str);
    if username.is_none_or(|u| u.is_empty()) && host.is_none_or(|h| h.is_empty()) {
        return Err(ApiError::invalid_param(
            "username",
            "one of username or host is required",
        ));
    }
    let _limit = get_limit(&obj)?;
    let detail = opt_bool(&obj, "detail", true)?;
    // Thin alias over the `users/show` username/host resolution
    // (upstream `users/search-by-username-and-host.ts` delegates to
    // `userSearchService.searchByUsernameAndHost`; pinetree resolves
    // a single account and answers a 1-element array).
    let mut resolved = None;
    if let Some(username) = username.filter(|u| !u.is_empty()) {
        match state.services.users_show_by_username(username, host) {
            Ok(account) => resolved = Some(account),
            Err(original @ PortError::NotFound(_)) => match host {
                Some(remote_host) if !remote_host.is_empty() => {
                    let account = webfinger_discover_actor_uri(
                        &state.services.federation_http,
                        username,
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
                    .map(|(account, _)| account);
                    match account {
                        Some(account) => resolved = Some(account),
                        None => return Err(ApiError::from_port_error(original)),
                    }
                }
                _ => {}
            },
            Err(err) => return Err(ApiError::from_port_error(err)),
        }
    }
    let mut out = Vec::new();
    if let Some(account) = resolved {
        if detail {
            out.push(pack_account_for(
                &account,
                caller.as_ref(),
                &state.services,
            )?);
        } else {
            out.push(
                serde_json::to_value(pack_user_lite(
                    &account,
                    &state.services.public_url,
                    state.services.clock.now_ms(),
                ))
                .map_err(|_| ApiError::internal())?,
            );
        }
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/users/search` (split from `api_dispatch_blocking`).
pub(crate) fn users_search(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["query", "offset", "limit", "origin", "detail"])?;
    let query = obj
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let offset = opt_usize(&obj, "offset")?.unwrap_or(0);
    let limit = get_limit(&obj)?;
    let accounts = state
        .services
        .accounts
        .search_accounts(&query, offset, limit)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(accounts.len());
    for account in &accounts {
        out.push(pack_account_for(account, caller.as_ref(), &state.services)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/username/available` (split from `api_dispatch_blocking`).
pub(crate) fn username_available(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    _caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["username"])?;
    let username = required_str(&obj, "username")?;
    let valid = !username.is_empty()
        && username.len() <= 20
        && username
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if !valid {
        return Err(ApiError::invalid_param(
            "username",
            "must match ^[A-Za-z0-9_]{1,20}$",
        ));
    }
    let available = state
        .services
        .accounts
        .is_username_available(&username, &state.services.preserved_usernames)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(serde_json::json!({ "available": available })).into_response())
}

/// `POST /api/users/report-abuse` (split from `api_dispatch_blocking`).
pub(crate) fn users_report_abuse(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "comment"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:report-abuse")?;
    state.rate_limiter.check(&rate_key, "users/report-abuse")?;
    let user_id = required_str(&obj, "userId")?;
    let comment = required_str(&obj, "comment")?;
    let report = state
        .services
        .report_abuse(&caller.account.id, &user_id, &comment)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(
        pack_abuse_report(&report, Some(&caller.account.id), &state.services)
            .map_err(ApiError::from_port_error)?,
    )
    .into_response())
}

/// `POST /api/users/notes` (split from `api_dispatch_blocking`).
pub(crate) fn users_notes(
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
            "userId",
            "withReplies",
            "withRenotes",
            "withChannelNotes",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
            "allowPartial",
            "withFiles",
        ],
    )?;
    let user_id = required_str(&obj, "userId")?;
    let cursor = parse_cursor(&obj)?;
    let with_replies = opt_bool(&obj, "withReplies", false)?;
    let with_renotes = opt_bool(&obj, "withRenotes", true)?;
    // No channels exist: `withChannelNotes` is accepted and ignored,
    // as is `allowPartial`.
    let with_files = opt_bool(&obj, "withFiles", false)?;
    // Misskey parity: the combination is rejected, not applied.
    if with_replies && with_files {
        return Err(ApiError::from_port_error(PortError::Validation(
            "BOTH_WITH_REPLIES_AND_WITH_FILES".to_string(),
        )));
    }
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    let notes = state
        .services
        .user_notes(
            &user_id,
            viewer_id,
            &cursor,
            with_replies,
            with_renotes,
            with_files,
        )
        .map_err(ApiError::from_port_error)?;
    pack_note_list(&state.services, &notes, viewer_id)
}

/// `POST /api/users/recommendation` (split from `api_dispatch_blocking`).
pub(crate) fn users_recommendation(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["limit", "offset"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let limit = get_limit(&obj)?.clamp(1, 100);
    let offset = opt_i64(&obj, "offset")?.unwrap_or(0).max(0) as usize;
    let users = state
        .services
        .recommended_users(&caller.account.id, limit, offset)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(users.len());
    for account in &users {
        out.push(
            pack_user_detailed_not_me(account, Some(&caller.account.id), &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/users/relation` (split from `api_dispatch_blocking`).
pub(crate) fn users_relation(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let me = caller.account.id.clone();
    // Misskey parity: always an array (a single `userId` yields a
    // one-element array), with no existence check on the targets.
    let ids: Vec<String> = match obj.get("userId") {
        Some(Value::String(id)) => vec![id.clone()],
        Some(Value::Array(ids)) => {
            let mut out = Vec::with_capacity(ids.len());
            for id in ids {
                out.push(
                    id.as_str()
                        .ok_or_else(|| {
                            ApiError::invalid_param(
                                "userId",
                                "must be a string or an array of strings",
                            )
                        })?
                        .to_string(),
                );
            }
            out
        }
        _ => {
            return Err(ApiError::invalid_param("userId", "this field is required"));
        }
    };
    let mut out = Vec::with_capacity(ids.len());
    for id in &ids {
        out.push(pack_user_relation(&me, id, &state.services).map_err(ApiError::from_port_error)?);
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/users/update-memo` (split from `api_dispatch_blocking`).
pub(crate) fn users_update_memo(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["userId", "memo"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "users/update-memo")?;
    let user_id = required_str(&obj, "userId")?;
    let memo = opt_string(&obj, "memo")?;
    let stored = state
        .services
        .update_memo(&caller.account.id, &user_id, memo)
        .map_err(|err| match &err {
            PortError::NotFound(msg) if msg == "NO_SUCH_USER" => {
                ApiError::update_memo_no_such_user()
            }
            _ => ApiError::from_port_error(err),
        })?;
    Ok(Json(stored.map(Value::String).unwrap_or(Value::Null)).into_response())
}

/// `POST /api/users/pages` (split from `api_dispatch_blocking`).
pub(crate) fn users_pages(
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
            "userId",
            "limit",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
        ],
    )?;
    let viewer = caller.as_ref().map(|caller| caller.account.id.clone());
    let user_id = required_str(&obj, "userId")?;
    let cursor = parse_cursor(&obj)?;
    let pages = state
        .services
        .pages_of_user(&user_id, &cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::with_capacity(pages.len());
    for page in &pages {
        out.push(
            pack_page(page, viewer.as_deref(), &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// Shared body for `users/followers`, `users/following` (split from `api_dispatch_blocking`).
fn users_followers_inner(
    state: &ApiState,
    endpoint: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(
        &obj,
        &[
            "userId",
            "username",
            "host",
            "sinceId",
            "untilId",
            "sinceDate",
            "untilDate",
            "limit",
        ],
    )?;
    let account = if let Some(user_id) = obj.get("userId").and_then(Value::as_str) {
        state
            .services
            .users_show_by_id(user_id)
            .map_err(ApiError::from_port_error)?
    } else if let Some(username) = obj.get("username").and_then(Value::as_str) {
        let host = match obj.get("host") {
            None => {
                return Err(ApiError::invalid_param(
                    "host",
                    "host is required with username",
                ));
            }
            Some(Value::Null) => None,
            Some(Value::String(h)) => Some(h.as_str()),
            Some(_) => {
                return Err(ApiError::invalid_param("host", "must be a string or null"));
            }
        };
        state
            .services
            .users_show_by_username(username, host)
            .map_err(ApiError::from_port_error)?
    } else {
        return Err(ApiError::invalid_param(
            "userId",
            "one of userId or username is required",
        ));
    };
    let cursor = parse_cursor(&obj)?;
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    let follows = if endpoint == "users/followers" {
        state
            .services
            .user_followers(&account.id, viewer_id, &cursor)
            .map_err(ApiError::from_port_error)?
    } else {
        state
            .services
            .user_following(&account.id, viewer_id, &cursor)
            .map_err(ApiError::from_port_error)?
    };
    let mut out = Vec::with_capacity(follows.len());
    for follow in &follows {
        out.push(
            pack_following(follow, viewer_id, &state.services)
                .map_err(ApiError::from_port_error)?,
        );
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/users/followers` (split from `api_dispatch_blocking`).
pub(crate) fn users_followers(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    users_followers_inner(
        state,
        "users/followers",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}

/// `POST /api/users/following` (split from `api_dispatch_blocking`).
pub(crate) fn users_following(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    users_followers_inner(
        state,
        "users/following",
        obj,
        caller,
        rate_key,
        client_ip,
        headers,
    )
}
