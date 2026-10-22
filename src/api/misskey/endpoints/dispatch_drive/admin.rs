//! Drive dispatcher arms (admin).
//!
//! Split from `endpoints/dispatch_drive.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::admin_status::walk_media_for_url;
use super::super::auth_http::{client_ip, require_caller};
use super::super::parse::{
    get_limit_with_default, map_drive_extra_error, map_drive_folder_error, opt_bool, opt_i64,
    opt_nullable_string, opt_string, pack_note_list, parse_cursor, required_str, str_list,
    update_string_field,
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

/// `POST /api/admin/drive/files` (split from `api_dispatch_blocking`).
pub(crate) fn admin_drive_files(
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
            "userId",
            "type",
            "origin",
            "hostname",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:drive")?;
    require_moderator(&caller)?;
    let cursor = TimelineCursor {
        since_id: opt_string(&obj, "sinceId")?,
        until_id: opt_string(&obj, "untilId")?,
        since_date: opt_i64(&obj, "sinceDate")?,
        until_date: opt_i64(&obj, "untilDate")?,
        limit: get_limit_with_default(&obj, 10)?,
    };
    let user_id = opt_string(&obj, "userId")?;
    let type_prefix = match opt_string(&obj, "type")? {
        None => None,
        // Upstream `type` is a MIME pattern (`image/*`); honor a
        // trailing `*` as a prefix match, anything else exact.
        Some(pattern) => Some(match pattern.strip_suffix('*') {
            Some(prefix) => (prefix.to_string(), true),
            None => (pattern, false),
        }),
    };
    let origin = opt_string(&obj, "origin")?.unwrap_or_else(|| "local".to_string());
    if !["combined", "local", "remote"].contains(&origin.as_str()) {
        return Err(ApiError::invalid_param(
            "origin",
            "must be one of combined, local, remote",
        ));
    }
    let hostname = opt_string(&obj, "hostname")?;
    let rows = state
        .services
        .media
        .list_all_media(&cursor)
        .map_err(ApiError::from_port_error)?;
    let mut out = Vec::new();
    for meta in &rows {
        if let Some(user_id) = user_id.as_deref() {
            if meta.owner_id.as_deref() != Some(user_id) {
                continue;
            }
        }
        if let Some((pattern, prefix)) = type_prefix.as_ref() {
            if *prefix {
                if !meta.mime.starts_with(pattern.as_str()) {
                    continue;
                }
            } else if meta.mime != *pattern {
                continue;
            }
        }
        // Resolve the owner's host once per row (small pages; the
        // admin list defaults to 10 rows).
        let host = match meta.owner_id.as_deref() {
            Some(owner) => state
                .services
                .accounts
                .find_by_id(owner)
                .map_err(ApiError::from_port_error)?
                .and_then(|account| account.host),
            None => None,
        };
        match origin.as_str() {
            "local" if host.is_some() => continue,
            "remote" if host.is_none() => continue,
            _ => {}
        }
        if let Some(hostname) = hostname.as_deref() {
            // Upstream: the local host is represented with `null` —
            // a hostname filter never matches local files.
            match host.as_deref() {
                Some(h) if h == hostname => {}
                _ => continue,
            }
        }
        out.push(pack_drive_file(meta, &state.services));
        if out.len() >= cursor.limit {
            break;
        }
    }
    Ok(Json(Value::Array(out)).into_response())
}

/// `POST /api/admin/drive/show-file` (split from `api_dispatch_blocking`).
pub(crate) fn admin_drive_show_file(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["fileId", "url"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:admin:drive")?;
    require_moderator(&caller)?;
    let (file_id, url) = match (obj.get("fileId"), obj.get("url")) {
        (Some(_), Some(_)) => {
            return Err(ApiError::invalid_param(
                "url",
                "specify either fileId or url, not both",
            ));
        }
        (None, None) => {
            return Err(ApiError::invalid_param(
                "fileId",
                "either fileId or url is required",
            ));
        }
        (id, url) => (
            id.and_then(Value::as_str).map(str::to_string),
            url.and_then(Value::as_str).map(str::to_string),
        ),
    };
    let meta = if let Some(file_id) = file_id {
        state
            .services
            .media
            .get(&file_id)
            .map_err(ApiError::from_port_error)?
    } else {
        // Batch H/C: the old `list_all_media(limit: 10_000)` single
        // read truncated at the adapter clamp and could miss rows.
        // Prefer the bounded `find_by_source_url` lookup; fall back
        // to a bounded cursor walk matching the served `url` too.
        let url = url.unwrap_or_default();
        let indexed = state
            .services
            .media
            .find_by_source_url(&url)
            .map_err(ApiError::from_port_error)?;
        match indexed
            .into_iter()
            .find(|m| m.url == url || m.source_url.as_deref() == Some(url.as_str()))
        {
            Some(meta) => Some(meta),
            None => walk_media_for_url(&state.services, &url)?,
        }
    };
    match meta {
        Some(meta) => Ok(Json(pack_drive_file(&meta, &state.services)).into_response()),
        None => Err(ApiError::drive_show_file_no_such_file()),
    }
}
