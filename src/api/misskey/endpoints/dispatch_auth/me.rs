//! Auth/session/meta/i dispatcher arms (me).
//!
//! Split from `endpoints/dispatch_auth.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{
    SIGNUP_FIELDS, SigninFlowError, SignupError, auth_ip_key, client_ip, do_signin, do_signin_flow,
    do_signup, record_2fa_auth_failure, record_signin_attempt, require_caller, send_email_now,
    set_email_sender, verify_and_upgrade_password,
};
use super::super::dispatch_rest::pack_announcement_view;
use super::super::parse::{
    MIAUTH_APPROVAL_WINDOW_MS, announcement_store, opt_bool, opt_string, parse_permissions,
    parse_profile_patch, random_code, required_str, required_str_array,
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

/// `POST /api/i` (split from `api_dispatch_blocking`).
pub(crate) fn i(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &[])?;
    let caller = require_caller(caller)?;
    caller.require_permission("read:account")?;
    let mut packed = pack_me_detailed(&caller.account, caller.is_first_party, &state.services)
        .map_err(ApiError::from_port_error)?;
    // Gaps final wiring (WP2b §4d): the real announcement badge.
    // `unreadAnnouncements` carries the unread packs and
    // `hasUnreadAnnouncement` mirrors their emptiness; chat stays
    // hardcoded false in the packer (channels/chat skipped).
    if let Some(map) = packed.as_object_mut() {
        let unread: Vec<Value> = state
            .services
            .announcements_list(announcement_store(), Some(&caller.account.id))
            .map_err(ApiError::from_port_error)?
            .iter()
            .filter(|view| !view.is_read)
            .map(pack_announcement_view)
            .collect();
        map.insert(
            "hasUnreadAnnouncement".to_string(),
            Value::Bool(!unread.is_empty()),
        );
        map.insert("unreadAnnouncements".to_string(), Value::Array(unread));
    }
    Ok(Json(packed).into_response())
}

/// `POST /api/i/update` (split from `api_dispatch_blocking`).
pub(crate) fn i_update(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    // Full profile store (§0B): all 39 `i/update.ts` params plus the
    // DIVERGENCE `rejectQuotes` (§3) and `defaultReaction` (§6)
    // params. Business rules (prohibited words, media checks, mute
    // limits, alias resolution, unlock-accept) live in
    // `AppServices::update_profile`; this arm only parses shapes.
    check_allowed_fields(
        &obj,
        &[
            "name",
            "description",
            "followedMessage",
            "location",
            "birthday",
            "lang",
            "avatarId",
            "avatarDecorations",
            "bannerId",
            "fields",
            "isLocked",
            "isExplorable",
            "hideOnlineStatus",
            "publicReactions",
            "carefulBot",
            "autoAcceptFollowed",
            "followApprovalLocalSeconds",
            "followApprovalRemoteSeconds",
            "noCrawle",
            "preventAiLearning",
            "requireSigninToViewContents",
            "makeNotesFollowersOnlyBefore",
            "makeNotesHiddenBefore",
            "isBot",
            "isCat",
            "injectFeaturedNote",
            "receiveAnnouncementEmail",
            "alwaysMarkNsfw",
            "autoSensitive",
            "followingVisibility",
            "followersVisibility",
            "chatScope",
            "pinnedPageId",
            "mutedWords",
            "hardMutedWords",
            "mutedInstances",
            "notificationRecieveConfig",
            "emailNotificationTypes",
            "alsoKnownAs",
            "rejectQuotes",
            "defaultReaction",
            // DIVERGENCE (Batch N WP8b): per-user remote-emoji opt-in.
            "showRemoteEmojis",
        ],
    )?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:account")?;
    state.rate_limiter.check(&rate_key, "i/update")?;
    let patch = parse_profile_patch(&obj)?;
    let show_remote_emojis_on = patch.show_remote_emojis == Some(true);
    let account = state
        .services
        .update_profile(&caller.account.id, patch)
        .map_err(ApiError::from_port_error)?;
    // WP8b lazy fetch: turning the opt-in on enqueues one bounded
    // fetch job per known remote emoji (metadata rows only; no fetch
    // happened at ingest). Off-request, crash-safe, retryable.
    if show_remote_emojis_on {
        if let Err(e) = state.services.enqueue_remote_emoji_fetches() {
            eprintln!("remote emoji fetch enqueue failed: {e}");
        }
    }
    let packed = pack_me_detailed(&account, caller.is_first_party, &state.services)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/i/change-password` (split from `api_dispatch_blocking`).
pub(crate) fn i_change_password(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["currentPassword", "newPassword", "token"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/change-password")?;
    let current = required_str(&obj, "currentPassword")?;
    let new = match obj.get("newPassword").and_then(Value::as_str) {
        Some(new) if !new.is_empty() => new.to_string(),
        _ => {
            return Err(ApiError::invalid_param(
                "newPassword",
                "must be a non-empty string",
            ));
        }
    };
    let acct_key = format!("acctid:{}", caller.account.id);
    let ip_key = auth_ip_key(&client_ip);
    state
        .rate_limiter
        .check_auth_failures("auth/change-password", &acct_key, &ip_key)?;
    let stored = state
        .services
        .auth
        .get_password_hash(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    let ok = tokio::runtime::Handle::current().block_on(verify_and_upgrade_password(
        &state,
        &caller.account.id,
        &current,
        stored,
    ))?;
    if !ok {
        state
            .rate_limiter
            .record_auth_failure("auth/change-password", &acct_key, &ip_key);
        return Err(ApiError::authentication_failed());
    }
    let hash = tokio::runtime::Handle::current().block_on(hash_password_async(new))?;
    state
        .services
        .auth
        .set_password_hash(&caller.account.id, &hash)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/regenerate-token` (split from `api_dispatch_blocking`).
pub(crate) fn i_regenerate_token(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["password"])?;
    let caller = require_caller(caller)?;
    caller.require_secure()?;
    state.rate_limiter.check(&rate_key, "i/regenerate-token")?;
    let password = required_str(&obj, "password")?;
    let acct_key = format!("acctid:{}", caller.account.id);
    let ip_key = auth_ip_key(&client_ip);
    state
        .rate_limiter
        .check_auth_failures("auth/regenerate-token", &acct_key, &ip_key)?;
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
            .record_auth_failure("auth/regenerate-token", &acct_key, &ip_key);
        return Err(ApiError::authentication_failed());
    }
    state
        .services
        .auth
        .delete_account_first_party_tokens(&caller.account.id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/i/revoke-token` (split from `api_dispatch_blocking`).
pub(crate) fn i_revoke_token(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["token", "tokenId"])?;
    let caller = require_caller(caller)?;
    // Any credential may revoke (Misskey parity); third-party tokens
    // are restricted to their own token below. No rate limit: token
    // revocation is rare and must never lock a user out.
    if obj.get("tokenId").is_some_and(|v| !v.is_null()) {
        // Named-grant revocation (upstream `revoke-token.ts`
        // `tokenId` branch): the grant must belong to the caller;
        // anything else (unknown id, another account's grant) is a
        // silent no-op success.
        let token_id = match obj.get("tokenId").and_then(Value::as_str) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => {
                return Err(ApiError::invalid_param(
                    "tokenId",
                    "must be a non-empty string",
                ));
            }
        };
        let target = state
            .services
            .auth
            .get_token_by_id(&token_id)
            .map_err(ApiError::from_port_error)?;
        let Some(target) = target else {
            return Ok(StatusCode::NO_CONTENT.into_response());
        };
        if target.account_id != caller.account.id {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        if !caller.is_first_party && target.token_hash != hash_token(&caller.raw_token) {
            return Err(ApiError::permission_denied());
        }
        state
            .services
            .auth
            .delete_token_by_hash(&target.token_hash)
            .map_err(ApiError::from_port_error)?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    let raw = match obj.get("token") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            return Err(ApiError::invalid_param("token", "must be a string"));
        }
    };
    match raw {
        // Both absent: revoke the caller's own token (profile §3).
        None => {
            state
                .services
                .auth
                .delete_token_by_hash(&hash_token(&caller.raw_token))
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        // Misskey parity: an empty `token` is a no-op success.
        Some(token) if token.is_empty() => Ok(StatusCode::NO_CONTENT.into_response()),
        Some(token) => {
            let hash = hash_token(&token);
            let record = state
                .services
                .auth
                .get_token_by_hash(&hash)
                .map_err(ApiError::from_port_error)?;
            let Some(record) = record else {
                return Ok(StatusCode::NO_CONTENT.into_response());
            };
            if record.account_id != caller.account.id {
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
            if !caller.is_first_party && hash != hash_token(&caller.raw_token) {
                return Err(ApiError::permission_denied());
            }
            state
                .services
                .auth
                .delete_token_by_hash(&hash)
                .map_err(ApiError::from_port_error)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
    }
}
