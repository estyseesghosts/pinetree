//! Shared request parsing, validation and packing helpers (invites).
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

/// Packed `InviteCode[]` for the invite listings (resolves `createdBy`/
/// `usedBy` through the account store, upstream `InviteCodeEntityService.
/// packMany`).
pub(crate) fn pack_invite_codes(
    state: &ApiState,
    invites: &[InviteCode],
) -> Result<Value, ApiError> {
    let mut packed = Vec::with_capacity(invites.len());
    for invite in invites {
        let (created_by, used_by) = state.services.invite_fields(invite);
        packed.push(pack_invite_code(
            &state.services,
            invite,
            created_by.as_ref(),
            used_by.as_ref(),
        ));
    }
    Ok(Value::Array(packed))
}

/// `admin/invite/create` `expiresAt` (upstream parses any `Date.parse`-
/// acceptable string and answers `INVALID_DATE_TIME` when it does not).
pub(crate) fn parse_invite_expires_at(raw: &str) -> Result<i64, ApiError> {
    let invalid =
        || ApiError::from_port_error(PortError::Validation("INVALID_DATE_TIME".to_string()));
    let trimmed = raw.trim();
    chrono::DateTime::parse_from_rfc3339(trimmed)
        .map(|dt| dt.timestamp_millis())
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f"))
                .map(|dt| dt.and_utc().timestamp_millis())
        })
        .map_err(|_| invalid())
}

// --- Web signup / signin ---------------------------------------------------
/// Parse + validate an `i/registry` scope array: default `[]`, every
/// segment matching upstream `^[a-zA-Z0-9_]+$`.
pub(crate) fn parse_registry_scope(obj: &Map<String, Value>) -> Result<Vec<String>, ApiError> {
    match obj.get("scope") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => {
            let mut scope = Vec::with_capacity(items.len());
            for item in items {
                match item.as_str() {
                    Some(s)
                        if !s.is_empty()
                            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') =>
                    {
                        scope.push(s.to_string())
                    }
                    _ => {
                        return Err(ApiError::invalid_param(
                            "scope",
                            "segments must match ^[a-zA-Z0-9_]+$",
                        ));
                    }
                }
            }
            Ok(scope)
        }
        Some(_) => Err(ApiError::invalid_param(
            "scope",
            "must be an array of strings",
        )),
    }
}

/// Resolve a registry call's domain (upstream `RegistryApiService` call
/// parity): the caller's access-token id when credentialed, else the
/// explicit `domain` param (or null). Token-id resolution failure falls
/// back to the explicit param — a missing grant row must not fail the
/// registry call.
pub(crate) fn registry_domain(
    services: &AppServices,
    caller: &AuthenticatedCaller,
    obj: &Map<String, Value>,
) -> Result<Option<String>, ApiError> {
    let explicit = match obj.get("domain") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            return Err(ApiError::invalid_param("domain", "must be a string"));
        }
    };
    let token_id = services
        .auth
        .get_token_by_hash(&hash_token(&caller.raw_token))
        .map_err(ApiError::from_port_error)?
        .map(|record| record.id);
    Ok(token_id.or(explicit))
}

/// Batch P helpers (email identity loop + account move + small reads).
///
/// Lowercase-alphanumeric random code (`EmailService` `secureRndstr`
/// parity: `L_CHARS`).
pub(crate) fn random_code(len: usize) -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..len)
        .map(|_| {
            let i = (rand::random::<u8>() as usize) % CHARS.len();
            CHARS[i] as char
        })
        .collect()
}
