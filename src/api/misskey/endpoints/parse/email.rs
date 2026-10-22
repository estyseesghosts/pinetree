//! Shared request parsing, validation and packing helpers (email).
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

/// Basic email shape check (upstream `validateEmailForAccount` parity on
/// the shape half: one `@`, non-empty local/domain, dot in domain,
/// 3..=254 chars).
pub(crate) fn email_shape_valid(addr: &str) -> bool {
    if addr.len() < 3 || addr.len() > 254 {
        return false;
    }
    let mut parts = addr.split('@');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(local), Some(domain), None) => {
            !local.is_empty() && !domain.is_empty() && domain.contains('.')
        }
        _ => false,
    }
}

/// Email availability (upstream `validateEmailForAccount` parity on the
/// uniqueness half): no OTHER account holds a VERIFIED copy of this address
/// (case-insensitive; unverified rows do not block, like upstream).
pub(crate) fn email_available(
    services: &AppServices,
    account_id: &str,
    addr: &str,
) -> Result<bool, ApiError> {
    let needle = addr.to_lowercase();
    let mut after: Option<String> = None;
    loop {
        let (page, next) = services
            .accounts
            .all_accounts(after.as_deref(), 500)
            .map_err(ApiError::from_port_error)?;
        if page.is_empty() {
            return Ok(true);
        }
        for acc in page {
            after = Some(acc.id.clone());
            if acc.id != account_id {
                if let Some(email) = acc.email.as_deref() {
                    if acc.email_verified && email.to_lowercase() == needle {
                        return Ok(false);
                    }
                }
            }
        }
        if next.is_none() {
            return Ok(true);
        }
    }
}

/// Split `user` / `user@host` (upstream `Acct.parse` parity).
pub(crate) fn parse_acct(acct: &str) -> (String, Option<String>) {
    let trimmed = acct.trim().trim_start_matches('@');
    match trimmed.split_once('@') {
        Some((user, host)) => (user.to_string(), Some(host.to_string())),
        None => (trimmed.to_string(), None),
    }
}

/// Root check for `i/move` (upstream `meta.rootUserId` parity): pinetree
/// stores no root id, so root is the instance's initial administrator —
/// the lowest account id among admins (DIVERGENCE, documented).
pub(crate) fn is_oldest_admin(services: &AppServices, account_id: &str) -> Result<bool, ApiError> {
    let mut after: Option<String> = None;
    loop {
        let (page, next) = services
            .accounts
            .all_accounts(after.as_deref(), 500)
            .map_err(ApiError::from_port_error)?;
        if page.is_empty() {
            return Ok(false);
        }
        for acc in page {
            after = Some(acc.id.clone());
            if acc.is_admin && acc.id.as_str() < account_id {
                return Ok(false);
            }
        }
        if next.is_none() {
            return Ok(true);
        }
    }
}

/// `postMoveProcess` follow migration parity (shared by outbound `i/move`
/// and inbound `Move`): every LOCAL follower of `old_id` unfollows it and
/// follows `new_id`. Paged, one transaction per edge; already-migrated
/// pairs converge (stale old edge dropped, existing new edge kept).
pub(crate) fn migrate_followers_to(
    services: &AppServices,
    old_id: &str,
    new_id: &str,
) -> Result<(), ApiError> {
    let mut cursor: Option<String> = None;
    loop {
        let page = services
            .follows
            .followers_of(old_id, cursor.as_deref(), 100)
            .map_err(ApiError::from_port_error)?;
        if page.is_empty() {
            break;
        }
        for edge in &page {
            cursor = Some(edge.id.clone());
            let follower = services
                .accounts
                .find_by_id(&edge.follower_id)
                .map_err(ApiError::from_port_error)?;
            let Some(follower) = follower else {
                continue;
            };
            if !follower.is_local() || follower.id == *new_id {
                continue;
            }
            if services
                .follows
                .is_following(&follower.id, new_id)
                .map_err(ApiError::from_port_error)?
            {
                let _ = services.unfollow(&follower.id, old_id);
                continue;
            }
            let _ = services.unfollow(&follower.id, old_id);
            let _ = services.follow(&follower.id, new_id, false);
        }
        if page.len() < 100 {
            break;
        }
    }
    Ok(())
}
