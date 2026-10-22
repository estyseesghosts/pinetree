//! Notes dispatcher arms (create).
//!
//! Split from `endpoints/dispatch_notes.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::auth_http::{client_ip, require_caller};
use super::super::parse::{
    draft_file_ids_for_key, draft_visible_user_ids, get_limit, get_limit_with_default, opt_bool,
    opt_draft_cw, opt_draft_text, opt_i64, opt_string, opt_usize, pack_note_list, parse_cursor,
    parse_draft_create, parse_draft_poll_param, parse_draft_update, parse_list_cursor,
    parse_poll_param, parse_reaction_acceptance, parse_visibility, required_str, str_list,
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

/// `POST /api/notes/create` (split from `api_dispatch_blocking`).
pub(crate) fn notes_create(
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
            "visibility",
            "visibleUserIds",
            "text",
            "cw",
            "localOnly",
            "reactionAcceptance",
            "noExtractMentions",
            "noExtractHashtags",
            "noExtractEmojis",
            "replyId",
            "renoteId",
            "channelId",
            "fileIds",
            "mediaIds",
            "poll",
        ],
    )?;
    if let Some(channel) = obj.get("channelId") {
        if !channel.is_null() {
            return Err(ApiError::unsupported_field("channelId"));
        }
    }
    // §4 polls (Misskey `notes/create.ts:168-184` parity).
    let poll_input = parse_poll_param(&obj, "poll", state.services.clock.now_ms())?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:notes")?;
    state.rate_limiter.check(&rate_key, "notes/create")?;
    // Misskey `notes/create.ts:226`: `fileIds ?? mediaIds ?? []`
    // — when `fileIds` is present it wins outright; `mediaIds`
    // is only a legacy fallback. (`notes/edit` mirrors the same
    // precedence.)
    let file_ids = if obj.contains_key("fileIds") {
        str_list(&obj, "fileIds")?
    } else {
        str_list(&obj, "mediaIds")?
    };
    let input = CreateNoteInput {
        author_id: caller.account.id.clone(),
        text: opt_string(&obj, "text")?,
        cw: opt_string(&obj, "cw")?,
        visibility: parse_visibility(obj.get("visibility"))?,
        local_only: opt_bool(&obj, "localOnly", false)?,
        reply_to: opt_string(&obj, "replyId")?,
        reply_parent_uri: None,
        renote_of: opt_string(&obj, "renoteId")?,
        file_ids,
        visible_users: str_list(&obj, "visibleUserIds")?,
        reaction_acceptance: parse_reaction_acceptance(obj.get("reactionAcceptance"))?,
        no_extract_mentions: opt_bool(&obj, "noExtractMentions", false)?,
        no_extract_hashtags: opt_bool(&obj, "noExtractHashtags", false)?,
        no_extract_emojis: opt_bool(&obj, "noExtractEmojis", false)?,
        has_poll: poll_input.is_some(),
        poll: poll_input,
        poll_initial_votes: None,
        has_channel: false,
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        uri: None,
        uri_base: String::new(),
        url: None,
        extra_mentions: Vec::new(),
        extra_tags: Vec::new(),
        quote: None,
    };
    let note = state
        .services
        .create_note(input)
        .map_err(ApiError::from_port_error)?;
    let packed = pack_note(&note, Some(&caller.account.id), &state.services, 0)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(serde_json::json!({ "createdNote": packed })).into_response())
}

/// `POST /api/notes/show` (split from `api_dispatch_blocking`).
pub(crate) fn notes_show(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    _rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let note_id = required_str(&obj, "noteId")?;
    let viewer_id = caller.as_ref().map(|caller| caller.account.id.as_str());
    let note = state
        .services
        .notes_show(&note_id, viewer_id)
        .map_err(ApiError::from_port_error)?;
    let packed =
        pack_note(&note, viewer_id, &state.services, 0).map_err(ApiError::from_port_error)?;
    Ok(Json(packed).into_response())
}

/// `POST /api/notes/delete` (split from `api_dispatch_blocking`).
pub(crate) fn notes_delete(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
    caller: Option<AuthenticatedCaller>,
    rate_key: &str,
    _client_ip: &str,
    _headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    check_allowed_fields(&obj, &["noteId"])?;
    let caller = require_caller(caller)?;
    caller.require_permission("write:notes")?;
    state.rate_limiter.check(&rate_key, "notes/delete")?;
    let note_id = required_str(&obj, "noteId")?;
    state
        .services
        .delete_note(&caller.account.id, &note_id)
        .map_err(ApiError::from_port_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /api/notes/edit` (split from `api_dispatch_blocking`).
pub(crate) fn notes_edit(
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
            "editId",
            "visibility",
            "visibleUserIds",
            "text",
            "cw",
            "localOnly",
            "reactionAcceptance",
            "noExtractMentions",
            "noExtractHashtags",
            "noExtractEmojis",
            "replyId",
            "renoteId",
            "channelId",
            "fileIds",
            "mediaIds",
            "poll",
        ],
    )?;
    if let Some(channel) = obj.get("channelId") {
        if !channel.is_null() {
            // No channels exist: same `UNSUPPORTED_FIELD` rule as
            // `notes/create` (the `NO_SUCH_CHANNEL` below is
            // unreachable while channels are absent).
            return Err(ApiError::unsupported_field("channelId"));
        }
    }
    // Missing/empty `editId` is `NEEDS_EDIT_ID`, not
    // `INVALID_PARAM` (Sharkey error-set parity).
    let edit_id = match obj.get("editId") {
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        _ => {
            return Err(ApiError::from_port_error(PortError::Validation(
                "NEEDS_EDIT_ID".to_string(),
            )));
        }
    };
    // Length pre-gates (Sharkey `edit.ts` parity — the endpoint
    // rejects, the service trims/truncates for the inbound path).
    // Counts are Rust chars (upstream counts UTF-16 code units).
    if let Some(Value::String(text)) = obj.get("text") {
        if text.chars().count() > MAX_NOTE_TEXT_CHARS {
            return Err(ApiError::from_port_error(PortError::Validation(
                "MAX_LENGTH".to_string(),
            )));
        }
    }
    if let Some(Value::String(cw)) = obj.get("cw") {
        if cw.chars().count() > MAX_CW_API_CHARS {
            return Err(ApiError::from_port_error(PortError::Validation(
                "MAX_CW_LENGTH".to_string(),
            )));
        }
    }
    let caller = require_caller(caller)?;
    caller.require_permission("write:notes")?;
    state.rate_limiter.check(&rate_key, "notes/edit")?;
    let requested_visibility = parse_visibility(obj.get("visibility"))?;
    // §8 poll params (same `parse_poll_param` as `notes/create`:
    // `expiredAfter` wins, past expiry is the exact
    // `CANNOT_CREATE_ALREADY_EXPIRED_POLL` ident).
    let poll_input = parse_poll_param(&obj, "poll", state.services.clock.now_ms())?;
    // The edit target must exist here so the renote/reply checks
    // below run against the original note (the service re-checks
    // authorship atomically; a deleted target is `NO_SUCH_NOTE`).
    let original = match state
        .services
        .notes
        .get_note(&edit_id)
        .map_err(ApiError::from_port_error)?
    {
        Some(note) => note,
        None => {
            return Err(ApiError::from_port_error(PortError::NotFound(
                "NO_SUCH_NOTE".to_string(),
            )));
        }
    };
    if original.author_id != caller.account.id {
        return Err(ApiError::from_port_error(PortError::Validation(
            "YOU_ARE_NOT_THE_AUTHOR".to_string(),
        )));
    }
    // Renote checks (Sharkey `edit.ts` endpoint order: self-quote,
    // existence, pure-renote target, quote-of-edited-note,
    // blocking, visibility).
    let mut renote_of: Option<String> = None;
    if let Some(renote_id) = opt_string(&obj, "renoteId")? {
        if renote_id == edit_id {
            return Err(ApiError::from_port_error(PortError::Validation(
                "CANNOT_QUOTE_THE_CURRENT_NOTE".to_string(),
            )));
        }
        let target = match state
            .services
            .notes
            .get_note(&renote_id)
            .map_err(ApiError::from_port_error)?
        {
            Some(note) => note,
            None => {
                return Err(ApiError::from_port_error(PortError::NotFound(
                    "NO_SUCH_RENOTE_TARGET".to_string(),
                )));
            }
        };
        if target.renote_id.is_some() && target.quote.is_none() {
            return Err(ApiError::from_port_error(PortError::Validation(
                "CANNOT_RENOTE_TO_A_PURE_RENOTE".to_string(),
            )));
        }
        if target.renote_id.as_deref() == Some(edit_id.as_str()) {
            return Err(ApiError::from_port_error(PortError::Validation(
                "CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE".to_string(),
            )));
        }
        if target.author_id != caller.account.id
            && state
                .services
                .blocks
                .is_blocking(&target.author_id, &caller.account.id)
                .map_err(ApiError::from_port_error)?
        {
            return Err(ApiError::from_port_error(PortError::Validation(
                "YOU_HAVE_BEEN_BLOCKED".to_string(),
            )));
        }
        if target.visibility == Visibility::Specified
            || (target.visibility == Visibility::Followers && target.author_id != caller.account.id)
        {
            return Err(ApiError::from_port_error(PortError::Validation(
                "CANNOT_RENOTE_DUE_TO_VISIBILITY".to_string(),
            )));
        }
        // Renote-in-channel narrowing (`NO_SUCH_CHANNEL` /
        // `CANNOT_RENOTE_OUTSIDE_OF_CHANNEL`) is unreachable: stored
        // notes never carry a channel (creation rejects them).
        renote_of = Some(renote_id);
    }
    // Reply checks (Sharkey `edit.ts` endpoint order). The supplied
    // reply is validated but then discarded: the service always keeps
    // the original parent (`replyId` immutable).
    if let Some(reply_id) = opt_string(&obj, "replyId")? {
        let reply = match state
            .services
            .notes
            .get_note(&reply_id)
            .map_err(ApiError::from_port_error)?
        {
            Some(note) => note,
            None => {
                return Err(ApiError::from_port_error(PortError::NotFound(
                    "NO_SUCH_REPLY_TARGET".to_string(),
                )));
            }
        };
        if reply.renote_id.is_some() && reply.quote.is_none() {
            return Err(ApiError::from_port_error(PortError::Validation(
                "CANNOT_REPLY_TO_A_PURE_RENOTE".to_string(),
            )));
        }
        let follows_author = reply.author_id == caller.account.id
            || state
                .services
                .follows
                .is_following(&caller.account.id, &reply.author_id)
                .map_err(ApiError::from_port_error)?;
        let viewer =
            crate::domain::policy::ViewerContext::local_user(&caller.account.id, follows_author);
        if !crate::domain::visibility::is_visible(&reply, &viewer) {
            return Err(ApiError::from_port_error(PortError::Validation(
                "CANNOT_REPLY_TO_AN_INVISIBLE_NOTE".to_string(),
            )));
        }
        if reply.visibility == Visibility::Specified
            && requested_visibility != Visibility::Specified
        {
            return Err(ApiError::from_port_error(PortError::Validation(
                "CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY".to_string(),
            )));
        }
        if reply.author_id != caller.account.id
            && state
                .services
                .blocks
                .is_blocking(&reply.author_id, &caller.account.id)
                .map_err(ApiError::from_port_error)?
        {
            return Err(ApiError::from_port_error(PortError::Validation(
                "YOU_HAVE_BEEN_BLOCKED".to_string(),
            )));
        }
    }
    // Misskey `notes/create.ts:226`: `fileIds ?? mediaIds ?? []`
    // — when `fileIds` is present it wins outright; `mediaIds`
    // is only a legacy fallback. (`notes/edit` mirrors the same
    // precedence.)
    let file_ids = if obj.contains_key("fileIds") {
        str_list(&obj, "fileIds")?
    } else {
        str_list(&obj, "mediaIds")?
    };
    let input = EditNoteInput {
        editor_id: caller.account.id.clone(),
        note_id: edit_id,
        text: opt_string(&obj, "text")?,
        cw: opt_string(&obj, "cw")?,
        renote_of,
        file_ids,
        poll: poll_input,
        reaction_acceptance: parse_reaction_acceptance(obj.get("reactionAcceptance"))?,
        visible_users: str_list(&obj, "visibleUserIds")?,
        // `noExtract*` params are accepted and ignored: pinetree
        // extracts mentions/tags unconditionally (same as the
        // `notes/create` arm, which reads them for validation only).
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        uri_base: String::new(),
    };
    let note = state
        .services
        .edit_note(input)
        .map_err(ApiError::from_port_error)?;
    let packed = pack_note(&note, Some(&caller.account.id), &state.services, 0)
        .map_err(ApiError::from_port_error)?;
    Ok(Json(serde_json::json!({ "createdNote": packed })).into_response())
}
