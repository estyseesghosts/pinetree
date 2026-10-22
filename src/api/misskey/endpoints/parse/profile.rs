//! Shared request parsing, validation and packing helpers (profile).
//!
//! Split from `endpoints/parse.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::files_http::identicon;
use super::update_bool_field;
use super::update_string_field;
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

/// Parse the full `i/update` body into an [`UpdateAccountPatch`]
/// (`i/update.ts` `paramDef` parity: all 39 params plus the DIVERGENCE
/// `rejectQuotes`/`defaultReaction`). Length/enum shapes are enforced here
/// as `INVALID_PARAM`; role-gated and cross-entity rules (prohibited words,
/// media checks, mute limits, alias resolution) live in
/// `AppServices::update_profile` with their exact Misskey error codes.
pub(crate) fn parse_profile_patch(
    obj: &Map<String, Value>,
) -> Result<UpdateAccountPatch, ApiError> {
    // `name`: raw value validated at 1..=50 chars (`nameSchema`); the
    // trim / `""` → null step runs in `update_profile`.
    if let Some(raw) = obj.get("name") {
        match raw {
            Value::Null => {}
            Value::String(s) => {
                let n = s.chars().count();
                if n == 0 || n > 50 {
                    return Err(ApiError::invalid_param("name", "must be 1..=50 chars"));
                }
            }
            _ => return Err(ApiError::invalid_param("name", "must be a string")),
        }
    }
    Ok(UpdateAccountPatch {
        display_name: update_string_field(obj, "name")?,
        description: bounded_nullable_string(obj, "description", 1500)?,
        followed_message: bounded_nullable_string(obj, "followedMessage", 256)?,
        location: bounded_nullable_string(obj, "location", 50)?,
        birthday: update_string_field(obj, "birthday")?,
        // Upstream restricts `lang` to its langmap enum; pinetree has no
        // langmap yet, so any 1..=32-char tag is accepted (TODO(§0B)).
        lang: bounded_nullable_string(obj, "lang", 32)?,
        avatar_id: profile_file_id(obj, "avatarId")?,
        avatar_decorations: parse_avatar_decorations(obj)?,
        banner_id: profile_file_id(obj, "bannerId")?,
        fields: parse_profile_fields(obj)?,
        is_locked: update_bool_field(obj, "isLocked")?,
        is_bot: update_bool_field(obj, "isBot")?,
        is_cat: update_bool_field(obj, "isCat")?,
        is_explorable: update_bool_field(obj, "isExplorable")?,
        hide_online_status: update_bool_field(obj, "hideOnlineStatus")?,
        public_reactions: update_bool_field(obj, "publicReactions")?,
        careful_bot: update_bool_field(obj, "carefulBot")?,
        auto_accept_followed: update_bool_field(obj, "autoAcceptFollowed")?,
        follow_approval_local_seconds: bounded_nullable_seconds(obj, "followApprovalLocalSeconds")?,
        follow_approval_remote_seconds: bounded_nullable_seconds(
            obj,
            "followApprovalRemoteSeconds",
        )?,
        no_crawle: update_bool_field(obj, "noCrawle")?,
        prevent_ai_learning: update_bool_field(obj, "preventAiLearning")?,
        require_signin_to_view_contents: update_bool_field(obj, "requireSigninToViewContents")?,
        make_notes_followers_only_before: nullable_integer(obj, "makeNotesFollowersOnlyBefore")?,
        make_notes_hidden_before: nullable_integer(obj, "makeNotesHiddenBefore")?,
        inject_featured_note: update_bool_field(obj, "injectFeaturedNote")?,
        receive_announcement_email: update_bool_field(obj, "receiveAnnouncementEmail")?,
        always_mark_nsfw: update_bool_field(obj, "alwaysMarkNsfw")?,
        auto_sensitive: update_bool_field(obj, "autoSensitive")?,
        following_visibility: parse_follow_visibility(obj, "followingVisibility")?,
        followers_visibility: parse_follow_visibility(obj, "followersVisibility")?,
        chat_scope: parse_chat_scope(obj)?,
        pinned_page_id: profile_file_id(obj, "pinnedPageId")?,
        muted_words: parse_muted_words(obj, "mutedWords")?,
        hard_muted_words: parse_muted_words(obj, "hardMutedWords")?,
        muted_instances: parse_string_array(obj, "mutedInstances", None)?,
        notification_recieve_config: parse_notification_recieve_config(obj)?,
        email_notification_types: parse_string_array(obj, "emailNotificationTypes", None)?,
        also_known_as: parse_string_array(obj, "alsoKnownAs", Some(10))?.map(Some),
        verified_links: None,
        reject_quotes: update_bool_field(obj, "rejectQuotes")?,
        default_reaction: bounded_nullable_string(obj, "defaultReaction", 260)?,
        // DIVERGENCE (Batch N WP8b): plain bool, absent = unchanged.
        show_remote_emojis: update_bool_field(obj, "showRemoteEmojis")?,
        moderator_note: None,
        // Batch P: email/move are endpoint-owned (`i/update-email`,
        // `verify-email`, `i/move`) — never through `i/update`.
        email: None,
        email_verified: None,
        email_verify_code: None,
        moved_to_uri: None,
        moved_at_ms: None,
    })
}

/// Nullable string with a 1..=max char bound (empty strings rejected like
/// upstream `minLength: 1` schemas).
pub(crate) fn bounded_nullable_string(
    obj: &Map<String, Value>,
    key: &str,
    max_chars: usize,
) -> Result<Option<Option<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(s)) => {
            let n = s.chars().count();
            if n == 0 || n > max_chars {
                return Err(ApiError::invalid_param(
                    key,
                    &format!("must be 1..={max_chars} chars"),
                ));
            }
            Ok(Some(Some(s.clone())))
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

/// Nullable file/page id: a non-empty string or explicit null.
pub(crate) fn profile_file_id(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Option<String>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(s)) => {
            if s.is_empty() {
                return Err(ApiError::invalid_param(key, "must be a non-empty id"));
            }
            Ok(Some(Some(s.clone())))
        }
        Some(_) => Err(ApiError::invalid_param(key, "must be a string")),
    }
}

/// Nullable integer (`followApproval*Seconds` are range-checked again in
/// domain validation; `makeNotes*Before` accept any integer).
pub(crate) fn nullable_integer(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Option<i64>>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::Number(n)) => match n.as_i64() {
            Some(v) => Ok(Some(Some(v))),
            None => Err(ApiError::invalid_param(key, "must be an integer")),
        },
        Some(_) => Err(ApiError::invalid_param(key, "must be an integer")),
    }
}

pub(crate) fn bounded_nullable_seconds(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Option<i64>>, ApiError> {
    match nullable_integer(obj, key)? {
        None => Ok(None),
        Some(None) => Ok(Some(None)),
        Some(Some(v)) => {
            if v < 0 || v > crate::domain::account::FOLLOW_APPROVAL_MAX_SECONDS {
                return Err(ApiError::invalid_param(
                    key,
                    "must be 0..=2592000 (30 days)",
                ));
            }
            Ok(Some(Some(v)))
        }
    }
}

pub(crate) fn parse_follow_visibility(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<FollowVisibility>, ApiError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => FollowVisibility::parse(s)
            .map(Some)
            .ok_or_else(|| ApiError::invalid_param(key, "must be public, followers, or private")),
        Some(_) => Err(ApiError::invalid_param(
            key,
            "must be public, followers, or private",
        )),
    }
}

pub(crate) fn parse_chat_scope(obj: &Map<String, Value>) -> Result<Option<ChatScope>, ApiError> {
    match obj.get("chatScope") {
        None => Ok(None),
        Some(Value::String(s)) => ChatScope::parse(s).map(Some).ok_or_else(|| {
            ApiError::invalid_param(
                "chatScope",
                "must be everyone, followers, following, mutual, or none",
            )
        }),
        Some(_) => Err(ApiError::invalid_param(
            "chatScope",
            "must be everyone, followers, following, mutual, or none",
        )),
    }
}

/// `fields`: max 16 `{name, value}` entries; blank-after-trim rows are
/// dropped (`update.ts` fields filter parity).
pub(crate) fn parse_profile_fields(
    obj: &Map<String, Value>,
) -> Result<Option<Vec<ProfileField>>, ApiError> {
    let Some(value) = obj.get("fields") else {
        return Ok(None);
    };
    let Value::Array(items) = value else {
        return Err(ApiError::invalid_param("fields", "must be an array"));
    };
    if items.len() > 16 {
        return Err(ApiError::invalid_param(
            "fields",
            "must have at most 16 entries",
        ));
    }
    let mut out = Vec::new();
    for item in items {
        let entry = item
            .as_object()
            .ok_or_else(|| ApiError::invalid_param("fields", "each entry must be {name, value}"))?;
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::invalid_param("fields", "each entry must be {name, value}"))?;
        let val = entry
            .get("value")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::invalid_param("fields", "each entry must be {name, value}"))?;
        let (name, val) = (name.trim(), val.trim());
        if name.is_empty() || val.is_empty() {
            continue;
        }
        out.push(ProfileField {
            name: name.to_string(),
            value: val.to_string(),
        });
    }
    Ok(Some(out))
}

/// `avatarDecorations`: max 16 entries with `update.ts` numeric ranges.
pub(crate) fn parse_avatar_decorations(
    obj: &Map<String, Value>,
) -> Result<Option<Vec<AvatarDecoration>>, ApiError> {
    let Some(value) = obj.get("avatarDecorations") else {
        return Ok(None);
    };
    let Value::Array(items) = value else {
        return Err(ApiError::invalid_param(
            "avatarDecorations",
            "must be an array",
        ));
    };
    if items.len() > 16 {
        return Err(ApiError::invalid_param(
            "avatarDecorations",
            "must have at most 16 entries",
        ));
    }
    let mut out = Vec::new();
    for item in items {
        let entry = item.as_object().ok_or_else(|| {
            ApiError::invalid_param("avatarDecorations", "each entry must be an object")
        })?;
        let id = entry.get("id").and_then(Value::as_str).ok_or_else(|| {
            ApiError::invalid_param("avatarDecorations", "each entry requires id")
        })?;
        if id.is_empty() {
            return Err(ApiError::invalid_param(
                "avatarDecorations",
                "each entry requires id",
            ));
        }
        let opt_num = |key: &str, min: f64, max: f64| -> Result<Option<f64>, ApiError> {
            match entry.get(key) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::Number(n)) => {
                    let v = n.as_f64().ok_or_else(|| {
                        ApiError::invalid_param("avatarDecorations", "bad numeric value")
                    })?;
                    if !v.is_finite() || v < min || v > max {
                        return Err(ApiError::invalid_param(
                            "avatarDecorations",
                            "numeric value out of range",
                        ));
                    }
                    Ok(Some(v))
                }
                Some(_) => Err(ApiError::invalid_param(
                    "avatarDecorations",
                    "bad numeric value",
                )),
            }
        };
        let flip_h = match entry.get("flipH") {
            None | Some(Value::Null) => None,
            Some(Value::Bool(b)) => Some(*b),
            Some(_) => {
                return Err(ApiError::invalid_param(
                    "avatarDecorations",
                    "flipH must be a boolean",
                ));
            }
        };
        out.push(AvatarDecoration {
            id: id.to_string(),
            angle: opt_num("angle", -0.5, 0.5)?,
            flip_h,
            offset_x: opt_num("offsetX", -0.25, 0.25)?,
            offset_y: opt_num("offsetY", -0.25, 0.25)?,
        });
    }
    Ok(Some(out))
}

/// `mutedWords`/`hardMutedWords`: each entry a string or string array.
/// Count and regex rules run in `update_profile`.
pub(crate) fn parse_muted_words(
    obj: &Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<MutedWord>>, ApiError> {
    let Some(value) = obj.get(key) else {
        return Ok(None);
    };
    let Value::Array(items) = value else {
        return Err(ApiError::invalid_param(key, "must be an array"));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::String(s) => out.push(MutedWord::Single(s.clone())),
            Value::Array(words) => {
                let mut and = Vec::with_capacity(words.len());
                for word in words {
                    match word {
                        Value::String(s) => and.push(s.clone()),
                        _ => {
                            return Err(ApiError::invalid_param(
                                key,
                                "entries must be strings or string arrays",
                            ));
                        }
                    }
                }
                out.push(MutedWord::And(and));
            }
            _ => {
                return Err(ApiError::invalid_param(
                    key,
                    "entries must be strings or string arrays",
                ));
            }
        }
    }
    Ok(Some(out))
}

/// Plain string arrays (`mutedInstances`, `emailNotificationTypes`,
/// `alsoKnownAs` with `maxItems: 10` + `uniqueItems` parity).
pub(crate) fn parse_string_array(
    obj: &Map<String, Value>,
    key: &str,
    max_items: Option<usize>,
) -> Result<Option<Vec<String>>, ApiError> {
    let Some(value) = obj.get(key) else {
        return Ok(None);
    };
    let Value::Array(items) = value else {
        return Err(ApiError::invalid_param(key, "must be an array"));
    };
    if let Some(max) = max_items {
        if items.len() > max {
            return Err(ApiError::invalid_param(
                key,
                &format!("must have at most {max} entries"),
            ));
        }
    }
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::String(s) => out.push(s.clone()),
            _ => return Err(ApiError::invalid_param(key, "must be an array of strings")),
        }
    }
    if key == "alsoKnownAs" {
        let mut seen = std::collections::HashSet::new();
        for entry in &out {
            if !seen.insert(entry) {
                return Err(ApiError::invalid_param(key, "entries must be unique"));
            }
        }
    }
    Ok(Some(out))
}

/// `notificationRecieveConfig` (upstream misspelling kept): an object whose
/// values are `{type}` or `{type: list, userListId}` shapes.
pub(crate) fn parse_notification_recieve_config(
    obj: &Map<String, Value>,
) -> Result<Option<std::collections::BTreeMap<String, NotificationRecieveConfig>>, ApiError> {
    let Some(value) = obj.get("notificationRecieveConfig") else {
        return Ok(None);
    };
    let Value::Object(map) = value else {
        return Err(ApiError::invalid_param(
            "notificationRecieveConfig",
            "must be an object",
        ));
    };
    let mut out = std::collections::BTreeMap::new();
    for (key, entry) in map {
        let entry_obj = entry.as_object().ok_or_else(|| {
            ApiError::invalid_param("notificationRecieveConfig", "each value must be an object")
        })?;
        let kind = entry_obj
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::invalid_param("notificationRecieveConfig", "each value requires type")
            })?;
        let user_list_id = match entry_obj.get("userListId") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => {
                return Err(ApiError::invalid_param(
                    "notificationRecieveConfig",
                    "userListId must be a string",
                ));
            }
        };
        let config = NotificationRecieveConfig {
            kind: kind.to_string(),
            user_list_id,
        };
        config
            .validate()
            .map_err(|_| ApiError::invalid_param("notificationRecieveConfig", "bad entry shape"))?;
        out.insert(key.clone(), config);
    }
    Ok(Some(out))
}
