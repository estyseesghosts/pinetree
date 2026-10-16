//! Misskey response packers (Slice D4).
//!
//! Render domain types into Misskey 2026.10 wire shapes
//! (`misskey/packages/backend/src/models/json-schema/`:
//! `note.ts`, `user.ts`, `drive-file.ts`, `meta.ts`). Field sets are
//! pinned by snapshot tests at the bottom of this file that read the
//! actual checkout schemas from disk. The tests skip (pass vacuously) when
//! the git-ignored `./misskey` checkout is absent, e.g. in clean clones.

use serde_json::{Map, Value};

use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::Account;
use crate::domain::follow::Follow;
use crate::domain::media::MediaMeta;
use crate::domain::note::{Note, Visibility};
use crate::domain::notification::Notification;
use crate::domain::visibility::{ViewerContext, is_visible};

/// Misskey server version we claim compatibility with.
pub use crate::activitypub::PACKER_VERSION;

/// Millisecond timestamps as ISO-8601 UTC strings
/// (`"YYYY-MM-DDTHH:MM:SS.sssZ"`).
pub use crate::activitypub::format_millis;

fn avatar_url(account: &Account, public_url: &str) -> String {
    let base = public_url.trim_end_matches('/');
    match &account.avatar_id {
        Some(id) => format!("{base}/files/{id}"),
        None => format!("{base}/identicon/{}", account.id),
    }
}

fn banner_url(account: &Account, public_url: &str) -> Value {
    match &account.banner_id {
        Some(id) => Value::String(format!("{}/files/{id}", public_url.trim_end_matches('/'))),
        None => Value::Null,
    }
}

fn note_time_ms(note: &Note, services: &AppServices) -> i64 {
    services
        .ids
        .parse_time(&note.id)
        .unwrap_or_else(|_| services.clock.now_ms())
}

/// `UserLite` (`user.ts` `packedUserLiteSchema`).
pub fn pack_user_lite(account: &Account, public_url: &str, clock_ms: i64) -> Value {
    let _ = clock_ms;
    serde_json::json!({
        "id": account.id,
        "name": account.display_name,
        "username": account.username,
        "host": account.host,
        "avatarUrl": avatar_url(account, public_url),
        "avatarBlurhash": null,
        "avatarDecorations": [],
        "isBot": account.is_bot,
        "isCat": account.is_cat,
        "emojis": {},
        "onlineStatus": "online",
    })
}

/// `UserDetailedNotMe` = `UserLite` + `UserDetailedNotMeOnly`, including
/// relation fields against `viewer_id` (`user.ts`
/// `packedUserDetailedNotMeOnlySchema`).
pub fn pack_user_detailed_not_me(
    account: &Account,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let now = services.clock.now_ms();
    let created_at = format_millis(
        services
            .ids
            .parse_time(&account.id)
            .unwrap_or(now),
    );
    let actor_url = account.actor_url(&services.public_url);
    let (is_following, is_followed) = match viewer_id {
        Some(viewer) if viewer != account.id => (
            services
                .follows
                .is_following(viewer, &account.id)
                .unwrap_or(false),
            services
                .follows
                .is_following(&account.id, viewer)
                .unwrap_or(false),
        ),
        _ => (false, false),
    };
    let (pending_from_you, pending_to_you) = match viewer_id {
        Some(viewer) if viewer != account.id => (
            services
                .follows
                .get_follow_request(viewer, &account.id)
                .map(|r| r.is_some())
                .unwrap_or(false),
            services
                .follows
                .get_follow_request(&account.id, viewer)
                .map(|r| r.is_some())
                .unwrap_or(false),
        ),
        _ => (false, false),
    };
    let mut obj = pack_user_lite(account, &services.public_url, now)
        .as_object()
        .cloned()
        .unwrap_or_default();
    obj.insert("url".to_string(), Value::String(actor_url.clone()));
    obj.insert("uri".to_string(), Value::String(actor_url));
    obj.insert("movedTo".to_string(), Value::Null);
    obj.insert("alsoKnownAs".to_string(), Value::Null);
    obj.insert("createdAt".to_string(), Value::String(created_at));
    obj.insert("updatedAt".to_string(), Value::Null);
    obj.insert("lastFetchedAt".to_string(), Value::Null);
    obj.insert("bannerUrl".to_string(), banner_url(account, &services.public_url));
    obj.insert("bannerBlurhash".to_string(), Value::Null);
    obj.insert("isLocked".to_string(), Value::Bool(account.is_locked));
    obj.insert("isSilenced".to_string(), Value::Bool(false));
    obj.insert(
        "isSuspended".to_string(),
        Value::Bool(account.is_suspended),
    );
    obj.insert(
        "description".to_string(),
        account
            .description
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "location".to_string(),
        account
            .location
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "birthday".to_string(),
        account
            .birthday
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert("lang".to_string(), Value::Null);
    obj.insert("fields".to_string(), Value::Array(Vec::new()));
    obj.insert("verifiedLinks".to_string(), Value::Array(Vec::new()));
    obj.insert(
        "followersCount".to_string(),
        Value::Number(account.followers_count.into()),
    );
    obj.insert(
        "followingCount".to_string(),
        Value::Number(account.following_count.into()),
    );
    obj.insert(
        "notesCount".to_string(),
        Value::Number(account.notes_count.into()),
    );
    obj.insert("pinnedNoteIds".to_string(), Value::Array(Vec::new()));
    obj.insert("pinnedNotes".to_string(), Value::Array(Vec::new()));
    obj.insert("pinnedPageId".to_string(), Value::Null);
    obj.insert("pinnedPage".to_string(), Value::Null);
    obj.insert("publicReactions".to_string(), Value::Bool(true));
    obj.insert(
        "followingVisibility".to_string(),
        Value::String("public".to_string()),
    );
    obj.insert(
        "followersVisibility".to_string(),
        Value::String("public".to_string()),
    );
    obj.insert(
        "chatScope".to_string(),
        Value::String("everyone".to_string()),
    );
    obj.insert("canChat".to_string(), Value::Bool(false));
    obj.insert("roles".to_string(), Value::Array(Vec::new()));
    obj.insert("memo".to_string(), Value::Null);
    obj.insert("isFollowing".to_string(), Value::Bool(is_following));
    obj.insert("isFollowed".to_string(), Value::Bool(is_followed));
    obj.insert(
        "hasPendingFollowRequestFromYou".to_string(),
        Value::Bool(pending_from_you),
    );
    obj.insert(
        "hasPendingFollowRequestToYou".to_string(),
        Value::Bool(pending_to_you),
    );
    obj.insert("isBlocking".to_string(), Value::Bool(false));
    obj.insert("isBlocked".to_string(), Value::Bool(false));
    obj.insert("isMuted".to_string(), Value::Bool(false));
    obj.insert("isRenoteMuted".to_string(), Value::Bool(false));
    Ok(Value::Object(obj))
}

/// `Following` (`following.ts` `packedFollowingSchema`):
/// `{id, createdAt, followerId, followeeId, follower, followee}` with both
/// sides packed as `UserDetailedNotMe` (`Null` when the account row is
/// gone). `createdAt` derives from the follow-record id timestamp (aidx ids
/// are time-ordered; profile §3).
pub fn pack_following(
    follow: &Follow,
    viewer_id: Option<&str>,
    services: &AppServices,
) -> Result<Value, PortError> {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&follow.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let follower = match services.accounts.find_by_id(&follow.follower_id)? {
        Some(account) => pack_user_detailed_not_me(&account, viewer_id, services)?,
        None => Value::Null,
    };
    let followee = match services.accounts.find_by_id(&follow.followee_id)? {
        Some(account) => pack_user_detailed_not_me(&account, viewer_id, services)?,
        None => Value::Null,
    };
    Ok(serde_json::json!({
        "id": follow.id,
        "createdAt": created_at,
        "followerId": follow.follower_id,
        "followeeId": follow.followee_id,
        "follower": follower,
        "followee": followee,
    }))
}

fn default_policies() -> Value {
    serde_json::json!({
        "gtlAvailable": true,
        "ltlAvailable": true,
        "canPublicNote": true,
        "canInvite": false,
        "canManageCustomEmojis": false,
        "canSearchNotes": true,
        "canHideAds": false,
        "driveCapacityMb": 1024,
        "alwaysMarkNsfw": false,
        "SkipNsfwDetection": false,
        "pinLimit": 5,
        "antennaLimit": 5,
        "wordMuteLimit": 200,
        "webhookLimit": 0,
        "clipLimit": 5,
        "noteEachClipsLimit": 200,
        "userListLimit": 5,
        "userEachUserListsLimit": 50,
        "rateLimitFactor": 1,
        "avatarDecorationLimit": 5,
        "canImportAntennas": false,
        "canImportBlocking": false,
        "canImportFollowing": false,
        "canImportMuting": false,
        "canImportUserLists": false,
        "chatAvailability": "unavailable",
    })
}

/// `MeDetailed` = `UserDetailedNotMe` + `MeDetailedOnly` (`user.ts`
/// `packedMeDetailedOnlySchema`). Secrets (`email`, `emailVerified`,
/// `securityKeysList`) are included only when `include_secrets` is true
/// (first-party session/signup).
pub fn pack_me_detailed(
    account: &Account,
    include_secrets: bool,
    services: &AppServices,
) -> Result<Value, PortError> {
    let mut obj = pack_user_detailed_not_me(account, Some(&account.id), services)?
        .as_object()
        .cloned()
        .unwrap_or_default();
    obj.insert(
        "avatarId".to_string(),
        account
            .avatar_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "bannerId".to_string(),
        account
            .banner_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert("followedMessage".to_string(), Value::Null);
    obj.insert("isModerator".to_string(), Value::Bool(false));
    obj.insert("isAdmin".to_string(), Value::Bool(false));
    obj.insert("injectFeaturedNote".to_string(), Value::Bool(false));
    obj.insert("receiveAnnouncementEmail".to_string(), Value::Bool(false));
    obj.insert("alwaysMarkNsfw".to_string(), Value::Bool(false));
    obj.insert("autoSensitive".to_string(), Value::Bool(false));
    obj.insert("carefulBot".to_string(), Value::Bool(false));
    obj.insert("autoAcceptFollowed".to_string(), Value::Bool(false));
    obj.insert("noCrawle".to_string(), Value::Bool(false));
    obj.insert("preventAiLearning".to_string(), Value::Bool(false));
    obj.insert(
        "isExplorable".to_string(),
        Value::Bool(account.is_explorable),
    );
    obj.insert("isDeleted".to_string(), Value::Bool(account.is_deleted));
    obj.insert(
        "twoFactorBackupCodesStock".to_string(),
        Value::String("none".to_string()),
    );
    obj.insert("hideOnlineStatus".to_string(), Value::Bool(false));
    obj.insert(
        "hasUnreadSpecifiedNotes".to_string(),
        Value::Bool(false),
    );
    obj.insert("hasUnreadMentions".to_string(), Value::Bool(false));
    obj.insert("hasUnreadAnnouncement".to_string(), Value::Bool(false));
    obj.insert("unreadAnnouncements".to_string(), Value::Array(Vec::new()));
    obj.insert("hasUnreadAntenna".to_string(), Value::Bool(false));
    obj.insert("hasUnreadChannel".to_string(), Value::Bool(false));
    obj.insert("hasUnreadChatMessages".to_string(), Value::Bool(false));
    obj.insert("hasUnreadNotification".to_string(), Value::Bool(false));
    obj.insert(
        "hasPendingReceivedFollowRequest".to_string(),
        Value::Bool(false),
    );
    obj.insert("unreadNotificationsCount".to_string(), Value::Number(0.into()));
    obj.insert("mutedWords".to_string(), Value::Array(Vec::new()));
    obj.insert("hardMutedWords".to_string(), Value::Array(Vec::new()));
    obj.insert("mutedInstances".to_string(), Value::Array(Vec::new()));
    obj.insert(
        "notificationRecieveConfig".to_string(),
        Value::Object(Map::new()),
    );
    obj.insert(
        "emailNotificationTypes".to_string(),
        Value::Array(Vec::new()),
    );
    obj.insert("achievements".to_string(), Value::Array(Vec::new()));
    obj.insert("loggedInDays".to_string(), Value::Number(1.into()));
    obj.insert("policies".to_string(), default_policies());
    obj.insert("twoFactorEnabled".to_string(), Value::Bool(false));
    obj.insert("usePasswordLessLogin".to_string(), Value::Bool(false));
    obj.insert("securityKeys".to_string(), Value::Bool(false));
    obj.insert("followApprovalLocalSeconds".to_string(), Value::Null);
    obj.insert("followApprovalRemoteSeconds".to_string(), Value::Null);
    if include_secrets {
        obj.insert("email".to_string(), Value::Null);
        obj.insert("emailVerified".to_string(), Value::Null);
        obj.insert("securityKeysList".to_string(), Value::Array(Vec::new()));
    }
    Ok(Value::Object(obj))
}

/// `DriveFile` (`drive-file.ts` `packedDriveFileSchema`).
pub fn pack_drive_file(media: &MediaMeta, services: &AppServices) -> Value {
    let created_at = format_millis(
        services
            .ids
            .parse_time(&media.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let mut properties = Map::new();
    if let Some(width) = media.properties.width {
        properties.insert("width".to_string(), Value::Number(width.into()));
    }
    if let Some(height) = media.properties.height {
        properties.insert("height".to_string(), Value::Number(height.into()));
    }
    if let Some(orientation) = media.properties.orientation {
        properties.insert("orientation".to_string(), Value::Number(orientation.into()));
    }
    if let Some(avg_color) = &media.properties.avg_color {
        properties.insert("avgColor".to_string(), Value::String(avg_color.clone()));
    }
    serde_json::json!({
        "id": media.id,
        "createdAt": created_at,
        "name": media.name,
        "type": media.mime,
        "md5": media.hash,
        "size": media.size_bytes,
        "isSensitive": media.is_sensitive,
        "blurhash": media.blurhash,
        "properties": properties,
        "url": media.url,
        "thumbnailUrl": media.thumbnail_url,
        "comment": media.comment,
        "folderId": null,
        "folder": null,
        "userId": media.owner_id,
        "user": null,
    })
}

fn viewer_context_for(
    services: &AppServices,
    viewer_id: Option<&str>,
    author_id: &str,
) -> ViewerContext {
    match viewer_id {
        None => ViewerContext::anonymous(),
        Some(viewer) => {
            let follows_author = if viewer == author_id {
                true
            } else {
                services
                    .follows
                    .is_following(viewer, author_id)
                    .unwrap_or(false)
            };
            ViewerContext::local_user(viewer, follows_author)
        }
    }
}

fn pack_nested_note(
    target_id: Option<&str>,
    viewer_id: Option<&str>,
    services: &AppServices,
    depth: u8,
) -> Result<Value, PortError> {
    let Some(target_id) = target_id else {
        return Ok(Value::Null);
    };
    if depth > 0 {
        return Ok(Value::Null);
    }
    let target = match services.notes.get_note(target_id)? {
        None => return Ok(Value::Null),
        Some(note) => note,
    };
    let viewer = viewer_context_for(services, viewer_id, &target.author_id);
    if !is_visible(&target, &viewer) {
        return Ok(Value::Null);
    }
    pack_note(&target, viewer_id, services, depth + 1)
}

/// `Note` (`note.ts` `packedNoteSchema`): `user` packed inline, `files`
/// from `file_ids`, `myReaction` for `viewer_id`, `visibleUserIds` for
/// specified notes, and one level of nested `reply`/`renote` when
/// `depth == 0` and visible to the viewer.
pub fn pack_note(
    note: &Note,
    viewer_id: Option<&str>,
    services: &AppServices,
    depth: u8,
) -> Result<Value, PortError> {
    let author = services
        .accounts
        .find_by_id(&note.author_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let user = pack_user_lite(&author, &services.public_url, services.clock.now_ms());
    let reaction_count: u64 = note.reactions.values().sum();
    let mut files = Vec::new();
    for fid in &note.file_ids {
        if let Some(meta) = services.media.get(fid)? {
            files.push(pack_drive_file(&meta, services));
        }
    }
    let my_reaction = match viewer_id {
        Some(viewer) => services
            .reactions
            .get_reaction(viewer, &note.id)?
            .map(|r| Value::String(r.reaction))
            .unwrap_or(Value::Null),
        None => Value::Null,
    };
    let mut obj = Map::new();
    obj.insert("id".to_string(), Value::String(note.id.clone()));
    obj.insert(
        "createdAt".to_string(),
        Value::String(format_millis(note_time_ms(note, services))),
    );
    obj.insert("userId".to_string(), Value::String(note.author_id.clone()));
    obj.insert("user".to_string(), user);
    obj.insert(
        "text".to_string(),
        note.text.clone().map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert(
        "cw".to_string(),
        note.cw.clone().map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert(
        "visibility".to_string(),
        serde_json::to_value(&note.visibility).unwrap_or(Value::String("public".to_string())),
    );
    obj.insert("localOnly".to_string(), Value::Bool(note.local_only));
    obj.insert(
        "reactionAcceptance".to_string(),
        note.reaction_acceptance
            .as_ref()
            .and_then(|a| serde_json::to_value(a).ok())
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "renoteCount".to_string(),
        Value::Number(note.renote_count.into()),
    );
    obj.insert(
        "repliesCount".to_string(),
        Value::Number(note.replies_count.into()),
    );
    obj.insert(
        "reactionCount".to_string(),
        Value::Number(reaction_count.into()),
    );
    obj.insert(
        "reactions".to_string(),
        serde_json::to_value(&note.reactions).unwrap_or(Value::Object(Map::new())),
    );
    obj.insert("reactionEmojis".to_string(), Value::Object(Map::new()));
    obj.insert(
        "fileIds".to_string(),
        Value::Array(note.file_ids.iter().map(|id| Value::String(id.clone())).collect()),
    );
    obj.insert("files".to_string(), Value::Array(files));
    obj.insert(
        "replyId".to_string(),
        note.reply_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "renoteId".to_string(),
        note.renote_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "reply".to_string(),
        pack_nested_note(note.reply_id.as_deref(), viewer_id, services, depth)?,
    );
    obj.insert(
        "renote".to_string(),
        pack_nested_note(note.renote_id.as_deref(), viewer_id, services, depth)?,
    );
    // Structured-quote extensions (Pinetree informational, non-standard).
    // Present on quotes only — never on ordinary pure renotes. Visibility
    // stays mandatory and separate from authorization: rejected/unapproved
    // quotes still render their relation subject to the normal visibility
    // rules below.
    if let Some(quote) = note.quote.as_ref() {
        obj.insert(
            "quoteAuthorizationStatus".to_string(),
            Value::String(quote.authorization.api_status().to_string()),
        );
        let quoting_public = matches!(
            note.visibility,
            Visibility::Public | Visibility::Home
        );
        match quote.target_note_id.as_deref() {
            Some(tid) => {
                let visible = match services.notes.get_note(tid)? {
                    Some(target) => {
                        let viewer =
                            viewer_context_for(services, viewer_id, &target.author_id);
                        crate::domain::visibility::is_visible(&target, &viewer)
                    }
                    // Deleted or missing target: stable unavailable fallback.
                    None => false,
                };
                if !visible {
                    // Never serialize the protected object or leak its URI;
                    // the `renote` slot is already null via `pack_nested_note`.
                    obj.insert("quoteTargetUnavailable".to_string(), Value::Bool(true));
                }
            }
            // Unresolved target: preserve the relationship with its URI when
            // safe to disclose (public quoting posts), otherwise a
            // controlled unavailable marker. Never fabricate a `renoteId`.
            None => {
                if quoting_public && !note.local_only {
                    obj.insert(
                        "quoteTargetUri".to_string(),
                        Value::String(quote.target_uri.clone()),
                    );
                } else {
                    obj.insert("quoteTargetUnavailable".to_string(), Value::Bool(true));
                }
            }
        }
    }
    obj.insert(
        "mentions".to_string(),
        Value::Array(note.mentions.iter().map(|id| Value::String(id.clone())).collect()),
    );
    if note.visibility == Visibility::Specified {
        obj.insert(
            "visibleUserIds".to_string(),
            Value::Array(
                note.visible_user_ids
                    .iter()
                    .map(|id| Value::String(id.clone()))
                    .collect(),
            ),
        );
    }
    obj.insert(
        "tags".to_string(),
        Value::Array(note.tags.iter().map(|tag| Value::String(tag.clone())).collect()),
    );
    obj.insert("emojis".to_string(), Value::Object(Map::new()));
    obj.insert("myReaction".to_string(), my_reaction);
    // Optional AP identity fields (Misskey `note.ts` `uri`/`url`); `channel`
    // is always null — channels do not exist here.
    obj.insert(
        "uri".to_string(),
        note.uri.clone().map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert(
        "url".to_string(),
        note.url.clone().map(Value::String).unwrap_or(Value::Null),
    );
    obj.insert("channel".to_string(), Value::Null);
    Ok(Value::Object(obj))
}

/// `Notification` pack shared by `i/notifications` and the streaming
/// worker's `main` fan-out (moved from the endpoint module in Phase 3 so
/// both adapters pack identically).
pub fn pack_notification(notification: &Notification, services: &AppServices) -> Value {
    let created_at = services
        .ids
        .parse_time(&notification.id)
        .map(format_millis)
        .unwrap_or_else(|_| format_millis(services.clock.now_ms()));
    let kind = serde_json::to_value(&notification.kind)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "follow".to_string());
    let user = match services.accounts.find_by_id(&notification.notifier_id) {
        Ok(Some(account)) => {
            pack_user_lite(&account, &services.public_url, services.clock.now_ms())
        }
        _ => Value::Null,
    };
    let packed_note = match notification.note_id.as_deref() {
        Some(note_id) => match services.notes.get_note(note_id) {
            Ok(Some(target)) => {
                pack_note(&target, Some(&notification.notifiee_id), services, 0)
                    .unwrap_or(Value::Null)
            }
            _ => Value::Null,
        },
        None => Value::Null,
    };
    // Misskey `notification.ts` is a per-type union: `note` exists only on
    // note-bearing variants, `reaction` only on the reaction variant.
    let mut obj = serde_json::json!({
        "id": notification.id,
        "createdAt": created_at,
        "type": kind,
        "isRead": notification.is_read,
        "userId": notification.notifier_id,
        "user": user,
    });
    if let Some(map) = obj.as_object_mut() {
        if notification.note_id.is_some() {
            map.insert("note".to_string(), packed_note);
        }
        if let Some(reaction) = notification.reaction.clone() {
            map.insert("reaction".to_string(), Value::String(reaction));
        }
    }
    obj
}

/// Instance metadata: `MetaLite` always, plus `MetaDetailedOnly` when
/// `detail` is true (`meta.ts`).
pub fn pack_meta(services: &AppServices, detail: bool) -> Value {    let uri = services.public_url.trim_end_matches('/').to_string();
    let mut obj = Map::new();
    obj.insert("maintainerName".to_string(), Value::Null);
    obj.insert("maintainerEmail".to_string(), Value::Null);
    obj.insert(
        "version".to_string(),
        Value::String(PACKER_VERSION.to_string()),
    );
    obj.insert("providesTarball".to_string(), Value::Bool(false));
    obj.insert("name".to_string(), Value::String("Pinetree".to_string()));
    obj.insert("shortName".to_string(), Value::String("Pinetree".to_string()));
    obj.insert("uri".to_string(), Value::String(uri.clone()));
    obj.insert("description".to_string(), Value::Null);
    obj.insert(
        "langs".to_string(),
        Value::Array(vec![Value::String("en".to_string())]),
    );
    obj.insert("tosUrl".to_string(), Value::Null);
    obj.insert("repositoryUrl".to_string(), Value::Null);
    obj.insert("feedbackUrl".to_string(), Value::Null);
    obj.insert("defaultDarkTheme".to_string(), Value::Null);
    obj.insert("defaultLightTheme".to_string(), Value::Null);
    obj.insert(
        "clientOptions".to_string(),
        serde_json::json!({
            "entrancePageStyle": "simple",
            "showTimelineForVisitor": true,
            "showActivitiesForVisitor": true,
        }),
    );
    obj.insert("disableRegistration".to_string(), Value::Bool(false));
    obj.insert("emailRequiredForSignup".to_string(), Value::Bool(false));
    obj.insert("enableHcaptcha".to_string(), Value::Bool(false));
    obj.insert("hcaptchaSiteKey".to_string(), Value::Null);
    obj.insert("enableMcaptcha".to_string(), Value::Bool(false));
    obj.insert("mcaptchaSiteKey".to_string(), Value::Null);
    obj.insert("mcaptchaInstanceUrl".to_string(), Value::Null);
    obj.insert("enableRecaptcha".to_string(), Value::Bool(false));
    obj.insert("recaptchaSiteKey".to_string(), Value::Null);
    obj.insert("enableTurnstile".to_string(), Value::Bool(false));
    obj.insert("turnstileSiteKey".to_string(), Value::Null);
    obj.insert("enableTestcaptcha".to_string(), Value::Bool(false));
    obj.insert("googleAnalyticsMeasurementId".to_string(), Value::Null);
    obj.insert("swPublickey".to_string(), Value::Null);
    obj.insert(
        "mascotImageUrl".to_string(),
        Value::String("/assets/ai.png".to_string()),
    );
    obj.insert("bannerUrl".to_string(), Value::Null);
    obj.insert("serverErrorImageUrl".to_string(), Value::Null);
    obj.insert("infoImageUrl".to_string(), Value::Null);
    obj.insert("notFoundImageUrl".to_string(), Value::Null);
    obj.insert("iconUrl".to_string(), Value::Null);
    obj.insert("maxNoteTextLength".to_string(), Value::Number(3000.into()));
    obj.insert("ads".to_string(), Value::Array(Vec::new()));
    obj.insert("notesPerOneAd".to_string(), Value::Number(0.into()));
    obj.insert("enableEmail".to_string(), Value::Bool(false));
    obj.insert("enableServiceWorker".to_string(), Value::Bool(true));
    obj.insert("translatorAvailable".to_string(), Value::Bool(false));
    obj.insert("sentryForFrontend".to_string(), Value::Null);
    obj.insert(
        "mediaProxy".to_string(),
        Value::String(format!("{uri}/proxy")),
    );
    obj.insert("enableUrlPreview".to_string(), Value::Bool(false));
    obj.insert("backgroundImageUrl".to_string(), Value::Null);
    obj.insert("impressumUrl".to_string(), Value::Null);
    obj.insert("logoImageUrl".to_string(), Value::Null);
    obj.insert("privacyPolicyUrl".to_string(), Value::Null);
    obj.insert("inquiryUrl".to_string(), Value::Null);
    obj.insert("serverRules".to_string(), Value::Array(Vec::new()));
    obj.insert("themeColor".to_string(), Value::Null);
    obj.insert("policies".to_string(), default_policies());
    obj.insert(
        "noteSearchableScope".to_string(),
        Value::String("local".to_string()),
    );
    obj.insert(
        "maxFileSize".to_string(),
        Value::Number(services.max_file_size.into()),
    );
    obj.insert(
        "federation".to_string(),
        Value::String("all".to_string()),
    );
    if detail {
        obj.insert(
            "features".to_string(),
            serde_json::json!({
                "registration": true,
                "emailRequiredForSignup": false,
                "localTimeline": true,
                "globalTimeline": true,
                "hcaptcha": false,
                "turnstile": false,
                "recaptcha": false,
                "objectStorage": false,
                "serviceWorker": true,
                "miauth": true,
            }),
        );
        obj.insert("proxyAccountName".to_string(), Value::Null);
        obj.insert("requireSetup".to_string(), Value::Bool(false));
        obj.insert("cacheRemoteFiles".to_string(), Value::Bool(false));
        obj.insert(
            "cacheRemoteSensitiveFiles".to_string(),
            Value::Bool(false),
        );
    }
    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use crate::application::ports::{
        AccountStore, ActivityDedup, ApiTokenRecord, AuthStore, BlobStore, CreateAccountInput,
        CreateNoteInput, FollowStore, InsertMediaInput, JobQueue, LegacyAuthSessionRecord,
        MediaStore, MiAuthSessionRecord, NewJob, NoteStore, NotificationStore, ReactionStore,
        StreamingSink, UpdateAccountPatch,
    };
    use crate::domain::follow::Follow;
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::domain::notification::Notification;
    use crate::domain::reaction::Reaction;

    // In-memory stub stores: only the reads the packers touch are
    // functional; everything else reports unavailability.
    struct Mem {
        accounts: Mutex<HashMap<String, Account>>,
        notes: Mutex<HashMap<String, Note>>,
        media: Mutex<HashMap<String, MediaMeta>>,
        reactions: Mutex<HashMap<(String, String), Reaction>>,
        follows: Mutex<HashSet<(String, String)>>,
    }

    impl Mem {
        fn empty() -> Self {
            Self {
                accounts: Mutex::new(HashMap::new()),
                notes: Mutex::new(HashMap::new()),
                media: Mutex::new(HashMap::new()),
                reactions: Mutex::new(HashMap::new()),
                follows: Mutex::new(HashSet::new()),
            }
        }
    }

    fn unavailable<T>(what: &str) -> Result<T, PortError> {
        Err(PortError::StorageUnavailable(format!("test stub: {what}")))
    }

    impl AccountStore for Mem {
        fn create_account(&self, _input: CreateAccountInput) -> Result<Account, PortError> {
            unavailable("create_account")
        }
        fn find_by_id(&self, id: &str) -> Result<Option<Account>, PortError> {
            Ok(self.accounts.lock().unwrap().get(id).cloned())
        }
        fn find_by_handle(&self, _handle: &str) -> Result<Option<Account>, PortError> {
            Ok(None)
        }
        fn find_by_username_and_host(
            &self,
            _username: &str,
            _host: Option<&str>,
        ) -> Result<Option<Account>, PortError> {
            Ok(None)
        }
        fn is_username_available(
            &self,
            _username: &str,
            _preserved: &[String],
        ) -> Result<bool, PortError> {
            Ok(true)
        }
        fn update_account(&self, _id: &str, _patch: UpdateAccountPatch, _jobs: Vec<NewJob>) -> Result<Account, PortError> {
            unavailable("update_account")
        }
        fn rename(&self, _id: &str, _new_handle: &str) -> Result<Account, PortError> {
            unavailable("rename")
        }
        fn get_owner(&self, _id: &str) -> Result<String, PortError> {
            Ok("test-node".to_string())
        }
        fn set_owner(&self, _id: &str, _node: &str, _proof: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn search_accounts(
            &self,
            _query: &str,
            _offset: usize,
            _limit: usize,
        ) -> Result<Vec<Account>, PortError> {
            Ok(Vec::new())
        }
    }

    impl NoteStore for Mem {
        fn create_note(&self, _input: CreateNoteInput, _jobs: Vec<NewJob>) -> Result<Note, PortError> {
            unavailable("create_note")
        }
        fn get_note(&self, id: &str) -> Result<Option<Note>, PortError> {
            Ok(self.notes.lock().unwrap().get(id).cloned())
        }
        fn get_note_including_deleted(&self, id: &str) -> Result<Option<Note>, PortError> {
            self.get_note(id)
        }
        fn delete_note(&self, _id: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            unavailable("delete_note")
        }
        fn notes_by_author(
            &self,
            _author: &str,
            _cursor: &crate::application::ports::TimelineCursor,
        ) -> Result<Vec<Note>, PortError> {
            Ok(Vec::new())
        }
        fn list_all_notes(
            &self,
            _cursor: &crate::application::ports::TimelineCursor,
        ) -> Result<Vec<Note>, PortError> {
            Ok(Vec::new())
        }
        fn attach_remote_media(
            &self,
            _note_id: &str,
            _input: InsertMediaInput,
            _jobs: Vec<NewJob>,
        ) -> Result<MediaMeta, PortError> {
            unavailable("attach_remote_media")
        }
    }

    impl FollowStore for Mem {
        fn follow(
            &self,
            _a: &str,
            _b: &str,
            _with_replies: bool,
            _notify: Option<crate::domain::follow::FollowNotify>,
            _jobs: Vec<NewJob>,
        ) -> Result<Follow, PortError> {
            unavailable("follow")
        }
        fn unfollow(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            unavailable("unfollow")
        }
        fn is_following(&self, a: &str, b: &str) -> Result<bool, PortError> {
            Ok(self
                .follows
                .lock()
                .unwrap()
                .contains(&(a.to_string(), b.to_string())))
        }
        fn get_follow(&self, _a: &str, _b: &str) -> Result<Option<Follow>, PortError> {
            Ok(None)
        }
        fn followers_of(
            &self,
            _id: &str,
            _cursor: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<Follow>, PortError> {
            Ok(Vec::new())
        }
        fn following_of(
            &self,
            _id: &str,
            _cursor: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<Follow>, PortError> {
            Ok(Vec::new())
        }
    }

    impl ReactionStore for Mem {
        fn react(
            &self,
            _user: &str,
            _note: &str,
            _reaction: &str,
            _upsert: bool,
            _jobs: Vec<NewJob>,
        ) -> Result<Reaction, PortError> {
            unavailable("react")
        }
        fn unreact(&self, _user: &str, _note: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            unavailable("unreact")
        }
        fn get_reaction(&self, user: &str, note: &str) -> Result<Option<Reaction>, PortError> {
            Ok(self
                .reactions
                .lock()
                .unwrap()
                .get(&(user.to_string(), note.to_string()))
                .cloned())
        }
    }

    impl JobQueue for Mem {
        fn claim_next(
            &self,
            _lease_ms: i64,
        ) -> Result<Option<crate::application::ports::Job>, PortError> {
            Ok(None)
        }
        fn complete(&self, _job_id: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn fail(&self, _job_id: &str, _retry_at_ms: i64) -> Result<(), PortError> {
            Ok(())
        }
        fn complete_with_followups(
            &self,
            _job_id: &str,
            _followups: Vec<NewJob>,
        ) -> Result<(), PortError> {
            Ok(())
        }
        fn enqueue_jobs(&self, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            Ok(())
        }
        fn defer(&self, _job_id: &str, _not_before_ms: i64) -> Result<(), PortError> {
            Ok(())
        }
        fn get_job(&self, _job_id: &str) -> Result<Option<crate::application::ports::Job>, PortError> {
            Ok(None)
        }
        fn pending_count(&self) -> Result<usize, PortError> {
            Ok(0)
        }
    }

    impl ActivityDedup for Mem {
        fn check_and_record(&self, _activity_id: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn is_recorded(&self, _activity_id: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn record_note_activity(
            &self,
            _activity_id: &str,
            _input: CreateNoteInput,
            _jobs: Vec<NewJob>,
        ) -> Result<Option<Note>, PortError> {
            Ok(None)
        }
        fn record_reject_follow_activity(
            &self,
            _activity_id: &str,
            _follower_id: &str,
            _followee_id: &str,
            _jobs: Vec<NewJob>,
        ) -> Result<bool, PortError> {
            Ok(false)
        }
    }

    impl MediaStore for Mem {
        fn insert_metadata(&self, _input: InsertMediaInput) -> Result<MediaMeta, PortError> {
            unavailable("insert_metadata")
        }
        fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError> {
            Ok(self.media.lock().unwrap().get(id).cloned())
        }
        fn find_by_owner_and_hash(
            &self,
            _owner: &str,
            _hash: &str,
        ) -> Result<Option<MediaMeta>, PortError> {
            Ok(None)
        }
        fn find_by_source_url(&self, source_url: &str) -> Result<Vec<MediaMeta>, PortError> {
            let target = source_url.trim();
            if target.is_empty() {
                return Ok(Vec::new());
            }
            Ok(self
                .media
                .lock()
                .unwrap()
                .values()
                .filter(|m| m.source_url.as_deref().map(str::trim) == Some(target))
                .cloned()
                .collect())
        }
        fn delete_metadata(&self, _id: &str) -> Result<(), PortError> {
            unavailable("delete_metadata")
        }
        fn list_by_owner(&self, owner_id: &str) -> Result<Vec<MediaMeta>, PortError> {
            let mut out: Vec<MediaMeta> = self
                .media
                .lock()
                .unwrap()
                .values()
                .filter(|m| m.owner_id.as_deref() == Some(owner_id))
                .cloned()
                .collect();
            out.sort_by(|a, b| b.id.cmp(&a.id));
            Ok(out)
        }
    }

    impl BlobStore for Mem {
        fn put(&self, _hash: &str, _bytes: &[u8]) -> Result<(), PortError> {
            Ok(())
        }
        fn get(&self, _hash: &str) -> Result<Option<Vec<u8>>, PortError> {
            Ok(None)
        }
        fn delete(&self, _hash: &str) -> Result<(), PortError> {
            Ok(())
        }
    }

    impl NotificationStore for Mem {
        fn insert_notification(&self, _n: Notification) -> Result<Notification, PortError> {
            unavailable("insert_notification")
        }
        fn list_notifications(
            &self,
            _notifiee: &str,
            _limit: usize,
        ) -> Result<Vec<Notification>, PortError> {
            Ok(Vec::new())
        }
        fn mark_all_read(&self, _notifiee: &str) -> Result<u64, PortError> {
            Ok(0)
        }
        fn list_notifications_filtered(
            &self,
            _notifiee: &str,
            _cursor: &crate::application::ports::TimelineCursor,
            _include: &[crate::domain::notification::NotificationKind],
            _exclude: &[crate::domain::notification::NotificationKind],
        ) -> Result<Vec<Notification>, PortError> {
            Ok(Vec::new())
        }
    }

    impl AuthStore for Mem {
        fn set_password_hash(&self, _id: &str, _hash: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn get_password_hash(&self, _id: &str) -> Result<Option<String>, PortError> {
            Ok(None)
        }
        fn store_token(&self, _record: ApiTokenRecord) -> Result<(), PortError> {
            Ok(())
        }
        fn get_token_by_hash(&self, _hash: &str) -> Result<Option<ApiTokenRecord>, PortError> {
            Ok(None)
        }
        fn delete_token_by_hash(&self, _token_hash: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn save_miauth_session(&self, _s: MiAuthSessionRecord) -> Result<(), PortError> {
            Ok(())
        }
        fn get_miauth_session(&self, _id: &str) -> Result<Option<MiAuthSessionRecord>, PortError> {
            Ok(None)
        }
        fn consume_miauth_session(
            &self,
            _id: &str,
        ) -> Result<Option<MiAuthSessionRecord>, PortError> {
            Ok(None)
        }
        fn save_legacy_session(&self, _s: LegacyAuthSessionRecord) -> Result<(), PortError> {
            Ok(())
        }
        fn get_legacy_session(&self, _token: &str) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
            Ok(None)
        }
        fn take_legacy_session(
            &self,
            _token: &str,
        ) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
            Ok(None)
        }
    }

    impl StreamingSink for Mem {
        fn publish(&self, _event_type: &str, _payload: &[u8]) -> Result<(), PortError> {
            Ok(())
        }
    }

    fn test_services(clock_ms: i64) -> (AppServices, Arc<Mem>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(clock_ms));
        let mem = Arc::new(Mem::empty());
        let services = AppServices {
            clock: clock.clone(),
            ids: Arc::new(AidxGenerator::with_node_id(clock.clone(), "test", 0)),
            accounts: mem.clone(),
            notes: mem.clone(),
            follows: mem.clone(),
            reactions: mem.clone(),
            jobs: mem.clone(),
            media: mem.clone(),
            blobs: mem.clone(),
            notifications: mem.clone(),
            auth: mem.clone(),
            streaming: mem.clone(),
            dedup: mem.clone() as Arc<dyn ActivityDedup>,
            federation_http: crate::federation::delivery::SafeHttpClient::new(
                crate::federation::delivery::SafeHttpConfig {
                    allow_private_ips: true,
                    ..Default::default()
                },
            ),
            public_url: "https://example.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
        };
        (services, mem, clock)
    }

    fn test_account(id: &str, username: &str) -> Account {
        let mut account = Account::new_local(id.to_string(), username.to_string()).unwrap();
        account.followers_count = 3;
        account.following_count = 5;
        account.notes_count = 7;
        account
    }

    fn test_note(id: &str, author_id: &str) -> Note {
        Note {
            id: id.to_string(),
            author_id: author_id.to_string(),
            text: Some("hello world".to_string()),
            cw: None,
            visibility: Visibility::Public,
            visible_user_ids: Vec::new(),
            local_only: false,
            reply_id: None,
            renote_id: None,
            reply_user_id: None,
            file_ids: Vec::new(),
            mentions: Vec::new(),
            mentioned_remote_users: "[]".to_string(),
            tags: vec!["rust".to_string()],
            emojis: Vec::new(),
            reactions: BTreeMap::from([
                ("❤️".to_string(), 2),
                ("👍".to_string(), 1),
            ]),
            renote_count: 1,
            replies_count: 2,
            reaction_acceptance: None,
            uri: None,
            url: None,
            user_host: None,
            is_deleted: false,
            reply_parent_uri: None,
            quote: None,
        }
    }

    fn test_media(id: &str, owner_id: &str) -> MediaMeta {
        MediaMeta {
            id: id.to_string(),
            owner_id: Some(owner_id.to_string()),
            name: "photo.png".to_string(),
            mime: "image/png".to_string(),
            size_bytes: 512,
            hash: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
            blob_key: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            blurhash: None,
            properties: crate::domain::media::MediaProperties {
                width: Some(1280),
                height: Some(720),
                orientation: None,
                avg_color: None,
            },
            is_sensitive: false,
            is_link: false,
            url: format!("https://example.local/files/{id}"),
            thumbnail_url: None,
            comment: None,
            source_url: None,
        }
    }

    // --- Checkout schema parsing -------------------------------------------

    fn schema_path(file: &str) -> String {
        format!(
            "{}/misskey/packages/backend/src/models/json-schema/{file}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    fn is_ident(name: &str) -> bool {
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    }

    fn parse_str_attr(line: &str, attr: &str) -> Option<String> {
        let needle = format!("{attr}:");
        for part in line.split(',') {
            let trimmed = part.trim();
            if let Some(rest) = trimmed.strip_prefix(&needle) {
                let rest = rest.trim().strip_prefix('\'')?;
                return Some(rest.split('\'').next().unwrap_or("").to_string());
            }
        }
        None
    }

    fn parse_bool_attr(line: &str, attr: &str) -> Option<bool> {
        let needle = format!("{attr}:");
        for part in line.split(',') {
            let trimmed = part.trim();
            if let Some(rest) = trimmed.strip_prefix(&needle) {
                let token: String = rest
                    .trim()
                    .chars()
                    .take_while(|c| c.is_ascii_alphabetic())
                    .collect();
                return match token.as_str() {
                    "true" => Some(true),
                    "false" => Some(false),
                    _ => None,
                };
            }
        }
        None
    }

    /// Parse the top-level `properties` of `export const {schema} = {...}`:
    /// `(name, type, nullable)` for every entry. Entries without
    /// `optional:` are required (Misskey treats a missing `optional` as
    /// required); entries with `optional: true` are returned too so tests
    /// can decide.
    fn parse_schema_props(src: &str, schema: &str) -> Vec<(String, String, bool, bool)> {
        let marker = format!("export const {schema} = {{");
        let start = src
            .find(&marker)
            .unwrap_or_else(|| panic!("schema {schema} not found"));
        let body = &src[start + marker.len()..];
        let mut depth: i32 = 1;
        let mut in_properties = false;
        let mut out = Vec::new();
        let mut current: Option<(String, Option<String>, Option<bool>, Option<bool>)> = None;
        for line in body.lines().skip(1) {
            let trimmed = line.trim();
            if !in_properties {
                if depth == 1 && trimmed.starts_with("properties:") {
                    in_properties = true;
                }
            } else if depth == 2 {
                if let Some(header) = trimmed.strip_suffix('{') {
                    if let Some(name) = header.trim().strip_suffix(':').map(str::trim) {
                        if is_ident(name) {
                            current = Some((name.to_string(), None, None, None));
                        }
                    }
                }
            } else if depth == 3 {
                if let Some((_, ty, opt, nul)) = current.as_mut() {
                    if let Some(v) = parse_str_attr(trimmed, "type") {
                        *ty = Some(v);
                    }
                    if let Some(v) = parse_bool_attr(trimmed, "optional") {
                        *opt = Some(v);
                    }
                    if let Some(v) = parse_bool_attr(trimmed, "nullable") {
                        *nul = Some(v);
                    }
                }
            }
            depth += line.chars().filter(|&c| c == '{').count() as i32;
            depth -= line.chars().filter(|&c| c == '}').count() as i32;
            if in_properties && depth == 2 {
                if let Some((name, ty, opt, nul)) = current.take() {
                    out.push((
                        name,
                        ty.unwrap_or_default(),
                        opt.unwrap_or(false),
                        nul.unwrap_or(false),
                    ));
                }
            }
            if in_properties && depth < 2 {
                break;
            }
            if depth < 1 {
                break;
            }
        }
        assert!(!out.is_empty(), "no props parsed for {schema}");
        out
    }

    fn load_props(file: &str, schema: &str) -> Option<Vec<(String, String, bool, bool)>> {
        let path = schema_path(file);
        let src = match std::fs::read_to_string(&path) {
            Ok(src) => src,
            // The `./misskey` reference checkout is git-ignored and absent in
            // clean clones; skip (don't fail) so `cargo test` stays green.
            // Run with the checkout present to actually verify the snapshots.
            Err(e) => {
                eprintln!("SKIP snapshot: cannot read Misskey schema {path}: {e}");
                return None;
            }
        };
        Some(parse_schema_props(&src, schema))
    }

    /// Assert `packed` contains every **required** (`optional: false`)
    /// schema key with a matching JSON type and nullability.
    fn assert_shape(packed: &Value, props: &[(String, String, bool, bool)], ctx: &str) {
        let obj = packed
            .as_object()
            .unwrap_or_else(|| panic!("{ctx} is not an object: {packed}"));
        for (name, ty, optional, nullable) in props {
            if *optional {
                continue;
            }
            let value = obj
                .get(name)
                .unwrap_or_else(|| panic!("{ctx} missing required key `{name}`"));
            if !nullable {
                assert!(
                    !value.is_null(),
                    "{ctx}.{name} must be non-null (nullable: false)"
                );
            }
            if value.is_null() || ty.is_empty() {
                continue;
            }
            let ok = match ty.as_str() {
                "string" => value.is_string(),
                "number" => value.is_number(),
                "integer" => value.is_number(),
                "boolean" => value.is_boolean(),
                "array" => value.is_array(),
                "object" => value.is_object(),
                _ => true,
            };
            assert!(ok, "{ctx}.{name} must be {ty}, got {value}");
        }
    }

    // --- Snapshot tests against the Misskey checkout ------------------------

    #[test]
    fn snapshot_user_lite_matches_checkout() {
        let account = test_account("aid00000000000001", "alice");
        let packed = pack_user_lite(&account, "https://example.local", 1_700_000_000_000);
        let Some(props) = load_props("user.ts", "packedUserLiteSchema") else {
            return;
        };
        assert_shape(&packed, &props, "UserLite");
        assert_eq!(packed["username"], "alice");
        assert_eq!(packed["host"], Value::Null);
        assert_eq!(
            packed["avatarUrl"],
            "https://example.local/identicon/aid00000000000001"
        );
    }

    #[test]
    fn snapshot_user_detailed_matches_checkout() {
        let (services, mem, _) = test_services(1_700_000_000_000);
        let alice = test_account("aid00000000000001", "alice");
        let mut bob = test_account("aid00000000000002", "bob");
        bob.display_name = Some("Bob".to_string());
        bob.avatar_id = Some("mid00000000000001".to_string());
        mem.accounts.lock().unwrap().insert(alice.id.clone(), alice.clone());
        mem.accounts.lock().unwrap().insert(bob.id.clone(), bob.clone());
        mem.follows.lock().unwrap().insert((alice.id.clone(), bob.id.clone()));

        let packed =
            pack_user_detailed_not_me(&bob, Some(&alice.id), &services).unwrap();
        let Some(lite) = load_props("user.ts", "packedUserLiteSchema") else {
            return;
        };
        let Some(detailed) = load_props("user.ts", "packedUserDetailedNotMeOnlySchema")
        else {
            return;
        };
        let mut all = lite;
        all.extend(detailed);
        assert_shape(&packed, &all, "UserDetailedNotMe");
        assert_eq!(packed["name"], "Bob");
        assert_eq!(packed["isFollowing"], true);
        assert_eq!(packed["isFollowed"], false);
        assert_eq!(
            packed["avatarUrl"],
            "https://example.local/files/mid00000000000001"
        );
    }

    #[test]
    fn snapshot_me_detailed_matches_checkout() {
        let (services, mem, _) = test_services(1_700_000_000_000);
        let alice = test_account("aid00000000000001", "alice");
        mem.accounts.lock().unwrap().insert(alice.id.clone(), alice.clone());

        let Some(lite) = load_props("user.ts", "packedUserLiteSchema") else {
            return;
        };
        let Some(detailed) = load_props("user.ts", "packedUserDetailedNotMeOnlySchema")
        else {
            return;
        };
        let Some(me_only) = load_props("user.ts", "packedMeDetailedOnlySchema") else {
            return;
        };

        let packed = pack_me_detailed(&alice, false, &services).unwrap();
        let mut all = lite.clone();
        all.extend(detailed.clone());
        all.extend(me_only.clone());
        // Secrets are `optional: true`; drop them from the required set.
        let required: Vec<_> = all
            .into_iter()
            .filter(|(name, _, optional, _)| {
                *optional == false && name != "email" && name != "emailVerified" && name != "securityKeysList"
            })
            .collect();
        assert_shape(&packed, &required, "MeDetailed");
        assert!(packed.get("email").is_none());

        let packed = pack_me_detailed(&alice, true, &services).unwrap();
        assert_eq!(packed["email"], Value::Null);
        assert_eq!(packed["securityKeysList"], Value::Array(Vec::new()));
        assert_eq!(packed["policies"]["driveCapacityMb"], 1024);
    }

    #[test]
    fn slice8_pack_drive_file_md5_stable_for_same_bytes() {
        // Slice 8 (D6, written before the fix): the client-visible `md5` is
        // the stored md5 field, byte-identical to the pre-slice value for
        // the same bytes — the blob-key migration must not move it, and the
        // SHA-256 address must never leak into the packed shape.
        let (services, _, _) = test_services(1_700_000_000_000);
        let media = test_media("mid00000000000001", "aid00000000000001");
        assert_eq!(media.hash, "d41d8cd98f00b204e9800998ecf8427e");
        let packed = pack_drive_file(&media, &services);
        assert_eq!(packed["md5"], "d41d8cd98f00b204e9800998ecf8427e");
        assert!(packed.get("blob_key").is_none());
        assert!(packed.get("sha256").is_none());
    }

    #[test]
    fn pack_following_carries_both_sides_and_id_time() {
        use crate::domain::follow::Follow;
        let (services, mem, _) = test_services(1_700_000_000_000);
        let alice = test_account("aid00000000000001", "alice");
        let bob = test_account("aid00000000000002", "bob");
        mem.accounts.lock().unwrap().insert(alice.id.clone(), alice.clone());
        mem.accounts.lock().unwrap().insert(bob.id.clone(), bob.clone());
        let follow = Follow {
            id: "fid00000000000001".to_string(),
            follower_id: alice.id.clone(),
            followee_id: bob.id.clone(),
            with_replies: true,
            notify: None,
        };
        let packed = pack_following(&follow, None, &services).unwrap();
        assert_eq!(packed["id"], "fid00000000000001");
        assert_eq!(packed["followerId"], alice.id);
        assert_eq!(packed["followeeId"], bob.id);
        assert_eq!(packed["follower"]["id"], alice.id);
        assert_eq!(packed["followee"]["id"], bob.id);
        assert_eq!(packed["follower"]["username"], "alice");
        assert_eq!(packed["followee"]["username"], "bob");
        assert!(packed["createdAt"].as_str().is_some());
    }

    #[test]
    fn snapshot_drive_file_matches_checkout() {
        let (services, _, _) = test_services(1_700_000_000_000);
        let media = test_media("mid00000000000001", "aid00000000000001");
        let packed = pack_drive_file(&media, &services);
        let Some(props) = load_props("drive-file.ts", "packedDriveFileSchema") else {
            return;
        };
        assert_shape(&packed, &props, "DriveFile");
        assert_eq!(packed["type"], "image/png");
        assert_eq!(packed["properties"]["width"], 1280);
    }

    #[test]
    fn snapshot_note_matches_checkout() {
        let (services, mem, _) = test_services(1_700_000_000_000);
        let alice = test_account("aid00000000000001", "alice");
        let bob = test_account("aid00000000000002", "bob");
        mem.accounts.lock().unwrap().insert(alice.id.clone(), alice.clone());
        mem.accounts.lock().unwrap().insert(bob.id.clone(), bob.clone());
        let media = test_media("mid00000000000001", "aid00000000000001");
        mem.media.lock().unwrap().insert(media.id.clone(), media);

        let target = test_note("nid00000000000001", &bob.id);
        let mut reply = test_note("nid00000000000002", &alice.id);
        reply.reply_id = Some(target.id.clone());
        reply.file_ids = vec!["mid00000000000001".to_string()];
        mem.notes.lock().unwrap().insert(target.id.clone(), target);
        mem.notes.lock().unwrap().insert(reply.id.clone(), reply.clone());
        mem.reactions.lock().unwrap().insert(
            (bob.id.clone(), reply.id.clone()),
            Reaction {
                id: "rid00000000000001".to_string(),
                user_id: bob.id.clone(),
                note_id: reply.id.clone(),
                reaction: "❤️".to_string(),
            },
        );

        let packed = pack_note(&reply, Some(&bob.id), &services, 0).unwrap();
        let Some(props) = load_props("note.ts", "packedNoteSchema") else {
            return;
        };
        assert_shape(&packed, &props, "Note");
        // Value checks beyond key presence.
        assert_eq!(packed["reactionCount"], 3);
        assert_eq!(packed["myReaction"], "❤️");
        assert_eq!(packed["files"].as_array().unwrap().len(), 1);
        assert_eq!(packed["reply"]["id"], "nid00000000000001");
        // Nested reply packs only one level deep.
        assert_eq!(packed["reply"]["reply"], Value::Null);
        // Specified notes carry visibleUserIds; public notes do not.
        assert!(packed.get("visibleUserIds").is_none());
        let mut specified = reply.clone();
        specified.visibility = Visibility::Specified;
        specified.visible_user_ids = vec![bob.id.clone()];
        let packed = pack_note(&specified, Some(&bob.id), &services, 0).unwrap();
        assert_eq!(packed["visibleUserIds"], serde_json::json!([bob.id]));
    }

    #[test]
    fn snapshot_meta_matches_checkout() {
        let (services, _, _) = test_services(1_700_000_000_000);
        let Some(lite) = load_props("meta.ts", "packedMetaLiteSchema") else {
            return;
        };
        let Some(detailed_only) = load_props("meta.ts", "packedMetaDetailedOnlySchema")
        else {
            return;
        };

        let packed = pack_meta(&services, false);
        assert_shape(&packed, &lite, "MetaLite");
        assert_eq!(packed["version"], PACKER_VERSION);
        assert_eq!(packed["uri"], "https://example.local");
        assert_eq!(packed["maxNoteTextLength"], 3000);

        let packed = pack_meta(&services, true);
        let mut all = lite;
        all.extend(detailed_only);
        assert_shape(&packed, &all, "MetaDetailed");
        assert_eq!(packed["features"]["miauth"], true);
        assert_eq!(packed["features"]["localTimeline"], true);
        assert_eq!(packed["features"]["registration"], true);
    }

    #[test]
    fn timestamps_are_iso8601_utc() {
        assert_eq!(format_millis(0), "1970-01-01T00:00:00.000Z");
        let s = format_millis(1_700_000_000_123);
        assert!(s.ends_with('Z'), "must be UTC: {s}");
        assert!(s.contains('T'), "must be ISO-8601: {s}");
    }

    // --- Quote-post API projection (docs/quote-posts.md) --------------------

    use crate::domain::quote::{QuoteAuthorizationState, QuoteReference};

    fn quote_note(
        id: &str,
        author_id: &str,
        target_note_id: Option<&str>,
        target_uri: &str,
        state: QuoteAuthorizationState,
    ) -> Note {
        let mut n = test_note(id, author_id);
        n.renote_id = target_note_id.map(|t| t.to_string());
        n.quote = Some(QuoteReference {
            target_uri: target_uri.to_string(),
            target_note_id: target_note_id.map(|t| t.to_string()),
            authorization: state,
            authorization_uri: None,
            request_activity_uri: None,
            last_verified_at: None,
        });
        n
    }

    fn insert_all(mem: &Mem, accounts: Vec<Account>, notes: Vec<Note>) {
        for acc in accounts {
            mem.accounts.lock().unwrap().insert(acc.id.clone(), acc);
        }
        for note in notes {
            mem.notes.lock().unwrap().insert(note.id.clone(), note);
        }
    }

    #[test]
    fn pack_quote_visible_target_projects_status_and_renote() {
        let (services, mem, _) = test_services(1_700_000_000_000);
        let alice = test_account("aid00000000000001", "alice");
        let bob = test_account("aid00000000000002", "bob");
        let mut target = test_note("nid00000000000009", &bob.id);
        target.visibility = Visibility::Public;
        let quote = quote_note(
            "nid00000000000010",
            &alice.id,
            Some(&target.id),
            "https://example.local/notes/nid00000000000009",
            QuoteAuthorizationState::Pending,
        );
        insert_all(&mem, vec![alice.clone(), bob], vec![target, quote.clone()]);
        let packed = pack_note(&quote, Some(&alice.id), &services, 0).unwrap();
        // Same renote shape regardless of wire format, plus the extension.
        assert_eq!(packed["renoteId"], "nid00000000000009");
        assert!(packed["renote"].is_object());
        assert_eq!(packed["quoteAuthorizationStatus"], "pending");
        assert!(packed.get("quoteTargetUri").is_none());
        assert!(packed.get("quoteTargetUnavailable").is_none());
    }

    #[test]
    fn pack_quote_invisible_target_marks_unavailable_never_leaks_uri() {
        let (services, mem, _) = test_services(1_700_000_000_000);
        let alice = test_account("aid00000000000001", "alice");
        let bob = test_account("aid00000000000002", "bob");
        let stranger = test_account("aid00000000000003", "stranger");
        let mut target = test_note("nid00000000000009", &bob.id);
        target.visibility = Visibility::Followers;
        let quote = quote_note(
            "nid00000000000010",
            &alice.id,
            Some(&target.id),
            "https://example.local/notes/nid00000000000009",
            QuoteAuthorizationState::Unapproved,
        );
        insert_all(
            &mem,
            vec![alice, bob, stranger.clone()],
            vec![target, quote.clone()],
        );
        let packed = pack_note(&quote, Some(&stranger.id), &services, 0).unwrap();
        // Authorization never overrides visibility; rejected/unapproved
        // quotes still render their relation as unavailable.
        assert_eq!(packed["quoteAuthorizationStatus"], "unapproved");
        assert!(packed["renote"].is_null());
        assert_eq!(packed["quoteTargetUnavailable"], true);
        assert!(packed.get("quoteTargetUri").is_none(), "no URI leak for protected targets");
    }

    #[test]
    fn pack_quote_unresolved_target_uri_rules() {
        let (services, mem, _) = test_services(1_700_000_000_000);
        let alice = test_account("aid00000000000001", "alice");
        // Public quoting post: the relationship survives with its URI.
        let quote = quote_note(
            "nid00000000000010",
            &alice.id,
            None,
            "https://remote.example/notes/1",
            QuoteAuthorizationState::Unapproved,
        );
        assert!(quote.renote_id.is_none(), "no fabricated renoteId");
        insert_all(&mem, vec![alice.clone()], vec![quote.clone()]);
        let packed = pack_note(&quote, None, &services, 0).unwrap();
        assert_eq!(packed["renoteId"], Value::Null);
        assert_eq!(packed["quoteTargetUri"], "https://remote.example/notes/1");
        assert_eq!(packed["quoteAuthorizationStatus"], "unapproved");

        // Followers-only quoting post: conservative unavailable marker.
        let mut restricted = quote.clone();
        restricted.id = "nid00000000000011".to_string();
        restricted.visibility = Visibility::Followers;
        mem.notes.lock().unwrap().insert(restricted.id.clone(), restricted.clone());
        let packed = pack_note(&restricted, Some(&alice.id), &services, 0).unwrap();
        assert!(packed.get("quoteTargetUri").is_none());
        assert_eq!(packed["quoteTargetUnavailable"], true);
    }

    #[test]
    fn pack_pure_renote_omits_quote_keys() {
        let (services, mem, _) = test_services(1_700_000_000_000);
        let alice = test_account("aid00000000000001", "alice");
        let bob = test_account("aid00000000000002", "bob");
        let target = test_note("nid00000000000009", &bob.id);
        let mut renote = test_note("nid00000000000010", &alice.id);
        renote.text = None;
        renote.renote_id = Some(target.id.clone());
        assert!(renote.quote.is_none());
        insert_all(&mem, vec![alice.clone(), bob], vec![target, renote.clone()]);
        let packed = pack_note(&renote, Some(&alice.id), &services, 0).unwrap();
        assert!(packed.get("quoteAuthorizationStatus").is_none());
        assert!(packed.get("quoteTargetUri").is_none());
        assert!(packed.get("quoteTargetUnavailable").is_none());
    }

    #[test]
    fn pack_rejected_quote_still_renders_with_status() {
        let (services, mem, _) = test_services(1_700_000_000_000);
        let alice = test_account("aid00000000000001", "alice");
        let bob = test_account("aid00000000000002", "bob");
        let target = test_note("nid00000000000009", &bob.id);
        let quote = quote_note(
            "nid00000000000010",
            &alice.id,
            Some(&target.id),
            "https://example.local/notes/nid00000000000009",
            QuoteAuthorizationState::Rejected,
        );
        insert_all(&mem, vec![alice.clone(), bob], vec![target, quote.clone()]);
        let packed = pack_note(&quote, Some(&alice.id), &services, 0).unwrap();
        assert_eq!(packed["quoteAuthorizationStatus"], "rejected");
        assert!(packed["renote"].is_object(), "rejected quotes keep rendering");
    }
}
