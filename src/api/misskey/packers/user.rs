//! Pack user (split from `packers.rs`). Pure move.
//!
#![allow(unused_imports)]
//! Misskey response packers (Slice D4).
//!
//! Render domain types into Misskey 2026.10 wire shapes
//! (`misskey/packages/backend/src/models/json-schema/`:
//! `note.ts`, `user.ts`, `drive-file.ts`, `meta.ts`). Field sets are
//! pinned by snapshot tests at the bottom of this file that read the
//! actual checkout schemas from disk. The tests skip (pass vacuously) when
//! the git-ignored `./misskey` checkout is absent, e.g. in clean clones.

use serde_json::{Map, Value};

use crate::activitypub::format_millis;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::{Account, FollowVisibility};
use crate::domain::blocking::Blocking;
use crate::domain::draft::NoteDraft;
use crate::domain::drive_folder::DriveFolder;
use crate::domain::follow::Follow;
use crate::domain::instance::InviteCode;
use crate::domain::media::MediaMeta;
use crate::domain::moderation::{AbuseReport, ModerationLogEntry};
use crate::domain::muting::{Muting, RenoteMuting};
use crate::domain::note::{Note, Visibility};
use crate::domain::notification::{Notification, NotificationKind};
use crate::domain::reaction::Reaction;
use crate::domain::visibility::{ViewerContext, is_visible};
use crate::domain::webhook::SystemWebhook;
use crate::domain::webhook::UserWebhook;

/// Misskey server version we claim compatibility with.

/// Millisecond timestamps as ISO-8601 UTC strings
/// (`"YYYY-MM-DDTHH:MM:SS.sssZ"`).
use super::common::{opt_number, opt_string};
use super::note::pack_note;

pub(crate) fn avatar_url(account: &Account, public_url: &str) -> String {
    let base = public_url.trim_end_matches('/');
    match &account.avatar_id {
        Some(id) => format!("{base}/files/{id}"),
        None => format!("{base}/identicon/{}", account.id),
    }
}
pub(crate) fn banner_url(account: &Account, public_url: &str) -> Value {
    match &account.banner_id {
        Some(id) => Value::String(format!("{}/files/{id}", public_url.trim_end_matches('/'))),
        None => Value::Null,
    }
}
/// `UserLite` (`user.ts` `packedUserLiteSchema`).
pub fn pack_user_lite(account: &Account, public_url: &str, clock_ms: i64) -> Value {
    let _ = clock_ms;
    let mut obj = serde_json::json!({
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
        "onlineStatus": if account.hide_online_status { "unknown" } else { "online" },
    });
    if let Some(map) = obj.as_object_mut() {
        // `requireSigninToViewContents === false ? undefined : true`.
        if account.require_signin_to_view_contents {
            map.insert("requireSigninToViewContents".to_string(), Value::Bool(true));
        }
        // `makeNotes*Before ?? undefined`.
        if let Some(seconds) = account.make_notes_followers_only_before {
            map.insert(
                "makeNotesFollowersOnlyBefore".to_string(),
                Value::Number(seconds.into()),
            );
        }
        if let Some(seconds) = account.make_notes_hidden_before {
            map.insert(
                "makeNotesHiddenBefore".to_string(),
                Value::Number(seconds.into()),
            );
        }
    }
    obj
}
/// Resolve stored `alsoKnownAs` actor URIs to account ids
/// (`UserEntityService.pack` `alsoKnownAs` parity: ids, `null` when empty).
/// Local actor URLs (`{public_url}/users/{id}`) carry no stored `uri`, so
/// they resolve by id suffix.
pub(crate) fn pack_also_known_as(account: &Account, services: &AppServices) -> Value {
    let Some(uris) = account.also_known_as.as_ref() else {
        return Value::Null;
    };
    let base = services.public_url.trim_end_matches('/');
    let mut ids = Vec::new();
    for uri in uris {
        if let Ok(Some(target)) = services.accounts.find_by_uri(uri) {
            ids.push(Value::String(target.id));
            continue;
        }
        if let Some(id) = uri.strip_prefix(&format!("{base}/users/")) {
            if !id.is_empty()
                && !id.contains('/')
                && services.accounts.find_by_id(id).ok().flatten().is_some()
            {
                ids.push(Value::String(id.to_string()));
            }
        }
    }
    if ids.is_empty() {
        Value::Null
    } else {
        Value::Array(ids)
    }
}
/// Stored avatar decorations in `UserEntityService` shape (`angle`/`flipH`/
/// offsets omitted when falsy, `... || undefined` parity).
/// TODO(§0B): upstream also attaches each decoration's `url` from the
/// avatar-decoration registry; pinetree has no registry yet.
pub(crate) fn pack_avatar_decorations(account: &Account) -> Value {
    Value::Array(
        account
            .avatar_decorations
            .iter()
            .map(|d| {
                let mut obj = Map::new();
                obj.insert("id".to_string(), Value::String(d.id.clone()));
                if let Some(angle) = d.angle {
                    if angle != 0.0 {
                        obj.insert(
                            "angle".to_string(),
                            serde_json::Number::from_f64(angle)
                                .map(Value::Number)
                                .unwrap_or(Value::Null),
                        );
                    }
                }
                if let Some(flip_h) = d.flip_h {
                    if flip_h {
                        obj.insert("flipH".to_string(), Value::Bool(true));
                    }
                }
                for (key, value) in [("offsetX", d.offset_x), ("offsetY", d.offset_y)] {
                    if let Some(v) = value {
                        if v != 0.0 {
                            obj.insert(
                                key.to_string(),
                                serde_json::Number::from_f64(v)
                                    .map(Value::Number)
                                    .unwrap_or(Value::Null),
                            );
                        }
                    }
                }
                Value::Object(obj)
            })
            .collect(),
    )
}
/// Masked follow counts (`UserEntityService.ts:465-473` parity):
/// `public` (or self — moderators bypass too, but pinetree has no roles
/// infrastructure yet, so no viewer is a moderator) shows the count;
/// `followers` shows it only to a viewer following the account; otherwise
/// the count packs as `null` (never omitted).
pub(crate) fn masked_follow_count(
    visibility: &FollowVisibility,
    viewer_follows_account: bool,
    viewer_id: Option<&str>,
    account_id: &str,
    count: u64,
) -> Value {
    let is_me = viewer_id == Some(account_id);
    let show = match visibility {
        FollowVisibility::Public => true,
        FollowVisibility::Followers => is_me || viewer_follows_account,
        FollowVisibility::Private => is_me,
    };
    if show {
        Value::Number(count.into())
    } else {
        Value::Null
    }
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
    let created_at = format_millis(services.ids.parse_time(&account.id).unwrap_or(now));
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
    // §0A: real relation lookups (were hardcoded `false`).
    let (is_blocking, is_blocked, is_muted, is_renote_muted) = match viewer_id {
        Some(viewer) if viewer != account.id => (
            services
                .blocks
                .is_blocking(viewer, &account.id)
                .unwrap_or(false),
            services
                .blocks
                .is_blocking(&account.id, viewer)
                .unwrap_or(false),
            services
                .mutes
                .is_muting(viewer, &account.id)
                .unwrap_or(false),
            services
                .mutes
                .is_renote_muting(viewer, &account.id)
                .unwrap_or(false),
        ),
        _ => (false, false, false, false),
    };
    let mut obj = pack_user_lite(account, &services.public_url, now)
        .as_object()
        .cloned()
        .unwrap_or_default();
    obj.insert("url".to_string(), Value::String(actor_url.clone()));
    obj.insert("uri".to_string(), Value::String(actor_url));
    // Batch P `i/move`: the destination actor URI (upstream `movedToUri`
    // parity; null until the account moves).
    obj.insert(
        "movedTo".to_string(),
        account
            .moved_to_uri
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "alsoKnownAs".to_string(),
        pack_also_known_as(account, services),
    );
    obj.insert("createdAt".to_string(), Value::String(created_at));
    obj.insert("updatedAt".to_string(), Value::Null);
    obj.insert("lastFetchedAt".to_string(), Value::Null);
    obj.insert(
        "bannerUrl".to_string(),
        banner_url(account, &services.public_url),
    );
    obj.insert("bannerBlurhash".to_string(), Value::Null);
    obj.insert("isLocked".to_string(), Value::Bool(account.is_locked));
    obj.insert("isSilenced".to_string(), Value::Bool(false));
    obj.insert("isSuspended".to_string(), Value::Bool(account.is_suspended));
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
    obj.insert(
        "avatarDecorations".to_string(),
        pack_avatar_decorations(account),
    );
    obj.insert("lang".to_string(), opt_string(&account.lang));
    obj.insert(
        "fields".to_string(),
        serde_json::to_value(&account.fields).unwrap_or(Value::Array(Vec::new())),
    );
    obj.insert(
        "verifiedLinks".to_string(),
        serde_json::to_value(&account.verified_links).unwrap_or(Value::Array(Vec::new())),
    );
    obj.insert(
        "followersCount".to_string(),
        masked_follow_count(
            &account.followers_visibility,
            is_following,
            viewer_id,
            &account.id,
            account.followers_count,
        ),
    );
    obj.insert(
        "followingCount".to_string(),
        masked_follow_count(
            &account.following_visibility,
            is_following,
            viewer_id,
            &account.id,
            account.following_count,
        ),
    );
    obj.insert(
        "notesCount".to_string(),
        Value::Number(account.notes_count.into()),
    );
    // §7: real pin lookups (Misskey `UserEntityService.ts:552-553`
    // `pinnedNoteIds: pins.map(pin => pin.noteId)` parity). Gone notes
    // cannot be packed and are skipped, like every other nullable pack.
    let pinned_ids: Vec<String> = services.pinned_note_ids(&account.id).unwrap_or_default();
    let viewer_for_pins = viewer_id;
    let mut pinned_notes = Vec::new();
    for note_id in &pinned_ids {
        if let Ok(Some(note)) = services.notes.get_note(note_id) {
            if let Ok(packed) = pack_note(&note, viewer_for_pins, services, 0) {
                pinned_notes.push(packed);
            }
        }
    }
    obj.insert(
        "pinnedNoteIds".to_string(),
        Value::Array(
            pinned_ids
                .iter()
                .map(|id| Value::String(id.clone()))
                .collect(),
        ),
    );
    obj.insert("pinnedNotes".to_string(), Value::Array(pinned_notes));
    obj.insert(
        "pinnedPageId".to_string(),
        opt_string(&account.pinned_page_id),
    );
    obj.insert("pinnedPage".to_string(), Value::Null);
    // `publicReactions: isLocal ? profile : false`
    // (misskey-dev/misskey#12964 parity).
    obj.insert(
        "publicReactions".to_string(),
        Value::Bool(account.host.is_none() && account.public_reactions),
    );
    obj.insert(
        "followingVisibility".to_string(),
        Value::String(account.following_visibility.as_str().to_string()),
    );
    obj.insert(
        "followersVisibility".to_string(),
        Value::String(account.followers_visibility.as_str().to_string()),
    );
    obj.insert(
        "chatScope".to_string(),
        Value::String(account.chat_scope.as_str().to_string()),
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
    obj.insert("isBlocking".to_string(), Value::Bool(is_blocking));
    obj.insert("isBlocked".to_string(), Value::Bool(is_blocked));
    obj.insert("isMuted".to_string(), Value::Bool(is_muted));
    obj.insert("isRenoteMuted".to_string(), Value::Bool(is_renote_muted));
    Ok(Value::Object(obj))
}
pub(crate) fn default_policies(services: &AppServices) -> Value {
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
        "pinLimit": services.pin_limit,
        "antennaLimit": 5,
        "wordMuteLimit": 200,
        "webhookLimit": 0,
        "clipLimit": 5,
        "noteEachClipsLimit": 200,
        "userListLimit": 10,
        "userEachUserListsLimit": 50,
        "rateLimitFactor": 1,
        "avatarDecorationLimit": 5,
        "canImportAntennas": false,
        "canImportBlocking": false,
        "canImportFollowing": false,
        "canImportMuting": false,
        "canImportUserLists": false,
        "chatAvailability": "unavailable",
        "noteDraftLimit": services.note_draft_limit,
        "scheduledNoteLimit": services.scheduled_note_limit,
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
    obj.insert(
        "followedMessage".to_string(),
        opt_string(&account.followed_message),
    );
    // Batch M §3.1: real role flags. Upstream packs `isModerator` as
    // `RoleService.isModerator`, which is true for admins too.
    obj.insert(
        "isModerator".to_string(),
        Value::Bool(account.has_moderator_powers()),
    );
    obj.insert("isAdmin".to_string(), Value::Bool(account.is_admin));
    obj.insert(
        "injectFeaturedNote".to_string(),
        Value::Bool(account.inject_featured_note),
    );
    obj.insert(
        "receiveAnnouncementEmail".to_string(),
        Value::Bool(account.receive_announcement_email),
    );
    obj.insert(
        "alwaysMarkNsfw".to_string(),
        Value::Bool(account.always_mark_nsfw),
    );
    obj.insert(
        "autoSensitive".to_string(),
        Value::Bool(account.auto_sensitive),
    );
    obj.insert("carefulBot".to_string(), Value::Bool(account.careful_bot));
    obj.insert(
        "autoAcceptFollowed".to_string(),
        Value::Bool(account.auto_accept_followed),
    );
    obj.insert("noCrawle".to_string(), Value::Bool(account.no_crawle));
    obj.insert(
        "preventAiLearning".to_string(),
        Value::Bool(account.prevent_ai_learning),
    );
    obj.insert(
        "isExplorable".to_string(),
        Value::Bool(account.is_explorable),
    );
    obj.insert("isDeleted".to_string(), Value::Bool(account.is_deleted));
    obj.insert(
        "twoFactorBackupCodesStock".to_string(),
        Value::String("none".to_string()),
    );
    obj.insert(
        "hideOnlineStatus".to_string(),
        Value::Bool(account.hide_online_status),
    );
    obj.insert("hasUnreadSpecifiedNotes".to_string(), Value::Bool(false));
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
    obj.insert(
        "unreadNotificationsCount".to_string(),
        Value::Number(0.into()),
    );
    obj.insert(
        "mutedWords".to_string(),
        serde_json::to_value(&account.muted_words).unwrap_or(Value::Array(Vec::new())),
    );
    obj.insert(
        "hardMutedWords".to_string(),
        serde_json::to_value(&account.hard_muted_words).unwrap_or(Value::Array(Vec::new())),
    );
    obj.insert(
        "mutedInstances".to_string(),
        serde_json::to_value(&account.muted_instances).unwrap_or(Value::Array(Vec::new())),
    );
    // Kept for wire compat (`UserEntityService` packs `[]`).
    obj.insert(
        "mutingNotificationTypes".to_string(),
        Value::Array(Vec::new()),
    );
    obj.insert(
        "notificationRecieveConfig".to_string(),
        serde_json::to_value(&account.notification_recieve_config)
            .unwrap_or(Value::Object(Map::new())),
    );
    obj.insert(
        "emailNotificationTypes".to_string(),
        serde_json::to_value(&account.email_notification_types).unwrap_or(Value::Array(Vec::new())),
    );
    obj.insert("achievements".to_string(), Value::Array(Vec::new()));
    obj.insert("loggedInDays".to_string(), Value::Number(1.into()));
    obj.insert("policies".to_string(), default_policies(services));
    // Phase 2 (P0): security flags reflect persisted TOTP state instead
    // of hardcoded `false`. `securityKeys` stays `false`: pinetree stores
    // no WebAuthn credentials, so there is nothing to reflect (TOTP via
    // `i/2fa/*` is the supported second factor). A store fault propagates
    // as an error so `i` never reports `twoFactorEnabled: false` on a
    // failed read; enforcement stays fail-closed on the signin paths too.
    // DIVERGENCE (adversarial-2 finding 7): `usePasswordLessLogin` packs
    // `false` always. Pinetree implements no passwordless sign-in —
    // `do_signin_flow` still requires the password before the TOTP
    // challenge — so advertising the capability would invite clients to
    // skip the password factor. The stored `TotpState::passwordless` bit
    // is legacy data only: it is never surfaced here, and the
    // `i/2fa/password-less` setter refuses to set it anew. The second
    // value of `totp_flags` is kept for that stored bit (other readers
    // may still inspect it) but is deliberately not client-visible.
    let (two_factor_enabled, _) = services.totp_flags(&account.id)?;
    obj.insert(
        "twoFactorEnabled".to_string(),
        Value::Bool(two_factor_enabled),
    );
    obj.insert("usePasswordLessLogin".to_string(), Value::Bool(false));
    obj.insert("securityKeys".to_string(), Value::Bool(false));
    obj.insert(
        "followApprovalLocalSeconds".to_string(),
        opt_number(account.follow_approval_local_seconds),
    );
    obj.insert(
        "followApprovalRemoteSeconds".to_string(),
        opt_number(account.follow_approval_remote_seconds),
    );
    // DIVERGENCE (§3 Sharkey-style quote rejection; §6 per-user default
    // reaction): packed on `MeDetailed` so later slices need no packer
    // changes. `defaultReaction` is `null` until the user sets one.
    obj.insert(
        "rejectQuotes".to_string(),
        Value::Bool(account.reject_quotes),
    );
    obj.insert(
        "defaultReaction".to_string(),
        opt_string(&account.default_reaction),
    );
    // DIVERGENCE (Batch N WP8b): per-user remote-emoji opt-in (upstream has
    // no such field).
    obj.insert(
        "showRemoteEmojis".to_string(),
        Value::Bool(account.show_remote_emojis),
    );
    if include_secrets {
        // Batch P: the stored address + verification flag (null when unset;
        // never packed without secrets — see the redaction assert below).
        obj.insert(
            "email".to_string(),
            account
                .email
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        obj.insert(
            "emailVerified".to_string(),
            Value::Bool(account.email_verified),
        );
        obj.insert("securityKeysList".to_string(), Value::Array(Vec::new()));
    }
    Ok(Value::Object(obj))
}
