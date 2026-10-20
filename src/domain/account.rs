//! Local/remote account entity: pure data + validation (no I/O).

use crate::domain::id::DomainError;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Local usernames: `^[A-Za-z0-9_]{1,20}$`.
pub fn validate_local_username(username: &str) -> Result<(), DomainError> {
    let ok = !username.is_empty()
        && username.len() <= 20
        && username
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(invalid(format!("invalid local username: {username}")))
    }
}

/// Remote usernames: 1..=128 chars matching
/// `^[A-Za-z0-9_]([A-Za-z0-9_.-]*[A-Za-z0-9_])?$`.
pub fn validate_remote_username(username: &str) -> Result<(), DomainError> {
    let len = username.chars().count();
    if len == 0 || len > 128 {
        return Err(invalid(format!("invalid remote username: {username}")));
    }
    if !username.bytes().all(|b| {
        b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-'
    }) {
        return Err(invalid(format!("invalid remote username: {username}")));
    }
    // All bytes are ASCII here, so byte indexing is character indexing.
    let bytes = username.as_bytes();
    let edge_ok = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    if !edge_ok(bytes[0]) {
        return Err(invalid(format!("invalid remote username: {username}")));
    }
    if len > 1 && !edge_ok(bytes[bytes.len() - 1]) {
        return Err(invalid(format!("invalid remote username: {username}")));
    }
    Ok(())
}

/// Birthdays are `YYYY-MM-DD` calendar dates.
pub fn validate_birthday(b: &str) -> Result<(), DomainError> {
    let bytes = b.as_bytes();
    let shape_ok = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit());
    if !shape_ok {
        return Err(invalid(format!(
            "invalid birthday (expected YYYY-MM-DD): {b}"
        )));
    }
    chrono::NaiveDate::parse_from_str(b, "%Y-%m-%d")
        .map(|_| ())
        .map_err(|_| invalid(format!("invalid birthday (not a calendar date): {b}")))
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Account {
    pub id: String,
    pub username: String,
    pub username_lower: String,
    pub host: Option<String>,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub birthday: Option<String>,
    pub uri: Option<String>,
    pub inbox: Option<String>,
    pub shared_inbox: Option<String>,
    pub followers_count: u64,
    pub following_count: u64,
    pub notes_count: u64,
    pub avatar_id: Option<String>,
    pub banner_id: Option<String>,
    pub is_locked: bool,
    pub is_bot: bool,
    pub is_suspended: bool,
    /// Fixed built-in role (Batch M §3.1): instance administrator.
    /// Implies moderator powers. No custom roles/policies (§8).
    #[serde(default)]
    pub is_admin: bool,
    /// Fixed built-in role (Batch M §3.1): moderator. Moderators cannot
    /// act on admins or other moderators (WP3 refusal rule).
    #[serde(default)]
    pub is_moderator: bool,
    pub is_explorable: bool,
    pub is_deleted: bool,
    #[serde(default)]
    pub moderator_note: Option<String>,
    pub is_cat: bool,
    // --- Full profile store (§0B; Misskey `MiUser` columns) -----------------
    /// `MiUser.hideOnlineStatus` (default false).
    #[serde(default)]
    pub hide_online_status: bool,
    /// `MiUser.avatarDecorations` (default `[]`, max 16).
    #[serde(default)]
    pub avatar_decorations: Vec<AvatarDecoration>,
    /// `MiUser.alsoKnownAs`: resolved actor URIs (default null, max 10).
    #[serde(default)]
    pub also_known_as: Option<Vec<String>>,
    /// `MiUser.requireSigninToViewContents` (default false).
    #[serde(default)]
    pub require_signin_to_view_contents: bool,
    /// `MiUser.makeNotesFollowersOnlyBefore` (seconds, default null).
    #[serde(default)]
    pub make_notes_followers_only_before: Option<i64>,
    /// `MiUser.makeNotesHiddenBefore` (seconds, default null).
    #[serde(default)]
    pub make_notes_hidden_before: Option<i64>,
    /// `MiUser.chatScope` (default `mutual`).
    #[serde(default)]
    pub chat_scope: ChatScope,
    // --- Full profile store (§0B; Misskey `MiUserProfile` columns) -----------
    /// `MiUserProfile.followedMessage` (1..=256 chars, default null).
    #[serde(default)]
    pub followed_message: Option<String>,
    /// `MiUserProfile.lang` (default null).
    #[serde(default)]
    pub lang: Option<String>,
    /// `MiUserProfile.fields` (default `[]`, max 16).
    #[serde(default)]
    pub fields: Vec<ProfileField>,
    /// `MiUserProfile.verifiedLinks` (default `[]`; reset on every update,
    /// re-verified off-request — see `update.ts` `verifyLink`).
    #[serde(default)]
    pub verified_links: Vec<String>,
    /// `MiUserProfile.publicReactions` (default true).
    #[serde(default = "default_true")]
    pub public_reactions: bool,
    /// `MiUserProfile.followingVisibility` (default `public`).
    #[serde(default)]
    pub following_visibility: FollowVisibility,
    /// `MiUserProfile.followersVisibility` (default `public`).
    #[serde(default)]
    pub followers_visibility: FollowVisibility,
    /// `MiUserProfile.autoAcceptFollowed` (default false).
    #[serde(default)]
    pub auto_accept_followed: bool,
    /// `MiUserProfile.followApprovalLocalSeconds/RemoteSeconds`
    /// (0..=2_592_000, default null).
    #[serde(default)]
    pub follow_approval_local_seconds: Option<i64>,
    #[serde(default)]
    pub follow_approval_remote_seconds: Option<i64>,
    /// `MiUserProfile.noCrawle` (upstream misspelling kept; default false).
    #[serde(default)]
    pub no_crawle: bool,
    /// `MiUserProfile.preventAiLearning` (default true).
    #[serde(default = "default_true")]
    pub prevent_ai_learning: bool,
    /// `MiUserProfile.alwaysMarkNsfw` (default false; role-guarded on write).
    #[serde(default)]
    pub always_mark_nsfw: bool,
    /// `MiUserProfile.autoSensitive` (default false).
    #[serde(default)]
    pub auto_sensitive: bool,
    /// `MiUserProfile.carefulBot` (default false).
    #[serde(default)]
    pub careful_bot: bool,
    /// `MiUserProfile.injectFeaturedNote` (default true).
    #[serde(default = "default_true")]
    pub inject_featured_note: bool,
    /// `MiUserProfile.receiveAnnouncementEmail` (default true).
    #[serde(default = "default_true")]
    pub receive_announcement_email: bool,
    /// `MiUserProfile.pinnedPageId` (default null; no pages store yet, so
    /// only null clears are accepted — see TODO(§0B) in `accounts.rs`).
    #[serde(default)]
    pub pinned_page_id: Option<String>,
    /// `MiUserProfile.enableWordMute` (derived: `mutedWords` non-empty).
    #[serde(default)]
    pub enable_word_mute: bool,
    /// `MiUserProfile.mutedWords/hardMutedWords` (default `[]`).
    #[serde(default)]
    pub muted_words: Vec<MutedWord>,
    #[serde(default)]
    pub hard_muted_words: Vec<MutedWord>,
    /// `MiUserProfile.mutedInstances` (default `[]`).
    #[serde(default)]
    pub muted_instances: Vec<String>,
    /// `MiUserProfile.notificationRecieveConfig` (upstream misspelling kept
    /// so §11 can use this column verbatim; default `{}`).
    #[serde(default)]
    pub notification_recieve_config: std::collections::BTreeMap<String, NotificationRecieveConfig>,
    /// `MiUserProfile.emailNotificationTypes`
    /// (default `["follow", "receiveFollowRequest"]`).
    #[serde(default = "default_email_notification_types")]
    pub email_notification_types: Vec<String>,
    // --- DIVERGENCE columns (defined now so §§3/6 need no migration) ---------
    /// Sharkey-style quote rejection (`User.rejectQuotes`, default false).
    /// Wired by §3; stored/validated/packed here.
    #[serde(default)]
    pub reject_quotes: bool,
    /// Per-user default reaction (nullable, 1..=260 chars like reactions).
    /// Resolved server-side by §6; stored/validated/packed here.
    #[serde(default)]
    pub default_reaction: Option<String>,
    /// DIVERGENCE (Batch N WP8b, owner-approved): per-user remote custom
    /// emoji opt-in, default false (upstream has no such field). Read back
    /// on `i` / `i/update`.
    #[serde(default)]
    pub show_remote_emojis: bool,
    // --- Batch P: email identity loop + account move ------------------------
    /// `MiUserProfile.email` (default null). Stored so the reset/verify
    /// loop can compare without a separate profile table; secrets never
    /// pack (see `packers.rs` me-detailed secrets redaction).
    #[serde(default)]
    pub email: Option<String>,
    /// `MiUserProfile.emailVerified` (default false).
    #[serde(default)]
    pub email_verified: bool,
    /// `MiUserProfile.emailVerifyCode` (default null; 16-char code minted
    /// on `i/update-email`, cleared on `verify-email`).
    #[serde(default)]
    pub email_verify_code: Option<String>,
    /// `MiUser.movedToUri` (default null; set once by `i/move`, never
    /// cleared — `prohibitMoved` refuses further moves).
    #[serde(default)]
    pub moved_to_uri: Option<String>,
    /// `MiUser.movedAt` as unix millis (default null; set with
    /// `moved_to_uri`).
    #[serde(default)]
    pub moved_at_ms: Option<i64>,
}

fn default_true() -> bool {
    true
}

fn default_email_notification_types() -> Vec<String> {
    vec!["follow".to_string(), "receiveFollowRequest".to_string()]
}

/// `MiUser.chatScope`: who may start a chat (`mutual` default).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatScope {
    Everyone,
    Followers,
    Following,
    #[default]
    Mutual,
    None,
}

impl ChatScope {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "everyone" => Some(Self::Everyone),
            "followers" => Some(Self::Followers),
            "following" => Some(Self::Following),
            "mutual" => Some(Self::Mutual),
            "none" => Some(Self::None),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Everyone => "everyone",
            Self::Followers => "followers",
            Self::Following => "following",
            Self::Mutual => "mutual",
            Self::None => "none",
        }
    }
}

/// `followingVisibilities` / `followersVisibilities`: `public` default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FollowVisibility {
    #[default]
    Public,
    Followers,
    Private,
}

impl FollowVisibility {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "public" => Some(Self::Public),
            "followers" => Some(Self::Followers),
            "private" => Some(Self::Private),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Followers => "followers",
            Self::Private => "private",
        }
    }
}

/// One `MiUserProfile.fields` entry (blank name/value rows are dropped on
/// write, `update.ts` fields filter parity).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProfileField {
    pub name: String,
    pub value: String,
}

/// One `MiUser.avatarDecorations` entry (`update.ts` param shape).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AvatarDecoration {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub angle: Option<f64>,
    #[serde(default, rename = "flipH", skip_serializing_if = "Option::is_none")]
    pub flip_h: Option<bool>,
    #[serde(default, rename = "offsetX", skip_serializing_if = "Option::is_none")]
    pub offset_x: Option<f64>,
    #[serde(default, rename = "offsetY", skip_serializing_if = "Option::is_none")]
    pub offset_y: Option<f64>,
}

impl AvatarDecoration {
    /// `update.ts` numeric ranges (`angle` ±0.5, offsets ±0.25).
    pub fn validate(&self) -> Result<(), DomainError> {
        if let Some(angle) = self.angle {
            if !angle.is_finite() || angle < -0.5 || angle > 0.5 {
                return Err(invalid("avatarDecorations angle must be within -0.5..=0.5".to_string()));
            }
        }
        for (label, value) in [("offsetX", self.offset_x), ("offsetY", self.offset_y)] {
            if let Some(v) = value {
                if !v.is_finite() || v < -0.25 || v > 0.25 {
                    return Err(invalid(format!(
                        "avatarDecorations {label} must be within -0.25..=0.25"
                    )));
                }
            }
        }
        if self.id.is_empty() || self.id.chars().count() > 128 {
            return Err(invalid("avatarDecorations id must be 1..=128 chars".to_string()));
        }
        Ok(())
    }
}

/// One `mutedWords`/`hardMutedWords` entry: a single word (plain substring
/// or `/pattern/flags` regex) or an AND-combination of words.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum MutedWord {
    Single(String),
    And(Vec<String>),
}

impl MutedWord {
    /// Total char count contribution (`update.ts` `checkMuteWordCount` parity).
    pub fn char_len(&self) -> usize {
        match self {
            Self::Single(s) => s.chars().count(),
            Self::And(words) => words.iter().map(|w| w.chars().count()).sum(),
        }
    }
}

/// One `notificationRecieveConfig` value (upstream misspelling kept):
/// `{type: all|never|following|follower|mutualFollow|followingOrFollower}` or
/// `{type: list, userListId}`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NotificationRecieveConfig {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "userListId", default, skip_serializing_if = "Option::is_none")]
    pub user_list_id: Option<String>,
}

impl NotificationRecieveConfig {
    /// Valid `type` values (`models/json-schema/user.ts`
    /// `notificationRecieveConfig` parity).
    pub fn validate(&self) -> Result<(), DomainError> {
        match self.kind.as_str() {
            "all" | "never" | "following" | "follower" | "mutualFollow" | "followingOrFollower" => {
                Ok(())
            }
            "list" => match &self.user_list_id {
                Some(id) if !id.is_empty() => Ok(()),
                _ => Err(invalid(
                    "notificationRecieveConfig list entries require userListId".to_string(),
                )),
            },
            _ => Err(invalid(format!(
                "unknown notificationRecieveConfig type: {}",
                self.kind
            ))),
        }
    }
}

/// `i/update.ts` `FOLLOW_APPROVAL_MAX_SECONDS` (30 days).
pub const FOLLOW_APPROVAL_MAX_SECONDS: i64 = 30 * 24 * 60 * 60;

/// `RoleService` default `wordMuteLimit` (no roles infra yet; see TODO(§0B)).
pub const WORD_MUTE_LIMIT_DEFAULT: usize = 200;

/// `RoleService` default `avatarDecorationLimit` (no roles infra yet).
pub const AVATAR_DECORATION_LIMIT_DEFAULT: usize = 1;

impl Account {
    pub fn new_local(id: String, username: String) -> Result<Self, DomainError> {
        validate_local_username(&username)?;
        Ok(Self {
            id,
            username_lower: username.to_ascii_lowercase(),
            username,
            host: None,
            display_name: None,
            description: None,
            location: None,
            birthday: None,
            uri: None,
            inbox: None,
            shared_inbox: None,
            followers_count: 0,
            following_count: 0,
            notes_count: 0,
            avatar_id: None,
            banner_id: None,
            is_locked: false,
            is_bot: false,
            is_suspended: false,
            is_admin: false,
            is_moderator: false,
            is_explorable: true,
            is_deleted: false,
            moderator_note: None,
            is_cat: false,
            hide_online_status: false,
            avatar_decorations: Vec::new(),
            also_known_as: None,
            require_signin_to_view_contents: false,
            make_notes_followers_only_before: None,
            make_notes_hidden_before: None,
            chat_scope: ChatScope::default(),
            followed_message: None,
            lang: None,
            fields: Vec::new(),
            verified_links: Vec::new(),
            public_reactions: true,
            following_visibility: FollowVisibility::default(),
            followers_visibility: FollowVisibility::default(),
            auto_accept_followed: false,
            follow_approval_local_seconds: None,
            follow_approval_remote_seconds: None,
            no_crawle: false,
            prevent_ai_learning: true,
            always_mark_nsfw: false,
            auto_sensitive: false,
            careful_bot: false,
            inject_featured_note: true,
            receive_announcement_email: true,
            pinned_page_id: None,
            enable_word_mute: false,
            muted_words: Vec::new(),
            hard_muted_words: Vec::new(),
            muted_instances: Vec::new(),
            notification_recieve_config: std::collections::BTreeMap::new(),
            email_notification_types: default_email_notification_types(),
            reject_quotes: false,
            default_reaction: None,
            show_remote_emojis: false,
            email: None,
            email_verified: false,
            email_verify_code: None,
            moved_to_uri: None,
            moved_at_ms: None,
        })
    }

    pub fn is_local(&self) -> bool {
        self.host.is_none()
    }

    /// Moderator powers (Batch M §3.1): an admin implies moderator, so both
    /// roles pass a `require_moderator` guard.
    pub fn has_moderator_powers(&self) -> bool {
        self.is_admin || self.is_moderator
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        match &self.host {
            None => {
                validate_local_username(&self.username)?;
                for (label, value) in [
                    ("uri", &self.uri),
                    ("inbox", &self.inbox),
                    ("shared_inbox", &self.shared_inbox),
                ] {
                    if value.is_some() {
                        return Err(invalid(format!(
                            "local account must have null {label}"
                        )));
                    }
                }
            }
            Some(host) => {
                if host.chars().count() > 128 {
                    return Err(invalid("host exceeds 128 chars".to_string()));
                }
                validate_remote_username(&self.username)?;
                match &self.uri {
                    Some(uri) if !uri.is_empty() && uri.chars().count() <= 512 => {}
                    _ => {
                        return Err(invalid(
                            "remote account requires uri of 1..=512 chars".to_string(),
                        ))
                    }
                }
                match &self.inbox {
                    Some(inbox) if !inbox.is_empty() && inbox.chars().count() <= 512 => {}
                    _ => {
                        return Err(invalid(
                            "remote account requires inbox of 1..=512 chars".to_string(),
                        ))
                    }
                }
                if let Some(shared_inbox) = &self.shared_inbox {
                    if shared_inbox.chars().count() > 512 {
                        return Err(invalid(
                            "shared_inbox exceeds 512 chars".to_string(),
                        ));
                    }
                }
            }
        }
        if let Some(display_name) = &self.display_name {
            let n = display_name.chars().count();
            if n == 0 || n > 128 {
                return Err(invalid(
                    "display_name must be 1..=128 chars".to_string(),
                ));
            }
        }
        if let Some(description) = &self.description {
            if description.chars().count() > 1500 {
                return Err(invalid("description exceeds 1500 chars".to_string()));
            }
        }
        if let Some(location) = &self.location {
            if location.chars().count() > 50 {
                return Err(invalid("location exceeds 50 chars".to_string()));
            }
        }
        if let Some(birthday) = &self.birthday {
            validate_birthday(birthday)?;
        }
        // --- §0B profile bounds (storage-level; the `i/update` endpoint
        // enforces the same bounds as `INVALID_PARAM` before reaching here).
        if let Some(followed_message) = &self.followed_message {
            let n = followed_message.chars().count();
            if n == 0 || n > 256 {
                return Err(invalid(
                    "followed_message must be 1..=256 chars".to_string(),
                ));
            }
        }
        if let Some(lang) = &self.lang {
            let n = lang.chars().count();
            if n == 0 || n > 32 {
                return Err(invalid("lang must be 1..=32 chars".to_string()));
            }
        }
        if self.fields.len() > 16 {
            return Err(invalid("fields exceeds 16 entries".to_string()));
        }
        if self.avatar_decorations.len() > 16 {
            return Err(invalid("avatarDecorations exceeds 16 entries".to_string()));
        }
        for decoration in &self.avatar_decorations {
            decoration.validate()?;
        }
        if let Some(also_known_as) = &self.also_known_as {
            if also_known_as.len() > 10 {
                return Err(invalid("alsoKnownAs exceeds 10 entries".to_string()));
            }
        }
        for approval in [
            self.follow_approval_local_seconds,
            self.follow_approval_remote_seconds,
        ] {
            if let Some(seconds) = approval {
                if seconds < 0 || seconds > FOLLOW_APPROVAL_MAX_SECONDS {
                    return Err(invalid(format!(
                        "follow approval seconds must be 0..={FOLLOW_APPROVAL_MAX_SECONDS}"
                    )));
                }
            }
        }
        for entry in self.notification_recieve_config.values() {
            entry.validate()?;
        }
        if let Some(default_reaction) = &self.default_reaction {
            let n = default_reaction.chars().count();
            if n == 0 || n > 260 {
                return Err(invalid(
                    "defaultReaction must be 1..=260 chars".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Local accounts resolve to `{public_url}/users/{id}`; remote accounts
    /// resolve to their stored `uri`. Public URLs never expose node hostnames.
    pub fn actor_url(&self, public_url: &str) -> String {
        if self.is_local() {
            format!("{}/users/{}", public_url.trim_end_matches('/'), self.id)
        } else {
            self.uri.clone().unwrap_or_default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_account() -> Account {
        Account::new_local("aid00000000000001".to_string(), "alice".to_string()).unwrap()
    }

    fn remote_account() -> Account {
        Account {
            id: "aid00000000000002".to_string(),
            username: "bob".to_string(),
            username_lower: "bob".to_string(),
            host: Some("remote.example".to_string()),
            display_name: None,
            description: None,
            location: None,
            birthday: None,
            uri: Some("https://remote.example/users/bob".to_string()),
            inbox: Some("https://remote.example/users/bob/inbox".to_string()),
            shared_inbox: None,
            followers_count: 0,
            following_count: 0,
            notes_count: 0,
            avatar_id: None,
            banner_id: None,
            is_locked: false,
            is_bot: false,
            is_suspended: false,
            is_admin: false,
            is_moderator: false,
            is_explorable: false,
            is_deleted: false,
            moderator_note: None,
            is_cat: false,
            hide_online_status: false,
            avatar_decorations: Vec::new(),
            also_known_as: None,
            require_signin_to_view_contents: false,
            make_notes_followers_only_before: None,
            make_notes_hidden_before: None,
            chat_scope: ChatScope::default(),
            followed_message: None,
            lang: None,
            fields: Vec::new(),
            verified_links: Vec::new(),
            public_reactions: true,
            following_visibility: FollowVisibility::default(),
            followers_visibility: FollowVisibility::default(),
            auto_accept_followed: false,
            follow_approval_local_seconds: None,
            follow_approval_remote_seconds: None,
            no_crawle: false,
            prevent_ai_learning: true,
            always_mark_nsfw: false,
            auto_sensitive: false,
            careful_bot: false,
            inject_featured_note: true,
            receive_announcement_email: true,
            pinned_page_id: None,
            enable_word_mute: false,
            muted_words: Vec::new(),
            hard_muted_words: Vec::new(),
            muted_instances: Vec::new(),
            notification_recieve_config: std::collections::BTreeMap::new(),
            email_notification_types: default_email_notification_types(),
            reject_quotes: false,
            default_reaction: None,
            show_remote_emojis: false,
            email: None,
            email_verified: false,
            email_verify_code: None,
            moved_to_uri: None,
            moved_at_ms: None,
        }
    }

    #[test]
    fn new_local_sets_defaults() {
        let a = local_account();
        assert_eq!(a.username_lower, "alice");
        assert_eq!(a.host, None);
        assert!(a.is_explorable);
        assert!(!a.is_locked && !a.is_bot && !a.is_suspended && !a.is_deleted && !a.is_cat);
        assert!(!a.is_admin && !a.is_moderator && !a.has_moderator_powers());
        assert_eq!((a.followers_count, a.following_count, a.notes_count), (0, 0, 0));
        assert!(a.validate().is_ok());
    }

    #[test]
    fn new_local_lowercases_username() {
        let a = Account::new_local("aid00000000000003".to_string(), "Alice_1".to_string())
            .unwrap();
        assert_eq!(a.username, "Alice_1");
        assert_eq!(a.username_lower, "alice_1");
        assert!(a.validate().is_ok());
    }

    #[test]
    fn local_username_bounds() {
        for good in ["a".to_string(), "A_09".to_string(), "x".repeat(20)] {
            assert!(validate_local_username(&good).is_ok(), "expected ok: {good}");
        }
        for bad in [
            "".to_string(),
            "x".repeat(21),
            "has space".to_string(),
            "hy-phen".to_string(),
            "dot.name".to_string(),
            "caf\u{e9}".to_string(),
        ] {
            assert!(validate_local_username(&bad).is_err(), "expected err: {bad}");
        }
    }

    #[test]
    fn remote_username_bounds() {
        for good in [
            "a".to_string(),
            "user.name-1_x".to_string(),
            "x".repeat(128),
            "0".to_string(),
        ] {
            assert!(validate_remote_username(&good).is_ok(), "expected ok: {good}");
        }
        for bad in [
            "".to_string(),
            "x".repeat(129),
            ".leading".to_string(),
            "trailing.".to_string(),
            "-leading".to_string(),
            "trailing-".to_string(),
            "has space".to_string(),
        ] {
            assert!(validate_remote_username(&bad).is_err(), "expected err: {bad}");
        }
        assert!(validate_remote_username("_").is_ok());
        assert!(validate_remote_username("a_b").is_ok());
    }

    #[test]
    fn description_and_location_bounds() {
        let mut a = local_account();
        a.description = Some("d".repeat(1500));
        assert!(a.validate().is_ok());
        a.description = Some("d".repeat(1501));
        assert!(a.validate().is_err());

        let mut a = local_account();
        a.location = Some("l".repeat(50));
        assert!(a.validate().is_ok());
        a.location = Some("l".repeat(51));
        assert!(a.validate().is_err());
    }

    #[test]
    fn birthday_bounds() {
        assert!(validate_birthday("1990-02-28").is_ok());
        assert!(validate_birthday("2000-02-29").is_ok()); // leap day
        assert!(validate_birthday("1990-02-30").is_err()); // no such date
        assert!(validate_birthday("1990-13-01").is_err());
        assert!(validate_birthday("not-a-date").is_err());
        assert!(validate_birthday("1990-2-8").is_err()); // not zero-padded
        assert!(validate_birthday("").is_err());

        let mut a = local_account();
        a.birthday = Some("1990-02-30".to_string());
        assert!(a.validate().is_err());
        a.birthday = Some("1990-02-28".to_string());
        assert!(a.validate().is_ok());
    }

    #[test]
    fn remote_uri_requirement() {
        assert!(remote_account().validate().is_ok());

        let mut no_uri = remote_account();
        no_uri.uri = None;
        assert!(no_uri.validate().is_err());

        let mut no_inbox = remote_account();
        no_inbox.inbox = None;
        assert!(no_inbox.validate().is_err());

        let mut local_with_uri = local_account();
        local_with_uri.uri = Some("https://example.local/users/x".to_string());
        assert!(local_with_uri.validate().is_err());
    }

    #[test]
    fn actor_url_forms() {
        let local = local_account();
        assert_eq!(
            local.actor_url("https://example.local/"),
            "https://example.local/users/aid00000000000001"
        );
        let remote = remote_account();
        assert_eq!(
            remote.actor_url("https://example.local"),
            "https://remote.example/users/bob"
        );
    }

    #[test]
    fn profile_store_defaults_match_upstream_columns() {
        // `MiUser` / `MiUserProfile` column defaults (verified against the
        // Misskey 2026.10.1-alpha.0 checkout: `models/User.ts`,
        // `models/UserProfile.ts`).
        let a = local_account();
        assert!(a.public_reactions);
        assert!(a.prevent_ai_learning);
        assert!(a.inject_featured_note);
        assert!(a.receive_announcement_email);
        assert_eq!(a.chat_scope, ChatScope::Mutual);
        assert_eq!(a.following_visibility, FollowVisibility::Public);
        assert_eq!(a.followers_visibility, FollowVisibility::Public);
        assert_eq!(
            a.email_notification_types,
            vec!["follow".to_string(), "receiveFollowRequest".to_string()]
        );
        assert!(!a.hide_online_status);
        assert!(!a.careful_bot);
        assert!(!a.auto_accept_followed);
        assert!(!a.no_crawle);
        assert!(!a.always_mark_nsfw);
        assert!(!a.auto_sensitive);
        assert!(!a.require_signin_to_view_contents);
        assert!(!a.enable_word_mute);
        assert!(!a.reject_quotes);
        assert!(!a.is_admin);
        assert!(!a.is_moderator);
        assert_eq!(a.default_reaction, None);
        assert!(a.validate().is_ok());
        // Serde round-trip preserves the defaults (old rows without the new
        // keys decode via `#[serde(default)]`).
        let json = serde_json::to_string(&a).unwrap();
        let back: Account = serde_json::from_str(&json).unwrap();
        assert_eq!(back, a);
        // Rows written before §0B (no profile keys) decode to the upstream
        // column defaults instead of failing.
        let legacy = serde_json::json!({
            "id": a.id,
            "username": a.username,
            "username_lower": "alice",
            "host": null,
            "display_name": null,
            "description": null,
            "location": null,
            "birthday": null,
            "uri": null,
            "inbox": null,
            "shared_inbox": null,
            "followers_count": 0,
            "following_count": 0,
            "notes_count": 0,
            "avatar_id": null,
            "banner_id": null,
            "is_locked": false,
            "is_bot": false,
            "is_suspended": false,
            "is_explorable": true,
            "is_deleted": false,
            "is_cat": false,
        });
        let decoded: Account = serde_json::from_value(legacy).unwrap();
        assert!(decoded.public_reactions);
        assert!(decoded.prevent_ai_learning);
        assert!(decoded.inject_featured_note);
        assert!(decoded.receive_announcement_email);
        assert_eq!(decoded.chat_scope, ChatScope::Mutual);
        // Rows written before Batch M carry no role keys: both default off.
        assert!(!decoded.is_admin && !decoded.is_moderator);
        assert!(decoded.validate().is_ok());
    }

    #[test]
    fn profile_store_bounds() {
        let mut a = local_account();
        a.followed_message = Some("".to_string());
        assert!(a.validate().is_err());
        a.followed_message = Some("m".repeat(256));
        assert!(a.validate().is_ok());
        a.followed_message = Some("m".repeat(257));
        assert!(a.validate().is_err());

        let mut a = local_account();
        a.fields = vec![
            ProfileField { name: "n".to_string(), value: "v".to_string() };
            17
        ];
        assert!(a.validate().is_err());

        let mut a = local_account();
        a.avatar_decorations = vec![
            AvatarDecoration {
                id: "d".to_string(),
                angle: Some(0.6),
                flip_h: None,
                offset_x: None,
                offset_y: None,
            };
            1
        ];
        assert!(a.validate().is_err());

        let mut a = local_account();
        a.follow_approval_local_seconds = Some(-1);
        assert!(a.validate().is_err());
        a.follow_approval_local_seconds = Some(FOLLOW_APPROVAL_MAX_SECONDS + 1);
        assert!(a.validate().is_err());
        a.follow_approval_local_seconds = Some(3600);
        assert!(a.validate().is_ok());

        let mut a = local_account();
        a.default_reaction = Some("".to_string());
        assert!(a.validate().is_err());
        a.default_reaction = Some("r".repeat(261));
        assert!(a.validate().is_err());
        a.default_reaction = Some("❤".to_string());
        assert!(a.validate().is_ok());

        let mut a = local_account();
        a.also_known_as = Some(vec!["u".to_string(); 11]);
        assert!(a.validate().is_err());

        let bad = NotificationRecieveConfig {
            kind: "sometimes".to_string(),
            user_list_id: None,
        };
        assert!(bad.validate().is_err());
        let bad_list = NotificationRecieveConfig {
            kind: "list".to_string(),
            user_list_id: None,
        };
        assert!(bad_list.validate().is_err());
        let good = NotificationRecieveConfig {
            kind: "mutualFollow".to_string(),
            user_list_id: None,
        };
        assert!(good.validate().is_ok());
    }

    #[test]
    fn roles_default_off_and_admin_implies_moderator() {
        let mut a = local_account();
        assert!(!a.has_moderator_powers());
        a.is_moderator = true;
        assert!(a.has_moderator_powers());
        a.is_moderator = false;
        a.is_admin = true;
        assert!(a.has_moderator_powers());
        assert!(a.validate().is_ok());
    }
}
