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
    if !username
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
    {
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
                return Err(invalid(
                    "avatarDecorations angle must be within -0.5..=0.5".to_string(),
                ));
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
            return Err(invalid(
                "avatarDecorations id must be 1..=128 chars".to_string(),
            ));
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
    #[serde(
        rename = "userListId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
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
                        return Err(invalid(format!("local account must have null {label}")));
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
                        ));
                    }
                }
                match &self.inbox {
                    Some(inbox) if !inbox.is_empty() && inbox.chars().count() <= 512 => {}
                    _ => {
                        return Err(invalid(
                            "remote account requires inbox of 1..=512 chars".to_string(),
                        ));
                    }
                }
                if let Some(shared_inbox) = &self.shared_inbox {
                    if shared_inbox.chars().count() > 512 {
                        return Err(invalid("shared_inbox exceeds 512 chars".to_string()));
                    }
                }
            }
        }
        if let Some(display_name) = &self.display_name {
            let n = display_name.chars().count();
            if n == 0 || n > 128 {
                return Err(invalid("display_name must be 1..=128 chars".to_string()));
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
                return Err(invalid("defaultReaction must be 1..=260 chars".to_string()));
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

/// Rename rule (WP6 domain layering: rename/move state machine lives here
/// so the API endpoint and the inbound `Update(Person)`/`Move` handler
/// share it). Local renames validate the new handle and require it to be
/// free (`handle_taken` comes from the store lookup); remote renames only
/// validate shape (authority was already checked at the federation
/// boundary). Suspended/deleted accounts cannot rename.
pub fn validate_rename(
    account: &Account,
    new_handle: &str,
    handle_taken: bool,
) -> Result<(), DomainError> {
    if account.is_suspended || account.is_deleted {
        return Err(invalid("ACCOUNT_UNAVAILABLE".to_string()));
    }
    if account.is_local() {
        validate_local_username(new_handle)?;
    } else {
        validate_remote_username(new_handle)?;
    }
    if handle_taken {
        return Err(DomainError::Conflict("USERNAME_TAKEN".to_string()));
    }
    Ok(())
}

/// Move rule (WP6; Misskey `i/move` parity: `movedToUri` is set once and
/// never cleared — `prohibitMoved` refuses further moves). Returns the
/// `(moved_to_uri, moved_at_ms)` to persist; the store commits it with the
/// delivery jobs in one transaction.
pub fn validate_move(
    account: &Account,
    target_uri: &str,
    now_ms: i64,
) -> Result<(String, i64), DomainError> {
    if !account.is_local() {
        return Err(invalid("MOVE_REMOTE_FORBIDDEN".to_string()));
    }
    if account.moved_to_uri.is_some() {
        return Err(invalid("ALREADY_MOVED".to_string()));
    }
    let uri = target_uri.trim();
    if !(uri.starts_with("https://") || uri.starts_with("http://")) {
        return Err(invalid("move target must be an http(s) URI".to_string()));
    }
    if uri.chars().count() > 512 {
        return Err(invalid("move target exceeds 512 chars".to_string()));
    }
    Ok((uri.to_string(), now_ms))
}

/// `pinned_page_id` update rule (WP6): `None` clears unconditionally;
/// `Some(id)` requires a non-empty id (existence is checked by the
/// application op against the page store — domain only enforces shape so
/// the API and federation paths cannot diverge).
pub fn validate_pinned_page_update(pinned_page_id: Option<&str>) -> Result<(), DomainError> {
    if let Some(id) = pinned_page_id {
        if id.trim().is_empty() {
            return Err(invalid("pinned_page_id must be non-empty".to_string()));
        }
    }
    Ok(())
}

/// `verifiedLinks` re-verify loop (WP6; Misskey `update.ts` `verifyLink`
/// parity): every profile update resets `verified_links` to `[]`, and
/// re-verification happens off-request. This constructor is the ONLY way
/// the application op clears the list, so no adapter can forget the reset.
pub fn reset_verified_links_for_reverify() -> Vec<String> {
    Vec::new()
}

/// True when the profile changed after the last link verification, i.e. a
/// re-verify job must be (re-)armed. `last_verified_at_ms == None` means
/// never verified.
pub fn needs_link_reverify(last_verified_at_ms: Option<i64>, profile_updated_at_ms: i64) -> bool {
    match last_verified_at_ms {
        None => true,
        Some(verified_at) => verified_at < profile_updated_at_ms,
    }
}

/// Signin input shape (WP6: credential check lives in the application op,
/// out of the API adapter — domain only enforces input shape so both the
/// password-signin endpoint and any future SSO bridge share it).
pub fn validate_signin_input(username_or_handle: &str, password: &str) -> Result<(), DomainError> {
    if username_or_handle.trim().is_empty() {
        return Err(invalid("username is required".to_string()));
    }
    if password.is_empty() || password.chars().count() > 512 {
        return Err(invalid("password must be 1..=512 chars".to_string()));
    }
    Ok(())
}

/// Password policy for signup / password set (Misskey `signup.ts` parity:
/// at least 8 chars; upper bound keeps the hash input bounded).
pub fn password_meets_policy(password: &str) -> Result<(), DomainError> {
    let n = password.chars().count();
    if n < 8 || n > 128 {
        return Err(invalid("password must be 8..=128 chars".to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
