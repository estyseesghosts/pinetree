use super::job_queue::NewJob;
use crate::domain::account::{
    Account, AvatarDecoration, ChatScope, FollowVisibility, MutedWord, NotificationRecieveConfig,
    ProfileField,
};
use crate::domain::id::PortError;
use crate::domain::moderation::ModerationLogEntry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateAccountInput {
    pub handle: String,
    pub host: Option<String>,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub birthday: Option<String>,
    pub uri: Option<String>,
    pub inbox: Option<String>,
    pub shared_inbox: Option<String>,
    pub is_bot: bool,
    pub is_cat: bool,
    pub is_locked: bool,
    pub preserved_usernames: Vec<String>,
}

impl CreateAccountInput {
    pub fn local(handle: impl Into<String>) -> Self {
        Self {
            handle: handle.into(),
            host: None,
            display_name: None,
            description: None,
            location: None,
            birthday: None,
            uri: None,
            inbox: None,
            shared_inbox: None,
            is_bot: false,
            is_cat: false,
            is_locked: false,
            preserved_usernames: Vec::new(),
        }
    }
}

/// `i/update` patch: `None` = param absent (leave unchanged),
/// `Some(None)` = explicit `null` (clear), `Some(Some(_))` = set.
/// Covers all 39 `i/update.ts` params plus the DIVERGENCE `rejectQuotes`
/// (§3) and `defaultReaction` (§6) params. Serialized into federated
/// `Update(Person)` jobs, so every field must stay small and bounded
/// (64KiB job payload cap; large lists are validated before enqueue).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UpdateAccountPatch {
    pub display_name: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub followed_message: Option<Option<String>>,
    pub location: Option<Option<String>>,
    pub birthday: Option<Option<String>>,
    pub lang: Option<Option<String>>,
    pub avatar_id: Option<Option<String>>,
    pub avatar_decorations: Option<Vec<AvatarDecoration>>,
    pub banner_id: Option<Option<String>>,
    pub fields: Option<Vec<ProfileField>>,
    pub is_locked: Option<bool>,
    pub is_bot: Option<bool>,
    pub is_cat: Option<bool>,
    pub is_explorable: Option<bool>,
    pub hide_online_status: Option<bool>,
    pub public_reactions: Option<bool>,
    pub careful_bot: Option<bool>,
    pub auto_accept_followed: Option<bool>,
    pub follow_approval_local_seconds: Option<Option<i64>>,
    pub follow_approval_remote_seconds: Option<Option<i64>>,
    pub no_crawle: Option<bool>,
    pub prevent_ai_learning: Option<bool>,
    pub require_signin_to_view_contents: Option<bool>,
    pub make_notes_followers_only_before: Option<Option<i64>>,
    pub make_notes_hidden_before: Option<Option<i64>>,
    pub inject_featured_note: Option<bool>,
    pub receive_announcement_email: Option<bool>,
    pub always_mark_nsfw: Option<bool>,
    pub auto_sensitive: Option<bool>,
    pub following_visibility: Option<FollowVisibility>,
    pub followers_visibility: Option<FollowVisibility>,
    pub chat_scope: Option<ChatScope>,
    pub pinned_page_id: Option<Option<String>>,
    pub muted_words: Option<Vec<MutedWord>>,
    pub hard_muted_words: Option<Vec<MutedWord>>,
    pub muted_instances: Option<Vec<String>>,
    pub notification_recieve_config:
        Option<std::collections::BTreeMap<String, NotificationRecieveConfig>>,
    pub email_notification_types: Option<Vec<String>>,
    pub also_known_as: Option<Option<Vec<String>>>,
    /// `verifiedLinks` reset (`update.ts` clears on every profile update;
    /// re-verification happens off-request). Set by `update_profile`, not by
    /// the endpoint: remote-actor refreshes must not wipe anything.
    pub verified_links: Option<Vec<String>>,
    /// DIVERGENCE (§3 Sharkey-style quote rejection, user-facing toggle).
    pub reject_quotes: Option<bool>,
    /// DIVERGENCE (§6 per-user default reaction, 1..=260 chars).
    pub default_reaction: Option<Option<String>>,
    /// DIVERGENCE (Batch N WP8b remote-emoji opt-in, default false).
    pub show_remote_emojis: Option<bool>,
    /// Batch M WP6 `admin/update-user-note`: moderator-only note text.
    /// `Some(Some(text))` sets, `Some(None)` clears, `None` leaves unchanged.
    pub moderator_note: Option<Option<String>>,
    /// Batch P email identity loop (`i/update-email`, `verify-email`):
    /// `Some(Some(addr))` sets + un-verifies, `Some(None)` clears,
    /// `None` leaves unchanged.
    pub email: Option<Option<String>>,
    /// Batch P: `Some(v)` sets verified flag (verify-email), else unchanged.
    pub email_verified: Option<bool>,
    /// Batch P: `Some(Some(code))` mints, `Some(None)` clears, else unchanged.
    pub email_verify_code: Option<Option<String>>,
    /// Batch P `i/move`: `Some(uri)` marks moved (+ `moved_at_ms`);
    /// never cleared (already-moved refusal guards it).
    pub moved_to_uri: Option<String>,
    /// Batch P `i/move`: unix millis of the move, set with `moved_to_uri`.
    pub moved_at_ms: Option<i64>,
}

impl UpdateAccountPatch {
    /// True when no param was supplied (no-op update).
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

pub trait AccountStore: Send + Sync {
    /// Create one account. `log` (when given) is appended in the SAME
    /// transaction as the account row (§2.3; Batch M WP2's
    /// `admin/accounts/create` logs `createAccount` with its account).
    fn create_account(
        &self,
        input: CreateAccountInput,
        log: Option<&ModerationLogEntry>,
    ) -> Result<Account, PortError>;
    fn find_by_id(&self, id: &str) -> Result<Option<Account>, PortError>;
    fn find_by_handle(&self, normalized_handle: &str) -> Result<Option<Account>, PortError>;
    fn find_by_username_and_host(
        &self,
        username: &str,
        host: Option<&str>,
    ) -> Result<Option<Account>, PortError>;
    fn is_username_available(
        &self,
        username: &str,
        preserved_usernames: &[String],
    ) -> Result<bool, PortError>;
    fn update_account(
        &self,
        id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError>;
    fn rename(&self, id: &str, new_handle: &str) -> Result<Account, PortError>;
    fn get_owner(&self, id: &str) -> Result<String, PortError>;
    fn set_owner(&self, id: &str, node: &str, migration_proof: &str) -> Result<(), PortError>;
    fn search_accounts(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Account>, PortError>;
    /// Look up a local account by its pending email-verification code
    /// (Batch P `verify-email`; Misskey queries `UserProfile` by
    /// `emailVerifyCode`). Default body keeps in-memory test doubles
    /// compiling; the redb adapter scans the accounts table.
    fn find_by_email_verify_code(&self, _code: &str) -> Result<Option<Account>, PortError> {
        Err(PortError::StorageUnavailable(
            "AccountStore::find_by_email_verify_code not implemented".to_string(),
        ))
    }
    /// Total stored account rows, local and remote (Batch P `stats`).
    fn count_accounts(&self) -> Result<u64, PortError> {
        Err(PortError::StorageUnavailable(
            "AccountStore::count_accounts not implemented".to_string(),
        ))
    }

    /// Batch M WP6: bounded enumeration of ALL accounts (including
    /// suspended and tombstoned) for admin read paths. Pages by id cursor.
    fn all_accounts(
        &self,
        after: Option<&str>,
        max_rows: usize,
    ) -> Result<(Vec<Account>, Option<String>), PortError>;
    /// Bounded enumeration of every knowable remote shared-inbox URL (Batch
    /// M WP3; upstream `UserSuspendService.postSuspend`/`postUnsuspend`
    /// deliver `Delete(Person)` / `Undo(Delete)` to "all knowable
    /// sharedInboxes", gathered there from the `followings` rows'
    /// `followerSharedInbox`/`followeeSharedInbox` snapshots — pinetree
    /// reads them from the cached remote accounts themselves).
    ///
    /// Scans at most `max_rows` account rows strictly after `after`
    /// (`None` = from the start, id order) and returns the non-empty
    /// `shared_inbox` values seen (duplicates can span pages; the caller
    /// deduplicates) plus the next cursor — `None` once the table is
    /// exhausted. Pages never truncate silently: the walk ends only at the
    /// table end. Required (no default): the WP3 sweep fan-out depends on
    /// it, so a backend that omits it must fail to compile.
    fn remote_shared_inboxes(
        &self,
        after: Option<&str>,
        max_rows: usize,
    ) -> Result<(Vec<String>, Option<String>), PortError>;
    fn find_by_uri(&self, uri: &str) -> Result<Option<Account>, PortError> {
        let _ = uri;
        Ok(None)
    }
}
