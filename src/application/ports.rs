use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::account::{
    Account, AvatarDecoration, ChatScope, FollowVisibility, MutedWord,
    NotificationRecieveConfig, ProfileField,
};
use crate::domain::blocking::Blocking;
use crate::domain::draft::NoteDraft;
use crate::domain::follow::{Follow, FollowNotify, FollowRequest};
use crate::domain::instance::{InstanceSettings, InviteCode, RegistrationPolicy};
use crate::domain::moderation::{AbuseReport, AbuseReportResolution, ModerationAction, ModerationLogEntry};
use crate::domain::muting::{Muting, RenoteMuting};
pub use crate::domain::id::{Clock, DomainError, IdGenerator, PortError};
use crate::domain::media::{MediaMeta, MediaProperties};
use crate::domain::note::{Note, NoteEdit, ReactionAcceptance, Visibility};
use crate::domain::notification::{Notification, NotificationKind};
use crate::domain::pinning::UserNotePining;
use crate::domain::poll::{Poll, PollInput, PollVote};
use crate::domain::quote::{QuoteAuthorizationState, QuoteReference, QuoteStamp};
use crate::domain::reaction::Reaction;

pub const MAX_JOB_PAYLOAD_BYTES: usize = 65_536;

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
    fn update_account(&self, id: &str, patch: UpdateAccountPatch, jobs: Vec<NewJob>) -> Result<Account, PortError>;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateNoteInput {
    pub author_id: String,
    pub text: Option<String>,
    pub cw: Option<String>,
    pub visibility: Visibility,
    pub local_only: bool,
    pub reply_to: Option<String>,
    pub renote_of: Option<String>,
    /// Unresolved ActivityPub reply parent (`docs/replies.md` Slice B):
    /// the canonical `inReplyTo` URI of a remotely authored reply whose
    /// parent was not cached at ingestion time. Inbox-only: the local
    /// `create_note` path rejects this (local replies resolve their target
    /// synchronously or fail). Must never be set together with `reply_to`
    /// (storage rejects the combination as conflicting). `#[serde(default)]`
    /// keeps in-flight job payloads decodable.
    #[serde(default)]
    pub reply_parent_uri: Option<String>,
    pub file_ids: Vec<String>,
    pub visible_users: Vec<String>,
    pub reaction_acceptance: Option<ReactionAcceptance>,
    pub no_extract_mentions: bool,
    pub no_extract_hashtags: bool,
    pub no_extract_emojis: bool,
    pub has_poll: bool,
    /// Validated poll definition for this note (`None` = no poll).
    /// `#[serde(default)]` keeps in-flight job payloads decodable.
    /// `has_poll` is kept as the cheap flag (storage asserts the two agree).
    #[serde(default)]
    pub poll: Option<PollInput>,
    /// Inbound-only seed tallies for a cached remote poll (`None` = zeroed;
    /// the local path always leaves this `None`). `Some` must carry exactly
    /// one count per choice; storage enforces the length.
    #[serde(default)]
    pub poll_initial_votes: Option<Vec<u64>>,
    pub has_channel: bool,
    pub prohibited_words: Vec<String>,
    pub sensitive_words: Vec<String>,
    #[serde(default)]
    pub uri: Option<String>,
    /// Public URL base (`https://domain`, no trailing slash). When `uri` is
    /// `None`, storage derives the canonical `{uri_base}/notes/{id}` once the
    /// note id is generated. `#[serde(default)]` keeps in-flight job payloads
    /// decodable.
    #[serde(default)]
    pub uri_base: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub extra_mentions: Vec<String>,
    #[serde(default)]
    pub extra_tags: Vec<String>,
    /// Structured quote relationship being created with this note, if any.
    /// `None` for ordinary notes and pure renotes. The application layer
    /// builds this (local quotes: `Pending`/exception; inbound quotes:
    /// `Unapproved`/`Approved`/exception after pre-transaction stamp
    /// verification); storage persists it verbatim after validation.
    /// `#[serde(default)]` keeps in-flight job payloads decodable.
    #[serde(default)]
    pub quote: Option<QuoteReference>,
}

impl CreateNoteInput {
    pub fn simple_public(author_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            author_id: author_id.into(),
            text: Some(text.into()),
            cw: None,
            visibility: Visibility::Public,
            local_only: false,
            reply_to: None,
            renote_of: None,
            reply_parent_uri: None,
            file_ids: Vec::new(),
            visible_users: Vec::new(),
            reaction_acceptance: None,
            no_extract_mentions: false,
            no_extract_hashtags: false,
            no_extract_emojis: false,
            has_poll: false,
            poll: None,
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
        }
    }
}

/// Input for creating one note draft (§5; Misskey `NoteDraftOptions`
/// parity: every `MiNoteDraft` column except `id`/`user`). The author id
/// travels separately (the storage row is built by the application op).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateDraftInput {
    pub text: Option<String>,
    pub cw: Option<String>,
    pub visibility: Visibility,
    pub visible_users: Vec<String>,
    pub local_only: bool,
    pub reply_to: Option<String>,
    pub renote_of: Option<String>,
    pub file_ids: Vec<String>,
    pub hashtag: Option<String>,
    pub reaction_acceptance: Option<ReactionAcceptance>,
    pub has_poll: bool,
    pub poll_choices: Vec<String>,
    pub poll_multiple: bool,
    pub poll_expires_at_ms: Option<i64>,
    pub poll_expired_after_ms: Option<i64>,
    pub scheduled_at_ms: Option<i64>,
    pub is_actually_scheduled: bool,
}

/// Patch for updating one note draft (§5; Misskey `update.ts` partial
/// `NoteDraftOptions` parity). `None` = param absent (leave unchanged);
/// `Some(None)` = explicit `null` (clear, where nullable);
/// `Some(Some(_))` = set. Non-nullable scalars use plain `Option`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateDraftPatch {
    pub text: Option<Option<String>>,
    pub cw: Option<Option<String>>,
    pub visibility: Option<Visibility>,
    pub visible_users: Option<Vec<String>>,
    pub local_only: Option<bool>,
    pub reply_to: Option<Option<String>>,
    pub renote_of: Option<Option<String>>,
    pub file_ids: Option<Vec<String>>,
    pub hashtag: Option<Option<String>>,
    pub reaction_acceptance: Option<Option<ReactionAcceptance>>,
    pub poll_choices: Option<Vec<String>>,
    pub poll_multiple: Option<bool>,
    pub poll_expires_at_ms: Option<Option<i64>>,
    pub poll_expired_after_ms: Option<Option<i64>>,
    pub scheduled_at_ms: Option<Option<i64>>,
    pub is_actually_scheduled: Option<bool>,
}

/// Input for editing one note (§8; Sharkey `notes/edit` + `NoteEditService`
/// parity). `editor_id` must own the note; `reply_id` is immutable (the
/// original parent is kept — no field carries a new one); `visibility` and
/// `local_only` are forced back to the original note's values (the endpoint
/// still parses the requested visibility for the specified-reply check, but
/// it never reaches storage). `renote_of`: `None` detaches the renote,
/// `Some(id)` attaches/keeps it. `poll`: `None` removes the poll,
/// `Some` adds/replaces it (votes reset). `text`/`cw`: `None` clears.
/// `visible_users` is honored only when the (original) visibility is
/// `specified` (upstream wipes the audience when the param is omitted —
/// copied faithfully).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditNoteInput {
    pub editor_id: String,
    pub note_id: String,
    pub text: Option<String>,
    pub cw: Option<String>,
    pub renote_of: Option<String>,
    pub file_ids: Vec<String>,
    pub poll: Option<PollInput>,
    pub reaction_acceptance: Option<ReactionAcceptance>,
    pub visible_users: Vec<String>,
    pub prohibited_words: Vec<String>,
    pub sensitive_words: Vec<String>,
    /// Public URL base (`https://domain`, no trailing slash) for deriving
    /// a local renote target's canonical URI when the target stores none
    /// (quote-relation maintenance only; never used for addressing).
    pub uri_base: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineCursor {
    pub since_id: Option<String>,
    pub until_id: Option<String>,
    pub since_date: Option<i64>,
    pub until_date: Option<i64>,
    pub limit: usize,
}

impl TimelineCursor {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            since_id: None,
            until_id: None,
            since_date: None,
            until_date: None,
            limit,
        }
    }
}

pub trait NoteStore: Send + Sync {
    fn create_note(&self, input: CreateNoteInput, jobs: Vec<NewJob>) -> Result<Note, PortError>;
    fn get_note(&self, id: &str) -> Result<Option<Note>, PortError>;
    fn get_note_including_deleted(&self, id: &str) -> Result<Option<Note>, PortError>;
    fn delete_note(&self, id: &str, jobs: Vec<NewJob>) -> Result<(), PortError>;
    /// Atomically insert one remote-cache media row AND append it to the
    /// note's `file_ids` (Phase 2.6 slice 7, decision D-d). The single
    /// transaction means a crash can never leave an inserted-but-unattached
    /// (orphan) row. Idempotent per (`source_url`, note): when
    /// `input.source_url` is `Some` and the note already references a row
    /// with that source URL, the existing row is returned and nothing is
    /// inserted — so job retries and inbox replays never orphan rows.
    /// `NotFound(NO_SUCH_NOTE)` when the note is missing or deleted;
    /// `Validation(NO_SUCH_FILE)` for unknown/unowned file rows is N/A
    /// here (the row is created by this call) but ownership of the new row
    /// follows the `create_note` rule (owner == author, or unowned rows for
    /// remote authors); `Validation` when the note would exceed 16 files.
    fn attach_remote_media(
        &self,
        note_id: &str,
        input: InsertMediaInput,
        jobs: Vec<NewJob>,
    ) -> Result<MediaMeta, PortError>;
    fn notes_by_author(
        &self,
        author_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError>;
    /// Direct children of one parent (`reply_id == reply_to`), newest-first,
    /// excluding deleted notes (`docs/replies.md` Slice A). `cursor` bounds
    /// (`since_id` exclusive, `until_id` exclusive, date bounds) apply with
    /// the same comparison semantics as the other cursor-paged note
    /// listings; `limit` is a **candidate scan limit** (backends return up
    /// to `limit` raw children — the application layer applies visibility
    /// on top and pages with its own scan loop, so short visible pages are
    /// refilled from later batches). `limit == 0` means a backend default.
    /// Unknown (or deleted) `reply_id` yields whatever live children still
    /// reference it — like the pre-index scan, there is no existence check.
    /// Backends without the parent-child index report `StorageUnavailable`
    /// so callers fail loudly instead of serving silently empty trees.
    fn notes_by_reply(
        &self,
        reply_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        let _ = (reply_id, cursor);
        Err(PortError::StorageUnavailable(
            "NoteStore::notes_by_reply not implemented".to_string(),
        ))
    }
    /// Live notes still waiting on the unresolved reply parent `parent_uri`
    /// (`reply_parent_uri == parent_uri`, deleted excluded), oldest-first,
    /// truncated to `limit` (`docs/replies.md` Slice B). Drives the
    /// `ReplyParentResolve` worker batch and the arrival-triggered check in
    /// the inbox. Defaults to `StorageUnavailable` like `notes_by_reply`.
    fn pending_reply_children(
        &self,
        parent_uri: &str,
        limit: usize,
    ) -> Result<Vec<Note>, PortError> {
        let _ = (parent_uri, limit);
        Err(PortError::StorageUnavailable(
            "NoteStore::pending_reply_children not implemented".to_string(),
        ))
    }
    /// Atomically link a pending child to its resolved parent
    /// (`docs/replies.md` Slice B): set `reply_id`, clear
    /// `reply_parent_uri`, adopt the parent's author as `reply_user_id`,
    /// re-apply parent-driven visibility narrowing (same rules as a reply
    /// whose parent was cached at ingestion), increment the parent's
    /// `replies_count`, insert the parent-child index row, and drop the
    /// pending index row — all in one transaction.
    ///
    /// `parent_canonical_uri` is the parent's canonical URI as computed by
    /// the caller (stored `uri` or `{base}/notes/{id}`); the link commits
    /// only when the child's pending URI matches it exactly.
    ///
    /// Idempotent: an already-linked child, a repeat of the same link, or a
    /// missing/deleted child converges to `Ok(None)` with no counter or
    /// index change. A missing parent also yields `Ok(None)` (the pending
    /// entry is kept for a later retry). Deterministic conflicts — a
    /// mismatched pending URI or a reply cycle — are `Validation` errors;
    /// the caller skips the child but keeps its pending entry.
    fn link_pending_reply(
        &self,
        child_id: &str,
        parent_id: &str,
        parent_canonical_uri: &str,
    ) -> Result<Option<Note>, PortError> {
        let _ = (child_id, parent_id, parent_canonical_uri);
        Err(PortError::StorageUnavailable(
            "NoteStore::link_pending_reply not implemented".to_string(),
        ))
    }
    /// Record an unresolved reply parent discovered later (opt-in repair,
    /// `docs/replies.md` §6.1): set the child's `reply_parent_uri`,
    /// maintain the pending index, and enqueue `jobs` (the resolver) — all
    /// in one transaction. Idempotent: `Ok(false)` when the child is
    /// missing, deleted, already linked, or already pending on the same
    /// URI; `Ok(true)` when a pending URI was (re)assigned. Replacing a
    /// *different* pending URI is allowed (repair refines stale data) and
    /// fixes the index row. `Validation` on empty/non-`http(s)` URIs.
    fn set_pending_reply_parent(
        &self,
        child_id: &str,
        parent_uri: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (child_id, parent_uri, jobs);
        Err(PortError::StorageUnavailable(
            "NoteStore::set_pending_reply_parent not implemented".to_string(),
        ))
    }
    fn list_all_notes(&self, cursor: &TimelineCursor) -> Result<Vec<Note>, PortError>;
    /// Total stored note rows, local and remote (Batch P `stats`).
    fn count_notes(&self) -> Result<u64, PortError> {
        Err(PortError::StorageUnavailable(
            "NoteStore::count_notes not implemented".to_string(),
        ))
    }
    /// Stored note rows by local authors (Batch P `stats`
    /// `originalNotesCount`).
    fn count_local_notes(&self) -> Result<u64, PortError> {
        Err(PortError::StorageUnavailable(
            "NoteStore::count_local_notes not implemented".to_string(),
        ))
    }
    fn find_by_uri(&self, uri: &str) -> Result<Option<Note>, PortError> {
        let _ = uri;
        Ok(None)
    }
    /// Atomically set a quote note's authorization state (e.g.
    /// `Pending` → `Approved`/`Rejected`, `Approved` → `Revoked`) and
    /// enqueue `jobs` (delivery of `Update`/accept replies) in the same
    /// transaction. Illegal transitions (per
    /// [`QuoteAuthorizationState::allows_transition`]) are rejected with
    /// `Validation`. `NotFound(NO_SUCH_QUOTE)` when the note is missing,
    /// deleted, or not a quote. Same-state repeats succeed as no-ops
    /// (idempotent replays) and still enqueue no jobs... callers pass jobs
    /// explicitly, so replays should pass none.
    fn set_quote_authorization(
        &self,
        note_id: &str,
        state: QuoteAuthorizationState,
        authorization_uri: Option<String>,
        verified_at_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        let _ = (note_id, state, authorization_uri, verified_at_ms, jobs);
        Err(PortError::StorageUnavailable(
            "NoteStore::set_quote_authorization not implemented".to_string(),
        ))
    }
    /// Find a local quote note by its outgoing `QuoteRequest` activity URI
    /// (for matching inbound `Accept`/`Reject` to the request). `Ok(None)`
    /// when unknown. Backends without an index may scan; the default
    /// reports unknown so test doubles keep compiling.
    fn find_note_by_quote_request(&self, request_uri: &str) -> Result<Option<Note>, PortError> {
        let _ = request_uri;
        Ok(None)
    }
    /// Find a local quote note by its verified stamp IRI (for matching a
    /// stamp `Delete` to the affected quote). `Ok(None)` when unknown.
    fn find_note_by_quote_stamp(&self, stamp_uri: &str) -> Result<Option<Note>, PortError> {
        let _ = stamp_uri;
        Ok(None)
    }
    /// Persist a locally authored `QuoteAuthorization` stamp (durable +
    /// dereferenceable) and enqueue `jobs` (the `Accept` delivery) in the
    /// same transaction. Overwrites on id repeat (accept replays converge).
    fn save_quote_stamp(&self, stamp: QuoteStamp, jobs: Vec<NewJob>) -> Result<(), PortError> {
        let _ = (stamp, jobs);
        Err(PortError::StorageUnavailable(
            "NoteStore::save_quote_stamp not implemented".to_string(),
        ))
    }
    /// Fetch a stamp by its IRI. `Ok(None)` when unknown.
    fn get_quote_stamp(&self, stamp_id: &str) -> Result<Option<QuoteStamp>, PortError> {
        let _ = stamp_id;
        Ok(None)
    }

    // --- Note drafts (§5) -------------------------------------------------
    //
    // Drafts + scheduling (`docs/misskey-parity-plan.md` §5). One storage
    // transaction commits the draft row, its by-user index row, and `jobs`
    // atomically. FDB inherits this path (no second path).
    /// Insert one draft row with its by-user index row and enqueue `jobs`
    /// (the `PostScheduledDraft` arming when scheduled) in ONE write
    /// transaction. `Conflict(TOO_MANY_DRAFTS)` / ... are enforced by the
    /// application op before this call; storage only enforces shape.
    fn create_draft(&self, draft: NoteDraft, jobs: Vec<NewJob>) -> Result<NoteDraft, PortError> {
        let _ = (draft, jobs);
        Err(PortError::StorageUnavailable(
            "NoteStore::create_draft not implemented".to_string(),
        ))
    }
    /// The live draft `id`, if any. No ownership check (the application op
    /// enforces it so the error precedence matches upstream).
    fn get_draft(&self, id: &str) -> Result<Option<NoteDraft>, PortError> {
        let _ = id;
        Err(PortError::StorageUnavailable(
            "NoteStore::get_draft not implemented".to_string(),
        ))
    }
    /// Replace the stored row for `draft.id` (which must exist:
    /// `NotFound(NO_SUCH_NOTE_DRAFT)` otherwise) and enqueue `jobs` in ONE
    /// write transaction.
    fn update_draft(&self, draft: NoteDraft, jobs: Vec<NewJob>) -> Result<NoteDraft, PortError> {
        let _ = (draft, jobs);
        Err(PortError::StorageUnavailable(
            "NoteStore::update_draft not implemented".to_string(),
        ))
    }
    /// Delete the draft row + index row and enqueue `jobs` in ONE write
    /// transaction. Missing rows are a no-op success (the application op
    /// checks existence first for the `NO_SUCH_NOTE_DRAFT` error).
    fn delete_draft(&self, id: &str, jobs: Vec<NewJob>) -> Result<(), PortError> {
        let _ = (id, jobs);
        Err(PortError::StorageUnavailable(
            "NoteStore::delete_draft not implemented".to_string(),
        ))
    }
    /// Drafts by `user_id`, newest-first, cursor-paged (`notes/drafts/list`
    /// read path). `scheduled == Some(true)` keeps only actually-scheduled
    /// drafts, `Some(false)` only unscheduled ones, `None` keeps both
    /// (Misskey `scheduled` param parity).
    fn drafts_by_user(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
        scheduled: Option<bool>,
    ) -> Result<Vec<NoteDraft>, PortError> {
        let _ = (user_id, cursor, scheduled);
        Err(PortError::StorageUnavailable(
            "NoteStore::drafts_by_user not implemented".to_string(),
        ))
    }
    /// Total drafts by `user_id` (`notes/drafts/count` + the
    /// `noteDraftLimit` gate).
    fn count_drafts(&self, user_id: &str) -> Result<usize, PortError> {
        let _ = user_id;
        Err(PortError::StorageUnavailable(
            "NoteStore::count_drafts not implemented".to_string(),
        ))
    }
    /// Actually-scheduled drafts by `user_id` (the `scheduledNoteLimit`
    /// gate).
    fn count_scheduled_drafts(&self, user_id: &str) -> Result<usize, PortError> {
        let _ = user_id;
        Err(PortError::StorageUnavailable(
            "NoteStore::count_scheduled_drafts not implemented".to_string(),
        ))
    }

    // --- Note pinnings (§7) ------------------------------------------------
    //
    // `pinnings UNIQUE(user, note)` (Misskey `MiUserNotePining` parity).
    // One storage transaction commits the pinning row and `jobs`
    // (federated `Add`/`Remove` of the featured collection) atomically.
    // FDB inherits this path (no second path).
    /// Insert one pinning row and enqueue `jobs` in ONE write transaction.
    /// `Conflict(ALREADY_PINNED)` on a duplicate pair.
    fn add_pinning(
        &self,
        pinning: UserNotePining,
        jobs: Vec<NewJob>,
    ) -> Result<UserNotePining, PortError> {
        let _ = (pinning, jobs);
        Err(PortError::StorageUnavailable(
            "NoteStore::add_pinning not implemented".to_string(),
        ))
    }
    /// Delete the `(user_id, note_id)` pinning row and enqueue `jobs` in
    /// ONE write transaction. A missing row is a no-op success (upstream
    /// `removePinned` deletes unconditionally).
    fn remove_pinning(
        &self,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let _ = (user_id, note_id, jobs);
        Err(PortError::StorageUnavailable(
            "NoteStore::remove_pinning not implemented".to_string(),
        ))
    }
    /// Every pinning row by `user_id`, oldest-first (Misskey
    /// `UserEntityService` `pins.map(...)` order parity: pinning rows read
    /// in id order).
    fn pinnings_for_user(&self, user_id: &str) -> Result<Vec<UserNotePining>, PortError> {
        let _ = user_id;
        Err(PortError::StorageUnavailable(
            "NoteStore::pinnings_for_user not implemented".to_string(),
        ))
    }

    // --- Note editing (§8) ------------------------------------------------
    //
    // `notes/edit` (Sharkey parity). One storage transaction commits the
    // updated note row, the `note_edits` history row (only on effective
    // edits), the poll add/remove/replace (with vote reset), and `jobs`
    // atomically. FDB inherits this path (no second path).
    /// Apply an edit: author-only (`Validation(YOU_ARE_NOT_THE_AUTHOR)` when
    /// `edit.editor_id` does not own the note), `NotFound(NO_SUCH_NOTE)`
    /// when the note is missing or deleted. `reply_id`, `visibility`, and
    /// `local_only` stay at their original values; poll add/remove/replace
    /// resets votes; text/cw arrive pre-trimmed by the caller (storage
    /// re-trims defensively to the storage bounds). A no-change edit
    /// returns the original note and writes no history row (and enqueues no
    /// jobs). Returns the edited (re-read) note.
    fn apply_note_edit(&self, edit: EditNoteInput, jobs: Vec<NewJob>) -> Result<Note, PortError> {
        let _ = (edit, jobs);
        Err(PortError::StorageUnavailable(
            "NoteStore::apply_note_edit not implemented".to_string(),
        ))
    }
    /// Version history for `note_id`, oldest-first (empty when never
    /// edited). Read path for tests and future history surfaces; there is
    /// no Misskey endpoint reading it.
    fn note_edits(&self, note_id: &str) -> Result<Vec<NoteEdit>, PortError> {
        let _ = note_id;
        Err(PortError::StorageUnavailable(
            "NoteStore::note_edits not implemented".to_string(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    StreamingPublish,
    Notify,
    FederateDeliver,
    TimelineMaterialize,
    /// Slice 7 (H2+C3, decision D-d): fetch-and-cache remote note
    /// attachments off the inbox request. The payload is
    /// [`RemoteMediaFetchPayload`]; `note_id` may be
    /// `"__PENDING_NOTE_ID__"` at enqueue time and is substituted with the
    /// committed note id inside the storage transaction (same mechanism as
    /// the federated `NoteCreated` payload).
    RemoteMediaFetch,
    /// Quote-post authorization: re-verify an offered `QuoteAuthorization`
    /// stamp off the inbox request. The payload is
    /// [`QuoteStampReverifyPayload`]. Transient fetch failures retry with
    /// the worker's standard backoff (quarantined after
    /// [`MAX_JOB_ATTEMPTS`]); invalid stamp content completes without
    /// recording (a later re-delivered `Accept` can still be processed).
    QuoteStampReverify,
    /// Branched replies (`docs/replies.md` Slice B): resolve an unresolved
    /// reply parent URI off the inbox request. The payload is
    /// [`ReplyParentResolvePayload`]. The handler fetches/caches the parent
    /// through the SSRF-protected remote-note path, then links waiting
    /// children in bounded batches via the idempotent
    /// `NoteStore::link_pending_reply`. Fetch failures retry with the
    /// worker's standard backoff; after the budget is exhausted the job
    /// quarantines but the pending index entry is kept, so a later parent
    /// arrival (which enqueues a fresh job for the same URI) can still link.
    ReplyParentResolve,
    /// Poll expiry (`docs/misskey-parity-plan.md` §4; Misskey
    /// `endedPollNotificationQueue` parity, sans notifications): fires once
    /// at `expires_at` and publishes the `pollEnded` streaming event. The
    /// payload is [`PollExpiryPayload`]. Idempotent: a missing note/poll or
    /// a not-yet-expired poll completes quietly (a re-armed job covers
    /// clock skew). Reuses the worker's standard lease/backoff.
    PollExpiry,
    /// Scheduled-draft post (`docs/misskey-parity-plan.md` §5; Misskey
    /// `postScheduledNoteQueue` parity): fires once at the draft's
    /// `scheduled_at` and posts the draft as a note. The payload is
    /// [`ScheduledDraftPayload`]. Idempotent: a missing draft, an
    /// unscheduled draft, or a time mismatch (the draft was updated and
    /// re-armed) completes quietly. Success deletes the draft and emits
    /// `ScheduledNotePosted`; post failure keeps the draft and emits
    /// `ScheduledNotePostFailed`. Reuses the worker's standard
    /// lease/backoff.
    PostScheduledDraft,
    /// Profile export (`docs/misskey-parity-plan.md` §13; Misskey `db`
    /// queue `exportFollowing`/`exportBlocking`/`exportMuting`/
    /// `exportNotes`/`exportUserLists`/`exportAntennas`/`exportClips`/
    /// `exportFavorites` parity): builds the entity file off-request,
    /// stores it in the caller's drive, and reports completion with an
    /// `exportCompleted` notification. The payload is [`ExportJobPayload`]
    /// (jobs carry ids only — never file content, per the 64KiB cap).
    /// Idempotent: the export bytes are deterministic for the same state,
    /// the drive upload dedups on content (`force == false`), and the
    /// completion `Notify` job carries a deterministic key, so a replay
    /// overwrites the same notification row instead of duplicating it.
    /// A gone user converges quietly. Reuses the worker's standard
    /// lease/backoff.
    Export,
    /// Profile import (`docs/misskey-parity-plan.md` §13; Misskey `db`
    /// queue `importFollowing`/`importBlocking`/`importMuting`/
    /// `importUserLists`/`importAntennas` parity): reads the caller's drive
    /// file off-request and applies its rows. The payload is
    /// [`ImportJobPayload`] (ids only). Idempotent: every row resolves
    /// through the same application ops as the interactive endpoints, so a
    /// replay converges on `ALREADY_*` conflicts (skipped, never failed);
    /// only storage faults retry the job. A gone user or file converges
    /// quietly. Bounded work per run (one transaction per row, at most the
    /// 64KiB file the endpoint guards admit). Reuses the worker's standard
    /// lease/backoff.
    Import,
    /// Silent unfollow sweep (Batch M WP3; upstream
    /// `UserSuspendService.unFollowAll`, which enqueues one `unfollow`
    /// relationship job per outgoing follow of a suspended account). The
    /// payload is [`UnfollowAllPayload`]. One sweep job is enqueued in the
    /// suspend transaction and the worker expands it at run time: each run
    /// removes one bounded batch of the account's outgoing follow rows
    /// (each row deletion commits atomically with its own
    /// [`JobKind::FederateDeliver`] `Undo(Follow)` job), and returns a
    /// continuation job while more rows remain. Idempotent and crash-safe:
    /// a re-run sees the already-removed rows gone, the cursor advances
    /// over skipped rows, and each row's delete + delivery job is one
    /// transaction. Upstream `silent: true` parity: no `unfollow`
    /// streaming event, no notification. Reuses the worker's standard
    /// lease/backoff.
    UnfollowAll,
    /// Remote custom-emoji lazy fetch (Batch N WP8b): download one known
    /// remote-emoji image off-request after a user opts in via
    /// `showRemoteEmojis`. The payload is [`RemoteEmojiFetchPayload`].
    /// Idempotent: an already-cached row or a gone row converges quietly;
    /// fetch failures (non-image content type, oversize, network) converge
    /// quietly too — the packed URL keeps pointing at the remote host (the
    /// documented privacy trade-off). Only storage faults retry.
    RemoteEmojiFetch,
    /// Antenna fan-in (Batch N WS4; guide WP15): one bounded job per created
    /// note carrying the note id. The worker loads the active antennas,
    /// matches, and appends `antenna_notes` rows (trimmed to the newest 200
    /// per antenna in the same transaction). Idempotent: a gone note
    /// converges quietly; replays re-match harmlessly (rows are keyed
    /// `(antenna, note)`). Only storage faults retry.
    AntennaMatch,
}

/// Maximum `fail()` retries before a job is auto-quarantined as
/// terminally failed (see `JobQueue::fail_permanent`). Every job kind,
/// including per-inbox federation delivery, shares this one exhaustion bound.
pub const MAX_JOB_ATTEMPTS: u32 = 8;

/// Maximum number of jobs one `JobQueue::enqueue_jobs` call (or the follow-up
/// batch of `complete_with_followups`) may insert in a single write
/// transaction. Keeps fan-out writes well under the FoundationDB ~10 MB
/// transaction cap; larger fan-outs are split across calls by the caller.
pub const MAX_JOBS_PER_TXN: usize = 500;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewJob {
    pub kind: JobKind,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub idempotency_key: String,
    pub not_before: i64,
}

impl NewJob {
    pub fn new_json<T: Serialize>(
        kind: JobKind,
        payload: &T,
        idempotency_key: impl Into<String>,
        not_before: i64,
    ) -> Result<Self, PortError> {
        let bytes = serde_json::to_vec(payload)
            .map_err(|e| PortError::Validation(format!("job payload serialize failed: {e}")))?;
        if bytes.len() > MAX_JOB_PAYLOAD_BYTES {
            return Err(PortError::Validation(format!(
                "job payload {} bytes exceeds {} byte cap",
                bytes.len(),
                MAX_JOB_PAYLOAD_BYTES
            )));
        }
        Ok(Self {
            kind,
            payload: bytes,
            idempotency_key: idempotency_key.into(),
            not_before,
        })
    }

    pub fn validate(&self) -> Result<(), PortError> {
        if self.idempotency_key.trim().is_empty() {
            return Err(PortError::Validation(
                "job idempotency_key is required".to_string(),
            ));
        }
        if self.payload.len() > MAX_JOB_PAYLOAD_BYTES {
            return Err(PortError::Validation(format!(
                "job payload {} bytes exceeds {} byte cap",
                self.payload.len(),
                MAX_JOB_PAYLOAD_BYTES
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub kind: JobKind,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub idempotency_key: String,
    pub not_before: i64,
    pub attempts: u32,
    pub lease_expires: Option<i64>,
    /// Terminal failure flag. Quarantined jobs leave the claimable set but
    /// stay inspectable via `get_job`. Defaults to `false` so rows written
    /// before this field existed still decode.
    #[serde(default)]
    pub failed: bool,
    /// Human-readable quarantine reason (`bad-payload: …`,
    /// `permanent-failure: …`, `retry-exhausted`). `None` unless quarantined.
    #[serde(default)]
    pub fail_reason: Option<String>,
}

pub trait JobQueue: Send + Sync {
    /// Claim the next due job and hold it under a lease until
    /// `lease_duration_ms` past now. Reclaiming an expired lease (a job
    /// whose `lease_expires` is `Some(exp)` with `exp <= now`) counts as an
    /// attempt: the backend increments `attempts` (`saturating_add(1)`) and,
    /// when `attempts >= MAX_JOB_ATTEMPTS`, quarantines the job
    /// (`failed = true`, `fail_reason = "retry-exhausted"`) instead of
    /// returning it. Fresh jobs (`lease_expires == None`) never increment.
    fn claim_next(&self, lease_duration_ms: i64) -> Result<Option<Job>, PortError>;
    fn complete(&self, job_id: &str) -> Result<(), PortError>;
    fn fail(&self, job_id: &str, retry_at_ms: i64) -> Result<(), PortError>;
    /// Quarantine a job as terminally failed: it leaves the claimable set
    /// (never returned by `claim_next` again) but stays inspectable via
    /// `get_job` with `failed == true` and `fail_reason` recorded.
    /// Idempotent: repeating it (or calling it on a missing job) is a
    /// no-op success that keeps the first recorded reason.
    /// The provided default is a no-op so in-memory test doubles keep
    /// compiling; durable backends must override it.
    fn fail_permanent(&self, job_id: &str, reason: &str) -> Result<(), PortError> {
        let _ = (job_id, reason);
        Ok(())
    }
    /// Settle `job_id` as done AND insert `followups` in ONE write
    /// transaction: either both happen or neither does. Followups are
    /// validated before any write; at most [`MAX_JOBS_PER_TXN`] per call.
    fn complete_with_followups(
        &self,
        job_id: &str,
        followups: Vec<NewJob>,
    ) -> Result<(), PortError>;
    /// Insert `jobs` in ONE write transaction (all or nothing), at most
    /// [`MAX_JOBS_PER_TXN`] per call. No de-duplication on idempotency key:
    /// a re-run of the producing job can enqueue duplicates, so every
    /// receiver of a re-enqueued federation delivery must dedupe by activity id.
    fn enqueue_jobs(&self, jobs: Vec<NewJob>) -> Result<(), PortError>;
    /// Release the lease on a claimed `job_id` and reschedule it at
    /// `not_before_ms` WITHOUT counting an attempt (used when a destination is
    /// in cooldown, which is not a failed delivery). Missing or quarantined
    /// jobs are a no-op success.
    fn defer(&self, job_id: &str, not_before_ms: i64) -> Result<(), PortError>;
    fn get_job(&self, job_id: &str) -> Result<Option<Job>, PortError>;
    fn pending_count(&self) -> Result<usize, PortError>;
    /// Remove every pending `PostScheduledDraft` job armed for `draft_id`
    /// (§5 `clearSchedule` parity: Misskey scans its delayed/waiting/active
    /// queues linearly — `TODO: 線形探索なのをどうにかする`). Called when a
    /// draft is updated or deleted so a stale arming can never post a
    /// superseded draft; the update path re-arms when the merged draft is
    /// still scheduled. Returns the number of jobs removed. The default is
    /// a no-op success so in-memory test doubles keep compiling; durable
    /// backends must override it.
    fn remove_scheduled_draft_jobs(&self, draft_id: &str) -> Result<usize, PortError> {
        let _ = draft_id;
        Ok(0)
    }
}

pub trait FollowStore: Send + Sync {
    fn follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError>;
    fn unfollow(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    fn is_following(&self, follower_id: &str, followee_id: &str) -> Result<bool, PortError>;
    fn get_follow(
        &self,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<Option<Follow>, PortError>;
    fn followers_of(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Follow>, PortError>;
    fn following_of(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Follow>, PortError>;

    // --- Pending follow requests (Phase 2.6 slice 4, decision D-a). A request
    // is NOT a follow edge: it never affects `is_following`, `followers_of`,
    // counters or visibility until accepted.
    /// Record a pending request. `Conflict(ALREADY_FOLLOWING)` if an edge
    /// exists, `Conflict(ALREADY_REQUESTED)` if a request exists.
    fn request_follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<FollowRequest, PortError> {
        let _ = (follower_id, followee_id, with_replies, notify, jobs);
        unimplemented!("FollowStore::request_follow not implemented")
    }
    fn get_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<Option<FollowRequest>, PortError> {
        let _ = (follower_id, followee_id);
        Ok(None)
    }
    /// Pending requests addressed to `account_id`, ordered by follower id.
    fn follow_requests_to(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<FollowRequest>, PortError> {
        let _ = (account_id, cursor, limit);
        Ok(Vec::new())
    }
    /// Atomically delete the request, create the follow edge (counters
    /// included) and enqueue `jobs`. `NotFound(NO_FOLLOW_REQUEST)` if
    /// there is no request.
    fn accept_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError> {
        let _ = (follower_id, followee_id, jobs);
        unimplemented!("FollowStore::accept_follow_request not implemented")
    }
    /// Atomically delete the request (reject or cancel) and enqueue `jobs`.
    /// `NotFound(NO_FOLLOW_REQUEST)` if there is no request.
    fn remove_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let _ = (follower_id, followee_id, jobs);
        unimplemented!("FollowStore::remove_follow_request not implemented")
    }
}

/// Cached per-host ActivityPub software name (`docs/misskey-parity-plan.md`
/// §6; backs the outbound `Like` emoji/plain split). One row per lowercased
/// host: the `software.name` from the host's NodeInfo document plus the fetch
/// time. Rows are read off-transaction by the delivery worker; staleness is
/// decided by the caller against
/// [`crate::federation::host_caps::HOST_SOFTWARE_TTL_MS`]. Probe failures are
/// never cached (the caller treats a miss as emoji-aware and retries the
/// probe on the next delivery).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostSoftware {
    pub host: String,
    pub software: Option<String>,
    pub fetched_at_ms: i64,
}

/// Directed blocks (`docs/misskey-parity-plan.md` §0A; Misskey
/// `MiBlocking{blocker,blockee}` unique).
///
/// `block` is the atomic unit: it validates the pair, rejects duplicates
/// with `Conflict(ALREADY_BLOCKING)`, removes both follow edges and both
/// pending follow requests (adjusting counters), inserts the block row, and
/// enqueues `jobs` — all in ONE storage transaction. The caller builds the
/// [`Blocking`] row (including its id) so the federated `Block` activity id
/// (`{url}/blocks/{id}`, Misskey `ApRendererService.renderBlock` parity) is
/// identical to the stored row id — and to the id a later `Undo(Block)`
/// references. Backends implement the whole method; the application op only
/// builds jobs (federated `Block` for local→remote, streaming hints). The
/// same method serves local `blocking/create` and inbound `Block`
/// (remote→local never federates, so no loop is possible).
pub trait BlockStore: Send + Sync {
    fn block(&self, blocking: Blocking, jobs: Vec<NewJob>) -> Result<Blocking, PortError>;
    /// Remove the block. `NotFound(NOT_BLOCKING)` when no row exists
    /// (Misskey `unblock` warns and returns; the endpoint checks first, so
    /// the application op surfaces the error).
    fn unblock(
        &self,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    fn get_blocking(
        &self,
        blocker_id: &str,
        blockee_id: &str,
    ) -> Result<Option<Blocking>, PortError>;
    /// True when `blocker_id` blocks `blockee_id`.
    fn is_blocking(&self, blocker_id: &str, blockee_id: &str) -> Result<bool, PortError>;
    /// Ids `blocker_id` blocks (read-time feed filter input).
    fn blocking_ids_for(&self, blocker_id: &str) -> Result<Vec<String>, PortError>;
    /// Ids blocking `blockee_id` (read-time feed filter input).
    fn blocked_by_ids_for(&self, blockee_id: &str) -> Result<Vec<String>, PortError>;
    /// Blocks created by `blocker_id`, newest-first, cursor-paged.
    fn list_blockings(
        &self,
        blocker_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Blocking>, PortError>;
}

/// Per-host capability cache backing [`HostSoftware`]: point lookups and
/// whole-row upserts only (no fan-out, no jobs — the delivery worker reads
/// and writes these rows outside any state transaction).
pub trait HostCapsStore: Send + Sync {
    fn get_host_software(&self, host: &str) -> Result<Option<HostSoftware>, PortError>;
    fn put_host_software(&self, entry: HostSoftware) -> Result<(), PortError>;
}

/// Mutes and renote-mutes (`docs/misskey-parity-plan.md` §0A; Misskey
/// `MiMuting{muter,mutee,expiresAt?}` and `MiRenoteMuting{muter,mutee}`).
///
/// Mutes never touch follow edges. `is_muting`/`muted_ids_for` report only
/// *active* mutes (`expires_at_ms` absent or in the future); `get_muting`
/// reports row existence (so `unmute` of an expired-but-present row still
/// resolves to `NOT_MUTING`-free deletion, mirroring Misskey's endpoint
/// existence check).
pub trait MuteStore: Send + Sync {
    /// `Ok(None)` when `expires_at_ms` is past (Misskey `mute/create`
    /// returns early without creating); `Conflict(ALREADY_MUTING)` on dup.
    fn mute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        expires_at_ms: Option<i64>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Muting>, PortError>;
    /// `NotFound(NOT_MUTING)` when no row exists.
    fn unmute(&self, muter_id: &str, mutee_id: &str, jobs: Vec<NewJob>) -> Result<(), PortError>;
    fn get_muting(
        &self,
        muter_id: &str,
        mutee_id: &str,
    ) -> Result<Option<Muting>, PortError>;
    /// True when an *active* mute row exists.
    fn is_muting(&self, muter_id: &str, mutee_id: &str) -> Result<bool, PortError>;
    /// Active mutee ids for `muter_id` (read-time feed filter input).
    fn muted_ids_for(&self, muter_id: &str) -> Result<Vec<String>, PortError>;
    /// Mute rows by `muter_id` (including expired), newest-first,
    /// cursor-paged (drives `mute/list`).
    fn list_mutings(
        &self,
        muter_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Muting>, PortError>;
    /// `Conflict(ALREADY_MUTING)` on dup.
    fn renote_mute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<RenoteMuting, PortError>;
    /// `NotFound(NOT_MUTING)` when no row exists.
    fn renote_unmute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    fn get_renote_muting(
        &self,
        muter_id: &str,
        mutee_id: &str,
    ) -> Result<Option<RenoteMuting>, PortError>;
    fn is_renote_muting(&self, muter_id: &str, mutee_id: &str) -> Result<bool, PortError>;
    /// Mutee ids whose pure renotes `muter_id` hides.
    fn renote_muted_ids_for(&self, muter_id: &str) -> Result<Vec<String>, PortError>;
    fn list_renote_mutings(
        &self,
        muter_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<RenoteMuting>, PortError>;
}

pub trait ReactionStore: Send + Sync {
    /// `reaction_id` becomes the stored row id. The application also federates
    /// the Like under this id, so a later `Undo(Like)` (built from the row id)
    /// names the activity the remote server actually received.
    fn react(
        &self,
        user_id: &str,
        note_id: &str,
        reaction: &str,
        reaction_id: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Reaction, PortError>;
    fn unreact(
        &self,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    fn get_reaction(
        &self,
        user_id: &str,
        note_id: &str,
    ) -> Result<Option<Reaction>, PortError>;
    /// Every reaction row by `user_id` (`users/reactions` read path;
    /// Misskey `NoteReactionsRepository` query parity). Backends return
    /// rows in any order; the application op sorts newest-first and pages.
    fn reactions_by_user(&self, user_id: &str) -> Result<Vec<Reaction>, PortError>;
    /// Every reaction row on `note_id` (§8 edit fan-out: remote reactors
    /// join the `Update(Note)` audience). Backends may scan; rows in any
    /// order.
    fn reactions_for_note(&self, note_id: &str) -> Result<Vec<Reaction>, PortError> {
        let _ = note_id;
        Err(PortError::StorageUnavailable(
            "ReactionStore::reactions_for_note not implemented".to_string(),
        ))
    }
    /// Total stored reaction rows (Batch P `stats`).
    fn count_reactions(&self) -> Result<u64, PortError> {
        Err(PortError::StorageUnavailable(
            "ReactionStore::count_reactions not implemented".to_string(),
        ))
    }
}

/// Note favorites (`notes/favorites/*`, `i/favorites`, `notes/state`;
/// Misskey `MiNoteFavorite` parity: unique over (user, note)).
pub trait FavoriteStore: Send + Sync {
    fn favorite_note(
        &self,
        favorite: crate::domain::favorite::NoteFavorite,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::favorite::NoteFavorite, PortError>;
    fn unfavorite_note(&self, user_id: &str, note_id: &str) -> Result<bool, PortError>;
    fn is_favorited(&self, user_id: &str, note_id: &str) -> Result<bool, PortError>;
    /// Favorites of `user_id`, newest-first (favorite-id order), cursor-paged.
    fn favorites_by_user(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<crate::domain::favorite::NoteFavorite>, PortError>;
}

/// Custom + remote emoji (Batch N WS1 + WP8b; Misskey `MiEmoji` parity:
/// local rows unique on case-insensitive shortcode, remote rows unique on
/// (lower host, lower shortcode)).
pub trait EmojiStore: Send + Sync {
    fn create_emoji(
        &self,
        emoji: crate::domain::emoji::CustomEmoji,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::emoji::CustomEmoji, PortError>;
    fn update_emoji(
        &self,
        emoji: crate::domain::emoji::CustomEmoji,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::emoji::CustomEmoji, PortError>;
    fn delete_emoji(&self, id: &str, log: ModerationLogEntry, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    fn get_emoji(&self, id: &str) -> Result<Option<crate::domain::emoji::CustomEmoji>, PortError>;
    fn get_emoji_by_name(&self, shortcode: &str) -> Result<Option<crate::domain::emoji::CustomEmoji>, PortError>;
    /// Local emoji, cursor-paged (`limit` default 10, 1..=100), newest-first,
    /// with optional case-insensitive substring `query` over
    /// name/aliases/category or exact `:name:` tokens (upstream list parity).
    fn list_emojis(
        &self,
        query: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<crate::domain::emoji::CustomEmoji>, PortError>;
    /// Every local emoji (pack path; single-node scan, bounded by the admin
    /// list — no per-note queries).
    fn all_local_emojis(&self) -> Result<Vec<crate::domain::emoji::CustomEmoji>, PortError>;
    /// Metadata-only remote-emoji upsert (AP `Emoji` tag ingest; no fetch).
    /// Updates `remote_url` when the (host, shortcode) row exists, inserts
    /// otherwise. Never touches `media_id`.
    fn upsert_remote_emoji(
        &self,
        emoji: crate::domain::emoji::RemoteEmoji,
    ) -> Result<crate::domain::emoji::RemoteEmoji, PortError>;
    fn get_remote_emoji(
        &self,
        host: &str,
        shortcode: &str,
    ) -> Result<Option<crate::domain::emoji::RemoteEmoji>, PortError>;
    /// Every known remote-emoji metadata row (lazy-fetch fan-out source).
    fn all_remote_emojis(&self) -> Result<Vec<crate::domain::emoji::RemoteEmoji>, PortError>;
    /// Record the lazily fetched local copy (`media_id`) for one remote row.
    fn set_remote_emoji_media(
        &self,
        host: &str,
        shortcode: &str,
        media_id: &str,
    ) -> Result<(), PortError>;
}

/// Antennas (Batch N WS4; guide WP15): rules + per-antenna feeds.
/// `create`/`update` take the moderation-style log? No — upstream publishes
/// `antennaCreated/Updated/Deleted` broadcast events, not moderation-log
/// rows. Pinetree publishes the `antenna` stream event post-commit through
/// `StreamingSink` (no moderation rows; admins need no log here either).
pub trait AntennaStore: Send + Sync {
    fn create_antenna(
        &self,
        antenna: crate::domain::antenna::Antenna,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::antenna::Antenna, PortError>;
    fn update_antenna(
        &self,
        antenna: crate::domain::antenna::Antenna,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::antenna::Antenna, PortError>;
    fn delete_antenna(&self, owner_id: &str, id: &str) -> Result<bool, PortError>;
    fn get_antenna(&self, owner_id: &str, id: &str) -> Result<Option<crate::domain::antenna::Antenna>, PortError>;
    /// Owner's antennas, newest-first.
    fn antennas_by_owner(&self, owner_id: &str) -> Result<Vec<crate::domain::antenna::Antenna>, PortError>;
    /// Every active antenna (match sweep source; worker pages in memory —
    /// bounded by the per-instance antenna count).
    fn all_active_antennas(&self) -> Result<Vec<crate::domain::antenna::Antenna>, PortError>;
    /// Touch `is_active` + `last_used_at_ms` (read-path reactivation,
    /// upstream `notes.ts` parity).
    fn touch_antenna(&self, owner_id: &str, id: &str, now_ms: i64) -> Result<(), PortError>;
}

/// Per-antenna feeds: `(antenna_id, note_id)` rows, newest-first reads,
/// trimmed to [`ANTENNA_NOTES_CAP`] on append (same transaction).
pub trait AntennaNoteStore: Send + Sync {
    /// Append `(antenna, note)` (deduplicated by key) and trim the feed to
    /// the newest [`ANTENNA_NOTES_CAP`] entries — one transaction.
    fn append_antenna_note(&self, antenna_id: &str, note_id: &str) -> Result<(), PortError>;
    /// Remove one entry (idempotent).
    fn remove_antenna_note(&self, antenna_id: &str, note_id: &str) -> Result<(), PortError>;
    /// Remove every entry of one antenna (delete cascade).
    fn delete_antenna_notes(&self, antenna_id: &str) -> Result<(), PortError>;
    /// Newest-first note ids of one antenna, cursor-paged.
    fn antenna_notes(
        &self,
        antenna_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<String>, PortError>;
}

pub trait ActivityDedup: Send + Sync {
    fn check_and_record(&self, activity_id: &str) -> Result<bool, PortError>;
    /// Read-only replay gate (Phase 2.6 slice 7): report whether
    /// `activity_id` was already recorded, without writing. The inbox
    /// calls this BEFORE any remote fetch so replays trigger zero network
    /// I/O. It is advisory only — `record_*_activity` stays the atomic
    /// authority, so the check-then-act race is benign (a concurrent
    /// first delivery simply does the fetch twice; dedup still converges).
    /// Backends must implement the real read; test doubles return `false`.
    fn is_recorded(&self, activity_id: &str) -> Result<bool, PortError>;
    /// Forget a recorded activity id so a later delivery with the same id is
    /// processed again. Used when `Undo(Follow)` retracts a Follow whose id
    /// the sender reuses for every re-follow of the same pair (Misskey's
    /// `/follows/{follower}/{followee}`). No silent default: backends and
    /// test doubles must implement this explicitly.
    fn forget_activity(&self, _activity_id: &str) -> Result<(), PortError> {
        unimplemented!("ActivityDedup::forget_activity not implemented")
    }
    fn record_note_activity(
        &self,
        activity_id: &str,
        input: CreateNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError>;
    fn record_delete_note_activity(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, note_id, jobs);
        unimplemented!("ActivityDedup::record_delete_note_activity not implemented")
    }
    /// Inbound `Delete` addressed by object URI (bench S6, 2026-10-17): the
    /// deleter may reach the store before the note's `Create`. Atomically
    /// deletes the live note at `object_uri` if one is held, else records a
    /// tombstone so a later `record_note_activity` / `create_note` for that
    /// URI is refused (`Ok(None)` / `Conflict`). Returns `true` only when a
    /// note was deleted. Replays of `activity_id` return `false`. Callers
    /// must have authorised the deleter against `object_uri` first.
    fn record_delete_uri_activity(
        &self,
        activity_id: &str,
        object_uri: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, object_uri, jobs);
        unimplemented!("ActivityDedup::record_delete_uri_activity not implemented")
    }
    fn record_follow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Follow>, PortError> {
        let _ = (
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        );
        unimplemented!("ActivityDedup::record_follow_activity not implemented")
    }
    /// Dedup + record a pending request from an inbound Follow to a locked
    /// account. `Ok(None)` when the activity id was already seen.
    fn record_follow_request_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<FollowRequest>, PortError> {
        let _ = (
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        );
        unimplemented!("ActivityDedup::record_follow_request_activity not implemented")
    }
    fn record_unfollow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, follower_id, followee_id, jobs);
        unimplemented!("ActivityDedup::record_unfollow_activity not implemented")
    }
    fn record_reject_follow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    /// Dedup + record an inbound `Block` (remote `blocker_id` blocks local
    /// `blockee_id`): the same atomic teardown as a local `block` (both
    /// follow edges, both pending requests) plus `jobs`, committed with the
    /// dedup mark in one transaction. `Ok(None)` when the activity id was
    /// already seen. A *different* activity re-blocking an existing pair
    /// converges to `Ok(None)` instead of erroring (no retry storm).
    fn record_block_activity(
        &self,
        activity_id: &str,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Blocking>, PortError> {
        let _ = (activity_id, blocker_id, blockee_id, jobs);
        unimplemented!("ActivityDedup::record_block_activity not implemented")
    }
    /// Dedup + remove the block for an inbound `Undo(Block)`.
    /// `Ok(false)` on replay or when no block row exists (Misskey `unblock`
    /// warns and returns in that case).
    fn record_unblock_activity(
        &self,
        activity_id: &str,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, blocker_id, blockee_id, jobs);
        unimplemented!("ActivityDedup::record_unblock_activity not implemented")
    }
    fn record_react_activity(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        reaction: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Reaction>, PortError> {
        let _ = (activity_id, user_id, note_id, reaction, upsert, jobs);
        unimplemented!("ActivityDedup::record_react_activity not implemented")
    }
    fn record_unreact_activity(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, user_id, note_id, jobs);
        unimplemented!("ActivityDedup::record_unreact_activity not implemented")
    }
    fn record_update_account_activity(
        &self,
        activity_id: &str,
        account_id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Account>, PortError> {
        let _ = (activity_id, account_id, patch, jobs);
        unimplemented!("ActivityDedup::record_update_account_activity not implemented")
    }
    /// Dedup + persist a locally authored `QuoteAuthorization` stamp and its
    /// `Accept` delivery job. `Ok(None)` when the activity id was already
    /// seen (replay: no duplicate stamp row, no duplicate `Accept`).
    fn record_quote_request_accept(
        &self,
        activity_id: &str,
        stamp: QuoteStamp,
        jobs: Vec<NewJob>,
    ) -> Result<Option<QuoteStamp>, PortError> {
        let _ = (activity_id, stamp, jobs);
        unimplemented!("ActivityDedup::record_quote_request_accept not implemented")
    }
    /// Dedup + enqueue a `Reject(QuoteRequest)` delivery job (no stamp is
    /// created). `Ok(false)` on replay.
    fn record_quote_request_reject(
        &self,
        activity_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, jobs);
        unimplemented!("ActivityDedup::record_quote_request_reject not implemented")
    }
    /// Dedup + move our pending quote to `Approved` with its verified stamp
    /// (inbound `Accept`). `Ok(None)` on replay.
    fn record_quote_decision_accept(
        &self,
        activity_id: &str,
        note_id: &str,
        stamp_uri: &str,
        verified_at_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let _ = (activity_id, note_id, stamp_uri, verified_at_ms, jobs);
        unimplemented!("ActivityDedup::record_quote_decision_accept not implemented")
    }
    /// Dedup + move our pending quote to `Rejected` (inbound `Reject`).
    /// The relation and API projection are retained. `Ok(None)` on replay.
    fn record_quote_decision_reject(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let _ = (activity_id, note_id, jobs);
        unimplemented!("ActivityDedup::record_quote_decision_reject not implemented")
    }
    /// Dedup + move our approved quote to `Revoked` (valid `Delete` of the
    /// stamp) and enqueue revocation-forwarding jobs. `Ok(None)` on replay.
    fn record_quote_stamp_delete(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let _ = (activity_id, note_id, jobs);
        unimplemented!("ActivityDedup::record_quote_stamp_delete not implemented")
    }
    /// Dedup + apply a federated note edit (inbound `Update(Note)`, §8):
    /// the same [`NoteStore::apply_note_edit`] storage path (no-change
    /// edits converge to the original), committed with the dedup mark in
    /// one transaction. `Ok(None)` on replay. `NotFound(NO_SUCH_NOTE)` when
    /// the cached copy is missing or deleted (callers map it to accept).
    fn record_note_edit_activity(
        &self,
        activity_id: &str,
        edit: EditNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let _ = (activity_id, edit, jobs);
        unimplemented!("ActivityDedup::record_note_edit_activity not implemented")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertMediaInput {
    pub id: Option<String>,
    pub owner_id: Option<String>,
    pub name: String,
    pub mime: String,
    pub size_bytes: u64,
    pub hash: String,
    /// SHA-256 hex of the stored bytes: the `BlobStore` content address
    /// (Phase 2.6 slice 8). `hash` stays the MD5 (packer `md5`, per-owner
    /// dedup key). Both are computed by the caller on upload (local +
    /// remote paths).
    pub blob_key: String,
    pub blurhash: Option<String>,
    pub properties: MediaProperties,
    pub is_sensitive: bool,
    pub is_link: bool,
    pub url: String,
    pub thumbnail_url: Option<String>,
    pub comment: Option<String>,
    /// Remote origin URL for cache rows (`None` for local uploads). With
    /// the owning note's `file_ids` this is the idempotent-insert key.
    pub source_url: Option<String>,
    pub max_file_size: u64,
}

/// One remote attachment carried inside a `RemoteMediaFetch` job payload.
/// Mirrors the ActivityPub `ParsedAttachment` shape in contract-stable
/// form so the application layer never imports ActivityPub types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteMediaAttachment {
    pub url: String,
    pub media_type: Option<String>,
    pub name: Option<String>,
    pub is_sensitive: bool,
    pub blurhash: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// Payload of a `JobKind::RemoteMediaFetch` job: the committed note to
/// attach to plus the (already count-truncated) remote attachments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteMediaFetchPayload {
    pub note_id: String,
    pub attachments: Vec<RemoteMediaAttachment>,
}

/// Payload of a `JobKind::QuoteStampReverify` job: re-dereference and verify
/// the stamp offered by an inbound `Accept(QuoteRequest)` whose first fetch
/// failed transiently. `accept_activity_id` is recorded only on successful
/// verification (with the state change, atomically), so retries and
/// re-deliveries converge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteStampReverifyPayload {
    pub accept_activity_id: String,
    pub request_uri: String,
    pub stamp_uri: String,
}

/// Payload of a `JobKind::ReplyParentResolve` job: resolve one pending
/// reply-parent URI (`docs/replies.md` Slice B). One URI-keyed job covers
/// all children waiting on that URI; the handler processes them in bounded
/// batches and enqueues a continuation when more remain. Duplicate jobs
/// for the same URI are harmless: linking is idempotent, so replays and
/// retries converge without double-counting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyParentResolvePayload {
    pub parent_uri: String,
}

/// Payload of a `JobKind::PollExpiry` job: the note whose poll may have
/// expired. `not_before` is armed at the poll's `expires_at`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollExpiryPayload {
    pub note_id: String,
}

/// Payload of a `JobKind::UnfollowAll` job (Batch M WP3): the suspended
/// account whose outgoing follows are silently removed, plus the walk
/// cursor. `after` is the last `followee_id` processed by the previous
/// batch (`None` = start); it advances monotonically so a skipped row can
/// never loop the sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnfollowAllPayload {
    pub account_id: String,
    #[serde(default)]
    pub after: Option<String>,
}

/// Payload of a `JobKind::RemoteEmojiFetch` job (Batch N WP8b): one remote
/// emoji to fetch-and-cache after a user opted in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteEmojiFetchPayload {
    pub host: String,
    pub shortcode: String,
}

/// Payload of a `JobKind::AntennaMatch` job (Batch N WS4): the committed
/// note to fan into antenna feeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntennaMatchPayload {
    pub note_id: String,
}

/// Payload of a `JobKind::PostScheduledDraft` job: the draft to post.
/// `scheduled_at_ms` is the `scheduled_at` the job was armed for — the
/// handler converges quietly when the stored draft no longer matches
/// (updated/rescheduled or unscheduled meanwhile). `not_before` is armed
/// at `scheduled_at_ms` (Misskey `postScheduledNoteQueue` delay parity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledDraftPayload {
    pub draft_id: String,
    pub scheduled_at_ms: i64,
}

/// Exportable profile entity (§13; Misskey `export*` job parity). The wire
/// filename stem, extension, and `exportedEntity` tag are fixed per entity
/// (see the `Export*ProcessorService.ts` files).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExportEntity {
    Following,
    Blocking,
    Muting,
    Notes,
    UserLists,
    Antennas,
    Clips,
    Favorites,
}

impl ExportEntity {
    /// Drive filename stem (`following-…`, `blocking-…`, `mute-…`,
    /// `notes-…`, `user-lists-…`, `antennas-…`, `clips-…`, `favorites-…`
    /// — verbatim upstream stems, including the `mute-` singular).
    pub fn file_stem(&self) -> &'static str {
        match self {
            ExportEntity::Following => "following",
            ExportEntity::Blocking => "blocking",
            ExportEntity::Muting => "mute",
            ExportEntity::Notes => "notes",
            ExportEntity::UserLists => "user-lists",
            ExportEntity::Antennas => "antennas",
            ExportEntity::Clips => "clips",
            ExportEntity::Favorites => "favorites",
        }
    }

    /// Drive file extension (CSV for the acct-list entities, JSON for the
    /// structured ones — verbatim upstream `ext`).
    pub fn file_ext(&self) -> &'static str {
        match self {
            ExportEntity::Following
            | ExportEntity::Blocking
            | ExportEntity::Muting
            | ExportEntity::UserLists => "csv",
            ExportEntity::Notes
            | ExportEntity::Antennas
            | ExportEntity::Clips
            | ExportEntity::Favorites => "json",
        }
    }

    /// Misskey `exportCompleted` `exportedEntity` tag (verbatim upstream
    /// values, including the `muting`/`note`/`userList` singulars).
    pub fn exported_entity(&self) -> &'static str {
        match self {
            ExportEntity::Following => "following",
            ExportEntity::Blocking => "blocking",
            ExportEntity::Muting => "muting",
            ExportEntity::Notes => "note",
            ExportEntity::UserLists => "userList",
            ExportEntity::Antennas => "antenna",
            ExportEntity::Clips => "clip",
            ExportEntity::Favorites => "favorite",
        }
    }
}

/// Importable profile entity (§13; Misskey `import*` job parity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ImportEntity {
    Following,
    Blocking,
    Muting,
    UserLists,
    Antennas,
}

/// Payload of a `JobKind::Export` job: which entity of `user_id` to export
/// into the caller's drive. `export_id` keys idempotency (deterministic
/// completion-notification key + content-deduped upload). `exclude_muting`
/// drops muted followees (`export-following` parity); `exclude_inactive`
/// is accepted and ignored — pinetree tracks no last-active timestamp
/// (DIVERGENCE, documented at the call sites).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportJobPayload {
    pub export_id: String,
    pub user_id: String,
    pub entity: ExportEntity,
    pub exclude_muting: bool,
    pub exclude_inactive: bool,
}

/// Payload of a `JobKind::Import` job: apply the caller's drive file
/// `file_id` as `entity` rows for `user_id`. `import_id` keys idempotency
/// (replays converge on per-row `ALREADY_*` conflicts). `with_replies`
/// seeds the follow `withReplies` default when a row carries none
/// (`import-following` parity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportJobPayload {
    pub import_id: String,
    pub user_id: String,
    pub entity: ImportEntity,
    pub file_id: String,
    pub with_replies: Option<bool>,
}

/// Polls and poll votes (`docs/misskey-parity-plan.md` §4; Misskey `MiPoll`
/// + `MiPollVote`). Poll rows are created atomically with their note in
/// `NoteStore::create_note`; this trait covers reads, voting, and inbound
/// `Question` vote-count sync. One storage transaction commits the vote +
/// counter bump + jobs atomically; FDB inherits this path (no second path).
pub trait PollStore: Send + Sync {
    /// The poll attached to `note_id`, if any.
    fn get_poll(&self, note_id: &str) -> Result<Option<Poll>, PortError>;
    /// Every vote by `user_id` on `note_id` (drives `isVoted` packing and
    /// duplicate-vote detection). Empty when the user never voted.
    fn votes_by_user_for_note(
        &self,
        user_id: &str,
        note_id: &str,
    ) -> Result<Vec<PollVote>, PortError>;
    /// Atomically record one vote and bump `votes[choice]` (Misskey
    /// `PollService.vote` parity). The backend re-checks, in order:
    /// note exists and is live (`NotFound(NO_SUCH_NOTE)`), poll exists
    /// (`Validation(NO_POLL)`), voter not blocked by the author
    /// (`Validation(YOU_HAVE_BEEN_BLOCKED)`; self-votes exempt), poll live
    /// (`Validation(ALREADY_EXPIRED)`), choice in range
    /// (`Validation(INVALID_CHOICE)`), no duplicate (`Conflict(ALREADY_VOTED)`
    /// — for `multiple` polls only the same choice conflicts). `jobs`
    /// commit in the same transaction.
    fn cast_vote(&self, vote: PollVote, jobs: Vec<NewJob>) -> Result<PollVote, PortError>;
    /// Overwrite a poll's vote counts by choice name (inbound `Question`
    /// update, Misskey `ApQuestionService.updateQuestion` parity): counts
    /// for stored choices present in `counts_by_name` are replaced, unknown
    /// names are ignored, stored choices missing from the update keep their
    /// counts. `NotFound(NO_POLL)` when the note has no poll.
    fn sync_poll_votes(
        &self,
        note_id: &str,
        counts_by_name: &[PollChoiceCount],
        jobs: Vec<NewJob>,
    ) -> Result<Poll, PortError>;
}

/// One choice's remotely reported vote count (inbound `Question` sync input;
/// mirrors the ActivityPub `oneOf`/`anyOf` entry shape in contract-stable
/// form so the application layer never imports ActivityPub types).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollChoiceCount {
    pub name: String,
    pub votes: u64,
}

pub trait MediaStore: Send + Sync {
    fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError>;
    fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError>;
    fn find_by_owner_and_hash(
        &self,
        owner_id: &str,
        hash: &str,
    ) -> Result<Option<MediaMeta>, PortError>;
    /// All media rows cached from `source_url` (Phase 2.6 slice 7). The
    /// worker intersects this with the note's `file_ids` to skip
    /// already-attached URLs without any network fetch.
    fn find_by_source_url(&self, source_url: &str) -> Result<Vec<MediaMeta>, PortError>;
    fn delete_metadata(&self, id: &str) -> Result<(), PortError>;
    /// Newest-first metadata rows owned by `owner_id` (Phase 3
    /// `drive/files`). Ordering is by row id (aidx ids are time-ordered).
    fn list_by_owner(&self, owner_id: &str) -> Result<Vec<MediaMeta>, PortError>;
    /// Newest-first metadata rows across all owners (Batch N WS6
    /// `admin/drive/files`). Backends return rows in any order; the
    /// application op sorts newest-first and pages.
    fn list_all_media(&self, cursor: &TimelineCursor) -> Result<Vec<MediaMeta>, PortError>;
}

pub trait BlobStore: Send + Sync {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError>;
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError>;
    fn delete(&self, hash: &str) -> Result<(), PortError>;
}

pub trait NotificationStore: Send + Sync {
    fn insert_notification(&self, notification: Notification) -> Result<Notification, PortError>;
    fn list_notifications(
        &self,
        notifiee_id: &str,
        limit: usize,
    ) -> Result<Vec<Notification>, PortError>;
    /// Mark every notification of `notifiee_id` read. Returns the count
    /// flipped. Used by `notifications/mark-all-as-read` and the streaming
    /// `readNotification` message (Phase 3).
    fn mark_all_read(&self, notifiee_id: &str) -> Result<u64, PortError>;
    /// Newest-first page of `notifiee_id`'s notifications filtered by
    /// `cursor` (ids + dates, limit) and kind allow/deny lists (Phase 3
    /// `i/notifications`). An empty `include` matches every kind; kinds in
    /// `exclude` never match.
    fn list_notifications_filtered(
        &self,
        notifiee_id: &str,
        cursor: &TimelineCursor,
        include: &[NotificationKind],
        exclude: &[NotificationKind],
    ) -> Result<Vec<Notification>, PortError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiTokenRecord {
    /// Opaque token id (Misskey `MiAccessToken.id`, aidx). Assigned at mint;
    /// rows written before Batch O backfill it at open. Addressed by
    /// `i/revoke-token {tokenId}` and listed by `i/apps`.
    #[serde(default)]
    pub id: String,
    pub token_hash: String,
    pub account_id: String,
    pub permissions: Vec<String>,
    pub is_first_party: bool,
    pub name: Option<String>,
    pub description: Option<String>,
    pub icon_url: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MiAuthSessionRecord {
    pub session: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub icon_url: Option<String>,
    pub callback: Option<String>,
    pub permissions: Vec<String>,
    pub account_id: Option<String>,
    pub token: Option<String>,
    pub fetched: bool,
    pub created_at_ms: i64,
    /// Approval deadline (Batch N WS7 hardening; upstream has no expiry).
    /// `None` = no deadline (rows written before expiry existed). New rows
    /// always carry `Some`; expired rows read as missing (deleted on read).
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyAuthSessionRecord {
    pub id: String,
    pub token: String,
    pub app_secret: String,
    pub app_name: String,
    pub permissions: Vec<String>,
    pub account_id: Option<String>,
    pub access_token: Option<String>,
    pub created_at_ms: i64,
    /// Approval deadline (Batch N WS7 hardening; upstream has no expiry).
    /// `None` = no deadline (rows written before expiry existed).
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorKeypairRecord {
    pub account_id: String,
    pub public_key_pem: String,
    pub private_key_pem: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemotePublicKeyRecord {
    pub key_id: String,
    pub actor_uri: String,
    pub account_id: Option<String>,
    pub public_key_pem: String,
    pub fetched_at_ms: i64,
}

pub trait AuthStore: Send + Sync {
    fn set_password_hash(&self, account_id: &str, password_hash: &str) -> Result<(), PortError>;
    fn get_password_hash(&self, account_id: &str) -> Result<Option<String>, PortError>;
    fn store_token(&self, record: ApiTokenRecord) -> Result<(), PortError>;
    fn get_token_by_hash(&self, token_hash: &str) -> Result<Option<ApiTokenRecord>, PortError>;
    /// Delete the token row addressed by `token_hash` (Phase 3
    /// `i/revoke-token`). A missing row is a no-op success.
    fn delete_token_by_hash(&self, token_hash: &str) -> Result<(), PortError>;
    /// Fetch one grant by its opaque id (Batch O `i/revoke-token`
    /// `{tokenId}`, `i/apps`). Rows minted before ids existed are backfilled
    /// at open, so every stored row resolves.
    fn get_token_by_id(&self, id: &str) -> Result<Option<ApiTokenRecord>, PortError>;
    /// All grants ever minted for `account_id`, oldest-first (Batch O
    /// `i/apps`, `i/regenerate-token`). No pagination: callers bound the
    /// account's live grants by construction (signin mints one row per
    /// login; see `MAX_SIGNIN_ROWS_PER_USER` for the analogous cap).
    fn tokens_for_account(&self, account_id: &str) -> Result<Vec<ApiTokenRecord>, PortError>;
    /// Delete every first-party (native) grant of `account_id` (Batch O
    /// `i/regenerate-token`: the single native token rotation). App grants
    /// (`is_first_party: false`) keep working. Returns rows removed.
    fn delete_account_first_party_tokens(&self, account_id: &str) -> Result<u64, PortError>;
    fn save_miauth_session(&self, session: MiAuthSessionRecord) -> Result<(), PortError>;
    fn get_miauth_session(&self, session_id: &str) -> Result<Option<MiAuthSessionRecord>, PortError>;
    fn consume_miauth_session(
        &self,
        session_id: &str,
    ) -> Result<Option<MiAuthSessionRecord>, PortError>;
    fn save_legacy_session(&self, session: LegacyAuthSessionRecord) -> Result<(), PortError>;
    fn get_legacy_session(&self, token: &str) -> Result<Option<LegacyAuthSessionRecord>, PortError>;
    fn take_legacy_session(
        &self,
        token: &str,
    ) -> Result<Option<LegacyAuthSessionRecord>, PortError>;
    fn get_actor_keypair(
        &self,
        account_id: &str,
    ) -> Result<Option<ActorKeypairRecord>, PortError> {
        let _ = account_id;
        Ok(None)
    }
    fn save_actor_keypair(&self, record: ActorKeypairRecord) -> Result<(), PortError> {
        let _ = record;
        Ok(())
    }
    /// Store `record` only if the account has no keypair yet, and return the keypair that is stored
    /// afterwards (the existing one when another caller won). Concurrent first fetches of an actor
    /// must all see one key, so stores should make this atomic; the default is get-then-save.
    fn save_actor_keypair_if_absent(
        &self,
        record: ActorKeypairRecord,
    ) -> Result<ActorKeypairRecord, PortError> {
        if let Some(existing) = self.get_actor_keypair(&record.account_id)? {
            return Ok(existing);
        }
        self.save_actor_keypair(record.clone())?;
        Ok(record)
    }
    fn get_remote_public_key(
        &self,
        key_id: &str,
    ) -> Result<Option<RemotePublicKeyRecord>, PortError> {
        let _ = key_id;
        Ok(None)
    }
    fn save_remote_public_key(&self, record: RemotePublicKeyRecord) -> Result<(), PortError> {
        let _ = record;
        Ok(())
    }
}

/// One signin attempt row (Batch O WP9; Misskey `MiSignin`: `id`, `userId`,
/// `ip varchar(128)`, `headers jsonb`, `success bool`, cascade-delete on
/// user). Written for password-signin success/failure (known account only,
/// like upstream `fail()`) and signup success. MiAuth and legacy-session
/// approvals are not signins upstream and write no row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SigninRecord {
    pub id: String,
    pub account_id: String,
    pub ip: String,
    pub headers: String,
    pub success: bool,
    pub created_at_ms: i64,
}

/// Bound on stored signin rows per account (Batch O WP9). Upstream keeps
/// every row; pinetree trims oldest-first in the same transaction so the
/// read path stays bounded (no FDB impact: the write is one row + bounded
/// deletes). No backfill possible — rows exist only from deploy forward.
pub const MAX_SIGNIN_ROWS_PER_USER: usize = 100;

pub trait SigninStore: Send + Sync {
    /// Insert one attempt row and trim the account's history to
    /// [`MAX_SIGNIN_ROWS_PER_USER`] oldest-first, atomically.
    fn record_signin(&self, record: SigninRecord) -> Result<(), PortError>;
    /// Newest-first page for `i/signin-history`. `limit` is clamped by the
    /// caller (1..=100, default 10). Cursor pairs mirror upstream
    /// (`sinceId`/`untilId` aidx ids, `sinceDate`/`untilDate` ms).
    #[allow(clippy::too_many_arguments)]
    fn list_signins(
        &self,
        account_id: &str,
        limit: usize,
        since_id: Option<&str>,
        until_id: Option<&str>,
        since_date: Option<i64>,
        until_date: Option<i64>,
    ) -> Result<Vec<SigninRecord>, PortError>;
}

/// One password-reset request row (Batch P; Misskey
/// `MiPasswordResetRequest`: `id` (aid, carries the 30-minute TTL —
/// `reset-password` rejects tokens older than 30 min by id timestamp),
/// `userId`, `token` (64-char). No expiry sweeper: rows are single-use
/// (deleted on consume) and unreadable rows fail closed (`NO_SUCH_CODE`
/// posture like `verify-email`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PasswordResetRequest {
    pub id: String,
    pub user_id: String,
    pub token: String,
}

/// One client registry item (Batch P; Misskey `MiRegistryItem`: `id`,
/// `userId`, `domain` (nullable; an access-token id when written with a
/// token-scoped call, else the explicit `domain` param or null), `scope`
/// (string array), `key`, `value` (arbitrary JSON), `updatedAt`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryItem {
    pub id: String,
    pub user_id: String,
    pub domain: Option<String>,
    pub scope: Vec<String>,
    pub key: String,
    pub value: Value,
    pub updated_at_ms: i64,
}

/// Password-reset tokens (Batch P; `request-reset-password` /
/// `reset-password`). Single-use rows keyed by token.
pub trait PasswordResetStore: Send + Sync {
    /// Insert one request row (caller mints id + 64-char token).
    fn create_reset_request(&self, request: PasswordResetRequest) -> Result<(), PortError>;
    /// Look up by token (`None` when unknown or already consumed).
    fn find_reset_request(&self, token: &str) -> Result<Option<PasswordResetRequest>, PortError>;
    /// Delete one row by id (consume on successful reset; best-effort on
    /// expiry — missing rows are not an error).
    fn delete_reset_request(&self, id: &str) -> Result<(), PortError>;
}

/// Client key-value registry (Batch P; `i/registry/*`). Upstream
/// `RegistryApiService` semantics: one row per
/// (user, domain, scope, key); `set` upserts; reads with a non-null
/// domain are token/app-scoped, null-domain rows are shared. Scope
/// segments are validated by the caller (`^[a-zA-Z0-9_]+$`).
pub trait RegistryStore: Send + Sync {
    /// Upsert one item (insert or replace value + `updated_at_ms`).
    fn registry_set(&self, item: RegistryItem) -> Result<(), PortError>;
    /// Fetch one item by (user, domain, scope, key).
    fn registry_get(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
        key: &str,
    ) -> Result<Option<RegistryItem>, PortError>;
    /// All items of one (user, domain, scope).
    fn registry_list(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
    ) -> Result<Vec<RegistryItem>, PortError>;
    /// Delete one item (missing rows are not an error, like upstream
    /// `remove`).
    fn registry_remove(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
        key: &str,
    ) -> Result<(), PortError>;
    /// Distinct (domain, scope) pairs holding at least one item for the
    /// user (`scopes-with-domain`).
    fn registry_scopes(&self, user_id: &str) -> Result<Vec<(Option<String>, Vec<String>)>, PortError>;
}

/// User lists (Batch O WP16; pull timelines, remote members, favorites).
pub trait UserListStore: Send + Sync {
    /// Insert one list; `Conflict(TOO_MANY_USERLISTS)` when the owner
    /// already holds [`crate::domain::user_list::USER_LIST_LIMIT`] lists.
    /// Count check + insert commit atomically (conformance K2).
    fn create_user_list(
        &self,
        list: crate::domain::user_list::UserList,
    ) -> Result<crate::domain::user_list::UserList, PortError>;
    /// Owner-scoped rewrite (name / visibility). `None` on miss.
    fn update_user_list(
        &self,
        list: crate::domain::user_list::UserList,
    ) -> Result<Option<crate::domain::user_list::UserList>, PortError>;
    /// Owner-scoped delete with membership + favorite cascade, one
    /// transaction (conformance K3). Returns true when a row existed.
    fn delete_user_list(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError>;
    fn get_user_list(
        &self,
        list_id: &str,
    ) -> Result<Option<crate::domain::user_list::UserList>, PortError>;
    /// Owner's lists, newest-first.
    fn lists_by_owner(
        &self,
        owner_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserList>, PortError>;
    /// Owner's public lists, newest-first (`users/lists/list{userId}`).
    fn public_lists_by_owner(
        &self,
        owner_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserList>, PortError>;
    /// Add one member; `Conflict(ALREADY_ADDED)` on duplicate,
    /// `Conflict(TOO_MANY_USERS)` past
    /// [`crate::domain::user_list::USER_LIST_MEMBER_LIMIT`] members.
    /// Cap check + insert + `jobs` commit atomically (conformance K2).
    fn add_list_member(
        &self,
        member: crate::domain::user_list::UserListMembership,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::user_list::UserListMembership, PortError>;
    /// Remove one member (idempotent) with `jobs`, one transaction.
    /// Returns true when a row existed.
    fn remove_list_member(
        &self,
        list_id: &str,
        member_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    /// Flip `with_replies`. `None` on miss.
    fn update_list_membership(
        &self,
        list_id: &str,
        member_id: &str,
        with_replies: bool,
    ) -> Result<Option<crate::domain::user_list::UserListMembership>, PortError>;
    fn get_list_membership(
        &self,
        list_id: &str,
        member_id: &str,
    ) -> Result<Option<crate::domain::user_list::UserListMembership>, PortError>;
    /// Every membership of one list (bounded by the member cap).
    fn members_of_list(
        &self,
        list_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserListMembership>, PortError>;
    /// Favorite one public list (idempotent insert). Returns true when the
    /// row was newly added.
    fn favorite_list(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError>;
    /// Unfavorite (idempotent). Returns true when a row existed.
    fn unfavorite_list(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError>;
    fn is_list_favorited(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError>;
    fn list_favorite_count(&self, list_id: &str) -> Result<u64, PortError>;
}

pub trait StreamingSink: Send + Sync {
    fn publish(&self, event_type: &str, payload: &[u8]) -> Result<(), PortError>;
    /// Fan out one `main`-channel event. The default is a no-op so
    /// test doubles keep working; the broadcast hub overrides it.
    fn push_main(&self, _msg: crate::application::events::MainMessage) -> Result<(), PortError> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct NoopActivityDedup;

impl ActivityDedup for NoopActivityDedup {
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

    fn record_block_activity(
        &self,
        _activity_id: &str,
        _blocker_id: &str,
        _blockee_id: &str,
        _jobs: Vec<NewJob>,
    ) -> Result<Option<crate::domain::blocking::Blocking>, PortError> {
        Ok(None)
    }

    fn record_unblock_activity(
        &self,
        _activity_id: &str,
        _blocker_id: &str,
        _blockee_id: &str,
        _jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        Ok(false)
    }
}

#[derive(Debug, Default, Clone)]
pub struct NoopStreamingSink;

impl StreamingSink for NoopStreamingSink {
    fn publish(&self, _event_type: &str, _payload: &[u8]) -> Result<(), PortError> {
        Ok(())
    }
}

// --- Batch M: moderation, registration policy, instance settings -----------
//
// New ports carry ONLY required methods (guide §2 rule 4): a backend that
// omits one must fail to compile. The atomicity contract (§2.3, conformance
// groups H/I) is structural like the rest of this file: pinetree ports have
// no transaction handle, so a moderation action's state change, its
// moderation-log row, and its outbound jobs are committed together by ONE
// port call — the compound methods below. `append` is only for log rows
// that are the whole action.

/// [`ModerationLogStore::list_logs`] filter (upstream
/// `admin/show-moderation-logs` params).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModerationLogFilter {
    /// Upstream `type`.
    pub action: Option<ModerationAction>,
    /// The moderator who performed the action (upstream `userId`).
    pub actor: Option<String>,
    /// The account or entity acted on (pinetree extra).
    pub target: Option<String>,
    /// Case-insensitive substring match on the details JSON (upstream
    /// `search`: `info::text ILIKE`).
    pub search: Option<String>,
}

pub trait ModerationLogStore: Send + Sync {
    /// WP3 suspension: flip `is_suspended`, delete follow requests in both
    /// directions when suspending (upstream `postSuspend`), append `log`,
    /// and enqueue `jobs` — one transaction (conformance H1/H2/H3). The
    /// refusal rule (never act on a moderator/admin) is application logic
    /// and lives in the command, not here.
    fn set_suspended(
        &self,
        account_id: &str,
        suspended: bool,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError>;

    /// §3.1 roles: set the fixed built-in role flags and append `log` in
    /// one transaction. This is the ONLY write path for `is_admin`/
    /// `is_moderator`; `UpdateAccountPatch` deliberately cannot set them.
    fn set_role(
        &self,
        account_id: &str,
        is_admin: bool,
        is_moderator: bool,
        log: ModerationLogEntry,
    ) -> Result<Account, PortError>;

    /// Account patch + moderation-log row + jobs in one transaction
    /// (conformance H4). Used by every Batch M admin action that changes an
    /// account (`admin/reject-quotes`, `admin/update-user-note`, …).
    fn update_account_logged(
        &self,
        account_id: &str,
        patch: UpdateAccountPatch,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError>;

    /// Batch M WP6 delete-account: tombstone the account (handle stays
    /// reserved), remove follows and follow requests in both directions,
    /// append `log` when given, and enqueue `jobs` — one transaction
    /// (conformance C3 + delete atomicity). `log` is `Some` for admin
    /// deletes and self-deletes (actor = the account itself).
    fn delete_account(
        &self,
        account_id: &str,
        log: Option<ModerationLogEntry>,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;

    /// Append one log row in its own transaction. Only for rows that ARE
    /// the action (nothing else commits with them); moderation actions use
    /// the compound methods above so a log row can never be lost against
    /// its action (H4).
    fn append(&self, entry: ModerationLogEntry) -> Result<(), PortError>;

    /// Newest-first page of log rows matching `filter`, cursor-paged by id
    /// (`TimelineCursor` since/until + limit).
    fn list_logs(
        &self,
        filter: &ModerationLogFilter,
        cursor: &TimelineCursor,
    ) -> Result<Vec<ModerationLogEntry>, PortError>;
}

/// [`InstanceSettingsStore::update`] patch (upstream `admin/update-meta`
/// subset used by Batch M). `blocked_hosts` is normalized by the store:
/// lowercased, trimmed, deduplicated, empties dropped.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceSettingsPatch {
    /// Maps to upstream `disableRegistration` (true = [`RegistrationPolicy::Closed`]).
    pub registration: Option<RegistrationPolicy>,
    pub blocked_hosts: Option<Vec<String>>,
    pub name: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub max_note_text_length: Option<usize>,
}

pub trait InstanceSettingsStore: Send + Sync {
    /// Current settings; a fresh store returns [`InstanceSettings::default()`]
    /// (registration open, no blocked hosts, `maxNoteTextLength` 3000).
    fn get(&self) -> Result<InstanceSettings, PortError>;
    /// Apply `patch` and append `log` in one transaction (§3.3).
    fn update(
        &self,
        patch: InstanceSettingsPatch,
        log: ModerationLogEntry,
    ) -> Result<InstanceSettings, PortError>;
}

// ---------------------------------------------------------------------------
// Invite codes (Batch M WP2; upstream `RegistrationTicket`)
// ---------------------------------------------------------------------------

/// One new invite code for [`InviteStore::create_invites`]. The code string
/// is generated by the application (upstream `misc/generate-invite-code.ts`
/// charset); the ticket id and `created_at_ms` are store-generated.
#[derive(Debug, Clone)]
pub struct CreateInviteInput {
    pub code: String,
    pub created_by: Option<String>,
    pub expires_at_ms: Option<i64>,
}

/// The atomic invite claim: consume `code` and create the account in ONE
/// transaction (conformance I1/I2). Mirrors upstream `SignupApiService`
/// (ticket consume) + `SignupService.signup` (account create).
#[derive(Debug, Clone)]
pub struct ConsumeInviteInput {
    pub code: String,
    pub account: CreateAccountInput,
    /// Written with the account row (upstream `SignupService` writes the
    /// password hash in the same transaction as the user).
    pub password_hash: Option<String>,
}

/// `invite/list` / `admin/invite/list` sort (upstream `sort`). Misskey's
/// convention: `+createdAt` is newest-first (upstream orders `id DESC`),
/// `-createdAt` oldest-first. `±usedAt` sorts by `usedAt` with unused codes
/// LAST for `+usedAt` and FIRST for `-usedAt` (upstream `NULLS LAST` /
/// `NULLS FIRST`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InviteSort {
    /// `+createdAt` (and the unset default): id descending.
    CreatedDesc,
    /// `-createdAt`: id ascending.
    CreatedAsc,
    /// `+usedAt`: used_at descending, unused last.
    UsedDesc,
    /// `-usedAt`: used_at ascending, unused first.
    UsedAsc,
}

impl Default for InviteSort {
    fn default() -> Self {
        Self::CreatedDesc
    }
}

/// `admin/invite/list` `type` filter (upstream `type`; `expired` compares
/// `expiresAt < now`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InviteTypeFilter {
    #[default]
    All,
    Unused,
    Used,
    Expired,
}

/// `invite/list` / `admin/invite/list` filters. `created_by: None` = every
/// creator (the `admin/invite/list` view); `invite/list` always scopes to
/// the caller. Keyset paging (since/until id or date) is the `invite/list`
/// shape; `offset` is the `admin/invite/list` shape.
#[derive(Debug, Clone)]
pub struct InviteListFilter {
    pub created_by: Option<String>,
    pub filter: InviteTypeFilter,
    pub since_id: Option<String>,
    pub until_id: Option<String>,
    pub since_date_ms: Option<i64>,
    pub until_date_ms: Option<i64>,
    pub sort: InviteSort,
    /// 1..=100 (endpoint-validated; 0 yields an empty page).
    pub limit: usize,
    /// 0-based row offset (upstream `offset`; `invite/list` has none).
    pub offset: usize,
}

impl Default for InviteListFilter {
    fn default() -> Self {
        Self {
            created_by: None,
            filter: InviteTypeFilter::All,
            since_id: None,
            until_id: None,
            since_date_ms: None,
            until_date_ms: None,
            sort: InviteSort::default(),
            limit: 30,
            offset: 0,
        }
    }
}

/// Abuse reports (Batch M WP4; upstream `AbuseUserReport`). Whole-port
/// methods: state change + moderation-log row + outbound jobs commit in
/// ONE transaction (§2.3). `create` carries no log (upstream logs only
/// on resolve/forward/note-update); `resolve`/`set_note`/`mark_forwarded`
/// are compound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbuseReportFilter {
    /// Upstream `state`: "resolved" | "unresolved" | None (all).
    pub state: Option<String>,
    /// Upstream `reporterOrigin`: combined/local/remote.
    pub reporter_origin: String,
    /// Upstream `targetUserOrigin`: combined/local/remote.
    pub target_origin: String,
}

pub trait AbuseReportStore: Send + Sync {
    fn create(&self, report: AbuseReport) -> Result<AbuseReport, PortError>;
    fn get(&self, id: &str) -> Result<Option<AbuseReport>, PortError>;

    /// Batch M WP6: number of abuse reports whose target is `account_id`.
    fn count_for_target(&self, account_id: &str) -> Result<usize, PortError>;

    fn list(
        &self,
        filter: &AbuseReportFilter,
        cursor: &TimelineCursor,
    ) -> Result<Vec<AbuseReport>, PortError>;

    /// Resolve: flip resolved/resolved_as/assignee, append `log`, enqueue
    /// `jobs` — one transaction.
    fn resolve(
        &self,
        id: &str,
        resolved_as: Option<AbuseReportResolution>,
        assignee: Option<String>,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<AbuseReport, PortError>;

    /// Set the moderation note. `log` is `Some` only when the note actually
    /// changed (upstream logs `updateAbuseReportNote` conditionally).
    fn set_note(
        &self,
        id: &str,
        note: String,
        log: Option<ModerationLogEntry>,
        jobs: Vec<NewJob>,
    ) -> Result<AbuseReport, PortError>;

    /// Mark forwarded; commits with `log` + `jobs` (the Flag delivery) in
    /// one transaction. Returns the updated report.
    fn mark_forwarded(
        &self,
        id: &str,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<AbuseReport, PortError>;

    /// Inbound `Flag` (upstream `ApInboxService.flag` parity): atomically
    /// check-and-record `activity_id` and insert `report`. `Ok(None)` on
    /// replay (already seen). The target must be a local account id; the
    /// reporter may be remote (its account id is stored; host is derivable
    /// from the account row).
    fn create_flag_report(
        &self,
        activity_id: &str,
        report: AbuseReport,
    ) -> Result<Option<AbuseReport>, PortError>;
}

/// Invite codes (Batch M WP2). Whole-port methods: any `log` row commits
/// with the state change in one transaction (§2.3). Invite creation and
/// deletion are moderator actions (§9.1 default policy), so the log rows
/// are not optional on the write paths that carry them.
pub trait InviteStore: Send + Sync {
    /// Create 1..=100 codes in one transaction; `log` is one
    /// `createInvitation` row covering the batch (upstream
    /// `admin/invite/create.ts` logs the created tickets together).
    fn create_invites(
        &self,
        inputs: &[CreateInviteInput],
        log: Option<&ModerationLogEntry>,
    ) -> Result<Vec<InviteCode>, PortError>;

    fn find_invite_by_id(&self, id: &str) -> Result<Option<InviteCode>, PortError>;

    /// Unique-code lookup (drives the signup pre-check and the claim).
    fn find_invite_by_code(&self, code: &str) -> Result<Option<InviteCode>, PortError>;

    fn list_invites(&self, filter: &InviteListFilter) -> Result<Vec<InviteCode>, PortError>;

    /// Delete one code and its unique-index row, appending `log` in the
    /// same transaction.
    fn delete_invite(
        &self,
        id: &str,
        log: Option<&ModerationLogEntry>,
    ) -> Result<(), PortError>;

    /// Claim `code` and create the account in ONE transaction: the code is
    /// marked used, its unique-index row is removed, and the account row
    /// (with `password_hash`) commits together — or nothing does. A
    /// missing, already-used, or expired code fails with
    /// [`PortError::NotFound`] and changes nothing (conformance I1/I2).
    fn consume_and_create_account(
        &self,
        input: ConsumeInviteInput,
    ) -> Result<(Account, InviteCode), PortError>;
}
