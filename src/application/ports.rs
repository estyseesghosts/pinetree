use serde::{Deserialize, Serialize};

use crate::domain::account::Account;
use crate::domain::follow::{Follow, FollowNotify, FollowRequest};
pub use crate::domain::id::{Clock, DomainError, IdGenerator, PortError};
use crate::domain::media::{MediaMeta, MediaProperties};
use crate::domain::note::{Note, ReactionAcceptance, Visibility};
use crate::domain::notification::{Notification, NotificationKind};
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateAccountPatch {
    pub display_name: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub location: Option<Option<String>>,
    pub birthday: Option<Option<String>>,
    pub avatar_id: Option<Option<String>>,
    pub banner_id: Option<Option<String>>,
    pub is_locked: Option<bool>,
    pub is_bot: Option<bool>,
    pub is_cat: Option<bool>,
    pub is_explorable: Option<bool>,
}

pub trait AccountStore: Send + Sync {
    fn create_account(&self, input: CreateAccountInput) -> Result<Account, PortError>;
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
    pub has_channel: bool,
    pub prohibited_words: Vec<String>,
    pub sensitive_words: Vec<String>,
    #[serde(default)]
    pub uri: Option<String>,
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
            has_channel: false,
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            uri: None,
            url: None,
            extra_mentions: Vec::new(),
            extra_tags: Vec::new(),
            quote: None,
        }
    }
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

pub trait ReactionStore: Send + Sync {
    fn react(
        &self,
        user_id: &str,
        note_id: &str,
        reaction: &str,
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
}

#[derive(Debug, Default, Clone)]
pub struct NoopStreamingSink;

impl StreamingSink for NoopStreamingSink {
    fn publish(&self, _event_type: &str, _payload: &[u8]) -> Result<(), PortError> {
        Ok(())
    }
}
