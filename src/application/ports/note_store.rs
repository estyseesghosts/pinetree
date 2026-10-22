use super::job_queue::NewJob;
use super::media_store::InsertMediaInput;
use crate::domain::draft::NoteDraft;
use crate::domain::id::PortError;
use crate::domain::media::MediaMeta;
use crate::domain::note::{Note, NoteEdit, ReactionAcceptance, Visibility};
use crate::domain::pinning::UserNotePining;
use crate::domain::poll::PollInput;
use crate::domain::quote::{QuoteAuthorizationState, QuoteReference, QuoteStamp};
use serde::{Deserialize, Serialize};

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
