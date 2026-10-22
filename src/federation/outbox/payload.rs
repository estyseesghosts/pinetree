//! Outbound delivery payload (moved from `src/federation/outbox.rs`).
//! Durable outbound ActivityPub delivery payloads and job execution
//! (Phase 2 Slice D3; Phase 2.6 Slice 10 delivery reliability).
//!
//! State mutations commit a compact [`FederateDeliverPayload`] inside a
//! [`JobKind::FederateDeliver`] job in the same storage transaction as the
//! domain state change. The background worker resolves destination inboxes
//! (preferring `shared_inbox` when available and deduplicating per inbox URL)
//! and performs signed HTTP POSTs with retry/backoff.
//!
//! Fan-out: a multi-recipient activity job never retries as a whole. Its
//! recipients are walked page by page; small fan-outs are delivered inline and
//! each failed inbox becomes a one-inbox job (`inbox_override`); large fan-outs
//! become one-inbox jobs without any inline send.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::application::ports::{JobKind, NewJob, PortError};

/// Followers are read this many at a time. The walk continues until a short
/// page, so no follower is ever silently truncated.
pub const FOLLOWER_PAGE_SIZE: usize = 1_000;

/// Fan-outs with more destination inboxes than this are not delivered inline:
/// each inbox becomes its own `Federate` job.
pub const FANOUT_INLINE_MAX: usize = 100;

/// Compact, structured payload stored inside a `JobKind::FederateDeliver` job
/// (always well under the 64 KiB job payload cap).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FederateDeliverPayload {
    /// Deliver a local note (`Create(Note)` or `Announce`).
    /// `note_id` may be `"__PENDING_NOTE_ID__"` at enqueue time and is
    /// substituted with the committed note ID inside the storage transaction.
    NoteCreated {
        note_id: String,
        author_id: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Delete(Tombstone)` for a deleted local note, or `Undo(Announce)`
    /// if the deleted note was a pure renote.
    NoteDeleted {
        note_id: String,
        author_id: String,
        note_uri: String,
        was_announce_of_uri: Option<String>,
        inbox_override: Option<String>,
    },
    /// Deliver `Follow` from a local follower to a remote followee's inbox.
    Follow {
        follow_id: String,
        follower_id: String,
        followee_id: String,
        inbox_url: String,
    },
    /// Deliver `Undo(Follow)` from a local follower to a remote followee's inbox.
    Unfollow {
        undo_id: String,
        follow_id: String,
        follower_id: String,
        followee_id: String,
        inbox_url: String,
    },
    /// Deliver `Accept(Follow)` or `Reject(Follow)` to a remote follower's inbox.
    FollowResponse {
        response_id: String,
        accept: bool,
        actor_id: String,
        inbox_url: String,
        follow_activity: Value,
    },
    /// Deliver `Block` from a local blocker to a remote blockee's inbox (§0A).
    Block {
        block_id: String,
        blocker_id: String,
        blockee_id: String,
        inbox_url: String,
    },
    /// Deliver `Undo(Block)` from a local blocker to a remote blockee's inbox.
    UndoBlock {
        undo_id: String,
        block_id: String,
        blocker_id: String,
        blockee_id: String,
        inbox_url: String,
    },
    /// Deliver `Like` (with `_misskey_reaction`) to a remote note author's inbox.
    Like {
        reaction_id: String,
        actor_id: String,
        target_note_uri: String,
        reaction: String,
        inbox_url: String,
    },
    /// Deliver `Undo(Like)` to a remote note author's inbox.
    UndoLike {
        undo_id: String,
        reaction_id: String,
        actor_id: String,
        target_note_uri: String,
        reaction: String,
        inbox_url: String,
    },
    /// Deliver a `Create` vote activity to a remote poll owner's inbox
    /// (§4; Misskey `ApRendererService.renderVote` parity).
    PollVote {
        vote_id: String,
        voter_id: String,
        target_note_uri: String,
        choice_name: String,
        inbox_url: String,
    },
    /// Deliver `Update(Question)` with fresh tallies to followers + relays
    /// (§4; Misskey `PollService.deliverQuestionUpdate` parity).
    /// `note_id` may be `"__PENDING_NOTE_ID__"` at enqueue time.
    QuestionUpdated {
        note_id: String,
        author_id: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Update(Person)` to remote follower inboxes.
    ActorUpdated {
        update_id: String,
        account_id: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Move(old → new)` to the old account's remote follower
    /// inboxes (Batch P `i/move`; Misskey `moveFromLocal` parity: the
    /// `Move` goes to old followers; the `Update(Person)` carrying
    /// `movedTo` is a separate `ActorUpdated` job). `activity_id` is the
    /// `{url}/moves/{src}/{dst}` id, minted once at enqueue so replays
    /// re-deliver the SAME id and receivers dedupe.
    MoveDelivered {
        activity_id: String,
        account_id: String,
        new_actor_uri: String,
        new_account_id: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Update(Note)` after a local `notes/edit` (§8; Sharkey
    /// `postNoteEdited` AP-deliver parity). `note_id` is the edited note
    /// (always known — no `__PENDING__` substitution needed).
    NoteUpdated {
        note_id: String,
        author_id: String,
        inbox_override: Option<String>,
    },
    /// Deliver a FEP-044f `QuoteRequest` to the quoted author's inbox.
    /// `quote_note_id` may be `"__PENDING_NOTE_ID__"` at enqueue time.
    QuoteRequest {
        request_id: String,
        quote_note_id: String,
        quoter_account_id: String,
        quoted_uri: String,
        target_actor_uri: String,
        inbox_url: String,
    },
    /// Deliver a FEP-044f `Accept` (with stamp `result`) or `Reject`
    /// answering a `QuoteRequest` to the quoter's inbox.
    QuoteResponse {
        response_id: String,
        accept: bool,
        decider_account_id: String,
        request_activity_uri: String,
        stamp_uri: Option<String>,
        quoter_actor_uri: String,
        inbox_url: String,
    },
    /// Deliver a FEP-044f stamp revocation (`Delete` of the
    /// `QuoteAuthorization`) to the quoting note's audience.
    QuoteStampDeleted {
        delete_id: String,
        actor_id: String,
        stamp_uri: String,
        quote_note_id: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Add`/`Remove` of a pinned note to/from the actor's
    /// featured collection (`{actor}/collections/featured`) to remote
    /// follower inboxes (pinning §7; Misskey
    /// `NotePiningService.deliverPinnedChange` parity, minus relays:
    /// pinetree has no relay infra, so delivery covers followers only).
    FeaturedChanged {
        actor_id: String,
        note_id: String,
        added: bool,
        inbox_override: Option<String>,
    },
    /// Deliver `Add`/`Remove` of a note to/from a federated clip
    /// collection (`/clips/{id}`) to the note author's inbox (promptness
    /// for membership sync; full-state pulls converge). `activity_id` is
    /// the bare id segment, minted at enqueue so replays re-deliver the
    /// SAME id and receivers dedupe.
    ClipMembershipChanged {
        activity_id: String,
        actor_id: String,
        clip_id: String,
        note_uri: String,
        added: bool,
        inbox_url: String,
    },
    /// Deliver `Create(Article)` for a new federated page to the author's
    /// remote follower inboxes.
    PageCreated {
        page_id: String,
        author_id: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Update(Article)` for an edited federated page to the
    /// author's remote follower inboxes (existing `Update` delivery path).
    PageUpdated {
        page_id: String,
        author_id: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Like` targeting a page URI to the origin's inbox (likes
    /// on materialized remote pages; co-located likes need no delivery).
    /// `activity_id` is the bare id segment, minted at enqueue.
    PageLiked {
        activity_id: String,
        actor_id: String,
        actor_uri: String,
        page_uri: String,
        inbox_url: String,
    },
    /// Deliver `Update(OrderedCollection)` for clip metadata edits
    /// (embedded full collection document, one round trip). Authority =
    /// owner; receivers refresh-or-evict; replays idempotent.
    ClipUpdated {
        clip_id: String,
        owner_id: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Delete(OrderedCollection)` on clip delete and on tier
    /// flips to a more restrictive tier. Receivers evict + prune follows.
    ClipDeleted {
        clip_id: String,
        owner_id: String,
        clip_uri: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Delete(Article)` on local page delete (same recipients as
    /// Create/Update). Inbound side already retracts.
    PageDeleted {
        page_id: String,
        author_id: String,
        page_uri: String,
        inbox_override: Option<String>,
    },
    /// Deliver `Delete(Person)` (suspend) or `Undo(Delete(Person))`
    /// (unsuspend) for a local account to every knowable shared inbox
    /// (Batch M WP3; upstream `UserSuspendService.postSuspend`/
    /// `postUnsuspend`, "知り得る全SharedInboxにDelete配信"). One sweep job
    /// per action commits in the suspend transaction and the worker
    /// expands it at run time: the inbox set is resolved from
    /// [`collect_known_shared_inboxes`] and fanned out through [`fan_out`]
    /// (inline ≤ [`FANOUT_INLINE_MAX`], one-inbox jobs beyond). `activity_id`
    /// is minted at enqueue time, so a re-run of the sweep re-delivers the
    /// SAME activity ids and receivers can dedupe.
    SuspendBroadcast {
        account_id: String,
        activity_id: String,
        /// `false` = `Delete(Person)` (suspend), `true` =
        /// `Undo(Delete(Person))` (unsuspend).
        undo: bool,
        inbox_override: Option<String>,
    },
    /// Deliver a `Flag` activity to a remote instance (Batch M WP4).
    Flag {
        flag_id: String,
        actor_id: String,
        target_account_id: String,
        content: String,
        inbox_url: String,
    },
}

impl FederateDeliverPayload {
    pub fn into_job(
        &self,
        idempotency_key: impl Into<String>,
        not_before: i64,
    ) -> Result<NewJob, PortError> {
        NewJob::new_json(JobKind::FederateDeliver, self, idempotency_key, not_before)
    }
}
