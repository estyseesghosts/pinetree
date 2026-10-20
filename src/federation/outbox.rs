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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::activitypub::objects::{
    render_accept_follow_activity, render_block_activity, render_create_or_announce_for_note,
    render_delete_note_activity, render_delete_stamp_activity, render_follow_activity,
    render_like_activity_for, render_note_audience, render_note_object, render_note_update_activity,
    render_quote_request_activity, render_quote_response_activity, render_reject_follow_activity,
    render_undo_activity, render_update_actor_activity, PUBLIC_COLLECTION_URI,
};
use crate::application::commands::AppServices;
use crate::application::ports::{JobKind, NewJob, PortError};
use crate::domain::account::Account;
use crate::domain::note::Visibility;
use crate::domain::policy::is_eligible_for_federation_delivery;
use crate::federation::delivery::{SafeHttpClient, signed_post_activity};
use crate::federation::host_caps::{fetch_nodeinfo_software, host_of_url, resolve_emoji_capable};
use crate::federation::retry::{
    DESTINATION_BACKOFF_BASE_MS, DeliveryOutcome, DestinationTracker, classify_inbox_status,
};

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

/// Prefer a remote account's `shared_inbox` when non-empty, falling back to `inbox`.
pub fn preferred_inbox(account: &Account) -> Option<String> {
    if account.is_local() {
        return None;
    }
    account
        .shared_inbox
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            account
                .inbox
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
        })
}

/// Walk a cursor-paged follower listing to its end. `fetch(cursor, limit)`
/// returns one page that starts strictly after `cursor` (`None` = the first
/// page). A short page ends the walk, so every follower is visited exactly once.
pub fn collect_follower_ids<F>(page_size: usize, mut fetch: F) -> Result<Vec<String>, PortError>
where
    F: FnMut(Option<&str>, usize) -> Result<Vec<String>, PortError>,
{
    let mut all = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = fetch(cursor.as_deref(), page_size)?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        let full = page.len() >= page_size;
        all.extend(page);
        if !full {
            break;
        }
        cursor = Some(last);
    }
    Ok(all)
}

/// Distinct remote shared inboxes the instance knows (Batch M WP3
/// `Delete(Person)` sweep). Upstream gathers these from the `followings`
/// rows' `followerSharedInbox`/`followeeSharedInbox` snapshots
/// (`postSuspend`/`postUnsuspend`); pinetree reads them from the cached
/// remote accounts (superset of upstream's set — a host known only through
/// a mention still holds a cached actor). Walked in bounded pages and
/// deduplicated here; the walk ends only at the table end.
pub fn collect_known_shared_inboxes(services: &AppServices) -> Result<Vec<String>, PortError> {
    const SCAN_PAGE: usize = 1_000;
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) = services
            .accounts
            .remote_shared_inboxes(cursor.as_deref(), SCAN_PAGE)?;
        for inbox in page {
            if seen.insert(inbox.clone()) {
                out.push(inbox);
            }
        }
        match next {
            None => break,
            Some(after) => cursor = Some(after),
        }
    }
    Ok(out)
}

/// Collect deduplicated remote inbox URLs for an author's followers plus any
/// explicitly mentioned / visible / replied-to / renoted remote accounts.
pub fn collect_recipient_inboxes_for_author(
    services: &AppServices,
    author_id: &str,
    extra_account_ids: &[String],
    include_followers: bool,
) -> Result<Vec<String>, PortError> {
    let mut seen = HashSet::new();
    let mut inboxes = Vec::new();
    let mut add_account = |acc: &Account| {
        if let Some(inbox) = preferred_inbox(acc) {
            if seen.insert(inbox.clone()) {
                inboxes.push(inbox);
            }
        }
    };

    if include_followers {
        let follower_ids = collect_follower_ids(FOLLOWER_PAGE_SIZE, |cursor, limit| {
            services
                .follows
                .followers_of(author_id, cursor, limit)
                .map(|page| {
                    page.into_iter()
                        .map(|f| f.follower_id)
                        .collect::<Vec<String>>()
                })
        })?;
        for follower_id in follower_ids {
            if let Some(acc) = services.accounts.find_by_id(&follower_id)? {
                add_account(&acc);
            }
        }
    }

    for uid in extra_account_ids {
        if let Some(acc) = services.accounts.find_by_id(uid)? {
            add_account(&acc);
        }
    }

    Ok(inboxes)
}

/// Account ids that interacted with `note` (§8 edit fan-out: Sharkey
/// `postNoteEdited` delivers the `Update` to remote users who replied to,
/// renoted, or reacted to the note, plus the reply/renote authors). Reads
/// only; the caller pages deliveries through the standard split, and remote
/// filtering happens in `collect_recipient_inboxes_for_author`.
fn collect_note_interacted_ids(
    services: &AppServices,
    note: &crate::domain::note::Note,
) -> Result<Vec<String>, PortError> {
    use crate::application::ports::TimelineCursor;
    let mut ids: Vec<String> = Vec::new();
    let mut push = |id: &str| {
        if id != note.author_id.as_str() && !ids.iter().any(|known| known == id) {
            ids.push(id.to_string());
        }
    };
    if let Some(uid) = note.reply_user_id.as_deref() {
        push(uid);
    }
    if let Some(rid) = note.renote_id.as_deref() {
        if let Some(target) = services.notes.get_note(rid)? {
            push(target.author_id.as_str());
        }
    }
    // Repliers: newest-first pages until exhausted.
    let mut cursor = TimelineCursor {
        limit: 100,
        ..Default::default()
    };
    loop {
        let batch = services.notes.notes_by_reply(&note.id, &cursor)?;
        if batch.is_empty() {
            break;
        }
        for reply in &batch {
            push(reply.author_id.as_str());
        }
        if batch.len() < 100 {
            break;
        }
        cursor.until_id = batch.last().map(|n| n.id.clone());
    }
    // Renoters: bounded scan (same 10k bound as the `notes/renotes` read).
    for candidate in services
        .notes
        .list_all_notes(&TimelineCursor::with_limit(10_000))?
    {
        if candidate.renote_id.as_deref() == Some(note.id.as_str()) {
            push(candidate.author_id.as_str());
        }
    }
    // Reactors.
    for reaction in services.reactions.reactions_for_note(&note.id)? {
        push(reaction.user_id.as_str());
    }
    Ok(ids)
}

/// Whether a fan-out with `inbox_count` destinations is split into per-inbox
/// jobs instead of being delivered inline.
pub fn fanout_is_split(inbox_count: usize) -> bool {
    inbox_count > FANOUT_INLINE_MAX
}

/// Execute one [`FederateDeliverPayload`] job. `job_key` is the job's
/// idempotency key; per-inbox follow-up jobs derive their keys from it.
///
/// A payload with `inbox_override == Some(inbox)` is a single delivery and
/// returns that send's outcome directly (the worker applies job-level retry).
/// A fan-out payload (`inbox_override == None`) resolves the deduplicated
/// inbox list and returns [`DeliveryOutcome::Split`] when any inbox needs a
/// later attempt; see [`fan_out`].
pub fn execute_federate_deliver(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    payload_bytes: &[u8],
) -> Result<DeliveryOutcome, PortError> {
    // Malformed payloads are caller faults, not delivery results: report a
    // parse failure so the worker quarantines the job with a reason instead
    // of retrying (or silently dropping) it.
    let parsed: FederateDeliverPayload = serde_json::from_slice(payload_bytes)
        .map_err(|e| PortError::Validation(format!("federate payload invalid: {e}")))?;

    let base = services.public_url.trim_end_matches('/');

    match parsed {
        FederateDeliverPayload::NoteCreated {
            note_id,
            author_id,
            inbox_override,
        } => {
            let Some(note) = services.notes.get_note(&note_id)? else {
                crate::fed_debug!("out skip note={note_id:?} reason=note-missing");
                return Ok(DeliveryOutcome::Delivered);
            };
            if !is_eligible_for_federation_delivery(&note) {
                crate::fed_debug!("out skip note={note_id:?} reason=not-eligible");
                return Ok(DeliveryOutcome::Delivered);
            }
            let activity = render_create_or_announce_for_note(services, &note)?;
            match inbox_override {
                Some(inbox) => {
                    deliver_single(services, client, tracker, &author_id, &inbox, &activity)
                }
                None => {
                    let mut extras = note.mentions.clone();
                    for uid in &note.visible_user_ids {
                        if !extras.contains(uid) {
                            extras.push(uid.clone());
                        }
                    }
                    if let Some(ruid) = &note.reply_user_id {
                        if !extras.contains(ruid) {
                            extras.push(ruid.clone());
                        }
                    }
                    if let Some(renote_id) = &note.renote_id {
                        if let Some(target) = services.notes.get_note(renote_id)? {
                            if !extras.contains(&target.author_id) {
                                extras.push(target.author_id);
                            }
                        }
                    }
                    let include_followers = note.visibility != Visibility::Specified;
                    let inboxes = collect_recipient_inboxes_for_author(
                        services,
                        &author_id,
                        &extras,
                        include_followers,
                    )?;
                    fan_out(
                        services,
                        client,
                        tracker,
                        job_key,
                        &author_id,
                        &inboxes,
                        &activity,
                        &|inbox: &str| FederateDeliverPayload::NoteCreated {
                            note_id: note_id.clone(),
                            author_id: author_id.clone(),
                            inbox_override: Some(inbox.to_string()),
                        },
                    )
                }
            }
        }

        FederateDeliverPayload::NoteDeleted {
            note_id,
            author_id,
            note_uri,
            was_announce_of_uri,
            inbox_override,
        } => {
            let author = match services.accounts.find_by_id(&author_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let actor_uri = author.actor_url(base);
            // Keep a copy for the per-inbox payloads; the activity below
            // consumes the original.
            let announce_of_for_jobs = was_announce_of_uri.clone();
            let activity = if let Some(target_uri) = was_announce_of_uri {
                let inner = serde_json::json!({
                    "id": format!("{base}/notes/{note_id}/activity"),
                    "type": "Announce",
                    "actor": actor_uri,
                    "object": target_uri,
                });
                render_undo_activity(base, &note_id, &actor_uri, &inner, None, None)
            } else {
                render_delete_note_activity(base, &note_id, &actor_uri, &note_uri)
            };
            match inbox_override {
                Some(inbox) => {
                    deliver_single(services, client, tracker, &author_id, &inbox, &activity)
                }
                None => {
                    let inboxes =
                        collect_recipient_inboxes_for_author(services, &author_id, &[], true)?;
                    fan_out(
                        services,
                        client,
                        tracker,
                        job_key,
                        &author_id,
                        &inboxes,
                        &activity,
                        &|inbox: &str| FederateDeliverPayload::NoteDeleted {
                            note_id: note_id.clone(),
                            author_id: author_id.clone(),
                            note_uri: note_uri.clone(),
                            was_announce_of_uri: announce_of_for_jobs.clone(),
                            inbox_override: Some(inbox.to_string()),
                        },
                    )
                }
            }
        }

        FederateDeliverPayload::Follow {
            follow_id,
            follower_id,
            followee_id,
            inbox_url,
        } => {
            let follower = match services.accounts.find_by_id(&follower_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let followee = match services.accounts.find_by_id(&followee_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let activity = render_follow_activity(
                base,
                &follow_id,
                &follower.actor_url(base),
                &followee.actor_url(base),
            );
            deliver_single(services, client, tracker, &follower_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::Unfollow {
            undo_id,
            follow_id,
            follower_id,
            followee_id,
            inbox_url,
        } => {
            let follower = match services.accounts.find_by_id(&follower_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let followee = match services.accounts.find_by_id(&followee_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let inner = render_follow_activity(
                base,
                &follow_id,
                &follower.actor_url(base),
                &followee.actor_url(base),
            );
            let activity = render_undo_activity(
                base,
                &undo_id,
                &follower.actor_url(base),
                &inner,
                None,
                None,
            );
            deliver_single(services, client, tracker, &follower_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::FollowResponse {
            response_id,
            accept,
            actor_id,
            inbox_url,
            follow_activity,
        } => {
            let actor = match services.accounts.find_by_id(&actor_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let actor_uri = actor.actor_url(base);
            let activity = if accept {
                render_accept_follow_activity(base, &response_id, &actor_uri, &follow_activity)
            } else {
                render_reject_follow_activity(base, &response_id, &actor_uri, &follow_activity)
            };
            deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::Block {
            block_id,
            blocker_id,
            blockee_id,
            inbox_url,
        } => {
            let (Some(blocker), Some(blockee)) = (
                services.accounts.find_by_id(&blocker_id)?,
                services.accounts.find_by_id(&blockee_id)?,
            ) else {
                return Ok(DeliveryOutcome::Delivered);
            };
            if !blocker.is_local() {
                return Ok(DeliveryOutcome::Delivered);
            }
            let activity = render_block_activity(
                base,
                &block_id,
                &blocker.actor_url(base),
                &blockee.actor_url(base),
            );
            deliver_single(services, client, tracker, &blocker_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::UndoBlock {
            undo_id,
            block_id,
            blocker_id,
            blockee_id,
            inbox_url,
        } => {
            let (Some(blocker), Some(blockee)) = (
                services.accounts.find_by_id(&blocker_id)?,
                services.accounts.find_by_id(&blockee_id)?,
            ) else {
                return Ok(DeliveryOutcome::Delivered);
            };
            if !blocker.is_local() {
                return Ok(DeliveryOutcome::Delivered);
            }
            let blocker_uri = blocker.actor_url(base);
            let inner = render_block_activity(
                base,
                &block_id,
                &blocker_uri,
                &blockee.actor_url(base),
            );
            let activity =
                render_undo_activity(base, &undo_id, &blocker_uri, &inner, None, None);
            deliver_single(services, client, tracker, &blocker_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::Like {
            reaction_id,
            actor_id,
            target_note_uri,
            reaction,
            inbox_url,
        } => {
            let actor = match services.accounts.find_by_id(&actor_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            // §6 federation split (decided): the NodeInfo probe runs here,
            // off-transaction in the delivery worker — never inside a state
            // transaction. Unknown/failed hosts read as emoji-aware.
            let emoji_capable = match host_of_url(&inbox_url) {
                Some(host) => resolve_emoji_capable(services, &host, services.clock.now_ms(), &|h| {
                    fetch_nodeinfo_software(client, h)
                }),
                None => true,
            };
            let activity = render_like_activity_for(
                base,
                &reaction_id,
                &actor.actor_url(base),
                &target_note_uri,
                &reaction,
                emoji_capable,
            );
            deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::UndoLike {
            undo_id,
            reaction_id,
            actor_id,
            target_note_uri,
            reaction,
            inbox_url,
        } => {
            let actor = match services.accounts.find_by_id(&actor_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let actor_uri = actor.actor_url(base);
            // Same per-host decision as `Like` so the undone activity matches
            // the form originally delivered (capability is TTL-stable).
            let emoji_capable = match host_of_url(&inbox_url) {
                Some(host) => resolve_emoji_capable(services, &host, services.clock.now_ms(), &|h| {
                    fetch_nodeinfo_software(client, h)
                }),
                None => true,
            };
            let inner = render_like_activity_for(
                base,
                &reaction_id,
                &actor_uri,
                &target_note_uri,
                &reaction,
                emoji_capable,
            );
            let activity =
                render_undo_activity(base, &undo_id, &actor_uri, &inner, None, None);
            deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::PollVote {
            vote_id,
            voter_id,
            target_note_uri,
            choice_name,
            inbox_url,
        } => {
            let voter = match services.accounts.find_by_id(&voter_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            // The poll owner is the audience: resolve their actor URI from
            // the cached note (fall back to the addressed inbox host).
            let poll_owner_uri = match services.notes.find_by_uri(&target_note_uri)? {
                Some(note) => match services.accounts.find_by_id(&note.author_id)? {
                    Some(owner) => owner.uri.clone().unwrap_or_else(|| owner.actor_url(base)),
                    None => return Ok(DeliveryOutcome::Delivered),
                },
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let activity = crate::activitypub::objects::render_vote_activity(
                base,
                &vote_id,
                &voter.actor_url(base),
                &poll_owner_uri,
                &target_note_uri,
                &choice_name,
                services.clock.now_ms(),
            );
            deliver_single(services, client, tracker, &voter_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::QuestionUpdated {
            note_id,
            author_id,
            inbox_override,
        } => {
            let author = match services.accounts.find_by_id(&author_id)? {
                Some(a) if a.is_local() => a,
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            let Some(note) = services.notes.get_note(&note_id)? else {
                return Ok(DeliveryOutcome::Delivered);
            };
            if note.is_deleted || note.local_only {
                return Ok(DeliveryOutcome::Delivered);
            }
            let Some(poll) = services.polls.get_poll(&note_id)? else {
                return Ok(DeliveryOutcome::Delivered);
            };
            let author_uri = author.actor_url(base);
            let question = crate::activitypub::objects::render_question(
                base,
                &author_uri,
                &note.id,
                note.text.as_deref(),
                &poll,
                services.clock.now_ms(),
            );
            let update_id = format!("updates/{}", services.ids.generate());
            let to = match note.visibility {
                Visibility::Public => vec![PUBLIC_COLLECTION_URI.to_string()],
                _ => vec![format!("{author_uri}/followers")],
            };
            let activity = crate::activitypub::objects::render_question_update_activity(
                base,
                &update_id,
                &author_uri,
                question,
                to,
                services.clock.now_ms(),
            );
            match inbox_override {
                Some(inbox) => {
                    deliver_single(services, client, tracker, &author_id, &inbox, &activity)
                }
                None => {
                    let inboxes =
                        collect_recipient_inboxes_for_author(services, &author_id, &[], true)?;
                    fan_out(
                        services,
                        client,
                        tracker,
                        job_key,
                        &author_id,
                        &inboxes,
                        &activity,
                        &|inbox: &str| FederateDeliverPayload::QuestionUpdated {
                            note_id: note_id.clone(),
                            author_id: author_id.clone(),
                            inbox_override: Some(inbox.to_string()),
                        },
                    )
                }
            }
        }

        FederateDeliverPayload::ActorUpdated {
            update_id,
            account_id,
            inbox_override,
        } => {
            let account = match services.accounts.find_by_id(&account_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let activity = render_update_actor_activity(services, &update_id, &account)?;
            match inbox_override {
                Some(inbox) => {
                    deliver_single(services, client, tracker, &account_id, &inbox, &activity)
                }
                None => {
                    let inboxes =
                        collect_recipient_inboxes_for_author(services, &account_id, &[], true)?;
                    fan_out(
                        services,
                        client,
                        tracker,
                        job_key,
                        &account_id,
                        &inboxes,
                        &activity,
                        &|inbox: &str| FederateDeliverPayload::ActorUpdated {
                            update_id: update_id.clone(),
                            account_id: account_id.clone(),
                            inbox_override: Some(inbox.to_string()),
                        },
                    )
                }
            }
        }

        FederateDeliverPayload::MoveDelivered {
            activity_id,
            account_id,
            new_actor_uri,
            new_account_id,
            inbox_override,
        } => {
            let account = match services.accounts.find_by_id(&account_id)? {
                Some(a) => a,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let activity = crate::activitypub::objects::render_move_activity(
                base,
                &account_id,
                &account.actor_url(base),
                &new_actor_uri,
                &new_account_id,
            );
            // The rendered id embeds src/dst; stamp the minted activity id
            // so replays carry the same id receivers dedupe on.
            let mut activity = activity;
            activity["id"] = Value::String(activity_id.clone());
            match inbox_override {
                Some(inbox) => {
                    deliver_single(services, client, tracker, &account_id, &inbox, &activity)
                }
                None => {
                    let inboxes =
                        collect_recipient_inboxes_for_author(services, &account_id, &[], true)?;
                    fan_out(
                        services,
                        client,
                        tracker,
                        job_key,
                        &account_id,
                        &inboxes,
                        &activity,
                        &|inbox: &str| FederateDeliverPayload::MoveDelivered {
                            activity_id: activity_id.clone(),
                            account_id: account_id.clone(),
                            new_actor_uri: new_actor_uri.clone(),
                            new_account_id: new_account_id.clone(),
                            inbox_override: Some(inbox.to_string()),
                        },
                    )
                }
            }
        }

        FederateDeliverPayload::NoteUpdated {
            note_id,
            author_id,
            inbox_override,
        } => {            let author = match services.accounts.find_by_id(&author_id)? {
                Some(a) if a.is_local() => a,
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            let Some(note) = services.notes.get_note(&note_id)? else {
                return Ok(DeliveryOutcome::Delivered);
            };
            if note.author_id != author_id || note.is_deleted {
                return Ok(DeliveryOutcome::Delivered);
            }
            if !is_eligible_for_federation_delivery(&note) {
                return Ok(DeliveryOutcome::Delivered);
            }
            let author_uri = author.actor_url(base);
            let object = render_note_object(services, &note, true)?;
            let (to, cc) = render_note_audience(services, &author, &note);
            let update_id = format!("updates/{}", services.ids.generate());
            let activity = render_note_update_activity(
                base,
                &update_id,
                &author_uri,
                object,
                to,
                cc,
                services.clock.now_ms(),
            );
            match inbox_override {
                Some(inbox) => {
                    deliver_single(services, client, tracker, &author_id, &inbox, &activity)
                }
                None => {
                    let mut extras = note.mentions.clone();
                    for uid in &note.visible_user_ids {
                        if !extras.contains(uid) {
                            extras.push(uid.clone());
                        }
                    }
                    for uid in collect_note_interacted_ids(services, &note)? {
                        if !extras.contains(&uid) {
                            extras.push(uid);
                        }
                    }
                    let include_followers = note.visibility != Visibility::Specified;
                    let inboxes = collect_recipient_inboxes_for_author(
                        services,
                        &author_id,
                        &extras,
                        include_followers,
                    )?;
                    fan_out(
                        services,
                        client,
                        tracker,
                        job_key,
                        &author_id,
                        &inboxes,
                        &activity,
                        &|inbox: &str| FederateDeliverPayload::NoteUpdated {
                            note_id: note_id.clone(),
                            author_id: author_id.clone(),
                            inbox_override: Some(inbox.to_string()),
                        },
                    )
                }
            }
        }

        FederateDeliverPayload::QuoteRequest {
            request_id,
            quote_note_id,
            quoter_account_id,
            quoted_uri,
            target_actor_uri,
            inbox_url,
        } => {
            let quoter = match services.accounts.find_by_id(&quoter_account_id)? {
                Some(a) if a.is_local() => a,
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            let Some(note) = services.notes.get_note(&quote_note_id)? else {
                return Ok(DeliveryOutcome::Delivered);
            };
            if note.author_id != quoter_account_id || !note.is_quote() {
                return Ok(DeliveryOutcome::Delivered);
            }
            let inline = render_note_object(services, &note, true)?;
            let activity = render_quote_request_activity(
                base,
                &request_id,
                &quoter.actor_url(base),
                &quoted_uri,
                &target_actor_uri,
                inline,
            );
            deliver_single(
                services,
                client,
                tracker,
                &quoter_account_id,
                &inbox_url,
                &activity,
            )
        }

        FederateDeliverPayload::QuoteResponse {
            response_id,
            accept,
            decider_account_id,
            request_activity_uri,
            stamp_uri,
            quoter_actor_uri,
            inbox_url,
        } => {
            let decider = match services.accounts.find_by_id(&decider_account_id)? {
                Some(a) if a.is_local() => a,
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            let activity = render_quote_response_activity(
                base,
                &response_id,
                accept,
                &decider.actor_url(base),
                &request_activity_uri,
                stamp_uri.as_deref(),
                &quoter_actor_uri,
            );
            deliver_single(
                services,
                client,
                tracker,
                &decider_account_id,
                &inbox_url,
                &activity,
            )
        }

        FederateDeliverPayload::QuoteStampDeleted {
            delete_id,
            actor_id,
            stamp_uri,
            quote_note_id,
            inbox_override,
        } => {
            let actor = match services.accounts.find_by_id(&actor_id)? {
                Some(a) if a.is_local() => a,
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            let actor_uri = actor.actor_url(base);
            // Fan out to the quoting note's audience (everyone who may have
            // seen the Create carrying the stamp).
            let extras = match services.notes.get_note(&quote_note_id)? {
                Some(note) => quote_note_extras(services, &note)?,
                None => Vec::new(),
            };
            let to_public = vec![PUBLIC_COLLECTION_URI.to_string()];
            let activity = render_delete_stamp_activity(
                base,
                &delete_id,
                &actor_uri,
                &stamp_uri,
                to_public,
            );
            match inbox_override {
                Some(inbox) => {
                    deliver_single(services, client, tracker, &actor_id, &inbox, &activity)
                }
                None => {
                    let inboxes = collect_recipient_inboxes_for_author(
                        services,
                        &actor_id,
                        &extras,
                        true,
                    )?;
                    fan_out(
                        services,
                        client,
                        tracker,
                        job_key,
                        &actor_id,
                        &inboxes,
                        &activity,
                        &|inbox: &str| FederateDeliverPayload::QuoteStampDeleted {
                            delete_id: delete_id.clone(),
                            actor_id: actor_id.clone(),
                            stamp_uri: stamp_uri.clone(),
                            quote_note_id: quote_note_id.clone(),
                            inbox_override: Some(inbox.to_string()),
                        },
                    )
                }
            }
        }

        FederateDeliverPayload::FeaturedChanged {
            actor_id,
            note_id,
            added,
            inbox_override,
        } => {
            let actor = match services.accounts.find_by_id(&actor_id)? {
                Some(a) if a.is_local() => a,
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            let Some(note) = services.notes.get_note(&note_id)? else {
                return Ok(DeliveryOutcome::Delivered);
            };
            if note.author_id != actor_id || note.is_deleted {
                return Ok(DeliveryOutcome::Delivered);
            }
            let actor_uri = actor.actor_url(base);
            let note_uri = note.uri.clone().unwrap_or_else(|| format!("{base}/notes/{}", note.id));
            let change_id = format!("featured-{}-{note_id}", if added { "add" } else { "remove" });
            let activity = crate::activitypub::objects::render_featured_change_activity(
                base,
                &change_id,
                &actor_uri,
                &note_uri,
                added,
            );
            match inbox_override {
                Some(inbox) => {
                    deliver_single(services, client, tracker, &actor_id, &inbox, &activity)
                }
                None => {
                    let inboxes =
                        collect_recipient_inboxes_for_author(services, &actor_id, &[], true)?;
                    fan_out(
                        services,
                        client,
                        tracker,
                        job_key,
                        &actor_id,
                        &inboxes,
                        &activity,
                        &|inbox: &str| FederateDeliverPayload::FeaturedChanged {
                            actor_id: actor_id.clone(),
                            note_id: note_id.clone(),
                            added,
                            inbox_override: Some(inbox.to_string()),
                        },
                    )
                }
            }
        }

        FederateDeliverPayload::ClipMembershipChanged {
            activity_id,
            actor_id,
            clip_id,
            note_uri,
            added,
            inbox_url,
        } => {
            use crate::activitypub::federated_collections::{
                clip_collection_uri, federation_switch_on, render_clip_membership_activity,
            };
            let actor = match services.accounts.find_by_id(&actor_id)? {
                Some(a) if a.is_local() => a,
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            let clip = match services.clips.get_clip(&clip_id)? {
                Some(c) if c.owner_id == actor_id => c,
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            // Stale job (clip unfederated after enqueue): skip quietly —
            // the collection fetch gate enforces the same switches.
            if !clip.federate || !federation_switch_on(services, "clips") {
                return Ok(DeliveryOutcome::Delivered);
            }
            let actor_uri = actor.actor_url(base);
            let activity = render_clip_membership_activity(
                base,
                &activity_id,
                &actor_uri,
                &note_uri,
                &clip_collection_uri(base, &clip.id),
                added,
            );
            deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::PageCreated {
            page_id,
            author_id,
            inbox_override,
        } => deliver_page_create_or_update(
            services, client, tracker, job_key, base, &page_id, &author_id, false,
            inbox_override,
        ),

        FederateDeliverPayload::PageUpdated {
            page_id,
            author_id,
            inbox_override,
        } => deliver_page_create_or_update(
            services, client, tracker, job_key, base, &page_id, &author_id, true,
            inbox_override,
        ),

        FederateDeliverPayload::PageLiked {
            activity_id,
            actor_id,
            actor_uri,
            page_uri,
            inbox_url,
        } => {
            use crate::activitypub::federated_collections::render_page_like_activity;
            // Stale job (liker gone) converges quietly.
            if services.accounts.find_by_id(&actor_id)?.is_none() {
                return Ok(DeliveryOutcome::Delivered);
            }
            let activity =
                render_page_like_activity(base, &activity_id, &actor_uri, &page_uri);
            deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
        }

        FederateDeliverPayload::SuspendBroadcast {
            account_id,
            activity_id,
            undo,
            inbox_override,
        } => {
            let actor = match services.accounts.find_by_id(&account_id)? {
                Some(a) if a.is_local() => a,
                // Gone or remote accounts have no Person to withdraw.
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            let actor_uri = actor.actor_url(base);
            let delete = crate::activitypub::objects::render_delete_person_activity(
                base,
                &activity_id,
                &actor_uri,
                services.clock.now_ms(),
            );
            // Upstream `renderUndo(renderDelete(...))` parity: the Undo id
            // hangs off the Delete id (`{id}/undo`, upstream's
            // `renderUndo` id rule).
            let activity = if undo {
                render_undo_activity(
                    base,
                    &format!("{activity_id}/undo"),
                    &actor_uri,
                    &delete,
                    None,
                    None,
                )
            } else {
                delete
            };
            match inbox_override {
                Some(inbox) => {
                    deliver_single(services, client, tracker, &account_id, &inbox, &activity)
                }
                None => {
                    let inboxes = collect_known_shared_inboxes(services)?;
                    fan_out(
                        services,
                        client,
                        tracker,
                        job_key,
                        &account_id,
                        &inboxes,
                        &activity,
                        &|inbox: &str| FederateDeliverPayload::SuspendBroadcast {
                            account_id: account_id.clone(),
                            activity_id: activity_id.clone(),
                            undo,
                            inbox_override: Some(inbox.to_string()),
                        },
                    )
                }
            }
        }

        FederateDeliverPayload::Flag {
            flag_id,
            actor_id,
            target_account_id,
            content,
            inbox_url,
        } => {
            let actor = match services.accounts.find_by_id(&actor_id)? {
                Some(a) if a.is_local() => a,
                _ => return Ok(DeliveryOutcome::Delivered),
            };
            let target = match services.accounts.find_by_id(&target_account_id)? {
                Some(t) => t,
                None => return Ok(DeliveryOutcome::Delivered),
            };
            let actor_uri = actor.actor_url(base);
            let target_uri = target.actor_url(base);
            let activity = crate::activitypub::objects::render_flag_activity(
                base,
                &flag_id,
                &actor_uri,
                &target_uri,
                &content,
            );
            deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
        }
    }
}

/// Extra recipient account ids for quote-stamp revocation fan-out: mirrors
/// the `NoteCreated` audience (mentions, specified users, reply author, and
/// the quoted note's author) so every party that saw the stamp learns of
/// the revocation.
fn quote_note_extras(
    services: &AppServices,
    note: &crate::domain::note::Note,
) -> Result<Vec<String>, PortError> {
    let mut extras = note.mentions.clone();
    for uid in &note.visible_user_ids {
        if !extras.contains(uid) {
            extras.push(uid.clone());
        }
    }
    if let Some(ruid) = &note.reply_user_id {
        if !extras.contains(ruid) {
            extras.push(ruid.clone());
        }
    }
    if let Some(renote_id) = &note.renote_id {
        if let Some(target) = services.notes.get_note(renote_id)? {
            if !extras.contains(&target.author_id) {
                extras.push(target.author_id);
            }
        }
    }
    Ok(extras)
}

/// Deliver one fan-out activity to `inboxes`. The calling job has no
/// `inbox_override`.
///
/// - More than [`FANOUT_INLINE_MAX`] inboxes: nothing is sent here. Every
///   inbox (or its cooldown end, if later) becomes a one-inbox job, returned as
///   [`DeliveryOutcome::Split`].
/// - Otherwise each inbox is sent inline. A destination in cooldown, or one
///   with a transient failure, becomes a one-inbox job scheduled at the
///   cooldown end or the failure's `Retry-After`/backoff time. Permanent
///   failures are logged and dropped.
///
/// The activity job itself is never retried as a whole: once it returns
/// `Split` (or `Delivered`) it completes, so a single slow or failing inbox
/// cannot cause the already-delivered inboxes to be sent again.
#[allow(clippy::too_many_arguments)]
/// Shared `Create(Article)` / `Update(Article)` fan-out for federated
/// pages (mirrors the `NoteUpdated` arm: author must be local, page must
/// still exist and still federate, else the stale job converges quietly).
#[allow(clippy::too_many_arguments)]
fn deliver_page_create_or_update(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    page_id: &str,
    author_id: &str,
    updated: bool,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
    use crate::activitypub::federated_collections::{
        federation_switch_on, render_page_activity, render_page_article,
    };
    let author = match services.accounts.find_by_id(author_id)? {
        Some(a) if a.is_local() => a,
        _ => return Ok(DeliveryOutcome::Delivered),
    };
    let page = match services.pages.get_page(page_id)? {
        Some(p) if p.owner_id == author_id => p,
        _ => return Ok(DeliveryOutcome::Delivered),
    };
    if !page.federate
        || page.tier != crate::domain::collection::CollectionTier::Listed
        || !federation_switch_on(services, "pages")
    {
        return Ok(DeliveryOutcome::Delivered);
    }
    let actor_uri = author.actor_url(base);
    let followers_uri = format!("{actor_uri}/followers");
    let article = render_page_article(services, &page)?;
    let activity_id = format!(
        "page-{}-{page_id}-{}",
        if updated { "update" } else { "create" },
        services.ids.generate()
    );
    let activity = render_page_activity(
        base,
        &activity_id,
        &actor_uri,
        &article,
        updated,
        &followers_uri,
    );
    match inbox_override {
        Some(inbox) => deliver_single(services, client, tracker, author_id, &inbox, &activity),
        None => {
            let inboxes =
                collect_recipient_inboxes_for_author(services, author_id, &[], true)?;
            let constructor = if updated {
                FederateDeliverPayload::PageUpdated {
                    page_id: page_id.to_string(),
                    author_id: author_id.to_string(),
                    inbox_override: None,
                }
            } else {
                FederateDeliverPayload::PageCreated {
                    page_id: page_id.to_string(),
                    author_id: author_id.to_string(),
                    inbox_override: None,
                }
            };
            fan_out(
                services,
                client,
                tracker,
                job_key,
                author_id,
                &inboxes,
                &activity,
                &|inbox: &str| match &constructor {
                    FederateDeliverPayload::PageUpdated { .. } => {
                        FederateDeliverPayload::PageUpdated {
                            page_id: page_id.to_string(),
                            author_id: author_id.to_string(),
                            inbox_override: Some(inbox.to_string()),
                        }
                    }
                    _ => FederateDeliverPayload::PageCreated {
                        page_id: page_id.to_string(),
                        author_id: author_id.to_string(),
                        inbox_override: Some(inbox.to_string()),
                    },
                },
            )
        }
    }
}

fn fan_out(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    sender_account_id: &str,
    inboxes: &[String],
    activity: &Value,
    per_inbox: &dyn Fn(&str) -> FederateDeliverPayload,
) -> Result<DeliveryOutcome, PortError> {
    if inboxes.is_empty() {
        return Ok(DeliveryOutcome::Delivered);
    }
    crate::fed_debug!(
        "out fanout type={} recipients={} split={}",
        activity.get("type").and_then(Value::as_str).unwrap_or("?"),
        inboxes.len(),
        fanout_is_split(inboxes.len())
    );
    let now_ms = services.clock.now_ms();
    if fanout_is_split(inboxes.len()) {
        let mut followups = Vec::with_capacity(inboxes.len());
        for inbox in inboxes {
            let not_before = tracker.cooldown_until(inbox, now_ms).unwrap_or(now_ms);
            followups.push(per_inbox_job(job_key, inbox, not_before, per_inbox)?);
        }
        return Ok(DeliveryOutcome::Split { followups });
    }

    let mut followups = Vec::new();
    for inbox in inboxes {
        match deliver_single(services, client, tracker, sender_account_id, inbox, activity)? {
            DeliveryOutcome::Delivered => {}
            DeliveryOutcome::PermanentFailure(reason) => {
                eprintln!("dropping delivery to {inbox}: {reason}");
            }
            DeliveryOutcome::TransientFailure { retry_after_ms, .. } => {
                let not_before = retry_after_ms.unwrap_or(now_ms);
                followups.push(per_inbox_job(job_key, inbox, not_before, per_inbox)?);
            }
            DeliveryOutcome::Deferred { until_ms } => {
                followups.push(per_inbox_job(job_key, inbox, until_ms, per_inbox)?);
            }
            // `deliver_single` never produces a fan-out.
            DeliveryOutcome::Split { .. } => {}
        }
    }
    if followups.is_empty() {
        Ok(DeliveryOutcome::Delivered)
    } else {
        Ok(DeliveryOutcome::Split { followups })
    }
}

/// One-inbox job for `inbox`, keyed `{job_key}:{inbox}` so a re-run of the
/// activity job derives the same key for the same inbox.
fn per_inbox_job(
    job_key: &str,
    inbox: &str,
    not_before: i64,
    per_inbox: &dyn Fn(&str) -> FederateDeliverPayload,
) -> Result<NewJob, PortError> {
    per_inbox(inbox).into_job(format!("{job_key}:{inbox}"), not_before)
}

/// Send one signed activity to one inbox and classify the result.
///
/// A destination in cooldown is deferred: nothing is sent and the caller
/// decides how to reschedule. The tracker is updated for every real attempt.
fn deliver_single(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    sender_account_id: &str,
    inbox_url: &str,
    activity: &Value,
) -> Result<DeliveryOutcome, PortError> {
    // Instance block (WP5): check at delivery time, not enqueue time. Skip
    // without failing the job so it completes rather than retrying.
    if let Some(host) = crate::federation::host_caps::host_of_url(inbox_url) {
        if crate::domain::instance::host_is_blocked(&services.blocked_hosts(), &host) {
            crate::fed_debug!(
                "out skip inbox={inbox_url:?} reason=blocked-host"
            );
            return Ok(DeliveryOutcome::Delivered);
        }
    }
    let now_ms = services.clock.now_ms();
    let kind = activity.get("type").and_then(Value::as_str).unwrap_or("?");
    if let Some(until_ms) = tracker.cooldown_until(inbox_url, now_ms) {
        crate::fed_debug!(
            "out defer type={kind} inbox={inbox_url:?} until_ms={until_ms} reason=destination-cooldown"
        );
        return Ok(DeliveryOutcome::Deferred { until_ms });
    }
    let body = serde_json::to_vec(activity)
        .map_err(|e| PortError::Validation(format!("serialize activity: {e}")))?;
    let started = std::time::Instant::now();
    let sent = signed_post_activity(services, client, sender_account_id, inbox_url, &body);
    let elapsed_ms = started.elapsed().as_millis();
    match sent {
        Ok(resp) => {
            let reason = String::from_utf8_lossy(&resp.body).into_owned();
            let retry_after = resp.header("retry-after").map(str::to_owned);
            let outcome = classify_inbox_status(
                resp.status,
                reason.trim(),
                retry_after.as_deref(),
                now_ms,
            );
            crate::fed_debug!(
                "out send type={kind} inbox={inbox_url:?} status={} ms={elapsed_ms} outcome={outcome:?}",
                resp.status
            );
            Ok(settle_outcome(tracker, inbox_url, now_ms, outcome))
        }
        Err(err) if err.kind.is_transient() => {
            crate::fed_debug!(
                "out send type={kind} inbox={inbox_url:?} error=transient kind={:?} ms={elapsed_ms} msg={:?}",
                err.kind,
                err.message
            );
            let outcome = DeliveryOutcome::TransientFailure {
                reason: err.message,
                retry_after_ms: None,
            };
            Ok(settle_outcome(tracker, inbox_url, now_ms, outcome))
        }
        Err(err) => {
            crate::fed_debug!(
                "out send type={kind} inbox={inbox_url:?} error=permanent kind={:?} ms={elapsed_ms} msg={:?}",
                err.kind,
                err.message
            );
            Ok(DeliveryOutcome::PermanentFailure(err.message))
        }
    }
}

/// Record a send's result in the destination tracker. A transient failure
/// starts or extends the host cooldown; its `retry_after_ms` becomes the later
/// of the server's `Retry-After` and that cooldown end.
fn settle_outcome(
    tracker: &DestinationTracker,
    inbox_url: &str,
    now_ms: i64,
    outcome: DeliveryOutcome,
) -> DeliveryOutcome {
    match outcome {
        DeliveryOutcome::Delivered => {
            tracker.record_success(inbox_url);
            DeliveryOutcome::Delivered
        }
        DeliveryOutcome::TransientFailure {
            reason,
            retry_after_ms,
        } => {
            let until = tracker.record_failure(inbox_url, now_ms, DESTINATION_BACKOFF_BASE_MS);
            DeliveryOutcome::TransientFailure {
                reason,
                retry_after_ms: Some(retry_after_ms.map_or(until, |t| t.max(until))),
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod slice10_tests {
    use super::*;

    fn inbox_payload(inbox: &str) -> FederateDeliverPayload {
        FederateDeliverPayload::ActorUpdated {
            update_id: "upd-1".to_string(),
            account_id: "acc-1".to_string(),
            inbox_override: Some(inbox.to_string()),
        }
    }

    #[test]
    fn slice10_follower_walk_pages_past_ten_thousand_without_truncation() {
        // Larger than the old 10_000 cap would have allowed, and more than
        // three pages at the production page size.
        let all: Vec<String> = (0..10_250).map(|i| format!("follower-{i:06}")).collect();
        let mut calls = 0usize;
        let got = collect_follower_ids(FOLLOWER_PAGE_SIZE, |cursor, limit| {
            calls += 1;
            // Mirror the store: a page starts strictly after the cursor.
            let start = match cursor {
                None => 0,
                Some(c) => all
                    .iter()
                    .position(|id| id == c)
                    .map(|i| i + 1)
                    .expect("cursor is a known follower id"),
            };
            Ok(all.iter().skip(start).take(limit).cloned().collect())
        })
        .expect("walk");
        assert_eq!(got.len(), 10_250, "no follower truncated");
        assert_eq!(got, all, "every follower exactly once, in order");
        assert_eq!(calls, 11, "ten full pages plus one short page");
    }

    #[test]
    fn slice10_follower_walk_handles_exact_page_multiple_and_empty() {
        let all: Vec<String> = (0..2_000).map(|i| format!("f{i}")).collect();
        let got = collect_follower_ids(1_000, |cursor, limit| {
            let start = match cursor {
                None => 0,
                Some(c) => all.iter().position(|id| id == c).map(|i| i + 1).unwrap_or(0),
            };
            Ok(all.iter().skip(start).take(limit).cloned().collect())
        })
        .expect("walk");
        assert_eq!(got.len(), 2_000);

        let empty = collect_follower_ids(1_000, |_, _| Ok(Vec::new())).expect("empty walk");
        assert!(empty.is_empty());
    }

    #[test]
    fn slice10_follower_walk_propagates_storage_errors() {
        let err = collect_follower_ids(10, |_, _| {
            Err(PortError::StorageUnavailable("boom".to_string()))
        })
        .unwrap_err();
        assert!(matches!(err, PortError::StorageUnavailable(_)));
    }

    #[test]
    fn slice10_fanout_split_threshold() {
        assert!(!fanout_is_split(0));
        assert!(!fanout_is_split(FANOUT_INLINE_MAX));
        assert!(fanout_is_split(FANOUT_INLINE_MAX + 1));
    }

    #[test]
    fn slice10_large_fanout_becomes_one_job_per_inbox_with_derived_keys() {
        let inboxes: Vec<String> = (0..2_500)
            .map(|i| format!("https://peer{i}.example/inbox"))
            .collect();
        let jobs: Vec<NewJob> = inboxes
            .iter()
            .map(|inbox| per_inbox_job("act-1", inbox, 42, &inbox_payload).expect("job"))
            .collect();
        assert_eq!(jobs.len(), 2_500);
        assert!(jobs.iter().all(|j| j.kind == JobKind::FederateDeliver));
        assert!(jobs.iter().all(|j| j.not_before == 42));
        assert_eq!(
            jobs[7].idempotency_key,
            "act-1:https://peer7.example/inbox",
            "re-running the activity job derives the same key"
        );
        let decoded: FederateDeliverPayload =
            serde_json::from_slice(&jobs[7].payload).expect("payload round-trips");
        assert_eq!(decoded, inbox_payload("https://peer7.example/inbox"));
    }
}
