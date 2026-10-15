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
    render_accept_follow_activity, render_create_or_announce_for_note,
    render_delete_note_activity, render_follow_activity, render_like_activity,
    render_reject_follow_activity, render_undo_activity, render_update_actor_activity,
};
use crate::application::commands::AppServices;
use crate::application::ports::{JobKind, NewJob, PortError};
use crate::domain::account::Account;
use crate::domain::note::Visibility;
use crate::domain::policy::is_eligible_for_federation_delivery;
use crate::federation::delivery::{SafeHttpClient, signed_post_activity};
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
    /// Deliver `Update(Person)` to remote follower inboxes.
    ActorUpdated {
        update_id: String,
        account_id: String,
        inbox_override: Option<String>,
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
                return Ok(DeliveryOutcome::Delivered);
            };
            if !is_eligible_for_federation_delivery(&note) {
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
            let activity = render_like_activity(
                base,
                &reaction_id,
                &actor.actor_url(base),
                &target_note_uri,
                &reaction,
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
            let inner =
                render_like_activity(base, &reaction_id, &actor_uri, &target_note_uri, &reaction);
            let activity =
                render_undo_activity(base, &undo_id, &actor_uri, &inner, None, None);
            deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
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
    }
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
    let now_ms = services.clock.now_ms();
    if let Some(until_ms) = tracker.cooldown_until(inbox_url, now_ms) {
        return Ok(DeliveryOutcome::Deferred { until_ms });
    }
    let body = serde_json::to_vec(activity)
        .map_err(|e| PortError::Validation(format!("serialize activity: {e}")))?;
    match signed_post_activity(services, client, sender_account_id, inbox_url, &body) {
        Ok(resp) => {
            let reason = String::from_utf8_lossy(&resp.body).into_owned();
            let retry_after = resp.header("retry-after").map(str::to_owned);
            let outcome = classify_inbox_status(
                resp.status,
                reason.trim(),
                retry_after.as_deref(),
                now_ms,
            );
            Ok(settle_outcome(tracker, inbox_url, now_ms, outcome))
        }
        Err(err) if err.kind.is_transient() => {
            let outcome = DeliveryOutcome::TransientFailure {
                reason: err.message,
                retry_after_ms: None,
            };
            Ok(settle_outcome(tracker, inbox_url, now_ms, outcome))
        }
        Err(err) => Ok(DeliveryOutcome::PermanentFailure(err.message)),
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
