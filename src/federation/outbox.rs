//! Durable outbound ActivityPub delivery payloads and job execution
//! (Phase 2 Slice D3).
//!
//! State mutations commit a compact [`FederateDeliverPayload`] inside a
//! [`JobKind::FederateDeliver`] job in the same storage transaction as the
//! domain state change. The background worker resolves destination inboxes
//! (preferring `shared_inbox` when available and deduplicating per inbox URL)
//! and performs signed HTTP POSTs with retry/backoff.

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
use crate::federation::retry::{DeliveryOutcome, DestinationTracker, classify_http_status};

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
        let followers = services.follows.followers_of(author_id, None, 10_000)?;
        for f in followers {
            if let Some(acc) = services.accounts.find_by_id(&f.follower_id)? {
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

/// Execute one [`FederateDeliverPayload`] job.
///
/// When a multi-recipient fan-out job (`NoteCreated`, `NoteDeleted`,
/// `ActorUpdated`) has `inbox_override == None`, it resolves the deduplicated
/// destination inbox list and delivers to each inbox; if any single destination
/// fails transiently, a per-inbox retry job (`inbox_override = Some(inbox)`)
/// is enqueued or the job returns a transient error so only failed destinations
/// are retried.
pub fn execute_federate_deliver(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
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
            let inboxes = match inbox_override {
                Some(inbox) => vec![inbox],
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
                    collect_recipient_inboxes_for_author(
                        services,
                        &author_id,
                        &extras,
                        include_followers,
                    )?
                }
            };
            deliver_to_inboxes(services, client, tracker, &author_id, &inboxes, &activity)
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
            let inboxes = match inbox_override {
                Some(inbox) => vec![inbox],
                None => collect_recipient_inboxes_for_author(services, &author_id, &[], true)?,
            };
            deliver_to_inboxes(services, client, tracker, &author_id, &inboxes, &activity)
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
            let inboxes = match inbox_override {
                Some(inbox) => vec![inbox],
                None => collect_recipient_inboxes_for_author(services, &account_id, &[], true)?,
            };
            deliver_to_inboxes(services, client, tracker, &account_id, &inboxes, &activity)
        }
    }
}

fn deliver_to_inboxes(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    sender_account_id: &str,
    inboxes: &[String],
    activity: &Value,
) -> Result<DeliveryOutcome, PortError> {
    if inboxes.is_empty() {
        return Ok(DeliveryOutcome::Delivered);
    }
    let mut first_transient: Option<String> = None;
    for inbox_url in inboxes {
        match deliver_single(services, client, tracker, sender_account_id, inbox_url, activity)? {
            DeliveryOutcome::Delivered | DeliveryOutcome::PermanentFailure(_) => {}
            DeliveryOutcome::TransientFailure(reason) => {
                if first_transient.is_none() {
                    first_transient = Some(reason);
                }
            }
        }
    }
    match first_transient {
        Some(reason) => Ok(DeliveryOutcome::TransientFailure(reason)),
        None => Ok(DeliveryOutcome::Delivered),
    }
}

fn deliver_single(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    sender_account_id: &str,
    inbox_url: &str,
    activity: &Value,
) -> Result<DeliveryOutcome, PortError> {
    let body = serde_json::to_vec(activity)
        .map_err(|e| PortError::Validation(format!("serialize activity: {e}")))?;
    let now_ms = services.clock.now_ms();
    match signed_post_activity(services, client, sender_account_id, inbox_url, &body) {
        Ok(resp) => {
            let reason = String::from_utf8_lossy(&resp.body).into_owned();
            let outcome = classify_http_status(resp.status, reason.trim());
            match &outcome {
                DeliveryOutcome::Delivered => tracker.record_success(inbox_url),
                DeliveryOutcome::TransientFailure(_) => {
                    tracker.record_failure(inbox_url, now_ms, 1000);
                }
                DeliveryOutcome::PermanentFailure(_) => {}
            }
            Ok(outcome)
        }
        Err(err) => {
            tracker.record_failure(inbox_url, now_ms, 1000);
            Ok(DeliveryOutcome::TransientFailure(err))
        }
    }
}
