//! Federated clip sync delivery helpers (moved from `src/federation/outbox.rs`).
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

use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

use super::dispatch::{deliver_single, fan_out};
use super::payload::FederateDeliverPayload;
use super::recipients::{collect_recipient_inboxes_for_author, preferred_inbox};

/// Recipient inboxes for clip sync fan-out: owner followers (mirrors
/// pinned-note delivery) + clip followers (remote rows join the set).
pub(crate) fn collect_clip_sync_inboxes(
    services: &AppServices,
    owner_id: &str,
    clip_id: &str,
) -> Result<Vec<String>, PortError> {
    let mut inboxes = collect_recipient_inboxes_for_author(services, owner_id, &[], true)?;
    for f in services.clips.followers_of_clip(clip_id, 1000)? {
        if let Some(acc) = services.accounts.find_by_id(&f.user_id)? {
            if !acc.is_local() {
                if let Some(inbox) = preferred_inbox(&acc) {
                    if !inboxes.contains(&inbox) {
                        inboxes.push(inbox);
                    }
                }
            }
        }
    }
    Ok(inboxes)
}

/// Shared `Update(OrderedCollection)` fan-out for federated clips: owner
/// must be local, clip must still exist and still federate Listed, else
/// the stale job converges quietly.
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_clip_update(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    clip_id: &str,
    owner_id: &str,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
    use crate::activitypub::federated_collections::{
        federation_switch_on, render_clip_collection, render_clip_update_activity,
    };
    let owner = match services.accounts.find_by_id(owner_id)? {
        Some(a) if a.is_local() => a,
        _ => return Ok(DeliveryOutcome::Delivered),
    };
    let clip = match services.clips.get_clip(clip_id)? {
        Some(c) if c.owner_id == owner_id => c,
        _ => return Ok(DeliveryOutcome::Delivered),
    };
    if !clip.federate
        || clip.tier != crate::domain::collection::CollectionTier::Listed
        || !federation_switch_on(services, "clips")
    {
        return Ok(DeliveryOutcome::Delivered);
    }
    let actor_uri = owner.actor_url(base);
    let collection = render_clip_collection(services, &clip, None)?;
    let activity_id = format!("clip-update-{clip_id}-{}", services.ids.generate());
    let activity = render_clip_update_activity(base, &activity_id, &actor_uri, &collection);
    match inbox_override {
        Some(inbox) => deliver_single(services, client, tracker, owner_id, &inbox, &activity),
        None => {
            let inboxes = collect_clip_sync_inboxes(services, owner_id, clip_id)?;
            fan_out(
                services,
                client,
                tracker,
                job_key,
                owner_id,
                &inboxes,
                &activity,
                &|inbox: &str| FederateDeliverPayload::ClipUpdated {
                    clip_id: clip_id.to_string(),
                    owner_id: owner_id.to_string(),
                    inbox_override: Some(inbox.to_string()),
                },
            )
        }
    }
}

/// Shared `Delete(OrderedCollection)` fan-out: fires on delete and on tier
/// flips to a more restrictive tier. Unlike updates, a delete fans out
/// even when the clip row is already gone (the URI is carried in the job).
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_clip_delete(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    clip_id: &str,
    owner_id: &str,
    clip_uri: &str,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
    use crate::activitypub::federated_collections::render_clip_delete_activity;
    let owner = match services.accounts.find_by_id(owner_id)? {
        Some(a) if a.is_local() => a,
        _ => return Ok(DeliveryOutcome::Delivered),
    };
    let actor_uri = owner.actor_url(base);
    let activity_id = format!("clip-delete-{clip_id}-{}", services.ids.generate());
    let activity = render_clip_delete_activity(base, &activity_id, &actor_uri, clip_uri);
    match inbox_override {
        Some(inbox) => deliver_single(services, client, tracker, owner_id, &inbox, &activity),
        None => {
            let inboxes = collect_clip_sync_inboxes(services, owner_id, clip_id)?;
            fan_out(
                services,
                client,
                tracker,
                job_key,
                owner_id,
                &inboxes,
                &activity,
                &|inbox: &str| FederateDeliverPayload::ClipDeleted {
                    clip_id: clip_id.to_string(),
                    owner_id: owner_id.to_string(),
                    clip_uri: clip_uri.to_string(),
                    inbox_override: Some(inbox.to_string()),
                },
            )
        }
    }
}
