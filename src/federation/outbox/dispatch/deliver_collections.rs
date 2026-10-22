use super::deliver_single;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

pub(crate) fn deliver_clip_membership_changed(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    activity_id: String,
    actor_id: String,
    clip_id: String,
    note_uri: String,
    added: bool,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
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
    // Stale job (clip unfederated or tier-restricted after
    // enqueue): skip quietly — the collection fetch gate enforces
    // the same switches.
    if !clip.federate
        || clip.tier != crate::domain::collection::CollectionTier::Listed
        || !federation_switch_on(services, "clips")
    {
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

pub(crate) fn deliver_page_liked(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    activity_id: String,
    actor_id: String,
    actor_uri: String,
    page_uri: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
    use crate::activitypub::federated_collections::render_page_like_activity;
    // Stale job (liker gone) converges quietly.
    if services.accounts.find_by_id(&actor_id)?.is_none() {
        return Ok(DeliveryOutcome::Delivered);
    }
    let activity = render_page_like_activity(base, &activity_id, &actor_uri, &page_uri);
    deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
}
