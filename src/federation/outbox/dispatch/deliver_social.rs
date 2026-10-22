use serde_json::Value;

use super::deliver_single;
use crate::activitypub::objects::{
    render_accept_follow_activity, render_block_activity, render_follow_activity,
    render_reject_follow_activity, render_undo_activity,
};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

pub(crate) fn deliver_follow(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    follow_id: String,
    follower_id: String,
    followee_id: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
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
    deliver_single(
        services,
        client,
        tracker,
        &follower_id,
        &inbox_url,
        &activity,
    )
}

pub(crate) fn deliver_unfollow(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    undo_id: String,
    follow_id: String,
    follower_id: String,
    followee_id: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
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
    deliver_single(
        services,
        client,
        tracker,
        &follower_id,
        &inbox_url,
        &activity,
    )
}

pub(crate) fn deliver_follow_response(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    response_id: String,
    accept: bool,
    actor_id: String,
    inbox_url: String,
    follow_activity: Value,
) -> Result<DeliveryOutcome, PortError> {
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

pub(crate) fn deliver_block(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    block_id: String,
    blocker_id: String,
    blockee_id: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
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
    deliver_single(
        services,
        client,
        tracker,
        &blocker_id,
        &inbox_url,
        &activity,
    )
}

pub(crate) fn deliver_undo_block(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    undo_id: String,
    block_id: String,
    blocker_id: String,
    blockee_id: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
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
    let inner = render_block_activity(base, &block_id, &blocker_uri, &blockee.actor_url(base));
    let activity = render_undo_activity(base, &undo_id, &blocker_uri, &inner, None, None);
    deliver_single(
        services,
        client,
        tracker,
        &blocker_id,
        &inbox_url,
        &activity,
    )
}
