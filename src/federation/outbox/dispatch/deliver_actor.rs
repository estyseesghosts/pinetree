use serde_json::Value;

use super::super::payload::FederateDeliverPayload;
use super::super::recipients::{
    SUSPEND_SWEEP_PAGE_ROWS, collect_recipient_inboxes_for_author, collect_shared_inbox_page,
};
use super::{deliver_single, fan_out};
use crate::activitypub::objects::{render_undo_activity, render_update_actor_activity};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

pub(crate) fn deliver_actor_updated(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    _base: &str,
    update_id: String,
    account_id: String,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
    let account = match services.accounts.find_by_id(&account_id)? {
        Some(a) => a,
        None => return Ok(DeliveryOutcome::Delivered),
    };
    let activity = render_update_actor_activity(services, &update_id, &account)?;
    match inbox_override {
        Some(inbox) => deliver_single(services, client, tracker, &account_id, &inbox, &activity),
        None => {
            let inboxes = collect_recipient_inboxes_for_author(services, &account_id, &[], true)?;
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

pub(crate) fn deliver_move_delivered(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    activity_id: String,
    account_id: String,
    new_actor_uri: String,
    new_account_id: String,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
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
        Some(inbox) => deliver_single(services, client, tracker, &account_id, &inbox, &activity),
        None => {
            let inboxes = collect_recipient_inboxes_for_author(services, &account_id, &[], true)?;
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

pub(crate) fn deliver_suspend_broadcast(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    account_id: String,
    activity_id: String,
    undo: bool,
    inbox_override: Option<String>,
    after: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
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
        Some(inbox) => deliver_single(services, client, tracker, &account_id, &inbox, &activity),
        None => {
            let (page, next) =
                collect_shared_inbox_page(services, after.as_deref(), SUSPEND_SWEEP_PAGE_ROWS)?;
            let mut outcome = fan_out(
                services,
                client,
                tracker,
                job_key,
                &account_id,
                &page,
                &activity,
                &|inbox: &str| FederateDeliverPayload::SuspendBroadcast {
                    account_id: account_id.clone(),
                    activity_id: activity_id.clone(),
                    undo,
                    inbox_override: Some(inbox.to_string()),
                    after: None,
                },
            )?;
            if let Some(cursor) = next {
                let cont = FederateDeliverPayload::SuspendBroadcast {
                    account_id: account_id.clone(),
                    activity_id: activity_id.clone(),
                    undo,
                    inbox_override: None,
                    after: Some(cursor.clone()),
                }
                .into_job(format!("{job_key}:sweep:{cursor}"), services.clock.now_ms())?;
                // `fan_out` only ever returns `Delivered` or `Split`.
                let mut followups = match outcome {
                    DeliveryOutcome::Split { followups } => followups,
                    DeliveryOutcome::Delivered => Vec::new(),
                    other => return Ok(other),
                };
                followups.push(cont);
                outcome = DeliveryOutcome::Split { followups };
            }
            Ok(outcome)
        }
    }
}

pub(crate) fn deliver_flag(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    flag_id: String,
    actor_id: String,
    target_account_id: String,
    content: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
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
