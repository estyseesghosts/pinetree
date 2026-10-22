use super::super::payload::FederateDeliverPayload;
use super::super::quotes::quote_note_extras;
use super::super::recipients::collect_recipient_inboxes_for_author;
use super::{deliver_single, fan_out};
use crate::activitypub::objects::{
    PUBLIC_COLLECTION_URI, render_delete_stamp_activity, render_note_object,
    render_quote_request_activity, render_quote_response_activity,
};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

pub(crate) fn deliver_quote_request(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    request_id: String,
    quote_note_id: String,
    quoter_account_id: String,
    quoted_uri: String,
    target_actor_uri: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
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

pub(crate) fn deliver_quote_response(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    response_id: String,
    accept: bool,
    decider_account_id: String,
    request_activity_uri: String,
    stamp_uri: Option<String>,
    quoter_actor_uri: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
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

pub(crate) fn deliver_quote_stamp_deleted(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    delete_id: String,
    actor_id: String,
    stamp_uri: String,
    quote_note_id: String,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
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
    let activity =
        render_delete_stamp_activity(base, &delete_id, &actor_uri, &stamp_uri, to_public);
    match inbox_override {
        Some(inbox) => deliver_single(services, client, tracker, &actor_id, &inbox, &activity),
        None => {
            let inboxes = collect_recipient_inboxes_for_author(services, &actor_id, &extras, true)?;
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

pub(crate) fn deliver_featured_changed(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    actor_id: String,
    note_id: String,
    added: bool,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
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
    let note_uri = note
        .uri
        .clone()
        .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
    let change_id = format!(
        "featured-{}-{note_id}",
        if added { "add" } else { "remove" }
    );
    let activity = crate::activitypub::objects::render_featured_change_activity(
        base, &change_id, &actor_uri, &note_uri, added,
    );
    match inbox_override {
        Some(inbox) => deliver_single(services, client, tracker, &actor_id, &inbox, &activity),
        None => {
            let inboxes = collect_recipient_inboxes_for_author(services, &actor_id, &[], true)?;
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
