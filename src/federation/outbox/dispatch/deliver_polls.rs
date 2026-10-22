use super::super::payload::FederateDeliverPayload;
use super::super::recipients::collect_recipient_inboxes_for_author;
use super::{deliver_single, fan_out};
use crate::activitypub::objects::PUBLIC_COLLECTION_URI;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::note::Visibility;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

pub(crate) fn deliver_poll_vote(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    vote_id: String,
    voter_id: String,
    target_note_uri: String,
    choice_name: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
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

pub(crate) fn deliver_question_updated(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    note_id: String,
    author_id: String,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
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
        Some(inbox) => deliver_single(services, client, tracker, &author_id, &inbox, &activity),
        None => {
            let inboxes = collect_recipient_inboxes_for_author(services, &author_id, &[], true)?;
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
