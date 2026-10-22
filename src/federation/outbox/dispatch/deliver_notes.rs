use super::super::payload::FederateDeliverPayload;
use super::super::recipients::{collect_note_interacted_ids, collect_recipient_inboxes_for_author};
use super::{deliver_single, fan_out};
use crate::activitypub::objects::{
    render_create_or_announce_for_note, render_delete_note_activity, render_note_audience,
    render_note_object, render_note_update_activity, render_undo_activity,
};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::note::Visibility;
use crate::domain::policy::is_eligible_for_federation_delivery;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

pub(crate) fn deliver_note_created(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    _base: &str,
    note_id: String,
    author_id: String,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
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
        Some(inbox) => deliver_single(services, client, tracker, &author_id, &inbox, &activity),
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

pub(crate) fn deliver_note_deleted(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    note_id: String,
    author_id: String,
    note_uri: String,
    was_announce_of_uri: Option<String>,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
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

pub(crate) fn deliver_note_updated(
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
        Some(inbox) => deliver_single(services, client, tracker, &author_id, &inbox, &activity),
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
