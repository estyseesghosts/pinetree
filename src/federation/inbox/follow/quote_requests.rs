use axum::response::Response;
use serde_json::Value;

use super::super::common::{find_note_by_uri, resolve_remote_account};
use super::super::{accepted, unauthorized};
use crate::activitypub::objects::{extract_object_uri, parse_remote_note};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::fetch_remote_note;

/// Inbound FEP-044f `QuoteRequest` for a local post: validate the instrument
/// (ownership + same target), then decide by policy (auto-approve
/// public/unlisted + exceptions, reject tighter) with an atomic
/// stamp+`Accept` / `Reject` commit. Replays are answered without new work.
pub(crate) fn handle_quote_request(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    // Read-only replay gate BEFORE any remote fetch (same benign-race
    // reasoning as `handle_create`).
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    let Some(quoted_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    // The instrument is the quoter's quote Note: inline object or IRI.
    let (instrument_uri, instrument) = match activity.get("instrument") {
        Some(Value::String(uri)) => {
            let uri = uri.trim();
            if uri.is_empty() {
                return Ok(accepted());
            }
            let client = &services.federation_http;
            match fetch_remote_note(services, client, uri) {
                Ok(parsed) => (parsed.uri.clone(), parsed),
                Err(_) => return Ok(accepted()),
            }
        }
        Some(obj @ Value::Object(_)) => match parse_remote_note(obj) {
            Ok(parsed) => (parsed.uri.clone(), parsed),
            Err(_) => return Ok(accepted()),
        },
        _ => return Ok(accepted()),
    };
    // Actor authority: the instrument must be the signer's own Note and must
    // quote exactly the targeted post.
    if instrument.attributed_to != actor_uri {
        return Ok(unauthorized());
    }
    let Some(instrument_target) = instrument.quote_target_uri.clone() else {
        return Ok(accepted());
    };
    if instrument_target != quoted_uri {
        return Ok(accepted());
    }
    let quoter = resolve_remote_account(services, actor_uri)?;
    // The quoted post must be a live local note; only its own server decides.
    let Some(target) = find_note_by_uri(services, &quoted_uri)? else {
        return Ok(accepted());
    };
    let target_author = match services.accounts.find_by_id(&target.author_id)? {
        Some(acc) => acc,
        None => return Ok(accepted()),
    };
    if !target_author.is_local() {
        return Ok(accepted());
    }
    // Enforce the target's actual visibility before deciding: a request for
    // a non-public post from an unauthorized party is rejected, never
    // answered with data. (Policy details live in the application handler;
    // the `Reject` itself reveals nothing beyond the request's own URIs.)
    let inbox = services.remote_inbox_for_account(&quoter);
    let _ = services.handle_inbound_quote_request(
        activity_id,
        &quoter.id,
        actor_uri,
        inbox.as_deref(),
        &target.id,
        &instrument_uri,
        &instrument_target,
        &instrument.attributed_to,
    )?;
    Ok(accepted())
}

/// Inbound `Accept(QuoteRequest)`: the signer must be the quoted post's
/// author, and the `result` stamp must dereference and verify. An `Accept`
/// without a valid stamp never approves.
pub(crate) fn handle_quote_accept(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    let Some(request_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    let Some(stamp_uri) = extract_object_uri(activity.get("result")) else {
        return Ok(accepted());
    };
    // The decider must be the quoted post's author.
    let Some(note) = services.notes.find_note_by_quote_request(&request_uri)? else {
        return Ok(accepted());
    };
    let Some(quote) = note.quote.as_ref() else {
        return Ok(accepted());
    };
    let Some(target_id) = quote.target_note_id.clone() else {
        return Ok(accepted());
    };
    let base = services.public_url.trim_end_matches('/').to_string();
    let target_author_uri = match services.notes.get_note(&target_id)? {
        Some(target) => match services.accounts.find_by_id(&target.author_id)? {
            Some(acc) => acc.actor_url(&base),
            None => return Ok(accepted()),
        },
        None => return Ok(accepted()),
    };
    if target_author_uri != actor_uri {
        return Ok(unauthorized());
    }
    // Transient stamp-fetch failure schedules a rate-limited re-verify job
    // (quote stays Pending); every other outcome is transport-accepted.
    if let crate::application::commands::inbound::quotes::QuoteDecisionOutcome::StampFetchPending(
        retry,
    ) = services.handle_inbound_quote_decision_accept(activity_id, &request_uri, &stamp_uri)?
    {
        services.jobs.enqueue_jobs(vec![retry])?;
    }
    Ok(accepted())
}

/// Inbound `Reject(QuoteRequest)`: only a matching request from the quoted
/// post's author flips our quote to `Rejected` (relation retained).
pub(crate) fn handle_quote_reject(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    // Embedded or bare-IRI object: the request activity IRI either way.
    let request_uri = match activity.get("object") {
        Some(Value::String(s)) if !s.trim().is_empty() => s.trim().to_string(),
        Some(Value::Object(map)) => match map.get("id").and_then(Value::as_str) {
            Some(id) if !id.trim().is_empty() => id.trim().to_string(),
            _ => return Ok(accepted()),
        },
        _ => return Ok(accepted()),
    };
    let Some(note) = services.notes.find_note_by_quote_request(&request_uri)? else {
        return Ok(accepted());
    };
    let Some(quote) = note.quote.as_ref() else {
        return Ok(accepted());
    };
    let Some(target_id) = quote.target_note_id.clone() else {
        return Ok(accepted());
    };
    let base = services.public_url.trim_end_matches('/').to_string();
    let target_author_uri = match services.notes.get_note(&target_id)? {
        Some(target) => match services.accounts.find_by_id(&target.author_id)? {
            Some(acc) => acc.actor_url(&base),
            None => return Ok(accepted()),
        },
        None => return Ok(accepted()),
    };
    if target_author_uri != actor_uri {
        return Ok(unauthorized());
    }
    let _ = services.handle_inbound_quote_decision_reject(activity_id, &request_uri)?;
    Ok(accepted())
}
