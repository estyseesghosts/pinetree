use axum::response::Response;
use serde_json::Value;

use super::super::common::{non_empty_str, resolve_local_account, resolve_remote_account};
use super::super::{accepted, unauthorized};
use super::quote_requests::{handle_quote_accept, handle_quote_reject};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::ActivityIdIndex;

/// `Accept` routes by object type: `Follow` (existing follow path) or
/// `QuoteRequest` (quote authorization grant). Anything else is ignored.
/// A bare `Accept` without a stamp `result` never approves a quote. A
/// legitimate `Accept(Follow)` carrying a `result` field must never be
/// misrouted: string objects resolve via the quote-request index first.
pub(crate) fn handle_accept(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(obj) = activity.get("object") else {
        return Ok(accepted());
    };
    if let Some(map) = obj.as_object() {
        if is_quote_request_shape(map) {
            return handle_quote_accept(services, activity, activity_id, actor_uri);
        }
        // Embedded Follow object stays on the follow path even with `result`.
        return handle_follow_response(services, activity, activity_id, actor_uri, true);
    }
    if let Some(iri) = obj.as_str().map(str::trim).filter(|s| !s.is_empty()) {
        // Bare-IRI Accept: quote path only when the IRI is a known
        // QuoteRequest AND a stamp `result` is present; otherwise follow.
        if activity.get("result").is_some()
            && services.notes.find_note_by_quote_request(iri)?.is_some()
        {
            return handle_quote_accept(services, activity, activity_id, actor_uri);
        }
    }
    handle_follow_response(services, activity, activity_id, actor_uri, true)
}

/// `Reject` routes the same way. Only an explicit `Reject` of our matching
/// `QuoteRequest` sets `Rejected`; absent stamps never do. A bare-IRI
/// object is looked up as a request id first (normal AP shape), so quote
/// rejects are never lost to the follow path.
pub(crate) fn handle_reject(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(obj) = activity.get("object") else {
        return Ok(accepted());
    };
    if let Some(map) = obj.as_object() {
        if is_quote_request_shape(map) {
            return handle_quote_reject(services, activity, activity_id, actor_uri);
        }
    } else if let Some(iri) = obj.as_str().map(str::trim).filter(|s| !s.is_empty()) {
        if services.notes.find_note_by_quote_request(iri)?.is_some() {
            return handle_quote_reject(services, activity, activity_id, actor_uri);
        }
    }
    handle_follow_response(services, activity, activity_id, actor_uri, false)
}

/// True when an embedded activity object is a `QuoteRequest` (short type
/// name or the full FEP type IRI).
pub(crate) fn is_quote_request_shape(map: &serde_json::Map<String, Value>) -> bool {
    let obj_type = map.get("type").and_then(Value::as_str).unwrap_or("");
    obj_type == "QuoteRequest" || obj_type == crate::activitypub::quotes::FEP044F_QUOTE_REQUEST_TYPE
}

pub(crate) fn handle_follow_response(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
    accept: bool,
) -> Result<Response, PortError> {
    let Some(inner) = activity.get("object").and_then(Value::as_object) else {
        return Ok(accepted());
    };
    if inner.get("type").and_then(Value::as_str).unwrap_or("") != "Follow" {
        return Ok(accepted());
    }
    let (Some(inner_actor), Some(inner_object)) = (
        non_empty_str(inner.get("actor")),
        non_empty_str(inner.get("object")),
    ) else {
        return Ok(accepted());
    };
    // The signer must be the followee answering its own follow.
    if inner_object != actor_uri {
        return Ok(unauthorized());
    }
    let Some(local_follower) = resolve_local_account(services, &inner_actor)? else {
        return Ok(accepted());
    };
    if !local_follower.is_local() {
        return Ok(accepted());
    }
    let remote_followee = resolve_remote_account(services, &inner_object)?;
    if accept {
        services.handle_inbound_accept_follow(
            activity_id,
            &local_follower.id,
            &remote_followee.id,
            false,
        )?;
        // WP5: seed the `Undo(Accept)`-by-ID index so a later bare-IRI
        // `Undo{object: <this Accept id>}` resolves to this pair.
        ActivityIdIndex::record_accept(&inner_actor, &inner_object, activity_id);
    } else {
        services.handle_inbound_reject_follow(
            activity_id,
            &local_follower.id,
            &remote_followee.id,
            false,
        )?;
    }
    Ok(accepted())
}
