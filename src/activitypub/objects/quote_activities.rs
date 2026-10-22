//! FEP-044f quote workflow activities + stamp documents: `QuoteRequest`,
//! accept/reject responses, dereferenceable `QuoteAuthorization` stamps,
//! and stamp revocations.

use serde_json::{Value, json};

use super::audience_context::ap_context;

// ---------------------------------------------------------------------------
// FEP-044f quote workflow activities + stamp documents
// ---------------------------------------------------------------------------

/// Render a FEP-044f `QuoteRequest`: `object` is the quoted Note URI,
/// `instrument` is the quoting Note (inlined per the FEP's SHOULD, never
/// swapped). `to` addresses the quoted author only.
pub fn render_quote_request_activity(
    public_url: &str,
    request_id: &str,
    quoter_actor_uri: &str,
    quoted_uri: &str,
    quoted_author_actor_uri: &str,
    inline_quote_note: Value,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{request_id}"),
        "type": "QuoteRequest",
        "actor": quoter_actor_uri,
        "to": [quoted_author_actor_uri],
        "object": quoted_uri,
        "instrument": inline_quote_note,
    })
}

/// Render an `Accept` (with `result` = stamp IRI) or `Reject` answering a
/// `QuoteRequest`. The `object` is the request activity IRI; a bare `Accept`
/// without a stamp `result` never counts as approval.
pub fn render_quote_response_activity(
    public_url: &str,
    response_id: &str,
    accept: bool,
    decider_actor_uri: &str,
    request_activity_uri: &str,
    stamp_uri: Option<&str>,
    quoter_actor_uri: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    let mut doc = json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{response_id}"),
        "type": if accept { "Accept" } else { "Reject" },
        "actor": decider_actor_uri,
        "to": [quoter_actor_uri],
        "object": request_activity_uri,
    });
    if accept {
        if let Some(stamp) = stamp_uri {
            doc["result"] = Value::String(stamp.to_string());
        }
    }
    doc
}

/// Render a dereferenceable `QuoteAuthorization` stamp document. The stamp
/// carries only URIs: it never embeds the quote Note, and the target is
/// referenced by URI (callers gate serving on target visibility).
pub fn render_quote_stamp_document(
    stamp_id: &str,
    author_actor_uri: &str,
    interacting_object_uri: &str,
    interaction_target_uri: &str,
) -> Value {
    json!({
        "@context": ap_context(),
        "id": stamp_id,
        "type": "QuoteAuthorization",
        "attributedTo": author_actor_uri,
        "interactingObject": interacting_object_uri,
        "interactionTarget": interaction_target_uri,
    })
}

/// Render a `Delete` revoking a `QuoteAuthorization` stamp (FEP-044f
/// revocation). The `object` is the stamp IRI.
pub fn render_delete_stamp_activity(
    public_url: &str,
    delete_id: &str,
    actor_uri: &str,
    stamp_uri: &str,
    to: Vec<String>,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{delete_id}"),
        "type": "Delete",
        "actor": actor_uri,
        "to": to,
        "object": stamp_uri,
    })
}
