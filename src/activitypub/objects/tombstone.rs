//! Deletion surface: `Delete(Person)` / `Delete(Note)` activities and the
//! served `Tombstone` documents (plus the inner-URI extractor for inbound
//! `Delete{Tombstone}` dispatch).

use serde_json::{Value, json};

use crate::activitypub::format_millis;

use super::audience_context::{PUBLIC_COLLECTION_URI, ap_context};

/// Render a `Delete(Person)` for a suspended local account (Batch M WP3;
/// upstream `ApRendererService.renderDelete(genLocalUserUri(user.id), user)`
/// parity: `object` is the actor URI string, no `to`/`cc`, `published` set).
/// Unlike upstream (which renders no activity id) pinetree gives the
/// activity an id so re-deliveries carry a stable id for receiver-side
/// dedup; the unsuspend `Undo` is keyed off it as `{id}/undo`.
pub fn render_delete_person_activity(
    public_url: &str,
    delete_id: &str,
    actor_uri: &str,
    now_ms: i64,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{delete_id}"),
        "type": "Delete",
        "actor": actor_uri,
        "object": actor_uri,
        "published": format_millis(now_ms),
    })
}

/// Render a `Delete(Tombstone)` activity for a deleted note.
pub fn render_delete_note_activity(
    public_url: &str,
    delete_id: &str,
    actor_uri: &str,
    note_uri: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{delete_id}"),
        "type": "Delete",
        "actor": actor_uri,
        "to": [PUBLIC_COLLECTION_URI],
        "object": {
            "id": note_uri,
            "type": "Tombstone",
            "formerType": "Note",
        },
    })
}

/// Render a `Tombstone` JSON-LD document for `GET /notes/{id}` when deleted.
pub fn render_tombstone(note_uri: &str) -> Value {
    json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": note_uri,
        "type": "Tombstone",
        "formerType": "Note",
    })
}

/// Extract the inner note URI from a `Tombstone`-typed embedded object
/// (`Delete{Tombstone{id}}` per Misskey dispatch, or a bare `Tombstone`
/// document served at a deleted-note URI). Returns `None` for non-Tombstone
/// objects — use [`super::object_handlers::extract_object_uri`] for the
/// general case.
///
/// A `type` value is Tombstone-shaped when it is exactly `"Tombstone"` or
/// ends with `#Tombstone` (compact- vs full-IRI JSON-LD forms).
pub fn tombstone_inner_id(object: Option<&Value>) -> Option<String> {
    let map = object?.as_object()?;
    let obj_type = map.get("type").and_then(Value::as_str).unwrap_or("");
    let is_tombstone = obj_type == "Tombstone" || obj_type.ends_with("#Tombstone");
    if !is_tombstone {
        return None;
    }
    map.get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
}
