//! Federated clip collections: `OrderedCollection` documents, the
//! actor clips index, `Add`/`Remove`/`Update`/`Delete` activities,
//! tombstones, and the fetch-gated HTTP serving layer.

use std::collections::HashMap;

use axum::{
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

use crate::activitypub::actor::parse_url_host;
use crate::activitypub::objects::ap_context;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::clip::Clip;
use crate::domain::visibility::{ViewerContext, is_visible};
use crate::federation::fetch_gate::{authenticate_fetch_viewer, decide_fetch};

use super::serving::{ap_response, federation_switch_on};

/// Page size for clip-collection paging (= the per-clip member cap, so a
/// full collection is always one page; the paging envelope still applies).
pub const CLIP_COLLECTION_PAGE_SIZE: usize = 200;

/// Clip collection URI (`{base}/clips/{id}`).
pub fn clip_collection_uri(base: &str, clip_id: &str) -> String {
    format!("{}/clips/{clip_id}", base.trim_end_matches('/'))
}

/// Actor clips-index URI (`{base}/users/{id}/clips`).
pub fn clips_index_uri(base: &str, account_id: &str) -> String {
    format!("{}/users/{account_id}/clips", base.trim_end_matches('/'))
}

/// Render the `OrderedCollection` document for a clip, with member note
/// URIs gated per-item against the verified fetcher. Notes the fetcher
/// may not see are omitted (never embedded, never leaked by URI).
pub fn render_clip_collection(
    services: &AppServices,
    clip: &Clip,
    viewer_actor_uri: Option<&str>,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let owner = services
        .accounts
        .find_by_id(&clip.owner_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let uri = clip_collection_uri(base, &clip.id);
    let base_viewer = fetch_viewer_context(services, viewer_actor_uri)?;
    let mut items = Vec::new();
    for note_id in services.clips.clip_note_ids(&clip.id)? {
        let Some(note) = services.notes.get_note(&note_id)? else {
            continue;
        };
        if note.is_deleted {
            continue;
        }
        // Per-item gate: the viewer reads with its real follow edge to the
        // note's author (unknown actors read as strangers).
        let mut viewer = base_viewer.clone();
        viewer.follows_author =
            viewer_follows_author(services, viewer.viewer_id.as_deref(), &note.author_id)?;
        if !is_visible(&note, &viewer) {
            continue;
        }
        let note_uri = note
            .uri
            .clone()
            .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
        items.push(Value::String(note_uri));
    }
    let total = items.len();
    let updated = crate::activitypub::format_millis(clip.updated_at_ms);
    Ok(json!({
        "@context": ap_context(),
        "id": uri.clone(),
        "type": "OrderedCollection",
        "attributedTo": owner.actor_url(base),
        "name": clip.name,
        "summary": clip.description,
        "totalItems": total,
        "updated": updated,
        "first": format!("{uri}?page=true"),
    }))
}

/// Render one full-state collection page (single page: the member cap
/// bounds every collection to [`CLIP_COLLECTION_PAGE_SIZE`] items).
pub fn render_clip_collection_page(
    services: &AppServices,
    clip: &Clip,
    viewer_actor_uri: Option<&str>,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let uri = clip_collection_uri(base, &clip.id);
    let collection = render_clip_collection(services, clip, viewer_actor_uri)?;
    // Re-resolve items through the same gate (single source: the index
    // renderer) — the page carries URIs only, capped at the page size.
    let mut items: Vec<Value> = Vec::new();
    let base_viewer = fetch_viewer_context(services, viewer_actor_uri)?;
    for note_id in services.clips.clip_note_ids(&clip.id)? {
        let Some(note) = services.notes.get_note(&note_id)? else {
            continue;
        };
        if note.is_deleted {
            continue;
        }
        let mut viewer = base_viewer.clone();
        viewer.follows_author =
            viewer_follows_author(services, viewer.viewer_id.as_deref(), &note.author_id)?;
        if !is_visible(&note, &viewer) {
            continue;
        }
        let note_uri = note
            .uri
            .clone()
            .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
        items.push(Value::String(note_uri));
        if items.len() >= CLIP_COLLECTION_PAGE_SIZE {
            break;
        }
    }
    let total = collection
        .get("totalItems")
        .and_then(Value::as_u64)
        .unwrap_or(items.len() as u64);
    Ok(json!({
        "@context": ap_context(),
        "id": format!("{uri}?page=true"),
        "type": "OrderedCollectionPage",
        "partOf": uri,
        "totalItems": total,
        "orderedItems": items,
    }))
}

/// Build the read-time viewer for collection item gating from a verified
/// fetcher actor URI (or anonymous). Unknown actors read as strangers
/// (no follows); remote viewers are marked remote so `local_only` notes
/// stay withheld.
fn fetch_viewer_context(
    services: &AppServices,
    viewer_actor_uri: Option<&str>,
) -> Result<ViewerContext, PortError> {
    let Some(actor_uri) = viewer_actor_uri else {
        return Ok(ViewerContext {
            viewer_id: None,
            viewer_host: None,
            follows_author: false,
            is_remote_request: true,
        });
    };
    let host = parse_url_host(actor_uri).map(|(_, h)| h);
    // `find_by_uri` never matches local rows: a local actor fetching
    // remotely is treated as a stranger (correct — same as Misskey, where
    // local accounts never fetch over AP).
    let viewer_id = services.accounts.find_by_uri(actor_uri)?.map(|a| a.id);
    Ok(ViewerContext {
        viewer_id,
        viewer_host: host,
        follows_author: false,
        is_remote_request: true,
    })
}

/// Per-note follow check for collection gating: whether `viewer_id` (a
/// known account row, local or cached-remote) follows `author_id`.
pub fn viewer_follows_author(
    services: &AppServices,
    viewer_id: Option<&str>,
    author_id: &str,
) -> Result<bool, PortError> {
    let Some(viewer_id) = viewer_id else {
        return Ok(false);
    };
    if viewer_id == author_id {
        return Ok(true);
    }
    Ok(services.follows.is_following(viewer_id, author_id)?)
}

/// Render the actor's advertised clips index: listed + federate-enabled
/// clips only (unlisted never advertised; nothing here gates per-item —
/// each collection enforces its own tier on fetch).
pub fn render_clips_index(services: &AppServices, owner_id: &str) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let owner = services
        .accounts
        .find_by_id(owner_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let uri = clips_index_uri(base, owner_id);
    let mut items = Vec::new();
    for clip in services.clips.clips_by_owner(owner_id)? {
        if clip.tier.advertised() && clip.federate {
            items.push(Value::String(clip_collection_uri(base, &clip.id)));
        }
    }
    let total = items.len();
    Ok(json!({
        "@context": ap_context(),
        "id": uri.clone(),
        "type": "OrderedCollection",
        "attributedTo": owner.actor_url(base),
        "totalItems": total,
        "first": format!("{uri}?page=true"),
        "orderedItems": items,
    }))
}

/// Render `Add`/`Remove` of a note to/from a clip collection
/// (`target` = collection URI; no new activity types — collection state
/// is the protocol).
pub fn render_clip_membership_activity(
    base: &str,
    activity_id: &str,
    actor_uri: &str,
    note_uri: &str,
    collection_uri: &str,
    added: bool,
) -> Value {
    let base = base.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{activity_id}"),
        "type": if added { "Add" } else { "Remove" },
        "actor": actor_uri,
        "target": collection_uri,
        "object": note_uri,
    })
}

/// Render `Update(OrderedCollection)` carrying the embedded full
/// collection document (one round trip, no refetch).
pub fn render_clip_update_activity(
    base: &str,
    activity_id: &str,
    actor_uri: &str,
    collection: &Value,
) -> Value {
    let base = base.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{activity_id}"),
        "type": "Update",
        "actor": actor_uri,
        "object": collection,
    })
}

/// Render `Delete(OrderedCollection)` for a clip URI.
pub fn render_clip_delete_activity(
    base: &str,
    activity_id: &str,
    actor_uri: &str,
    collection_uri: &str,
) -> Value {
    let base = base.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{activity_id}"),
        "type": "Delete",
        "actor": actor_uri,
        "object": {
            "id": collection_uri,
            "type": "OrderedCollection",
        },
    })
}

/// Render a clip `Tombstone` (served at deleted URIs).
pub fn render_clip_tombstone(collection_uri: &str) -> Value {
    json!({
        "@context": ap_context(),
        "id": collection_uri,
        "type": "Tombstone",
        "formerType": "OrderedCollection",
    })
}

/// `GET /clips/{id}[?page=true]` — serves the `OrderedCollection` (or one
/// full-state page) through the fetch gate. Per-item gating inside the
/// renderer; unknown/deleted/ungated clips 404.
pub async fn get_clip_collection(
    State(services): State<std::sync::Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    OriginalUri(original): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let target = original.to_string();
    let clip = match services.clips.get_clip(&id) {
        Ok(Some(c)) => Some(c),
        Ok(None) => None,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let Some(clip) = clip else {
        // Tombstone (Phase 2a): a deleted clip URI serves `Tombstone`
        // (`formerType: OrderedCollection`) so receivers distinguish
        // "deleted, drop your copy" from 404 — but only while the
        // instance switch is on (switch off ⇒ zero protocol footprint).
        if !federation_switch_on(&services, "clips") {
            return StatusCode::NOT_FOUND.into_response();
        }
        let base = services.public_url.trim_end_matches('/').to_string();
        let uri = clip_collection_uri(&base, &id);
        match services.clips.clip_tombstone(&uri) {
            Ok(Some(_)) => {
                return ap_response(StatusCode::GONE, &render_clip_tombstone(&uri));
            }
            Ok(None) => return StatusCode::NOT_FOUND.into_response(),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    };
    // Pre-authenticate cheaply to learn the fetcher before deciding: the
    // callback needs no I/O, so run the gate in two steps (viewer first,
    // then decision) rather than through `fetch_pre_gate`.
    if !federation_switch_on(&services, "clips") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let viewer = authenticate_fetch_viewer(&services, &target, &headers);
    let decision = AppServices::authorize_clip_fetch(&clip, true, viewer.actor_uri.as_deref());
    let actor_uri = match decide_fetch(&viewer, decision) {
        Ok(()) => viewer.actor_uri.clone(),
        Err(status) => return status.into_response(),
    };
    let doc = if params.get("page").map(|v| v == "true").unwrap_or(false) {
        render_clip_collection_page(&services, &clip, actor_uri.as_deref())
    } else {
        render_clip_collection(&services, &clip, actor_uri.as_deref())
    };
    match doc {
        Ok(doc) => ap_response(StatusCode::OK, &doc),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// `GET /users/{id}/clips[?page=true]` — the advertised clips index
/// (listed + federate-enabled only). Global switch off ⇒ 404. The index
/// itself is ungated (it reveals nothing unlisted); each collection
/// enforces its own tier on fetch.
pub async fn get_clips_index(
    State(services): State<std::sync::Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !federation_switch_on(&services, "clips") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let account = match services.accounts.find_by_id(&id) {
        Ok(Some(a)) if a.is_local() && !a.is_suspended && !a.is_deleted => a,
        Ok(_) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let index = match render_clips_index(&services, &account.id) {
        Ok(doc) => doc,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if params.get("page").map(|v| v == "true").unwrap_or(false) {
        // Single full-state page (bounded by the per-owner clip cap).
        let base = services.public_url.trim_end_matches('/');
        let uri = clips_index_uri(base, &account.id);
        let page = json!({
            "@context": ap_context(),
            "id": format!("{uri}?page=true"),
            "type": "OrderedCollectionPage",
            "partOf": uri,
            "totalItems": index.get("totalItems").cloned().unwrap_or(json!(0)),
            "orderedItems": index.get("orderedItems").cloned().unwrap_or(json!([])),
        });
        return ap_response(StatusCode::OK, &page);
    }
    ap_response(StatusCode::OK, &index)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Membership auth matrix at the op level (the gate maps these through
    /// `decide_fetch` to serve/403/404).
    #[test]
    fn clip_authorize_matrix() {
        use crate::domain::clip::Clip;
        use crate::domain::collection::CollectionTier::*;
        use crate::federation::fetch_gate::FetchDecision;
        let mut clip = Clip::new(
            "c".to_string(),
            "u".to_string(),
            "n".to_string(),
            None,
            true,
            0,
        );
        // Global or per-clip flag off ⇒ hidden, every tier.
        for tier in [Private, LocalApproved, Unlisted, Listed, RemoteApproved] {
            clip.tier = tier;
            clip.federate = true;
            assert_eq!(
                AppServices::authorize_clip_fetch(&clip, false, None),
                FetchDecision::Hidden
            );
            clip.federate = false;
            assert_eq!(
                AppServices::authorize_clip_fetch(&clip, true, None),
                FetchDecision::Hidden
            );
            clip.federate = true;
        }
        // Per-tier answers with both switches on.
        clip.tier = Private;
        assert_eq!(
            AppServices::authorize_clip_fetch(&clip, true, None),
            FetchDecision::Hidden
        );
        clip.tier = LocalApproved;
        assert_eq!(
            AppServices::authorize_clip_fetch(&clip, true, None),
            FetchDecision::Forbidden
        );
        clip.tier = Unlisted;
        assert_eq!(
            AppServices::authorize_clip_fetch(&clip, true, None),
            FetchDecision::Serve
        );
        clip.tier = Listed;
        assert_eq!(
            AppServices::authorize_clip_fetch(&clip, true, Some("https://x.example/u")),
            FetchDecision::Serve
        );
        // Remote-approved: approver served, stranger forbidden, anonymous
        // forbidden (the gate collapses the anonymous case to 404).
        clip.tier = RemoteApproved;
        clip.approved_actor_uris = vec!["https://ok.example/a".to_string()];
        assert_eq!(
            AppServices::authorize_clip_fetch(&clip, true, Some("https://ok.example/a")),
            FetchDecision::Serve
        );
        assert_eq!(
            AppServices::authorize_clip_fetch(&clip, true, Some("https://no.example/b")),
            FetchDecision::Forbidden
        );
        assert_eq!(
            AppServices::authorize_clip_fetch(&clip, true, None),
            FetchDecision::Forbidden
        );
    }
}
