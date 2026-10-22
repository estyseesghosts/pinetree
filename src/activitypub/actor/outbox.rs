//! Actor outbox: the `GET /users/{id}/outbox` ActivityPub
//! `OrderedCollection` / `OrderedCollectionPage` of the actor's public/home
//! non-local-only notes wrapped in `Create` or `Announce`.
//!
//! Paging contract: `docs/activitypub-collections.md`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::Value;

use crate::activitypub::objects::render_create_or_announce_for_note;
use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::note::Visibility;
use crate::domain::visibility::{ViewerContext, is_visible};

use super::actor_document::find_visible_local_user;
use super::collections::{
    COLLECTION_PAGE_SIZE, collection_cursor, ordered_collection_index, ordered_collection_page,
    wants_collection_page,
};

/// Raw notes read per store call while building an outbox page (§3.1).
const OUTBOX_SCAN_BATCH: usize = 40;
/// Upper bound on outbox store reads per request (§3.1).
const OUTBOX_MAX_SCAN_BATCHES: usize = 25;

/// `GET /users/{id}/outbox` — ActivityPub `OrderedCollection` / `OrderedCollectionPage`
/// of the actor's public/home non-local-only notes wrapped in `Create` or `Announce`.
/// Paging contract: `docs/activitypub-collections.md`.
pub async fn get_actor_outbox(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    let base = services.public_url.trim_end_matches('/');
    let outbox_url = format!("{base}/users/{}/outbox", account.id);
    if !wants_collection_page(&params) {
        // Outbox totalItems is omitted (docs/activitypub-collections.md §2).
        return ordered_collection_index(&outbox_url, None);
    }
    let cursor = match collection_cursor(&params) {
        Ok(c) => c,
        Err(code) => return code.into_response(),
    };

    let viewer = ViewerContext::anonymous();
    let mut items: Vec<Value> = Vec::new();
    // Note IDs of `items`, in the same order; the last kept one is the cursor.
    let mut item_note_ids: Vec<String> = Vec::new();
    // Last raw note read from the store; the next scan starts before it.
    let mut scan_position: Option<String> = cursor.clone();
    let mut exhausted = false;
    let mut batches: usize = 0;
    'scan: while !exhausted && batches < OUTBOX_MAX_SCAN_BATCHES {
        batches += 1;
        let batch_cursor = TimelineCursor {
            until_id: scan_position.clone(),
            ..TimelineCursor::with_limit(OUTBOX_SCAN_BATCH)
        };
        let notes = match services.notes.notes_by_author(&account.id, &batch_cursor) {
            Ok(notes) => notes,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        exhausted = notes.len() < OUTBOX_SCAN_BATCH;
        for note in &notes {
            scan_position = Some(note.id.clone());
            if note.local_only
                || !matches!(note.visibility, Visibility::Public | Visibility::Home)
                || !is_visible(note, &viewer)
            {
                continue;
            }
            match render_create_or_announce_for_note(&services, note) {
                Ok(activity) => {
                    items.push(activity);
                    item_note_ids.push(note.id.clone());
                }
                // A storage error is never a silent skip.
                Err(PortError::StorageUnavailable(_)) => {
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                Err(err) => eprintln!(
                    "outbox: account {} note {} skipped: {err}",
                    account.id, note.id
                ),
            }
            // One visible item past the page size proves a `next` page exists.
            if items.len() > COLLECTION_PAGE_SIZE {
                break 'scan;
            }
        }
    }

    let next_position = if items.len() > COLLECTION_PAGE_SIZE {
        items.truncate(COLLECTION_PAGE_SIZE);
        item_note_ids.truncate(COLLECTION_PAGE_SIZE);
        item_note_ids.last().cloned()
    } else if !exhausted {
        // Scan budget spent: continue from the last raw note read so nothing is
        // skipped. The page may then be short or empty while `next` is present.
        scan_position
    } else {
        None
    };
    ordered_collection_page(
        &outbox_url,
        cursor.as_deref(),
        next_position.as_deref(),
        None,
        items,
    )
}
