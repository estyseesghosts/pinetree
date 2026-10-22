//! Axum GET handlers for `/notes/{id}` and `/notes/{id}/activity`
//! (plus `/users/{id}/stamps/{stamp}`), with the shared AP response
//! envelope and the small URI/string extractors the handlers need.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::Value;

use crate::application::commands::AppServices;
use crate::domain::note::Visibility;
use crate::domain::visibility::{ViewerContext, is_visible};

use super::activities::render_create_or_announce_for_note;
use super::audience_context::AP_CONTENT_TYPE;
use super::note_document::render_note_object;
use super::quote_activities::render_quote_stamp_document;
use super::tombstone::render_tombstone;

// ---------------------------------------------------------------------------
// Axum GET handlers for `/notes/{id}` and `/notes/{id}/activity`
// ---------------------------------------------------------------------------

pub(crate) fn ap_response(status: StatusCode, body: &Value) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(AP_CONTENT_TYPE),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    let payload = serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string());
    (status, headers, payload).into_response()
}

/// `GET /notes/{id}` — serves the ActivityPub `Note` JSON-LD document for
/// public/home local notes, `410 Gone` `Tombstone` for deleted notes, and
/// `404 Not Found` for non-public or local-only notes.
pub async fn get_note_ap(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
) -> Response {
    let base = services.public_url.trim_end_matches('/');
    let note = match services.notes.get_note_including_deleted(&id) {
        Ok(Some(n)) => n,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if note.is_deleted {
        let note_uri = note
            .uri
            .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
        return ap_response(StatusCode::GONE, &render_tombstone(&note_uri));
    }
    if note.user_host.is_some()
        || note.local_only
        || !matches!(note.visibility, Visibility::Public | Visibility::Home)
        || !is_visible(&note, &ViewerContext::anonymous())
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    match render_note_object(&services, &note, true) {
        Ok(doc) => ap_response(StatusCode::OK, &doc),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// `GET /users/{id}/stamps/{stamp}` — serves a FEP-044f
/// `QuoteAuthorization` stamp document authored by a local user.
///
/// - Unknown or revoked stamps → `404` (a deleted stamp must not
///   dereference; verifiers treat fetch failure as inconclusive, never as
///   approval).
/// - Stamps for non-public targets → `404` (the stamp must be
///   dereferenceable by parties allowed to view the original post; the doc
///   carries URIs only, but nothing is served for protected targets).
/// - The stamp never embeds the quote Note.
pub async fn get_quote_stamp(
    State(services): State<Arc<AppServices>>,
    Path((id, stamp)): Path<(String, String)>,
) -> Response {
    let base = services.public_url.trim_end_matches('/');
    let account = match services.accounts.find_by_id(&id) {
        Ok(Some(acc)) if acc.is_local() => acc,
        Ok(_) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let stamp_uri = format!("{base}/users/{}/stamps/{stamp}", account.id);
    let record = match services.notes.get_quote_stamp(&stamp_uri) {
        Ok(Some(r)) if !r.revoked => r,
        Ok(_) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    // Gate on the quoted post's actual visibility.
    let target = match services.notes.get_note(&record.interaction_target_note_id) {
        Ok(Some(n)) => n,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if target.is_deleted
        || target.local_only
        || !matches!(target.visibility, Visibility::Public | Visibility::Home)
        || !is_visible(&target, &ViewerContext::anonymous())
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let target_uri = target
        .uri
        .unwrap_or_else(|| format!("{base}/notes/{}", target.id));
    let doc = render_quote_stamp_document(
        &stamp_uri,
        &account.actor_url(base),
        &record.interacting_object_uri,
        &target_uri,
    );
    ap_response(StatusCode::OK, &doc)
}

/// `GET /notes/{id}/activity` — serves the `Create(Note)` or `Announce`
/// ActivityPub document for a local public/home note.
pub async fn get_note_activity_ap(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
) -> Response {
    let note = match services.notes.get_note(&id) {
        Ok(Some(n)) => n,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if note.user_host.is_some()
        || note.local_only
        || !matches!(note.visibility, Visibility::Public | Visibility::Home)
        || !is_visible(&note, &ViewerContext::anonymous())
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    match render_create_or_announce_for_note(&services, &note) {
        Ok(doc) => ap_response(StatusCode::OK, &doc),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Extract an object URI from `activity["object"]` whether it is a string URI
/// or an embedded JSON object with `"id"`.
pub fn extract_object_uri(val: Option<&Value>) -> Option<String> {
    match val? {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Value::Object(m) => m
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned),
        _ => None,
    }
}

/// Deduplicate a list of strings while preserving order.
pub fn dedup_strings(items: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for item in items {
        if !item.is_empty() && seen.insert(item.clone()) {
            out.push(item);
        }
    }
    out
}
