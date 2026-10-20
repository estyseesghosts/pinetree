//! Federated clips + pages wire surface (Phases 0–3 of
//! `docs/handoff-federated-clips-pages.md`; the fetch-gate contract is
//! documented in `docs/federated-collections.md`).
//!
//! Clips and pages are server-local in Misskey (no AP representation), so
//! this entire surface is a pinetree-first DIVERGENCE — pinetree↔pinetree
//! only, gracefully ignored by everyone else.
//!
//! * Clips federate as **`OrderedCollection`** at `/clips/{id}` carrying
//!   `attributedTo`, `name`, `summary`, `totalItems`, and paged **note URIs
//!   only** (never embedded notes; page size 200, the per-clip cap). No
//!   new activity types: collection state is the protocol. Membership
//!   sync is full-state paging with TTL revalidation plus prompt inbound
//!   `Add`/`Remove` (idempotent, authority = collection owner).
//! * Pages federate as **`Article`** at `/pages/{id}` (`name` = title,
//!   `content` = server-rendered sanitized HTML, `summary`, `url` = page
//!   URL, `attributedTo`, `published`/`updated`, `attachment` = eye-catch
//!   image; `source` carries content blocks + `variables` for
//!   pinetree↔pinetree fidelity). **`script` is NEVER serialized into any
//!   federated field, structurally** — the serializer below has no code
//!   path that emits it. Updates ride `Update(Article)`.
//! * Item privacy composes: every served note URI resolves through the
//!   existing note path on the fetching side, and the serving side only
//!   includes notes visible to the verified fetcher. A non-visible note in
//!   a visible collection resolves to nothing — by construction, never by
//!   special-casing.
//! * Advertisement: listed clips appear in the actor's `clips` collection
//!   reference (`/users/{id}/clips`); unlisted never do; neither appears
//!   in NodeInfo, search, or public timelines.
//! * Admin `federateClips`/`federatePages` switches (default OFF) AND the
//!   per-object `federate` flags (default OFF) gate everything: either off
//!   ⇒ local-only behavior, zero protocol footprint (global-off answers
//!   404 on these routes and omits the actor `clips` reference).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::{
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{json, Map, Value};

use crate::activitypub::actor::parse_url_host;
use crate::activitypub::format_millis;
use crate::activitypub::objects::{
    ap_context, PUBLIC_COLLECTION_URI,
};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::clip::Clip;
use crate::domain::page::Page;
use crate::domain::visibility::{is_visible, ViewerContext};
use crate::federation::fetch_gate::{authenticate_fetch_viewer, decide_fetch};

/// Page size for clip-collection paging (= the per-clip member cap, so a
/// full collection is always one page; the paging envelope still applies).
pub const CLIP_COLLECTION_PAGE_SIZE: usize = 200;

/// TTL for cached remote-clip membership (`resolve_remote_clip`): refetch
/// at most this often; `Add`/`Remove` deliveries converge faster in
/// between.
pub const REMOTE_CLIP_TTL: Duration = Duration::from_secs(10 * 60);

/// Clip collection URI (`{base}/clips/{id}`).
pub fn clip_collection_uri(base: &str, clip_id: &str) -> String {
    format!("{}/clips/{clip_id}", base.trim_end_matches('/'))
}

/// Actor clips-index URI (`{base}/users/{id}/clips`).
pub fn clips_index_uri(base: &str, account_id: &str) -> String {
    format!("{}/users/{account_id}/clips", base.trim_end_matches('/'))
}

/// Page Article URI (`{base}/pages/{id}`).
pub fn page_uri(base: &str, page_id: &str) -> String {
    format!("{}/pages/{page_id}", base.trim_end_matches('/'))
}

/// Escape text for HTML construction (the Article renderer builds markup
/// from escaped text + a fixed tag whitelist — safe by construction).
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

/// Attribute values additionally reject quotes + event-handler smuggling;
/// `None` when the URL is not an `http(s)` reference.
fn safe_http_url(url: &str) -> Option<String> {
    let url = url.trim();
    if url.starts_with("https://") || url.starts_with("http://") {
        if url.contains(['"', '\'', '<', '>', ' ', '\n', '\r', '\t']) {
            return None;
        }
        Some(url.to_string())
    } else {
        None
    }
}

/// Server-render page blocks to sanitized HTML (federation `content`).
/// Text/section structure is preserved; images become `<figure><img>` only
/// for drive files that exist locally (URLs constructed server-side, never
/// from block input); note blocks become quote links when the target
/// resolves, otherwise they are dropped. Unknown block types are skipped.
/// `script` is never read here — there is no code path from `script` to
/// this output.
pub fn render_page_html(services: &AppServices, page: &Page) -> String {
    let base = services.public_url.trim_end_matches('/');
    let mut out = String::new();
    render_blocks_into(services, base, &page.content, &mut out);
    out
}

fn render_blocks_into(
    services: &AppServices,
    base: &str,
    content: &Value,
    out: &mut String,
) {
    let Some(blocks) = content.as_array() else {
        return;
    };
    for block in blocks {
        let Some(obj) = block.as_object() else {
            continue;
        };
        match obj.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = obj.get("text").and_then(Value::as_str) {
                    out.push_str("<p>");
                    out.push_str(&escape_html(text));
                    out.push_str("</p>");
                }
            }
            Some("section") => {
                out.push_str("<section>");
                if let Some(title) = obj.get("title").and_then(Value::as_str) {
                    out.push_str("<h2>");
                    out.push_str(&escape_html(title));
                    out.push_str("</h2>");
                }
                if let Some(children) = obj.get("children") {
                    render_blocks_into(services, base, children, out);
                }
                out.push_str("</section>");
            }
            Some("image") => {
                if let Some(id) = obj.get("fileId").and_then(Value::as_str) {
                    if let Ok(Some(meta)) = services.media.get(id) {
                        let url = if meta.url.starts_with("http") {
                            meta.url.clone()
                        } else {
                            format!("{base}/files/{}", meta.id)
                        };
                        if let Some(safe) = safe_http_url(&url) {
                            out.push_str("<figure><img src=\"");
                            out.push_str(&safe);
                            out.push_str("\"></figure>");
                        }
                    }
                }
            }
            Some("note") => {
                if let Some(link) = render_note_block_link(services, base, obj) {
                    out.push_str("<blockquote><a href=\"");
                    out.push_str(&link);
                    out.push_str("\">");
                    out.push_str(&escape_html(&link));
                    out.push_str("</a></blockquote>");
                }
            }
            _ => {}
        }
    }
}

/// Quote link for a page `note` block: a local note id resolves to its
/// canonical URI; a remote `http(s)` id/URL passes through only when it
/// parses as one. Anything else is dropped (renders as nothing).
fn render_note_block_link(
    services: &AppServices,
    base: &str,
    obj: &Map<String, Value>,
) -> Option<String> {
    let target = obj.get("note")?;
    if let Some(id) = target.as_str() {
        if let Some(safe) = safe_http_url(id) {
            return Some(safe);
        }
        if let Ok(Some(note)) = services.notes.get_note(id) {
            return Some(
                note.uri
                    .clone()
                    .unwrap_or_else(|| format!("{base}/notes/{}", note.id)),
            );
        }
        return None;
    }
    if let Some(target_obj) = target.as_object() {
        for key in ["url", "id", "uri"] {
            if let Some(s) = target_obj.get(key).and_then(Value::as_str) {
                if let Some(safe) = safe_http_url(s) {
                    return Some(safe);
                }
            }
        }
    }
    None
}

/// Render the `Article` document for a page. `script` is structurally
/// excluded: no field below reads `page.script`.
pub fn render_page_article(
    services: &AppServices,
    page: &Page,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let owner = services
        .accounts
        .find_by_id(&page.owner_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let actor_uri = owner.actor_url(base);
    let uri = page_uri(base, &page.id);
    let mut attachments = Vec::new();
    if let Some(id) = &page.eye_catching_image_id {
        if let Some(meta) = services.media.get(id)? {
            let url = if meta.url.starts_with("http") {
                meta.url.clone()
            } else {
                format!("{base}/files/{}", meta.id)
            };
            attachments.push(json!({
                "type": if meta.mime.starts_with("image/") { "Image" } else { "Document" },
                "mediaType": meta.mime,
                "url": url,
                "name": meta.comment.clone().unwrap_or_else(|| meta.name.clone()),
            }));
        }
    }
    // Remote pages re-emit their origin attachments verbatim (http(s)
    // URLs filtered on ingest; never fetched by us).
    for att in &page.remote_attachments {
        attachments.push(att.clone());
    }
    // Audience mirrors tier + visibility (Phase 3 audit): Public ⇒
    // public collection; Followers ⇒ followers collection; Specified ⇒
    // addressed actors (local ids resolved to actor URIs; remote ids kept
    // verbatim); capability tiers carry no public claim. The fetch gate,
    // not this claim, enforces who reads.
    let (to, cc) = if page.tier != crate::domain::collection::CollectionTier::Listed {
        (Vec::new(), Vec::new())
    } else {
        match page.visibility {
            crate::domain::note::Visibility::Public
            | crate::domain::note::Visibility::Home => (
                vec![PUBLIC_COLLECTION_URI.to_string()],
                vec![format!("{actor_uri}/followers")],
            ),
            crate::domain::note::Visibility::Followers => (
                vec![format!("{actor_uri}/followers")],
                Vec::new(),
            ),
            crate::domain::note::Visibility::Specified => {
                let mut tos = Vec::new();
                let base = base.to_string();
                for id in &page.visible_user_ids {
                    if let Ok(Some(acc)) = services.accounts.find_by_id(id) {
                        tos.push(acc.actor_url(&base));
                    } else {
                        tos.push(id.clone());
                    }
                }
                (tos, Vec::new())
            }
        }
    };
    Ok(json!({
        "@context": ap_context(),
        "id": uri,
        "type": "Article",
        "attributedTo": actor_uri,
        "to": to,
        "cc": cc,
        "name": page.title,
        "summary": page.summary,
        "content": render_page_html(services, page),
        "url": uri,
        "published": format_millis(page.remote_published_ms.unwrap_or_else(|| services.ids.parse_time(&page.id).unwrap_or_else(|_| services.clock.now_ms()))),
        "updated": format_millis(page.remote_updated_ms.unwrap_or(page.updated_at_ms)),
        "attachment": attachments,
        "tag": [],
        "source": {
            "content": page.content,
            "variables": page.variables,
            "presentation": {
                "font": page.font.as_str(),
                "alignCenter": page.align_center,
                "hideTitleWhenPinned": page.hide_title_when_pinned,
            },
            "slug": page.name,
            "mediaType": "application/json",
        },
    }))
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
    let viewer_id = services
        .accounts
        .find_by_uri(actor_uri)?
        .map(|a| a.id);
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
pub fn render_clips_index(
    services: &AppServices,
    owner_id: &str,
) -> Result<Value, PortError> {
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

/// Render `Delete(Article)` for a page URI (outbound local delete).
pub fn render_page_delete_activity(
    base: &str,
    activity_id: &str,
    actor_uri: &str,
    page_uri: &str,
) -> Value {
    let base = base.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{activity_id}"),
        "type": "Delete",
        "actor": actor_uri,
        "object": {
            "id": page_uri,
            "type": "Article",
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

/// Render `Create`/`Update` wrapping a page `Article` for fan-out.
/// Audience matches the served `Article` (`to`/`cc` taken from the
/// article doc so Create/Update never over-claim).
pub fn render_page_activity(
    base: &str,
    activity_id: &str,
    actor_uri: &str,
    article: &Value,
    updated: bool,
    _followers_uri: &str,
) -> Value {
    let base = base.trim_end_matches('/');
    let to = article.get("to").cloned().unwrap_or(Value::Null);
    let cc = article.get("cc").cloned().unwrap_or(Value::Null);
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{activity_id}"),
        "type": if updated { "Update" } else { "Create" },
        "actor": actor_uri,
        "published": article.get("published").cloned().unwrap_or(Value::Null),
        "to": to,
        "cc": cc,
        "object": article,
    })
}

/// Render a `Like` targeting a page URI (pinetree↔pinetree; ignored
/// elsewhere). `activity_id` is the bare id segment.
pub fn render_page_like_activity(
    base: &str,
    activity_id: &str,
    actor_uri: &str,
    page_uri: &str,
) -> Value {
    let base = base.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{activity_id}"),
        "type": "Like",
        "actor": actor_uri,
        "object": page_uri,
        "content": "❤",
    })
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

fn ap_response(status: StatusCode, body: &Value) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static(
            "application/activity+json; charset=utf-8",
        ),
    );
    let payload = serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string());
    (status, headers, payload).into_response()
}

/// Whether the global federation switch for `kind` is on (`clips` /
/// `pages`; default OFF ⇒ zero protocol footprint).
pub fn federation_switch_on(services: &AppServices, kind: &str) -> bool {
    match services.instance_settings.get() {
        Ok(settings) => match kind {
            "clips" => settings.federate_clips,
            "pages" => settings.federate_pages,
            _ => false,
        },
        Err(_) => false,
    }
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
    let decision =
        AppServices::authorize_clip_fetch(&clip, true, viewer.actor_uri.as_deref());
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

/// `GET /pages/{id}` — serves the `Article` through the fetch gate.
/// Unknown/ungated pages 404.
pub async fn get_page_article(
    State(services): State<std::sync::Arc<AppServices>>,
    Path(id): Path<String>,
    OriginalUri(original): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let target = original.to_string();
    let page = match services.pages.get_page(&id) {
        Ok(Some(p)) => p,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if !federation_switch_on(&services, "pages") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let viewer = authenticate_fetch_viewer(&services, &target, &headers);
    let decision =
        AppServices::authorize_page_fetch(&page, true, viewer.actor_uri.as_deref());
    if let Err(status) = decide_fetch(&viewer, decision) {
        return status.into_response();
    }
    // Page-level read gating (Phase 1: gate lands before any non-public
    // Article becomes a page). Owner always passes; moderators bypass;
    // followers-only admits a *local* viewer following the owner; remote
    // viewers of Followers/Specified pages fail closed (same posture as
    // federated DMs).
    if page.visibility != crate::domain::note::Visibility::Public
        && page.visibility != crate::domain::note::Visibility::Home
    {
        // Local viewers pass via the same rule as `pages/show`
        // (`page_visible_to`); remote viewers of a Followers page fail
        // closed (their follow edge is not checkable — same posture as
        // federated DMs); Specified admits listed ids (local or cached).
        let allowed = match &viewer.actor_uri {
            Some(actor_uri) => {
                match services.accounts.find_by_uri(actor_uri) {
                    Ok(Some(acc)) => {
                        if acc.id == page.owner_id {
                            true
                        } else if acc.is_local()
                            && (acc.is_admin || acc.is_moderator)
                        {
                            true
                        } else {
                            match page.visibility {
                                crate::domain::note::Visibility::Followers => {
                                    acc.is_local()
                                        && services
                                            .follows
                                            .is_following(&acc.id, &page.owner_id)
                                            .unwrap_or(false)
                                }
                                crate::domain::note::Visibility::Specified => {
                                    page.visible_user_ids.iter().any(|id| id == &acc.id)
                                }
                                _ => false,
                            }
                        }
                    }
                    _ => false,
                }
            }
            None => false,
        };
        if !allowed {
            let status = match viewer.actor_uri {
                Some(_) => StatusCode::FORBIDDEN,
                None => StatusCode::NOT_FOUND,
            };
            return status.into_response();
        }
    }
    match render_page_article(&services, &page) {
        Ok(doc) => ap_response(StatusCode::OK, &doc),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

// ---------------------------------------------------------------------------
// Remote resolution (full-state pull + TTL)
// ---------------------------------------------------------------------------

/// One cached remote-clip membership pull.
#[derive(Debug, Clone)]
struct RemoteClipEntry {
    fetched_at: Instant,
    note_uris: Vec<String>,
    /// Last-seen ActivityStreams `updated` stamp of the collection doc
    /// (Phase 2a: revalidation compares it before full pulls).
    updated: Option<String>,
}

fn remote_clip_cache() -> &'static Mutex<HashMap<String, RemoteClipEntry>> {
    static CACHE: OnceLock<Mutex<HashMap<String, RemoteClipEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn remote_clip_cache_insert(uri: &str, note_uris: Vec<String>, updated: Option<String>) {
    let Ok(mut cache) = remote_clip_cache().lock() else {
        return;
    };
    if cache.len() >= 100 {
        // Evict the oldest entry (bounded process-memory cache, same
        // discipline as the `url-preview` cache).
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, e)| e.fetched_at)
            .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(
        uri.to_string(),
        RemoteClipEntry {
            fetched_at: Instant::now(),
            note_uris,
            updated,
        },
    );
}

/// Resolve a remote clip collection to its member note URIs (full-state
/// pull). Fresh cache entries (younger than [`REMOTE_CLIP_TTL`]) serve
/// without network I/O; `Add`/`Remove` deliveries converge faster in
/// between. Unsigned fetch (listed collections are public); unlisted
/// collections resolve by capability URI; `RemoteApproved` collections
/// need a signed fetch the caller performs itself.
pub fn resolve_remote_clip(
    services: &AppServices,
    clip_uri: &str,
) -> Result<Vec<String>, PortError> {
    resolve_remote_clip_with_ttl(services, clip_uri, REMOTE_CLIP_TTL)
}

/// [`resolve_remote_clip`] with an explicit TTL (the integration seam:
/// unit tests pin revalidation behavior deterministically).
pub fn resolve_remote_clip_with_ttl(
    services: &AppServices,
    clip_uri: &str,
    ttl: Duration,
) -> Result<Vec<String>, PortError> {
    if let Ok(cache) = remote_clip_cache().lock() {
        if let Some(entry) = cache.get(clip_uri) {
            if entry.fetched_at.elapsed() < ttl {
                return Ok(entry.note_uris.clone());
            }
        }
    }
    let client = &services.federation_http;
    let fetch_doc = |url: &str| -> Result<Value, PortError> {
        let resp = client
            .send(
                "GET",
                url,
                &[("Accept", "application/activity+json")],
                None,
                65_536,
            )
            .map_err(|e| PortError::StorageUnavailable(format!("clip fetch failed: {e}")))?;
        if resp.status != 200 {
            return Err(PortError::StorageUnavailable(format!(
                "clip fetch status {}",
                resp.status
            )));
        }
        serde_json::from_slice(&resp.body)
            .map_err(|e| PortError::StorageUnavailable(format!("clip doc invalid: {e}")))
    };
    let doc = fetch_doc(clip_uri)?;
    // `updated` fast-path (Phase 2a): when the served collection carries
    // an `updated` stamp equal to the cached stamp, reuse the cached
    // membership without walking `first` — the doc fetch itself is the
    // revalidation, the item pull is what we skip.
    if let Some(remote_updated) = doc.get("updated").and_then(Value::as_str) {
        let hit = remote_clip_cache()
            .lock()
            .ok()
            .and_then(|c| c.get(clip_uri).cloned());
        if let Some(entry) = hit {
            if entry.updated.as_deref() == Some(remote_updated) {
                remote_clip_cache_insert(clip_uri, entry.note_uris.clone(), entry.updated);
                if let Ok(cache) = remote_clip_cache().lock() {
                    if let Some(e) = cache.get(clip_uri) {
                        return Ok(e.note_uris.clone());
                    }
                }
            }
        }
    }
    let served_updated = doc
        .get("updated")
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut uris = Vec::new();
    // Full-state: inline `orderedItems` or one `first` page hop (the
    // serving side always fits a full collection on one page).
    let mut pages = vec![doc];
    for _ in 0..2 {
        let Some(current) = pages.pop() else {
            break;
        };
        if let Some(items) = current.get("orderedItems").and_then(Value::as_array) {
            for item in items {
                if let Some(uri) = item.as_str() {
                    if uris.len() < CLIP_COLLECTION_PAGE_SIZE && !uris.contains(&uri.to_string()) {
                        uris.push(uri.to_string());
                    }
                }
            }
            break;
        }
        if let Some(first) = current.get("first").and_then(Value::as_str) {
            pages.push(fetch_doc(first)?);
        } else {
            break;
        }
    }
    remote_clip_cache_insert(clip_uri, uris.clone(), served_updated);
    Ok(uris)
}

/// Ingest an embedded `OrderedCollection` document into the membership
/// cache (Phase 2a `Update(Collection)` fast path: one round trip, no
/// refetch). Extracts string `orderedItems` (capped) + the `updated`
/// stamp. Idempotent — replays converge.
pub fn ingest_remote_clip_doc(clip_uri: &str, doc: &Value) {
    let mut uris = Vec::new();
    if let Some(items) = doc.get("orderedItems").and_then(Value::as_array) {
        for item in items {
            if let Some(uri) = item.as_str() {
                if uris.len() < CLIP_COLLECTION_PAGE_SIZE && !uris.contains(&uri.to_string()) {
                    uris.push(uri.to_string());
                }
            }
        }
    }
    let updated = doc
        .get("updated")
        .and_then(Value::as_str)
        .map(str::to_string);
    remote_clip_cache_insert(clip_uri, uris, updated);
}

/// Test hook: forget one cached remote-clip entry.
#[cfg(test)]
pub fn forget_remote_clip_cache(uri: &str) {
    if let Ok(mut cache) = remote_clip_cache().lock() {
        cache.remove(uri);
    }
}

/// Introspection for tests: URIs currently held in the remote-clip
/// cache (process-memory, bounded — same discipline as the `url-preview`
/// cache).
pub fn remote_clip_cached_uris() -> Vec<String> {
    remote_clip_cache()
        .lock()
        .map(|cache| cache.keys().cloned().collect())
        .unwrap_or_default()
}

/// Introspection for tests: cached membership URIs for one collection
/// (`None` = not cached — never fetched or evicted).
pub fn remote_clip_cached_for(uri: &str) -> Option<Vec<String>> {
    remote_clip_cache()
        .lock()
        .ok()?
        .get(uri)
        .map(|e| e.note_uris.clone())
}

/// Evict one cached remote-clip entry (inbound `Add`/`Remove` promptness;
/// idempotent — evicting an absent entry is a no-op).
pub fn evict_remote_clip_cache(uri: &str) {
    if let Ok(mut cache) = remote_clip_cache().lock() {
        cache.remove(uri);
    }
}

/// Fetch a remote collection document (signed GET as the system actor)
/// and report its `attributedTo` owner. `None` when the collection is
/// unreachable, gated for us, or malformed — callers treat that as
/// "cannot verify ownership" and ignore the activity.
pub fn fetch_collection_owner(services: &AppServices, clip_uri: &str) -> Option<String> {
    let client = &services.federation_http;
    let doc = crate::federation::delivery::signed_get_json(
        services,
        client,
        clip_uri,
        "application/activity+json",
    )
    .ok()?;
    doc.get("attributedTo").and_then(Value::as_str).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_services() -> AppServices {
        crate::application::commands::tests::test_services("fed-collections")
    }

    fn test_page(owner: &str) -> Page {
        Page {
            id: "aid000000000001".to_string(),
            owner_id: owner.to_string(),
            title: "T <b>esc</b>".to_string(),
            name: "t".to_string(),
            summary: Some("S".to_string()),
            content: serde_json::json!([
                { "type": "text", "text": "Hello <world> & friends" },
                { "type": "section", "title": "Sec \"q\"", "children": [
                    { "type": "text", "text": "inner" },
                ] },
                { "type": "image", "fileId": "missing" },
                { "type": "note", "note": "https://quotes.example/x" },
                { "type": "note", "note": { "url": "javascript:alert(1)" } },
                { "type": "mystery", "x": 1 },
            ]),
            variables: serde_json::json!([{"name": "v"}]),
            script: "let evil = `run()`; <script>alert(1)</script>".to_string(),
            eye_catching_image_id: None,
            font: crate::domain::page::PageFont::Serif,
            align_center: true,
            hide_title_when_pinned: false,
            tier: crate::domain::collection::CollectionTier::Listed,
            visibility: crate::domain::note::Visibility::Public,
            visible_user_ids: Vec::new(),
            federate: true,
            approved_actor_uris: Vec::new(),
            uri: None,
            remote_attachments: Vec::new(),
            remote_published_ms: None,
            remote_updated_ms: None,
            liked_count: 0,
            created_at_ms: 1,
            updated_at_ms: 2,
        }
    }

    /// The Article serializer emits no `script` bytes: serialize every
    /// federated field and grep. (Phase 3 gate.)
    #[test]
    fn article_never_carries_script_bytes() {
        let services = test_services();
        let alice = services.create_account("alice", None).unwrap();
        let mut page = test_page(&alice.id);
        page.script = "let s3cret = `run()`; alert(1)".to_string();
        let article = render_page_article(&services, &page).unwrap();
        let serialized = serde_json::to_string(&article).unwrap();
        assert!(!serialized.contains("s3cret"), "{serialized}");
        assert!(!serialized.contains("<script>"), "{serialized}");
        assert!(!serialized.contains("run()`"), "{serialized}");
        // ...but the local pack still carries the script (local parity).
        let packed = crate::api::misskey::packers::pack_page(&page, None, &services).unwrap();
        assert_eq!(
            packed["script"],
            Value::String("let s3cret = `run()`; alert(1)".to_string())
        );
        // And the plain-text body renders escaped, with structure kept.
        let content = article["content"].as_str().unwrap();
        assert!(content.contains("<p>Hello &lt;world&gt; &amp; friends</p>"), "{content}");
        assert!(content.contains("<section><h2>Sec &quot;q&quot;</h2>"), "{content}");
        assert!(content.contains("<p>inner</p>"), "{content}");
        // The well-formed quote link survives; unknown blocks dropped;
        // bad-URL note links dropped.
        assert!(content.contains("<a href=\"https://quotes.example/x\">"), "{content}");
        assert!(!content.contains("mystery"), "{content}");
        assert!(!content.contains("javascript:"), "{content}");
        assert_eq!(article["name"], Value::String("T <b>esc</b>".to_string()));
        // `source` carries blocks + variables for pinetree↔pinetree
        // fidelity (and still no script).
        assert!(article["source"]["content"].is_array());
        assert!(article["source"].get("script").is_none());
    }

    /// Full-state pull + TTL revalidation against a loopback collection
    /// server (hit-counted): fresh entries serve without network I/O, a
    /// zero TTL always revalidates, and eviction forces a re-pull.
    /// (Phase 2 gate, deterministic half — the live half is the
    /// two-pinetree test in `tests/federation.rs`.)
    #[test]
    fn remote_clip_pull_caches_and_revalidates() {
        use std::io::{Read, Write};
        use std::sync::atomic::{AtomicUsize, Ordering};

        static HITS: AtomicUsize = AtomicUsize::new(0);
        HITS.store(0, Ordering::SeqCst);
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("loopback addr");
        let body = serde_json::json!({
            "@context": ["https://www.w3.org/ns/activitystreams"],
            "id": format!("http://{addr}/clips/1"),
            "type": "OrderedCollection",
            "totalItems": 2,
            "orderedItems": [
                format!("http://{addr}/notes/1"),
                format!("http://{addr}/notes/2"),
            ],
        });
        let payload = serde_json::to_vec(&body).unwrap();
        std::thread::spawn(move || {
            for _ in 0..16 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                if req.starts_with("GET /clips/1") {
                    HITS.fetch_add(1, Ordering::SeqCst);
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/activity+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        payload.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&payload);
                } else {
                    let _ = stream.write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope",
                    );
                }
            }
        });

        let services = test_services();
        let uri = format!("http://{addr}/clips/1");
        forget_remote_clip_cache(&uri);
        // Pull populates the cache (one hit).
        let first =
            resolve_remote_clip_with_ttl(&services, &uri, Duration::from_secs(3600)).unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(HITS.load(Ordering::SeqCst), 1);
        // Fresh entry: no second hit.
        let second =
            resolve_remote_clip_with_ttl(&services, &uri, Duration::from_secs(3600)).unwrap();
        assert_eq!(second, first);
        assert_eq!(HITS.load(Ordering::SeqCst), 1);
        // Zero TTL: always revalidates (second hit).
        let third = resolve_remote_clip_with_ttl(&services, &uri, Duration::ZERO).unwrap();
        assert_eq!(third, first);
        assert_eq!(HITS.load(Ordering::SeqCst), 2);
        // Eviction forces a re-pull (third hit).
        evict_remote_clip_cache(&uri);
        let fourth =
            resolve_remote_clip_with_ttl(&services, &uri, Duration::from_secs(3600)).unwrap();
        assert_eq!(fourth, first);
        assert_eq!(HITS.load(Ordering::SeqCst), 3);
        // Errors never populate the cache.
        let missing = format!("http://{addr}/clips/missing");
        assert!(resolve_remote_clip_with_ttl(&services, &missing, Duration::ZERO).is_err());
        assert!(!remote_clip_cached_uris().contains(&missing));
        forget_remote_clip_cache(&uri);
    }

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
