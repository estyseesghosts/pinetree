//! Federated pages: `Article` documents, page activities, and the
//! fetch-gated `GET /pages/{id}` serving layer.

use axum::{
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Map, Value, json};

use crate::activitypub::format_millis;
use crate::activitypub::objects::{PUBLIC_COLLECTION_URI, ap_context};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::page::Page;
use crate::federation::fetch_gate::{authenticate_fetch_viewer, decide_fetch};

use super::serving::{ap_response, federation_switch_on};

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

fn render_blocks_into(services: &AppServices, base: &str, content: &Value, out: &mut String) {
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
pub fn render_page_article(services: &AppServices, page: &Page) -> Result<Value, PortError> {
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
            crate::domain::note::Visibility::Public | crate::domain::note::Visibility::Home => (
                vec![PUBLIC_COLLECTION_URI.to_string()],
                vec![format!("{actor_uri}/followers")],
            ),
            crate::domain::note::Visibility::Followers => {
                (vec![format!("{actor_uri}/followers")], Vec::new())
            }
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
    let decision = AppServices::authorize_page_fetch(&page, true, viewer.actor_uri.as_deref());
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
            Some(actor_uri) => match services.accounts.find_by_uri(actor_uri) {
                Ok(Some(acc)) => {
                    if acc.id == page.owner_id {
                        true
                    } else if acc.is_local() && (acc.is_admin || acc.is_moderator) {
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
            },
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
        assert!(
            content.contains("<p>Hello &lt;world&gt; &amp; friends</p>"),
            "{content}"
        );
        assert!(
            content.contains("<section><h2>Sec &quot;q&quot;</h2>"),
            "{content}"
        );
        assert!(content.contains("<p>inner</p>"), "{content}");
        // The well-formed quote link survives; unknown blocks dropped;
        // bad-URL note links dropped.
        assert!(
            content.contains("<a href=\"https://quotes.example/x\">"),
            "{content}"
        );
        assert!(!content.contains("mystery"), "{content}");
        assert!(!content.contains("javascript:"), "{content}");
        assert_eq!(article["name"], Value::String("T <b>esc</b>".to_string()));
        // `source` carries blocks + variables for pinetree↔pinetree
        // fidelity (and still no script).
        assert!(article["source"]["content"].is_array());
        assert!(article["source"].get("script").is_none());
    }
}
