//! First-party link preview (Batch O WP12, Keystone-requested).
//!
//! Not a Misskey endpoint: upstream renders previews on its web server
//! (`UrlPreviewService`). Pinetree exposes `POST /api/url-preview {url}`
//! with Keystone's shape `{title?, description?, siteName?, image?, icon?}`
//! (`frontend/src/api/linkpreview.ts:18-24`).
//!
//! Rules: credential required (anonymous preview is an open proxy); the
//! §2-rule-6 fetcher (SSRF guard, timeouts, redirect cap, size cap); HTML
//! only (non-HTML answers return `{}`); dependency-free `<head>` parse
//! (`og:*` + `<title>` fallback only). Results cache in process memory
//! (cap + TTL) — explicitly NOT in the store, so there is no FDB impact.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use serde_json::Value;

use crate::federation::delivery::SafeHttpClient;

/// Keystone's head cap (`linkpreview.ts:28`).
pub const PREVIEW_MAX_BYTES: usize = 256 * 1024;
/// Upstream `MemoryKVCache` shape: 100 entries, 1h TTL.
const PREVIEW_CACHE_MAX_ENTRIES: usize = 100;
const PREVIEW_CACHE_TTL_MS: i64 = 3_600_000;

struct PreviewCache {
    rows: HashMap<String, (Value, i64)>,
}

fn cache() -> &'static Mutex<PreviewCache> {
    static CACHE: OnceLock<Mutex<PreviewCache>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(PreviewCache {
            rows: HashMap::new(),
        })
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Fetch + parse a preview (successes only are cached).
pub fn fetch_preview(client: &SafeHttpClient, url: &str) -> Result<Value, String> {
    let key = url.trim().to_string();
    {
        let guard = cache()
            .lock()
            .map_err(|_| "preview cache poisoned".to_string())?;
        if let Some((value, at)) = guard.rows.get(&key) {
            if now_ms().saturating_sub(*at) <= PREVIEW_CACHE_TTL_MS {
                return Ok(value.clone());
            }
        }
    }
    let value = fetch_preview_uncached(client, &key)?;
    if let Ok(mut guard) = cache().lock() {
        if guard.rows.len() >= PREVIEW_CACHE_MAX_ENTRIES {
            // Evict the oldest entry (small map; linear scan is fine).
            if let Some(oldest) = guard
                .rows
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(k, _)| k.clone())
            {
                guard.rows.remove(&oldest);
            }
        }
        guard.rows.insert(key, (value.clone(), now_ms()));
    }
    Ok(value)
}

fn fetch_preview_uncached(client: &SafeHttpClient, url: &str) -> Result<Value, String> {
    let resp = client
        .send("GET", url, &[], None, PREVIEW_MAX_BYTES)
        .map_err(|e| e.to_string())?;
    if !(200..300).contains(&resp.status) {
        return Err(format!("preview GET returned HTTP {}", resp.status));
    }
    let content_type = resp.header("content-type").unwrap_or("").to_lowercase();
    // HTML only: never proxy arbitrary content types or full bodies.
    if !content_type.is_empty() && !content_type.contains("html") && !content_type.contains("xhtml")
    {
        return Ok(Value::Object(Default::default()));
    }
    let head = String::from_utf8_lossy(&resp.body);
    Ok(parse_preview_head(&head, &resp.final_url))
}

/// Minimal `<head>` parse: OpenGraph + `<title>` + icon link. Unknown or
/// non-absolute asset URLs are dropped (never proxy); only `http(s)`
/// targets survive.
pub fn parse_preview_head(head: &str, page_url: &str) -> Value {
    let head_end = head.find("</head>").map(|i| i + 7).unwrap_or(head.len());
    let head = &head[..head_end.min(head.len())];
    let mut title: Option<String> = None;
    let mut description: Option<String> = None;
    let mut site_name: Option<String> = None;
    let mut image: Option<String> = None;
    let mut icon: Option<String> = None;

    for tag in meta_tags(head) {
        let (name, property, content) = (&tag.0, &tag.1, &tag.2);
        let content = content.trim();
        if content.is_empty() {
            continue;
        }
        match property.to_lowercase().as_str() {
            "og:title" if title.is_none() => title = Some(content.to_string()),
            "og:description" if description.is_none() => {
                description = Some(content.to_string());
            }
            "og:site_name" if site_name.is_none() => {
                site_name = Some(content.to_string());
            }
            "og:image" if image.is_none() => {
                if let Some(abs) = absolutize(page_url, content) {
                    image = Some(abs);
                }
            }
            _ => {
                if name.eq_ignore_ascii_case("description") && description.is_none() {
                    description = Some(content.to_string());
                }
            }
        }
    }
    if title.is_none() {
        title = title_tag(head);
    }
    if icon.is_none() {
        icon = icon_link(head, page_url);
    }

    let mut out = serde_json::Map::new();
    if let Some(v) = title.map(clean) {
        out.insert("title".to_string(), Value::String(v));
    }
    if let Some(v) = description.map(clean) {
        out.insert("description".to_string(), Value::String(v));
    }
    if let Some(v) = site_name.map(clean) {
        out.insert("siteName".to_string(), Value::String(v));
    }
    if let Some(v) = image {
        out.insert("image".to_string(), Value::String(v));
    }
    if let Some(v) = icon {
        out.insert("icon".to_string(), Value::String(v));
    }
    Value::Object(out)
}

fn clean(raw: String) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Extract `(name, property, content)` triples from `<meta ...>` tags.
fn meta_tags(head: &str) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    let lower = head.to_lowercase();
    let mut rest = lower.as_str();
    let mut offset = 0usize;
    while let Some(start) = rest.find("<meta") {
        let abs = offset + start;
        let tag_src = &head[abs..];
        let end = tag_src.find('>').map(|i| abs + i).unwrap_or(head.len());
        let tag = &head[abs..end.min(head.len())];
        out.push((
            attr(tag, "name").unwrap_or_default(),
            attr(tag, "property").unwrap_or_default(),
            attr(tag, "content").unwrap_or_default(),
        ));
        let next = end + 1;
        if next >= head.len() {
            break;
        }
        offset = next;
        rest = &lower[next..];
    }
    out
}

fn attr(tag: &str, name: &str) -> Option<String> {
    let lower = tag.to_lowercase();
    let mut rest = lower.as_str();
    let mut skipped = 0usize;
    loop {
        let pos = rest.find(name)?;
        let abs = skipped + pos;
        let after = abs + name.len();
        let bytes = tag.as_bytes();
        // Attribute name must start at a boundary and be followed by `=`.
        let boundary = abs == 0 || !bytes[abs - 1].is_ascii_alphanumeric();
        if boundary && bytes.get(after) == Some(&b'=') {
            let raw = tag[after + 1..].trim_start();
            let quote = raw.as_bytes().first().copied().unwrap_or(b' ');
            if quote == b'"' || quote == b'\'' {
                let end = raw[1..]
                    .find(quote as char)
                    .map(|i| i + 1)
                    .unwrap_or(raw.len());
                return Some(raw[1..end.min(raw.len())].to_string());
            }
            let end = raw
                .find(|c: char| c.is_whitespace() || c == '>')
                .unwrap_or(raw.len());
            return Some(raw[..end].to_string());
        }
        skipped = abs + name.len();
        rest = &lower[skipped.min(lower.len())..];
        if skipped >= lower.len() {
            return None;
        }
    }
}

fn tag_text(head: &str, tag: &str) -> Option<String> {
    let lower = head.to_lowercase();
    let open = lower.find(&format!("<{tag}"))?;
    let start = head[open..].find('>')? + open + 1;
    let close = lower[start..].find(&format!("</{tag}>"))? + start;
    let text = head[start..close].trim();
    if text.is_empty() {
        None
    } else {
        Some(html_unescape(text))
    }
}

fn title_tag(head: &str) -> Option<String> {
    tag_text(head, "title")
}

fn html_unescape(raw: &str) -> String {
    raw.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
}

/// First icon-ish `<link>` (`rel` containing `icon`), absolutized.
fn icon_link(head: &str, page_url: &str) -> Option<String> {
    let lower = head.to_lowercase();
    let mut rest = lower.as_str();
    let mut offset = 0usize;
    while let Some(start) = rest.find("<link") {
        let abs = offset + start;
        let tag_src = &head[abs..];
        let end = tag_src.find('>').map(|i| abs + i).unwrap_or(head.len());
        let tag = &head[abs..end.min(head.len())];
        let rel = attr(tag, "rel").unwrap_or_default().to_lowercase();
        if rel.split_whitespace().any(|t| t.contains("icon")) {
            if let Some(href) = attr(tag, "href") {
                if let Some(abs) = absolutize(page_url, href.trim()) {
                    return Some(abs);
                }
            }
        }
        let next = end + 1;
        if next >= head.len() {
            break;
        }
        offset = next;
        rest = &lower[next..];
    }
    None
}

/// Absolutize `href` against `page_url`; only `http(s)` results survive.
fn absolutize(page_url: &str, href: &str) -> Option<String> {
    if href.is_empty() {
        return None;
    }
    if let Some(rest) = href
        .strip_prefix("https://")
        .or_else(|| href.strip_prefix("http://"))
    {
        if !rest.is_empty() {
            return Some(href.to_string());
        }
        return None;
    }
    if href.starts_with("//") {
        let scheme = page_url.split("://").next().unwrap_or("https");
        if scheme == "http" || scheme == "https" {
            return Some(format!("{scheme}:{href}"));
        }
        return None;
    }
    // Root-absolute or relative path: join with the page origin + dir.
    // Anything else with a scheme (`data:`, `javascript:`, …) is dropped.
    if href.contains(':') {
        return None;
    }
    let (origin, dir) = page_origin_dir(page_url)?;
    if let Some(path) = href.strip_prefix('/') {
        return Some(format!("{origin}/{path}"));
    }
    Some(format!("{dir}/{href}"))
}

fn page_origin_dir(page_url: &str) -> Option<(String, String)> {
    let (scheme, rest) = page_url.split_once("://")?;
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let host_end = rest.find('/').unwrap_or(rest.len());
    let origin = format!("{scheme}://{}", &rest[..host_end]);
    let path = &rest[host_end..];
    let dir = match path.rfind('/') {
        Some(0) | None => String::new(),
        Some(i) => path[..i].to_string(),
    };
    Some((origin.clone(), format!("{origin}{dir}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_og_and_falls_back_to_title() {
        let html = r#"<html><head>
<meta property="og:title" content="OG Title">
<meta property="og:description" content="OG Desc">
<meta property="og:site_name" content="Example">
<meta property="og:image" content="/img.png">
<link rel="icon" href="/favicon.ico">
<title>Fallback</title>
</head></html>"#;
        let v = parse_preview_head(html, "https://example.com/page");
        assert_eq!(v["title"], "OG Title");
        assert_eq!(v["description"], "OG Desc");
        assert_eq!(v["siteName"], "Example");
        assert_eq!(v["image"], "https://example.com/img.png");
        assert_eq!(v["icon"], "https://example.com/favicon.ico");

        let html = r#"<html><head><title>Only Title</title></head></html>"#;
        let v = parse_preview_head(html, "https://example.com/");
        assert_eq!(v["title"], "Only Title");
        assert!(v.get("image").is_none());
    }

    #[test]
    fn drops_non_http_asset_urls() {
        let html = r#"<html><head>
<meta property="og:image" content="javascript:alert(1)">
<link rel="icon" href="data:image/png;base64,aaa">
</head></html>"#;
        let v = parse_preview_head(html, "https://example.com/");
        assert!(v.get("image").is_none());
        assert!(v.get("icon").is_none());
    }
}
