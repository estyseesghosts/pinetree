//! ActivityPub JSON-LD `@context`, object/activity rendering and parsing,
//! `to`/`cc` ↔ `Visibility` audience mapping, and HTML ↔ MFM conversion +
//! sanitization (Phase 2 Slices B1 & B2).
//!
//! Never emits deprecated `_misskey_content` (uses `source: { content,
//! mediaType: "text/x.misskeymarkdown" }` instead). Reactions use
//! `_misskey_reaction` alongside `content` per proposal §7.

use std::collections::HashSet;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::activitypub::webfinger::extract_host;
use crate::api::misskey::ApiState;
use crate::api::misskey::packers::format_millis;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::Account;
use crate::domain::media::MediaMeta;
use crate::domain::note::{Note, Visibility};
use crate::domain::reaction::FALLBACK_REACTION;
use crate::domain::visibility::{ViewerContext, is_visible};

/// Canonical ActivityPub content-type header value.
pub const AP_CONTENT_TYPE: &str = "application/activity+json; charset=utf-8";

/// Public audience URI constants recognized by ActivityPub peers.
pub const PUBLIC_COLLECTION_URI: &str = "https://www.w3.org/ns/activitystreams#Public";
pub const PUBLIC_COLLECTION_COMPACT_1: &str = "as:Public";
pub const PUBLIC_COLLECTION_COMPACT_2: &str = "Public";

/// Check whether an incoming `Accept` header requests ActivityPub JSON-LD
/// (or is absent / `*/*`). Returns `false` only when the client explicitly
/// asks for `text/html` without accepting ActivityPub or JSON.
pub fn is_ap_accept(headers: &HeaderMap) -> bool {
    let Some(raw) = headers.get(header::ACCEPT).and_then(|v| v.to_str().ok()) else {
        return true;
    };
    let lower = raw.to_ascii_lowercase();
    if lower.contains("application/activity+json")
        || lower.contains("application/ld+json")
        || lower.contains("application/json")
    {
        return true;
    }
    if lower.contains("text/html") || lower.contains("application/xhtml+xml") {
        return false;
    }
    true
}

/// Canonical `@context` array matching Misskey/Sharkey (`context.ts`),
/// omitting deprecated `_misskey_content`.
pub fn ap_context() -> Value {
    json!([
        "https://www.w3.org/ns/activitystreams",
        "https://w3id.org/security/v1",
        {
            "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
            "sensitive": "as:sensitive",
            "Hashtag": "as:Hashtag",
            "quoteUrl": "as:quoteUrl",
            "toot": "http://joinmastodon.org/ns#",
            "Emoji": "toot:Emoji",
            "featured": "toot:featured",
            "discoverable": "toot:discoverable",
            "indexable": "toot:indexable",
            "schema": "http://schema.org#",
            "PropertyValue": "schema:PropertyValue",
            "value": "schema:value",
            "misskey": "https://misskey-hub.net/ns#",
            "_misskey_summary": "misskey:_misskey_summary",
            "_misskey_quote": "misskey:_misskey_quote",
            "_misskey_Reaction": "misskey:_misskey_Reaction",
            "_misskey_reaction": "misskey:_misskey_reaction",
            "_misskey_talk": "misskey:_misskey_talk",
            "_misskey_requireSigninToViewContents": "misskey:_misskey_requireSigninToViewContents",
            "_misskey_makeNotesHiddenBefore": "misskey:_misskey_makeNotesHiddenBefore",
            "_misskey_license": "misskey:_misskey_license",
            "isCat": "misskey:isCat",
            "vcard": "http://www.w3.org/2006/vcard/ns#"
        }
    ])
}

/// Return true if `uri` is one of the ActivityStreams Public collection URIs.
pub fn is_public_collection_uri(uri: &str) -> bool {
    matches!(
        uri.trim(),
        PUBLIC_COLLECTION_URI | PUBLIC_COLLECTION_COMPACT_1 | PUBLIC_COLLECTION_COMPACT_2
    )
}

/// Extract a list of URI strings from an ActivityPub `to` or `cc` JSON value
/// (accepting a single string, an object with `"id"`, or an array).
pub fn extract_uri_list(val: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    let Some(val) = val else {
        return out;
    };
    let mut push_one = |item: &Value| match item {
        Value::String(s) if !s.trim().is_empty() => out.push(s.trim().to_string()),
        Value::Object(m) => {
            if let Some(id) = m.get("id").and_then(Value::as_str) {
                if !id.trim().is_empty() {
                    out.push(id.trim().to_string());
                }
            }
        }
        _ => {}
    };
    match val {
        Value::Array(arr) => {
            for item in arr {
                push_one(item);
            }
        }
        other => push_one(other),
    }
    out
}

/// Map a local note's `(visibility, visible_user_ids, mentions)` onto
/// ActivityPub `(to, cc)` arrays matching `ApAudienceService.renderNoteAudience`.
pub fn render_note_audience(
    services: &AppServices,
    author: &Account,
    note: &Note,
) -> (Vec<String>, Vec<String>) {
    let base = services.public_url.trim_end_matches('/');
    let followers_collection = format!("{}/followers", author.actor_url(base));

    let mut mention_uris = Vec::new();
    for uid in &note.mentions {
        if let Ok(Some(acc)) = services.accounts.find_by_id(uid) {
            let uri = acc.actor_url(base);
            if !uri.is_empty() && !mention_uris.contains(&uri) {
                mention_uris.push(uri);
            }
        }
    }

    match note.visibility {
        Visibility::Public => {
            let to = vec![PUBLIC_COLLECTION_URI.to_string()];
            let mut cc = vec![followers_collection];
            for u in mention_uris {
                if !cc.contains(&u) {
                    cc.push(u);
                }
            }
            (to, cc)
        }
        Visibility::Home => {
            let to = vec![followers_collection];
            let mut cc = vec![PUBLIC_COLLECTION_URI.to_string()];
            for u in mention_uris {
                if !cc.contains(&u) {
                    cc.push(u);
                }
            }
            (to, cc)
        }
        Visibility::Followers => {
            let to = vec![followers_collection];
            let cc = mention_uris;
            (to, cc)
        }
        Visibility::Specified => {
            let mut to = Vec::new();
            for uid in &note.visible_user_ids {
                if let Ok(Some(acc)) = services.accounts.find_by_id(uid) {
                    let uri = acc.actor_url(base);
                    if !uri.is_empty() && !to.contains(&uri) {
                        to.push(uri);
                    }
                }
            }
            for u in mention_uris {
                if !to.contains(&u) {
                    to.push(u);
                }
            }
            (to, Vec::new())
        }
    }
}

/// Map incoming ActivityPub `(to, cc)` lists and the author's `followers`
/// collection URI onto `(Visibility, specified_actor_uris)` per
/// `ApAudienceService.resolveNoteAudience`.
pub fn parse_note_audience(
    to: &[String],
    cc: &[String],
    author_followers_uri: Option<&str>,
) -> (Visibility, Vec<String>) {
    let to_public = to.iter().any(|u| is_public_collection_uri(u));
    let cc_public = cc.iter().any(|u| is_public_collection_uri(u));

    let is_followers_uri = |u: &str| -> bool {
        if let Some(expected) = author_followers_uri {
            if u == expected {
                return true;
            }
        }
        u.ends_with("/followers")
    };

    let has_followers = to.iter().chain(cc.iter()).any(|u| is_followers_uri(u));

    let visibility = if to_public {
        Visibility::Public
    } else if cc_public {
        Visibility::Home
    } else if has_followers {
        Visibility::Followers
    } else {
        Visibility::Specified
    };

    let mut recipient_uris = Vec::new();
    for u in to.iter().chain(cc.iter()) {
        if !is_public_collection_uri(u) && !is_followers_uri(u) && !recipient_uris.contains(u) {
            recipient_uris.push(u.clone());
        }
    }

    (visibility, recipient_uris)
}

// ---------------------------------------------------------------------------
// Slice B2: MFM ↔ HTML conversion and sanitization
// ---------------------------------------------------------------------------

pub fn html_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

fn html_unescape(input: &str) -> String {
    input
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&#x27;", "'")
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
}

/// Convert local MFM / plain note text into safe ActivityPub HTML (`<p>`,
/// `<br>`, `<a class="u-url mention">`, `#hashtag` links).
pub fn mfm_to_html(text: &str, public_url: &str) -> String {
    let base = public_url.trim_end_matches('/');
    let mut rendered_lines = Vec::new();
    for line in text.split('\n') {
        rendered_lines.push(render_mfm_line(line, base));
    }
    format!("<p><span>{}</span></p>", rendered_lines.join("<br>"))
}

fn render_mfm_line(line: &str, base_url: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::with_capacity(line.len() + 16);
    let mut i = 0;
    while i < chars.len() {
        let boundary = i == 0 || !(chars[i - 1].is_alphanumeric() || chars[i - 1] == '_');
        if chars[i] == '@' && boundary {
            // Parse @user or @user@host
            let mut j = i + 1;
            while j < chars.len()
                && (chars[j].is_ascii_alphanumeric()
                    || chars[j] == '_'
                    || chars[j] == '.'
                    || chars[j] == '-')
            {
                j += 1;
            }
            if j > i + 1 {
                let mut host_end = j;
                if j < chars.len() && chars[j] == '@' {
                    let mut k = j + 1;
                    while k < chars.len()
                        && (chars[k].is_ascii_alphanumeric()
                            || chars[k] == '.'
                            || chars[k] == '-'
                            || chars[k] == ':')
                    {
                        k += 1;
                    }
                    // Trim trailing dot
                    while k > j + 1 && chars[k - 1] == '.' {
                        k -= 1;
                    }
                    if k > j + 1 {
                        host_end = k;
                    }
                }
                let mention: String = chars[i..host_end].iter().collect();
                let username: String = chars[i + 1..j].iter().collect();
                let href = if host_end > j {
                    let host: String = chars[j + 1..host_end].iter().collect();
                    format!("https://{host}/@{username}")
                } else {
                    format!("{base_url}/@{username}")
                };
                out.push_str(&format!(
                    "<a href=\"{}\" class=\"u-url mention\">{}</a>",
                    html_escape(&href),
                    html_escape(&mention)
                ));
                i = host_end;
                continue;
            }
        } else if chars[i] == '#' && boundary {
            let mut j = i + 1;
            while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            if j > i + 1 {
                let tag: String = chars[i + 1..j].iter().collect();
                let href = format!("{base_url}/tags/{}", html_escape(&tag.to_lowercase()));
                out.push_str(&format!(
                    "<a href=\"{href}\" rel=\"tag\">#{}</a>",
                    html_escape(&tag)
                ));
                i = j;
                continue;
            }
        }
        match chars[i] {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            ch => out.push(ch),
        }
        i += 1;
    }
    out
}

/// Sanitize untrusted inbound HTML or MFM text into safe plain/MFM text.
///
/// - Completely drops `<script>...</script>`, `<style>...</style>`,
///   `<iframe>...</iframe>`, `<object>...</object>` blocks and their inner text.
/// - Converts `<br>`, `<br/>`, `</p>`, `</div>`, `</li>` into newlines.
/// - Strips all other HTML tags (including any `on*` event handler attributes
///   or `javascript:` / `data:` / `vbscript:` links).
/// - Neutralizes any `javascript:`, `vbscript:`, or `data:text/html` URL schemes
///   inside MFM links `[label](url)`.
pub fn sanitize_html_to_mfm(input: &str) -> String {
    let mut s = input.to_string();
    for tag in ["script", "style", "iframe", "object"] {
        s = strip_element_with_body(&s, tag);
    }

    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'<' {
            if let Some(end_rel) = s[i..].find('>') {
                let tag_body = s[i + 1..i + end_rel].trim().to_ascii_lowercase();
                let tag_name = tag_body
                    .trim_start_matches('/')
                    .split(|c: char| c.is_ascii_whitespace() || c == '/')
                    .next()
                    .unwrap_or("");
                let is_closing = tag_body.starts_with('/');
                match (is_closing, tag_name) {
                    (false, "br") => out.push('\n'),
                    (true, "p" | "div" | "li" | "blockquote" | "pre") => out.push('\n'),
                    _ => {}
                }
                i += end_rel + 1;
                continue;
            }
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }

    let unescaped = html_unescape(&out);
    let cleaned = sanitize_mfm_text(&unescaped);
    cleaned.trim().to_string()
}

/// Sanitize inbound MFM source text (`source.content` or `_misskey_summary`),
/// stripping dangerous HTML tags (`<script>`, `<style>`, `<iframe>`, `<object>`,
/// `on*=` attributes) and neutralizing `javascript:`/`vbscript:`/`data:` URLs.
pub fn sanitize_mfm_text(input: &str) -> String {
    let mut s = input.to_string();
    for tag in ["script", "style", "iframe", "object"] {
        s = strip_element_with_body(&s, tag);
    }
    // Strip any raw HTML tags with event handlers or dangerous schemes.
    let mut cleaned = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s.as_bytes()[i] == b'<' {
            if let Some(end_rel) = s[i..].find('>') {
                let inner = s[i + 1..i + end_rel].to_ascii_lowercase();
                let tag_name = inner
                    .trim_start_matches('/')
                    .split(|c: char| c.is_ascii_whitespace() || c == '/')
                    .next()
                    .unwrap_or("");
                if matches!(
                    tag_name,
                    "script"
                        | "style"
                        | "iframe"
                        | "object"
                        | "embed"
                        | "form"
                        | "input"
                        | "button"
                        | "svg"
                        | "math"
                        | "img"
                        | "a"
                        | "div"
                        | "span"
                        | "p"
                        | "br"
                ) || inner.contains("on")
                    || inner.contains("javascript:")
                    || inner.contains("data:")
                    || inner.contains("vbscript:")
                {
                    if tag_name == "br" {
                        cleaned.push('\n');
                    }
                    i += end_rel + 1;
                    continue;
                }
            }
        }
        let ch = s[i..].chars().next().unwrap();
        cleaned.push(ch);
        i += ch.len_utf8();
    }

    // Neutralize dangerous schemes in MFM links `[text](javascript:...)`.
    let mut final_out = String::with_capacity(cleaned.len());
    let lower = cleaned.to_ascii_lowercase();
    let mut pos = 0;
    while pos < cleaned.len() {
        let rest_lower = &lower[pos..];
        if rest_lower.starts_with("javascript:")
            || rest_lower.starts_with("vbscript:")
            || rest_lower.starts_with("data:text/html")
        {
            final_out.push_str("blocked:");
            if let Some(colon) = cleaned[pos..].find(':') {
                pos += colon + 1;
                continue;
            }
        }
        let ch = cleaned[pos..].chars().next().unwrap();
        final_out.push(ch);
        pos += ch.len_utf8();
    }
    // Collapse 3+ consecutive newlines down to 2.
    while final_out.contains("\n\n\n") {
        final_out = final_out.replace("\n\n\n", "\n\n");
    }
    final_out.trim().to_string()
}

fn strip_element_with_body(input: &str, tag: &str) -> String {
    let open_pat = format!("<{tag}");
    let close_pat = format!("</{tag}>");
    let mut s = input.to_string();
    loop {
        let lower = s.to_ascii_lowercase();
        let Some(start) = lower.find(&open_pat) else {
            break;
        };
        if let Some(close_rel) = lower[start..].find(&close_pat) {
            let end = start + close_rel + close_pat.len();
            s.replace_range(start..end, "");
        } else if let Some(gt_rel) = lower[start..].find('>') {
            let end = start + gt_rel + 1;
            s.replace_range(start..end, "");
        } else {
            s.replace_range(start..s.len(), "");
            break;
        }
    }
    s
}

// ---------------------------------------------------------------------------
// Outbound Note & Activity rendering (Slice B1)
// ---------------------------------------------------------------------------

/// Render a local [`Note`] as an ActivityPub `Note` object.
///
/// Includes `@context` when `include_context` is true (top-level `/notes/{id}`
/// GET response). Never emits `_misskey_content`; emits `source` with
/// `mediaType: "text/x.misskeymarkdown"` instead.
pub fn render_note_object(
    services: &AppServices,
    note: &Note,
    include_context: bool,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let author = services
        .accounts
        .find_by_id(&note.author_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let note_uri = note
        .uri
        .clone()
        .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
    let author_uri = author.actor_url(base);
    let published = format_millis(
        services
            .ids
            .parse_time(&note.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let (to, cc) = render_note_audience(services, &author, note);

    let in_reply_to = match &note.reply_id {
        Some(rid) => match services.notes.get_note(rid)? {
            Some(parent) => Some(
                parent
                    .uri
                    .unwrap_or_else(|| format!("{base}/notes/{}", parent.id)),
            ),
            None => None,
        },
        None => None,
    };

    let content_html = note.text.as_deref().map(|t| mfm_to_html(t, base));
    let source = note.text.as_ref().map(|t| {
        json!({
            "content": t,
            "mediaType": "text/x.misskeymarkdown",
        })
    });

    // Attachments from `file_ids`
    let mut attachments = Vec::new();
    let mut any_sensitive_file = false;
    for fid in &note.file_ids {
        if let Some(meta) = services.media.get(fid)? {
            if meta.is_sensitive {
                any_sensitive_file = true;
            }
            attachments.push(render_attachment(&meta));
        }
    }

    // Tags: Mentions + Hashtags
    let mut tags = Vec::new();
    for uid in &note.mentions {
        if let Some(acc) = services.accounts.find_by_id(uid)? {
            let href = acc.actor_url(base);
            let handle = match &acc.host {
                Some(h) => format!("@{}@{h}", acc.username),
                None => format!("@{}@{}", acc.username, extract_host(base)),
            };
            tags.push(json!({
                "type": "Mention",
                "href": href,
                "name": handle,
            }));
        }
    }
    for tag in &note.tags {
        tags.push(json!({
            "type": "Hashtag",
            "href": format!("{base}/tags/{}", tag.to_lowercase()),
            "name": format!("#{tag}"),
        }));
    }

    let mut obj = json!({
        "id": note_uri,
        "type": "Note",
        "attributedTo": author_uri,
        "summary": note.cw,
        "content": content_html,
        "source": source,
        "inReplyTo": in_reply_to,
        "published": published,
        "to": to,
        "cc": cc,
        "tag": tags,
        "attachment": attachments,
        "sensitive": note.cw.is_some() || any_sensitive_file,
    });
    if include_context {
        if let Some(map) = obj.as_object_mut() {
            map.insert("@context".to_string(), ap_context());
        }
    }
    Ok(obj)
}

fn render_attachment(meta: &MediaMeta) -> Value {
    let doc_type = if meta.mime.starts_with("image/") {
        "Image"
    } else if meta.mime.starts_with("video/") {
        "Video"
    } else if meta.mime.starts_with("audio/") {
        "Audio"
    } else {
        "Document"
    };
    let mut att = json!({
        "type": doc_type,
        "mediaType": meta.mime,
        "url": meta.url,
        "name": meta.comment.clone().unwrap_or_else(|| meta.name.clone()),
        "sensitive": meta.is_sensitive,
    });
    if let Some(bh) = &meta.blurhash {
        att["blurhash"] = Value::String(bh.clone());
    }
    if let (Some(w), Some(h)) = (meta.properties.width, meta.properties.height) {
        att["width"] = Value::Number(w.into());
        att["height"] = Value::Number(h.into());
    }
    att
}

/// Render a `Create(Note)` or `Announce` activity for `note` (`GET /notes/{id}/activity`
/// and outbox items).
pub fn render_create_or_announce_for_note(
    services: &AppServices,
    note: &Note,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let author = services
        .accounts
        .find_by_id(&note.author_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let author_uri = author.actor_url(base);
    let published = format_millis(
        services
            .ids
            .parse_time(&note.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let (to, cc) = render_note_audience(services, &author, note);

    // Pure renote (no text, no cw, no files) renders as `Announce`.
    if let Some(renote_id) = &note.renote_id {
        if note.text.is_none() && note.cw.is_none() && note.file_ids.is_empty() {
            let target = services
                .notes
                .get_note(renote_id)?
                .ok_or_else(|| PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()))?;
            let target_uri = target
                .uri
                .unwrap_or_else(|| format!("{base}/notes/{}", target.id));
            return Ok(json!({
                "@context": ap_context(),
                "id": format!("{base}/notes/{}/activity", note.id),
                "type": "Announce",
                "actor": author_uri,
                "published": published,
                "to": to,
                "cc": cc,
                "object": target_uri,
            }));
        }
    }

    let note_obj = render_note_object(services, note, false)?;
    Ok(json!({
        "@context": ap_context(),
        "id": format!("{base}/notes/{}/activity", note.id),
        "type": "Create",
        "actor": note_obj["attributedTo"].clone(),
        "published": note_obj["published"].clone(),
        "to": note_obj["to"].clone(),
        "cc": note_obj["cc"].clone(),
        "object": note_obj,
    }))
}

/// Render a `Follow` activity.
pub fn render_follow_activity(
    public_url: &str,
    follow_id: &str,
    follower_uri: &str,
    followee_uri: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/follows/{follow_id}"),
        "type": "Follow",
        "actor": follower_uri,
        "object": followee_uri,
    })
}

/// Render an `Accept(Follow)` activity.
pub fn render_accept_follow_activity(
    public_url: &str,
    accept_id: &str,
    actor_uri: &str,
    follow_activity: &Value,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{accept_id}"),
        "type": "Accept",
        "actor": actor_uri,
        "object": follow_activity,
    })
}

/// Render a `Reject(Follow)` activity.
pub fn render_reject_follow_activity(
    public_url: &str,
    reject_id: &str,
    actor_uri: &str,
    follow_activity: &Value,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{reject_id}"),
        "type": "Reject",
        "actor": actor_uri,
        "object": follow_activity,
    })
}

/// Render an `Undo` activity wrapping an inner activity (`Follow`, `Like`, `Announce`).
pub fn render_undo_activity(
    public_url: &str,
    undo_id: &str,
    actor_uri: &str,
    inner_activity: &Value,
    to: Option<Vec<String>>,
    cc: Option<Vec<String>>,
) -> Value {
    let base = public_url.trim_end_matches('/');
    let mut doc = json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{undo_id}"),
        "type": "Undo",
        "actor": actor_uri,
        "object": inner_activity,
    });
    if let Some(to) = to {
        doc["to"] = json!(to);
    }
    if let Some(cc) = cc {
        doc["cc"] = json!(cc);
    }
    doc
}

/// Render a `Like` activity with Misskey `_misskey_reaction`.
pub fn render_like_activity(
    public_url: &str,
    reaction_id: &str,
    actor_uri: &str,
    target_note_uri: &str,
    reaction: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/likes/{reaction_id}"),
        "type": "Like",
        "actor": actor_uri,
        "object": target_note_uri,
        "content": reaction,
        "_misskey_reaction": reaction,
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

/// Render an `Update(Person)` activity when a local account updates its profile.
pub fn render_update_actor_activity(
    services: &AppServices,
    update_id: &str,
    account: &Account,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let actor_doc = crate::activitypub::actor::render_actor_document(services, account)?;
    let actor_uri = account.actor_url(base);
    Ok(json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{update_id}"),
        "type": "Update",
        "actor": actor_uri,
        "to": [PUBLIC_COLLECTION_URI],
        "object": actor_doc,
    }))
}

// ---------------------------------------------------------------------------
// Inbound Note & Activity parsing (Slice B1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedAttachment {
    pub url: String,
    pub media_type: Option<String>,
    pub name: Option<String>,
    pub is_sensitive: bool,
    pub blurhash: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedRemoteNote {
    pub uri: String,
    pub url: Option<String>,
    pub attributed_to: String,
    pub text: Option<String>,
    pub cw: Option<String>,
    pub in_reply_to: Option<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub mentioned_actor_uris: Vec<String>,
    pub hashtags: Vec<String>,
    pub attachments: Vec<ParsedAttachment>,
    pub sensitive: bool,
}

/// Parse an ActivityPub `Note` JSON object into [`ParsedRemoteNote`].
pub fn parse_remote_note(obj_val: &Value) -> Result<ParsedRemoteNote, String> {
    let obj = obj_val
        .as_object()
        .ok_or_else(|| "Note must be a JSON object".to_string())?;
    let note_type = obj.get("type").and_then(Value::as_str).unwrap_or("Note");
    if !matches!(note_type, "Note" | "Article" | "Page" | "Question") {
        return Err(format!("unsupported note object type: {note_type}"));
    }
    let uri = obj
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Note missing id".to_string())?
        .to_string();
    let attributed_to = match obj.get("attributedTo") {
        Some(Value::String(s)) if !s.trim().is_empty() => s.trim().to_string(),
        Some(Value::Object(m)) => m
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "Note attributedTo object missing id".to_string())?
            .to_string(),
        _ => return Err("Note missing attributedTo".to_string()),
    };

    let url = match obj.get("url") {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        _ => Some(uri.clone()),
    };

    // Prefer `source.content` when `source.mediaType == "text/x.misskeymarkdown"`,
    // falling back to sanitized `content` HTML.
    let source_mfm = obj
        .get("source")
        .and_then(Value::as_object)
        .and_then(|src| {
            let media_type = src.get("mediaType").and_then(Value::as_str).unwrap_or("");
            if media_type.eq_ignore_ascii_case("text/x.misskeymarkdown")
                || media_type.eq_ignore_ascii_case("text/markdown")
            {
                src.get("content").and_then(Value::as_str)
            } else {
                None
            }
        });

    let text = if let Some(mfm) = source_mfm {
        let cleaned = sanitize_mfm_text(mfm);
        if cleaned.is_empty() {
            None
        } else {
            Some(cleaned)
        }
    } else if let Some(html) = obj.get("content").and_then(Value::as_str) {
        let cleaned = sanitize_html_to_mfm(html);
        if cleaned.is_empty() {
            None
        } else {
            Some(cleaned)
        }
    } else {
        None
    };

    let cw = obj
        .get("summary")
        .and_then(Value::as_str)
        .map(sanitize_html_to_mfm)
        .filter(|s| !s.is_empty());

    let in_reply_to = match obj.get("inReplyTo") {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Some(Value::Object(m)) => m
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned),
        _ => None,
    };

    let to = extract_uri_list(obj.get("to"));
    let cc = extract_uri_list(obj.get("cc"));

    let mut mentioned_actor_uris = Vec::new();
    let mut hashtags = Vec::new();
    if let Some(tag_val) = obj.get("tag") {
        let items: Vec<&Value> = match tag_val {
            Value::Array(arr) => arr.iter().collect(),
            other => vec![other],
        };
        for item in items {
            let Some(m) = item.as_object() else { continue };
            let t = m.get("type").and_then(Value::as_str).unwrap_or("");
            match t {
                "Mention" => {
                    if let Some(href) = m.get("href").and_then(Value::as_str) {
                        let href = href.trim().to_string();
                        if !href.is_empty() && !mentioned_actor_uris.contains(&href) {
                            mentioned_actor_uris.push(href);
                        }
                    }
                }
                "Hashtag" => {
                    if let Some(name) = m.get("name").and_then(Value::as_str) {
                        let clean = name.trim().trim_start_matches('#').to_string();
                        if !clean.is_empty() && !hashtags.contains(&clean) {
                            hashtags.push(clean);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    let sensitive = obj
        .get("sensitive")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let mut attachments = Vec::new();
    if let Some(att_val) = obj.get("attachment") {
        let items: Vec<&Value> = match att_val {
            Value::Array(arr) => arr.iter().collect(),
            other => vec![other],
        };
        for item in items {
            let Some(m) = item.as_object() else { continue };
            let url_opt = m
                .get("url")
                .and_then(|u| match u {
                    Value::String(s) => Some(s.as_str()),
                    Value::Array(arr) => arr.first().and_then(|f| f.get("href")).and_then(Value::as_str),
                    Value::Object(uo) => uo.get("href").and_then(Value::as_str),
                    _ => None,
                })
                .or_else(|| m.get("href").and_then(Value::as_str))
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let Some(att_url) = url_opt else { continue };
            let media_type = m
                .get("mediaType")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned);
            let name = m
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned);
            let is_sensitive = m
                .get("sensitive")
                .and_then(Value::as_bool)
                .unwrap_or(sensitive);
            let blurhash = m
                .get("blurhash")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let width = m
                .get("width")
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok());
            let height = m
                .get("height")
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok());
            attachments.push(ParsedAttachment {
                url: att_url.to_string(),
                media_type,
                name,
                is_sensitive,
                blurhash,
                width,
                height,
            });
        }
    }

    Ok(ParsedRemoteNote {
        uri,
        url,
        attributed_to,
        text,
        cw,
        in_reply_to,
        to,
        cc,
        mentioned_actor_uris,
        hashtags,
        attachments,
        sensitive,
    })
}

/// Extract the reaction string from an incoming `Like` activity:
/// prefers `_misskey_reaction`, then `content`, falling back to `❤️`.
pub fn extract_like_reaction(activity: &Value) -> String {
    if let Some(r) = activity
        .get("_misskey_reaction")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return r.to_string();
    }
    if let Some(c) = activity
        .get("content")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return c.to_string();
    }
    FALLBACK_REACTION.to_string()
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

// ---------------------------------------------------------------------------
// Axum GET handlers for `/notes/{id}` and `/notes/{id}/activity`
// ---------------------------------------------------------------------------

fn ap_response(status: StatusCode, body: &Value) -> Response {
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
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Response {
    let base = state.services.public_url.trim_end_matches('/');
    let note = match state.services.notes.get_note_including_deleted(&id) {
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
    match render_note_object(&state.services, &note, true) {
        Ok(doc) => ap_response(StatusCode::OK, &doc),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// `GET /notes/{id}/activity` — serves the `Create(Note)` or `Announce`
/// ActivityPub document for a local public/home note.
pub async fn get_note_activity_ap(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Response {
    let note = match state.services.notes.get_note(&id) {
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
    match render_create_or_announce_for_note(&state.services, &note) {
        Ok(doc) => ap_response(StatusCode::OK, &doc),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::actor::{parse_remote_actor, render_actor_document};
    use crate::application::ports::{ActivityDedup, CreateNoteInput, NoopStreamingSink};
    use crate::domain::id::{AidxGenerator, Clock, IdGenerator, ManualClock};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-ap-obj-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            reactions: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
        }
    }

    #[test]
    fn context_never_advertises_deprecated_misskey_content() {
        let ctx = serde_json::to_string(&ap_context()).unwrap();
        assert!(!ctx.contains("_misskey_content"));
        assert!(ctx.contains("_misskey_reaction"));
    }

    #[test]
    fn audience_mapping_roundtrips_all_visibilities() {
        let services = test_services("aud");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let followers_uri = format!("https://pinetree.local/users/{}/followers", alice.id);
        let bob_uri = format!("https://pinetree.local/users/{}", bob.id);

        for (vis, visible_users) in [
            (Visibility::Public, vec![]),
            (Visibility::Home, vec![]),
            (Visibility::Followers, vec![]),
            (Visibility::Specified, vec![bob.id.clone()]),
        ] {
            let mut input = CreateNoteInput::simple_public(&alice.id, "hello @bob #rust");
            input.visibility = vis;
            input.visible_users = visible_users;
            let note = services.create_note(input).unwrap();
            let (to, cc) = render_note_audience(&services, &alice, &note);
            let (parsed_vis, recipients) =
                parse_note_audience(&to, &cc, Some(&followers_uri));
            assert_eq!(parsed_vis, vis, "visibility roundtrip for {vis:?}");
            assert!(recipients.contains(&bob_uri));
        }
    }

    #[test]
    fn mfm_to_html_and_sanitization_strip_xss_payloads() {
        let html = mfm_to_html("Hello @alice@remote.example #fediverse\nLine 2", "https://pinetree.local");
        assert!(html.contains("<a href=\"https://remote.example/@alice\" class=\"u-url mention\">@alice@remote.example</a>"));
        assert!(html.contains("<a href=\"https://pinetree.local/tags/fediverse\" rel=\"tag\">#fediverse</a>"));
        assert!(html.contains("<br>"));

        let dirty = "<p>Safe text<script>alert('xss')</script><style>body{display:none}</style><iframe src=\"https://evil.example\"></iframe><img src=x onerror=\"alert(1)\"><a href=\"javascript:alert(2)\">click</a><br>Second line</p>";
        let cleaned = sanitize_html_to_mfm(dirty);
        assert_eq!(cleaned, "Safe textclick\nSecond line");
        assert!(!cleaned.contains("script"));
        assert!(!cleaned.contains("alert"));
        assert!(!cleaned.contains("onerror"));
        assert!(!cleaned.contains("javascript:"));

        let dirty_mfm = "Hi [bad](javascript:alert(1)) <script>evil()</script> world";
        let cleaned_mfm = sanitize_mfm_text(dirty_mfm);
        assert!(!cleaned_mfm.contains("javascript:"));
        assert!(!cleaned_mfm.contains("<script>"));
        assert!(cleaned_mfm.contains("Hi [bad](blocked:alert(1))"));
    }

    #[test]
    fn actor_and_note_documents_roundtrip_parse() {
        let services = test_services("roundtrip");
        let alice = services.create_account("alice", None).unwrap();
        let actor_doc = render_actor_document(&services, &alice).unwrap();
        let parsed_actor = parse_remote_actor(&actor_doc).unwrap();
        assert_eq!(parsed_actor.username, "alice");
        assert_eq!(parsed_actor.host, "pinetree.local");
        assert_eq!(
            parsed_actor.uri,
            format!("https://pinetree.local/users/{}", alice.id)
        );
        assert_eq!(
            parsed_actor.public_key_id,
            format!("https://pinetree.local/users/{}#main-key", alice.id)
        );
        assert!(parsed_actor.public_key_pem.contains("BEGIN PUBLIC KEY"));

        let note = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "hello #test"))
            .unwrap();
        let note_doc = render_note_object(&services, &note, true).unwrap();
        let serialized = serde_json::to_string(&note_doc).unwrap();
        assert!(!serialized.contains("_misskey_content"));
        assert_eq!(
            note_doc["source"]["mediaType"],
            "text/x.misskeymarkdown"
        );
        let parsed_note = parse_remote_note(&note_doc).unwrap();
        assert_eq!(parsed_note.text.as_deref(), Some("hello #test"));
        assert_eq!(parsed_note.hashtags, vec!["test".to_string()]);
    }
}
