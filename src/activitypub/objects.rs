//! ActivityPub JSON-LD `@context`, object/activity rendering and parsing,
//! `to`/`cc` ↔ `Visibility` audience mapping, and HTML ↔ MFM conversion +
//! sanitization (Phase 2 Slices B1 & B2).
//!
//! Never emits deprecated `_misskey_content` (uses `source: { content,
//! mediaType: "text/x.misskeymarkdown" }` instead). Reactions use
//! `_misskey_reaction` alongside `content` per proposal §7.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::activitypub::format_millis;
use crate::activitypub::webfinger::extract_host;
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
/// omitting deprecated `_misskey_content`, plus FEP-044f quote terms
/// (canonical `quote`/`quoteAuthorization`, Fedibird `quoteUri`, GoToSocial
/// interaction-policy terms) and the `quoteUrl` convention.
pub fn ap_context() -> Value {
    let mut ctx = json!([
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
    ]);
    // Merge the FEP-044f / compatibility quote terms into the trailing
    // context object (never the deprecated `_misskey_content`).
    if let Some(entries) = ctx.as_array_mut()
        .and_then(|arr| arr.last_mut())
        .and_then(|v| v.as_object_mut())
    {
        for (k, v) in super::quotes::quote_context_entries()
            .as_object()
            .cloned()
            .unwrap_or_default()
        {
            entries.insert(k, v);
        }
    }
    ctx
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

    // Slice B: a pending (unresolved-parent) reply never leaks its
    // `reply_parent_uri` — neither as `inReplyTo` here nor through the
    // Misskey packer. Such notes are withheld from the AP object endpoints
    // by `is_visible` anyway; this keeps the renderer total even if one
    // ever reaches it (e.g. outbound delivery of a note linked between
    // packing and rendering cannot happen, but defense in depth is cheap).
    let in_reply_to = match &note.reply_id {
        Some(rid) if note.reply_parent_uri.is_none() => {
            match services.notes.get_note(rid)? {
                Some(parent) => Some(
                    parent
                        .uri
                        .unwrap_or_else(|| format!("{base}/notes/{}", parent.id)),
                ),
                None => None,
            }
        }
        _ => None,
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

    // Structured quote (FEP-044f canonical + legacy aliases + policy +
    // single textual fallback). The stored body is never mutated: the
    // fallback is generated on the serialized `content` only, and only
    // when the target is safe to disclose to this post's audience.
    let mut content_html = content_html;
    let mut quote_extra = serde_json::Map::new();
    if let Some(quote) = note.quote.as_ref() {
        use crate::activitypub::quotes::{
            has_terminal_re_fallback, render_quote_fields, render_quote_link_tag,
            render_quote_policy,
        };
        use crate::domain::quote::QuoteAuthorizationState;
        // `quoteAuthorization` only with a valid stamp (Approved carries
        // one; exceptions and all other states serialize no stamp).
        let stamp = match quote.authorization {
            QuoteAuthorizationState::Approved => quote.authorization_uri.as_deref(),
            _ => None,
        };
        for (k, v) in render_quote_fields(&quote.target_uri, stamp)
            .as_object()
            .cloned()
            .unwrap_or_default()
        {
            quote_extra.insert(k, v);
        }
        // Advisory quoting policy for this object (never proof of approval,
        // and never `manualApproval`: no review workflow exists).
        let quoting_public_audience =
            matches!(note.visibility, Visibility::Public | Visibility::Home);
        for (k, v) in render_quote_policy(quoting_public_audience, &author_uri)
            .as_object()
            .cloned()
            .unwrap_or_default()
        {
            quote_extra.insert(k, v);
        }
        // FEP-e232 compatibility Link tag (single, non-duplicated).
        tags.push(render_quote_link_tag(&quote.target_uri));
        // Textual `RE:` fallback: public quoting posts only, known-public
        // targets only, never twice. Unresolved targets get no fallback:
        // their visibility is unknown (possibly private) and a public
        // `RE:` would leak existence.
        let target_public = match quote.target_note_id.as_deref() {
            Some(tid) => match services.notes.get_note(tid)? {
                Some(target) => {
                    matches!(target.visibility, Visibility::Public | Visibility::Home)
                        && !target.local_only
                        && !target.is_deleted
                }
                None => false,
            },
            None => false,
        };
        if quoting_public_audience
            && target_public
            && !has_terminal_re_fallback(note.text.as_deref(), &quote.target_uri)
        {
            let escaped = html_escape(&quote.target_uri);
            let suffix = format!(
                "<br><br><span class=\"quote-inline\">RE: <a href=\"{escaped}\">{escaped}</a></span>"
            );
            match content_html.as_mut() {
                Some(html) => html.push_str(&suffix),
                None => {
                    content_html = Some(format!(
                        "<p><span class=\"quote-inline\">RE: <a href=\"{escaped}\">{escaped}</a></span></p>"
                    ));
                }
            }
        }
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
    // §4 polls: a poll note renders as a `Question` object carrying
    // `oneOf`/`anyOf` tallies (Misskey `renderNote` `...asPoll` parity —
    // the spread overwrites `type` there too).
    if let Some(poll) = services.polls.get_poll(&note.id)? {
        let question = render_question(
            base,
            &author_uri,
            &note.id,
            note.text.as_deref(),
            &poll,
            services.clock.now_ms(),
        );
        if let Some(qmap) = question.as_object() {
            for key in ["oneOf", "anyOf", "endTime", "closed"] {
                if let Some(v) = qmap.get(key) {
                    obj[key] = v.clone();
                }
            }
        }
        obj["type"] = Value::String("Question".to_string());
    }
    for (k, v) in quote_extra {
        obj[k] = v;
    }
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

/// Render a `Block` activity (§0A; Misskey `ApRendererService.renderBlock`
/// parity: `{url}/blocks/{id}`, actor = blocker, object = blockee URI).
pub fn render_block_activity(
    public_url: &str,
    block_id: &str,
    blocker_uri: &str,
    blockee_uri: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/blocks/{block_id}"),
        "type": "Block",
        "actor": blocker_uri,
        "object": blockee_uri,
    })
}

/// Render a `Flag` activity (Batch M WP4; upstream
/// `ApRendererService.renderFlag`: type Flag, actor = reporting (system)
/// account, content = report comment, object = reported account URI).
pub fn render_flag_activity(
    public_url: &str,
    flag_id: &str,
    actor_uri: &str,
    object_uri: &str,
    content: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/flags/{flag_id}"),
        "type": "Flag",
        "actor": actor_uri,
        "content": content,
        "object": object_uri,
    })
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

/// Render an `Add`/`Remove` of a note to/from the actor's featured
/// collection (pinning §7; Misskey `NotePiningService.deliverPinnedChange`
/// `renderAdd`/`renderRemove` parity: `target =
/// {actor}/collections/featured`, `object` = the note URI).
pub fn render_featured_change_activity(
    public_url: &str,
    change_id: &str,
    actor_uri: &str,
    note_uri: &str,
    added: bool,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{change_id}"),
        "type": if added { "Add" } else { "Remove" },
        "actor": actor_uri,
        "target": format!("{actor_uri}/collections/featured"),
        "object": note_uri,
    })
}

/// Render a `Like` activity, choosing the emoji-reaction form or the plain
/// Mastodon-favourite form per recipient capability (§6
/// `docs/misskey-parity-plan.md`): emoji-aware servers get `Like` +
/// `_misskey_reaction`, others get a plain `Like` with neither `content` nor
/// `_misskey_reaction`. Both variants share [`ap_context`]: its
/// `_misskey_reaction` entry is an inert namespace declaration (and upstream
/// Misskey always sends the full context too), so the distinguishing feature
/// is the absence of the `_misskey_reaction` field.
pub fn render_like_activity_for(
    public_url: &str,
    reaction_id: &str,
    actor_uri: &str,
    target_note_uri: &str,
    reaction: &str,
    emoji_capable: bool,
) -> Value {
    let base = public_url.trim_end_matches('/');
    if emoji_capable {
        json!({
            "@context": ap_context(),
            "id": format!("{base}/likes/{reaction_id}"),
            "type": "Like",
            "actor": actor_uri,
            "object": target_note_uri,
            "content": reaction,
            "_misskey_reaction": reaction,
        })
    } else {
        json!({
            "@context": ap_context(),
            "id": format!("{base}/likes/{reaction_id}"),
            "type": "Like",
            "actor": actor_uri,
            "object": target_note_uri,
        })
    }
}

/// Render a `Like` activity with Misskey `_misskey_reaction` (the
/// emoji-aware form; see [`render_like_activity_for`]).
pub fn render_like_activity(
    public_url: &str,
    reaction_id: &str,
    actor_uri: &str,
    target_note_uri: &str,
    reaction: &str,
) -> Value {
    render_like_activity_for(
        public_url,
        reaction_id,
        actor_uri,
        target_note_uri,
        reaction,
        true,
    )
}

// ---------------------------------------------------------------------------
// Polls (§4; Misskey `ApRendererService.renderQuestion` + `renderVote`)
// ---------------------------------------------------------------------------

/// Render a standalone `Question` object for a poll note (Misskey
/// `renderQuestion` parity): `{ type: Question, id: {base}/questions/{id},
/// actor, content, oneOf|anyOf: [{ name, _misskey_votes,
/// replies: { type: Collection, totalItems } }] }`. `multiple` selects
/// `anyOf`, otherwise `oneOf`.
pub fn render_question(
    public_url: &str,
    author_actor_uri: &str,
    note_id: &str,
    text: Option<&str>,
    poll: &crate::domain::poll::Poll,
    now_ms: i64,
) -> Value {
    let base = public_url.trim_end_matches('/');
    let options: Vec<Value> = poll
        .choices
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let count = poll.votes.get(i).copied().unwrap_or(0);
            json!({
                "name": name,
                "_misskey_votes": count,
                "replies": { "type": "Collection", "totalItems": count },
            })
        })
        .collect();
    let mut doc = json!({
        "type": "Question",
        "id": format!("{base}/questions/{note_id}"),
        "actor": author_actor_uri,
        "attributedTo": author_actor_uri,
        "content": text.unwrap_or(""),
        if poll.multiple { "anyOf" } else { "oneOf" }: options,
    });
    if let Some(expires_at_ms) = poll.expires_at_ms {
        // Misskey `renderNote` parity: an already-past expiry serializes
        // as `closed`, a future one as `endTime`.
        doc[if expires_at_ms < now_ms { "closed" } else { "endTime" }] =
            Value::String(format_millis(expires_at_ms));
    }
    doc
}

/// Render an `Update(Question)` activity carrying the fresh tallies
/// (Misskey `PollService.deliverQuestionUpdate` parity:
/// `renderUpdate(renderNote(note))` to followers + relays).
pub fn render_question_update_activity(
    public_url: &str,
    update_id: &str,
    author_actor_uri: &str,
    question: Value,
    to: Vec<String>,
    now_ms: i64,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{update_id}"),
        "actor": author_actor_uri,
        "type": "Update",
        "to": to,
        "object": question,
        "published": format_millis(now_ms),
    })
}

/// Render a `Create` vote activity for a vote on a remote poll note
/// (Misskey `ApRendererService.renderVote` parity): the object is a `Note`
/// with `inReplyTo` = poll note URI and `name` = choice text.
#[allow(clippy::too_many_arguments)]
pub fn render_vote_activity(
    public_url: &str,
    vote_id: &str,
    voter_actor_uri: &str,
    poll_owner_uri: &str,
    target_note_uri: &str,
    choice_name: &str,
    now_ms: i64,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/users/{vote_id}/votes/activity"),
        "actor": voter_actor_uri,
        "type": "Create",
        "to": [poll_owner_uri],
        "published": format_millis(now_ms),
        "object": {
            "id": format!("{base}/users/{vote_id}/votes"),
            "type": "Note",
            "attributedTo": voter_actor_uri,
            "to": [poll_owner_uri],
            "inReplyTo": target_note_uri,
            "name": choice_name,
        },
    })
}

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

/// Render a `Tombstone` JSON-LD document for `GET /notes/{id}` when deleted.
pub fn render_tombstone(note_uri: &str) -> Value {
    json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": note_uri,
        "type": "Tombstone",
        "formerType": "Note",
    })
}

/// Render an `Update(Note)` activity for an edited local note (§8;
/// Sharkey `postNoteEdited` AP-deliver parity: the full re-rendered note
/// object wrapped in `Update`, addressed like the create path).
pub fn render_note_update_activity(
    public_url: &str,
    update_id: &str,
    author_actor_uri: &str,
    note: Value,
    to: Vec<String>,
    cc: Vec<String>,
    now_ms: i64,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{update_id}"),
        "actor": author_actor_uri,
        "type": "Update",
        "to": to,
        "cc": cc,
        "object": note,
        "published": format_millis(now_ms),
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
    /// The object's `name` (vote choice text on `Create` vote activities;
    /// Misskey `ApNoteService` vote branch reads `note.name`).
    #[serde(default)]
    pub name: Option<String>,
    pub in_reply_to: Option<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub mentioned_actor_uris: Vec<String>,
    pub hashtags: Vec<String>,
    pub attachments: Vec<ParsedAttachment>,
    pub sensitive: bool,
    /// The attached poll when the object carries `oneOf`/`anyOf`
    /// (Misskey `ApQuestionService.extractPollFromQuestion` parity).
    #[serde(default)]
    pub poll: Option<ParsedPoll>,
    /// Normalized structured quote target (any of the five carriers), if any.
    /// Quote detection is independent of authorization metadata.
    #[serde(default)]
    pub quote_target_uri: Option<String>,
    /// Raw `quoteAuthorization` IRI, if present. Presence alone never proves
    /// approval — the stamp must be dereferenced and verified.
    #[serde(default)]
    pub quote_authorization_uri: Option<String>,
    /// True when several carriers disagreed on the target (canonical `quote`
    /// won). Diagnostic only; the winning target is in `quote_target_uri`.
    #[serde(default)]
    pub quote_has_conflict: bool,
}

/// Parse an ActivityPub `Note` JSON object into [`ParsedRemoteNote`].
pub fn parse_remote_note(obj_val: &Value) -> Result<ParsedRemoteNote, String> {
    let obj = obj_val
        .as_object()
        .ok_or_else(|| "Note must be a JSON object".to_string())?;
    let note_type = obj.get("type").and_then(Value::as_str).unwrap_or("Note");
    // `Answer` is Pleroma/Akkoma's poll-vote object (`Create(Answer{name,
    // inReplyTo})`); it is only meaningful to the vote branch of the inbox.
    if !matches!(note_type, "Note" | "Article" | "Page" | "Question" | "Answer") {
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

    let normalized = super::quotes::normalize_inbound_quote(obj_val);
    Ok(ParsedRemoteNote {
        uri,
        url,
        attributed_to,
        text,
        cw,
        name: obj
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned),
        in_reply_to,
        to,
        cc,
        mentioned_actor_uris,
        hashtags,
        attachments,
        sensitive,
        poll: parse_question_poll(obj),
        quote_target_uri: normalized.as_ref().map(|q| q.target_uri.clone()),
        quote_authorization_uri: super::quotes::inbound_authorization_iri(obj_val),
        quote_has_conflict: normalized
            .as_ref()
            .map(|q| !q.conflicting_aliases.is_empty())
            .unwrap_or(false),
    })
}

/// A poll carried on an inbound `Note`/`Question` object (Misskey
/// `ApQuestionService.extractPollFromQuestion` parity): `oneOf` selects
/// single-choice, `anyOf` multi-choice; expiry reads `endTime` (else
/// `closed`); per-choice counts read `replies.totalItems` (else
/// `_misskey_votes`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedPoll {
    pub choices: Vec<String>,
    pub votes: Vec<u64>,
    pub multiple: bool,
    pub expires_at_ms: Option<i64>,
}

/// Extract a [`ParsedPoll`] from a Note/Question object map, or `None`
/// when the object carries no usable `oneOf`/`anyOf` (fewer than 2 named
/// choices — the storage invariant minimum).
pub fn parse_question_poll(
    obj: &serde_json::Map<String, Value>,
) -> Option<ParsedPoll> {
    let one_of = obj.get("oneOf").and_then(Value::as_array);
    let any_of = obj.get("anyOf").and_then(Value::as_array);
    // Misskey parity: `multiple = oneOf === undefined` (an object carrying
    // both reads as `oneOf`).
    let (options, multiple) = match (one_of, any_of) {
        (Some(options), _) => (options, false),
        (None, Some(options)) => (options, true),
        (None, None) => return None,
    };
    let mut choices = Vec::new();
    let mut votes = Vec::new();
    for option in options {
        let name = option.get("name").and_then(Value::as_str)?;
        choices.push(name.to_string());
        let count = option
            .get("replies")
            .and_then(|r| r.get("totalItems"))
            .and_then(Value::as_u64)
            .or_else(|| option.get("_misskey_votes").and_then(Value::as_u64))
            .unwrap_or(0);
        votes.push(count);
    }
    if choices.len() < 2 {
        return None;
    }
    let expires_at_ms = obj
        .get("endTime")
        .or_else(|| obj.get("closed"))
        .and_then(parse_ap_time_ms);
    Some(ParsedPoll {
        choices,
        votes,
        multiple,
        expires_at_ms,
    })
}

/// Parse an ActivityPub time value (unix millis integer or ISO-8601/RFC3339
/// string) to unix millis. `None` on missing/unparseable input.
pub fn parse_ap_time_ms(val: &Value) -> Option<i64> {
    match val {
        Value::Number(n) => n.as_i64().or_else(|| {
            n.as_u64()
                .and_then(|u| i64::try_from(u).ok())
        }),
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return None;
            }
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.timestamp_millis())
        }
        _ => None,
    }
}

/// Extract the reaction string from an incoming `Like` activity:
/// prefers `_misskey_reaction`, then `content`, then the bare-heart fallback.
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
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        }
    }

    #[test]
    fn context_never_advertises_deprecated_misskey_content() {
        let ctx = serde_json::to_string(&ap_context()).unwrap();
        assert!(!ctx.contains("_misskey_content"));
        assert!(ctx.contains("_misskey_reaction"));
    }

    #[test]
    fn like_render_splits_emoji_aware_and_plain() {
        let emoji = render_like_activity_for(
            "https://pinetree.local",
            "r1",
            "https://pinetree.local/users/alice",
            "https://remote.example/notes/1",
            "🎉",
            true,
        );
        assert_eq!(emoji["type"], "Like");
        assert_eq!(emoji["_misskey_reaction"], "🎉");
        assert_eq!(emoji["content"], "🎉");

        let plain = render_like_activity_for(
            "https://pinetree.local",
            "r1",
            "https://pinetree.local/users/alice",
            "https://remote.example/notes/1",
            "🎉",
            false,
        );
        assert_eq!(plain["type"], "Like");
        assert_eq!(plain["actor"], "https://pinetree.local/users/alice");
        assert_eq!(plain["object"], "https://remote.example/notes/1");
        assert!(
            plain.get("_misskey_reaction").is_none(),
            "plain Like carries no _misskey_reaction"
        );
        assert!(
            plain.get("content").is_none(),
            "plain Like carries no content"
        );

        // The legacy entry point keeps rendering the emoji-aware form.
        let legacy = render_like_activity(
            "https://pinetree.local",
            "r1",
            "https://pinetree.local/users/alice",
            "https://remote.example/notes/1",
            "🎉",
        );
        assert_eq!(legacy, emoji);
    }

    #[test]
    fn featured_change_renders_add_remove_to_featured_collection() {
        let actor = "https://pinetree.local/users/alice";
        let note = "https://pinetree.local/notes/n1";
        let add = render_featured_change_activity("https://pinetree.local/", "c1", actor, note, true);
        assert_eq!(add["type"], "Add");
        assert_eq!(add["actor"], actor);
        assert_eq!(add["target"], format!("{actor}/collections/featured"));
        assert_eq!(add["object"], note);
        assert_eq!(add["id"], "https://pinetree.local/activities/c1");
        let remove =
            render_featured_change_activity("https://pinetree.local", "c2", actor, note, false);
        assert_eq!(remove["type"], "Remove");
        assert_eq!(remove["target"], format!("{actor}/collections/featured"));
        assert_eq!(remove["object"], note);
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

#[cfg(test)]
mod quote_render_tests {
    use super::*;
    use crate::application::ports::{ActivityDedup, CreateNoteInput, NoopStreamingSink};
    use crate::domain::id::{AidxGenerator, Clock, IdGenerator, ManualClock};
    use crate::domain::quote::{QuoteAuthorizationState, QuoteReference};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-quote-render-{}-{prefix}-{}",
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
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        }
    }

    fn quote_of(services: &AppServices, quoter: &str, target: &Note, text: &str) -> Note {
        let mut input = CreateNoteInput::simple_public(quoter, text);
        input.renote_of = Some(target.id.clone());
        services.create_note(input).unwrap()
    }

    #[test]
    fn quote_renders_canonical_plus_aliases_policy_and_single_fallback() {
        let services = test_services("full");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let target_uri = format!("https://pinetree.local/notes/{}", target.id);
        let quote = quote_of(&services, &bob.id, &target, "commentary");

        let doc = render_note_object(&services, &quote, true).unwrap();
        // Canonical FEP-044f + legacy compatibility aliases.
        assert_eq!(doc["quote"], target_uri.as_str());
        assert_eq!(doc["_misskey_quote"], target_uri.as_str());
        assert_eq!(doc["quoteUrl"], target_uri.as_str());
        assert_eq!(doc["quoteUri"], target_uri.as_str());
        // No stamp, no authorization field.
        assert!(doc.get("quoteAuthorization").is_none());
        // Advisory policy, never manualApproval (no review workflow exists).
        assert!(doc["interactionPolicy"]["canQuote"]["automaticApproval"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "https://www.w3.org/ns/activitystreams#Public"));
        assert!(doc["interactionPolicy"]["canQuote"].get("manualApproval").is_none());
        // Exactly one FEP-e232 Link tag.
        let links: Vec<&Value> = doc["tag"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t.get("type").and_then(Value::as_str) == Some("Link"))
            .collect();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["href"], target_uri.as_str());
        // Single textual fallback in serialized content only (the URI
        // appears twice inside it: link href + link text).
        let content = doc["content"].as_str().unwrap();
        assert_eq!(content.matches("RE:").count(), 1, "exactly one fallback");
        assert!(content.contains("quote-inline"));
        // Stored body untouched.
        let stored = services.notes.get_note(&quote.id).unwrap().unwrap();
        assert_eq!(stored.text.as_deref(), Some("commentary"));
        // Context carries the quote terms, never the deprecated field.
        let ctx = serde_json::to_string(&doc["@context"]).unwrap();
        assert!(ctx.contains("https://w3id.org/fep/044f#quote"));
        assert!(!ctx.contains("_misskey_content"));
    }

    #[test]
    fn quote_render_never_duplicates_author_fallback() {
        let services = test_services("dup");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let target_uri = format!("https://pinetree.local/notes/{}", target.id);
        let quote = quote_of(
            &services,
            &bob.id,
            &target,
            &format!("commentary\nRE: {target_uri}"),
        );
        let doc = render_note_object(&services, &quote, false).unwrap();
        let content = doc["content"].as_str().unwrap();
        assert_eq!(content.matches("RE:").count(), 1, "no second fallback appended");
    }

    #[test]
    fn quote_render_restricted_post_adds_no_public_fallback() {
        let services = test_services("restricted");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let target_uri = format!("https://pinetree.local/notes/{}", target.id);
        // Followers-only quoting post: structured fields stay (restricted
        // audience), but no public RE: fallback is generated.
        let mut input = CreateNoteInput::simple_public(&bob.id, "commentary");
        input.visibility = Visibility::Followers;
        input.renote_of = Some(target.id.clone());
        let quote = services.create_note(input).unwrap();
        assert!(quote.is_quote());
        let doc = render_note_object(&services, &quote, false).unwrap();
        assert_eq!(doc["quote"], target_uri.as_str());
        let content = doc["content"].as_str().unwrap_or("");
        assert!(!content.contains("RE:"), "no fallback for restricted posts");
    }

    /// Slice C: outbound `inReplyTo` is always the DIRECT parent's canonical
    /// URI — a reply-to-a-reply targets its immediate parent, never the
    /// thread root. A pending (unresolved-parent) reply renders no
    /// `inReplyTo` at all: the internal URI is never disclosed.
    #[test]
    fn reply_renders_direct_parent_in_reply_to() {
        let services = test_services("reply-parent");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let root = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "root"))
            .unwrap();
        let mut a_input = CreateNoteInput::simple_public(&bob.id, "reply A");
        a_input.reply_to = Some(root.id.clone());
        let a = services.create_note(a_input).unwrap();
        let mut a1_input = CreateNoteInput::simple_public(&alice.id, "reply A1");
        a1_input.reply_to = Some(a.id.clone());
        let a1 = services.create_note(a1_input).unwrap();

        let root_uri = format!("https://pinetree.local/notes/{}", root.id);
        let a_uri = format!("https://pinetree.local/notes/{}", a.id);
        let doc_a = render_note_object(&services, &a, false).unwrap();
        assert_eq!(doc_a["inReplyTo"], root_uri.as_str());
        let doc_a1 = render_note_object(&services, &a1, false).unwrap();
        assert_eq!(
            doc_a1["inReplyTo"],
            a_uri.as_str(),
            "A1 targets its direct parent A, not the root"
        );

        // Pending replies disclose nothing: no inReplyTo, and the stored
        // pending URI appears nowhere in the document.
        let pending_uri = "https://remote.example/notes/parent";
        let mut pending = a1.clone();
        pending.reply_id = None;
        pending.reply_parent_uri = Some(pending_uri.to_string());
        let doc_pending = render_note_object(&services, &pending, true).unwrap();
        assert!(
            doc_pending.get("inReplyTo").is_none()
                || doc_pending["inReplyTo"].is_null(),
            "pending replies render no inReplyTo"
        );
        let serialized = serde_json::to_string(&doc_pending).unwrap();
        assert!(
            !serialized.contains(pending_uri),
            "pending parent URI never leaks into the document"
        );
    }

    #[test]
    fn quote_render_stamp_only_with_approval() {
        let services = test_services("stamp");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let target_uri = format!("https://pinetree.local/notes/{}", target.id);
        let quote = quote_of(&services, &bob.id, &target, "commentary");

        // Local quotes self-verify as exceptions: no stamp, no field.
        let doc = render_note_object(&services, &quote, false).unwrap();
        assert!(doc.get("quoteAuthorization").is_none());

        // Approved with a valid stamp: the stamp IRI serializes.
        let mut approved = quote.clone();
        approved.quote = Some(QuoteReference {
            target_uri: target_uri.clone(),
            target_note_id: Some(target.id.clone()),
            authorization: QuoteAuthorizationState::Approved,
            authorization_uri: Some("https://remote.example/stamps/1".to_string()),
            request_activity_uri: None,
            last_verified_at: None,
        });
        let doc = render_note_object(&services, &approved, false).unwrap();
        assert_eq!(doc["quoteAuthorization"], "https://remote.example/stamps/1");

        // Rejected quotes serialize no stamp (retained relation, no approval).
        let mut rejected = quote.clone();
        rejected.quote = Some(QuoteReference {
            target_uri,
            target_note_id: Some(target.id.clone()),
            authorization: QuoteAuthorizationState::Rejected,
            authorization_uri: None,
            request_activity_uri: None,
            last_verified_at: None,
        });
        let doc = render_note_object(&services, &rejected, false).unwrap();
        assert!(doc.get("quoteAuthorization").is_none(), "rejected quotes carry no stamp");
        assert!(doc.get("quote").is_some(), "rejected quotes keep the relation");
    }

    #[test]
    fn pure_renote_still_renders_announce_without_quote_fields() {
        let services = test_services("announce");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let mut input = CreateNoteInput::simple_public(&bob.id, "x");
        input.text = None;
        input.renote_of = Some(target.id.clone());
        let renote = services.create_note(input).unwrap();
        assert!(!renote.is_quote());
        let activity = render_create_or_announce_for_note(&services, &renote).unwrap();
        assert_eq!(activity["type"], "Announce");
        assert!(activity.get("quote").is_none());
    }

    // --- §4 polls: Question render/parse -----------------------------------

    fn poll_fixture() -> crate::domain::poll::Poll {
        crate::domain::poll::Poll {
            note_id: "note1".to_string(),
            choices: vec!["red".to_string(), "blue".to_string()],
            votes: vec![3, 7],
            multiple: false,
            expires_at_ms: Some(1_760_400_060_000),
        }
    }

    #[test]
    fn render_question_matches_misskey_shape() {
        let now = 1_760_400_000_000;
        let poll = poll_fixture();
        let doc = render_question(
            "https://pinetree.local",
            "https://pinetree.local/users/alice",
            "note1",
            Some("pick a color"),
            &poll,
            now,
        );
        assert_eq!(doc["type"], "Question");
        assert_eq!(doc["id"], "https://pinetree.local/questions/note1");
        // Single-choice → `oneOf`, future expiry → `endTime`.
        assert!(doc.get("oneOf").is_some());
        assert!(doc.get("anyOf").is_none());
        assert!(doc.get("endTime").is_some());
        assert!(doc.get("closed").is_none());
        assert_eq!(doc["oneOf"][0]["name"], "red");
        assert_eq!(doc["oneOf"][0]["replies"]["totalItems"], 3);
        assert_eq!(doc["oneOf"][0]["_misskey_votes"], 3);
        assert_eq!(doc["oneOf"][1]["replies"]["totalItems"], 7);

        // Multi-choice → `anyOf`; past expiry → `closed`.
        let mut multi = poll;
        multi.multiple = true;
        let doc = render_question(
            "https://pinetree.local",
            "https://pinetree.local/users/alice",
            "note1",
            None,
            &multi,
            1_760_400_061_000,
        );
        assert!(doc.get("anyOf").is_some());
        assert!(doc.get("oneOf").is_none());
        assert!(doc.get("closed").is_some());
        assert!(doc.get("endTime").is_none());
    }

    #[test]
    fn parse_question_poll_reads_oneof_anyof_and_times() {
        // `oneOf` with `replies.totalItems`.
        let doc = serde_json::json!({
            "type": "Question",
            "id": "https://remote.example/questions/1",
            "oneOf": [
                {"name": "a", "replies": {"type": "Collection", "totalItems": 2}},
                {"name": "b", "replies": {"type": "Collection", "totalItems": 5}},
            ],
            "endTime": "2026-10-16T00:01:00.000Z",
        });
        let parsed = parse_question_poll(doc.as_object().unwrap()).unwrap();
        assert!(!parsed.multiple);
        assert_eq!(parsed.choices, vec!["a", "b"]);
        assert_eq!(parsed.votes, vec![2, 5]);
        assert_eq!(
            parsed.expires_at_ms,
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-10-16T00:01:00.000Z")
                    .unwrap()
                    .timestamp_millis()
            )
        );

        // `anyOf` with `_misskey_votes` fallback + `closed`.
        let doc = serde_json::json!({
            "type": "Question",
            "id": "https://remote.example/questions/2",
            "anyOf": [
                {"name": "x", "_misskey_votes": 1},
                {"name": "y", "_misskey_votes": 0},
            ],
            "closed": "2026-10-15T00:00:00.000Z",
        });
        let parsed = parse_question_poll(doc.as_object().unwrap()).unwrap();
        assert!(parsed.multiple);
        assert_eq!(parsed.votes, vec![1, 0]);
        assert!(parsed.expires_at_ms.is_some());

        // No options / fewer than two named choices → no poll.
        let doc = serde_json::json!({"type": "Note", "id": "https://x/1"});
        assert!(parse_question_poll(doc.as_object().unwrap()).is_none());
        let doc = serde_json::json!({
            "type": "Question", "id": "https://x/2",
            "oneOf": [{"name": "lonely"}],
        });
        assert!(parse_question_poll(doc.as_object().unwrap()).is_none());
    }

    #[test]
    fn question_render_parse_roundtrip() {
        let now = 1_760_400_000_000;
        let poll = poll_fixture();
        let doc = render_question(
            "https://pinetree.local",
            "https://pinetree.local/users/alice",
            "note1",
            Some("pick a color"),
            &poll,
            now,
        );
        let back = parse_question_poll(doc.as_object().unwrap()).unwrap();
        assert_eq!(back.choices, poll.choices);
        assert_eq!(back.votes, poll.votes);
        assert_eq!(back.multiple, poll.multiple);
        assert_eq!(back.expires_at_ms, poll.expires_at_ms);
    }

    #[test]
    fn render_note_object_embeds_question_for_poll_notes() {
        let services = test_services("question-embed");
        let alice = services.create_account("alice", None).unwrap();
        let mut input = CreateNoteInput::simple_public(&alice.id, "pick one");
        input.poll = Some(crate::domain::poll::PollInput {
            choices: vec!["a".to_string(), "b".to_string()],
            multiple: true,
            expires_at_ms: None,
        });
        input.has_poll = true;
        let note = services.create_note(input).unwrap();

        let doc = render_note_object(&services, &note, true).unwrap();
        // Misskey `renderNote` `...asPoll` parity: the poll overwrites the type.
        assert_eq!(doc["type"], "Question");
        assert!(doc.get("anyOf").is_some());
        assert_eq!(doc["anyOf"][0]["replies"]["totalItems"], 0);

        // Poll-less notes still render as `Note` with no options.
        let plain = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "plain"))
            .unwrap();
        let doc = render_note_object(&services, &plain, true).unwrap();
        assert_eq!(doc["type"], "Note");
        assert!(doc.get("oneOf").is_none());
        assert!(doc.get("anyOf").is_none());

        // The rendered Question re-parses (federation round-trip).
        let doc = render_note_object(&services, &note, true).unwrap();
        let parsed = parse_remote_note(&doc).unwrap();
        let poll = parsed.poll.unwrap();
        assert_eq!(poll.choices, vec!["a", "b"]);
        assert!(poll.multiple);
    }

    #[test]
    fn render_vote_activity_matches_misskey_shape() {
        let doc = render_vote_activity(
            "https://pinetree.local",
            "vote1",
            "https://pinetree.local/users/bob",
            "https://remote.example/users/alice",
            "https://remote.example/notes/1",
            "blue",
            1_760_400_000_000,
        );
        assert_eq!(doc["type"], "Create");
        assert_eq!(doc["actor"], "https://pinetree.local/users/bob");
        assert_eq!(doc["to"], serde_json::json!(["https://remote.example/users/alice"]));
        assert_eq!(doc["object"]["type"], "Note");
        assert_eq!(doc["object"]["inReplyTo"], "https://remote.example/notes/1");
        assert_eq!(doc["object"]["name"], "blue");
    }
}
