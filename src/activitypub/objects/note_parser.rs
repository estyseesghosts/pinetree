//! Inbound Note parsing: [`ParsedRemoteNote`] / [`ParsedAttachment`] and
//! the `Note`-object parser plus small reaction/object-URI extractors
//! (Slice B1).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::audience_context::extract_uri_list;
use super::markup::{sanitize_html_to_mfm, sanitize_mfm_text};
use super::poll::parse_question_poll;

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
    /// Pre-parsed AP `Emoji` tags (`(shortcode, icon_url)`), capped at
    /// 32 per object (WP8b ingest cap, stated in the session log).
    /// WP8b ingest source (metadata only).
    pub emojis: Vec<(String, String)>,
    pub attachments: Vec<ParsedAttachment>,
    pub sensitive: bool,
    /// The attached poll when the object carries `oneOf`/`anyOf`
    /// (Misskey `ApQuestionService.extractPollFromQuestion` parity).
    #[serde(default)]
    pub poll: Option<super::poll::ParsedPoll>,
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
    if !matches!(
        note_type,
        "Note" | "Article" | "Page" | "Question" | "Answer"
    ) {
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
    let mut emojis: Vec<(String, String)> = Vec::new();
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
                // WP8b: remote-emoji metadata only (no fetch at ingest).
                // Capped per object; unparseable entries are skipped.
                "Emoji" => {
                    if emojis.len() >= 32 {
                        continue;
                    }
                    if let Some((shortcode, url)) = crate::domain::emoji::parse_emoji_tag(item) {
                        emojis.push((shortcode, url));
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
                    Value::Array(arr) => arr
                        .first()
                        .and_then(|f| f.get("href"))
                        .and_then(Value::as_str),
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

    let normalized = crate::activitypub::quotes::normalize_inbound_quote(obj_val);
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
        emojis,
        attachments,
        sensitive,
        poll: parse_question_poll(obj),
        quote_target_uri: normalized.as_ref().map(|q| q.target_uri.clone()),
        quote_authorization_uri: crate::activitypub::quotes::inbound_authorization_iri(obj_val),
        quote_has_conflict: normalized
            .as_ref()
            .map(|q| !q.conflicting_aliases.is_empty())
            .unwrap_or(false),
    })
}

/// Extract the reaction string from an incoming `Like`/`EmojiReact`
/// activity: prefers `_misskey_reaction`, then `content`, then an `Emoji`
/// tag `name` (Akkoma custom-emoji shape), then the bare-heart fallback.
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
    // Akkoma custom-emoji `EmojiReact` carries `content: ":name:"` too, but
    // fall back to the `Emoji` tag name when `content` is absent.
    if let Some(tag_name) = activity
        .get("tag")
        .and_then(Value::as_array)
        .and_then(|tags| {
            tags.iter().find_map(|tag| {
                let map = tag.as_object()?;
                if map.get("type").and_then(Value::as_str) != Some("Emoji") {
                    return None;
                }
                map.get("name")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
            })
        })
    {
        return tag_name.to_string();
    }
    crate::domain::reaction::FALLBACK_REACTION.to_string()
}
