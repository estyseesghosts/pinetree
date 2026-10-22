//! FEP-044f quote posts + legacy quote compatibility (federation boundary).
//!
//! All wire-format parsing lives here. Adapters and domain logic consume only
//! the normalized [`NormalizedQuote`] / domain [`QuoteReference`][crate::domain::quote::QuoteReference],
//! never the raw field spellings.
//!
//! Supported inbound quote carriers (any one suffices):
//!
//! 1. FEP-044f `quote` (`https://w3id.org/fep/044f#quote`) — canonical.
//! 2. Misskey `_misskey_quote` (`https://misskey-hub.net/ns#_misskey_quote`,
//!    plus the slash variant `https://misskey-hub.net/ns/#_misskey_quote`).
//! 3. `quoteUrl` (ActivityStreams-namespace convention used by Misskey,
//!    Mastodon, Pleroma).
//! 4. Fedibird `quoteUri` (`http://fedibird.com/ns#quoteUri`).
//! 5. FEP-e232 `Link` in `tag` whose `rel` identifies the Misskey quote
//!    relation.
//!
//! Each carrier accepts scalar (IRI string), object (`{"id": ...}`), and
//! array forms. Only valid `http`/`https` URIs are accepted. When several
//! carriers agree they collapse to one relationship; when canonical `quote`
//! conflicts with legacy fields, `quote` wins and the conflict is reported
//! in diagnostics.

use serde_json::Value;

use crate::domain::quote::QuoteReference;

/// FEP-044f canonical quote property.
pub const FEP044F_QUOTE: &str = "https://w3id.org/fep/044f#quote";
/// FEP-044f quote authorization stamp property / type namespace.
pub const FEP044F_QUOTE_AUTHORIZATION: &str = "https://w3id.org/fep/044f#quoteAuthorization";
/// FEP-044f `QuoteAuthorization` object type IRI.
pub const FEP044F_QUOTE_AUTHORIZATION_TYPE: &str = "https://w3id.org/fep/044f#QuoteAuthorization";
/// FEP-044f `QuoteRequest` activity type IRI.
pub const FEP044F_QUOTE_REQUEST_TYPE: &str = "https://w3id.org/fep/044f#QuoteRequest";
/// GoToSocial interaction-policy namespace (hosts `interactionPolicy`,
/// `interactingObject`, `interactionTarget`).
pub const GOTOSOCIAL_NS: &str = "https://gotosocial.org/ns#";
/// Misskey `_misskey_quote`: documented no-slash form (what we emit).
pub const MISSKEY_QUOTE: &str = "https://misskey-hub.net/ns#_misskey_quote";
/// Misskey `_misskey_quote` slash variant (accepted, per FEP-044f's example).
pub const MISSKEY_QUOTE_SLASH: &str = "https://misskey-hub.net/ns/#_misskey_quote";
/// Legacy `quoteUrl` convention (ActivityStreams namespace; NOT part of the
/// core ActivityStreams standard).
pub const QUOTE_URL: &str = "https://www.w3.org/ns/activitystreams#quoteUrl";
/// Fedibird `quoteUri`.
pub const QUOTE_URI_FEDIBIRD: &str = "http://fedibird.com/ns#quoteUri";
/// FEP-e232 quote `Link` relation (Misskey quote relation IRI).
pub const QUOTE_LINK_REL: &str = "https://misskey-hub.net/ns#_misskey_quote";

/// Which wire carrier a normalized quote target came from (diagnostics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteSource {
    Fep044f,
    MisskeyQuote,
    QuoteUrl,
    QuoteUri,
    LinkTag,
}

/// A normalized inbound quote: one target URI plus diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedQuote {
    /// Canonical ActivityPub object ID of the quoted post.
    pub target_uri: String,
    /// Carrier that supplied the winning target.
    pub source: QuoteSource,
    /// Carriers that pointed at the same target (collapsed aliases).
    pub collapsed_from: Vec<QuoteSource>,
    /// True when legacy carriers pointed at a *different* target than the
    /// canonical `quote` (which won). The losing URIs are listed.
    pub conflicting_aliases: Vec<String>,
}

/// Extract candidate IRIs from a scalar / object / array property value.
fn extract_iris(val: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let mut push = |item: &Value| match item {
        Value::String(s) => {
            let s = s.trim();
            if !s.is_empty() {
                out.push(s.to_string());
            }
        }
        Value::Object(m) => {
            if let Some(id) = m.get("id").and_then(Value::as_str) {
                let id = id.trim();
                if !id.is_empty() {
                    out.push(id.to_string());
                }
            }
            // Some serializers nest the IRI under `href` (Link-style).
            if let Some(href) = m.get("href").and_then(Value::as_str) {
                let href = href.trim();
                if !href.is_empty() {
                    out.push(href.to_string());
                }
            }
        }
        _ => {}
    };
    match val {
        Value::Array(arr) => {
            for item in arr {
                push(item);
            }
        }
        other => push(other),
    }
    out
}

fn is_http_uri(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

/// First valid `http(s)` IRI from a property value, if any.
fn first_http_iri(val: Option<&Value>) -> Option<String> {
    val.and_then(|v| extract_iris(v).into_iter().find(|s| is_http_uri(s)))
}

/// Normalize the ActivityPub object `obj` into one [`NormalizedQuote`].
///
/// Returns `None` when no recognized carrier yields a valid `http(s)` URI.
/// Quote detection is independent of `quoteAuthorization`,
/// `interactionPolicy`, and request activities: legacy-only payloads yield a
/// quote all the same.
pub fn normalize_inbound_quote(obj: &Value) -> Option<NormalizedQuote> {
    let map = obj.as_object()?;
    // Compact (`quote`) and expanded-IRI (`https://w3id.org/fep/044f#quote`)
    // JSON-LD forms of the canonical carrier.
    let canonical =
        first_http_iri(map.get("quote")).or_else(|| first_http_iri(map.get(FEP044F_QUOTE)));
    let misskey = first_http_iri(map.get("_misskey_quote"))
        .or_else(|| first_http_iri(map.get(MISSKEY_QUOTE)))
        .or_else(|| first_http_iri(map.get(MISSKEY_QUOTE_SLASH)));
    let quote_url =
        first_http_iri(map.get("quoteUrl")).or_else(|| first_http_iri(map.get(QUOTE_URL)));
    // Fedibird has shipped both `quoteUri` and the `quoteURL` capitalization;
    // Pleroma normalizes all three spellings.
    let quote_uri = first_http_iri(map.get("quoteUri"))
        .or_else(|| first_http_iri(map.get("quoteURL")))
        .or_else(|| first_http_iri(map.get(QUOTE_URI_FEDIBIRD)));
    let link_tag = first_http_iri_from_link_tag(map.get("tag"));

    // Canonical `quote` wins on conflict; matching aliases collapse.
    if let Some(target) = canonical {
        let mut collapsed = Vec::new();
        let mut conflicting = Vec::new();
        for (src, uri) in [
            (QuoteSource::MisskeyQuote, misskey),
            (QuoteSource::QuoteUrl, quote_url),
            (QuoteSource::QuoteUri, quote_uri),
            (QuoteSource::LinkTag, link_tag),
        ] {
            if let Some(u) = uri {
                if u == target {
                    collapsed.push(src);
                } else if !conflicting.contains(&u) {
                    conflicting.push(u);
                }
            }
        }
        return Some(NormalizedQuote {
            target_uri: target,
            source: QuoteSource::Fep044f,
            collapsed_from: collapsed,
            conflicting_aliases: conflicting,
        });
    }

    // No canonical carrier: first legacy hit wins (fixed priority order),
    // the rest collapse or conflict among themselves.
    let legacy = [
        (QuoteSource::MisskeyQuote, misskey),
        (QuoteSource::QuoteUrl, quote_url),
        (QuoteSource::QuoteUri, quote_uri),
        (QuoteSource::LinkTag, link_tag),
    ];
    let mut winner: Option<(QuoteSource, String)> = None;
    let mut conflicting = Vec::new();
    for (src, uri) in legacy {
        if let Some(u) = uri {
            match &winner {
                None => winner = Some((src, u)),
                Some((_, w)) if *w == u => {}
                Some(_) => {
                    if !conflicting.contains(&u) {
                        conflicting.push(u);
                    }
                }
            }
        }
    }
    winner.map(|(source, target_uri)| NormalizedQuote {
        target_uri,
        source,
        collapsed_from: Vec::new(),
        conflicting_aliases: conflicting,
    })
}

/// Extract the first FEP-e232 quote `Link` from a `tag` value: a `Link`
/// object whose `rel` (string or array) names the Misskey quote relation
/// (either slash variant). Returns its `href` when it is an `http(s)` URI.
fn first_http_iri_from_link_tag(tag: Option<&Value>) -> Option<String> {
    let items: Vec<&Value> = match tag {
        Some(Value::Array(arr)) => arr.iter().collect(),
        Some(other) => vec![other],
        None => return None,
    };
    for item in items {
        let map = match item.as_object() {
            Some(m) => m,
            None => continue,
        };
        let type_ok = matches!(
            map.get("type").and_then(Value::as_str).unwrap_or(""),
            "Link" | "ObjectLink" | "Linkage"
        );
        if !type_ok {
            continue;
        }
        let rel_matches = match map.get("rel") {
            Some(Value::String(r)) => r == MISSKEY_QUOTE || r == MISSKEY_QUOTE_SLASH,
            Some(Value::Array(rels)) => rels.iter().any(|r| {
                r.as_str()
                    .map(|s| s == MISSKEY_QUOTE || s == MISSKEY_QUOTE_SLASH)
                    .unwrap_or(false)
            }),
            _ => {
                // Some emitters put the relation on `rel` as an object with `id`/`href`.
                match map.get("rel") {
                    Some(Value::Object(rm)) => ["id", "href"].iter().any(|k| {
                        rm.get(*k)
                            .and_then(Value::as_str)
                            .map(|s| s == MISSKEY_QUOTE || s == MISSKEY_QUOTE_SLASH)
                            .unwrap_or(false)
                    }),
                    _ => false,
                }
            }
        };
        if !rel_matches {
            continue;
        }
        if let Some(href) = map.get("href").and_then(Value::as_str) {
            let href = href.trim();
            if is_http_uri(href) {
                return Some(href.to_string());
            }
        }
    }
    None
}

/// Raw `quoteAuthorization` IRI on an inbound Note object, if present and an
/// `http(s)` URI. Presence alone never proves approval — the stamp must be
/// dereferenced and verified.
pub fn inbound_authorization_iri(obj: &Value) -> Option<String> {
    let map = obj.as_object()?;
    let val = map
        .get("quoteAuthorization")
        .or_else(|| map.get(FEP044F_QUOTE_AUTHORIZATION))?;
    extract_iris(val).into_iter().find(|s| is_http_uri(s))
}

impl From<&NormalizedQuote> for QuoteReference {
    /// Legacy/no-evidence normalization: `unapproved`, never `rejected`.
    fn from(n: &NormalizedQuote) -> Self {
        QuoteReference::unapproved(n.target_uri.clone())
    }
}

// ---------------------------------------------------------------------------
// Outbound serialization helpers
// ---------------------------------------------------------------------------

/// JSON-LD context entries for quote properties (merged into `ap_context`).
pub fn quote_context_entries() -> serde_json::Value {
    serde_json::json!({
        "quote": { "@id": FEP044F_QUOTE, "@type": "@id" },
        "quoteAuthorization": { "@id": FEP044F_QUOTE_AUTHORIZATION, "@type": "@id" },
        "_misskey_quote": MISSKEY_QUOTE,
        "quoteUrl": QUOTE_URL,
        "quoteUri": QUOTE_URI_FEDIBIRD,
        "gts": GOTOSOCIAL_NS,
        "interactionPolicy": { "@id": "gts:interactionPolicy", "@type": "@id" },
        "interactingObject": { "@id": "gts:interactingObject", "@type": "@id" },
        "interactionTarget": { "@id": "gts:interactionTarget", "@type": "@id" },
        "automaticApproval": { "@id": "gts:automaticApproval", "@type": "@id" },
        "manualApproval": { "@id": "gts:manualApproval", "@type": "@id" },
    })
}

/// Canonical + legacy compatibility fields for a structured quote target.
/// `authorization_uri` is emitted as `quoteAuthorization` only when `Some`.
pub fn render_quote_fields(target_uri: &str, authorization_uri: Option<&str>) -> serde_json::Value {
    let mut obj = serde_json::json!({
        "quote": target_uri,
        "_misskey_quote": target_uri,
        "quoteUrl": target_uri,
        "quoteUri": target_uri,
    });
    if let Some(stamp) = authorization_uri {
        obj["quoteAuthorization"] = serde_json::Value::String(stamp.to_string());
    }
    obj
}

/// FEP-e232 compatibility `Link` tag for a quote target.
pub fn render_quote_link_tag(target_uri: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "Link",
        "mediaType": "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"",
        "href": target_uri,
        "rel": MISSKEY_QUOTE,
    })
}

/// Advisory `interactionPolicy.canQuote` for a local note.
///
/// MVP default: public/unlisted posts allow automatic approval for the
/// Public collection; anything more restricted narrows to the author's own
/// actor IRI (FEP-044f's recommended "disallow others" form) rather than an
/// empty array. `manualApproval` is never advertised: no durable
/// pending-request review UI exists. These fields are advisory only and never
/// prove a particular quote is approved.
pub fn render_quote_policy(is_public_audience: bool, author_actor_uri: &str) -> serde_json::Value {
    let automatic = if is_public_audience {
        serde_json::json!(["https://www.w3.org/ns/activitystreams#Public"])
    } else {
        serde_json::json!([author_actor_uri])
    };
    serde_json::json!({
        "interactionPolicy": {
            "canQuote": { "automaticApproval": automatic }
        }
    })
}

/// True when the author's stored text already ends with an equivalent
/// terminal `RE: <url>` (or Fedibird `QT: <url>`) fallback line for the same
/// URL (trailing whitespace tolerated). Only a terminal line counts, so
/// mid-text mentions never suppress the fallback. Prefix match is
/// case-insensitive (`re:`/`qt:` count).
pub fn has_terminal_re_fallback(text: Option<&str>, target_url: &str) -> bool {
    let Some(text) = text else { return false };
    let target = target_url.trim();
    // Last non-empty line must be `RE: <url>` or `QT: <url>`
    // (angle-bracket-wrapped OK).
    if let Some(last) = text.lines().map(str::trim).filter(|l| !l.is_empty()).last() {
        let upper = last.to_ascii_uppercase();
        let rest = last
            .get(3..)
            .filter(|_| upper.starts_with("RE:") || upper.starts_with("QT:"))
            .map(str::trim)
            .unwrap_or("");
        let rest = rest
            .strip_prefix('<')
            .and_then(|s| s.strip_suffix('>'))
            .unwrap_or(rest);
        return rest.trim() == target;
    }
    false
}

/// Render the textual fallback suffix for serialized `content`.
/// Returns `None` when no fallback should be added (private target, or the
/// author already wrote an equivalent terminal line).
pub fn re_fallback_suffix(
    text: Option<&str>,
    target_url: &str,
    target_is_public: bool,
) -> Option<String> {
    if !target_is_public {
        return None;
    }
    if has_terminal_re_fallback(text, target_url) {
        return None;
    }
    Some(format!("RE: {target_url}"))
}

// ---------------------------------------------------------------------------
// Stamp verification + FEP-044f exceptions (pure; fetching is the caller's job)
// ---------------------------------------------------------------------------

/// Validate a dereferenced `QuoteAuthorization` stamp document against all
/// five checks: object type, `interactingObject`, `interactionTarget`,
/// `attributedTo`, and non-emptiness of the stamp id itself (authenticity of
/// the fetch — trusted ActivityPub identity + safe dereference path — is
/// established by the caller before invoking this).
pub fn verify_stamp(
    stamp: &Value,
    stamp_id: &str,
    quote_note_uri: &str,
    quoted_target_uri: &str,
    quoted_author_actor_uri: &str,
) -> Result<(), String> {
    let map = stamp
        .as_object()
        .ok_or_else(|| "stamp must be a JSON object".to_string())?;
    let stamp_type = map.get("type").and_then(Value::as_str).unwrap_or("");
    if stamp_type != "QuoteAuthorization" {
        return Err(format!(
            "stamp type must be QuoteAuthorization, got {stamp_type}"
        ));
    }
    let id = map
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if id.is_empty() {
        return Err("stamp missing id".to_string());
    }
    if id != stamp_id.trim() {
        return Err("stamp id mismatch".to_string());
    }
    let interacting = single_iri(map.get("interactingObject"))
        .ok_or_else(|| "stamp missing interactingObject".to_string())?;
    if interacting != quote_note_uri.trim() {
        return Err("stamp interactingObject mismatch".to_string());
    }
    let target = single_iri(map.get("interactionTarget"))
        .ok_or_else(|| "stamp missing interactionTarget".to_string())?;
    if target != quoted_target_uri.trim() {
        return Err("stamp interactionTarget mismatch".to_string());
    }
    let attributed = single_iri(map.get("attributedTo"))
        .ok_or_else(|| "stamp missing attributedTo".to_string())?;
    if attributed != quoted_author_actor_uri.trim() {
        return Err("stamp attributedTo is not the quoted author".to_string());
    }
    Ok(())
}

/// Full stamp check for a concrete quote: the five document checks in
/// [`verify_stamp`] plus authority — the stamp IRI must live on the quoted
/// author's host (a stamp minted elsewhere, even with a copied
/// `attributedTo`, is invalid). No `Accept` alone and no advertised policy
/// ever substitutes for this.
pub fn verify_stamp_for_quote(
    stamp: &Value,
    stamp_id: &str,
    quote_note_uri: &str,
    quoted_target_uri: &str,
    quoted_author_actor_uri: &str,
) -> Result<(), String> {
    verify_stamp(
        stamp,
        stamp_id,
        quote_note_uri,
        quoted_target_uri,
        quoted_author_actor_uri,
    )?;
    // Authority is EXACT-host (WP5): the stamp IRI must live on the quoted
    // author's exact `host[:port]` — not a subdomain, not a sibling port,
    // not a bare-host-vs-port split. Comparison runs through
    // `fed_policy::exact_authority_allows` (lowercase, trailing-dot
    // tolerant, scheme- and port-sensitive) rather than a substring or
    // registrable-domain check.
    if !crate::domain::fed_policy::exact_authority_allows(stamp_id, quoted_author_actor_uri) {
        return Err("stamp authority: stamp must live on the quoted author's host".to_string());
    }
    Ok(())
}

fn single_iri(val: Option<&Value>) -> Option<String> {
    match val {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Some(Value::Object(m)) => m
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned),
        Some(Value::Array(arr)) => arr.iter().find_map(|i| single_iri(Some(i))),
        _ => None,
    }
}

/// FEP-044f approval exceptions (pure identity comparison — never substring
/// matching):
///
/// - self-quote: the quote author IS the quoted author;
/// - mention: the quoted post's structured mentions include the quote
///   author's actor URI.
pub fn approval_exception(
    quote_author_actor_uri: &str,
    quoted_author_actor_uri: &str,
    quoted_mention_actor_uris: &[String],
) -> bool {
    if quote_author_actor_uri.trim() == quoted_author_actor_uri.trim() {
        return true;
    }
    quoted_mention_actor_uris
        .iter()
        .any(|m| m.trim() == quote_author_actor_uri.trim())
}

#[cfg(test)]
mod tests;
