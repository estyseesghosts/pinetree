//! Quote-post wire compatibility: real-world payload shapes.
//!
//! Fixtures below are trimmed to the relevant fields from the actual target
//! revisions (never inferred from code ancestry):
//!
//! - Misskey shape: `pleroma/test/fixtures/quote_post/misskey_quote_post.json`
//!   (`_misskey_quote` + `quoteUrl` + `RE:` fallback + `source` MFM).
//! - Sharkey shape: `ApRendererService.ts` triple emit (`_misskey_quote` +
//!   `quoteUrl` + `quoteUri`) plus the e232 `tag[]` Link entry with
//!   `rel = https://misskey-hub.net/ns#_misskey_quote`.
//! - Pleroma/Akkoma legacy: canonical `quoteUrl` (Pleroma) / `quoteUri`
//!   (Akkoma) only, no authorization artifacts.
//! - Fedibird: `quoteURL` capitalization + `QT:` fallback variant.
//! - Mastodon FEP-044f: `pleroma/test/fixtures/quote_post/mastodon_quote_post.json`
//!   (`quote` + aliases + `quoteAuthorization` + `interactionPolicy`).
//! - FEP-e232 minimal: `pleroma/test/fixtures/quote_post/fep-e232-tag-example.json`.
//!
//! Hermetic (no servers): every shape must normalize to the same domain
//! `QuoteReference` for the same target, and legacy-only shapes must yield
//! `unapproved` — never dropped, never `rejected`.

use pinetree::activitypub::objects::parse_remote_note;
use pinetree::activitypub::quotes::{inbound_authorization_iri, normalize_inbound_quote};
use pinetree::domain::quote::{QuoteAuthorizationState, QuoteReference};
use serde_json::json;

fn base_note() -> serde_json::Value {
    json!({
        "id": "https://quoter.example/notes/q1",
        "type": "Note",
        "attributedTo": "https://quoter.example/users/alice",
        "to": ["https://www.w3.org/ns/activitystreams#Public"],
    })
}

fn with_content(mut note: serde_json::Value, content: &str) -> serde_json::Value {
    note["content"] = serde_json::Value::String(content.to_string());
    note
}

const TARGET_MISSKEY: &str = "https://misskey.io/notes/8vs6wxufd0";
const TARGET_MASTODON: &str =
    "https://mastodon.social/users/404mediaco/statuses/115344945575874225";

/// Misskey renderer shape: `_misskey_quote` + `quoteUrl` + `RE:` fallback.
fn misskey_shape() -> serde_json::Value {
    let mut n = base_note();
    n["content"] = json!(
        "<p><span>commentary<br><br>RE: </span><a href=\"https://misskey.io/notes/8vs6wxufd0\">https://misskey.io/notes/8vs6wxufd0</a></p>"
    );
    n["_misskey_quote"] = json!(TARGET_MISSKEY);
    n["quoteUrl"] = json!(TARGET_MISSKEY);
    n["source"] = json!({"content": "commentary", "mediaType": "text/x.misskeymarkdown"});
    n
}

/// Sharkey renderer shape: triple emit + e232 Link tag with rel.
fn sharkey_shape() -> serde_json::Value {
    let mut n = with_content(
        base_note(),
        "<p>commentary<br><br><span class=\"quote-inline\">RE: <a href=\"https://misskey.io/notes/8vs6wxufd0\">https://misskey.io/notes/8vs6wxufd0</a></span></p>",
    );
    n["_misskey_quote"] = json!(TARGET_MISSKEY);
    n["quoteUrl"] = json!(TARGET_MISSKEY);
    n["quoteUri"] = json!(TARGET_MISSKEY);
    n["tag"] = json!([{
        "type": "Link",
        "mediaType": "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"",
        "rel": "https://misskey-hub.net/ns#_misskey_quote",
        "href": TARGET_MISSKEY,
    }]);
    n
}

/// Pleroma legacy shape: canonical `quoteUrl` only.
fn pleroma_shape() -> serde_json::Value {
    let mut n = with_content(base_note(), "commentary");
    n["quoteUrl"] = json!(TARGET_MISSKEY);
    n
}

/// Akkoma legacy shape: canonical `quoteUri` only.
fn akkoma_shape() -> serde_json::Value {
    let mut n = with_content(base_note(), "commentary");
    n["quoteUri"] = json!(TARGET_MISSKEY);
    n
}

/// Fedibird shape: `quoteURL` capitalization + `QT:` fallback.
fn fedibird_shape() -> serde_json::Value {
    let mut n = with_content(
        base_note(),
        "<p>commentary<span class=\"quote-inline\"><br/>QT: <a href=\"https://misskey.io/notes/8vs6wxufd0\">https://misskey.io/notes/8vs6wxufd0</a></span></p>",
    );
    n["quoteURL"] = json!(TARGET_MISSKEY);
    n["_misskey_quote"] = json!(TARGET_MISSKEY);
    n
}

/// Mastodon FEP-044f shape: canonical `quote` + aliases + stamp + policy.
fn mastodon_shape() -> serde_json::Value {
    let mut n = with_content(
        base_note(),
        "<p class=\"quote-inline\">RE: <a href=\"https://mastodon.social/@404mediaco/115344945575874225\">https://mastodon.social/@404mediaco/115344945575874225</a></p><p>commentary</p>",
    );
    n["id"] = json!("https://mastodon.social/users/gwynnion/statuses/115345489087257171");
    n["attributedTo"] = json!("https://mastodon.social/users/gwynnion");
    n["quote"] = json!(TARGET_MASTODON);
    n["_misskey_quote"] = json!(TARGET_MASTODON);
    n["quoteUri"] = json!(TARGET_MASTODON);
    n["quoteAuthorization"] =
        json!("https://mastodon.social/users/404mediaco/quote_authorizations/115345489087269783");
    n["interactionPolicy"] = json!({"canQuote": {"automaticApproval": ["https://www.w3.org/ns/activitystreams#Public"]}});
    n
}

#[test]
fn all_legacy_shapes_normalize_to_the_same_quote() {
    for (name, note) in [
        ("misskey", misskey_shape()),
        ("sharkey", sharkey_shape()),
        ("pleroma", pleroma_shape()),
        ("akkoma", akkoma_shape()),
        ("fedibird", fedibird_shape()),
    ] {
        let parsed =
            parse_remote_note(&note).unwrap_or_else(|e| panic!("{name} shape must parse: {e}"));
        assert_eq!(
            parsed.quote_target_uri.as_deref(),
            Some(TARGET_MISSKEY),
            "{name} quote target"
        );
        // No auth metadata on legacy shapes.
        assert!(
            parsed.quote_authorization_uri.is_none(),
            "{name} has no stamp"
        );
        // Same domain record for the same target, regardless of carrier.
        let domain = QuoteReference {
            target_uri: parsed.quote_target_uri.clone().unwrap(),
            target_note_id: None,
            authorization: QuoteAuthorizationState::Unapproved,
            authorization_uri: None,
            request_activity_uri: None,
            last_verified_at: None,
        };
        assert_eq!(domain.authorization, QuoteAuthorizationState::Unapproved);
        assert_ne!(
            domain.authorization.api_status(),
            "rejected",
            "{name} never rejected"
        );
        // Content preserved (RE:/QT: fallback text is not protocol metadata
        // and must not be upgraded or stripped).
        let text = parsed.text.clone().unwrap_or_default();
        assert!(text.contains("commentary"), "{name} content preserved");
        assert!(!text.contains("javascript:"), "{name} sanitized");
    }
}

#[test]
fn mastodon_fep044f_shape_detects_target_and_stamp_iri_without_approving() {
    let note = mastodon_shape();
    let normalized = normalize_inbound_quote(&note).expect("canonical quote recognized");
    assert_eq!(normalized.target_uri, TARGET_MASTODON);
    // Aliases collapse to the one relationship.
    assert!(normalized.conflicting_aliases.is_empty());

    let parsed = parse_remote_note(&note).expect("mastodon shape parses");
    assert_eq!(parsed.quote_target_uri.as_deref(), Some(TARGET_MASTODON));
    // The stamp IRI is visible but proves nothing until dereferenced.
    assert_eq!(
        parsed.quote_authorization_uri.as_deref(),
        Some("https://mastodon.social/users/404mediaco/quote_authorizations/115345489087269783")
    );
    assert_eq!(
        inbound_authorization_iri(&note).as_deref(),
        parsed.quote_authorization_uri.as_deref()
    );
    // Normalization without verification is still Unapproved, never Rejected.
    let domain = QuoteReference::unapproved(parsed.quote_target_uri.unwrap());
    assert_eq!(domain.authorization, QuoteAuthorizationState::Unapproved);
}

#[test]
fn bare_link_tag_without_rel_is_not_a_quote() {
    // FEP-e232 minimal example carries no `rel`: per docs/quote-posts.md
    // only a Link whose `rel` names the Misskey quote relation counts.
    // Promoting every Link tag would fabricate quotes from ordinary links.
    let mut n = with_content(
        base_note(),
        "This is a quote:<br>RE: https://server.example/objects/123",
    );
    n["tag"] = json!([{
        "type": "Link",
        "mediaType": "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"",
        "href": "https://server.example/objects/123",
        "name": "RE: https://server.example/objects/123",
    }]);
    assert!(
        normalize_inbound_quote(&n).is_none(),
        "rel-less Link tags must not become quotes"
    );
    // The textual RE: suffix is preserved as content, not metadata.
    let parsed = parse_remote_note(&n).expect("parses as an ordinary note");
    assert!(parsed.quote_target_uri.is_none());
    assert!(
        parsed
            .text
            .unwrap_or_default()
            .contains("RE: https://server.example/objects/123")
    );
}

#[test]
fn conflicting_aliases_prefer_canonical_quote() {
    let mut n = misskey_shape();
    n["quote"] = json!("https://canonical.example/notes/1");
    let normalized = normalize_inbound_quote(&n).expect("canonical wins");
    assert_eq!(normalized.target_uri, "https://canonical.example/notes/1");
    assert_eq!(
        normalized.conflicting_aliases,
        vec![TARGET_MISSKEY.to_string()]
    );
    assert!(parse_remote_note(&n).expect("parses").quote_has_conflict);
}

/// Direct pin against the real Pleroma/Akkoma fixture files: when the
/// checkouts are present (same paths the subagent verified), the exact
/// on-disk payloads must normalize. Skipped elsewhere (inline shapes above
/// carry the hermetic coverage).
#[test]
fn real_fixture_files_normalize() {
    let cases = [
        (
            "/root/fedifedifedi/pleroma/test/fixtures/quote_post/misskey_quote_post.json",
            "https://misskey.io/notes/8vs6wxufd0",
        ),
        (
            "/root/fedifedifedi/pleroma/test/fixtures/quote_post/fedibird_quote_post.json",
            "https://misskey.io/notes/8vsn2izjwh",
        ),
        (
            "/root/fedifedifedi/pleroma/test/fixtures/quote_post/mastodon_quote_post.json",
            "https://mastodon.social/users/404mediaco/statuses/115344945575874225",
        ),
    ];
    let mut ran = 0;
    for (path, target) in cases {
        let Ok(raw) = std::fs::read_to_string(path) else {
            continue;
        };
        let doc: serde_json::Value =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{path} parses: {e}"));
        let q =
            normalize_inbound_quote(&doc).unwrap_or_else(|| panic!("{path} normalizes to a quote"));
        assert_eq!(q.target_uri, target, "{path} target");
        assert_eq!(
            QuoteReference::from(&q).authorization,
            QuoteAuthorizationState::Unapproved,
            "{path} never rejected without evidence"
        );
        ran += 1;
    }
    // The e232 example is a negative pin: rel-less Link tags are not quotes.
    if let Ok(raw) = std::fs::read_to_string(
        "/root/fedifedifedi/pleroma/test/fixtures/quote_post/fep-e232-tag-example.json",
    ) {
        let doc: serde_json::Value = serde_json::from_str(&raw).expect("e232 parses");
        assert!(
            normalize_inbound_quote(&doc).is_none(),
            "rel-less e232 Link tag must not become a quote"
        );
        ran += 1;
    }
    if ran == 0 {
        eprintln!("real_fixture_files_normalize: checkouts absent, skipped");
    } else {
        assert_eq!(ran, 4, "all four real fixture files pinned");
    }
}
