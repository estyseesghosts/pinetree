// nt: unit tests moved verbatim from `quotes.rs`; `use super::*` resolves identically.
use super::*;
use serde_json::json;

fn note_with(fields: serde_json::Value) -> Value {
    let mut base = json!({
        "id": "https://remote.example/notes/q1",
        "type": "Note",
        "attributedTo": "https://remote.example/users/alice",
        "content": "commentary",
        "to": ["https://www.w3.org/ns/activitystreams#Public"],
    });
    for (k, v) in fields.as_object().unwrap() {
        base[k] = v.clone();
    }
    base
}

#[test]
fn parses_fep044f_quote_string_and_object_forms() {
    let target = "https://target.example/notes/1";
    let n = note_with(json!({"quote": target}));
    let q = normalize_inbound_quote(&n).unwrap();
    assert_eq!(q.target_uri, target);
    assert_eq!(q.source, QuoteSource::Fep044f);

    let n = note_with(json!({"quote": {"id": target}}));
    assert_eq!(normalize_inbound_quote(&n).unwrap().target_uri, target);

    let n = note_with(json!({"quote": [target]}));
    assert_eq!(normalize_inbound_quote(&n).unwrap().target_uri, target);

    // Expanded-IRI JSON-LD form of the canonical carrier.
    let n = note_with(json!({FEP044F_QUOTE: target}));
    let q = normalize_inbound_quote(&n).unwrap();
    assert_eq!(q.target_uri, target);
    assert_eq!(q.source, QuoteSource::Fep044f);
}

#[test]
fn parses_all_legacy_carriers_with_no_auth_metadata() {
    let target = "https://target.example/notes/1";
    for (field, value) in [
        ("_misskey_quote", json!(target)),
        ("_misskey_quote", json!({"id": target})),
        ("_misskey_quote", json!([target])),
        ("quoteUrl", json!(target)),
        ("quoteUrl", json!([{"id": target}])),
        ("quoteUri", json!(target)),
        // Fedibird's `quoteURL` capitalization (real-world fixture).
        ("quoteURL", json!(target)),
    ] {
        let n = note_with(json!({field: value}));
        let q =
            normalize_inbound_quote(&n).unwrap_or_else(|| panic!("carrier {field} not recognized"));
        assert_eq!(q.target_uri, target, "carrier {field}");
        // Legacy-only with no auth metadata still yields a quote.
        let domain = QuoteReference::from(&q);
        assert_eq!(
            domain.authorization,
            crate::domain::quote::QuoteAuthorizationState::Unapproved
        );
    }
    // Full-IRI key forms.
    for key in [
        MISSKEY_QUOTE,
        MISSKEY_QUOTE_SLASH,
        QUOTE_URL,
        QUOTE_URI_FEDIBIRD,
    ] {
        let n = note_with(json!({key: target}));
        assert!(
            normalize_inbound_quote(&n).is_some(),
            "full-IRI carrier {key} not recognized"
        );
    }
}

#[test]
fn parses_fep_e232_link_tag() {
    let target = "https://target.example/notes/1";
    let n = note_with(json!({"tag": [{
        "type": "Link",
        "mediaType": "application/activity+json",
        "href": target,
        "rel": MISSKEY_QUOTE,
    }]}));
    let q = normalize_inbound_quote(&n).unwrap();
    assert_eq!(q.source, QuoteSource::LinkTag);
    assert_eq!(q.target_uri, target);

    // Wrong rel is ignored.
    let n = note_with(json!({"tag": [{
        "type": "Link",
        "href": target,
        "rel": "https://example.com/other",
    }]}));
    assert!(normalize_inbound_quote(&n).is_none());
}

#[test]
fn collapses_matching_aliases_and_prefers_canonical_on_conflict() {
    let target = "https://target.example/notes/1";
    let n = note_with(json!({
        "quote": target,
        "_misskey_quote": target,
        "quoteUrl": target,
        "quoteUri": target,
    }));
    let q = normalize_inbound_quote(&n).unwrap();
    assert_eq!(q.target_uri, target);
    assert_eq!(q.source, QuoteSource::Fep044f);
    assert_eq!(q.collapsed_from.len(), 3);
    assert!(q.conflicting_aliases.is_empty());

    let other = "https://other.example/notes/2";
    let n = note_with(json!({"quote": target, "_misskey_quote": other}));
    let q = normalize_inbound_quote(&n).unwrap();
    assert_eq!(q.target_uri, target, "canonical quote wins");
    assert_eq!(q.conflicting_aliases, vec![other.to_string()]);
}

#[test]
fn rejects_non_http_targets_and_empty_objects() {
    for val in [
        json!("gopher://x/y"),
        json!(""),
        json!({"id": "ftp://x/y"}),
        json!(42),
    ] {
        let n = note_with(json!({"quote": val, "_misskey_quote": val}));
        assert!(
            normalize_inbound_quote(&n).is_none(),
            "non-http carrier must not normalize: {val}"
        );
    }
    assert!(normalize_inbound_quote(&json!({"type": "Note"})).is_none());
}

#[test]
fn authorization_iri_extraction_never_implies_approval() {
    let n = note_with(json!({
        "_misskey_quote": "https://target.example/notes/1",
        "quoteAuthorization": "https://target.example/stamps/1",
    }));
    assert_eq!(
        inbound_authorization_iri(&n).as_deref(),
        Some("https://target.example/stamps/1")
    );
    // No stamp: None, and normalization still yields Unapproved.
    let n = note_with(json!({"quoteUrl": "https://target.example/notes/1"}));
    assert!(inbound_authorization_iri(&n).is_none());
    let domain = QuoteReference::from(&normalize_inbound_quote(&n).unwrap());
    assert_ne!(
        domain.authorization.api_status(),
        "rejected",
        "missing auth metadata is never rejected"
    );
}

#[test]
fn stamp_verification_all_five_checks() {
    let stamp_id = "https://target.example/stamps/1";
    let quote_uri = "https://quoting.example/notes/q";
    let target_uri = "https://target.example/notes/1";
    let author_uri = "https://target.example/users/bob";
    let good = json!({
        "id": stamp_id,
        "type": "QuoteAuthorization",
        "attributedTo": author_uri,
        "interactingObject": quote_uri,
        "interactionTarget": target_uri,
    });
    assert!(verify_stamp(&good, stamp_id, quote_uri, target_uri, author_uri).is_ok());

    // Wrong type.
    let mut bad = good.clone();
    bad["type"] = json!("Note");
    assert!(verify_stamp(&bad, stamp_id, quote_uri, target_uri, author_uri).is_err());
    // Wrong quote note.
    let mut bad = good.clone();
    bad["interactingObject"] = json!("https://evil.example/notes/x");
    assert!(verify_stamp(&bad, stamp_id, quote_uri, target_uri, author_uri).is_err());
    // Wrong target.
    let mut bad = good.clone();
    bad["interactionTarget"] = json!("https://evil.example/notes/x");
    assert!(verify_stamp(&bad, stamp_id, quote_uri, target_uri, author_uri).is_err());
    // Wrong author.
    let mut bad = good.clone();
    bad["attributedTo"] = json!("https://evil.example/users/eve");
    assert!(verify_stamp(&bad, stamp_id, quote_uri, target_uri, author_uri).is_err());
    // Malformed: missing id.
    let mut bad = good.clone();
    bad.as_object_mut().unwrap().remove("id");
    assert!(verify_stamp(&bad, stamp_id, quote_uri, target_uri, author_uri).is_err());
}

#[test]
fn stamp_authority_is_exact_host() {
    let quote_uri = "https://quoting.example/notes/q";
    let target_uri = "https://target.example/notes/1";
    let stamp_id = "https://target.example/stamps/1";
    let author_uri = "https://target.example/users/bob";
    let good = json!({
        "id": stamp_id,
        "type": "QuoteAuthorization",
        "attributedTo": author_uri,
        "interactingObject": quote_uri,
        "interactionTarget": target_uri,
    });
    assert!(verify_stamp_for_quote(&good, stamp_id, quote_uri, target_uri, author_uri).is_ok());
    // Sibling port: different authority.
    let elsewhere = "https://target.example:8443/stamps/1";
    let mut bad = good.clone();
    bad["id"] = json!(elsewhere);
    assert!(verify_stamp_for_quote(&bad, elsewhere, quote_uri, target_uri, author_uri).is_err());
    // Subdomain: different authority.
    let sub = "https://sub.target.example/stamps/1";
    let mut bad = good.clone();
    bad["id"] = json!(sub);
    assert!(verify_stamp_for_quote(&bad, sub, quote_uri, target_uri, author_uri).is_err());
    // Trailing dot: same authority (tolerant).
    let dotted = "https://target.example./stamps/1";
    let mut ok = good.clone();
    ok["id"] = json!(dotted);
    assert!(verify_stamp_for_quote(&ok, dotted, quote_uri, target_uri, author_uri).is_ok());
    // Scheme downgrade: different authority.
    let plain = "http://target.example/stamps/1";
    let mut bad = good.clone();
    bad["id"] = json!(plain);
    assert!(verify_stamp_for_quote(&bad, plain, quote_uri, target_uri, author_uri).is_err());
}

#[test]
fn exceptions_use_structured_identity_not_substrings() {
    let alice = "https://a.example/users/alice";
    let bob = "https://b.example/users/bob";
    assert!(approval_exception(alice, alice, &[]), "self-quote");
    // Structured identity: a URI that merely contains the quoter as a
    // substring-adjacent value must not match.
    assert!(
        !approval_exception(alice, bob, &["https://b.example/users/other".to_string()]),
        "must not be a substring match"
    );
    assert!(
        approval_exception(alice, bob, &[alice.to_string()]),
        "mentioned quoter"
    );
    assert!(!approval_exception(alice, bob, &[]));
}

#[test]
fn re_fallback_rules() {
    let url = "https://target.example/notes/1";
    // Appended once for public targets.
    assert_eq!(
        re_fallback_suffix(Some("commentary"), url, true),
        Some(format!("RE: {url}"))
    );
    // Never twice.
    assert_eq!(
        re_fallback_suffix(
            Some("commentary\nRE: https://target.example/notes/1"),
            url,
            true
        ),
        None
    );
    assert_eq!(
        re_fallback_suffix(
            Some("commentary\nRE: https://target.example/notes/1\n"),
            url,
            true
        ),
        None
    );
    // Mid-text mention does not suppress (only terminal lines count).
    assert!(
        re_fallback_suffix(
            Some("see RE: https://target.example/notes/1 for context\nmore"),
            url,
            true
        )
        .is_some()
    );
    // Fedibird QT: is an equivalent terminal fallback (no second RE:).
    assert_eq!(
        re_fallback_suffix(
            Some("commentary\nQT: https://target.example/notes/1"),
            url,
            true
        ),
        None
    );
    assert_eq!(
        re_fallback_suffix(
            Some("commentary\nqt: https://target.example/notes/1"),
            url,
            true
        ),
        None
    );
    // Never for private targets.
    assert_eq!(re_fallback_suffix(Some("commentary"), url, false), None);
    // Different URL still gets a fallback.
    assert!(
        re_fallback_suffix(
            Some("commentary\nRE: https://other.example/notes/2"),
            url,
            true
        )
        .is_some()
    );
}

#[test]
fn render_helpers_shape() {
    let fields = render_quote_fields("https://t.example/1", None);
    assert_eq!(fields["quote"], "https://t.example/1");
    assert_eq!(fields["_misskey_quote"], "https://t.example/1");
    assert_eq!(fields["quoteUrl"], "https://t.example/1");
    assert_eq!(fields["quoteUri"], "https://t.example/1");
    assert!(fields.get("quoteAuthorization").is_none());

    let fields = render_quote_fields("https://t.example/1", Some("https://t.example/s/1"));
    assert_eq!(fields["quoteAuthorization"], "https://t.example/s/1");

    // Policy: no manualApproval advertised without a review workflow.
    let policy = render_quote_policy(true, "https://a.example/users/x");
    assert!(
        policy["interactionPolicy"]["canQuote"]["automaticApproval"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "https://www.w3.org/ns/activitystreams#Public")
    );
    assert!(
        policy["interactionPolicy"]["canQuote"]
            .get("manualApproval")
            .is_none()
    );
    let restricted = render_quote_policy(false, "https://a.example/users/x");
    assert_eq!(
        restricted["interactionPolicy"]["canQuote"]["automaticApproval"],
        json!(["https://a.example/users/x"])
    );

    let ctx = quote_context_entries();
    assert_eq!(ctx["quote"]["@id"], serde_json::json!(FEP044F_QUOTE));
    assert_eq!(ctx["_misskey_quote"], serde_json::json!(MISSKEY_QUOTE));
    assert_eq!(ctx["quoteUrl"], serde_json::json!(QUOTE_URL));
    // e232 Link tag uses the ld+json activitystreams profile (Sharkey parity).
    let tag = render_quote_link_tag("https://t.example/1");
    assert_eq!(
        tag["mediaType"],
        "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\""
    );
    assert_eq!(tag["rel"], serde_json::json!(MISSKEY_QUOTE));
}
