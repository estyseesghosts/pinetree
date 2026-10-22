//! Batch E overflow hardening (E7 fix verification): the RFC 9421 and
//! legacy timestamp skew checks run on attacker-controlled `i64` values, so
//! extreme inputs must reject with `Err`, never panic (debug overflow) or
//! wrap (release arithmetic).
//!
//! Standalone integration target (see `tests/federation_sig_mode_e5.rs` for
//! the mode/interop matrix): every test signs with an in-process key and
//! calls the public verifiers directly. No network I/O.

use std::sync::OnceLock;

use axum::http::{HeaderMap, HeaderName, HeaderValue};

use pinetree::activitypub::signatures::{
    VerifyOutcome, apply_sig_mode, generate_rsa_keypair_2048, parse_http_date,
    private_key_to_pkcs8_pem, public_key_to_spki_pem, sign_request_headers, sign_rfc9421_headers,
    verify_http_signature, verify_rfc9421_signature,
};
use pinetree::config::Rfc9421Mode;

/// One shared 2048-bit pair (ring refuses smaller keys; keygen runs once per
/// test binary).
fn keypair_pems() -> (String, String) {
    static KEYS: OnceLock<(String, String)> = OnceLock::new();
    KEYS.get_or_init(|| {
        let (private_key, public_key) =
            generate_rsa_keypair_2048().expect("generate Batch E-fix keypair");
        (
            private_key_to_pkcs8_pem(&private_key),
            public_key_to_spki_pem(&public_key),
        )
    })
    .clone()
}

fn header_map(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.insert(
            name.parse::<HeaderName>().expect("test header name parses"),
            value
                .parse::<HeaderValue>()
                .expect("test header value parses"),
        );
    }
    map
}

fn fixture_now_ms() -> i64 {
    parse_http_date("Wed, 14 Oct 2026 12:00:00 GMT").expect("fixture date parses")
}

#[test]
fn rfc9421_created_extremes_reject_without_panic() {
    let (priv_pem, pub_pem) = keypair_pems();
    let body = br#"{"type":"Follow"}"#;
    let key_id = "https://example.local/users/123#main-key";
    let now_ms = fixture_now_ms();
    // `now_secs - created` overflows (debug: panic) for these inputs; the
    // hardened check must return `Err` instead.
    for created in [i64::MIN, i64::MIN + 1, i64::MAX, i64::MAX - 1] {
        let signed = sign_rfc9421_headers(
            "POST",
            "/inbox",
            "https",
            "example.local",
            Some(body),
            key_id,
            &priv_pem,
            created,
        )
        .expect("signing an extreme created succeeds");
        let headers = header_map(&[
            ("host", "example.local"),
            ("content-digest", signed.content_digest.as_deref().unwrap()),
            ("signature-input", &signed.signature_input),
            ("signature", &signed.signature),
        ]);
        let result = verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers,
            Some(body),
            &pub_pem,
            Some(now_ms),
            300,
        );
        assert!(result.is_err(), "created={created} must reject, not panic");
    }
    // Extreme server clocks against a normal `created` must also fail
    // closed rather than overflow.
    let now_secs = now_ms.div_euclid(1000);
    let signed = sign_rfc9421_headers(
        "POST",
        "/inbox",
        "https",
        "example.local",
        Some(body),
        key_id,
        &priv_pem,
        now_secs,
    )
    .unwrap();
    let headers = header_map(&[
        ("host", "example.local"),
        ("content-digest", signed.content_digest.as_deref().unwrap()),
        ("signature-input", &signed.signature_input),
        ("signature", &signed.signature),
    ]);
    for now in [i64::MIN, i64::MAX] {
        let result = verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers,
            Some(body),
            &pub_pem,
            Some(now),
            300,
        );
        assert!(result.is_err(), "now_ms={now} must reject, not panic");
    }
}

#[test]
fn rfc9421_expires_overflow_rejects_without_panic() {
    let (priv_pem, pub_pem) = keypair_pems();
    let body = br#"{"type":"Follow"}"#;
    let key_id = "https://example.local/users/123#main-key";
    let now_ms = fixture_now_ms();
    let signed = sign_rfc9421_headers(
        "POST",
        "/inbox",
        "https",
        "example.local",
        Some(body),
        key_id,
        &priv_pem,
        now_ms.div_euclid(1000),
    )
    .unwrap();
    // `expires + skew` wraps (release) or panics (debug) at the top of the
    // range; the hardened check fails closed instead. `i64::MIN` is
    // genuinely expired and must also reject.
    for expires in [i64::MAX, i64::MAX - 1, i64::MIN] {
        let input = signed
            .signature_input
            .replace(";alg=", &format!(";expires={expires};alg="));
        let headers = header_map(&[
            ("host", "example.local"),
            ("content-digest", signed.content_digest.as_deref().unwrap()),
            ("signature-input", &input),
            ("signature", &signed.signature),
        ]);
        let result = verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers,
            Some(body),
            &pub_pem,
            Some(now_ms),
            300,
        );
        assert!(result.is_err(), "expires={expires} must reject, not panic");
    }
}

#[test]
fn legacy_extreme_clocks_reject_without_panic() {
    let (priv_pem, pub_pem) = keypair_pems();
    let date = "Wed, 14 Oct 2026 12:00:00 GMT";
    let now_ms = parse_http_date(date).unwrap();
    let key_id = "https://example.local/users/123#main-key";
    let signed = sign_request_headers(
        "GET",
        "/users/123",
        "example.local",
        date,
        None,
        key_id,
        &priv_pem,
    )
    .unwrap();
    let headers = header_map(&[
        ("date", date),
        ("host", "example.local"),
        ("signature", &signed.signature),
    ]);
    // `now - date_ms` overflows for `now_ms = i64::MIN` against any modern
    // date (debug: panic); both extremes must fail closed.
    for now in [i64::MIN, i64::MAX] {
        let result = verify_http_signature(
            "GET",
            "/users/123",
            &headers,
            None,
            &pub_pem,
            Some(now),
            300,
        );
        assert!(result.is_err(), "now_ms={now} must reject, not panic");
    }
    // An absurd configured skew must not wrap (`skew * 1000`) either.
    for skew in [i64::MAX, i64::MIN] {
        let result = verify_http_signature(
            "GET",
            "/users/123",
            &headers,
            None,
            &pub_pem,
            Some(now_ms),
            skew,
        );
        assert!(result.is_err(), "skew={skew} must reject, not panic");
    }
}

#[test]
fn legacy_far_future_date_rejects_without_panic() {
    let (priv_pem, pub_pem) = keypair_pems();
    let date = "Wed, 14 Oct 2026 12:00:00 GMT";
    let now_ms = parse_http_date(date).unwrap();
    let key_id = "https://example.local/users/123#main-key";
    let signed = sign_request_headers(
        "GET",
        "/users/123",
        "example.local",
        date,
        None,
        key_id,
        &priv_pem,
    )
    .unwrap();
    let base = header_map(&[
        ("date", date),
        ("host", "example.local"),
        ("signature", &signed.signature),
    ]);
    // A +292M-year `Date` is outside chrono's representable range (parse
    // error); year 9999 parses but is far outside any skew window. Both
    // must reject, never panic.
    for bad_date in [
        "Mon, 01 Jan 292000000 00:00:00 GMT",
        "Sat, 01 Jan 9999 00:00:00 GMT",
    ] {
        let mut headers = base.clone();
        headers.insert("date", bad_date.parse::<HeaderValue>().unwrap());
        let result = verify_http_signature(
            "GET",
            "/users/123",
            &headers,
            None,
            &pub_pem,
            Some(now_ms),
            300,
        );
        assert!(result.is_err(), "date={bad_date} must reject, not panic");
    }
}

#[test]
fn sig_mode_permissive_enforce_matrix_unchanged() {
    let alice = VerifyOutcome::Verified("https://x/users/alice".to_string());
    let bad = VerifyOutcome::Failed("bad signature".to_string());
    // Permissive accepts a valid legacy signature on its own ...
    assert_eq!(
        apply_sig_mode(Rfc9421Mode::Permissive, Some(alice.clone()), None),
        Ok("https://x/users/alice".to_string())
    );
    // ... and still verifies RFC 9421 fields when present (a bad legacy
    // signature does not drag down a good RFC 9421 one).
    assert_eq!(
        apply_sig_mode(
            Rfc9421Mode::Permissive,
            Some(bad.clone()),
            Some(alice.clone())
        ),
        Ok("https://x/users/alice".to_string())
    );
    // Enforce requires a valid RFC 9421 signature ...
    assert_eq!(
        apply_sig_mode(Rfc9421Mode::Enforce, None, Some(alice.clone())),
        Ok("https://x/users/alice".to_string())
    );
    // ... and rejects legacy-only requests ...
    assert!(apply_sig_mode(Rfc9421Mode::Enforce, Some(alice.clone()), None).is_err());
    // ... including a valid legacy signature paired with a bad RFC 9421 one.
    assert!(apply_sig_mode(Rfc9421Mode::Enforce, Some(alice.clone()), Some(bad.clone())).is_err());
}
