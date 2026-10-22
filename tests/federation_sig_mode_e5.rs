//! Batch E (`docs/prod-ready.md` §6): RFC 9421 signature mode + verifier
//! integration coverage.
//!
//! Hermetic by construction: every test signs with in-process keys (one
//! static 2048-bit fixture pair plus one runtime-generated pair), resolves
//! keys from a pre-seeded local cache, and dispatches activities whose type
//! the inbox accepts-and-ignores without fetching. No test performs network
//! I/O; a test that accidentally needed the network would fail (the harness
//! points at reserved `.example`/`.test` names with no route).
//!
//! Requirement map (§6 item 7 plus items 3–6):
//!
//! * legacy-only accept: `legacy_only_post_accepts`,
//!   `inbox_accepts_legacy_only_under_permissive`.
//! * RFC-9421-only accept: `rfc9421_only_post_accepts`,
//!   `inbox_accepts_rfc9421_only_under_permissive`,
//!   `inbox_accepts_rfc9421_only_under_enforce`.
//! * valid dual-signature accept: `dual_signatures_agree_on_owner`,
//!   `inbox_accepts_dual_signed_post`.
//! * invalid signature reject: `corrupted_and_wrong_key_signatures_rejected`.
//! * tampered body reject: `tampered_body_rejected`,
//!   `inbox_rejects_tampered_body`.
//! * malformed/missing covered components reject:
//!   `malformed_and_missing_components_rejected`.
//! * replay/stale timestamps reject: `stale_and_future_timestamps_rejected`,
//!   `inbox_rejects_stale_signatures`.
//! * key rotation: `rotated_key_replaces_old_key` (crypto layer). The
//!   inbox/fetch refresh-on-rotation path re-fetches the actor document over
//!   the network (`resolve_remote_public_key(..., force_refresh = true)`),
//!   so it cannot run hermetically; see the test's limitation note.
//! * actor mismatch reject: `signer_disagreement_fails_closed`,
//!   `inbox_rejects_actor_mismatch`.
//! * mode selection: `mode_selection_matrix`,
//!   `inbox_rejects_legacy_only_under_enforce`,
//!   `fetch_gate_mode_selection` (legacy-only rejected under enforce,
//!   RFC-9421-only accepted under enforce, in both gates).
//! * actor-owner/object-authority gates still run after verification (§6
//!   item 5): `inbox_rejects_actor_mismatch` (actor `==` signer) and
//!   `inbox_rejects_foreign_object_create` (Create object same-host rule).
//! * legacy strength unweakened (§6 item 6): legacy assertions appear in
//!   every relevant test; no existing `signatures.rs` test is modified.

use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use serde_json::json;

use pinetree::activitypub::signatures;
use pinetree::activitypub::signatures::VerifyOutcome;
use pinetree::application::commands::AppServices;
use pinetree::application::ports::ActivityDedup;
use pinetree::application::ports::Clock;
use pinetree::application::ports::NoopStreamingSink;
use pinetree::config::RemoteCacheMode;
use pinetree::config::Rfc9421Mode;
use pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT;
use pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT;
use pinetree::domain::id::SystemClock;
use pinetree::domain::pinning::DEFAULT_PIN_LIMIT;
use pinetree::federation::delivery;
use pinetree::federation::delivery::SafeHttpClient;
use pinetree::federation::delivery::SafeHttpConfig;
use pinetree::federation::fetch_gate;
use pinetree::federation::inbox;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Remote signer under test (key owner for every cached-key assertion).
const REMOTE_ACTOR: &str = "https://remote.example/users/alice";
/// Key id bound to [`REMOTE_ACTOR`] in the pre-seeded key cache.
const REMOTE_KEY_ID: &str = "https://remote.example/users/alice#main-key";
/// A second actor used only for mismatch/authority-negative tests.
const OTHER_ACTOR: &str = "https://remote.example/users/mallory";
/// Second actor's key id (signed with the runtime-generated key).
const OTHER_KEY_ID: &str = "https://remote.example/users/mallory#main-key";
/// Shared-inbox request target exercised by every inbox test.
const INBOX_PATH: &str = "/inbox";
/// `Host` header carried by every signed test request.
const REQUEST_HOST: &str = "local.test";
/// `public_url` of the harness services (drives `@scheme` reconstruction).
const REQUEST_PUBLIC_URL: &str = "https://local.test";

/// Static 2048-bit fixture pair (the Node-crypto interop pair from the
/// `signatures.rs` unit tests, copied so this integration binary pays no
/// key-generation cost for the common case).
const FIXTURE_PRIVATE_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0HwE4w8IWM74o\n\
G5kVUz5SVn3z7nKJpNEUlhMzM0e0/4nWOmGJN7pVEstNj3aZUz6nzOIjPuGSWHu1\n\
r9j7sesR86SwRkb7t29KikYl/1gFeYJ5g0Y6MASNguHH4jzwJCzDde+7+efNckM5\n\
B7TuFaa8XsTUDKKac5WLduNA2urIJDflT2EMlkVQB9zygd5iOp27s3+PLRVi35T6\n\
8JCh3xrP4W5+Kntu/+FKdUEs5RvrXN3jiSwN/WumIDdDrXuhiHn/XxTmPJxpNvF5\n\
ZT+I18wdU3Q31tnw6ksJGVVaXhE3m5iHFuOui8qzBZaUdfyQL/ueSnc1cejpVjkU\n\
RW2hxUd5AgMBAAECggEASXhMG0deJ4g7jEiVjKlDxnoZLiKY/aQI1IjSlBPar+17\n\
37I4jqv4iTL9UkmuaqQV7oQXyaCgif0lmNp9e0z7QXtMNcq0sYAFGTaFnoIk+Taz\n\
DbWKM7naYY59YfbaPGnFCIVWoCozbw2odd664lJajINrbcMpoC59VUHNOGsTUUcl\n\
Zu6XJb41vJR984iBWVl3H4IBQ0fq8omwSdDfnVesTUFAI5+UWbVsFSkR2GxeafeW\n\
wCcxb/HZGmHdZA0r+wcE5DHlvKWEuNyTLeaL+5Ls4i4tF1u5M4VObR6p8pOXCuDr\n\
bhlrMC29ppcytPA6gp3rfYjQB3KGSCez85QOZEoiMQKBgQDfFkkAYRDJXOFmiKMI\n\
5Es9iEKNDRtnJGZHsY7EUaY0KjwARnf0B107GOcpDP/rAvDcE1AgrH8WWp2U/F7c\n\
Tn0ivFoC71mcgqDW8CpxWm6gFfJxIDcQHab4pCePZTSokCGvQHkQ8SCF9CiYPF8e\n\
N36OYIfCcqzw10S61wdoneWpywKBgQDOsfNbnEM4MqP3slJ4xfZEMJiCmn9A0XLN\n\
FKlg64WwZYuPf0tVGLUsFk7v2bxKjPdIB/CN0k/bmBgbTsCVNaxN2JQgCWVN1Lkn\n\
RK74TDk1U54AoEEg5wFDboKxwila4O+QzEv9Kvc4wKfP6m3UKwcsjCHOJ8IeKNdr\n\
g6/sV7h7SwKBgQDMcmLyuwIkWD+ltSEc/APMX30GQXXUn7IwkBPT7TQRaIdWhT5X\n\
YQxbJaZro6NCII7WUcjEOPG7bc4vBN52rmPWGqGvQOVyHvECKlXrTkJF6fcnH6v2\n\
1AX6po4ux0RsGrMUJlUNZr40pR8y7VkKmzcbinHAjVHa4p31US1T/ijE4wKBgBsK\n\
kOoFUY4Cogz/2WZPj95v9SPxDoDuZfSKpXY2mfDv+Ag4ASUK7HjuGjmq5MVkF93m\n\
nAQR5by280BvVyauNPKiSQ6/6+74y5hllfaKEln1rJAsZxjk3/dwd8zFuB9Zmj35\n\
XF+sxVc5bTr4UpIVZwTe+HU+IllLwpMgUV28RDhtAoGBAKGCggYfQ1Q54vmljH1c\n\
me9QQFofDzsoq8Gj1xuhDFFNmHqW1z0DQkkf1gdKgx9E/uE4ceXTLE9z313cof8v\n\
mzqBLgLPPqB2yu9VRXgr43i6mJ3jiW6NK+tPIj7U7GNdGRd8LAmjP0W/8S6CFoYk\n\
/Xx9/OtP1rd1WHkDZbdd3gi9\n\
-----END PRIVATE KEY-----\n";
const FIXTURE_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtB8BOMPCFjO+KBuZFVM+\n\
UlZ98+5yiaTRFJYTMzNHtP+J1jphiTe6VRLLTY92mVM+p8ziIz7hklh7ta/Y+7Hr\n\
EfOksEZG+7dvSopGJf9YBXmCeYNGOjAEjYLhx+I88CQsw3Xvu/nnzXJDOQe07hWm\n\
vF7E1AyimnOVi3bjQNrqyCQ35U9hDJZFUAfc8oHeYjqdu7N/jy0VYt+U+vCQod8a\n\
z+Fufip7bv/hSnVBLOUb61zd44ksDf1rpiA3Q617oYh5/18U5jycaTbxeWU/iNfM\n\
HVN0N9bZ8OpLCRlVWl4RN5uYhxbjrovKswWWlHX8kC/7nkp3NXHo6VY5FEVtocVH\n\
eQIDAQAB\n\
-----END PUBLIC KEY-----\n";

/// Second 2048-bit signer, generated once per test-binary run (rotation and
/// mismatch tests need two distinct keys; everything else uses the static
/// fixture above).
fn second_keypair() -> (String, String) {
    static KEYS: OnceLock<(String, String)> = OnceLock::new();
    KEYS.get_or_init(|| {
        let (private_key, public_key) =
            signatures::generate_rsa_keypair_2048().expect("generate second Batch E keypair");
        (
            signatures::private_key_to_pkcs8_pem(&private_key),
            signatures::public_key_to_spki_pem(&public_key),
        )
    })
    .clone()
}

/// Fixed reference time for the pure-crypto tests (deterministic: no wall
/// clock involved).
fn fixed_now_ms() -> i64 {
    signatures::parse_http_date("Wed, 14 Oct 2026 12:00:00 GMT").expect("fixture date parses")
}

/// `Date` header matching [`fixed_now_ms`].
fn fixed_date_header() -> String {
    delivery::format_http_date(fixed_now_ms())
}

/// `created` seconds matching [`fixed_now_ms`].
fn fixed_created_secs() -> i64 {
    fixed_now_ms().div_euclid(1000)
}

fn header_map(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        let key: HeaderName = name.parse().expect("test header name parses");
        let header_value: HeaderValue = value.parse().expect("test header value parses");
        map.insert(key, header_value);
    }
    map
}

/// Legacy-signed POST headers covering `(request-target) date host digest`.
fn legacy_post_headers(
    path: &str,
    host: &str,
    date: &str,
    body: &[u8],
    key_id: &str,
    private_pem: &str,
) -> HeaderMap {
    let signed =
        signatures::sign_request_headers("POST", path, host, date, Some(body), key_id, private_pem)
            .expect("legacy test signing works");
    let digest = signed
        .digest
        .as_deref()
        .expect("POST signing emits a digest");
    header_map(&[
        ("date", date),
        ("host", host),
        ("digest", digest),
        ("signature", signed.signature.as_str()),
    ])
}

/// RFC 9421-signed POST headers covering `@method @authority @path`
/// plus `content-digest`.
fn rfc9421_post_headers(
    path: &str,
    scheme: &str,
    host: &str,
    body: &[u8],
    key_id: &str,
    private_pem: &str,
    created_secs: i64,
) -> HeaderMap {
    let signed = signatures::sign_rfc9421_headers(
        "POST",
        path,
        scheme,
        host,
        Some(body),
        key_id,
        private_pem,
        created_secs,
    )
    .expect("RFC 9421 test signing works");
    let digest = signed
        .content_digest
        .as_deref()
        .expect("POST signing emits content-digest");
    header_map(&[
        ("host", host),
        ("content-digest", digest),
        ("signature-input", signed.signature_input.as_str()),
        ("signature", signed.signature.as_str()),
    ])
}

/// Dual-signed POST headers: the RFC 9421 dictionary travels in `signature`
/// while the legacy value travels in `authorization` (both slots are
/// consulted by each parser, so either order verifies).
#[allow(clippy::too_many_arguments)]
fn dual_post_headers(
    path: &str,
    scheme: &str,
    host: &str,
    date: &str,
    body: &[u8],
    key_id: &str,
    private_pem: &str,
    created_secs: i64,
) -> HeaderMap {
    let legacy =
        signatures::sign_request_headers("POST", path, host, date, Some(body), key_id, private_pem)
            .expect("legacy test signing works");
    let rfc9421 = signatures::sign_rfc9421_headers(
        "POST",
        path,
        scheme,
        host,
        Some(body),
        key_id,
        private_pem,
        created_secs,
    )
    .expect("RFC 9421 test signing works");
    let legacy_digest = legacy
        .digest
        .as_deref()
        .expect("POST signing emits a digest");
    let rfc9421_digest = rfc9421
        .content_digest
        .as_deref()
        .expect("POST signing emits content-digest");
    let authorization = format!("Signature {}", legacy.signature);
    header_map(&[
        ("date", date),
        ("host", host),
        ("digest", legacy_digest),
        ("content-digest", rfc9421_digest),
        ("signature-input", rfc9421.signature_input.as_str()),
        ("signature", rfc9421.signature.as_str()),
        ("authorization", authorization.as_str()),
    ])
}

/// Fixed POST body shared by the pure-crypto tests.
fn post_body() -> Vec<u8> {
    br#"{"type":"Follow","id":"https://remote.example/acts/1"}"#.to_vec()
}

// ---------------------------------------------------------------------------
// Process-wide mode handling (serialized: the mode cell is global)
// ---------------------------------------------------------------------------

/// Serializes every test that touches the process-wide signature-mode cell.
/// A Tokio mutex (not `std`): the guard is held across `.await` while the
/// mode is in force, which is exactly what a Tokio mutex is built for (and
/// what `clippy::await_holding_lock` accepts).
static MODE_MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn mode_mutex() -> &'static tokio::sync::Mutex<()> {
    MODE_MUTEX.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Run an async test body with the process-wide mode set, restoring
/// `permissive` afterwards (even on panic, via the drop guard; the Tokio
/// mutex itself never poisons).
async fn with_sig_mode<R>(mode: Rfc9421Mode, test: impl Future<Output = R>) -> R {
    let _held = mode_mutex().lock().await;
    struct RestoreMode;
    impl Drop for RestoreMode {
        fn drop(&mut self) {
            signatures::set_rfc9421_mode(Rfc9421Mode::Permissive);
        }
    }
    let _restore = RestoreMode;
    signatures::set_rfc9421_mode(mode);
    test.await
}

// ---------------------------------------------------------------------------
// Pure signature-scheme tests (fixed clock, no services, no network)
// ---------------------------------------------------------------------------

#[test]
fn legacy_only_post_accepts() {
    let body = post_body();
    let date = fixed_date_header();
    let map = legacy_post_headers(
        INBOX_PATH,
        REQUEST_HOST,
        &date,
        &body,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
    );
    assert!(
        signatures::has_legacy_signature(&map),
        "legacy presence heuristic fires"
    );
    let parsed = signatures::verify_http_signature(
        "POST",
        INBOX_PATH,
        &map,
        Some(&body),
        FIXTURE_PUBLIC_PEM,
        Some(fixed_now_ms()),
        inbox::MAX_CLOCK_SKEW_SECS,
    )
    .expect("valid legacy POST verifies");
    assert_eq!(parsed.key_id, REMOTE_KEY_ID, "key id round-trips");
    assert_eq!(
        signatures::actor_uri_from_key_id(&parsed.key_id),
        REMOTE_ACTOR,
        "key id locates the actor document"
    );
}

#[test]
fn rfc9421_only_post_accepts() {
    let body = post_body();
    let created = fixed_created_secs();
    let map = rfc9421_post_headers(
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        &body,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        created,
    );
    assert!(
        !signatures::has_legacy_signature(&map),
        "RFC 9421 dictionary alone is not a legacy signature"
    );
    let parsed = signatures::verify_rfc9421_signature(
        "POST",
        INBOX_PATH,
        "https",
        &map,
        Some(&body),
        FIXTURE_PUBLIC_PEM,
        Some(fixed_now_ms()),
        inbox::MAX_CLOCK_SKEW_SECS,
    )
    .expect("valid RFC 9421 POST verifies");
    assert_eq!(parsed.key_id, REMOTE_KEY_ID, "key id round-trips");
    assert_eq!(parsed.label, "sig1", "first dictionary member verifies");
    assert_eq!(parsed.created, Some(created), "created round-trips");
    assert_eq!(parsed.algorithm, "rsa-v1_5-sha256", "profile algorithm");
    assert_eq!(
        signatures::scheme_of_public_url(REQUEST_PUBLIC_URL),
        "https",
        "harness public_url implies https @scheme"
    );
}

#[test]
fn dual_signatures_agree_on_owner() {
    let body = post_body();
    let date = fixed_date_header();
    let created = fixed_created_secs();
    let map = dual_post_headers(
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        &date,
        &body,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        created,
    );
    // Both schemes verify independently against the same key.
    let legacy = signatures::verify_http_signature(
        "POST",
        INBOX_PATH,
        &map,
        Some(&body),
        FIXTURE_PUBLIC_PEM,
        Some(fixed_now_ms()),
        inbox::MAX_CLOCK_SKEW_SECS,
    )
    .expect("legacy half of a dual-signed POST verifies");
    let rfc9421 = signatures::verify_rfc9421_signature(
        "POST",
        INBOX_PATH,
        "https",
        &map,
        Some(&body),
        FIXTURE_PUBLIC_PEM,
        Some(fixed_now_ms()),
        inbox::MAX_CLOCK_SKEW_SECS,
    )
    .expect("RFC 9421 half of a dual-signed POST verifies");
    assert_eq!(legacy.key_id, REMOTE_KEY_ID, "legacy key id");
    assert_eq!(rfc9421.key_id, REMOTE_KEY_ID, "RFC 9421 key id");
    // Agreeing owners authenticate under both modes.
    for mode in [Rfc9421Mode::Permissive, Rfc9421Mode::Enforce] {
        let outcome = signatures::apply_sig_mode(
            mode,
            Some(VerifyOutcome::Verified(REMOTE_ACTOR.to_string())),
            Some(VerifyOutcome::Verified(REMOTE_ACTOR.to_string())),
        );
        assert_eq!(
            outcome,
            Ok(REMOTE_ACTOR.to_string()),
            "dual-signed agreeing owners authenticate under {mode:?}"
        );
    }
}

#[test]
fn corrupted_and_wrong_key_signatures_rejected() {
    let body = post_body();
    let date = fixed_date_header();
    let created = fixed_created_secs();
    let (_second_private, second_public) = second_keypair();

    // Legacy: the wrong key fails, and corrupted signature bytes fail.
    let map = legacy_post_headers(
        INBOX_PATH,
        REQUEST_HOST,
        &date,
        &body,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
    );
    assert!(
        signatures::verify_http_signature(
            "POST",
            INBOX_PATH,
            &map,
            Some(&body),
            second_public.as_str(),
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "legacy signature does not verify under the wrong key"
    );
    let raw = map
        .get("signature")
        .expect("signature header present")
        .to_str()
        .expect("signature header is visible ASCII")
        .to_string();
    let marker = "signature=\"";
    let start = raw.find(marker).expect("legacy value carries a signature") + marker.len();
    let end = raw[start..].find('"').expect("closing quote") + start;
    let mut corrupted = raw.clone();
    corrupted.replace_range(start..end, "AAAA");
    let corrupted_map = header_map(&[
        ("date", date.as_str()),
        ("host", REQUEST_HOST),
        (
            "digest",
            map.get("digest")
                .expect("digest present")
                .to_str()
                .expect("digest is visible ASCII"),
        ),
        ("signature", corrupted.as_str()),
    ]);
    assert!(
        signatures::verify_http_signature(
            "POST",
            INBOX_PATH,
            &corrupted_map,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "corrupted legacy signature bytes are rejected"
    );

    // RFC 9421: the wrong key fails, and a corrupted entry fails.
    let rfc_map = rfc9421_post_headers(
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        &body,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        created,
    );
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &rfc_map,
            Some(&body),
            second_public.as_str(),
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "RFC 9421 signature does not verify under the wrong key"
    );
    let mut corrupted_rfc = rfc_map.clone();
    corrupted_rfc.insert("signature", "sig1=:QUJD:".parse().expect("header value"));
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &corrupted_rfc,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "substituted RFC 9421 signature bytes are rejected"
    );
}

#[test]
fn tampered_body_rejected() {
    let body = post_body();
    let tampered = br#"{"type":"Follow","id":"https://remote.example/acts/1","admin":true}"#;
    let date = fixed_date_header();
    let created = fixed_created_secs();

    let legacy_map = legacy_post_headers(
        INBOX_PATH,
        REQUEST_HOST,
        &date,
        &body,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
    );
    assert!(
        signatures::verify_http_signature(
            "POST",
            INBOX_PATH,
            &legacy_map,
            Some(tampered),
            FIXTURE_PUBLIC_PEM,
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "legacy Digest mismatch rejects the tampered body"
    );

    let rfc_map = rfc9421_post_headers(
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        &body,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        created,
    );
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &rfc_map,
            Some(tampered),
            FIXTURE_PUBLIC_PEM,
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "Content-Digest mismatch rejects the tampered body"
    );
}

#[test]
fn malformed_and_missing_components_rejected() {
    // Unparseable `Signature-Input` dictionaries are rejected, never skipped.
    for bad in [
        "",
        "sig1=",
        "sig1=()",
        "sig1=(\"@method\" \"@authority\");created=1",
        "sig1=(\"@method\" \"@authority\" \"@method\");created=1;keyid=\"k\"",
        "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\";alg=\"hmac-sha256\"",
        "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\";alg=\"ed25519\"",
        "SIG1=(\"@method\" \"@authority\" \"@path\");created=1;keyid=\"k\"",
    ] {
        assert!(
            signatures::parse_signature_input(bad).is_err(),
            "malformed input is rejected: {bad}"
        );
    }
    // A `Signature` dictionary entry for another label does not satisfy ours.
    assert!(
        signatures::parse_http_signature_value("other=:QUJD:", "sig1").is_err(),
        "missing Signature entry is an error"
    );
    // No signature headers at all: the legacy locator finds nothing.
    assert!(
        signatures::parse_first_legacy_signature(&HeaderMap::new()).is_none(),
        "absent legacy headers locate nothing"
    );

    let body = post_body();
    let date = fixed_date_header();
    let created = fixed_created_secs();
    let signed = signatures::sign_rfc9421_headers(
        "POST",
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        Some(&body),
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        created,
    )
    .expect("RFC 9421 test signing works");

    // Missing `Signature-Input` header.
    let no_input = header_map(&[("host", REQUEST_HOST)]);
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &no_input,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "missing Signature-Input is rejected"
    );

    // Stripped `created`: parsing succeeds, verification names the gap.
    let stripped = signed
        .signature_input
        .replacen(&format!(";created={created}"), "", 1);
    assert_ne!(stripped, signed.signature_input, "strip helper works");
    let digest = signed.content_digest.as_deref().expect("digest present");
    let no_created = header_map(&[
        ("host", REQUEST_HOST),
        ("content-digest", digest),
        ("signature-input", stripped.as_str()),
        ("signature", signed.signature.as_str()),
    ]);
    let error = signatures::verify_rfc9421_signature(
        "POST",
        INBOX_PATH,
        "https",
        &no_created,
        Some(&body),
        FIXTURE_PUBLIC_PEM,
        Some(fixed_now_ms()),
        inbox::MAX_CLOCK_SKEW_SECS,
    )
    .expect_err("missing created is rejected");
    assert!(error.contains("created"), "gap is named: {error}");

    // Dropped `@authority`: the required covered set names the gap.
    let shrunk = signed.signature_input.replacen(
        "(\"@method\" \"@authority\" \"@path\" \"content-digest\")",
        "(\"@method\" \"@path\" \"content-digest\")",
        1,
    );
    assert_ne!(shrunk, signed.signature_input, "shrink helper works");
    let no_authority = header_map(&[
        ("host", REQUEST_HOST),
        ("content-digest", digest),
        ("signature-input", shrunk.as_str()),
        ("signature", signed.signature.as_str()),
    ]);
    let error = signatures::verify_rfc9421_signature(
        "POST",
        INBOX_PATH,
        "https",
        &no_authority,
        Some(&body),
        FIXTURE_PUBLIC_PEM,
        Some(fixed_now_ms()),
        inbox::MAX_CLOCK_SKEW_SECS,
    )
    .expect_err("missing @authority is rejected");
    assert!(error.contains("@authority"), "gap is named: {error}");

    // Body submitted against a bodyless signature: no digest is covered.
    let bodyless = signatures::sign_rfc9421_headers(
        "POST",
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        None,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        created,
    )
    .expect("bodyless signing works");
    let bodyless_map = header_map(&[
        ("host", REQUEST_HOST),
        ("signature-input", bodyless.signature_input.as_str()),
        ("signature", bodyless.signature.as_str()),
    ]);
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &bodyless_map,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "a body with no covered digest is rejected"
    );

    // Response-only covered components cannot build a request base.
    let status_covered = signatures::parse_signature_input(
        "sig1=(\"@method\" \"@authority\" \"@path\" \"@status\");created=1;keyid=\"k\"",
    )
    .expect("shape parses; base construction refuses it");
    let dummy = header_map(&[("host", REQUEST_HOST)]);
    assert!(
        signatures::build_rfc9421_signature_base(
            "GET",
            "/items/1",
            "https",
            REQUEST_HOST,
            &dummy,
            &status_covered,
        )
        .is_err(),
        "response-only @status is rejected at base construction"
    );

    // Legacy: a body submitted against a bodyless signature lacks `digest`.
    let legacy_bodyless = signatures::sign_request_headers(
        "POST",
        INBOX_PATH,
        REQUEST_HOST,
        &date,
        None,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
    )
    .expect("legacy bodyless signing works");
    let legacy_map = header_map(&[
        ("date", date.as_str()),
        ("host", REQUEST_HOST),
        ("signature", legacy_bodyless.signature.as_str()),
    ]);
    assert!(
        signatures::verify_http_signature(
            "POST",
            INBOX_PATH,
            &legacy_map,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "legacy body with no covered digest is rejected"
    );
}

#[test]
fn stale_and_future_timestamps_rejected() {
    let body = post_body();
    let now_ms = fixed_now_ms();
    let now_secs = now_ms.div_euclid(1000);

    // Legacy replay: the same bytes verified an hour later are stale.
    let old_date = delivery::format_http_date(now_ms - 3_600_000);
    let legacy_map = legacy_post_headers(
        INBOX_PATH,
        REQUEST_HOST,
        &old_date,
        &body,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
    );
    assert!(
        signatures::verify_http_signature(
            "POST",
            INBOX_PATH,
            &legacy_map,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(now_ms),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "hour-old legacy Date is outside the skew window"
    );

    // RFC 9421 replay: an hour-old `created` is stale.
    let stale = signatures::sign_rfc9421_headers(
        "POST",
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        Some(&body),
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        now_secs - 3600,
    )
    .expect("stale signing works");
    let stale_digest = stale.content_digest.as_deref().expect("digest present");
    let stale_map = header_map(&[
        ("host", REQUEST_HOST),
        ("content-digest", stale_digest),
        ("signature-input", stale.signature_input.as_str()),
        ("signature", stale.signature.as_str()),
    ]);
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &stale_map,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(now_ms),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "hour-old created is outside the skew window"
    );

    // Far-future `created` is equally outside the window (the signature
    // itself is valid, so only the freshness check can reject it).
    let future = signatures::sign_rfc9421_headers(
        "POST",
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        Some(&body),
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        now_secs + 3600,
    )
    .expect("future signing works");
    let future_digest = future.content_digest.as_deref().expect("digest present");
    let future_map = header_map(&[
        ("host", REQUEST_HOST),
        ("content-digest", future_digest),
        ("signature-input", future.signature_input.as_str()),
        ("signature", future.signature.as_str()),
    ]);
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &future_map,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(now_ms),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "hour-future created is outside the skew window"
    );

    // A past `expires` rejects even with a fresh `created`.
    let fresh = signatures::sign_rfc9421_headers(
        "POST",
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        Some(&body),
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        now_secs,
    )
    .expect("fresh signing works");
    let expired_input =
        fresh
            .signature_input
            .replacen(";alg=", &format!(";expires={};alg=", now_secs - 3600), 1);
    let fresh_digest = fresh.content_digest.as_deref().expect("digest present");
    let expired_map = header_map(&[
        ("host", REQUEST_HOST),
        ("content-digest", fresh_digest),
        ("signature-input", expired_input.as_str()),
        ("signature", fresh.signature.as_str()),
    ]);
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &expired_map,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(now_ms),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "past expires is rejected"
    );
}

#[test]
fn rotated_key_replaces_old_key() {
    // Rotation keeps the key id but changes the key material: signatures
    // under the old key stop verifying against the new key and vice versa.
    let (second_private, second_public) = second_keypair();
    let body = post_body();
    let created = fixed_created_secs();

    let signed_old = signatures::sign_rfc9421_headers(
        "POST",
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        Some(&body),
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        created,
    )
    .expect("old-key signing works");
    let old_digest = signed_old
        .content_digest
        .as_deref()
        .expect("digest present");
    let old_map = header_map(&[
        ("host", REQUEST_HOST),
        ("content-digest", old_digest),
        ("signature-input", signed_old.signature_input.as_str()),
        ("signature", signed_old.signature.as_str()),
    ]);
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &old_map,
            Some(&body),
            FIXTURE_PUBLIC_PEM,
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_ok(),
        "old key verifies before rotation"
    );
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &old_map,
            Some(&body),
            second_public.as_str(),
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_err(),
        "old signatures fail against the rotated key"
    );

    let signed_new = signatures::sign_rfc9421_headers(
        "POST",
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        Some(&body),
        REMOTE_KEY_ID,
        second_private.as_str(),
        created,
    )
    .expect("new-key signing works");
    let new_digest = signed_new
        .content_digest
        .as_deref()
        .expect("digest present");
    let new_map = header_map(&[
        ("host", REQUEST_HOST),
        ("content-digest", new_digest),
        ("signature-input", signed_new.signature_input.as_str()),
        ("signature", signed_new.signature.as_str()),
    ]);
    assert!(
        signatures::verify_rfc9421_signature(
            "POST",
            INBOX_PATH,
            "https",
            &new_map,
            Some(&body),
            second_public.as_str(),
            Some(fixed_now_ms()),
            inbox::MAX_CLOCK_SKEW_SECS,
        )
        .is_ok(),
        "new key verifies after rotation"
    );

    // Limitation (disclosed, §6 item 7): the inbox/fetch refresh path picks
    // up the rotated key by re-fetching the actor document over the network
    // (`resolve_remote_public_key` with `force_refresh`), which cannot run
    // hermetically. What this test pins is the cryptographic half: after
    // rotation the old key no longer authenticates, so a stale cache entry
    // cannot be abused once verification runs against fresh material.
}

#[test]
fn signer_disagreement_fails_closed() {
    // Two distinct keys, two distinct owners, both cryptographically valid:
    // treating either owner as authenticated would be a confused-deputy
    // hole, so the mode gate fails closed under both modes.
    let (second_private, second_public) = second_keypair();
    let body = post_body();
    let created = fixed_created_secs();

    let alice_map = rfc9421_post_headers(
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        &body,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        created,
    );
    let bob_map = rfc9421_post_headers(
        INBOX_PATH,
        "https",
        REQUEST_HOST,
        &body,
        OTHER_KEY_ID,
        second_private.as_str(),
        created,
    );
    let alice = signatures::verify_rfc9421_signature(
        "POST",
        INBOX_PATH,
        "https",
        &alice_map,
        Some(&body),
        FIXTURE_PUBLIC_PEM,
        Some(fixed_now_ms()),
        inbox::MAX_CLOCK_SKEW_SECS,
    )
    .expect("alice verifies under her key");
    let bob = signatures::verify_rfc9421_signature(
        "POST",
        INBOX_PATH,
        "https",
        &bob_map,
        Some(&body),
        second_public.as_str(),
        Some(fixed_now_ms()),
        inbox::MAX_CLOCK_SKEW_SECS,
    )
    .expect("mallory verifies under his key");
    assert_eq!(alice.key_id, REMOTE_KEY_ID, "alice key id");
    assert_eq!(bob.key_id, OTHER_KEY_ID, "mallory key id");
    for mode in [Rfc9421Mode::Permissive, Rfc9421Mode::Enforce] {
        let outcome = signatures::apply_sig_mode(
            mode,
            Some(VerifyOutcome::Verified(REMOTE_ACTOR.to_string())),
            Some(VerifyOutcome::Verified(OTHER_ACTOR.to_string())),
        );
        assert!(
            outcome.is_err(),
            "owner disagreement fails closed under {mode:?}"
        );
    }
}

#[test]
fn mode_selection_matrix() {
    let alice = VerifyOutcome::Verified(REMOTE_ACTOR.to_string());
    let mallory = VerifyOutcome::Verified(OTHER_ACTOR.to_string());
    let legacy_bad = VerifyOutcome::Failed("legacy bad".to_string());
    let rfc9421_bad = VerifyOutcome::Failed("rfc9421 bad".to_string());

    // Permissive: either valid signature authenticates (transitional
    // dual-sign interop); a broken second scheme never vetoes a good one.
    assert_eq!(
        signatures::apply_sig_mode(Rfc9421Mode::Permissive, Some(alice.clone()), None),
        Ok(REMOTE_ACTOR.to_string()),
        "permissive accepts legacy-only"
    );
    assert_eq!(
        signatures::apply_sig_mode(Rfc9421Mode::Permissive, None, Some(alice.clone())),
        Ok(REMOTE_ACTOR.to_string()),
        "permissive accepts RFC 9421-only"
    );
    assert_eq!(
        signatures::apply_sig_mode(
            Rfc9421Mode::Permissive,
            Some(alice.clone()),
            Some(alice.clone())
        ),
        Ok(REMOTE_ACTOR.to_string()),
        "permissive accepts agreeing dual signatures"
    );
    assert_eq!(
        signatures::apply_sig_mode(
            Rfc9421Mode::Permissive,
            Some(legacy_bad.clone()),
            Some(alice.clone())
        ),
        Ok(REMOTE_ACTOR.to_string()),
        "permissive keeps a valid RFC 9421 signature past broken legacy"
    );
    assert_eq!(
        signatures::apply_sig_mode(
            Rfc9421Mode::Permissive,
            Some(alice.clone()),
            Some(rfc9421_bad.clone())
        ),
        Ok(REMOTE_ACTOR.to_string()),
        "permissive keeps a valid legacy signature past broken RFC 9421"
    );
    assert!(
        signatures::apply_sig_mode(
            Rfc9421Mode::Permissive,
            Some(alice.clone()),
            Some(mallory.clone())
        )
        .is_err(),
        "permissive fails closed on owner disagreement"
    );
    assert!(
        signatures::apply_sig_mode(
            Rfc9421Mode::Permissive,
            Some(legacy_bad.clone()),
            Some(rfc9421_bad.clone())
        )
        .is_err(),
        "permissive rejects two broken signatures"
    );
    assert!(
        signatures::apply_sig_mode(Rfc9421Mode::Permissive, None, None).is_err(),
        "permissive rejects unsigned requests"
    );

    // Enforce: a valid RFC 9421 signature is required; legacy-only is
    // rejected even when cryptographically valid.
    assert_eq!(
        signatures::apply_sig_mode(Rfc9421Mode::Enforce, None, Some(alice.clone())),
        Ok(REMOTE_ACTOR.to_string()),
        "enforce accepts RFC 9421-only"
    );
    assert_eq!(
        signatures::apply_sig_mode(
            Rfc9421Mode::Enforce,
            Some(alice.clone()),
            Some(alice.clone())
        ),
        Ok(REMOTE_ACTOR.to_string()),
        "enforce accepts agreeing dual signatures"
    );
    let error = signatures::apply_sig_mode(Rfc9421Mode::Enforce, Some(alice.clone()), None)
        .expect_err("enforce rejects legacy-only");
    assert!(
        error.contains("enforce"),
        "rejection names the mode: {error}"
    );
    assert!(
        signatures::apply_sig_mode(Rfc9421Mode::Enforce, None, None).is_err(),
        "enforce rejects unsigned requests"
    );
    assert!(
        signatures::apply_sig_mode(
            Rfc9421Mode::Enforce,
            Some(alice.clone()),
            Some(rfc9421_bad.clone())
        )
        .is_err(),
        "enforce rejects a broken RFC 9421 signature despite valid legacy"
    );
    assert!(
        signatures::apply_sig_mode(
            Rfc9421Mode::Enforce,
            Some(alice.clone()),
            Some(mallory.clone())
        )
        .is_err(),
        "enforce fails closed on owner disagreement"
    );
}

#[tokio::test]
async fn mode_cell_default_is_permissive() {
    // Serialized with the mode-switching tests: every setter restores
    // permissive, so a held cell always reads as the config default.
    let _held = mode_mutex().lock().await;
    assert_eq!(
        signatures::rfc9421_mode(),
        Rfc9421Mode::Permissive,
        "process-wide default matches Config::default()"
    );
    signatures::set_rfc9421_mode(Rfc9421Mode::Enforce);
    assert_eq!(
        signatures::rfc9421_mode(),
        Rfc9421Mode::Enforce,
        "mode cell round-trips"
    );
    signatures::set_rfc9421_mode(Rfc9421Mode::Permissive);
}

// ---------------------------------------------------------------------------
// Harness: local services with a pre-seeded remote key (no network)
// ---------------------------------------------------------------------------

/// Build `AppServices` on a fresh temp-dir store (deleted first, so reruns
/// are repeatable) with the harness `public_url`. No listener, no worker:
/// the tests call `inbox_handler` and the fetch gate directly.
fn boot_services(directory_tag: &str) -> Arc<AppServices> {
    let directory = std::env::temp_dir().join(format!("pinetree-sigmode-e5-{directory_tag}"));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("create Batch E temp dir");
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let store = RedbStore::open_with_default_ids(directory.join("store.redb"), clock.clone())
        .expect("open Batch E store");
    let blobs = Arc::new(FsBlobStore::new(directory.join("blobs")).expect("open blob store"));
    Arc::new(AppServices {
        clock: clock.clone(),
        ids: store.ids().clone(),
        accounts: store.clone(),
        notes: store.clone(),
        follows: store.clone(),
        blocks: store.clone(),
        mutes: store.clone(),
        reactions: store.clone(),
        favorites: store.clone(),
        emojis: store.clone(),
        antennas: store.clone(),
        antenna_notes: store.clone(),
        polls: store.clone(),
        jobs: store.clone(),
        media: store.clone(),
        blobs: blobs.clone(),
        notifications: store.clone(),
        auth: store.clone(),
        host_caps: store.clone(),
        moderation: store.clone(),
        instance_settings: store.clone(),
        invites: store.clone(),
        abuse_reports: store.clone(),
        lists: store.clone(),
        signin: store.clone(),
        password_resets: store.clone(),
        registry: store.clone(),
        clips: store.clone(),
        pages: store.clone(),
        folders: store.clone(),
        system_webhooks: store.clone(),
        user_webhooks: store.clone(),
        abuse_recipients: store.clone(),
        memos: store.clone(),
        thread_mutes: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            // Hermetic: nothing under test should fetch. Short timeouts fail
            // fast (instead of hanging) if that invariant ever breaks.
            allow_private_ips: false,
            timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(5),
            ..Default::default()
        }),
        public_url: REQUEST_PUBLIC_URL.to_string(),
        max_file_size: 10_000_000,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs.clone(),
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    })
}

/// Seed the remote-key cache so verification resolves locally (cache hit =
/// no fetch). The key id stays bound to [`REMOTE_ACTOR`]: the inbox compares
/// `activity.actor` against this cached owner, never against key-id text.
fn seed_fixture_key(services: &AppServices) {
    services
        .cache_remote_public_key(REMOTE_KEY_ID, REMOTE_ACTOR, None, FIXTURE_PUBLIC_PEM)
        .expect("cache fixture remote key");
}

/// Activity the inbox accepts and ignores without fetching (unknown types
/// authenticate, then dispatch returns 202 with no storage or network work
/// beyond signature verification).
fn ignored_activity(id: &str, actor: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"type": "Ignore", "id": id, "actor": actor}))
        .expect("activity serializes")
}

/// `Create` whose object lives on a foreign host (trips the
/// object-authority gate after successful verification).
fn foreign_object_create(id: &str, actor: &str) -> Vec<u8> {
    serde_json::to_vec(
        &json!({"type": "Create", "id": id, "actor": actor, "object": "https://other.example/notes/1"}),
    )
    .expect("activity serializes")
}

async fn post_shared_inbox(
    services: &Arc<AppServices>,
    headers: HeaderMap,
    body: Vec<u8>,
) -> StatusCode {
    inbox::inbox_handler(State(services.clone()), headers, Bytes::from(body))
        .await
        .status()
}

// ---------------------------------------------------------------------------
// Inbox handler tests (live clock, pre-seeded key, no network)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inbox_accepts_legacy_only_under_permissive() {
    with_sig_mode(Rfc9421Mode::Permissive, async {
        let services = boot_services("inbox-legacy-accept");
        seed_fixture_key(&services);
        let now_ms = services.clock.now_ms();
        let body = ignored_activity("https://remote.example/acts/legacy-1", REMOTE_ACTOR);
        let date = delivery::format_http_date(now_ms);
        let headers = legacy_post_headers(
            INBOX_PATH,
            REQUEST_HOST,
            &date,
            &body,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
        );
        let status = post_shared_inbox(&services, headers, body).await;
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "valid legacy-only POST is accepted"
        );
    })
    .await;
}

#[tokio::test]
async fn inbox_accepts_rfc9421_only_under_permissive() {
    with_sig_mode(Rfc9421Mode::Permissive, async {
        let services = boot_services("inbox-rfc9421-accept");
        seed_fixture_key(&services);
        let now_ms = services.clock.now_ms();
        let body = ignored_activity("https://remote.example/acts/rfc9421-1", REMOTE_ACTOR);
        let headers = rfc9421_post_headers(
            INBOX_PATH,
            "https",
            REQUEST_HOST,
            &body,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
            now_ms.div_euclid(1000),
        );
        let status = post_shared_inbox(&services, headers, body).await;
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "valid RFC 9421-only POST is accepted"
        );
    })
    .await;
}

#[tokio::test]
async fn inbox_accepts_dual_signed_post() {
    with_sig_mode(Rfc9421Mode::Permissive, async {
        let services = boot_services("inbox-dual-accept");
        seed_fixture_key(&services);
        let now_ms = services.clock.now_ms();
        let body = ignored_activity("https://remote.example/acts/dual-1", REMOTE_ACTOR);
        let date = delivery::format_http_date(now_ms);
        let headers = dual_post_headers(
            INBOX_PATH,
            "https",
            REQUEST_HOST,
            &date,
            &body,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
            now_ms.div_euclid(1000),
        );
        let status = post_shared_inbox(&services, headers, body).await;
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "valid dual-signed POST is accepted"
        );
    })
    .await;
}

#[tokio::test]
async fn inbox_rejects_legacy_only_under_enforce() {
    with_sig_mode(Rfc9421Mode::Enforce, async {
        let services = boot_services("inbox-legacy-enforce");
        seed_fixture_key(&services);
        let now_ms = services.clock.now_ms();
        let body = ignored_activity("https://remote.example/acts/legacy-enforced", REMOTE_ACTOR);
        let date = delivery::format_http_date(now_ms);
        let headers = legacy_post_headers(
            INBOX_PATH,
            REQUEST_HOST,
            &date,
            &body,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
        );
        let status = post_shared_inbox(&services, headers, body).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "legacy-only POST is rejected under enforce"
        );
    })
    .await;
}

#[tokio::test]
async fn inbox_accepts_rfc9421_only_under_enforce() {
    with_sig_mode(Rfc9421Mode::Enforce, async {
        let services = boot_services("inbox-rfc9421-enforce");
        seed_fixture_key(&services);
        let now_ms = services.clock.now_ms();
        let body = ignored_activity("https://remote.example/acts/rfc9421-enforced", REMOTE_ACTOR);
        let headers = rfc9421_post_headers(
            INBOX_PATH,
            "https",
            REQUEST_HOST,
            &body,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
            now_ms.div_euclid(1000),
        );
        let status = post_shared_inbox(&services, headers, body).await;
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "RFC 9421-only POST is accepted under enforce"
        );
    })
    .await;
}

#[tokio::test]
async fn inbox_rejects_tampered_body() {
    for mode in [Rfc9421Mode::Permissive, Rfc9421Mode::Enforce] {
        with_sig_mode(mode, async {
            let tag = match mode {
                Rfc9421Mode::Permissive => "tamper-permissive",
                Rfc9421Mode::Enforce => "tamper-enforce",
            };
            let services = boot_services(&format!("inbox-{tag}"));
            seed_fixture_key(&services);
            let now_ms = services.clock.now_ms();
            let body = ignored_activity("https://remote.example/acts/tamper-1", REMOTE_ACTOR);
            let date = delivery::format_http_date(now_ms);
            // Sign the honest body, then deliver one with an extra field.
            let headers = dual_post_headers(
                INBOX_PATH,
                "https",
                REQUEST_HOST,
                &date,
                &body,
                REMOTE_KEY_ID,
                FIXTURE_PRIVATE_PEM,
                now_ms.div_euclid(1000),
            );
            let mut tampered =
                serde_json::from_slice::<serde_json::Value>(&body).expect("body parses");
            tampered["tampered"] = json!(true);
            let tampered_body = serde_json::to_vec(&tampered).expect("tampered body serializes");
            let status = post_shared_inbox(&services, headers, tampered_body).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "tampered body is rejected under {mode:?}"
            );
        })
        .await;
    }
}

#[tokio::test]
async fn inbox_rejects_actor_mismatch() {
    // The actor-owner gate (`activity.actor == key-owner signer`) runs after
    // verification: the control POST (matching actor) is accepted, proving
    // the signature verifies, while the same key signing for another actor
    // is rejected.
    with_sig_mode(Rfc9421Mode::Permissive, async {
        let services = boot_services("inbox-actor-mismatch");
        seed_fixture_key(&services);
        let now_ms = services.clock.now_ms();
        let date = delivery::format_http_date(now_ms);

        let body = ignored_activity("https://remote.example/acts/match-1", REMOTE_ACTOR);
        let headers = legacy_post_headers(
            INBOX_PATH,
            REQUEST_HOST,
            &date,
            &body,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
        );
        let status = post_shared_inbox(&services, headers, body).await;
        assert_eq!(status, StatusCode::ACCEPTED, "matching actor is accepted");

        let forged = ignored_activity("https://remote.example/acts/forged-1", OTHER_ACTOR);
        let forged_headers = legacy_post_headers(
            INBOX_PATH,
            REQUEST_HOST,
            &date,
            &forged,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
        );
        let status = post_shared_inbox(&services, forged_headers, forged).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "activity.actor != key owner is rejected after verification"
        );
    })
    .await;
}

#[tokio::test]
async fn inbox_rejects_foreign_object_create() {
    // The object-authority gate (`Create` objects live on the signer's host)
    // runs after verification: the signature over this exact body verifies
    // at the crypto layer, so the 401 comes from the authority gate.
    with_sig_mode(Rfc9421Mode::Permissive, async {
        let services = boot_services("inbox-object-authority");
        seed_fixture_key(&services);
        let now_ms = services.clock.now_ms();
        let body = foreign_object_create("https://remote.example/acts/create-1", REMOTE_ACTOR);
        let created = now_ms.div_euclid(1000);
        let headers = rfc9421_post_headers(
            INBOX_PATH,
            "https",
            REQUEST_HOST,
            &body,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
            created,
        );
        assert!(
            signatures::verify_rfc9421_signature(
                "POST",
                INBOX_PATH,
                "https",
                &headers,
                Some(&body),
                FIXTURE_PUBLIC_PEM,
                Some(now_ms),
                inbox::MAX_CLOCK_SKEW_SECS,
            )
            .is_ok(),
            "the Create signature itself verifies (401 must come from the gate)"
        );
        let status = post_shared_inbox(&services, headers, body).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "Create for a foreign-host object is rejected after verification"
        );
    })
    .await;
}

#[tokio::test]
async fn inbox_rejects_stale_signatures() {
    // Replays: bytes signed an hour ago fail the freshness check on arrival.
    with_sig_mode(Rfc9421Mode::Permissive, async {
        let services = boot_services("inbox-stale");
        seed_fixture_key(&services);
        let now_ms = services.clock.now_ms();

        let body = ignored_activity("https://remote.example/acts/stale-legacy", REMOTE_ACTOR);
        let old_date = delivery::format_http_date(now_ms - 3_600_000);
        let headers = legacy_post_headers(
            INBOX_PATH,
            REQUEST_HOST,
            &old_date,
            &body,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
        );
        let status = post_shared_inbox(&services, headers, body).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "stale legacy Date is rejected"
        );

        let replay = ignored_activity("https://remote.example/acts/stale-rfc9421", REMOTE_ACTOR);
        let replay_headers = rfc9421_post_headers(
            INBOX_PATH,
            "https",
            REQUEST_HOST,
            &replay,
            REMOTE_KEY_ID,
            FIXTURE_PRIVATE_PEM,
            now_ms.div_euclid(1000) - 3600,
        );
        let status = post_shared_inbox(&services, replay_headers, replay).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "stale created is rejected"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// Fetch-gate mode selection (explicit mode, pre-seeded key, no network)
// ---------------------------------------------------------------------------

#[test]
fn fetch_gate_mode_selection() {
    let services = boot_services("fetch-gate-modes");
    seed_fixture_key(&services);
    let now_ms = services.clock.now_ms();

    // RFC 9421-only GET identifies under both modes.
    let signed = signatures::sign_rfc9421_headers(
        "GET",
        "/items/1",
        "https",
        REQUEST_HOST,
        None,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
        now_ms.div_euclid(1000),
    )
    .expect("RFC 9421 GET signing works");
    let rfc_map = header_map(&[
        ("host", REQUEST_HOST),
        ("signature-input", signed.signature_input.as_str()),
        ("signature", signed.signature.as_str()),
    ]);
    for mode in [Rfc9421Mode::Permissive, Rfc9421Mode::Enforce] {
        let viewer =
            fetch_gate::authenticate_fetch_viewer_with_mode(&services, "/items/1", &rfc_map, mode);
        assert_eq!(
            viewer.actor_uri.as_deref(),
            Some(REMOTE_ACTOR),
            "RFC 9421-only GET identifies under {mode:?}"
        );
    }
    // The same bytes fetched as another path fail closed under both modes.
    for mode in [Rfc9421Mode::Permissive, Rfc9421Mode::Enforce] {
        let viewer =
            fetch_gate::authenticate_fetch_viewer_with_mode(&services, "/items/2", &rfc_map, mode);
        assert!(
            viewer.is_anonymous(),
            "wrong-path GET is anonymous under {mode:?}"
        );
    }

    // Legacy-only GET identifies under permissive and collapses to
    // anonymous (gated objects 404) under enforce.
    let date = delivery::format_http_date(now_ms);
    let legacy = signatures::sign_request_headers(
        "GET",
        "/items/1",
        REQUEST_HOST,
        &date,
        None,
        REMOTE_KEY_ID,
        FIXTURE_PRIVATE_PEM,
    )
    .expect("legacy GET signing works");
    let legacy_map = header_map(&[
        ("date", date.as_str()),
        ("host", REQUEST_HOST),
        ("signature", legacy.signature.as_str()),
    ]);
    let viewer = fetch_gate::authenticate_fetch_viewer_with_mode(
        &services,
        "/items/1",
        &legacy_map,
        Rfc9421Mode::Permissive,
    );
    assert_eq!(
        viewer.actor_uri.as_deref(),
        Some(REMOTE_ACTOR),
        "legacy-only GET identifies under permissive"
    );
    let viewer = fetch_gate::authenticate_fetch_viewer_with_mode(
        &services,
        "/items/1",
        &legacy_map,
        Rfc9421Mode::Enforce,
    );
    assert!(
        viewer.is_anonymous(),
        "legacy-only GET is anonymous under enforce"
    );
}
