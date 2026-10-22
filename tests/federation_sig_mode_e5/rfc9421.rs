//! RFC 9421-only signature-mode tests. Moved verbatim from
//! `tests/federation_sig_mode_e5.rs`.

use super::common::{
    FIXTURE_PRIVATE_PEM, FIXTURE_PUBLIC_PEM, INBOX_PATH, REMOTE_ACTOR, REMOTE_KEY_ID, REQUEST_HOST,
    REQUEST_PUBLIC_URL, boot_services, fixed_created_secs, fixed_now_ms, ignored_activity,
    post_body, post_shared_inbox, rfc9421_post_headers, seed_fixture_key, with_sig_mode,
};

use pinetree::activitypub::signatures;
use pinetree::application::ports::Clock;
use pinetree::config::Rfc9421Mode;
use pinetree::federation::inbox;

use axum::http::StatusCode;

// ---------------------------------------------------------------------------
// RFC-9421-only acceptance (crypto layer + inbox under permissive/enforce)
// ---------------------------------------------------------------------------

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
