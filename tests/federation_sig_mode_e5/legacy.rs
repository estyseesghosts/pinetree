//! Legacy (draft-cavanaugh) signature-mode tests. Moved verbatim from
//! `tests/federation_sig_mode_e5.rs`.

use super::common::{
    FIXTURE_PRIVATE_PEM, FIXTURE_PUBLIC_PEM, INBOX_PATH, REMOTE_ACTOR, REMOTE_KEY_ID, REQUEST_HOST,
    boot_services, fixed_date_header, fixed_now_ms, ignored_activity, legacy_post_headers,
    post_body, post_shared_inbox, seed_fixture_key, with_sig_mode,
};

use pinetree::activitypub::signatures;
use pinetree::application::ports::Clock;
use pinetree::config::Rfc9421Mode;
use pinetree::federation::delivery;
use pinetree::federation::inbox;

use axum::http::StatusCode;

// ---------------------------------------------------------------------------
// Legacy-only acceptance (crypto layer + inbox under permissive/enforce)
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
