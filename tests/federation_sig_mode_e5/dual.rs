//! Dual-signed (legacy + RFC 9421) tests. Moved verbatim from
//! `tests/federation_sig_mode_e5.rs`.

use super::common::{
    FIXTURE_PRIVATE_PEM, FIXTURE_PUBLIC_PEM, INBOX_PATH, REMOTE_ACTOR, REMOTE_KEY_ID, REQUEST_HOST,
    boot_services, dual_post_headers, fixed_created_secs, fixed_date_header, fixed_now_ms,
    ignored_activity, post_body, post_shared_inbox, seed_fixture_key, with_sig_mode,
};

use pinetree::activitypub::signatures;
use pinetree::activitypub::signatures::VerifyOutcome;
use pinetree::application::ports::Clock;
use pinetree::config::Rfc9421Mode;
use pinetree::federation::delivery;
use pinetree::federation::inbox;

use axum::http::StatusCode;

// ---------------------------------------------------------------------------
// Dual-signature acceptance (both schemes verify, agreeing owners
// authenticate under both modes; inbox accepts under permissive)
// ---------------------------------------------------------------------------

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
