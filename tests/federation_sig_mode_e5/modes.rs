//! Mode-selection matrix, mode-cell default, and fetch-gate mode tests.
//! Moved verbatim from `tests/federation_sig_mode_e5.rs`.

use super::common::{
    FIXTURE_PRIVATE_PEM, OTHER_ACTOR, REMOTE_ACTOR, REMOTE_KEY_ID, REQUEST_HOST, boot_services,
    header_map, mode_mutex, seed_fixture_key,
};

use pinetree::activitypub::signatures;
use pinetree::activitypub::signatures::VerifyOutcome;
use pinetree::application::ports::Clock;
use pinetree::config::Rfc9421Mode;
use pinetree::federation::delivery;
use pinetree::federation::fetch_gate;

// ---------------------------------------------------------------------------
// Mode selection (permissive vs enforce) + fetch-gate mode selection
// ---------------------------------------------------------------------------

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
