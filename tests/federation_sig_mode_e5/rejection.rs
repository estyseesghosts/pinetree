//! Cross-scheme rejection tests (crypto layer + inbox gates). Moved
//! verbatim from `tests/federation_sig_mode_e5.rs`.

use super::common::{
    FIXTURE_PRIVATE_PEM, FIXTURE_PUBLIC_PEM, INBOX_PATH, OTHER_ACTOR, OTHER_KEY_ID, REMOTE_ACTOR,
    REMOTE_KEY_ID, REQUEST_HOST, boot_services, dual_post_headers, fixed_created_secs,
    fixed_date_header, fixed_now_ms, foreign_object_create, header_map, ignored_activity,
    legacy_post_headers, post_body, post_shared_inbox, rfc9421_post_headers, second_keypair,
    seed_fixture_key, with_sig_mode,
};

use serde_json::json;

use pinetree::activitypub::signatures;
use pinetree::activitypub::signatures::VerifyOutcome;
use pinetree::application::ports::Clock;
use pinetree::config::Rfc9421Mode;
use pinetree::federation::delivery;
use pinetree::federation::inbox;

use axum::http::{HeaderMap, StatusCode};

// ---------------------------------------------------------------------------
// Rejections: wrong keys, corrupted bytes, tampered bodies, malformed
// components, stale/future timestamps, rotation, owner disagreement,
// actor mismatch, foreign-object authority, stale inbox replays
// ---------------------------------------------------------------------------

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
