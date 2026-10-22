//! Endpoint tests_batch_pq tests (split from `endpoints/mod.rs`).
//!
//! Pure move, no behavior change.

#![allow(unused_imports)]

use super::files_http::proxy_media;
use super::parse::parse_rss_feed;
use super::tests_common::{call, create_note, note_ids, scoped_token, signup_user, test_state};
use super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::application::ports::PortError;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use serde_json::Value;
use std::sync::Arc;
#[tokio::test]
async fn batch_p_email_identity_loop() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;

    // Wrong password → INCORRECT_PASSWORD (verbatim id).
    let (status, value) = call(
        &state,
        "i/update-email",
        serde_json::json!({ "password": "wrong-pw", "email": "alice@example.com" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INCORRECT_PASSWORD");
    assert_eq!(value["id"], "e54c1d7e-e7d6-4103-86b6-0a95069b4ad3");
    // Wrong-password attempts arm the progressive auth-failure delay
    // (change-password convention): step past it.
    clock.advance_ms(61_000);

    // Malformed address → UNAVAILABLE (shape half).
    let (status, value) = call(
        &state,
        "i/update-email",
        serde_json::json!({ "password": "s3cret-pw", "email": "not-an-email" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "UNAVAILABLE");

    // Set → MeDetailed, unverified, code minted.
    let (status, me) = call(
        &state,
        "i/update-email",
        serde_json::json!({ "password": "s3cret-pw", "email": "alice@example.com" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["email"], serde_json::json!("alice@example.com"));
    assert_eq!(me["emailVerified"], serde_json::json!(false));
    let stored = state
        .services
        .accounts
        .find_by_id(&alice_id)
        .expect("lookup")
        .expect("alice");
    let code = stored.email_verify_code.clone().expect("code minted");
    assert_eq!(code.len(), 16);

    // Unknown code → NO_SUCH_CODE.
    let (status, value) = call(
        &state,
        "verify-email",
        serde_json::json!({ "code": "zzzzzzzzzzzzzzzz" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
    assert_eq!(value["code"], "NO_SUCH_CODE");

    // Dormant transport: reset requests fail loudly and UNIFORMLY
    // (unknown user and valid user answer the same error — no oracle).
    for body in [
        serde_json::json!({ "username": "ghost", "email": "ghost@example.com" }),
        serde_json::json!({ "username": "alice", "email": "alice@example.com" }),
    ] {
        let (status, value) = call(&state, "request-reset-password", body, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "EMAIL_NOT_CONFIGURED");
    }

    // Unknown reset token → 500 (upstream `findOneByOrFail` parity).
    let (status, _) = call(
        &state,
        "reset-password",
        serde_json::json!({ "token": "tok-never-minted", "password": "new-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // Full rotation via a directly-created row (the send half is
    // dormant; the consume half is real).
    let reset_id = state.services.ids.generate();
    state
        .services
        .password_resets
        .create_reset_request(crate::application::ports::PasswordResetRequest {
            id: reset_id.clone(),
            user_id: alice_id.clone(),
            token: "tok-alice-rotate".to_string(),
        })
        .expect("create row");
    let (status, _) = call(
        &state,
        "reset-password",
        serde_json::json!({ "token": "tok-alice-rotate", "password": "r0tated-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // Single-use: replay misses → 500.
    let (status, _) = call(
        &state,
        "reset-password",
        serde_json::json!({ "token": "tok-alice-rotate", "password": "r0tated-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    // Roll past the 3/h `i/update-email` window for the rotation check.
    clock.advance_ms(61 * 60 * 1000);
    // The new password verifies (old one does not).
    let (status, _) = call(
        &state,
        "i/update-email",
        serde_json::json!({ "password": "r0tated-pw", "email": "alice2@example.com" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Expiry: 30-minute TTL read off the aidx id timestamp.
    let reset_id = state.services.ids.generate();
    state
        .services
        .password_resets
        .create_reset_request(crate::application::ports::PasswordResetRequest {
            id: reset_id,
            user_id: alice_id.clone(),
            token: "tok-alice-stale".to_string(),
        })
        .expect("create row");
    clock.advance_ms(31 * 60 * 1000);
    let (status, _) = call(
        &state,
        "reset-password",
        serde_json::json!({ "token": "tok-alice-stale", "password": "x" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // Verify the minted code → verified + cleared.
    let stored = state
        .services
        .accounts
        .find_by_id(&alice_id)
        .expect("lookup")
        .expect("alice");
    let code = stored.email_verify_code.clone().expect("second code");
    let (status, _) = call(
        &state,
        "verify-email",
        serde_json::json!({ "code": code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let stored = state
        .services
        .accounts
        .find_by_id(&alice_id)
        .expect("lookup")
        .expect("alice");
    assert!(stored.email_verified);
    assert!(stored.email_verify_code.is_none());
}

#[tokio::test]
async fn batch_p_move_local_guards_and_migration() {
    use crate::application::ports::AccountStore as _;
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;
    let (dave_id, dave_token) = signup_user(&state, "dave").await;
    let base = "https://example.local";
    let bob_uri = format!("{base}/users/{bob_id}");
    let carol_uri = format!("{base}/users/{carol_id}");

    // Root refusal: the oldest admin cannot migrate.
    state
        .services
        .set_role(None, &alice_id, true, false)
        .expect("make admin");
    let (status, value) = call(
        &state,
        "i/move",
        serde_json::json!({ "moveToAccount": "bob" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NOT_ROOT_FORBIDDEN");

    // Unknown destination → NO_SUCH_USER.
    let (status, value) = call(
        &state,
        "i/move",
        serde_json::json!({ "moveToAccount": "ghost" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");

    // No alias handshake → DESTINATION_ACCOUNT_FORBIDS.
    let (status, value) = call(
        &state,
        "i/move",
        serde_json::json!({ "moveToAccount": "carol" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "DESTINATION_ACCOUNT_FORBIDS");

    // Carol lists bob as a known-as alias; dave follows bob.
    let (status, _) = call(
        &state,
        "i/update",
        serde_json::json!({ "alsoKnownAs": [bob_uri] }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&dave_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Move succeeds: movedTo packs the destination URI.
    let (status, me) = call(
        &state,
        "i/move",
        serde_json::json!({ "moveToAccount": "carol" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["movedTo"], serde_json::json!(carol_uri));
    state.worker.drain(100).expect("drain move jobs");

    // Follower migration: dave unfollowed-old + follows-new.
    assert!(
        state
            .services
            .follows
            .is_following(&dave_id, &carol_id)
            .expect("edge")
    );
    assert!(
        !state
            .services
            .follows
            .is_following(&dave_id, &bob_id)
            .expect("edge")
    );

    // Already moved → ALREADY_MOVED.
    let (status, value) = call(
        &state,
        "i/move",
        serde_json::json!({ "moveToAccount": "carol" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_MOVED");
}

#[tokio::test]
async fn batch_p_registry_round_trip() {
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "alice").await;

    // Bad scope segment → INVALID_PARAM.
    let (status, _) = call(
        &state,
        "i/registry/set",
        serde_json::json!({ "key": "k", "value": 1, "scope": ["no-dashes"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Set → get → get-all → keys → keys-with-type → get-detail.
    let (status, _) = call(
        &state,
        "i/registry/set",
        serde_json::json!({ "key": "theme", "value": "dark", "scope": ["client"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "i/registry/get",
        serde_json::json!({ "key": "theme", "scope": ["client"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value, serde_json::json!("dark"));
    let (status, value) = call(
        &state,
        "i/registry/get-all",
        serde_json::json!({ "scope": ["client"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value, serde_json::json!({ "theme": "dark" }));
    let (status, value) = call(
        &state,
        "i/registry/keys",
        serde_json::json!({ "scope": ["client"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value, serde_json::json!(["theme"]));
    let (status, value) = call(
        &state,
        "i/registry/keys-with-type",
        serde_json::json!({ "scope": ["client"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value, serde_json::json!({ "theme": "string" }));
    let (status, value) = call(
        &state,
        "i/registry/get-detail",
        serde_json::json!({ "key": "theme", "scope": ["client"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["value"], serde_json::json!("dark"));
    assert!(value["updatedAt"].is_string(), "{value}");

    // Domain isolation: explicit-domain rows are invisible by default.
    let (status, _) = call(
        &state,
        "i/registry/set",
        serde_json::json!({ "key": "theme", "value": "blue", "scope": ["client"], "domain": null }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // scopes-with-domain lists the token-domain pair.
    let (status, value) = call(
        &state,
        "i/registry/scopes-with-domain",
        serde_json::json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert!(
        value
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["scopes"] == serde_json::json!([["client"]])
                || row["scopes"] == serde_json::json!(["client"]))
    );

    // Remove → NO_SUCH_KEY; re-remove stays silent.
    let (status, _) = call(
        &state,
        "i/registry/remove",
        serde_json::json!({ "key": "theme", "scope": ["client"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "i/registry/get",
        serde_json::json!({ "key": "theme", "scope": ["client"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
    assert_eq!(value["code"], "NO_SUCH_KEY");
    let (status, _) = call(
        &state,
        "i/registry/remove",
        serde_json::json!({ "key": "theme", "scope": ["client"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn batch_p_small_reads() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;
    create_note(&state, serde_json::json!({ "text": "hello" }), &alice_token).await;

    // Online count follows hub presence (pruned past the threshold).
    let now = state.services.clock.now_ms();
    state.hub.mark_present(&alice_id, now);
    state.hub.mark_present("stale", now - 11 * 60 * 1000);
    let (status, value) = call(
        &state,
        "get-online-users-count",
        serde_json::json!({}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["count"], serde_json::json!(1));

    // Pinned users: shape-compatible empty (no admin surface exists).
    let (status, value) = call(&state, "pinned-users", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value, serde_json::json!([]));

    // Server info shape.
    let (status, value) = call(&state, "server-info", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert!(value["machine"].is_string(), "{value}");
    assert!(value["cpu"]["cores"].as_u64().unwrap_or(0) >= 1, "{value}");
    assert!(value["cpu"]["model"].is_string(), "{value}");
    assert!(value["mem"]["total"].is_u64(), "{value}");
    assert!(value["fs"]["total"].is_u64(), "{value}");

    // Stats count real rows.
    let (status, value) = call(&state, "stats", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert!(value["usersCount"].as_u64().unwrap_or(0) >= 2, "{value}");
    assert!(
        value["originalUsersCount"].as_u64().unwrap_or(0) >= 2,
        "{value}"
    );
    assert!(value["notesCount"].as_u64().unwrap_or(0) >= 1, "{value}");
    assert_eq!(value["instances"], serde_json::json!(0));

    // fetch-rss rejects non-http(s) with the verbatim id.
    let (status, value) = call(
        &state,
        "fetch-rss",
        serde_json::json!({ "url": "ftp://example.com/feed" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_URL");
    assert_eq!(value["id"], "89b7ee05-ccfc-4bdd-9b13-61172fd1e06c");

    // RSS + Atom subset parsing (unit level).
    let rss = parse_rss_feed(
        r#"<?xml version="1.0"?><rss version="2.0"><channel><title>Blog</title><link>https://b.example</link><description>posts</description><item><title><![CDATA[Hi & bye]]></title><link>https://b.example/1</link><guid>1</guid><pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate><category>tech</category></item></channel></rss>"#,
        "https://b.example/feed",
    )
    .expect("rss parses");
    assert_eq!(rss["title"], serde_json::json!("Blog"));
    assert_eq!(rss["items"][0]["title"], serde_json::json!("Hi & bye"));
    assert_eq!(
        rss["items"][0]["isoDate"],
        serde_json::json!("Mon, 01 Jan 2024 00:00:00 GMT")
    );
    assert_eq!(rss["items"][0]["categories"], serde_json::json!(["tech"]));
    let atom = parse_rss_feed(
        r#"<feed xmlns="http://www.w3.org/2005/Atom"><title>Feed</title><link href="https://a.example/"/><entry><title>E1</title><link href="https://a.example/1"/><id>1</id><updated>2024-01-01T00:00:00Z</updated></entry></feed>"#,
        "https://a.example/feed",
    )
    .expect("atom parses");
    assert_eq!(
        atom["items"][0]["link"],
        serde_json::json!("https://a.example/1")
    );
    assert!(parse_rss_feed("<html>nope</html>", "https://x.example").is_none());
    let _ = bob_token;
}

#[tokio::test]
async fn batch_p_fetch_external_resources_round_trip() {
    use std::io::{Read, Write};
    // Loopback JSON peer (test `SafeHttpClient` allows private IPs).
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind json peer");
    let addr = listener.local_addr().expect("peer addr");
    let body = serde_json::json!({ "type": "text", "data": "line1\r\nline2" }).to_string();
    std::thread::spawn(move || {
        for _ in 0..2 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(body.as_bytes());
        }
    });
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "alice").await;
    let url = format!("http://{addr}/res.json");
    // sha512 hex of the CRLF-normalized data.
    let digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha512::digest("line1\nline2".as_bytes()).as_slice())
    };
    let (status, value) = call(
        &state,
        "fetch-external-resources",
        serde_json::json!({ "url": url, "hash": digest }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["type"], serde_json::json!("text"));
    // Wrong hash → verbatim mismatch error.
    let (status, value) = call(
        &state,
        "fetch-external-resources",
        serde_json::json!({ "url": url, "hash": "00".repeat(64) }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "EXT_RESOURCE_HASH_DIDNT_MATCH");
}

#[tokio::test]
async fn batch_p_mentions_timeline() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;

    let m1 = create_note(
        &state,
        serde_json::json!({ "text": "@bob hello there" }),
        &alice_token,
    )
    .await;
    let m2 = create_note(
        &state,
        serde_json::json!({ "text": "@bob self ping" }),
        &bob_token,
    )
    .await;
    // Followers-only mention invisible to non-follower bob.
    create_note(
        &state,
        serde_json::json!({ "text": "@bob secret", "visibility": "followers" }),
        &carol_token,
    )
    .await;

    // Bob sees both public mentions (alice's + his own).
    let (status, value) = call(
        &state,
        "notes/mentions",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let ids: Vec<&str> = value
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&m1.as_str()), "{ids:?}");
    assert!(ids.contains(&m2.as_str()), "{ids:?}");
    // Frozen visibility policy (§3): mentioned viewers see followers
    // notes, so carol's note is included for bob.
    assert_eq!(ids.len(), 3, "{ids:?}");

    // following=true keeps only followed authors + self (bob follows
    // nobody → only his own).
    let (status, value) = call(
        &state,
        "notes/mentions",
        serde_json::json!({ "following": true }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let ids: Vec<&str> = value
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![m2.as_str()]);

    // visibility=followers narrows; unknown values match nothing.
    let (status, value) = call(
        &state,
        "notes/mentions",
        serde_json::json!({ "visibility": "followers" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, value) = call(
        &state,
        "notes/mentions",
        serde_json::json!({ "visibility": "bogus" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value.as_array().unwrap().len(), 0);
    let _ = (alice_id, bob_id, carol_id);
}

#[tokio::test]
async fn batch_p_notification_utils() {
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "alice").await;

    // Test notification flows through the Notify path.
    let (status, _) = call(
        &state,
        "notifications/test-notification",
        serde_json::json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).expect("drain notify");
    let (status, value) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "limit": 20, "markAsRead": false, "includeTypes": ["test"] }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value.as_array().unwrap().len(), 1, "{value}");
    assert_eq!(value[0]["type"], serde_json::json!("test"));

    // Flush marks everything read (closest honest equivalent of the
    // Redis-timeline drop).
    let (status, _) = call(
        &state,
        "notifications/flush",
        serde_json::json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "limit": 20, "markAsRead": false }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert!(
        value
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n["isRead"] == serde_json::json!(true)),
        "{value}"
    );
}

#[tokio::test]
async fn batch_p_discovery_versions_translate() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;
    let (dave_id, dave_token) = signup_user(&state, "dave").await;

    // Follower counts: dave ← {bob, carol} (2), bob ← {alice} (1),
    // carol ← {} (0). Alice follows bob, so bob is excluded for her.
    for (token, target) in [
        (&alice_token, &bob_id),
        (&bob_token, &dave_id),
        (&carol_token, &dave_id),
    ] {
        let (status, _) = call(
            &state,
            "following/create",
            serde_json::json!({ "userId": target }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    // Recommendation: dave (2 followers) before carol (0); self and the
    // already-followed bob are skipped for alice.
    let (status, value) = call(
        &state,
        "users/recommendation",
        serde_json::json!({ "limit": 10 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let ids: Vec<&str> = value
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["id"].as_str().unwrap())
        .collect();
    assert!(!ids.contains(&alice_id.as_str()), "{ids:?}");
    assert!(!ids.contains(&bob_id.as_str()), "{ids:?}");
    assert_eq!(ids.first(), Some(&dave_id.as_str()), "{ids:?}");

    // Notes recommendations: carol's note surfaces for alice.
    let cn = create_note(
        &state,
        serde_json::json!({ "text": "carol post" }),
        &carol_token,
    )
    .await;
    let (status, value) = call(
        &state,
        "notes/recommendations",
        serde_json::json!({ "limit": 10 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let ids: Vec<&str> = value
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&cn.as_str()), "{ids:?}");

    // Versions: one edit → one history row (Sharkey shape).
    let (status, value) = call(
        &state,
        "notes/edit",
        serde_json::json!({ "editId": cn, "text": "carol post v2" }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let (status, value) = call(
        &state,
        "notes/versions",
        serde_json::json!({ "noteId": cn }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value.as_array().unwrap().len(), 1, "{value}");
    assert_eq!(value[0]["text"], serde_json::json!("carol post"));
    assert!(value[0]["oldDate"].is_string(), "{value}");
    assert!(value[0]["updatedAt"].is_string(), "{value}");
    // Unknown note → shared NO_SUCH_NOTE (400 by house convention,
    // like `notes/show`).
    let (status, value) = call(
        &state,
        "notes/versions",
        serde_json::json!({ "noteId": "oid00000000000000" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");

    // Translate: invisible note → CANNOT_TRANSLATE_INVISIBLE_NOTE.
    let secret = create_note(
        &state,
        serde_json::json!({ "text": "secret", "visibility": "followers" }),
        &alice_token,
    )
    .await;
    let (status, value) = call(
        &state,
        "notes/translate",
        serde_json::json!({ "noteId": secret, "targetLang": "en-US" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CANNOT_TRANSLATE_INVISIBLE_NOTE");
    // Visible note → dormant transport loud failure.
    let (status, value) = call(
        &state,
        "notes/translate",
        serde_json::json!({ "noteId": cn, "targetLang": "en-US" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "UNAVAILABLE");
    let _ = (alice_id, bob_id, carol_id);
}

/// Batch Q Phase 1, item 1: `following/requests/list` requires
/// `read:following` (upstream `list.ts:18`), not `read:account`.
#[tokio::test]
async fn batch_q_requests_list_permission_kind() {
    let (state, _) = test_state();
    let (alice_id, _) = signup_user(&state, "alice").await;
    let following_only = scoped_token(&state, &alice_id, vec!["read:following"]).await;
    let account_only = scoped_token(&state, &alice_id, vec!["read:account"]).await;
    let (status, value) = call(
        &state,
        "following/requests/list",
        serde_json::json!({}),
        Some(&following_only),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let (status, value) = call(
        &state,
        "following/requests/list",
        serde_json::json!({}),
        Some(&account_only),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["code"], "PERMISSION_DENIED");
}

/// Batch Q Phase 1, item 4: `i/notifications` carries the upstream
/// 30/30 s bucket, and an empty filter returns `[]` *before* the
/// mark-as-read step (upstream `notifications.ts:72-79` vs `:93-95`).
#[tokio::test]
async fn batch_q_notifications_bucket_and_empty_filter() {
    use crate::domain::notification::{Notification, NotificationKind};
    let (state, _) = test_state();
    let (alice_id, _) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    state
        .services
        .notifications
        .insert_notification(Notification {
            clip_id: None,
            page_id: None,
            id: state.services.ids.generate(),
            notifiee_id: bob_id.clone(),
            notifier_id: alice_id.clone(),
            kind: NotificationKind::Follow,
            note_id: None,
            reaction: None,
            draft_id: None,
            file_id: None,
            exported_entity: None,
            is_read: false,
        })
        .unwrap();
    // Empty filter with default markAsRead=true: `[]`, and nothing
    // is marked read.
    let (status, value) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "includeTypes": [] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value.as_array().unwrap().len(), 0);
    let (status, value) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value.as_array().unwrap().len(), 1);
    assert_eq!(value[0]["isRead"], false);
    // Same for the grouped variant.
    let (status, value) = call(
        &state,
        "i/notifications-grouped",
        serde_json::json!({ "includeTypes": [] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value.as_array().unwrap().len(), 0);
    let (status, value) = call(
        &state,
        "i/notifications-grouped",
        serde_json::json!({ "markAsRead": false }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value[0]["isRead"], false);

    // Bucket: a fresh account gets exactly 30 requests per 30 s.
    let (_, carol_token) = signup_user(&state, "carol").await;
    for n in 0..30 {
        let (status, value) = call(
            &state,
            "i/notifications",
            serde_json::json!({ "markAsRead": false }),
            Some(&carol_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "request {n}: {value}");
    }
    let (status, value) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
    assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");
}

/// Batch Q Phase 1, item 5: omitted `withReplies` defaults to false
/// (upstream column default `Following.ts:49-52`), on both
/// `following/create` and `i/import-following`.
#[tokio::test]
async fn batch_q_with_replies_default_false() {
    use crate::application::ports::FollowStore;
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;
    let (dave_id, _) = signup_user(&state, "dave").await;
    let (status, value) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let edge = state
        .services
        .follows
        .get_follow(&alice_id, &bob_id)
        .unwrap()
        .expect("follow edge");
    assert!(!edge.with_replies, "omitted withReplies must be false");
    let (status, value) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": dave_id, "withReplies": true }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let edge = state
        .services
        .follows
        .get_follow(&carol_id, &dave_id)
        .unwrap()
        .expect("follow edge");
    assert!(edge.with_replies, "explicit withReplies=true honored");
}

/// Batch Q Phase 1, item 6: `notes/create` (and `notes/edit`)
/// `fileIds ?? mediaIds` — when `fileIds` is present it wins outright.
#[tokio::test]
async fn batch_q_file_ids_wins_over_media_ids() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let file_a = state
        .services
        .upload_media(&alice_id, "a.txt".to_string(), b"aaa", None, false, true)
        .unwrap();
    let file_b = state
        .services
        .upload_media(&alice_id, "b.txt".to_string(), b"bbb", None, false, true)
        .unwrap();
    // Both keys: only `fileIds` attaches.
    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({
            "text": "both keys",
            "fileIds": [file_a.id],
            "mediaIds": [file_b.id],
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(
        value["createdNote"]["fileIds"],
        serde_json::json!([file_a.id])
    );
    // `mediaIds` alone still works as the legacy fallback.
    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "legacy key", "mediaIds": [file_b.id] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(
        value["createdNote"]["fileIds"],
        serde_json::json!([file_b.id])
    );
}

/// Batch Q Phase 1, item 9: `GET /proxy` (advertised as
/// `meta.mediaProxy`) validation branches. The happy path runs against
/// a loopback stub image server (`allow_private_ips: true` in tests).
#[tokio::test]
async fn batch_q_proxy_route() {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    async fn proxy(
        state: &ApiState,
        params: HashMap<String, String>,
        user_agent: Option<&str>,
    ) -> (StatusCode, Vec<u8>, HeaderMap) {
        let mut headers = HeaderMap::new();
        if let Some(ua) = user_agent {
            headers.insert("user-agent", ua.parse::<HeaderValue>().unwrap());
        }
        let response = proxy_media(State(state.clone()), Query(params), headers).await;
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        (status, bytes, headers)
    }
    fn params(url: &str) -> HashMap<String, String> {
        HashMap::from([("url".to_string(), url.to_string())])
    }

    let (state, _) = test_state();
    // Missing User-Agent → 400.
    let (status, _, _) = proxy(&state, params("https://example.com/a.png"), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Missing url → 400 INVALID_PARAM.
    let (status, body, _) = proxy(&state, HashMap::new(), Some("test-agent")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["code"], "INVALID_PARAM");
    // Another proxy in the loop → 403.
    let (status, _, _) = proxy(
        &state,
        params("https://example.com/a.png"),
        Some("Misskey/2026 (proxy)"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Non-http(s) URL → 400.
    let (status, _, _) = proxy(&state, params("file:///etc/passwd"), Some("test-agent")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Unreachable host → 404, not a 500.
    let (status, _, _) = proxy(
        &state,
        params("https://proxy-test.invalid/a.png"),
        Some("test-agent"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Loopback stub: one PNG response, one text response.
    const PNG_1PX: &[u8] = &[
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f,
        0x15, 0xc4, 0x89, 0x00, 0x00, 0x00, 0x0a, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00,
        0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
        0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    ];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = stream.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]);
            let (content_type, body): (&str, &[u8]) = if head.contains("GET /pic.png") {
                ("image/png", PNG_1PX)
            } else {
                ("text/plain", b"hello")
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.write_all(body);
        }
    });
    let (status, body, headers) = proxy(
        &state,
        params(&format!("http://{addr}/pic.png")),
        Some("test-agent"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, PNG_1PX);
    assert_eq!(headers["content-type"], "image/png");
    assert_eq!(headers["cache-control"], "max-age=31536000, immutable");
    // Non-media is refused, never proxied as active content.
    let (status, _, _) = proxy(
        &state,
        params(&format!("http://{addr}/note.txt")),
        Some("test-agent"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Batch Q Phase 1, item 3a: `users/show` 404s suspended users for
/// non-moderators (upstream `users/show.ts:174` verbatim id + 404),
/// moderators still see them, and the `userIds` batch omits hidden rows.
#[tokio::test]
async fn batch_q_users_show_suspension() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;
    let (_, carol_token) = signup_user(&state, "carol").await;
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("make alice moderator");
    let (status, _) = call(
        &state,
        "admin/suspend-user",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Anonymous + non-moderator: 404 with the verbatim upstream id.
    for token in [None, Some(carol_token.as_str())] {
        let (status, value) = call(
            &state,
            "users/show",
            serde_json::json!({ "userId": bob_id }),
            token,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
        assert_eq!(value["code"], "NO_SUCH_USER");
        assert_eq!(value["error"]["id"], "4362f8dc-731f-4ad8-a694-be5a88922a24");
    }
    // Moderator still sees the suspended user.
    let (status, shown) = call(
        &state,
        "users/show",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["id"], bob_id);
    // Batch omits the hidden row for non-mods, keeps it for mods.
    let (status, batch) = call(
        &state,
        "users/show",
        serde_json::json!({ "userIds": [alice_id, bob_id] }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{batch}");
    assert_eq!(batch.as_array().unwrap().len(), 1);
    assert_eq!(batch[0]["id"], alice_id);
    let (status, batch) = call(
        &state,
        "users/show",
        serde_json::json!({ "userIds": [alice_id, bob_id] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{batch}");
    assert_eq!(batch.as_array().unwrap().len(), 2);
    // Missing users answer the same verbatim 404 on the single path.
    let (status, value) = call(
        &state,
        "users/show",
        serde_json::json!({ "userId": "aid00000000000099" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");
    assert_eq!(value["error"]["id"], "4362f8dc-731f-4ad8-a694-be5a88922a24");
}

/// Batch Q Phase 1, item 3b: `notes/show` content gates —
/// `CONTENT_RESTRICTED_BY_USER` (author opt-in) and
/// `CONTENT_RESTRICTED_BY_SERVER` (`ugcVisibilityForVisitor`), both
/// verbatim, both anonymous-only and ordered before visibility.
#[tokio::test]
async fn batch_q_notes_show_content_restricted() {
    use crate::application::ports::{AccountStore, CreateAccountInput};
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_bob_id, bob_token) = signup_user(&state, "bob").await;
    let (_, carol_token) = signup_user(&state, "carol").await;
    // Bob opts into signin-gated contents.
    let (status, _) = call(
        &state,
        "i/update",
        serde_json::json!({ "requireSigninToViewContents": true }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let gated = create_note(
        &state,
        serde_json::json!({ "text": "gated note" }),
        &bob_token,
    )
    .await;
    let open = create_note(
        &state,
        serde_json::json!({ "text": "open note" }),
        &alice_token,
    )
    .await;
    // Anonymous: gated → verbatim CONTENT_RESTRICTED_BY_USER.
    let (status, value) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": gated }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CONTENT_RESTRICTED_BY_USER");
    assert_eq!(value["error"]["id"], "fbcc002d-37d9-4944-a6b0-d9e29f2d33ab");
    // Signed-in viewers and open notes are unaffected.
    let (status, _) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": gated }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": open }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Remote-author note under an opted-in `local` policy: anonymous
    // → CONTENT_RESTRICTED_BY_SERVER. (Pinetree default is `all` —
    // DIVERGENCE from upstream's `local` default, documented on the
    // enum — so opt in explicitly. Signup tokens carry every scope,
    // so Alice's token works for admin endpoints once the role lands.)
    state
        .services
        .set_role(None, &alice_id, true, false)
        .expect("make alice admin");
    let (status, value) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({ "ugcVisibilityForVisitor": "local" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    let (status, meta) = call(
        &state,
        "admin/meta",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{meta}");
    assert_eq!(meta["ugcVisibilityForVisitor"], "local");
    let remote = state
        .services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: "rina".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/rina".to_string()),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .unwrap();
    let input =
        crate::application::ports::CreateNoteInput::simple_public(&remote.id, "remote note");
    let remote_note = state.services.create_note(input).unwrap().id;
    let (status, value) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": remote_note }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CONTENT_RESTRICTED_BY_SERVER");
    assert_eq!(value["error"]["id"], "145f88d2-b03d-4087-8143-a78928883c4b");
    let (status, _) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": remote_note }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // `none` restricts even local notes for anonymous visitors.
    let (status, value) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({ "ugcVisibilityForVisitor": "none" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    let (status, value) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": open }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CONTENT_RESTRICTED_BY_SERVER");
    // ...while signed-in readers still pass, and `all` restores anon.
    let (status, _) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": open }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({ "ugcVisibilityForVisitor": "all" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": remote_note }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Invalid values are refused, never stored.
    let (status, value) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({ "ugcVisibilityForVisitor": "everywhere" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
}

/// Batch Q Phase 1, item 7: home + hybrid timelines honor the
/// `include*` trio through the dispatcher (defaults true).
#[tokio::test]
async fn batch_q_timeline_include_flags() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (status, _) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let target = create_note(
        &state,
        serde_json::json!({ "text": "bob's note" }),
        &bob_token,
    )
    .await;
    // Alice pure-renotes Bob's note: a bare reshare in her home feed.
    let (status, _) = call(
        &state,
        "notes/create",
        serde_json::json!({ "renoteId": target }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    for endpoint in ["notes/timeline", "notes/hybrid-timeline"] {
        let (status, feed) =
            call(&state, endpoint, serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {feed}");
        assert_eq!(note_ids(&feed).len(), 2, "{endpoint} default: {feed}");
        let (status, feed) = call(
            &state,
            endpoint,
            serde_json::json!({ "includeMyRenotes": false }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {feed}");
        assert_eq!(
            note_ids(&feed),
            vec![target.clone()],
            "{endpoint} flag: {feed}"
        );
    }
}
