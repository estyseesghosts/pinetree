//! Endpoint tests_notes tests (threads).
//!
//! Split from `endpoints/tests_notes.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::tests_common::{call, create_note, note_ids, signup_user, test_state};
use super::super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::api::misskey::response::RateLimiter;
use crate::application::ports::PortError;
use crate::domain::id::Clock;
use crate::domain::note::Visibility;
use axum::http::StatusCode;
use serde_json::Value;
use std::sync::Arc;

#[tokio::test]
async fn phase3_note_relations() {
    let (state, clock) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    let root = create_note(&state, serde_json::json!({ "text": "root" }), &alice_token).await;
    clock.advance_ms(1_000);
    let reply1 = create_note(
        &state,
        serde_json::json!({ "text": "reply one", "replyId": root }),
        &bob_token,
    )
    .await;
    clock.advance_ms(1_000);
    let reply2 = create_note(
        &state,
        serde_json::json!({ "text": "reply two", "replyId": root }),
        &bob_token,
    )
    .await;
    clock.advance_ms(1_000);
    let pure = create_note(&state, serde_json::json!({ "renoteId": root }), &bob_token).await;
    clock.advance_ms(1_000);
    let quote = create_note(
        &state,
        serde_json::json!({ "renoteId": root, "text": "quoting" }),
        &bob_token,
    )
    .await;
    clock.advance_ms(1_000);

    let (status, replies) = call(
        &state,
        "notes/replies",
        serde_json::json!({ "noteId": root }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replies}");
    assert_eq!(note_ids(&replies), vec![reply2.clone(), reply1.clone()]);

    let (status, renotes) = call(
        &state,
        "notes/renotes",
        serde_json::json!({ "noteId": root }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{renotes}");
    assert_eq!(note_ids(&renotes), vec![quote.clone(), pure.clone()]);

    // Children: replies plus the quote, never the pure renote.
    let (status, children) = call(
        &state,
        "notes/children",
        serde_json::json!({ "noteId": root }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{children}");
    assert_eq!(
        note_ids(&children),
        vec![quote.clone(), reply2.clone(), reply1.clone()]
    );

    // Pagination over children.
    let (status, page) = call(
        &state,
        "notes/children",
        serde_json::json!({ "noteId": root, "limit": 1 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(note_ids(&page), vec![quote.clone()]);
    let (status, page) = call(
        &state,
        "notes/children",
        serde_json::json!({ "noteId": root, "untilId": quote }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(note_ids(&page), vec![reply2.clone(), reply1.clone()]);

    // Unknown targets: renotes 404s (Misskey parity), replies/children
    // answer [].
    let (status, value) = call(
        &state,
        "notes/renotes",
        serde_json::json!({ "noteId": "nid00000000000099" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
    for endpoint in ["notes/replies", "notes/children"] {
        let (status, list) = call(
            &state,
            endpoint,
            serde_json::json!({ "noteId": "nid00000000000099" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {list}");
        assert_eq!(list.as_array().unwrap().len(), 0);
    }

    let (status, value) = call(&state, "notes/replies", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
}

#[tokio::test]
async fn phase3_note_conversation() {
    let (state, clock) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    let a = create_note(&state, serde_json::json!({ "text": "first" }), &alice_token).await;
    clock.advance_ms(1_000);
    let b = create_note(
        &state,
        serde_json::json!({ "text": "second", "replyId": a }),
        &bob_token,
    )
    .await;
    clock.advance_ms(1_000);
    let c = create_note(
        &state,
        serde_json::json!({ "text": "third", "replyId": b }),
        &alice_token,
    )
    .await;
    clock.advance_ms(1_000);

    // Oldest-first ancestor chain.
    let (status, conv) = call(
        &state,
        "notes/conversation",
        serde_json::json!({ "noteId": c }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{conv}");
    assert_eq!(note_ids(&conv), vec![a.clone(), b.clone()]);

    let (status, conv) = call(
        &state,
        "notes/conversation",
        serde_json::json!({ "noteId": b }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{conv}");
    assert_eq!(note_ids(&conv), vec![a.clone()]);

    let (status, conv) = call(
        &state,
        "notes/conversation",
        serde_json::json!({ "noteId": a }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{conv}");
    assert_eq!(conv.as_array().unwrap().len(), 0);

    // Offset skips from the oldest side; limit caps.
    let (status, conv) = call(
        &state,
        "notes/conversation",
        serde_json::json!({ "noteId": c, "offset": 1 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{conv}");
    assert_eq!(note_ids(&conv), vec![b.clone()]);
    let (status, conv) = call(
        &state,
        "notes/conversation",
        serde_json::json!({ "noteId": c, "limit": 1 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{conv}");
    assert_eq!(note_ids(&conv), vec![a.clone()]);

    let (status, value) = call(
        &state,
        "notes/conversation",
        serde_json::json!({ "noteId": "nid00000000000099" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
}

#[tokio::test]
async fn phase3_unrenote() {
    let (state, clock) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    let root = create_note(
        &state,
        serde_json::json!({ "text": "renote me" }),
        &alice_token,
    )
    .await;
    clock.advance_ms(1_000);
    let pure = create_note(&state, serde_json::json!({ "renoteId": root }), &bob_token).await;
    clock.advance_ms(1_000);
    let quote = create_note(
        &state,
        serde_json::json!({ "renoteId": root, "text": "quoting" }),
        &bob_token,
    )
    .await;
    clock.advance_ms(1_000);

    // Unauthenticated callers are rejected before any state change.
    let (status, value) = call(
        &state,
        "notes/unrenote",
        serde_json::json!({ "noteId": root }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{value}");

    let (status, _) = call(
        &state,
        "notes/unrenote",
        serde_json::json!({ "noteId": root }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Both of Bob's renotes are gone; Alice's root survives.
    for gone in [pure, quote] {
        let (status, value) = call(
            &state,
            "notes/show",
            serde_json::json!({ "noteId": gone }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_NOTE");
    }
    let (status, _) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": root }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Idempotent: nothing left to delete is still success. (The shared
    // notes/delete bucket enforces a 1 s minimum interval, so the clock
    // moves past it first.)
    clock.advance_ms(1_000);
    let (status, _) = call(
        &state,
        "notes/unrenote",
        serde_json::json!({ "noteId": root }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    clock.advance_ms(1_000);
    let (status, value) = call(
        &state,
        "notes/unrenote",
        serde_json::json!({ "noteId": "nid00000000000099" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
}

/// §4 polls + §6 wire compat, endpoint level: `notes/create` poll
/// params, `notes/polls/vote` flows and exact Misskey error codes, and
/// reaction normalization on `notes/reactions/create`.
#[tokio::test]
async fn notes_polls_vote_flows_and_error_codes() {
    let (state, clock) = test_state();
    let (_alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_bob_id, bob_token) = signup_user(&state, "bob").await;
    let now = clock.now_ms();

    // Poll choices validation: too few, dupes, overlong.
    for poll in [
        serde_json::json!({ "choices": ["only"] }),
        serde_json::json!({ "choices": ["a", "a"] }),
        serde_json::json!({ "choices": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"] }),
        serde_json::json!({ "choices": ["", "b"] }),
    ] {
        let (status, value) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": "t", "poll": poll }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{poll}: {value}");
        assert_eq!(value["code"], "INVALID_PARAM");
    }
    // Already-expired poll → exact Misskey ident + UUID.
    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "t", "poll": { "choices": ["a", "b"], "expiresAt": now - 1 } }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CANNOT_CREATE_ALREADY_EXPIRED_POLL");
    assert_eq!(value["error"]["id"], "04da457d-b083-4055-9082-955525eda5a5");
    assert_eq!(value["message"], "Poll is already expired.");

    // A live poll creates with the Misskey pack shape.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({
            "text": "pick one",
            "poll": { "choices": ["a", "b"], "multiple": false, "expiredAfter": 3_600_000 },
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    assert_eq!(created["createdNote"]["hasPoll"], serde_json::json!(true));
    assert_eq!(
        created["createdNote"]["poll"]["choices"][0]["text"],
        serde_json::json!("a")
    );

    // A poll stands in for text (Misskey `notes/create.ts` parity).
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "poll": { "choices": ["a", "b"] } }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(created["createdNote"]["hasPoll"], serde_json::json!(true));

    // Vote: 204, then duplicate → ALREADY_VOTED (exact UUID).
    let (status, _) = call(
        &state,
        "notes/polls/vote",
        serde_json::json!({ "noteId": note_id, "choice": 0 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "notes/polls/vote",
        serde_json::json!({ "noteId": note_id, "choice": 0 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_VOTED");
    assert_eq!(value["error"]["id"], "0963fc77-efac-419b-9424-b391608dc6d8");

    // Tallies + isVoted visible on show.
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["poll"]["choices"][0]["votes"], serde_json::json!(1));
    assert_eq!(
        shown["poll"]["choices"][0]["isVoted"],
        serde_json::json!(true)
    );
    assert_eq!(
        shown["poll"]["choices"][1]["isVoted"],
        serde_json::json!(false)
    );

    // Full error set with exact codes.
    let (status, value) = call(
        &state,
        "notes/polls/vote",
        serde_json::json!({ "noteId": "aid00000000000099", "choice": 0 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
    let (status, value) = call(
        &state,
        "notes/polls/vote",
        serde_json::json!({ "noteId": note_id, "choice": 9 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_CHOICE");
    assert_eq!(value["error"]["id"], "e0cc9a04-f2e8-41e4-a5f1-4127293260cc");

    // NO_POLL on a plain note.
    let (status, plain) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "plain" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{plain}");
    let plain_id = plain["createdNote"]["id"].as_str().unwrap();
    assert_eq!(plain["createdNote"]["hasPoll"], serde_json::json!(false));
    assert!(plain["createdNote"]["poll"].is_null());
    let (status, value) = call(
        &state,
        "notes/polls/vote",
        serde_json::json!({ "noteId": plain_id, "choice": 0 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_POLL");
    assert_eq!(value["error"]["id"], "5f979967-52d9-4314-a911-1c673727f92f");

    // Blocked voter → YOU_HAVE_BEEN_BLOCKED (author blocked the voter).
    let (status, _) = call(
        &state,
        "blocking/create",
        serde_json::json!({ "userId": _bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "alice blocks bob");
    let (status, value) = call(
        &state,
        "notes/polls/vote",
        serde_json::json!({ "noteId": note_id, "choice": 1 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "YOU_HAVE_BEEN_BLOCKED");
}

#[tokio::test]
async fn phase3_notes_search() {
    let (state, clock) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    create_note(
        &state,
        serde_json::json!({ "text": "hello rust world #rust" }),
        &alice_token,
    )
    .await;
    clock.advance_ms(1_000);
    create_note(
        &state,
        serde_json::json!({ "text": "goodbye moon" }),
        &alice_token,
    )
    .await;
    clock.advance_ms(1_000);
    let bob_rust = create_note(
        &state,
        serde_json::json!({ "text": "hello rust again" }),
        &bob_token,
    )
    .await;
    clock.advance_ms(1_000);
    create_note(
        &state,
        serde_json::json!({ "text": "secret rust", "visibility": "followers" }),
        &alice_token,
    )
    .await;
    clock.advance_ms(1_000);

    // Substring match over local notes; Bob cannot see Alice's
    // followers-only note.
    let (status, hits) = call(
        &state,
        "notes/search",
        serde_json::json!({ "query": "rust" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hits}");
    assert_eq!(hits.as_array().unwrap().len(), 2);

    // Alice sees her own followers-only note too.
    let (status, hits) = call(
        &state,
        "notes/search",
        serde_json::json!({ "query": "rust" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hits}");
    assert_eq!(hits.as_array().unwrap().len(), 3);

    // userId + host filters.
    let (status, hits) = call(
        &state,
        "notes/search",
        serde_json::json!({ "query": "rust", "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hits}");
    assert_eq!(note_ids(&hits), vec![bob_rust.clone()]);
    let (status, hits) = call(
        &state,
        "notes/search",
        serde_json::json!({ "query": "rust", "host": "." }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hits}");
    assert_eq!(hits.as_array().unwrap().len(), 3);
    let (status, hits) = call(
        &state,
        "notes/search",
        serde_json::json!({ "query": "rust", "host": "remote.example" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hits}");
    assert_eq!(hits.as_array().unwrap().len(), 0);

    // search-by-tag: case-insensitive.
    let (status, hits) = call(
        &state,
        "notes/search-by-tag",
        serde_json::json!({ "tag": "RUST" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hits}");
    assert_eq!(hits.as_array().unwrap().len(), 1);

    // Misskey parity: public endpoint — anonymous search works and
    // sees exactly the public notes (not the followers-only one).
    let (status, hits) = call(
        &state,
        "notes/search",
        serde_json::json!({ "query": "rust" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hits}");
    assert_eq!(hits.as_array().unwrap().len(), 2);
    let (status, value) = call(
        &state,
        "notes/search",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    // `offset` is accepted (applies before `limit`); `channelId` and
    // the `query[][]` tag form stay rejected (no channels here; the
    // profile is `tag`-only).
    let (status, _) = call(
        &state,
        "notes/search",
        serde_json::json!({ "query": "x", "offset": 0 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    for (endpoint, body) in [
        (
            "notes/search",
            serde_json::json!({ "query": "x", "channelId": "y" }),
        ),
        (
            "notes/search-by-tag",
            serde_json::json!({ "tag": "rust", "query": [] }),
        ),
    ] {
        let (status, value) = call(&state, endpoint, body, Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{endpoint}: {value}");
        assert_eq!(value["code"], "UNSUPPORTED_FIELD");
    }
}
