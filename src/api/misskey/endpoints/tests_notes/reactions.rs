//! Endpoint tests_notes tests (reactions).
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
async fn notes_like_reacts_toggles_and_replaces() {
    let (mut state, clock) = test_state();
    // The 2/sec `notes/like` bucket would trip on back-to-back test
    // calls (frozen clock): swap in an open limiter. Bucket behaviour
    // is covered by the shared limiter tests.
    state.rate_limiter = Arc::new(RateLimiter::new(clock));
    let (_, alice_token) = signup_user(&state, "alice").await;

    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "like me" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let note_id = value["createdNote"]["id"].as_str().unwrap().to_string();

    // Implicit like resolves to the bare heart (no user/instance default).
    let (status, _) = call(
        &state,
        "notes/like",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, note_value) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{note_value}");
    assert_eq!(note_value["myReaction"], "❤");

    // Same-emoji re-press toggles off (Sharkey parity).
    let (status, _) = call(
        &state,
        "notes/like",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, note_value) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{note_value}");
    assert_eq!(note_value["myReaction"], Value::Null);

    // A different emoji replaces (Catodon parity).
    for reaction in ["👍", "🎉", "👍"] {
        let (status, _) = call(
            &state,
            "notes/like",
            serde_json::json!({ "noteId": note_id, "override": reaction }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{reaction}");
    }
    let (status, note_value) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{note_value}");
    // 👍 set, 🎉 replaced it, 👍 replaced that back.
    assert_eq!(note_value["myReaction"], "👍");
}

#[tokio::test]
async fn notes_like_rejects_bad_input_and_anonymous() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;

    // Unknown field.
    let (status, value) = call(
        &state,
        "notes/like",
        serde_json::json!({ "noteId": "x", "bogus": 1 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "UNSUPPORTED_FIELD");

    // Anonymous callers are refused.
    let (status, _) = call(
        &state,
        "notes/like",
        serde_json::json!({ "noteId": "x" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Unknown note.
    let (status, value) = call(
        &state,
        "notes/like",
        serde_json::json!({ "noteId": "n00000000000000" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
}

#[tokio::test]
async fn reactions_create_rejects_duplicates() {
    let (state, clock) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;

    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "react to me" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let note_id = value["createdNote"]["id"].as_str().unwrap().to_string();

    let (status, _) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": note_id, "reaction": "❤" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, value) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": note_id, "reaction": "❤" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_REACTED");

    clock.advance_ms(5_000);
    let (status, _) = call(
        &state,
        "notes/reactions/delete",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn notes_reactions_lists_reactors() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (_, carol_token) = signup_user(&state, "carol").await;
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "hello" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    for (token, reaction) in [(&alice_token, "👍"), (&bob_token, "❤")] {
        let (status, v) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": reaction }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{v}");
    }

    // Anonymous read of a public note: newest first, upstream shape.
    let (status, rows) = call(
        &state,
        "notes/reactions",
        serde_json::json!({ "noteId": note_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["user"]["id"], bob_id);
    assert_eq!(rows[0]["type"], "❤");
    assert_eq!(rows[1]["user"]["id"], alice_id);
    assert!(rows[1]["createdAt"].is_string() && rows[1].get("note").is_none());

    // `type` filter and paging.
    let (_, only) = call(
        &state,
        "notes/reactions",
        serde_json::json!({ "noteId": note_id, "type": "👍" }),
        None,
    )
    .await;
    assert_eq!(only.as_array().unwrap().len(), 1);
    let (_, page) = call(
        &state,
        "notes/reactions",
        serde_json::json!({ "noteId": note_id, "limit": 1, "untilId": rows[0]["id"] }),
        None,
    )
    .await;
    assert_eq!(page.as_array().unwrap().len(), 1);
    assert_eq!(page[0]["user"]["id"], alice_id);

    // Unknown note, and a followers-only note hidden from a stranger.
    let (status, v) = call(
        &state,
        "notes/reactions",
        serde_json::json!({ "noteId": "nosuchnote0000000" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
    let (_, private) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "fo", "visibility": "followers" }),
        Some(&bob_token),
    )
    .await;
    let pid = private["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, v) = call(
        &state,
        "notes/reactions",
        serde_json::json!({ "noteId": pid }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
}

/// §6 wire compat, endpoint level: legacy names, VS16 fallback, FE0F
/// stripping, and `likeOnly` coercion on `notes/reactions/create`.
#[tokio::test]
async fn reactions_normalize_like_misskey() {
    use crate::application::ports::ReactionStore;

    let (state, _clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_bob_id, bob_token) = signup_user(&state, "bob").await;

    // A likeOnly note coerces everything to the bare heart.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "like only", "reactionAcceptance": "likeOnly" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, _) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": note_id, "reaction": "👍" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let stored = state
        .services
        .reactions
        .get_reaction(&_bob_id, &note_id)
        .unwrap()
        .unwrap();
    assert_eq!(stored.reaction, "❤");

    // Legacy names map; VS16 hearts normalize to the bare fallback.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "open" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let open_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, _) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": open_id, "reaction": "like" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let stored = state
        .services
        .reactions
        .get_reaction(&_bob_id, &open_id)
        .unwrap()
        .unwrap();
    assert_eq!(stored.reaction, "👍");

    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "open 2" }),
        Some(&alice_token),
    )
    .await;
    // Alice reacts with the VS16 heart on her own note (first-party
    // token path exercises the same normalization).
    let open2_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    assert_eq!(status, StatusCode::OK, "{created}");
    let (status, _) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": open2_id, "reaction": "❤️" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let stored = state
        .services
        .reactions
        .get_reaction(&alice_id, &open2_id)
        .unwrap()
        .unwrap();
    assert_eq!(stored.reaction, "❤");
}

#[tokio::test]
async fn parity_default_reaction_heart_click() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    async fn alice_note(state: &ApiState, alice_token: &str, body: Value) -> String {
        let (status, note) = call(state, "notes/create", body, Some(alice_token)).await;
        assert_eq!(status, StatusCode::OK, "{note}");
        note["createdNote"]["id"].as_str().unwrap().to_string()
    }

    async fn heart_click(state: &ApiState, bob_token: &str, note_id: &str, reaction: Value) {
        let mut body = serde_json::json!({ "noteId": note_id });
        if !reaction.is_null() {
            body["reaction"] = reaction;
        }
        let (status, value) = call(state, "notes/reactions/create", body, Some(bob_token)).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    }

    fn stored(state: &ApiState, user_id: &str, note_id: &str) -> String {
        state
            .services
            .reactions
            .get_reaction(user_id, note_id)
            .unwrap()
            .unwrap()
            .reaction
    }

    // No default set: heart-click with no `reaction` param → FALLBACK.
    let note_id = alice_note(&state, &alice_token, serde_json::json!({ "text": "n1" })).await;
    heart_click(&state, &bob_token, &note_id, Value::Null).await;
    assert_eq!(stored(&state, &bob_id, &note_id), "❤");

    // Explicit empty `reaction` is still rejected (upstream parity).
    let (status, value) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": note_id, "reaction": "" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");

    // Bob sets a per-user default; heart-click resolves to it.
    let (status, me) = call(
        &state,
        "i/update",
        serde_json::json!({ "defaultReaction": "🎉" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    let note_id = alice_note(&state, &alice_token, serde_json::json!({ "text": "n2" })).await;
    heart_click(&state, &bob_token, &note_id, Value::Null).await;
    assert_eq!(stored(&state, &bob_id, &note_id), "🎉");

    // Explicit reaction wins over the stored default.
    let note_id = alice_note(&state, &alice_token, serde_json::json!({ "text": "n3" })).await;
    heart_click(&state, &bob_token, &note_id, serde_json::json!("👍")).await;
    assert_eq!(stored(&state, &bob_id, &note_id), "👍");

    // A VS16-form default normalizes through the standard pipeline.
    let (status, me) = call(
        &state,
        "i/update",
        serde_json::json!({ "defaultReaction": "❤️" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    let note_id = alice_note(&state, &alice_token, serde_json::json!({ "text": "n4" })).await;
    heart_click(&state, &bob_token, &note_id, Value::Null).await;
    assert_eq!(stored(&state, &bob_id, &note_id), "❤");

    // `likeOnly` coercion still wins over the default.
    let note_id = alice_note(
        &state,
        &alice_token,
        serde_json::json!({ "text": "n5", "reactionAcceptance": "likeOnly" }),
    )
    .await;
    heart_click(&state, &bob_token, &note_id, Value::Null).await;
    assert_eq!(stored(&state, &bob_id, &note_id), "❤");
}
