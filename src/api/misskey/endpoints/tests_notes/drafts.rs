//! Endpoint tests_notes tests (drafts).
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
async fn drafts_crud_count_and_error_codes() {
    use crate::domain::notification::NotificationKind;

    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    // Auth: no credential → 401.
    let (status, value) = call(
        &state,
        "notes/drafts/create",
        serde_json::json!({ "text": "hi" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Create a plain draft.
    let (status, created) = call(
        &state,
        "notes/drafts/create",
        serde_json::json!({ "text": "hello drafts", "visibility": "public" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let draft_id = created["createdDraft"]["id"].as_str().unwrap().to_string();
    assert_eq!(created["createdDraft"]["text"], "hello drafts");
    assert_eq!(
        created["createdDraft"]["scheduledAt"],
        serde_json::Value::Null
    );
    assert_eq!(created["createdDraft"]["isActuallyScheduled"], false);
    assert_eq!(created["createdDraft"]["userId"], alice_id);

    // List + count.
    let (status, list) = call(
        &state,
        "notes/drafts/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list.as_array().unwrap().len(), 1);
    let (status, count) = call(
        &state,
        "notes/drafts/count",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{count}");
    assert_eq!(count, 1);

    // Update merges.
    let (status, updated) = call(
        &state,
        "notes/drafts/update",
        serde_json::json!({ "draftId": draft_id, "text": "edited" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["updatedDraft"]["text"], "edited");
    // Unknown draft → NO_SUCH_NOTE_DRAFT.
    let (status, value) = call(
        &state,
        "notes/drafts/update",
        serde_json::json!({ "draftId": draft_id, "text": "x" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE_DRAFT");

    // Schedule gates carry the exact upstream codes.
    let (status, value) = call(
        &state,
        "notes/drafts/create",
        serde_json::json!({ "text": "sched", "isActuallyScheduled": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "SCHEDULED_AT_REQUIRED");
    let (status, value) = call(
        &state,
        "notes/drafts/create",
        serde_json::json!({
            "text": "sched",
            "scheduledAt": clock.now_ms(),
            "isActuallyScheduled": true,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "SCHEDULED_AT_MUST_BE_IN_FUTURE");

    // One scheduled draft fits the default cap of 1...
    let (status, scheduled) = call(
        &state,
        "notes/drafts/create",
        serde_json::json!({
            "text": "future",
            "scheduledAt": clock.now_ms() + 60_000,
            "isActuallyScheduled": true,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{scheduled}");
    let scheduled_id = scheduled["createdDraft"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    // ...the second is rejected with the exact upstream code.
    let (status, value) = call(
        &state,
        "notes/drafts/create",
        serde_json::json!({
            "text": "future 2",
            "scheduledAt": clock.now_ms() + 120_000,
            "isActuallyScheduled": true,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "TOO_MANY_SCHEDULED_NOTES");

    // Delete: foreign drafts are ACCESS_DENIED, missing ones are
    // NO_SUCH_NOTE_DRAFT, success is 204.
    let (status, value) = call(
        &state,
        "notes/drafts/delete",
        serde_json::json!({ "draftId": draft_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["code"], "ACCESS_DENIED");
    let (status, value) = call(
        &state,
        "notes/drafts/delete",
        serde_json::json!({ "draftId": "did00000000000001" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE_DRAFT");
    let (status, _) = call(
        &state,
        "notes/drafts/delete",
        serde_json::json!({ "draftId": draft_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // The scheduled draft posts through the worker once due.
    clock.advance_ms(61_000);
    state.worker.drain(100).unwrap();
    let (status, list) = call(
        &state,
        "notes/drafts/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert!(list.as_array().unwrap().is_empty());
    let (status, count) = call(
        &state,
        "notes/drafts/count",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{count}");
    assert_eq!(count, 0);
    // The posted note is visible on the author's timeline.
    let (status, notes) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert_eq!(notes.as_array().unwrap().len(), 1);
    assert_eq!(notes[0]["text"], "future");
    // ...and the scheduling notification exists (findable by kind).
    let (status, notifs) = call(
        &state,
        "i/notifications",
        serde_json::json!({
            "markAsRead": false,
            "includeTypes": ["scheduledNotePosted"],
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notifs}");
    assert_eq!(notifs.as_array().unwrap().len(), 1);
    assert_eq!(notifs[0]["type"], "scheduledNotePosted");
    assert!(notifs[0].get("note").is_some());
    let _ = (scheduled_id, NotificationKind::ScheduledNotePosted);
}

#[tokio::test]
async fn pin_unpin_flows_and_errors() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // Auth: no credential → 401.
    let (status, _) = call(&state, "i/pin", serde_json::json!({ "noteId": "x" }), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Alice posts a note; Bob posts one too.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "pin me" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "bob note" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let bob_note = created["createdNote"]["id"].as_str().unwrap().to_string();

    // Pinning a foreign note is NO_SUCH_NOTE (must own the note).
    let (status, value) = call(
        &state,
        "i/pin",
        serde_json::json!({ "noteId": bob_note }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");

    // Pin works and packs into MeDetailed.
    let (status, me) = call(
        &state,
        "i/pin",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["pinnedNoteIds"][0], note_id);
    assert_eq!(me["pinnedNotes"][0]["id"], note_id);
    assert_eq!(me["pinnedNotes"][0]["text"], "pin me");

    // Duplicate pins conflict with the exact upstream code.
    let (status, value) = call(
        &state,
        "i/pin",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_PINNED");

    // Fill to the pin limit of 5, then exceed it.
    let mut pinned = vec![note_id.clone()];
    for n in 0..4 {
        let (status, created) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": format!("note {n}") }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let id = created["createdNote"]["id"].as_str().unwrap().to_string();
        let (status, _) = call(
            &state,
            "i/pin",
            serde_json::json!({ "noteId": id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        pinned.push(id);
    }
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "one too many" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let sixth = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, value) = call(
        &state,
        "i/pin",
        serde_json::json!({ "noteId": sixth }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "PIN_LIMIT_EXCEEDED");

    // Unpin removes the id; unpinning again is a no-op success.
    let (status, me) = call(
        &state,
        "i/unpin",
        serde_json::json!({ "noteId": pinned[0] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["pinnedNoteIds"].as_array().unwrap().len(), 4);
    let (status, _) = call(
        &state,
        "i/unpin",
        serde_json::json!({ "noteId": pinned[0] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Unpinning a foreign note is NO_SUCH_NOTE.
    let (status, value) = call(
        &state,
        "i/unpin",
        serde_json::json!({ "noteId": bob_note }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
    let _ = (alice_id, bob_id);
}

#[tokio::test]
async fn featured_endpoints_return_ranked_notes() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // Alice posts two notes; Bob reacts to both and renotes one.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "first note" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let first = created["createdNote"]["id"].as_str().unwrap().to_string();
    clock.advance_ms(1_000);
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "second note" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let second = created["createdNote"]["id"].as_str().unwrap().to_string();
    for target in [&first, &second] {
        let (status, _) = call(
            &state,
            "notes/reactions/create",
            serde_json::json!({ "noteId": target, "reaction": "❤" }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "renoteId": second }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let rt = created["createdNote"]["id"].as_str().unwrap().to_string();

    // Global featured returns all three notes newest-first (the pure
    // renote is itself a recent public note with no sensitivity flags,
    // so it ranks too).
    let (status, featured) = call(
        &state,
        "notes/featured",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{featured}");
    let ids: Vec<&str> = featured
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&first.as_str()) && ids.contains(&second.as_str()));
    assert_eq!(featured[0]["id"], rt, "newest first");
    assert_eq!(featured[1]["id"], second);
    assert_eq!(featured[2]["id"], first);
    // Anonymous callers are served too; unknown params are rejected.
    let (status, anon) = call(&state, "notes/featured", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{anon}");
    assert_eq!(anon.as_array().unwrap().len(), ids.len());
    let (status, value) = call(
        &state,
        "notes/featured",
        serde_json::json!({ "bogus": 1 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "UNSUPPORTED_FIELD");
    // A non-null channelId ranks empty (no channels exist).
    let (status, empty) = call(
        &state,
        "notes/featured",
        serde_json::json!({ "channelId": "ch1" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{empty}");
    assert!(empty.as_array().unwrap().is_empty());
    // `untilId` pages newest-first.
    let (status, page) = call(
        &state,
        "notes/featured",
        serde_json::json!({ "untilId": second }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page.as_array().unwrap().len(), 1);
    assert_eq!(page[0]["id"], first);

    // Per-user featured scopes to the author; unknown users rank empty.
    let (status, user_featured) = call(
        &state,
        "users/featured-notes",
        serde_json::json!({ "userId": alice_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{user_featured}");
    assert_eq!(user_featured.as_array().unwrap().len(), 2);
    let (status, bob_featured) = call(
        &state,
        "users/featured-notes",
        serde_json::json!({ "userId": bob_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bob_featured}");
    assert_eq!(bob_featured.as_array().unwrap().len(), 1);
    let (status, ghost) = call(
        &state,
        "users/featured-notes",
        serde_json::json!({ "userId": "aid00000000000099" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ghost}");
    assert!(ghost.as_array().unwrap().is_empty());
    // Missing userId is INVALID_PARAM.
    let (status, value) = call(&state, "users/featured-notes", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
}
