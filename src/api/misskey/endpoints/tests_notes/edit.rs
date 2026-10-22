//! Endpoint tests_notes tests (edit).
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
async fn parity_notes_edit_endpoint_gates() {
    let (state, _) = test_state();
    let (_alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_bob_id, bob_token) = signup_user(&state, "bob").await;

    // Missing and empty `editId` are NEEDS_EDIT_ID (exact UUID), not
    // INVALID_PARAM (Sharkey error-set parity).
    for body in [
        serde_json::json!({ "text": "x" }),
        serde_json::json!({ "editId": "", "text": "x" }),
    ] {
        let (status, value) = call(&state, "notes/edit", body, Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NEEDS_EDIT_ID");
        assert_eq!(value["error"]["id"], "d697edc8-8c73-4de8-bded-35fd198b79e5");
    }
    // Unknown id → NO_SUCH_NOTE.
    let (status, value) = call(
        &state,
        "notes/edit",
        serde_json::json!({ "editId": "aid00000000000099", "text": "x" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
    assert_eq!(value["error"]["id"], "24fcbfc6-2e37-42b6-8388-c29b3861a08d");

    // Alice's note: bob is not the author.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "original" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, value) = call(
        &state,
        "notes/edit",
        serde_json::json!({ "editId": note_id, "text": "hijack" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "YOU_ARE_NOT_THE_AUTHOR");
    assert_eq!(value["error"]["id"], "c6e61685-411d-43d0-b90a-a448d2539001");

    // Unknown fields are UNSUPPORTED_FIELD (same `check_allowed_fields`
    // rule as `notes/create`).
    let (status, value) = call(
        &state,
        "notes/edit",
        serde_json::json!({ "editId": note_id, "bogus": 1 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "UNSUPPORTED_FIELD");

    // Successful edit returns `{createdNote}` with the new text and a
    // non-null `updatedAt`; `notes/show` reflects it.
    let (status, edited) = call(
        &state,
        "notes/edit",
        serde_json::json!({ "editId": note_id, "text": "revised" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(
        edited["createdNote"]["id"].as_str().unwrap(),
        note_id.as_str()
    );
    assert_eq!(edited["createdNote"]["text"], serde_json::json!("revised"));
    assert!(edited["createdNote"]["updatedAt"].is_string(), "{edited}");
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["text"], serde_json::json!("revised"));
    assert!(shown["updatedAt"].is_string(), "{shown}");
}

#[tokio::test]
async fn parity_notes_edit_noop_and_history() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "v1" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    assert!(created["createdNote"]["updatedAt"].is_null());

    // Identical-text edit: `updatedAt` stays null, no history row, and
    // the endpoint's side-effect jobs are dropped (pending unchanged).
    let before = state.services.jobs.pending_count().unwrap();
    let (status, noop) = call(
        &state,
        "notes/edit",
        serde_json::json!({ "editId": note_id, "text": "v1" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{noop}");
    assert!(noop["createdNote"]["updatedAt"].is_null(), "{noop}");
    assert!(
        state
            .services
            .notes
            .note_edits(&note_id)
            .unwrap()
            .is_empty(),
        "no history row on no-op"
    );
    assert_eq!(
        state.services.jobs.pending_count().unwrap(),
        before,
        "no jobs enqueued on no-op"
    );

    // Two effective edits chain two history rows through the endpoint path.
    for text in ["v2", "v3"] {
        let (status, edited) = call(
            &state,
            "notes/edit",
            serde_json::json!({ "editId": note_id, "text": text }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{edited}");
        assert!(edited["createdNote"]["updatedAt"].is_string(), "{edited}");
    }
    let rows = state.services.notes.note_edits(&note_id).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].new_text.as_deref(), Some("v2"));
    assert_eq!(rows[1].old_text.as_deref(), Some("v2"));
    assert_eq!(rows[1].new_text.as_deref(), Some("v3"));
    assert_eq!(rows[1].old_date_ms, rows[0].updated_at_ms);
}

#[tokio::test]
async fn parity_notes_edit_reply_immutable_visibility_forced() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    // Bob posts the parent; alice replies (public, non-local-only).
    let (status, parent) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "parent" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{parent}");
    let parent_id = parent["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, reply) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "reply v1", "replyId": parent_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let reply_id = reply["createdNote"]["id"].as_str().unwrap().to_string();

    // A second valid note to offer as the (discarded) new parent.
    let (status, other) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "other" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{other}");
    let other_id = other["createdNote"]["id"].as_str().unwrap().to_string();

    // The edit succeeds but keeps the original parent, visibility, and
    // localOnly (Sharkey forced-values parity).
    let (status, edited) = call(
        &state,
        "notes/edit",
        serde_json::json!({
            "editId": reply_id,
            "replyId": other_id,
            "text": "reply v2",
            "visibility": "followers",
            "localOnly": true,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(edited["createdNote"]["text"], serde_json::json!("reply v2"));
    let stored = state
        .services
        .notes
        .get_note(&reply_id)
        .unwrap()
        .expect("reply stored");
    assert_eq!(
        stored.reply_id.as_deref(),
        Some(parent_id.as_str()),
        "reply parent is immutable"
    );
    assert_eq!(
        stored.visibility,
        Visibility::Public,
        "visibility forced back to the original"
    );
    assert!(!stored.local_only, "localOnly forced back to the original");
}

#[tokio::test]
async fn parity_notes_edit_poll_resets_votes() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    // Alice posts a note with a live poll; bob votes choice 0.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({
            "text": "pick one",
            "poll": { "choices": ["a", "b"], "expiredAfter": 3_600_000 },
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, _) = call(
        &state,
        "notes/polls/vote",
        serde_json::json!({ "noteId": note_id, "choice": 0 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Alice replaces the poll: tallies zero out for the new choices.
    let (status, edited) = call(
        &state,
        "notes/edit",
        serde_json::json!({
            "editId": note_id,
            "text": "pick one",
            "poll": { "choices": ["x", "y", "z"], "expiredAfter": 7_200_000 },
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    for (i, choice) in shown["poll"]["choices"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        assert_eq!(choice["votes"], serde_json::json!(0), "choice {i} zeroed");
        assert_eq!(choice["isVoted"], serde_json::json!(false));
    }

    // The old vote no longer blocks: bob can vote again...
    let (status, _) = call(
        &state,
        "notes/polls/vote",
        serde_json::json!({ "noteId": note_id, "choice": 0 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // ...but a removed choice index is INVALID_CHOICE (exact UUID).
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

    // An identical-poll edit preserves the new tally (no reset).
    let (status, edited) = call(
        &state,
        "notes/edit",
        serde_json::json!({
            "editId": note_id,
            "text": "pick one",
            "poll": { "choices": ["x", "y", "z"], "expiredAfter": 7_200_000 },
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["poll"]["choices"][0]["votes"], serde_json::json!(1));
}

#[tokio::test]
async fn parity_notes_edit_edited_notification() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    // Bob posts; alice replies; alice edits her reply → bob is notified.
    let (status, parent) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "parent" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{parent}");
    let parent_id = parent["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, reply) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "reply v1", "replyId": parent_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let reply_id = reply["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, _) = call(
        &state,
        "notes/edit",
        serde_json::json!({ "editId": reply_id, "text": "reply v2" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    state.worker.drain(100).unwrap();
    let (status, notifs) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "includeTypes": ["edited"] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notifs}");
    let notifs = notifs.as_array().unwrap();
    assert_eq!(notifs.len(), 1, "{notifs:?}");
    assert_eq!(notifs[0]["type"], serde_json::json!("edited"));
    assert_eq!(notifs[0]["note"]["id"].as_str().unwrap(), reply_id.as_str());

    // Alice edits her own non-reply note → no `edited` notification.
    let (status, own) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "solo" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{own}");
    let own_id = own["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, _) = call(
        &state,
        "notes/edit",
        serde_json::json!({ "editId": own_id, "text": "solo v2" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Self-reply edit: alice replies to her own note and edits it →
    // still no notification (Sharkey `postNoteEdited` parity: the
    // `Notify` worker gate drops self-notifies).
    let (status, self_reply) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "self reply", "replyId": own_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{self_reply}");
    let self_reply_id = self_reply["createdNote"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, _) = call(
        &state,
        "notes/edit",
        serde_json::json!({ "editId": self_reply_id, "text": "self reply v2" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    state.worker.drain(100).unwrap();
    let (status, bob_notifs) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "includeTypes": ["edited"] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bob_notifs}");
    assert_eq!(
        bob_notifs.as_array().unwrap().len(),
        1,
        "bob keeps only the first edit notification: {bob_notifs}"
    );
    let (status, alice_notifs) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "includeTypes": ["edited"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{alice_notifs}");
    assert!(
        alice_notifs.as_array().unwrap().is_empty(),
        "alice has no edited notifications: {alice_notifs}"
    );
}

#[tokio::test]
async fn parity_reject_quotes_end_to_end() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;

    // Alice posts a note for bob to quote; bob posts the target for
    // alice's and carol's quotes.
    let (status, alice_note) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "alice note" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{alice_note}");
    let alice_note_id = alice_note["createdNote"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, target) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "quotable" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{target}");
    let target_id = target["createdNote"]["id"].as_str().unwrap().to_string();

    // (a) Self-service toggle: alice opts out, quotes fail with the
    // exact Sharkey code; opting back in restores quoting.
    let (status, me) = call(
        &state,
        "i/update",
        serde_json::json!({ "rejectQuotes": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["rejectQuotes"], serde_json::json!(true));
    let (status, me) = call(&state, "i", serde_json::json!({}), Some(&alice_token)).await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["rejectQuotes"], serde_json::json!(true));
    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "renoteId": target_id, "text": "quoting" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "QUOTE_DISABLED_FOR_USER");
    assert_eq!(value["error"]["id"], "1c0ea108-d1e3-4e8e-aa3f-4d2487626153");
    let (status, me) = call(
        &state,
        "i/update",
        serde_json::json!({ "rejectQuotes": false }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["rejectQuotes"], serde_json::json!(false));
    let (status, quote) = call(
        &state,
        "notes/create",
        serde_json::json!({ "renoteId": target_id, "text": "quoting" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{quote}");

    // (b) Moderation toggle: bob quotes alice's note BEFORE the flag is
    // set. Batch M WP1: a plain user cannot call `admin/reject-quotes`
    // (403, no state change), a moderator can (and the action writes a
    // Sharkey-shaped moderation-log row in the same commit), and a
    // moderator cannot act on another moderator (WP3 refusal rule).
    let (status, bob_quote) = call(
        &state,
        "notes/create",
        serde_json::json!({ "renoteId": alice_note_id, "text": "bob quoting" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bob_quote}");
    let bob_quote_id = bob_quote["createdNote"]["id"].as_str().unwrap().to_string();

    // Plain user: refused, nothing changes.
    let (status, value) = call(
        &state,
        "admin/reject-quotes",
        serde_json::json!({ "userId": bob_id, "rejectQuotes": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["code"], "PERMISSION_DENIED");
    assert!(
        !state
            .services
            .accounts
            .find_by_id(&bob_id)
            .unwrap()
            .unwrap()
            .reject_quotes,
        "a refused call must not change state"
    );

    // Make alice a moderator (the CLI `set-role` path) and retry.
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("set role");
    let (status, packed) = call(
        &state,
        "admin/reject-quotes",
        serde_json::json!({ "userId": bob_id, "rejectQuotes": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{packed}");
    assert_eq!(packed["rejectQuotes"], serde_json::json!(true));
    // The moderation-log row is in the same commit: it is readable
    // through `admin/show-moderation-logs` immediately.
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "type": "rejectQuotesUser" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    let rows = logs.as_array().unwrap();
    assert_eq!(rows.len(), 1, "one log row: {logs}");
    assert_eq!(rows[0]["type"], "rejectQuotesUser");
    assert_eq!(rows[0]["userId"], serde_json::json!(alice_id));
    assert_eq!(rows[0]["user"]["username"], "alice");
    assert_eq!(rows[0]["info"]["userId"], serde_json::json!(bob_id));
    assert_eq!(rows[0]["info"]["userUsername"], "bob");

    // WP3 refusal rule: a moderator cannot act on a moderator.
    state
        .services
        .set_role(None, &carol_id, false, true)
        .expect("set role");
    let (status, value) = call(
        &state,
        "admin/reject-quotes",
        serde_json::json!({ "userId": carol_id, "rejectQuotes": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert!(
        !state
            .services
            .accounts
            .find_by_id(&carol_id)
            .unwrap()
            .unwrap()
            .reject_quotes,
        "a refused call must not change state"
    );

    // New quotes fail...
    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "renoteId": alice_note_id, "text": "again" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "QUOTE_DISABLED_FOR_USER");
    assert_eq!(value["error"]["id"], "1c0ea108-d1e3-4e8e-aa3f-4d2487626153");
    // ...and so does editing the pre-flag quote while keeping its
    // quote shape (`notes/edit.ts` §3 wiring: the effective shape is
    // still a quote, so the quoter's flag rejects it).
    let (status, value) = call(
        &state,
        "notes/edit",
        serde_json::json!({
            "editId": bob_quote_id,
            "renoteId": alice_note_id,
            "text": "bob quoting v2",
        }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "QUOTE_DISABLED_FOR_USER");
    assert_eq!(value["error"]["id"], "1c0ea108-d1e3-4e8e-aa3f-4d2487626153");

    // (c) Missing or non-boolean `rejectQuotes` is INVALID_PARAM.
    for body in [
        serde_json::json!({ "userId": bob_id }),
        serde_json::json!({ "userId": bob_id, "rejectQuotes": "yes" }),
    ] {
        let (status, value) = call(&state, "admin/reject-quotes", body, Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM");
    }

    // (d) Control: carol is never flagged, so her quotes work throughout.
    let (status, quote) = call(
        &state,
        "notes/create",
        serde_json::json!({ "renoteId": target_id, "text": "carol quoting" }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{quote}");
}
