//! Endpoint tests_batch_n tests (antennas).
//!
//! Split from `endpoints/tests_batch_n.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::tests_common::{
    call, create_note, export_completed_notifications, read_export_bytes, signup_user, test_state,
};
use super::super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::application::ports::PortError;
use crate::domain::id::Clock;
use axum::http::StatusCode;
use serde_json::Value;
use std::sync::Arc;

#[tokio::test]
async fn batch_n_antennas_crud_notes_and_errors() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // Bad src → INVALID_PARAM.
    let (status, value) = call(
        &state,
        "antennas/create",
        serde_json::json!({
            "name": "x", "src": "nope", "keywords": [["x"]],
            "excludeKeywords": [], "users": [],
            "caseSensitive": false, "withReplies": true, "withFile": false,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");

    // Empty keywords → EMPTY_KEYWORD (verbatim id).
    let (status, value) = call(
        &state,
        "antennas/create",
        serde_json::json!({
            "name": "x", "src": "all", "keywords": [[" "]], "excludeKeywords": [],
            "users": [], "caseSensitive": false, "withReplies": true, "withFile": false,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "EMPTY_KEYWORD");
    assert_eq!(value["error"]["id"], "53ee222e-1ddd-4f9a-92e5-9fb82ddb463a");

    // src=list with an unknown list: NO_SUCH_USER_LIST (antennas
    // endpoint id).
    let (status, value) = call(
        &state,
        "antennas/create",
        serde_json::json!({
            "name": "x", "src": "list", "userListId": "aaaaaaaaaaaaaaaa",
            "keywords": [["x"]], "excludeKeywords": [], "users": [],
            "caseSensitive": false, "withReplies": true, "withFile": false,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER_LIST");

    // Happy path: keyword antenna over everything.
    let (status, packed) = call(
        &state,
        "antennas/create",
        serde_json::json!({
            "name": "kw", "src": "all", "keywords": [["hello"]],
            "excludeKeywords": [["spam"]], "users": [],
            "caseSensitive": false, "withReplies": false, "withFile": false,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{packed}");
    let antenna_id = packed["id"].as_str().unwrap().to_string();
    assert_eq!(packed["src"], "all");
    assert_eq!(packed["hasUnreadNote"], false);
    assert_eq!(packed["isActive"], true);

    // Cap: 4 more succeed, the 6th refuses with TOO_MANY_ANTENNAS.
    for i in 0..4 {
        let (status, _) = call(
            &state,
            "antennas/create",
            serde_json::json!({
                "name": format!("extra{i}"), "src": "all", "keywords": [["z"]],
                "excludeKeywords": [], "users": [],
                "caseSensitive": false, "withReplies": true, "withFile": false,
            }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{i}");
    }
    let (status, value) = call(
        &state,
        "antennas/create",
        serde_json::json!({
            "name": "toomany", "src": "all", "keywords": [["z"]],
            "excludeKeywords": [], "users": [],
            "caseSensitive": false, "withReplies": true, "withFile": false,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "TOO_MANY_ANTENNAS");
    assert_eq!(value["error"]["id"], "faf47050-e8b5-438c-913c-db2b1576fde4");

    // Show + list.
    let (status, shown) = call(
        &state,
        "antennas/show",
        serde_json::json!({ "antennaId": antenna_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["name"], "kw");
    let (status, rows) = call(
        &state,
        "antennas/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows.as_array().unwrap().len(), 5, "{rows:?}");
    // Foreign viewers see nothing (owner-scoped).
    let (status, value) = call(
        &state,
        "antennas/show",
        serde_json::json!({ "antennaId": antenna_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_ANTENNA");
    assert_eq!(value["error"]["id"], "10c673ac-8852-48eb-aa1f-f5b67f069290");

    // Update: rename + narrow to users-source.
    let (status, updated) = call(
        &state,
        "antennas/update",
        serde_json::json!({ "antennaId": antenna_id, "name": "kw2", "src": "users", "users": ["bob"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["name"], "kw2");
    assert_eq!(updated["src"], "users");

    // Notes flow is covered below against a fresh src=all antenna; first
    // reset this one and check the empty feed + remove-note on empty.
    let (status, notes) = call(
        &state,
        "antennas/notes",
        serde_json::json!({ "antennaId": antenna_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert_eq!(notes.as_array().unwrap().len(), 0, "{notes:?}");
    let (status, _) = call(
        &state,
        "antennas/remove-note",
        serde_json::json!({ "antennaId": antenna_id, "noteId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Feed: a fresh match-all antenna collects a matching note through
    // the worker (note-create transaction enqueues the match job).
    // (An empty keyword list is EMPTY_KEYWORD at the API — like
    // upstream — so the antenna carries a real keyword.)
    let (status, packed) = call(
        &state,
        "antennas/create",
        serde_json::json!({
            "name": "all2", "src": "all", "keywords": [["hello"]],
            "excludeKeywords": [], "users": [],
            "caseSensitive": false, "withReplies": true, "withFile": false,
        }),
        Some(&bob_token),
    )
    .await;
    // Bob already has 0 antennas; this is fine — but Alice is at cap, so
    // use Bob's account for the feed antenna.
    assert_eq!(status, StatusCode::OK, "{packed}");
    let feed_id = packed["id"].as_str().unwrap().to_string();
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "hello world" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    state.worker.drain(200).unwrap();
    let (status, notes) = call(
        &state,
        "antennas/notes",
        serde_json::json!({ "antennaId": feed_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert!(
        notes.as_array().unwrap().iter().any(|n| n["id"] == note_id),
        "{notes:?}"
    );
    // Read-time visibility filtering: a followers-only note from a
    // stranger stays out of Bob's feed.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "locked hello", "visibility": "followers" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let locked_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    state.worker.drain(200).unwrap();
    let (status, notes) = call(
        &state,
        "antennas/notes",
        serde_json::json!({ "antennaId": feed_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert!(
        !notes
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["id"] == locked_id),
        "{notes:?}"
    );
    // remove-note drops the visible one.
    let (status, _) = call(
        &state,
        "antennas/remove-note",
        serde_json::json!({ "antennaId": feed_id, "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, notes) = call(
        &state,
        "antennas/notes",
        serde_json::json!({ "antennaId": feed_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert!(
        !notes.as_array().unwrap().iter().any(|n| n["id"] == note_id),
        "{notes:?}"
    );

    // Delete cascades; the feed is gone afterwards.
    let (status, _) = call(
        &state,
        "antennas/delete",
        serde_json::json!({ "antennaId": feed_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "antennas/notes",
        serde_json::json!({ "antennaId": feed_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_ANTENNA");
    let _ = (alice_id, bob_id);
}

#[tokio::test]
async fn batch_n_word_mutes_soft_flag_packed_hard_dropped() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    // Alice: soft /spoiler/i, hard /secret/i (i/update transport is the
    // Misskey `/pattern/flags` string form).
    let (status, _) = call(
        &state,
        "i/update",
        serde_json::json!({
            "mutedWords": ["/spoiler/i"],
            "hardMutedWords": ["/secret/i"],
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Bob posts a soft hit, a hard hit, and a plain note.
    let (status, soft) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "a spoiler here" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{soft}");
    let soft_id = soft["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, hard) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "a secret here" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hard}");
    let hard_id = hard["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, plain) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "just a note" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{plain}");
    let plain_id = plain["createdNote"]["id"].as_str().unwrap().to_string();

    // Soft hit packs the viewer flag; hard hit reads as missing.
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": soft_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["isSoftMuted"], true);
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": plain_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["isSoftMuted"], false);
    let (status, value) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": hard_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");

    // Bob (no mutes) sees everything unflagged.
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": soft_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["isSoftMuted"], false);
}

#[tokio::test]
async fn batch_n_import_antennas_applies_valid_rows() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let valid = serde_json::json!([
        {
            "name": "a",
            "src": "all",
            "keywords": [["x"]],
            "excludeKeywords": [],
            "users": [],
            "userListAccts": null,
            "caseSensitive": false,
            "localOnly": false,
            "excludeBots": false,
            "withReplies": true,
            "withFile": false,
        },
        // Empty-keyword rows skip (upstream verbatim rule).
        {
            "name": "empty",
            "src": "all",
            "keywords": [[""]],
            "excludeKeywords": [],
            "users": [],
            "caseSensitive": false,
            "localOnly": false,
            "excludeBots": false,
            "withReplies": true,
            "withFile": false,
        },
        // `src=list` + accts rewrites to `users` (upstream parity).
        {
            "name": "listed",
            "src": "list",
            "keywords": [["y"]],
            "excludeKeywords": [],
            "users": [],
            "userListAccts": ["bob@example.com"],
            "caseSensitive": false,
            "localOnly": false,
            "excludeBots": false,
            "withReplies": true,
            "withFile": false,
        },
        // Schema-invalid rows skip.
        { "name": "broken" },
    ]);
    let file = state
        .services
        .upload_media(
            &alice_id,
            "antennas.json".to_string(),
            &serde_json::to_vec(&valid).unwrap(),
            None,
            false,
            true,
        )
        .unwrap();
    let (status, _) = call(
        &state,
        "i/import-antennas",
        serde_json::json!({ "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // The worker applies 2 rows (a, listed), skips 2 (empty, broken);
    // imports never notify.
    state.worker.drain(100).unwrap();
    let (status, rows) = call(
        &state,
        "antennas/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    let names: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"a"), "{rows:?}");
    assert!(names.contains(&"listed"), "{rows:?}");
    assert_eq!(rows.as_array().unwrap().len(), 2, "{rows:?}");
    let listed = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "listed")
        .unwrap();
    assert_eq!(listed["src"], "users");
    assert_eq!(listed["users"], serde_json::json!(["bob@example.com"]));

    // Malformed JSON is INVALID_PARAM (DIVERGENCE: upstream throws 500).
    clock.advance_ms(3_600_000);
    let bad = state
        .services
        .upload_media(
            &alice_id,
            "bad.json".to_string(),
            b"not json",
            None,
            false,
            true,
        )
        .unwrap();
    let (status, value) = call(
        &state,
        "i/import-antennas",
        serde_json::json!({ "fileId": bad.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
}
