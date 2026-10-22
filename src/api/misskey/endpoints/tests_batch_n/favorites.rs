//! Endpoint tests_batch_n tests (favorites).
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
async fn batch_n_favorites_crud_state_pack_and_errors() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    // Alice posts; Bob favorites it.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "fav target" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();

    let (status, _) = call(
        &state,
        "notes/favorites/create",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Duplicate → ALREADY_FAVORITED with the upstream UUID.
    let (status, value) = call(
        &state,
        "notes/favorites/create",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_FAVORITED");
    assert_eq!(value["error"]["id"], "a402c12b-34dd-41d2-9d8d-4d2ffd96a1a6");

    // notes/show packs isFavorited for the favoriter only.
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["isFavorited"], true);
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["isFavorited"], false);

    // notes/state shape.
    let (status, st) = call(
        &state,
        "notes/state",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{st}");
    assert_eq!(
        st,
        serde_json::json!({ "isFavorited": true, "isMutedThread": false })
    );

    // i/favorites lists the note.
    let (status, favs) = call(
        &state,
        "i/favorites",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{favs}");
    assert!(
        favs.as_array().unwrap().iter().any(|n| n["id"] == note_id),
        "{favs:?}"
    );

    // Unfavorite; second delete → NOT_FAVORITED with the upstream UUID.
    let (status, _) = call(
        &state,
        "notes/favorites/delete",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "notes/favorites/delete",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NOT_FAVORITED");
    assert_eq!(value["error"]["id"], "b625fc69-635e-4e89-baa6-dbefbef35af5");

    // Unknown note → NO_SUCH_NOTE on both paths.
    let (status, value) = call(
        &state,
        "notes/favorites/create",
        serde_json::json!({ "noteId": "aaaaaaaaaaaaaaaa" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
    let (status, value) = call(
        &state,
        "notes/state",
        serde_json::json!({ "noteId": "aaaaaaaaaaaaaaaa" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");

    // Invisible (followers-only, strangers) note → NO_SUCH_NOTE on create.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "locked", "visibility": "followers" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let priv_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, value) = call(
        &state,
        "notes/favorites/create",
        serde_json::json!({ "noteId": priv_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
    let _ = alice_id;
}
