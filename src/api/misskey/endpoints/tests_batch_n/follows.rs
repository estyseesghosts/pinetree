//! Endpoint tests_batch_n tests (follows).
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
async fn batch_n_follow_request_accepted_emitted_and_filterable() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // Bob locks his account; Alice's follow becomes a pending request.
    let (status, _) = call(
        &state,
        "i/update",
        serde_json::json!({ "isLocked": true }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Bob accepts; the worker emits followRequestAccepted to Alice.
    let (alice_id, _) = {
        let (status, me) = call(&state, "i", serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::OK);
        (me["id"].as_str().unwrap().to_string(), ())
    };
    let (status, _) = call(
        &state,
        "following/requests/accept",
        serde_json::json!({ "userId": alice_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();
    let (status, notifs) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "limit": 20, "markAsRead": false, "includeTypes": ["followRequestAccepted"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notifs}");
    assert!(
        notifs
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["type"] == "followRequestAccepted"),
        "{notifs:?}"
    );
    // Excluding it hides it.
    let (status, notifs) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "limit": 20, "markAsRead": false, "excludeTypes": ["followRequestAccepted"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notifs}");
    assert!(
        notifs
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n["type"] != "followRequestAccepted"),
        "{notifs:?}"
    );
}
