//! Endpoint tests_batch_n tests (schedule).
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
async fn sharkey_schedule_shim_crud_and_errors() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // scheduleNote.scheduledAt is required and must be in the future.
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "text": "x" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let past = clock.now_ms() - 1_000;
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "text": "x", "scheduleNote": { "scheduledAt": past } }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CANNOT_CREATE_ALREADY_EXPIRED_SCHEDULE");
    assert_eq!(value["error"]["id"], "8a9bfb90-fc7e-4878-a3e8-d97faaf5fb07");

    // anyOf content rule.
    let future = clock.now_ms() + 3_600_000;
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "scheduleNote": { "scheduledAt": future } }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");

    // Up-front target errors carry verbatim Sharkey ids.
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "renoteId": "aaaaaaaaaaaaaaaa", "scheduleNote": { "scheduledAt": future } }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_RENOTE_TARGET");
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "replyId": "aaaaaaaaaaaaaaaa", "text": "x", "scheduleNote": { "scheduledAt": future } }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_REPLY_TARGET");
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "renoteId": "aaaaaaaaaaaaaaaa", "scheduleNote": { "scheduledAt": future } }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_RENOTE_TARGET");

    // Pure-renote gates.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "target" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let target_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "renoteId": target_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let pure_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "renoteId": pure_id, "scheduleNote": { "scheduledAt": future } }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CANNOT_RENOTE_TO_A_PURE_RENOTE");
    assert_eq!(value["error"]["id"], "fd4cc33e-2a37-48dd-99cc-9b806eb2031a");
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "replyId": pure_id, "text": "x", "scheduleNote": { "scheduledAt": future } }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CANNOT_REPLY_TO_A_PURE_RENOTE");
    assert_eq!(value["error"]["id"], "3ac74a84-8fd5-4bb0-870f-01804f82ce15");

    // Renote visibility gate.
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "followers only", "visibility": "followers" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let followers_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "renoteId": followers_id, "scheduleNote": { "scheduledAt": future } }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CANNOT_RENOTE_DUE_TO_VISIBILITY");

    // Poll expiry is judged against the scheduled time, not now.
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({
            "text": "x",
            "poll": { "choices": ["a", "b"], "expiresAt": future - 1 },
            "scheduleNote": { "scheduledAt": future },
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CANNOT_CREATE_ALREADY_EXPIRED_POLL");

    // Happy path: full shape + defaults, packed schedule-note shape.
    // `mediaIds` is accepted as the file-key form (upstream parity;
    // `fileIds` wins when both are present).
    let media = state
        .services
        .upload_media(
            &alice_id,
            "sched.png".to_string(),
            b"fakepng",
            None,
            false,
            true,
        )
        .expect("upload");
    let (status, packed) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({
            "text": "sched hello",
            "visibility": "public",
            "mediaIds": [media.id],
            "scheduleNote": { "scheduledAt": future },
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{packed}");
    assert_eq!(packed["userId"], alice_id);
    assert!(
        packed["scheduledAt"].as_str().unwrap().ends_with('Z'),
        "{packed:?}"
    );
    assert_eq!(packed["note"]["text"], "sched hello");
    assert_eq!(packed["note"]["isSchedule"], true);
    assert_eq!(packed["note"]["visibility"], "public");
    assert_eq!(packed["note"]["reactionAcceptance"], Value::Null);
    assert!(
        packed["note"]["visibleUsers"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        packed["note"]["fileIds"].as_array().unwrap().len(),
        1,
        "{packed:?}"
    );
    let schedule_id = packed["id"].as_str().unwrap().to_string();

    // Second scheduled entry overflows the default limit of 1.
    let (status, value) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "text": "y", "scheduleNote": { "scheduledAt": future } }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "SCHEDULE_NOTE_MAX");
    assert_eq!(value["error"]["id"], "168707c3-e7da-4031-989e-f42aa3a274b2");

    // List shows the entry in the upstream shape.
    let (status, rows) = call(
        &state,
        "notes/schedule/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows.as_array().unwrap().len(), 1, "{rows:?}");
    assert_eq!(rows[0]["id"], schedule_id);

    // Delete: unknown id → NO_SUCH_NOTE (a58056ba); foreign → 403.
    let (status, value) = call(
        &state,
        "notes/schedule/delete",
        serde_json::json!({ "noteId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
    assert_eq!(value["error"]["id"], "a58056ba-8ba1-4323-8ebf-e0b585bc244f");
    let (status, value) = call(
        &state,
        "notes/schedule/delete",
        serde_json::json!({ "noteId": schedule_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["code"], "PERMISSION_DENIED");
    assert_eq!(value["error"]["id"], "c0da2fed-8f61-4c47-a41d-431992607b5c");

    // Delete then advance past the time: nothing posts (job cleared).
    let (status, _) = call(
        &state,
        "notes/schedule/delete",
        serde_json::json!({ "noteId": schedule_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    clock.advance_ms(3_601_000);
    state.worker.drain(100).unwrap();
    let (status, rows) = call(
        &state,
        "notes/schedule/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert!(rows.as_array().unwrap().is_empty(), "{rows:?}");

    // End to end: a shim-created entry publishes through the normal path.
    let fire_at = clock.now_ms() + 60_000;
    let (status, packed) = call(
        &state,
        "notes/schedule/create",
        serde_json::json!({ "text": "fires", "scheduleNote": { "scheduledAt": fire_at } }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{packed}");
    state.worker.drain(100).unwrap();
    let (status, mine) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{mine}");
    assert!(
        !mine
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["text"] == "fires"),
        "not yet due"
    );
    clock.advance_ms(61_000);
    state.worker.drain(100).unwrap();
    let (status, mine) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{mine}");
    assert!(
        mine.as_array()
            .unwrap()
            .iter()
            .any(|n| n["text"] == "fires"),
        "{mine:?}"
    );
    let _ = bob_id;
}
