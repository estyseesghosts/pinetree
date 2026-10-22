//! Endpoint tests_batch_n tests (admin).
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
async fn batch_n_admin_remainder_unset_media_files() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("moderator");
    state
        .services
        .set_role(None, &carol_id, true, false)
        .expect("admin");

    // Non-moderator cannot unset.
    let (status, _) = call(
        &state,
        "admin/unset-user-avatar",
        serde_json::json!({ "userId": bob_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Bob sets an avatar first.
    let png = [
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F,
        0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x62, 0x60,
        0x01, 0x00, 0x00, 0xFF, 0xFF, 0x03, 0x00, 0x00, 0x06, 0x00, 0x05, 0x72, 0x76, 0x01, 0x82,
        0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ];
    let file = state
        .services
        .upload_media(&bob_id, "a.png".to_string(), &png, None, false, true)
        .expect("upload");
    let (status, _) = call(
        &state,
        "i/update",
        serde_json::json!({ "avatarId": file.id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Moderator unsets → 204, avatar cleared, log row written.
    let (status, _) = call(
        &state,
        "admin/unset-user-avatar",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, shown) = call(
        &state,
        "users/show",
        serde_json::json!({ "userId": bob_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert!(
        shown.get("avatarId").is_none() || shown["avatarId"].is_null(),
        "{shown:?}"
    );
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "type": "unsetUserAvatar" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    assert_eq!(logs.as_array().unwrap().len(), 1, "{logs:?}");

    // Second unset is a no-op (no second row).
    let (status, _) = call(
        &state,
        "admin/unset-user-avatar",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "type": "unsetUserAvatar" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    assert_eq!(logs.as_array().unwrap().len(), 1, "{logs:?}");

    // Refusal rule on admins (DIVERGENCE from upstream's role-only gate).
    let (status, value) = call(
        &state,
        "admin/unset-user-avatar",
        serde_json::json!({ "userId": carol_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    // Unknown target → NO_SUCH_USER.
    let (status, value) = call(
        &state,
        "admin/unset-user-banner",
        serde_json::json!({ "userId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");

    // delete-all-files: moderator (non-admin) refused; admin clears.
    let (status, _) = call(
        &state,
        "admin/delete-all-files-of-a-user",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, files) = call(
        &state,
        "drive/files",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{files}");
    assert!(!files.as_array().unwrap().is_empty(), "bob has files");
    let (status, _) = call(
        &state,
        "admin/delete-all-files-of-a-user",
        serde_json::json!({ "userId": bob_id }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, files) = call(
        &state,
        "drive/files",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{files}");
    assert!(files.as_array().unwrap().is_empty(), "{files:?}");

    // admin/drive/files: moderator can list with filters.
    let (status, rows) = call(
        &state,
        "admin/drive/files",
        serde_json::json!({ "limit": 10 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    let (status, rows) = call(
        &state,
        "admin/drive/files",
        serde_json::json!({ "userId": bob_id, "origin": "combined" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert!(rows.as_array().unwrap().is_empty(), "{rows:?}");
    // Non-moderator refused.
    let (status, _) = call(
        &state,
        "admin/drive/files",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn batch_n_admin_drive_show_file() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("moderator");
    let file = state
        .services
        .upload_media(
            &alice_id,
            "s.png".to_string(),
            b"fakepng-bytes",
            None,
            false,
            true,
        )
        .expect("upload");

    // By fileId (any owner — admin view).
    let (status, shown) = call(
        &state,
        "admin/drive/show-file",
        serde_json::json!({ "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["id"], file.id);
    // By served url.
    let (status, shown) = call(
        &state,
        "admin/drive/show-file",
        serde_json::json!({ "url": shown["url"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["id"], file.id);
    // Unknown → verbatim NO_SUCH_FILE.
    let (status, value) = call(
        &state,
        "admin/drive/show-file",
        serde_json::json!({ "fileId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_FILE");
    assert_eq!(value["error"]["id"], "caf3ca38-c6e5-472e-a30c-b05377dcc240");
    // Both/neither → INVALID_PARAM.
    let (status, _) = call(
        &state,
        "admin/drive/show-file",
        serde_json::json!({ "fileId": file.id, "url": "x" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(
        &state,
        "admin/drive/show-file",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Non-moderator refused.
    let (status, _) = call(
        &state,
        "admin/drive/show-file",
        serde_json::json!({ "fileId": file.id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn batch_n_abuse_reports_wire_up() {
    use crate::application::ports::CreateAccountInput;

    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // Self-report → CANNOT_REPORT_YOURSELF (verbatim upstream id).
    let (status, value) = call(
        &state,
        "users/report-abuse",
        serde_json::json!({ "userId": alice_id, "comment": "me" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CANNOT_REPORT_YOURSELF");
    assert_eq!(value["error"]["id"], "1e13149e-b1e8-43cf-902e-c01dbfcb202f");

    // Make Bob an admin (CLI set-role path); reporting him →
    // CANNOT_REPORT_THE_ADMIN.
    state
        .services
        .set_role(None, &bob_id, true, false)
        .expect("set role");
    let (status, value) = call(
        &state,
        "users/report-abuse",
        serde_json::json!({ "userId": bob_id, "comment": "him" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "CANNOT_REPORT_THE_ADMIN");

    // Alice reports Carol (plain user).
    let (carol_id, _) = signup_user(&state, "carol").await;
    let (status, report) = call(
        &state,
        "users/report-abuse",
        serde_json::json!({ "userId": carol_id, "comment": "spam" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    let report_id = report["id"].as_str().unwrap().to_string();
    assert_eq!(report["comment"], "spam");
    assert_eq!(report["resolved"], false);
    assert_eq!(report["reporterId"], serde_json::json!(alice_id));

    // Non-moderator cannot list.
    let (status, _) = call(
        &state,
        "admin/abuse-user-reports",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Promote Alice to moderator; list shows the report.
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("set role");
    let (status, rows) = call(
        &state,
        "admin/abuse-user-reports",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert!(
        rows.as_array()
            .unwrap()
            .iter()
            .any(|r| r["id"] == report_id),
        "{rows:?}"
    );

    // Resolve as accept → resolved + assignee set.
    let (status, _) = call(
        &state,
        "admin/resolve-abuse-user-report",
        serde_json::json!({ "reportId": report_id, "resolvedAs": "accept" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, rows) = call(
        &state,
        "admin/abuse-user-reports",
        serde_json::json!({ "state": "resolved" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    let row = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == report_id)
        .cloned()
        .unwrap();
    assert_eq!(row["resolved"], true);
    assert_eq!(row["resolvedAs"], "accept");
    assert_eq!(row["assigneeId"], serde_json::json!(alice_id));

    // Moderation-note update round-trips.
    let (status, _) = call(
        &state,
        "admin/update-abuse-user-report",
        serde_json::json!({ "reportId": report_id, "moderationNote": "handled" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, rows) = call(
        &state,
        "admin/abuse-user-reports",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    let row = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == report_id)
        .cloned()
        .unwrap();
    assert_eq!(row["moderationNote"], "handled");

    // Unknown report → NO_SUCH_ABUSE_REPORT.
    let (status, value) = call(
        &state,
        "admin/resolve-abuse-user-report",
        serde_json::json!({ "reportId": "aaaaaaaaaaaaaaaa", "resolvedAs": "reject" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
    assert_eq!(value["code"], "NO_SUCH_ABUSE_REPORT");

    // Forward on a local target is refused.
    let (status, value) = call(
        &state,
        "admin/forward-abuse-user-report",
        serde_json::json!({ "reportId": report_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");

    // Remote target: report → forward enqueues the Flag job and marks
    // the report forwarded; a second forward is refused.
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
        .expect("remote account");
    let (status, report) = call(
        &state,
        "users/report-abuse",
        serde_json::json!({ "userId": remote.id, "comment": "remote spam" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    let remote_report = report["id"].as_str().unwrap().to_string();
    let before = state.services.jobs.pending_count().expect("pending");
    let (status, _) = call(
        &state,
        "admin/forward-abuse-user-report",
        serde_json::json!({ "reportId": remote_report }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        state.services.jobs.pending_count().expect("pending") > before,
        "the Flag delivery job is enqueued"
    );
    let (status, value) = call(
        &state,
        "admin/forward-abuse-user-report",
        serde_json::json!({ "reportId": remote_report }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
}
