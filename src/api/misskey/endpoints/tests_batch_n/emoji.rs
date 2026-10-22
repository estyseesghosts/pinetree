//! Endpoint tests_batch_n tests (emoji).
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
async fn batch_n_emoji_admin_pack_and_opt_in() {
    use crate::domain::moderation::ModerationAction;

    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    // Public listing starts empty in the Misskey `{emojis: [...]}` shape.
    let (status, body) = call(&state, "emojis", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, serde_json::json!({ "emojis": [] }));

    // Non-moderator add is refused with no state change.
    let (status, _) = call(
        &state,
        "admin/emoji/add",
        serde_json::json!({ "name": "blobcat", "fileId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Moderator: unknown file → upstream add-UUID NO_SUCH_FILE.
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("set role");
    let (status, value) = call(
        &state,
        "admin/emoji/add",
        serde_json::json!({ "name": "blobcat", "fileId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_FILE");
    assert_eq!(value["error"]["id"], "fc46b5a4-6b92-4c33-ac66-b806659bb5cf");

    // Bad shortcode → INVALID_PARAM naming name.
    let file = state
        .services
        .upload_media(
            &alice_id,
            "e.png".to_string(),
            &[
                0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
                0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
                0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78,
                0x9C, 0x62, 0x60, 0x01, 0x00, 0x00, 0xFF, 0xFF, 0x03, 0x00, 0x00, 0x06, 0x00, 0x05,
                0x72, 0x76, 0x01, 0x82, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42,
                0x60, 0x82,
            ],
            None,
            false,
            true,
        )
        .expect("upload png");
    let (status, value) = call(
        &state,
        "admin/emoji/add",
        serde_json::json!({ "name": "has-dash", "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");

    // Non-image file → UNSUPPORTED_FILE_TYPE.
    let text = state
        .services
        .upload_media(&alice_id, "t.txt".to_string(), b"hello", None, false, true)
        .expect("upload text");
    let (status, value) = call(
        &state,
        "admin/emoji/add",
        serde_json::json!({ "name": "blobcat", "fileId": text.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "UNSUPPORTED_FILE_TYPE");
    assert_eq!(value["error"]["id"], "f7599d96-8750-af68-1633-9575d625c1a7");

    // Happy-path add packs `EmojiDetailed`.
    let (status, packed) = call(
        &state,
        "admin/emoji/add",
        serde_json::json!({ "name": "blobcat", "fileId": file.id, "category": "c", "aliases": ["bc"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{packed}");
    let emoji_id = packed["id"].as_str().unwrap().to_string();
    assert_eq!(packed["name"], "blobcat");
    assert!(
        packed["url"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/files/{}", file.id))
    );

    // Duplicate (case-insensitive) → DUPLICATE_NAME.
    let (status, value) = call(
        &state,
        "admin/emoji/add",
        serde_json::json!({ "name": "BlobCat", "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "DUPLICATE_NAME");

    // Moderation log row committed with the add.
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "type": "addCustomEmoji" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    assert_eq!(logs.as_array().unwrap().len(), 1, "{logs:?}");

    // Public `emojis` + single `emoji` serve it.
    let (status, body) = call(&state, "emojis", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let names: Vec<&str> = body["emojis"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["blobcat"]);
    let (status, single) = call(
        &state,
        "emoji",
        serde_json::json!({ "name": "blobcat" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{single}");
    assert_eq!(single["id"], serde_json::json!(emoji_id));
    let (status, value) = call(&state, "emoji", serde_json::json!({ "name": "nope" }), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_EMOJI");

    // Update by name: category reset via explicit null.
    let (status, updated) = call(
        &state,
        "admin/emoji/update",
        serde_json::json!({ "name": "blobcat", "category": null, "isSensitive": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert!(updated.get("category").is_none() || updated["category"].is_null());
    assert_eq!(updated["isSensitive"], true);

    // Update on a missing row → NO_SUCH_EMOJI.
    let (status, value) = call(
        &state,
        "admin/emoji/update",
        serde_json::json!({ "id": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_EMOJI");

    // A second emoji, then rename onto the taken name → SAME_NAME_EMOJI_EXISTS.
    let (status, second) = call(
        &state,
        "admin/emoji/add",
        serde_json::json!({ "name": "free", "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    let second_id = second["id"].as_str().unwrap().to_string();
    let (status, value) = call(
        &state,
        "admin/emoji/update",
        serde_json::json!({ "id": second_id, "name": "BLOBCAT" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "SAME_NAME_EMOJI_EXISTS");
    assert_eq!(value["error"]["id"], "7180fe9d-1ee3-bff9-647d-fe9896d2ffb8");
    // Rename to a free name succeeds.
    let (status, renamed) = call(
        &state,
        "admin/emoji/update",
        serde_json::json!({ "id": second_id, "name": "freed" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{renamed}");
    assert_eq!(renamed["name"], "freed");

    // Admin list with a query filter.
    let (status, rows) = call(
        &state,
        "admin/emoji/list",
        serde_json::json!({ "query": "blob" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows.as_array().unwrap().len(), 1, "{rows:?}");

    // Delete → NO_CONTENT; the name resolves empty afterwards.
    let (status, _) = call(
        &state,
        "admin/emoji/delete",
        serde_json::json!({ "id": emoji_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "emoji",
        serde_json::json!({ "name": "blobcat" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_EMOJI");

    // WP8b opt-in: `i/update {showRemoteEmojis}` round-trips on `i`.
    let (status, _) = call(
        &state,
        "i/update",
        serde_json::json!({ "showRemoteEmojis": true }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, me) = call(&state, "i", serde_json::json!({}), Some(&bob_token)).await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["showRemoteEmojis"], true);

    // Seed one remote-emoji row directly (ingest path is covered by the
    // federation unit tests); a note packs its URL for the opted-in
    // viewer only.
    state
        .services
        .emojis
        .upsert_remote_emoji(
            crate::domain::emoji::RemoteEmoji::new(
                "remote.example",
                "party",
                "https://remote.example/e/party.png",
                1_700_000_000_000,
            )
            .unwrap(),
        )
        .expect("seed remote emoji");
    let (status, created) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "hi" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(
        shown["emojis"]["party@remote.example"],
        "https://remote.example/e/party.png"
    );
    // Opted-out viewers see local-only maps.
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert!(
        shown["emojis"].get("party@remote.example").is_none(),
        "{shown:?}"
    );
    // Anonymous viewers likewise.
    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert!(
        shown["emojis"].get("party@remote.example").is_none(),
        "{shown:?}"
    );

    // Moderator-only gate on list/update/delete (bob is a plain user).
    let (status, _) = call(
        &state,
        "admin/emoji/list",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let _ = ModerationAction::AddCustomEmoji;
}
