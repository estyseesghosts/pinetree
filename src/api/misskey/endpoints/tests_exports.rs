//! Endpoint tests_exports tests (split from `endpoints/mod.rs`).
//!
//! Pure move, no behavior change.

#![allow(unused_imports)]

use super::tests_common::{
    call, create_note, export_completed_notifications, read_export_bytes, remote_account,
    signup_user, test_state,
};
use super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::application::ports::{PortError, TimelineCursor};
use axum::http::StatusCode;
use serde_json::Value;
use std::sync::Arc;
#[tokio::test]
async fn parity_export_following_csv_exact_and_notification() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;
    let (carol_id, _) = signup_user(&state, "carol").await;
    let eve_id = remote_account(&state.services, "eve", "remote.example");
    state.services.follow(&alice_id, &bob_id, true).unwrap();
    clock.advance_ms(1_000);
    state.services.follow(&alice_id, &carol_id, false).unwrap();
    clock.advance_ms(1_000);
    state.services.follow(&alice_id, &eve_id, true).unwrap();

    let (status, _) = call(
        &state,
        "i/export-following",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();

    // `exportCompleted` carries the drive file id + entity tag.
    let notes = export_completed_notifications(&state, &alice_token).await;
    let notes = notes.as_array().unwrap();
    assert_eq!(notes.len(), 1, "{notes:?}");
    assert_eq!(notes[0]["type"], "exportCompleted");
    assert_eq!(notes[0]["exportedEntity"], "following");
    let file_id = notes[0]["fileId"].as_str().unwrap().to_string();

    // Exact upstream row shape: `{acct},withReplies={bool}`, one per
    // line, follow-row-id order, no header row. Locals are bare
    // usernames (DIVERGENCE), remotes are `user@host`.
    let bytes = read_export_bytes(&state, &file_id).await;
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "bob,withReplies=true\ncarol,withReplies=false\neve@remote.example,withReplies=true\n",
    );
}

#[tokio::test]
async fn parity_export_following_exclude_muting_and_inactive() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;
    let (carol_id, _) = signup_user(&state, "carol").await;
    state.services.follow(&alice_id, &bob_id, true).unwrap();
    clock.advance_ms(1_000);
    state.services.follow(&alice_id, &carol_id, true).unwrap();
    state.services.mute(&alice_id, &carol_id, None).unwrap();

    // Default export keeps muted followees.
    let (status, _) = call(
        &state,
        "i/export-following",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();
    let notes = export_completed_notifications(&state, &alice_token).await;
    let file_id = notes[0]["fileId"].as_str().unwrap().to_string();
    let bytes = read_export_bytes(&state, &file_id).await;
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "bob,withReplies=true\ncarol,withReplies=true\n",
    );

    // `excludeMuting` drops them (second call needs a fresh hour).
    clock.advance_ms(3_600_000);
    let (status, _) = call(
        &state,
        "i/export-following",
        serde_json::json!({ "excludeMuting": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();
    let notes = export_completed_notifications(&state, &alice_token).await;
    let file_id = notes[0]["fileId"].as_str().unwrap().to_string();
    let bytes = read_export_bytes(&state, &file_id).await;
    assert_eq!(String::from_utf8(bytes).unwrap(), "bob,withReplies=true\n");

    // `excludeInactive` is accepted and ignored (DIVERGENCE: no
    // last-active tracking): same bytes as the default export.
    clock.advance_ms(3_600_000);
    let (status, _) = call(
        &state,
        "i/export-following",
        serde_json::json!({ "excludeInactive": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();
    let notes = export_completed_notifications(&state, &alice_token).await;
    let file_id = notes[0]["fileId"].as_str().unwrap().to_string();
    let bytes = read_export_bytes(&state, &file_id).await;
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "bob,withReplies=true\ncarol,withReplies=true\n",
    );
}

#[tokio::test]
async fn parity_export_blocking_and_mute_csv() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;
    let (carol_id, _) = signup_user(&state, "carol").await;
    let (dave_id, _) = signup_user(&state, "dave").await;
    state.services.block(&alice_id, &bob_id).unwrap();
    state.services.mute(&alice_id, &carol_id, None).unwrap();
    // Expiring mutes are NOT exported (upstream `IsNull()` parity).
    state
        .services
        .mute(
            &alice_id,
            &dave_id,
            Some(state.services.clock.now_ms() + 100_000),
        )
        .unwrap();

    let (status, _) = call(
        &state,
        "i/export-blocking",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(
        &state,
        "i/export-mute",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();

    let notes = export_completed_notifications(&state, &alice_token).await;
    let notes = notes.as_array().unwrap();
    assert_eq!(notes.len(), 2, "{notes:?}");
    let mut seen = std::collections::HashMap::new();
    for note in notes {
        let entity = note["exportedEntity"].as_str().unwrap().to_string();
        let file_id = note["fileId"].as_str().unwrap().to_string();
        let bytes = read_export_bytes(&state, &file_id).await;
        seen.insert(entity, String::from_utf8(bytes).unwrap());
    }
    assert_eq!(seen["blocking"], "bob\n");
    assert_eq!(seen["muting"], "carol\n");
}

#[tokio::test]
async fn parity_export_notes_json_roundtrip() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let first = state
        .services
        .create_note(crate::application::ports::CreateNoteInput::simple_public(
            &alice_id,
            "first note",
        ))
        .unwrap();
    clock.advance_ms(1_000);
    let second = state
        .services
        .create_note(crate::application::ports::CreateNoteInput::simple_public(
            &alice_id,
            "second note",
        ))
        .unwrap();

    let (status, _) = call(
        &state,
        "i/export-notes",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();

    let notes = export_completed_notifications(&state, &alice_token).await;
    assert_eq!(notes[0]["exportedEntity"], "note");
    let file_id = notes[0]["fileId"].as_str().unwrap().to_string();
    let bytes = read_export_bytes(&state, &file_id).await;
    let items: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
    // Newest-first, upstream DTO keys.
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["id"], second.id);
    assert_eq!(items[0]["text"], "second note");
    assert_eq!(items[1]["id"], first.id);
    for item in &items {
        for key in [
            "id",
            "text",
            "createdAt",
            "fileIds",
            "files",
            "replyId",
            "renoteId",
            "poll",
            "cw",
            "visibility",
            "visibleUserIds",
            "localOnly",
            "reactionAcceptance",
        ] {
            assert!(item.get(key).is_some(), "missing {key} in {item}");
        }
        assert_eq!(item["visibility"], "public");
        assert_eq!(item["poll"], Value::Null);
    }
}

#[tokio::test]
async fn parity_import_blocking_guards_use_upstream_errors() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;

    // Unknown id → NO_SUCH_FILE with the import-blocking UUID.
    let (status, value) = call(
        &state,
        "i/import-blocking",
        serde_json::json!({ "fileId": "mid00000000000999" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_FILE");
    assert_eq!(value["error"]["id"], "ebb53e5f-6574-9c0c-0b92-7ca6def56d7e");

    // Foreign (bob's) file → same NO_SUCH_FILE (ownership is hidden).
    clock.advance_ms(3_600_000);
    let foreign = state
        .services
        .upload_media(
            &bob_id,
            "list.csv".to_string(),
            b"alice\n",
            None,
            false,
            true,
        )
        .unwrap();
    let (status, value) = call(
        &state,
        "i/import-blocking",
        serde_json::json!({ "fileId": foreign.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_FILE");
    assert_eq!(value["error"]["id"], "ebb53e5f-6574-9c0c-0b92-7ca6def56d7e");

    // Empty file → EMPTY_FILE.
    clock.advance_ms(3_600_000);
    let empty = state
        .services
        .upload_media(&alice_id, "empty.csv".to_string(), b"", None, false, true)
        .unwrap();
    let (status, value) = call(
        &state,
        "i/import-blocking",
        serde_json::json!({ "fileId": empty.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "EMPTY_FILE");
    assert_eq!(value["error"]["id"], "6f3a4dcc-f060-a707-4950-806fbdbe60d6");

    // Over 64KiB → TOO_BIG_FILE.
    clock.advance_ms(3_600_000);
    let big = vec![b'a'; 65_537];
    let oversized = state
        .services
        .upload_media(&alice_id, "big.csv".to_string(), &big, None, false, true)
        .unwrap();
    let (status, value) = call(
        &state,
        "i/import-blocking",
        serde_json::json!({ "fileId": oversized.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "TOO_BIG_FILE");
    assert_eq!(value["error"]["id"], "b7fbf0b1-aeef-3b21-29ef-fadd4cb72ccf");
}

#[tokio::test]
async fn parity_import_blocking_rows_and_replay() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;
    let file = state
        .services
        .upload_media(
            &alice_id,
            "blocking.csv".to_string(),
            b"bob\nno-such-user-xyz\n",
            None,
            false,
            true,
        )
        .unwrap();

    let (status, _) = call(
        &state,
        "i/import-blocking",
        serde_json::json!({ "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();

    // Valid rows applied, unresolvable rows skipped, batch intact.
    assert!(
        state
            .services
            .blocks
            .is_blocking(&alice_id, &bob_id)
            .unwrap()
    );
    let list = || {
        state
            .services
            .blocks
            .list_blockings(
                &alice_id,
                &TimelineCursor {
                    limit: 100,
                    ..Default::default()
                },
            )
            .unwrap()
    };
    assert_eq!(list().len(), 1);

    // A replay of the same file converges (conflicts skip, no dupes).
    clock.advance_ms(3_600_000);
    let (status, _) = call(
        &state,
        "i/import-blocking",
        serde_json::json!({ "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();
    assert_eq!(list().len(), 1);
}

#[tokio::test]
async fn parity_import_following_acct_and_legacy_rows() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;
    let (carol_id, _) = signup_user(&state, "carol").await;
    let (dave_id, _) = signup_user(&state, "dave").await;
    let file = state
        .services
        .upload_media(
            &alice_id,
            "following.csv".to_string(),
            b"bob\ncarol,withReplies=false\ndeadbeef,dave,example.local\nghost@unknown.invalid\n",
            None,
            false,
            true,
        )
        .unwrap();

    let (status, _) = call(
        &state,
        "i/import-following",
        serde_json::json!({ "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();

    // Bare acct, acct + row `withReplies=`, and the legacy 3-column
    // (`id,username,host`) variant all resolve; the unresolvable remote
    // row skips without failing the batch.
    let bob = state
        .services
        .follows
        .get_follow(&alice_id, &bob_id)
        .unwrap()
        .unwrap();
    assert!(
        !bob.with_replies,
        "endpoint default withReplies is false (Batch Q item 5)"
    );
    let carol = state
        .services
        .follows
        .get_follow(&alice_id, &carol_id)
        .unwrap()
        .unwrap();
    assert!(!carol.with_replies, "row withReplies=false wins");
    assert!(
        state
            .services
            .follows
            .is_following(&alice_id, &dave_id)
            .unwrap()
    );
    assert_eq!(
        state
            .services
            .follows
            .following_of(&alice_id, None, 100)
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn parity_import_muting_applies_rows() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;
    let file = state
        .services
        .upload_media(
            &alice_id,
            "mute.csv".to_string(),
            b"bob\n",
            None,
            false,
            true,
        )
        .unwrap();

    let (status, _) = call(
        &state,
        "i/import-muting",
        serde_json::json!({ "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();
    assert!(state.services.mutes.is_muting(&alice_id, &bob_id).unwrap());
}

#[tokio::test]
async fn parity_empty_exports_are_valid() {
    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;

    for endpoint in [
        "i/export-user-lists",
        "i/export-antennas",
        "i/export-clips",
        "i/export-favorites",
    ] {
        let (status, _) = call(&state, endpoint, serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{endpoint}");
    }
    state.worker.drain(100).unwrap();

    let notes = export_completed_notifications(&state, &alice_token).await;
    let notes = notes.as_array().unwrap();
    assert_eq!(notes.len(), 4, "{notes:?}");
    let mut seen = std::collections::HashMap::new();
    for note in notes {
        let entity = note["exportedEntity"].as_str().unwrap().to_string();
        let file_id = note["fileId"].as_str().unwrap().to_string();
        let bytes = read_export_bytes(&state, &file_id).await;
        seen.insert(entity, String::from_utf8(bytes).unwrap());
    }
    // Zero user lists export as zero CSV rows; antennas/clips/favorites
    // export as empty JSON arrays (what upstream writes when empty —
    // antennas/favorites now have subsystems, so this is the empty case
    // of the real exporters).
    assert_eq!(seen["userList"], "");
    assert_eq!(seen["antenna"], "[]");
    assert_eq!(seen["clip"], "[]");
    assert_eq!(seen["favorite"], "[]");
}

#[tokio::test]
async fn parity_export_rate_limit_trips_and_recovers() {
    let (state, clock) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;

    let (status, _) = call(
        &state,
        "i/export-notes",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // Second call inside the hour trips the 1/hr bucket.
    let (status, value) = call(
        &state,
        "i/export-notes",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
    assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");
    // A fresh hour admits again.
    clock.advance_ms(3_600_000);
    let (status, _) = call(
        &state,
        "i/export-notes",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn parity_import_user_lists_validates_but_applies_nothing() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_bob_id, _bob_token) = signup_user(&state, "bob").await;
    let file = state
        .services
        .upload_media(
            &alice_id,
            "lists.csv".to_string(),
            b"Friends,bob\nnot-a-valid-row\n",
            None,
            false,
            true,
        )
        .unwrap();
    let (status, _) = call(
        &state,
        "i/import-user-lists",
        serde_json::json!({ "fileId": file.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // No user-list subsystem: validates, applies zero, completes.
    state.worker.drain(100).unwrap();
}

// --- Batch P ----------------------------------------------------------
