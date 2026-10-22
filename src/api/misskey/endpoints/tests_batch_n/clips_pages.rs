//! Endpoint tests_batch_n tests (clips_pages).
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
async fn clips_crud_caps_favorites_and_notes() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // create → 200 packed; name bounds → INVALID_PARAM.
    let (status, value) = call(
        &state,
        "clips/create",
        serde_json::json!({ "name": "" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let (status, clip) = call(
        &state,
        "clips/create",
        serde_json::json!({ "name": "reading", "isPublic": true, "description": "d" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{clip}");
    assert_eq!(clip["name"], "reading");
    assert_eq!(clip["isPublic"], true);
    assert_eq!(clip["description"], "d");
    assert_eq!(clip["favoritedCount"], 0);
    assert!(clip.get("isFavorited").is_some());
    assert!(clip.get("notesCount").is_some());
    assert!(
        clip.get("tier").is_none(),
        "federation columns stay internal: {clip}"
    );
    let clip_id = clip["id"].as_str().unwrap().to_string();

    // Second clip (private). Cap: 10 clips per owner → TOO_MANY_CLIPS.
    let (status, priv_clip) = call(
        &state,
        "clips/create",
        serde_json::json!({ "name": "mine" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{priv_clip}");
    assert_eq!(priv_clip["isPublic"], false);
    let priv_id = priv_clip["id"].as_str().unwrap().to_string();
    for n in 0..8 {
        let (status, _) = call(
            &state,
            "clips/create",
            serde_json::json!({ "name": format!("c{n}") }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{n}");
    }
    let (status, value) = call(
        &state,
        "clips/create",
        serde_json::json!({ "name": "overflow" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "TOO_MANY_CLIPS");
    assert_eq!(value["error"]["id"], "920f7c2d-6208-4b76-8082-e632020f5883");

    // show: owner sees; stranger sees the public one but the private
    // one hides as NO_SUCH_CLIP (verbatim show UUID).
    let (status, _) = call(
        &state,
        "clips/show",
        serde_json::json!({ "clipId": clip_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, value) = call(
        &state,
        "clips/show",
        serde_json::json!({ "clipId": priv_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_CLIP");
    assert_eq!(value["error"]["id"], "c3c5fe33-d62c-44d2-9ea5-d997703f5c20");
    // Anonymous readers see the public clip without isFavorited.
    let (status, anon) = call(
        &state,
        "clips/show",
        serde_json::json!({ "clipId": clip_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{anon}");
    assert!(anon.get("isFavorited").is_none());

    // Foreign update/delete → NO_SUCH_CLIP (verbatim ids).
    let (status, value) = call(
        &state,
        "clips/update",
        serde_json::json!({ "clipId": clip_id, "name": "hijacked" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"]["id"], "b4d92d70-b216-46fa-9a3f-a8c811699257");
    let (status, value) = call(
        &state,
        "clips/delete",
        serde_json::json!({ "clipId": priv_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"]["id"], "70ca08ba-6865-4630-b6fb-8494759aa754");

    // update own clip (rename + privatize + extension fields).
    let (status, updated) = call(
        &state,
        "clips/update",
        serde_json::json!({ "clipId": clip_id, "name": "reading2", "isPublic": false, "visibility": "unlisted", "federate": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["name"], "reading2");
    assert_eq!(updated["isPublic"], false);

    // Notes: create two notes, clip both.
    let n1 = create_note(
        &state,
        serde_json::json!({ "text": "hello world" }),
        &alice_token,
    )
    .await;
    let n2 = create_note(
        &state,
        serde_json::json!({ "text": "second note" }),
        &bob_token,
    )
    .await;
    for note_id in [&n1, &n2] {
        let (status, _) = call(
            &state,
            "clips/add-note",
            serde_json::json!({ "clipId": priv_id, "noteId": note_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
    // Duplicate → ALREADY_CLIPPED; unknown note → NO_SUCH_NOTE;
    // unknown clip → NO_SUCH_CLIP (verbatim ids).
    let (status, value) = call(
        &state,
        "clips/add-note",
        serde_json::json!({ "clipId": priv_id, "noteId": n1 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_CLIPPED");
    assert_eq!(value["error"]["id"], "734806c4-542c-463a-9311-15c512803965");
    let (status, value) = call(
        &state,
        "clips/add-note",
        serde_json::json!({ "clipId": priv_id, "noteId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
    assert_eq!(value["error"]["id"], "fc8c0b49-c7a3-4664-a0a6-b418d386bb8b");
    let (status, value) = call(
        &state,
        "clips/notes",
        serde_json::json!({ "clipId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"]["id"], "1d7645e6-2b6d-4635-b0fe-fe22b0e72e00");

    // notes listing + word search.
    let (status, notes) = call(
        &state,
        "clips/notes",
        serde_json::json!({ "clipId": priv_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert_eq!(notes.as_array().unwrap().len(), 2);
    let (status, notes) = call(
        &state,
        "clips/notes",
        serde_json::json!({ "clipId": priv_id, "search": "second" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert_eq!(notes.as_array().unwrap().len(), 1);

    // remove-note; missing pair → NO_SUCH_NOTE (verbatim remove id).
    let (status, _) = call(
        &state,
        "clips/remove-note",
        serde_json::json!({ "clipId": priv_id, "noteId": n2 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "clips/remove-note",
        serde_json::json!({ "clipId": priv_id, "noteId": n2 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
    assert_eq!(value["error"]["id"], "aff017de-190e-434b-893e-33a9ff5049d8");

    // Favorites: bob favorites alice's private clip? No — private
    // hides as NO_SUCH_CLIP. Favorite the (now private) reading2 clip
    // as owner, then my-favorites round-trip.
    let (status, value) = call(
        &state,
        "clips/favorite",
        serde_json::json!({ "clipId": priv_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"]["id"], "4c2aaeae-80d8-4250-9606-26cb1fdb77a5");
    // Make reading2 public again and favorite as bob.
    let (status, _) = call(
        &state,
        "clips/update",
        serde_json::json!({ "clipId": clip_id, "isPublic": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &state,
        "clips/favorite",
        serde_json::json!({ "clipId": clip_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "clips/favorite",
        serde_json::json!({ "clipId": clip_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_FAVORITED");
    assert_eq!(value["error"]["id"], "92658936-c625-4273-8326-2d790129256e");
    let (status, favs) = call(
        &state,
        "clips/my-favorites",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{favs}");
    assert_eq!(favs.as_array().unwrap().len(), 1);
    assert_eq!(favs[0]["id"], clip_id);
    // unfavorite; repeat → NOT_FAVORITED (verbatim id).
    let (status, _) = call(
        &state,
        "clips/unfavorite",
        serde_json::json!({ "clipId": clip_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "clips/unfavorite",
        serde_json::json!({ "clipId": clip_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NOT_FAVORITED");
    assert_eq!(value["error"]["id"], "90c3a9e8-b321-4dae-bf57-2bf79bbcc187");
    // unfavorite of an unknown clip → NO_SUCH_CLIP (verbatim id, no
    // visibility check).
    let (status, value) = call(
        &state,
        "clips/unfavorite",
        serde_json::json!({ "clipId": "aaaaaaaaaaaaaaaa" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"]["id"], "2603966e-b865-426c-94a7-af4a01241dc1");

    // list shows all 10 own clips; delete cascades.
    let (status, lists) = call(
        &state,
        "clips/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lists}");
    assert_eq!(lists.as_array().unwrap().len(), 10);
    let (status, _) = call(
        &state,
        "clips/delete",
        serde_json::json!({ "clipId": priv_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, lists) = call(
        &state,
        "clips/list",
        serde_json::json!({ "limit": 100 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lists}");
    assert_eq!(lists.as_array().unwrap().len(), 9);
    let _ = (alice_id, bob_id);
}

#[tokio::test]
async fn pages_crud_likes_and_featured() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    let body = |title: &str, name: &str| {
        serde_json::json!({
            "title": title,
            "name": name,
            "content": [{ "type": "text", "text": "hello" }],
            "variables": [],
            "script": "let x = 1",
        })
    };
    // create → 200 packed; bad slug → INVALID_PARAM.
    let (status, value) = call(
        &state,
        "pages/create",
        body("T", "bad name"),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let (status, page) = call(
        &state,
        "pages/create",
        body("Hello", "hello"),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["title"], "Hello");
    assert_eq!(page["name"], "hello");
    assert_eq!(page["font"], "sans-serif");
    assert_eq!(page["alignCenter"], false);
    assert_eq!(page["likedCount"], 0);
    assert!(page.get("isLiked").is_some());
    assert_eq!(page["script"], "let x = 1");
    let page_id = page["id"].as_str().unwrap().to_string();
    // Pages federate by default (owner decision 2026-10-20): absent
    // `federate` param means true (clips stay opt-in). The flag is
    // internal-only, so read it back through the store.
    assert!(
        state
            .services
            .pages
            .get_page(&page_id)
            .unwrap()
            .unwrap()
            .federate,
        "pages default to federate=true"
    );
    // Duplicate slug → NAME_ALREADY_EXISTS (verbatim create UUID).
    let (status, value) = call(
        &state,
        "pages/create",
        body("Other", "hello"),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NAME_ALREADY_EXISTS");
    assert_eq!(value["error"]["id"], "4650348e-301c-499a-83c9-6aa988c66bc1");

    // show by id (anonymous) and by name+username.
    let (status, shown) = call(
        &state,
        "pages/show",
        serde_json::json!({ "pageId": page_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["title"], "Hello");
    assert!(shown.get("isLiked").is_none());
    let (status, shown) = call(
        &state,
        "pages/show",
        serde_json::json!({ "name": "hello", "username": "alice" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["id"], page_id);
    // Unknown id / name → NO_SUCH_PAGE (verbatim show UUID).
    let (status, value) = call(
        &state,
        "pages/show",
        serde_json::json!({ "pageId": "aaaaaaaaaaaaaaaa" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"]["id"], "222120c0-3ead-4528-811b-b96f233388d7");

    // update (partial): rename + clear summary + set eye-catch later.
    let (status, _) = call(
        &state,
        "pages/update",
        serde_json::json!({ "pageId": page_id, "title": "Hello 2", "summary": "s", "font": "serif" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, shown) = call(
        &state,
        "pages/show",
        serde_json::json!({ "pageId": page_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["title"], "Hello 2");
    assert_eq!(shown["font"], "serif");
    assert_eq!(shown["summary"], "s");
    // Foreign update → NO_SUCH_PAGE (upstream: owner-only).
    let (status, value) = call(
        &state,
        "pages/update",
        serde_json::json!({ "pageId": page_id, "title": "hijacked" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_PAGE");
    assert_eq!(value["error"]["id"], "21149b9e-3616-4778-9592-c4ce89f5a864");
    // Foreign delete → ACCESS_DENIED (verbatim delete UUID, 403).
    let (status, value) = call(
        &state,
        "pages/delete",
        serde_json::json!({ "pageId": page_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["code"], "ACCESS_DENIED");
    assert_eq!(value["error"]["id"], "8b741b3e-2c22-44b3-a15f-29949aa1601e");

    // like: own page → YOUR_PAGE; bob likes → ALREADY_LIKED on
    // repeat; unlike flows.
    let (status, value) = call(
        &state,
        "pages/like",
        serde_json::json!({ "pageId": page_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "YOUR_PAGE");
    assert_eq!(value["error"]["id"], "28800466-e6db-40f2-8fae-bf9e82aa92b8");
    let (status, _) = call(
        &state,
        "pages/like",
        serde_json::json!({ "pageId": page_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "pages/like",
        serde_json::json!({ "pageId": page_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_LIKED");
    assert_eq!(value["error"]["id"], "d4c1edbe-7da2-4eae-8714-1acfd2d63941");
    let (status, _) = call(
        &state,
        "pages/unlike",
        serde_json::json!({ "pageId": page_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "pages/unlike",
        serde_json::json!({ "pageId": page_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NOT_LIKED");
    assert_eq!(value["error"]["id"], "f5e586b0-ce93-4050-b0e3-7f31af5259ee");
    // featured: empty until a like lands (query-based, upstream parity).
    let (status, featured) = call(&state, "pages/featured", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{featured}");
    assert_eq!(featured.as_array().unwrap().len(), 0);
    let (status, _) = call(
        &state,
        "pages/like",
        serde_json::json!({ "pageId": page_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, featured) = call(&state, "pages/featured", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{featured}");
    assert_eq!(featured.as_array().unwrap().len(), 1);
    assert_eq!(featured[0]["id"], page_id);

    // delete by owner → show 404s after.
    let (status, _) = call(
        &state,
        "pages/delete",
        serde_json::json!({ "pageId": page_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "pages/show",
        serde_json::json!({ "pageId": page_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_PAGE");
    let _ = (alice_id, bob_id);
}

#[tokio::test]
async fn clips_export_import_round_trip() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;

    let n1 = create_note(
        &state,
        serde_json::json!({ "text": "export me" }),
        &alice_token,
    )
    .await;
    let (status, clip) = call(
        &state,
        "clips/create",
        serde_json::json!({ "name": "backup", "description": "d" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{clip}");
    let clip_id = clip["id"].as_str().unwrap().to_string();
    let (status, _) = call(
        &state,
        "clips/add-note",
        serde_json::json!({ "clipId": clip_id, "noteId": n1 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Export → real upstream-shaped data (not an empty file).
    let (status, _) = call(
        &state,
        "i/export-clips",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();
    let notes = export_completed_notifications(&state, &alice_token).await;
    assert_eq!(notes[0]["exportedEntity"], "clip");
    let file_id = notes[0]["fileId"].as_str().unwrap().to_string();
    let bytes = read_export_bytes(&state, &file_id).await;
    let items: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "backup");
    assert_eq!(items[0]["description"], "d");
    assert!(items[0].get("lastClippedAt").is_some());
    let members = items[0]["clipNotes"].as_array().unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0]["note"]["id"], n1);
    assert!(members[0]["note"].get("text").is_some());
    assert!(members[0]["note"]["user"].get("username").is_some());

    // Import guards: unknown → NO_SUCH_FILE, empty → EMPTY_FILE
    // (pinetree-fixed UUIDs; no upstream endpoint exists).
    let (status, value) = call(
        &state,
        "i/import-clips",
        serde_json::json!({ "fileId": "mid00000000000999" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_FILE");
    assert_eq!(value["error"]["id"], "7c1d9e02-4b5a-4f6e-9d1a-2c3e5f6a7b81");
    let empty = state
        .services
        .upload_media(&alice_id, "empty.json".to_string(), b"", None, false, true)
        .unwrap();
    let (status, value) = call(
        &state,
        "i/import-clips",
        serde_json::json!({ "fileId": empty.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "EMPTY_FILE");
    assert_eq!(value["error"]["id"], "8d2e0f13-5c6b-4a7f-8e2b-3d4f6a7b8c92");

    // Delete the clip, then re-import the export: find-or-create by
    // name restores the clip and re-attaches the visible note.
    let (status, _) = call(
        &state,
        "clips/delete",
        serde_json::json!({ "clipId": clip_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let reimport = state
        .services
        .upload_media(
            &alice_id,
            "clips-import.json".to_string(),
            &bytes,
            None,
            false,
            true,
        )
        .unwrap();
    let (status, _) = call(
        &state,
        "i/import-clips",
        serde_json::json!({ "fileId": reimport.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();
    let (status, lists) = call(
        &state,
        "clips/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lists}");
    assert_eq!(lists.as_array().unwrap().len(), 1);
    assert_eq!(lists[0]["name"], "backup");
    let restored_id = lists[0]["id"].as_str().unwrap().to_string();
    assert_ne!(restored_id, clip_id, "re-import creates a fresh clip row");
    let (status, notes) = call(
        &state,
        "clips/notes",
        serde_json::json!({ "clipId": restored_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert_eq!(notes.as_array().unwrap().len(), 1);
    assert_eq!(notes[0]["id"], n1);

    // Malformed import files converge quietly (whole file skipped).
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
    let (status, _) = call(
        &state,
        "i/import-clips",
        serde_json::json!({ "fileId": bad.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    state.worker.drain(100).unwrap();
    let (status, lists) = call(
        &state,
        "clips/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lists}");
    assert_eq!(
        lists.as_array().unwrap().len(),
        1,
        "bad import applied nothing"
    );
}
