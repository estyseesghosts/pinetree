//! Endpoint tests_batch_o tests (split from `endpoints/mod.rs`).
//!
//! Pure move, no behavior change.

#![allow(unused_imports)]

use super::tests_common::{call, create_note, signup_user, test_state};
use super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::api::misskey::auth::{generate_random_token, hash_token};
use crate::application::ports::{ApiTokenRecord, PortError};
use axum::http::StatusCode;
use serde_json::Value;
use std::sync::Arc;
#[tokio::test]
async fn batch_o_user_lists_crud_timeline_memberships_and_favorites() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;

    // create → 200 packed; bad name → INVALID_PARAM.
    let (status, value) = call(
        &state,
        "users/lists/create",
        serde_json::json!({ "name": "" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let (status, list) = call(
        &state,
        "users/lists/create",
        serde_json::json!({ "name": "friends" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list["name"], "friends");
    assert_eq!(list["isPublic"], false);
    assert_eq!(list["userIds"].as_array().unwrap().len(), 0);
    let list_id = list["id"].as_str().unwrap().to_string();

    // push bob + carol; dup → ALREADY_ADDED (verbatim id).
    for user_id in [&bob_id, &carol_id] {
        let (status, _) = call(
            &state,
            "users/lists/push",
            serde_json::json!({ "listId": list_id, "userId": user_id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
    let (status, value) = call(
        &state,
        "users/lists/push",
        serde_json::json!({ "listId": list_id, "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_ADDED");
    assert_eq!(value["error"]["id"], "1de7c884-1595-49e9-857e-61f12f4d4fc5");
    // Unknown list/user → verbatim ids; foreign list → NO_SUCH_LIST.
    let (status, value) = call(
        &state,
        "users/lists/push",
        serde_json::json!({ "listId": "aaaaaaaaaaaaaaaa", "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_LIST");
    assert_eq!(value["error"]["id"], "7f44670e-ab16-43b8-b4c1-ccd2ee89cc02");
    let (status, value) = call(
        &state,
        "users/lists/push",
        serde_json::json!({ "listId": list_id, "userId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");
    assert_eq!(value["error"]["id"], "a89abd3d-f0bc-4cce-beb1-2f446f4f1e6a");
    let (status, value) = call(
        &state,
        "users/lists/push",
        serde_json::json!({ "listId": list_id, "userId": bob_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_LIST");

    // Blocked push: carol blocks alice → alice pushing carol fails.
    let (status, _) = call(
        &state,
        "blocking/create",
        serde_json::json!({ "userId": alice_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, value) = call(
        &state,
        "users/lists/push",
        serde_json::json!({ "listId": list_id, "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    // (already a member → ALREADY_ADDED fires before the block check;
    // pull bob, then the block refusal surfaces.)
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    let (status, _) = call(
        &state,
        "users/lists/pull",
        serde_json::json!({ "listId": list_id, "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "users/lists/push",
        serde_json::json!({ "listId": list_id, "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "YOU_HAVE_BEEN_BLOCKED");
    assert_eq!(value["error"]["id"], "990232c5-3f9d-4d83-9f3f-ef27b6332a4b");
    // Self-push skips the block check.
    let (status, _) = call(
        &state,
        "users/lists/push",
        serde_json::json!({ "listId": list_id, "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // Unblock for the rest of the test.
    let (status, _) = call(
        &state,
        "blocking/delete",
        serde_json::json!({ "userId": alice_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &state,
        "users/lists/push",
        serde_json::json!({ "listId": list_id, "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // show: owner reads; strangers denied on private lists.
    let (status, shown) = call(
        &state,
        "users/lists/show",
        serde_json::json!({ "listId": list_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["userIds"].as_array().unwrap().len(), 3);
    let (status, value) = call(
        &state,
        "users/lists/show",
        serde_json::json!({ "listId": list_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_LIST");
    assert_eq!(value["error"]["id"], "7bc05c21-1d7a-41ae-88f1-66820f4dc686");
    // list: own lists need a credential; userId selects public lists.
    let (status, value) = call(&state, "users/lists/list", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{value}");
    let (status, lists) = call(
        &state,
        "users/lists/list",
        serde_json::json!({ "userId": alice_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lists}");
    assert!(lists.as_array().unwrap().is_empty(), "private list hidden");
    // Make it public; update echoes the pack.
    let (status, updated) = call(
        &state,
        "users/lists/update",
        serde_json::json!({ "listId": list_id, "isPublic": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["isPublic"], true);
    let (status, lists) = call(
        &state,
        "users/lists/list",
        serde_json::json!({ "userId": alice_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lists}");
    assert_eq!(lists.as_array().unwrap().len(), 1);
    assert_eq!(lists[0]["userIds"].as_array().unwrap().len(), 3);

    // get-memberships: packed members newest-…; update-membership flips
    // withReplies (silent no-op on strangers).
    let (status, members) = call(
        &state,
        "users/lists/get-memberships",
        serde_json::json!({ "listId": list_id, "forPublic": true }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{members}");
    assert_eq!(members.as_array().unwrap().len(), 3);
    assert!(members[0].get("user").is_some());
    let (status, _) = call(
        &state,
        "users/lists/update-membership",
        serde_json::json!({ "listId": list_id, "userId": bob_id, "withReplies": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Timeline: bob posts; carol replies to bob (third-party reply —
    // hidden while carol's membership has withReplies=false, visible
    // after the flip). Alice reads the pull timeline.
    let bob_note = create_note(
        &state,
        serde_json::json!({ "text": "bob here" }),
        &bob_token,
    )
    .await;
    let carol_reply = create_note(
        &state,
        serde_json::json!({ "text": "carol replies", "replyId": bob_note }),
        &carol_token,
    )
    .await;
    let (status, timeline) = call(
        &state,
        "notes/user-list-timeline",
        serde_json::json!({ "listId": list_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{timeline}");
    let ids: Vec<&str> = timeline
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&bob_note.as_str()), "{ids:?}");
    assert!(
        !ids.contains(&carol_reply.as_str()),
        "third-party reply gated: {ids:?}"
    );
    // Flip carol's withReplies → the reply backfills (pull, no fanout).
    let (status, _) = call(
        &state,
        "users/lists/update-membership",
        serde_json::json!({ "listId": list_id, "userId": carol_id, "withReplies": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, timeline) = call(
        &state,
        "notes/user-list-timeline",
        serde_json::json!({ "listId": list_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{timeline}");
    let ids: Vec<&str> = timeline
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&carol_reply.as_str()), "backfilled: {ids:?}");
    // Strangers cannot read the timeline.
    let (status, value) = call(
        &state,
        "notes/user-list-timeline",
        serde_json::json!({ "listId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_LIST");
    assert_eq!(value["error"]["id"], "8fb1fbd5-e476-4c37-9fb0-43d55b63a2ff");

    // favorites: public lists only; dup + miss map verbatim.
    let (status, _) = call(
        &state,
        "users/lists/favorite",
        serde_json::json!({ "listId": list_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "users/lists/favorite",
        serde_json::json!({ "listId": list_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_FAVORITED");
    assert_eq!(value["error"]["id"], "6425bba0-985b-461e-af1b-518070e72081");
    let (status, shown) = call(
        &state,
        "users/lists/show",
        serde_json::json!({ "listId": list_id, "forPublic": true }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["likedCount"], 1);
    assert_eq!(shown["isLiked"], true);
    let (status, _) = call(
        &state,
        "users/lists/unfavorite",
        serde_json::json!({ "listId": list_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "users/lists/unfavorite",
        serde_json::json!({ "listId": list_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "ALREADY_FAVORITED", "unfavorite-miss quirk");
    assert_eq!(value["error"]["id"], "835c4b27-463d-4cfa-969b-a9058678d465");
    // Private lists cannot be favorited.
    let (status, _) = call(
        &state,
        "users/lists/update",
        serde_json::json!({ "listId": list_id, "isPublic": false }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, value) = call(
        &state,
        "users/lists/favorite",
        serde_json::json!({ "listId": list_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER_LIST");
    assert_eq!(value["error"]["id"], "7dbaf3cf-7b42-4b8f-b431-b3919e580dbe");

    // delete removes memberships and stops dependent antennas.
    let (status, antenna) = call(
        &state,
        "antennas/create",
        serde_json::json!({
            "name": "lst", "src": "list", "userListId": list_id,
            "keywords": [["x"]], "excludeKeywords": [], "users": [],
            "caseSensitive": false, "withReplies": true, "withFile": false,
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{antenna}");
    let (status, _) = call(
        &state,
        "users/lists/delete",
        serde_json::json!({ "listId": list_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "users/lists/show",
        serde_json::json!({ "listId": list_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_LIST");
    let (status, value) = call(
        &state,
        "antennas/show",
        serde_json::json!({ "antennaId": antenna["id"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_ANTENNA");
}

#[tokio::test]
async fn batch_o_drive_extras_and_url_preview() {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    // Inline loopback origin: PNG at /pic.png, HTML at /page,
    // plain text at /txt. `allow_private_ips` is on in test state.
    static PNG: &[u8] = &[
        0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x01, 0x02, 0x03,
    ];
    static HTML: &[u8] = br#"<html><head>
<meta property="og:title" content="OG Title">
<meta name="description" content="Meta Desc">
<meta property="og:site_name" content="Example">
<meta property="og:image" content="/img.png">
<link rel="shortcut icon" href="/favicon.ico">
<title>Fallback</title>
</head><body>hi</body></html>"#;
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback origin");
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for _ in 0..16 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
            let mut buf = [0u8; 2048];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            let (status, ctype, body): (&str, &str, &[u8]) = if req.starts_with("GET /pic.png") {
                ("200 OK", "image/png", PNG)
            } else if req.starts_with("GET /page") {
                ("200 OK", "text/html; charset=utf-8", HTML)
            } else if req.starts_with("GET /txt") {
                ("200 OK", "text/plain", b"just text")
            } else {
                let _ = stream.write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope",
                );
                continue;
            };
            let hdr = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(hdr.as_bytes());
            let _ = stream.write_all(body);
        }
    });
    let base = format!("http://{addr}");

    let (state, _) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;

    // upload-from-url stores + packs the file.
    let (status, file) = call(
        &state,
        "drive/files/upload-from-url",
        serde_json::json!({ "url": format!("{base}/pic.png") }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{file}");
    assert_eq!(file["name"], "pic.png");
    let file_id = file["id"].as_str().unwrap().to_string();
    let md5 = file["md5"].as_str().unwrap().to_string();

    // Second fetch without force dedups to the same row.
    let (status, file2) = call(
        &state,
        "drive/files/upload-from-url",
        serde_json::json!({ "url": format!("{base}/pic.png"), "marker": "opaque-ok" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{file2}");
    assert_eq!(file2["id"], file_id);
    // force=true inserts a second row.
    let (status, file3) = call(
        &state,
        "drive/files/upload-from-url",
        serde_json::json!({ "url": format!("{base}/pic.png"), "force": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{file3}");
    assert_ne!(file3["id"], file_id);

    // find-by-hash resolves md5, never blob keys.
    let (status, found) = call(
        &state,
        "drive/files/find-by-hash",
        serde_json::json!({ "md5": md5 }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{found}");
    assert_eq!(found["id"], file_id);
    let (status, value) = call(
        &state,
        "drive/files/find-by-hash",
        serde_json::json!({ "md5": "0".repeat(32) }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_FILE");
    // folderId must stay null.
    let (status, value) = call(
        &state,
        "drive/files/upload-from-url",
        serde_json::json!({ "url": format!("{base}/pic.png"), "folderId": "f1" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    // 404 origin → 400, not a file row.
    let (status, value) = call(
        &state,
        "drive/files/upload-from-url",
        serde_json::json!({ "url": format!("{base}/missing") }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");

    // url-preview: credential required, OG parse, absolutized assets.
    let (status, _) = call(
        &state,
        "url-preview",
        serde_json::json!({ "url": format!("{base}/page") }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, preview) = call(
        &state,
        "url-preview",
        serde_json::json!({ "url": format!("{base}/page") }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    assert_eq!(preview["title"], "OG Title");
    assert_eq!(preview["description"], "Meta Desc");
    assert_eq!(preview["siteName"], "Example");
    assert_eq!(preview["image"], format!("{base}/img.png"));
    assert_eq!(preview["icon"], format!("{base}/favicon.ico"));
    // Cached second call answers the same.
    let (status, preview2) = call(
        &state,
        "url-preview",
        serde_json::json!({ "url": format!("{base}/page") }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{preview2}");
    assert_eq!(preview, preview2);
    // Non-HTML answers `{}`.
    let (status, preview) = call(
        &state,
        "url-preview",
        serde_json::json!({ "url": format!("{base}/pic.png") }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    assert_eq!(preview, serde_json::json!({}));
    // Non-fetchable scheme is rejected without network.
    let (status, value) = call(
        &state,
        "url-preview",
        serde_json::json!({ "url": "ftp://example.com/x" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
}

#[tokio::test]
async fn batch_o_self_service_tokens_and_signin_history() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;

    // i/signin-history: signup recorded one success row.
    let (status, history) = call(
        &state,
        "i/signin-history",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    assert_eq!(history.as_array().unwrap().len(), 1);
    assert_eq!(history[0]["success"], true);
    assert!(history[0].get("ip").is_some());

    // Failed password signin appends success:false; unknown user does not.
    let (status, _) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "alice", "password": "wrong-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    clock.advance_ms(61_000);
    let (status, _) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "nobody", "password": "wrong-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    clock.advance_ms(61_000);
    let (status, history) = call(
        &state,
        "i/signin-history",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    let rows = history.as_array().unwrap();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0]["success"], false, "newest first");
    assert_eq!(rows[1]["success"], true);
    // Cursor paging.
    let (status, page) = call(
        &state,
        "i/signin-history",
        serde_json::json!({ "limit": 1, "untilId": rows[0]["id"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page.as_array().unwrap().len(), 1);
    assert_eq!(page[0]["success"], true);
    // Anonymous → 401.
    let (status, _) = call(&state, "i/signin-history", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // change-password: wrong current → 401, token still valid.
    let (status, _) = call(
        &state,
        "i/change-password",
        serde_json::json!({ "currentPassword": "nope", "newPassword": "n3w-pw" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    clock.advance_ms(61_000);
    let (status, _) = call(&state, "i", serde_json::json!({}), Some(&alice_token)).await;
    assert_eq!(status, StatusCode::OK);
    // Empty newPassword → INVALID_PARAM.
    let (status, value) = call(
        &state,
        "i/change-password",
        serde_json::json!({ "currentPassword": "s3cret-pw", "newPassword": "" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    clock.advance_ms(61_000);
    // Rotate the password.
    let (status, _) = call(
        &state,
        "i/change-password",
        serde_json::json!({ "currentPassword": "s3cret-pw", "newPassword": "n3w-pw" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // Old password fails, new password signs in; existing sessions stay.
    let (status, _) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    clock.advance_ms(61_000);
    let (status, _) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "alice", "password": "n3w-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&state, "i", serde_json::json!({}), Some(&alice_token)).await;
    assert_eq!(status, StatusCode::OK);

    // Mint an app grant (white-box): i/apps lists it, authorized-apps
    // shapes it, tokenId revocation kills it.
    let grant_token = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&grant_token),
            account_id: alice_id.clone(),
            permissions: vec!["read:account".to_string()],
            is_first_party: false,
            name: Some("third-party".to_string()),
            description: Some("a grant".to_string()),
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .unwrap();
    let (status, apps) = call(&state, "i/apps", serde_json::json!({}), Some(&alice_token)).await;
    assert_eq!(status, StatusCode::OK, "{apps}");
    let apps = apps.as_array().unwrap();
    assert_eq!(apps.len(), 1, "{apps:?}");
    assert_eq!(apps[0]["name"], "third-party");
    assert_eq!(apps[0]["permission"], serde_json::json!(["read:account"]));
    assert_eq!(apps[0]["description"], "a grant");
    assert!(apps[0].get("createdAt").is_some());
    let grant_id = apps[0]["id"].as_str().unwrap().to_string();
    // Bad sort → INVALID_PARAM.
    let (status, value) = call(
        &state,
        "i/apps",
        serde_json::json!({ "sort": "sideways" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let (status, authed) = call(
        &state,
        "i/authorized-apps",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{authed}");
    let authed = authed.as_array().unwrap();
    assert_eq!(authed.len(), 1, "{authed:?}");
    assert_eq!(authed[0]["id"], grant_id);
    assert_eq!(authed[0]["isAuthorized"], true);
    // Third-party credentials cannot use self-service (secure:true).
    let (status, _) = call(&state, "i/apps", serde_json::json!({}), Some(&grant_token)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // tokenId revocation is real now.
    let (status, _) = call(
        &state,
        "i/revoke-token",
        serde_json::json!({ "tokenId": grant_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, apps) = call(&state, "i/apps", serde_json::json!({}), Some(&alice_token)).await;
    assert_eq!(status, StatusCode::OK, "{apps}");
    assert!(apps.as_array().unwrap().is_empty());
    // Unknown tokenId stays a silent no-op.
    let (status, _) = call(
        &state,
        "i/revoke-token",
        serde_json::json!({ "tokenId": grant_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // regenerate-token: wrong password → 401; correct rotates native
    // tokens only (caller logged out, fresh signin works).
    let (status, _) = call(
        &state,
        "i/regenerate-token",
        serde_json::json!({ "password": "wrong" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    clock.advance_ms(61_000);
    // Re-mint a grant so rotation has something to spare.
    let grant2 = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&grant2),
            account_id: alice_id.clone(),
            permissions: vec!["read:account".to_string()],
            is_first_party: false,
            name: Some("keeper".to_string()),
            description: None,
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .unwrap();
    let (status, _) = call(
        &state,
        "i/regenerate-token",
        serde_json::json!({ "password": "n3w-pw" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(&state, "i", serde_json::json!({}), Some(&alice_token)).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "native token rotated away"
    );
    let (status, value) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "alice", "password": "n3w-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let fresh = value["token"].as_str().unwrap().to_string();
    let (status, apps) = call(&state, "i/apps", serde_json::json!({}), Some(&fresh)).await;
    assert_eq!(status, StatusCode::OK, "{apps}");
    assert_eq!(apps.as_array().unwrap().len(), 1, "app grant survived");
    let _ = fresh;
}

#[tokio::test]
async fn batch_o_invalidate_search_alias_and_ap_get() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // Bob follows Alice; Alice invalidates (removes) him.
    let (status, _) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": alice_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, follower) = call(
        &state,
        "following/invalidate",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{follower}");
    assert_eq!(follower["id"], bob_id);
    // Edge is gone: second invalidate → NOT_FOLLOWING (verbatim id).
    let (status, value) = call(
        &state,
        "following/invalidate",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NOT_FOLLOWING");
    assert_eq!(value["error"]["id"], "918faac3-074f-41ae-9c43-ed5d2946770d");
    // Self → FOLLOWER_IS_YOURSELF (verbatim id).
    let (status, value) = call(
        &state,
        "following/invalidate",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "FOLLOWER_IS_YOURSELF");
    assert_eq!(value["error"]["id"], "07dc03b9-03da-422d-885b-438313707662");
    // Unknown → NO_SUCH_USER (invalidate endpoint UUID).
    let (status, value) = call(
        &state,
        "following/invalidate",
        serde_json::json!({ "userId": "aaaaaaaaaaaaaaaa" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");
    assert_eq!(value["error"]["id"], "b77e6ae6-a3e5-40da-9cc8-c240115479cc");

    // search-by-username-and-host: single-user alias, 1-element array.
    let (status, users) = call(
        &state,
        "users/search-by-username-and-host",
        serde_json::json!({ "username": "bob" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{users}");
    assert_eq!(users.as_array().unwrap().len(), 1);
    assert_eq!(users[0]["username"], "bob");
    // Lite form.
    let (status, users) = call(
        &state,
        "users/search-by-username-and-host",
        serde_json::json!({ "username": "bob", "detail": false }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{users}");
    assert_eq!(users.as_array().unwrap().len(), 1);
    // Neither username nor host → INVALID_PARAM.
    let (status, value) = call(
        &state,
        "users/search-by-username-and-host",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");

    // ap/get mirrors ap/show's packed shape for a local user URI.
    let base = state.services.public_url.trim_end_matches('/').to_string();
    let (status, got) = call(
        &state,
        "ap/get",
        serde_json::json!({ "uri": format!("{base}/users/{alice_id}") }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["type"], "User");
    assert_eq!(got["object"]["id"], alice_id);
    // Anonymous → 401 (credential required, no open fetch).
    let (status, _) = call(
        &state,
        "ap/get",
        serde_json::json!({ "uri": format!("{base}/users/{alice_id}") }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Remote follower: edge removed, follower packed back. (The
    // federation direction — Reject authored by the followee, not an
    // Undo forged as the remote — is covered by the op construction in
    // `remove_follower`; the job executes through the same outbox path
    // as the accept/reject request flows.)
    {
        use crate::application::ports::{AccountStore, CreateAccountInput, FollowStore};
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
                    also_known_as: None,
                    moved_to_uri: None,
                    moved_at_ms: None,
                },
                None,
            )
            .expect("remote account");
        state
            .services
            .follows
            .follow(&remote.id, &alice_id, false, None, Vec::new())
            .unwrap();
        let (status, follower) = call(
            &state,
            "following/invalidate",
            serde_json::json!({ "userId": remote.id }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{follower}");
        assert_eq!(follower["id"], remote.id);
        assert!(
            state
                .services
                .follows
                .get_follow(&remote.id, &alice_id)
                .unwrap()
                .is_none()
        );
    }
}
