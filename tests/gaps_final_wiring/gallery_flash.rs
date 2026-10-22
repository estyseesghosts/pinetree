//! Gallery and Play arms: gallery/* and flash/*.

use serde_json::json;

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_token: &str, file_id: &str) {
    // --- gallery + flash ------------------------------------------------------------
    let (status, value) = srv
        .api(
            "gallery/posts/create",
            json!({"title": "t", "fileIds": []}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "FILE_REQUIRED");
    let (status, value) = srv
        .api(
            "gallery/posts/create",
            json!({"title": "t", "fileIds": [file_id]}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["likedCount"], json!(0));
    assert!(value.get("isLiked").is_none());
    let post_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api("gallery/posts/show", json!({"postId": "nope"}), None)
        .await;
    assert_eq!(status, 400);
    let (status, value) = srv
        .api("gallery/posts/show", json!({"postId": post_id}), None)
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["title"], json!("t"));
    let (status, _) = srv
        .api(
            "gallery/posts/update",
            json!({"postId": post_id, "title": "t2"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "gallery/posts/like",
            json!({"postId": post_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "gallery/posts/like",
            json!({"postId": post_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "ALREADY_LIKED");
    let (status, _) = srv.api("gallery/featured", json!(null), None).await;
    assert_eq!(status, 200);
    let (status, _) = srv.api("gallery/popular", json!(null), None).await;
    assert_eq!(status, 200);
    let (status, value) = srv.api("gallery/posts", json!(null), None).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api(
            "gallery/posts/unlike",
            json!({"postId": post_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "gallery/posts/unlike",
            json!({"postId": post_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NOT_LIKED");
    let (status, _) = srv
        .api(
            "gallery/posts/delete",
            json!({"postId": post_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "flash/create",
            json!({"title": "play", "script": "let x = 1;", "visibility": "bogus"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, value) = srv
        .api(
            "flash/create",
            json!({"title": "play", "script": "let x = 1;"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let flash_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api("flash/show", json!({"flashId": "nope"}), None)
        .await;
    assert_eq!(status, 400);
    let (status, _) = srv
        .api(
            "flash/update",
            json!({"flashId": flash_id, "summary": "s"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "flash/like",
            json!({"flashId": flash_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv.api("flash/featured", json!(null), None).await;
    assert_eq!(status, 200);
    let (status, value) = srv.api("flash/my", json!(null), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, value) = srv
        .api("flash/my-likes", json!(null), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api(
            "flash/unlike",
            json!({"flashId": flash_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "flash/delete",
            json!({"flashId": flash_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
}
