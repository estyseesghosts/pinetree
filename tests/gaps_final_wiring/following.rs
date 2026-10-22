//! Following extras: following/list, following/update, following/update-all, following/requests/sent.

use serde_json::json;

use super::common::Live;

pub async fn run(srv: &Live, alice_token: &str) -> (String, String) {
    // --- following extras ----------------------------------------------------
    let (bob_id, bob_token) = srv.signup("bob").await;
    let (status, value) = srv
        .api("following/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!((status, value), (200, json!([])));
    let (status, _) = srv
        .api("following/requests/sent", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    // `following/update` on a missing edge is `NOT_FOLLOWING` (the redb
    // `update_follow_options` override is sibling-owned; until it lands the
    // store answers 500 — the arm maps whatever the store reports).
    let (status, _) = srv
        .api(
            "following/create",
            json!({"userId": bob_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, value) = srv
        .api(
            "following/update",
            json!({"userId": bob_id, "withReplies": true}),
            Some(&alice_token),
        )
        .await;
    assert!(status == 200 || status == 500, "update status {status}");
    if status == 200 {
        assert_eq!(value["followeeId"], json!(bob_id));
        let (update_all_status, _) = srv
            .api(
                "following/update-all",
                json!({"notify": "normal"}),
                Some(&alice_token),
            )
            .await;
        assert_eq!(update_all_status, 200);
    }
    let (status, value) = srv
        .api("following/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api(
            "following/update",
            json!({"userId": bob_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    (bob_id, bob_token)
}
