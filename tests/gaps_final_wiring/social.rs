//! Social reads: users/pages and users/lists/create-from-public.

use serde_json::json;

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_token: &str) {
    // --- social reads ------------------------------------------------------------
    let (status, value) = srv
        .api("users/pages", json!({"userId": "nobody"}), None)
        .await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api(
            "users/lists/create-from-public",
            json!({"name": "copy", "listId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_LIST");
}
