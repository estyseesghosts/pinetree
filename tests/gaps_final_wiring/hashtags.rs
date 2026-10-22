//! Hashtag arms: hashtags/list, hashtags/show, hashtags/trend, hashtags/users.

use serde_json::json;

use super::common::{Live, code_of};

pub async fn run(srv: &Live) {
    // --- hashtags ------------------------------------------------------------
    let (status, value) = srv.api("hashtags/list", json!({}), None).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv.api("hashtags/show", json!({"tag": "nope"}), None).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_HASHTAG");
    let (status, value) = srv.api("hashtags/trend", json!({}), None).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api("hashtags/users", json!({"tag": "nope"}), None)
        .await;
    assert_eq!((status, value), (200, json!([])));
}
