//! Admin emoji-list arm: v2/admin/emoji/list.

use serde_json::json;

use super::common::Live;

pub async fn run(srv: &Live, alice_token: &str) {
    // --- v2/admin/emoji/list ----------------------------------------------
    let (status, value) = srv
        .api("v2/admin/emoji/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!((status, value), (200, json!([])));
}
