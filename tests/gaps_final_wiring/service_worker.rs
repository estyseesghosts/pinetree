//! Push-subscription arms: sw/register, sw/show-registration, sw/update-registration, sw/unregister.

use serde_json::json;

use super::common::Live;

pub async fn run(srv: &Live, alice_token: &str) {
    // --- sw/* ------------------------------------------------------------
    let sub = json!({"endpoint": "https://push.example/sub/1", "auth": "a", "publickey": "p"});
    let (status, _) = srv
        .api("sw/register", sub.clone(), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "sw/show-registration",
            json!({"endpoint": "https://push.example/sub/1"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["auth"].clone()), (200, json!("a")));
    let (status, _) = srv
        .api(
            "sw/show-registration",
            json!({"endpoint": "https://push.example/nope"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    let (status, _) = srv
        .api(
            "sw/update-registration",
            json!({"endpoint": "https://push.example/sub/1", "auth": "a2", "publickey": "p2"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "sw/unregister",
            json!({"endpoint": "https://push.example/sub/1"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
}
