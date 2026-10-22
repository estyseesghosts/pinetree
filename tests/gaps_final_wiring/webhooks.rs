//! Webhook arms: system-webhook/* (admin) and webhooks/* (owner).

use serde_json::json;

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_token: &str, bob_token: &str) {
    // --- system-webhook/* (admin) + webhooks/* (owner) --------------------------
    let (status, _) = srv
        .api(
            "system-webhook/create",
            json!({"name": "ops", "url": "https://ops.example/hook", "trigger": "abuseReport"}),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 403);
    let (status, value) = srv
        .api(
            "system-webhook/create",
            json!({"name": "ops", "url": "https://ops.example/hook", "trigger": "nope"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, value) = srv
        .api(
            "system-webhook/create",
            json!({"name": "ops", "url": "https://ops.example/hook", "trigger": "abuseReport"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["trigger"], json!("abuseReport"));
    assert!(!value["secret"].as_str().unwrap().is_empty());
    let hook_id = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api("system-webhook/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, value) = srv
        .api(
            "system-webhook/show",
            json!({"webhookId": hook_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["name"].clone()), (200, json!("ops")));
    let (status, value) = srv
        .api(
            "system-webhook/update",
            json!({"webhookId": hook_id, "isActive": false}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["isActive"].clone()), (200, json!(false)));
    let (status, value) = srv
        .api(
            "system-webhook/show",
            json!({"webhookId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_WEBHOOK");
    assert_eq!(
        value["error"]["id"],
        json!("722a824a-ba81-4d2e-8f84-bbc358bfdb42")
    );
    let (status, _) = srv
        .api(
            "system-webhook/delete",
            json!({"webhookId": hook_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "system-webhook/delete",
            json!({"webhookId": hook_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_WEBHOOK");
    // Owner webhooks: five succeed, the sixth hits the cap.
    for name in ["w1", "w2", "w3", "w4", "w5"] {
        let (status, _) = srv
            .api(
                "webhooks/create",
                json!({"name": name, "url": "https://bob.example/hook", "trigger": "mention"}),
                Some(&bob_token),
            )
            .await;
        assert_eq!(status, 200, "{name}");
    }
    let (status, value) = srv
        .api(
            "webhooks/create",
            json!({"name": "w6", "url": "https://bob.example/hook", "trigger": "mention"}),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "TOO_MANY_WEBHOOKS");
    assert_eq!(
        value["error"]["id"],
        json!("adadc6d4-090b-4f02-a74a-fdb5d5a1f4c7")
    );
    let (status, value) = srv.api("webhooks/list", json!({}), Some(&bob_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 5);
    let first_id = value[0]["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api(
            "webhooks/show",
            json!({"webhookId": first_id}),
            Some(&bob_token),
        )
        .await;
    assert_eq!((status, value["name"].clone()), (200, json!("w1")));
    // Alice cannot see bob's webhook (foreign reads as not-found).
    let (status, value) = srv
        .api(
            "webhooks/show",
            json!({"webhookId": first_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_WEBHOOK");
    let (status, _) = srv
        .api(
            "webhooks/delete",
            json!({"webhookId": first_id}),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 204);
}
