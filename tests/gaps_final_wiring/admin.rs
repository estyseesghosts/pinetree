//! Admin families: roles, relays, ads, announcements, stats, federation, queue, abuse recipients.

use serde_json::{Value, json};

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_id: &str, alice_token: &str) {
    // --- admin families ------------------------------------------------------
    let (status, value) = srv
        .api(
            "admin/roles/create",
            json!({"name": "vip", "target": "manual"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let role_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api("admin/roles/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "admin/roles/show",
            json!({"roleId": role_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "admin/roles/assign",
            json!({"roleId": role_id, "userId": alice_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "admin/roles/users",
            json!({"roleId": role_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api(
            "admin/roles/unassign",
            json!({"roleId": role_id, "userId": alice_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "admin/roles/update",
            json!({"roleId": role_id, "name": "vip2"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "admin/roles/delete",
            json!({"roleId": role_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "admin/relays/add",
            json!({"inbox": "https://r.example/inbox"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["status"], json!("requesting"));
    let (status, value) = srv
        .api("admin/relays/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api(
            "admin/relays/remove",
            json!({"inbox": "https://r.example/inbox"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let ad = json!({
        "url": "https://ad.example/", "memo": "m", "place": "square",
        "ratio": 1.0, "priority": "high", "expiresAt": 9_999_999_999_999_i64,
        "startsAt": 1_i64, "imageUrl": "https://ad.example/i.png", "dayOfWeek": -1_i64,
    });
    let (status, value) = srv
        .api("admin/ad/create", ad.clone(), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let ad_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api("admin/ad/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let mut update = ad.clone();
    update["id"] = json!(ad_id);
    let (status, _) = srv.api("admin/ad/update", update, Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("admin/ad/refresh", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api("admin/ad/delete", json!({"id": ad_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "admin/announcements/create",
            json!({"title": "hello", "text": "world"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let ann_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api("admin/announcements/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "admin/announcements/update",
            json!({"id": ann_id, "title": "hi", "text": "there"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "admin/announcements/delete",
            json!({"id": ann_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv.api("admin/stats", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("admin/server-info", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, value) = srv
        .api(
            "admin/get-user-ips",
            json!({"userId": alice_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert!(value.is_array());
    let (status, value) = srv
        .api(
            "admin/accounts/find-by-email",
            json!({"email": "nobody@example.com"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, Value::Null));
    let (status, _) = srv
        .api(
            "admin/update-proxy-account",
            json!({"userId": Value::Null}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api("admin/proxy-account", json!({}), Some(&alice_token))
        .await;
    assert_eq!((status, value["userId"].clone()), (200, Value::Null));
    let (status, _) = srv
        .api(
            "admin/unset-mfa",
            json!({"userId": alice_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "admin/reset-password",
            json!({"userId": alice_id, "password": "new-pw-new-pw"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api("federation/instances", json!({}), Some(&alice_token))
        .await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api(
            "federation/update-instance",
            json!({"host": "remote.example", "isSuspended": true}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["isSuspended"], json!(true));
    let (status, value) = srv
        .api(
            "federation/show-instance",
            json!({"host": "remote.example"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["host"], json!("remote.example"));
    let (status, _) = srv
        .api(
            "federation/refresh-remote-instance-metadata",
            json!({"host": "remote.example"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api("admin/federation/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "federation/find-by-email",
            json!({"email": "nobody@example.com"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv.api("queue/stats", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, value) = srv
        .api("queue/deliver-delayed", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, _) = srv
        .api("admin/queue/stats", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, value) = srv
        .api(
            "abuse-report-notification-recipient/create",
            json!({"name": "ops", "method": "email", "email": "ops@example.com"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let recipient_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api(
            "abuse-report-notification-recipient/list",
            json!({}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "abuse-report-notification-recipient/show",
            json!({"id": recipient_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "abuse-report-notification-recipient/update",
            json!({"id": recipient_id, "name": "ops2", "method": "email", "email": "ops@example.com"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, value) = srv
        .api(
            "abuse-report-notification-recipient/show",
            json!({"id": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(
        code_of(&value),
        "NO_SUCH_ABUSE_REPORT_NOTIFICATION_RECIPIENT"
    );
    let (status, _) = srv
        .api(
            "abuse-report-notification-recipient/delete",
            json!({"id": recipient_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
}
