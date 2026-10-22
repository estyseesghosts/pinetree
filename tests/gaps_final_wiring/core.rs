//! Core dispatcher arms: ping, endpoint(s), retention, reset-db, plus the alice admin bootstrap.

use serde_json::json;

use super::common::{Live, code_of};

pub async fn run(srv: &Live) -> (String, String) {
    // --- misc -----------------------------------------------------------
    let (status, value) = srv.api("ping", json!({}), None).await;
    assert_eq!((status, value), (200, json!({})));
    let (status, value) = srv.api("endpoints", json!({}), None).await;
    assert_eq!(status, 200);
    assert!(value.as_array().unwrap().iter().any(|n| n == "ping"));
    let (status, value) = srv.api("endpoint", json!({"endpoint": "ping"}), None).await;
    assert_eq!((status, value["name"].clone()), (200, json!("ping")));
    let (status, value) = srv.api("endpoint", json!({"endpoint": "nope"}), None).await;
    assert_eq!(status, 404);
    assert_eq!(code_of(&value), "NO_SUCH_ENDPOINT");
    let (status, value) = srv.api("retention", json!({}), Some("x")).await;
    assert_eq!(status, 401);
    assert_eq!(code_of(&value), "AUTHENTICATION_FAILED");
    let (alice_id, alice_token) = srv.signup("alice").await;
    let (status, value) = srv.api("retention", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 403);
    assert_eq!(code_of(&value), "PERMISSION_DENIED");
    srv.services.set_role(None, &alice_id, true, true).unwrap();
    let (status, value) = srv.api("retention", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value), (200, json!({})));
    let (status, value) = srv
        .api(
            "retention",
            json!({"policy": {"keepDays": 90}}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, json!({"keepDays": 90})));
    let (status, value) = srv.api("reset-db", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    (alice_id, alice_token)
}
