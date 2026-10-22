//! Admin send-email arm: permission check, address validation, job enqueue.

use serde_json::json;

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_token: &str, bob_token: &str) {
    // --- admin/send-email --------------------------------------------------------
    let (status, _) = srv
        .api(
            "admin/send-email",
            json!({"to": "ops@example.com", "subject": "hi", "text": "hello"}),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 403);
    let (status, value) = srv
        .api(
            "admin/send-email",
            json!({"to": "not-an-email", "subject": "hi", "text": "hello"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let before = srv.services.jobs.pending_count().unwrap();
    let (status, _) = srv
        .api(
            "admin/send-email",
            json!({"to": "ops@example.com", "subject": "hi", "text": "hello"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    assert_eq!(srv.services.jobs.pending_count().unwrap(), before + 1);
}
