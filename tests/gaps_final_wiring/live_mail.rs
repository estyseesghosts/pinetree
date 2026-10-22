//! Live-mail wiring: i/update-email, verify-email, request-reset-password, reset-password.

use std::sync::Arc;

use serde_json::json;

use super::common::Live;

use pinetree::api::misskey::endpoints::set_email_sender;
use pinetree::application::ports::PasswordResetRequest;
use pinetree::services::email::EmailSender;

pub async fn run(srv: &Live, alice_id: &str, alice_token: &str) {
    // Publish the recording sender: live-mail paths send through it.
    let sender: Arc<dyn EmailSender> = srv.recorder.clone();
    set_email_sender(sender);

    // --- live mail ------------------------------------------------------
    let (status, value) = srv
        .api(
            "i/update-email",
            json!({"password": "new-pw-new-pw", "email": "alice@example.com"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["id"], json!(alice_id));
    assert_eq!(srv.recorder.sent.lock().unwrap().len(), 1);
    let account = srv
        .services
        .accounts
        .find_by_username_and_host("alice", None)
        .unwrap()
        .unwrap();
    let verify_code = account.email_verify_code.clone().unwrap();
    let (status, _) = srv
        .api("verify-email", json!({"code": verify_code}), None)
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "request-reset-password",
            json!({"username": "alice", "email": "alice@example.com"}),
            None,
        )
        .await;
    assert_eq!(status, 204);
    assert_eq!(srv.recorder.sent.lock().unwrap().len(), 2);
    let reset_subjects: Vec<String> = srv
        .recorder
        .sent
        .lock()
        .unwrap()
        .iter()
        .map(|msg| msg.subject.clone())
        .collect();
    assert!(reset_subjects.iter().any(|s| s.contains("Verify")));
    assert!(reset_subjects.iter().any(|s| s.contains("Reset")));
    // Direct row + arm round-trip for `reset-password`.
    let token = "tok-".to_string() + &"t".repeat(60);
    srv.services
        .password_resets
        .create_reset_request(PasswordResetRequest {
            id: srv.services.ids.generate(),
            user_id: alice_id.to_string(),
            token: token.clone(),
        })
        .unwrap();
    let (status, _) = srv
        .api(
            "reset-password",
            json!({"token": token, "password": "brand-new-pw"}),
            None,
        )
        .await;
    assert_eq!(status, 204);
}
