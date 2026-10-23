//! Two-factor arms (i/2fa/*) plus my/apps and app/create + app/show.

use serde_json::json;

use super::common::{Live, code_of, now_secs};

use pinetree::application::commands::auth_2fa::totp_token;

pub async fn run(srv: &Live, alice_id: &str, alice_token: &str) {
    // --- 2fa + apps ---------------------------------------------------------
    // A failed attempt arms a ~1 s progressive delay on the shared throttle
    // keys (same class as `auth/signin`); the live server runs on the real
    // clock, so pause past the backoff before the legitimate retry.
    let (status, value) = srv
        .api(
            "i/2fa/register",
            json!({"password": "wrong"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INCORRECT_PASSWORD");
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let (status, value) = srv
        .api(
            "i/2fa/register",
            json!({"password": "pw-pw-pw-pw"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let secret = value["secret"].as_str().unwrap().to_string();
    assert!(!value["qr"].as_str().unwrap().is_empty());
    let good = totp_token(&secret, now_secs()).unwrap();
    let (status, _) = srv
        .api("i/2fa/done", json!({"token": "000000"}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let (status, _) = srv
        .api("i/2fa/done", json!({"token": good}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    // DIVERGENCE (adversarial-2 slice 6): passwordless sign-in is not
    // implemented, so enabling it is refused with INVALID_PARAM instead
    // of 204. The rest of the 2fa flow does not depend on the flag.
    let (status, value) = srv
        .api(
            "i/2fa/password-less",
            json!({"value": true}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, value) = srv
        .api(
            "i/2fa/update-key",
            json!({"password": "pw-pw-pw-pw"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let secret2 = value["secret"].as_str().unwrap().to_string();
    assert_ne!(secret, secret2);
    let good2 = totp_token(&secret2, now_secs()).unwrap();
    let (status, _) = srv
        .api("i/2fa/done", json!({"token": good2}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    // Second factor is challenge-bound (Phase 2 P0): the password step of
    // `signin-flow` issues the one-time challenge; bare `userId + token`
    // is rejected without it.
    let (status, value) = srv
        .api(
            "signin-flow",
            json!({"username": "alice", "password": "pw-pw-pw-pw"}),
            None,
        )
        .await;
    assert_eq!(status, 200);
    let challenge = value["challenge"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api(
            "i/2fa/authenticate",
            json!({"userId": alice_id, "challenge": challenge, "token": "000000"}),
            None,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_TOKEN");
    // A failed code does not consume the challenge, but it arms the
    // progressive delay: pause, then complete under the same challenge.
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let (status, value) = srv
        .api(
            "i/2fa/authenticate",
            json!({"userId": alice_id, "challenge": challenge, "token": good2}),
            None,
        )
        .await;
    assert_eq!(status, 200);
    assert!(!value["token"].as_str().unwrap().is_empty());
    let (status, _) = srv
        .api(
            "i/2fa/unregister",
            json!({"password": "pw-pw-pw-pw"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api("i/2fa/remove-key", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, value) = srv.api("my/apps", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api(
            "app/create",
            json!({"name": "helper", "permission": ["read:account"]}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert!(!value["secret"].as_str().unwrap().is_empty());
    let app_id = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api("app/show", json!({"appId": app_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["name"], json!("helper"));
    let (status, value) = srv
        .api("app/show", json!({"appId": "nope"}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_APP");
}
