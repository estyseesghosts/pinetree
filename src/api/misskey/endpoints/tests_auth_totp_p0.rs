//! Phase 2 (P0 TOTP) failing-first regression tests.
//!
//! Covers the required invariant: password-only sign-in must not issue a
//! session for a TOTP-enabled account; the second factor completes only
//! with a short-lived, one-time, account-bound challenge issued after a
//! password success; challenges expire and are single-use; TOTP codes
//! cannot be replayed across challenges; packed `MeDetailed` security
//! flags reflect storage; suspended/deleted accounts are blocked at the
//! second factor; and the `auth/2fa*` throttle buckets are effective once
//! registered.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::header::{CONTENT_TYPE, SET_COOKIE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};

use super::super::auth_http::miauth_submit;
use super::super::tests_common::{call, signup_user, test_peer, test_state};
use crate::api::misskey::response::RateLimiter;
use crate::application::commands::auth_2fa::totp_token;
use crate::domain::id::ManualClock;

fn now_secs(state: &super::super::ApiState) -> u64 {
    (state.services.clock.now_ms() / 1000) as u64
}

async fn issue_challenge(state: &crate::api::misskey::endpoints::ApiState) -> String {
    let (status, value) = call(
        state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    value["challenge"].as_str().expect("challenge").to_string()
}

/// Enroll + verify TOTP for the account behind `user_token`; returns the
/// raw base32 secret so tests can mint codes like an authenticator app.
async fn enable_totp(
    state: &super::super::ApiState,
    user_token: &str,
    _clock: &Arc<ManualClock>,
) -> String {
    let (status, value) = call(
        state,
        "i/2fa/register",
        serde_json::json!({ "password": "s3cret-pw" }),
        Some(user_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let secret = value["secret"]
        .as_str()
        .expect("register secret")
        .to_string();
    let code = totp_token(&secret, now_secs(state)).expect("mint code");
    let (status, value) = call(
        state,
        "i/2fa/done",
        serde_json::json!({ "token": code }),
        Some(user_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    secret
}

async fn me(state: &super::super::ApiState, token: &str) -> serde_json::Value {
    let (status, value) = call(state, "i", serde_json::json!({}), Some(token)).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    value
}

/// `i/2fa/done` enables the verified secret and `MeDetailed` reflects it.
#[tokio::test]
async fn totp_done_enables_and_packer_reflects() {
    let (state, _clock) = test_state();
    let (_, token) = signup_user(&state, "alice").await;
    let before = me(&state, &token).await;
    assert_eq!(before["twoFactorEnabled"], false);
    assert_eq!(before["usePasswordLessLogin"], false);
    assert_eq!(before["securityKeys"], false);

    enable_totp(&state, &token, &_clock).await;

    let after = me(&state, &token).await;
    assert_eq!(after["twoFactorEnabled"], true);
    assert_eq!(after["usePasswordLessLogin"], false);
    assert_eq!(after["securityKeys"], false);
}

/// Legacy password-only `signin` is rejected for a TOTP-enabled account.
#[tokio::test]
async fn password_only_signin_rejected_for_totp_account() {
    let (state, clock) = test_state();
    let (_, token) = signup_user(&state, "alice").await;
    enable_totp(&state, &token, &clock).await;

    let (status, value) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"]["code"], "INVALID_PARAM");
    assert!(value.get("token").is_none(), "{value}");
    assert!(value.get("i").is_none(), "{value}");
}

/// `signin-flow` answers the password step with a one-time challenge.
#[tokio::test]
async fn signin_flow_issues_challenge_for_totp_account() {
    let (state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    enable_totp(&state, &token, &clock).await;

    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["finished"], false);
    assert_eq!(value["next"], "2fa");
    assert_eq!(value["id"], alice_id);
    let challenge = value["challenge"].as_str().expect("challenge");
    assert_eq!(challenge.len(), 32);
    assert!(value.get("i").is_none(), "{value}");

    // A TOTP `token` smuggled into the password step does not bypass the
    // challenge: the client must complete the second step explicitly.
    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({
            "username": "alice",
            "password": "s3cret-pw",
            "token": "123456",
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["finished"], false);
    assert_eq!(value["next"], "2fa");
    assert!(value["challenge"].as_str().is_some(), "{value}");
    assert!(value.get("i").is_none(), "{value}");
}

/// Password + challenge + TOTP completes the login and mints a session.
#[tokio::test]
async fn valid_two_step_login_succeeds() {
    let (state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    let secret = enable_totp(&state, &token, &clock).await;

    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let challenge = value["challenge"].as_str().expect("challenge").to_string();

    let code = totp_token(&secret, now_secs(&state)).expect("code");
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge, "token": code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let session = value["token"].as_str().expect("session token").to_string();

    let packed = me(&state, &session).await;
    assert_eq!(packed["id"], alice_id);
    assert_eq!(packed["twoFactorEnabled"], true);
}

/// `i/2fa/authenticate` without a challenge is rejected.
#[tokio::test]
async fn authenticate_missing_challenge_rejected() {
    let (state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    let secret = enable_totp(&state, &token, &clock).await;
    let code = totp_token(&secret, now_secs(&state)).expect("code");

    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "token": code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
}

/// A forged challenge is rejected.
#[tokio::test]
async fn authenticate_wrong_challenge_rejected() {
    let (state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    let secret = enable_totp(&state, &token, &clock).await;
    let code = totp_token(&secret, now_secs(&state)).expect("code");

    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({
            "userId": alice_id,
            "challenge": "00000000000000000000000000000000",
            "token": code,
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
}

/// An expired challenge is rejected even with a currently-valid code.
#[tokio::test]
async fn authenticate_expired_challenge_rejected() {
    let (state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    let secret = enable_totp(&state, &token, &clock).await;

    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let challenge = value["challenge"].as_str().expect("challenge").to_string();

    // Past the 5-minute challenge TTL.
    clock.advance_ms(6 * 60 * 1000);
    let code = totp_token(&secret, now_secs(&state)).expect("code");
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge, "token": code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
}

/// A wrong TOTP code is rejected with `INVALID_TOKEN`.
#[tokio::test]
async fn authenticate_wrong_token_rejected() {
    let (state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    enable_totp(&state, &token, &clock).await;

    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let challenge = value["challenge"].as_str().expect("challenge").to_string();

    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge, "token": "000000" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"]["code"], "INVALID_TOKEN");
}

/// A consumed challenge cannot mint a second session.
#[tokio::test]
async fn challenge_replay_rejected() {
    let (state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    let secret = enable_totp(&state, &token, &clock).await;

    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let challenge = value["challenge"].as_str().expect("challenge").to_string();
    let code = totp_token(&secret, now_secs(&state)).expect("code");

    let (status, first) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge, "token": code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert!(first["token"].as_str().is_some());

    // Same challenge + same code: the challenge is spent.
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge, "token": code }),
        None,
    )
    .await;
    assert_ne!(status, StatusCode::OK, "{value}");
    assert!(value.get("token").is_none(), "{value}");
}

/// A TOTP code cannot be replayed under a fresh challenge; the account is
/// not bricked afterwards (a later code still works).
#[tokio::test]
async fn totp_code_replay_new_challenge_rejected() {
    let (state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    let secret = enable_totp(&state, &token, &clock).await;

    let challenge_a = issue_challenge(&state).await;
    let code = totp_token(&secret, now_secs(&state)).expect("code");
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge_a, "token": code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");

    // Same code under a fresh challenge: replay, rejected.
    let challenge_b = issue_challenge(&state).await;
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge_b, "token": code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");

    // A later code still completes a fresh challenge.
    clock.advance_ms(60 * 1000);
    let challenge_c = issue_challenge(&state).await;
    let fresh = totp_token(&secret, now_secs(&state)).expect("fresh code");
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge_c, "token": fresh }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
}

/// Removing TOTP restores plain password login.
#[tokio::test]
async fn unregister_restores_password_login() {
    let (state, clock) = test_state();
    let (_, token) = signup_user(&state, "alice").await;
    enable_totp(&state, &token, &clock).await;

    let (status, value) = call(
        &state,
        "i/2fa/unregister",
        serde_json::json!({ "password": "s3cret-pw" }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");

    let (status, value) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert!(value["token"].as_str().is_some());

    let packed = me(&state, &token).await;
    assert_eq!(packed["twoFactorEnabled"], false);
}

/// Suspended and deleted accounts are blocked at both factors.
#[tokio::test]
async fn suspended_and_deleted_blocked() {
    let (state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    let secret = enable_totp(&state, &token, &clock).await;

    // Issue a challenge while the account is healthy.
    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let challenge = value["challenge"].as_str().expect("challenge").to_string();

    state
        .services
        .suspend_user(&alice_id, &alice_id)
        .expect("suspend alice");
    let code = totp_token(&secret, now_secs(&state)).expect("code");
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge, "token": code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");

    // Past the 1 s progressive delay the failed attempt above armed on the
    // shared test IP (same throttle class as `auth/signin`).
    clock.advance_ms(5_000);

    // Password step also refuses suspended accounts.
    let (status, _) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Deleted accounts fail closed at both factors. (Storage marks a
    // deleted account suspended too, so — like `authenticate()` — the
    // suspended gate answers first with 403.)
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let bob_secret = enable_totp(&state, &bob_token, &clock).await;
    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "bob", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let bob_challenge = value["challenge"].as_str().expect("challenge").to_string();
    state
        .services
        .delete_account(None, &bob_id, None)
        .expect("delete bob");
    let bob_code = totp_token(&bob_secret, now_secs(&state)).expect("code");
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": bob_id, "challenge": bob_challenge, "token": bob_code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["error"]["code"], "USER_SUSPENDED", "{value}");

    // Past the progressive delay the failed attempt above armed on the
    // shared test IP.
    clock.advance_ms(5_000);

    let (status, value) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "bob", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_ne!(status, StatusCode::OK, "{value}");
}

/// Passwordless mode never downgrades a TOTP account to password-only.
#[tokio::test]
async fn passwordless_does_not_downgrade() {
    let (state, clock) = test_state();
    let (_, token) = signup_user(&state, "alice").await;
    let secret = enable_totp(&state, &token, &clock).await;

    let (status, value) = call(
        &state,
        "i/2fa/password-less",
        serde_json::json!({ "value": true }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");

    let packed = me(&state, &token).await;
    assert_eq!(packed["twoFactorEnabled"], true);
    assert_eq!(packed["usePasswordLessLogin"], true);

    // Password-only is still rejected.
    let (status, value) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");

    // The challenge flow still completes.
    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let challenge = value["challenge"].as_str().expect("challenge").to_string();
    let code = totp_token(&secret, now_secs(&state)).expect("code");
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({
            "userId": packed["id"].as_str().unwrap(),
            "challenge": challenge,
            "token": code,
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
}

/// MiAuth password approval mints nothing for a TOTP-enabled account.
#[tokio::test]
async fn miauth_submit_blocked_for_totp_account() {
    let (state, clock) = test_state();
    let (_, token) = signup_user(&state, "alice").await;
    enable_totp(&state, &token, &clock).await;

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    let response = miauth_submit(
        State(state.clone()),
        test_peer(),
        Path("totpsess".to_string()),
        headers,
        Bytes::from("username=alice&password=s3cret-pw"),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().get(SET_COOKIE).is_none(),
        "no browser session for TOTP accounts"
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("two-factor") || text.contains("two factor"),
        "{text}"
    );
}

/// Failure-counted `auth/2fa*` buckets refuse once registered and capped.
#[tokio::test]
async fn second_factor_throttle_effective() {
    let (mut state, clock) = test_state();
    let (alice_id, token) = signup_user(&state, "alice").await;
    let secret = enable_totp(&state, &token, &clock).await;

    // Register the production buckets the parent wires into
    // `with_misskey_defaults` (proposed snippet): small caps for the test.
    state.rate_limiter = Arc::new(
        RateLimiter::new(clock.clone())
            .with_auth_limit("auth/2fa-authenticate", 2, 30, 60_000)
            .with_auth_limit("auth/2fa", 1, 30, 60_000),
    );

    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let challenge = value["challenge"].as_str().expect("challenge").to_string();

    // Two counted failures (backoff waits between them), then even a valid
    // code is refused while the account cap holds.
    let bad = serde_json::json!({ "userId": alice_id, "challenge": challenge, "token": "000000" });
    let (status, _) = call(&state, "i/2fa/authenticate", bad.clone(), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    clock.advance_ms(1_000);
    let (status, _) = call(&state, "i/2fa/authenticate", bad.clone(), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    clock.advance_ms(2_000);
    let code = totp_token(&secret, now_secs(&state)).expect("code");
    let (status, value) = call(
        &state,
        "i/2fa/authenticate",
        serde_json::json!({ "userId": alice_id, "challenge": challenge, "token": code }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");

    // The `auth/2fa` management bucket counts wrong-password attempts.
    let (status, _) = call(
        &state,
        "i/2fa/register",
        serde_json::json!({ "password": "wrong-pw" }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, value) = call(
        &state,
        "i/2fa/register",
        serde_json::json!({ "password": "wrong-pw" }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
}

/// Ordinary `i/2fa/*` buckets refuse once registered and capped.
#[tokio::test]
async fn second_factor_ordinary_bucket_effective() {
    let (mut state, clock) = test_state();
    state.rate_limiter =
        Arc::new(RateLimiter::new(clock.clone()).with_limit("i/2fa/done", 1, 3_600_000, None));
    let (_, token) = signup_user(&state, "alice").await;
    let (status, value) = call(
        &state,
        "i/2fa/register",
        serde_json::json!({ "password": "s3cret-pw" }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let secret = value["secret"].as_str().expect("secret").to_string();
    let code = totp_token(&secret, now_secs(&state)).expect("code");

    let (status, value) = call(
        &state,
        "i/2fa/done",
        serde_json::json!({ "token": code }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");

    // Second call trips the registered ordinary bucket before any crypto.
    let (status, value) = call(
        &state,
        "i/2fa/done",
        serde_json::json!({ "token": code }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
}

/// Unknown accounts and TOTP-less accounts answer identically: no oracle.
#[tokio::test]
async fn authenticate_without_totp_is_no_such_2fa() {
    let (state, clock) = test_state();
    let (alice_id, _) = signup_user(&state, "alice").await;

    for user_id in [alice_id.as_str(), "00000000000000000000000000"] {
        let (status, value) = call(
            &state,
            "i/2fa/authenticate",
            serde_json::json!({
                "userId": user_id,
                "challenge": "00000000000000000000000000000000",
                "token": "123456",
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["error"]["code"], "NO_SUCH_2FA", "{value}");
        // Each failure arms a progressive delay on the shared test IP;
        // advance past it so the next probe tests the oracle, not the
        // throttle.
        clock.advance_ms(5_000);
    }
}
