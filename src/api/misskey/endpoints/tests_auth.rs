//! Endpoint tests_auth tests (split from `endpoints/mod.rs`).
//!
//! Pure move, no behavior change.

#![allow(unused_imports)]

use super::auth_http::{client_ip, miauth_check, miauth_page, miauth_submit, signin};
use super::middleware::{healthz, preflight};
use super::tests_common::{
    call, call_from, legacy_password_hash, peer_at, read_json, signup_user, test_peer, test_state,
};
use super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::api::misskey::auth::{generate_random_token, hash_token, verify_password};
use crate::api::misskey::response::RateLimiter;
use crate::application::ports::{ApiTokenRecord, PortError};
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::header::{CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use serde_json::Value;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

// Phase 2 (P0 TOTP) regression tests live in their own file so parallel
// workstreams do not collide; wired here (not in the parent-owned
// `mod.rs`) to keep the shared module list untouched.
#[cfg(test)]
#[path = "tests_auth_totp_p0.rs"]
mod tests_auth_totp_p0;
/// `signin-flow` contract (`HANDOFF_auth-signin.md`): the exact
/// request/response shapes `MkSignin.vue` drives — username-only step
/// returns the `captcha` hint, the password step mints a first-party
/// token usable as `i`, and the upstream error ids are verbatim.
#[tokio::test]
async fn signin_flow_password_login_roundtrip() {
    let (state, _) = test_state();
    let (alice_id, _) = signup_user(&state, "alice").await;

    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["finished"], false);
    assert_eq!(value["next"], "captcha");

    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["finished"], true);
    assert_eq!(value["id"], alice_id);
    let token = value["i"].as_str().expect("flow mints i").to_string();

    let (status, me) = call(&state, "i", serde_json::json!({}), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["id"], alice_id);
}

#[tokio::test]
async fn signin_flow_unknown_user_is_404_no_such_user() {
    let (state, _) = test_state();
    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "nobody", "password": "whatever" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
    assert_eq!(value["error"]["id"], "6cc579cc-885d-43d8-95c2-b8c7fc963280");
}

#[tokio::test]
async fn signin_flow_wrong_password_is_403_incorrect_password() {
    let (state, _) = test_state();
    signup_user(&state, "alice").await;
    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "wrong-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["error"]["id"], "932c904e-9460-45b7-9ce6-7ed33be7eb2c");
}

#[tokio::test]
async fn signin_flow_missing_or_misshaped_fields_are_empty_400() {
    let (state, _) = test_state();
    signup_user(&state, "alice").await;
    for body in [
        serde_json::json!({}),
        serde_json::json!({ "username": 42 }),
        serde_json::json!({ "username": "alice", "password": 42 }),
        serde_json::json!({ "username": "alice", "password": "s3cret-pw", "token": 42 }),
    ] {
        let (status, value) = call(&state, "signin-flow", body, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        // Plain empty 400: no JSON error envelope, like upstream's
        // `reply.code(400); return;`.
        assert!(value.is_null(), "{value}");
    }
}

#[tokio::test]
async fn signin_flow_empty_username_is_404_no_such_user() {
    // Upstream only 400s a non-string `username`; `""` proceeds to the
    // lookup and answers 404 like any unknown name.
    let (state, _) = test_state();
    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
    assert_eq!(value["error"]["id"], "6cc579cc-885d-43d8-95c2-b8c7fc963280");
}

#[tokio::test]
async fn signin_flow_suspended_is_403_before_password_check() {
    let (state, _) = test_state();
    let (alice_id, _) = signup_user(&state, "alice").await;
    state
        .services
        .suspend_user(&alice_id, &alice_id)
        .expect("suspend alice");
    // Correct password still refuses: the suspension gate runs before
    // the password check (upstream order).
    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["error"]["id"], "e03a5f46-d309-4865-9b69-56282d94e1eb");
}

#[tokio::test]
async fn signin_flow_ignores_totp_token_without_2fa() {
    let (state, _) = test_state();
    signup_user(&state, "alice").await;
    // No account can enroll 2FA yet, so upstream's `!twoFactorEnabled`
    // branch (password-only, token ignored) is the whole surface.
    let (status, value) = call(
        &state,
        "signin-flow",
        serde_json::json!({ "username": "alice", "password": "s3cret-pw", "token": "123456" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["finished"], true);
}

#[tokio::test]
async fn signin_flow_repeated_failures_throttle_with_upstream_id() {
    let (mut state, clock) = test_state();
    signup_user(&state, "alice").await;
    // Small account cap; same bucket the legacy `signin` arm uses.
    state.rate_limiter =
        Arc::new(RateLimiter::new(clock.clone()).with_auth_limit("auth/signin", 3, 30, 60_000));
    let bad = serde_json::json!({ "username": "alice", "password": "wrong-pw" });
    let good = serde_json::json!({ "username": "alice", "password": "s3cret-pw" });

    let (status, _) = call(&state, "signin-flow", bad.clone(), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Right after a failure the progressive delay refuses with the
    // verbatim upstream id (not pinetree's `RATE_LIMIT_EXCEEDED`).
    let (status, value) = call(&state, "signin-flow", bad.clone(), None).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
    assert_eq!(value["error"]["id"], "22d05606-fbcf-421a-a2db-b32610dcfd1b");

    clock.advance_ms(1_000);
    let (status, _) = call(&state, "signin-flow", bad.clone(), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    clock.advance_ms(2_000);
    let (status, _) = call(&state, "signin-flow", bad, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Account cap reached: even the right password waits for the window.
    let (status, value) = call(&state, "signin-flow", good.clone(), None).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
    assert_eq!(value["error"]["id"], "22d05606-fbcf-421a-a2db-b32610dcfd1b");
    clock.advance_ms(60_000);
    let (status, value) = call(&state, "signin-flow", good, None).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["finished"], true);
}

#[tokio::test]
async fn signin_with_passkey_is_400_unsupported() {
    let (state, _) = test_state();
    let (status, value) = call(&state, "signin-with-passkey", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"]["code"], "INVALID_PARAM");
}

#[tokio::test]
async fn preflight_answers_204_with_cors_headers() {
    let response = preflight().await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let headers = response.headers();
    assert_eq!(headers["access-control-allow-origin"], "*");
    assert!(headers.contains_key("access-control-allow-methods"));
    assert!(headers.contains_key("access-control-allow-headers"));
}

/// Slice 10: API writes do not wait for peers. The follower's inbox
/// accepts the connection, reads the request and then stalls; the worker
/// is busy with it in the background while `notes/create` returns fast.
#[tokio::test]
async fn slice10_api_latency_independent_of_slow_peer() {
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    use crate::application::ports::{AccountStore, CreateAccountInput, FollowStore};

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind slow peer");
    let addr = listener.local_addr().expect("slow peer addr");
    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            std::thread::sleep(Duration::from_secs(3));
        }
    });

    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let remote = state
        .services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: "slowpoke".to_string(),
                host: Some("slow.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://slow.example/users/slowpoke".to_string()),
                inbox: Some(format!("http://{addr}/inbox")),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
                also_known_as: None,
                moved_to_uri: None,
                moved_at_ms: None,
            },
            None,
        )
        .expect("remote follower");
    state
        .services
        .follows
        .follow(&remote.id, &alice_id, false, None, Vec::new())
        .expect("remote account follows alice");

    // Production runs the worker on a background loop; do the same here.
    let _worker = state.worker.clone().spawn(Duration::from_millis(10));

    let started = Instant::now();
    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "hello slow peers" }),
        Some(&alice_token),
    )
    .await;
    let elapsed = started.elapsed();
    assert_eq!(status, StatusCode::OK, "{value}");
    assert!(
        elapsed < Duration::from_millis(500),
        "notes/create waited on a federation peer: {elapsed:?}"
    );
}

#[tokio::test]
async fn healthz_returns_ok() {
    assert_eq!(healthz().await, "ok");
}

#[tokio::test]
async fn meta_reports_version() {
    let (state, _) = test_state();
    let (status, value) = call(&state, "meta", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["name"], "Pinetree");
    assert!(value["version"].as_str().unwrap().starts_with("2026"));
    let (status, lite) = call(&state, "meta", serde_json::json!({ "detail": false }), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(lite.get("features").is_none());
}

#[test]
fn dispatch_fits_bounded_stack() {
    // Guardrail for the 2026-10-21 stack overflow: the sync `/api`
    // dispatch match needs just over 2 MiB in dev builds, which
    // aborted every `/api` call on tokio's default 2 MiB threads
    // (production now runs 8 MiB; see `main.rs`). This test pins the
    // need below 4 MiB: representative dispatches must complete on a
    // 4 MiB thread under a 4 MiB-stack runtime. If it fails, the match
    // grew again — split it instead of raising the runtime further.
    std::thread::Builder::new()
        .stack_size(4 << 20)
        .spawn(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_stack_size(4 << 20)
                .enable_all()
                .build()
                .expect("bounded test runtime");
            rt.block_on(async {
                let (state, _) = test_state();
                let (status, _) = call(&state, "meta", serde_json::json!({}), None).await;
                assert_eq!(status, StatusCode::OK);
                let (status, _) = call(
                    &state,
                    "username/available",
                    serde_json::json!({ "username": "alice" }),
                    None,
                )
                .await;
                assert_eq!(status, StatusCode::OK);
            });
        })
        .expect("spawn bounded-stack thread")
        .join()
        .expect("bounded-stack dispatch completes");
}

#[tokio::test]
async fn signup_signin_and_i_roundtrip() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;

    let (status, me) = call(&state, "i", serde_json::json!({}), Some(&alice_token)).await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["id"], alice_id);
    assert_eq!(me["username"], "alice");

    // No credential -> 401.
    let (status, value) = call(&state, "i", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "CREDENTIAL_REQUIRED");

    // Bad token -> 401.
    let (status, _) = call(&state, "i", serde_json::json!({}), Some("nope")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Web signin route returns id/token/i/user + cookie.
    let response = signin(
        State(state.clone()),
        test_peer(),
        HeaderMap::new(),
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "username": "alice", "password": "s3cret-pw",
            }))
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    let (status, value, headers) = read_json(response).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["id"], alice_id);
    assert!(value["token"].as_str().is_some());
    assert_eq!(value["i"], value["token"]);
    assert_eq!(value["user"]["username"], "alice");
    assert!(headers.get(SET_COOKIE).is_some());

    // Wrong password -> 401.
    let (status, _) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "alice", "password": "wrong" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn slice9_legacy_hash_verifies_then_upgrades_to_argon2id_on_signin() {
    let (state, _) = test_state();
    let legacy = legacy_password_hash("00112233445566778899aabbccddeeff", "legacy-pw");
    let account = state
        .services
        .create_account("legacy_user", Some(&legacy))
        .expect("create legacy account");
    let response = signin(
        State(state.clone()),
        test_peer(),
        HeaderMap::new(),
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "username": "legacy_user", "password": "legacy-pw",
            }))
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    let (status, value, _) = read_json(response).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["id"], account.id);
    let stored = state
        .services
        .auth
        .get_password_hash(&account.id)
        .unwrap()
        .expect("stored hash");
    assert!(stored.starts_with("$argon2id$"), "{stored}");
    assert!(verify_password("legacy-pw", &stored));
    assert!(!verify_password("wrong", &stored));
}

#[tokio::test]
async fn slice9_signup_stores_argon2id_and_rejects_wrong_password() {
    let (state, _) = test_state();
    let (account_id, _) = signup_user(&state, "slice9_new").await;
    let stored = state
        .services
        .auth
        .get_password_hash(&account_id)
        .unwrap()
        .expect("stored hash");
    assert!(stored.starts_with("$argon2id$"), "{stored}");
    let (status, _) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "slice9_new", "password": "wrong" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn slice9_signin_brute_force_returns_rate_limit_exceeded() {
    let (mut state, clock) = test_state();
    signup_user(&state, "victim").await;
    // Three failures per account trip the cap; the per-IP cap is generous here.
    state.rate_limiter =
        Arc::new(RateLimiter::new(clock.clone()).with_auth_limit("auth/signin", 3, 30, 60_000));
    let bad = serde_json::json!({ "username": "victim", "password": "guess" });
    let good = serde_json::json!({ "username": "victim", "password": "s3cret-pw" });

    let (status, _) = call(&state, "signin", bad.clone(), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Right after a failure the progressive delay refuses every attempt.
    let (status, value) = call(&state, "signin", good.clone(), None).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
    assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");

    clock.advance_ms(1_000);
    let (status, _) = call(&state, "signin", bad.clone(), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    clock.advance_ms(2_000);
    let (status, _) = call(&state, "signin", bad, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Account cap reached: even the right password waits for the window.
    let (status, value) = call(&state, "signin", good.clone(), None).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
    clock.advance_ms(60_000);
    let (status, value) = call(&state, "signin", good, None).await;
    assert_eq!(status, StatusCode::OK, "{value}");
}

#[tokio::test]
async fn slice9_signin_per_ip_bucket_trips_across_accounts() {
    let (mut state, clock) = test_state();
    signup_user(&state, "ipa").await;
    signup_user(&state, "ipb").await;
    // One failure per IP is enough; the per-account cap is generous.
    state.rate_limiter =
        Arc::new(RateLimiter::new(clock.clone()).with_auth_limit("auth/signin", 100, 1, 60_000));
    let (status, _) = call_from(
        &state,
        "signin",
        serde_json::json!({ "username": "ipa", "password": "x" }),
        None,
        peer_at("198.51.100.20"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // A different account from the same IP is refused by the IP bucket.
    let (status, value) = call_from(
        &state,
        "signin",
        serde_json::json!({ "username": "ipb", "password": "x" }),
        None,
        peer_at("198.51.100.20"),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
    assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");
    // Another IP still signs in.
    let (status, value) = call_from(
        &state,
        "signin",
        serde_json::json!({ "username": "ipb", "password": "s3cret-pw" }),
        None,
        peer_at("198.51.100.21"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
}

#[test]
fn slice9_spoofed_xff_from_untrusted_peer_is_ignored() {
    let peer: SocketAddr = "203.0.113.9:4000".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-for", HeaderValue::from_static("192.0.2.77"));
    headers.insert("x-real-ip", HeaderValue::from_static("192.0.2.78"));
    assert_eq!(client_ip(peer, &headers, &[]), "203.0.113.9");
    // Another proxy being trusted does not make this untrusted peer's header count.
    let trusted = ["10.0.0.1".parse::<IpAddr>().unwrap()];
    assert_eq!(client_ip(peer, &headers, &trusted), "203.0.113.9");
}

#[test]
fn slice9_xff_honored_only_from_trusted_proxy() {
    let proxy: SocketAddr = "10.0.0.1:4000".parse().unwrap();
    let trusted = ["10.0.0.1".parse::<IpAddr>().unwrap()];
    let mut headers = HeaderMap::new();
    // The client prepended a fake hop; the trusted proxy appended the real client.
    headers.insert(
        "x-forwarded-for",
        HeaderValue::from_static("198.51.100.1, 192.0.2.77"),
    );
    assert_eq!(client_ip(proxy, &headers, &trusted), "192.0.2.77");
    // A trusted hop on the right is skipped.
    headers.insert(
        "x-forwarded-for",
        HeaderValue::from_static("192.0.2.77, 10.0.0.1"),
    );
    assert_eq!(client_ip(proxy, &headers, &trusted), "192.0.2.77");
    // All hops trusted: the leftmost entry is used.
    let both = [
        "10.0.0.1".parse::<IpAddr>().unwrap(),
        "10.0.0.2".parse().unwrap(),
    ];
    let second: SocketAddr = "10.0.0.2:4000".parse().unwrap();
    headers.insert(
        "x-forwarded-for",
        HeaderValue::from_static("10.0.0.1, 10.0.0.2"),
    );
    assert_eq!(client_ip(second, &headers, &both), "10.0.0.1");
    // An unparseable hop makes the header untrustworthy: use the peer.
    headers.insert(
        "x-forwarded-for",
        HeaderValue::from_static("203.0.113.5, garbage"),
    );
    assert_eq!(client_ip(second, &headers, &both), "10.0.0.2");
}

#[tokio::test]
async fn miauth_gen_token_and_check_are_one_shot() {
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "dave").await;

    let (status, value) = call(
        &state,
        "miauth/gen-token",
        serde_json::json!({
            "session": "sess-001",
            "permission": ["read:account"],
            "name": "Test App",
        }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let app_token = value["token"].as_str().unwrap().to_string();
    assert_eq!(app_token.len(), 32);

    // The minted token carries only the granted permission.
    let (status, _) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "should fail" }),
        Some(&app_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // First check consumes the session.
    let response = miauth_check(
        State(state.clone()),
        test_peer(),
        HeaderMap::new(),
        Path("sess-001".to_string()),
    )
    .await
    .unwrap();
    let (status, value, _) = read_json(response).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["ok"], true);
    assert_eq!(value["token"], app_token);
    assert_eq!(value["user"]["username"], "dave");

    // Second check is spent.
    let response = miauth_check(
        State(state.clone()),
        test_peer(),
        HeaderMap::new(),
        Path("sess-001".to_string()),
    )
    .await
    .unwrap();
    let (status, value, _) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["ok"], false);
}

#[tokio::test]
async fn miauth_web_flow_with_callback_redirects() {
    let (state, _) = test_state();
    signup_user(&state, "erin").await;

    // Approval page renders.
    let mut query = HashMap::new();
    query.insert("name".to_string(), "Web App".to_string());
    query.insert("callback".to_string(), "https://app.example/cb".to_string());
    let response = miauth_page(
        State(state.clone()),
        Path("websess".to_string()),
        Query(query),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Approve without a callback -> confirmation page + cookie.
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    let response = miauth_submit(
        State(state.clone()),
        test_peer(),
        Path("websess".to_string()),
        headers,
        Bytes::from("username=erin&password=s3cret-pw"),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response.headers()[SET_COOKIE].to_str().unwrap().to_string();
    assert!(cookie.starts_with("pinetree_session="));

    // Session cookie authenticates API calls.
    let mut cookie_headers = HeaderMap::new();
    cookie_headers.insert(
        COOKIE,
        cookie
            .split(';')
            .next()
            .unwrap()
            .parse::<HeaderValue>()
            .unwrap(),
    );
    let response = match api_dispatch(
        State(state.clone()),
        test_peer(),
        Path("i".to_string()),
        cookie_headers,
        Bytes::from("{}"),
    )
    .await
    {
        Ok(response) => response,
        Err(err) => err.into_response(),
    };
    let (status, value, _) = read_json(response).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["username"], "erin");

    // Approve with a safe callback -> 302 to callback + session.
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    let response = miauth_submit(
        State(state.clone()),
        test_peer(),
        Path("cbsess".to_string()),
        headers,
        Bytes::from("username=erin&password=s3cret-pw&callback=https%3A%2F%2Fapp.example%2Fcb"),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        response.headers()[LOCATION],
        "https://app.example/cb?session=cbsess"
    );

    // The approved session checks out exactly once.
    let response = miauth_check(
        State(state.clone()),
        test_peer(),
        HeaderMap::new(),
        Path("cbsess".to_string()),
    )
    .await
    .unwrap();
    let (status, value, _) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["ok"], true);
}

#[tokio::test]
async fn legacy_auth_flow() {
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "frank").await;

    let (status, value) = call(
        &state,
        "auth/session/generate",
        serde_json::json!({ "appSecret": "s3cret" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let session_token = value["token"].as_str().unwrap().to_string();
    assert!(value["url"].as_str().unwrap().contains(&session_token));

    let (status, shown) = call(
        &state,
        "auth/session/show",
        serde_json::json!({ "token": session_token }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["token"], session_token);

    // userkey before accept is pending.
    let (status, value) = call(
        &state,
        "auth/session/userkey",
        serde_json::json!({ "appSecret": "s3cret", "token": session_token }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "PENDING_SESSION");

    // Unknown session -> NO_SUCH_SESSION.
    let (status, value) = call(
        &state,
        "auth/session/show",
        serde_json::json!({ "token": "missing" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
    assert_eq!(value["code"], "NO_SUCH_SESSION");

    // Accept authorizes the session…
    let (status, _) = call(
        &state,
        "auth/accept",
        serde_json::json!({ "token": session_token }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // …and userkey now yields an access token + user.
    let (status, value) = call(
        &state,
        "auth/session/userkey",
        serde_json::json!({ "appSecret": "s3cret", "token": session_token }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert!(value["accessToken"].as_str().is_some());
    assert_eq!(value["user"]["username"], "frank");
}

#[tokio::test]
async fn miauth_hardening_expiry_params_suspension() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, _bob_token) = signup_user(&state, "bob").await;

    // The approval page renders without persisting anything (Batch N
    // WS7: the GET never writes, even for attacker-chosen ids).
    let mut query = HashMap::new();
    query.insert("name".to_string(), "Probe".to_string());
    let response = miauth_page(
        State(state.clone()),
        Path("probe-sess".to_string()),
        Query(query),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        state
            .services
            .auth
            .get_miauth_session("probe-sess")
            .unwrap()
            .is_none(),
        "GET minted no row"
    );

    // gen-token requires both params (upstream parity).
    let (status, value) = call(
        &state,
        "miauth/gen-token",
        serde_json::json!({ "permission": ["read:account"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let (status, value) = call(
        &state,
        "miauth/gen-token",
        serde_json::json!({ "session": "s1" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");

    // Mint, then let the approval window lapse: check reads ok:false
    // and the row is gone (deleted on read).
    let (status, value) = call(
        &state,
        "miauth/gen-token",
        serde_json::json!({ "session": "expiry-sess", "permission": ["read:account"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    clock.advance_ms(31 * 60 * 1000);
    let response = miauth_check(
        State(state.clone()),
        test_peer(),
        HeaderMap::new(),
        Path("expiry-sess".to_string()),
    )
    .await
    .unwrap();
    let (status, value, _) = read_json(response).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["ok"], false);
    assert!(
        state
            .services
            .auth
            .get_miauth_session("expiry-sess")
            .unwrap()
            .is_none(),
        "expired row deleted on read"
    );

    // Fresh session still approves after unrelated expiry.
    let (status, value) = call(
        &state,
        "miauth/gen-token",
        serde_json::json!({ "session": "fresh-sess", "permission": ["read:account"] }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");

    // Fresh session still approves after unrelated expiry.
    let response = miauth_check(
        State(state.clone()),
        test_peer(),
        HeaderMap::new(),
        Path("fresh-sess".to_string()),
    )
    .await
    .unwrap();
    let (status, value, _) = read_json(response).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["ok"], true);

    // Suspended accounts cannot approve: submit renders the error page
    // and mints no session row.
    let (carol_id, _) = signup_user(&state, "carol").await;
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("moderator");
    state
        .services
        .suspend_user(&alice_id, &carol_id)
        .expect("suspend");
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    let response = miauth_submit(
        State(state.clone()),
        test_peer(),
        Path("susp-sess".to_string()),
        headers,
        Bytes::from("username=carol&password=s3cret-pw"),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("suspended"), "{body:?}");
    assert!(
        state
            .services
            .auth
            .get_miauth_session("susp-sess")
            .unwrap()
            .is_none(),
        "no session row minted for a suspended approver"
    );

    // Legacy sessions expire the same way: userkey on an expired token
    // reads as missing (NO_SUCH_SESSION, never PENDING).
    let (status, value) = call(
        &state,
        "auth/session/generate",
        serde_json::json!({ "appSecret": "s3cret" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let legacy_token = value["token"].as_str().unwrap().to_string();
    clock.advance_ms(31 * 60 * 1000);
    let (status, value) = call(
        &state,
        "auth/session/userkey",
        serde_json::json!({ "appSecret": "s3cret", "token": legacy_token }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
    assert_eq!(value["code"], "NO_SUCH_SESSION");

    // Wrong appSecret on userkey fails closed (late, like upstream).
    let (status, value) = call(
        &state,
        "auth/session/generate",
        serde_json::json!({ "appSecret": "s3cret" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let legacy_token = value["token"].as_str().unwrap().to_string();
    let (status, value) = call(
        &state,
        "auth/session/userkey",
        serde_json::json!({ "appSecret": "wrong", "token": legacy_token }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{value}");
}

#[tokio::test]
async fn phase3_revoke_token() {
    let (state, _) = test_state();
    let (_, token_a) = signup_user(&state, "alice").await;

    // Baseline: the token authenticates.
    let (status, _) = call(&state, "i", serde_json::json!({}), Some(&token_a)).await;
    assert_eq!(status, StatusCode::OK);

    // No params revokes the caller's own token.
    let (status, _) = call(
        &state,
        "i/revoke-token",
        serde_json::json!({}),
        Some(&token_a),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(&state, "i", serde_json::json!({}), Some(&token_a)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Explicit token revocation.
    let (_, token_b) = signup_user(&state, "bob").await;
    let (status, _) = call(
        &state,
        "i/revoke-token",
        serde_json::json!({ "token": token_b }),
        Some(&token_b),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(&state, "i", serde_json::json!({}), Some(&token_b)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Unknown tokens and tokenIds are no-op successes.
    let (_, token_c) = signup_user(&state, "carol").await;
    let (status, _) = call(
        &state,
        "i/revoke-token",
        serde_json::json!({ "token": "deadbeefdeadbeefdeadbeefdeadbeef" }),
        Some(&token_c),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(
        &state,
        "i/revoke-token",
        serde_json::json!({ "tokenId": "tid00000000000001" }),
        Some(&token_c),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // Carol's own token still works.
    let (status, _) = call(&state, "i", serde_json::json!({}), Some(&token_c)).await;
    assert_eq!(status, StatusCode::OK);

    // A third-party token cannot revoke a different token.
    let third_party = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&third_party),
            account_id: {
                let (status, me) = call(&state, "i", serde_json::json!({}), Some(&token_c)).await;
                assert_eq!(status, StatusCode::OK, "{me}");
                me["id"].as_str().unwrap().to_string()
            },
            permissions: vec!["read:account".to_string()],
            is_first_party: false,
            name: None,
            description: None,
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .unwrap();
    let (status, value) = call(
        &state,
        "i/revoke-token",
        serde_json::json!({ "token": token_c }),
        Some(&third_party),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["code"], "PERMISSION_DENIED");
    // ...but it can revoke itself.
    let (status, _) = call(
        &state,
        "i/revoke-token",
        serde_json::json!({ "token": third_party }),
        Some(&third_party),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // No credential at all is rejected.
    let (status, _) = call(&state, "i/revoke-token", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
