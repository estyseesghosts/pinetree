//! Endpoint tests_admin tests (split from `endpoints/mod.rs`).
//!
//! Pure move, no behavior change.

#![allow(unused_imports)]

use super::tests_common::{call, capture_inbox, signup_user, test_state};
use super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::activitypub::actor as ap_actor;
use crate::application::ports::PortError;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use serde_json::Value;
use std::sync::Arc;
/// Batch M §3.2 `admin/show-moderation-logs`: moderator-only, upstream
/// packing, cursor paging, and the upstream `type`-miss behavior.
#[tokio::test]
async fn admin_show_moderation_logs_moderator_only() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // Plain users are refused the log itself.
    let (status, value) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["code"], "PERMISSION_DENIED");

    // Two moderator actions generate two rows (the `setRole` row and
    // the `rejectQuotesUser` row).
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("set role");
    let (status, _) = call(
        &state,
        "admin/reject-quotes",
        serde_json::json!({ "userId": bob_id, "rejectQuotes": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    let rows = logs.as_array().unwrap();
    assert_eq!(rows.len(), 2, "newest first: {logs}");
    assert_eq!(rows[0]["type"], "rejectQuotesUser");
    assert_eq!(rows[1]["type"], "setRole");
    assert_eq!(rows[1]["info"]["isModerator"], serde_json::json!(true));
    assert_eq!(rows[1]["user"], serde_json::json!(null)); // operator (CLI) row
    for row in rows {
        for key in ["id", "createdAt", "type", "info", "userId", "user"] {
            assert!(row.get(key).is_some(), "row has {key}: {row}");
        }
    }

    // `userId` filters on the acting moderator; an unknown `type`
    // matches no rows (upstream `log.type = :type`).
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    assert_eq!(logs.as_array().unwrap().len(), 1, "{logs}");
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "type": "noSuchAction" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    assert!(logs.as_array().unwrap().is_empty(), "{logs}");

    // Cursor paging: `untilId` at the newest row returns the older one.
    let newest_id = rows[0]["id"].as_str().unwrap();
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "untilId": newest_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    let page = logs.as_array().unwrap();
    assert_eq!(page.len(), 1, "{logs}");
    assert_eq!(page[0]["type"], "setRole");
}

/// §6 per-user `defaultReaction` heart-click (`docs/misskey-parity-plan.md`
/// §6 DIVERGENCE: `notes/reactions/create` leaves `reaction` optional and
/// resolves an absent param server-side to `defaultReaction ?? FALLBACK`).
// --- Batch M WP2: registration policy + invite codes -----------------

#[tokio::test]
async fn wp2_signup_policy_gate_and_invite_claim() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;

    // Non-admin: 403 on admin/update-meta.
    let (status, value) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({ "disableRegistration": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");

    // Admin closes registration; meta reflects the setting.
    state
        .services
        .set_role(None, &alice_id, true, false)
        .expect("set role");
    let (status, value) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({ "disableRegistration": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    let (status, meta) = call(&state, "meta", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{meta}");
    assert_eq!(meta["disableRegistration"], serde_json::json!(true));

    // Closed with no invite: plain empty 400 (upstream
    // `SignupApiService` gate), no account created.
    let (status, value) = call(
        &state,
        "signup",
        serde_json::json!({ "username": "nosuchluck", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert!(
        value.get("error").is_none(),
        "the registration gate answers a plain 400: {value}"
    );
    assert!(
        state
            .services
            .accounts
            .find_by_handle("nosuchluck")
            .unwrap()
            .is_none(),
        "a refused signup creates nothing"
    );

    // A bad code is refused the same way.
    let (status, value) = call(
        &state,
        "signup",
        serde_json::json!({
            "username": "nosuchluck2",
            "password": "s3cret-pw",
            "invitationCode": "NOTACODE0AAAA",
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");

    // Moderator issues one code; exactly one signup consumes it.
    let (status, codes) = call(
        &state,
        "admin/invite/create",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{codes}");
    let code = codes.as_array().unwrap()[0]["code"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, value) = call(
        &state,
        "signup",
        serde_json::json!({
            "username": "newbie",
            "password": "s3cret-pw",
            "invitationCode": code,
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["username"], serde_json::json!("newbie"));
    let (status, value) = call(
        &state,
        "signup",
        serde_json::json!({
            "username": "latecomer",
            "password": "s3cret-pw",
            "invitationCode": code,
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert!(
        state
            .services
            .accounts
            .find_by_handle("latecomer")
            .unwrap()
            .is_none(),
        "the second consume creates nothing"
    );

    // Reopened: signup needs no code again.
    let (status, _) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({ "disableRegistration": false }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "signup",
        serde_json::json!({ "username": "freedom", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
}

#[tokio::test]
async fn wp2_admin_accounts_create_admin_only_bypasses_gate() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    // Non-admin: 403 (upstream ACCESS_DENIED).
    let (status, value) = call(
        &state,
        "admin/accounts/create",
        serde_json::json!({ "username": "made1", "password": "s3cret-pw" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");

    // Close registration: the public path is shut…
    state
        .services
        .set_role(None, &alice_id, true, false)
        .expect("set role");
    let (status, _) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({ "disableRegistration": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // …but admin/accounts/create creates accounts anyway (its
    // purpose: it bypasses the gate).
    let (status, value) = call(
        &state,
        "admin/accounts/create",
        serde_json::json!({ "username": "made2", "password": "s3cret-pw" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["username"], serde_json::json!("made2"));
    assert!(value["token"].is_string(), "{value}");

    // The `createAccount` row committed with the account.
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "type": "createAccount" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    let rows = logs.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{logs}");
    assert_eq!(rows[0]["type"], "createAccount");
    assert_eq!(rows[0]["info"]["userUsername"], serde_json::json!("made2"));
}

#[tokio::test]
async fn wp2_invite_lifecycle_endpoints() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_bob_id, bob_token) = signup_user(&state, "bob").await;
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("set role");

    // §9.1 default: plain users cannot create codes (upstream
    // `canInvite`, default false).
    let (status, value) = call(
        &state,
        "invite/create",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");

    // Moderator creates one code (single object, upstream shape).
    let (status, code1) = call(
        &state,
        "invite/create",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{code1}");
    assert!(code1["code"].is_string(), "{code1}");
    assert!(code1["id"].is_string(), "{code1}");
    assert_eq!(code1["used"], serde_json::json!(false));
    assert_eq!(code1["createdBy"]["username"], serde_json::json!("alice"));

    // invite/list is scoped to the caller.
    let (status, mine) = call(
        &state,
        "invite/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{mine}");
    assert_eq!(mine.as_array().unwrap().len(), 1, "{mine}");
    let (status, value) = call(
        &state,
        "invite/list",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");

    // invite/limit: `remaining` is null with the default (uncapped) policy.
    let (status, limit) = call(
        &state,
        "invite/limit",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{limit}");
    assert_eq!(limit["remaining"], serde_json::json!(null));

    // admin/invite/create: `count` + `expiresAt`; unparsable datetime
    // is upstream's INVALID_DATE_TIME.
    let (status, value) = call(
        &state,
        "admin/invite/create",
        serde_json::json!({ "count": 3, "expiresAt": "not-a-date" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_DATE_TIME");
    assert_eq!(value["id"], "f1380b15-3760-4c6c-a1db-5c3aaf1cbd49");
    let (status, batch) = call(
        &state,
        "admin/invite/create",
        serde_json::json!({ "count": 3, "expiresAt": "2030-01-02T03:04:05.000Z" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{batch}");
    let rows = batch.as_array().unwrap();
    assert_eq!(rows.len(), 3, "{batch}");
    for row in rows {
        assert_eq!(
            row["expiresAt"],
            serde_json::json!("2030-01-02T03:04:05.000Z"),
            "{row}"
        );
    }

    // admin/invite/list: every creator's codes, `type` filter.
    let (status, all) = call(
        &state,
        "admin/invite/list",
        serde_json::json!({ "type": "unused" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{all}");
    assert_eq!(all.as_array().unwrap().len(), 4, "{all}");
    let (status, used) = call(
        &state,
        "admin/invite/list",
        serde_json::json!({ "type": "used" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{used}");
    assert_eq!(used.as_array().unwrap().len(), 0, "{used}");

    // invite/delete: own code goes (204); unknown id is upstream's
    // NO_SUCH_INVITE_CODE (400).
    let code1_id = code1["id"].as_str().unwrap().to_string();
    let (status, value) = call(
        &state,
        "invite/delete",
        serde_json::json!({ "inviteId": code1_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    let (status, after) = call(
        &state,
        "admin/invite/list",
        serde_json::json!({ "type": "unused" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(after.as_array().unwrap().len(), 3, "{after}");
    let (status, value) = call(
        &state,
        "invite/delete",
        serde_json::json!({ "inviteId": "missing-invite-id" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_INVITE_CODE");
    assert_eq!(value["id"], "cd4f9ae4-7854-4e3e-8df9-c296f051e634");
}

#[tokio::test]
async fn wp2_admin_meta_and_blocked_hosts() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;

    // Both admin endpoints are admin-only.
    for endpoint in ["admin/meta", "admin/update-meta"] {
        let (status, value) =
            call(&state, endpoint, serde_json::json!({}), Some(&alice_token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{endpoint}: {value}");
    }

    state
        .services
        .set_role(None, &alice_id, true, false)
        .expect("set role");
    // blockedHosts: lowercased, deduped, empties dropped (upstream
    // `admin/update-meta.ts` normalization).
    let (status, value) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({
            "blockedHosts": ["Example.COM", "", " example.com ", "sub.example.com"],
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    let (status, meta) = call(
        &state,
        "admin/meta",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{meta}");
    assert_eq!(
        meta["blockedHosts"],
        serde_json::json!(["example.com", "sub.example.com"])
    );
    assert_eq!(meta["disableRegistration"], serde_json::json!(false));

    // Public meta carries the setting too (WP2 acceptance).
    let (status, public_meta) = call(&state, "meta", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{public_meta}");
    assert_eq!(public_meta["disableRegistration"], serde_json::json!(false));

    // Fields outside the WP2 subset are refused explicitly (WP6
    // completes the parameter set) — never silently ignored.
    let (status, value) = call(
        &state,
        "admin/update-meta",
        serde_json::json!({ "name": "Nope" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
}

#[tokio::test]
async fn wp3_suspend_unsuspend_endpoints() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, _carol_token) = signup_user(&state, "carol").await;
    let (dan_id, _dan_token) = signup_user(&state, "dan").await;

    async fn actor_status(state: &ApiState, id: &str) -> StatusCode {
        ap_actor::get_actor(
            State(state.services.clone()),
            Path(id.to_string()),
            HeaderMap::new(),
        )
        .await
        .status()
    }

    fn is_suspended(state: &ApiState, id: &str) -> bool {
        state
            .services
            .accounts
            .find_by_id(id)
            .unwrap()
            .unwrap()
            .is_suspended
    }

    // Plain users are refused both admin actions (moderator role in the
    // handler; scopes alone are not authorization).
    for endpoint in ["admin/suspend-user", "admin/unsuspend-user"] {
        let (status, value) = call(
            &state,
            endpoint,
            serde_json::json!({ "userId": bob_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{endpoint}: {value}");
        assert_eq!(value["code"], "PERMISSION_DENIED");
    }

    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("set role");

    // Missing target: typed `NO_SUCH_USER` (upstream throws an untyped
    // `user not found` here — its API layer answers 500; manifest §23).
    let (status, value) = call(
        &state,
        "admin/suspend-user",
        serde_json::json!({ "userId": "aidmissingmissingmi" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");

    // WP3 refusal rule (applied to both endpoints, guide §3.1): a
    // moderator cannot act on an admin or another moderator. Upstream's
    // own suspend message is carried verbatim.
    state
        .services
        .set_role(None, &carol_id, false, true)
        .expect("set role");
    state
        .services
        .set_role(None, &dan_id, true, false)
        .expect("set role");
    for (endpoint, target, message) in [
        (
            "admin/suspend-user",
            &carol_id,
            "cannot suspend moderator account",
        ),
        (
            "admin/suspend-user",
            &dan_id,
            "cannot suspend moderator account",
        ),
        (
            "admin/unsuspend-user",
            &carol_id,
            "cannot unsuspend moderator account",
        ),
    ] {
        let (status, value) = call(
            &state,
            endpoint,
            serde_json::json!({ "userId": target }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{endpoint}: {value}");
        assert_eq!(value["code"], "INVALID_PARAM");
        assert_eq!(value["error"]["message"], message);
        assert!(!is_suspended(&state, target), "refusal must change nothing");
    }

    // Suspend bob: 204, and the enforcement surface flips together.
    let (status, value) = call(
        &state,
        "admin/suspend-user",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    assert!(is_suspended(&state, &bob_id));
    // Existing token: 403 USER_SUSPENDED.
    let (status, value) = call(&state, "i", serde_json::json!({}), Some(&bob_token)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["code"], "USER_SUSPENDED");
    // Sign-in refused (after the password verifies).
    let (status, value) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "bob", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{value}");
    assert_eq!(value["code"], "USER_SUSPENDED");
    // AP actor fetch not served.
    assert_eq!(actor_status(&state, &bob_id).await, StatusCode::NOT_FOUND);

    // One log row per action, in the same commit (the refused calls
    // wrote none).
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "type": "suspend" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    let rows = logs.as_array().unwrap();
    assert_eq!(rows.len(), 1, "one suspend row: {logs}");
    assert_eq!(rows[0]["type"], "suspend");
    assert_eq!(rows[0]["userId"], serde_json::json!(alice_id));
    assert_eq!(rows[0]["user"]["username"], "alice");
    assert_eq!(rows[0]["info"]["userId"], serde_json::json!(bob_id));
    assert_eq!(rows[0]["info"]["userUsername"], "bob");
    assert_eq!(rows[0]["info"]["userHost"], Value::Null);

    // Unsuspend restores the enforcement surface.
    let (status, value) = call(
        &state,
        "admin/unsuspend-user",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    assert!(!is_suspended(&state, &bob_id));
    let (status, _) = call(&state, "i", serde_json::json!({}), Some(&bob_token)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, value) = call(
        &state,
        "signin",
        serde_json::json!({ "username": "bob", "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(actor_status(&state, &bob_id).await, StatusCode::OK);
    let (status, logs) = call(
        &state,
        "admin/show-moderation-logs",
        serde_json::json!({ "type": "unsuspend" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{logs}");
    let rows = logs.as_array().unwrap();
    assert_eq!(rows.len(), 1, "one unsuspend row: {logs}");
    assert_eq!(rows[0]["type"], "unsuspend");
    assert_eq!(rows[0]["info"]["userId"], serde_json::json!(bob_id));
}

/// WP3 bounded fan-out: the suspend transaction queues ONE sweep job
/// per outbound concern and the worker expands them at run time —
/// `Delete(Person)` to every knowable shared inbox (upstream
/// `postSuspend`) and a silent `Undo(Follow)` per outgoing follow
/// (upstream `unFollowAll`). The removed follow requests and outgoing
/// follows stay removed after unsuspend (upstream parity).
#[tokio::test]
async fn wp3_suspend_fanout_sweep_and_follow_teardown() {
    use std::time::{Duration, Instant};

    use crate::application::ports::{AccountStore, CreateAccountInput, FollowStore};

    let (inbox, bodies) = capture_inbox(16);
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _bob_token) = signup_user(&state, "bob").await;
    let (carol_id, _carol_token) = signup_user(&state, "carol").await;
    state
        .services
        .set_role(None, &alice_id, false, true)
        .expect("set role");

    // One remote account whose shared inbox is the stub: it is both the
    // Delete(Person) target (known shared inbox) and the Undo(Follow)
    // target (bob's outgoing follow to it).
    let remote = state
        .services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: "faraway".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/faraway".to_string()),
                inbox: Some(inbox.clone()),
                shared_inbox: Some(inbox.clone()),
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .expect("remote account");

    // Outgoing follows (removed by the sweep) and one incoming follow
    // (kept — upstream `unFollowAll` is outgoing-only).
    state
        .services
        .follows
        .follow(&bob_id, &remote.id, false, None, Vec::new())
        .expect("bob -> remote");
    state
        .services
        .follows
        .follow(&bob_id, &carol_id, false, None, Vec::new())
        .expect("bob -> carol");
    state
        .services
        .follows
        .follow(&remote.id, &bob_id, false, None, Vec::new())
        .expect("remote -> bob");
    // Pending requests in both directions (torn down at suspend).
    state
        .services
        .follows
        .request_follow(&alice_id, &bob_id, false, None, Vec::new())
        .expect("alice -> bob request");
    state
        .services
        .follows
        .request_follow(&bob_id, &alice_id, false, None, Vec::new())
        .expect("bob -> alice request");

    // Production runs the worker on a background loop; do the same here.
    let _worker = state.worker.clone().spawn(Duration::from_millis(10));

    let (status, value) = call(
        &state,
        "admin/suspend-user",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    // Follow requests are gone in both directions in the same commit.
    assert!(
        state
            .services
            .follows
            .get_follow_request(&alice_id, &bob_id)
            .unwrap()
            .is_none()
    );
    assert!(
        state
            .services
            .follows
            .get_follow_request(&bob_id, &alice_id)
            .unwrap()
            .is_none()
    );

    // Poll until the sweep has fanned out: one Delete(Person) for the
    // actor and one Undo(Follow) for the remote edge.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        {
            let captured = bodies.lock().unwrap();
            let has_delete = captured.iter().any(|b| {
                b.contains("\"type\":\"Delete\"") && b.contains(&format!("users/{bob_id}"))
            });
            let has_undo_follow = captured
                .iter()
                .any(|b| b.contains("\"type\":\"Undo\"") && b.contains("\"type\":\"Follow\""));
            if has_delete && has_undo_follow {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "suspend sweeps did not fan out: {:?}",
            bodies.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Outgoing follows removed (local and remote), incoming kept.
    assert!(
        !state
            .services
            .follows
            .is_following(&bob_id, &remote.id)
            .unwrap()
    );
    assert!(
        !state
            .services
            .follows
            .is_following(&bob_id, &carol_id)
            .unwrap()
    );
    assert!(
        state
            .services
            .follows
            .is_following(&remote.id, &bob_id)
            .unwrap()
    );

    // Unsuspend: the same knowable shared inboxes get
    // Undo(Delete(Person)); removed follows and requests stay removed.
    let (status, value) = call(
        &state,
        "admin/unsuspend-user",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        {
            let captured = bodies.lock().unwrap();
            let has_undo_delete = captured.iter().any(|b| {
                b.contains("\"type\":\"Undo\"")
                    && b.contains("\"type\":\"Delete\"")
                    && b.contains(&format!("users/{bob_id}"))
            });
            if has_undo_delete {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "unsuspend broadcast did not fan out: {:?}",
            bodies.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !state
            .services
            .follows
            .is_following(&bob_id, &remote.id)
            .unwrap()
    );
    assert!(
        !state
            .services
            .follows
            .is_following(&bob_id, &carol_id)
            .unwrap()
    );
    assert!(
        state
            .services
            .follows
            .get_follow_request(&alice_id, &bob_id)
            .unwrap()
            .is_none(),
        "removed follow requests stay removed"
    );
}
