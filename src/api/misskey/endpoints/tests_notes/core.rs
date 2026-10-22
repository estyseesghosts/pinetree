//! Endpoint tests_notes tests (core).
//!
//! Split from `endpoints/tests_notes.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::tests_common::{call, create_note, note_ids, signup_user, test_state};
use super::super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::api::misskey::response::RateLimiter;
use crate::application::ports::PortError;
use crate::domain::id::Clock;
use crate::domain::note::Visibility;
use axum::http::StatusCode;
use serde_json::Value;
use std::sync::Arc;

#[tokio::test]
async fn notes_lifecycle() {
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "gina").await;

    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "hello world" }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let note_id = value["createdNote"]["id"].as_str().unwrap().to_string();
    assert_eq!(value["createdNote"]["text"], "hello world");

    // channelId / poll are rejected per the Phase 1 surface.
    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "x", "channelId": "c" }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "UNSUPPORTED_FIELD");
    let (status, value) = call(
        &state,
        "notes/create",
        serde_json::json!({ "text": "x", "poll": {} }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "INVALID_PARAM");

    let (status, shown) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["id"], note_id);

    let (status, _) = call(
        &state,
        "notes/delete",
        serde_json::json!({ "noteId": note_id }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, value) = call(
        &state,
        "notes/show",
        serde_json::json!({ "noteId": note_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_NOTE");
}

#[tokio::test]
async fn timelines_page_with_cursors() {
    let (state, clock) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    for text in ["one", "two", "three"] {
        let (status, _) = call(
            &state,
            "notes/create",
            serde_json::json!({ "text": text }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        clock.advance_ms(1_000);
    }

    let (status, home) = call(
        &state,
        "notes/timeline",
        serde_json::json!({ "limit": 10 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{home}");
    // Bob follows nobody: empty home timeline.
    assert_eq!(home.as_array().unwrap().len(), 0);

    let (status, local) = call(
        &state,
        "notes/local-timeline",
        serde_json::json!({ "limit": 10 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{local}");
    assert_eq!(local.as_array().unwrap().len(), 3);

    let newest = local[0]["id"].as_str().unwrap().to_string();
    let oldest = local[2]["id"].as_str().unwrap().to_string();
    let (status, page) = call(
        &state,
        "notes/local-timeline",
        serde_json::json!({ "untilId": newest }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page.as_array().unwrap().len(), 2);
    let (status, page) = call(
        &state,
        "notes/local-timeline",
        serde_json::json!({ "sinceId": oldest }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page.as_array().unwrap().len(), 2);

    // Hybrid requires auth and merges home + local.
    let (status, _) = call(&state, "notes/hybrid-timeline", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, hybrid) = call(
        &state,
        "notes/hybrid-timeline",
        serde_json::json!({ "limit": 10 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hybrid}");
    assert_eq!(hybrid.as_array().unwrap().len(), 3);

    // Bad limits are rejected.
    let (status, value) = call(
        &state,
        "notes/local-timeline",
        serde_json::json!({ "limit": 500 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "INVALID_PARAM");
}

#[tokio::test]
async fn global_timeline_carries_remote_public_notes() {
    use crate::application::ports::{AccountStore, CreateAccountInput, CreateNoteInput};
    use crate::domain::note::Visibility;

    let (state, clock) = test_state();
    let (_, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    // Remote actor, as cached from federation.
    let remote = state
        .services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: "rina".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/rina".to_string()),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .expect("remote account");

    async fn post(state: &ApiState, body: Value, token: Option<&str>) -> String {
        let (status, value) = call(state, "notes/create", body, token).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        value["createdNote"]["id"].as_str().unwrap().to_string()
    }

    let alice_public = post(
        &state,
        serde_json::json!({ "text": "alice public" }),
        Some(&alice_token),
    )
    .await;
    clock.advance_ms(1_000);
    post(
        &state,
        serde_json::json!({ "text": "alice home", "visibility": "home" }),
        Some(&alice_token),
    )
    .await;
    clock.advance_ms(1_000);
    post(
        &state,
        serde_json::json!({ "text": "alice local-only", "localOnly": true }),
        Some(&alice_token),
    )
    .await;
    clock.advance_ms(1_000);
    post(
        &state,
        serde_json::json!({ "text": "alice followers", "visibility": "followers" }),
        Some(&alice_token),
    )
    .await;
    clock.advance_ms(1_000);
    // A self-reply: excluded from local-timeline by default, but the
    // global timeline has no reply filter (Misskey has no `withReplies`
    // on `notes/global-timeline`).
    let alice_reply = post(
        &state,
        serde_json::json!({ "text": "alice self-reply", "replyId": alice_public }),
        Some(&alice_token),
    )
    .await;
    clock.advance_ms(1_000);

    // Remote notes arrive through the same inbound op the inbox uses.
    let mut inbound = |activity: &str, uri: &str, text: &str, visibility: Visibility| {
        let mut input = CreateNoteInput::simple_public(remote.id.clone(), text);
        input.visibility = visibility;
        input.uri = Some(uri.to_string());
        state
            .services
            .handle_inbound_create_note(activity, input, Vec::new())
            .expect("inbound note")
            .expect("first delivery commits")
            .id
    };
    let remote_public = inbound(
        "https://remote.example/activities/1",
        "https://remote.example/notes/1",
        "remote public",
        Visibility::Public,
    );
    clock.advance_ms(1_000);
    inbound(
        "https://remote.example/activities/2",
        "https://remote.example/notes/2",
        "remote followers",
        Visibility::Followers,
    );
    clock.advance_ms(1_000);

    // Anonymous global timeline: local + remote public notes (plus the
    // self-reply, which is public), newest first. Home, local-only and
    // followers notes never appear.
    let (status, global) = call(
        &state,
        "notes/global-timeline",
        serde_json::json!({ "limit": 10 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{global}");
    let ids: Vec<&str> = global
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![
            remote_public.as_str(),
            alice_reply.as_str(),
            alice_public.as_str()
        ],
        "{global}"
    );

    // `withReplies` is not a global-timeline param (Misskey parity).
    let (status, value) = call(
        &state,
        "notes/global-timeline",
        serde_json::json!({ "withReplies": true }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "UNSUPPORTED_FIELD");

    // Authenticated reads see the same public surface.
    let (status, authed) = call(
        &state,
        "notes/global-timeline",
        serde_json::json!({ "limit": 10 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{authed}");
    assert_eq!(authed, global);

    // Local timeline still excludes the remote note.
    let (status, local) = call(
        &state,
        "notes/local-timeline",
        serde_json::json!({ "limit": 10 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{local}");
    assert!(
        local
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n["id"].as_str().unwrap() != remote_public),
        "{local}"
    );

    // `withRenotes=false` drops pure renotes; `withFiles=true` keeps
    // only notes with attachments (none here).
    let alice_renote = post(
        &state,
        serde_json::json!({ "renoteId": alice_public }),
        Some(&alice_token),
    )
    .await;
    clock.advance_ms(1_000);
    let (status, with_renotes) = call(
        &state,
        "notes/global-timeline",
        serde_json::json!({ "limit": 10 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{with_renotes}");
    assert_eq!(with_renotes.as_array().unwrap().len(), ids.len() + 1);
    assert_eq!(
        with_renotes[0]["id"].as_str().unwrap(),
        alice_renote.as_str()
    );
    let (status, no_renotes) = call(
        &state,
        "notes/global-timeline",
        serde_json::json!({ "withRenotes": false }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{no_renotes}");
    assert_eq!(no_renotes.as_array().unwrap().len(), ids.len());
    let (status, with_files) = call(
        &state,
        "notes/global-timeline",
        serde_json::json!({ "withFiles": true }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{with_files}");
    assert_eq!(with_files.as_array().unwrap().len(), 0);

    // Cursor pagination over the global timeline.
    let (status, page) = call(
        &state,
        "notes/global-timeline",
        serde_json::json!({ "limit": 1 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page.as_array().unwrap().len(), 1);
    assert_eq!(page[0]["id"].as_str().unwrap(), alice_renote.as_str());
    let (status, rest) = call(
        &state,
        "notes/global-timeline",
        serde_json::json!({ "untilId": alice_renote }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rest}");
    assert_eq!(rest.as_array().unwrap().len(), ids.len());
}
