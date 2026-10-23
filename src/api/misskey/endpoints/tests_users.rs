//! Endpoint tests_users tests (split from `endpoints/mod.rs`).
//!
//! Pure move, no behavior change.

#![allow(unused_imports)]

use super::tests_common::{
    assert_forbidden, call, create_note, ff_lists, note_ids, set_ff_visibility, signup_user,
    test_state,
};
use super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::api::misskey::response::RateLimiter;
use crate::application::ports::PortError;
use axum::http::StatusCode;
use serde_json::Value;
use std::sync::Arc;
#[tokio::test]
async fn i_update_and_unsupported_field() {
    let (mut state, clock) = test_state();
    let (_, token) = signup_user(&state, "carol").await;

    let (status, me) = call(
        &state,
        "i/update",
        serde_json::json!({ "name": "Carol", "isCat": true }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["name"], "Carol");
    assert_eq!(me["isCat"], true);

    let (status, value) = call(
        &state,
        "i/update",
        serde_json::json!({ "name": "X", "channelId": "abc" }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "UNSUPPORTED_FIELD");
    assert_eq!(value["error"]["info"]["param"], "channelId");

    // Rate limit bucket trips on the second quick update.
    state.rate_limiter =
        Arc::new(RateLimiter::new(clock.clone()).with_limit("i/update", 1, 3_600_000, None));
    let (status, _) = call(
        &state,
        "i/update",
        serde_json::json!({ "name": "Again" }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, value) = call(
        &state,
        "i/update",
        serde_json::json!({ "name": "Third" }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{value}");
    assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");
}

#[tokio::test]
async fn i_update_full_profile_round_trip_and_defaults() {
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "prof").await;

    // Fresh accounts pack the upstream column defaults.
    let (status, me) = call(&state, "i", serde_json::json!({}), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["preventAiLearning"], true);
    assert_eq!(me["injectFeaturedNote"], true);
    assert_eq!(me["receiveAnnouncementEmail"], true);
    assert_eq!(me["chatScope"], "mutual");
    assert_eq!(me["publicReactions"], true);
    assert_eq!(me["followingVisibility"], "public");
    assert_eq!(me["followersVisibility"], "public");
    assert_eq!(
        me["emailNotificationTypes"],
        serde_json::json!(["follow", "receiveFollowRequest"])
    );
    assert_eq!(me["mutedWords"], serde_json::json!([]));
    assert_eq!(me["notificationRecieveConfig"], serde_json::json!({}));
    assert_eq!(me["rejectQuotes"], false);
    assert_eq!(me["defaultReaction"], Value::Null);

    // Full round-trip across the new params.
    let (status, me) = call(
        &state,
        "i/update",
        serde_json::json!({
            "name": "  Prof  ",
            "description": "bio",
            "followedMessage": "thanks for following",
            "location": "nowhere",
            "birthday": "1990-02-28",
            "lang": "en",
            "fields": [
                {"name": " site ", "value": " https://example.local "},
                {"name": "  ", "value": "blank"},
            ],
            "isLocked": true,
            "hideOnlineStatus": true,
            "publicReactions": false,
            "carefulBot": true,
            "autoAcceptFollowed": true,
            "followApprovalLocalSeconds": 3600,
            "followApprovalRemoteSeconds": null,
            "noCrawle": true,
            "preventAiLearning": false,
            "requireSigninToViewContents": true,
            "makeNotesFollowersOnlyBefore": 123,
            "makeNotesHiddenBefore": null,
            "injectFeaturedNote": false,
            "receiveAnnouncementEmail": false,
            "alwaysMarkNsfw": true,
            "autoSensitive": true,
            "followingVisibility": "followers",
            "followersVisibility": "private",
            "chatScope": "followers",
            "mutedWords": [["foo", "bar"], "/^spam/i"],
            "hardMutedWords": [],
            "mutedInstances": ["spam.example"],
            "notificationRecieveConfig": {
                "mention": {"type": "following"},
                "follow": {"type": "list", "userListId": "lst1"},
            },
            "emailNotificationTypes": ["mention"],
            "isBot": true,
            "isCat": true,
            "isExplorable": false,
            "avatarDecorations": [{"id": "deco1", "angle": 0.1, "flipH": true}],
            "rejectQuotes": true,
            "defaultReaction": "❤",
        }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["name"], "Prof");
    assert_eq!(me["followedMessage"], "thanks for following");
    assert_eq!(me["lang"], "en");
    assert_eq!(
        me["fields"],
        serde_json::json!([{"name": "site", "value": "https://example.local"}])
    );
    assert_eq!(me["isLocked"], true);
    assert_eq!(me["hideOnlineStatus"], true);
    assert_eq!(me["onlineStatus"], "unknown");
    assert_eq!(me["requireSigninToViewContents"], true);
    assert_eq!(me["makeNotesFollowersOnlyBefore"], 123);
    assert_eq!(me["publicReactions"], false);
    assert_eq!(me["followApprovalLocalSeconds"], 3600);
    assert_eq!(me["followApprovalRemoteSeconds"], Value::Null);
    assert_eq!(me["preventAiLearning"], false);
    assert_eq!(me["chatScope"], "followers");
    assert_eq!(me["followingVisibility"], "followers");
    assert_eq!(me["followersVisibility"], "private");
    assert_eq!(
        me["mutedWords"],
        serde_json::json!([["foo", "bar"], "/^spam/i"])
    );
    assert_eq!(me["mutedInstances"], serde_json::json!(["spam.example"]));
    assert_eq!(
        me["notificationRecieveConfig"]["mention"]["type"],
        "following"
    );
    assert_eq!(
        me["notificationRecieveConfig"]["follow"]["userListId"],
        "lst1"
    );
    assert_eq!(me["emailNotificationTypes"], serde_json::json!(["mention"]));
    assert_eq!(
        me["avatarDecorations"],
        serde_json::json!([{"id": "deco1", "angle": 0.1, "flipH": true}])
    );
    assert_eq!(me["rejectQuotes"], true);
    assert_eq!(me["defaultReaction"], "❤");
    assert_eq!(me["verifiedLinks"], serde_json::json!([]));
}

#[tokio::test]
async fn i_update_profile_validation_errors() {
    use crate::application::ports::{InsertMediaInput, MediaStore};

    let (state, _) = test_state();
    let (alice_id, token) = signup_user(&state, "vald").await;

    async fn expect_code(state: &ApiState, token: &str, body: Value, code: &str) -> Value {
        let (status, value) = call(state, "i/update", body, Some(token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], code, "{value}");
        value
    }

    expect_code(
        &state,
        &token,
        serde_json::json!({"chatScope": "everyone!"}),
        "INVALID_PARAM",
    )
    .await;
    expect_code(
        &state,
        &token,
        serde_json::json!({"name": "x".repeat(51)}),
        "INVALID_PARAM",
    )
    .await;
    expect_code(
        &state,
        &token,
        serde_json::json!({"avatarDecorations": [{"id": "d", "angle": 0.9}]}),
        "INVALID_PARAM",
    )
    .await;
    // Plain (non-`/regex/`) single words are rejected like upstream.
    expect_code(
        &state,
        &token,
        serde_json::json!({"mutedWords": ["plain"]}),
        "INVALID_REGEXP",
    )
    .await;
    expect_code(
        &state,
        &token,
        serde_json::json!({"mutedWords": ["/(/"]}),
        "INVALID_REGEXP",
    )
    .await;
    // 201 mute chars exceed the default wordMuteLimit of 200.
    expect_code(
        &state,
        &token,
        serde_json::json!({"mutedWords": [format!("/{}/", "a".repeat(199))]}),
        "TOO_MANY_MUTED_WORDS",
    )
    .await;
    // Own username resolves locally to self.
    expect_code(
        &state,
        &token,
        serde_json::json!({"alsoKnownAs": ["vald"]}),
        "FORBIDDEN_TO_SET_YOURSELF",
    )
    .await;
    // `uniqueItems` parity.
    expect_code(
        &state,
        &token,
        serde_json::json!({"alsoKnownAs": ["someone@example.local", "someone@example.local"]}),
        "INVALID_PARAM",
    )
    .await;
    // Unknown page id → NO_SUCH_PAGE (upstream `update.ts:436`
    // parity; the pages store now exists).
    expect_code(
        &state,
        &token,
        serde_json::json!({"pinnedPageId": "pag00000000000001"}),
        "NO_SUCH_PAGE",
    )
    .await;
    // A real owned page pins; a foreign page is NO_SUCH_PAGE.
    let (status, page) = call(
        &state,
        "pages/create",
        serde_json::json!({"title": "T", "name": "t", "content": [], "variables": [], "script": ""}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let page_id = page["id"].as_str().unwrap().to_string();
    let (status, me) = call(
        &state,
        "i/update",
        serde_json::json!({"pinnedPageId": page_id}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (status, value) = call(
        &state,
        "i/update",
        serde_json::json!({"pinnedPageId": page_id}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_PAGE");
    expect_code(
        &state,
        &token,
        serde_json::json!({"avatarId": "mid00000000000009"}),
        "NO_SUCH_AVATAR",
    )
    .await;
    expect_code(
        &state,
        &token,
        serde_json::json!({"notificationRecieveConfig": {"mention": {"type": "sometimes"}}}),
        "INVALID_PARAM",
    )
    .await;
    // Misskey spells it `name`; camelCase guesses stay unsupported.
    expect_code(
        &state,
        &token,
        serde_json::json!({"displayName": "x"}),
        "UNSUPPORTED_FIELD",
    )
    .await;

    // A non-image file cannot be an avatar; an image can (and clears).
    let text_file = state
        .services
        .media
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: Some(alice_id.clone()),
            name: "note.txt".to_string(),
            mime: "text/plain".to_string(),
            size_bytes: 8,
            hash: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
            blob_key: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            blurhash: None,
            properties: crate::domain::media::MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: "https://example.local/files/note".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: None,
            max_file_size: 10_000_000,
        })
        .unwrap();
    expect_code(
        &state,
        &token,
        serde_json::json!({"avatarId": text_file.id}),
        "AVATAR_NOT_AN_IMAGE",
    )
    .await;
    let image_file = state
        .services
        .media
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: Some(alice_id.clone()),
            name: "photo.png".to_string(),
            mime: "image/png".to_string(),
            size_bytes: 512,
            hash: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
            blob_key: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            blurhash: None,
            properties: crate::domain::media::MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: "https://example.local/files/photo".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: None,
            max_file_size: 10_000_000,
        })
        .unwrap();
    let (status, me) = call(
        &state,
        "i/update",
        serde_json::json!({"avatarId": image_file.id}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["avatarId"], image_file.id);
    let (status, me) = call(
        &state,
        "i/update",
        serde_json::json!({"avatarId": null}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["avatarId"], Value::Null);
}

#[tokio::test]
async fn follows_and_notifications() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    let (status, lite) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lite}");
    assert_eq!(lite["id"], bob_id);

    // Bob sees a follow notification. The endpoint no longer drains the
    // worker: delivery and notify jobs run in the background, so this
    // test drains explicitly to stay deterministic.
    state
        .worker
        .drain(64)
        .expect("drain follow notification job");
    let (status, notifications) = call(
        &state,
        "i/notifications",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notifications}");
    assert_eq!(notifications[0]["type"], "follow");
    assert_eq!(notifications[0]["userId"], alice_id);

    let (status, lite) = call(
        &state,
        "following/delete",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lite}");
    assert_eq!(lite["id"], bob_id);
}

#[tokio::test]
async fn users_endpoints() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    signup_user(&state, "bob").await;

    let (status, shown) = call(
        &state,
        "users/show",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    // Own account packs as MeDetailed.
    assert!(shown.get("policies").is_some());

    let (status, shown) = call(
        &state,
        "users/show",
        serde_json::json!({ "username": "bob" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["username"], "bob");

    let (status, shown) = call(
        &state,
        "users/show",
        serde_json::json!({ "userIds": [alice_id] }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown.as_array().unwrap().len(), 1);

    let (status, results) = call(
        &state,
        "users/search",
        serde_json::json!({ "query": "ali", "limit": 10 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{results}");
    assert!(
        results
            .as_array()
            .unwrap()
            .iter()
            .any(|u| u["username"] == "alice")
    );

    let (status, value) = call(
        &state,
        "username/available",
        serde_json::json!({ "username": "alice" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["available"], false);
    let (status, value) = call(
        &state,
        "username/available",
        serde_json::json!({ "username": "fresh" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["available"], true);
    let (status, value) = call(
        &state,
        "username/available",
        serde_json::json!({ "username": "has space" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "INVALID_PARAM");
}

#[tokio::test]
async fn blocking_mute_endpoints_and_relation() {
    let (state, _clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // blocking/create packs the blockee as UserDetailedNotMe.
    let (status, packed) = call(
        &state,
        "blocking/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{packed}");
    assert_eq!(packed["id"], bob_id);

    // Duplicate → ALREADY_BLOCKING (exact Misskey code).
    let (status, err) = call(
        &state,
        "blocking/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "ALREADY_BLOCKING");
    assert_eq!(err["error"]["code"], "ALREADY_BLOCKING");

    // Self-block → BLOCKEE_IS_YOURSELF.
    let (status, err) = call(
        &state,
        "blocking/create",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "BLOCKEE_IS_YOURSELF");

    // Unknown user → NO_SUCH_USER.
    let (status, err) = call(
        &state,
        "blocking/create",
        serde_json::json!({ "userId": "aid00000000009999" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "NO_SUCH_USER");

    // blocking/list carries the row with the packed blockee.
    let (status, list) = call(
        &state,
        "blocking/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["blockeeId"], bob_id);
    assert_eq!(list[0]["blockee"]["id"], bob_id);

    // users/relation reflects the block both ways (single → 1-array).
    let (status, rel) = call(
        &state,
        "users/relation",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rel}");
    assert_eq!(rel.as_array().unwrap().len(), 1);
    assert_eq!(rel[0]["isBlocking"], true);
    assert_eq!(rel[0]["isBlocked"], false);
    let (status, rel) = call(
        &state,
        "users/relation",
        serde_json::json!({ "userId": [bob_id.clone(), alice_id.clone()] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rel}");
    assert_eq!(rel[0]["id"], bob_id);
    assert_eq!(rel[1]["isBlocked"], true);
    assert_eq!(rel[1]["isBlocking"], false);
    assert_eq!(rel[1]["id"], alice_id);

    // Blocked follow → BLOCKING / BLOCKED through the HTTP envelope.
    let (status, err) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "BLOCKING");
    let (status, err) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": alice_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "BLOCKED");

    // blocking/delete removes the row; repeat → NOT_BLOCKING.
    let (status, packed) = call(
        &state,
        "blocking/delete",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{packed}");
    assert_eq!(packed["id"], bob_id);
    let (status, err) = call(
        &state,
        "blocking/delete",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "NOT_BLOCKING");

    // mute/create → 204; duplicate → ALREADY_MUTING.
    let (status, _) = call(
        &state,
        "mute/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, err) = call(
        &state,
        "mute/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "ALREADY_MUTING");
    // mute/list carries the row; expiresAt is null for indefinite mutes.
    let (status, list) = call(
        &state,
        "mute/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["muteeId"], bob_id);
    assert!(list[0]["expiresAt"].is_null());
    // mute/delete → 204; repeat → NOT_MUTING.
    let (status, _) = call(
        &state,
        "mute/delete",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, err) = call(
        &state,
        "mute/delete",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "NOT_MUTING");

    // renote-mute endpoints mirror mute (no expiresAt).
    let (status, _) = call(
        &state,
        "renote-mute/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, err) = call(
        &state,
        "renote-mute/create",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "MUTEE_IS_YOURSELF");
    let (status, list) = call(
        &state,
        "renote-mute/list",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["muteeId"], bob_id);
    let (status, _) = call(
        &state,
        "renote-mute/delete",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn phase3_users_notes_filters_and_pagination() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    let root = create_note(
        &state,
        serde_json::json!({ "text": "root note" }),
        &alice_token,
    )
    .await;
    clock.advance_ms(1_000);
    let reply = create_note(
        &state,
        serde_json::json!({ "text": "a reply", "replyId": root }),
        &alice_token,
    )
    .await;
    clock.advance_ms(1_000);
    let pure = create_note(
        &state,
        serde_json::json!({ "renoteId": root }),
        &alice_token,
    )
    .await;
    clock.advance_ms(1_000);
    let quote = create_note(
        &state,
        serde_json::json!({ "renoteId": root, "text": "quoted" }),
        &alice_token,
    )
    .await;
    clock.advance_ms(1_000);

    // Defaults: no replies, renotes included.
    let (status, notes) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert_eq!(
        note_ids(&notes),
        vec![quote.clone(), pure.clone(), root.clone()]
    );

    // With replies.
    let (status, notes) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id, "withReplies": true }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert_eq!(
        note_ids(&notes),
        vec![quote.clone(), pure.clone(), reply.clone(), root.clone()]
    );

    // Without renotes: the pure renote drops out, the quote stays.
    let (status, notes) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id, "withRenotes": false }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    assert_eq!(note_ids(&notes), vec![quote.clone(), root.clone()]);

    // Cursor pagination.
    let (status, page) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id, "limit": 1 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(note_ids(&page), vec![quote.clone()]);
    let (status, page) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id, "untilId": quote }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page.as_array().unwrap().len(), 2);

    // Unknown author is NO_SUCH_USER (Misskey parity).
    let (status, value) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": "aid00000000000099" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");

    // Param validation.
    let (status, value) = call(&state, "users/notes", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    // Misskey parity: withReplies + withFiles is rejected, not applied.
    let (status, value) = call(
        &state,
        "users/notes",
        serde_json::json!({
            "userId": "aid00000000000001",
            "withReplies": true,
            "withFiles": true,
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "BOTH_WITH_REPLIES_AND_WITH_FILES");
    let (status, value) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id, "limit": 500 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let (status, value) = call(
        &state,
        "users/notes",
        serde_json::json!({ "userId": alice_id, "channelId": "x" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "UNSUPPORTED_FIELD");
}

#[tokio::test]
async fn phase3_follow_lists() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, _) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;

    let (status, _) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    clock.advance_ms(1_000);
    let (status, _) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    clock.advance_ms(1_000);

    // Followers of Bob: Following[] with the follower side populated.
    let (status, followers) = call(
        &state,
        "users/followers",
        serde_json::json!({ "userId": bob_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{followers}");
    let followers = followers.as_array().unwrap();
    assert_eq!(followers.len(), 2);
    // Newest first: Carol followed after Alice.
    assert_eq!(followers[0]["follower"]["id"], carol_id);
    assert_eq!(followers[1]["follower"]["id"], alice_id);
    for entry in followers {
        assert_eq!(entry["followeeId"], bob_id);
        assert_eq!(entry["followee"]["id"], bob_id);
        assert!(entry["id"].as_str().is_some());
        assert!(entry["createdAt"].as_str().is_some());
        assert_eq!(entry["followerId"], entry["follower"]["id"]);
    }

    // Following of Alice: the followee side populated.
    let (status, following) = call(
        &state,
        "users/following",
        serde_json::json!({ "userId": alice_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{following}");
    let following = following.as_array().unwrap();
    assert_eq!(following.len(), 1);
    assert_eq!(following[0]["followee"]["id"], bob_id);
    assert_eq!(following[0]["follower"]["id"], alice_id);

    // username+host resolution (null host = local).
    let (status, via_name) = call(
        &state,
        "users/followers",
        serde_json::json!({ "username": "bob", "host": null }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{via_name}");
    assert_eq!(via_name.as_array().unwrap().len(), 2);
    assert_eq!(via_name[0]["follower"]["id"], carol_id);

    // Cursor pagination.
    let (status, page) = call(
        &state,
        "users/followers",
        serde_json::json!({ "userId": bob_id, "limit": 1 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page.as_array().unwrap().len(), 1);
    assert_eq!(page[0]["follower"]["id"], carol_id);

    // Errors.
    let (status, value) = call(
        &state,
        "users/followers",
        serde_json::json!({ "userId": "aid00000000000099" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");
    let (status, value) = call(&state, "users/following", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let (status, value) = call(
        &state,
        "users/followers",
        serde_json::json!({ "username": "bob" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
}

#[tokio::test]
async fn ff_visibility_gates_and_count_masking() {
    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;

    // Bob follows Alice; Alice follows Carol.
    for (token, target) in [
        (&alice_token, carol_id.as_str()),
        (&bob_token, alice_id.as_str()),
    ] {
        let (status, value) = call(
            &state,
            "following/create",
            serde_json::json!({ "userId": target }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
    }

    // Public (default): everyone reads both directions.
    let ((status, followers), (status2, following)) = ff_lists(&state, &alice_id, None).await;
    assert_eq!(status, StatusCode::OK, "{followers}");
    assert_eq!(followers.as_array().unwrap().len(), 1);
    assert_eq!(status2, StatusCode::OK, "{following}");
    assert_eq!(following.as_array().unwrap().len(), 1);
    let (status, counts) = call(
        &state,
        "users/show",
        serde_json::json!({ "userId": alice_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{counts}");
    assert_eq!(counts["followersCount"], 1);
    assert_eq!(counts["followingCount"], 1);

    // `followers` mode: owner + follower read; strangers denied.
    set_ff_visibility(&state, &alice_token, "followers", "followers").await;
    let ((status, value), (status2, value2)) = ff_lists(&state, &alice_id, None).await;
    assert_forbidden(status, &value);
    assert_forbidden(status2, &value2);
    let ((status, value), (status2, value2)) =
        ff_lists(&state, &alice_id, Some(&carol_token)).await;
    assert_forbidden(status, &value);
    assert_forbidden(status2, &value2);
    // Bob follows Alice: both directions read fine.
    let ((status, followers), (status2, following)) =
        ff_lists(&state, &alice_id, Some(&bob_token)).await;
    assert_eq!(status, StatusCode::OK, "{followers}");
    assert_eq!(followers.as_array().unwrap().len(), 1);
    assert_eq!(status2, StatusCode::OK, "{following}");
    assert_eq!(following[0]["followee"]["id"], carol_id);
    // Self reads fine.
    let ((status, _), (status2, _)) = ff_lists(&state, &alice_id, Some(&alice_token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status2, StatusCode::OK);

    // Count masking in `followers` mode: follower + self see counts,
    // strangers see null.
    let (status, masked) = call(
        &state,
        "users/show",
        serde_json::json!({ "userId": alice_id }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{masked}");
    assert_eq!(masked["followersCount"], Value::Null);
    assert_eq!(masked["followingCount"], Value::Null);
    let (status, shown) = call(
        &state,
        "users/show",
        serde_json::json!({ "userId": alice_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["followersCount"], 1);
    assert_eq!(shown["followingCount"], 1);
    let (status, me) = call(
        &state,
        "users/show",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{me}");
    assert_eq!(me["followersCount"], 1);
    assert_eq!(me["followingCount"], 1);

    // `private` mode: only self reads; follower denied; counts masked
    // for everyone but self.
    set_ff_visibility(&state, &alice_token, "private", "private").await;
    let ((status, value), (status2, value2)) = ff_lists(&state, &alice_id, Some(&bob_token)).await;
    assert_forbidden(status, &value);
    assert_forbidden(status2, &value2);
    let ((status, _), (status2, _)) = ff_lists(&state, &alice_id, Some(&alice_token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status2, StatusCode::OK);
    let (status, masked) = call(
        &state,
        "users/show",
        serde_json::json!({ "userId": alice_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{masked}");
    assert_eq!(masked["followersCount"], Value::Null);
    assert_eq!(masked["followingCount"], Value::Null);

    // Unknown users still report NO_SUCH_USER (not FORBIDDEN).
    let (status, value) = call(
        &state,
        "users/followers",
        serde_json::json!({ "userId": "aid00000000000099" }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");
    let _ = bob_id;
}

#[tokio::test]
async fn users_reactions_gates_and_filters() {
    use crate::application::ports::{AccountStore, CreateAccountInput};

    async fn post(state: &ApiState, body: Value, token: Option<&str>) -> String {
        let (status, value) = call(state, "notes/create", body, token).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        value["createdNote"]["id"].as_str().unwrap().to_string()
    }

    let (state, _) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, carol_token) = signup_user(&state, "carol").await;
    let (dave_id, dave_token) = signup_user(&state, "dave").await;

    // Bob's public note; Alice reacts to it.
    let note_id = post(
        &state,
        serde_json::json!({ "text": "bob note" }),
        Some(&bob_token),
    )
    .await;
    let (status, value) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": note_id, "reaction": "👍" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");

    // Public target: anonymous readers see the reaction with its note.
    let (status, reactions) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": alice_id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reactions}");
    let reactions = reactions.as_array().unwrap();
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0]["user"]["id"], alice_id);
    assert_eq!(reactions[0]["type"], "👍");
    assert_eq!(reactions[0]["note"]["id"], note_id);
    assert!(reactions[0]["createdAt"].as_str().is_some());

    // Unknown target → NO_SUCH_USER; missing userId → INVALID_PARAM.
    let (status, value) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": "aid00000000000099" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_USER");
    let (status, value) = call(&state, "users/reactions", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");

    // Remote target → IS_REMOTE_USER (exact upstream strings).
    let remote = state
        .services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: "rem".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/rem".to_string()),
                inbox: Some("https://remote.example/users/rem/inbox".to_string()),
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
        .expect("remote account");
    let (status, value) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": remote.id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "IS_REMOTE_USER");
    assert_eq!(value["error"]["kind"], "client");
    assert_eq!(
        value["message"],
        "Currently unavailable to display reactions of remote users. See https://github.com/misskey-dev/misskey/issues/12964"
    );

    // Private reactions: strangers denied, self allowed.
    let (status, value) = call(
        &state,
        "i/update",
        serde_json::json!({ "publicReactions": false }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    for token in [None, Some(bob_token.as_str()), Some(carol_token.as_str())] {
        let (status, value) = call(
            &state,
            "users/reactions",
            serde_json::json!({ "userId": alice_id }),
            token,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "REACTIONS_NOT_PUBLIC");
        assert_eq!(value["error"]["kind"], "client");
        assert_eq!(value["message"], "Reactions of the user is not public.");
    }
    let (status, own) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{own}");
    assert_eq!(own.as_array().unwrap().len(), 1);

    // Blocked-by-target reads as silent `[]` (re-open reactions first:
    // the gate order is remote → private → blocked).
    let (status, value) = call(
        &state,
        "i/update",
        serde_json::json!({ "publicReactions": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let (status, value) = call(
        &state,
        "blocking/create",
        serde_json::json!({ "userId": carol_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let (status, reactions) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": alice_id }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reactions}");
    assert_eq!(reactions.as_array().unwrap().len(), 0);
    // Non-blocked strangers still read.
    let (status, reactions) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": alice_id }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reactions}");
    assert_eq!(reactions.as_array().unwrap().len(), 1);
    // Unblock so later filter checks exercise visibility, not the gate.
    let (status, value) = call(
        &state,
        "blocking/delete",
        serde_json::json!({ "userId": carol_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");

    // Visibility filter: Alice reacts to Dave's followers-only note
    // (Dave follows Alice? no — Alice cannot see it either unless she
    // follows Dave or authored it). Setup: Dave posts followers-only;
    // Alice follows Dave so her reaction path is valid, then Carol
    // (stranger) must not see the reaction while Alice still does.
    let (status, value) = call(
        &state,
        "following/create",
        serde_json::json!({ "userId": dave_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let followers_note = post(
        &state,
        serde_json::json!({ "text": "dave followers", "visibility": "followers" }),
        Some(&dave_token),
    )
    .await;
    let (status, value) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": followers_note, "reaction": "❤" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    // Carol cannot see Dave's followers-only note, so that reaction is
    // filtered from her view while Bob's public-note reaction stays;
    // Alice (follower) still sees both.
    let (status, reactions) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": alice_id }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reactions}");
    let reactions = reactions.as_array().unwrap();
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0]["note"]["id"], note_id);
    let (status, reactions) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": alice_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reactions}");
    assert_eq!(reactions.as_array().unwrap().len(), 2);

    // Mute filter: Carol mutes Bob (note author). Dave reacts to Bob's
    // public note and to his own public note; Carol's view of Dave's
    // reactions drops the Bob-authored one while the reaction to Dave's
    // own note stays (the target-author exemption).
    let own_note = post(
        &state,
        serde_json::json!({ "text": "dave public" }),
        Some(&dave_token),
    )
    .await;
    let (status, value) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": own_note, "reaction": "🎉" }),
        Some(&dave_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    let (status, value) = call(
        &state,
        "mute/create",
        serde_json::json!({ "userId": bob_id }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    let (status, value) = call(
        &state,
        "notes/reactions/create",
        serde_json::json!({ "noteId": note_id, "reaction": "👀" }),
        Some(&dave_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{value}");
    let (status, reactions) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": dave_id }),
        Some(&carol_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reactions}");
    let reactions = reactions.as_array().unwrap();
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0]["note"]["id"], own_note);
    // Unmuted viewers still see both of Dave's reactions.
    let (status, reactions) = call(
        &state,
        "users/reactions",
        serde_json::json!({ "userId": dave_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reactions}");
    assert_eq!(reactions.as_array().unwrap().len(), 2);
}

/// §9 full follower list sweep: 250 follows page through completely in
/// both directions with no duplicates or gaps across cursor pages.
#[tokio::test]
async fn follow_list_sweep_250_both_directions() {
    use crate::application::ports::{AccountStore, CreateAccountInput, FollowStore};

    let (state, _) = test_state();
    let owner = state
        .services
        .accounts
        .create_account(CreateAccountInput::local("owner"), None)
        .expect("owner account");
    let mut fans: Vec<String> = Vec::with_capacity(250);
    for i in 0..250 {
        let fan = state
            .services
            .accounts
            .create_account(CreateAccountInput::local(format!("fan{i:03}")), None)
            .expect("fan account");
        // Both directions: every fan follows the owner and vice versa.
        state
            .services
            .follows
            .follow(&fan.id, &owner.id, false, None, Vec::new())
            .expect("fan follows owner");
        state
            .services
            .follows
            .follow(&owner.id, &fan.id, false, None, Vec::new())
            .expect("owner follows fan");
        fans.push(fan.id);
    }

    // Limit validation: range 1–100, default 10.
    let (status, value) = call(
        &state,
        "users/followers",
        serde_json::json!({ "userId": owner.id, "limit": 101 }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let (status, page) = call(
        &state,
        "users/followers",
        serde_json::json!({ "userId": owner.id }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page.as_array().unwrap().len(), 10);

    // Walk every page (limit 100 → 100 + 100 + 50) in both directions.
    async fn walk(state: &ApiState, endpoint: &str, target: &str, side: &str) -> Vec<String> {
        let mut seen: Vec<String> = Vec::new();
        let mut last_entry_id: Option<String> = None;
        loop {
            let mut body = serde_json::json!({ "userId": target, "limit": 100 });
            if let Some(until) = &last_entry_id {
                body["untilId"] = Value::String(until.clone());
            }
            let (status, page) = call(state, endpoint, body, None).await;
            assert_eq!(status, StatusCode::OK, "{page}");
            let page = page.as_array().unwrap();
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 100);
            for entry in page {
                // Newest-first: entry ids strictly descend across pages.
                let entry_id = entry["id"].as_str().unwrap().to_string();
                if let Some(prev) = &last_entry_id {
                    assert!(prev.as_str() > entry_id.as_str(), "cursor order broke");
                }
                last_entry_id = Some(entry_id);
                let side_id = entry[side].as_str().unwrap().to_string();
                assert!(!seen.contains(&side_id), "duplicate {side_id}");
                seen.push(side_id);
            }
            if page.len() < 100 {
                break;
            }
        }
        seen
    }

    let mut newest_first = fans.clone();
    newest_first.reverse();
    let followers = walk(&state, "users/followers", &owner.id, "followerId").await;
    assert_eq!(followers.len(), 250, "no gaps in followers sweep");
    assert_eq!(followers, newest_first, "followers newest-first, no dupes");
    let following = walk(&state, "users/following", &owner.id, "followeeId").await;
    assert_eq!(following.len(), 250, "no gaps in following sweep");
    assert_eq!(following, newest_first, "following newest-first, no dupes");
}
