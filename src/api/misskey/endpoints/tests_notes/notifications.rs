//! Endpoint tests_notes tests (notifications).
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
async fn phase3_notifications_list_and_mark_all() {
    use crate::domain::notification::{Notification, NotificationKind};

    let (state, clock) = test_state();
    let (alice_id, _) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // Twelve notifications for Bob (direct store inserts, deterministic
    // ids): even indexes are follows, odd are mentions.
    for n in 0..12 {
        let kind = if n % 2 == 0 {
            NotificationKind::Follow
        } else {
            NotificationKind::Mention
        };
        state
            .services
            .notifications
            .insert_notification(Notification {
                clip_id: None,
                page_id: None,
                id: state.services.ids.generate(),
                notifiee_id: bob_id.clone(),
                notifier_id: alice_id.clone(),
                kind,
                note_id: None,
                reaction: None,
                draft_id: None,
                file_id: None,
                exported_entity: None,
                is_read: false,
            })
            .unwrap();
        clock.advance_ms(1_000);
    }

    // Default limit is 10 (Misskey-aligned), newest first.
    let (status, first) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let first = first.as_array().unwrap();
    assert_eq!(first.len(), 10);
    assert_eq!(first[0]["isRead"], false);

    // Cursor pagination reaches the remainder without overlap.
    let oldest_seen = first[9]["id"].as_str().unwrap().to_string();
    let (status, second) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "untilId": oldest_seen }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second.as_array().unwrap().len(), 2);

    // includeTypes filters by kind.
    let (status, follows) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "includeTypes": ["follow"] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{follows}");
    assert_eq!(follows.as_array().unwrap().len(), 6);
    assert!(
        follows
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n["type"] == "follow")
    );
    // Excluding everything matches nothing.
    let (status, none) = call(
        &state,
        "i/notifications",
        serde_json::json!({
            "markAsRead": false,
            "excludeTypes": ["follow", "mention", "reply", "renote", "reaction", "receiveFollowRequest", "scheduledNotePosted", "scheduledNotePostFailed"],
        }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{none}");
    assert_eq!(none.as_array().unwrap().len(), 0);
    // Explicitly empty includeTypes matches nothing (Misskey parity).
    let (status, none) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "includeTypes": [] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{none}");
    assert_eq!(none.as_array().unwrap().len(), 0);
    // Unknown type strings are INVALID_PARAM; obsolete ones are dropped.
    let (status, value) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "includeTypes": ["bogus"] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let (status, none) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "includeTypes": ["pollVote"] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{none}");
    assert_eq!(none.as_array().unwrap().len(), 0);

    // Default markAsRead=true flips everything to read.
    let (status, _) = call(
        &state,
        "i/notifications",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, after) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{after}");
    assert!(
        after
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n["isRead"] == true),
        "{after}"
    );

    // notifications/mark-all-as-read marks unread rows read.
    state
        .services
        .notifications
        .insert_notification(Notification {
            clip_id: None,
            page_id: None,
            id: state.services.ids.generate(),
            notifiee_id: bob_id.clone(),
            notifier_id: alice_id.clone(),
            kind: NotificationKind::Reaction,
            note_id: None,
            reaction: Some("❤".to_string()),
            draft_id: None,
            file_id: None,
            exported_entity: None,
            is_read: false,
        })
        .unwrap();
    let (status, _) = call(
        &state,
        "notifications/mark-all-as-read",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, after) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "limit": 1 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{after}");
    assert_eq!(after[0]["isRead"], true);

    // Auth + param validation.
    let (status, _) = call(&state, "i/notifications", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(
        &state,
        "notifications/mark-all-as-read",
        serde_json::json!({}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, value) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "limit": 101 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
}

#[tokio::test]
async fn notifications_grouped_shapes() {
    use crate::domain::notification::{Notification, NotificationKind};

    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;
    let (carol_id, _) = signup_user(&state, "carol").await;
    let (dave_id, _) = signup_user(&state, "dave").await;

    // Two notes by Alice; Bob renotes the first twice (two renote notes).
    let n1 = state
        .services
        .create_note(crate::application::ports::CreateNoteInput::simple_public(
            &alice_id, "n1",
        ))
        .unwrap();
    clock.advance_ms(1_000);
    let n2 = state
        .services
        .create_note(crate::application::ports::CreateNoteInput::simple_public(
            &alice_id, "n2",
        ))
        .unwrap();
    clock.advance_ms(1_000);
    let r1 = state
        .services
        .create_note(crate::application::ports::CreateNoteInput {
            renote_of: Some(n1.id.clone()),
            ..crate::application::ports::CreateNoteInput::simple_public(&bob_id, "rt1")
        })
        .unwrap();
    clock.advance_ms(1_000);
    let r2 = state
        .services
        .create_note(crate::application::ports::CreateNoteInput {
            renote_of: Some(n1.id.clone()),
            ..crate::application::ports::CreateNoteInput::simple_public(&carol_id, "rt2")
        })
        .unwrap();

    // Flat notifications for Bob, oldest first (clock advances make ids
    // time-ordered): follow, react(n1, bob), react(n1, carol),
    // react(n2, dave), renote(r1), renote(r2), mention(n2).
    let mut insert =
        |kind: NotificationKind, notifier: &str, note: Option<String>, reaction: Option<String>| {
            let id = state.services.ids.generate();
            state
                .services
                .notifications
                .insert_notification(Notification {
                    clip_id: None,
                    page_id: None,
                    id,
                    notifiee_id: bob_id.clone(),
                    notifier_id: notifier.to_string(),
                    kind,
                    note_id: note,
                    reaction,
                    draft_id: None,
                    file_id: None,
                    exported_entity: None,
                    is_read: false,
                })
                .unwrap();
            clock.advance_ms(1_000);
        };
    insert(NotificationKind::Follow, &alice_id, None, None);
    insert(
        NotificationKind::Reaction,
        &bob_id,
        Some(n1.id.clone()),
        Some("❤".to_string()),
    );
    insert(
        NotificationKind::Reaction,
        &carol_id,
        Some(n1.id.clone()),
        Some("👍".to_string()),
    );
    insert(
        NotificationKind::Reaction,
        &dave_id,
        Some(n2.id.clone()),
        Some("❤".to_string()),
    );
    insert(NotificationKind::Renote, &bob_id, Some(r1.id.clone()), None);
    insert(
        NotificationKind::Renote,
        &carol_id,
        Some(r2.id.clone()),
        None,
    );
    insert(
        NotificationKind::Mention,
        &alice_id,
        Some(n2.id.clone()),
        None,
    );

    // Grouped fetch: newest-first =
    // mention | renote:grouped(r2+r1) | reaction(n2) |
    // reaction:grouped(n1+n1) | follow.
    let (status, grouped) = call(
        &state,
        "i/notifications-grouped",
        serde_json::json!({ "markAsRead": false }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{grouped}");
    let grouped = grouped.as_array().unwrap();
    assert_eq!(grouped.len(), 5);
    assert_eq!(grouped[0]["type"], "mention");
    assert!(
        grouped[0].get("userId").is_some(),
        "plain packs keep userId"
    );
    assert_eq!(grouped[1]["type"], "renote:grouped");
    assert_eq!(
        grouped[1]["note"]["id"], r2.id,
        "group carries the newest renote"
    );
    assert_eq!(
        grouped[1]["users"].as_array().unwrap().len(),
        2,
        "both renoters merge: {}",
        grouped[1]
    );
    assert!(
        grouped[1].get("userId").is_none(),
        "groups drop top-level userId"
    );
    assert_eq!(
        grouped[2]["type"], "reaction",
        "lone reactions pass through"
    );
    assert_eq!(grouped[2]["note"]["id"], n2.id);
    assert_eq!(grouped[2]["reaction"], "❤");
    assert_eq!(grouped[3]["type"], "reaction:grouped");
    assert_eq!(grouped[3]["note"]["id"], n1.id);
    let reactions = grouped[3]["reactions"].as_array().unwrap();
    assert_eq!(reactions.len(), 2);
    assert_eq!(reactions[0]["reaction"], "👍", "newest member first");
    assert_eq!(reactions[1]["reaction"], "❤");
    assert_eq!(grouped[4]["type"], "follow");

    // Same params as `i/notifications` are accepted (full param set).
    let (status, full) = call(
        &state,
        "i/notifications-grouped",
        serde_json::json!({
            "limit": 100,
            "sinceId": grouped[4]["id"],
            "untilId": grouped[0]["id"],
            "sinceDate": 1,
            "untilDate": 9_999_999_999_999i64,
            "markAsRead": false,
            "includeTypes": ["reaction", "renote", "follow", "mention"],
            "excludeTypes": [],
        }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{full}");
    // includeTypes filters before grouping (mention + follow dropped).
    assert_eq!(full.as_array().unwrap().len(), 3);

    // markAsRead defaults to true and flips everything read.
    let (status, _) = call(
        &state,
        "i/notifications-grouped",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, after) = call(
        &state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "limit": 100 }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{after}");
    assert!(
        after
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n["isRead"] == true)
    );

    // Grouped pseudo-types are valid filter entries (and match nothing
    // alone, upstream parity).
    let (status, none) = call(
        &state,
        "i/notifications-grouped",
        serde_json::json!({ "markAsRead": false, "includeTypes": ["reaction:grouped"] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{none}");
    assert!(none.as_array().unwrap().is_empty());
    // Auth + unknown-type validation mirror `i/notifications`.
    let (status, _) = call(
        &state,
        "i/notifications-grouped",
        serde_json::json!({}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, value) = call(
        &state,
        "i/notifications-grouped",
        serde_json::json!({ "includeTypes": ["bogus"] }),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");
    let _ = alice_token;
}

/// §11 read-time parity: `includeTypes`/`excludeTypes` accept the same
/// 20 upstream type names on `i/notifications` and
/// `i/notifications-grouped`, with identical matching. Covers the
/// remaining name mappings not exercised by the per-endpoint tests:
/// `quote` selects renote rows, unemitted `pollEnded` matches nothing,
/// obsolete `pollVote`/`groupInvited` are silently dropped (so an
/// include-list of only obsolete types matches nothing), and the
/// grouped pseudo-types match nothing alone while excluding nothing.
#[tokio::test]
async fn notifications_type_filters_agree_on_both_endpoints() {
    use crate::domain::notification::{Notification, NotificationKind};

    let (state, clock) = test_state();
    let (alice_id, _) = signup_user(&state, "alice").await;
    let (bob_id, bob_token) = signup_user(&state, "bob").await;

    // One notification per emittable kind (direct inserts, oldest first).
    let kinds = [
        NotificationKind::Follow,
        NotificationKind::ReceiveFollowRequest,
        NotificationKind::Mention,
        NotificationKind::Reply,
        NotificationKind::Renote,
        NotificationKind::Reaction,
        NotificationKind::ScheduledNotePosted,
        NotificationKind::ScheduledNotePostFailed,
    ];
    for kind in kinds {
        state
            .services
            .notifications
            .insert_notification(Notification {
                clip_id: None,
                page_id: None,
                id: state.services.ids.generate(),
                notifiee_id: bob_id.clone(),
                notifier_id: alice_id.clone(),
                kind,
                note_id: None,
                reaction: None,
                draft_id: None,
                file_id: None,
                exported_entity: None,
                is_read: false,
            })
            .unwrap();
        clock.advance_ms(1_000);
    }

    // `quote` selects the renote row on both endpoints (local quotes
    // notify as `Renote`).
    for endpoint in ["i/notifications", "i/notifications-grouped"] {
        let (status, rows) = call(
            &state,
            endpoint,
            serde_json::json!({ "markAsRead": false, "includeTypes": ["quote"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {rows}");
        let rows = rows.as_array().unwrap();
        assert_eq!(rows.len(), 1, "{endpoint}: quote selects the renote row");
        assert!(rows[0]["type"] == "renote" || rows[0]["type"] == "renote:grouped");
    }
    // Unemitted `pollEnded` matches nothing on both endpoints.
    for endpoint in ["i/notifications", "i/notifications-grouped"] {
        let (status, rows) = call(
            &state,
            endpoint,
            serde_json::json!({ "markAsRead": false, "includeTypes": ["pollEnded"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {rows}");
        assert!(rows.as_array().unwrap().is_empty(), "{endpoint}");
    }
    // Obsolete types are dropped: an include-list of only obsolete
    // types matches nothing (presence of `includeTypes` still filters).
    for endpoint in ["i/notifications", "i/notifications-grouped"] {
        let (status, rows) = call(
            &state,
            endpoint,
            serde_json::json!({ "markAsRead": false, "includeTypes": ["pollVote", "groupInvited"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {rows}");
        assert!(rows.as_array().unwrap().is_empty(), "{endpoint}");
    }
    // Grouped pseudo-types match nothing alone and exclude nothing, on
    // both endpoints identically.
    for endpoint in ["i/notifications", "i/notifications-grouped"] {
        let (status, none) = call(
            &state,
            endpoint,
            serde_json::json!({ "markAsRead": false, "includeTypes": ["reaction:grouped", "renote:grouped"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {none}");
        assert!(none.as_array().unwrap().is_empty(), "{endpoint}");
        let (status, all) = call(
            &state,
            endpoint,
            serde_json::json!({ "markAsRead": false, "excludeTypes": ["reaction:grouped", "renote:grouped"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {all}");
        assert_eq!(all.as_array().unwrap().len(), 8, "{endpoint}");
    }
    // Excluding every emittable kind matches nothing on both endpoints.
    for endpoint in ["i/notifications", "i/notifications-grouped"] {
        let (status, none) = call(
            &state,
            endpoint,
            serde_json::json!({
                "markAsRead": false,
                "excludeTypes": ["follow", "receiveFollowRequest", "mention", "reply", "renote", "reaction", "scheduledNotePosted", "scheduledNotePostFailed"],
            }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {none}");
        assert!(none.as_array().unwrap().is_empty(), "{endpoint}");
    }
    // Unknown names are INVALID_PARAM on both endpoints.
    for endpoint in ["i/notifications", "i/notifications-grouped"] {
        let (status, value) = call(
            &state,
            endpoint,
            serde_json::json!({ "includeTypes": ["bogus"] }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{endpoint}");
        assert_eq!(value["code"], "INVALID_PARAM");
    }
}
