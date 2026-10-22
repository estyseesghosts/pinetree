//! Federated-clips integration tests. Moved verbatim from
//! `tests/federation.rs`.

use super::common::{
    api_json, api_post, boot_node, clip_items, clip_items_signed, drain_all, http_get, pump_until,
    signed_get_to_url, signed_post_to_inbox, test_client,
};

use std::time::Duration;

use serde_json::{Value, json};

use pinetree::application::ports::{AccountStore, Clock, FollowStore, NoteStore};

// ---------------------------------------------------------------------------
// Federated clips (handoff gate 5): two-local-pinetree clip federation.
// Listed collection resolves with per-item gating; unlisted works by
// capability URI and stays unadvertised; remote-approved refuses strangers
// (403) and 404s unknown/unsigned fetchers; TTL revalidation + Add/Remove
// converge; switches/flags off ⇒ zero remote footprint.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn federation_clips_listed_unlisted_approved_and_sync() {
    use pinetree::activitypub::federated_collections::{
        remote_clip_cached_uris, resolve_remote_clip,
    };

    let a = boot_node("clips-a");
    let b = boot_node("clips-b");
    let client = test_client();
    let deadline = Duration::from_secs(25);

    // Signup: alice@A, bob + mallory@B.
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", a.base_url),
            None,
            &json!({"username": "alice", "password": "s3cret-pw"}),
        ),
        "A signup alice",
    );
    let alice_id = v["id"].as_str().unwrap().to_string();
    let alice_token = v["token"].as_str().unwrap().to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", b.base_url),
            None,
            &json!({"username": "bob", "password": "s3cret-pw"}),
        ),
        "B signup bob",
    );
    let bob_id = v["id"].as_str().unwrap().to_string();
    let bob_token = v["token"].as_str().unwrap().to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", b.base_url),
            None,
            &json!({"username": "mallory", "password": "s3cret-pw"}),
        ),
        "B signup mallory",
    );
    let mallory_id = v["id"].as_str().unwrap().to_string();
    let mallory_token = v["token"].as_str().unwrap().to_string();
    a.services
        .ensure_actor_keypair(&alice_id)
        .expect("alice key");
    b.services.ensure_actor_keypair(&bob_id).expect("bob key");
    b.services
        .ensure_actor_keypair(&mallory_id)
        .expect("mallory key");
    let alice_uri = format!("{}/users/{}", a.base_url, alice_id);
    let bob_uri = format!("{}/users/{}", b.base_url, bob_id);

    // Federation switches ON on A (B serves nothing in this test).
    a.services
        .update_instance_settings(
            &alice_id,
            None,
            None,
            Some(true),
            None,
            None,
            a.services.clock.now_ms(),
        )
        .expect("federateClips on");

    // Alice: public note + followers-only note + listed federated clip.
    let n1 = api_json(
        &api_post(
            &client,
            &format!("{}/api/notes/create", a.base_url),
            Some(&alice_token),
            &json!({"text": "clip me"}),
        ),
        "A note n1",
    )["createdNote"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let n2 = api_json(
        &api_post(
            &client,
            &format!("{}/api/notes/create", a.base_url),
            Some(&alice_token),
            &json!({"text": "followers only", "visibility": "followers"}),
        ),
        "A note n2",
    )["createdNote"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let n1_uri = format!("{}/notes/{}", a.base_url, n1);
    let n2_uri = format!("{}/notes/{}", a.base_url, n2);
    let clip = api_json(
        &api_post(
            &client,
            &format!("{}/api/clips/create", a.base_url),
            Some(&alice_token),
            &json!({"name": "x", "isPublic": true, "visibility": "listed", "federate": true}),
        ),
        "A clip create",
    );
    let clip_id = clip["id"].as_str().unwrap().to_string();
    let clip_uri = format!("{}/clips/{}", a.base_url, clip_id);
    for note_id in [&n1, &n2] {
        let r = api_post(
            &client,
            &format!("{}/api/clips/add-note", a.base_url),
            Some(&alice_token),
            &json!({"clipId": clip_id, "noteId": note_id}),
        );
        assert_eq!(
            r.status,
            204,
            "add-note {note_id}: {}",
            String::from_utf8_lossy(&r.body)
        );
    }

    // Bob@B follows alice@A (per-item gating needs the follow edge on A).
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/users/show", b.base_url),
            Some(&bob_token),
            &json!({"username": "alice", "host": a.host}),
        ),
        "B resolves alice",
    );
    let alice_on_b = v["id"].as_str().unwrap().to_string();
    let r = api_post(
        &client,
        &format!("{}/api/following/create", b.base_url),
        Some(&bob_token),
        &json!({"userId": alice_on_b}),
    );
    assert!(
        (200..300).contains(&r.status),
        "bob follow alice: {}",
        r.status
    );
    pump_until(&a, &b, "A sees bob as follower", deadline, &mut || {
        a.services
            .accounts
            .find_by_uri(&bob_uri)
            .expect("lookup bob")
            .map(|acc| {
                a.services
                    .follows
                    .is_following(&acc.id, &alice_id)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    })
    .await;

    // Anonymous fetch: listed collection serves, but only the public item.
    let items = clip_items(
        &http_get(&client, &clip_uri, "application/activity+json"),
        "anon listed",
    );
    assert_eq!(
        items,
        vec![n1_uri.clone()],
        "anonymous sees public items only"
    );
    // Signed fetch as bob (follower): per-item gate admits the followers note.
    let items = clip_items_signed(
        &signed_get_to_url(
            &client,
            &b.services,
            &bob_id,
            &clip_uri,
            "application/activity+json",
        ),
        "bob listed",
        Some((&b.services, &bob_id)),
    );
    assert!(items.contains(&n1_uri), "bob sees {items:?}");
    assert!(
        items.contains(&n2_uri),
        "follower sees followers item: {items:?}"
    );
    // Signed fetch as mallory (stranger): followers item withheld.
    let r = signed_get_to_url(
        &client,
        &b.services,
        &mallory_id,
        &clip_uri,
        "application/activity+json",
    );
    let items = clip_items(&r, "mallory listed");
    assert_eq!(
        items,
        vec![n1_uri.clone()],
        "stranger sees public only: {items:?}"
    );

    // Advertisement: actor doc references the clips index; index lists it.
    let actor_doc: Value =
        serde_json::from_slice(&http_get(&client, &alice_uri, "application/activity+json").body)
            .expect("actor json");
    let index_url = actor_doc["clips"]
        .as_str()
        .expect("actor clips ref")
        .to_string();
    assert_eq!(
        index_url,
        format!("{}/users/{}/clips", a.base_url, alice_id)
    );
    let index_items = clip_items(
        &http_get(&client, &index_url, "application/activity+json"),
        "clips index",
    );
    assert_eq!(index_items, vec![clip_uri.clone()]);

    // Unlisted clip: capability URI serves, index stays silent.
    let unlisted = api_json(
        &api_post(
            &client,
            &format!("{}/api/clips/create", a.base_url),
            Some(&alice_token),
            &json!({"name": "u", "isPublic": false, "visibility": "unlisted", "federate": true}),
        ),
        "A unlisted create",
    );
    let unlisted_id = unlisted["id"].as_str().unwrap().to_string();
    let unlisted_uri = format!("{}/clips/{}", a.base_url, unlisted_id);
    let r = api_post(
        &client,
        &format!("{}/api/clips/add-note", a.base_url),
        Some(&alice_token),
        &json!({"clipId": unlisted_id, "noteId": n1}),
    );
    assert_eq!(r.status, 204);
    let items = clip_items(
        &http_get(&client, &unlisted_uri, "application/activity+json"),
        "anon unlisted",
    );
    assert_eq!(items, vec![n1_uri.clone()], "bearer capability serves");
    let index_items = clip_items(
        &http_get(&client, &index_url, "application/activity+json"),
        "index hides unlisted",
    );
    assert_eq!(
        index_items,
        vec![clip_uri.clone()],
        "unlisted never advertised"
    );

    // Remote-approved clip: stranger signed → 403; unknown/unsigned → 404.
    let approved = api_json(
        &api_post(
            &client,
            &format!("{}/api/clips/create", a.base_url),
            Some(&alice_token),
            &json!({"name": "r", "isPublic": false, "visibility": "remote_approved", "federate": true, "approvedActorUris": [bob_uri]}),
        ),
        "A approved create",
    );
    let approved_uri = format!("{}/clips/{}", a.base_url, approved["id"].as_str().unwrap());
    let r = signed_get_to_url(
        &client,
        &b.services,
        &mallory_id,
        &approved_uri,
        "application/activity+json",
    );
    assert_eq!(
        r.status,
        403,
        "stranger refused: {}",
        String::from_utf8_lossy(&r.body)
    );
    let r = http_get(&client, &approved_uri, "application/activity+json");
    assert_eq!(r.status, 404, "unsigned gets no existence signal");
    let r = clip_items_signed(
        &signed_get_to_url(
            &client,
            &b.services,
            &bob_id,
            &approved_uri,
            "application/activity+json",
        ),
        "approver",
        Some((&b.services, &bob_id)),
    );
    assert!(r.is_empty(), "approver served, zero members: {r:?}");
    // Private clip: 404 for everyone remote.
    let private = api_json(
        &api_post(
            &client,
            &format!("{}/api/clips/create", a.base_url),
            Some(&alice_token),
            &json!({"name": "p", "federate": true}),
        ),
        "A private create",
    );
    let private_uri = format!("{}/clips/{}", a.base_url, private["id"].as_str().unwrap());
    let r = signed_get_to_url(
        &client,
        &b.services,
        &bob_id,
        &private_uri,
        "application/activity+json",
    );
    assert_eq!(r.status, 404, "private never leaves");
    let _ = mallory_token;

    // Membership sync: B primes its cache, then Add/Remove converge.
    // Alice follows bob so bob's public note reaches A for clipping.
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/users/show", a.base_url),
            Some(&alice_token),
            &json!({"username": "bob", "host": b.host}),
        ),
        "A resolves bob",
    );
    let bob_on_a = v["id"].as_str().unwrap().to_string();
    let r = api_post(
        &client,
        &format!("{}/api/following/create", a.base_url),
        Some(&alice_token),
        &json!({"userId": bob_on_a}),
    );
    assert!(
        (200..300).contains(&r.status),
        "alice follow bob: {}",
        r.status
    );
    pump_until(&a, &b, "B sees alice as follower", deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup alice")
            .map(|acc| {
                b.services
                    .follows
                    .is_following(&acc.id, &bob_id)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    })
    .await;
    // B primes the cache (anonymous pull: public items only).
    let pulled = resolve_remote_clip(&b.services, &clip_uri).expect("prime pull");
    assert_eq!(pulled, vec![n1_uri.clone()]);
    assert!(remote_clip_cached_uris().contains(&clip_uri));
    // Bob posts; alice (follower) ingests; alice clips it.
    let m1_b = api_json(
        &api_post(
            &client,
            &format!("{}/api/notes/create", b.base_url),
            Some(&bob_token),
            &json!({"text": "clip this too"}),
        ),
        "B note m1",
    )["createdNote"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let m1_uri = format!("{}/notes/{}", b.base_url, m1_b);
    pump_until(&a, &b, "A ingests m1", deadline, &mut || {
        a.services
            .notes
            .find_by_uri(&m1_uri)
            .expect("lookup m1")
            .is_some()
    })
    .await;
    let m1_on_a = a
        .services
        .notes
        .find_by_uri(&m1_uri)
        .expect("m1 row")
        .unwrap()
        .id
        .clone();
    let r = api_post(
        &client,
        &format!("{}/api/clips/add-note", a.base_url),
        Some(&alice_token),
        &json!({"clipId": clip_id, "noteId": m1_on_a}),
    );
    assert_eq!(
        r.status,
        204,
        "clip remote note: {}",
        String::from_utf8_lossy(&r.body)
    );
    // The Add reaches B's inbox and evicts the cache; the re-pull converges.
    pump_until(&a, &b, "B cache evicted by Add", deadline, &mut || {
        drain_all(&a, &b);
        !remote_clip_cached_uris().contains(&clip_uri)
    })
    .await;
    let pulled = resolve_remote_clip(&b.services, &clip_uri).expect("re-pull");
    assert!(
        pulled.contains(&n1_uri) && pulled.contains(&m1_uri),
        "Add converged: {pulled:?}"
    );
    // Remove converges the same way.
    let r = api_post(
        &client,
        &format!("{}/api/clips/remove-note", a.base_url),
        Some(&alice_token),
        &json!({"clipId": clip_id, "noteId": m1_on_a}),
    );
    assert_eq!(r.status, 204);
    pump_until(&a, &b, "B cache evicted by Remove", deadline, &mut || {
        drain_all(&a, &b);
        !remote_clip_cached_uris().contains(&clip_uri)
    })
    .await;
    let pulled = resolve_remote_clip(&b.services, &clip_uri).expect("re-pull 2");
    assert_eq!(pulled, vec![n1_uri.clone()], "Remove converged: {pulled:?}");

    // Switch off ⇒ zero footprint: routes 404, advertisement gone.
    a.services
        .update_instance_settings(
            &alice_id,
            None,
            None,
            Some(false),
            None,
            None,
            a.services.clock.now_ms(),
        )
        .expect("federateClips off");
    let r = http_get(&client, &clip_uri, "application/activity+json");
    assert_eq!(r.status, 404, "switch off 404s collections");
    let r = http_get(&client, &index_url, "application/activity+json");
    assert_eq!(r.status, 404, "switch off 404s the index");
    let actor_doc: Value =
        serde_json::from_slice(&http_get(&client, &alice_uri, "application/activity+json").body)
            .expect("actor json 2");
    assert!(
        actor_doc.get("clips").is_none(),
        "no advertisement when off"
    );
    // Evict B's cached resolution first: the switch-off must break the
    // NETWORK path (a warm cache legitimately serves without fetching).
    pinetree::activitypub::federated_collections::evict_remote_clip_cache(&clip_uri);
    assert!(
        resolve_remote_clip(&b.services, &clip_uri).is_err(),
        "no resolution when off"
    );
}

/// Phase 2 gates (clips): subscribe handshake (`Follow`/`Undo` converge,
/// forged updates ignored), `Update`/`Delete` converge incl.
/// tier-restriction pruning, fan-out receipt at followers, author +
/// follower notifications, TTL + `updated` revalidation, tombstone at the
/// deleted URI.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn federation_clip_follow_sync_update_delete() {
    use pinetree::activitypub::federated_collections::{
        evict_remote_clip_cache, remote_clip_cached_for, resolve_remote_clip,
        resolve_remote_clip_with_ttl,
    };
    use pinetree::domain::notification::NotificationKind;

    let a = boot_node("clip2-a");
    let b = boot_node("clip2-b");
    let client = test_client();
    let deadline = Duration::from_secs(25);

    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", a.base_url),
            None,
            &json!({"username": "alice", "password": "s3cret-pw"}),
        ),
        "A signup alice",
    );
    let alice_id = v["id"].as_str().unwrap().to_string();
    let alice_token = v["token"].as_str().unwrap().to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", b.base_url),
            None,
            &json!({"username": "bob", "password": "s3cret-pw"}),
        ),
        "B signup bob",
    );
    let bob_id = v["id"].as_str().unwrap().to_string();
    let bob_token = v["token"].as_str().unwrap().to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", b.base_url),
            None,
            &json!({"username": "mallory", "password": "s3cret-pw"}),
        ),
        "B signup mallory",
    );
    let mallory_id = v["id"].as_str().unwrap().to_string();
    a.services
        .ensure_actor_keypair(&alice_id)
        .expect("alice key");
    b.services.ensure_actor_keypair(&bob_id).expect("bob key");
    b.services
        .ensure_actor_keypair(&mallory_id)
        .expect("mallory key");
    let alice_uri = format!("{}/users/{}", a.base_url, alice_id);
    let bob_uri = format!("{}/users/{}", b.base_url, bob_id);
    let mallory_uri = format!("{}/users/{}", b.base_url, mallory_id);
    a.services
        .update_instance_settings(
            &alice_id,
            None,
            None,
            Some(true),
            None,
            None,
            a.services.clock.now_ms(),
        )
        .expect("federateClips on");

    // Alice: public note + listed federated clip.
    let n1 = api_json(
        &api_post(
            &client,
            &format!("{}/api/notes/create", a.base_url),
            Some(&alice_token),
            &json!({"text": "clip me"}),
        ),
        "A note n1",
    )["createdNote"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let n1_uri = format!("{}/notes/{}", a.base_url, n1);
    let clip = api_json(
        &api_post(
            &client,
            &format!("{}/api/clips/create", a.base_url),
            Some(&alice_token),
            &json!({"name": "x", "isPublic": true, "visibility": "listed", "federate": true}),
        ),
        "A clip create",
    );
    let clip_id = clip["id"].as_str().unwrap().to_string();
    let clip_uri = format!("{}/clips/{}", a.base_url, clip_id);
    let r = api_post(
        &client,
        &format!("{}/api/clips/add-note", a.base_url),
        Some(&alice_token),
        &json!({"clipId": clip_id, "noteId": n1}),
    );
    assert_eq!(r.status, 204);

    // Served doc carries `updated` (Phase 2a revalidation stamp).
    let doc: Value =
        serde_json::from_slice(&http_get(&client, &clip_uri, "application/activity+json").body)
            .expect("collection json");
    assert!(
        doc.get("updated").and_then(Value::as_str).is_some(),
        "updated served: {doc}"
    );

    // Bob follows alice (fan-out edge) + B primes its membership cache.
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/users/show", b.base_url),
            Some(&bob_token),
            &json!({"username": "alice", "host": a.host}),
        ),
        "B resolves alice",
    );
    let alice_on_b = v["id"].as_str().unwrap().to_string();
    let r = api_post(
        &client,
        &format!("{}/api/following/create", b.base_url),
        Some(&bob_token),
        &json!({"userId": alice_on_b}),
    );
    assert!((200..300).contains(&r.status));
    pump_until(&a, &b, "A sees bob as follower", deadline, &mut || {
        a.services
            .accounts
            .find_by_uri(&bob_uri)
            .expect("lookup bob")
            .map(|acc| {
                a.services
                    .follows
                    .is_following(&acc.id, &alice_id)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    })
    .await;
    let bob_on_a = a
        .services
        .accounts
        .find_by_uri(&bob_uri)
        .expect("bob on A")
        .expect("bob row")
        .id;
    let uris = resolve_remote_clip(&b.services, &clip_uri).expect("prime");
    assert_eq!(uris, vec![n1_uri.clone()]);

    // Subscribe handshake: bob's Follow(clip) → alice's personal inbox.
    let alice_inbox = format!("{}/users/{}/inbox", a.base_url, alice_id);
    let follow = json!({
        "id": format!("{}/activities/bob-follow-clip", b.base_url),
        "type": "Follow", "actor": bob_uri, "object": clip_uri,
    });
    let body = serde_json::to_vec(&follow).unwrap();
    let r = signed_post_to_inbox(&client, &b.services, &bob_id, &alice_inbox, &body, None);
    assert!((200..300).contains(&r.status), "follow POST: {}", r.status);
    assert!(
        a.services
            .clips
            .is_following_clip(&bob_on_a, &clip_id)
            .expect("is-following")
    );
    // Undo converges; re-follow for the later steps.
    let undo = json!({
        "id": format!("{}/activities/bob-unfollow-clip", b.base_url),
        "type": "Undo", "actor": bob_uri,
        "object": {"id": format!("{}/activities/bob-follow-clip", b.base_url), "type": "Follow", "actor": bob_uri, "object": clip_uri},
    });
    let body = serde_json::to_vec(&undo).unwrap();
    let r = signed_post_to_inbox(&client, &b.services, &bob_id, &alice_inbox, &body, None);
    assert!((200..300).contains(&r.status));
    assert!(
        !a.services
            .clips
            .is_following_clip(&bob_on_a, &clip_id)
            .expect("unfollowed")
    );
    let body = serde_json::to_vec(&follow).unwrap();
    let r = signed_post_to_inbox(&client, &b.services, &bob_id, &alice_inbox, &body, None);
    assert!((200..300).contains(&r.status));
    assert!(
        a.services
            .clips
            .is_following_clip(&bob_on_a, &clip_id)
            .expect("re-follow")
    );

    // Forged Update (mallory-signed, empty items) is ignored: cache intact.
    let forged = json!({
        "id": format!("{}/activities/forged-update", b.base_url),
        "type": "Update", "actor": mallory_uri,
        "object": {"id": clip_uri, "type": "OrderedCollection", "attributedTo": alice_uri, "orderedItems": []},
    });
    let body = serde_json::to_vec(&forged).unwrap();
    let r = signed_post_to_inbox(
        &client,
        &b.services,
        &mallory_id,
        &format!("{}/inbox", b.base_url),
        &body,
        None,
    );
    assert!((200..300).contains(&r.status));
    // (Delivered to B's own inbox: B holds no such clip → converges quietly.)
    assert_eq!(
        remote_clip_cached_for(&clip_uri),
        Some(vec![n1_uri.clone()]),
        "forged update ignored"
    );

    // Owner-signed Update with an embedded full document applies in one
    // round trip (refresh-or-evict): bogus member lands in B's cache.
    let bogus = format!("{}/notes/bogus", a.base_url);
    let owner_update = json!({
        "id": format!("{}/activities/alice-update-1", a.base_url),
        "type": "Update", "actor": alice_uri,
        "object": {"id": clip_uri, "type": "OrderedCollection", "attributedTo": alice_uri,
            "updated": doc.get("updated").cloned().unwrap_or(Value::Null),
            "orderedItems": [bogus]},
    });
    let body = serde_json::to_vec(&owner_update).unwrap();
    // Sign as alice@A, deliver to B's shared inbox.
    let r = signed_post_to_inbox(
        &client,
        &a.services,
        &alice_id,
        &format!("{}/inbox", b.base_url),
        &body,
        None,
    );
    assert!((200..300).contains(&r.status));
    assert_eq!(
        remote_clip_cached_for(&clip_uri),
        Some(vec![bogus.clone()]),
        "owner update applies embedded doc"
    );
    // Re-prime honestly for the steps below.
    evict_remote_clip_cache(&clip_uri);
    let uris = resolve_remote_clip(&b.services, &clip_uri).expect("re-prime");
    assert_eq!(uris, vec![n1_uri.clone()]);

    // Metadata edit notifies clip followers (bob) with the clip ref.
    let r = api_post(
        &client,
        &format!("{}/api/clips/update", a.base_url),
        Some(&alice_token),
        &json!({"clipId": clip_id, "name": "renamed"}),
    );
    assert!(
        (200..300).contains(&r.status),
        "rename: {}",
        String::from_utf8_lossy(&r.body)
    );
    drain_all(&a, &b);
    // Update fan-out refreshes B's cache (entry present again post-drain).
    let _ = resolve_remote_clip_with_ttl(&b.services, &clip_uri, Duration::from_secs(600))
        .expect("post-update resolve");
    let notes = a
        .services
        .notifications
        .list_notifications(&bob_on_a, 50)
        .expect("notifs");
    assert!(
        notes.iter().any(|n| n.kind == NotificationKind::ClipUpdated
            && n.clip_id.as_deref() == Some(clip_id.as_str())),
        "bob notified of metadata edit: {:?}",
        notes
            .iter()
            .map(|n| (&n.kind, &n.clip_id))
            .collect::<Vec<_>>()
    );

    // Membership: bob's note clipped → author notified + Add delivered.
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/users/show", a.base_url),
            Some(&alice_token),
            &json!({"username": "bob", "host": b.host}),
        ),
        "A resolves bob",
    );
    let bob_on_a2 = v["id"].as_str().unwrap().to_string();
    assert_eq!(bob_on_a, bob_on_a2);
    let r = api_post(
        &client,
        &format!("{}/api/following/create", a.base_url),
        Some(&alice_token),
        &json!({"userId": bob_on_a}),
    );
    assert!((200..300).contains(&r.status));
    pump_until(&a, &b, "B sees alice as follower", deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup alice")
            .map(|acc| {
                b.services
                    .follows
                    .is_following(&acc.id, &bob_id)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    })
    .await;
    let bn = api_json(
        &api_post(
            &client,
            &format!("{}/api/notes/create", b.base_url),
            Some(&bob_token),
            &json!({"text": "bob note"}),
        ),
        "B note",
    )["createdNote"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let bn_uri = format!("{}/notes/{}", b.base_url, bn);
    pump_until(&a, &b, "A ingests bob note", deadline, &mut || {
        a.services
            .notes
            .notes_by_author(
                &bob_on_a,
                &pinetree::application::ports::TimelineCursor::with_limit(10),
            )
            .map(|ns| !ns.is_empty())
            .unwrap_or(false)
    })
    .await;
    let a_copy = a
        .services
        .notes
        .notes_by_author(
            &bob_on_a,
            &pinetree::application::ports::TimelineCursor::with_limit(10),
        )
        .expect("a copy")
        .into_iter()
        .next()
        .expect("a copy row")
        .id;
    let r = api_post(
        &client,
        &format!("{}/api/clips/add-note", a.base_url),
        Some(&alice_token),
        &json!({"clipId": clip_id, "noteId": a_copy}),
    );
    assert_eq!(r.status, 204);
    drain_all(&a, &b);
    let notes = a
        .services
        .notifications
        .list_notifications(&bob_on_a, 50)
        .expect("notifs2");
    assert!(
        notes
            .iter()
            .any(|n| n.kind == NotificationKind::ClipUpdated && n.note_id.is_some()),
        "author notified of membership: {:?}",
        notes
            .iter()
            .map(|n| (&n.kind, &n.note_id))
            .collect::<Vec<_>>()
    );
    // Add arrives at B: cache evicted, refetch converges on the new member.
    let uris = resolve_remote_clip_with_ttl(&b.services, &clip_uri, Duration::from_secs(600))
        .expect("post-add");
    assert!(uris.contains(&bn_uri), "Add converged: {uris:?}");

    // Delete: tombstone served at the URI; receivers evict + prune.
    let r = api_post(
        &client,
        &format!("{}/api/clips/delete", a.base_url),
        Some(&alice_token),
        &json!({"clipId": clip_id}),
    );
    assert_eq!(
        r.status,
        204,
        "delete: {}",
        String::from_utf8_lossy(&r.body)
    );
    drain_all(&a, &b);
    let r = http_get(&client, &clip_uri, "application/activity+json");
    assert_eq!(r.status, 410, "tombstone, not 404: {}", r.status);
    let tomb: Value = serde_json::from_slice(&r.body).expect("tomb json");
    assert_eq!(tomb["type"], Value::String("Tombstone".to_string()));
    assert_eq!(
        tomb["formerType"],
        Value::String("OrderedCollection".to_string())
    );
    assert_eq!(
        remote_clip_cached_for(&clip_uri),
        None,
        "B evicted on Delete"
    );
    // Deletion notifies followers with the clip ref.
    let notes = a
        .services
        .notifications
        .list_notifications(&bob_on_a, 50)
        .expect("notifs3");
    assert!(
        notes.iter().any(|n| n.kind == NotificationKind::ClipUpdated
            && n.clip_id.as_deref() == Some(clip_id.as_str())),
        "follower notified of delete"
    );

    // Tier-restriction pruning: re-listed clip → restrict → Delete fans
    // out, remote follow pruned, capability fetch still serves.
    let clip2 = api_json(
        &api_post(
            &client,
            &format!("{}/api/clips/create", a.base_url),
            Some(&alice_token),
            &json!({"name": "y", "isPublic": true, "visibility": "listed", "federate": true}),
        ),
        "A clip2 create",
    );
    let clip2_id = clip2["id"].as_str().unwrap().to_string();
    let clip2_uri = format!("{}/clips/{}", a.base_url, clip2_id);
    let r = api_post(
        &client,
        &format!("{}/api/clips/add-note", a.base_url),
        Some(&alice_token),
        &json!({"clipId": clip2_id, "noteId": n1}),
    );
    assert_eq!(r.status, 204);
    let follow2 = json!({
        "id": format!("{}/activities/bob-follow-clip2", b.base_url),
        "type": "Follow", "actor": bob_uri, "object": clip2_uri,
    });
    let body = serde_json::to_vec(&follow2).unwrap();
    let r = signed_post_to_inbox(&client, &b.services, &bob_id, &alice_inbox, &body, None);
    assert!((200..300).contains(&r.status));
    assert!(
        a.services
            .clips
            .is_following_clip(&bob_on_a, &clip2_id)
            .expect("following2")
    );
    let uris = resolve_remote_clip(&b.services, &clip2_uri).expect("prime2");
    assert_eq!(uris, vec![n1_uri.clone()]);
    let r = api_post(
        &client,
        &format!("{}/api/clips/update", a.base_url),
        Some(&alice_token),
        &json!({"clipId": clip2_id, "visibility": "unlisted"}),
    );
    assert!(
        (200..300).contains(&r.status),
        "restrict: {}",
        String::from_utf8_lossy(&r.body)
    );
    drain_all(&a, &b);
    assert_eq!(
        remote_clip_cached_for(&clip2_uri),
        None,
        "B evicted on restrict"
    );
    assert!(
        !a.services
            .clips
            .is_following_clip(&bob_on_a, &clip2_id)
            .expect("pruned"),
        "remote follow pruned"
    );
    // Capability fetch still serves with zero broadcast.
    let items = clip_items(
        &http_get(&client, &clip2_uri, "application/activity+json"),
        "unlisted bearer",
    );
    assert_eq!(items, vec![n1_uri.clone()]);
    let _ = mallory_id;
}
