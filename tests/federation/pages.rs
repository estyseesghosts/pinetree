//! Federated-pages integration tests. Moved verbatim from
//! `tests/federation.rs`.

use super::common::{
    api_json, api_post, boot_node, drain_all, http_get, pump_until, signed_get_to_url,
    signed_post_to_inbox, test_client,
};

use std::time::Duration;

use serde_json::{Value, json};

use pinetree::application::ports::{AccountStore, Clock, FollowStore, NoteStore};

// ---------------------------------------------------------------------------
// Federated pages (handoff gate 6): two-local-pinetree page federation.
// Article round-trip (title/summary/content/eye-catch), Update(Article)
// converges, scripted page federates as static-only (no script bytes in
// ANY federated payload), page-like round-trip, inbound Article fixture
// renders sanitized.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn federation_pages_article_round_trip_update_and_likes() {
    use pinetree::application::ports::PageStore;

    let a = boot_node("pages-a");
    let b = boot_node("pages-b");
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
    a.services
        .ensure_actor_keypair(&alice_id)
        .expect("alice key");
    b.services.ensure_actor_keypair(&bob_id).expect("bob key");
    let alice_uri = format!("{}/users/{}", a.base_url, alice_id);
    let bob_uri = format!("{}/users/{}", b.base_url, bob_id);

    // Federation switches ON on A.
    a.services
        .update_instance_settings(
            &alice_id,
            None,
            None,
            None,
            Some(true),
            None,
            a.services.clock.now_ms(),
        )
        .expect("federatePages on");

    // Bob follows alice BEFORE the page exists (fan-out resolves at send).
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

    // Alice creates a scripted federated page.
    let page = api_json(
        &api_post(
            &client,
            &format!("{}/api/pages/create", a.base_url),
            Some(&alice_token),
            &json!({
                "title": "Hello",
                "name": "hello",
                "content": [{ "type": "text", "text": "federated body" }],
                "variables": [],
                "script": "let s3cret = `run()`;",
                "federate": true,
            }),
        ),
        "A page create",
    );
    let page_id = page["id"].as_str().unwrap().to_string();
    let article_uri = format!("{}/pages/{}", a.base_url, page_id);

    // B receives Create(Article) → materializes a remote page (not a
    // note): title/content fidelity, script never stored.
    pump_until(&a, &b, "B ingests the Article", deadline, &mut || {
        drain_all(&a, &b);
        b.services
            .pages
            .get_page_by_uri(&article_uri)
            .expect("lookup article")
            .is_some()
    })
    .await;
    let stored = b
        .services
        .pages
        .get_page_by_uri(&article_uri)
        .expect("article page")
        .unwrap();
    assert_eq!(stored.title, "Hello");
    assert_eq!(stored.script, "", "remote script never stored");
    assert_eq!(stored.uri.as_deref(), Some(article_uri.as_str()));
    assert!(stored.owner_id != alice_id, "remote-owned row");
    let content = serde_json::to_string(&stored.content).unwrap();
    assert!(
        content.contains("federated body"),
        "body survived: {content:?}"
    );
    assert!(
        !content.contains("s3cret") && !content.contains("run()`"),
        "no script: {content:?}"
    );
    // No note row shadows the page.
    assert!(
        b.services
            .notes
            .find_by_uri(&article_uri)
            .expect("note lookup")
            .is_none()
    );

    // The served Article itself carries no script bytes anywhere.
    let article: Value =
        serde_json::from_slice(&http_get(&client, &article_uri, "application/activity+json").body)
            .expect("article json");
    assert_eq!(article["type"], "Article");
    assert_eq!(article["name"], "Hello");
    assert!(
        article["content"]
            .as_str()
            .unwrap()
            .contains("federated body")
    );
    let wire = serde_json::to_string(&article).unwrap();
    assert!(
        !wire.contains("s3cret") && !wire.contains("run()`"),
        "wire has no script"
    );

    // Update converges: alice edits → Update(Article) → B's page updates.
    let r = api_post(
        &client,
        &format!("{}/api/pages/update", a.base_url),
        Some(&alice_token),
        &json!({"pageId": page_id, "title": "Hello 2", "content": [{ "type": "text", "text": "updated body" }]}),
    );
    assert_eq!(
        r.status,
        204,
        "page update: {}",
        String::from_utf8_lossy(&r.body)
    );
    pump_until(&a, &b, "B page updated", deadline, &mut || {
        drain_all(&a, &b);
        b.services
            .pages
            .get_page_by_uri(&article_uri)
            .expect("lookup")
            .map(|p| {
                p.title == "Hello 2"
                    && serde_json::to_string(&p.content)
                        .unwrap()
                        .contains("updated body")
            })
            .unwrap_or(false)
    })
    .await;

    // Page-like round-trip: bob's Like(page URI) → A's inbox → count bumps.
    let like = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/likes/bob-1", b.base_url),
        "type": "Like",
        "actor": bob_uri,
        "object": article_uri,
    });
    let body = serde_json::to_vec(&like).unwrap();
    let r = signed_post_to_inbox(
        &client,
        &b.services,
        &bob_id,
        &format!("{}/inbox", a.base_url),
        &body,
        None,
    );
    assert!((200..300).contains(&r.status), "like POST: {}", r.status);
    let count = a
        .services
        .pages
        .get_page(&page_id)
        .expect("page")
        .unwrap()
        .liked_count;
    assert_eq!(count, 1, "page like recorded");
    // Replay converges (no double count).
    let r = signed_post_to_inbox(
        &client,
        &b.services,
        &bob_id,
        &format!("{}/inbox", a.base_url),
        &body,
        None,
    );
    assert!((200..300).contains(&r.status));
    let count = a
        .services
        .pages
        .get_page(&page_id)
        .expect("page")
        .unwrap()
        .liked_count;
    assert_eq!(count, 1, "replay converges");
    // Unknown-page likes are ignored, not errors.
    let like_unknown = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/likes/bob-2", b.base_url),
        "type": "Like",
        "actor": bob_uri,
        "object": format!("{}/pages/aaaaaaaaaaaaaaaa", a.base_url),
    });
    let body = serde_json::to_vec(&like_unknown).unwrap();
    let r = signed_post_to_inbox(
        &client,
        &b.services,
        &bob_id,
        &format!("{}/inbox", a.base_url),
        &body,
        None,
    );
    assert!((200..300).contains(&r.status));

    // Inbound Article fixture with hostile markup materializes as a
    // sanitized page (never a note, never executable).
    let evil = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/articles/evil-1", b.base_url),
        "type": "Create",
        "actor": bob_uri,
        "object": {
            "id": format!("{}/articles/evil-1", b.base_url),
            "type": "Article",
            "attributedTo": bob_uri,
            "name": "evil",
            "content": "<p>hi</p><script>alert(1)</script><img src=x onerror=alert(2)>",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
        },
    });
    let body = serde_json::to_vec(&evil).unwrap();
    let r = signed_post_to_inbox(
        &client,
        &b.services,
        &bob_id,
        &format!("{}/inbox", a.base_url),
        &body,
        None,
    );
    assert!(
        (200..300).contains(&r.status),
        "evil article POST: {}",
        r.status
    );
    let stored = a
        .services
        .pages
        .get_page_by_uri(&format!("{}/articles/evil-1", b.base_url))
        .expect("evil lookup")
        .expect("evil stored as page");
    assert_eq!(stored.script, "");
    let content = serde_json::to_string(&stored.content).unwrap();
    assert!(
        !content.contains("<script") && !content.contains("onerror"),
        "sanitized: {content:?}"
    );
    assert!(content.contains("hi"), "readable content kept: {content:?}");
    assert!(
        a.services
            .notes
            .find_by_uri(&format!("{}/articles/evil-1", b.base_url))
            .expect("evil note lookup")
            .is_none(),
        "no shadow note row"
    );
}

/// Phase 2 gates (pages): gated `Article` → remote page row (not a note)
/// for the followers audience; strangers get 403/404 with zero content
/// bytes on every surface (show/AP/like/featured); `Update`/`Delete`/`Like`
/// converge; origin timestamps survive the round trip; non-public content
/// never appears in unauthorized readers' packs.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn federation_page_gated_visibility_and_lifecycle() {
    use pinetree::domain::note::Visibility;

    let a = boot_node("page2-a");
    let b = boot_node("page2-b");
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
    let mallory_token = v["token"].as_str().unwrap().to_string();
    a.services
        .ensure_actor_keypair(&alice_id)
        .expect("alice key");
    b.services.ensure_actor_keypair(&bob_id).expect("bob key");
    b.services
        .ensure_actor_keypair(&mallory_id)
        .expect("mallory key");
    let alice_uri = format!("{}/users/{}", a.base_url, alice_id);
    a.services
        .update_instance_settings(
            &alice_id,
            None,
            None,
            None,
            Some(true),
            None,
            a.services.clock.now_ms(),
        )
        .expect("federatePages on");

    // Bob follows alice (Create(Article) fan-out edge).
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
            .find_by_uri(&format!("{}/users/{}", b.base_url, bob_id))
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

    // Alice creates a followers-visibility page (federate defaults on).
    let body = json!({
        "title": "Secret Plans", "name": "secret-plans",
        "content": [{"type": "text", "text": "followers hello"}],
        "variables": [], "script": "",
        "pageVisibility": "followers",
    });
    let page = api_json(
        &api_post(
            &client,
            &format!("{}/api/pages/create", a.base_url),
            Some(&alice_token),
            &body,
        ),
        "A page create",
    );
    let page_id = page["id"].as_str().unwrap().to_string();
    let article_uri = format!("{}/pages/{}", a.base_url, page_id);
    // Local default would be Public; assert the gate took the param.
    assert_eq!(
        a.services
            .pages
            .get_page(&page_id)
            .expect("get")
            .expect("row")
            .visibility,
        Visibility::Followers
    );

    // Fan-out delivers Create(Article) to bob's inbox → B materializes a
    // remote page row (NOT a note) with Followers visibility.
    pump_until(&a, &b, "B materializes gated page", deadline, &mut || {
        b.services
            .pages
            .get_page_by_uri(&article_uri)
            .expect("lookup")
            .is_some()
    })
    .await;
    let b_row = b
        .services
        .pages
        .get_page_by_uri(&article_uri)
        .expect("lookup")
        .expect("row");
    assert_eq!(b_row.visibility, Visibility::Followers);
    assert_eq!(b_row.owner_id, alice_on_b);
    assert_eq!(b_row.script, "");
    assert!(
        b.services
            .notes
            .find_by_uri(&article_uri)
            .expect("note lookup")
            .is_none(),
        "no shadow note"
    );
    // Origin timestamps survive the round trip (stored on the materialized
    // row; remote pages are deliver-only, never re-served: B's copy has
    // `federate: false` so its AP surface stays 404).
    // Origin timestamps survive the round trip (stored on the materialized
    // row; remote pages are deliver-only, never re-served: B's copy has
    // `federate: false` so its AP surface stays 404). Owner AP fetches are
    // not exercised over the loopback server (the gate resolves keys over
    // HTTP, so a local signer would self-fetch; production nodes never do
    // this — owner reads go through the API, covered below).
    let first_published = b_row.remote_published_ms;
    assert!(first_published.is_some(), "origin timestamp stored");

    // Stranger surfaces: 403 signed / 404 anonymous with zero content bytes.
    let r = signed_get_to_url(
        &client,
        &b.services,
        &mallory_id,
        &article_uri,
        "application/activity+json",
    );
    assert_eq!(r.status, 403, "stranger signed → 403");
    assert!(
        !String::from_utf8_lossy(&r.body).contains("followers hello"),
        "zero content bytes (403)"
    );
    let r = http_get(&client, &article_uri, "application/activity+json");
    assert_eq!(r.status, 404, "anonymous → 404");
    assert!(
        !String::from_utf8_lossy(&r.body).contains("followers hello"),
        "zero content bytes (404)"
    );
    // pages/show hides from strangers (B-side copy, mallory viewer;
    // Misskey surfaces NO_SUCH_PAGE as HTTP 400 with the code).
    let r = api_post(
        &client,
        &format!("{}/api/pages/show", b.base_url),
        Some(&mallory_token),
        &json!({"pageId": b_row.id}),
    );
    assert_eq!(
        r.status,
        400,
        "show hides: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert!(String::from_utf8_lossy(&r.body).contains("NO_SUCH_PAGE"));
    assert!(
        !String::from_utf8_lossy(&r.body).contains("followers hello"),
        "show leaks nothing"
    );
    // pages/like by a stranger is rejected without a distinguishing error.
    let r = api_post(
        &client,
        &format!("{}/api/pages/like", b.base_url),
        Some(&mallory_token),
        &json!({"pageId": b_row.id}),
    );
    assert_eq!(r.status, 400, "like hides: {}", r.status);
    assert!(String::from_utf8_lossy(&r.body).contains("NO_SUCH_PAGE"));
    // Owner + follower still read.
    let r = api_post(
        &client,
        &format!("{}/api/pages/show", a.base_url),
        Some(&alice_token),
        &json!({"pageId": page_id}),
    );
    assert!((200..300).contains(&r.status), "owner reads");
    let r = api_post(
        &client,
        &format!("{}/api/pages/show", b.base_url),
        Some(&bob_token),
        &json!({"pageId": b_row.id}),
    );
    assert!((200..300).contains(&r.status), "follower reads");

    // Like converges + notifies the owner (closes the silent-likes gap).
    let r = api_post(
        &client,
        &format!("{}/api/pages/like", b.base_url),
        Some(&bob_token),
        &json!({"pageId": b_row.id}),
    );
    assert_eq!(
        r.status,
        204,
        "bob likes: {}",
        String::from_utf8_lossy(&r.body)
    );
    drain_all(&a, &b);
    assert_eq!(
        a.services
            .pages
            .get_page(&page_id)
            .expect("get")
            .expect("row")
            .liked_count,
        1
    );
    let notes = a
        .services
        .notifications
        .list_notifications(&alice_id, 50)
        .expect("notifs");
    assert!(
        notes.iter().any(
            |n| n.kind == pinetree::domain::notification::NotificationKind::PageLiked
                && n.page_id.as_deref() == Some(page_id.as_str())
        ),
        "owner notified of page like"
    );
    // Featured is public-only (pure query): the gated page never surfaces,
    // even for followers. A public control page does — for everyone.
    let open = api_json(
        &api_post(
            &client,
            &format!("{}/api/pages/create", a.base_url),
            Some(&alice_token),
            &json!({
                "title": "Open Plans", "name": "open-plans",
                "content": [{"type": "text", "text": "open hello"}],
                "variables": [], "script": "",
            }),
        ),
        "A open page create",
    );
    let open_uri = format!("{}/pages/{}", a.base_url, open["id"].as_str().unwrap());
    pump_until(&a, &b, "B materializes open page", deadline, &mut || {
        b.services
            .pages
            .get_page_by_uri(&open_uri)
            .expect("lookup")
            .is_some()
    })
    .await;
    let open_copy = b
        .services
        .pages
        .get_page_by_uri(&open_uri)
        .expect("lookup")
        .expect("open row")
        .id;
    let r = api_post(
        &client,
        &format!("{}/api/pages/like", b.base_url),
        Some(&bob_token),
        &json!({"pageId": open_copy}),
    );
    assert_eq!(r.status, 204);
    drain_all(&a, &b);
    let featured_bob: Value = serde_json::from_slice(
        &api_post(
            &client,
            &format!("{}/api/pages/featured", b.base_url),
            Some(&bob_token),
            &json!({}),
        )
        .body,
    )
    .expect("featured json");
    let bl = featured_bob.as_array().cloned().unwrap_or_default();
    assert!(
        !bl.iter().any(|p| p["id"] == b_row.id),
        "featured never lists gated"
    );
    assert!(
        bl.iter().any(|p| p["id"] == open_copy),
        "featured lists public for follower"
    );
    let featured_mal: Value = serde_json::from_slice(
        &api_post(
            &client,
            &format!("{}/api/pages/featured", b.base_url),
            Some(&mallory_token),
            &json!({}),
        )
        .body,
    )
    .expect("featured json");
    let ml = featured_mal.as_array().cloned().unwrap_or_default();
    assert!(
        !ml.iter().any(|p| p["id"] == b_row.id),
        "featured hides gated"
    );
    assert!(
        ml.iter().any(|p| p["id"] == open_copy),
        "featured lists public for stranger"
    );

    // Update converges (visibility carried forward).
    let r = api_post(
        &client,
        &format!("{}/api/pages/update", a.base_url),
        Some(&alice_token),
        &json!({"pageId": page_id, "title": "Secret Plans v2"}),
    );
    assert!(
        (200..300).contains(&r.status),
        "update: {}",
        String::from_utf8_lossy(&r.body)
    );
    pump_until(&a, &b, "B refreshes page", deadline, &mut || {
        b.services
            .pages
            .get_page_by_uri(&article_uri)
            .expect("lookup")
            .map(|p| p.title == "Secret Plans v2")
            .unwrap_or(false)
    })
    .await;
    let b_row2 = b
        .services
        .pages
        .get_page_by_uri(&article_uri)
        .expect("lookup")
        .expect("row");
    assert_eq!(b_row2.visibility, Visibility::Followers);
    assert!(
        b_row2.remote_updated_ms != b_row.remote_updated_ms,
        "origin updated tracks the new served doc"
    );

    // Delete converges (retracts the materialized row).
    let r = api_post(
        &client,
        &format!("{}/api/pages/delete", a.base_url),
        Some(&alice_token),
        &json!({"pageId": page_id}),
    );
    assert_eq!(
        r.status,
        204,
        "delete: {}",
        String::from_utf8_lossy(&r.body)
    );
    pump_until(&a, &b, "B retracts page", deadline, &mut || {
        b.services
            .pages
            .get_page_by_uri(&article_uri)
            .expect("lookup")
            .is_none()
    })
    .await;
    let _ = alice_uri;
}
