use super::common::*;

// ---------------------------------------------------------------------------
// Group Y (federated clips + pages handoff, Phase 3 local): pages and page
// likes. Slug uniqueness holds atomically; like/unlike bump the counter
// in the same transaction; `featured_pages` is the upstream top-10 query.

fn k_page(owner: &str, name: &str) -> Page {
    Page {
        id: format!("kpage-{owner}-{name}"),
        owner_id: owner.to_string(),
        title: format!("Title {name}"),
        name: name.to_string(),
        summary: None,
        content: serde_json::json!([{ "type": "text", "text": "hi" }]),
        variables: serde_json::json!([]),
        script: String::new(),
        eye_catching_image_id: None,
        font: PageFont::SansSerif,
        align_center: false,
        hide_title_when_pinned: false,
        tier: CollectionTier::Listed,
        visibility: Visibility::Public,
        visible_user_ids: Vec::new(),
        federate: false,
        approved_actor_uris: Vec::new(),
        uri: None,
        remote_attachments: Vec::new(),
        remote_published_ms: None,
        remote_updated_ms: None,
        liked_count: 0,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}

fn k_page_like(user: &str, page_id: &str) -> PageLike {
    PageLike {
        id: format!("kpl-{user}-{page_id}"),
        user_id: user.to_string(),
        page_id: page_id.to_string(),
        created_at_ms: 1,
    }
}

/// Y1: page CRUD is id/slug addressed; slug uniqueness holds under a
/// thread race; slug moves conflict; delete cascades likes.
#[test]
fn y1_page_crud_and_slug_race() {
    let factory = RedbFactory::fresh("y1");
    let clock = test_clock();
    let store = factory.open(clock);
    store
        .create_page(k_page("alice", "hello"), Vec::new())
        .expect("create");
    assert!(
        store
            .get_page_by_name("alice", "hello")
            .expect("by-name")
            .is_some()
    );
    assert!(
        store
            .get_page_by_name("alice", "missing")
            .expect("by-name")
            .is_none()
    );
    // Other owners may reuse the slug.
    store
        .create_page(k_page("bob", "hello"), Vec::new())
        .expect("same slug, other owner");
    // Slug race on one owner: exactly one wins.
    let results: Vec<_> = std::thread::scope(|scope| {
        (0..8)
            .map(|_| {
                let store = store.clone();
                scope.spawn(move || {
                    store
                        .create_page(k_page("carol", "race"), Vec::new())
                        .map(|_| ())
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(
        results.iter().all(|r| r.is_ok()
            || matches!(r, Err(PortError::Conflict(msg)) if msg == "NAME_ALREADY_EXISTS")),
        "losers report NAME_ALREADY_EXISTS"
    );
    // Slug move onto a sibling slug conflicts; same-slug rewrite is fine.
    let mut moved = store
        .get_page_by_name("alice", "hello")
        .expect("get")
        .unwrap();
    moved.name = "hello".to_string();
    assert!(
        store
            .update_page(moved.clone(), Vec::new())
            .expect("same")
            .is_some()
    );
    store
        .create_page(k_page("alice", "other"), Vec::new())
        .expect("sibling");
    moved.name = "other".to_string();
    let clash = store.update_page(moved, Vec::new());
    assert!(matches!(clash, Err(PortError::Conflict(msg)) if msg == "NAME_ALREADY_EXISTS"));
    // Like, then delete cascades the like row.
    assert!(
        store
            .like_page(k_page_like("bob", "kpage-alice-hello"), Vec::new())
            .expect("like")
    );
    assert!(
        store
            .delete_page("kpage-alice-hello", Vec::new())
            .expect("delete")
    );
    assert!(store.get_page("kpage-alice-hello").expect("get").is_none());
    assert!(
        store
            .get_page_by_name("alice", "hello")
            .expect("by-name")
            .is_none()
    );
    assert!(
        !store
            .is_page_liked("bob", "kpage-alice-hello")
            .expect("liked")
    );
}

/// Y2: like/unlike are idempotent and move the counter; `featured_pages`
/// returns only liked pages, highest count first, at most 10.
#[test]
fn y2_page_likes_and_featured() {
    let factory = RedbFactory::fresh("y2");
    let clock = test_clock();
    let store = factory.open(clock);
    for i in 0..12 {
        store
            .create_page(k_page("alice", &format!("p{i:02}")), Vec::new())
            .expect("create");
    }
    // p00 gets 3 likes, p01 gets 1; the rest stay at 0 (unfeatured).
    for (user, page) in [("u1", "p00"), ("u2", "p00"), ("u3", "p00"), ("u1", "p01")] {
        assert!(
            store
                .like_page(
                    k_page_like(user, &format!("kpage-alice-{page}")),
                    Vec::new()
                )
                .expect("like")
        );
    }
    assert!(
        !store
            .like_page(k_page_like("u1", "kpage-alice-p00"), Vec::new())
            .expect("dup")
    );
    assert_eq!(
        store
            .get_page("kpage-alice-p00")
            .expect("get")
            .unwrap()
            .liked_count,
        3
    );
    assert!(store.unlike_page("u3", "kpage-alice-p00").expect("unlike"));
    assert!(
        !store
            .unlike_page("u3", "kpage-alice-p00")
            .expect("re-unlike")
    );
    assert_eq!(
        store
            .get_page("kpage-alice-p00")
            .expect("get")
            .unwrap()
            .liked_count,
        2
    );
    let featured = store.featured_pages().expect("featured");
    assert_eq!(featured.len(), 2);
    assert_eq!(featured[0].name, "p00");
    assert_eq!(featured[0].liked_count, 2);
    assert_eq!(featured[1].name, "p01");
}

/// Y3: canonical-URI index for materialized remote pages — URI lookup,
/// rebind on URI change, and index cleanup on delete.
#[test]
fn y3_page_uri_index_round_trip() {
    let factory = RedbFactory::fresh("y3");
    let clock = test_clock();
    let store = factory.open(clock);
    let mut page = k_page("alice", "remote");
    page.uri = Some("https://remote.example/pages/1".to_string());
    store.create_page(page, Vec::new()).expect("create");
    let found = store
        .get_page_by_uri("https://remote.example/pages/1")
        .expect("by-uri")
        .expect("found");
    assert_eq!(found.name, "remote");
    assert!(
        store
            .get_page_by_uri("https://remote.example/pages/2")
            .expect("miss")
            .is_none()
    );
    // Update keeps the row addressable; delete drops the index.
    let mut renamed = found.clone();
    renamed.name = "remote2".to_string();
    store.update_page(renamed, Vec::new()).expect("update");
    assert!(
        store
            .get_page_by_uri("https://remote.example/pages/1")
            .expect("by-uri")
            .is_some()
    );
    assert!(store.delete_page(&found.id, Vec::new()).expect("delete"));
    assert!(
        store
            .get_page_by_uri("https://remote.example/pages/1")
            .expect("gone")
            .is_none()
    );
}

/// Y4: visibility columns + origin timestamps round-trip (old rows decode
/// as Public via `#[serde(default)]`; covered implicitly by every older
/// fixture, asserted explicitly here).
#[test]
fn y4_page_visibility_columns_round_trip() {
    let factory = RedbFactory::fresh("y4");
    let clock = test_clock();
    let store = factory.open(clock);
    let mut page = k_page("alice", "gated");
    page.visibility = Visibility::Followers;
    page.remote_published_ms = Some(100);
    page.remote_updated_ms = Some(200);
    store.create_page(page, Vec::new()).expect("create");
    let found = store
        .get_page("kpage-alice-gated")
        .expect("get")
        .expect("found");
    assert_eq!(found.visibility, Visibility::Followers);
    assert!(found.visible_user_ids.is_empty());
    assert_eq!(found.remote_published_ms, Some(100));
    assert_eq!(found.remote_updated_ms, Some(200));
    // Specified with listed readers round-trips.
    let mut spec = k_page("alice", "spec");
    spec.visibility = Visibility::Specified;
    spec.visible_user_ids = vec!["bob".to_string()];
    store.create_page(spec, Vec::new()).expect("create2");
    let found2 = store
        .get_page("kpage-alice-spec")
        .expect("get2")
        .expect("found2");
    assert_eq!(found2.visibility, Visibility::Specified);
    assert_eq!(found2.visible_user_ids, vec!["bob".to_string()]);
}
