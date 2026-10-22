use super::common::*;

// ---------------------------------------------------------------------------
// Group X (federated clips + pages handoff, Phase 1): clips, clip-notes,
// clip favorites. Cap checks + inserts commit atomically; delete cascades
// memberships + favorites in the same commit; foreign-owner deletes are
// misses, not deletes.

fn k_clip(owner: &str, name: &str) -> Clip {
    Clip {
        id: format!("kclip-{owner}-{name}"),
        owner_id: owner.to_string(),
        name: name.to_string(),
        description: None,
        is_public: false,
        tier: CollectionTier::Private,
        federate: false,
        approved_actor_uris: Vec::new(),
        created_at_ms: 1,
        last_clipped_at_ms: None,
        updated_at_ms: 1,
    }
}

fn k_clip_note(clip_id: &str, note_id: &str) -> ClipNote {
    ClipNote {
        id: format!("kcn-{clip_id}-{note_id}"),
        clip_id: clip_id.to_string(),
        note_id: note_id.to_string(),
        created_at_ms: 1,
    }
}

fn k_clip_fav(user: &str, clip_id: &str) -> ClipFavorite {
    ClipFavorite {
        id: format!("kcf-{user}-{clip_id}"),
        user_id: user.to_string(),
        clip_id: clip_id.to_string(),
        created_at_ms: 1,
    }
}

/// X1: clip CRUD is owner-scoped; the per-owner cap holds under a thread
/// race (exactly the cap wins, losers report `TOO_MANY_CLIPS`).
#[test]
fn x1_clip_crud_and_cap_race() {
    let factory = RedbFactory::fresh("x1");
    let clock = test_clock();
    let store = factory.open(clock);
    let clip = store.create_clip(k_clip("alice", "a")).expect("create");
    assert_eq!(clip.owner_id, "alice");
    // Owner-scoped update; foreign update is a miss.
    let mut renamed = clip.clone();
    renamed.name = "a2".to_string();
    assert!(
        store
            .update_clip(renamed, Vec::new())
            .expect("update")
            .is_some()
    );
    let mut foreign = clip.clone();
    foreign.owner_id = "mallory".to_string();
    assert!(
        store
            .update_clip(foreign, Vec::new())
            .expect("foreign")
            .is_none()
    );
    assert_eq!(store.clips_by_owner("alice").expect("list").len(), 1);
    // Cap race.
    let results: Vec<_> = std::thread::scope(|scope| {
        (0..(CLIP_LIMIT_PER_OWNER + 4))
            .map(|i| {
                let store = store.clone();
                scope.spawn(move || {
                    store
                        .create_clip(k_clip("bob", &format!("c{i:02}")))
                        .map(|_| ())
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    assert_eq!(
        results.iter().filter(|r| r.is_ok()).count(),
        CLIP_LIMIT_PER_OWNER,
        "exactly the cap wins"
    );
    assert!(
        results
            .iter()
            .all(|r| r.is_ok()
                || matches!(r, Err(PortError::Conflict(msg)) if msg == "TOO_MANY_CLIPS")),
        "losers report TOO_MANY_CLIPS"
    );
    // Foreign-owner delete is a miss, not a delete.
    assert!(
        !store
            .delete_clip("mallory", "kclip-alice-a", Vec::new())
            .expect("foreign")
    );
    assert!(store.get_clip("kclip-alice-a").expect("get").is_some());
}

/// X2: clip-note membership (unique pair, per-clip cap, last-clipped
/// touch) + favorite lifecycle + delete cascade.
#[test]
fn x2_clip_notes_favorites_and_cascade() {
    let factory = RedbFactory::fresh("x2");
    let clock = test_clock();
    let store = factory.open(clock);
    store.create_clip(k_clip("alice", "a")).expect("create");
    // Duplicate pair converges on ALREADY_CLIPPED.
    store
        .add_clip_note(k_clip_note("kclip-alice-a", "n1"), 100, Vec::new())
        .expect("add");
    let dup = store.add_clip_note(k_clip_note("kclip-alice-a", "n1"), 101, Vec::new());
    assert!(matches!(dup, Err(PortError::Conflict(msg)) if msg == "ALREADY_CLIPPED"));
    // last_clipped_at touched on add only.
    assert_eq!(
        store
            .get_clip("kclip-alice-a")
            .expect("get")
            .unwrap()
            .last_clipped_at_ms,
        Some(100)
    );
    assert!(
        store
            .remove_clip_note("kclip-alice-a", "n1", 2, Vec::new())
            .expect("remove")
    );
    assert!(
        !store
            .remove_clip_note("kclip-alice-a", "n1", 2, Vec::new())
            .expect("re-remove")
    );
    assert!(
        store
            .get_clip("kclip-alice-a")
            .expect("get")
            .unwrap()
            .last_clipped_at_ms
            == Some(100),
        "remove never clears last_clipped_at"
    );
    // Per-clip cap holds.
    for i in 0..CLIP_NOTES_PER_CLIP {
        store
            .add_clip_note(
                k_clip_note("kclip-alice-a", &format!("m{i:03}")),
                200,
                Vec::new(),
            )
            .expect("fill");
    }
    let over = store.add_clip_note(k_clip_note("kclip-alice-a", "overflow"), 201, Vec::new());
    assert!(matches!(over, Err(PortError::Conflict(msg)) if msg == "TOO_MANY_CLIP_NOTES"));
    assert_eq!(
        store.count_clip_notes("kclip-alice-a").expect("count"),
        CLIP_NOTES_PER_CLIP as u64
    );
    // Newest-first member ids.
    let ids = store.clip_note_ids("kclip-alice-a").expect("ids");
    assert_eq!(ids.len(), CLIP_NOTES_PER_CLIP);
    assert!(ids[0] > ids[ids.len() - 1], "newest-first");
    // Favorites: idempotent insert, count, cascade on delete.
    assert!(
        store
            .favorite_clip(k_clip_fav("bob", "kclip-alice-a"))
            .expect("fav")
    );
    assert!(
        !store
            .favorite_clip(k_clip_fav("bob", "kclip-alice-a"))
            .expect("dup")
    );
    assert!(
        store
            .is_clip_favorited("bob", "kclip-alice-a")
            .expect("check")
    );
    assert_eq!(
        store.clip_favorite_count("kclip-alice-a").expect("count"),
        1
    );
    assert_eq!(store.clip_favorites_by_user("bob").expect("favs").len(), 1);
    assert!(
        store
            .unfavorite_clip("bob", "kclip-alice-a")
            .expect("unfav")
    );
    assert!(
        !store
            .unfavorite_clip("bob", "kclip-alice-a")
            .expect("re-unfav")
    );
    store
        .favorite_clip(k_clip_fav("bob", "kclip-alice-a"))
        .expect("re-fav");
    assert!(
        store
            .delete_clip("alice", "kclip-alice-a", Vec::new())
            .expect("delete")
    );
    assert!(
        store
            .clip_note_ids("kclip-alice-a")
            .expect("ids")
            .is_empty(),
        "memberships cascaded"
    );
    assert_eq!(
        store.clip_favorite_count("kclip-alice-a").expect("count"),
        0,
        "favorites cascaded"
    );
    assert!(
        store
            .clip_favorites_by_user("bob")
            .expect("favs")
            .is_empty()
    );
}

// Group Z (Phase 2b/2a: clip follows + clip tombstones). Follows are
// cheap rows: duplicate converges, unfollow idempotent, delete cascades
// follows in the same commit; tombstones are tiny uri → deleted_at rows,
// kept forever in v1 (documented).

fn k_clip_follow(user: &str, clip_id: &str) -> ClipFollow {
    ClipFollow {
        id: format!("kcfol-{user}-{clip_id}"),
        user_id: user.to_string(),
        clip_id: clip_id.to_string(),
        created_at_ms: 1,
    }
}

/// Z1: follow converges (duplicate → false), unfollow idempotent, list
/// newest-first bounded, delete cascades follows.
#[test]
fn z1_clip_follow_converges_and_cascades() {
    let factory = RedbFactory::fresh("z1");
    let clock = test_clock();
    let store = factory.open(clock);
    store.create_clip(k_clip("alice", "a")).expect("create");
    assert!(
        store
            .follow_clip(k_clip_follow("bob", "kclip-alice-a"))
            .expect("follow")
    );
    // Duplicate converges (idempotent bool).
    assert!(
        !store
            .follow_clip(k_clip_follow("bob", "kclip-alice-a"))
            .expect("dup")
    );
    assert!(store.is_following_clip("bob", "kclip-alice-a").expect("is"));
    assert!(
        !store
            .is_following_clip("carol", "kclip-alice-a")
            .expect("isnt")
    );
    store
        .follow_clip(k_clip_follow("carol", "kclip-alice-a"))
        .expect("follow2");
    let followers = store.followers_of_clip("kclip-alice-a", 100).expect("list");
    assert_eq!(followers.len(), 2);
    let mine = store.followed_clips_of("bob").expect("mine");
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].clip_id, "kclip-alice-a");
    // Unfollow idempotent.
    assert!(
        store
            .unfollow_clip("bob", "kclip-alice-a")
            .expect("unfollow")
    );
    assert!(
        !store
            .unfollow_clip("bob", "kclip-alice-a")
            .expect("re-unfollow")
    );
    assert!(
        !store
            .is_following_clip("bob", "kclip-alice-a")
            .expect("gone")
    );
    // Delete cascades the remaining follow.
    assert!(
        store
            .delete_clip("alice", "kclip-alice-a", Vec::new())
            .expect("delete")
    );
    assert!(
        store
            .followers_of_clip("kclip-alice-a", 100)
            .expect("empty")
            .is_empty()
    );
    assert!(store.followed_clips_of("carol").expect("empty2").is_empty());
}

/// Z2: clip tombstones record + read (delete path writes one; serving
/// maps it to `Tombstone` instead of 404).
#[test]
fn z2_clip_tombstone_round_trip() {
    let factory = RedbFactory::fresh("z2");
    let clock = test_clock();
    let store = factory.open(clock);
    assert!(
        store
            .clip_tombstone("https://local.example/clips/x")
            .expect("miss")
            .is_none()
    );
    store
        .record_clip_tombstone("https://local.example/clips/x", 42)
        .expect("record");
    assert_eq!(
        store
            .clip_tombstone("https://local.example/clips/x")
            .expect("hit"),
        Some(42)
    );
    // Re-record overwrites (idempotent converge on replay).
    store
        .record_clip_tombstone("https://local.example/clips/x", 43)
        .expect("re-record");
    assert_eq!(
        store
            .clip_tombstone("https://local.example/clips/x")
            .expect("hit2"),
        Some(43)
    );
}
