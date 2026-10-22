use super::common::*;

// ---------------------------------------------------------------------------
// Group K — user lists (Batch O WP16)
// ---------------------------------------------------------------------------

fn k_list(owner: &str, name: &str) -> UserList {
    UserList {
        id: format!("klist-{owner}-{name}"),
        owner_id: owner.to_string(),
        name: name.to_string(),
        is_public: false,
        created_at_ms: 1,
    }
}

fn k_member(list_id: &str, member_id: &str) -> UserListMembership {
    UserListMembership {
        id: format!("km-{list_id}-{member_id}"),
        list_id: list_id.to_string(),
        member_id: member_id.to_string(),
        with_replies: false,
        created_at_ms: 1,
    }
}

/// V2: concurrent duplicate pushes converge to exactly one membership
/// (the rest `ALREADY_ADDED`).
#[test]
fn v2_concurrent_duplicate_push_converges() {
    let factory = RedbFactory::fresh("v2");
    let clock = test_clock();
    let factory = RedbFactory::fresh("v2");
    let clock = test_clock();
    let store = factory.open(clock);
    store
        .create_user_list(k_list("alice", "l"))
        .expect("create");
    let results: Vec<_> = std::thread::scope(|scope| {
        (0..8)
            .map(|_| {
                let store = store.clone();
                scope.spawn(move || {
                    store
                        .add_list_member(k_member("klist-alice-l", "bob"), Vec::new())
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
        1,
        "exactly one push wins"
    );
    assert!(
        results.iter().all(|r| r.as_ref().is_ok_and(|_| true)
            || matches!(r, Err(PortError::Conflict(msg)) if msg == "ALREADY_ADDED")),
        "losers report ALREADY_ADDED: {results:?}"
    );
}

/// V3: cap races — exactly `USER_LIST_LIMIT` lists per owner and exactly
/// `USER_LIST_MEMBER_LIMIT` members per list; the rest conflict.
#[test]
fn v3_cap_races_hold() {
    let factory = RedbFactory::fresh("v3");
    let clock = test_clock();
    let store = factory.open(clock);
    let list_results: Vec<_> = std::thread::scope(|scope| {
        (0..(USER_LIST_LIMIT + 4))
            .map(|i| {
                let store = store.clone();
                scope.spawn(move || {
                    store
                        .create_user_list(k_list("alice", &format!("l{i}")))
                        .map(|_| ())
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    assert_eq!(
        list_results.iter().filter(|r| r.is_ok()).count(),
        USER_LIST_LIMIT,
        "exactly the cap wins"
    );
    assert!(
        list_results.iter().all(|r| r.is_ok()
            || matches!(r, Err(PortError::Conflict(msg)) if msg == "TOO_MANY_USERLISTS")),
        "losers report TOO_MANY_USERLISTS"
    );
    store.create_user_list(k_list("bob", "m")).expect("create");
    let member_results: Vec<_> = std::thread::scope(|scope| {
        (0..(USER_LIST_MEMBER_LIMIT + 4))
            .map(|i| {
                let store = store.clone();
                scope.spawn(move || {
                    store
                        .add_list_member(k_member("klist-bob-m", &format!("u{i:03}")), Vec::new())
                        .map(|_| ())
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    assert_eq!(
        member_results.iter().filter(|r| r.is_ok()).count(),
        USER_LIST_MEMBER_LIMIT,
        "exactly the member cap wins"
    );
    assert!(
        member_results
            .iter()
            .all(|r| r.is_ok()
                || matches!(r, Err(PortError::Conflict(msg)) if msg == "TOO_MANY_USERS")),
        "losers report TOO_MANY_USERS"
    );
}

/// V4: delete cascades memberships + favorites in the same commit.
#[test]
fn v4_delete_cascades() {
    let factory = RedbFactory::fresh("v4");
    let clock = test_clock();
    let store = factory.open(clock);
    store
        .create_user_list(k_list("alice", "l"))
        .expect("create");
    store
        .add_list_member(k_member("klist-alice-l", "bob"), Vec::new())
        .expect("push");
    assert!(store.favorite_list("alice", "klist-alice-l").expect("fav"));
    assert!(
        store
            .delete_user_list("alice", "klist-alice-l")
            .expect("delete")
    );
    assert!(
        store
            .members_of_list("klist-alice-l")
            .expect("members")
            .is_empty(),
        "memberships cascaded"
    );
    assert!(
        !store
            .is_list_favorited("alice", "klist-alice-l")
            .expect("fav check"),
        "favorites cascaded"
    );
    assert_eq!(
        store.list_favorite_count("klist-alice-l").expect("count"),
        0
    );
    // Foreign-owner delete is a miss, not a delete.
    store
        .create_user_list(k_list("alice", "l2"))
        .expect("create");
    assert!(
        !store
            .delete_user_list("mallory", "klist-alice-l2")
            .expect("foreign")
    );
    assert!(
        store
            .get_user_list("klist-alice-l2")
            .expect("get")
            .is_some()
    );
}
