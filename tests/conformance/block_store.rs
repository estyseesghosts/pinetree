use super::common::*;

// ---------------------------------------------------------------------------
// Group G — §0A blocking / muting persistence contract (backend-generic;
// Phase 4 runs these against FoundationDB unchanged)
// ---------------------------------------------------------------------------

fn full_cursor(limit: usize) -> TimelineCursor {
    TimelineCursor {
        since_id: None,
        until_id: None,
        since_date: None,
        until_date: None,
        limit,
    }
}

fn blocking_row(seq: u64, blocker: &str, blockee: &str) -> pinetree::domain::blocking::Blocking {
    pinetree::domain::blocking::Blocking {
        // Zero-padded sequence: lexicographic order == creation order, so
        // newest-first paging assertions hold without an id generator.
        id: format!("gblock{seq:08}"),
        blocker_id: blocker.to_string(),
        blockee_id: blockee.to_string(),
    }
}

/// G1: block CRUD, dup/missing codes, directional id lists, cursor paging.
fn case_g1_block_crud<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "g1_alice");
    let bob = create_user(store, "g1_bob");
    let carol = create_user(store, "g1_carol");
    assert!(!store.is_blocking(&alice.id, &bob.id).unwrap());
    let row = store
        .block(blocking_row(2, &alice.id, &bob.id), vec![])
        .expect("block");
    assert_eq!(
        (row.blocker_id.as_str(), row.blockee_id.as_str()),
        (alice.id.as_str(), bob.id.as_str())
    );
    assert!(store.is_blocking(&alice.id, &bob.id).unwrap());
    assert!(!store.is_blocking(&bob.id, &alice.id).unwrap());
    assert!(matches!(
        store.block(blocking_row(3, &alice.id, &bob.id), vec![]),
        Err(PortError::Conflict(msg)) if msg == "ALREADY_BLOCKING"
    ));
    assert!(matches!(
        store.block(blocking_row(4, &alice.id, &alice.id), vec![]),
        Err(PortError::Validation(msg)) if msg == "BLOCKEE_IS_YOURSELF"
    ));
    assert!(matches!(
        store.block(blocking_row(5, &alice.id, "missing"), vec![]),
        Err(PortError::NotFound(msg)) if msg == "NO_SUCH_USER"
    ));
    assert_eq!(
        store.blocking_ids_for(&alice.id).unwrap(),
        vec![bob.id.clone()]
    );
    assert_eq!(
        store.blocked_by_ids_for(&bob.id).unwrap(),
        vec![alice.id.clone()]
    );
    assert!(store.blocked_by_ids_for(&alice.id).unwrap().is_empty());
    store
        .block(blocking_row(6, &alice.id, &carol.id), vec![])
        .expect("second block");
    let page = store.list_blockings(&alice.id, &full_cursor(1)).unwrap();
    assert_eq!(page.len(), 1, "limit applies");
    let all = store.list_blockings(&alice.id, &full_cursor(100)).unwrap();
    assert_eq!(all.len(), 2);
    assert!(all[0].id > all[1].id, "newest-first");
    // untilId paging walks without gaps.
    let second = store
        .list_blockings(
            &alice.id,
            &TimelineCursor {
                until_id: Some(all[0].id.clone()),
                ..full_cursor(100)
            },
        )
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, all[1].id);
    store.unblock(&alice.id, &bob.id, vec![]).expect("unblock");
    assert!(!store.is_blocking(&alice.id, &bob.id).unwrap());
    assert!(matches!(
        store.unblock(&alice.id, &bob.id, vec![]),
        Err(PortError::NotFound(msg)) if msg == "NOT_BLOCKING"
    ));
}

#[test]
fn g1_block_crud() {
    let factory = RedbFactory::fresh("g1");
    let store = factory.open(test_clock());
    case_g1_block_crud(&*store);
}

/// G2: one `block` commits the row, both follow edges, both pending
/// requests, and the jobs atomically.
fn case_g2_block_atomicity<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "g2_alice");
    let bob = create_user(store, "g2_bob");
    store
        .follow(&alice.id, &bob.id, false, None, vec![])
        .unwrap();
    store
        .follow(&bob.id, &alice.id, false, None, vec![])
        .unwrap();
    let jobs_before = store.pending_count().unwrap();
    store
        .block(
            blocking_row(1, &alice.id, &bob.id),
            vec![streaming_job(clock, "g2-block-job")],
        )
        .expect("block");
    assert!(!store.is_following(&alice.id, &bob.id).unwrap());
    assert!(!store.is_following(&bob.id, &alice.id).unwrap());
    let accounts = |id: &str| store.find_by_id(id).unwrap().unwrap();
    assert_eq!(accounts(&alice.id).following_count, 0);
    assert_eq!(accounts(&bob.id).followers_count, 0);
    assert_eq!(store.pending_count().unwrap(), jobs_before + 1);
}

/// G2b: `block` also cancels pending follow requests both ways.
fn case_g2b_block_cancels_requests<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "g2b_alice");
    let bob = create_user(store, "g2b_bob");
    store
        .request_follow(&alice.id, &bob.id, false, None, vec![])
        .unwrap();
    store
        .request_follow(&bob.id, &alice.id, false, None, vec![])
        .unwrap();
    store
        .block(blocking_row(1, &alice.id, &bob.id), vec![])
        .expect("block");
    assert!(
        store
            .get_follow_request(&alice.id, &bob.id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_follow_request(&bob.id, &alice.id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn g2_block_atomicity() {
    let factory = RedbFactory::fresh("g2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_g2_block_atomicity(&*store, &clock);
    let factory = RedbFactory::fresh("g2b");
    let store = factory.open(test_clock());
    case_g2b_block_cancels_requests(&*store);
}

/// G3: storage-level enforcement — follow both directions, react, and pure
/// renotes are rejected; quotes pass.
fn case_g3_enforcement<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "g3_alice");
    let bob = create_user(store, "g3_bob");
    let target = note_with_job(
        store,
        &ManualClock::new(1_760_400_000_000),
        &bob.id,
        "t",
        "g3",
    );
    store
        .block(blocking_row(1, &bob.id, &alice.id), vec![])
        .expect("block");
    assert!(matches!(
        store.follow(&alice.id, &bob.id, false, None, vec![]),
        Err(PortError::Validation(msg)) if msg == "BLOCKED"
    ));
    assert!(matches!(
        store.follow(&bob.id, &alice.id, false, None, vec![]),
        Err(PortError::Validation(msg)) if msg == "BLOCKING"
    ));
    assert!(matches!(
        store.request_follow(&alice.id, &bob.id, false, None, vec![]),
        Err(PortError::Validation(msg)) if msg == "BLOCKED"
    ));
    assert!(matches!(
        store.react(&alice.id, &target.id, "❤", "blocked-reaction-id", false, vec![]),
        Err(PortError::Validation(msg)) if msg == "YOU_HAVE_BEEN_BLOCKED"
    ));
    let mut pure = CreateNoteInput::simple_public(&alice.id, "x");
    pure.text = None;
    pure.renote_of = Some(target.id.clone());
    assert!(matches!(
        store.create_note(pure, vec![]),
        Err(PortError::Validation(msg)) if msg == "YOU_HAVE_BEEN_BLOCKED"
    ));
    let mut quote = CreateNoteInput::simple_public(&alice.id, "quoting");
    quote.renote_of = Some(target.id.clone());
    store
        .create_note(quote, vec![])
        .expect("quote passes the block gate");
}

#[test]
fn g3_enforcement() {
    let factory = RedbFactory::fresh("g3");
    let store = factory.open(test_clock());
    case_g3_enforcement(&*store);
}

/// G4: mute + renote-mute lifecycle, dup/missing codes, expiry filtering.
fn case_g4_mutes<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "g4_alice");
    let bob = create_user(store, "g4_bob");
    assert!(matches!(
        store.mute(&alice.id, &alice.id, None, vec![]),
        Err(PortError::Validation(msg)) if msg == "MUTEE_IS_YOURSELF"
    ));
    assert!(matches!(
        store.mute(&alice.id, "missing", None, vec![]),
        Err(PortError::NotFound(msg)) if msg == "NO_SUCH_USER"
    ));
    // Past expiry: no row, no error.
    assert_eq!(
        store
            .mute(&alice.id, &bob.id, Some(clock.now_ms() - 1), vec![])
            .unwrap(),
        None
    );
    assert!(!store.is_muting(&alice.id, &bob.id).unwrap());
    assert!(store.muted_ids_for(&alice.id).unwrap().is_empty());
    let future = clock.now_ms() + 60_000;
    assert!(
        store
            .mute(&alice.id, &bob.id, Some(future), vec![])
            .unwrap()
            .is_some()
    );
    assert!(store.is_muting(&alice.id, &bob.id).unwrap());
    assert_eq!(
        store.muted_ids_for(&alice.id).unwrap(),
        vec![bob.id.clone()]
    );
    assert!(matches!(
        store.mute(&alice.id, &bob.id, None, vec![]),
        Err(PortError::Conflict(msg)) if msg == "ALREADY_MUTING"
    ));
    clock.set_ms(future + 1);
    assert!(!store.is_muting(&alice.id, &bob.id).unwrap());
    assert!(store.muted_ids_for(&alice.id).unwrap().is_empty());
    // The expired row still unmutes cleanly (existence, not activity).
    store
        .unmute(&alice.id, &bob.id, vec![])
        .expect("unmute expired row");
    assert!(matches!(
        store.unmute(&alice.id, &bob.id, vec![]),
        Err(PortError::NotFound(msg)) if msg == "NOT_MUTING"
    ));
    // Renote-mutes: no expiry, same codes.
    store
        .renote_mute(&alice.id, &bob.id, vec![])
        .expect("renote mute");
    assert!(store.is_renote_muting(&alice.id, &bob.id).unwrap());
    assert_eq!(
        store.renote_muted_ids_for(&alice.id).unwrap(),
        vec![bob.id.clone()]
    );
    assert!(matches!(
        store.renote_mute(&alice.id, &bob.id, vec![]),
        Err(PortError::Conflict(msg)) if msg == "ALREADY_MUTING"
    ));
    assert_eq!(
        store
            .list_renote_mutings(&alice.id, &full_cursor(100))
            .unwrap()
            .len(),
        1
    );
    store
        .renote_unmute(&alice.id, &bob.id, vec![])
        .expect("renote unmute");
    assert!(matches!(
        store.renote_unmute(&alice.id, &bob.id, vec![]),
        Err(PortError::NotFound(msg)) if msg == "NOT_MUTING"
    ));
}

#[test]
fn g4_mutes() {
    let factory = RedbFactory::fresh("g4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_g4_mutes(&*store, &clock);
}
