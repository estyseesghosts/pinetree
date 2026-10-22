use super::common::*;

// ---------------------------------------------------------------------------
// Group D — activity dedup (generic over the store backend)
// ---------------------------------------------------------------------------

fn case_d1_first_seen<S: ConformanceStore>(store: &S) {
    assert_eq!(
        store.check_and_record("activity:d1:1").expect("first"),
        false,
        "new activity ID reports not-seen"
    );
    assert_eq!(
        store.check_and_record("activity:d1:1").expect("second"),
        true,
        "repeat reports already-seen"
    );
    assert_eq!(
        store.check_and_record("activity:d1:2").expect("other"),
        false,
        "distinct ID is new"
    );
}

#[test]
fn d1_first_seen() {
    let factory = RedbFactory::fresh("d1");
    let clock = test_clock();
    let store = factory.open(clock);
    case_d1_first_seen(&*store);
}

/// A forgotten activity ID is processed again (Misskey reuses one Follow id
/// per follower/followee pair across unfollow + re-follow).
#[test]
fn d1_forget_activity_allows_reprocessing() {
    let factory = RedbFactory::fresh("d1-forget");
    let store = factory.open(test_clock());
    assert!(!store.check_and_record("activity:d1f:1").expect("first"));
    assert!(store.check_and_record("activity:d1f:1").expect("dup"));
    store.forget_activity("activity:d1f:1").expect("forget");
    assert!(
        !store.check_and_record("activity:d1f:1").expect("again"),
        "forgotten id is new again"
    );
    store
        .forget_activity("activity:d1f:never")
        .expect("forget unknown is a no-op");
}

/// D2: dedup + state atomicity — a pre-commit crash leaves no mark, note, or
/// job; once committed, a retry is a no-op returning `Ok(None)`.
#[test]
fn d2_dedup_and_state_atomicity() {
    let factory = RedbFactory::fresh("d2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let alice = create_user(&*store, "alice");

    // Crashed-before-commit attempt persists nothing.
    store.set_abort_before_commit(true);
    let err = store
        .record_note_activity(
            "activity:d2:1",
            CreateNoteInput::simple_public(&alice.id, "crashed inbox note"),
            vec![streaming_job(&clock, "d2:job")],
        )
        .expect_err("injected abort must fail");
    assert!(
        matches!(err, PortError::StorageUnavailable(_)),
        "got {err:?}"
    );
    assert_eq!(list_all(&*store).len(), 0);
    assert_eq!(store.pending_count().expect("count"), 0);

    // Committed attempt wins; retry with the same ID is a silent no-op.
    let won = store
        .record_note_activity(
            "activity:d2:1",
            CreateNoteInput::simple_public(&alice.id, "committed inbox note"),
            vec![streaming_job(&clock, "d2:job")],
        )
        .expect("committed record")
        .expect("first commit wins");
    assert_eq!(
        store.check_and_record("activity:d2:1").expect("recheck"),
        true,
        "committed attempt recorded dedup mark"
    );
    let retry = store
        .record_note_activity(
            "activity:d2:1",
            CreateNoteInput::simple_public(&alice.id, "duplicate retry"),
            vec![streaming_job(&clock, "d2:retry-job")],
        )
        .expect("retry");
    assert_eq!(
        retry, None,
        "retry observes already-seen, no duplicate note"
    );
    assert_eq!(list_all(&*store).len(), 1);
    assert_eq!(store.pending_count().expect("count"), 1);
    let _ = won;
}

/// D3: 8 parallel threads, same activity ID → exactly 1 winner.
#[test]
fn d3_concurrent_duplicates() {
    let factory = RedbFactory::fresh("d3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let store: Arc<RedbStore> = store;
    let alice = create_user(&*store, "alice");
    let alice_id = alice.id.clone();
    let results: Vec<_> = std::thread::scope(|s| {
        (0..8)
            .map(|i| {
                let store = store.clone();
                let clock = clock.clone();
                let author = alice_id.clone();
                s.spawn(move || {
                    store.record_note_activity(
                        "activity:d3:shared",
                        CreateNoteInput::simple_public(&author, format!("racer {i}")),
                        vec![streaming_job(&clock, &format!("d3:job-{i}"))],
                    )
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().expect("thread join"))
            .collect()
    });
    for r in &results {
        assert!(
            r.is_ok(),
            "no racer errors, only winner/loser outcomes: {r:?}"
        );
    }
    let winners = results.iter().filter(|r| matches!(r, Ok(Some(_)))).count();
    let losers = results.iter().filter(|r| matches!(r, Ok(None))).count();
    assert_eq!(winners, 1, "exactly one winner: {results:?}");
    assert_eq!(losers, 7, "seven losers return Ok(None)");
    assert_eq!(list_all(&*store).len(), 1, "only one note persisted");
    assert_eq!(
        store.pending_count().expect("count"),
        1,
        "only the winner's job persisted"
    );
}

/// D4 normal path (generic over the store backend so FDB inherits it):
/// first Reject removes the edge (`Ok(true)`), redelivery is a no-op
/// (`Ok(false)`), and rejecting a missing edge still counts as seen
/// (`Ok(true)`).
fn case_d4_reject_follow<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    store
        .follow(&alice.id, &bob.id, false, None, vec![])
        .expect("follow");
    assert!(
        store
            .is_following(&alice.id, &bob.id)
            .expect("is_following"),
        "edge exists before the Reject"
    );

    // First delivery removes the edge.
    assert_eq!(
        store
            .record_reject_follow_activity("activity:d4:1", &alice.id, &bob.id, vec![])
            .expect("first reject"),
        true,
        "first Reject removes the edge"
    );
    assert!(
        !store
            .is_following(&alice.id, &bob.id)
            .expect("is_following"),
        "edge is gone after the Reject"
    );

    // Redelivery of the same Reject id is a silent no-op.
    assert_eq!(
        store
            .record_reject_follow_activity("activity:d4:1", &alice.id, &bob.id, vec![])
            .expect("redelivery"),
        false,
        "redelivery observes already-seen"
    );
    assert!(
        !store
            .is_following(&alice.id, &bob.id)
            .expect("is_following"),
        "edge stays gone after redelivery"
    );

    // Reject of a missing edge still counts the Reject as seen.
    assert_eq!(
        store
            .record_reject_follow_activity("activity:d4:2", &alice.id, &bob.id, vec![])
            .expect("reject of non-edge"),
        true,
        "Reject of a non-edge still returns Ok(true)"
    );
}

#[test]
fn d4_reject_follow() {
    let factory = RedbFactory::fresh("d4");
    let clock = test_clock();
    let store = factory.open(clock);
    case_d4_reject_follow(&*store);
}

/// D4 atomicity under injected crash (redb-specific, d2 style): abort before
/// commit persists no dedup mark, no unfollow, and no job; the retry then
/// removes the edge and redelivery is a no-op.
#[test]
fn d4_reject_follow_atomicity() {
    let factory = RedbFactory::fresh("d4-atomic");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let alice = create_user(&*store, "alice");
    let bob = create_user(&*store, "bob");
    store
        .follow(&alice.id, &bob.id, false, None, vec![])
        .expect("follow");

    // Crashed-before-commit attempt persists nothing.
    store.set_abort_before_commit(true);
    let err = store
        .record_reject_follow_activity(
            "activity:d4a:1",
            &alice.id,
            &bob.id,
            vec![streaming_job(&clock, "d4a:job")],
        )
        .expect_err("injected abort must fail");
    assert!(
        matches!(err, PortError::StorageUnavailable(_)),
        "got {err:?}"
    );
    assert!(
        store
            .is_following(&alice.id, &bob.id)
            .expect("is_following"),
        "crashed attempt removed no edge"
    );
    assert_eq!(store.pending_count().expect("count"), 0);

    // The dedup mark rolled back too: the retry is treated as new and wins.
    store.set_abort_before_commit(false);
    assert_eq!(
        store
            .record_reject_follow_activity("activity:d4a:1", &alice.id, &bob.id, vec![])
            .expect("retry"),
        true,
        "retry after the crash removes the edge"
    );
    assert!(
        !store
            .is_following(&alice.id, &bob.id)
            .expect("is_following"),
        "edge is gone after the retry"
    );
    assert_eq!(
        store
            .record_reject_follow_activity("activity:d4a:1", &alice.id, &bob.id, vec![])
            .expect("redelivery"),
        false,
        "redelivery observes already-seen"
    );
}
