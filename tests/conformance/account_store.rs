use super::common::*;

// ---------------------------------------------------------------------------
// Group C — accounts and handles (generic over the store backend)
// ---------------------------------------------------------------------------

/// C1: 8 parallel threads, same handle in mixed case → exactly 1 wins.
#[test]
fn c1_uniqueness_race() {
    let factory = RedbFactory::fresh("c1");
    let clock = test_clock();
    let store = factory.open(clock);
    let store: Arc<RedbStore> = store;
    let handles = [
        "Alice", "alice", "ALICE", "aLiCe", "ALice", "alICE", "aLICE", "alicE",
    ];
    let results: Vec<_> = std::thread::scope(|s| {
        handles
            .iter()
            .map(|h| {
                let store = store.clone();
                let h = h.to_string();
                s.spawn(move || store.create_account(CreateAccountInput::local(&h), None))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().expect("thread join"))
            .collect()
    });
    let ok = results.iter().filter(|r| r.is_ok()).count();
    let conflicts = results
        .iter()
        .filter(|r| matches!(r, Err(PortError::Conflict(_))))
        .count();
    assert_eq!(ok, 1, "exactly one racer wins: {results:?}");
    assert_eq!(conflicts, 7, "all losers see Conflict: {results:?}");
}

fn case_c2_case_insensitivity<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "Alice_C2");
    for probe in ["Alice_C2", "alice_c2", "ALICE_C2", "aLiCe_C2"] {
        let found = store
            .find_by_handle(probe)
            .unwrap_or_else(|e| panic!("find_by_handle({probe}): {e:?}"))
            .unwrap_or_else(|| panic!("find_by_handle({probe}) must resolve"));
        assert_eq!(found.id, alice.id, "case-insensitive resolve of {probe}");
    }
    let err = store
        .create_account(CreateAccountInput::local("ALICE_c2"), None)
        .expect_err("differing-case create must collide");
    assert!(is_conflict(&err), "got {err:?}");
}

#[test]
fn c2_case_insensitivity() {
    let factory = RedbFactory::fresh("c2");
    let clock = test_clock();
    let store = factory.open(clock);
    case_c2_case_insensitivity(&*store);
}

/// C3: rename tombstone — old handle never reusable, ID + actor URL stable.
#[test]
fn c3_rename_tombstone() {
    let factory = RedbFactory::fresh("c3");
    let clock = test_clock();
    let store = factory.open(clock);
    let alice = create_user(&*store, "alice");
    let before_url = alice.actor_url("https://example.local");
    assert_eq!(
        before_url,
        format!("https://example.local/users/{}", alice.id)
    );
    let renamed = store.rename(&alice.id, "alicia").expect("rename");
    assert_eq!(renamed.id, alice.id, "rename preserves the account ID");
    assert_eq!(
        renamed.actor_url("https://example.local"),
        before_url,
        "actor URL unchanged across rename"
    );
    let err = store
        .create_account(CreateAccountInput::local("alice"), None)
        .expect_err("old handle must stay tombstoned");
    assert!(is_conflict(&err), "got {err:?}");
    assert!(
        store.find_by_handle("alice").expect("find old").is_none(),
        "old handle unresolvable"
    );
    let found = store
        .find_by_handle("ALICIA")
        .expect("find new")
        .expect("new handle resolves (any case)");
    assert_eq!(found.id, alice.id);
}

#[test]
fn c4_unknown_handle() {
    let factory = RedbFactory::fresh("c4");
    let clock = test_clock();
    let store = factory.open(clock);
    assert!(
        store
            .find_by_handle("nobody-here")
            .expect("find free name")
            .is_none(),
        "free handle returns Ok(None), not an error"
    );
}
