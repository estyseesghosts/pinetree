use super::common::*;

// ---------------------------------------------------------------------------
// Group H — moderation (Batch M §3.4; `docs/implementer-guide-batch-m.md`)
// ---------------------------------------------------------------------------

/// H1: suspend commits the flag, the follow-request teardown (upstream
/// `postSuspend`), the log row, and the outbox jobs together — and a failed
/// log append aborts the whole action (state unchanged, no job).
fn case_h1_suspend_atomicity<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "h1_alice");
    let bob = create_user(store, "h1_bob");
    store
        .request_follow(&bob.id, &alice.id, false, None, Vec::new())
        .expect("request bob->alice");
    store
        .request_follow(&alice.id, &bob.id, false, None, Vec::new())
        .expect("request alice->bob");

    // Fail-closed first: an invalid log row must abort the action.
    let err = store
        .set_suspended(
            &alice.id,
            true,
            invalid_log_entry("h1logx", ModerationAction::Suspend),
            vec![streaming_job(clock, "h1:nope")],
        )
        .expect_err("invalid log refuses the suspend");
    assert!(matches!(err, PortError::Validation(_)), "got {err:?}");
    assert!(
        !store.find_by_id(&alice.id).unwrap().unwrap().is_suspended,
        "no state change without the log row"
    );
    assert!(log_rows(store).is_empty(), "no orphan log row");
    assert!(
        store.claim_next(500).expect("claim").is_none(),
        "no orphan job"
    );

    // Success: state + follow-request teardown + log + jobs together.
    let updated = store
        .set_suspended(
            &alice.id,
            true,
            log_entry(
                "h1log1",
                Some(&bob.id),
                ModerationAction::Suspend,
                Some(&alice.id),
                serde_json::json!({"userId": alice.id, "userUsername": alice.username, "userHost": null}),
            ),
            vec![federate_job(clock, "h1:delete")],
        )
        .expect("suspend");
    assert!(updated.is_suspended);
    assert!(store.find_by_id(&alice.id).unwrap().unwrap().is_suspended);
    assert!(
        store
            .get_follow_request(&bob.id, &alice.id)
            .expect("fwd lookup")
            .is_none(),
        "requests to the account are deleted"
    );
    assert!(
        store
            .get_follow_request(&alice.id, &bob.id)
            .expect("rev lookup")
            .is_none(),
        "requests from the account are deleted"
    );
    let rows = log_rows(store);
    assert_eq!(rows.len(), 1, "exactly one row: {rows:?}");
    assert_eq!(rows[0].action, ModerationAction::Suspend);
    assert_eq!(rows[0].target.as_deref(), Some(alice.id.as_str()));
    let job = store
        .claim_next(500)
        .expect("claim")
        .expect("job committed with the action");
    assert_eq!(job.idempotency_key, "h1:delete");
}

/// H2: unsuspend flips the flag back and commits its log row and outbound
/// (Undo-Delete) job with it; the removed follow requests stay removed.
fn case_h2_unsuspend_atomicity<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "h2_alice");
    let bob = create_user(store, "h2_bob");
    store
        .request_follow(&bob.id, &alice.id, false, None, Vec::new())
        .expect("request");
    store
        .set_suspended(
            &alice.id,
            true,
            log_entry(
                "h2log1",
                Some(&bob.id),
                ModerationAction::Suspend,
                Some(&alice.id),
                serde_json::json!({"userId": alice.id}),
            ),
            Vec::new(),
        )
        .expect("suspend");

    let err = store
        .set_suspended(
            &alice.id,
            false,
            invalid_log_entry("h2logx", ModerationAction::Unsuspend),
            vec![streaming_job(clock, "h2:nope")],
        )
        .expect_err("invalid log refuses the unsuspend");
    assert!(matches!(err, PortError::Validation(_)), "got {err:?}");
    assert!(
        store.find_by_id(&alice.id).unwrap().unwrap().is_suspended,
        "still suspended after the aborted action"
    );

    let updated = store
        .set_suspended(
            &alice.id,
            false,
            log_entry(
                "h2log2",
                Some(&bob.id),
                ModerationAction::Unsuspend,
                Some(&alice.id),
                serde_json::json!({"userId": alice.id}),
            ),
            vec![federate_job(clock, "h2:undo-delete")],
        )
        .expect("unsuspend");
    assert!(!updated.is_suspended);
    assert!(
        store
            .get_follow_request(&bob.id, &alice.id)
            .expect("fwd lookup")
            .is_none(),
        "the removed request stays removed"
    );
    let rows = log_rows(store);
    assert_eq!(rows.len(), 2, "one row per action: {rows:?}");
    assert_eq!(rows[0].action, ModerationAction::Unsuspend, "newest first");
    let job = store
        .claim_next(500)
        .expect("claim")
        .expect("undo job committed with the action");
    assert_eq!(job.idempotency_key, "h2:undo-delete");
}

/// H3: concurrent suspend and unsuspend — every call lands (no lost update),
/// and no log row is lost or duplicated.
fn case_h3_suspend_unsuspend_race<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "h3_alice");
    const THREADS: usize = 8;
    const PER_THREAD: usize = 20;
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let alice_id = alice.id.clone();
            scope.spawn(move || {
                for i in 0..PER_THREAD {
                    let suspended = (t + i) % 2 == 0;
                    let action = if suspended {
                        ModerationAction::Suspend
                    } else {
                        ModerationAction::Unsuspend
                    };
                    let log = log_entry(
                        &format!("h3log{t:02}{i:02}"),
                        None,
                        action,
                        Some(&alice_id),
                        serde_json::json!({"userId": alice_id}),
                    );
                    store
                        .set_suspended(&alice_id, suspended, log, Vec::new())
                        .expect("race call lands");
                }
            });
        }
    });
    let rows = log_rows(store);
    assert_eq!(
        rows.len(),
        THREADS * PER_THREAD,
        "one row per action, none lost or duplicated"
    );
    let unique: HashSet<&str> = rows.iter().map(|row| row.id.as_str()).collect();
    assert_eq!(unique.len(), rows.len(), "no duplicate rows");
    let account = store.find_by_id(&alice.id).unwrap().unwrap();
    assert!(account.validate().is_ok(), "account row survives the race");
}

/// H4: a log row is never lost against the action it records — the patch,
/// the row, and the jobs commit together, and a failing row aborts all.
fn case_h4_log_never_lost<S: ConformanceStore>(store: &S, clock: &ManualClock) -> String {
    let alice = create_user(store, "h4_alice");
    let patch = UpdateAccountPatch {
        reject_quotes: Some(true),
        ..Default::default()
    };
    let err = store
        .update_account_logged(
            &alice.id,
            patch.clone(),
            invalid_log_entry("h4logx", ModerationAction::RejectQuotesUser),
            vec![streaming_job(clock, "h4:nope")],
        )
        .expect_err("invalid log refuses the action");
    assert!(matches!(err, PortError::Validation(_)), "got {err:?}");
    let untouched = store.find_by_id(&alice.id).unwrap().unwrap();
    assert!(!untouched.reject_quotes, "patch must not apply alone");
    assert!(log_rows(store).is_empty(), "no orphan log row");
    assert!(
        store.claim_next(500).expect("claim").is_none(),
        "no orphan job"
    );

    store
        .update_account_logged(
            &alice.id,
            patch,
            log_entry(
                "h4log1",
                Some(&alice.id),
                ModerationAction::RejectQuotesUser,
                Some(&alice.id),
                serde_json::json!({"userId": alice.id}),
            ),
            vec![streaming_job(clock, "h4:updated")],
        )
        .expect("logged update");
    assert!(
        store.find_by_id(&alice.id).unwrap().unwrap().reject_quotes,
        "patch applied with its row"
    );
    assert_eq!(log_rows(store).len(), 1);
    let job = store
        .claim_next(500)
        .expect("claim")
        .expect("job committed with the action");
    assert_eq!(job.idempotency_key, "h4:updated");
    alice.id
}

#[test]
fn h1_suspend_atomicity() {
    let factory = RedbFactory::fresh("h1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_h1_suspend_atomicity(&*store, &clock);
}

#[test]
fn h2_unsuspend_atomicity() {
    let factory = RedbFactory::fresh("h2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_h2_unsuspend_atomicity(&*store, &clock);
}

#[test]
fn h3_suspend_unsuspend_race() {
    let factory = RedbFactory::fresh("h3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_h3_suspend_unsuspend_race(&*store);
}

#[test]
fn h4_log_never_lost() {
    let factory = RedbFactory::fresh("h4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let alice_id = case_h4_log_never_lost(&*store, &clock);
    // Durable across reopen: the row survived with its action.
    drop(store);
    let store = factory.reopen(clock);
    assert_eq!(log_rows(&*store).len(), 1, "log row survived the reopen");
    assert!(
        store.find_by_id(&alice_id).unwrap().unwrap().reject_quotes,
        "the state change survived with the row"
    );
}

/// H5 (Batch M WP3): the knowable-shared-inbox enumeration behind the
/// `Delete(Person)` sweep (`AccountStore::remote_shared_inboxes`) walks the
/// accounts table exactly once — bounded pages, exclusive explicit cursor,
/// NO silent truncation (a short scan is never mistaken for the end) — and
/// yields only non-empty remote shared inboxes. Duplicates across rows are
/// the caller's dedup (the sweep dedupes with a set).
fn case_h5_shared_inbox_enumeration<S: ConformanceStore>(store: &S) {
    let _local = create_user(store, "h5_local");
    let remote = |handle: &str, shared: Option<&str>, inbox: &str| {
        store
            .create_account(
                CreateAccountInput {
                    handle: handle.to_string(),
                    host: Some("remote.example".to_string()),
                    display_name: None,
                    description: None,
                    location: None,
                    birthday: None,
                    uri: Some(format!("https://remote.example/users/{handle}")),
                    inbox: Some(inbox.to_string()),
                    shared_inbox: shared.map(str::to_string),
                    is_bot: false,
                    is_cat: false,
                    is_locked: false,
                    preserved_usernames: Vec::new(),
                },
                None,
            )
            .unwrap_or_else(|e| panic!("remote account {handle}: {e:?}"))
    };
    remote(
        "h5_r1",
        Some("https://a.example/inbox"),
        "https://a.example/personal",
    );
    remote(
        "h5_r2",
        Some("https://a.example/inbox"),
        "https://a.example/personal2",
    );
    remote(
        "h5_r3",
        Some("https://b.example/inbox"),
        "https://b.example/personal",
    );
    // No shared inbox: upstream delivers only to shared inboxes (its
    // followings rows snapshot `sharedInbox` only) — skipped here too.
    remote("h5_r4", None, "https://c.example/personal");

    // Walk with one row per page: every row is visited exactly once (the
    // cursor advances monotonically) and the walk ends with a `None` cursor.
    let mut seen: Vec<String> = Vec::new();
    let mut pages = 0usize;
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) = store
            .remote_shared_inboxes(cursor.as_deref(), 1)
            .expect("shared-inbox page");
        pages += 1;
        assert!(pages <= 10, "walk must terminate");
        for inbox in page {
            if !seen.contains(&inbox) {
                seen.push(inbox);
            }
        }
        match next {
            None => break,
            Some(after) => {
                assert_ne!(
                    Some(after.as_str()),
                    cursor.as_deref(),
                    "cursor must advance"
                );
                cursor = Some(after);
            }
        }
    }
    // 5 account rows: one page per row plus the terminal empty page.
    assert_eq!(pages, 6, "one page per row + terminal: {pages}");
    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec![
            "https://a.example/inbox".to_string(),
            "https://b.example/inbox".to_string(),
        ],
        "remote shared inboxes only, dupes left to the caller: {seen:?}"
    );

    // A large page returns the same set in one walk.
    let (page, next) = store
        .remote_shared_inboxes(None, 1_000)
        .expect("one-shot page");
    assert!(next.is_none(), "the full scan reports table end");
    let mut sorted = page.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted,
        vec![
            "https://a.example/inbox".to_string(),
            "https://b.example/inbox".to_string(),
        ]
    );
}

#[test]
fn h5_shared_inbox_enumeration() {
    let factory = RedbFactory::fresh("h5");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_h5_shared_inbox_enumeration(&*store);
}
