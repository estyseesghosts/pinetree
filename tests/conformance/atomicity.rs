use super::common::*;

// ---------------------------------------------------------------------------
// Group A — atomic state + job commit
// ---------------------------------------------------------------------------

fn child_a1() -> ! {
    let (clock, store) = child_store();
    let alice = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    store
        .create_note(
            CreateNoteInput::simple_public(&alice.id, "hello after crash"),
            vec![streaming_job(&clock, "a1:job")],
        )
        .expect("child create_note");
    drop(store);
    drop(clock);
    ready_and_park();
}

/// A1: create-then-crash — note AND job survive a SIGKILL right after commit.
#[test]
fn a1_create_then_crash() {
    if child_mode_is("a1") {
        child_a1();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("a1");
    let clock = test_clock();
    let alice_id = {
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        let id = alice.id.clone();
        drop(store);
        id
    };
    run_crash_child("a1", "atomicity::a1_create_then_crash", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2.clone());
    let notes = store
        .notes_by_author(
            &alice_id,
            &TimelineCursor {
                limit: 100,
                ..Default::default()
            },
        )
        .expect("notes_by_author after reopen");
    assert_eq!(notes.len(), 1, "exactly one note survived the crash");
    let note = &notes[0];
    let job = store
        .claim_next(60_000)
        .expect("claim_next")
        .expect("job survived the crash alongside the note");
    let payload = String::from_utf8(job.payload.clone()).expect("job payload utf8");
    assert!(
        payload.contains(&note.id),
        "job payload references the note ID (got {payload:?})"
    );
    assert_eq!(job.idempotency_key, "a1:job");
}

fn child_a2() -> ! {
    let (clock, store) = child_store();
    let alice = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    store.set_abort_before_commit(true);
    let err = store
        .create_note(
            CreateNoteInput::simple_public(&alice.id, "never committed"),
            vec![streaming_job(&clock, "a2:job")],
        )
        .expect_err("injected pre-commit abort must fail the op");
    assert!(
        matches!(err, PortError::StorageUnavailable(_)),
        "abort surfaces StorageUnavailable, got {err:?}"
    );
    drop(store);
    drop(clock);
    ready_and_park();
}

/// A2: crash-before-commit — nothing is visible after recovery.
#[test]
fn a2_crash_before_commit() {
    if child_mode_is("a2") {
        child_a2();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("a2");
    let clock = test_clock();
    {
        let store = factory.open(clock.clone());
        create_user(&*store, "alice");
        drop(store);
    }
    run_crash_child("a2", "atomicity::a2_crash_before_commit", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    assert_eq!(
        list_all(&*store).len(),
        0,
        "no note may survive a pre-commit crash"
    );
    assert_eq!(
        store.pending_count().expect("pending_count"),
        0,
        "no job may survive a pre-commit crash"
    );
}

fn child_a3() -> ! {
    let (clock, store) = child_store();
    let alice = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    store
        .create_note(
            CreateNoteInput::simple_public(&alice.id, "three jobs atomically"),
            vec![
                streaming_job(&clock, "a3:job-1"),
                streaming_job(&clock, "a3:job-2"),
                streaming_job(&clock, "a3:job-3"),
            ],
        )
        .expect("child create_note with 3 jobs");
    drop(store);
    drop(clock);
    ready_and_park();
}

/// A3: multi-job atomicity — all 3 jobs + note survive together; a single
/// invalid job fails the whole op with nothing persisted.
#[test]
fn a3_multi_job_atomicity() {
    if child_mode_is("a3") {
        child_a3();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("a3");
    let clock = test_clock();
    let alice_id = {
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        let id = alice.id.clone();
        drop(store);
        id
    };
    run_crash_child("a3", "atomicity::a3_multi_job_atomicity", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2.clone());
    let notes = store
        .notes_by_author(
            &alice_id,
            &TimelineCursor {
                limit: 100,
                ..Default::default()
            },
        )
        .expect("notes_by_author");
    assert_eq!(notes.len(), 1);
    let note = &notes[0];
    let mut keys = HashSet::new();
    for _ in 0..3 {
        let job = store
            .claim_next(60_000)
            .expect("claim")
            .expect("all 3 jobs survived the crash");
        let payload = String::from_utf8(job.payload.clone()).expect("utf8 payload");
        assert!(
            payload.contains(&note.id),
            "job payload links note {}",
            note.id
        );
        keys.insert(job.idempotency_key.clone());
    }
    assert_eq!(keys.len(), 3, "all three distinct jobs present");
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "no fourth job"
    );
    drop(store);

    // Failure path: one invalid job rejects the whole create, persisting nothing.
    let clock3 = test_clock();
    let store = factory.reopen(clock3.clone());
    let bad = NewJob {
        kind: JobKind::StreamingPublish,
        payload: b"{}".to_vec(),
        idempotency_key: String::new(),
        not_before: clock3.now_ms(),
    };
    let before = list_all(&*store).len();
    let err = store
        .create_note(
            CreateNoteInput::simple_public(&alice_id, "bad job"),
            vec![bad],
        )
        .expect_err("empty idempotency_key must fail validation");
    assert!(is_validation(&err), "got {err:?}");
    assert_eq!(
        list_all(&*store).len(),
        before,
        "failed op persisted no note"
    );
}

fn child_a4() -> ! {
    let (_clock, store) = child_store();
    let alice = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    let notes = store
        .notes_by_author(
            &alice.id,
            &TimelineCursor {
                limit: 10,
                ..Default::default()
            },
        )
        .expect("notes_by_author");
    let target = notes.first().expect("note exists").id.clone();
    let job = NewJob {
        kind: JobKind::StreamingPublish,
        payload: format!(r#"{{"deleted_note_id":"{target}"}}"#).into_bytes(),
        idempotency_key: "a4:delete-job".to_string(),
        not_before: store.clock().now_ms(),
    };
    store
        .delete_note(&target, vec![job])
        .expect("child delete_note");
    drop(store);
    ready_and_park();
}

/// A4: delete atomicity — tombstone + jobs appear together; body gone for
/// non-author viewers.
#[test]
fn a4_delete_atomicity() {
    if child_mode_is("a4") {
        child_a4();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("a4");
    let clock = test_clock();
    let note_id = {
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        let note = note_with_job(&*store, &clock, &alice.id, "to be deleted", "a4:create");
        let id = note.id.clone();
        drop(store);
        id
    };
    run_crash_child("a4", "atomicity::a4_delete_atomicity", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    let tombstone = store
        .get_note_including_deleted(&note_id)
        .expect("get including deleted")
        .expect("tombstone row exists");
    assert!(tombstone.is_deleted, "tombstone marks is_deleted");
    assert_eq!(tombstone.text, None, "tombstone drops the body");
    assert!(
        store.get_note(&note_id).expect("get_note").is_none(),
        "get_note hides the tombstone from non-author viewers"
    );
    // Both the create job and the delete job were committed; the delete job
    // links the tombstoned note.
    let mut saw_delete_job = false;
    let mut claimed = 0;
    while let Some(job) = store.claim_next(60_000).expect("claim") {
        claimed += 1;
        if job.idempotency_key == "a4:delete-job" {
            let payload = String::from_utf8(job.payload.clone()).expect("utf8");
            assert!(
                payload.contains(&note_id),
                "delete job links note {note_id}"
            );
            saw_delete_job = true;
        }
    }
    assert_eq!(claimed, 2, "create job + delete job both survived");
    assert!(saw_delete_job, "delete job committed with the tombstone");
}

fn child_a5() -> ! {
    let (clock, store) = child_store();
    let alice = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    let bob = store
        .find_by_handle("bob")
        .expect("find bob")
        .expect("bob exists");
    store
        .follow(
            &alice.id,
            &bob.id,
            false,
            None,
            vec![federate_job(&clock, "a5:deliver")],
        )
        .expect("child follow");
    drop(store);
    drop(clock);
    ready_and_park();
}

/// A5: follow atomicity — relationship + `FederateDeliver` job appear together.
#[test]
fn a5_follow_atomicity() {
    if child_mode_is("a5") {
        child_a5();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("a5");
    let clock = test_clock();
    let (alice_id, bob_id) = {
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        let bob = create_user(&*store, "bob");
        let ids = (alice.id.clone(), bob.id.clone());
        drop(store);
        ids
    };
    run_crash_child("a5", "atomicity::a5_follow_atomicity", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    assert!(
        store
            .is_following(&alice_id, &bob_id)
            .expect("is_following"),
        "follow row survived the crash"
    );
    let job = store
        .claim_next(60_000)
        .expect("claim")
        .expect("FederateDeliver job survived alongside the follow");
    assert_eq!(job.kind, JobKind::FederateDeliver);
}

// ---------------------------------------------------------------------------
// Exit-box-2 acceptance test
// ---------------------------------------------------------------------------

fn child_exit_box_2() -> ! {
    let (clock, store) = child_store();
    let alice = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    store
        .create_note(
            CreateNoteInput::simple_public(&alice.id, "exit box 2 note"),
            vec![streaming_job(&clock, "exit:job")],
        )
        .expect("child create_note");
    drop(store);
    drop(clock);
    ready_and_park();
}

/// `conformance_exit_box_2`: one durable job through the storage interface,
/// SIGKILL, reopen — note + job both present and linked.
#[test]
fn conformance_exit_box_2() {
    if child_mode_is("exit_box_2") {
        child_exit_box_2();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("exit-box-2");
    let clock = test_clock();
    {
        let store = factory.open(clock.clone());
        create_user(&*store, "alice");
        drop(store);
    }
    run_crash_child(
        "exit_box_2",
        "atomicity::conformance_exit_box_2",
        factory.path(),
    );

    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    let notes = list_all(&*store);
    assert_eq!(notes.len(), 1, "note present after crash");
    let job = store
        .claim_next(60_000)
        .expect("claim")
        .expect("job present after crash");
    let payload = String::from_utf8(job.payload.clone()).expect("utf8 payload");
    assert!(
        payload.contains(&notes[0].id),
        "job payload references note {} (got {payload:?})",
        notes[0].id
    );
}
