use super::common::*;

// ---------------------------------------------------------------------------
// Group R — branched replies (docs/replies.md Slices A–C): parent-child
// index + migration, visibility-aware scan, counter consistency, pending
// parents + durable resolution, read-surface withholding, repair.
// Store-level cases are generic over ConformanceStore so Phase 4 runs them
// against FoundationDB unchanged; application/worker/network cases are
// redb-backed `#[test]`s over `open_worker_stack` (precedent: B10/B11).
// The FoundationDB adapter is still a 0-byte stub, so only redb is green —
// recorded honestly in docs/persistence-conformance.md.
// ---------------------------------------------------------------------------

/// Reply to `parent_id` as `author_id`.
fn reply_note<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
    author_id: &str,
    text: &str,
    parent_id: &str,
    key: &str,
) -> Note {
    let mut input = CreateNoteInput::simple_public(author_id, text);
    input.reply_to = Some(parent_id.to_string());
    store
        .create_note(input, vec![streaming_job(clock, key)])
        .expect("create reply")
}

/// Remote-style reply waiting on an unresolved parent URI (store-level
/// producers only; the local application path rejects this field).
fn pending_reply_note<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
    author_id: &str,
    text: &str,
    parent_uri: &str,
    key: &str,
) -> Note {
    let mut input = CreateNoteInput::simple_public(author_id, text);
    input.reply_parent_uri = Some(parent_uri.to_string());
    store
        .create_note(input, vec![streaming_job(clock, key)])
        .expect("create pending reply")
}

fn note_page(cursor_limit: usize) -> TimelineCursor {
    TimelineCursor {
        limit: cursor_limit,
        ..Default::default()
    }
}

/// R1: creating replies writes the adjacency index, bumps the parent
/// counter, and commits the durable jobs atomically. Unknown and empty
/// parents yield `[]` (no existence check, Misskey parity).
fn case_r1_reply_index_and_counter<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let root = note_with_job(store, clock, &alice.id, "r1 root", "r1:root");
    clock.advance_ms(1);
    let a = reply_note(store, clock, &bob.id, "r1 A", &root.id, "r1:a");
    clock.advance_ms(1);
    let b = reply_note(store, clock, &alice.id, "r1 B", &root.id, "r1:b");

    let children = store
        .notes_by_reply(&root.id, &note_page(100))
        .expect("notes_by_reply");
    assert_eq!(
        children.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![b.id.clone(), a.id.clone()],
        "direct children only, newest-first"
    );
    assert!(
        store
            .notes_by_reply(&a.id, &note_page(100))
            .expect("leaf")
            .is_empty(),
        "leaf has no children"
    );
    assert!(
        store
            .notes_by_reply("missing-parent", &note_page(100))
            .expect("unknown")
            .is_empty(),
        "unknown parent yields []"
    );
    assert!(
        store
            .notes_by_reply("", &note_page(100))
            .expect("empty")
            .is_empty(),
        "empty parent yields []"
    );
    let parent = store
        .get_note(&root.id)
        .expect("get parent")
        .expect("parent exists");
    assert_eq!(parent.replies_count, 2, "counter counts direct replies");
    assert_eq!(
        a.reply_user_id.as_deref(),
        Some(alice.id.as_str()),
        "reply records the parent author"
    );
    assert_eq!(
        store.pending_count().expect("pending jobs"),
        3,
        "root + 2 replies committed their jobs atomically"
    );
}

#[test]
fn r1_reply_index_and_counter() {
    let factory = RedbFactory::fresh("r1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_r1_reply_index_and_counter(&*store, &clock);
}

/// R2: `notes_by_reply` paging — newest-first, exclusive `sinceId` /
/// `untilId`, `limit` as a candidate scan cap — and deleted children are
/// excluded at the storage layer.
fn case_r2_reply_paging_and_deleted<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let root = note_with_job(store, clock, &alice.id, "r2 root", "r2:root");
    let mut kids = Vec::new();
    for (i, key) in ["r2:c1", "r2:c2", "r2:c3", "r2:c4", "r2:c5"]
        .into_iter()
        .enumerate()
    {
        clock.advance_ms(1);
        kids.push(reply_note(
            store,
            clock,
            &alice.id,
            &format!("r2 kid {i}"),
            &root.id,
            key,
        ));
    }
    // Delete the middle child.
    store
        .delete_note(&kids[2].id, Vec::new())
        .expect("delete middle child");

    let full = store
        .notes_by_reply(&root.id, &note_page(100))
        .expect("full page");
    assert_eq!(
        full.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![
            kids[4].id.clone(),
            kids[3].id.clone(),
            kids[1].id.clone(),
            kids[0].id.clone()
        ],
        "deleted child excluded, rest newest-first"
    );
    let capped = store
        .notes_by_reply(&root.id, &note_page(2))
        .expect("capped page");
    assert_eq!(
        capped.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![kids[4].id.clone(), kids[3].id.clone()],
        "limit caps the candidate batch"
    );
    let older = store
        .notes_by_reply(
            &root.id,
            &TimelineCursor {
                until_id: Some(kids[3].id.clone()),
                limit: 100,
                ..Default::default()
            },
        )
        .expect("untilId page");
    assert_eq!(
        older.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![kids[1].id.clone(), kids[0].id.clone()],
        "untilId is exclusive and skips the deleted row"
    );
    let newer = store
        .notes_by_reply(
            &root.id,
            &TimelineCursor {
                since_id: Some(kids[1].id.clone()),
                limit: 100,
                ..Default::default()
            },
        )
        .expect("sinceId page");
    assert_eq!(
        newer.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![kids[4].id.clone(), kids[3].id.clone()],
        "sinceId is exclusive"
    );
}

#[test]
fn r2_reply_paging_and_deleted() {
    let factory = RedbFactory::fresh("r2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_r2_reply_paging_and_deleted(&*store, &clock);
}

/// R3: delete decrements the parent's `replies_count` exactly once
/// (saturating; repeats are `NotFound` and change nothing), drops the index
/// row, and never reparents descendants. A reply that also carries
/// renote/quote metadata decrements `replies_count` while keeping the
/// pre-existing pure-renote-only `renote_count` rule.
fn case_r3_delete_counter_consistency<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let root = note_with_job(store, clock, &alice.id, "r3 root", "r3:root");
    clock.advance_ms(1);
    let a = reply_note(store, clock, &bob.id, "r3 A", &root.id, "r3:a");
    clock.advance_ms(1);
    let a1 = reply_note(store, clock, &alice.id, "r3 A1", &a.id, "r3:a1");

    store
        .delete_note(&a1.id, Vec::new())
        .expect("delete leaf reply");
    let a_after = store
        .get_note_including_deleted(&a.id)
        .expect("get A")
        .expect("A row survives");
    assert_eq!(a_after.replies_count, 0, "counter decremented exactly once");
    assert!(
        store
            .notes_by_reply(&a.id, &note_page(100))
            .expect("index after delete")
            .is_empty(),
        "deleted child leaves the index"
    );
    let repeat = store
        .delete_note(&a1.id, Vec::new())
        .expect_err("repeat delete");
    assert!(
        matches!(repeat, PortError::NotFound(_)),
        "repeat delete is NotFound, got {repeat:?}"
    );
    assert_eq!(
        store
            .get_note_including_deleted(&a.id)
            .expect("get A again")
            .expect("A row")
            .replies_count,
        0,
        "repeat delete cannot decrement twice (saturating + already-deleted guard)"
    );

    // Deleting a parent keeps descendants' reply_id (no reparenting) while
    // the parent row itself hides from general reads.
    clock.advance_ms(1);
    let b = reply_note(store, clock, &bob.id, "r3 B", &root.id, "r3:b");
    store
        .delete_note(&root.id, Vec::new())
        .expect("delete parent");
    assert!(
        store
            .get_note(&root.id)
            .expect("get deleted root")
            .is_none(),
        "deleted parent hides from general reads"
    );
    let b_row = store
        .get_note_including_deleted(&b.id)
        .expect("get B")
        .expect("B survives");
    assert_eq!(
        b_row.reply_id.as_deref(),
        Some(root.id.as_str()),
        "descendant keeps its reply_id (no silent reparent)"
    );
    assert_eq!(
        store
            .notes_by_reply(&root.id, &note_page(100))
            .expect("children of deleted parent")
            .iter()
            .map(|n| n.id.clone())
            .collect::<Vec<_>>(),
        vec![b.id.clone(), a.id.clone()],
        "live children of a deleted parent stay listed at the storage layer"
    );

    // Reply that also carries renote metadata (quote shape): create bumps
    // replies_count once (no double count across the two target writes) and
    // renote_count once; delete drops replies_count, keeps renote_count per
    // the pre-existing pure-renote-only rule.
    let root2 = note_with_job(store, clock, &alice.id, "r3 root2", "r3:root2");
    clock.advance_ms(1);
    let mut combo = CreateNoteInput::simple_public(&bob.id, "r3 combo with commentary");
    combo.reply_to = Some(root2.id.clone());
    combo.renote_of = Some(root2.id.clone());
    let combo_note = store
        .create_note(combo, Vec::new())
        .expect("create reply+renote combo");
    let root2_after_create = store
        .get_note(&root2.id)
        .expect("get root2")
        .expect("root2 exists");
    assert_eq!(
        root2_after_create.replies_count, 1,
        "combo bumps replies once"
    );
    assert_eq!(
        root2_after_create.renote_count, 1,
        "combo bumps renotes once"
    );
    store
        .delete_note(&combo_note.id, Vec::new())
        .expect("delete combo");
    let root2_after_delete = store
        .get_note_including_deleted(&root2.id)
        .expect("get root2 deleted?")
        .expect("root2 row survives");
    assert_eq!(
        root2_after_delete.replies_count, 0,
        "combo delete drops replies"
    );
    assert_eq!(
        root2_after_delete.renote_count, 1,
        "non-pure renote keeps renote_count (pre-existing rule)"
    );
}

#[test]
fn r3_delete_counter_consistency() {
    let factory = RedbFactory::fresh("r3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_r3_delete_counter_consistency(&*store, &clock);
}

/// R4: `note_replies` serves the indexed scan with visibility applied per
/// candidate — hidden/deleted rows never produce short pages while later
/// visible siblings exist, and cursor paging has no duplicates or gaps.
#[test]
fn r4_replies_scan_is_visibility_aware() {
    let factory = RedbFactory::fresh("r4");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, services, _worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    let bob = create_user(&*store, "bob");
    let carol = create_user(&*store, "carol");
    let dave = create_user(&*store, "dave");
    // Carol follows alice, so alice's followers-only notes are visible to
    // carol but hidden from stranger dave.
    store
        .follow(&carol.id, &alice.id, false, None, Vec::new())
        .expect("carol follows alice");

    let root = note_with_job(&*store, &clock, &alice.id, "r4 root", "r4:root");
    clock.advance_ms(1);
    let p1 = reply_note(&*store, &clock, &bob.id, "r4 public one", &root.id, "r4:p1");
    let mut followers_kids = Vec::new();
    for (i, key) in ["r4:f1", "r4:f2", "r4:f3"].into_iter().enumerate() {
        clock.advance_ms(1);
        let mut input = CreateNoteInput::simple_public(&alice.id, &format!("r4 followers {i}"));
        input.visibility = Visibility::Followers;
        input.reply_to = Some(root.id.clone());
        followers_kids.push(
            store
                .create_note(input, vec![streaming_job(&clock, key)])
                .expect("followers reply"),
        );
    }
    clock.advance_ms(1);
    let p2 = reply_note(&*store, &clock, &bob.id, "r4 public two", &root.id, "r4:p2");
    clock.advance_ms(1);
    let gone = reply_note(&*store, &clock, &bob.id, "r4 gone", &root.id, "r4:gone");
    store
        .delete_note(&gone.id, Vec::new())
        .expect("delete newest child");

    // Stranger dave: only live public replies, newest-first.
    let full = services
        .note_replies(&root.id, Some(&dave.id), &note_page(10))
        .expect("dave full page");
    assert_eq!(
        full.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![p2.id.clone(), p1.id.clone()],
        "hidden and deleted rows never surface, page stays full"
    );
    // A limit-1 page still finds the visible head past nothing hidden, and
    // the next page advances past the whole hidden run without gaps.
    let head = services
        .note_replies(&root.id, Some(&dave.id), &note_page(1))
        .expect("dave head page");
    assert_eq!(
        head.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![p2.id.clone()]
    );
    let tail = services
        .note_replies(
            &root.id,
            Some(&dave.id),
            &TimelineCursor {
                until_id: Some(p2.id.clone()),
                limit: 10,
                ..Default::default()
            },
        )
        .expect("dave tail page");
    assert_eq!(
        tail.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![p1.id.clone()],
        "internal untilId advance crosses hidden/deleted rows without gaps"
    );
    // Follower carol sees the followers-only run too.
    let carol_full = services
        .note_replies(&root.id, Some(&carol.id), &note_page(10))
        .expect("carol full page");
    assert_eq!(
        carol_full.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![
            p2.id.clone(),
            followers_kids[2].id.clone(),
            followers_kids[1].id.clone(),
            followers_kids[0].id.clone(),
            p1.id.clone()
        ],
        "viewer with access sees hidden rows in order"
    );
    // sinceId stays exclusive across the scan.
    let newer = services
        .note_replies(
            &root.id,
            Some(&dave.id),
            &TimelineCursor {
                since_id: Some(p1.id.clone()),
                limit: 10,
                ..Default::default()
            },
        )
        .expect("dave sinceId page");
    assert_eq!(
        newer.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![p2.id.clone()]
    );
}

fn child_r5() -> ! {
    let (clock, store) = child_store();
    let alice = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    let parent = store
        .notes_by_author(&alice.id, &note_page(100))
        .expect("list parent")
        .into_iter()
        .find(|n| n.reply_id.is_none())
        .expect("parent note exists");
    store.set_abort_before_commit(true);
    let mut input = CreateNoteInput::simple_public(&alice.id, "never committed reply");
    input.reply_to = Some(parent.id.clone());
    let err = store
        .create_note(input, vec![streaming_job(&clock, "r5:job")])
        .expect_err("injected pre-commit abort must fail the op");
    assert!(
        matches!(err, PortError::StorageUnavailable(_)),
        "abort surfaces StorageUnavailable, got {err:?}"
    );
    drop(store);
    drop(clock);
    ready_and_park();
}

/// R5: crash-before-commit on a reply create — note, index row, parent
/// counter bump, and jobs are all absent after recovery.
#[test]
fn r5_reply_crash_before_commit() {
    if child_mode_is("r5") {
        child_r5();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("r5");
    let parent_id = {
        let clock = test_clock();
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        let parent = note_with_job(&*store, &clock, &alice.id, "r5 parent", "r5:parent");
        let id = parent.id.clone();
        drop(store);
        id
    };
    run_crash_child(
        "r5",
        "note_replies::r5_reply_crash_before_commit",
        factory.path(),
    );

    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    assert_eq!(
        list_all(&*store).len(),
        1,
        "no reply may survive a pre-commit crash"
    );
    let parent = store
        .get_note(&parent_id)
        .expect("get parent")
        .expect("parent survives");
    assert_eq!(
        parent.replies_count, 0,
        "counter untouched by the aborted reply"
    );
    assert!(
        store
            .notes_by_reply(&parent_id, &note_page(100))
            .expect("index after crash")
            .is_empty(),
        "no index row may survive a pre-commit crash"
    );
    assert_eq!(
        store.pending_count().expect("pending jobs"),
        1,
        "only the parent's own job survives"
    );
}

/// R6: pending-link atomicity and idempotency — one link sets `reply_id`,
/// clears the pending URI, adopts the parent author, re-applies
/// parent-driven narrowing, bumps the counter, and swaps the index rows.
/// Replays converge to `Ok(None)` with no double count; mismatches and
/// cycles are `Validation`.
fn case_r6_pending_link_idempotent<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let carol = create_user(store, "carol");
    let dave = create_user(store, "dave");
    // Carol follows the reply author (bob): after narrowing, bob's
    // followers-only reply is visible to carol but hidden from dave.
    store
        .follow(&carol.id, &bob.id, false, None, Vec::new())
        .expect("carol follows bob");

    // Followers-only parent: linking re-applies narrowing (public child
    // tightens to followers, like a reply cached at ingestion).
    let mut parent_input = CreateNoteInput::simple_public(&alice.id, "r6 parent");
    parent_input.visibility = Visibility::Followers;
    parent_input.uri = Some("https://remote.example/notes/r6p".to_string());
    let parent = store
        .create_note(parent_input, Vec::new())
        .expect("create parent");
    let canonical = "https://remote.example/notes/r6p";

    let mut child_input = CreateNoteInput::simple_public(&bob.id, "r6 pending child");
    child_input.uri = Some("https://remote.example/notes/r6c".to_string());
    child_input.reply_parent_uri = Some(canonical.to_string());
    let child = store
        .create_note(child_input, Vec::new())
        .expect("create pending child");

    // Before linking: pending index lists the child, the reply index does
    // not, and no counter moved.
    assert_eq!(
        store
            .pending_reply_children(canonical, 100)
            .expect("pending before link")
            .iter()
            .map(|n| n.id.clone())
            .collect::<Vec<_>>(),
        vec![child.id.clone()]
    );
    assert!(
        store
            .notes_by_reply(&parent.id, &note_page(100))
            .expect("reply index before link")
            .is_empty()
    );

    let linked = store
        .link_pending_reply(&child.id, &parent.id, canonical)
        .expect("link")
        .expect("first link reports the note");
    assert_eq!(linked.reply_id.as_deref(), Some(parent.id.as_str()));
    assert_eq!(linked.reply_parent_uri, None, "pending URI cleared");
    assert_eq!(
        linked.reply_user_id.as_deref(),
        Some(alice.id.as_str()),
        "parent author adopted"
    );
    assert_eq!(
        linked.visibility,
        Visibility::Followers,
        "parent-driven narrowing re-applied at link time"
    );
    assert!(
        !is_visible(&linked, &ViewerContext::local_user(&dave.id, false)),
        "narrowed child hidden from strangers"
    );
    assert!(
        is_visible(&linked, &ViewerContext::local_user(&carol.id, true)),
        "narrowed child visible to a follower"
    );
    let parent_after = store
        .get_note(&parent.id)
        .expect("get parent")
        .expect("parent exists");
    assert_eq!(parent_after.replies_count, 1);
    assert_eq!(
        store
            .notes_by_reply(&parent.id, &note_page(100))
            .expect("reply index after link")
            .iter()
            .map(|n| n.id.clone())
            .collect::<Vec<_>>(),
        vec![child.id.clone()]
    );
    assert!(
        store
            .pending_reply_children(canonical, 100)
            .expect("pending after link")
            .is_empty(),
        "pending row removed atomically with the link"
    );

    // Replay converges: no double count, no duplicate index row.
    assert!(
        store
            .link_pending_reply(&child.id, &parent.id, canonical)
            .expect("replay link")
            .is_none(),
        "second link converges to None"
    );
    assert_eq!(
        store
            .get_note(&parent.id)
            .expect("get parent again")
            .expect("parent")
            .replies_count,
        1,
        "replay must not double-count"
    );
    assert!(
        !store
            .set_pending_reply_parent(&child.id, canonical, Vec::new())
            .expect("mark after link"),
        "already-linked child is not re-marked"
    );

    // Mismatch: the child's pending URI must equal the parent canonical.
    let other = pending_reply_note(
        store,
        clock,
        &bob.id,
        "r6 other",
        "https://remote.example/notes/elsewhere",
        "r6:other",
    );
    let mismatch = store
        .link_pending_reply(&other.id, &parent.id, canonical)
        .expect_err("URI mismatch must not link");
    assert!(
        matches!(mismatch, PortError::Validation(_)),
        "mismatch is Validation, got {mismatch:?}"
    );

    // Cycle: A waits on B's URI while B already replies to A (the store
    // permits replying to a pending row by existence; the application
    // path would reject it as invisible). Linking A under B is rejected;
    // both entries survive for operator inspection.
    let a_pending = pending_reply_note(
        store,
        clock,
        &alice.id,
        "r6 A waits on B",
        "https://remote.example/notes/r6b",
        "r6:apend",
    );
    let mut b_input = CreateNoteInput::simple_public(&bob.id, "r6 B replies to pending A");
    b_input.reply_to = Some(a_pending.id.clone());
    b_input.uri = Some("https://remote.example/notes/r6b".to_string());
    let b_note = store.create_note(b_input, Vec::new()).expect("create B");
    let cycle = store
        .link_pending_reply(
            &a_pending.id,
            &b_note.id,
            "https://remote.example/notes/r6b",
        )
        .expect_err("cycle must not link");
    assert!(
        matches!(cycle, PortError::Validation(_)),
        "cycle is Validation, got {cycle:?}"
    );
    // Self-link is the trivial cycle.
    let self_pending = pending_reply_note(
        store,
        clock,
        &bob.id,
        "r6 self",
        "https://remote.example/notes/r6self",
        "r6:self",
    );
    let self_cycle = store
        .link_pending_reply(
            &self_pending.id,
            &self_pending.id,
            "https://remote.example/notes/r6self",
        )
        .expect_err("self-link must not link");
    assert!(
        matches!(self_cycle, PortError::Validation(_)),
        "self-link is Validation, got {self_cycle:?}"
    );
}

#[test]
fn r6_pending_link_idempotent() {
    let factory = RedbFactory::fresh("r6");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_r6_pending_link_idempotent(&*store, &clock);
}

/// R7: the Slice A migration backfills `NOTES_BY_REPLY` for pre-index data,
/// resumes after an interrupted run, and is idempotent — without touching
/// note rows, counters, or jobs.
#[test]
fn r7_reply_index_migration_backfills_and_resumes() {
    let factory = RedbFactory::fresh("r7");
    let clock = test_clock();
    {
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        let bob = create_user(&*store, "bob");
        let root = note_with_job(&*store, &clock, &alice.id, "r7 root", "r7:root");
        clock.advance_ms(1);
        let a = reply_note(&*store, &clock, &bob.id, "r7 A", &root.id, "r7:a");
        clock.advance_ms(1);
        let b = reply_note(&*store, &clock, &alice.id, "r7 B", &root.id, "r7:b");
        clock.advance_ms(1);
        let a1 = reply_note(&*store, &clock, &bob.id, "r7 A1", &a.id, "r7:a1");
        // Sanity on the live index before simulating legacy data.
        assert_eq!(
            store
                .notes_by_reply(&root.id, &note_page(100))
                .expect("live index")
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            vec![b.id.clone(), a.id.clone()]
        );
        // Simulate a pre-Slice-A database: drop the index + marker.
        store.remove_reply_index_for_test().expect("wipe index");
        assert!(
            store
                .notes_by_reply(&root.id, &note_page(100))
                .expect("wiped index")
                .is_empty(),
            "legacy data is invisible to the index until backfilled"
        );
        // Interrupted run: the injected pre-commit abort fails the batch.
        store.set_abort_before_commit(true);
        let err = store
            .run_reply_index_backfill()
            .expect_err("interrupted backfill must fail");
        assert!(
            matches!(err, PortError::StorageUnavailable(_)),
            "interruption surfaces StorageUnavailable, got {err:?}"
        );
        // Resume completes from the persisted cursor.
        store.set_abort_before_commit(false);
        store
            .run_reply_index_backfill()
            .expect("resumed backfill completes");
        assert_eq!(
            store
                .notes_by_reply(&root.id, &note_page(100))
                .expect("backfilled root")
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            vec![b.id.clone(), a.id.clone()],
            "backfill restores direct children, newest-first"
        );
        assert_eq!(
            store
                .notes_by_reply(&a.id, &note_page(100))
                .expect("backfilled A")
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            vec![a1.id.clone()],
            "backfill restores multi-level edges"
        );
        // Untouched: counters, bodies, jobs.
        assert_eq!(
            store
                .get_note(&root.id)
                .expect("get root")
                .expect("root")
                .replies_count,
            2
        );
        assert_eq!(
            store
                .get_note(&a.id)
                .expect("get A")
                .expect("A")
                .replies_count,
            1
        );
        assert_eq!(list_all(&*store).len(), 4, "no note rows gained or lost");
        assert_eq!(
            store.pending_count().expect("jobs"),
            4,
            "no jobs gained or lost"
        );
        // Idempotent re-run is a marker-hit no-op.
        store
            .run_reply_index_backfill()
            .expect("second run is a no-op");
        drop(store);
    }
    // Reopen runs the migration path again (done marker → no-op) and the
    // index survives the restart.
    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    let root_id = list_all(&*store)
        .into_iter()
        .find(|n| n.text.as_deref() == Some("r7 root"))
        .expect("root survives reopen")
        .id
        .clone();
    assert_eq!(
        store
            .notes_by_reply(&root_id, &note_page(100))
            .expect("index after reopen")
            .len(),
        2,
        "backfilled index survives restart"
    );
}

/// R8: unresolved replies are withheld from every ordinary read surface
/// (fail-closed), never counted as roots, and the local create path rejects
/// the inbox-only pending field.
#[test]
fn r8_pending_withheld_from_reads() {
    use pinetree::domain::policy::is_visible as policy_visible;
    let factory = RedbFactory::fresh("r8");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, services, _worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    let reno = slice10_remote_account(&*store, "reno", "https://remote.example/inbox");
    let viewer = Some(alice.id.as_str());

    let mut control_input = CreateNoteInput::simple_public(&reno.id, "r8control visible root");
    control_input.uri = Some("https://remote.example/notes/r8control".to_string());
    let control = store
        .create_note(control_input, Vec::new())
        .expect("control root");
    let pending = pending_reply_note(
        &*store,
        &clock,
        &reno.id,
        "r8pending hidden reply",
        "https://remote.example/notes/r8parent",
        "r8:pending",
    );

    // Store-level reads still see the row (resolver/repair need it); the
    // shared read predicate withholds it.
    assert!(
        store
            .get_note_including_deleted(&pending.id)
            .expect("get pending row")
            .is_some(),
        "pending row stays stored"
    );
    assert!(
        !policy_visible(
            &store
                .get_note_including_deleted(&pending.id)
                .expect("get")
                .expect("row"),
            &ViewerContext::local_user(&alice.id, false)
        ),
        "is_visible withholds pending"
    );
    // Detail, profiles (both reply filters), timelines, search,
    // conversation: all empty / NotFound.
    assert!(
        matches!(
            services.notes_show(&pending.id, viewer),
            Err(PortError::NotFound(_))
        ),
        "notes/show withholds pending"
    );
    for with_replies in [false, true] {
        let profile = services
            .user_notes(&reno.id, viewer, &note_page(100), with_replies, true, false)
            .expect("users/notes");
        assert_eq!(
            profile.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
            vec![control.id.clone()],
            "profiles never list pending (with_replies={with_replies})"
        );
    }
    let timeline = services
        .local_timeline(viewer, &note_page(100), false, true, true)
        .expect("local timeline");
    assert!(
        !timeline.iter().any(|n| n.id == pending.id),
        "timelines withhold pending"
    );
    // Remote-authored notes are not local-timeline eligible, so the
    // control's presence is checked on the global timeline instead.
    let global = services
        .global_timeline(None, &note_page(100), false, true)
        .expect("global timeline");
    assert!(
        !global.iter().any(|n| n.id == pending.id),
        "global timeline withholds pending"
    );
    assert!(global.iter().any(|n| n.id == control.id), "control present");
    let search = services
        .search_notes("r8pending", viewer, &note_page(100), None, None)
        .expect("search");
    assert!(search.is_empty(), "search withholds pending bodies");
    assert!(
        matches!(
            services.note_conversation(&pending.id, viewer, 10, 0),
            Err(PortError::NotFound(_))
        ),
        "conversation of a pending note is NotFound"
    );
    assert!(
        services
            .note_replies(
                "https://remote.example/notes/r8parent",
                viewer,
                &note_page(10)
            )
            .expect("replies")
            .is_empty(),
        "pending never appears in notes/replies"
    );
    // Local creates cannot set the inbox-only field; the store rejects
    // conflicting (resolved + pending) parents.
    let mut local_input = CreateNoteInput::simple_public(&alice.id, "r8 local");
    local_input.reply_parent_uri = Some("https://remote.example/notes/x".to_string());
    assert!(
        matches!(
            services.create_note(local_input),
            Err(PortError::Validation(_))
        ),
        "local create rejects reply_parent_uri"
    );
    let mut both = CreateNoteInput::simple_public(&alice.id, "r8 both");
    both.reply_to = Some(control.id.clone());
    both.reply_parent_uri = Some("https://remote.example/notes/x".to_string());
    assert!(
        matches!(
            store.create_note(both, Vec::new()),
            Err(PortError::Validation(_))
        ),
        "store rejects resolved + pending parents together"
    );
}

/// R9: the opt-in bounded repair re-fetches historical remote roots and
/// marks only those whose origin document still declares an `inReplyTo` —
/// never guessing from text. Unreachable origins, genuine roots, and
/// self-links stay untouched.
#[test]
fn r9_repair_marks_historical_pending() {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub origin");
    let addr = listener.local_addr().expect("stub addr");
    let base = format!("http://{addr}");
    let stub_base = base.clone();
    std::thread::spawn(move || {
        for _ in 0..16 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            let request = String::from_utf8_lossy(&buf);
            let path = request
                .lines()
                .next()
                .unwrap_or("")
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .to_string();
            let doc = |id: &str, in_reply_to: Option<&str>| {
                let irt = in_reply_to
                    .map(|u| format!(",\"inReplyTo\":\"{u}\""))
                    .unwrap_or_default();
                format!(
                    "{{\"@context\":\"https://www.w3.org/ns/activitystreams\",\"id\":\"{id}\",\
                     \"type\":\"Note\",\"attributedTo\":\"{stub_base}/users/x\",\
                     \"content\":\"historical\",\"to\":[\"https://www.w3.org/ns/activitystreams#Public\"]{irt}}}"
                )
            };
            let (status, body) = match path.as_str() {
                "/notes/9" => (
                    "HTTP/1.1 200 OK",
                    doc(
                        &format!("{stub_base}/notes/9"),
                        Some(&format!("{stub_base}/notes/1")),
                    ),
                ),
                "/notes/7" => (
                    "HTTP/1.1 200 OK",
                    doc(&format!("{stub_base}/notes/7"), None),
                ),
                "/notes/8" => (
                    "HTTP/1.1 200 OK",
                    doc(
                        &format!("{stub_base}/notes/8"),
                        Some(&format!("{stub_base}/notes/8")),
                    ),
                ),
                _ => ("HTTP/1.1 404 Not Found", String::new()),
            };
            let resp = format!(
                "{status}\r\ncontent-type: application/activity+json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });

    let factory = RedbFactory::fresh("r9");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, services, _worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let reno = slice10_remote_account(&*store, "reno", "https://remote.example/inbox");
    let mut historical_input =
        CreateNoteInput::simple_public(&reno.id, "r9 historical reply flattened long ago");
    historical_input.uri = Some(format!("{base}/notes/9"));
    let historical = store
        .create_note(historical_input, Vec::new())
        .expect("historical");
    let mut genuine_input = CreateNoteInput::simple_public(&reno.id, "r9 genuine root");
    genuine_input.uri = Some(format!("{base}/notes/7"));
    let genuine = store
        .create_note(genuine_input, Vec::new())
        .expect("genuine");
    let mut self_input = CreateNoteInput::simple_public(&reno.id, "r9 self link");
    self_input.uri = Some(format!("{base}/notes/8"));
    let self_note = store.create_note(self_input, Vec::new()).expect("self");
    let mut gone_input = CreateNoteInput::simple_public(&reno.id, "r9 origin gone");
    gone_input.uri = Some("http://127.0.0.1:1/notes/1".to_string());
    let gone = store.create_note(gone_input, Vec::new()).expect("gone");

    let report =
        pinetree::application::commands::inbound::replies::repair_unresolved_reply_parents(
            &services, 100,
        )
        .expect("repair runs");
    assert_eq!(report.scanned, 4, "bounded scan covers the four candidates");
    assert_eq!(report.marked, 1, "only the declared reply is marked");
    let healed = store
        .get_note_including_deleted(&historical.id)
        .expect("get healed")
        .expect("healed row");
    assert_eq!(
        healed.reply_parent_uri.as_deref(),
        Some(format!("{base}/notes/1").as_str()),
        "pending URI taken from the fetched document, not text"
    );
    for (id, label) in [
        (&genuine.id, "genuine"),
        (&self_note.id, "self"),
        (&gone.id, "gone"),
    ] {
        let row = store
            .get_note_including_deleted(id)
            .expect("get")
            .expect("row");
        assert_eq!(row.reply_parent_uri, None, "{label} root stays a root");
        assert_eq!(row.reply_id, None, "{label} root stays unlinked");
    }
    // The mark enqueued exactly one URI-keyed resolver job, atomically.
    let job = store
        .claim_next(60_000)
        .expect("claim")
        .expect("resolver job");
    assert_eq!(job.kind, JobKind::ReplyParentResolve);
    let payload: ReplyParentResolvePayload =
        serde_json::from_slice(&job.payload).expect("resolver payload decodes");
    assert_eq!(payload.parent_uri, format!("{base}/notes/1"));
}

/// R10: the worker's resolver links children of an already-cached parent
/// with zero network I/O, converges duplicates, and continues past full
/// batches. An unresolvable parent retries (job stays pending); the
/// pending entry is kept.
#[test]
fn r10_resolver_links_cached_parent() {
    use pinetree::application::commands::inbound::replies::{
        ReplyResolveOutcome, handle_reply_parent_resolve, resolve_reply_parent,
    };
    let factory = RedbFactory::fresh("r10");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, services, worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    let bob = create_user(&*store, "bob");

    let mut parent_input = CreateNoteInput::simple_public(&alice.id, "r10 parent");
    parent_input.uri = Some("https://remote.example/notes/r10p".to_string());
    let parent = store.create_note(parent_input, Vec::new()).expect("parent");
    let canonical = "https://remote.example/notes/r10p";
    let c1 = pending_reply_note(&*store, &clock, &bob.id, "r10 c1", canonical, "r10:c1");
    let c2 = pending_reply_note(&*store, &clock, &bob.id, "r10 c2", canonical, "r10:c2");

    // Bounded pass with batch 1 links one child and reports more work.
    match resolve_reply_parent(&services, canonical, 1).expect("first pass") {
        ReplyResolveOutcome::Continue => {}
        ReplyResolveOutcome::Done => panic!("full batch of 2 with limit 1 must Continue"),
    }
    assert_eq!(
        store
            .pending_reply_children(canonical, 100)
            .expect("pending after pass 1")
            .len(),
        1,
        "one child linked, one still waiting"
    );
    // The continuation (full batch size) finishes the set through the
    // worker entry point.
    let payload = ReplyParentResolvePayload {
        parent_uri: canonical.to_string(),
    };
    let bytes = serde_json::to_vec(&payload).expect("payload encodes");
    assert!(
        handle_reply_parent_resolve(&services, &bytes)
            .expect("second pass")
            .is_none(),
        "no continuation once the set drains"
    );
    for child_id in [&c1.id, &c2.id] {
        let row = store
            .get_note(child_id)
            .expect("get child")
            .expect("child visible after link");
        assert_eq!(row.reply_id.as_deref(), Some(parent.id.as_str()));
        assert_eq!(row.reply_parent_uri, None);
    }
    assert_eq!(
        store
            .get_note(&parent.id)
            .expect("get parent")
            .expect("parent")
            .replies_count,
        2,
        "each child counted exactly once"
    );
    // Duplicate resolver jobs converge with no double count. (Drain the
    // two reply streaming jobs first so the dupe is what gets claimed.)
    assert!(worker.process_one().expect("drain stream 1"), "stream 1");
    assert!(worker.process_one().expect("drain stream 2"), "stream 2");
    store
        .enqueue_jobs(vec![
            NewJob::new_json(
                JobKind::ReplyParentResolve,
                &payload,
                "r10:dupe",
                clock.now_ms(),
            )
            .expect("dupe job encodes"),
        ])
        .expect("enqueue dupe");
    assert!(worker.process_one().expect("process dupe"), "dupe claimed");
    assert_eq!(
        store
            .get_note(&parent.id)
            .expect("get parent")
            .expect("parent")
            .replies_count,
        2,
        "duplicate resolution never double-counts"
    );
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "converged job completes instead of looping"
    );

    // Unresolvable parent (closed port): the job retries with backoff and
    // the pending entry is kept — never flattened, never exposed.
    let missing_uri = "http://127.0.0.1:1/notes/missing-parent";
    let orphan = pending_reply_note(
        &*store,
        &clock,
        &bob.id,
        "r10 orphan",
        missing_uri,
        "r10:orphan",
    );
    // Drain the orphan's streaming job so the resolver is claimed next.
    assert!(
        worker.process_one().expect("drain orphan stream"),
        "orphan stream"
    );
    store
        .enqueue_jobs(vec![
            NewJob::new_json(
                JobKind::ReplyParentResolve,
                &ReplyParentResolvePayload {
                    parent_uri: missing_uri.to_string(),
                },
                "r10:missing",
                clock.now_ms(),
            )
            .expect("missing job encodes"),
        ])
        .expect("enqueue missing");
    assert!(
        worker.process_one().expect("process missing"),
        "missing claimed"
    );
    assert_eq!(
        store.pending_count().expect("pending after retry"),
        1,
        "failed resolution reschedules (retry), it does not complete"
    );
    let still_pending = store
        .get_note_including_deleted(&orphan.id)
        .expect("get orphan")
        .expect("orphan stored");
    assert_eq!(still_pending.reply_id, None, "orphan stays unlinked");
    assert_eq!(
        still_pending.reply_parent_uri.as_deref(),
        Some(missing_uri),
        "orphan keeps its pending URI for a later parent arrival"
    );
}

/// R11: `notes/children` keeps Misskey semantics — direct replies plus
/// quote-renotes (renotes with text/files); pure renotes stay out.
#[test]
fn r11_children_semantics_unchanged() {
    let factory = RedbFactory::fresh("r11");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, services, _worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    let bob = create_user(&*store, "bob");
    let root = note_with_job(&*store, &clock, &alice.id, "r11 root", "r11:root");
    clock.advance_ms(1);
    let reply = reply_note(&*store, &clock, &bob.id, "r11 reply", &root.id, "r11:reply");
    clock.advance_ms(1);
    let mut quote_renote = CreateNoteInput::simple_public(&bob.id, "r11 quote commentary");
    quote_renote.renote_of = Some(root.id.clone());
    let quote = store
        .create_note(quote_renote, Vec::new())
        .expect("quote renote");
    clock.advance_ms(1);
    let mut pure = CreateNoteInput::simple_public(&bob.id, "x");
    pure.text = None;
    pure.renote_of = Some(root.id.clone());
    let _pure_renote = store.create_note(pure, Vec::new()).expect("pure renote");

    let kids = services
        .note_children(&root.id, Some(&alice.id), &note_page(10))
        .expect("children");
    assert_eq!(
        kids.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![quote.id.clone(), reply.id.clone()],
        "children = replies + quote-renotes, newest-first, no pure renotes"
    );
}

/// R12: `notes/conversation` stays oldest-first over the visible ancestor
/// chain — invisible ancestors are skipped but walked through — with
/// `offset`/`limit` paging and `NO_SUCH_NOTE` for unknown targets.
#[test]
fn r12_conversation_semantics_unchanged() {
    let factory = RedbFactory::fresh("r12");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, services, _worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    let bob = create_user(&*store, "bob");
    let dave = create_user(&*store, "dave");
    // Dave follows bob (so the narrowed leaf is visible to him) but not
    // alice (so the followers-only middle ancestor stays hidden).
    store
        .follow(&dave.id, &bob.id, false, None, Vec::new())
        .expect("dave follows bob");
    let root = note_with_job(&*store, &clock, &alice.id, "r12 root", "r12:root");
    clock.advance_ms(1);
    // Middle ancestor is followers-only: dave (stranger) skips it, alice
    // (author) sees the whole chain.
    let mut mid_input = CreateNoteInput::simple_public(&alice.id, "r12 mid");
    mid_input.visibility = Visibility::Followers;
    mid_input.reply_to = Some(root.id.clone());
    let mid = store.create_note(mid_input, Vec::new()).expect("mid");
    clock.advance_ms(1);
    let leaf = reply_note(&*store, &clock, &bob.id, "r12 leaf", &mid.id, "r12:leaf");

    let full = services
        .note_conversation(&leaf.id, Some(&alice.id), 10, 0)
        .expect("alice chain");
    assert_eq!(
        full.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![root.id.clone(), mid.id.clone()],
        "oldest-first visible chain"
    );
    let paged = services
        .note_conversation(&leaf.id, Some(&alice.id), 1, 1)
        .expect("offset page");
    assert_eq!(
        paged.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![mid.id.clone()],
        "offset/limit page over the chain"
    );
    let stranger = services
        .note_conversation(&leaf.id, Some(&dave.id), 10, 0)
        .expect("dave chain");
    assert_eq!(
        stranger.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        vec![root.id.clone()],
        "invisible ancestors skipped but walked through"
    );
    assert!(
        matches!(
            services.note_conversation("missing", Some(&alice.id), 10, 0),
            Err(PortError::NotFound(_))
        ),
        "unknown conversation target is NO_SUCH_NOTE"
    );
}
