use super::common::*;

// ---------------------------------------------------------------------------
// Group B — job queue semantics (generic over the store backend)
// ---------------------------------------------------------------------------

fn case_b1_lease_redelivery<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let first = note_with_job(store, clock, &alice.id, "lease me", "b1:job");
    let claimed = store
        .claim_next(500)
        .expect("claim")
        .expect("job claimable");
    assert_eq!(claimed.idempotency_key, "b1:job");
    // Never completed: advance past the 500ms lease → redelivered, same key.
    clock.advance_ms(5_000);
    let redelivered = store
        .claim_next(500)
        .expect("reclaim")
        .expect("lease expiry redelivers the job");
    assert_eq!(redelivered.id, claimed.id);
    assert_eq!(redelivered.idempotency_key, claimed.idempotency_key);
    let payload = String::from_utf8(redelivered.payload.clone()).expect("utf8");
    assert!(payload.contains(&first.id));
}

#[test]
fn b1_lease_redelivery() {
    let factory = RedbFactory::fresh("b1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_b1_lease_redelivery(&*store, &clock);
}

/// B1b (Phase 2.6 Slice 1): reclaiming an expired lease counts as an
/// attempt. Claim, advance past the lease, reclaim → `attempts == 1` and the
/// job is still actionable; the `fail()` path keeps working on top.
fn case_b1b_lease_expiry_counts_as_attempt<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    note_with_job(store, clock, &alice.id, "lease counts", "b1b:job");
    let first = store
        .claim_next(500)
        .expect("claim")
        .expect("job claimable");
    assert_eq!(first.attempts, 0, "fresh claim burns no attempt");
    assert_eq!(first.lease_expires.is_some(), true);
    // Never completed: advance past the 500ms lease → redelivered with one
    // attempt burned by the expiry itself.
    clock.advance_ms(5_000);
    let second = store
        .claim_next(500)
        .expect("reclaim")
        .expect("lease expiry redelivers the job");
    assert_eq!(second.id, first.id);
    assert_eq!(
        second.attempts, 1,
        "expired-lease reclaim counts as an attempt"
    );
    assert!(!second.failed, "job still actionable after one expiry");
    assert_eq!(store.pending_count().expect("count"), 1);
    // The `fail()` path still works on top of the lease-counted attempt:
    // `fail` releases the lease and burns one more attempt.
    let retry_at = clock.now_ms() + 10_000;
    store.fail(&second.id, retry_at).expect("fail");
    clock.advance_ms(11_000);
    let third = store
        .claim_next(60_000)
        .expect("claim")
        .expect("claimable after retry_at");
    assert_eq!(third.id, first.id);
    assert_eq!(
        third.attempts, 2,
        "fail() on top of one expiry → attempts 2"
    );
}

#[test]
fn b1b_lease_expiry_counts_as_attempt() {
    let factory = RedbFactory::fresh("b1b");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_b1b_lease_expiry_counts_as_attempt(&*store, &clock);
}

fn case_b2_no_double_claim<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    note_with_job(store, clock, &alice.id, "only once", "b2:job");
    let first = store
        .claim_next(60_000)
        .expect("claim")
        .expect("job claimable");
    assert_eq!(first.idempotency_key, "b2:job");
    assert!(
        store.claim_next(60_000).expect("second claim").is_none(),
        "job under valid lease is invisible to concurrent claim_next"
    );
}

#[test]
fn b2_no_double_claim() {
    let factory = RedbFactory::fresh("b2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_b2_no_double_claim(&*store, &clock);
}

fn case_b3_fail_retry<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    note_with_job(store, clock, &alice.id, "retry me", "b3:job");
    let claimed = store.claim_next(60_000).expect("claim").expect("job");
    let retry_at = clock.now_ms() + 10_000;
    store.fail(&claimed.id, retry_at).expect("fail");
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "failed job unclaimable before retry_at"
    );
    clock.advance_ms(5_000);
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "still unclaimable halfway to retry_at"
    );
    clock.advance_ms(6_000);
    let retried = store
        .claim_next(60_000)
        .expect("claim")
        .expect("claimable after retry_at");
    assert_eq!(retried.id, claimed.id);
    assert_eq!(retried.attempts, claimed.attempts + 1);
    assert_eq!(retried.attempts, 1);
}

#[test]
fn b3_fail_retry() {
    let factory = RedbFactory::fresh("b3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_b3_fail_retry(&*store, &clock);
}

/// B4: completed jobs never reappear across store restarts.
#[test]
fn b4_complete_is_terminal() {
    let factory = RedbFactory::fresh("b4");
    let clock = test_clock();
    let job_id = {
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        note_with_job(&*store, &clock, &alice.id, "done", "b4:job");
        let job = store.claim_next(60_000).expect("claim").expect("job");
        store.complete(&job.id).expect("complete");
        assert!(
            store.claim_next(60_000).expect("claim").is_none(),
            "completed job gone immediately"
        );
        let id = job.id.clone();
        drop(store);
        id
    };
    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "completed job never reappears after restart"
    );
    assert!(
        store.get_job(&job_id).expect("get_job").is_none(),
        "completed job row is gone"
    );
    assert_eq!(store.pending_count().expect("count"), 0);
}

fn child_b5() -> ! {
    let (_clock, store) = child_store();
    let claimed = store
        .claim_next(500)
        .expect("child claim")
        .expect("child finds the job");
    assert_eq!(claimed.idempotency_key, "b5:job");
    // Crash before complete: drop the handle (claim already committed) and park.
    drop(store);
    ready_and_park();
}

/// B5: crash-during-handler — SIGKILL between claim and complete redelivers
/// the job after lease expiry.
#[test]
fn b5_crash_during_handler() {
    if child_mode_is("b5") {
        child_b5();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("b5");
    let clock = test_clock();
    {
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        note_with_job(&*store, &clock, &alice.id, "crashy handler", "b5:job");
        drop(store);
    }
    run_crash_child("b5", "job_queue::b5_crash_during_handler", factory.path());

    // Same clock epoch as the child: advance past its 500ms lease.
    let clock2 = test_clock();
    let store = factory.reopen(clock2.clone());
    clock2.advance_ms(5_000);
    let redelivered = store
        .claim_next(60_000)
        .expect("claim")
        .expect("crashed handler's job is redelivered after lease expiry");
    assert_eq!(redelivered.idempotency_key, "b5:job");
}

fn case_b6_payload_bound<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    assert_eq!(MAX_JOB_PAYLOAD_BYTES, 65_536);
    let alice = create_user(store, "alice");
    // Direct constructor-level rejection.
    let big = vec![b'x'; MAX_JOB_PAYLOAD_BYTES + 1];
    let err = NewJob::new_json(JobKind::StreamingPublish, &big, "b6:big", clock.now_ms())
        .expect_err("payload over 64 KiB must be rejected");
    assert!(is_validation(&err), "got {err:?}");
    let direct = NewJob {
        kind: JobKind::Notify,
        payload: big,
        idempotency_key: "b6:direct".to_string(),
        not_before: clock.now_ms(),
    };
    assert!(direct.validate().is_err());
    // Enqueue-path rejection: the whole create fails and persists nothing.
    let before = list_all(store).len();
    let err = store
        .create_note(
            CreateNoteInput::simple_public(&alice.id, "big job"),
            vec![direct],
        )
        .expect_err("oversized job must fail create_note");
    assert!(is_validation(&err), "got {err:?}");
    assert_eq!(list_all(store).len(), before);
    assert_eq!(store.pending_count().expect("count"), 0);
    // Boundary: exactly 64 KiB is accepted.
    let edge = NewJob {
        kind: JobKind::StreamingPublish,
        payload: vec![b'y'; MAX_JOB_PAYLOAD_BYTES],
        idempotency_key: "b6:edge".to_string(),
        not_before: clock.now_ms(),
    };
    edge.validate().expect("exactly 64 KiB is within the cap");
}

#[test]
fn b6_payload_bound() {
    let factory = RedbFactory::fresh("b6");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_b6_payload_bound(&*store, &clock);
}

// ---------------------------------------------------------------------------
// Group B continued — terminal job failure / quarantine (Phase 2.5 Slice 5)
// ---------------------------------------------------------------------------

fn case_b7_quarantine_never_reclaimed<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    note_with_job(store, clock, &alice.id, "quarantine me", "b7:job");
    let claimed = store.claim_next(60_000).expect("claim").expect("job");
    store
        .fail_permanent(&claimed.id, "b7: poison payload")
        .expect("quarantine");
    // Even far past any lease/backoff, the job is never re-claimed.
    clock.advance_ms(3_600_000);
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "quarantined job is never re-claimed"
    );
    // ...but the row stays inspectable with attempts + reason recorded.
    let stored = store
        .get_job(&claimed.id)
        .expect("get_job")
        .expect("quarantined row stays inspectable");
    assert!(stored.failed, "failed flag set");
    assert_eq!(
        stored.fail_reason.as_deref(),
        Some("b7: poison payload"),
        "quarantine reason recorded"
    );
    assert_eq!(stored.attempts, claimed.attempts, "attempts preserved");
    // Repeat quarantine is an idempotent no-op that keeps the first reason.
    store
        .fail_permanent(&claimed.id, "b7: second reason")
        .expect("repeat quarantine is a no-op");
    let again = store
        .get_job(&claimed.id)
        .expect("get_job")
        .expect("row still there");
    assert_eq!(
        again.fail_reason.as_deref(),
        Some("b7: poison payload"),
        "first reason wins"
    );
    // `fail` on a quarantined job is a no-op success that never re-queues it.
    store
        .fail(&claimed.id, clock.now_ms())
        .expect("fail on quarantined job is a no-op");
    clock.advance_ms(3_600_000);
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "fail() must not resurrect a quarantined job"
    );
    assert_eq!(
        store.pending_count().expect("count"),
        0,
        "quarantined jobs are not pending work"
    );
}

#[test]
fn b7_quarantine_never_reclaimed() {
    let factory = RedbFactory::fresh("b7");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_b7_quarantine_never_reclaimed(&*store, &clock);
}

fn case_b8_terminal_ops_on_missing<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    // Terminal operations on missing jobs are no-op successes (as `complete`
    // already was); they add no unbounded error paths for workers.
    store
        .fail("job:missing", clock.now_ms())
        .expect("fail on missing job is a no-op success");
    store
        .fail_permanent("job:missing", "no such job")
        .expect("fail_permanent on missing job is a no-op success");
    store
        .complete("job:missing")
        .expect("complete on missing job is a no-op success");
}

#[test]
fn b8_terminal_ops_on_missing() {
    let factory = RedbFactory::fresh("b8");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_b8_terminal_ops_on_missing(&*store, &clock);
}

fn case_b9_retry_exhaustion_quarantines<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    assert_eq!(MAX_JOB_ATTEMPTS, 8, "retry budget pins the contract");
    let alice = create_user(store, "alice");
    note_with_job(store, clock, &alice.id, "exhaust me", "b9:job");
    let first = store.claim_next(60_000).expect("claim").expect("job");
    let job_id = first.id.clone();
    store.fail(&job_id, clock.now_ms()).expect("fail 1");
    for _ in 1..MAX_JOB_ATTEMPTS {
        clock.advance_ms(1);
        let claimed = store
            .claim_next(60_000)
            .expect("claim")
            .expect("job still claimable before the cap");
        assert_eq!(claimed.id, job_id);
        store.fail(&job_id, clock.now_ms()).expect("fail");
    }
    // The cap-crossing `fail` auto-quarantined instead of rescheduling.
    clock.advance_ms(3_600_000);
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "exhausted job is never re-claimed"
    );
    let stored = store
        .get_job(&job_id)
        .expect("get_job")
        .expect("exhausted row stays inspectable");
    assert!(stored.failed, "failed flag set");
    assert_eq!(
        stored.attempts, MAX_JOB_ATTEMPTS,
        "attempts stop at the cap"
    );
    assert_eq!(
        stored.fail_reason.as_deref(),
        Some("retry-exhausted"),
        "exhaustion reason recorded"
    );
}

#[test]
fn b9_retry_exhaustion_quarantines() {
    let factory = RedbFactory::fresh("b9");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_b9_retry_exhaustion_quarantines(&*store, &clock);
}

/// Claim a job only to learn its id, then release it as due immediately so a
/// later `process_one` picks it up (counting one attempt).
fn claim_id_and_release<S: ConformanceStore>(store: &S, clock: &ManualClock) -> String {
    let claimed = store.claim_next(60_000).expect("claim").expect("job");
    let id = claimed.id.clone();
    store.fail(&id, clock.now_ms()).expect("release");
    id
}

/// B10: the worker quarantines a Notify job with an unparseable payload
/// (reason recorded) instead of retrying it forever.
#[test]
fn b10_worker_quarantines_bad_notify_payload() {
    let factory = RedbFactory::fresh("b10");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, _services, worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    let bad = NewJob {
        kind: JobKind::Notify,
        payload: b"not-json{{{".to_vec(),
        idempotency_key: "b10:bad".to_string(),
        not_before: clock.now_ms(),
    };
    store
        .create_note(
            CreateNoteInput::simple_public(&alice.id, "bad notify"),
            vec![bad],
        )
        .expect("enqueue job with garbage payload");
    let job_id = claim_id_and_release(&*store, &clock);
    assert!(
        worker.process_one().expect("process"),
        "worker handled the job"
    );
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "bad-payload job is never re-claimed"
    );
    let stored = store
        .get_job(&job_id)
        .expect("get_job")
        .expect("quarantined row stays inspectable");
    assert!(stored.failed, "failed flag set");
    let reason = stored.fail_reason.clone().unwrap_or_default();
    assert!(
        reason.contains("bad-payload"),
        "quarantine reason recorded, got {reason:?}"
    );
    assert_eq!(
        store.pending_count().expect("count"),
        0,
        "quarantined jobs are not pending work"
    );
}

/// B11: the worker quarantines a FederateDeliver job whose inbox answers with
/// a permanent (4xx) failure, recording the reason.
#[test]
fn b11_worker_quarantines_permanent_federate_failure() {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    // Stub inbox: every POST gets a 400. The worker needs exactly one hit.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub inbox");
    let addr = listener.local_addr().expect("stub addr");
    std::thread::spawn(move || {
        for _ in 0..8 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(
                b"HTTP/1.1 400 Bad Request\r\ncontent-length: 11\r\nconnection: close\r\n\r\nbad request",
            );
            let _ = stream.flush();
        }
    });

    let factory = RedbFactory::fresh("b11");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, _services, worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    let bob = create_user(&*store, "bob");
    let payload = FederateDeliverPayload::Follow {
        follow_id: "b11:follow".to_string(),
        follower_id: alice.id.clone(),
        followee_id: bob.id.clone(),
        inbox_url: format!("http://{addr}/inbox"),
    };
    let job = NewJob::new_json(
        JobKind::FederateDeliver,
        &payload,
        "b11:deliver",
        clock.now_ms(),
    )
    .expect("valid follow payload");
    store
        .follow(&alice.id, &bob.id, false, None, vec![job])
        .expect("follow with deliver job");
    let job_id = claim_id_and_release(&*store, &clock);
    assert!(
        worker.process_one().expect("process"),
        "worker handled the job"
    );
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "permanently-failed job is never re-claimed"
    );
    let stored = store
        .get_job(&job_id)
        .expect("get_job")
        .expect("quarantined row stays inspectable");
    assert!(stored.failed, "failed flag set");
    let reason = stored.fail_reason.clone().unwrap_or_default();
    assert!(
        reason.contains("permanent-failure") && reason.contains("400"),
        "quarantine reason recorded, got {reason:?}"
    );
}

struct AlwaysFailSink;

impl StreamingSink for AlwaysFailSink {
    fn publish(&self, _event_type: &str, _payload: &[u8]) -> Result<(), PortError> {
        Err(PortError::StorageUnavailable("b12: sink down".to_string()))
    }
}

/// B12: retry exhaustion at the worker level quarantines after the cap for a
/// non-federate kind (StreamingPublish), with the reason recorded.
#[test]
fn b12_worker_retry_exhaustion_quarantines() {
    let factory = RedbFactory::fresh("b12");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(AlwaysFailSink);
    let (store, _services, worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    note_with_job(&*store, &clock, &alice.id, "exhaust me", "b12:job");
    // The releasing fail counts as attempt 1; the worker then retries until
    // the cap (attempts 2..=7 reschedule, the attempt-8 claim quarantines).
    let job_id = claim_id_and_release(&*store, &clock);
    let mut processed = 0;
    for _ in 0..MAX_JOB_ATTEMPTS as usize {
        clock.advance_ms(61_000);
        if !worker.process_one().expect("process") {
            break;
        }
        processed += 1;
    }
    assert_eq!(
        processed, 7,
        "worker retried until the cap, then quarantined"
    );
    assert!(
        !worker.process_one().expect("process"),
        "quarantined job is never re-claimed"
    );
    let stored = store
        .get_job(&job_id)
        .expect("get_job")
        .expect("exhausted row stays inspectable");
    assert!(stored.failed, "failed flag set");
    assert_eq!(
        stored.attempts, 7,
        "attempts stop where the worker quarantined"
    );
    assert_eq!(
        stored.fail_reason.as_deref(),
        Some("retry-exhausted"),
        "exhaustion reason recorded"
    );
    assert_eq!(
        store.pending_count().expect("count"),
        0,
        "quarantined jobs are not pending work"
    );
}

// ---------------------------------------------------------------------------
// Slice 10 — delivery reliability (Phase 2.6). Job-queue primitives are
// generic cases (FDB inherits them); worker scenarios run over redb.
// ---------------------------------------------------------------------------

fn slice10_job(clock: &ManualClock, key: &str) -> NewJob {
    NewJob {
        kind: JobKind::StreamingPublish,
        payload: br#"{"note_id":"x"}"#.to_vec(),
        idempotency_key: key.to_string(),
        not_before: clock.now_ms(),
    }
}

/// `complete_with_followups` settles the parent and inserts its followups in
/// one write; a call with any invalid followup changes nothing.
fn case_slice10_complete_with_followups<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    note_with_job(store, clock, &alice.id, "fan-out parent", "s10:parent");
    let parent = store
        .claim_next(60_000)
        .expect("claim")
        .expect("parent job");
    let parent_id = parent.id.clone();

    let bad = NewJob {
        idempotency_key: String::new(),
        ..slice10_job(clock, "s10:bad")
    };
    assert!(
        store
            .complete_with_followups(&parent_id, vec![slice10_job(clock, "s10:ok-1"), bad],)
            .is_err(),
        "an invalid followup rejects the whole call"
    );
    assert!(
        store.get_job(&parent_id).expect("get_job").is_some(),
        "a rejected completion leaves the parent row in place"
    );
    assert_eq!(
        store.pending_count().expect("count"),
        1,
        "no followup from the rejected call"
    );

    store
        .complete_with_followups(
            &parent_id,
            vec![
                slice10_job(clock, "s10:ok-1"),
                slice10_job(clock, "s10:ok-2"),
            ],
        )
        .expect("valid completion with followups");
    assert!(
        store.get_job(&parent_id).expect("get_job").is_none(),
        "parent completed"
    );
    let mut keys = HashSet::new();
    while let Some(job) = store.claim_next(60_000).expect("claim") {
        keys.insert(job.idempotency_key.clone());
    }
    assert_eq!(
        keys,
        HashSet::from(["s10:ok-1".to_string(), "s10:ok-2".to_string()]),
        "both followups committed with the completion"
    );
}

#[test]
fn slice10_complete_with_followups() {
    let factory = RedbFactory::fresh("s10-cwf");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_slice10_complete_with_followups(&*store, &clock);
}

/// `defer` releases the lease and reschedules without counting an attempt.
fn case_slice10_defer_keeps_attempts<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    note_with_job(store, clock, &alice.id, "defer me", "s10:defer");
    let claimed = store.claim_next(60_000).expect("claim").expect("job");
    let until = clock.now_ms() + 5_000;
    store.defer(&claimed.id, until).expect("defer");

    let stored = store
        .get_job(&claimed.id)
        .expect("get_job")
        .expect("deferred row kept");
    assert_eq!(stored.attempts, 0, "a deferral is not an attempt");
    assert_eq!(
        stored.not_before, until,
        "rescheduled at the requested time"
    );
    assert!(stored.lease_expires.is_none(), "lease released");
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "not claimable before the deferral ends"
    );

    clock.advance_ms(5_000);
    let again = store
        .claim_next(60_000)
        .expect("claim")
        .expect("claimable once the deferral ends");
    assert_eq!(again.id, claimed.id);
    assert_eq!(
        again.attempts, 0,
        "reclaiming a deferred job counts nothing"
    );

    // Missing jobs are a no-op success, like the other terminal operations.
    store
        .defer("job:missing", until)
        .expect("defer on a missing job is a no-op success");
}

#[test]
fn slice10_defer_keeps_attempts() {
    let factory = RedbFactory::fresh("s10-defer");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_slice10_defer_keeps_attempts(&*store, &clock);
}

/// `enqueue_jobs` inserts a batch atomically and enforces the per-call cap.
fn case_slice10_enqueue_jobs<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let batch: Vec<NewJob> = (0..3)
        .map(|i| slice10_job(clock, &format!("s10:batch-{i}")))
        .collect();
    store.enqueue_jobs(batch).expect("batch enqueue");
    assert_eq!(store.pending_count().expect("count"), 3);

    let too_many: Vec<NewJob> = (0..MAX_JOBS_PER_TXN + 1)
        .map(|i| slice10_job(clock, &format!("s10:big-{i}")))
        .collect();
    assert!(
        store.enqueue_jobs(too_many).is_err(),
        "over the per-call cap"
    );
    assert_eq!(
        store.pending_count().expect("count"),
        3,
        "a rejected batch writes nothing"
    );

    let mixed = vec![
        slice10_job(clock, "s10:mixed-ok"),
        NewJob {
            idempotency_key: " ".to_string(),
            ..slice10_job(clock, "s10:unused")
        },
    ];
    assert!(
        store.enqueue_jobs(mixed).is_err(),
        "one invalid job rejects the batch"
    );
    assert_eq!(store.pending_count().expect("count"), 3);

    let full: Vec<NewJob> = (0..MAX_JOBS_PER_TXN)
        .map(|i| slice10_job(clock, &format!("s10:full-{i}")))
        .collect();
    store
        .enqueue_jobs(full)
        .expect("a full-size batch is accepted");
    assert_eq!(store.pending_count().expect("count"), 3 + MAX_JOBS_PER_TXN);
}

#[test]
fn slice10_enqueue_jobs() {
    let factory = RedbFactory::fresh("s10-enq");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_slice10_enqueue_jobs(&*store, &clock);
}

/// Stub inbox on 127.0.0.1: answers each request with `status_line` and counts
/// the requests it receives. Serves at most `max_requests` connections.
fn slice10_stub_inbox(status_line: &'static str, max_requests: usize) -> (String, Arc<AtomicU64>) {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub inbox");
    let addr = listener.local_addr().expect("stub addr");
    let hits = Arc::new(AtomicU64::new(0));
    let counter = hits.clone();
    std::thread::spawn(move || {
        for _ in 0..max_requests {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            counter.fetch_add(1, Ordering::SeqCst);
            let resp = format!("{status_line}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });
    (format!("http://{addr}/inbox"), hits)
}

/// A fan-out over three inboxes where one answers 503: the activity job
/// completes, the two healthy inboxes are never sent to again, and exactly
/// one follow-up job (for the failed inbox) is retried.
#[test]
fn slice10_one_failing_inbox_retries_only_that_inbox() {
    let (ok1, hits1) = slice10_stub_inbox("HTTP/1.1 202 Accepted", 16);
    let (ok2, hits2) = slice10_stub_inbox("HTTP/1.1 202 Accepted", 16);
    let (bad, bad_hits) = slice10_stub_inbox("HTTP/1.1 503 Service Unavailable", 16);

    let factory = RedbFactory::fresh("s10-fan");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, services, worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    let r1 = slice10_remote_account(&*store, "r1", &ok1);
    let r2 = slice10_remote_account(&*store, "r2", &ok2);
    let r3 = slice10_remote_account(&*store, "r3", &bad);

    let mut input = CreateNoteInput::simple_public(&alice.id, "one of three fails");
    input.extra_mentions = vec![r1.id.clone(), r2.id.clone(), r3.id.clone()];
    services.create_note(input).expect("note with fan-out");

    // First pass: the activity job fans out inline and settles.
    worker.drain(16).expect("drain fan-out");
    assert_eq!(hits1.load(Ordering::SeqCst), 1, "healthy inbox 1 sent once");
    assert_eq!(hits2.load(Ordering::SeqCst), 1, "healthy inbox 2 sent once");
    assert_eq!(
        bad_hits.load(Ordering::SeqCst),
        1,
        "failing inbox sent once"
    );
    assert_eq!(
        store.pending_count().expect("count"),
        1,
        "only the failed inbox remains queued"
    );

    // The follow-up waits out the destination cooldown, then retries alone.
    clock.advance_ms(2_000);
    worker.drain(16).expect("drain follow-up");
    assert_eq!(
        hits1.load(Ordering::SeqCst),
        1,
        "healthy inbox 1 not re-sent"
    );
    assert_eq!(
        hits2.load(Ordering::SeqCst),
        1,
        "healthy inbox 2 not re-sent"
    );
    assert_eq!(
        bad_hits.load(Ordering::SeqCst),
        2,
        "only the failed inbox is retried"
    );

    // The follow-up is a one-inbox job with the job-level retry budget.
    clock.advance_ms(3_600_000);
    let retry = store
        .claim_next(60_000)
        .expect("claim")
        .expect("follow-up still queued for retry");
    assert_eq!(retry.kind, JobKind::FederateDeliver);
    assert_eq!(retry.attempts, 1, "the retried send counted one attempt");
    match serde_json::from_slice::<FederateDeliverPayload>(&retry.payload).expect("payload") {
        FederateDeliverPayload::NoteCreated { inbox_override, .. } => {
            assert_eq!(inbox_override.as_deref(), Some(bad.as_str()))
        }
        other => panic!("follow-up must be a per-inbox job, got {other:?}"),
    }
}

/// A destination in cooldown is deferred, not sent to and not counted as an
/// attempt; once the cooldown ends the same job delivers normally.
#[test]
fn slice10_cooldown_host_is_deferred_not_attempted() {
    let (inbox, hits) = slice10_stub_inbox("HTTP/1.1 202 Accepted", 4);

    let factory = RedbFactory::fresh("s10-cool");
    let clock = test_clock();
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
    let (store, _services, worker) = open_worker_stack(&factory, clock.clone(), streaming);
    let alice = create_user(&*store, "alice");
    let bob = slice10_remote_account(&*store, "bob", &inbox);

    // An earlier failure put this host in cooldown.
    let until = worker
        .delivery_tracker
        .record_failure(&inbox, clock.now_ms(), 1_000);

    let payload = FederateDeliverPayload::Follow {
        follow_id: "s10-cool:follow".to_string(),
        follower_id: alice.id.clone(),
        followee_id: bob.id.clone(),
        inbox_url: inbox.clone(),
    };
    let job = NewJob::new_json(
        JobKind::FederateDeliver,
        &payload,
        "s10-cool:deliver",
        clock.now_ms(),
    )
    .expect("follow payload");
    store
        .follow(&alice.id, &bob.id, false, None, vec![job])
        .expect("follow with deliver job");

    assert!(
        worker.process_one().expect("process"),
        "job claimed and settled"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "no send into a cooling host"
    );
    assert_eq!(
        store.pending_count().expect("count"),
        1,
        "the deferred job stays queued"
    );
    assert!(
        store.claim_next(60_000).expect("claim").is_none(),
        "not claimable before the cooldown ends"
    );

    clock.advance_ms((until - clock.now_ms()).max(0));
    let due = store
        .claim_next(60_000)
        .expect("claim")
        .expect("due when the cooldown ends");
    assert_eq!(due.attempts, 0, "deferral never counted an attempt");
    store.complete(&due.id).expect("settle probe claim");
    assert_eq!(hits.load(Ordering::SeqCst), 0, "still no send");
}
