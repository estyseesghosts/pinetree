//! Wave 6 (fan-out split) crash atomicity: `jobs/worker/dispatch.rs`
//! settles a fan-out parent with one `enqueue_jobs` per full batch plus a
//! final `complete_with_followups`, each in its own transaction. A crash
//! after batch 1 of N followed by a lease-expiry reclaim replays the same
//! batches; `enqueue_jobs` / `complete_with_followups` are idempotent on
//! `idempotency_key`, so the replay converges on one row per key instead of
//! duplicating per-inbox POSTs.
//!
//! - Restart after batch 1 of N inserts no second row for the same key:
//!   replaying batch 1 leaves `pending_count` unchanged.
//! - A full replay (both batches plus the atomic tail) yields exactly one
//!   claimable row per follow-up key and the parent completed.
//! - Within-batch repeats collapse to one row; quarantined rows do NOT
//!   suppress a same-key re-enqueue (a failed row is never claimed again).
//!
//! Fails on old code: the replayed batch inserts a second row per key, so
//! the intermediate `pending_count` is `1 + 2 * 500` instead of `1 + 500`
//! and the final count is `1700` instead of `1200`.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::ports::{JobKind, JobQueue, MAX_JOBS_PER_TXN, NewJob};
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::jobs::worker::split_followup_batches;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

fn open_store() -> (Arc<RedbStore>, Arc<ManualClock>) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir =
        std::env::temp_dir().join(format!("pinetree-wave6-fanout-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let clock: Arc<ManualClock> = Arc::new(ManualClock::new(BASE_MS));
    let store: Arc<RedbStore> =
        RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).expect("open");
    (store, clock)
}

fn follow_up(clock: &ManualClock, key: &str) -> NewJob {
    NewJob {
        kind: JobKind::FederateDeliver,
        payload: br#"{"inbox":"https://peer.example/inbox"}"#.to_vec(),
        idempotency_key: key.to_string(),
        not_before: clock.now_ms(),
    }
}

fn follow_up_key(i: usize) -> String {
    format!("wave6:fanout:{i:04}")
}

#[test]
fn wave6_fanout_restart_after_first_batch_inserts_no_duplicate_row() {
    let (store, clock) = open_store();

    store
        .enqueue_jobs(vec![follow_up(&clock, "wave6:parent")])
        .expect("parent enqueue");
    let parent_id = store
        .claim_next(60_000)
        .expect("claim parent")
        .expect("parent claimable")
        .id;

    // More than two full batches so the crash point (after batch 1) leaves
    // at least one more full batch plus the atomic tail uncommitted.
    let total = 2 * MAX_JOBS_PER_TXN + 200;
    let all_keys: Vec<String> = (0..total).map(follow_up_key).collect();
    let rebuild =
        |keys: &[String]| -> Vec<NewJob> { keys.iter().map(|k| follow_up(&clock, k)).collect() };
    let (batches, last) = split_followup_batches(rebuild(&all_keys));
    assert_eq!(
        batches.len(),
        2,
        "fixture must span two full batches plus a tail"
    );
    assert!(batches.iter().all(|b| b.len() == MAX_JOBS_PER_TXN));
    assert_eq!(last.len(), 200);

    // Batch 1 commits; the worker "crashes" before batch 2. The parent is
    // still pending (its lease expires and it is reclaimed).
    store.enqueue_jobs(batches[0].clone()).expect("batch 1");
    assert_eq!(
        store.pending_count().expect("count"),
        1 + MAX_JOBS_PER_TXN,
        "batch 1 plus the still-pending parent"
    );

    // Reclaim replays the identical batches (same derived keys) and settles
    // the parent with the atomic tail.
    store
        .enqueue_jobs(rebuild(&all_keys[..MAX_JOBS_PER_TXN]))
        .expect("replayed batch 1");
    assert_eq!(
        store.pending_count().expect("count"),
        1 + MAX_JOBS_PER_TXN,
        "restart after batch 1 of N must not insert a second row per key"
    );
    store
        .enqueue_jobs(rebuild(&all_keys[MAX_JOBS_PER_TXN..2 * MAX_JOBS_PER_TXN]))
        .expect("batch 2");
    store
        .complete_with_followups(&parent_id, last)
        .expect("atomic tail plus completion");

    assert_eq!(
        store.pending_count().expect("count"),
        total,
        "exactly one row per follow-up key with the parent gone"
    );
    assert!(
        store.get_job(&parent_id).expect("get parent").is_none(),
        "parent completed with the tail"
    );

    let mut seen = HashSet::new();
    let mut claimed = 0;
    while let Some(job) = store.claim_next(60_000).expect("drain") {
        assert_eq!(job.kind, JobKind::FederateDeliver);
        assert!(
            seen.insert(job.idempotency_key.clone()),
            "duplicate row for key {}",
            job.idempotency_key
        );
        store.complete(&job.id).expect("settle drained job");
        claimed += 1;
    }
    assert_eq!(claimed, total, "every follow-up delivered exactly once");
    assert_eq!(
        seen.len(),
        total,
        "one claimable row per follow-up key, no duplicates"
    );
    for key in &all_keys {
        assert!(seen.contains(key), "missing follow-up for key {key}");
    }
    assert_eq!(
        store.pending_count().expect("count"),
        0,
        "dedupe path keeps the maintained pending counter exact"
    );
}

#[test]
fn wave6_fanout_enqueue_dedupes_keys_but_not_quarantined_rows() {
    let (store, clock) = open_store();

    // Within-batch repeats collapse to the first occurrence.
    store
        .enqueue_jobs(vec![
            follow_up(&clock, "wave6:dup-a"),
            follow_up(&clock, "wave6:dup-b"),
            follow_up(&clock, "wave6:dup-a"),
        ])
        .expect("enqueue with repeat");
    assert_eq!(store.pending_count().expect("count"), 2);

    // Replaying a stored key is a no-op; a fresh key still inserts.
    store
        .enqueue_jobs(vec![
            follow_up(&clock, "wave6:dup-b"),
            follow_up(&clock, "wave6:dup-c"),
        ])
        .expect("replay plus fresh key");
    assert_eq!(
        store.pending_count().expect("count"),
        3,
        "stored keys are skipped, fresh keys insert"
    );

    // A quarantined row never becomes claimable again, so it must NOT
    // suppress a same-key re-enqueue — otherwise the retry would be lost.
    store
        .enqueue_jobs(vec![follow_up(&clock, "wave6:quar")])
        .expect("quarantine fixture");
    let doomed = loop {
        let job = store
            .claim_next(60_000)
            .expect("claim")
            .expect("quarantine fixture claimable");
        if job.idempotency_key == "wave6:quar" {
            break job;
        }
    };
    store
        .fail_permanent(&doomed.id, "wave6 fixture quarantine")
        .expect("quarantine");
    let pending_before = store.pending_count().expect("count");
    store
        .enqueue_jobs(vec![follow_up(&clock, "wave6:quar")])
        .expect("same-key retry after quarantine");
    assert_eq!(
        store.pending_count().expect("count"),
        pending_before + 1,
        "quarantined rows do not suppress a same-key retry"
    );
    let mut saw_retry = false;
    while let Some(job) = store.claim_next(60_000).expect("drain") {
        if job.idempotency_key == "wave6:quar" {
            assert_ne!(
                job.id, doomed.id,
                "the retry is a new row, not the quarantined one"
            );
            saw_retry = true;
        }
        store.complete(&job.id).expect("settle");
    }
    assert!(
        saw_retry,
        "same-key retry after quarantine stays deliverable"
    );
}
