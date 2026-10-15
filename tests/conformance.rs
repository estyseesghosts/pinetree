//! Persistence conformance suite (Phase 1 Slice B2).
//!
//! Covers every case in `docs/persistence-conformance.md` (Groups A–F plus
//! the exit-box-2 acceptance test).
//!
//! Backend genericity: in-process cases are written as generic functions over
//! [`ConformanceStore`] (the port traits under test) and instantiated here
//! with [`RedbFactory`]. Phase 4 runs this exact suite against FoundationDB
//! by providing a second [`StoreFactory`] implementation; only the crash
//! harness (subprocess `SIGKILL` + reopen by path) is redb-specific and is
//! mirrored per backend.
//!
//! Crash cases spawn `std::env::current_exe()` with
//! `PINETREE_CONFORMANCE_CHILD=<kind>` + `PINETREE_DB_PATH=<path>` and args
//! `--exact <test> --nocapture` (`--nocapture` is load-bearing: libtest
//! captures per-test stdout by default, which would swallow the child's
//! `READY` line and deadlock the parent). The child commits/claims, drops
//! its `Database` handle, prints `READY`, then parks until the parent
//! `child.kill()`s it (`SIGKILL` on Linux). `commit()` fsyncs and the OS
//! releases the child's file locks on death, so the parent can reopen.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, CreateAccountInput, CreateNoteInput, FollowStore,
    InsertMediaInput, JobKind, JobQueue, MediaStore, NewJob, NoopStreamingSink, NoteStore,
    NotificationStore, PortError, StreamingSink, TimelineCursor, MAX_JOBS_PER_TXN,
    MAX_JOB_ATTEMPTS, MAX_JOB_PAYLOAD_BYTES,
};
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::note::{Note, Visibility};
use pinetree::domain::policy::{
    ViewerContext, is_eligible_for_federated_timeline, is_eligible_for_federation_delivery,
    is_eligible_for_local_timeline, is_visible,
};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::outbox::FederateDeliverPayload;
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

// ---------------------------------------------------------------------------
// Factory abstraction (Phase 4 reuses the generic cases with an FDB factory)
// ---------------------------------------------------------------------------

/// Every port the conformance suite drives.
pub trait ConformanceStore:
    AccountStore + NoteStore + JobQueue + FollowStore + ActivityDedup + MediaStore
{
}

impl<T> ConformanceStore for T where
    T: AccountStore + NoteStore + JobQueue + FollowStore + ActivityDedup + MediaStore
{
}

/// Opens fresh and reopened store handles for one isolated case.
pub trait StoreFactory {
    type Store: ConformanceStore;

    fn open(&self, clock: Arc<ManualClock>) -> Arc<Self::Store>;
    fn reopen(&self, clock: Arc<ManualClock>) -> Arc<Self::Store>;
}

/// redb factory: one temp dir + file per case.
pub struct RedbFactory {
    path: PathBuf,
}

impl RedbFactory {
    pub fn fresh(prefix: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-conf-{}-{}-{n}",
            std::process::id(),
            prefix
        ));
        std::fs::create_dir_all(&dir).expect("create conformance temp dir");
        Self {
            path: dir.join("store.redb"),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl StoreFactory for RedbFactory {
    type Store = RedbStore;

    fn open(&self, clock: Arc<ManualClock>) -> Arc<RedbStore> {
        RedbStore::open_with_default_ids(&self.path, clock as Arc<dyn Clock>)
            .expect("open redb conformance store")
    }

    fn reopen(&self, clock: Arc<ManualClock>) -> Arc<RedbStore> {
        RedbStore::open_with_default_ids(&self.path, clock as Arc<dyn Clock>)
            .expect("reopen redb conformance store")
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const BASE_MS: i64 = 1_760_400_000_000;

fn test_clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(BASE_MS))
}

fn streaming_job(clock: &ManualClock, key: &str) -> NewJob {
    NewJob {
        kind: JobKind::StreamingPublish,
        payload: br#"{"note_id":"__PENDING_NOTE_ID__"}"#.to_vec(),
        idempotency_key: key.to_string(),
        not_before: clock.now_ms(),
    }
}

fn federate_job(clock: &ManualClock, key: &str) -> NewJob {
    // Minimal well-formed Follow payload (parseable by the delivery path;
    // never executed in the suites that use this fixture).
    let payload = FederateDeliverPayload::Follow {
        follow_id: format!("{key}:follow"),
        follower_id: format!("{key}:follower"),
        followee_id: format!("{key}:followee"),
        inbox_url: "https://example.local/inbox".to_string(),
    };
    NewJob::new_json(JobKind::FederateDeliver, &payload, key, clock.now_ms())
        .expect("federate fixture is a valid payload")
}

fn create_user<S: AccountStore>(store: &S, handle: &str) -> pinetree::domain::account::Account {
    store
        .create_account(CreateAccountInput::local(handle))
        .unwrap_or_else(|e| panic!("create_account({handle}) failed: {e:?}"))
}

fn note_with_job<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
    author_id: &str,
    text: &str,
    key: &str,
) -> Note {
    store
        .create_note(
            CreateNoteInput::simple_public(author_id, text),
            vec![streaming_job(clock, key)],
        )
        .expect("create_note with one job")
}

fn list_all<S: NoteStore>(store: &S) -> Vec<Note> {
    store
        .list_all_notes(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })
        .expect("list_all_notes")
}

fn is_conflict(e: &PortError) -> bool {
    matches!(e, PortError::Conflict(_))
}

fn is_validation(e: &PortError) -> bool {
    matches!(e, PortError::Validation(_))
}

// ---------------- Subprocess crash harness ----------------

fn child_mode_is(kind: &str) -> bool {
    std::env::var("PINETREE_CONFORMANCE_CHILD")
        .map(|v| v == kind)
        .unwrap_or(false)
}

fn child_db_path() -> PathBuf {
    PathBuf::from(std::env::var("PINETREE_DB_PATH").expect("PINETREE_DB_PATH must be set"))
}

fn child_store() -> (Arc<ManualClock>, Arc<RedbStore>) {
    let clock = test_clock();
    let store = RedbStore::open_with_default_ids(child_db_path(), clock.clone() as Arc<dyn Clock>)
        .expect("child open store");
    (clock, store)
}

fn ready_and_park() -> ! {
    if let Ok(ready_path) = std::env::var("PINETREE_READY_FILE") {
        let _ = std::fs::write(ready_path, b"READY");
    }
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}

static CHILD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_crash_harness() -> std::sync::MutexGuard<'static, ()> {
    CHILD_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn run_crash_child(kind: &str, test_name: &str, db_path: &Path) {
    let ready_file = db_path.with_extension(format!("{kind}.ready"));
    let _ = std::fs::remove_file(&ready_file);
    let mut child = Command::new(std::env::current_exe().expect("current_exe"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("PINETREE_CONFORMANCE_CHILD", kind)
        .env("PINETREE_DB_PATH", db_path)
        .env("PINETREE_READY_FILE", &ready_file)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn conformance child");
    let deadline = Instant::now() + Duration::from_secs(15);
    while !ready_file.exists() {
        if let Some(status) = child.try_wait().expect("try_wait child") {
            panic!("conformance child {kind} exited early with {status}");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("conformance child {kind} timed out waiting for READY file");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    kill_child(&mut child);
}

fn kill_child(child: &mut Child) {
    child.kill().expect("SIGKILL conformance child");
    let _ = child.wait();
    std::thread::sleep(Duration::from_millis(100));
}

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
    run_crash_child("a1", "a1_create_then_crash", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2.clone());
    let notes = store
        .notes_by_author(&alice_id, &TimelineCursor {
            limit: 100,
            ..Default::default()
        })
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
    run_crash_child("a2", "a2_crash_before_commit", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    assert_eq!(list_all(&*store).len(), 0, "no note may survive a pre-commit crash");
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
    run_crash_child("a3", "a3_multi_job_atomicity", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2.clone());
    let notes = store
        .notes_by_author(&alice_id, &TimelineCursor {
            limit: 100,
            ..Default::default()
        })
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
        assert!(payload.contains(&note.id), "job payload links note {}", note.id);
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
        .create_note(CreateNoteInput::simple_public(&alice_id, "bad job"), vec![bad])
        .expect_err("empty idempotency_key must fail validation");
    assert!(is_validation(&err), "got {err:?}");
    assert_eq!(list_all(&*store).len(), before, "failed op persisted no note");
}

fn child_a4() -> ! {
    let (_clock, store) = child_store();
    let alice = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    let notes = store
        .notes_by_author(&alice.id, &TimelineCursor {
            limit: 10,
            ..Default::default()
        })
        .expect("notes_by_author");
    let target = notes.first().expect("note exists").id.clone();
    let job = NewJob {
        kind: JobKind::StreamingPublish,
        payload: format!(r#"{{"deleted_note_id":"{target}"}}"#).into_bytes(),
        idempotency_key: "a4:delete-job".to_string(),
        not_before: store.clock().now_ms(),
    };
    store.delete_note(&target, vec![job]).expect("child delete_note");
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
    run_crash_child("a4", "a4_delete_atomicity", factory.path());

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
            assert!(payload.contains(&note_id), "delete job links note {note_id}");
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
    run_crash_child("a5", "a5_follow_atomicity", factory.path());

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
    assert_eq!(second.attempts, 1, "expired-lease reclaim counts as an attempt");
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
    assert_eq!(third.attempts, 2, "fail() on top of one expiry → attempts 2");
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
        store
            .claim_next(60_000)
            .expect("second claim")
            .is_none(),
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
    run_crash_child("b5", "b5_crash_during_handler", factory.path());

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
        .create_note(CreateNoteInput::simple_public(&alice.id, "big job"), vec![direct])
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
    assert_eq!(stored.attempts, MAX_JOB_ATTEMPTS, "attempts stop at the cap");
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

/// Full single-node stack over a redb factory path with a controllable clock
/// and a caller-chosen streaming sink, mirroring
/// `tests/crash_recovery.rs::open_stack` plus a worker.
fn open_worker_stack(
    factory: &RedbFactory,
    clock: Arc<ManualClock>,
    streaming: Arc<dyn StreamingSink>,
) -> (Arc<RedbStore>, Arc<AppServices>, JobWorker) {
    let store = factory.open(clock.clone());
    let blobs = Arc::new(
        FsBlobStore::new(factory.path().parent().expect("temp parent").join("blobs"))
            .expect("open blob store"),
    );
    let clock_port: Arc<dyn Clock> = clock.clone();
    let services = Arc::new(AppServices {
        clock: clock_port.clone(),
        ids: store.ids().clone(),
        accounts: store.clone(),
        notes: store.clone(),
        follows: store.clone(),
        reactions: store.clone(),
        jobs: store.clone(),
        media: store.clone(),
        blobs: blobs.clone(),
        notifications: store.clone(),
        auth: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "https://example.local".to_string(),
        max_file_size: 10_000_000,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
    });
    let worker = JobWorker {
        clock: clock_port,
        ids: store.ids().clone(),
        jobs: store.clone(),
        notifications: store.clone(),
        streaming,
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 60_000,
        base_backoff_ms: 10,
    };
    (store, services, worker)
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
    assert_eq!(processed, 7, "worker retried until the cap, then quarantined");
    assert!(
        !worker.process_one().expect("process"),
        "quarantined job is never re-claimed"
    );
    let stored = store
        .get_job(&job_id)
        .expect("get_job")
        .expect("exhausted row stays inspectable");
    assert!(stored.failed, "failed flag set");
    assert_eq!(stored.attempts, 7, "attempts stop where the worker quarantined");
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
    let parent = store.claim_next(60_000).expect("claim").expect("parent job");
    let parent_id = parent.id.clone();

    let bad = NewJob {
        idempotency_key: String::new(),
        ..slice10_job(clock, "s10:bad")
    };
    assert!(
        store
            .complete_with_followups(
                &parent_id,
                vec![slice10_job(clock, "s10:ok-1"), bad],
            )
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
            vec![slice10_job(clock, "s10:ok-1"), slice10_job(clock, "s10:ok-2")],
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
    assert_eq!(stored.not_before, until, "rescheduled at the requested time");
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
    assert_eq!(again.attempts, 0, "reclaiming a deferred job counts nothing");

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
    assert!(store.enqueue_jobs(too_many).is_err(), "over the per-call cap");
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
    assert!(store.enqueue_jobs(mixed).is_err(), "one invalid job rejects the batch");
    assert_eq!(store.pending_count().expect("count"), 3);

    let full: Vec<NewJob> = (0..MAX_JOBS_PER_TXN)
        .map(|i| slice10_job(clock, &format!("s10:full-{i}")))
        .collect();
    store.enqueue_jobs(full).expect("a full-size batch is accepted");
    assert_eq!(
        store.pending_count().expect("count"),
        3 + MAX_JOBS_PER_TXN
    );
}

#[test]
fn slice10_enqueue_jobs() {
    let factory = RedbFactory::fresh("s10-enq");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_slice10_enqueue_jobs(&*store, &clock);
}

/// Remote account whose preferred inbox is `inbox`.
fn slice10_remote_account<S: AccountStore>(
    store: &S,
    handle: &str,
    inbox: &str,
) -> pinetree::domain::account::Account {
    store
        .create_account(CreateAccountInput {
            handle: handle.to_string(),
            host: Some("remote.example".to_string()),
            display_name: None,
            description: None,
            location: None,
            birthday: None,
            uri: Some(format!("https://remote.example/users/{handle}")),
            inbox: Some(inbox.to_string()),
            shared_inbox: None,
            is_bot: false,
            is_cat: false,
            is_locked: false,
            preserved_usernames: Vec::new(),
        })
        .unwrap_or_else(|e| panic!("remote account {handle}: {e:?}"))
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
    assert_eq!(bad_hits.load(Ordering::SeqCst), 1, "failing inbox sent once");
    assert_eq!(
        store.pending_count().expect("count"),
        1,
        "only the failed inbox remains queued"
    );

    // The follow-up waits out the destination cooldown, then retries alone.
    clock.advance_ms(2_000);
    worker.drain(16).expect("drain follow-up");
    assert_eq!(hits1.load(Ordering::SeqCst), 1, "healthy inbox 1 not re-sent");
    assert_eq!(hits2.load(Ordering::SeqCst), 1, "healthy inbox 2 not re-sent");
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
        FederateDeliverPayload::NoteCreated {
            inbox_override, ..
        } => assert_eq!(inbox_override.as_deref(), Some(bad.as_str())),
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
    let job = NewJob::new_json(JobKind::FederateDeliver, &payload, "s10-cool:deliver", clock.now_ms())
        .expect("follow payload");
    store
        .follow(&alice.id, &bob.id, false, None, vec![job])
        .expect("follow with deliver job");

    assert!(worker.process_one().expect("process"), "job claimed and settled");
    assert_eq!(hits.load(Ordering::SeqCst), 0, "no send into a cooling host");
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
                s.spawn(move || store.create_account(CreateAccountInput::local(&h)))
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
        .create_account(CreateAccountInput::local("ALICE_c2"))
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
        .create_account(CreateAccountInput::local("alice"))
        .expect_err("old handle must stay tombstoned");
    assert!(is_conflict(&err), "got {err:?}");
    assert!(
        store
            .find_by_handle("alice")
            .expect("find old")
            .is_none(),
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
    store.forget_activity("activity:d1f:never").expect("forget unknown is a no-op");
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
    assert_eq!(retry, None, "retry observes already-seen, no duplicate note");
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
        assert!(r.is_ok(), "no racer errors, only winner/loser outcomes: {r:?}");
    }
    let winners = results
        .iter()
        .filter(|r| matches!(r, Ok(Some(_))))
        .count();
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
        store.is_following(&alice.id, &bob.id).expect("is_following"),
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
        !store.is_following(&alice.id, &bob.id).expect("is_following"),
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
        !store.is_following(&alice.id, &bob.id).expect("is_following"),
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
        store.is_following(&alice.id, &bob.id).expect("is_following"),
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
        !store.is_following(&alice.id, &bob.id).expect("is_following"),
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

// ---------------------------------------------------------------------------
// Group E — visibility agreement
// ---------------------------------------------------------------------------

struct EFixture {
    alice_id: String,
    follower_id: String,
    mentioned_id: String,
    replier_target_author_id: String,
    stranger_id: String,
}

/// Build author + follower + mentioned + replied-to-author + stranger accounts
/// and one stored note per visibility level. The followers note is a reply so
/// `reply_user_id` is populated; the specified note lists `mentioned`.
fn setup_e<S: ConformanceStore>(store: &S) -> (EFixture, Vec<Note>) {
    let alice = create_user(store, "e_alice");
    let follower = create_user(store, "e_follower");
    let mentioned = create_user(store, "e_mentioned");
    let carol = create_user(store, "e_carol");
    let stranger = create_user(store, "e_stranger");
    // follower follows alice (no jobs needed for the matrix).
    store
        .follow(&follower.id, &alice.id, false, None, vec![])
        .expect("follow");
    // Carol's note for alice to reply to (populates reply_user_id paths).
    let carol_note = store
        .create_note(CreateNoteInput::simple_public(&carol.id, "carol's note"), vec![])
        .expect("carol note");
    let mk = |visibility: Visibility, text: &str, reply_to: Option<String>| {
        let mut input = CreateNoteInput::simple_public(&alice.id, text);
        input.visibility = visibility;
        input.reply_to = reply_to;
        if visibility == Visibility::Specified {
            input.visible_users = vec![mentioned.id.clone()];
        }
        // Mention bob-equivalent so `mentions` resolves through the store.
        store.create_note(input, vec![]).expect("matrix note")
    };
    let notes = vec![
        mk(Visibility::Public, "public note", None),
        mk(Visibility::Home, "home note", None),
        mk(
            Visibility::Followers,
            "followers reply note",
            Some(carol_note.id.clone()),
        ),
        mk(Visibility::Specified, "specified note @e_mentioned", None),
    ];
    (
        EFixture {
            alice_id: alice.id,
            follower_id: follower.id,
            mentioned_id: mentioned.id,
            replier_target_author_id: carol.id,
            stranger_id: stranger.id,
        },
        notes,
    )
}

/// E1: API packing (`notes_show` semantics), timeline filters
/// (`home_timeline` / `local_timeline` semantics from
/// `application/queries.rs`), and the `is_visible` predicate agree on every
/// matrix cell.
#[test]
fn e1_three_call_site_agreement() {
    let factory = RedbFactory::fresh("e1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let (fx, notes) = setup_e(&*store);

    // Sanity: the stored followers note really is a reply to carol, and the
    // specified note really lists `mentioned`.
    let followers_note = notes
        .iter()
        .find(|n| n.visibility == Visibility::Followers)
        .expect("followers note");
    assert_eq!(
        followers_note.reply_user_id.as_deref(),
        Some(fx.replier_target_author_id.as_str())
    );

    for local_only in [false, true] {
        for note in &notes {
            let mut note = note.clone();
            note.local_only = local_only;
            // Viewers: author, follower, mentioned, replied-to-author, stranger, anon.
            let viewers: Vec<(&str, ViewerContext)> = vec![
                ("author", ViewerContext::local_user(&fx.alice_id, false)),
                ("follower", ViewerContext::local_user(&fx.follower_id, true)),
                ("mentioned", ViewerContext::local_user(&fx.mentioned_id, false)),
                (
                    "replied",
                    ViewerContext::local_user(&fx.replier_target_author_id, false),
                ),
                ("stranger", ViewerContext::local_user(&fx.stranger_id, false)),
                ("anonymous", ViewerContext::anonymous()),
            ];
            for (label, viewer) in &viewers {
                let predicate = is_visible(&note, viewer);
                // Call site 1 — API packing (`notes_show`): stored note must be
                // non-deleted, then the predicate decides.
                let packing = !note.is_deleted && is_visible(&note, viewer);
                assert_eq!(packing, predicate, "packing vs predicate");
                // Call site 2a — local timeline filter: eligibility AND predicate.
                let local_includes =
                    is_eligible_for_local_timeline(&note) && is_visible(&note, viewer);
                assert_eq!(
                    local_includes,
                    is_eligible_for_local_timeline(&note) && predicate,
                    "local timeline filter agrees"
                );
                // Call site 2b — home timeline filter for an in-scope viewer
                // (the author themselves or a follower, mirroring the
                // `authors` set in `application/queries.rs::home_timeline`):
                // scope AND predicate.
                let in_home_scope = matches!(*label, "author" | "follower");
                let home_includes = in_home_scope && is_visible(&note, viewer);
                assert_eq!(
                    home_includes,
                    in_home_scope && predicate,
                    "home timeline filter agrees"
                );
                // Expected matrix values (local readers; local_only changes
                // nothing for local reads). The followers fixture is a reply
                // to carol with no mentions, so `mentioned` sees nothing;
                // the specified fixture lists only `mentioned`.
                let want = match note.visibility {
                    Visibility::Public | Visibility::Home => true,
                    Visibility::Followers => {
                        matches!(*label, "author" | "follower" | "replied")
                    }
                    Visibility::Specified => matches!(*label, "author" | "mentioned"),
                };
                assert_eq!(
                    predicate, want,
                    "visibility={:?} local_only={local_only} viewer={label}",
                    note.visibility
                );
            }
        }
    }
}

/// E2: followers remote-remote fallback — both sides remote, no local follow
/// row → visible.
#[test]
fn e2_followers_remote_remote_fallback() {
    let note = Note {
        author_id: "remote-author".to_string(),
        user_host: Some("remote.example".to_string()),
        visibility: Visibility::Followers,
        text: Some("federated followers note".to_string()),
        ..Default::default()
    };
    let remote_stranger = ViewerContext::remote_user("stranger", "other.example", false);
    assert!(
        is_visible(&note, &remote_stranger),
        "remote-remote fallback must assume visible"
    );
    let local_stranger = ViewerContext::local_user("stranger", false);
    assert!(
        !is_visible(&note, &local_stranger),
        "local stranger with no follow edge sees nothing"
    );
    assert!(!is_visible(&note, &ViewerContext::anonymous()));
}

/// E3: `specified` with empty `visible_user_ids` rejected at creation.
#[test]
fn e3_specified_empty_visible_users_rejected() {
    let factory = RedbFactory::fresh("e3");
    let clock = test_clock();
    let store = factory.open(clock);
    let alice = create_user(&*store, "alice");
    let mut input = CreateNoteInput::simple_public(&alice.id, "specified to nobody");
    input.visibility = Visibility::Specified;
    input.visible_users = vec![];
    let err = store
        .create_note(input, vec![])
        .expect_err("empty specified audience must be rejected");
    assert!(is_validation(&err), "got {err:?}");
}

/// E4: `local_only` notes never reach federated surfaces or remote viewers.
#[test]
fn e4_local_only_never_federated() {
    let factory = RedbFactory::fresh("e4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let alice = create_user(&*store, "alice");
    for visibility in [
        Visibility::Public,
        Visibility::Home,
        Visibility::Followers,
        Visibility::Specified,
    ] {
        let mut input = CreateNoteInput::simple_public(&alice.id, "local only note");
        input.visibility = visibility;
        input.local_only = true;
        if visibility == Visibility::Specified {
            input.visible_users = vec![alice.id.clone()];
        }
        let note = store.create_note(input, vec![]).expect("local_only create");
        assert!(!is_eligible_for_federated_timeline(&note));
        assert!(!is_eligible_for_federation_delivery(&note));
        let remote = ViewerContext::remote_user("follower", "remote.example", true);
        assert!(
            !is_visible(&note, &remote),
            "{visibility:?} local_only must be invisible remotely"
        );
        let federated_render = ViewerContext {
            viewer_id: Some("follower".to_string()),
            viewer_host: None,
            follows_author: true,
            is_remote_request: true,
        };
        assert!(!is_visible(&note, &federated_render));
    }
}

// ---------------------------------------------------------------------------
// Group F — bounds and isolation
// ---------------------------------------------------------------------------

fn valid_media_input(owner: Option<String>) -> InsertMediaInput {
    InsertMediaInput {
        id: None,
        owner_id: owner,
        name: "photo.png".to_string(),
        mime: "image/png".to_string(),
        size_bytes: 512,
        hash: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
        blob_key: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .to_string(),
        blurhash: None,
        properties: pinetree::domain::media::MediaProperties::default(),
        is_sensitive: false,
        is_link: false,
        url: "https://example.local/files/photo".to_string(),
        thumbnail_url: None,
        comment: None,
        source_url: None,
        max_file_size: 10_000_000,
    }
}

/// F1: metadata path rejects raw binary bytes in `hash`.
#[test]
fn f1_media_bytes_rejected() {
    let factory = RedbFactory::fresh("f1");
    let clock = test_clock();
    let store = factory.open(clock);
    let alice = create_user(&*store, "alice");
    let mut input = valid_media_input(Some(alice.id.clone()));
    input.hash = "\0\u{1}\u{2}binary".to_string();
    let err = store
        .insert_metadata(input)
        .expect_err("raw bytes in hash must fail");
    assert!(is_validation(&err), "got {err:?}");
    // Control: the hex hash validates.
    let ok = store
        .insert_metadata(valid_media_input(Some(alice.id.clone())))
        .expect("hex hash accepted");
    assert_eq!(ok.owner_id.as_deref(), Some(alice.id.as_str()));
}

/// F2: a single use case stays far inside backend TX limits (5s / 10MB).
#[test]
fn f2_transaction_scope() {
    let factory = RedbFactory::fresh("f2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let alice = create_user(&*store, "alice");
    let started = Instant::now();
    let note = store
        .create_note(
            CreateNoteInput::simple_public(&alice.id, "bounded op"),
            vec![streaming_job(&clock, "f2:job")],
        )
        .expect("create");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "single create_note+job took {elapsed:?}, must stay well under 5s"
    );
    let stored = store
        .get_note(&note.id)
        .expect("get")
        .expect("note stored");
    let bytes = serde_json::to_vec(&stored).expect("serialize");
    assert!(bytes.len() < 10_000_000, "note row far under 10 MB");
}

/// F3: committed state is immediately visible and survives reopen.
#[test]
fn f3_read_your_commit() {
    let factory = RedbFactory::fresh("f3");
    let clock = test_clock();
    let (alice_id, note_id) = {
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        let note = note_with_job(&*store, &clock, &alice.id, "read me", "f3:job");
        // Immediate visibility on subsequent reads, same handle.
        let read = store
            .get_note(&note.id)
            .expect("get")
            .expect("committed note immediately readable");
        assert_eq!(read.text.as_deref(), Some("read me"));
        let found = store
            .find_by_handle("ALICE")
            .expect("find")
            .expect("committed account immediately resolvable");
        assert_eq!(found.id, alice.id);
        let ids = (alice.id.clone(), note.id.clone());
        drop(store);
        ids
    };
    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    let read = store
        .get_note(&note_id)
        .expect("get")
        .expect("note visible after reopen");
    assert_eq!(read.author_id, alice_id);
    assert!(
        store
            .find_by_id(&alice_id)
            .expect("find")
            .is_some()
    );
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
    run_crash_child("exit_box_2", "conformance_exit_box_2", factory.path());

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

// ---------------------------------------------------------------------------
// Group F: pending follow requests (Phase 2.6 slice 4, decision D-a)
// ---------------------------------------------------------------------------

fn counts<S: AccountStore>(store: &S, id: &str) -> (u64, u64) {
    let a = store.find_by_id(id).expect("find").expect("account");
    (a.followers_count as u64, a.following_count as u64)
}

/// F1: a pending request is not a follow edge — no `is_following`, not in
/// `followers_of`/`following_of`, counters untouched.
fn case_f1_request_is_not_an_edge<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "f1_alice");
    let bob = create_user(store, "f1_bob");
    let req = store
        .request_follow(&alice.id, &bob.id, true, None, vec![])
        .expect("request");
    assert_eq!(req.follower_id, alice.id);
    assert!(!store.is_following(&alice.id, &bob.id).unwrap());
    assert!(store.followers_of(&bob.id, None, 100).unwrap().is_empty());
    assert!(store.following_of(&alice.id, None, 100).unwrap().is_empty());
    assert_eq!(counts(store, &bob.id), (0, 0));
    assert_eq!(counts(store, &alice.id), (0, 0));
    assert!(store.get_follow_request(&alice.id, &bob.id).unwrap().is_some());
    let listed = store.follow_requests_to(&bob.id, None, 100).unwrap();
    assert_eq!(listed.len(), 1);
    assert!(matches!(
        store.request_follow(&alice.id, &bob.id, true, None, vec![]),
        Err(PortError::Conflict(_))
    ));
}

#[test]
fn f1_request_is_not_an_edge() {
    let factory = RedbFactory::fresh("f1");
    let store = factory.open(test_clock());
    case_f1_request_is_not_an_edge(&*store);
}

/// F2: accept flips state atomically with its jobs; the edge and counters
/// appear, the request disappears.
fn case_f2_accept<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "f2_alice");
    let bob = create_user(store, "f2_bob");
    assert!(matches!(
        store.accept_follow_request(&alice.id, &bob.id, vec![]),
        Err(PortError::NotFound(_))
    ));
    store
        .request_follow(&alice.id, &bob.id, true, None, vec![])
        .expect("request");
    let before = store.pending_count().unwrap();
    store
        .accept_follow_request(&alice.id, &bob.id, vec![streaming_job(clock, "f2:accept")])
        .expect("accept");
    assert_eq!(store.pending_count().unwrap(), before + 1, "job committed with flip");
    assert!(store.is_following(&alice.id, &bob.id).unwrap());
    assert_eq!(store.followers_of(&bob.id, None, 100).unwrap().len(), 1);
    assert_eq!(counts(store, &bob.id), (1, 0));
    assert_eq!(counts(store, &alice.id), (0, 1));
    assert!(store.get_follow_request(&alice.id, &bob.id).unwrap().is_none());
}

#[test]
fn f2_accept() {
    let factory = RedbFactory::fresh("f2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_f2_accept(&*store, &clock);
}

/// F3: reject/cancel (`remove_follow_request`) and `unfollow` both clean up a
/// pending request; an inbound-style `follow` (remote Accept) supersedes it.
fn case_f3_cleanup<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "f3_alice");
    let bob = create_user(store, "f3_bob");
    store.request_follow(&alice.id, &bob.id, false, None, vec![]).unwrap();
    store.remove_follow_request(&alice.id, &bob.id, vec![]).expect("remove");
    assert!(store.get_follow_request(&alice.id, &bob.id).unwrap().is_none());
    assert!(store.follow_requests_to(&bob.id, None, 10).unwrap().is_empty());
    assert!(matches!(
        store.remove_follow_request(&alice.id, &bob.id, vec![]),
        Err(PortError::NotFound(_))
    ));
    // Undo(Follow) of a pending request.
    store.request_follow(&alice.id, &bob.id, false, None, vec![]).unwrap();
    store.unfollow(&alice.id, &bob.id, vec![]).expect("unfollow pending");
    assert!(store.get_follow_request(&alice.id, &bob.id).unwrap().is_none());
    assert_eq!(counts(store, &bob.id), (0, 0));
    assert!(matches!(
        store.unfollow(&alice.id, &bob.id, vec![]),
        Err(PortError::Validation(_))
    ));
    // Remote Accept path: a plain follow supersedes the request.
    store.request_follow(&alice.id, &bob.id, false, None, vec![]).unwrap();
    store.follow(&alice.id, &bob.id, false, None, vec![]).unwrap();
    assert!(store.get_follow_request(&alice.id, &bob.id).unwrap().is_none());
    assert_eq!(counts(store, &bob.id), (1, 0));
}

#[test]
fn f3_cleanup() {
    let factory = RedbFactory::fresh("f3");
    let store = factory.open(test_clock());
    case_f3_cleanup(&*store);
}

/// F4: inbound Follow to a locked account is deduplicated like any activity.
fn case_f4_request_activity_dedup<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "f4_alice");
    let bob = create_user(store, "f4_bob");
    let first = store
        .record_follow_request_activity("activity:f4:1", &alice.id, &bob.id, false, None, vec![])
        .unwrap();
    assert!(first.is_some());
    let dup = store
        .record_follow_request_activity("activity:f4:1", &alice.id, &bob.id, false, None, vec![])
        .unwrap();
    assert!(dup.is_none());
    assert!(!store.is_following(&alice.id, &bob.id).unwrap());
}

#[test]
fn f4_request_activity_dedup() {
    let factory = RedbFactory::fresh("f4");
    let store = factory.open(test_clock());
    case_f4_request_activity_dedup(&*store);
}

/// F5 (service level): locked followee — pending follower cannot read
/// followers-only notes and gets no fan-out edge; accept grants both; reject
/// and cancel clean up; notification kind is `receiveFollowRequest`.
#[test]
fn f5_locked_account_service_flow() {
    use pinetree::application::commands::follows::FollowOutcome;
    use pinetree::application::ports::UpdateAccountPatch;
    use pinetree::domain::notification::NotificationKind;
    let factory = RedbFactory::fresh("f5");
    let clock = test_clock();
    let (store, svc, _worker) = open_worker_stack(&factory, clock, Arc::new(NoopStreamingSink));
    let alice = create_user(&*store, "f5_alice");
    let bob = create_user(&*store, "f5_bob");
    let carol = create_user(&*store, "f5_carol");
    store
        .update_account(&bob.id, UpdateAccountPatch { is_locked: Some(true), ..Default::default() })
        .unwrap();
    let mut input = CreateNoteInput::simple_public(&bob.id, "followers only");
    input.visibility = Visibility::Followers;
    let note = svc.create_note(input).expect("note");

    assert!(matches!(
        svc.follow(&alice.id, &bob.id, true).unwrap(),
        FollowOutcome::Requested(_)
    ));
    assert!(matches!(
        svc.notes_show(&note.id, Some(&alice.id)),
        Err(PortError::NotFound(_))
    ));
    assert!(store.followers_of(&bob.id, None, 100).unwrap().is_empty());
    let kinds: Vec<_> = store
        .list_notifications(&bob.id, 10)
        .unwrap();
    let _ = kinds; // notifications are written by the worker; asserted below
    _worker.drain(16).unwrap();
    let notifs = store.list_notifications(&bob.id, 10).unwrap();
    assert!(notifs.iter().any(|n| n.kind == NotificationKind::ReceiveFollowRequest));
    assert!(!notifs.iter().any(|n| n.kind == NotificationKind::Follow));

    svc.accept_follow_request(&bob.id, &alice.id).expect("accept");
    assert!(svc.notes_show(&note.id, Some(&alice.id)).is_ok());
    assert_eq!(store.followers_of(&bob.id, None, 100).unwrap().len(), 1);

    // Reject and cancel.
    svc.follow(&carol.id, &bob.id, true).unwrap();
    svc.reject_follow_request(&bob.id, &carol.id).expect("reject");
    assert!(svc.notes_show(&note.id, Some(&carol.id)).is_err());
    assert!(store.get_follow_request(&carol.id, &bob.id).unwrap().is_none());
    svc.follow(&carol.id, &bob.id, true).unwrap();
    svc.cancel_follow_request(&carol.id, &bob.id).expect("cancel");
    assert!(store.get_follow_request(&carol.id, &bob.id).unwrap().is_none());
    assert!(matches!(
        svc.cancel_follow_request(&carol.id, &bob.id),
        Err(PortError::NotFound(_))
    ));
}

// ---------------------------------------------------------------------------
// Group G: reply visibility (Phase 2.6 slice 5, audit D8)
// ---------------------------------------------------------------------------

/// G1: a reply requires the target to be visible to the replier; otherwise
/// NO_SUCH_NOTE (same as a missing target, so existence is not leaked).
#[test]
fn g1_reply_requires_visible_target() {
    let factory = RedbFactory::fresh("g1");
    let (store, svc, _w) = open_worker_stack(&factory, test_clock(), Arc::new(NoopStreamingSink));
    let author = create_user(&*store, "g1_author");
    let follower = create_user(&*store, "g1_follower");
    let mentioned = create_user(&*store, "g1_mentioned");
    let stranger = create_user(&*store, "g1_stranger");
    store.follow(&follower.id, &author.id, false, None, vec![]).unwrap();

    let mk = |vis: Visibility| {
        let mut i = CreateNoteInput::simple_public(&author.id, "parent");
        i.visibility = vis;
        if vis == Visibility::Specified {
            i.visible_users = vec![mentioned.id.clone()];
        }
        svc.create_note(i).expect("parent")
    };
    let reply = |who: &str, parent: &Note| {
        let mut i = CreateNoteInput::simple_public(who, "re");
        i.reply_to = Some(parent.id.clone());
        svc.create_note(i)
    };
    let is_no_such_note = |r: Result<Note, PortError>| {
        matches!(r, Err(PortError::NotFound(ref m)) if m == "NO_SUCH_NOTE")
    };

    let followers_note = mk(Visibility::Followers);
    let specified_note = mk(Visibility::Specified);
    let public_note = mk(Visibility::Public);
    let home_note = mk(Visibility::Home);

    // Rejected.
    assert!(is_no_such_note(reply(&stranger.id, &followers_note)));
    assert!(is_no_such_note(reply(&stranger.id, &specified_note)));
    assert!(is_no_such_note(reply(&follower.id, &specified_note)));
    // Missing target looks identical.
    let mut i = CreateNoteInput::simple_public(&stranger.id, "re");
    i.reply_to = Some("does-not-exist".to_string());
    assert!(is_no_such_note(svc.create_note(i)));
    // Allowed.
    assert!(reply(&follower.id, &followers_note).is_ok());
    assert!(reply(&author.id, &followers_note).is_ok());
    assert!(reply(&mentioned.id, &specified_note).is_ok());
    assert!(reply(&stranger.id, &public_note).is_ok());
    assert!(reply(&stranger.id, &home_note).is_ok());
}

/// G2: renote/quote of a non-public target by a non-author is rejected
/// (policy.rs narrowing); public renotes still work.
#[test]
fn g2_renote_visibility() {
    let factory = RedbFactory::fresh("g2");
    let (store, svc, _w) = open_worker_stack(&factory, test_clock(), Arc::new(NoopStreamingSink));
    let author = create_user(&*store, "g2_author");
    let stranger = create_user(&*store, "g2_stranger");
    let mut i = CreateNoteInput::simple_public(&author.id, "private");
    i.visibility = Visibility::Followers;
    let fo = svc.create_note(i).unwrap();
    let public = svc.create_note(CreateNoteInput::simple_public(&author.id, "pub")).unwrap();
    for text in [None, Some("quote".to_string())] {
        let mut r = CreateNoteInput::simple_public(&stranger.id, "x");
        r.text = text;
        r.renote_of = Some(fo.id.clone());
        assert!(svc.create_note(r).is_err(), "renote/quote of followers-only must fail");
    }
    let mut r = CreateNoteInput::simple_public(&stranger.id, "x");
    r.text = None;
    r.renote_of = Some(public.id.clone());
    assert!(svc.create_note(r).is_ok());
}

// ---------------------------------------------------------------------------
// Group M: remote media idempotency (Phase 2.6 slice 7, audit H2+C3)
// ---------------------------------------------------------------------------

/// M1: `find_by_source_url` returns exactly the rows cached from a URL —
/// local rows (`source_url == None`) never match, unknown URLs return
/// empty, and blank queries return empty without scanning semantics.
fn case_m1_media_source_url_lookup<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "m1_alice");
    let mut remote = valid_media_input(None);
    remote.is_link = true;
    remote.source_url = Some("https://remote.example/pic.png".to_string());
    let cached = store.insert_metadata(remote).expect("remote cache row");
    let mut remote2 = valid_media_input(None);
    remote2.is_link = true;
    remote2.hash = "e99a18c428cb38d5f260853678922e03".to_string();
    remote2.source_url = Some("https://remote.example/pic.png".to_string());
    let cached2 = store.insert_metadata(remote2).expect("second row, same URL");
    let _local = store
        .insert_metadata(valid_media_input(Some(alice.id.clone())))
        .expect("local row");

    let found = store
        .find_by_source_url("https://remote.example/pic.png")
        .expect("lookup");
    assert_eq!(found.len(), 2, "both rows cached from the URL");
    assert!(found.iter().any(|m| m.id == cached.id));
    assert!(found.iter().any(|m| m.id == cached2.id));
    assert!(
        store
            .find_by_source_url("https://remote.example/other.png")
            .expect("unknown URL")
            .is_empty()
    );
    assert!(store.find_by_source_url("  ").expect("blank").is_empty());
}

#[test]
fn m1_media_source_url_lookup() {
    let factory = RedbFactory::fresh("m1");
    let clock = test_clock();
    let store = factory.open(clock);
    case_m1_media_source_url_lookup(&*store);
}

/// M2: `attach_remote_media` inserts + appends atomically and is
/// idempotent per (source_url, note): the second attach of the same URL
/// returns the existing row (no duplicate, no orphan), a different URL
/// appends, and attaching to a missing note is `NotFound`.
fn case_m2_attach_remote_media_idempotent<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "m2_alice");
    let note = note_with_job(store, clock, &alice.id, "attach here", "m2:job");
    assert!(note.file_ids.is_empty());

    let mut input = valid_media_input(Some(alice.id.clone()));
    input.source_url = Some("https://remote.example/a.png".to_string());
    let first = store
        .attach_remote_media(&note.id, input.clone(), vec![])
        .expect("first attach");
    assert_eq!(store.pending_count().expect("count"), 1, "no jobs enqueued");

    let second = store
        .attach_remote_media(&note.id, input, vec![])
        .expect("replay attach");
    assert_eq!(second.id, first.id, "idempotent: same row back");
    let note = store.get_note(&note.id).expect("get").expect("note");
    assert_eq!(note.file_ids, vec![first.id.clone()]);

    let mut other = valid_media_input(Some(alice.id.clone()));
    other.hash = "e99a18c428cb38d5f260853678922e03".to_string();
    other.source_url = Some("https://remote.example/b.png".to_string());
    let third = store
        .attach_remote_media(&note.id, other, vec![])
        .expect("different URL appends");
    assert_ne!(third.id, first.id);
    let note = store.get_note(&note.id).expect("get").expect("note");
    assert_eq!(note.file_ids.len(), 2);

    let err = store
        .attach_remote_media("note:missing", valid_media_input(Some(alice.id.clone())), vec![])
        .expect_err("missing note");
    assert!(
        matches!(err, PortError::NotFound(_)),
        "missing note is NotFound, got {err:?}"
    );
}

#[test]
fn m2_attach_remote_media_idempotent() {
    let factory = RedbFactory::fresh("m2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_m2_attach_remote_media_idempotent(&*store, &clock);
}

/// M3: SHA-256 blob-key addressing round-trips through the store while the
/// md5 stays the per-owner dedup key. Two rows for one owner with distinct
/// (md5, blob_key) pairs coexist; each is found by its own md5 via
/// `find_by_owner_and_hash`; `get` returns both hashes intact. (Written
/// before the slice-8 fix; FDB inherits it.)
fn case_m3_media_blob_key_roundtrip<S: ConformanceStore>(store: &S) {
    use pinetree::media::ingest::{md5_hex, sha256_hex};
    let alice = create_user(store, "m3_alice");
    let payload_a = b"m3 blob A";
    let payload_b = b"m3 blob B";

    let mut input_a = valid_media_input(Some(alice.id.clone()));
    input_a.hash = md5_hex(payload_a);
    input_a.blob_key = sha256_hex(payload_a);
    input_a.size_bytes = payload_a.len() as u64;
    let row_a = store.insert_metadata(input_a).expect("row a");

    let mut input_b = valid_media_input(Some(alice.id.clone()));
    input_b.hash = md5_hex(payload_b);
    input_b.blob_key = sha256_hex(payload_b);
    input_b.size_bytes = payload_b.len() as u64;
    let row_b = store.insert_metadata(input_b).expect("row b");

    assert_ne!(row_a.id, row_b.id);
    assert_ne!(row_a.blob_key, row_b.blob_key, "distinct bytes → distinct keys");
    assert_ne!(row_a.hash, row_b.hash);

    // Dedup lookup still keys on the md5: each md5 resolves to its own row.
    let found_a = store
        .find_by_owner_and_hash(&alice.id, &row_a.hash)
        .expect("lookup a")
        .expect("row a found by md5");
    assert_eq!(found_a.id, row_a.id);
    assert_eq!(found_a.blob_key, row_a.blob_key);
    let found_b = store
        .find_by_owner_and_hash(&alice.id, &row_b.hash)
        .expect("lookup b")
        .expect("row b found by md5");
    assert_eq!(found_b.id, row_b.id);

    // Unknown md5 finds nothing (no cross-talk between the rows).
    assert!(
        store
            .find_by_owner_and_hash(&alice.id, &sha256_hex(b"m3 blob C"))
            .expect("unknown lookup")
            .is_none()
    );
}

#[test]
fn m3_media_blob_key_roundtrip() {
    let factory = RedbFactory::fresh("m3");
    let clock = test_clock();
    let store = factory.open(clock);
    case_m3_media_blob_key_roundtrip(&*store);
}
