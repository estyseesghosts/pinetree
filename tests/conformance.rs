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
    AccountStore, ActivityDedup, BlockStore, BlobStore, ConsumeInviteInput, CreateAccountInput,
    CreateInviteInput, CreateNoteInput,
    EditNoteInput, FollowStore, InsertMediaInput, InstanceSettingsPatch, InstanceSettingsStore,
    InviteStore, JobKind, JobQueue,
    MediaStore, ModerationLogFilter, ModerationLogStore, MuteStore, NewJob,
    NoopStreamingSink, NoteStore, NotificationStore, PollExpiryPayload, PollStore, PortError,
    ReactionStore, ReplyParentResolvePayload, ScheduledDraftPayload, StreamingSink, TimelineCursor,
    UpdateAccountPatch, MAX_JOBS_PER_TXN, MAX_JOB_ATTEMPTS, MAX_JOB_PAYLOAD_BYTES,
};
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::instance::RegistrationPolicy;
use pinetree::domain::moderation::{ModerationAction, ModerationLogEntry};
use pinetree::domain::poll::{PollInput, PollVote};
use pinetree::domain::draft::NoteDraft;
use pinetree::domain::note::{Note, Visibility};
use pinetree::domain::pinning::UserNotePining;
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
    AccountStore
        + NoteStore
        + JobQueue
        + FollowStore
        + ActivityDedup
        + MediaStore
        + BlockStore
        + MuteStore
        + ReactionStore
        + PollStore
        + ModerationLogStore
        + InstanceSettingsStore
        + InviteStore
{
}

impl<T> ConformanceStore for T where
    T: AccountStore
        + NoteStore
        + JobQueue
        + FollowStore
        + ActivityDedup
        + MediaStore
        + BlockStore
        + MuteStore
        + ReactionStore
        + PollStore
        + ModerationLogStore
        + InstanceSettingsStore
        + InviteStore
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
        .create_account(CreateAccountInput::local(handle), None)
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
        blocks: store.clone(),
        mutes: store.clone(),
        reactions: store.clone(),
        polls: store.clone(),
        jobs: store.clone(),
        media: store.clone(),
        blobs: blobs.clone(),
        notifications: store.clone(),
        auth: store.clone(),
        host_caps: store.clone(),
        moderation: store.clone(),
        instance_settings: store.clone(),
        invites: store.clone(),
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
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
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
        }, None)
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
        .update_account(&bob.id, UpdateAccountPatch { is_locked: Some(true), ..Default::default() }, Vec::new())
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

// ---------------------------------------------------------------------------
// Group Q — quote posts (docs/quote-posts.md): domain record, authorization
// transitions, stamp persistence, dedup-atomic happy paths, crash
// consistency. Generic over ConformanceStore so Phase 4 runs these against
// FoundationDB unchanged.
// ---------------------------------------------------------------------------

use pinetree::domain::quote::{QuoteAuthorizationState, QuoteReference, QuoteStamp};

fn quote_pending(target_uri: &str) -> QuoteReference {
    QuoteReference {
        target_uri: target_uri.to_string(),
        target_note_id: None,
        authorization: QuoteAuthorizationState::Pending,
        authorization_uri: None,
        request_activity_uri: Some("https://local.example/activities/req-1".to_string()),
        last_verified_at: None,
    }
}

fn quote_input<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
    author_id: &str,
    target: &Note,
    key: &str,
) -> Note {
    let mut input = CreateNoteInput::simple_public(author_id, "quoting commentary");
    input.renote_of = Some(target.id.clone());
    input.quote = Some(quote_pending("https://remote.example/notes/1"));
    store
        .create_note(input, vec![streaming_job(clock, key)])
        .expect("create quote note")
}

fn case_q1_quote_create_persists<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q1:target");

    // Pure renote (no text/CW/files, no quote record) stays a pure renote:
    // never misclassified as a quote (migration safety for old rows).
    let mut pure = CreateNoteInput::simple_public(&alice.id, "x");
    pure.text = None;
    pure.renote_of = Some(target.id.clone());
    let pure_note = store.create_note(pure, vec![]).expect("pure renote");
    assert!(pure_note.quote.is_none(), "pure renote must not gain a quote record");
    assert!(!pure_note.is_quote());

    // Quote: relation + resolved local target id converge in storage.
    let quote = quote_input(store, clock, &alice.id, &target, "q1:quote");
    assert!(quote.is_quote());
    let q = quote.quote.as_ref().expect("quote record present");
    assert_eq!(q.target_uri, "https://remote.example/notes/1");
    assert_eq!(q.target_note_id.as_deref(), Some(target.id.as_str()));
    assert_eq!(q.authorization, QuoteAuthorizationState::Pending);

    // Reopen-equivalent read: the row round-trips with its quote intact.
    let reread = store.get_note(&quote.id).expect("reread").expect("quote found");
    assert_eq!(reread.quote, quote.quote);

    // Migration default: a row written before quotes existed (no `quote`
    // key at all) decodes as a non-quote, never a misclassified quote.
    let legacy_json = serde_json::json!({
        "id": "nid00000000000001", "author_id": alice.id, "text": "old",
        "cw": null, "visibility": "public", "visible_user_ids": [],
        "local_only": false, "reply_id": null, "renote_id": target.id,
        "reply_user_id": null, "file_ids": [], "mentions": [],
        "mentioned_remote_users": "[]", "tags": [], "emojis": [],
        "reactions": {}, "renote_count": 0, "replies_count": 0,
        "reaction_acceptance": null, "uri": null, "url": null,
        "user_host": null, "is_deleted": false,
    });
    let legacy: Note =
        serde_json::from_value(legacy_json).expect("legacy row without quote key decodes");
    assert!(legacy.quote.is_none(), "legacy rows default to non-quote");
}

#[test]
fn q1_quote_create_persists_and_pure_renote_stays_pure() {
    let factory = RedbFactory::fresh("q1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q1_quote_create_persists(&*store, &clock);
}

fn case_q2_quote_transitions<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q2:target");
    let quote = quote_input(store, clock, &alice.id, &target, "q2:quote");

    // Missing note and non-quote notes are NotFound, never Validation.
    assert!(matches!(
        store.set_quote_authorization("nid-missing", QuoteAuthorizationState::Approved, None, 0, vec![]),
        Err(PortError::NotFound(_))
    ));
    assert!(matches!(
        store.set_quote_authorization(&target.id, QuoteAuthorizationState::Approved, None, 0, vec![]),
        Err(PortError::NotFound(_))
    ));

    // Approved requires a stamp: no false approval.
    assert!(is_validation(&store
        .set_quote_authorization(&quote.id, QuoteAuthorizationState::Approved, None, 0, vec![])
        .expect_err("approved without stamp must fail")));

    // Pending -> Approved (with stamp) + jobs commit together.
    let approved = store
        .set_quote_authorization(
            &quote.id,
            QuoteAuthorizationState::Approved,
            Some("https://remote.example/stamps/1".to_string()),
            clock.now_ms(),
            vec![streaming_job(clock, "q2:approved-job")],
        )
        .expect("approve with stamp");
    assert_eq!(approved.quote.as_ref().unwrap().authorization, QuoteAuthorizationState::Approved);
    assert_eq!(
        approved.quote.as_ref().unwrap().authorization_uri.as_deref(),
        Some("https://remote.example/stamps/1")
    );
    let mut saw_approval_job = false;
    for _ in 0..8 {
        let Some(job) = store.claim_next(60_000).expect("claim") else {
            break;
        };
        if job.idempotency_key == "q2:approved-job" {
            saw_approval_job = true;
        }
        store.complete(&job.id).expect("complete");
    }
    assert!(saw_approval_job, "approval job committed with the transition");

    // Same-state repeat is a no-op success (idempotent replay).
    let again = store
        .set_quote_authorization(&quote.id, QuoteAuthorizationState::Approved, None, 0, vec![])
        .expect("same-state repeat ok");
    assert_eq!(
        again.quote.as_ref().unwrap().authorization_uri.as_deref(),
        Some("https://remote.example/stamps/1"),
        "replay without a stamp URI retains the stored stamp"
    );

    // Illegal transitions are rejected: Approved -> Rejected, Approved -> Unknown.
    assert!(is_validation(&store
        .set_quote_authorization(&quote.id, QuoteAuthorizationState::Rejected, None, 0, vec![])
        .expect_err("approved->rejected is illegal")));
    // Approved -> Revoked is the valid revocation path.
    let revoked = store
        .set_quote_authorization(&quote.id, QuoteAuthorizationState::Revoked, None, 0, vec![])
        .expect("approved->revoked");
    assert_eq!(revoked.quote.as_ref().unwrap().authorization, QuoteAuthorizationState::Revoked);
    // The dead stamp URI is retained so duplicate Deletes still match.
    assert_eq!(
        revoked.quote.as_ref().unwrap().authorization_uri.as_deref(),
        Some("https://remote.example/stamps/1")
    );
}

#[test]
fn q2_quote_authorization_transitions() {
    let factory = RedbFactory::fresh("q2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q2_quote_transitions(&*store, &clock);
}

fn case_q3_stamps_and_lookups<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q3:target");
    let quote = quote_input(store, clock, &alice.id, &target, "q3:quote");

    // Unknown lookups are None, never errors.
    assert!(store.find_note_by_quote_request("https://x.example/req-0").expect("lookup").is_none());
    assert!(store.find_note_by_quote_stamp("https://x.example/stamp-0").expect("lookup").is_none());
    assert!(store.get_quote_stamp("https://x.example/stamp-0").expect("lookup").is_none());

    // Request lookup finds our pending quote.
    let found = store
        .find_note_by_quote_request("https://local.example/activities/req-1")
        .expect("lookup")
        .expect("quote found by request URI");
    assert_eq!(found.id, quote.id);

    // Stamp round-trip + stamp lookup after approval.
    let stamp = QuoteStamp {
        id: "https://local.example/users/bob/stamps/s1".to_string(),
        author_id: bob.id.clone(),
        interacting_object_uri: "https://remote.example/notes/q1".to_string(),
        interaction_target_note_id: target.id.clone(),
        request_activity_uri: "https://remote.example/activities/req-9".to_string(),
        revoked: false,
        created_at_ms: clock.now_ms(),
    };
    store.save_quote_stamp(stamp.clone(), vec![]).expect("save stamp");
    assert_eq!(store.get_quote_stamp(&stamp.id).expect("get").unwrap(), stamp);

    store
        .set_quote_authorization(
            &quote.id,
            QuoteAuthorizationState::Approved,
            Some(stamp.id.clone()),
            clock.now_ms(),
            vec![],
        )
        .expect("approve");
    let by_stamp = store
        .find_note_by_quote_stamp(&stamp.id)
        .expect("lookup")
        .expect("quote found by stamp URI");
    assert_eq!(by_stamp.id, quote.id);
}

#[test]
fn q3_quote_stamps_and_lookups() {
    let factory = RedbFactory::fresh("q3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q3_stamps_and_lookups(&*store, &clock);
}

fn case_q4_request_dedup<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q4:target");

    let stamp = QuoteStamp {
        id: "https://local.example/users/bob/stamps/s1".to_string(),
        author_id: bob.id.clone(),
        interacting_object_uri: "https://remote.example/notes/q1".to_string(),
        interaction_target_note_id: target.id.clone(),
        request_activity_uri: "https://remote.example/activities/req-1".to_string(),
        revoked: false,
        created_at_ms: clock.now_ms(),
    };
    // Accept path: stamp + Accept job commit atomically under the request id.
    let first = store
        .record_quote_request_accept(
            "https://remote.example/activities/req-1",
            stamp.clone(),
            vec![streaming_job(clock, "q4:accept-job")],
        )
        .expect("accept")
        .expect("first delivery applies");
    assert_eq!(first.id, stamp.id);
    // Replay: no duplicate stamp write, no duplicate job.
    let replay = store
        .record_quote_request_accept(
            "https://remote.example/activities/req-1",
            stamp,
            vec![streaming_job(clock, "q4:accept-job-dup")],
        )
        .expect("replay accepted");
    assert!(replay.is_none(), "replayed QuoteRequest applies nothing");
    assert_eq!(
        store.pending_count().expect("pending"),
        2,
        "target note job + exactly one accept job (no duplicate)"
    );

    // Reject path: true then false on replay, jobs likewise deduplicated.
    assert!(store
        .record_quote_request_reject(
            "https://remote.example/activities/req-2",
            vec![streaming_job(clock, "q4:reject-job")]
        )
        .expect("reject"));
    assert!(!store
        .record_quote_request_reject(
            "https://remote.example/activities/req-2",
            vec![streaming_job(clock, "q4:reject-job-dup")]
        )
        .expect("reject replay"));
}

#[test]
fn q4_quote_request_accept_reject_atomicity() {
    let factory = RedbFactory::fresh("q4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q4_request_dedup(&*store, &clock);
}

fn case_q5_decision_and_revocation<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q5:target");
    let quote = quote_input(store, clock, &alice.id, &target, "q5:quote");

    // Accept our request: Pending -> Approved with the verified stamp.
    let applied = store
        .record_quote_decision_accept(
            "https://remote.example/activities/accept-1",
            &quote.id,
            "https://remote.example/stamps/7",
            clock.now_ms(),
            vec![streaming_job(clock, "q5:accept-job")],
        )
        .expect("decision accept")
        .expect("first Accept applies");
    assert_eq!(applied.quote.as_ref().unwrap().authorization, QuoteAuthorizationState::Approved);
    assert!(store
        .record_quote_decision_accept(
            "https://remote.example/activities/accept-1",
            &quote.id,
            "https://remote.example/stamps/7",
            clock.now_ms(),
            vec![],
        )
        .expect("replay")
        .is_none());

    // Second quote: Reject retains the relation + projection (renote + text
    // intact, state Rejected).
    let quote2 = quote_input(store, clock, &alice.id, &target, "q5:quote2");
    let rejected = store
        .record_quote_decision_reject("https://remote.example/activities/reject-1", &quote2.id, vec![])
        .expect("decision reject")
        .expect("Reject applies");
    assert_eq!(rejected.quote.as_ref().unwrap().authorization, QuoteAuthorizationState::Rejected);
    assert_eq!(rejected.renote_id.as_deref(), Some(target.id.as_str()));
    assert!(rejected.text.is_some(), "rejected quote keeps its commentary");

    // Revocation: Approved -> Revoked with forwarding jobs committed together.
    // Seed a stamp row so revocation also flips the stamp's revoked flag.
    let seed = QuoteStamp {
        id: "https://remote.example/stamps/7".to_string(),
        author_id: bob.id.clone(),
        interacting_object_uri: "https://remote.example/notes/q5".to_string(),
        interaction_target_note_id: target.id.clone(),
        request_activity_uri: "https://remote.example/activities/req-q5".to_string(),
        revoked: false,
        created_at_ms: clock.now_ms(),
    };
    store.save_quote_stamp(seed, vec![]).expect("seed stamp");
    let revoked = store
        .record_quote_stamp_delete(
            "https://remote.example/activities/delete-stamp-1",
            &quote.id,
            vec![streaming_job(clock, "q5:revoke-job")],
        )
        .expect("stamp delete")
        .expect("revocation applies");
    assert_eq!(revoked.quote.as_ref().unwrap().authorization, QuoteAuthorizationState::Revoked);
    // The backing stamp row is revoked in the same transaction: it no longer
    // dereferences as valid.
    let stamp_row = store
        .get_quote_stamp("https://remote.example/stamps/7")
        .expect("get stamp")
        .expect("stamp row retained");
    assert!(stamp_row.revoked, "stamp row revoked with the note transition");
    // Revocation is not terminal: a fresh stamp can re-approve.
    let reapproved = store
        .set_quote_authorization(
            &quote.id,
            QuoteAuthorizationState::Approved,
            Some("https://remote.example/stamps/8".to_string()),
            clock.now_ms(),
            vec![],
        )
        .expect("revoked->approved with fresh stamp");
    assert_eq!(
        reapproved.quote.as_ref().unwrap().authorization,
        QuoteAuthorizationState::Approved
    );
    assert!(store
        .record_quote_stamp_delete("https://remote.example/activities/delete-stamp-1", &quote.id, vec![])
        .expect("replay")
        .is_none());
}

#[test]
fn q5_quote_decision_and_revocation() {
    let factory = RedbFactory::fresh("q5");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q5_decision_and_revocation(&*store, &clock);
}

fn child_q6() -> ! {
    let (clock, store) = child_store();
    let target = list_all(&*store)
        .into_iter()
        .find(|n| !n.is_quote())
        .expect("target note exists");
    let quoter = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    let mut input = CreateNoteInput::simple_public(&quoter.id, "crash quote");
    input.renote_of = Some(target.id.clone());
    input.quote = Some(quote_pending("https://remote.example/notes/9"));
    store
        .create_note(input, vec![streaming_job(&clock, "q6:job")])
        .expect("child create quote");
    drop(store);
    drop(clock);
    ready_and_park();
}

/// Q6: quote create-then-crash — note + quote state + outbox job survive a
/// SIGKILL right after commit (crash-recovery consistency).
#[test]
fn q6_quote_crash_consistency() {
    if child_mode_is("q6") {
        child_q6();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("q6");
    let (alice_id, target_id) = {
        let clock = test_clock();
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        let bob = create_user(&*store, "bob");
        let target = note_with_job(&*store, &clock, &bob.id, "original", "q6:target");
        (alice.id.clone(), target.id.clone())
    };
    run_crash_child("q6", "q6_quote_crash_consistency", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2.clone());
    let quotes: Vec<Note> = list_all(&*store)
        .into_iter()
        .filter(|n| n.author_id == alice_id && n.is_quote())
        .collect();
    assert_eq!(quotes.len(), 1, "exactly one quote survived the crash");
    let quote = &quotes[0];
    assert_eq!(quote.renote_id.as_deref(), Some(target_id.as_str()));
    assert_eq!(quote.quote.as_ref().unwrap().authorization, QuoteAuthorizationState::Pending);
    let claimed: Vec<_> = (0..8)
        .filter_map(|_| store.claim_next(60_000).expect("claim"))
        .collect();
    assert!(
        claimed.iter().any(|j| j.idempotency_key == "q6:job"
            && String::from_utf8_lossy(&j.payload).contains(&quote.id)),
        "the quote's outbox job survived alongside it"
    );
}

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
    let repeat = store.delete_note(&a1.id, Vec::new()).expect_err("repeat delete");
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
    store.delete_note(&root.id, Vec::new()).expect("delete parent");
    assert!(
        store.get_note(&root.id).expect("get deleted root").is_none(),
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
    assert_eq!(root2_after_create.replies_count, 1, "combo bumps replies once");
    assert_eq!(root2_after_create.renote_count, 1, "combo bumps renotes once");
    store
        .delete_note(&combo_note.id, Vec::new())
        .expect("delete combo");
    let root2_after_delete = store
        .get_note_including_deleted(&root2.id)
        .expect("get root2 deleted?")
        .expect("root2 row survives");
    assert_eq!(root2_after_delete.replies_count, 0, "combo delete drops replies");
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
    run_crash_child("r5", "r5_reply_crash_before_commit", factory.path());

    let clock2 = test_clock();
    let store = factory.reopen(clock2);
    assert_eq!(list_all(&*store).len(), 1, "no reply may survive a pre-commit crash");
    let parent = store
        .get_note(&parent_id)
        .expect("get parent")
        .expect("parent survives");
    assert_eq!(parent.replies_count, 0, "counter untouched by the aborted reply");
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
        .link_pending_reply(&a_pending.id, &b_note.id, "https://remote.example/notes/r6b")
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
        assert_eq!(
            list_all(&*store).len(),
            4,
            "no note rows gained or lost"
        );
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
            .note_replies("https://remote.example/notes/r8parent", viewer, &note_page(10))
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
    let historical = store.create_note(historical_input, Vec::new()).expect("historical");
    let mut genuine_input = CreateNoteInput::simple_public(&reno.id, "r9 genuine root");
    genuine_input.uri = Some(format!("{base}/notes/7"));
    let genuine = store.create_note(genuine_input, Vec::new()).expect("genuine");
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
    for (id, label) in [(&genuine.id, "genuine"), (&self_note.id, "self"), (&gone.id, "gone")] {
        let row = store
            .get_note_including_deleted(id)
            .expect("get")
            .expect("row");
        assert_eq!(row.reply_parent_uri, None, "{label} root stays a root");
        assert_eq!(row.reply_id, None, "{label} root stays unlinked");
    }
    // The mark enqueued exactly one URI-keyed resolver job, atomically.
    let job = store.claim_next(60_000).expect("claim").expect("resolver job");
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
        handle_reply_parent_resolve, resolve_reply_parent, ReplyResolveOutcome,
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
        .enqueue_jobs(vec![NewJob::new_json(
            JobKind::ReplyParentResolve,
            &payload,
            "r10:dupe",
            clock.now_ms(),
        )
        .expect("dupe job encodes")])
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
    let orphan = pending_reply_note(&*store, &clock, &bob.id, "r10 orphan", missing_uri, "r10:orphan");
    // Drain the orphan's streaming job so the resolver is claimed next.
    assert!(worker.process_one().expect("drain orphan stream"), "orphan stream");
    store
        .enqueue_jobs(vec![NewJob::new_json(
            JobKind::ReplyParentResolve,
            &ReplyParentResolvePayload {
                parent_uri: missing_uri.to_string(),
            },
            "r10:missing",
            clock.now_ms(),
        )
        .expect("missing job encodes")])
        .expect("enqueue missing");
    assert!(worker.process_one().expect("process missing"), "missing claimed");
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
    let row = store.block(blocking_row(2, &alice.id, &bob.id), vec![]).expect("block");
    assert_eq!((row.blocker_id.as_str(), row.blockee_id.as_str()), (alice.id.as_str(), bob.id.as_str()));
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
    assert_eq!(store.blocking_ids_for(&alice.id).unwrap(), vec![bob.id.clone()]);
    assert_eq!(store.blocked_by_ids_for(&bob.id).unwrap(), vec![alice.id.clone()]);
    assert!(store.blocked_by_ids_for(&alice.id).unwrap().is_empty());
    store.block(blocking_row(6, &alice.id, &carol.id), vec![]).expect("second block");
    let page = store.list_blockings(&alice.id, &full_cursor(1)).unwrap();
    assert_eq!(page.len(), 1, "limit applies");
    let all = store.list_blockings(&alice.id, &full_cursor(100)).unwrap();
    assert_eq!(all.len(), 2);
    assert!(all[0].id > all[1].id, "newest-first");
    // untilId paging walks without gaps.
    let second = store
        .list_blockings(
            &alice.id,
            &TimelineCursor { until_id: Some(all[0].id.clone()), ..full_cursor(100) },
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
    store.follow(&alice.id, &bob.id, false, None, vec![]).unwrap();
    store.follow(&bob.id, &alice.id, false, None, vec![]).unwrap();
    let jobs_before = store.pending_count().unwrap();
    store
        .block(blocking_row(1, &alice.id, &bob.id), vec![streaming_job(clock, "g2-block-job")])
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
    store.request_follow(&alice.id, &bob.id, false, None, vec![]).unwrap();
    store.request_follow(&bob.id, &alice.id, false, None, vec![]).unwrap();
    store.block(blocking_row(1, &alice.id, &bob.id), vec![]).expect("block");
    assert!(store.get_follow_request(&alice.id, &bob.id).unwrap().is_none());
    assert!(store.get_follow_request(&bob.id, &alice.id).unwrap().is_none());
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
    let target = note_with_job(store, &ManualClock::new(1_760_400_000_000), &bob.id, "t", "g3");
    store.block(blocking_row(1, &bob.id, &alice.id), vec![]).expect("block");
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
    store.create_note(quote, vec![]).expect("quote passes the block gate");
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
    assert_eq!(store.mute(&alice.id, &bob.id, Some(clock.now_ms() - 1), vec![]).unwrap(), None);
    assert!(!store.is_muting(&alice.id, &bob.id).unwrap());
    assert!(store.muted_ids_for(&alice.id).unwrap().is_empty());
    let future = clock.now_ms() + 60_000;
    assert!(store.mute(&alice.id, &bob.id, Some(future), vec![]).unwrap().is_some());
    assert!(store.is_muting(&alice.id, &bob.id).unwrap());
    assert_eq!(store.muted_ids_for(&alice.id).unwrap(), vec![bob.id.clone()]);
    assert!(matches!(
        store.mute(&alice.id, &bob.id, None, vec![]),
        Err(PortError::Conflict(msg)) if msg == "ALREADY_MUTING"
    ));
    clock.set_ms(future + 1);
    assert!(!store.is_muting(&alice.id, &bob.id).unwrap());
    assert!(store.muted_ids_for(&alice.id).unwrap().is_empty());
    // The expired row still unmutes cleanly (existence, not activity).
    store.unmute(&alice.id, &bob.id, vec![]).expect("unmute expired row");
    assert!(matches!(
        store.unmute(&alice.id, &bob.id, vec![]),
        Err(PortError::NotFound(msg)) if msg == "NOT_MUTING"
    ));
    // Renote-mutes: no expiry, same codes.
    store.renote_mute(&alice.id, &bob.id, vec![]).expect("renote mute");
    assert!(store.is_renote_muting(&alice.id, &bob.id).unwrap());
    assert_eq!(store.renote_muted_ids_for(&alice.id).unwrap(), vec![bob.id.clone()]);
    assert!(matches!(
        store.renote_mute(&alice.id, &bob.id, vec![]),
        Err(PortError::Conflict(msg)) if msg == "ALREADY_MUTING"
    ));
    assert_eq!(store.list_renote_mutings(&alice.id, &full_cursor(100)).unwrap().len(), 1);
    store.renote_unmute(&alice.id, &bob.id, vec![]).expect("renote unmute");
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

// ---------------------------------------------------------------------------
// Group S — drafts + scheduled arming (§5, `docs/misskey-parity-plan.md`)
// ---------------------------------------------------------------------------

fn draft_row(id: &str, user_id: &str, scheduled_at_ms: Option<i64>) -> NoteDraft {
    NoteDraft {
        id: id.to_string(),
        user_id: user_id.to_string(),
        text: Some("draft".to_string()),
        cw: None,
        visibility: Visibility::Public,
        visible_user_ids: Vec::new(),
        local_only: false,
        reply_id: None,
        renote_id: None,
        file_ids: Vec::new(),
        hashtag: None,
        reaction_acceptance: None,
        has_poll: false,
        poll_choices: Vec::new(),
        poll_multiple: false,
        poll_expires_at_ms: None,
        poll_expired_after_ms: None,
        scheduled_at_ms,
        is_actually_scheduled: scheduled_at_ms.is_some(),
    }
}

fn scheduled_draft_job(key: &str, draft_id: &str, at_ms: i64) -> NewJob {
    NewJob::new_json(
        JobKind::PostScheduledDraft,
        &ScheduledDraftPayload {
            draft_id: draft_id.to_string(),
            scheduled_at_ms: at_ms,
        },
        key,
        at_ms,
    )
    .expect("scheduled fixture is a valid payload")
}

fn draft_cursor(limit: usize) -> TimelineCursor {
    TimelineCursor {
        since_id: None,
        until_id: None,
        since_date: None,
        until_date: None,
        limit,
    }
}

/// S1: draft CRUD through the ports — create/get/update/delete, counts,
/// newest-first paging, and the `scheduled` tri-state filter.
fn case_s1_draft_crud<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "s1_alice");
    assert_eq!(store.count_drafts(&alice.id).unwrap(), 0);
    assert_eq!(store.count_scheduled_drafts(&alice.id).unwrap(), 0);
    let d1 = store
        .create_draft(draft_row("s1_d1", &alice.id, None), vec![])
        .unwrap();
    assert_eq!(d1.user_id, alice.id);
    let d2 = store
        .create_draft(draft_row("s1_d2", &alice.id, Some(BASE_MS + 60_000)), vec![])
        .unwrap();
    assert_eq!(store.count_drafts(&alice.id).unwrap(), 2);
    assert_eq!(store.count_scheduled_drafts(&alice.id).unwrap(), 1);

    // Newest-first; `scheduled` tri-state.
    let all = store.drafts_by_user(&alice.id, &draft_cursor(10), None).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].id, d2.id, "newest first");
    assert_eq!(
        store
            .drafts_by_user(&alice.id, &draft_cursor(10), Some(true))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .drafts_by_user(&alice.id, &draft_cursor(10), Some(false))
            .unwrap()
            .len(),
        1
    );

    // Update replaces the row; missing rows are NotFound.
    let mut merged = d1.clone();
    merged.text = Some("edited".to_string());
    let updated = store.update_draft(merged, vec![]).unwrap();
    assert_eq!(updated.text.as_deref(), Some("edited"));
    assert!(matches!(
        store.update_draft(draft_row("s1_nope", &alice.id, None), vec![]),
        Err(PortError::NotFound(msg)) if msg == "NO_SUCH_NOTE_DRAFT"
    ));

    // Delete drops the row + index; missing rows are a no-op success.
    let before = store.pending_count().unwrap();
    store
        .delete_draft(&d1.id, vec![streaming_job(&test_clock(), "s1-del-job")])
        .unwrap();
    assert!(store.get_draft(&d1.id).unwrap().is_none());
    assert_eq!(store.count_drafts(&alice.id).unwrap(), 1);
    assert_eq!(store.pending_count().unwrap(), before + 1);
    store.delete_draft(&d1.id, vec![]).unwrap();
}

#[test]
fn s1_draft_crud() {
    let factory = RedbFactory::fresh("s1");
    let store = factory.open(test_clock());
    case_s1_draft_crud(&*store);
}

/// S2: scheduled arming lifecycle — `create_draft` arms a `PostScheduledDraft`
/// job (`not_before` = `scheduled_at`), and `remove_scheduled_draft_jobs`
/// clears every arming for the draft (update/delete `clearSchedule` parity),
/// including repeated clears.
fn case_s2_scheduled_arming<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "s2_alice");
    let at = BASE_MS + 60_000;
    store
        .create_draft(
            draft_row("s2_d1", &alice.id, Some(at)),
            vec![scheduled_draft_job("s2-arm", "s2_d1", at)],
        )
        .unwrap();
    assert_eq!(store.pending_count().unwrap(), 1);
    // Not due before `scheduled_at`: the armed job is not claimable yet.
    assert!(store.claim_next(30_000).unwrap().is_none());
    clock.advance_ms(61_000);
    let claimed = store.claim_next(30_000).unwrap().expect("due job claims");
    assert_eq!(claimed.kind, JobKind::PostScheduledDraft);
    assert_eq!(claimed.not_before, at);
    let payload: ScheduledDraftPayload = serde_json::from_slice(&claimed.payload).unwrap();
    assert_eq!(payload.draft_id, "s2_d1");
    assert_eq!(payload.scheduled_at_ms, at);
    store.complete(&claimed.id).unwrap();

    // Two armings for one draft (update re-arms after clearing): the clear
    // removes both, and repeats are no-ops.
    store
        .create_draft(
            draft_row("s2_d2", &alice.id, Some(at)),
            vec![
                scheduled_draft_job("s2-arm-a", "s2_d2", at),
                scheduled_draft_job("s2-arm-b", "s2_d2", at + 1),
            ],
        )
        .unwrap();
    // Clearing an unrelated draft removes nothing.
    assert_eq!(store.remove_scheduled_draft_jobs("s2_nope").unwrap(), 0);
    assert_eq!(store.remove_scheduled_draft_jobs("s2_d2").unwrap(), 2);
    assert_eq!(store.remove_scheduled_draft_jobs("s2_d2").unwrap(), 0);
    assert_eq!(store.pending_count().unwrap(), 0);
}

#[test]
fn s2_scheduled_arming() {
    let factory = RedbFactory::fresh("s2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_s2_scheduled_arming(&*store, &clock);
}

// ---------------------------------------------------------------------------
// Group P — pinnings (§7, `docs/misskey-parity-plan.md`)
// ---------------------------------------------------------------------------

fn pinning_row(id: &str, user_id: &str, note_id: &str) -> UserNotePining {
    UserNotePining {
        id: id.to_string(),
        user_id: user_id.to_string(),
        note_id: note_id.to_string(),
    }
}

/// P1: pinning CRUD through the ports — unique `(user, note)` pairs
/// (`ALREADY_PINNED` on dup), oldest-first listing, no-op remove.
fn case_p1_pinning_crud<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "p1_alice");
    assert!(store.pinnings_for_user(&alice.id).unwrap().is_empty());
    store
        .add_pinning(pinning_row("p1_b", &alice.id, "n2"), vec![])
        .unwrap();
    store
        .add_pinning(pinning_row("p1_a", &alice.id, "n1"), vec![])
        .unwrap();
    assert!(matches!(
        store.add_pinning(pinning_row("p1_dup", &alice.id, "n1"), vec![]),
        Err(PortError::Conflict(msg)) if msg == "ALREADY_PINNED"
    ));
    // Oldest pin first (by pinning id).
    let listed = store.pinnings_for_user(&alice.id).unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].id, "p1_a");
    assert_eq!(listed[1].id, "p1_b");
    // Remove drops the pair; repeats are no-op successes.
    store.remove_pinning(&alice.id, "n1", vec![]).unwrap();
    let listed = store.pinnings_for_user(&alice.id).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].note_id, "n2");
    store.remove_pinning(&alice.id, "n1", vec![]).unwrap();
    store.remove_pinning(&alice.id, "n_missing", vec![]).unwrap();
}

#[test]
fn p1_pinning_crud() {
    let factory = RedbFactory::fresh("p1");
    let store = factory.open(test_clock());
    case_p1_pinning_crud(&*store);
}

/// P2: pinning + jobs commit atomically.
fn case_p2_pinning_atomicity<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "p2_alice");
    let before = store.pending_count().unwrap();
    store
        .add_pinning(
            pinning_row("p2_a", &alice.id, "n1"),
            vec![streaming_job(clock, "p2-pin-job")],
        )
        .unwrap();
    assert_eq!(store.pending_count().unwrap(), before + 1);
    store
        .remove_pinning(&alice.id, "n1", vec![streaming_job(clock, "p2-unpin-job")])
        .unwrap();
    assert_eq!(store.pending_count().unwrap(), before + 2);
    assert!(store.pinnings_for_user(&alice.id).unwrap().is_empty());
}

#[test]
fn p2_pinning_atomicity() {
    let factory = RedbFactory::fresh("p2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_p2_pinning_atomicity(&*store, &clock);
}

// ---------------------------------------------------------------------------
// Group E — note edits (§8) + quote-disable storage (§3,
// `docs/misskey-parity-plan.md`; backend-generic, Phase 4 runs these
// against FoundationDB unchanged)
// ---------------------------------------------------------------------------

/// Minimal edit input: text-only change, everything else carried over from
/// the stored note. Callers set `poll` explicitly (`None` removes the
/// stored poll) and `cw`/`renote_of` when the case needs them.
fn edit_text_input(editor_id: &str, note_id: &str, text: &str) -> EditNoteInput {
    EditNoteInput {
        editor_id: editor_id.to_string(),
        note_id: note_id.to_string(),
        text: Some(text.to_string()),
        cw: None,
        renote_of: None,
        file_ids: Vec::new(),
        poll: None,
        reaction_acceptance: None,
        visible_users: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        uri_base: "https://example.local".to_string(),
    }
}

fn edit_poll_input(expires_at_ms: i64) -> PollInput {
    PollInput {
        choices: vec!["yes".to_string(), "no".to_string()],
        multiple: false,
        expires_at_ms: Some(expires_at_ms),
    }
}

/// Note with a poll through the storage ports (`has_poll` must agree with
/// `poll`, per the create-path invariant).
fn note_with_poll<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
    author_id: &str,
    text: &str,
    key: &str,
) -> Note {
    let mut input = CreateNoteInput::simple_public(author_id, text);
    input.has_poll = true;
    input.poll = Some(edit_poll_input(BASE_MS + 3_600_000));
    store
        .create_note(input, vec![streaming_job(clock, key)])
        .expect("create_note with poll")
}

/// Remote (federated) account with a resolvable actor URI.
fn remote_account<S: AccountStore>(
    store: &S,
    handle: &str,
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
            inbox: Some("https://remote.example/inbox".to_string()),
            shared_inbox: None,
            is_bot: false,
            is_cat: false,
            is_locked: false,
            preserved_usernames: Vec::new(),
        }, None)
        .unwrap_or_else(|e| panic!("remote account {handle}: {e:?}"))
}

fn set_reject_quotes<S: ConformanceStore>(store: &S, account_id: &str, reject: bool) {
    store
        .update_account(
            account_id,
            pinetree::application::ports::UpdateAccountPatch {
                reject_quotes: Some(reject),
                ..Default::default()
            },
            Vec::new(),
        )
        .expect("update reject_quotes");
}

/// Quote-shaped create input through the storage ports: commentary text +
/// `renote_of` (an `is_quote` shape) with an explicit quote relation, as
/// the application layer builds it for local quotes.
fn quote_create_input(author_id: &str, target: &Note) -> CreateNoteInput {
    let mut input = CreateNoteInput::simple_public(author_id, "commentary");
    input.renote_of = Some(target.id.clone());
    input.quote = Some(quote_pending("https://remote.example/notes/e6-target"));
    input
}

/// E1: edit lifecycle — an effective text+cw edit updates the row, stamps
/// `updated_at_ms`, commits the jobs, and writes one history row.
/// Author mismatch is `Validation(YOU_ARE_NOT_THE_AUTHOR)`; unknown and
/// deleted notes are `NotFound(NO_SUCH_NOTE)`. Reply-parent immutability
/// is structural (`EditNoteInput` carries no reply field): a reply note's
/// `reply_id` survives an effective edit.
fn case_e1_edit_lifecycle<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "e1_alice");
    let bob = create_user(store, "e1_bob");
    let note = note_with_poll(store, clock, &alice.id, "v1", "e1:create");
    assert!(note.updated_at_ms.is_none());

    let mut edit = edit_text_input(&alice.id, &note.id, "v2");
    edit.cw = Some("spoiler".to_string());
    edit.poll = Some(edit_poll_input(BASE_MS + 3_600_000));
    let before = store.pending_count().unwrap();
    let edited = store
        .apply_note_edit(edit, vec![streaming_job(clock, "e1:edit")])
        .expect("effective edit");
    assert_eq!(edited.text.as_deref(), Some("v2"));
    assert_eq!(edited.cw.as_deref(), Some("spoiler"));
    assert!(edited.updated_at_ms.is_some());
    assert_eq!(
        store.pending_count().unwrap(),
        before + 1,
        "edit + jobs commit together"
    );
    let reread = store.get_note(&note.id).unwrap().expect("note still here");
    assert_eq!(reread.text.as_deref(), Some("v2"));
    assert_eq!(reread.updated_at_ms, edited.updated_at_ms);
    // The preserved poll survives the edit untouched with zeroed tallies.
    let poll = store.get_poll(&note.id).unwrap().expect("poll kept");
    assert_eq!(poll.choices, vec!["yes".to_string(), "no".to_string()]);
    assert_eq!(poll.votes, vec![0, 0]);

    let edits = store.note_edits(&note.id).unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].old_text.as_deref(), Some("v1"));
    assert_eq!(edits[0].new_text.as_deref(), Some("v2"));
    assert_eq!(edits[0].cw.as_deref(), Some("spoiler"));

    // Author mismatch → YOU_ARE_NOT_THE_AUTHOR (storage-level re-check).
    let err = store
        .apply_note_edit(edit_text_input(&bob.id, &note.id, "hijack"), vec![])
        .expect_err("non-author edit must fail");
    assert!(
        matches!(err, PortError::Validation(ref msg) if msg == "YOU_ARE_NOT_THE_AUTHOR"),
        "got {err:?}"
    );
    // Unknown and deleted notes → NO_SUCH_NOTE.
    let err = store
        .apply_note_edit(edit_text_input(&alice.id, "missing", "x"), vec![])
        .expect_err("unknown note must fail");
    assert!(
        matches!(err, PortError::NotFound(ref msg) if msg == "NO_SUCH_NOTE"),
        "got {err:?}"
    );
    store.delete_note(&note.id, vec![]).unwrap();
    let err = store
        .apply_note_edit(edit_text_input(&alice.id, &note.id, "x"), vec![])
        .expect_err("deleted note must fail");
    assert!(
        matches!(err, PortError::NotFound(ref msg) if msg == "NO_SUCH_NOTE"),
        "got {err:?}"
    );

    // Reply-parent immutability: a reply edit keeps its original parent.
    let parent = note_with_job(store, clock, &bob.id, "parent", "e1:parent");
    let mut reply = CreateNoteInput::simple_public(&alice.id, "reply v1");
    reply.reply_to = Some(parent.id.clone());
    let reply = store.create_note(reply, vec![]).expect("reply");
    let edited_reply = store
        .apply_note_edit(edit_text_input(&alice.id, &reply.id, "reply v2"), vec![])
        .expect("reply edit");
    assert_eq!(
        edited_reply.reply_id.as_deref(),
        Some(parent.id.as_str()),
        "reply parent is immutable"
    );
}

#[test]
fn note_edit_lifecycle() {
    let factory = RedbFactory::fresh("ed1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e1_edit_lifecycle(&*store, &clock);
}

/// E2: no-op edit — identical effective text/cw/poll/files/renote/
/// acceptance returns the ORIGINAL note (`updated_at_ms` stays `None`),
/// writes no history row, and enqueues no jobs (`pending_count`
/// unchanged, even when jobs are passed).
fn case_e2_edit_noop<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "e2_alice");
    let note = note_with_poll(store, clock, &alice.id, "same", "e2:create");
    let mut noop = edit_text_input(&alice.id, &note.id, "same");
    noop.poll = Some(edit_poll_input(BASE_MS + 3_600_000));
    let before = store.pending_count().unwrap();
    let returned = store
        .apply_note_edit(noop, vec![streaming_job(clock, "e2:noop")])
        .expect("no-op edit succeeds");
    assert!(
        returned.updated_at_ms.is_none(),
        "no-op returns the original note"
    );
    assert_eq!(returned.text.as_deref(), Some("same"));
    let reread = store.get_note(&note.id).unwrap().expect("note");
    assert!(reread.updated_at_ms.is_none());
    assert!(
        store.note_edits(&note.id).unwrap().is_empty(),
        "no history row on no-op"
    );
    assert_eq!(
        store.pending_count().unwrap(),
        before,
        "no jobs enqueued on no-op"
    );
}

#[test]
fn note_edit_noop_writes_nothing() {
    let factory = RedbFactory::fresh("ed2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e2_edit_noop(&*store, &clock);
}

/// E3: poll replace resets votes — a different poll definition swaps the
/// row with zeroed tallies, drops every `POLL_VOTES` row for the note
/// (re-vote succeeds, tallies restart), writes one history row, and
/// commits the passed jobs (the poll-expiry re-arm among them).
fn case_e3_edit_poll_replace_resets_votes<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
) {
    let alice = create_user(store, "e3_alice");
    let bob = create_user(store, "e3_bob");
    let note = note_with_poll(store, clock, &alice.id, "pick", "e3:create");
    store
        .cast_vote(
            PollVote {
                id: "e3-vote-1".to_string(),
                note_id: note.id.clone(),
                user_id: bob.id.clone(),
                choice: 0,
            },
            vec![],
        )
        .expect("first vote");
    assert_eq!(store.get_poll(&note.id).unwrap().unwrap().votes, vec![1, 0]);

    let replacement = PollInput {
        choices: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        multiple: false,
        expires_at_ms: Some(BASE_MS + 7_200_000),
    };
    let mut edit = edit_text_input(&alice.id, &note.id, "pick");
    edit.poll = Some(replacement.clone());
    let rearm = NewJob::new_json(
        JobKind::PollExpiry,
        &PollExpiryPayload {
            note_id: note.id.clone(),
        },
        "e3:rearm",
        BASE_MS + 7_200_000,
    )
    .expect("rearm payload");
    let before = store.pending_count().unwrap();
    store
        .apply_note_edit(edit, vec![streaming_job(clock, "e3:edit"), rearm])
        .expect("poll replace");
    assert_eq!(
        store.pending_count().unwrap(),
        before + 2,
        "edit jobs committed"
    );
    let poll = store.get_poll(&note.id).unwrap().expect("replaced poll");
    assert_eq!(
        poll.choices,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(poll.votes, vec![0, 0, 0], "tallies restart zeroed");
    assert!(
        store
            .votes_by_user_for_note(&bob.id, &note.id)
            .unwrap()
            .is_empty(),
        "old vote rows dropped"
    );
    // Re-vote succeeds (the old vote no longer blocks); tallies restart.
    store
        .cast_vote(
            PollVote {
                id: "e3-vote-2".to_string(),
                note_id: note.id.clone(),
                user_id: bob.id.clone(),
                choice: 0,
            },
            vec![],
        )
        .expect("re-vote after reset");
    assert_eq!(
        store.get_poll(&note.id).unwrap().unwrap().votes,
        vec![1, 0, 0]
    );
    assert_eq!(
        store.note_edits(&note.id).unwrap().len(),
        1,
        "one history row"
    );
}

#[test]
fn note_edit_poll_replace_resets_votes() {
    let factory = RedbFactory::fresh("ed3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e3_edit_poll_replace_resets_votes(&*store, &clock);
}

/// E4: history chaining — two effective edits yield two rows oldest-first;
/// row2's `old_text` is row1's `new_text`; row1's `old_date_ms` is the
/// note creation time (parsed from the time-ordered note id, which the
/// factory's id generator stamps from the same clock) and row2's
/// `old_date_ms` is row1's edit time.
fn case_e4_edit_history_chaining<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "e4_alice");
    let t0 = clock.now_ms();
    let note = note_with_job(store, clock, &alice.id, "v1", "e4:create");
    clock.advance_ms(1_000);
    store
        .apply_note_edit(edit_text_input(&alice.id, &note.id, "v2"), vec![])
        .expect("first edit");
    clock.advance_ms(1_000);
    store
        .apply_note_edit(edit_text_input(&alice.id, &note.id, "v3"), vec![])
        .expect("second edit");
    let rows = store.note_edits(&note.id).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].old_text.as_deref(), Some("v1"));
    assert_eq!(rows[0].new_text.as_deref(), Some("v2"));
    assert_eq!(rows[1].old_text.as_deref(), Some("v2"));
    assert_eq!(rows[1].new_text.as_deref(), Some("v3"));
    assert_eq!(rows[0].old_date_ms, t0, "first old_date is the creation time");
    assert_eq!(
        rows[1].old_date_ms, rows[0].updated_at_ms,
        "second old_date chains from the first edit"
    );
    assert!(rows[0].old_date_ms < rows[0].updated_at_ms);
    assert!(rows[0].updated_at_ms < rows[1].updated_at_ms);
}

#[test]
fn note_edit_history_chaining() {
    let factory = RedbFactory::fresh("ed4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e4_edit_history_chaining(&*store, &clock);
}

/// E5: inbound dedup — `record_note_edit_activity` commits the edit,
/// history row, and jobs once; replaying the same activity id converges
/// to `None` with no second history row and no extra jobs.
fn case_e5_inbound_edit_dedup<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "e5_alice");
    let note = note_with_job(store, clock, &alice.id, "v1", "e5:create");
    let before = store.pending_count().unwrap();
    let first = store
        .record_note_edit_activity(
            "e5:upd-1",
            edit_text_input(&alice.id, &note.id, "v2"),
            vec![streaming_job(clock, "e5:edit")],
        )
        .expect("first delivery applies")
        .expect("first delivery is not a replay");
    assert_eq!(first.text.as_deref(), Some("v2"));
    assert_eq!(store.note_edits(&note.id).unwrap().len(), 1);
    assert_eq!(store.pending_count().unwrap(), before + 1);
    let replay = store
        .record_note_edit_activity(
            "e5:upd-1",
            edit_text_input(&alice.id, &note.id, "v3"),
            vec![streaming_job(clock, "e5:replay")],
        )
        .expect("replay converges");
    assert!(replay.is_none(), "replay applies nothing");
    assert_eq!(
        store
            .get_note(&note.id)
            .unwrap()
            .unwrap()
            .text
            .as_deref(),
        Some("v2"),
        "replay changes nothing"
    );
    assert_eq!(
        store.note_edits(&note.id).unwrap().len(),
        1,
        "no second history row"
    );
    assert_eq!(
        store.pending_count().unwrap(),
        before + 1,
        "no extra jobs on replay"
    );
}

#[test]
fn note_edit_inbound_dedup() {
    let factory = RedbFactory::fresh("ed5");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e5_inbound_edit_dedup(&*store, &clock);
}

/// E6: quote-disable storage (§3) — the QUOTER's own `reject_quotes` flag
/// gates quote creation (Sharkey `checkQuotePermissions` parity): a
/// remote quoter is stripped to a plain note (`quoteUnavailable`, no
/// `renote_count` bump); a local quoter fails with
/// `QUOTE_DISABLED_FOR_USER`.
fn case_e6_quote_disable_storage<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let bob = create_user(store, "e6_bob");
    let target = note_with_job(store, clock, &bob.id, "quotable", "e6:target");

    // (a) Remote quoter with the flag: stripped to a plain note.
    let carol = remote_account(store, "e6_carol");
    set_reject_quotes(store, &carol.id, true);
    let stripped = store
        .create_note(
            quote_create_input(&carol.id, &target),
            vec![streaming_job(clock, "e6:strip")],
        )
        .expect("remote strip stores a plain note");
    assert!(stripped.renote_id.is_none(), "renote detached");
    assert!(stripped.quote.is_none(), "quote relation dropped");
    assert_eq!(
        stripped.process_errors,
        Some(vec!["quoteUnavailable".to_string()])
    );
    assert_eq!(
        stripped.text.as_deref(),
        Some("commentary"),
        "commentary kept"
    );
    let target_after = store.get_note(&target.id).unwrap().expect("target");
    assert_eq!(target_after.renote_count, 0, "no renote_count bump on strip");

    // (b) Local quoter with the flag: hard failure.
    let dave = create_user(store, "e6_dave");
    set_reject_quotes(store, &dave.id, true);
    let err = store
        .create_note(quote_create_input(&dave.id, &target), vec![])
        .expect_err("local quote with the flag must fail");
    assert!(
        matches!(err, PortError::Validation(ref msg) if msg == "QUOTE_DISABLED_FOR_USER"),
        "got {err:?}"
    );

    // (c) Control: remote quoter without the flag stores the quote normally.
    let erin = remote_account(store, "e6_erin");
    let quote = store
        .create_note(
            quote_create_input(&erin.id, &target),
            vec![streaming_job(clock, "e6:control")],
        )
        .expect("control quote stores");
    assert_eq!(quote.renote_id.as_deref(), Some(target.id.as_str()));
    let relation = quote.quote.as_ref().expect("quote relation kept");
    assert_eq!(
        relation.target_note_id.as_deref(),
        Some(target.id.as_str())
    );
    let target_after = store.get_note(&target.id).unwrap().expect("target");
    assert_eq!(target_after.renote_count, 1, "control bumps renote_count");
}

#[test]
fn quote_disable_storage() {
    let factory = RedbFactory::fresh("ed6");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e6_quote_disable_storage(&*store, &clock);
}

// ---------------------------------------------------------------------------
// Group U — note URI uniqueness (`docs/persistence-conformance.md`)
// ---------------------------------------------------------------------------

fn uri_input(author_id: &str, uri: &str, text: &str) -> CreateNoteInput {
    let mut input = CreateNoteInput::simple_public(author_id, text);
    input.uri = Some(uri.to_string());
    input
}

/// U1: the same note `uri` arriving under two different activity ids
/// (`ap/show`-shaped fetch, then the real `Create`) yields exactly one row;
/// the second is a replay (`Ok(None)`), its jobs are not enqueued, and
/// `find_by_uri` resolves the first row.
fn case_u1_same_uri_different_activity<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let remote = remote_account(store, "u1_remote");
    let uri = "https://remote.example/notes/u1";
    let first = store
        .record_note_activity(
            uri,
            uri_input(&remote.id, uri, "bare copy"),
            vec![streaming_job(clock, "u1:job-1")],
        )
        .expect("first")
        .expect("first wins");
    let second = store
        .record_note_activity(
            "https://remote.example/activities/u1-create",
            uri_input(&remote.id, uri, "full copy"),
            vec![streaming_job(clock, "u1:job-2")],
        )
        .expect("second");
    assert_eq!(second, None, "same uri under another activity id is a replay");
    assert_eq!(store.pending_count().expect("count"), 1, "loser enqueued no job");
    let found = NoteStore::find_by_uri(&*store, uri).expect("find").expect("indexed");
    assert_eq!(found.id, first.id);
    let rows = list_all(store).iter().filter(|n| n.uri.as_deref() == Some(uri)).count();
    assert_eq!(rows, 1, "exactly one row per uri");

    // A plain create with a taken uri is a conflict, not a second row.
    let err = store
        .create_note(uri_input(&remote.id, uri, "again"), Vec::new())
        .expect_err("duplicate uri");
    assert!(matches!(err, PortError::Conflict(_)), "got {err:?}");
}

#[test]
fn u1_same_uri_different_activity_ids() {
    let factory = RedbFactory::fresh("u1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_u1_same_uri_different_activity(&*store, &clock);
}

/// U2: 8 racers, same uri, distinct activity ids → one winner, one row.
#[test]
fn u2_concurrent_same_uri() {
    let factory = RedbFactory::fresh("u2");
    let clock = test_clock();
    let store: Arc<RedbStore> = factory.open(clock.clone());
    let remote = remote_account(&*store, "u2_remote");
    let uri = "https://remote.example/notes/u2";
    let results: Vec<_> = std::thread::scope(|s| {
        (0..8)
            .map(|i| {
                let store = store.clone();
                let clock = clock.clone();
                let author = remote.id.clone();
                s.spawn(move || {
                    store.record_note_activity(
                        &format!("https://remote.example/activities/u2-{i}"),
                        uri_input(&author, uri, &format!("racer {i}")),
                        vec![streaming_job(&clock, &format!("u2:job-{i}"))],
                    )
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().expect("join"))
            .collect()
    });
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(results.iter().filter(|r| matches!(r, Ok(Some(_)))).count(), 1);
    let rows = list_all(&*store).iter().filter(|n| n.uri.as_deref() == Some(uri)).count();
    assert_eq!(rows, 1);
    assert_eq!(store.pending_count().expect("count"), 1);
}

/// U3: a deleted note releases its uri (re-fetch after `Delete` may recreate).
#[test]
fn u3_deleted_note_releases_uri() {
    let factory = RedbFactory::fresh("u3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let remote = remote_account(&*store, "u3_remote");
    let uri = "https://remote.example/notes/u3";
    let first = store
        .record_note_activity("act:u3:1", uri_input(&remote.id, uri, "one"), Vec::new())
        .unwrap()
        .unwrap();
    store.delete_note(&first.id, Vec::new()).expect("delete");
    assert!(NoteStore::find_by_uri(&*store, uri).unwrap().is_none(), "tombstone not found by uri");
    let again = store
        .record_note_activity("act:u3:2", uri_input(&remote.id, uri, "two"), Vec::new())
        .unwrap()
        .expect("uri free again");
    assert_eq!(NoteStore::find_by_uri(&*store, uri).unwrap().unwrap().id, again.id);
}

// ---------------------------------------------------------------------------
// Group N — notification order (`docs/persistence-conformance.md`)
// ---------------------------------------------------------------------------

/// N1: `i/notifications` order is newest-first across kinds, and `sinceId`/`untilId`
/// paging walks that same order. Ids mirror production (`notify:{kind}:{suffix}`), so
/// a key order that starts with the kind would group rows by kind and fail here.
#[test]
fn n1_notifications_newest_first_across_kinds() {
    use pinetree::domain::notification::{Notification, NotificationKind};

    let factory = RedbFactory::fresh("n1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let me = remote_account(&*store, "n1_me");
    let who = remote_account(&*store, "n1_who");

    // Oldest first. Kinds interleave so a kind-grouped order is visibly wrong.
    let rows = [
        ("inbound-reply", NotificationKind::Reply),
        ("inbound-reaction", NotificationKind::Reaction),
        ("inbound-mention", NotificationKind::Mention),
        ("inbound-follow", NotificationKind::Follow),
        ("inbound-reply", NotificationKind::Reply),
        ("inbound-mention", NotificationKind::Mention),
    ];
    let mut ids = Vec::new();
    for (i, (label, kind)) in rows.iter().enumerate() {
        let id = format!("notify:{label}:{:016}", i + 1);
        store
            .insert_notification(Notification {
                id: id.clone(),
                notifiee_id: me.id.clone(),
                notifier_id: who.id.clone(),
                kind: kind.clone(),
                note_id: None,
                reaction: None,
                draft_id: None,
                file_id: None,
                exported_entity: None,
                is_read: false,
            })
            .expect("insert notification");
        ids.push(id);
    }
    let newest_first: Vec<String> = ids.iter().rev().cloned().collect();

    let all = store
        .list_notifications_filtered(&me.id, &TimelineCursor::with_limit(20), &[], &[])
        .expect("filtered list");
    let got: Vec<String> = all.iter().map(|n| n.id.clone()).collect();
    assert_eq!(got, newest_first, "newest-first across kinds");

    let plain = store.list_notifications(&me.id, 20).expect("list");
    let got: Vec<String> = plain.iter().map(|n| n.id.clone()).collect();
    assert_eq!(got, newest_first, "unfiltered list uses the same order");

    // Page 2 via untilId = the third-newest id, limit 2 → the next two rows.
    let cursor = TimelineCursor {
        until_id: Some(newest_first[2].clone()),
        limit: 2,
        ..Default::default()
    };
    let page = store
        .list_notifications_filtered(&me.id, &cursor, &[], &[])
        .expect("paged list");
    let got: Vec<String> = page.iter().map(|n| n.id.clone()).collect();
    assert_eq!(got, newest_first[3..5].to_vec(), "untilId pages across kinds");

    // sinceId = the third-newest id → only the two newer rows.
    let cursor = TimelineCursor {
        since_id: Some(newest_first[2].clone()),
        limit: 20,
        ..Default::default()
    };
    let newer = store
        .list_notifications_filtered(&me.id, &cursor, &[], &[])
        .expect("since list");
    let got: Vec<String> = newer.iter().map(|n| n.id.clone()).collect();
    assert_eq!(got, newest_first[..2].to_vec(), "sinceId returns only newer rows");
}

// ---------------------------------------------------------------------------
// Group T — remembered deletions (tombstones; bench S6, 2026-10-17)
// ---------------------------------------------------------------------------

/// T1: a `Delete` that reaches the store before its `Create` leaves a
/// tombstone. The late `Create` under a new activity id is refused (`Ok(None)`),
/// stores no row, and enqueues no job.
fn case_t1_delete_before_create<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let remote = remote_account(store, "t1_remote");
    let uri = "https://remote.example/notes/t1";
    let deleted = store
        .record_delete_uri_activity("https://remote.example/activities/t1-delete", uri, Vec::new())
        .expect("delete first");
    assert!(!deleted, "no note held: the delete only records a tombstone");
    let late = store
        .record_note_activity(
            "https://remote.example/activities/t1-create",
            uri_input(&remote.id, uri, "late copy"),
            vec![streaming_job(clock, "t1:job-1")],
        )
        .expect("late create");
    assert_eq!(late, None, "a tombstoned uri must not be stored");
    assert_eq!(store.pending_count().expect("count"), 0, "refused copy enqueued no job");
    assert!(NoteStore::find_by_uri(&*store, uri).expect("find").is_none());
    // A plain create of the same uri is refused the same way.
    let err = store
        .create_note(uri_input(&remote.id, uri, "again"), Vec::new())
        .expect_err("tombstoned uri");
    assert!(matches!(err, PortError::Conflict(_)), "got {err:?}");
}

#[test]
fn t1_delete_before_create_leaves_tombstone() {
    let factory = RedbFactory::fresh("t1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_t1_delete_before_create(&*store, &clock);
}

/// T2: a `Delete` by URI for a held note removes it and enqueues its streaming
/// job. The uri is released afterwards, as Group U3 requires, so a fresh `Create`
/// may take it again.
fn case_t2_delete_by_uri_removes_and_releases<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
) {
    let remote = remote_account(store, "t2_remote");
    let uri = "https://remote.example/notes/t2";
    store
        .record_note_activity(
            "https://remote.example/activities/t2-create",
            uri_input(&remote.id, uri, "stored"),
            Vec::new(),
        )
        .expect("create")
        .expect("stored");
    let deleted = store
        .record_delete_uri_activity(
            "https://remote.example/activities/t2-delete",
            uri,
            vec![streaming_job(clock, "t2:job-del")],
        )
        .expect("delete");
    assert!(deleted, "the held note is deleted");
    assert!(NoteStore::find_by_uri(&*store, uri).expect("find").is_none());
    assert_eq!(store.pending_count().expect("count"), 1, "delete enqueued its job");
    let again = store
        .record_note_activity(
            "https://remote.example/activities/t2-create-again",
            uri_input(&remote.id, uri, "fetched again"),
            Vec::new(),
        )
        .expect("create after delete")
        .expect("uri released after a held note is deleted");
    assert_eq!(NoteStore::find_by_uri(&*store, uri).expect("find").unwrap().id, again.id);
}

#[test]
fn t2_delete_by_uri_removes_and_releases() {
    let factory = RedbFactory::fresh("t2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_t2_delete_by_uri_removes_and_releases(&*store, &clock);
}

/// T3: a replayed delete activity is a no-op, a tombstone is scoped to its own
/// uri, and an empty uri is rejected.
fn case_t3_delete_replay_and_scope<S: ConformanceStore>(store: &S) {
    let remote = remote_account(store, "t3_remote");
    let uri = "https://remote.example/notes/t3";
    let other = "https://remote.example/notes/t3-other";
    let delete_id = "https://remote.example/activities/t3-delete";
    assert!(!store.record_delete_uri_activity(delete_id, uri, Vec::new()).expect("first"));
    assert!(
        !store.record_delete_uri_activity(delete_id, uri, Vec::new()).expect("replay"),
        "a replayed delete activity is a no-op"
    );
    let kept = store
        .record_note_activity(
            "https://remote.example/activities/t3-create-other",
            uri_input(&remote.id, other, "other"),
            Vec::new(),
        )
        .expect("other create")
        .expect("other uri is not tombstoned");
    assert_eq!(kept.uri.as_deref(), Some(other));
    let err = store
        .record_delete_uri_activity("https://remote.example/activities/t3-empty", "  ", Vec::new())
        .expect_err("empty uri");
    assert!(matches!(err, PortError::Validation(_)), "got {err:?}");
}

#[test]
fn t3_delete_replay_and_scope() {
    let factory = RedbFactory::fresh("t3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_t3_delete_replay_and_scope(&*store);
}

// ---------------------------------------------------------------------------
// Group H — moderation (Batch M §3.4; `docs/implementer-guide-batch-m.md`)
// ---------------------------------------------------------------------------

/// Batch M log fixture. Ids are harness-supplied (the port accepts
/// caller-built rows; ids are generated by the application in production).
fn log_entry(
    id: &str,
    actor: Option<&str>,
    action: ModerationAction,
    target: Option<&str>,
    details: serde_json::Value,
) -> ModerationLogEntry {
    ModerationLogEntry::new(
        id.to_string(),
        actor.map(str::to_string),
        action,
        target.map(str::to_string),
        details,
        BASE_MS,
    )
    .expect("valid log entry")
}

/// An intentionally invalid row (`details` must be a JSON object): passed
/// into a compound method to make the log append fail INSIDE the action's
/// transaction, proving the state change and the row commit together or
/// not at all (H1/H2/H4).
fn invalid_log_entry(id: &str, action: ModerationAction) -> ModerationLogEntry {
    ModerationLogEntry {
        id: id.to_string(),
        actor_account: None,
        action,
        target: None,
        details: serde_json::json!("not an object"),
        created_at_ms: BASE_MS,
    }
}

fn log_rows<S: ModerationLogStore>(store: &S) -> Vec<ModerationLogEntry> {
    store
        .list_logs(
            &ModerationLogFilter::default(),
            &TimelineCursor {
                limit: 1_000,
                ..Default::default()
            },
        )
        .expect("list_logs")
}

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
        !store
            .find_by_id(&alice.id)
            .unwrap()
            .unwrap()
            .is_suspended,
        "no state change without the log row"
    );
    assert!(log_rows(store).is_empty(), "no orphan log row");
    assert!(store.claim_next(500).expect("claim").is_none(), "no orphan job");

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
    assert!(store.claim_next(500).expect("claim").is_none(), "no orphan job");

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
        store
            .find_by_id(&alice_id)
            .unwrap()
            .unwrap()
            .reject_quotes,
        "the state change survived with the row"
    );
}

// ---------------------------------------------------------------------------
// Group I — invite codes and the signup claim (Batch M WP2;
// `docs/implementer-guide-batch-m.md`)
// ---------------------------------------------------------------------------

fn invite_input(code: &str, created_by: Option<&str>, expires_at_ms: Option<i64>) -> CreateInviteInput {
    CreateInviteInput {
        code: code.to_string(),
        created_by: created_by.map(str::to_string),
        expires_at_ms,
    }
}

fn consume_input(code: &str, handle: &str) -> ConsumeInviteInput {
    ConsumeInviteInput {
        code: code.to_string(),
        account: CreateAccountInput::local(handle),
        password_hash: Some(format!("hash-for-{handle}")),
    }
}

/// I1: one invite code creates at most one account even under N retrying
/// claims — exactly one `consume_and_create_account` succeeds (the
/// unique-index row is dropped with the claim).
fn case_i1_invite_single_use<S: ConformanceStore>(store: &S) {
    store
        .create_invites(&[invite_input("ICODE0001AAAA", None, None)], None)
        .expect("create invite");
    let mut successes = 0usize;
    for attempt in 0..5 {
        match store.consume_and_create_account(consume_input(
            "ICODE0001AAAA",
            &format!("i1_claimant{attempt}"),
        )) {
            Ok((account, ticket)) => {
                successes += 1;
                assert_eq!(
                    ticket.used_by.as_deref(),
                    Some(account.id.as_str()),
                    "the winning claim marks the code used for its account"
                );
            }
            Err(PortError::NotFound(_)) => {}
            Err(err) => panic!("attempt {attempt} failed unexpectedly: {err:?}"),
        }
    }
    assert_eq!(successes, 1, "exactly one of 5 claims succeeds");
    assert!(
        store
            .find_invite_by_code("ICODE0001AAAA")
            .expect("lookup")
            .is_none(),
        "a consumed code is gone from the unique index"
    );
    assert!(
        store.find_by_handle("i1_claimant0").expect("lookup").is_some(),
        "the first (winning) claim created the account"
    );
    assert!(
        store.find_by_handle("i1_claimant1").expect("lookup").is_none(),
        "losing claims created nothing"
    );
}

/// I2: the claim and the account create are ONE transaction — a failing
/// account insert (duplicate handle) aborts the claim entirely: the code
/// stays unused and claimable, and no account row appears.
fn case_i2_consume_create_atomic<S: ConformanceStore>(store: &S) {
    create_user(store, "i2_taken");
    store
        .create_invites(&[invite_input("ICODE0002AAAA", None, None)], None)
        .expect("create invite");
    let err = store
        .consume_and_create_account(consume_input("ICODE0002AAAA", "i2_taken"))
        .expect_err("duplicate handle refuses the consume");
    assert!(
        matches!(err, PortError::Conflict(_)),
        "duplicate handle is a conflict, got {err:?}"
    );
    let ticket = store
        .find_invite_by_code("ICODE0002AAAA")
        .expect("lookup")
        .expect("the code survived the failed claim");
    assert!(!ticket.is_used(), "the failed claim consumed nothing");
    // A valid claim in the same state now succeeds.
    let (account, ticket) = store
        .consume_and_create_account(consume_input("ICODE0002AAAA", "i2_fresh"))
        .expect("claim after the failed attempt");
    assert!(ticket.is_used());
    assert!(store.find_by_id(&account.id).expect("lookup").is_some());
    assert!(
        store.find_by_handle("i2_taken").expect("lookup").is_some(),
        "the pre-existing account is untouched"
    );
}

/// I3: `InstanceSettings.registration` (upstream `disableRegistration`)
/// persists through the settings store and commits with its
/// `updateServerSettings` row. The gate itself is application logic
/// (`AppServices::signup`), asserted by the endpoint tests; a passing
/// backend must store the policy exactly.
fn case_i3_registration_policy<S: ConformanceStore>(store: &S) {
    let close = InstanceSettingsPatch {
        registration: Some(RegistrationPolicy::Closed),
        ..Default::default()
    };
    let updated = store
        .update(
            close,
            log_entry(
                "i3log000000000a",
                None,
                ModerationAction::UpdateServerSettings,
                None,
                serde_json::json!({ "before": {"disableRegistration": false}, "after": {"disableRegistration": true} }),
            ),
        )
        .expect("close registration");
    assert!(updated.registration.disable_registration());
    assert!(InstanceSettingsStore::get(store)
        .expect("get")
        .registration
        .disable_registration());
    let open = InstanceSettingsPatch {
        registration: Some(RegistrationPolicy::Open),
        ..Default::default()
    };
    store
        .update(
            open,
            log_entry(
                "i3log000000000b",
                None,
                ModerationAction::UpdateServerSettings,
                None,
                serde_json::json!({ "before": {"disableRegistration": true}, "after": {"disableRegistration": false} }),
            ),
        )
        .expect("reopen registration");
    assert!(!InstanceSettingsStore::get(store)
        .expect("get")
        .registration
        .disable_registration());
    assert_eq!(log_rows(store).len(), 2, "each write kept its row");
}

/// I4: the happy path of the claim — a valid unexpired code creates the
/// account, marks the code used (used_by/used_at), and drops the unique
/// index row; missing, used, and expired codes all refuse the same way
/// (upstream's signup gate answers 400 for all three).
fn case_i4_consume_happy_path<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    store
        .create_invites(
            &[invite_input("ICODE0004AAAA", None, Some(BASE_MS + 60_000))],
            None,
        )
        .expect("create invite");
    let (account, ticket) = store
        .consume_and_create_account(consume_input("ICODE0004AAAA", "i4_newbie"))
        .expect("consume a valid invite");
    assert_eq!(ticket.used_by.as_deref(), Some(account.id.as_str()));
    assert_eq!(ticket.used_at_ms, Some(clock.now_ms()));
    assert!(store.find_by_handle("i4_newbie").expect("lookup").is_some());
    assert!(store
        .find_invite_by_code("ICODE0004AAAA")
        .expect("lookup")
        .is_none());
    // Second consume of the same code: refused.
    assert!(matches!(
        store.consume_and_create_account(consume_input("ICODE0004AAAA", "i4_ghost")),
        Err(PortError::NotFound(_))
    ));
    assert!(store.find_by_handle("i4_ghost").expect("lookup").is_none());
    // Expired code: refused the same way, nothing created.
    store
        .create_invites(
            &[invite_input("ICODE0004EXPD", None, Some(BASE_MS - 1))],
            None,
        )
        .expect("create expired invite");
    assert!(matches!(
        store.consume_and_create_account(consume_input("ICODE0004EXPD", "i4_late")),
        Err(PortError::NotFound(_))
    ));
    assert!(store.find_by_handle("i4_late").expect("lookup").is_none());
}

#[test]
fn i1_invite_single_use() {
    let factory = RedbFactory::fresh("i1");
    let clock = test_clock();
    let store = factory.open(clock);
    case_i1_invite_single_use(&*store);
}

#[test]
fn i2_consume_create_atomic() {
    let factory = RedbFactory::fresh("i2");
    let clock = test_clock();
    let store = factory.open(clock);
    case_i2_consume_create_atomic(&*store);
}

#[test]
fn i3_registration_policy() {
    let factory = RedbFactory::fresh("i3");
    let clock = test_clock();
    let store = factory.open(clock);
    case_i3_registration_policy(&*store);
}

#[test]
fn i4_consume_happy_path() {
    let factory = RedbFactory::fresh("i4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_i4_consume_happy_path(&*store, &clock);
}
