//! Slice C4 crash-recovery tests (Phase 1).
//!
//! Kill the server mid-traffic (subprocess `SIGKILL`), restart on the same
//! data directory, and assert no lost notes or jobs and that the queue
//! drains. Follows the subprocess pattern from `tests/conformance.rs`: the
//! child process does the writes, signals readiness through a `.ready` file
//! on disk (no stdout buffering deadlocks), and then keeps running until the
//! parent `child.kill()`s it (`SIGKILL` on Linux). `commit()` fsyncs and the
//! OS releases the child's file locks on death, so the parent can reopen.
//!
//! Child processes are spawned as `std::env::current_exe()` with
//! `PINETREE_C4_CHILD=<kind>` + `PINETREE_C4_DIR=<dir>` and args
//! `--exact <test> --nocapture` (`--nocapture` is load-bearing: libtest
//! captures per-test stdout by default, which would swallow panics and stall
//! debugging of the child).

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, CreateNoteInput, FollowStore, JobQueue, MediaStore,
    NoopStreamingSink, NoteStore, NotificationStore, ReactionStore, TimelineCursor,
};
use pinetree::domain::id::SystemClock;
use pinetree::domain::notification::NotificationKind;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

// ---------------------------------------------------------------------------
// Shared harness
// ---------------------------------------------------------------------------

static CASE_COUNTER: AtomicU64 = AtomicU64::new(0);
static CHILD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_crash_harness() -> std::sync::MutexGuard<'static, ()> {
    CHILD_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn fresh_data_dir(prefix: &str) -> PathBuf {
    let n = CASE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-c4-{prefix}-{}-{n}", std::process::id()));
    fs::create_dir_all(&dir).expect("create c4 temp dir");
    dir
}

fn child_mode_is(kind: &str) -> bool {
    std::env::var("PINETREE_C4_CHILD")
        .map(|v| v == kind)
        .unwrap_or(false)
}

fn child_data_dir() -> PathBuf {
    PathBuf::from(std::env::var("PINETREE_C4_DIR").expect("PINETREE_C4_DIR must be set"))
}

fn ready_and_park(ready_file: &Path) -> ! {
    fs::write(ready_file, b"READY").expect("write READY file");
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}

/// Open the full single-node stack on `dir`: redb store, fs blob store, and
/// the application services over them (noop streaming sink, as in Phase 1).
fn open_stack(dir: &Path) -> (Arc<RedbStore>, Arc<FsBlobStore>, Arc<AppServices>) {
    let clock: Arc<dyn pinetree::application::ports::Clock> = Arc::new(SystemClock);
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone())
        .expect("open redb store");
    let blobs = Arc::new(FsBlobStore::new(dir.join("blobs")).expect("open blob store"));
    let services = Arc::new(AppServices {
        clock: clock.clone(),
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
    (store, blobs, services)
}

fn open_worker(store: &Arc<RedbStore>, services: &Arc<AppServices>) -> JobWorker {
    JobWorker {
        clock: store.clock().clone(),
        ids: store.ids().clone(),
        jobs: store.clone(),
        notifications: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 60_000,
        base_backoff_ms: 1_000,
    }
}

/// Spawn the current test binary as a child running `test_name` in child
/// mode `kind`, wait for its `.ready` file, then SIGKILL it.
fn kill_child_at_ready(kind: &str, test_name: &str, dir: &Path) {
    let ready_file = dir.join(format!("{kind}.ready"));
    let _ = fs::remove_file(&ready_file);
    let mut child = Command::new(std::env::current_exe().expect("current_exe"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("PINETREE_C4_CHILD", kind)
        .env("PINETREE_C4_DIR", dir)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn c4 child");
    let deadline = Instant::now() + Duration::from_secs(120);
    while !ready_file.exists() {
        if let Some(status) = child.try_wait().expect("try_wait child") {
            panic!("c4 child {kind} exited early with {status}");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("c4 child {kind} timed out waiting for READY file");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    // The child keeps generating traffic after READY; kill it immediately so
    // the kill lands mid-traffic, possibly mid-transaction.
    kill_child(&mut child);
}

fn kill_child(child: &mut Child) {
    child.kill().expect("SIGKILL c4 child");
    let _ = child.wait();
    std::thread::sleep(Duration::from_millis(100));
}

/// Drain every pending job; jobs use real-time leases here, so also let
/// wall-clock time pass any lease the dead child could still hold (the
/// children below never claim, but the sleep makes the drain robust to an
/// in-flight claim at SIGKILL time).
fn drain_until_empty(worker: &JobWorker, store: &RedbStore) {
    std::thread::sleep(Duration::from_millis(500));
    for _ in 0..20 {
        worker.drain(10_000).expect("worker drain");
        if store.pending_count().expect("pending_count") == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        store.pending_count().expect("pending_count"),
        0,
        "job queue drains completely after restart"
    );
}

// ---------------------------------------------------------------------------
// Test 1: kill mid-traffic, restart, no lost notes or jobs, queue drains
// ---------------------------------------------------------------------------

/// Append-only ledger of acknowledged note IDs. Every `Ok(note)` from
/// `create_note` is appended and synced before the next op, so the parent
/// can require every single entry to be present after the kill.
struct Ledger {
    file: fs::File,
}

impl Ledger {
    fn open(dir: &Path) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("acknowledged_notes.log"))
            .expect("open ledger");
        Self { file }
    }

    fn record(&mut self, note_id: &str) {
        writeln!(self.file, "{note_id}").expect("append to ledger");
        self.file.flush().expect("flush ledger");
        self.file.sync_all().expect("sync ledger");
    }
}

fn child_c4kill() -> ! {
    let dir = child_data_dir();
    let ready_file = dir.join("c4kill.ready");
    let (_store, _blobs, svc) = open_stack(&dir);
    let mut ledger = Ledger::open(&dir);

    let alice = svc.create_account("alice", None).expect("create alice");
    let bob = svc.create_account("bob", None).expect("create bob");
    let carol = svc.create_account("carol", None).expect("create carol");
    // alice -> bob produces the Follow notification for bob.
    svc.follow(&alice.id, &bob.id, false).expect("alice follows bob");

    // Anchors: bob's anchor collects alice's replies + reactions (Reply and
    // Reaction notifications for bob); alice's anchor is the renote target.
    let bob_anchor = svc
        .create_note(CreateNoteInput::simple_public(&bob.id, "c4kill bob anchor"))
        .expect("bob anchor");
    ledger.record(&bob_anchor.id);
    let alice_anchor = svc
        .create_note(CreateNoteInput::simple_public(&alice.id, "c4kill alice anchor"))
        .expect("alice anchor");
    ledger.record(&alice_anchor.id);

    let mut acked: usize = 2;
    let mut alice_text_notes = vec![alice_anchor.id.clone()];
    let mut bob_text_notes = vec![bob_anchor.id.clone()];
    let mut ready_done = false;

    // Traffic loop: plain notes, @bob mentions, replies to bob, renotes of
    // alice, plus reactions in both directions (bob reacts to alice's notes
    // per the slice plan; alice reacts to bob's so bob also banks a
    // Reaction notification). No worker runs here, so the queue only grows.
    let mut i: usize = 0;
    while acked < 400 {
        let note = match i % 6 {
            0 => svc.create_note(CreateNoteInput::simple_public(
                &alice.id,
                format!("c4kill plain {i} from alice"),
            )),
            1 => svc.create_note(CreateNoteInput::simple_public(
                &alice.id,
                format!("@bob c4kill mention {i} from alice"),
            )),
            2 => {
                let mut input = CreateNoteInput::simple_public(
                    &alice.id,
                    format!("c4kill reply {i} to bob"),
                );
                input.reply_to = Some(bob_anchor.id.clone());
                svc.create_note(input)
            }
            3 => {
                // Pure renote of alice's anchor (no text of its own).
                let mut input = CreateNoteInput::simple_public(&carol.id, "placeholder");
                input.text = None;
                input.renote_of = Some(alice_anchor.id.clone());
                svc.create_note(input)
            }
            4 => svc.create_note(CreateNoteInput::simple_public(
                &carol.id,
                format!("@bob c4kill mention {i} from carol"),
            )),
            _ => svc.create_note(CreateNoteInput::simple_public(
                &bob.id,
                format!("c4kill plain {i} from bob"),
            )),
        }
        .unwrap_or_else(|e| panic!("traffic create_note {i} failed: {e:?}"));
        ledger.record(&note.id);
        acked += 1;
        if note.text.is_some() {
            if note.author_id == alice.id {
                alice_text_notes.push(note.id.clone());
            } else if note.author_id == bob.id {
                bob_text_notes.push(note.id.clone());
            }
        }
        // Reactions ride along with traffic; failures (e.g. racing a kill)
        // must not stop the loop, but pre-ready they should not happen.
        if let Some(target) = alice_text_notes.last() {
            let _ = svc.react(&bob.id, target, "👍", true);
        }
        if let Some(target) = bob_text_notes.last() {
            let _ = svc.react(&alice.id, target, "🎉", true);
        }

        // Signal readiness once 15+ notes are acknowledged but the queue is
        // still full (no worker ever drains it here), then keep generating
        // traffic so the parent's SIGKILL lands mid-stream.
        if acked >= 15 && !ready_done {
            fs::write(&ready_file, b"READY").expect("write READY file");
            ready_done = true;
        }
        i += 1;
    }
    // Extremely slow parent fallback: park instead of exiting so the parent
    // always has a live process to SIGKILL.
    ready_and_park(&ready_file);
}

/// Kill the server mid-traffic, restart, assert no lost notes or jobs, and
/// the queue drains.
#[test]
fn c4_kill_mid_traffic_and_recover() {
    if child_mode_is("c4kill") {
        child_c4kill();
    }
    let _guard = lock_crash_harness();
    let dir = fresh_data_dir("kill");
    kill_child_at_ready("c4kill", "c4_kill_mid_traffic_and_recover", &dir);

    let (store, _blobs, svc) = open_stack(&dir);

    // 1. Every acknowledged note survived: 0 lost notes.
    let ledger_raw =
        fs::read_to_string(dir.join("acknowledged_notes.log")).expect("read ledger");
    let acked: Vec<String> = ledger_raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    assert!(
        acked.len() >= 15,
        "child must acknowledge at least 15 notes before the kill (got {})",
        acked.len()
    );
    for id in &acked {
        assert!(
            store
                .get_note(id)
                .unwrap_or_else(|e| panic!("get_note({id}): {e:?}"))
                .is_some(),
            "acknowledged note {id} lost across the kill"
        );
    }

    // 2. Every committed note still has its jobs: nothing was drained (no
    // worker ran pre-kill), so at least one pending job per acked note plus
    // the two follow jobs must exist.
    let pending = store.pending_count().expect("pending_count");
    assert!(
        pending >= acked.len(),
        "expected >= {} pending jobs (one per acked note), got {pending}",
        acked.len()
    );

    // 3. The queue drains completely: advance wall-clock past any active
    // lease, then drain until empty.
    let worker = open_worker(&store, &svc);
    drain_until_empty(&worker, &store);

    // 4. Draining produced bob's Follow, Mention, Reply, and Reaction
    // notifications.
    let bob = store
        .find_by_handle("bob")
        .expect("find bob")
        .expect("bob survives restart");
    let alice = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice survives restart");
    let notifs = store
        .list_notifications(&bob.id, 10_000)
        .expect("list bob notifications");
    for kind in [
        NotificationKind::Follow,
        NotificationKind::Mention,
        NotificationKind::Reply,
        NotificationKind::Reaction,
    ] {
        assert!(
            notifs.iter().any(|n| n.kind == kind),
            "bob is missing {kind:?} notification after drain (got {:?})",
            notifs.iter().map(|n| n.kind).collect::<Vec<_>>(),
        );
    }
    let follow = notifs
        .iter()
        .find(|n| n.kind == NotificationKind::Follow)
        .expect("follow notification");
    assert_eq!(
        follow.notifier_id, alice.id,
        "follow notification comes from alice"
    );
    assert!(
        notifs
            .iter()
            .any(|n| n.kind == NotificationKind::Mention && n.note_id.is_some()),
        "mention notification links its note"
    );
}

/// Media bytes used by the restart test (shared by child and parent: same
/// binary, one const).
const C4_RESTART_MEDIA_BYTES: &[u8] = b"c4-restart-media-payload-pinetree-phase1";

fn child_c4restart() -> ! {
    let dir = child_data_dir();
    let ready_file = dir.join("c4restart.ready");
    let (_store, _blobs, svc) = open_stack(&dir);

    let alice = svc.create_account("alice", None).expect("create alice");
    let bob = svc.create_account("bob", None).expect("create bob");
    svc.follow(&alice.id, &bob.id, false).expect("alice follows bob");

    let media = svc
        .upload_media(
            &alice.id,
            "avatar.png".to_string(),
            C4_RESTART_MEDIA_BYTES,
            Some("restart test file".to_string()),
            false,
            false,
        )
        .expect("upload media");

    let n1 = svc
        .create_note(CreateNoteInput::simple_public(&alice.id, "c4restart hello"))
        .expect("n1");
    let mut reply_input =
        CreateNoteInput::simple_public(&bob.id, "c4restart replying to alice");
    reply_input.reply_to = Some(n1.id.clone());
    let n2 = svc.create_note(reply_input).expect("n2");
    let n3 = svc
        .create_note(CreateNoteInput::simple_public(&alice.id, "@bob c4restart hi"))
        .expect("n3");
    let mut renote_input = CreateNoteInput::simple_public(&bob.id, "placeholder");
    renote_input.text = None;
    renote_input.renote_of = Some(n1.id.clone());
    let n4 = svc.create_note(renote_input).expect("n4");
    let n5 = svc
        .create_note(CreateNoteInput::simple_public(&bob.id, "c4restart bob note"))
        .expect("n5");

    let r1 = svc.react(&bob.id, &n1.id, "👍", false).expect("bob reacts to n1");
    let r2 = svc
        .react(&alice.id, &n5.id, "🎉", false)
        .expect("alice reacts to n5");

    let manifest = serde_json::json!({
        "alice_id": alice.id,
        "bob_id": bob.id,
        "notes": [n1.id, n2.id, n3.id, n4.id, n5.id],
        "media_id": media.id,
        "media_hash": media.hash,
        "media_blob_key": media.blob_key,
        "reactions": [
            {"user_id": r1.user_id, "note_id": r1.note_id, "reaction": r1.reaction},
            {"user_id": r2.user_id, "note_id": r2.note_id, "reaction": r2.reaction},
        ],
    });
    let manifest_path = dir.join("restart_state.json");
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&manifest_path)
        .expect("open manifest");
    f.write_all(serde_json::to_vec_pretty(&manifest).expect("encode manifest").as_slice())
        .expect("write manifest");
    f.flush().expect("flush manifest");
    f.sync_all().expect("sync manifest");
    drop(f);

    ready_and_park(&ready_file);
}

/// End-to-end restart check: accounts, follows, reactions, media blobs, and
/// home/local/hybrid timelines survive a process restart intact.
#[test]
fn c4_restart_preserves_accounts_follows_reactions_and_timelines() {
    if child_mode_is("c4restart") {
        child_c4restart();
    }
    let _guard = lock_crash_harness();
    let dir = fresh_data_dir("restart");
    kill_child_at_ready(
        "c4restart",
        "c4_restart_preserves_accounts_follows_reactions_and_timelines",
        &dir,
    );

    let (store, blobs, svc) = open_stack(&dir);
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(dir.join("restart_state.json")).expect("read manifest"),
    )
    .expect("parse manifest");
    let str_field = |key: &str| {
        manifest[key]
            .as_str()
            .unwrap_or_else(|| panic!("manifest missing {key}"))
            .to_string()
    };
    let alice_id = str_field("alice_id");
    let bob_id = str_field("bob_id");
    let media_id = str_field("media_id");
    let media_hash = str_field("media_hash");
    let media_blob_key = str_field("media_blob_key");
    let note_ids: Vec<String> = manifest["notes"]
        .as_array()
        .expect("manifest notes array")
        .iter()
        .map(|v| v.as_str().expect("note id string").to_string())
        .collect();
    assert_eq!(note_ids.len(), 5, "manifest carries all 5 notes");

    // Accounts survive with stable IDs.
    for (handle, id) in [("alice", &alice_id), ("bob", &bob_id)] {
        let by_handle = store
            .find_by_handle(handle)
            .unwrap_or_else(|e| panic!("find_by_handle({handle}): {e:?}"))
            .unwrap_or_else(|| panic!("{handle} missing after restart"));
        assert_eq!(&by_handle.id, id, "{handle} ID stable across restart");
        let by_id = store
            .find_by_id(id)
            .unwrap_or_else(|e| panic!("find_by_id({id}): {e:?}"))
            .expect("account resolvable by ID after restart");
        assert_eq!(by_id.id, *id);
    }

    // Follows survive.
    assert!(
        store
            .is_following(&alice_id, &bob_id)
            .expect("is_following"),
        "alice -> bob follow survives restart"
    );

    // Notes survive and stay readable through the query layer.
    for id in &note_ids {
        assert!(
            store
                .get_note(id)
                .unwrap_or_else(|e| panic!("get_note({id}): {e:?}"))
                .is_some(),
            "note {id} survives restart"
        );
    }
    let shown = svc
        .notes_show(&note_ids[0], Some(&bob_id))
        .expect("notes_show after restart");
    assert_eq!(shown.id, note_ids[0]);

    // Reactions survive.
    let stored_r1 = store
        .get_reaction(&bob_id, &note_ids[0])
        .expect("get_reaction bob->n1")
        .expect("bob's reaction survives restart");
    assert_eq!(stored_r1.reaction, "👍");
    let stored_r2 = store
        .get_reaction(&alice_id, &note_ids[4])
        .expect("get_reaction alice->n5")
        .expect("alice's reaction survives restart");
    assert_eq!(stored_r2.reaction, "🎉");

    // Media metadata + blob bytes survive.
    let meta = store
        .get(&media_id)
        .expect("get media meta")
        .expect("media metadata survives restart");
    assert_eq!(meta.hash, media_hash);
    assert_eq!(meta.blob_key, media_blob_key);
    assert_eq!(meta.owner_id.as_deref(), Some(alice_id.as_str()));
    let bytes = blobs
        .get(&media_blob_key)
        .expect("get blob")
        .expect("media blob survives restart");
    assert_eq!(
        bytes.as_slice(),
        C4_RESTART_MEDIA_BYTES,
        "blob bytes intact across restart"
    );

    // Timelines survive intact: every note is public local, so the local
    // timeline holds all five; alice's home holds her own notes plus bob's
    // (she follows him); bob's hybrid merges both sources.
    let cursor = TimelineCursor {
        limit: 100,
        ..Default::default()
    };
    let local = svc
        .local_timeline(Some(&alice_id), &cursor, false, true, true)
        .expect("local timeline");
    let local_ids: Vec<&str> = local.iter().map(|n| n.id.as_str()).collect();
    for id in &note_ids {
        assert!(
            local_ids.contains(&id.as_str()),
            "local timeline holds {id} after restart"
        );
    }
    let home = svc
        .home_timeline(&alice_id, &cursor, false, true)
        .expect("home timeline");
    let home_ids: Vec<&str> = home.iter().map(|n| n.id.as_str()).collect();
    for id in &note_ids {
        assert!(
            home_ids.contains(&id.as_str()),
            "alice home timeline holds {id} after restart"
        );
    }
    let hybrid = svc
        .hybrid_timeline(&bob_id, &cursor, false, true, true)
        .expect("hybrid timeline");
    let hybrid_ids: Vec<&str> = hybrid.iter().map(|n| n.id.as_str()).collect();
    for id in &note_ids {
        assert!(
            hybrid_ids.contains(&id.as_str()),
            "bob hybrid timeline holds {id} after restart"
        );
    }

    // The queue still drains after the restart (jobs were never lost).
    let worker = open_worker(&store, &svc);
    drain_until_empty(&worker, &store);
}
