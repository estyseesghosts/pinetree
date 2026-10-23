//! Wave 6 (Agent dedup-mark) replay-flood guard.
//!
//! `record_note_activity` used to abort the whole write transaction on
//! `DUPLICATE_NOTE_URI` / `TOMBSTONED_NOTE_URI`, dropping the new activity's
//! dedup mark. Every redelivery of that activity then ran another full note
//! insert attempt instead of short-circuiting on `already_seen`. The fix
//! commits the dedup mark on those two conflict outcomes, so the next
//! delivery returns `Ok(None)` from the replay gate.
//!
//! Fails on old code: after the conflicting second delivery,
//! `is_recorded(second_activity_id)` is `false` (the mark was aborted away),
//! so a replay flood keeps hitting the insert path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::ports::{
    AccountStore, ActivityDedup, CreateAccountInput, CreateNoteInput, JobKind, JobQueue, NewJob,
    NoteStore, TimelineCursor,
};
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

fn open_store() -> (Arc<RedbStore>, Arc<ManualClock>) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-wave6-dedup-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create wave6 dedup temp dir");
    let clock = Arc::new(ManualClock::new(BASE_MS));
    let clock_port: Arc<dyn Clock> = clock.clone();
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock_port)
        .expect("open wave6 dedup store");
    (store, clock)
}

fn local_user(store: &RedbStore, handle: &str) -> String {
    store
        .create_account(CreateAccountInput::local(handle), None)
        .expect("create local account")
        .id
}

fn note_input(author_id: &str, text: &str, uri: &str) -> CreateNoteInput {
    let mut input = CreateNoteInput::simple_public(author_id, text);
    input.uri = Some(uri.to_string());
    input
}

fn streaming_job(clock: &ManualClock, key: &str) -> NewJob {
    NewJob {
        kind: JobKind::StreamingPublish,
        payload: br#"{"note_id":"__PENDING_NOTE_ID__"}"#.to_vec(),
        idempotency_key: key.to_string(),
        not_before: clock.now_ms(),
    }
}

fn live_note_count(store: &RedbStore) -> usize {
    store
        .list_all_notes(&TimelineCursor {
            limit: 100,
            ..Default::default()
        })
        .expect("list notes")
        .len()
}

#[test]
fn wave6_duplicate_note_uri_commits_dedup_mark() {
    let (store, clock) = open_store();
    let alice = local_user(&store, "alice");
    let uri = "https://remote.example/notes/wave6-dedup-mark-1";

    let first = store
        .record_note_activity(
            "wave6-dedup-mark:A1",
            note_input(&alice, "first delivery wins", uri),
            vec![streaming_job(&clock, "wave6-dedup:job-a1")],
        )
        .expect("first delivery commits");
    assert!(
        first.is_some(),
        "first delivery of a fresh URI stores the note"
    );
    assert_eq!(
        live_note_count(&store),
        1,
        "one live note after first delivery"
    );
    assert_eq!(
        store.pending_count().expect("pending count"),
        1,
        "first delivery enqueues exactly one job"
    );

    // Same note URI under a *different* activity id: the note insert hits
    // `DUPLICATE_NOTE_URI`, which must still commit this activity's dedup
    // mark (and enqueue nothing).
    let replay = store
        .record_note_activity(
            "wave6-dedup-mark:B1",
            note_input(&alice, "re-wrapped copy of the same note", uri),
            Vec::new(),
        )
        .expect("duplicate-URI delivery converges to Ok(None)");
    assert_eq!(replay, None, "duplicate URI is a replay, not a second note");
    assert!(
        store
            .is_recorded("wave6-dedup-mark:B1")
            .expect("is_recorded"),
        "conflicting delivery must leave its dedup mark committed"
    );
    assert_eq!(
        live_note_count(&store),
        1,
        "replay flood must not store a second note"
    );
    assert_eq!(
        store.pending_count().expect("pending count"),
        1,
        "replay must not enqueue a duplicate job"
    );

    // The flood itself: redelivering B1 is now `already_seen` without
    // touching the note insert path again.
    let flood = store
        .record_note_activity(
            "wave6-dedup-mark:B1",
            note_input(&alice, "re-wrapped copy of the same note", uri),
            Vec::new(),
        )
        .expect("flood redelivery is already_seen");
    assert_eq!(flood, None, "flood redelivery stays a no-op");
    assert_eq!(
        live_note_count(&store),
        1,
        "note count is stable under flood"
    );
    assert_eq!(
        store.pending_count().expect("pending count"),
        1,
        "job count is stable under flood"
    );
}

#[test]
fn wave6_tombstoned_note_uri_commits_dedup_mark() {
    let (store, _clock) = open_store();
    let alice = local_user(&store, "alice");
    let uri = "https://remote.example/notes/wave6-dedup-mark-2";

    // `Delete` overtakes `Create`: no note is held, so a tombstone is
    // planted (returns `false` — nothing was deleted).
    assert_eq!(
        store
            .record_delete_uri_activity("wave6-dedup-mark:del-t", uri, Vec::new())
            .expect("tombstone plants"),
        false,
        "delete with no note held records a tombstone"
    );

    // A late copy of the same URI hits `TOMBSTONED_NOTE_URI`: still commit
    // the new activity's dedup mark so its own flood is `already_seen`.
    let late = store
        .record_note_activity(
            "wave6-dedup-mark:late-t",
            note_input(&alice, "late copy of a deleted note", uri),
            Vec::new(),
        )
        .expect("tombstoned delivery converges to Ok(None)");
    assert_eq!(late, None, "tombstoned URI is a replay, not a new note");
    assert!(
        store
            .is_recorded("wave6-dedup-mark:late-t")
            .expect("is_recorded"),
        "tombstoned delivery must leave its dedup mark committed"
    );

    let flood = store
        .record_note_activity(
            "wave6-dedup-mark:late-t",
            note_input(&alice, "late copy of a deleted note", uri),
            Vec::new(),
        )
        .expect("flood redelivery is already_seen");
    assert_eq!(flood, None, "flood redelivery stays a no-op");
    assert_eq!(
        live_note_count(&store),
        0,
        "no note is resurrected by flood"
    );
}
