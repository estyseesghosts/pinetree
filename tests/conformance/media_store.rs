use super::common::*;

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
        blob_key: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
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
    let stored = store.get_note(&note.id).expect("get").expect("note stored");
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
    assert!(store.find_by_id(&alice_id).expect("find").is_some());
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
    let cached2 = store
        .insert_metadata(remote2)
        .expect("second row, same URL");
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
        .attach_remote_media(
            "note:missing",
            valid_media_input(Some(alice.id.clone())),
            vec![],
        )
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
    assert_ne!(
        row_a.blob_key, row_b.blob_key,
        "distinct bytes → distinct keys"
    );
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
