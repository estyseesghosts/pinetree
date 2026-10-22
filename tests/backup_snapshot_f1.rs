//! Batch F (`docs/prod-ready.md` §7 F6): live backup + offline restore.
//!
//! Hermetic (temp dirs only): seed a real `RedbStore` + real pools, back up
//! under concurrent writes, restore into a fresh dir, and verify via direct
//! store reads. Corrupt/incomplete/unsupported bundles must refuse.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::ports::{
    AccountStore, ActivityDedup, ActorKeypairRecord, ApiTokenRecord, AuthStore, BlobStore,
    BlockStore, CreateAccountInput, CreateNoteInput, FollowStore, InsertMediaInput, JobKind,
    JobQueue, MediaStore, NewJob, NoteStore, PollStore, ReactionStore, RegistryItem, RegistryStore,
};
use pinetree::backup::bundle::{Manifest, compute_overall, decode_table_file, table_entry_name};
use pinetree::backup::{
    BackupContext, BackupError, BackupState, backup_status, cleanup_abandoned_partials,
    list_backup_statuses, request_backup, restore_bundle, run_backup_blocking, run_next_backup,
};
use pinetree::domain::blocking::Blocking;
use pinetree::domain::id::ManualClock;
use pinetree::domain::media::MediaProperties;
use pinetree::domain::poll::{PollInput, PollVote};
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::storage::{FsBlobStore, acquire_backup_gate, media_gate_snapshot};
use pinetree::persistence::redb::{
    MAX_MEDIA_PAGE_ROWS, RedbStore, backup_meta_marker_keys, backup_table_names,
};

const BASE_MS: i64 = 1_760_400_000_000;
static HARNESS_SEQ: AtomicU64 = AtomicU64::new(0);

/// Serializes the tests that run a backup or hold the media gate: the gate
/// is process-global and exclusive, so two such tests overlapping in time
/// would see spurious `Busy` failures. (Each `#[test]` still uses hermetic
/// temp dirs; this only orders the gate-holding sections.)
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Harness {
    store: Arc<RedbStore>,
    durable: FsBlobStore,
    cache: FsBlobStore,
    ctx: BackupContext,
    dir: PathBuf,
    backup_dir: PathBuf,
}

fn harness(tag: &str) -> Harness {
    let n = HARNESS_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-backup-f1-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock = Arc::new(ManualClock::new(BASE_MS));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock).unwrap();
    let durable = FsBlobStore::new(dir.join("blobs_durable")).unwrap();
    let cache = FsBlobStore::new(dir.join("blobs_cache")).unwrap();
    let backup_dir = dir.join("backups");
    let ctx = BackupContext {
        store: store.clone(),
        durable_pool: dir.join("blobs_durable"),
        cache_pool: dir.join("blobs_cache"),
        backup_dir: backup_dir.clone(),
        max_bundle_bytes: None,
        retention_max_count: None,
        retention_max_bytes: None,
    };
    Harness {
        store,
        durable,
        cache,
        ctx,
        dir,
        backup_dir,
    }
}

struct Seed {
    alice: String,
    bob: String,
    note: String,
    poll_note: String,
    blob_key: String,
    cache_key: String,
}

fn seed(h: &Harness) -> Seed {
    let alice = h
        .store
        .create_account(CreateAccountInput::local("f1_alice"), None)
        .unwrap()
        .id;
    let bob = h
        .store
        .create_account(CreateAccountInput::local("f1_bob"), None)
        .unwrap()
        .id;
    let note = h
        .store
        .create_note(
            CreateNoteInput::simple_public(&alice, "backup seed note"),
            vec![],
        )
        .unwrap()
        .id;
    let mut poll_input = CreateNoteInput::simple_public(&alice, "backup poll note");
    poll_input.has_poll = true;
    poll_input.poll = Some(PollInput {
        choices: vec!["yes".to_string(), "no".to_string()],
        multiple: false,
        expires_at_ms: None,
    });
    let poll_note = h.store.create_note(poll_input, vec![]).unwrap().id;
    h.store.follow(&alice, &bob, false, None, vec![]).unwrap();
    h.store
        .block(
            Blocking {
                id: "f1blk1".to_string(),
                blocker_id: bob.clone(),
                blockee_id: alice.clone(),
            },
            vec![],
        )
        .unwrap();
    // A reaction from alice on her own note (no block in that direction).
    h.store
        .react(&alice, &note, "?", "f1react1", false, vec![])
        .unwrap();
    h.store
        .cast_vote(
            PollVote {
                id: "f1vote1".to_string(),
                note_id: poll_note.clone(),
                user_id: bob.clone(),
                choice: 0,
            },
            vec![],
        )
        .unwrap();
    // Durable media row + bytes.
    let bytes = b"backup-seed-durable-bytes";
    let blob_key = sha256_hex(bytes);
    h.durable.put(&blob_key, bytes).unwrap();
    h.store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: Some(alice.clone()),
            name: "seed.bin".to_string(),
            mime: "application/octet-stream".to_string(),
            size_bytes: bytes.len() as u64,
            hash: md5_hex(bytes),
            blob_key: blob_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: "https://example.local/files/seed.bin".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: None,
            max_file_size: 10 * 1024 * 1024,
        })
        .unwrap();
    // Cache-style row + bytes (owner-less, remote origin).
    let cbytes = b"backup-seed-cache-bytes";
    let cache_key = sha256_hex(cbytes);
    h.cache.put(&cache_key, cbytes).unwrap();
    h.store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "application/octet-stream".to_string(),
            size_bytes: cbytes.len() as u64,
            hash: md5_hex(cbytes),
            blob_key: cache_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: "https://example.local/files/remote.bin".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some("https://remote.example/f1.png".to_string()),
            max_file_size: 10 * 1024 * 1024,
        })
        .unwrap();
    // Key material (actor keypair + token), a durable job, dedup state, and
    // one registry row (instance/client state).
    h.store
        .save_actor_keypair(ActorKeypairRecord {
            account_id: alice.clone(),
            public_key_pem: "f1-public".to_string(),
            private_key_pem: "f1-private".to_string(),
            created_at_ms: BASE_MS,
        })
        .unwrap();
    h.store
        .store_token(ApiTokenRecord {
            id: "f1tokid".to_string(),
            token_hash: "f1tokenhash".to_string(),
            account_id: alice.clone(),
            permissions: vec!["read".to_string()],
            is_first_party: true,
            name: None,
            description: None,
            icon_url: None,
            created_at_ms: BASE_MS,
        })
        .unwrap();
    h.store
        .enqueue_jobs(vec![NewJob {
            kind: JobKind::StreamingPublish,
            payload: b"{}".to_vec(),
            idempotency_key: "f1:job1".to_string(),
            not_before: BASE_MS,
        }])
        .unwrap();
    assert!(!h.store.check_and_record("f1:activity:1").unwrap());
    h.store
        .registry_set(RegistryItem {
            id: "f1reg1".to_string(),
            user_id: alice.clone(),
            domain: None,
            scope: vec!["client".to_string()],
            key: "theme".to_string(),
            value: serde_json::json!("dark"),
            updated_at_ms: BASE_MS,
        })
        .unwrap();
    Seed {
        alice,
        bob,
        note,
        poll_note,
        blob_key,
        cache_key,
    }
}

/// Full row multiset of a store: `(table, key) -> value`.
fn export_all(store: &RedbStore) -> BTreeMap<(String, Vec<u8>), Vec<u8>> {
    let mut out = BTreeMap::new();
    store
        .export_backup_snapshot(&mut |table, key, value| {
            let prev = out.insert((table.to_string(), key), value);
            assert!(prev.is_none(), "duplicate row in table {table}");
            Ok(())
        })
        .unwrap();
    out
}

fn pool_inventory(pool: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    let objects = pool.join("objects");
    if !objects.is_dir() {
        return out;
    }
    let mut shards: Vec<PathBuf> = std::fs::read_dir(&objects)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    shards.sort();
    for shard in shards {
        if !shard.is_dir() {
            continue;
        }
        let mut files: Vec<PathBuf> = std::fs::read_dir(&shard)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        files.sort();
        for file in files {
            if !file.is_file() {
                continue;
            }
            let rel = format!(
                "{}/{}",
                shard.file_name().unwrap().to_string_lossy(),
                file.file_name().unwrap().to_string_lossy()
            );
            out.insert(rel, std::fs::read(&file).unwrap());
        }
    }
    out
}

// --- F1: registry completeness -------------------------------------------

#[test]
fn f1_registry_covers_every_live_table_and_no_multimaps() {
    let h = harness("registry");
    seed(&h);
    let mut live = h.store.live_table_names().unwrap();
    let mut registry = backup_table_names();
    live.sort();
    registry.sort();
    assert_eq!(live, registry, "every live table must have backup coverage");
    assert!(
        h.store.live_multimap_names().unwrap().is_empty(),
        "schema uses no multimaps; a new one must fail this test until backup supports it"
    );
    // Every registry table streams through export (no missing dispatch arm).
    let counts = h
        .store
        .export_backup_snapshot(&mut |_, _, _| Ok(()))
        .unwrap();
    let exported: BTreeSet<String> = counts.into_iter().map(|c| c.name).collect();
    let expected: BTreeSet<String> = registry.into_iter().map(str::to_string).collect();
    assert_eq!(exported, expected);
}

// --- F2/F3/F5: seed → backup → restore → verify ---------------------------

#[test]
fn f1_live_backup_restores_everything_verbatim() {
    let _serial = serial();
    let h = harness("roundtrip");
    let s = seed(&h);

    let job_id = request_backup(&h.ctx).unwrap();
    let status = backup_status(&h.ctx, &job_id).unwrap();
    assert_eq!(status.state, BackupState::Queued);
    let bundle = run_backup_blocking(&h.ctx, &job_id).unwrap();
    let status = backup_status(&h.ctx, &job_id).unwrap();
    assert_eq!(status.state, BackupState::Completed);
    assert!(status.bytes_written.unwrap() > 0);
    // The bundle materializes every registry table (empty ones as zero-row
    // files) so restore can validate the full table set.
    assert_eq!(status.tables_exported as usize, backup_table_names().len());
    assert!(status.media_files >= 4, "objects + sidecars of both pools");
    assert!(bundle.exists());

    // No staging file may survive a successful publish.
    for entry in std::fs::read_dir(&h.backup_dir)
        .unwrap()
        .filter_map(|e| e.ok())
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        assert!(!name.starts_with(".partial-"), "staging file left behind");
    }

    let fresh = h.dir.join("restored");
    let report = restore_bundle(&bundle, &fresh).unwrap();
    assert_eq!(report.tables_restored as usize, backup_table_names().len());
    assert!(report.rows_total > 0);
    assert!(fresh.join("RESTORE_READY.json").exists());

    // Direct store reads: identical row multisets, refcounts, owner bytes.
    let clock = Arc::new(ManualClock::new(BASE_MS));
    let restored = RedbStore::open_with_default_ids(fresh.join("pinetree.redb"), clock).unwrap();
    assert_eq!(export_all(&h.store), export_all(&restored));
    assert_eq!(
        h.store.blob_refcount(&s.blob_key).unwrap(),
        restored.blob_refcount(&s.blob_key).unwrap()
    );
    assert_eq!(
        h.store.durable_bytes(&s.alice).unwrap(),
        restored.durable_bytes(&s.alice).unwrap()
    );
    // Domain rows resolve on the restored store.
    assert!(restored.get_note(&s.note).unwrap().is_some());
    assert!(restored.get_poll(&s.poll_note).unwrap().is_some());
    // The seed blocks bob→alice after following alice→bob, and block drops
    // follow edges in both directions: absence must round-trip faithfully,
    // while the block itself survives.
    assert!(!h.store.is_following(&s.alice, &s.bob).unwrap());
    assert!(!restored.is_following(&s.alice, &s.bob).unwrap());
    assert!(restored.is_blocking(&s.bob, &s.alice).unwrap());
    assert!(restored.get_reaction(&s.alice, &s.note).unwrap().is_some());
    assert!(restored.get_actor_keypair(&s.alice).unwrap().is_some());
    assert!(restored.is_recorded("f1:activity:1").unwrap());
    // get_job is keyed by generated job id (not the idempotency key), so
    // compare the actionable queue depth instead: the seeded job survives.
    assert_eq!(h.store.pending_count().unwrap(), 1);
    assert_eq!(restored.pending_count().unwrap(), 1);
    // Batch C META migration markers survive the round-trip.
    let source_rows = export_all(&h.store);
    for marker in backup_meta_marker_keys() {
        let key = ("meta".to_string(), marker.as_bytes().to_vec());
        if let Some(value) = source_rows.get(&key) {
            let restored_rows = export_all(&restored);
            assert_eq!(
                restored_rows.get(&key),
                Some(value),
                "marker {marker} must survive"
            );
        }
    }
    // Pools: byte-identical trees incl. sidecars.
    assert_eq!(
        pool_inventory(&h.dir.join("blobs_durable")),
        pool_inventory(&fresh.join("blobs_durable"))
    );
    assert_eq!(
        pool_inventory(&h.dir.join("blobs_cache")),
        pool_inventory(&fresh.join("blobs_cache"))
    );
    // Restored accounting is consistent by Batch C's own math.
    assert!(restored.verify_storage_accounting().unwrap().is_empty());
    // Restored durable bytes are servable with valid sidecars.
    let probe = FsBlobStore::new(fresh.join("blobs_durable")).unwrap();
    assert_eq!(
        probe.byte_len(&s.blob_key).unwrap(),
        Some(b"backup-seed-durable-bytes".len() as u64)
    );
    let cache_probe = FsBlobStore::new(fresh.join("blobs_cache")).unwrap();
    assert_eq!(
        cache_probe.byte_len(&s.cache_key).unwrap(),
        Some(b"backup-seed-cache-bytes".len() as u64)
    );
}

#[test]
fn f1_live_backup_under_concurrent_writes_stays_coherent() {
    let _serial = serial();
    let h = Arc::new(harness("concurrent"));
    let s = seed(&h);
    let ctx = h.ctx.clone();

    // REST-ish churn on threads: notes, media puts, deletes of own rows.
    let mut threads = Vec::new();
    for worker in 0..4 {
        let store = h.ctx.store.clone();
        let durable_pool = h.ctx.durable_pool.clone();
        threads.push(std::thread::spawn(move || {
            let pool = FsBlobStore::new(&durable_pool).unwrap();
            for i in 0..25 {
                let user = store
                    .create_account(CreateAccountInput::local(format!("f1w{worker}u{i}")), None)
                    .unwrap();
                let note = store
                    .create_note(
                        CreateNoteInput::simple_public(&user.id, format!("churn {worker}/{i}")),
                        vec![],
                    )
                    .unwrap();
                let bytes = format!("churn-bytes-{worker}-{i}").into_bytes();
                let key = sha256_hex(&bytes);
                pool.put(&key, &bytes).unwrap();
                let meta = store
                    .insert_metadata(InsertMediaInput {
                        id: None,
                        owner_id: Some(user.id.clone()),
                        name: "churn.bin".to_string(),
                        mime: "application/octet-stream".to_string(),
                        size_bytes: bytes.len() as u64,
                        hash: md5_hex(&bytes),
                        blob_key: key.clone(),
                        blurhash: None,
                        properties: MediaProperties::default(),
                        is_sensitive: false,
                        is_link: false,
                        url: "https://example.local/files/churn.bin".to_string(),
                        thumbnail_url: None,
                        comment: None,
                        source_url: None,
                        max_file_size: 10 * 1024 * 1024,
                    })
                    .unwrap();
                // Delete every other row again (metadata + bytes, gated).
                if i % 2 == 0 {
                    store.delete_metadata(&meta.id).unwrap();
                    pool.delete(&key).unwrap();
                }
                assert!(store.get_note(&note.id).unwrap().is_some());
            }
        }));
    }
    let job_id = request_backup(&ctx).unwrap();
    let bundle = run_backup_blocking(&ctx, &job_id).unwrap();
    for thread in threads {
        thread.join().unwrap();
    }
    // Coherence proof: the bundle restores cleanly (restore refuses any
    // bundle whose snapshot references missing bytes).
    let fresh = h.dir.join("restored-concurrent");
    let report = restore_bundle(&bundle, &fresh).unwrap();
    assert!(report.rows_total > 0);
    // Pre-churn seed rows are intact on the restored side.
    let clock = Arc::new(ManualClock::new(BASE_MS));
    let restored = RedbStore::open_with_default_ids(fresh.join("pinetree.redb"), clock).unwrap();
    assert!(restored.get_note(&s.note).unwrap().is_some());
    // Seed blocks after following (block drops both follow directions), so
    // the follow must be absent on both sides while the block survives.
    assert!(!restored.is_following(&s.alice, &s.bob).unwrap());
    assert!(restored.is_blocking(&s.bob, &s.alice).unwrap());
    let drift = restored.verify_storage_accounting().unwrap();
    assert!(drift.is_empty(), "restored accounting drifts: {drift:?}");
    // Media ops waited at most (never failed) behind the gate.
    let snapshot = media_gate_snapshot();
    assert!(snapshot.ops_total > 0);
}

#[test]
fn f1_backup_drain_queue_and_status_lifecycle() {
    let _serial = serial();
    let h = harness("drain");
    seed(&h);
    let first = request_backup(&h.ctx).unwrap();
    let second = request_backup(&h.ctx).unwrap();
    assert_ne!(first, second);
    assert_eq!(list_backup_statuses(&h.backup_dir).len(), 2);
    assert_eq!(run_next_backup(&h.ctx).unwrap(), Some(first.clone()));
    assert_eq!(run_next_backup(&h.ctx).unwrap(), Some(second.clone()));
    assert_eq!(run_next_backup(&h.ctx).unwrap(), None);
    assert_eq!(
        backup_status(&h.ctx, &first).unwrap().state,
        BackupState::Completed
    );
    assert_eq!(
        backup_status(&h.ctx, &second).unwrap().state,
        BackupState::Completed
    );
    assert!(backup_status(&h.ctx, "bk-nope").is_none());
}

#[test]
fn f1_exclusive_gate_rejects_second_backup_and_blocks_media() {
    let _serial = serial();
    let _held = acquire_backup_gate("f1-holder").unwrap();
    let busy = acquire_backup_gate("f1-second").unwrap_err();
    assert_eq!(busy.active_backup_id, "f1-holder");
    // A media op from another thread waits (never fails) behind the gate.
    let dir = std::env::temp_dir().join(format!("pinetree-backup-f1-gate-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let pool = FsBlobStore::new(dir.join("pool")).unwrap();
    let before = media_gate_snapshot().wait_ms_total;
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done_child = done.clone();
    let child = std::thread::spawn(move || {
        pool.put(&sha256_hex(b"gated-bytes"), b"gated-bytes")
            .unwrap();
        done_child.store(true, Ordering::SeqCst);
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        !done.load(Ordering::SeqCst),
        "media op must wait behind the gate"
    );
    drop(_held);
    child.join().unwrap();
    assert!(done.load(Ordering::SeqCst));
    assert!(media_gate_snapshot().wait_ms_total >= before);
    let _ = std::fs::remove_dir_all(&dir);
}

// --- Corruption / version / traversal: restore refuses --------------------

/// Rewrite a bundle entry-by-entry, optionally replacing entry bytes.
type ZipReplace<'a> = dyn Fn(&str, Vec<u8>) -> Option<Vec<u8>> + 'a;
fn rewrite_zip(src: &Path, dst: &Path, replace: &ZipReplace<'_>) {
    let file = std::fs::File::open(src).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();
    let out = std::fs::File::create(dst).unwrap();
    let mut writer = zip::ZipWriter::new(out);
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        let name = entry.name().to_string();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        drop(entry);
        let bytes = replace(&name, bytes).unwrap_or_else(|| {
            let mut original = Vec::new();
            archive
                .by_index(i)
                .unwrap()
                .read_to_end(&mut original)
                .unwrap();
            original
        });
        writer.start_file(&name, options).unwrap();
        writer.write_all(&bytes).unwrap();
    }
    writer.finish().unwrap();
}

fn read_manifest(src: &Path) -> Manifest {
    let file = std::fs::File::open(src).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();
    let mut entry = archive.by_name("manifest.json").unwrap();
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn good_bundle(tag: &str) -> (Harness, PathBuf) {
    let h = harness(tag);
    seed(&h);
    let job_id = request_backup(&h.ctx).unwrap();
    let bundle = run_backup_blocking(&h.ctx, &job_id).unwrap();
    (h, bundle)
}

#[test]
fn f1_restore_refuses_corrupt_manifest() {
    let _serial = serial();
    let (h, bundle) = good_bundle("corrupt-manifest");
    let bad = h.dir.join("bad-manifest.zip");
    rewrite_zip(&bundle, &bad, &|name, mut bytes| {
        if name == "manifest.json" {
            let pos = bytes.len() / 2;
            bytes[pos] ^= 0xFF;
            Some(bytes)
        } else {
            None
        }
    });
    let err = restore_bundle(&bad, &h.dir.join("fresh")).unwrap_err();
    assert!(matches!(err, BackupError::Corrupt(_)), "got {err:?}");
    assert!(
        !h.dir.join("fresh").exists(),
        "partial restore must be removed"
    );
}

#[test]
fn f1_restore_refuses_corrupt_table_payload() {
    let _serial = serial();
    let (h, bundle) = good_bundle("corrupt-table");
    let manifest = read_manifest(&bundle);
    // Corrupt the payload of the first NON-EMPTY table (empty tables are
    // magic-only files with no payload bytes to flip).
    let target_table = manifest
        .tables
        .iter()
        .find(|t| t.rows > 0)
        .expect("seeded bundle must contain a non-empty table");
    let target = table_entry_name(&target_table.name);
    // Sanity: the table codec round-trips on valid bytes.
    {
        let file = std::fs::File::open(&bundle).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let mut entry = archive.by_name(&target).unwrap();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        let rows = decode_table_file(&bytes, target_table.rows).unwrap();
        assert_eq!(rows.len() as u64, target_table.rows);
    }
    let bad = h.dir.join("bad-table.zip");
    rewrite_zip(&bundle, &bad, &|name, mut bytes| {
        if name == target && bytes.len() > 20 {
            bytes[20] ^= 0xFF;
            Some(bytes)
        } else {
            None
        }
    });
    let err = restore_bundle(&bad, &h.dir.join("fresh")).unwrap_err();
    assert!(matches!(err, BackupError::Corrupt(_)), "got {err:?}");
}

#[test]
fn f1_restore_refuses_corrupt_blob() {
    let _serial = serial();
    let (h, bundle) = good_bundle("corrupt-blob");
    let manifest = read_manifest(&bundle);
    let target = manifest
        .media_files
        .iter()
        .find(|f| !f.path.ends_with(".len"))
        .map(|f| f.path.clone())
        .unwrap();
    let bad = h.dir.join("bad-blob.zip");
    rewrite_zip(&bundle, &bad, &|name, mut bytes| {
        if name == target && !bytes.is_empty() {
            bytes[0] ^= 0xFF;
            Some(bytes)
        } else {
            None
        }
    });
    let err = restore_bundle(&bad, &h.dir.join("fresh")).unwrap_err();
    assert!(matches!(err, BackupError::Corrupt(_)), "got {err:?}");
}

#[test]
fn f1_restore_refuses_corrupt_sidecar_and_bad_checksum() {
    let _serial = serial();
    let (h, bundle) = good_bundle("corrupt-sidecar");
    let manifest = read_manifest(&bundle);
    let target = manifest
        .media_files
        .iter()
        .find(|f| f.path.ends_with(".len"))
        .map(|f| f.path.clone())
        .unwrap();
    // Corrupt sidecar content (checksum entry untouched → checksum refusal).
    let bad = h.dir.join("bad-sidecar.zip");
    rewrite_zip(&bundle, &bad, &|name, _bytes| {
        if name == target {
            Some(b"999999999".to_vec())
        } else {
            None
        }
    });
    let err = restore_bundle(&bad, &h.dir.join("fresh-a")).unwrap_err();
    assert!(matches!(err, BackupError::Corrupt(_)), "got {err:?}");
    // Tampered manifest checksum field (overall untouched → overall refusal).
    let bad2 = h.dir.join("bad-checksum.zip");
    rewrite_zip(&bundle, &bad2, &|name, bytes| {
        if name == "manifest.json" {
            let mut manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
            manifest.tables[0].sha256 = "00".repeat(32);
            Some(serde_json::to_vec_pretty(&manifest).unwrap())
        } else {
            None
        }
    });
    let err = restore_bundle(&bad2, &h.dir.join("fresh-b")).unwrap_err();
    assert!(matches!(err, BackupError::Corrupt(_)), "got {err:?}");
}

#[test]
fn f1_restore_refuses_unsupported_version() {
    let _serial = serial();
    let (h, bundle) = good_bundle("version");
    let bad = h.dir.join("bad-version.zip");
    rewrite_zip(&bundle, &bad, &|name, bytes| {
        if name == "manifest.json" {
            let mut manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
            manifest.format_version = 999;
            manifest.overall_sha256 = compute_overall(
                &manifest.snapshot_id,
                &manifest.tables,
                &manifest.media_files,
            );
            Some(serde_json::to_vec_pretty(&manifest).unwrap())
        } else {
            None
        }
    });
    let err = restore_bundle(&bad, &h.dir.join("fresh")).unwrap_err();
    assert!(
        matches!(err, BackupError::UnsupportedVersion(_)),
        "got {err:?}"
    );
}

#[test]
fn f1_restore_refuses_traversal_and_absolute_entries() {
    let _serial = serial();
    let (h, bundle) = good_bundle("traversal");
    for (tag, evil) in [("trav", "../evil"), ("abs", "/abs-evil")] {
        let evil_bundle = h.dir.join(format!("evil-{tag}.zip"));
        {
            let file = std::fs::File::open(&bundle).unwrap();
            let mut archive = zip::ZipArchive::new(file).unwrap();
            let out = std::fs::File::create(&evil_bundle).unwrap();
            let mut writer = zip::ZipWriter::new(out);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            for i in 0..archive.len() {
                let mut entry = archive.by_index(i).unwrap();
                let name = entry.name().to_string();
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes).unwrap();
                writer.start_file(&name, options).unwrap();
                writer.write_all(&bytes).unwrap();
            }
            writer.start_file(evil, options).unwrap();
            writer.write_all(b"evil").unwrap();
            writer.finish().unwrap();
        }
        let err = restore_bundle(&evil_bundle, &h.dir.join(format!("fresh-{tag}"))).unwrap_err();
        assert!(matches!(err, BackupError::Corrupt(_)), "{tag}: got {err:?}");
    }
}

#[test]
fn f1_restore_refuses_missing_table_component() {
    let _serial = serial();
    let (h, bundle) = good_bundle("missing-table");
    let manifest = read_manifest(&bundle);
    let drop_target = table_entry_name(&manifest.tables[0].name);
    // Drop one table entry but keep the manifest (entry-set mismatch).
    let bad = h.dir.join("bad-missing.zip");
    {
        let file = std::fs::File::open(&bundle).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let out = std::fs::File::create(&bad).unwrap();
        let mut writer = zip::ZipWriter::new(out);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for i in 0..archive.len() {
            let mut entry = archive.by_index(i).unwrap();
            let name = entry.name().to_string();
            if name == drop_target {
                continue;
            }
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).unwrap();
            writer.start_file(&name, options).unwrap();
            writer.write_all(&bytes).unwrap();
        }
        writer.finish().unwrap();
    }
    let err = restore_bundle(&bad, &h.dir.join("fresh")).unwrap_err();
    assert!(matches!(err, BackupError::Corrupt(_)), "got {err:?}");
}

// --- Kill-mid-backup: no final artifact + startup cleanup ---------------

#[test]
fn f1_killed_backup_leaves_no_final_artifact_and_cleanup_repairs() {
    let _serial = serial();
    let h = harness("kill");
    seed(&h);
    // Accepted but never run: no final bundle may exist.
    let pending = request_backup(&h.ctx).unwrap();
    assert!(backup_status(&h.ctx, &pending).is_some());
    assert!(find_bundle_opt(&h.backup_dir).is_none());

    // Simulate a kill mid-run: staging output + a stale Running record.
    let staging = h.backup_dir.join(format!(".partial-{pending}.zip"));
    std::fs::write(&staging, b"incomplete-bundle-bytes").unwrap();
    let running_path = h.backup_dir.join(format!("{pending}.status.json"));
    let mut status: pinetree::backup::BackupStatus =
        serde_json::from_slice(&std::fs::read(&running_path).unwrap()).unwrap();
    status.state = BackupState::Running;
    std::fs::write(&running_path, serde_json::to_vec_pretty(&status).unwrap()).unwrap();

    let report = cleanup_abandoned_partials(&h.backup_dir).unwrap();
    assert_eq!(report.partials_removed, 1);
    assert_eq!(report.stale_running_failed, 1);
    assert!(!staging.exists());
    assert!(find_bundle_opt(&h.backup_dir).is_none());
    let status = backup_status(&h.ctx, &pending).unwrap();
    assert_eq!(status.state, BackupState::Failed);

    // The worker can still back up cleanly afterwards.
    let next = request_backup(&h.ctx).unwrap();
    let bundle = run_backup_blocking(&h.ctx, &next).unwrap();
    assert!(bundle.exists());
}

fn find_bundle_opt(backup_dir: &Path) -> Option<PathBuf> {
    std::fs::read_dir(backup_dir)
        .ok()?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().and_then(|s| s.to_str()) == Some("zip"))
}

// --- Status hygiene: no secrets ------------------------------------------

#[test]
fn f1_failure_reasons_carry_no_secrets() {
    let redacted =
        pinetree::backup::redact_failure("boom token=abc123 private_key=zzz password=hunter2");
    assert!(!redacted.contains("abc123"));
    assert!(!redacted.contains("hunter2"));
    assert!(!redacted.contains("zzz"));
    // Unknown job status is None (never an error-shaped leak).
    let h = harness("redact");
    assert!(backup_status(&h.ctx, "bk-missing").is_none());
    let err = run_backup_blocking(&h.ctx, "bk-missing").unwrap_err();
    assert!(matches!(err, BackupError::Invalid(_)));
}

// --- Paging-constant sanity (shared contract, Batch C owns the value) -----

#[test]
fn f1_media_page_constant_is_sane() {
    // Tripwire against lowering the shared paging contract below
    // multi-page-walk coverage (const-evaluable by construction).
    #[allow(clippy::assertions_on_constants)]
    {
        assert!(MAX_MEDIA_PAGE_ROWS >= 100, "must cover multi-page walks");
    }
}
