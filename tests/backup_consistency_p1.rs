//! Phase 3 (P1) backup consistency: snapshot/inventory atomicity, missing
//! blobs fail publishing, evicted rows need no bytes, malicious bundles
//! refuse, restore round-trips into a fresh dir with `RESTORE_READY.json`
//! written last.
//!
//! Hermetic (temp dirs only). Serializes gate-holding tests on a process
//! global mutex because the media backup gate is exclusive.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::ports::{
    AccountStore, BlobStore, CreateAccountInput, CreateNoteInput, InsertMediaInput, MediaStore,
    NoteStore,
};
use pinetree::backup::{
    BackupContext, BackupError, BackupPolicy, BackupState, backup_status,
    create_bundle_with_policy, request_backup, restore_bundle, restore_bundle_with_policy,
    run_backup_blocking,
};
use pinetree::domain::id::ManualClock;
use pinetree::domain::media::MediaProperties;
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::persistence::redb::RedbStore;

static HARNESS_SEQ: AtomicU64 = AtomicU64::new(0);
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Harness {
    store: Arc<RedbStore>,
    ctx: BackupContext,
    dir: PathBuf,
    backup_dir: PathBuf,
}

fn harness(tag: &str) -> Harness {
    let n = HARNESS_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-backup-p1-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock = Arc::new(ManualClock::new(1_760_400_000_000));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock).unwrap();
    let durable_pool = dir.join("blobs_durable");
    let cache_pool = dir.join("blobs_cache");
    std::fs::create_dir_all(durable_pool.join("objects")).unwrap();
    std::fs::create_dir_all(cache_pool.join("objects")).unwrap();
    let backup_dir = dir.join("backups");
    let ctx = BackupContext {
        store: store.clone(),
        durable_pool: durable_pool.clone(),
        cache_pool: cache_pool.clone(),
        backup_dir: backup_dir.clone(),
        max_bundle_bytes: None,
        retention_max_count: None,
        retention_max_bytes: None,
    };
    Harness {
        store,
        ctx,
        dir,
        backup_dir,
    }
}

fn seed_media(h: &Harness, owner: Option<String>, name: &str, bytes: &[u8]) -> (String, String) {
    use pinetree::media::storage::FsBlobStore;
    let blob_key = sha256_hex(bytes);
    let pool = if owner.is_some() {
        FsBlobStore::new(&h.ctx.durable_pool).unwrap()
    } else {
        FsBlobStore::new(&h.ctx.cache_pool).unwrap()
    };
    pool.put(&blob_key, bytes).unwrap();
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: owner,
            name: name.to_string(),
            mime: "application/octet-stream".to_string(),
            size_bytes: bytes.len() as u64,
            hash: md5_hex(bytes),
            blob_key: blob_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: format!("https://example.local/files/{name}"),
            thumbnail_url: None,
            comment: None,
            source_url: None,
            max_file_size: 10 * 1024 * 1024,
        })
        .unwrap();
    (meta.id, blob_key)
}

fn seed_cache_row(h: &Harness, bytes: &[u8]) -> (String, String) {
    use pinetree::media::storage::FsBlobStore;
    let blob_key = sha256_hex(bytes);
    let pool = FsBlobStore::new(&h.ctx.cache_pool).unwrap();
    pool.put(&blob_key, bytes).unwrap();
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "application/octet-stream".to_string(),
            size_bytes: bytes.len() as u64,
            hash: md5_hex(bytes),
            blob_key: blob_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: "https://example.local/files/remote.bin".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some("https://remote.example/p1.png".to_string()),
            max_file_size: 10 * 1024 * 1024,
        })
        .unwrap();
    (meta.id, blob_key)
}

fn mark_media_evicted(store: &RedbStore, media_id: &str) {
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    store
        .export_backup_snapshot(&mut |table, key, value| {
            if table == "media_meta" {
                rows.push((key, value));
            }
            Ok(())
        })
        .unwrap();
    let (key, value) = rows
        .into_iter()
        .find(|(k, _)| k == &media_id.as_bytes().to_vec())
        .expect("media row must exist for eviction marking");
    let mut json: serde_json::Value = serde_json::from_slice(&value).unwrap();
    json["evicted"] = serde_json::Value::Bool(true);
    let updated = serde_json::to_vec(&json).unwrap();
    store
        .import_backup_rows("media_meta", &[(key, updated)])
        .unwrap();
}

fn read_manifest(bundle: &Path) -> pinetree::backup::bundle::Manifest {
    let file = std::fs::File::open(bundle).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();
    let mut entry = archive.by_name("manifest.json").unwrap();
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

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
        let mut original = Vec::new();
        archive
            .by_index(i)
            .unwrap()
            .read_to_end(&mut original)
            .unwrap();
        let bytes = replace(&name, bytes).unwrap_or(original);
        writer.start_file(&name, options).unwrap();
        writer.write_all(&bytes).unwrap();
    }
    writer.finish().unwrap();
}

fn good_bundle(tag: &str) -> (Harness, PathBuf) {
    let h = harness(tag);
    let alice = h
        .store
        .create_account(CreateAccountInput::local("p1_alice"), None)
        .unwrap();
    h.store
        .create_note(
            CreateNoteInput::simple_public(&alice.id, "p1 seed note"),
            vec![],
        )
        .unwrap();
    seed_media(&h, Some(alice.id.clone()), "seed.bin", b"p1-durable-bytes");
    seed_cache_row(&h, b"p1-cache-bytes");
    let job = request_backup(&h.ctx).unwrap();
    let bundle = run_backup_blocking(&h.ctx, &job).unwrap();
    (h, bundle)
}

// --- Missing durable blob must abort publishing (never Completed) ---------

#[test]
fn p1_missing_durable_blob_aborts_publishing() {
    let _serial = serial();
    let h = harness("missing-durable");
    let alice = h
        .store
        .create_account(CreateAccountInput::local("p1_missing"), None)
        .unwrap();
    let (_id, blob_key) = seed_media(&h, Some(alice.id.clone()), "doomed.bin", b"doomed-bytes");
    // Simulate a pre-existing tear: bytes gone, metadata row remains.
    {
        use pinetree::media::storage::FsBlobStore;
        let pool = FsBlobStore::new(&h.ctx.durable_pool).unwrap();
        pool.delete(&blob_key).unwrap();
    }
    let job = request_backup(&h.ctx).unwrap();
    let err = run_backup_blocking(&h.ctx, &job).unwrap_err();
    assert!(
        matches!(err, BackupError::Corrupt(_) | BackupError::Storage(_)),
        "missing durable blob must fail creation, got {err:?}"
    );
    let status = backup_status(&h.ctx, &job).unwrap();
    assert_eq!(status.state, BackupState::Failed);
    assert!(status.output_file.is_none());
    // No final bundle may exist; only staging (if any) is left for startup
    // cleanup, never a Completed artifact.
    let finals: Vec<PathBuf> = std::fs::read_dir(&h.backup_dir)
        .map(|r| r.filter_map(|e| e.ok()).collect::<Vec<_>>())
        .unwrap_or_default()
        .into_iter()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("zip"))
        .filter(|p| {
            !p.file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .starts_with(".partial-")
        })
        .collect();
    assert!(
        finals.is_empty(),
        "no final bundle may be published: {finals:?}"
    );
}

// --- Evicted cache row needs no bytes -------------------------------------

#[test]
fn p1_evicted_cache_row_needs_no_bytes() {
    let _serial = serial();
    let h = harness("evicted");
    let alice = h
        .store
        .create_account(CreateAccountInput::local("p1_evict"), None)
        .unwrap();
    h.store
        .create_note(
            CreateNoteInput::simple_public(&alice.id, "evict seed"),
            vec![],
        )
        .unwrap();
    let (media_id, blob_key) = seed_cache_row(&h, b"evictable-bytes");
    mark_media_evicted(&h.store, &media_id);
    {
        use pinetree::media::storage::FsBlobStore;
        let pool = FsBlobStore::new(&h.ctx.cache_pool).unwrap();
        pool.delete(&blob_key).unwrap();
    }
    let job = request_backup(&h.ctx).unwrap();
    let bundle = run_backup_blocking(&h.ctx, &job).unwrap();
    assert!(bundle.exists());
    let manifest = read_manifest(&bundle);
    assert!(manifest.media_gaps.is_empty());
    let fresh = h.dir.join("restored-evicted");
    let report = restore_bundle(&bundle, &fresh).unwrap();
    assert!(report.rows_total > 0);
    assert!(fresh.join("RESTORE_READY.json").exists());
}

// --- Snapshot rows match inventory under concurrent churn ------------------

#[test]
fn p1_snapshot_rows_match_inventory_under_churn() {
    let _serial = serial();
    let h = Arc::new(harness("churn"));
    let alice = h
        .store
        .create_account(CreateAccountInput::local("p1_churn"), None)
        .unwrap();
    seed_media(
        &h,
        Some(alice.id.clone()),
        "churn-seed.bin",
        b"churn-seed-bytes",
    );
    let mut threads = Vec::new();
    for worker in 0..4 {
        let store = h.store.clone();
        let durable_pool = h.ctx.durable_pool.clone();
        threads.push(std::thread::spawn(move || {
            use pinetree::media::storage::FsBlobStore;
            let pool = FsBlobStore::new(&durable_pool).unwrap();
            for i in 0..20 {
                let user = store
                    .create_account(CreateAccountInput::local(format!("p1w{worker}u{i}")), None)
                    .unwrap();
                let bytes = format!("p1-churn-{worker}-{i}").into_bytes();
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
                if i % 2 == 0 {
                    store.delete_metadata(&meta.id).unwrap();
                    let _ = pool.delete(&key);
                }
            }
        }));
    }
    let job = request_backup(&h.ctx).unwrap();
    let bundle = run_backup_blocking(&h.ctx, &job).unwrap();
    for t in threads {
        t.join().unwrap();
    }
    // The bundle's table snapshot and archived blobs must describe one
    // state: every non-evicted media row in the archived tables must have
    // bytes in the archive (no gaps), and restore must accept it.
    let manifest = read_manifest(&bundle);
    assert!(
        manifest.media_gaps.is_empty(),
        "concurrent backup must not publish gaps: {:?}",
        manifest.media_gaps
    );
    // Decode archived media_meta rows; every non-evicted ref must resolve
    // to an archived object (+ sidecar).
    let archived: std::collections::BTreeSet<String> = manifest
        .media_files
        .iter()
        .map(|f| f.path.clone())
        .collect();
    let file = std::fs::File::open(&bundle).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();
    for table in manifest.tables.iter().filter(|t| t.name == "media_meta") {
        let mut entry = archive
            .by_name(&pinetree::backup::bundle::table_entry_name(&table.name))
            .unwrap();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        drop(entry);
        let rows = pinetree::backup::bundle::decode_table_file(&bytes, table.rows).unwrap();
        for (_, value) in rows {
            let meta: pinetree::domain::media::MediaMeta = serde_json::from_slice(&value).unwrap();
            if meta.evicted || meta.blob_key.is_empty() {
                continue;
            }
            let needs_durable = meta.owner_id.is_some();
            let shard = if meta.blob_key.len() >= 2 {
                &meta.blob_key[..2]
            } else {
                continue;
            };
            let durable_obj = format!(
                "{}objects/{shard}/{}",
                pinetree::backup::bundle::MEDIA_DURABLE_PREFIX,
                meta.blob_key
            );
            let cache_obj = format!(
                "{}objects/{shard}/{}",
                pinetree::backup::bundle::MEDIA_CACHE_PREFIX,
                meta.blob_key
            );
            let (obj, sidecar) = if needs_durable {
                (durable_obj.clone(), format!("{durable_obj}.len"))
            } else {
                (cache_obj.clone(), format!("{cache_obj}.len"))
            };
            let fallback = if needs_durable {
                cache_obj
            } else {
                durable_obj
            };
            let fallback_sidecar = format!("{fallback}.len");
            let ok_primary = archived.contains(&obj) && archived.contains(&sidecar);
            let ok_fallback = archived.contains(&fallback) && archived.contains(&fallback_sidecar);
            assert!(
                ok_primary || ok_fallback,
                "archived snapshot misses bytes for blob {}",
                meta.blob_key
            );
        }
        // Re-open for the next table read (borrow discipline).
        let file2 = std::fs::File::open(&bundle).unwrap();
        archive = zip::ZipArchive::new(file2).unwrap();
    }
    let fresh = h.dir.join("restored-churn");
    restore_bundle(&bundle, &fresh).unwrap();
    assert!(fresh.join("RESTORE_READY.json").exists());
}

// --- Restore round-trip: reopen + verify bytes ------------------------------

#[test]
fn p1_restore_roundtrip_reopen_verify_bytes() {
    let _serial = serial();
    let (h, bundle) = good_bundle("roundtrip");
    let fresh = h.dir.join("restored-roundtrip");
    let report = restore_bundle(&bundle, &fresh).unwrap();
    assert!(report.rows_total > 0);
    assert!(report.media_files >= 4);
    // Reopen the restored DB with no active transactions and verify.
    let clock = Arc::new(ManualClock::new(1_760_400_000_000));
    let restored = RedbStore::open_with_default_ids(fresh.join("pinetree.redb"), clock).unwrap();
    assert!(restored.verify_storage_accounting().unwrap().is_empty());
    drop(restored);
    assert!(
        pinetree::persistence::redb::verify_backup_integrity(&fresh.join("pinetree.redb")).unwrap()
    );
    // Every non-evicted restored row must have servable bytes.
    use pinetree::application::ports::TimelineCursor;
    use pinetree::persistence::redb::MAX_MEDIA_PAGE_ROWS;
    let reopened = RedbStore::open_with_default_ids(
        fresh.join("pinetree.redb"),
        Arc::new(ManualClock::new(1_760_400_000_000)),
    )
    .unwrap();
    let mut cursor = TimelineCursor::with_limit(MAX_MEDIA_PAGE_ROWS);
    use pinetree::media::storage::FsBlobStore;
    let durable = FsBlobStore::new(fresh.join("blobs_durable")).unwrap();
    let cache = FsBlobStore::new(fresh.join("blobs_cache")).unwrap();
    loop {
        let page = reopened.list_all_media(&cursor).unwrap();
        if page.is_empty() {
            break;
        }
        for meta in &page {
            if meta.evicted || meta.blob_key.is_empty() {
                continue;
            }
            let (primary, fallback) = if meta.owner_id.is_some() {
                (&durable, &cache)
            } else {
                (&cache, &durable)
            };
            let present = primary
                .byte_len(&meta.blob_key)
                .unwrap()
                .or(fallback.byte_len(&meta.blob_key).unwrap());
            assert!(present.is_some(), "restored row misses bytes");
        }
        let last = page.last().map(|m| m.id.clone()).unwrap_or_default();
        cursor = TimelineCursor::with_limit(MAX_MEDIA_PAGE_ROWS);
        cursor.until_id = Some(last);
    }
    // Pool trees byte-identical.
    fn inventory(pool: &Path) -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        let objects = pool.join("objects");
        let mut shards: Vec<PathBuf> = std::fs::read_dir(&objects)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        shards.sort();
        for shard in shards {
            let mut files: Vec<PathBuf> = std::fs::read_dir(&shard)
                .unwrap()
                .filter_map(|e| e.ok().map(|e| e.path()))
                .collect();
            files.sort();
            for f in files {
                let rel = format!(
                    "{}/{}",
                    shard.file_name().unwrap().to_string_lossy(),
                    f.file_name().unwrap().to_string_lossy()
                );
                out.insert(rel, std::fs::read(&f).unwrap());
            }
        }
        out
    }
    assert_eq!(
        inventory(&h.dir.join("blobs_durable")),
        inventory(&fresh.join("blobs_durable"))
    );
    assert_eq!(
        inventory(&h.dir.join("blobs_cache")),
        inventory(&fresh.join("blobs_cache"))
    );
}

// --- Restore rejects a pre-existing target ---------------------------------

#[test]
fn p1_restore_rejects_preexisting_target() {
    let _serial = serial();
    let (h, bundle) = good_bundle("preexisting");
    let fresh = h.dir.join("restored-exists");
    std::fs::create_dir_all(&fresh).unwrap();
    std::fs::write(fresh.join("sentinel.txt"), b"operator data").unwrap();
    let err = restore_bundle(&bundle, &fresh).unwrap_err();
    assert!(matches!(err, BackupError::Invalid(_)), "got {err:?}");
    assert!(fresh.join("sentinel.txt").exists());
    assert!(!fresh.join("RESTORE_READY.json").exists());
}

// --- RESTORE_READY.json is written last -------------------------------------

#[test]
fn p1_restore_ready_written_last() {
    let _serial = serial();
    let (h, bundle) = good_bundle("ready-last");
    // Corrupt a blob so restore fails: no READY marker may appear.
    let manifest = read_manifest(&bundle);
    let target = manifest
        .media_files
        .iter()
        .find(|f| !f.path.ends_with(".len"))
        .map(|f| f.path.clone())
        .unwrap();
    let bad = h.dir.join("bad-ready.zip");
    rewrite_zip(&bundle, &bad, &|name, mut bytes| {
        if name == target && !bytes.is_empty() {
            bytes[0] ^= 0xFF;
            Some(bytes)
        } else {
            None
        }
    });
    let fresh_bad = h.dir.join("restored-bad");
    let err = restore_bundle(&bad, &fresh_bad).unwrap_err();
    assert!(matches!(err, BackupError::Corrupt(_)), "got {err:?}");
    assert!(
        !fresh_bad.exists() || !fresh_bad.join("RESTORE_READY.json").exists(),
        "failed restore must never leave a READY marker"
    );
    // Good restore: marker exists and names the manifest snapshot.
    let fresh = h.dir.join("restored-good");
    let report = restore_bundle(&bundle, &fresh).unwrap();
    let marker: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fresh.join("RESTORE_READY.json")).unwrap()).unwrap();
    assert_eq!(marker["ready"], serde_json::Value::Bool(true));
    assert_eq!(
        marker["snapshot_id"],
        serde_json::Value::String(report.snapshot_id.clone())
    );
}

// --- Policy: configured cap is enforced, hard ceiling never raisable -----

#[test]
fn p1_policy_rejects_bad_config() {
    let _serial = serial();
    assert_eq!(
        BackupPolicy::from_config_value(None).unwrap(),
        BackupPolicy::default()
    );
    assert_eq!(
        BackupPolicy::default().cap(),
        pinetree::backup::MAX_BUNDLE_BYTES
    );
    assert!(matches!(
        BackupPolicy::from_config_value(Some(0)),
        Err(BackupError::Invalid(_))
    ));
    assert!(matches!(
        BackupPolicy::from_config_value(Some(u64::MAX)),
        Err(BackupError::Invalid(_))
    ));
    assert!(matches!(
        BackupPolicy::from_config_value(Some(pinetree::backup::MAX_BUNDLE_BYTES + 1)),
        Err(BackupError::Invalid(_))
    ));
    let tight = BackupPolicy::from_config_value(Some(1 << 20)).unwrap();
    assert_eq!(tight.cap(), 1 << 20);
}

#[test]
fn p1_writer_cap_boundary() {
    let _serial = serial();
    let h = harness("writer-cap");
    let alice = h
        .store
        .create_account(CreateAccountInput::local("p1_wcap"), None)
        .unwrap();
    seed_media(&h, Some(alice.id.clone()), "cap.bin", b"cap-bytes");
    // Far below the source estimate: preflight refuses before staging.
    let tiny = BackupPolicy::from_config_value(Some(1024)).unwrap();
    let err = create_bundle_with_policy(&h.ctx, "cap-tiny", tiny).unwrap_err();
    assert!(
        matches!(err, BackupError::Invalid(_)),
        "tiny cap must refuse, got {err:?}"
    );
    // Just below the real archive size: a streamed/final boundary refuses.
    let job = request_backup(&h.ctx).unwrap();
    let bundle = run_backup_blocking(&h.ctx, &job).unwrap();
    let size = std::fs::metadata(&bundle).unwrap().len();
    let below = BackupPolicy::from_config_value(Some(size.saturating_sub(1))).unwrap();
    let err = create_bundle_with_policy(&h.ctx, "cap-below", below).unwrap_err();
    assert!(
        matches!(
            err,
            BackupError::Invalid(_) | BackupError::Storage(_) | BackupError::Corrupt(_)
        ),
        "just-below cap must refuse, got {err:?} (size {size})"
    );
    // Just above: creation succeeds.
    let above = BackupPolicy::from_config_value(Some(size.saturating_add(1 << 20))).unwrap();
    let outcome = create_bundle_with_policy(&h.ctx, "cap-above", above).unwrap();
    assert!(outcome.bytes_written > 0);
}

#[test]
fn p1_restore_cap_boundary() {
    let _serial = serial();
    let (h, bundle) = good_bundle("restore-cap");
    let size = std::fs::metadata(&bundle).unwrap().len();
    // Far below the bundle size: input-size check refuses.
    let tiny = BackupPolicy::from_config_value(Some(1024)).unwrap();
    let err = restore_bundle_with_policy(&bundle, &h.dir.join("fresh-tiny"), tiny).unwrap_err();
    assert!(
        matches!(err, BackupError::Corrupt(_) | BackupError::Invalid(_)),
        "tiny restore cap must refuse, got {err:?}"
    );
    // Just below the bundle size: refuses as well.
    let below = BackupPolicy::from_config_value(Some(size.saturating_sub(1))).unwrap();
    let err = restore_bundle_with_policy(&bundle, &h.dir.join("fresh-below"), below).unwrap_err();
    assert!(
        matches!(err, BackupError::Corrupt(_) | BackupError::Invalid(_)),
        "just-below restore cap must refuse, got {err:?} (size {size})"
    );
    // Default policy restores fine.
    let report = restore_bundle(&bundle, &h.dir.join("fresh-ok")).unwrap();
    assert!(report.rows_total > 0);
}

// --- Malicious ZIP shapes refuse --------------------------------------------

#[test]
fn p1_restore_refuses_malicious_zip_shapes() {
    let _serial = serial();
    let (h, bundle) = good_bundle("malicious");
    // Traversal entry.
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
    // Duplicate entry.
    {
        let dup = h.dir.join("evil-dup.zip");
        let file = std::fs::File::open(&bundle).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let (first_name, first_bytes): (String, Vec<u8>) = {
            let mut entry = archive.by_index(0).unwrap();
            let name = entry.name().to_string();
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).unwrap();
            (name, bytes)
        };
        let out = std::fs::File::create(&dup).unwrap();
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
        // zip-rs may reject duplicates at write time; if it does, the
        // refusal itself satisfies the invariant (no hostile bundle built).
        let dup_result = writer.start_file(&first_name, options);
        if dup_result.is_ok() {
            writer.write_all(&first_bytes).unwrap();
            writer.finish().unwrap();
            let err = restore_bundle(&dup, &h.dir.join("fresh-dup")).unwrap_err();
            assert!(matches!(err, BackupError::Corrupt(_)), "dup: got {err:?}");
        } else {
            let _ = writer.finish();
        }
    }
    // Oversized declared entry: rewrite manifest to declare a huge table.
    {
        let bad = h.dir.join("evil-huge.zip");
        rewrite_zip(&bundle, &bad, &|name, bytes| {
            if name == "manifest.json" {
                let mut manifest: pinetree::backup::bundle::Manifest =
                    serde_json::from_slice(&bytes).unwrap();
                manifest.tables[0].bytes = u64::MAX;
                Some(serde_json::to_vec_pretty(&manifest).unwrap())
            } else {
                None
            }
        });
        let err = restore_bundle(&bad, &h.dir.join("fresh-huge")).unwrap_err();
        assert!(
            matches!(
                err,
                BackupError::Corrupt(_)
                    | BackupError::UnsupportedVersion(_)
                    | BackupError::Incompatible(_)
            ),
            "huge: got {err:?}"
        );
    }
}
