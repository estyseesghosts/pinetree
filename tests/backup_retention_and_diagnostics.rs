//! B3 backup retention + diagnostics regressions (WP3 section 5).
//!
//! Hermetic (temp dirs only). Serializes gate-holding sections on a process
//! global mutex because the media backup gate is exclusive.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::ports::{
    AccountStore, BlobStore, CreateAccountInput, InsertMediaInput, MediaStore,
};
use pinetree::backup::{
    BackupContext, BackupError, BackupState, BackupStatus, backup_status, list_backup_statuses,
    newest_backup_statuses, prune_retained_backups, request_backup, run_backup_blocking,
};
use pinetree::domain::id::ManualClock;
use pinetree::domain::media::MediaProperties;
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

static SEQ: AtomicU64 = AtomicU64::new(0);
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
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-b3-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock = Arc::new(ManualClock::new(1_760_400_000_000i64 + n as i64));
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

fn seed_account(store: &Arc<RedbStore>, name: &str) -> String {
    store
        .create_account(CreateAccountInput::local(name), None)
        .expect("seed account")
        .id
}

fn seed_durable_media(h: &Harness, owner: &str, name: &str, bytes: &[u8]) -> (String, String) {
    let blob_key = sha256_hex(bytes);
    let pool = FsBlobStore::new(&h.ctx.durable_pool).unwrap();
    pool.put(&blob_key, bytes).unwrap();
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: Some(owner.to_string()),
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

fn complete_backup(ctx: &BackupContext, store: &Arc<RedbStore>, name: &str) -> String {
    let _ = seed_account(store, name);
    let job = request_backup(ctx).expect("request");
    run_backup_blocking(ctx, &job).expect("run");
    let status = backup_status(ctx, &job).expect("status");
    assert_eq!(status.state, BackupState::Completed);
    job
}

fn status_file_path(backup_dir: &std::path::Path, job_id: &str) -> PathBuf {
    backup_dir.join(format!("{job_id}.status.json"))
}

fn final_zips(backup_dir: &std::path::Path) -> Vec<PathBuf> {
    std::fs::read_dir(backup_dir)
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
        .collect()
}

#[test]
fn retention_zip_delete_failure_leaves_status_then_retries() {
    let _serial = serial();
    let h = harness("retention-fail");
    let first = complete_backup(&h.ctx, &h.store, "alice");
    let _second = complete_backup(&h.ctx, &h.store, "bob");
    let _third = complete_backup(&h.ctx, &h.store, "carol");
    let mut completed: Vec<BackupStatus> = list_backup_statuses(&h.backup_dir)
        .into_iter()
        .filter(|s| s.state == BackupState::Completed)
        .collect();
    completed.sort_by_key(|s| s.finished_ms.unwrap_or(0));
    assert_eq!(completed.len(), 3);
    assert_eq!(completed[0].job_id, first);
    let oldest_zip = completed[0].output_file.clone().expect("output");
    let zip_path = h.backup_dir.join(&oldest_zip);
    assert!(zip_path.exists());
    std::fs::remove_file(&zip_path).unwrap();
    std::fs::create_dir(&zip_path).unwrap();
    let mut ctx = h.ctx.clone();
    ctx.retention_max_count = Some(2);
    let report = prune_retained_backups(&ctx);
    assert_eq!(report.bundles_removed, 0);
    assert_eq!(report.bytes_reclaimed, 0);
    assert!(status_file_path(&h.backup_dir, &first).exists());
    assert!(backup_status(&ctx, &first).is_some());
    std::fs::remove_dir(&zip_path).unwrap();
    let report2 = prune_retained_backups(&ctx);
    assert_eq!(report2.bundles_removed, 1);
    assert!(!status_file_path(&h.backup_dir, &first).exists());
    assert!(backup_status(&ctx, &first).is_none());
    let _ = std::fs::remove_dir_all(&h.dir);
}

#[test]
fn retention_notfound_zip_removes_status() {
    let _serial = serial();
    let h = harness("retention-notfound");
    let first = complete_backup(&h.ctx, &h.store, "alice");
    let second = complete_backup(&h.ctx, &h.store, "bob");
    let oldest_zip = backup_status(&h.ctx, &first)
        .unwrap()
        .output_file
        .expect("output");
    std::fs::remove_file(h.backup_dir.join(&oldest_zip)).unwrap();
    let mut ctx = h.ctx.clone();
    ctx.retention_max_count = Some(1);
    let report = prune_retained_backups(&ctx);
    assert_eq!(report.bundles_removed, 1);
    assert!(backup_status(&ctx, &first).is_none());
    assert!(backup_status(&ctx, &second).is_some());
    let _ = std::fs::remove_dir_all(&h.dir);
}

fn write_status_file(
    backup_dir: &std::path::Path,
    job_id: &str,
    state: BackupState,
    requested_ms: u64,
    finished_ms: Option<u64>,
    bytes: Option<u64>,
) {
    let status = BackupStatus {
        job_id: job_id.to_string(),
        state,
        requested_ms,
        started_ms: Some(requested_ms),
        finished_ms,
        attempts: 1,
        output_file: if state == BackupState::Completed {
            Some(format!("{job_id}.zip"))
        } else {
            None
        },
        bytes_written: bytes,
        tables_exported: 0,
        media_files: 0,
        gate_wait_ms: 0,
        failure_redacted: if state == BackupState::Failed {
            Some("failed".to_string())
        } else {
            None
        },
    };
    let path = status_file_path(backup_dir, job_id);
    std::fs::write(&path, serde_json::to_vec_pretty(&status).unwrap()).unwrap();
}

#[test]
fn diagnostics_newest_tail_over_80() {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-b3-diag-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let base: u64 = 1_760_400_000_000;
    for i in 0..85u64 {
        let job_id = format!("bk-diag-{i:05}");
        let requested = base + i;
        if i == 83 {
            write_status_file(
                &dir,
                &job_id,
                BackupState::Failed,
                requested,
                Some(requested + 5),
                None,
            );
        } else if i == 84 {
            write_status_file(
                &dir,
                &job_id,
                BackupState::Completed,
                requested,
                Some(requested + 10),
                Some(9999),
            );
        } else {
            write_status_file(
                &dir,
                &job_id,
                BackupState::Completed,
                requested,
                Some(requested),
                Some(100 + i),
            );
        }
    }
    let newest = newest_backup_statuses(&dir, 80);
    assert_eq!(newest.len(), 80);
    assert!(newest.iter().all(|s| s.requested_ms >= base + 5));
    assert!(newest.iter().all(|s| s.requested_ms <= base + 84));
    assert!(!newest.iter().any(|s| s.requested_ms == base));
    let last_success = newest
        .iter()
        .filter(|s| s.state == BackupState::Completed)
        .filter_map(|s| s.finished_ms)
        .max()
        .unwrap();
    assert_eq!(last_success, base + 84 + 10);
    let last_failure = newest
        .iter()
        .filter(|s| s.state == BackupState::Failed)
        .filter_map(|s| s.finished_ms)
        .max()
        .unwrap();
    assert_eq!(last_failure, base + 83 + 5);
    let last_bytes: Option<u64> = newest
        .iter()
        .filter(|s| s.state == BackupState::Completed)
        .filter_map(|s| s.finished_ms.map(|finished| (finished, s.bytes_written)))
        .max_by_key(|(finished, _)| *finished)
        .and_then(|(_, bytes)| bytes);
    assert_eq!(last_bytes, Some(9999));
    let recent: Vec<String> = newest.iter().take(20).map(|s| s.job_id.clone()).collect();
    assert_eq!(recent[0], "bk-diag-00084");
    assert_eq!(recent[1], "bk-diag-00083");
    assert!(!recent.contains(&"bk-diag-00000".to_string()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn diagnostics_tie_break_descending_job_id() {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-b3-tie-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let requested: u64 = 1_760_400_001_000;
    write_status_file(
        &dir,
        "bk-tie-a",
        BackupState::Completed,
        requested,
        Some(requested),
        Some(1),
    );
    write_status_file(
        &dir,
        "bk-tie-b",
        BackupState::Completed,
        requested,
        Some(requested),
        Some(2),
    );
    let newest = newest_backup_statuses(&dir, 10);
    assert_eq!(newest.len(), 2);
    assert_eq!(newest[0].job_id, "bk-tie-b");
    assert_eq!(newest[1].job_id, "bk-tie-a");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn malformed_media_meta_fails_before_publish() {
    let _serial = serial();
    let h = harness("malformed-meta");
    let alice = seed_account(&h.store, "alice");
    let (media_id, _blob) = seed_durable_media(&h, &alice, "seed.bin", b"seed-bytes");
    h.store
        .import_backup_rows(
            "media_meta",
            &[(media_id.into_bytes(), b"{not-json".to_vec())],
        )
        .unwrap();
    let job = request_backup(&h.ctx).unwrap();
    let err = run_backup_blocking(&h.ctx, &job).unwrap_err();
    assert!(matches!(
        err,
        BackupError::Corrupt(_) | BackupError::Storage(_)
    ));
    let status = backup_status(&h.ctx, &job).unwrap();
    assert_eq!(status.state, BackupState::Failed);
    assert!(status.output_file.is_none());
    assert!(final_zips(&h.backup_dir).is_empty());
    let _ = std::fs::remove_dir_all(&h.dir);
}

#[test]
fn mismatched_len_fails_no_final_zip() {
    let _serial = serial();
    let h = harness("mismatched-len");
    let alice = seed_account(&h.store, "alice");
    let (_media_id, blob_key) = seed_durable_media(&h, &alice, "seed.bin", b"seed-bytes-len");
    let shard = &blob_key[..2];
    let sidecar = h
        .ctx
        .durable_pool
        .join("objects")
        .join(shard)
        .join(format!("{blob_key}.len"));
    std::fs::write(&sidecar, b"999999999").unwrap();
    let job = request_backup(&h.ctx).unwrap();
    let err = run_backup_blocking(&h.ctx, &job).unwrap_err();
    assert!(matches!(
        err,
        BackupError::Corrupt(_) | BackupError::Storage(_)
    ));
    let status = backup_status(&h.ctx, &job).unwrap();
    assert_eq!(status.state, BackupState::Failed);
    assert!(final_zips(&h.backup_dir).is_empty());
    let _ = std::fs::remove_dir_all(&h.dir);
}

#[test]
fn valid_sidecar_backup_succeeds() {
    let _serial = serial();
    let h = harness("valid-sidecar");
    let alice = seed_account(&h.store, "alice");
    let (_media_id, blob_key) = seed_durable_media(&h, &alice, "seed.bin", b"valid-bytes");
    let job = request_backup(&h.ctx).unwrap();
    let bundle = run_backup_blocking(&h.ctx, &job).unwrap();
    assert!(bundle.exists());
    let status = backup_status(&h.ctx, &job).unwrap();
    assert_eq!(status.state, BackupState::Completed);
    let pool = FsBlobStore::new(&h.ctx.durable_pool).unwrap();
    assert!(pool.byte_len(&blob_key).unwrap().is_some());
    let _ = std::fs::remove_dir_all(&h.dir);
}
