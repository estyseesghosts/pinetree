//! Batch F fix regressions (subagent owned file, no edits to
//! `tests/backup_snapshot_f1.rs` in place).
//!
//! * Multipage media walk reaches the oldest row past a short final page
//!   (the `bundle.rs` collect pattern after the short-page-break removal).
//! * A multipage backup carries no `media_gaps`.
//! * The exclusive gate pins `MAX_CONCURRENT_BACKUPS == 1`.

use std::collections::BTreeSet;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::ports::{BlobStore, InsertMediaInput, MediaStore, TimelineCursor};
use pinetree::backup::{
    BackupContext, BackupState, MAX_CONCURRENT_BACKUPS, backup_status, request_backup,
    run_backup_blocking,
};
use pinetree::domain::id::ManualClock;
use pinetree::domain::media::MediaProperties;
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::storage::{FsBlobStore, acquire_backup_gate, media_gate_snapshot};
use pinetree::persistence::redb::{MAX_MEDIA_PAGE_ROWS, RedbStore};

const BASE_MS: i64 = 1_760_400_000_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

/// Serializes gate-holding sections: the gate is process-global, so two
/// such tests overlapping would see spurious `Busy` failures.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct Harness {
    store: Arc<RedbStore>,
    durable: FsBlobStore,
    cache: FsBlobStore,
    ctx: BackupContext,
    dir: std::path::PathBuf,
}

fn harness(tag: &str) -> Harness {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-ffix-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock = Arc::new(ManualClock::new(BASE_MS));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock).unwrap();
    let durable = FsBlobStore::new(dir.join("blobs_durable")).unwrap();
    let cache = FsBlobStore::new(dir.join("blobs_cache")).unwrap();
    let ctx = BackupContext {
        store: store.clone(),
        durable_pool: dir.join("blobs_durable"),
        cache_pool: dir.join("blobs_cache"),
        backup_dir: dir.join("backups"),
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
    }
}

fn insert_media_row(h: &Harness, seq: usize, owner: Option<&str>) -> String {
    let bytes = format!("ffix-bytes-{seq:05}").into_bytes();
    let key = sha256_hex(&bytes);
    if owner.is_some() {
        h.durable.put(&key, &bytes).unwrap();
    } else {
        h.cache.put(&key, &bytes).unwrap();
    }
    h.store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: owner.map(str::to_string),
            name: format!("ffix{seq}.bin"),
            mime: "application/octet-stream".to_string(),
            size_bytes: bytes.len() as u64,
            hash: md5_hex(&bytes),
            blob_key: key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: owner.is_none(),
            url: format!("https://example.local/files/ffix{seq}"),
            thumbnail_url: None,
            comment: None,
            source_url: owner
                .is_none()
                .then(|| format!("https://remote.example/{seq}.bin")),
            max_file_size: 10 * 1024 * 1024,
        })
        .unwrap();
    key
}

/// Fixed newest-first walk (mirrors `bundle.rs` after the fix): advance
/// `until_id` past every non-empty page, stop only on empty.
fn walk_all_ids(store: &RedbStore) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = TimelineCursor::with_limit(MAX_MEDIA_PAGE_ROWS);
    loop {
        let page = store.list_all_media(&cursor).unwrap();
        if page.is_empty() {
            break;
        }
        let last = page.last().map(|m| m.id.clone()).unwrap_or_default();
        out.extend(page.into_iter().map(|m| m.id));
        cursor = TimelineCursor::with_limit(MAX_MEDIA_PAGE_ROWS);
        cursor.until_id = Some(last);
    }
    out
}

#[test]
fn multipage_collect_reaches_oldest_past_short_final_page() {
    // 2*MAX + 1 rows: two full pages + a 1-row short final page. Only the
    // trailing empty read terminates; the short page never does.
    let h = harness("multipage-collect");
    let want_total = 2 * MAX_MEDIA_PAGE_ROWS + 1;
    for i in 0..want_total {
        insert_media_row(&h, i, Some("alice"));
    }
    let ids = walk_all_ids(&h.store);
    assert_eq!(ids.len(), want_total);
    let uniq: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
    assert_eq!(uniq.len(), want_total, "exact-once, no skip/repeat");
    // Newest-first ordering holds across page joints.
    for w in ids.windows(2) {
        assert!(w[0] > w[1], "newest-first violated");
    }
    // Exact multiple also needs the trailing empty read (no short break).
    let h2 = harness("exact-multiple");
    for i in 0..(2 * MAX_MEDIA_PAGE_ROWS) {
        insert_media_row(&h2, i, Some("alice"));
    }
    assert_eq!(walk_all_ids(&h2.store).len(), 2 * MAX_MEDIA_PAGE_ROWS);
}

#[test]
fn multipage_backup_carries_no_media_gaps() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let h = harness("no-gaps");
    // More than one page of referenced blobs so the old short-page break
    // would have truncated the reference set.
    let total = MAX_MEDIA_PAGE_ROWS + 25;
    for i in 0..total {
        let owner = if i % 2 == 0 { Some("alice") } else { None };
        insert_media_row(&h, i, owner);
    }
    assert_eq!(walk_all_ids(&h.store).len(), total);
    let job_id = request_backup(&h.ctx).unwrap();
    let bundle = run_backup_blocking(&h.ctx, &job_id).unwrap();
    assert!(bundle.exists());
    let status = backup_status(&h.ctx, &job_id).unwrap();
    assert_eq!(status.state, BackupState::Completed);
    // The manifest must record zero gaps: every referenced blob was found.
    let file = std::fs::File::open(&bundle).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();
    let mut entry = archive.by_name("manifest.json").unwrap();
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let gaps = manifest
        .get("media_gaps")
        .and_then(|g| g.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(gaps.is_empty(), "bundle must not record gaps: {gaps:?}");
    let _ = std::fs::remove_dir_all(&h.dir);
}

#[test]
fn exclusive_gate_pins_single_backup() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    // The const the gate enforces: exactly one concurrent backup.
    assert_eq!(MAX_CONCURRENT_BACKUPS, 1);
    let _held = acquire_backup_gate("ffix-holder").unwrap();
    let busy = acquire_backup_gate("ffix-second").unwrap_err();
    assert_eq!(busy.active_backup_id, "ffix-holder");
    // Gate diagnostics stay truthful while held.
    let snap = media_gate_snapshot();
    assert!(snap.backup_active);
    assert_eq!(snap.active_backup_id, "ffix-holder");
}
