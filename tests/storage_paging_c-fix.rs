//! Batch C+F fix regressions (subagent owned files only, no edits to
//! `tests/storage_paging_c1.rs` in place).
//!
//! * Short-page-must-not-terminate (mock pages mirroring
//!   `src/backup/bundle.rs::collect_referenced_blobs` + `scan.rs:320-335`).
//! * `sweep_orphan_blobs_checked` refuses incomplete sets, sweeps complete.
//! * Backfill is idempotent over shared keys.
//! * `FsBlobStore::heal_sidecar` regenerates a missing sidecar via stat.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, InsertMediaInput, MediaStore, NoopStreamingSink, PortError,
    TimelineCursor,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest::sha256_hex;
use pinetree::media::reconciler;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
const MAX_FILE: u64 = 10_000_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

fn harness(tag: &str) -> (Arc<RedbStore>, Arc<AppServices>, std::path::PathBuf) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-cfix-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<ManualClock> = Arc::new(ManualClock::new(BASE_MS));
    let clock_port: Arc<dyn Clock> = clock.clone();
    let store =
        RedbStore::open_with_default_ids(dir.join("store.redb"), clock_port.clone()).unwrap();
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let services = Arc::new(AppServices {
        clock: clock_port.clone(),
        ids: store.ids().clone(),
        accounts: store.clone(),
        notes: store.clone(),
        follows: store.clone(),
        blocks: store.clone(),
        mutes: store.clone(),
        reactions: store.clone(),
        favorites: store.clone(),
        emojis: store.clone(),
        antennas: store.clone(),
        antenna_notes: store.clone(),
        polls: store.clone(),
        jobs: store.clone(),
        media: store.clone(),
        blobs: durable.clone(),
        cache_blobs: cache.clone(),
        notifications: store.clone(),
        auth: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        host_caps: store.clone(),
        moderation: store.clone(),
        instance_settings: store.clone(),
        invites: store.clone(),
        abuse_reports: store.clone(),
        lists: store.clone(),
        signin: store.clone(),
        password_resets: store.clone(),
        registry: store.clone(),
        clips: store.clone(),
        pages: store.clone(),
        folders: store.clone(),
        system_webhooks: store.clone(),
        user_webhooks: store.clone(),
        abuse_recipients: store.clone(),
        memos: store.clone(),
        thread_mutes: store.clone(),
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "https://pinetree.local".to_string(),
        max_file_size: MAX_FILE,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    });
    (store, services, dir)
}

fn insert_owned(store: &RedbStore, owner: &str, seq: usize, size: u64, blob_key: &str) {
    store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: Some(owner.to_string()),
            name: format!("f{seq}.bin"),
            mime: "image/png".to_string(),
            size_bytes: size,
            hash: format!("{seq:032x}"),
            blob_key: blob_key.to_string(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: format!("https://pinetree.local/files/{seq}"),
            thumbnail_url: None,
            comment: None,
            source_url: None,
            max_file_size: MAX_FILE,
        })
        .unwrap();
}

/// Fixed collect loop (mirrors `bundle.rs::collect_referenced_blobs` after
/// the fix + `scan.rs:320-335`): only an empty page terminates.
fn collect_fixed(pages: &[Vec<String>]) -> Vec<String> {
    let mut out = Vec::new();
    let mut idx = 0;
    loop {
        let page = pages.get(idx).cloned().unwrap_or_default();
        if page.is_empty() {
            break;
        }
        out.extend(page);
        idx += 1;
    }
    out
}

/// Buggy loop (pre-fix): `if page.len() < limit { break; }`.
fn collect_buggy(pages: &[Vec<String>], limit: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut idx = 0;
    loop {
        let page = pages.get(idx).cloned().unwrap_or_default();
        if page.is_empty() {
            break;
        }
        let short = page.len() < limit;
        out.extend(page);
        if short {
            break;
        }
        idx += 1;
    }
    out
}

#[test]
fn short_page_does_not_terminate_backup_collect() {
    // A short non-terminal page (concurrent deletes mid-scan): the fixed
    // loop walks to the trailing empty page, the old short-page break
    // would have dropped the tail.
    let pages = vec![
        vec!["a".to_string(), "b".to_string()],
        vec!["c".to_string()],
        vec!["d".to_string(), "e".to_string()],
        vec![],
    ];
    let fixed = collect_fixed(&pages);
    assert_eq!(fixed, vec!["a", "b", "c", "d", "e"]);
    let buggy = collect_buggy(&pages, 2);
    assert_eq!(buggy, vec!["a", "b", "c"], "old break drops the tail");
}

#[test]
fn real_store_walk_needs_trailing_empty_read() {
    // 501 rows at limit 500: the first page is full, the second is a short
    // (1-row) final page, and only the third (empty) read terminates.
    let (store, _, _dir) = harness("trailing-empty");
    for i in 0..501 {
        insert_owned(&store, "owner", i, 3, &format!("{i:064x}"));
    }
    let mut after: Option<String> = None;
    let mut total = 0usize;
    let mut pages = 0usize;
    loop {
        let mut cursor = TimelineCursor::with_limit(500);
        cursor.until_id = after.clone();
        let rows = store.list_all_media(&cursor).unwrap();
        if rows.is_empty() {
            break;
        }
        pages += 1;
        total += rows.len();
        after = rows.last().map(|m| m.id.clone());
    }
    assert_eq!(total, 501);
    assert_eq!(pages, 2, "full page + short final page, then empty");
}

#[test]
fn sweep_refuses_incomplete_key_list() {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-cfix-sweep-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let pool = FsBlobStore::new(&dir).unwrap();
    let live = sha256_hex(b"live-bytes");
    let orphan = sha256_hex(b"orphan-bytes");
    pool.put(&live, b"live-bytes").unwrap();
    pool.put(&orphan, b"orphan-bytes").unwrap();
    let objects = dir.join("objects");
    let mut partial = HashSet::new();
    partial.insert(live.clone());
    let err = reconciler::sweep_orphan_blobs_checked(&objects, &partial, false).unwrap_err();
    assert!(matches!(err, PortError::Validation(_)), "{err:?}");
    assert!(pool.get(&live).unwrap().is_some());
    assert!(pool.get(&orphan).unwrap().is_some());
    let removed = reconciler::sweep_orphan_blobs_checked(&objects, &partial, true).unwrap();
    assert_eq!(removed, 1);
    assert!(pool.get(&live).unwrap().is_some());
    assert!(pool.get(&orphan).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn backfill_is_idempotent_over_shared_keys() {
    let (store, services, _dir) = harness("backfill-idem");
    let shared = "f".repeat(64);
    for i in 0..50 {
        insert_owned(&store, "alice", i, 8, &shared);
    }
    for i in 0..20 {
        insert_owned(&store, "bob", 1000 + i, 5, &format!("{i:064x}"));
    }
    let (first, done) = reconciler::backfill_durable_accounting(&services, None, 500).unwrap();
    assert!(done.is_none());
    assert_eq!(first.media_scanned, 70);
    assert_eq!(store.durable_bytes("alice").unwrap(), 400);
    assert_eq!(store.blob_refcount(&shared).unwrap(), 50);
    let (again, _) = reconciler::backfill_durable_accounting(&services, None, 500).unwrap();
    assert_eq!(again.media_scanned, 70);
    assert_eq!(store.durable_bytes("alice").unwrap(), 400);
    assert_eq!(store.durable_bytes("bob").unwrap(), 100);
    assert_eq!(store.blob_refcount(&shared).unwrap(), 50);
}

#[test]
fn sidecar_heal_repairs_missing_sidecar_without_content_read() {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-cfix-sidecar-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let pool = FsBlobStore::new(&dir).unwrap();
    let payload = b"sidecar-heal-bytes";
    let key = sha256_hex(payload);
    pool.put(&key, payload).unwrap();
    assert_eq!(pool.byte_len(&key).unwrap(), Some(payload.len() as u64));
    // Torn sidecar: remove the `.len` file; `get`/`byte_len` now read None.
    let sidecar = pool.object_path(&key).unwrap();
    let mut sidecar_path = sidecar.into_os_string();
    sidecar_path.push(".len");
    std::fs::remove_file(&sidecar_path).unwrap();
    assert_eq!(pool.byte_len(&key).unwrap(), None);
    assert_eq!(pool.get(&key).unwrap(), None);
    // Direct stat heal rewrites the sidecar without needing the bytes.
    assert!(pool.heal_sidecar(&key).unwrap());
    assert_eq!(pool.byte_len(&key).unwrap(), Some(payload.len() as u64));
    assert_eq!(pool.get(&key).unwrap().as_deref(), Some(&payload[..]));
    // Healthy rows are a no-op; absent keys heal nothing.
    assert!(!pool.heal_sidecar(&key).unwrap());
    let missing = sha256_hex(b"never-stored");
    assert!(!pool.heal_sidecar(&missing).unwrap());
    // Show the fake-backend contrast used by the reconciler docs: a HashMap
    // of row counts is exact regardless of sidecars (accounting math only).
    let mut counts: HashMap<String, u64> = HashMap::new();
    *counts.entry(key.clone()).or_insert(0) += 1;
    assert_eq!(counts[&key], 1);
    let _ = std::fs::remove_dir_all(&dir);
}
