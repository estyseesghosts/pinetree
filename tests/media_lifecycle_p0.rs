//! Phase 1 (P0) media-lifecycle regression tests (`docs/prod-ready-2.md`).
//!
//! Every test uses deterministic barriers/channels, never sleeps. They pin
//! the lifecycle invariant: at every committed, externally visible state,
//! every live non-evicted row has readable bytes, and an object is physically
//! deleted only after competing operations are serialized and no live
//! row/index requires the key.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use pinetree::application::commands::AppServices;
use pinetree::application::ports::AccountStore;
use pinetree::application::ports::ActivityDedup;
use pinetree::application::ports::BlobStore;
use pinetree::application::ports::CreateAccountInput;
use pinetree::application::ports::InsertMediaInput;
use pinetree::application::ports::MediaClass;
use pinetree::application::ports::MediaStore;
use pinetree::application::ports::NoopStreamingSink;
use pinetree::application::ports::RemoteCacheRecord;
use pinetree::application::ports::TimelineCursor;
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::Clock;
use pinetree::domain::id::ManualClock;
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::SafeHttpClient;
use pinetree::federation::delivery::SafeHttpConfig;
use pinetree::media::ingest::md5_hex;
use pinetree::media::ingest::sha256_hex;
use pinetree::media::reconcile::collect_live_blob_keys;
use pinetree::media::reconcile::reconcile_media;
use pinetree::media::reconcile::sweep_orphan_blobs_checked;
use pinetree::media::reconcile::sweep_orphan_pools;
use pinetree::media::remote_cache::ensure_cache_budget;
use pinetree::media::storage::FsBlobStore;
use pinetree::media::storage::acquire_blob_key;
use pinetree::media::storage::acquire_media_exclusive;
use pinetree::media::storage::acquire_media_op;
use pinetree::media::storage::try_acquire_media_exclusive;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
static P0_SEQ: AtomicU64 = AtomicU64::new(0);

struct P0Harness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    durable: Arc<FsBlobStore>,
    cache: Arc<FsBlobStore>,
    dir: std::path::PathBuf,
}

fn p0_harness(cache_max_bytes: u64) -> (P0Harness, String) {
    let n = P0_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir =
        std::env::temp_dir().join(format!("pinetree-lifecycle-p0-{}-{n}", std::process::id()));
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
        max_file_size: 10_000_000,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: cache_max_bytes,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    });
    let owner = store
        .create_account(CreateAccountInput::local(format!("p0_user_{n}")), None)
        .expect("create owner")
        .id;
    (
        P0Harness {
            store,
            services,
            durable,
            cache,
            dir,
        },
        owner,
    )
}

/// All metadata rows, newest-first paging to the empty page.
fn all_rows(services: &AppServices) -> Vec<pinetree::domain::media::MediaMeta> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut cursor = TimelineCursor::with_limit(50);
        cursor.until_id = after.clone();
        let page = services.media.list_all_media(&cursor).unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().map(|m| m.id.clone());
        out.extend(page);
    }
    out
}

/// Global quiescence invariant: every live row has bytes in its own pool,
/// refcounts equal the true per-key row counts, and durable owner totals
/// equal the true per-owner byte sums.
fn assert_quiescent(h: &P0Harness) {
    let rows = all_rows(&h.services);
    let mut key_counts: HashMap<String, u64> = HashMap::new();
    let mut owner_sums: HashMap<String, u64> = HashMap::new();
    for meta in &rows {
        *key_counts.entry(meta.blob_key.clone()).or_insert(0) += 1;
        if let Some(owner) = meta.owner_id.as_deref() {
            *owner_sums.entry(owner.to_string()).or_insert(0) += meta.size_bytes;
        }
        if meta.owner_id.is_some() {
            assert_eq!(
                h.durable.byte_len(&meta.blob_key).unwrap(),
                Some(meta.size_bytes),
                "durable row {} must have intact bytes",
                meta.id
            );
        } else {
            assert_eq!(
                h.cache.byte_len(&meta.blob_key).unwrap(),
                Some(meta.size_bytes),
                "live cache row {} must have intact bytes",
                meta.id
            );
        }
    }
    for (key, expected) in &key_counts {
        assert_eq!(
            h.store.blob_refcount(key).unwrap(),
            *expected,
            "refcount drift for key {key}"
        );
    }
    let mut owners_seen = HashSet::new();
    for meta in &rows {
        if let Some(owner) = meta.owner_id.as_deref() {
            owners_seen.insert(owner.to_string());
        }
    }
    for owner in &owners_seen {
        assert_eq!(
            h.store.durable_bytes(owner).unwrap(),
            owner_sums[owner],
            "durable-bytes drift for owner {owner}"
        );
    }
}

fn insert_cache_row(
    h: &P0Harness,
    source_url: &str,
    bytes: &[u8],
) -> pinetree::domain::media::MediaMeta {
    let key = sha256_hex(bytes);
    h.cache.put(&key, bytes).unwrap();
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: bytes.len() as u64,
            hash: md5_hex(bytes),
            blob_key: key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: format!("https://pinetree.local/files/{key}"),
            thumbnail_url: None,
            comment: None,
            source_url: Some(source_url.to_string()),
            max_file_size: 10_000_000,
        })
        .unwrap();
    h.store
        .remote_cache_insert(&RemoteCacheRecord {
            source_url: source_url.to_string(),
            variant: "original".to_string(),
            media_id: meta.id.clone(),
            blob_key: key,
            size_bytes: bytes.len() as u64,
            class: MediaClass::Preview,
            fetched_at_ms: BASE_MS,
            last_access_ms: BASE_MS,
            access_count: 1,
            bytes_absent: false,
            generation: 0,
        })
        .unwrap();
    h.store
        .cache_accounting_apply(
            bytes.len() as i64,
            MediaClass::Preview,
            bytes.len() as i64,
            0,
            1,
            bytes.len() as u64,
            0,
            0,
        )
        .unwrap();
    meta
}

#[test]
fn p0_same_bytes_delete_vs_upload_preserves_live_row() {
    // Forced interleaving of the P0 race: a delete observes zero references
    // while an upload of identical bytes recreates a reference before the
    // bytes are removed. The deferred recheck must preserve the reused bytes.
    let (h, owner) = p0_harness(512 << 20);
    let bytes = b"same-bytes-payload-p0-1";
    let first = h
        .services
        .upload_media(&owner, "a.bin".to_string(), bytes, None, false, false)
        .unwrap();
    let key = first.blob_key.clone();
    // Pause the delete after its zero-ref transition (record gone, refcount
    // decremented in the same redb transaction).
    let removed = h.store.delete_media_record(&first.id).unwrap();
    assert!(removed.is_some());
    assert_eq!(h.store.blob_refcount(&key).unwrap(), 0);
    // Concurrent upload of identical bytes recreates the reference.
    let second = h
        .services
        .upload_media(&owner, "b.bin".to_string(), bytes, None, false, true)
        .unwrap();
    assert_eq!(second.blob_key, key);
    assert_eq!(h.store.blob_refcount(&key).unwrap(), 1);
    // Resuming the delete must recheck and preserve the reused bytes.
    assert!(
        !h.services
            .delete_durable_bytes_if_unreferenced(&key)
            .unwrap()
    );
    assert_eq!(h.durable.get(&key).unwrap().as_deref(), Some(&bytes[..]));
    assert!(h.store.get(&second.id).unwrap().is_some());
    // The full public delete path converges once the last reference is gone.
    h.services.delete_media(&owner, &second.id).unwrap();
    assert_eq!(h.store.blob_refcount(&key).unwrap(), 0);
    assert!(h.durable.get(&key).unwrap().is_none());
    assert!(
        h.services
            .delete_durable_bytes_if_unreferenced(&key)
            .unwrap()
    );
}

#[test]
fn p0_shared_blob_two_owners_delete_either_order() {
    // One SHA-256 key shared by two owners survives either delete order; the
    // last delete collects the bytes.
    let (h, owner_a) = p0_harness(512 << 20);
    let owner_b = h
        .store
        .create_account(CreateAccountInput::local("p0_shared_b"), None)
        .unwrap()
        .id;
    let bytes = b"shared-blob-payload-p0-2";
    for (first_owner, second_owner) in [(&owner_a, &owner_b), (&owner_b, &owner_a)] {
        let a = h
            .services
            .upload_media(first_owner, "s.bin".to_string(), bytes, None, false, true)
            .unwrap();
        let b = h
            .services
            .upload_media(second_owner, "s.bin".to_string(), bytes, None, false, true)
            .unwrap();
        assert_eq!(a.blob_key, b.blob_key);
        assert_eq!(h.store.blob_refcount(&a.blob_key).unwrap(), 2);
        h.services.delete_media(first_owner, &a.id).unwrap();
        assert_eq!(h.store.blob_refcount(&a.blob_key).unwrap(), 1);
        let survivor = h.store.get(&b.id).unwrap().expect("survivor row");
        assert_eq!(
            h.durable.get(&survivor.blob_key).unwrap().as_deref(),
            Some(&bytes[..]),
            "surviving owner must keep readable bytes"
        );
        h.services.delete_media(second_owner, &b.id).unwrap();
        assert_eq!(h.store.blob_refcount(&a.blob_key).unwrap(), 0);
        assert!(h.durable.get(&a.blob_key).unwrap().is_none());
    }
    assert_quiescent(&h);
}

#[test]
fn p0_upload_paused_while_sweep_waits_or_preserves() {
    // The destructive sweep owns the exclusive barrier across collection and
    // deletion; a whole-transition writer (shared barrier) either commits
    // first (sweep sees the row) or blocks the sweep until it commits.
    let (h, owner) = p0_harness(512 << 20);
    let bytes = b"paused-upload-payload-p0-3";
    let key = sha256_hex(bytes);
    let gate = acquire_media_op("p0-test-setup").unwrap();
    let key_guard = acquire_blob_key(&key).unwrap();
    h.durable.put(&key, bytes).unwrap();
    let (attempting_tx, attempting_rx) = std::sync::mpsc::channel::<()>();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<u64>();
    let services = h.services.clone();
    let dir = h.dir.clone();
    let sweeper = std::thread::spawn(move || {
        attempting_tx.send(()).unwrap();
        let report = sweep_orphan_pools(
            &services,
            &dir.join("blobs_durable").join("objects"),
            &dir.join("blobs_cache").join("objects"),
        )
        .unwrap();
        done_tx
            .send(report.cache_removed + report.durable_removed)
            .unwrap();
    });
    // The sweep is attempting while the writer still holds the shared side:
    // the exclusive barrier cannot be granted yet.
    attempting_rx.recv().unwrap();
    assert!(try_acquire_media_exclusive("p0-test-probe").is_none());
    // Commit the metadata while still holding the barrier, then release.
    h.services
        .upload_media(&owner, "c.bin".to_string(), bytes, None, false, true)
        .unwrap();
    drop(key_guard);
    drop(gate);
    let removed = done_rx.recv().unwrap();
    sweeper.join().unwrap();
    assert_eq!(removed, 0, "live bytes must never be swept");
    assert_eq!(h.durable.get(&key).unwrap().as_deref(), Some(&bytes[..]));
    assert_quiescent(&h);
}

#[test]
fn p0_cache_evict_refetch_reconcile_twice() {
    // Two remote URLs sharing one blob: evict, refetch, reconcile twice.
    // Rows and index entries survive every transition; counters stay exact.
    let (h, _owner) = p0_harness(48);
    let bytes = b"shared-cache-payload-p0-4";
    let key = sha256_hex(bytes);
    let first = insert_cache_row(&h, "https://r.example/p0-first.png", bytes);
    let second = insert_cache_row(&h, "https://r.example/p0-second.png", bytes);
    assert_eq!(first.blob_key, key);
    assert_eq!(second.blob_key, key);
    // Force budget pressure: the shared bytes evict once (one accounting
    // adjustment per key, rows and index entries stay for re-fetch).
    ensure_cache_budget(&h.services, 48, MediaClass::Preview).unwrap();
    assert!(h.cache.get(&key).unwrap().is_none());
    assert!(h.store.get(&first.id).unwrap().is_some());
    assert!(h.store.get(&second.id).unwrap().is_some());
    assert!(
        h.store
            .remote_cache_lookup("https://r.example/p0-first.png", "original")
            .unwrap()
            .is_some()
    );
    // The durable pool never participates in cache eviction.
    assert!(h.durable.get(&key).unwrap().is_none());
    // Refetch restores the shared bytes; reconciling twice converges.
    h.cache.put(&key, bytes).unwrap();
    h.store
        .cache_accounting_apply(
            bytes.len() as i64,
            MediaClass::Preview,
            bytes.len() as i64,
            0,
            1,
            bytes.len() as u64,
            0,
            0,
        )
        .unwrap();
    let first_report = reconcile_media(&h.services).unwrap();
    let second_report = reconcile_media(&h.services).unwrap();
    assert_eq!(second_report.missing_marked_evicted, 0);
    assert_eq!(h.cache.get(&key).unwrap().as_deref(), Some(&bytes[..]));
    let stats = h.store.cache_stats().unwrap();
    assert_eq!(stats.total_bytes, stats.preview_bytes);
    let _ = first_report;
    assert_quiescent(&h);
}

#[test]
fn p0_incomplete_scan_deletes_nothing() {
    // An explicitly incomplete live-key set refuses the destructive sweep.
    let (h, _owner) = p0_harness(512 << 20);
    let live_bytes = b"live-payload-p0-5";
    let live_key = sha256_hex(live_bytes);
    h.cache.put(&live_key, live_bytes).unwrap();
    let orphan_bytes = b"orphan-payload-p0-5";
    let orphan_key = sha256_hex(orphan_bytes);
    h.cache.put(&orphan_key, orphan_bytes).unwrap();
    let mut live = HashSet::new();
    live.insert(live_key.clone());
    let err = sweep_orphan_blobs_checked(&h.dir.join("blobs_cache").join("objects"), &live, false)
        .expect_err("incomplete scan must refuse");
    assert!(format!("{err:?}").contains("incomplete"));
    assert_eq!(
        h.cache.get(&live_key).unwrap().as_deref(),
        Some(&live_bytes[..])
    );
    assert_eq!(
        h.cache.get(&orphan_key).unwrap().as_deref(),
        Some(&orphan_bytes[..])
    );
    // A complete pooled sweep removes exactly the true orphan.
    let report = sweep_orphan_pools(
        &h.services,
        &h.dir.join("blobs_durable").join("objects"),
        &h.dir.join("blobs_cache").join("objects"),
    )
    .unwrap();
    assert!(report.complete);
    assert_eq!(report.cache_removed, 2);
    assert_eq!(report.durable_removed, 0);
    assert!(h.cache.get(&orphan_key).unwrap().is_none());
}

#[test]
fn p0_exclusive_barrier_blocks_shared_writers() {
    // While the exclusive sweep barrier is held, a second exclusive acquire
    // fails fast and shared writers behind it observe contention ordering.
    let exclusive = acquire_media_exclusive("p0-test-sweep").unwrap();
    assert!(try_acquire_media_exclusive("p0-test-second").is_none());
    drop(exclusive);
    assert!(try_acquire_media_exclusive("p0-test-after").is_some());
    let _shared = acquire_media_op("p0-test-shared").unwrap();
    let _key = acquire_blob_key("abc123").unwrap();
}

#[test]
fn p0_missing_durable_bytes_reported_not_evicted() {
    // A durable row whose bytes are unreadable stays visible and is reported
    // as damage; it is never normalized into a routine cache eviction.
    let (h, owner) = p0_harness(512 << 20);
    let meta = h
        .services
        .upload_media(
            &owner,
            "d.bin".to_string(),
            b"durable-p0-7",
            None,
            false,
            false,
        )
        .unwrap();
    h.durable.delete(&meta.blob_key).unwrap();
    let report = reconcile_media(&h.services).unwrap();
    assert_eq!(report.durable_missing, 1, "{report:?}");
    assert!(report.missing_marked_evicted == 0);
    assert!(h.store.get(&meta.id).unwrap().is_some());
}

#[test]
fn p0_concurrent_lifecycle_stress() {
    // Barrier-started upload/delete/reconcile/sweep hammer; after quiescence
    // every invariant in `assert_quiescent` holds exactly.
    let (h, owner) = p0_harness(512 << 20);
    let (durable_keys, _) = collect_live_blob_keys(&h.services).unwrap();
    assert!(durable_keys.is_empty());
    let threads = 4usize;
    let iters = 8usize;
    let barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::new();
    for t in 0..threads {
        let services = h.services.clone();
        let owner = owner.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for i in 0..iters {
                let payload = format!("stress-p0-t{t}-i{i}-payload");
                let bytes = payload.as_bytes();
                let meta = services
                    .upload_media(&owner, format!("s-{t}-{i}.bin"), bytes, None, false, true)
                    .unwrap();
                if i % 2 == 0 {
                    services.delete_media(&owner, &meta.id).unwrap();
                }
                if i % 4 == 3 {
                    let _ = reconcile_media(&services).unwrap();
                }
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    // Converge once in quiescence (no writers active): a repair written by
    // a mid-stress complete walk may be stale, the quiescent walk is exact.
    let _ = reconcile_media(&h.services).unwrap();
    assert_quiescent(&h);
    // Exactly the odd-iteration rows survive per thread.
    assert_eq!(all_rows(&h.services).len(), threads * iters / 2);
}
