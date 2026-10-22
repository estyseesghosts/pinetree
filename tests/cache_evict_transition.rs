//! Cache-eviction shared-key transition (WP1 §3 + missed-by-audit): one
//! physical key debits once no matter how many index rows share it;
//! eviction re-validates under the key lock (touch/refetch/replace skips);
//! refetch restores without double-charging (same id for the same URL);
//! reconcile never debits twice; backup skips evicted rows; the orphan
//! sweep only drops truly unreferenced bytes; per-class TTL orders
//! candidates.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, CacheStats, InsertMediaInput, MediaClass, MediaStore,
    NoopStreamingSink, RemoteCacheRecord,
};
use pinetree::backup::{BackupContext, request_backup, run_backup_blocking};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::{MediaMeta, MediaProperties};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest;
use pinetree::media::reconcile::{collect_live_blob_keys, reconcile_media, sweep_orphan_pools};
use pinetree::media::remote_cache::{ensure_cache_budget, fetch_on_first_request};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
const DAY_MS: i64 = 86_400_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

/// Serializes the exclusive-gate tests (backup + orphan sweep): the media
/// gate is process-global, so two such tests overlapping in time would
/// serialize on the lock anyway — this only keeps the gate-holding sections
/// ordered for hermetic temp dirs.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Harness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    cache: Arc<FsBlobStore>,
    dir: std::path::PathBuf,
}

fn png_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    bytes.resize(len, 0xA5);
    bytes
}

fn harness(remote_cache_max_bytes: u64) -> Harness {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-cachetrans-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(BASE_MS));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    store.set_cache_cap(64 << 20).unwrap();
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let services = Arc::new(AppServices {
        clock: clock.clone(),
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
        remote_cache_max_bytes,
        remote_cache_avatar_bytes: 30,
        remote_cache_preview_bytes: 40,
        remote_cache_full_bytes: 30,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    });
    Harness {
        store,
        services,
        cache,
        dir,
    }
}

/// Insert one cache row the way the fetch path does (bytes + metadata +
/// index). Accounting is applied separately by the caller so shared-key
/// fixtures charge exactly once.
fn insert_cache_row(
    h: &Harness,
    source_url: &str,
    class: MediaClass,
    bytes: &[u8],
    fetched_at_ms: i64,
    last_access_ms: i64,
) -> MediaMeta {
    let key = ingest::sha256_hex(bytes);
    h.cache.put(&key, bytes).unwrap();
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: bytes.len() as u64,
            hash: ingest::md5_hex(bytes),
            blob_key: key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: format!("https://pinetree.local/files/remote-{source_url}"),
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
            class,
            fetched_at_ms,
            last_access_ms,
            access_count: 1,
            bytes_absent: false,
        })
        .unwrap();
    meta
}

fn charge(h: &Harness, class: MediaClass, bytes: u64) {
    h.store
        .cache_accounting_apply(bytes as i64, class, bytes as i64, 0, 1, bytes, 0, 0)
        .unwrap();
}

fn stats(h: &Harness) -> CacheStats {
    h.store.cache_stats().unwrap()
}

fn index_row(h: &Harness, url: &str) -> RemoteCacheRecord {
    h.store
        .remote_cache_lookup(url, "original")
        .unwrap()
        .unwrap()
}

/// Serve one canned body on every path; returns the bound address.
fn serve_bytes(body: Vec<u8>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for _ in 0..8 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let hdr = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(hdr.as_bytes());
            let _ = stream.write_all(&body);
        }
    });
    addr
}

#[test]
fn repeated_ensure_on_one_key_moves_stats_once() {
    let h = harness(100);
    let bytes = png_bytes(40);
    let meta = insert_cache_row(
        &h,
        "https://cdn.example/one.png",
        MediaClass::Preview,
        &bytes,
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 40);
    assert_eq!(stats(&h).total_bytes, 40);
    // 40 + 70 > 100: evicts the one key.
    ensure_cache_budget(&h.services, 70, MediaClass::Preview).unwrap();
    let after = stats(&h);
    assert_eq!(after.total_bytes, 0);
    assert_eq!(after.evictions, 1);
    assert!(
        h.cache
            .byte_len(&ingest::sha256_hex(&bytes))
            .unwrap()
            .is_none()
    );
    let rec = index_row(&h, "https://cdn.example/one.png");
    assert!(rec.bytes_absent && rec.size_bytes == 0);
    assert!(h.store.get(&meta.id).unwrap().unwrap().evicted);
    // Second call fits already: counters must not move.
    ensure_cache_budget(&h.services, 70, MediaClass::Preview).unwrap();
    assert_eq!(stats(&h), after);
}

#[test]
fn shared_key_two_urls_single_charge_evict_and_restore() {
    let h = harness(100);
    let bytes = png_bytes(40);
    let key = ingest::sha256_hex(&bytes);
    let addr = serve_bytes(bytes.clone());
    let url_a = format!("http://{addr}/a.png");
    let url_b = format!("http://{addr}/b.png");
    let meta_a = insert_cache_row(&h, &url_a, MediaClass::Preview, &bytes, BASE_MS, BASE_MS);
    let meta_b = insert_cache_row(&h, &url_b, MediaClass::Preview, &bytes, BASE_MS, BASE_MS);
    // Two URLs, one SHA-256: accounting equals one object.
    charge(&h, MediaClass::Preview, 40);
    assert_eq!(stats(&h).total_bytes, 40);
    // Evicting once deletes the file once and transitions both rows.
    ensure_cache_budget(&h.services, 70, MediaClass::Preview).unwrap();
    let after = stats(&h);
    assert_eq!(after.total_bytes, 0);
    assert_eq!(after.evictions, 1);
    assert!(h.cache.byte_len(&key).unwrap().is_none());
    for url in [&url_a, &url_b] {
        let rec = index_row(&h, url);
        assert!(rec.bytes_absent && rec.size_bytes == 0, "{url}");
    }
    assert!(h.store.get(&meta_a.id).unwrap().unwrap().evicted);
    assert!(h.store.get(&meta_b.id).unwrap().unwrap().evicted);
    // Second call moves nothing.
    ensure_cache_budget(&h.services, 70, MediaClass::Preview).unwrap();
    assert_eq!(stats(&h), after);
    // Refetching the second URL restores both rows but charges only the
    // one object again (same media id for the refetched URL).
    let meta = fetch_on_first_request(&h.services, &url_b, "original").unwrap();
    assert_eq!(meta.id, meta_b.id);
    assert_eq!(stats(&h).total_bytes, 40);
    for url in [&url_a, &url_b] {
        let rec = index_row(&h, url);
        assert!(!rec.bytes_absent && rec.size_bytes == 40, "{url}");
    }
    assert!(!h.store.get(&meta_a.id).unwrap().unwrap().evicted);
    assert!(!h.store.get(&meta_b.id).unwrap().unwrap().evicted);
    assert_eq!(h.cache.get(&key).unwrap().unwrap(), bytes);
}

#[test]
fn eviction_skips_replaced_bytes_and_leaves_counters() {
    // Byte-length divergence (a refetch replacing pool bytes after the
    // candidate listing): the body skips, bytes stay readable, counters
    // stay put.
    let h = harness(100);
    let bytes = png_bytes(40);
    let key = ingest::sha256_hex(&bytes);
    insert_cache_row(
        &h,
        "https://cdn.example/replaced.png",
        MediaClass::Preview,
        &bytes,
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 40);
    let before = stats(&h);
    h.cache.put(&key, &png_bytes(10)).unwrap();
    ensure_cache_budget(&h.services, 70, MediaClass::Preview).unwrap();
    assert_eq!(h.cache.get(&key).unwrap().unwrap(), png_bytes(10));
    assert_eq!(stats(&h), before);
}

#[test]
fn evict_then_reconcile_marks_nothing_twice() {
    let h = harness(100);
    let bytes = png_bytes(40);
    let key = ingest::sha256_hex(&bytes);
    insert_cache_row(
        &h,
        "https://cdn.example/gone.png",
        MediaClass::Preview,
        &bytes,
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 40);
    ensure_cache_budget(&h.services, 70, MediaClass::Preview).unwrap();
    assert!(h.cache.byte_len(&key).unwrap().is_none());
    let after_evict = stats(&h);
    let report = reconcile_media(&h.services).unwrap();
    assert_eq!(report.missing_marked_evicted, 0);
    assert_eq!(stats(&h), after_evict);
}

#[test]
fn missing_bytes_reconcile_once_for_shared_key_then_idempotent() {
    let h = harness(1 << 20);
    let bytes = png_bytes(40);
    let key = ingest::sha256_hex(&bytes);
    let meta_a = insert_cache_row(
        &h,
        "https://cdn.example/sa.png",
        MediaClass::Preview,
        &bytes,
        BASE_MS,
        BASE_MS,
    );
    let meta_b = insert_cache_row(
        &h,
        "https://cdn.example/sb.png",
        MediaClass::Preview,
        &bytes,
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 40);
    h.cache.delete(&key).unwrap();
    // One shared key debits exactly once even with two rows missing bytes.
    let first = reconcile_media(&h.services).unwrap();
    assert_eq!(first.missing_marked_evicted, 1);
    assert_eq!(stats(&h).total_bytes, 0);
    for url in ["https://cdn.example/sa.png", "https://cdn.example/sb.png"] {
        let rec = index_row(&h, url);
        assert!(rec.bytes_absent && rec.size_bytes == 0, "{url}");
    }
    assert!(h.store.get(&meta_a.id).unwrap().unwrap().evicted);
    assert!(h.store.get(&meta_b.id).unwrap().unwrap().evicted);
    // Re-run is idempotent: nothing moves.
    let second = reconcile_media(&h.services).unwrap();
    assert_eq!(second.missing_marked_evicted, 0);
    assert_eq!(stats(&h).total_bytes, 0);
}

#[test]
fn evict_then_backup_succeeds_without_restored_bytes() {
    let _guard = serial();
    let h = harness(100);
    let bytes = png_bytes(40);
    insert_cache_row(
        &h,
        "https://cdn.example/backup.png",
        MediaClass::Preview,
        &bytes,
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 40);
    ensure_cache_budget(&h.services, 70, MediaClass::Preview).unwrap();
    // Evicted rows are excluded from the media inventory, so backup
    // creation must not require the deleted bytes.
    let ctx = BackupContext {
        store: h.store.clone(),
        durable_pool: h.dir.join("blobs_durable"),
        cache_pool: h.dir.join("blobs_cache"),
        backup_dir: h.dir.join("backups"),
        max_bundle_bytes: None,
        retention_max_count: None,
        retention_max_bytes: None,
    };
    let id = request_backup(&ctx).unwrap();
    let path = run_backup_blocking(&ctx, &id).unwrap();
    assert!(path.exists());
}

#[test]
fn orphan_sweep_keeps_referenced_drops_unreferenced() {
    let _guard = serial();
    let h = harness(20);
    let live_bytes = png_bytes(24);
    let live_key = ingest::sha256_hex(&live_bytes);
    let live_meta = insert_cache_row(
        &h,
        "https://cdn.example/live.png",
        MediaClass::Preview,
        &live_bytes,
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 24);
    let dead_key = "deaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddead".to_string();
    h.cache.put(&dead_key, b"orphan").unwrap();
    let report = sweep_orphan_pools(
        &h.services,
        &h.dir.join("blobs_durable").join("objects"),
        &h.dir.join("blobs_cache").join("objects"),
    )
    .unwrap();
    assert!(report.complete);
    assert_eq!(report.cache_removed, 1);
    assert_eq!(h.cache.get(&live_key).unwrap().unwrap(), live_bytes);
    assert!(h.cache.byte_len(&dead_key).unwrap().is_none());
    // Live sets skip evicted owner-less rows (and key-less rows).
    let (durable_live, cache_live) = collect_live_blob_keys(&h.services).unwrap();
    assert!(durable_live.is_empty());
    assert!(cache_live.contains(&live_key));
    // Evict the live row, restore its bytes without clearing flags, and
    // confirm the evicted row no longer protects the key.
    ensure_cache_budget(&h.services, 30, MediaClass::Preview).unwrap();
    assert!(h.store.get(&live_meta.id).unwrap().unwrap().evicted);
    assert!(h.cache.byte_len(&live_key).unwrap().is_none());
    h.cache.put(&live_key, &live_bytes).unwrap();
    let (_, cache_live) = collect_live_blob_keys(&h.services).unwrap();
    assert!(!cache_live.contains(&live_key));
}

#[test]
fn candidate_order_uses_candidate_class_ttl() {
    // 30 (avatar) + 40 (full) = 70; need 40 over a 100 cap evicts exactly
    // one. The avatar is LRU-older (8d) but fresh under its 30d TTL; the
    // full row (4d) is stale under its 3d TTL — stale-first order evicts
    // the full row even though LRU alone would pick the avatar.
    let h = harness(100);
    let avatar_bytes = png_bytes(30);
    let full_bytes = png_bytes(40);
    let avatar_meta = insert_cache_row(
        &h,
        "https://cdn.example/av.png",
        MediaClass::Avatar,
        &avatar_bytes,
        BASE_MS - 8 * DAY_MS,
        BASE_MS - 8 * DAY_MS,
    );
    let full_meta = insert_cache_row(
        &h,
        "https://cdn.example/full.bin",
        MediaClass::Full,
        &full_bytes,
        BASE_MS - 4 * DAY_MS,
        BASE_MS - 4 * DAY_MS,
    );
    charge(&h, MediaClass::Avatar, 30);
    charge(&h, MediaClass::Full, 40);
    ensure_cache_budget(&h.services, 40, MediaClass::Full).unwrap();
    assert_eq!(
        h.cache
            .get(&ingest::sha256_hex(&avatar_bytes))
            .unwrap()
            .unwrap(),
        avatar_bytes
    );
    assert!(
        h.cache
            .byte_len(&ingest::sha256_hex(&full_bytes))
            .unwrap()
            .is_none()
    );
    assert!(!h.store.get(&avatar_meta.id).unwrap().unwrap().evicted);
    assert!(h.store.get(&full_meta.id).unwrap().unwrap().evicted);
}
