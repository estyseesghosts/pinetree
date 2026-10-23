//! Eviction starvation (adversarial-2 slice 2: finding 2 plus the
//! tombstone half of finding 9). Charged-key pagination replaces the
//! oldest-8 raw-row window; tombstones never occupy the candidate window;
//! alias completeness comes from the blob-key index under the key lock,
//! independent of the listing window; a same-millisecond touch counts as
//! newer through the per-row generation; and a bounded retention pass
//! forgets stale tombstones whose media row is already gone (never ones a
//! live media row still references, never any bytes).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, CacheStats, InsertMediaInput, MediaClass, MediaStore,
    NoopStreamingSink, RemoteCacheRecord,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::{MediaMeta, MediaProperties};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest;
use pinetree::media::remote_cache::ensure_cache_budget;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
const DAY_MS: i64 = 86_400_000;
const OLD_MS: i64 = BASE_MS - 30 * DAY_MS;
static SEQ: AtomicU64 = AtomicU64::new(0);

struct Harness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    cache: Arc<FsBlobStore>,
}

fn harness(remote_cache_max_bytes: u64) -> Harness {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-evstarve-{}-{n}", std::process::id()));
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
        // Class caps stay far above the tiny global under test so only the
        // global budget drives these reservations.
        remote_cache_avatar_bytes: 1 << 20,
        remote_cache_preview_bytes: 1 << 20,
        remote_cache_full_bytes: 1 << 20,
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
    }
}

/// Distinct PNG-ish bytes per seed so every row owns its blob key.
fn row_bytes(len: usize, seed: u8) -> Vec<u8> {
    let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    bytes.resize(len, seed);
    bytes[8] = seed;
    bytes
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
            generation: 0,
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

#[test]
fn tombstones_do_not_starve_live_rows() {
    // Ten historical tombstones (30d old) followed by three newer live
    // rows: the reservation must evict the live rows even though the
    // tombstones would have filled the old oldest-8 window twice over.
    let h = harness(100);
    let mut tombstone_urls = Vec::new();
    let mut tombstone_ids = Vec::new();
    for i in 0..10 {
        let url = format!("https://cdn.example/old-{i}.png");
        let meta = insert_cache_row(
            &h,
            &url,
            MediaClass::Preview,
            &row_bytes(10, i as u8),
            OLD_MS,
            OLD_MS,
        );
        charge(&h, MediaClass::Preview, 10);
        tombstone_urls.push(url);
        tombstone_ids.push(meta.id);
    }
    assert_eq!(stats(&h).total_bytes, 100);
    ensure_cache_budget(&h.services, 100, MediaClass::Preview).unwrap();
    assert_eq!(stats(&h).total_bytes, 0);
    assert_eq!(stats(&h).evictions, 10);
    // Tombstones keep their age: eviction must not advance last_access_ms
    // as a fake freshness fix.
    for url in &tombstone_urls {
        let rec = index_row(&h, url);
        assert!(rec.bytes_absent && rec.size_bytes == 0, "{url}");
        assert_eq!(rec.last_access_ms, OLD_MS, "{url}");
    }
    // The charged window holds no tombstones.
    assert!(
        h.store
            .remote_cache_charged_page(0, 100)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        h.store.remote_cache_tombstone_page(0, 100).unwrap().len(),
        10
    );

    let mut live_urls = Vec::new();
    let mut live_keys = Vec::new();
    for i in 0..3 {
        let url = format!("https://cdn.example/live-{i}.png");
        let bytes = row_bytes(30, 100 + i);
        insert_cache_row(&h, &url, MediaClass::Preview, &bytes, BASE_MS, BASE_MS);
        charge(&h, MediaClass::Preview, 30);
        live_urls.push(url);
        live_keys.push(ingest::sha256_hex(&bytes));
    }
    assert_eq!(stats(&h).total_bytes, 90);
    // Orphan one tombstone's media row: the retention pass may forget it.
    // The other nine stay anchored by their live media rows.
    h.store.delete_metadata(&tombstone_ids[0]).unwrap();
    ensure_cache_budget(&h.services, 50, MediaClass::Preview).unwrap();
    // Retention dropped exactly the media-less tombstone.
    assert!(
        h.store
            .remote_cache_lookup(&tombstone_urls[0], "original")
            .unwrap()
            .is_none()
    );
    for url in &tombstone_urls[1..] {
        let rec = index_row(&h, url);
        assert!(rec.bytes_absent && rec.size_bytes == 0, "{url}");
    }
    // 90 + 50 > 100 over a 100 cap: two 30-byte live rows evicted, one
    // survives with bytes and counters intact.
    assert_eq!(stats(&h).total_bytes, 30);
    assert_eq!(stats(&h).evictions, 12);
    let evicted_live: Vec<usize> = live_urls
        .iter()
        .enumerate()
        .filter(|(_, url)| index_row(&h, url).bytes_absent)
        .map(|(i, _)| i)
        .collect();
    assert_eq!(evicted_live.len(), 2);
    for i in evicted_live {
        let rec = index_row(&h, &live_urls[i]);
        assert_eq!(rec.size_bytes, 0);
        assert!(h.cache.byte_len(&live_keys[i]).unwrap().is_none());
    }
    // The surviving reservation fits: the failure-then-stuck old code left
    // the total at 90 and could not reserve.
    assert!(h.store.reserve_cache_bytes(50).unwrap());
    h.store.release_cache_reservation(50).unwrap();
}

#[test]
fn reservation_needing_many_evictions_succeeds() {
    // Eight old tombstones permanently occupy the head of the raw index,
    // followed by twelve live 10-byte rows under a 100 cap needing 100:
    // twelve evictions in one call. The old oldest-8 window never looked
    // past the tombstones, so the reservation starved with 120 reclaimable
    // bytes on disk.
    let h = harness(100);
    for i in 0..8 {
        let url = format!("https://cdn.example/old-{i}.png");
        insert_cache_row(
            &h,
            &url,
            MediaClass::Preview,
            &row_bytes(10, i as u8),
            OLD_MS,
            OLD_MS,
        );
        charge(&h, MediaClass::Preview, 10);
    }
    ensure_cache_budget(&h.services, 100, MediaClass::Preview).unwrap();
    assert_eq!(stats(&h).total_bytes, 0);
    assert_eq!(stats(&h).evictions, 8);
    let mut urls = Vec::new();
    for i in 0..12 {
        let url = format!("https://cdn.example/many-{i}.png");
        insert_cache_row(
            &h,
            &url,
            MediaClass::Preview,
            &row_bytes(10, 100 + i as u8),
            BASE_MS,
            BASE_MS,
        );
        charge(&h, MediaClass::Preview, 10);
        urls.push(url);
    }
    assert_eq!(stats(&h).total_bytes, 120);
    ensure_cache_budget(&h.services, 100, MediaClass::Preview).unwrap();
    assert_eq!(stats(&h).total_bytes, 0);
    assert_eq!(stats(&h).evictions, 20);
    for url in &urls {
        let rec = index_row(&h, url);
        assert!(rec.bytes_absent && rec.size_bytes == 0, "{url}");
    }
    assert!(h.store.reserve_cache_bytes(100).unwrap());
    h.store.release_cache_reservation(100).unwrap();
}

#[test]
fn stuck_window_pages_forward() {
    // Thirty-three replaced-bytes rows (oldest, permanently skipped: the
    // pool bytes no longer match the charged size) precede two live rows.
    // The loop must page past the stuck window instead of retrying it.
    let h = harness(100);
    for i in 0..33 {
        let url = format!("https://cdn.example/stuck-{i}.png");
        let bytes = row_bytes(10, i as u8);
        let key = ingest::sha256_hex(&bytes);
        insert_cache_row(&h, &url, MediaClass::Preview, &bytes, OLD_MS, OLD_MS);
        charge(&h, MediaClass::Preview, 10);
        h.cache.put(&key, &[0xEEu8; 3]).unwrap();
    }
    let mut live_urls = Vec::new();
    for i in 0..2 {
        let url = format!("https://cdn.example/unstuck-{i}.png");
        insert_cache_row(
            &h,
            &url,
            MediaClass::Preview,
            &row_bytes(20, 200 + i),
            BASE_MS,
            BASE_MS,
        );
        charge(&h, MediaClass::Preview, 20);
        live_urls.push(url);
    }
    assert_eq!(stats(&h).total_bytes, 370);
    ensure_cache_budget(&h.services, 50, MediaClass::Preview).unwrap();
    // Both live rows evicted; every stuck row left exactly as found (its
    // replacement bytes still readable, its charge untouched).
    for url in &live_urls {
        let rec = index_row(&h, url);
        assert!(rec.bytes_absent && rec.size_bytes == 0, "{url}");
    }
    assert_eq!(stats(&h).total_bytes, 330);
    assert_eq!(stats(&h).evictions, 2);
    for i in 0..33 {
        let url = format!("https://cdn.example/stuck-{i}.png");
        let rec = index_row(&h, &url);
        assert!(!rec.bytes_absent && rec.size_bytes == 10, "{url}");
        let key = ingest::sha256_hex(&row_bytes(10, i as u8));
        assert_eq!(h.cache.get(&key).unwrap().unwrap(), vec![0xEEu8; 3]);
    }
}

#[test]
fn shared_blob_many_aliases_evicts_once() {
    // One 40-byte physical blob shared by ten URL rows with different
    // access times: the complete alias set resolves under the key lock,
    // so the key evicts once and debits once no matter how the aliases
    // straddle the listing window.
    let h = harness(100);
    let bytes = row_bytes(40, 0xA5);
    let key = ingest::sha256_hex(&bytes);
    h.cache.put(&key, &bytes).unwrap();
    let mut urls = Vec::new();
    for i in 0..10 {
        let url = format!("https://cdn.example/shared-{i}.png");
        let meta = h
            .store
            .insert_metadata(InsertMediaInput {
                id: None,
                owner_id: None,
                name: "remote.bin".to_string(),
                mime: "image/png".to_string(),
                size_bytes: bytes.len() as u64,
                hash: ingest::md5_hex(&bytes),
                blob_key: key.clone(),
                blurhash: None,
                properties: MediaProperties::default(),
                is_sensitive: false,
                is_link: true,
                url: format!("https://pinetree.local/files/remote-{url}"),
                thumbnail_url: None,
                comment: None,
                source_url: Some(url.clone()),
                max_file_size: 10_000_000,
            })
            .unwrap();
        h.store
            .remote_cache_insert(&RemoteCacheRecord {
                source_url: url.clone(),
                variant: "original".to_string(),
                media_id: meta.id.clone(),
                blob_key: key.clone(),
                size_bytes: bytes.len() as u64,
                class: MediaClass::Preview,
                fetched_at_ms: BASE_MS - (i as i64) * DAY_MS,
                last_access_ms: BASE_MS - (i as i64) * DAY_MS,
                access_count: 1,
                bytes_absent: false,
                generation: 0,
            })
            .unwrap();
        urls.push((url, meta.id));
    }
    // Ten aliases, one charge.
    charge(&h, MediaClass::Preview, 40);
    assert_eq!(stats(&h).total_bytes, 40);
    assert_eq!(h.store.cache_entries_for_blob_key(&key).unwrap().len(), 10);
    ensure_cache_budget(&h.services, 70, MediaClass::Preview).unwrap();
    assert_eq!(stats(&h).total_bytes, 0);
    assert_eq!(stats(&h).evictions, 1);
    assert!(h.cache.byte_len(&key).unwrap().is_none());
    for (url, media_id) in &urls {
        let rec = index_row(&h, url);
        assert!(rec.bytes_absent && rec.size_bytes == 0, "{url}");
        assert!(h.store.get(media_id).unwrap().unwrap().evicted, "{url}");
    }
    // Second call moves nothing: the debit happened exactly once.
    let settled = stats(&h);
    ensure_cache_budget(&h.services, 70, MediaClass::Preview).unwrap();
    assert_eq!(stats(&h), settled);
}

#[test]
fn same_millisecond_touch_advances_generation() {
    // Guard-half mechanism (finding 5): two touches at the same
    // millisecond leave every timestamp standing still, so timestamps
    // alone would read "unchanged" — the generation must still move. The
    // eviction skip on a generation mismatch is pinned by the same-ms
    // unit test in `media::remote_cache::evict`.
    let h = harness(100);
    let url = "https://cdn.example/clock.png";
    insert_cache_row(
        &h,
        url,
        MediaClass::Preview,
        &row_bytes(10, 7),
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 10);
    let first = index_row(&h, url);
    h.store
        .remote_cache_touch(url, "original", BASE_MS)
        .unwrap();
    let second = index_row(&h, url);
    h.store
        .remote_cache_touch(url, "original", BASE_MS)
        .unwrap();
    let third = index_row(&h, url);
    assert_eq!(first.last_access_ms, BASE_MS);
    assert_eq!(second.last_access_ms, BASE_MS);
    assert_eq!(third.last_access_ms, BASE_MS);
    assert!(first.generation < second.generation);
    assert!(second.generation < third.generation);
}
