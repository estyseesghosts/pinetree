//! Wave 2 eviction scans and tombstone retention (`docs/adversarial-3.md`).
//!
//! - M1: charged/tombstone windows are cursor-backed (no full-table
//!   collect/decode/sort per page); the eviction commit resolves aliases
//!   through the blob-key index and probes durability without a
//!   write-locked `MEDIA_META` scan. A large tombstone + charged prefix
//!   still evicts a known tail key, and a touch reorders the charged
//!   cursor.
//! - M2: the retention pass skip-counts unremovable head rows and pages
//!   forward in the same pass (bounded), so one pass removes an expired
//!   tail sitting behind 64 unremovable heads. The old
//!   `tombstone_page(0, 64)` + `continue` logic only ever visited rows
//!   `[0, 64)`, so the tail at index 64 was unreachable in one pass.
//! - L2: read-only outcomes (`AlreadyAbsent`, `SkippedDurableRef`) come
//!   from the read-phase snapshot; the write transaction only opens when a
//!   mutation happens. These pin the outcomes, not the lock scope.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, CacheEvictionOutcome, CacheStats, InsertMediaInput, MediaClass,
    MediaStore, NoopStreamingSink, RemoteCacheRecord,
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
static WAVE2_SEQ: AtomicU64 = AtomicU64::new(0);

struct Wave2Harness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    cache: Arc<FsBlobStore>,
}

fn wave2_harness(remote_cache_max_bytes: u64) -> Wave2Harness {
    let n = WAVE2_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-wave2scan-{}-{n}", std::process::id()));
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
        remote_cache_avatar_bytes: 1 << 20,
        remote_cache_preview_bytes: 1 << 20,
        remote_cache_full_bytes: 1 << 20,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    });
    Wave2Harness {
        store,
        services,
        cache,
    }
}

/// Distinct bytes per index so every row owns its blob key.
fn wave2_bytes(seed: usize) -> Vec<u8> {
    let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    bytes.extend_from_slice(&(seed as u64).to_le_bytes());
    bytes.resize(24, (seed % 250) as u8);
    bytes
}

/// Insert one CHARGED cache row (pool bytes + metadata + index + Caller
/// charges accounting separately so shared-key fixtures charge once).
fn wave2_insert_charged(
    h: &Wave2Harness,
    source_url: &str,
    bytes: &[u8],
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
            url: format!("https://pinetree.local/files/wave2-{source_url}"),
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
            fetched_at_ms: last_access_ms,
            last_access_ms,
            access_count: 1,
            bytes_absent: false,
            generation: 0,
        })
        .unwrap();
    meta
}

/// Insert one TOMBSTONE cache row directly (zero-byte/absent, no pool
/// bytes, no accounting charge): the exact row shape the eviction commit
/// produces. `live_media` controls whether the retention pass must skip
/// it (refetch anchor) or may forget it.
fn wave2_insert_tombstone(
    h: &Wave2Harness,
    tag: &str,
    index: usize,
    last_access_ms: i64,
    live_media: bool,
) -> (String, String) {
    let url = format!("https://cdn.example/wave2-{tag}-{index}.png");
    let blob_key = ingest::sha256_hex(format!("wave2-{tag}-{index}").as_bytes());
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: 10,
            hash: ingest::md5_hex(format!("wave2-{tag}-{index}").as_bytes()),
            blob_key: blob_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: format!("https://pinetree.local/files/wave2-{tag}-{index}"),
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
            blob_key: blob_key.clone(),
            size_bytes: 0,
            class: MediaClass::Preview,
            fetched_at_ms: last_access_ms,
            last_access_ms,
            access_count: 1,
            bytes_absent: true,
            generation: 0,
        })
        .unwrap();
    if !live_media {
        h.store.delete_metadata(&meta.id).unwrap();
    }
    (url, blob_key)
}

fn wave2_charge(h: &Wave2Harness, bytes: u64) {
    h.store
        .cache_accounting_apply(
            bytes as i64,
            MediaClass::Preview,
            bytes as i64,
            0,
            1,
            bytes,
            0,
            0,
        )
        .unwrap();
}

fn wave2_stats(h: &Wave2Harness) -> CacheStats {
    h.store.cache_stats().unwrap()
}

#[test]
fn wave2_charged_window_skips_tombstone_prefix_and_evicts_tail() {
    // M1: 400 tombstones (oldest) + 128 charged prefix rows, then a known
    // tail key that is newest in LRU order. The charged window must skip
    // the tombstone prefix entirely, page past the charged prefix, and
    // evict the tail — via the alias-anchored commit path (empty
    // `index_keys`, so the old write-locked full `REMOTE_CACHE` scan would
    // have been the fallback).
    let h = wave2_harness(1 << 30);
    for i in 0..400 {
        wave2_insert_tombstone(&h, "prefix", i, OLD_MS, true);
    }
    for i in 0..128 {
        let url = format!("https://cdn.example/wave2-live-{i}.png");
        let bytes = wave2_bytes(1000 + i);
        wave2_insert_charged(&h, &url, &bytes, BASE_MS - 200_000 + i as i64);
        wave2_charge(&h, bytes.len() as u64);
    }
    // Tail key, newest in LRU order.
    let tail_bytes = wave2_bytes(999_999);
    let tail_url = "https://cdn.example/wave2-tail.png".to_string();
    let tail_meta = wave2_insert_charged(&h, &tail_url, &tail_bytes, BASE_MS);
    wave2_charge(&h, tail_bytes.len() as u64);
    let tail_key = ingest::sha256_hex(&tail_bytes);

    // The charged window holds exactly the 129 charged rows, oldest first,
    // with the tail last; no tombstone ever occupies it.
    let first_page = h.store.remote_cache_charged_page(0, 200).unwrap();
    assert_eq!(first_page.len(), 129);
    assert!(
        first_page
            .iter()
            .all(|r| !r.bytes_absent && r.size_bytes > 0)
    );
    assert_eq!(first_page.last().unwrap().source_url, tail_url);
    // Paging past the whole charged prefix lands on the tail alone.
    let tail_page = h.store.remote_cache_charged_page(128, 32).unwrap();
    assert_eq!(tail_page.len(), 1);
    assert_eq!(tail_page[0].source_url, tail_url);
    // The tombstone window holds exactly the 400 prefix rows.
    let tombs = h.store.remote_cache_tombstone_page(0, 500).unwrap();
    assert_eq!(tombs.len(), 400);
    assert!(tombs.iter().all(|r| r.bytes_absent || r.size_bytes == 0));

    // Evict the tail through the alias-anchored commit path (empty
    // `index_keys`): single debit, generation bump, no fake freshness.
    let before = wave2_stats(&h);
    match h.store.commit_cache_eviction(
        &tail_key,
        tail_bytes.len() as u64,
        MediaClass::Preview,
        &[],
    ) {
        Ok(CacheEvictionOutcome::Evicted { debited_bytes }) => {
            assert_eq!(debited_bytes, tail_bytes.len() as u64)
        }
        other => panic!("expected Evicted, got {other:?}"),
    }
    let after = wave2_stats(&h);
    assert_eq!(
        after.total_bytes,
        before.total_bytes - tail_bytes.len() as u64
    );
    assert_eq!(
        after.preview_bytes,
        before.preview_bytes - tail_bytes.len() as u64
    );
    assert_eq!(after.evictions, before.evictions + 1);
    let tombed = h
        .store
        .remote_cache_lookup(&tail_url, "original")
        .unwrap()
        .unwrap();
    assert!(tombed.bytes_absent && tombed.size_bytes == 0);
    assert_eq!(tombed.generation, 2);
    assert_eq!(tombed.last_access_ms, BASE_MS);
    assert!(h.store.get(&tail_meta.id).unwrap().unwrap().evicted);
}

#[test]
fn wave2_shared_key_evicts_once_with_complete_aliases() {
    // M1 alias-completeness: two aliases share one physical key; the
    // commit tombstones both and debits once (production charges once per
    // key, so the fixture charges once too).
    let h = wave2_harness(1 << 30);
    let bytes = wave2_bytes(4242);
    let key = ingest::sha256_hex(&bytes);
    h.cache.put(&key, &bytes).unwrap();
    let mut urls = Vec::new();
    let mut metas = Vec::new();
    for i in 0..2 {
        let url = format!("https://cdn.example/wave2-shared-{i}.png");
        let meta = wave2_insert_charged(&h, &url, &bytes, BASE_MS - 10 + i as i64);
        urls.push(url);
        metas.push(meta);
    }
    wave2_charge(&h, bytes.len() as u64);
    assert_eq!(wave2_stats(&h).total_bytes, bytes.len() as u64);

    let index_keys: Vec<(String, String)> = urls
        .iter()
        .map(|u| (u.clone(), "original".to_string()))
        .collect();
    match h
        .store
        .commit_cache_eviction(&key, bytes.len() as u64, MediaClass::Preview, &index_keys)
    {
        Ok(CacheEvictionOutcome::Evicted { debited_bytes }) => {
            assert_eq!(debited_bytes, bytes.len() as u64)
        }
        other => panic!("expected Evicted, got {other:?}"),
    }
    assert_eq!(wave2_stats(&h).total_bytes, 0);
    for (url, meta) in urls.iter().zip(metas.iter()) {
        let rec = h
            .store
            .remote_cache_lookup(url, "original")
            .unwrap()
            .unwrap();
        assert!(rec.bytes_absent && rec.size_bytes == 0, "{url}");
        assert!(h.store.get(&meta.id).unwrap().unwrap().evicted);
    }
    // Idempotent: a second commit reports AlreadyAbsent and moves nothing.
    let stats = wave2_stats(&h);
    match h
        .store
        .commit_cache_eviction(&key, bytes.len() as u64, MediaClass::Preview, &index_keys)
    {
        Ok(CacheEvictionOutcome::AlreadyAbsent) => {}
        other => panic!("expected AlreadyAbsent, got {other:?}"),
    }
    assert_eq!(wave2_stats(&h), stats);
}

#[test]
fn wave2_touch_reorders_charged_cursor() {
    // M1 cursor maintenance: a touch moves the row to the fresh end of the
    // charged window (same transaction as the row write).
    let h = wave2_harness(1 << 30);
    let mut urls = Vec::new();
    for i in 0..4 {
        let url = format!("https://cdn.example/wave2-order-{i}.png");
        let bytes = wave2_bytes(5000 + i);
        wave2_insert_charged(&h, &url, &bytes, BASE_MS - 40 + i as i64);
        wave2_charge(&h, bytes.len() as u64);
        urls.push(url);
    }
    let before: Vec<String> = h
        .store
        .remote_cache_charged_page(0, 10)
        .unwrap()
        .iter()
        .map(|r| r.source_url.clone())
        .collect();
    assert_eq!(before, urls);
    h.store
        .remote_cache_touch(&urls[0], "original", BASE_MS + 1)
        .unwrap();
    let after: Vec<String> = h
        .store
        .remote_cache_charged_page(0, 10)
        .unwrap()
        .iter()
        .map(|r| r.source_url.clone())
        .collect();
    assert_eq!(
        after,
        vec![
            urls[1].clone(),
            urls[2].clone(),
            urls[3].clone(),
            urls[0].clone()
        ]
    );
}

#[test]
fn wave2_retention_pages_past_sixty_four_unremovable_heads() {
    // M2: 64 unremovable head tombstones (expired by TTL but still
    // referenced by live media rows) plus one expired tail tombstone with
    // no media row. One retention pass — driven here through a single
    // over-budget `ensure_cache_budget` call, which runs retention before
    // the (empty) eviction loop — must remove the tail.
    let h = wave2_harness(100);
    for i in 0..64 {
        wave2_insert_tombstone(&h, "head", i, OLD_MS, true);
    }
    let (tail_url, tail_key) = wave2_insert_tombstone(&h, "tail", 0, OLD_MS + 1, false);
    assert_eq!(wave2_stats(&h).total_bytes, 0);

    ensure_cache_budget(&h.services, 200, MediaClass::Full).unwrap();

    assert!(
        h.store
            .remote_cache_lookup(&tail_url, "original")
            .unwrap()
            .is_none(),
        "expired tail tombstone must be removed in one pass"
    );
    assert!(h.cache.byte_len(&tail_key).unwrap().is_none());
    // All 64 heads survive (still tombstones, still anchored).
    let heads = h.store.remote_cache_tombstone_page(0, 100).unwrap();
    assert_eq!(heads.len(), 64);
    assert!(heads.iter().all(|r| r.bytes_absent));
    // Retention never debits accounting and never evicts.
    assert_eq!(wave2_stats(&h).total_bytes, 0);
    assert_eq!(wave2_stats(&h).evictions, 0);
}

#[test]
fn wave2_commit_read_only_outcomes_and_durable_skip() {
    // L2: read-only outcomes come from the read-phase snapshot; a missing
    // key and a zero-size commit mutate nothing, and a live durable
    // reference fails closed with `SkippedDurableRef` leaving bytes,
    // rows, and counters exactly as found.
    let h = wave2_harness(1 << 30);
    let before = wave2_stats(&h);

    // Missing key, zero size: AlreadyAbsent, no write side effects.
    match h.store.commit_cache_eviction(
        "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        0,
        MediaClass::Preview,
        &[],
    ) {
        Ok(CacheEvictionOutcome::AlreadyAbsent) => {}
        other => panic!("expected AlreadyAbsent, got {other:?}"),
    }
    assert_eq!(wave2_stats(&h), before);

    // Live charged row, zero size: AlreadyAbsent, row still charged.
    let url = "https://cdn.example/wave2-live-row.png".to_string();
    let bytes = wave2_bytes(777);
    let key = ingest::sha256_hex(&bytes);
    wave2_insert_charged(&h, &url, &bytes, BASE_MS);
    wave2_charge(&h, bytes.len() as u64);
    let mid = wave2_stats(&h);
    match h
        .store
        .commit_cache_eviction(&key, 0, MediaClass::Preview, &[])
    {
        Ok(CacheEvictionOutcome::AlreadyAbsent) => {}
        other => panic!("expected AlreadyAbsent, got {other:?}"),
    }
    assert_eq!(wave2_stats(&h), mid);
    let rec = h
        .store
        .remote_cache_lookup(&url, "original")
        .unwrap()
        .unwrap();
    assert!(!rec.bytes_absent && rec.size_bytes == bytes.len() as u64);

    // Durable sharer with the same blob key: eviction fails closed.
    let durable_meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: Some("user-durable-1".to_string()),
            name: "durable.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: bytes.len() as u64,
            hash: ingest::md5_hex(b"wave2-durable-other"),
            blob_key: key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: "https://pinetree.local/files/durable".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: None,
            max_file_size: 10_000_000,
        })
        .unwrap();
    let index_keys = vec![(url.clone(), "original".to_string())];
    match h
        .store
        .commit_cache_eviction(&key, bytes.len() as u64, MediaClass::Preview, &index_keys)
    {
        Ok(CacheEvictionOutcome::SkippedDurableRef) => {}
        other => panic!("expected SkippedDurableRef, got {other:?}"),
    }
    assert_eq!(wave2_stats(&h), mid);
    assert_eq!(h.cache.get(&key).unwrap().unwrap(), bytes);
    let rec = h
        .store
        .remote_cache_lookup(&url, "original")
        .unwrap()
        .unwrap();
    assert!(!rec.bytes_absent && rec.size_bytes == bytes.len() as u64);
    assert!(!h.store.get(&durable_meta.id).unwrap().unwrap().evicted);
}
