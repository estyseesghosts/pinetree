//! Per-class cache accounting (adversarial-2 slice 5: finding 6). One
//! rule everywhere: `AudioVideo` has no class counter and is governed by
//! `total_bytes` alone (apply, evict, budget checks, admin stats), and
//! eviction debits the authoritative per-key accounting class recorded at
//! first charge — never the class of the largest alias or whichever row
//! iteration returned first.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::ActivityDedup;
use pinetree::application::ports::BlobStore;
use pinetree::application::ports::CacheEvictionOutcome;
use pinetree::application::ports::CacheStats;
use pinetree::application::ports::InsertMediaInput;
use pinetree::application::ports::MediaClass;
use pinetree::application::ports::MediaStore;
use pinetree::application::ports::NoopStreamingSink;
use pinetree::application::ports::RemoteCacheRecord;
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::Clock;
use pinetree::domain::id::ManualClock;
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::SafeHttpClient;
use pinetree::federation::delivery::SafeHttpConfig;
use pinetree::media::ingest;
use pinetree::media::remote_cache::ensure_cache_budget;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

struct Harness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    cache: Arc<FsBlobStore>,
}

fn harness(remote_cache_max_bytes: u64, full_cap_bytes: u64) -> Harness {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-clsacct-{}-{n}", std::process::id()));
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
        remote_cache_full_bytes: full_cap_bytes,
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

/// Distinct bytes per seed so every fixture owns its blob key. `video`
/// selects an `ftyp` head (video/mp4 sniff); otherwise a PNG head.
fn row_bytes(len: usize, seed: u8, video: bool) -> Vec<u8> {
    let mut bytes = if video {
        let mut head = vec![0x00, 0x00, 0x00, 0x20];
        head.extend_from_slice(b"ftypisom");
        head
    } else {
        vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]
    };
    bytes.resize(len, seed);
    bytes[8] = seed;
    bytes
}

/// Insert one cache row the production way (pool bytes + metadata +
/// index). The caller charges separately so shared-key fixtures charge
/// exactly once. Returns the blob key.
fn insert_cache_row(
    h: &Harness,
    source_url: &str,
    class: MediaClass,
    mime: &str,
    bytes: &[u8],
) -> String {
    let key = ingest::sha256_hex(bytes);
    h.cache.put(&key, bytes).unwrap();
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: mime.to_string(),
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
            blob_key: key.clone(),
            size_bytes: bytes.len() as u64,
            class,
            fetched_at_ms: BASE_MS,
            last_access_ms: BASE_MS,
            access_count: 1,
            bytes_absent: false,
            generation: 0,
        })
        .unwrap();
    key
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
fn audiovideo_charge_ignores_full_counter() {
    // The one rule: an AudioVideo charge moves `total_bytes` only.
    // Fails on the old code, which folded AudioVideo into `full_bytes`
    // in both apply and evict.
    let h = harness(1 << 20, 1 << 20);
    h.store
        .cache_accounting_apply(100, MediaClass::AudioVideo, 100, 0, 1, 100, 0, 0)
        .unwrap();
    let s = stats(&h);
    assert_eq!(s.total_bytes, 100);
    assert_eq!(s.avatar_bytes, 0);
    assert_eq!(s.preview_bytes, 0);
    assert_eq!(s.full_bytes, 0, "AudioVideo must not count as Full");
    // Full still counts.
    charge(&h, MediaClass::Full, 50);
    let s = stats(&h);
    assert_eq!(s.total_bytes, 150);
    assert_eq!(s.full_bytes, 50);
    // Evicting an AudioVideo key debits the total only.
    let bytes = row_bytes(100, 0x11, true);
    let key = insert_cache_row(
        &h,
        "https://cdn.example/clip.mp4",
        MediaClass::AudioVideo,
        "video/mp4",
        &bytes,
    );
    let outcome = h
        .store
        .commit_cache_eviction(
            &key,
            100,
            MediaClass::AudioVideo,
            &[(
                "https://cdn.example/clip.mp4".to_string(),
                "original".to_string(),
            )],
        )
        .unwrap();
    assert_eq!(
        outcome,
        CacheEvictionOutcome::Evicted { debited_bytes: 100 }
    );
    let s = stats(&h);
    assert_eq!(s.total_bytes, 50);
    assert_eq!(s.full_bytes, 50, "AudioVideo evict must not touch Full");
    assert_eq!(s.evictions, 1);
}

#[test]
fn audiovideo_bytes_do_not_consume_full_budget() {
    // End to end through the budget predicate: 60 AudioVideo bytes under a
    // 100 global cap with a 10 Full cap leave a 5-byte Full reservation
    // fitting (class pressure reads 0 for AudioVideo). The old code saw
    // `full_bytes == 60` and evicted. The same 60 bytes still pressure the
    // global cap: a 50-byte Full reservation evicts them and still leaves
    // `full_bytes` at 0.
    let h = harness(100, 10);
    let bytes = row_bytes(60, 0x22, true);
    insert_cache_row(
        &h,
        "https://cdn.example/budget.mp4",
        MediaClass::AudioVideo,
        "video/mp4",
        &bytes,
    );
    charge(&h, MediaClass::AudioVideo, 60);
    assert_eq!(stats(&h).full_bytes, 0);
    ensure_cache_budget(&h.services, 5, MediaClass::Full).unwrap();
    assert_eq!(stats(&h).evictions, 0, "Full budget must ignore AV bytes");
    assert_eq!(stats(&h).total_bytes, 60);
    ensure_cache_budget(&h.services, 50, MediaClass::Full).unwrap();
    let s = stats(&h);
    assert_eq!(s.total_bytes, 0, "global pressure still evicts AV bytes");
    assert_eq!(s.full_bytes, 0);
    assert_eq!(s.evictions, 1);
    let rec = index_row(&h, "https://cdn.example/budget.mp4");
    assert!(rec.bytes_absent && rec.size_bytes == 0);
}

#[test]
fn shared_blob_debits_first_charge_class() {
    // One 40-byte blob shared by a Preview alias (charged first) and a
    // Full alias (never charged). Eviction — even when asked with the
    // other alias's class, the old max-alias guess — debits Preview.
    let h = harness(1 << 20, 1 << 20);
    let bytes = row_bytes(40, 0x33, false);
    let key = insert_cache_row(
        &h,
        "https://cdn.example/first.png",
        MediaClass::Preview,
        "image/png",
        &bytes,
    );
    charge(&h, MediaClass::Preview, 40);
    let key2 = insert_cache_row(
        &h,
        "https://cdn.example/second.bin",
        MediaClass::Full,
        "application/octet-stream",
        &bytes,
    );
    assert_eq!(key2, key, "aliases must share one physical key");
    let s = stats(&h);
    assert_eq!((s.total_bytes, s.preview_bytes, s.full_bytes), (40, 40, 0));
    let outcome = h
        .store
        .commit_cache_eviction(
            &key,
            40,
            MediaClass::Full,
            &[
                (
                    "https://cdn.example/first.png".to_string(),
                    "original".to_string(),
                ),
                (
                    "https://cdn.example/second.bin".to_string(),
                    "original".to_string(),
                ),
            ],
        )
        .unwrap();
    assert_eq!(outcome, CacheEvictionOutcome::Evicted { debited_bytes: 40 });
    let s = stats(&h);
    assert_eq!(s.total_bytes, 0);
    assert_eq!(s.preview_bytes, 0, "must debit the first-charge class");
    assert_eq!(s.full_bytes, 0);
    for url in [
        "https://cdn.example/first.png",
        "https://cdn.example/second.bin",
    ] {
        let rec = index_row(&h, url);
        assert!(rec.bytes_absent && rec.size_bytes == 0, "{url}");
    }
}

#[test]
fn shared_blob_first_charge_full_debited() {
    // Mirror image: Full charged first, Preview alias second. Eviction
    // asked with Preview still debits Full.
    let h = harness(1 << 20, 1 << 20);
    let bytes = row_bytes(40, 0x44, false);
    let key = insert_cache_row(
        &h,
        "https://cdn.example/alpha.bin",
        MediaClass::Full,
        "application/octet-stream",
        &bytes,
    );
    charge(&h, MediaClass::Full, 40);
    let key2 = insert_cache_row(
        &h,
        "https://cdn.example/beta.png",
        MediaClass::Preview,
        "image/png",
        &bytes,
    );
    assert_eq!(key2, key, "aliases must share one physical key");
    let outcome = h
        .store
        .commit_cache_eviction(
            &key,
            40,
            MediaClass::Preview,
            &[
                (
                    "https://cdn.example/alpha.bin".to_string(),
                    "original".to_string(),
                ),
                (
                    "https://cdn.example/beta.png".to_string(),
                    "original".to_string(),
                ),
            ],
        )
        .unwrap();
    assert_eq!(outcome, CacheEvictionOutcome::Evicted { debited_bytes: 40 });
    let s = stats(&h);
    assert_eq!(s.total_bytes, 0);
    assert_eq!(s.full_bytes, 0, "must debit the first-charge class");
    assert_eq!(s.preview_bytes, 0);
}

#[test]
fn restore_charge_replaces_accounting_class() {
    // After eviction zeroes every alias, the next (restore) charge records
    // its own class: pre-migration keys likewise land on the class
    // recorded at next charge, never on a guess.
    let h = harness(1 << 20, 1 << 20);
    let bytes = row_bytes(40, 0x55, false);
    let url_a = "https://cdn.example/restore-a.png";
    let url_b = "https://cdn.example/restore-b.bin";
    let key = insert_cache_row(&h, url_a, MediaClass::Preview, "image/png", &bytes);
    charge(&h, MediaClass::Preview, 40);
    insert_cache_row(&h, url_b, MediaClass::Preview, "image/png", &bytes);
    h.store
        .commit_cache_eviction(
            &key,
            40,
            MediaClass::Preview,
            &[
                (url_a.to_string(), "original".to_string()),
                (url_b.to_string(), "original".to_string()),
            ],
        )
        .unwrap();
    assert_eq!(stats(&h).preview_bytes, 0);
    // Restore refetches the same bytes as Full on the first alias only.
    h.store
        .remote_cache_insert(&RemoteCacheRecord {
            source_url: url_a.to_string(),
            variant: "original".to_string(),
            media_id: index_row(&h, url_a).media_id.clone(),
            blob_key: key.clone(),
            size_bytes: bytes.len() as u64,
            class: MediaClass::Full,
            fetched_at_ms: BASE_MS,
            last_access_ms: BASE_MS,
            access_count: 2,
            bytes_absent: false,
            generation: 0,
        })
        .unwrap();
    charge(&h, MediaClass::Full, 40);
    let s = stats(&h);
    assert_eq!((s.total_bytes, s.preview_bytes, s.full_bytes), (40, 0, 40));
    // Eviction asked with the stale Preview class still debits Full.
    let outcome = h
        .store
        .commit_cache_eviction(
            &key,
            40,
            MediaClass::Preview,
            &[
                (url_a.to_string(), "original".to_string()),
                (url_b.to_string(), "original".to_string()),
            ],
        )
        .unwrap();
    assert_eq!(outcome, CacheEvictionOutcome::Evicted { debited_bytes: 40 });
    let s = stats(&h);
    assert_eq!(s.total_bytes, 0);
    assert_eq!(s.full_bytes, 0, "restore charge must own the debit");
    assert_eq!(s.preview_bytes, 0);
}
