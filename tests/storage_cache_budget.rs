//! Remote-cache storage budget: the global cap (default 512 MiB) is never
//! exceeded — `reserve_cache_bytes` admits or refuses atomically — and
//! aggressive cache eviction never touches durable bytes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, CreateAccountInput, InsertMediaInput, MediaStore,
    NoopStreamingSink,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
const MIB: u64 = 1024 * 1024;
static CACHE_SEQ: AtomicU64 = AtomicU64::new(0);

struct CacheHarness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    durable: Arc<FsBlobStore>,
    cache: Arc<FsBlobStore>,
}

fn cache_harness() -> (CacheHarness, String) {
    let n = CACHE_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-cachebudget-{}-{n}", std::process::id()));
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
    let owner = store
        .create_account(CreateAccountInput::local(format!("cache_user_{n}")), None)
        .expect("create owner")
        .id;
    (
        CacheHarness {
            store,
            services,
            durable,
            cache,
        },
        owner,
    )
}

#[test]
fn cache_concurrent_reservations_never_exceed_cap() {
    // 16 threads race to reserve 64 MiB each against a 512 MiB global cap.
    // The store serializes the check-and-set in one write transaction, so
    // exactly 8 reservations succeed and the in-flight total never exceeds
    // the cap however the threads interleave.
    let (h, _owner) = cache_harness();
    h.store.set_cache_cap(512 * MIB).expect("set cap");
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let handles: Vec<_> = (0..16)
        .map(|i| {
            let store = h.store.clone();
            let cache = h.cache.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let admitted = store.reserve_cache_bytes(64 * MIB).expect("reserve");
                if admitted {
                    let key = format!("cachemarker{:04}", i);
                    cache
                        .put(key.as_str(), format!("marker-{i}").as_bytes())
                        .unwrap();
                    let stats = store.cache_stats().expect("stats");
                    assert!(
                        stats.total_bytes + stats.in_flight_bytes <= 512 * MIB,
                        "cap exceeded after reserve by thread {i}"
                    );
                    cache.delete(&key).unwrap();
                }
                admitted
            })
        })
        .collect();
    let mut admitted = 0u32;
    for handle in handles {
        if handle.join().expect("worker panicked") {
            admitted += 1;
        }
    }
    assert_eq!(admitted, 8, "exactly 8 x 64MiB reservations fit in 512MiB");
    let stats = h.store.cache_stats().expect("stats");
    assert!(
        stats.total_bytes + stats.in_flight_bytes <= 512 * MIB,
        "in-flight total never exceeds the cap"
    );
    assert_eq!(stats.in_flight_bytes, 512 * MIB, "8 admitted x 64MiB held");
    for _ in 0..admitted {
        h.store
            .release_cache_reservation(64 * MIB)
            .expect("release");
    }
    assert_eq!(
        h.store.cache_stats().expect("stats").in_flight_bytes,
        0,
        "all reservations released"
    );
}

#[test]
fn cache_remote_fetch_lifecycle_never_exceeds_cap() {
    // reserve → import (put) → serve (get) → release → evict, repeated.
    let (h, _owner) = cache_harness();
    h.store.set_cache_cap(128 * MIB).expect("set cap");
    for i in 0..3u32 {
        let key = format!("remotefetch{:04}", i);
        assert!(
            h.store.reserve_cache_bytes(64 * MIB).expect("reserve"),
            "fetch {i} admitted"
        );
        let bytes = vec![0x89, b'P', b'N', b'G', i as u8, 0x01, 0x02, 0x03];
        h.cache.put(&key, &bytes).unwrap();
        let stats = h.store.cache_stats().expect("stats");
        assert!(
            stats.total_bytes + stats.in_flight_bytes <= 128 * MIB,
            "fetch {i} keeps the total within cap"
        );
        assert_eq!(
            h.cache.get(&key).unwrap().as_deref(),
            Some(bytes.as_slice()),
            "imported bytes serve back"
        );
        h.store
            .release_cache_reservation(64 * MIB)
            .expect("release");
        h.cache.delete(&key).unwrap();
        assert_eq!(h.cache.get(&key).unwrap(), None);
    }
    assert_eq!(h.store.cache_stats().expect("stats").in_flight_bytes, 0);
    // An over-cap single reservation is refused and changes nothing.
    assert!(
        !h.store.reserve_cache_bytes(256 * MIB).expect("reserve"),
        "256MiB refused against a 128MiB cap"
    );
    assert_eq!(h.store.cache_stats().expect("stats").in_flight_bytes, 0);
}

#[test]
fn cache_aggressive_eviction_preserves_durable_uploads() {
    let (h, owner) = cache_harness();
    // Durable local upload: bytes land in the durable pool, never the cache.
    let durable_bytes = vec![0x89, b'P', b'N', b'G', 0xAA, 0xBB, 0xCC, 0xDD];
    let meta = h
        .services
        .upload_media(
            &owner,
            "keep.png".to_string(),
            &durable_bytes,
            None,
            false,
            false,
        )
        .expect("durable upload");
    assert_eq!(
        h.durable.get(&meta.blob_key).unwrap().as_deref(),
        Some(durable_bytes.as_slice())
    );

    // Fill the remote cache pool, then evict ALL of it aggressively.
    let mut cache_keys = Vec::new();
    for i in 0..8u32 {
        let key = format!("evictme{:04}", i);
        h.cache
            .put(&key, format!("cache-payload-{i}").as_bytes())
            .unwrap();
        cache_keys.push(key);
    }
    for key in &cache_keys {
        h.cache.delete(key).unwrap();
    }
    for key in &cache_keys {
        assert_eq!(h.cache.get(key).unwrap(), None, "cache fully evicted");
    }

    // Durable bytes and metadata are untouched by the cache eviction.
    assert_eq!(
        h.durable.get(&meta.blob_key).unwrap().as_deref(),
        Some(durable_bytes.as_slice()),
        "durable bytes survive cache eviction"
    );
    let row = h.store.get(&meta.id).unwrap().expect("metadata present");
    assert_eq!(row.owner_id.as_deref(), Some(owner.as_str()));
    assert_eq!(row.size_bytes, durable_bytes.len() as u64);
    assert!(!row.is_link, "durable row is not a cache row");
    // A second durable upload with identical bytes dedups to the same row.
    let again = h
        .services
        .upload_media(
            &owner,
            "keep.png".to_string(),
            &durable_bytes,
            None,
            false,
            false,
        )
        .expect("dedup");
    assert_eq!(again.id, meta.id);

    // Cache rows (owner None) coexist without disturbing durable accounting.
    let remote = vec![0x89, b'P', b'N', b'G', 0x11, 0x22];
    let remote_key = sha256_hex(&remote);
    h.cache.put(&remote_key, &remote).unwrap();
    h.store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.png".to_string(),
            mime: "image/png".to_string(),
            size_bytes: remote.len() as u64,
            hash: md5_hex(&remote),
            blob_key: remote_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: "https://pinetree.local/files/remote".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some("https://remote.example/pic.png".to_string()),
            max_file_size: h.services.max_file_size,
        })
        .unwrap();
    assert_eq!(
        h.store.durable_bytes(&owner).unwrap(),
        durable_bytes.len() as u64,
        "cache bytes are not charged to the owner"
    );
}
