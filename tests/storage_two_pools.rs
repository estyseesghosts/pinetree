//! Two-pool storage layout (`docs/database.md` agent B): durable local
//! uploads live in `blobs_durable/` (never evicted), remote fetches in
//! `blobs_cache/` (capped, LRU+TTL). A pre-overhaul `blobs/` root migrates
//! by rename and stays readable under the durable pool.

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
static POOL_SEQ: AtomicU64 = AtomicU64::new(0);

struct PoolHarness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    durable: Arc<FsBlobStore>,
    cache: Arc<FsBlobStore>,
    dir: std::path::PathBuf,
}

fn pool_harness() -> (PoolHarness, String) {
    let n = POOL_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-twopools-{}-{n}", std::process::id()));
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
        .create_account(CreateAccountInput::local(format!("pool_user_{n}")), None)
        .expect("create owner")
        .id;
    (
        PoolHarness {
            store,
            services,
            durable,
            cache,
            dir,
        },
        owner,
    )
}

#[test]
fn durable_and_cache_roots_are_isolated() {
    let (h, _owner) = pool_harness();
    let bytes_a = b"durable-payload";
    let bytes_b = b"cache-payload";
    let key_a = sha256_hex(bytes_a);
    let key_b = sha256_hex(bytes_b);
    h.durable.put(&key_a, bytes_a).unwrap();
    h.cache.put(&key_b, bytes_b).unwrap();
    // Each pool serves its own keys and nothing else.
    assert!(h.durable.exists(&key_a).unwrap());
    assert!(!h.durable.exists(&key_b).unwrap());
    assert!(h.cache.exists(&key_b).unwrap());
    assert!(!h.cache.exists(&key_a).unwrap());
    assert_eq!(h.durable.get(&key_a).unwrap().unwrap(), bytes_a);
    assert_eq!(h.cache.get(&key_b).unwrap().unwrap(), bytes_b);
}

#[test]
fn legacy_blobs_dir_migrates_by_rename_and_stays_readable() {
    // Pre-overhaul layout: a single `blobs/` root. The boot migration is a
    // plain rename to `blobs_durable/` (see `main.rs`); the content-addressed
    // layout is unchanged, so bytes stay readable under the durable pool.
    let (h, _owner) = pool_harness();
    let legacy_root = h.dir.join("blobs");
    let legacy_store = FsBlobStore::new(&legacy_root).unwrap();
    let bytes = b"legacy-payload";
    let key = sha256_hex(bytes);
    legacy_store.put(&key, bytes).unwrap();
    drop(legacy_store);
    let durable_root = h.dir.join("migrated_durable");
    std::fs::rename(&legacy_root, &durable_root).unwrap();
    let migrated = FsBlobStore::new(&durable_root).unwrap();
    assert_eq!(migrated.get(&key).unwrap().unwrap(), bytes);
    assert!(!legacy_root.exists());
}

#[test]
fn identical_bytes_cache_evict_spares_durable() {
    // Byte-identical content in both pools: dropping the cache copy (what
    // eviction does) must leave the durable upload byte-identical.
    let (h, owner) = pool_harness();
    let bytes = b"shared-payload";
    let meta = h
        .services
        .upload_media(&owner, "shared.bin".to_string(), bytes, None, false, false)
        .unwrap();
    h.cache.put(&meta.blob_key, bytes).unwrap();
    assert_eq!(h.durable.get(&meta.blob_key).unwrap().unwrap(), bytes);
    h.cache.delete(&meta.blob_key).unwrap();
    assert!(h.cache.get(&meta.blob_key).unwrap().is_none());
    assert_eq!(h.durable.get(&meta.blob_key).unwrap().unwrap(), bytes);
    // The durable row still references the key (eviction is bytes-only).
    assert!(h.store.blob_refcount(&meta.blob_key).unwrap() >= 1);
    let _ = md5_hex(bytes);
}
