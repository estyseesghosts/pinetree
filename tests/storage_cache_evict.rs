//! Cache eviction (`docs/database.md` agent C): the global cap is enforced
//! by evicting bytes only (LRU + TTL, stale first) — metadata rows and the
//! `(source_url, variant)` index stay so bytes re-fetch while the source
//! lives. The avatar reserve is preferred over full originals under
//! pressure, and eviction runs as a `JobKind::CacheEvict` job.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, CacheEvictPayload, CreateAccountInput,
    InsertMediaInput, JobKind, JobQueue, MediaClass, MediaStore, NewJob, NoopStreamingSink,
    RemoteCacheRecord,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
const DAY_MS: i64 = 86_400_000;
static EVICT_SEQ: AtomicU64 = AtomicU64::new(0);

struct EvictHarness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    cache: Arc<FsBlobStore>,
    worker: JobWorker,
}

fn evict_harness() -> EvictHarness {
    let n = EVICT_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-cacheevict-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<ManualClock> = Arc::new(ManualClock::new(BASE_MS));
    let clock_port: Arc<dyn Clock> = clock.clone();
    let store =
        RedbStore::open_with_default_ids(dir.join("store.redb"), clock_port.clone()).unwrap();
    store.set_cache_cap(100).unwrap();
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
        // Tiny global cap so eviction triggers with byte-sized fixtures.
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 100,
        remote_cache_avatar_bytes: 30,
        remote_cache_preview_bytes: 40,
        remote_cache_full_bytes: 30,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 7,
        remote_cache_warming_enabled: false,
    });
    let worker = JobWorker {
        clock: clock_port.clone(),
        ids: store.ids().clone(),
        jobs: store.clone(),
        notifications: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 30_000,
        base_backoff_ms: 1_000,
        transcode_config: TranscodeConfig::default(),
        email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
    };
    EvictHarness {
        store,
        services,
        cache,
        worker,
    }
}

/// Insert one remote-cache row exactly the way the production fetch path
/// does: bytes in the cache pool, metadata row, `(source_url, variant)`
/// index entry, and accounting deltas.
fn insert_cache_row(
    h: &EvictHarness,
    source_url: &str,
    class: MediaClass,
    size: u64,
    fetched_at_ms: i64,
    last_access_ms: i64,
    mime: &str,
) -> pinetree::domain::media::MediaMeta {
    let bytes = vec![0xABu8; size as usize];
    let key = sha256_hex(&bytes);
    h.cache.put(&key, &bytes).unwrap();
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: mime.to_string(),
            size_bytes: size,
            hash: md5_hex(&bytes),
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
            size_bytes: size,
            class,
            fetched_at_ms,
            last_access_ms,
            access_count: 1,
        })
        .unwrap();
    h.store
        .cache_accounting_apply(size as i64, class, size as i64, 0, 1, size, 0, 0)
        .unwrap();
    meta
}

#[test]
fn stale_full_evicts_first_and_metadata_survives() {
    // 30 (avatar, fresh) + 40 + 40 (full, TTL-stale) = 110 > cap 100.
    // Evicting for 10 more Full bytes must drop one stale full row's bytes
    // while its metadata + index rows stay for re-fetch.
    let h = evict_harness();
    let avatar = insert_cache_row(
        &h,
        "https://cdn.example/a.png",
        MediaClass::Avatar,
        30,
        BASE_MS,
        BASE_MS,
        "image/png",
    );
    let stale_old = insert_cache_row(
        &h,
        "https://cdn.example/old.jpg",
        MediaClass::Full,
        40,
        BASE_MS - 8 * DAY_MS,
        BASE_MS - 8 * DAY_MS,
        "image/jpeg",
    );
    let stale_mid = insert_cache_row(
        &h,
        "https://cdn.example/mid.jpg",
        MediaClass::Full,
        40,
        BASE_MS - 8 * DAY_MS,
        BASE_MS - 7 * DAY_MS,
        "image/jpeg",
    );
    pinetree::media::remote_cache::ensure_cache_budget(&h.services, 10, MediaClass::Full).unwrap();
    let stats = h.store.cache_stats().unwrap();
    assert!(stats.total_bytes + 10 <= 100, "{stats:?}");
    // At least one stale full's bytes are gone; the avatar is untouched.
    let old_gone = h.cache.get(&stale_old.blob_key).unwrap().is_none();
    let mid_gone = h.cache.get(&stale_mid.blob_key).unwrap().is_none();
    assert!(old_gone || mid_gone, "a stale full must evict");
    assert!(h.cache.get(&avatar.blob_key).unwrap().is_some());
    // Metadata + index survive for every row (re-fetch bookkeeping).
    for meta in [&avatar, &stale_old, &stale_mid] {
        assert!(h.store.get(&meta.id).unwrap().is_some());
    }
    for url in [
        "https://cdn.example/a.png",
        "https://cdn.example/old.jpg",
        "https://cdn.example/mid.jpg",
    ] {
        assert!(h
            .store
            .remote_cache_lookup(url, "original")
            .unwrap()
            .is_some());
    }
    assert!(stats.evictions >= 1);
}

#[test]
fn cache_evict_job_flows_through_worker() {
    let h = evict_harness();
    let payload = CacheEvictPayload {
        need_bytes: 10,
        class: MediaClass::Full,
    };
    h.store
        .enqueue_jobs(vec![
            NewJob::new_json(JobKind::CacheEvict, &payload, "evict-job-1", BASE_MS).unwrap(),
        ])
        .unwrap();
    assert!(h.worker.process_one().unwrap());
    // Job completed (empty cache: nothing to evict, still Done).
    assert!(h.store.claim_next(30_000).unwrap().is_none());
}

#[test]
fn storage_job_payloads_stay_under_the_64kib_cap() {
    let big_url = format!("https://cdn.example/{}", "x".repeat(70_000));
    let payload = CacheEvictPayload {
        need_bytes: 10,
        class: MediaClass::Full,
    };
    assert!(NewJob::new_json(JobKind::CacheEvict, &payload, "k", BASE_MS).is_ok());
    // A payload above the cap is rejected at construction, never enqueued.
    let oversized = serde_json::json!({ "need_bytes": 1, "padding": big_url });
    assert!(NewJob::new_json(JobKind::CacheEvict, &oversized, "k", BASE_MS).is_err());
}
