//! Storage reconciler (`docs/database.md` agent D): the idempotent sweep
//! heals what the DB/filesystem split cannot make atomic — orphan bytes,
//! missing cache bytes (marked byte-less for re-fetch, never metadata
//! deletes), drifted accounting, pre-slice-8 MD5 rows (flagged, never
//! purged), and `.len` sidecar mismatches. Runs as
//! `JobKind::ReconcileStorage`; the post-upgrade counter rebuild runs as
//! `JobKind::BackfillDurableAccounting`.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, CreateAccountInput, InsertMediaInput, JobKind,
    JobQueue, MediaStore, NewJob, NoopStreamingSink,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::reconciler;
use pinetree::media::storage::FsBlobStore;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};

const BASE_MS: i64 = 1_760_400_000_000;
static RECON_SEQ: AtomicU64 = AtomicU64::new(0);

struct ReconHarness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    durable: Arc<FsBlobStore>,
    cache: Arc<FsBlobStore>,
    dir: std::path::PathBuf,
    worker: JobWorker,
}

fn recon_harness() -> (ReconHarness, String) {
    let n = RECON_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-reconcile-{}-{n}", std::process::id()));
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
    let owner = store
        .create_account(CreateAccountInput::local(format!("recon_user_{n}")), None)
        .expect("create owner")
        .id;
    (
        ReconHarness {
            store,
            services,
            durable,
            cache,
            dir,
            worker,
        },
        owner,
    )
}

fn insert_remote_row(
    h: &ReconHarness,
    source_url: &str,
    bytes: &[u8],
    with_bytes: bool,
) -> pinetree::domain::media::MediaMeta {
    use pinetree::application::ports::{MediaClass, RemoteCacheRecord};
    let key = sha256_hex(bytes);
    if with_bytes {
        h.cache.put(&key, bytes).unwrap();
    }
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
fn orphan_sweep_removes_only_unlisted_keys() {
    // Crash between `blobs.put` and the metadata commit leaves bytes no row
    // references. The sweep deletes exactly those, keeping live keys.
    let (h, _owner) = recon_harness();
    let live_bytes = b"live-payload";
    let live_key = sha256_hex(live_bytes);
    h.cache.put(&live_key, live_bytes).unwrap();
    let orphan_bytes = b"orphan-payload";
    let orphan_key = sha256_hex(orphan_bytes);
    h.cache.put(&orphan_key, orphan_bytes).unwrap();
    let mut live = HashSet::new();
    live.insert(live_key.clone());
    let removed =
        reconciler::sweep_orphan_blobs(&h.dir.join("blobs_cache").join("objects"), &live).unwrap();
    assert_eq!(removed, 1);
    assert_eq!(h.cache.get(&live_key).unwrap().unwrap(), live_bytes);
    assert!(h.cache.get(&orphan_key).unwrap().is_none());
}

#[test]
fn missing_cache_bytes_marked_and_legacy_rows_flagged_not_purged() {
    let (h, _owner) = recon_harness();
    // Torn cache row: metadata + accounting exist, bytes are gone.
    let torn = insert_remote_row(&h, "https://cdn.example/torn.png", b"torn-bytes", true);
    h.cache.delete(&torn.blob_key).unwrap();
    // Pre-slice-8 legacy row: MD5-length key, never purged.
    let legacy_key = "d41d8cd98f00b204e9800998ecf8427e".to_string();
    let legacy = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "legacy.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: 5,
            hash: legacy_key.clone(),
            blob_key: legacy_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: "https://pinetree.local/files/legacy".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some("https://cdn.example/legacy.png".to_string()),
            max_file_size: 10_000_000,
        })
        .unwrap();
    let report = reconciler::reconcile_media(&h.services).unwrap();
    assert_eq!(report.missing_marked_evicted, 1, "{report:?}");
    assert_eq!(report.md5_rows_purged_or_flagged, 1, "{report:?}");
    // Neither metadata row is deleted; the torn index row stays for re-fetch.
    assert!(h.store.get(&torn.id).unwrap().is_some());
    assert!(h.store.get(&legacy.id).unwrap().is_some());
    assert!(
        h.store
            .remote_cache_lookup("https://cdn.example/torn.png", "original")
            .unwrap()
            .is_some()
    );
    // Re-running converges (the torn row stays byte-less, counted again but
    // never deleted).
    let again = reconciler::reconcile_media(&h.services).unwrap();
    assert!(h.store.get(&torn.id).unwrap().is_some());
    assert!(h.store.get(&legacy.id).unwrap().is_some());
    let _ = again;
}

#[test]
fn backfill_restores_zeroed_counters() {
    // Simulate a pre-accounting upgrade: live rows but zeroed counters.
    let (h, owner) = recon_harness();
    let a = h
        .services
        .upload_media(&owner, "a.bin".to_string(), b"aaa", None, false, false)
        .unwrap();
    let b = h
        .services
        .upload_media(&owner, "b.bin".to_string(), b"bb", None, false, false)
        .unwrap();
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), 5);
    h.store.set_durable_bytes(&owner, 0).unwrap();
    h.store.set_blob_refcount(&a.blob_key, 0).unwrap();
    let (report, done) = reconciler::backfill_durable_accounting(&h.services, None, 500).unwrap();
    assert!(done.is_none());
    assert_eq!(report.media_scanned, 2, "{report:?}");
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), 5);
    assert_eq!(h.store.blob_refcount(&a.blob_key).unwrap(), 1);
    assert_eq!(h.store.blob_refcount(&b.blob_key).unwrap(), 1);
    // Idempotent: a second run writes the same sums.
    let (report2, _) = reconciler::backfill_durable_accounting(&h.services, None, 500).unwrap();
    assert_eq!(report2.media_scanned, 2);
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), 5);
}

#[test]
fn reconcile_and_backfill_jobs_flow_through_worker() {
    use pinetree::application::ports::{BackfillDurablePayload, ReconcileStoragePayload};
    let (h, _owner) = recon_harness();
    h.store
        .enqueue_jobs(vec![
            NewJob::new_json(
                JobKind::ReconcileStorage,
                &ReconcileStoragePayload { scan_cap: None },
                "recon-job-1",
                BASE_MS,
            )
            .unwrap(),
            NewJob::new_json(
                JobKind::BackfillDurableAccounting,
                &BackfillDurablePayload {
                    after: None,
                    batch_size: None,
                },
                "backfill-job-1",
                BASE_MS,
            )
            .unwrap(),
        ])
        .unwrap();
    assert!(h.worker.process_one().unwrap());
    assert!(h.worker.process_one().unwrap());
    assert!(h.store.claim_next(30_000).unwrap().is_none());
    let _ = &h.durable;
}
