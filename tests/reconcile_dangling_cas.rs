//! Dangling-index reconcile CAS (adversarial-2 slice 4: finding 4 plus
//! finding E).
//!
//! The reconciler must not select dangling candidates from the
//! LRU-oldest-N window (newer dangling rows would starve behind old live
//! rows and tombstones), and it must delete an index row only while the
//! exact `(source_url, variant)` row still points at the candidate
//! `media_id` / `blob_key` — a concurrent refetch that installs `K_new`
//! on the same URL/variant must survive with its bytes and its charge
//! intact. Accounting debits the size still charged on the row actually
//! removed, never the stale candidate's size, and never a row that was
//! already `bytes_absent` / zero-size.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, CacheEvictionOutcome, CacheStats, InsertMediaInput, MediaClass,
    MediaStore, NoopStreamingSink, PortError, RemoteCacheRecord, TimelineCursor,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::{MediaMeta, MediaProperties};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest;
use pinetree::media::reconcile::reconcile_media;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
const OLD_MS: i64 = BASE_MS - 30 * 86_400_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

struct Harness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    cache: Arc<FsBlobStore>,
}

fn open_backend() -> (
    Arc<RedbStore>,
    Arc<FsBlobStore>,
    Arc<FsBlobStore>,
    Arc<dyn Clock>,
    std::path::PathBuf,
) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-dangcas-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(BASE_MS));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    (store, cache, durable, clock, dir)
}

fn services_with(
    store: &Arc<RedbStore>,
    cache: &Arc<FsBlobStore>,
    durable: &Arc<FsBlobStore>,
    clock: &Arc<dyn Clock>,
    media: Arc<dyn MediaStore>,
) -> Arc<AppServices> {
    Arc::new(AppServices {
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
        media,
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
    })
}

fn harness() -> Harness {
    let (store, cache, durable, clock, _dir) = open_backend();
    let media: Arc<dyn MediaStore> = store.clone();
    let services = services_with(&store, &cache, &durable, &clock, media);
    Harness {
        store,
        services,
        cache,
    }
}

/// Distinct bytes per seed so every row owns its blob key.
fn row_bytes(len: usize, seed: u8) -> Vec<u8> {
    let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    bytes.resize(len.max(8), seed);
    bytes.truncate(len);
    if bytes.len() > 8 {
        bytes[8] = seed;
    }
    bytes
}

/// Insert one cache row the way the fetch path does (bytes + metadata +
/// index). The caller applies accounting separately so shared-key
/// fixtures charge exactly once.
fn insert_cache_row(
    h: &Harness,
    id: &str,
    source_url: &str,
    bytes: &[u8],
    fetched_at_ms: i64,
    last_access_ms: i64,
) -> (MediaMeta, String) {
    let key = ingest::sha256_hex(bytes);
    h.cache.put(&key, bytes).unwrap();
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: Some(id.to_string()),
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
            url: format!("https://pinetree.local/files/{id}"),
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
            class: MediaClass::Preview,
            fetched_at_ms,
            last_access_ms,
            access_count: 1,
            bytes_absent: false,
            generation: 0,
        })
        .unwrap();
    (meta, key)
}

fn charge(h: &Harness, class: MediaClass, bytes: u64) {
    h.store
        .cache_accounting_apply(bytes as i64, class, bytes as i64, 0, 1, bytes, 0, 0)
        .unwrap();
}

fn stats(h: &Harness) -> CacheStats {
    h.store.cache_stats().unwrap()
}

#[test]
fn dangling_selection_ignores_lru_order() {
    // Ten old live rows fill any oldest-N window; one NEWER dangling row
    // must still be selected. The old selector
    // (`remote_cache_evict_candidates`, oldest N) never surfaces it.
    let h = harness();
    for i in 0..10 {
        let url = format!("https://cdn.example/live-{i}.png");
        let id = format!("midlive{i:06}");
        insert_cache_row(&h, &id, &url, &row_bytes(10, i as u8), OLD_MS, OLD_MS);
        charge(&h, MediaClass::Preview, 10);
    }
    let (dangling_meta, _) = insert_cache_row(
        &h,
        "middangling0001",
        "https://cdn.example/dangling.png",
        &row_bytes(12, 200),
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 12);
    h.store.delete_metadata(&dangling_meta.id).unwrap();

    let page = h.store.remote_cache_dangling_page(0, 64).unwrap();
    assert_eq!(page.len(), 1, "only the media-less row is dangling");
    assert_eq!(page[0].media_id, dangling_meta.id);
    assert_eq!(page[0].source_url, "https://cdn.example/dangling.png");

    let lru = h.store.remote_cache_evict_candidates(8).unwrap();
    assert_eq!(lru.len(), 8);
    assert!(
        lru.iter().all(|rec| rec.media_id != dangling_meta.id),
        "oldest-8 window never surfaces the newer dangling row"
    );
}

#[test]
fn conditional_remove_rejects_stale_candidate() {
    // Direct CAS check on the production backend (Slice 2 helper, reused
    // here — never duplicated): after a refetch replaces the row, the
    // stale (`media_id`, `blob_key`) pair removes nothing.
    let h = harness();
    let url = "https://cdn.example/cas.png";
    let (old_meta, old_key) = insert_cache_row(
        &h,
        "midcasold00001",
        url,
        &row_bytes(9, 11),
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 9);

    // Concurrent refetch installs K_new on the same URL/variant.
    let new_bytes = row_bytes(7, 77);
    let new_key = ingest::sha256_hex(&new_bytes);
    h.cache.put(&new_key, &new_bytes).unwrap();
    let new_meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: Some("midcasnew00001".to_string()),
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: new_bytes.len() as u64,
            hash: ingest::md5_hex(&new_bytes),
            blob_key: new_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: "https://pinetree.local/files/midcasnew00001".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some(url.to_string()),
            max_file_size: 10_000_000,
        })
        .unwrap();
    h.store
        .remote_cache_insert(&RemoteCacheRecord {
            source_url: url.to_string(),
            variant: "original".to_string(),
            media_id: new_meta.id.clone(),
            blob_key: new_key.clone(),
            size_bytes: new_bytes.len() as u64,
            class: MediaClass::Preview,
            fetched_at_ms: BASE_MS,
            last_access_ms: BASE_MS,
            access_count: 1,
            bytes_absent: false,
            generation: 0,
        })
        .unwrap();

    assert!(
        !h.store
            .remote_cache_remove_if(url, "original", &old_meta.id, &old_key)
            .unwrap(),
        "stale pair must not remove the refetched row"
    );
    let current = h
        .store
        .remote_cache_lookup(url, "original")
        .unwrap()
        .expect("refetched row survives the stale CAS");
    assert_eq!(current.media_id, new_meta.id);
    assert_eq!(current.blob_key, new_key);
    assert!(h.cache.byte_len(&new_key).unwrap().is_some());
    assert!(h.store.get(&new_meta.id).unwrap().is_some());

    // The fresh pair still removes.
    assert!(
        h.store
            .remote_cache_remove_if(url, "original", &new_meta.id, &new_key)
            .unwrap()
    );
    assert!(
        h.store
            .remote_cache_lookup(url, "original")
            .unwrap()
            .is_none()
    );
}

/// Refetch a [`RacyMedia`] installs at the reconciler's locked re-read
/// point: the first `remote_cache_lookup` for the armed key performs a
/// full fetch-path install (bytes + media row + index row + charge) and
/// only then returns the fresh row. Deterministic stand-in for a
/// concurrent refetch landing between candidate listing and the CAS.
struct RefetchPlan {
    source_url: String,
    variant: String,
    media_id: String,
    bytes: Vec<u8>,
    class: MediaClass,
    now_ms: i64,
}

struct RacyMedia {
    inner: Arc<RedbStore>,
    cache: Arc<FsBlobStore>,
    plan: Mutex<Option<RefetchPlan>>,
    fired: AtomicU64,
}

impl RacyMedia {
    fn plan(&self) -> MutexGuard<'_, Option<RefetchPlan>> {
        self.plan.lock().unwrap()
    }

    fn maybe_refetch(&self, source_url: &str, variant: &str) {
        if self.fired.load(Ordering::SeqCst) != 0 {
            return;
        }
        let plan = self.plan().take();
        let Some(plan) = plan else {
            return;
        };
        if plan.source_url != source_url || plan.variant != variant {
            *self.plan() = Some(plan);
            return;
        }
        let key = ingest::sha256_hex(&plan.bytes);
        self.cache.put(&key, &plan.bytes).unwrap();
        let meta = self
            .inner
            .insert_metadata(InsertMediaInput {
                id: Some(plan.media_id.clone()),
                owner_id: None,
                name: "remote.bin".to_string(),
                mime: "image/png".to_string(),
                size_bytes: plan.bytes.len() as u64,
                hash: ingest::md5_hex(&plan.bytes),
                blob_key: key.clone(),
                blurhash: None,
                properties: MediaProperties::default(),
                is_sensitive: false,
                is_link: true,
                url: format!("https://pinetree.local/files/{}", plan.media_id),
                thumbnail_url: None,
                comment: None,
                source_url: Some(plan.source_url.clone()),
                max_file_size: 10_000_000,
            })
            .unwrap();
        self.inner
            .remote_cache_insert(&RemoteCacheRecord {
                source_url: plan.source_url.clone(),
                variant: plan.variant.clone(),
                media_id: meta.id,
                blob_key: key,
                size_bytes: plan.bytes.len() as u64,
                class: plan.class,
                fetched_at_ms: plan.now_ms,
                last_access_ms: plan.now_ms,
                access_count: 1,
                bytes_absent: false,
                generation: 0,
            })
            .unwrap();
        // Fetch-path charge for the new bytes (mirrors the commit path).
        let size = plan.bytes.len() as i64;
        self.inner
            .cache_accounting_apply(size, plan.class, size, 0, 1, size as u64, 0, 0)
            .unwrap();
        self.fired.fetch_add(1, Ordering::SeqCst);
    }
}

impl MediaStore for RacyMedia {
    fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError> {
        self.inner.insert_metadata(input)
    }
    fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError> {
        self.inner.get(id)
    }
    fn find_by_owner_and_hash(
        &self,
        owner_id: &str,
        hash: &str,
    ) -> Result<Option<MediaMeta>, PortError> {
        self.inner.find_by_owner_and_hash(owner_id, hash)
    }
    fn find_by_source_url(&self, source_url: &str) -> Result<Vec<MediaMeta>, PortError> {
        self.inner.find_by_source_url(source_url)
    }
    fn delete_metadata(&self, id: &str) -> Result<(), PortError> {
        self.inner.delete_metadata(id)
    }
    fn list_by_owner(&self, owner_id: &str) -> Result<Vec<MediaMeta>, PortError> {
        self.inner.list_by_owner(owner_id)
    }
    fn list_all_media(&self, cursor: &TimelineCursor) -> Result<Vec<MediaMeta>, PortError> {
        self.inner.list_all_media(cursor)
    }
    fn durable_bytes(&self, owner_id: &str) -> Result<u64, PortError> {
        self.inner.durable_bytes(owner_id)
    }
    fn count_drive_files(&self) -> Result<u64, PortError> {
        self.inner.count_drive_files()
    }
    fn blob_refcount(&self, blob_key: &str) -> Result<u64, PortError> {
        self.inner.blob_refcount(blob_key)
    }
    fn set_blob_refcount(&self, blob_key: &str, count: u64) -> Result<(), PortError> {
        self.inner.set_blob_refcount(blob_key, count)
    }
    fn set_durable_bytes(&self, owner_id: &str, bytes: u64) -> Result<(), PortError> {
        self.inner.set_durable_bytes(owner_id, bytes)
    }
    fn insert_metadata_with_durable_quota(
        &self,
        input: InsertMediaInput,
        quota_bytes: u64,
    ) -> Result<MediaMeta, PortError> {
        self.inner
            .insert_metadata_with_durable_quota(input, quota_bytes)
    }
    fn delete_media_record(&self, id: &str) -> Result<Option<(MediaMeta, u64)>, PortError> {
        self.inner.delete_media_record(id)
    }
    fn remote_cache_lookup(
        &self,
        source_url: &str,
        variant: &str,
    ) -> Result<Option<RemoteCacheRecord>, PortError> {
        self.maybe_refetch(source_url, variant);
        self.inner.remote_cache_lookup(source_url, variant)
    }
    fn remote_cache_insert(&self, rec: &RemoteCacheRecord) -> Result<(), PortError> {
        self.inner.remote_cache_insert(rec)
    }
    fn remote_cache_touch(
        &self,
        source_url: &str,
        variant: &str,
        now_ms: i64,
    ) -> Result<(), PortError> {
        self.inner.remote_cache_touch(source_url, variant, now_ms)
    }
    fn remote_cache_evict_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        self.inner.remote_cache_evict_candidates(limit)
    }
    fn remote_cache_charged_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        self.inner.remote_cache_charged_page(offset, limit)
    }
    fn remote_cache_tombstone_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        self.inner.remote_cache_tombstone_page(offset, limit)
    }
    fn remote_cache_dangling_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        self.inner.remote_cache_dangling_page(offset, limit)
    }
    fn remote_cache_remove_if(
        &self,
        source_url: &str,
        variant: &str,
        media_id: &str,
        blob_key: &str,
    ) -> Result<bool, PortError> {
        self.inner
            .remote_cache_remove_if(source_url, variant, media_id, blob_key)
    }
    fn remote_cache_remove(&self, source_url: &str, variant: &str) -> Result<(), PortError> {
        self.inner.remote_cache_remove(source_url, variant)
    }
    fn cache_stats(&self) -> Result<CacheStats, PortError> {
        self.inner.cache_stats()
    }
    fn commit_cache_eviction(
        &self,
        blob_key: &str,
        size_bytes: u64,
        class: MediaClass,
        index_keys: &[(String, String)],
    ) -> Result<CacheEvictionOutcome, PortError> {
        self.inner
            .commit_cache_eviction(blob_key, size_bytes, class, index_keys)
    }
    fn cache_entries_for_blob_key(
        &self,
        blob_key: &str,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        self.inner.cache_entries_for_blob_key(blob_key)
    }
    fn cache_accounting_apply(
        &self,
        total_delta: i64,
        class: MediaClass,
        class_delta: i64,
        hits: u64,
        misses: u64,
        bytes_fetched: u64,
        evictions: u64,
        failed_origins: u64,
    ) -> Result<(), PortError> {
        self.inner.cache_accounting_apply(
            total_delta,
            class,
            class_delta,
            hits,
            misses,
            bytes_fetched,
            evictions,
            failed_origins,
        )
    }
    fn reserve_cache_bytes(&self, bytes: u64) -> Result<bool, PortError> {
        self.inner.reserve_cache_bytes(bytes)
    }
    fn release_cache_reservation(&self, bytes: u64) -> Result<(), PortError> {
        self.inner.release_cache_reservation(bytes)
    }
    fn set_cache_cap(&self, cap_bytes: u64) -> Result<(), PortError> {
        self.inner.set_cache_cap(cap_bytes)
    }
}

#[test]
fn reconcile_skips_row_replaced_by_concurrent_refetch() {
    // The reconciler lists a stale row for K_old with a missing media id;
    // a concurrent refetch installs K_new on the same URL/variant before
    // the locked re-read. Reconcile must not delete the new row or its
    // bytes, and must not debit K_new (nor K_old, whose bytes it never
    // removed).
    let (store, cache, durable, clock, _dir) = open_backend();
    let racy = Arc::new(RacyMedia {
        inner: store.clone(),
        cache: cache.clone(),
        plan: Mutex::new(None),
        fired: AtomicU64::new(0),
    });
    let media: Arc<dyn MediaStore> = racy.clone();
    let services = services_with(&store, &cache, &durable, &clock, media);
    let h = Harness {
        store,
        services,
        cache,
    };

    let url = "https://cdn.example/race.png";
    let old_bytes = row_bytes(9, 11);
    let (old_meta, old_key) =
        insert_cache_row(&h, "midraceold0001", url, &old_bytes, BASE_MS, BASE_MS);
    charge(&h, MediaClass::Preview, 9);
    h.store.delete_metadata(&old_meta.id).unwrap();
    assert_eq!(stats(&h).total_bytes, 9);

    // Arm the mid-run refetch: 7 fresh bytes under a new media id.
    let new_bytes = row_bytes(7, 77);
    let new_key = ingest::sha256_hex(&new_bytes);
    *racy.plan() = Some(RefetchPlan {
        source_url: url.to_string(),
        variant: "original".to_string(),
        media_id: "midracenew0001".to_string(),
        bytes: new_bytes,
        class: MediaClass::Preview,
        now_ms: BASE_MS,
    });

    let report = reconcile_media(&h.services).unwrap();
    assert_eq!(
        racy.fired.load(Ordering::SeqCst),
        1,
        "refetch landed at the locked re-read"
    );

    // New index row, media row, and bytes all intact.
    let current = h
        .store
        .remote_cache_lookup(url, "original")
        .unwrap()
        .expect("refetched index row survives reconcile");
    assert_eq!(current.media_id, "midracenew0001");
    assert_eq!(current.blob_key, new_key);
    assert!(h.store.get("midracenew0001").unwrap().is_some());
    assert_eq!(h.cache.byte_len(&new_key).unwrap(), Some(7));
    // Old bytes were never removed either: the row mismatched before any
    // delete, so the now-orphaned K_old bytes stay for the orphan sweep.
    assert_eq!(h.cache.byte_len(&old_key).unwrap(), Some(9));
    // No debit of either key: the old charge and the refetch credit both
    // still stand, and no eviction was counted.
    let after = stats(&h);
    assert_eq!(after.total_bytes, 9 + 7);
    assert_eq!(after.evictions, 0);
    assert_eq!(report.orphans_removed, 0);
}

#[test]
fn reconcile_sweeps_plain_dangling_row_and_debits_once() {
    // No race: a genuinely dangling charged row is removed, its bytes
    // deleted, and its still-charged size debited exactly once.
    let h = harness();
    let (old_meta, old_key) = insert_cache_row(
        &h,
        "midsweepold001",
        "https://cdn.example/sweep.png",
        &row_bytes(9, 13),
        BASE_MS,
        BASE_MS,
    );
    charge(&h, MediaClass::Preview, 9);
    h.store.delete_metadata(&old_meta.id).unwrap();

    let report = reconcile_media(&h.services).unwrap();
    assert_eq!(report.orphans_removed, 1);
    assert!(
        h.store
            .remote_cache_lookup("https://cdn.example/sweep.png", "original")
            .unwrap()
            .is_none()
    );
    assert!(h.cache.byte_len(&old_key).unwrap().is_none());
    let after = stats(&h);
    assert_eq!(after.total_bytes, 0);
    assert_eq!(after.evictions, 1);
}

#[test]
fn reconcile_debits_shared_key_once() {
    // Two dangling index rows sharing one physical key: both rows go and
    // the bytes go, but accounting moves exactly once per key per pass.
    let h = harness();
    let shared = row_bytes(9, 21);
    let (meta_a, key) = insert_cache_row(
        &h,
        "midshared000a",
        "https://cdn.example/shared-a.png",
        &shared,
        BASE_MS,
        BASE_MS,
    );
    assert_eq!(key, ingest::sha256_hex(&shared));
    // Second alias over the same bytes: metadata + index only, no second
    // byte write and no second charge (fetch-path shared-blob rule).
    let meta_b = h
        .store
        .insert_metadata(InsertMediaInput {
            id: Some("midshared000b".to_string()),
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: shared.len() as u64,
            hash: ingest::md5_hex(&shared),
            blob_key: key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: "https://pinetree.local/files/midshared000b".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some("https://cdn.example/shared-b.png".to_string()),
            max_file_size: 10_000_000,
        })
        .unwrap();
    h.store
        .remote_cache_insert(&RemoteCacheRecord {
            source_url: "https://cdn.example/shared-b.png".to_string(),
            variant: "original".to_string(),
            media_id: meta_b.id.clone(),
            blob_key: key.clone(),
            size_bytes: shared.len() as u64,
            class: MediaClass::Preview,
            fetched_at_ms: BASE_MS,
            last_access_ms: BASE_MS,
            access_count: 1,
            bytes_absent: false,
            generation: 0,
        })
        .unwrap();
    charge(&h, MediaClass::Preview, 9);
    h.store.delete_metadata(&meta_a.id).unwrap();
    h.store.delete_metadata(&meta_b.id).unwrap();

    let report = reconcile_media(&h.services).unwrap();
    assert!(
        h.store
            .remote_cache_lookup("https://cdn.example/shared-a.png", "original")
            .unwrap()
            .is_none()
    );
    assert!(
        h.store
            .remote_cache_lookup("https://cdn.example/shared-b.png", "original")
            .unwrap()
            .is_none()
    );
    assert!(h.cache.byte_len(&key).unwrap().is_none());
    // Exactly one debit for the shared key even though two rows were
    // removed (the second byte delete is an idempotent no-op, counted per
    // row as before).
    let after = stats(&h);
    assert_eq!(after.total_bytes, 0);
    assert_eq!(after.evictions, 1);
    assert_eq!(report.orphans_removed, 2);
}

#[test]
fn reconcile_removes_dangling_tombstone_without_debit() {
    // A dangling row that was already charged off: the stale index row
    // still goes, but no accounting moves and no orphan is counted.
    let h = harness();
    let url = "https://cdn.example/tomb.png";
    let tomb_key = ingest::sha256_hex(b"tombstone-marker");
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: Some("midtomb0000001".to_string()),
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: 0,
            hash: ingest::md5_hex(b"tombstone-marker"),
            blob_key: tomb_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: "https://pinetree.local/files/midtomb0000001".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some(url.to_string()),
            max_file_size: 10_000_000,
        })
        .unwrap();
    h.store
        .remote_cache_insert(&RemoteCacheRecord {
            source_url: url.to_string(),
            variant: "original".to_string(),
            media_id: meta.id.clone(),
            blob_key: tomb_key.clone(),
            size_bytes: 0,
            class: MediaClass::Preview,
            // Fresh: the Slice 2 tombstone-retention TTL gate would keep
            // this row, so only the dangling path can remove it here.
            fetched_at_ms: BASE_MS,
            last_access_ms: BASE_MS,
            access_count: 1,
            bytes_absent: true,
            generation: 1,
        })
        .unwrap();
    h.store.delete_metadata(&meta.id).unwrap();
    assert_eq!(stats(&h).total_bytes, 0);

    let report = reconcile_media(&h.services).unwrap();
    assert!(
        h.store
            .remote_cache_lookup(url, "original")
            .unwrap()
            .is_none(),
        "dangling tombstone index row is removed"
    );
    let after = stats(&h);
    assert_eq!(after.total_bytes, 0, "no debit for an already-absent row");
    assert_eq!(after.evictions, 0);
    assert_eq!(report.orphans_removed, 0);
}
