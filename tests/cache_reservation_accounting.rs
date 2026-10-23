//! Slice 1 (reservation accounting): after `reserve_cache_bytes` succeeds,
//! every fetch exit releases the FULL reservation exactly once, the real
//! size is added to `total_bytes` exactly once for the charge owner only,
//! `ensure_cache_budget` counts `in_flight_bytes` in global pressure, and
//! the sniffed (real) class is re-checked before commit.
//!
//! Each test below fails on the pre-slice-1 code: successful fetches leak
//! `actual` into `in_flight_bytes`, shared-blob hits leave a phantom
//! reservation, post-reserve `?` paths (locks/lookups/touch) leak the full
//! reservation, ignored release failures hide leaks, and a Full object
//! sails through on Preview room with no second class check.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, CacheEvictionOutcome, InsertMediaInput, MediaClass, MediaStore,
    NoopStreamingSink, PortError, RemoteCacheRecord, TimelineCursor,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::{MediaMeta, MediaProperties};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::remote_cache::fetch_on_first_request;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

/// Fault switches consulted by the wrapping stores. All default to off, so
/// a plain harness behaves exactly like production redb + `FsBlobStore`.
#[derive(Default)]
struct FaultState {
    lookup_calls: AtomicU64,
    /// Fail the Nth `remote_cache_lookup` call (0 = never). The miss-path
    /// fetch looks up once before the reserve and once under the commit
    /// locks, so `2` faults the post-reserve recheck only.
    fail_lookup_on_call: AtomicU64,
    fail_insert_metadata: AtomicBool,
    fail_remote_cache_insert: AtomicBool,
    fail_touch: AtomicBool,
    fail_import: AtomicBool,
    fail_release: AtomicBool,
}

struct FaultMedia {
    inner: Arc<dyn MediaStore>,
    faults: Arc<FaultState>,
}

impl MediaStore for FaultMedia {
    fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError> {
        if self.faults.fail_insert_metadata.load(Ordering::SeqCst) {
            return Err(PortError::StorageUnavailable(
                "injected insert failure".to_string(),
            ));
        }
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
        let call = self.faults.lookup_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let fail_on = self.faults.fail_lookup_on_call.load(Ordering::SeqCst);
        if fail_on != 0 && call == fail_on {
            return Err(PortError::StorageUnavailable(
                "injected lookup failure".to_string(),
            ));
        }
        self.inner.remote_cache_lookup(source_url, variant)
    }
    fn remote_cache_insert(&self, rec: &RemoteCacheRecord) -> Result<(), PortError> {
        if self.faults.fail_remote_cache_insert.load(Ordering::SeqCst) {
            return Err(PortError::StorageUnavailable(
                "injected index failure".to_string(),
            ));
        }
        self.inner.remote_cache_insert(rec)
    }
    fn remote_cache_touch(
        &self,
        source_url: &str,
        variant: &str,
        now_ms: i64,
    ) -> Result<(), PortError> {
        if self.faults.fail_touch.load(Ordering::SeqCst) {
            return Err(PortError::StorageUnavailable(
                "injected touch failure".to_string(),
            ));
        }
        self.inner.remote_cache_touch(source_url, variant, now_ms)
    }
    fn remote_cache_evict_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        self.inner.remote_cache_evict_candidates(limit)
    }
    fn remote_cache_remove(&self, source_url: &str, variant: &str) -> Result<(), PortError> {
        self.inner.remote_cache_remove(source_url, variant)
    }
    fn cache_stats(&self) -> Result<pinetree::application::ports::CacheStats, PortError> {
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
        if self.faults.fail_release.load(Ordering::SeqCst) {
            return Err(PortError::StorageUnavailable(
                "injected release failure".to_string(),
            ));
        }
        self.inner.release_cache_reservation(bytes)
    }
    fn set_cache_cap(&self, cap_bytes: u64) -> Result<(), PortError> {
        self.inner.set_cache_cap(cap_bytes)
    }
}

struct FaultBlobs {
    inner: Arc<dyn BlobStore>,
    faults: Arc<FaultState>,
}

impl BlobStore for FaultBlobs {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        self.inner.put(hash, bytes)
    }
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        self.inner.get(hash)
    }
    fn delete(&self, hash: &str) -> Result<(), PortError> {
        self.inner.delete(hash)
    }
    fn byte_len(&self, hash: &str) -> Result<Option<u64>, PortError> {
        self.inner.byte_len(hash)
    }
    fn import_file(&self, hash: &str, src: &std::path::Path) -> Result<(), PortError> {
        if self.faults.fail_import.load(Ordering::SeqCst) {
            return Err(PortError::StorageUnavailable(
                "injected import failure".to_string(),
            ));
        }
        self.inner.import_file(hash, src)
    }
}

struct Harness {
    services: AppServices,
    faults: Arc<FaultState>,
    cache: Arc<FsBlobStore>,
}

fn harness_with(
    max_file_size: u64,
    max_bytes: u64,
    preview_bytes: u64,
    full_bytes: u64,
) -> Harness {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-reservation-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<ManualClock> = Arc::new(ManualClock::new(BASE_MS));
    let clock_port: Arc<dyn Clock> = clock.clone();
    let store =
        RedbStore::open_with_default_ids(dir.join("store.redb"), clock_port.clone()).unwrap();
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    let real_cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let faults = Arc::new(FaultState::default());
    let media_inner: Arc<dyn MediaStore> = store.clone();
    let media: Arc<dyn MediaStore> = Arc::new(FaultMedia {
        inner: media_inner,
        faults: faults.clone(),
    });
    let blobs_inner: Arc<dyn BlobStore> = real_cache.clone();
    let cache_blobs: Arc<dyn BlobStore> = Arc::new(FaultBlobs {
        inner: blobs_inner,
        faults: faults.clone(),
    });
    let services = AppServices {
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
        media,
        blobs: durable,
        cache_blobs,
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
        max_file_size,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: max_bytes,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: preview_bytes,
        remote_cache_full_bytes: full_bytes,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    };
    Harness {
        services,
        faults,
        cache: real_cache,
    }
}

fn default_harness() -> Harness {
    harness_with(4096, 512 << 20, 256 << 20, 128 << 20)
}

/// Minimal PNG body: sniffs `image/png`, hence class Preview.
fn png_bytes(tag: u8) -> Vec<u8> {
    vec![
        0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, tag, 1, 2, 3, 4, 5, 6, 7,
    ]
}

/// Serve one canned body per path on 127.0.0.1 (the harness client allows
/// private IPs, mirroring the existing fetch tests).
fn serve_bodies(bodies: Vec<(String, Vec<u8>)>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for _ in 0..bodies.len() + 8 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).into_owned();
            let path = req
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_string();
            match bodies.iter().find(|(p, _)| *p == path) {
                Some((_, body)) => {
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(body);
                }
                None => {
                    let _ = stream.write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                }
            }
            let _ = stream.flush();
        }
    });
    addr
}

/// Insert one charged Full row exactly the way the production fetch path
/// does (bytes + metadata + index + accounting).
fn insert_full_row(harness: &Harness, source_url: &str, bytes: &[u8]) {
    let key = sha256_hex(bytes);
    harness.cache.put(&key, bytes).unwrap();
    let meta = harness
        .services
        .media
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "prefill.txt".to_string(),
            mime: "text/plain".to_string(),
            size_bytes: bytes.len() as u64,
            hash: md5_hex(bytes),
            blob_key: key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: "https://pinetree.local/files/prefill-row".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some(source_url.to_string()),
            max_file_size: harness.services.max_file_size,
        })
        .unwrap();
    harness
        .services
        .media
        .remote_cache_insert(&RemoteCacheRecord {
            source_url: source_url.to_string(),
            variant: "original".to_string(),
            media_id: meta.id,
            blob_key: key,
            size_bytes: bytes.len() as u64,
            class: MediaClass::Full,
            fetched_at_ms: BASE_MS,
            last_access_ms: BASE_MS,
            access_count: 1,
            bytes_absent: false,
            generation: 0,
        })
        .unwrap();
    harness
        .services
        .media
        .cache_accounting_apply(
            bytes.len() as i64,
            MediaClass::Full,
            bytes.len() as i64,
            0,
            1,
            bytes.len() as u64,
            0,
            0,
        )
        .unwrap();
}

#[test]
fn three_successful_fetches_leave_no_reservation() {
    let harness = default_harness();
    let bodies = vec![png_bytes(0), png_bytes(1), png_bytes(2)];
    let addr = serve_bodies(vec![
        ("/t0.png".to_string(), bodies[0].clone()),
        ("/t1.png".to_string(), bodies[1].clone()),
        ("/t2.png".to_string(), bodies[2].clone()),
    ]);
    let mut total = 0u64;
    for (i, name) in ["/t0.png", "/t1.png", "/t2.png"].iter().enumerate() {
        let url = format!("http://{addr}{name}");
        fetch_on_first_request(&harness.services, &url, "original").expect("fetch must succeed");
        total += bodies[i].len() as u64;
        let stats = harness.services.media.cache_stats().unwrap();
        assert_eq!(stats.in_flight_bytes, 0, "fetch {i} leaked its reservation");
        assert_eq!(
            stats.total_bytes, total,
            "fetch {i} must charge its bytes exactly once"
        );
    }
}

#[test]
fn shared_blob_second_url_does_not_double_count() {
    let harness = default_harness();
    let body = png_bytes(9);
    let addr = serve_bodies(vec![
        ("/s1.png".to_string(), body.clone()),
        ("/s2.png".to_string(), body.clone()),
    ]);
    let url1 = format!("http://{addr}/s1.png");
    let url2 = format!("http://{addr}/s2.png");
    let first = fetch_on_first_request(&harness.services, &url1, "original").unwrap();
    let after_first = harness.services.media.cache_stats().unwrap();
    assert_eq!(after_first.in_flight_bytes, 0);
    assert_eq!(after_first.total_bytes, body.len() as u64);
    let second = fetch_on_first_request(&harness.services, &url2, "original").unwrap();
    assert_ne!(
        first.id, second.id,
        "distinct source URLs get distinct index rows"
    );
    assert_eq!(
        first.blob_key, second.blob_key,
        "identical bytes share one physical blob"
    );
    let after_second = harness.services.media.cache_stats().unwrap();
    assert_eq!(
        after_second.in_flight_bytes, 0,
        "shared-blob hit left a phantom reservation"
    );
    assert_eq!(
        after_second.total_bytes, after_first.total_bytes,
        "shared blob must not double-charge total_bytes"
    );
}

#[test]
fn lookup_failure_between_reserve_and_commit_releases_reservation() {
    // Second `remote_cache_lookup` is the winner recheck under the commit
    // locks: faulting it exercises a post-reserve `?` that leaked the full
    // reservation before slice 1.
    let harness = default_harness();
    harness
        .faults
        .fail_lookup_on_call
        .store(2, Ordering::SeqCst);
    let body = png_bytes(21);
    let addr = serve_bodies(vec![("/lk.png".to_string(), body)]);
    let url = format!("http://{addr}/lk.png");
    let err = fetch_on_first_request(&harness.services, &url, "original")
        .expect_err("injected lookup failure must surface");
    assert!(
        matches!(err, PortError::StorageUnavailable(ref m) if m == "injected lookup failure"),
        "unexpected: {err:?}"
    );
    let stats = harness.services.media.cache_stats().unwrap();
    assert_eq!(
        stats.in_flight_bytes, 0,
        "failed lookup leaked the reservation"
    );
    assert_eq!(stats.total_bytes, 0, "failed lookup must not charge");
}

#[test]
fn insert_failure_between_reserve_and_commit_releases_reservation() {
    let harness = default_harness();
    harness
        .faults
        .fail_insert_metadata
        .store(true, Ordering::SeqCst);
    let body = png_bytes(22);
    let addr = serve_bodies(vec![("/ins.png".to_string(), body)]);
    let url = format!("http://{addr}/ins.png");
    let err = fetch_on_first_request(&harness.services, &url, "original")
        .expect_err("injected insert failure must surface");
    assert!(
        matches!(err, PortError::StorageUnavailable(ref m) if m == "injected insert failure"),
        "unexpected: {err:?}"
    );
    let stats = harness.services.media.cache_stats().unwrap();
    assert_eq!(
        stats.in_flight_bytes, 0,
        "failed insert leaked the reservation"
    );
    assert_eq!(stats.total_bytes, 0, "failed insert must not charge");
}

#[test]
fn stage_import_failure_between_reserve_and_commit_releases_reservation() {
    let harness = default_harness();
    harness.faults.fail_import.store(true, Ordering::SeqCst);
    let body = png_bytes(23);
    let addr = serve_bodies(vec![("/st.png".to_string(), body)]);
    let url = format!("http://{addr}/st.png");
    let err = fetch_on_first_request(&harness.services, &url, "original")
        .expect_err("injected import failure must surface");
    assert!(
        matches!(err, PortError::StorageUnavailable(ref m) if m == "injected import failure"),
        "unexpected: {err:?}"
    );
    let stats = harness.services.media.cache_stats().unwrap();
    assert_eq!(
        stats.in_flight_bytes, 0,
        "failed stage leaked the reservation"
    );
    assert_eq!(stats.total_bytes, 0, "failed stage must not charge");
}

#[test]
fn failed_release_surfaces_as_error() {
    // A failed `release_cache_reservation` must propagate as `PortError`,
    // never be discarded with `let _ =` while reporting success.
    let harness = default_harness();
    harness.faults.fail_release.store(true, Ordering::SeqCst);
    let body = png_bytes(24);
    let addr = serve_bodies(vec![("/rel.png".to_string(), body)]);
    let url = format!("http://{addr}/rel.png");
    let err = fetch_on_first_request(&harness.services, &url, "original")
        .expect_err("injected release failure must surface, not report success");
    assert!(
        matches!(err, PortError::StorageUnavailable(ref m) if m == "injected release failure"),
        "unexpected: {err:?}"
    );
}

#[test]
fn sniffed_class_mismatch_fails_closed_without_commit() {
    // Full is capped at 16 bytes against an 8192-byte global with one
    // 4096-byte Full row already charged (total 4096, slack 4096). A new
    // text/plain fetch (~70 bytes, class Full) reserves fine under the
    // provisional Preview budget, then must fail closed on the real Full
    // budget with no bytes and no reservation committed.
    let harness = harness_with(4096, 8192, 8192, 16);
    let prefill = vec![b'a'; 4096];
    insert_full_row(&harness, "https://remote.example/prefill.txt", &prefill);
    let before = harness.services.media.cache_stats().unwrap();
    assert_eq!(before.total_bytes, 4096);
    assert_eq!(before.full_bytes, 4096);
    assert_eq!(before.in_flight_bytes, 0);
    let late_body =
        b"late plain-text payload for the slice-1 class recheck test 0123456789".to_vec();
    assert!(late_body.len() > 16 && (late_body.len() as u64) <= 4096);
    let addr = serve_bodies(vec![("/late.txt".to_string(), late_body.clone())]);
    let url = format!("http://{addr}/late.txt");
    let err = fetch_on_first_request(&harness.services, &url, "original")
        .expect_err("Full sniff over a 16-byte Full cap must fail closed");
    assert!(
        matches!(err, PortError::Validation(ref m) if m == "CACHE_CAPACITY_EXCEEDED"),
        "unexpected: {err:?}"
    );
    let after = harness.services.media.cache_stats().unwrap();
    assert_eq!(
        after.total_bytes, before.total_bytes,
        "rejected class must not move totals"
    );
    assert_eq!(after.full_bytes, before.full_bytes);
    assert_eq!(
        after.preview_bytes, before.preview_bytes,
        "a Full object must never charge the Preview budget"
    );
    assert_eq!(
        after.in_flight_bytes, 0,
        "rejected class must not hold a reservation"
    );
    assert!(
        harness
            .services
            .media
            .remote_cache_lookup(&url, "original")
            .unwrap()
            .is_none(),
        "rejected fetch must not index"
    );
}
