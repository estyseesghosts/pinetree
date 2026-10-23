//! Slice 3 (adversarial-2 findings 3, 5-hit-path, D): `GET /files/{id}`
//! and `GET /proxy` serve off the Tokio worker, stream from the blob
//! store, share a bounded cold-fetch slot budget, touch the LRU on hits,
//! and serve refreshed metadata after a refetch.
//!
//! Black-box through the real router: a loopback server plus a mutable
//! canned origin exercise the HTTP surface, so no handler internals are
//! coupled here. The slot-exhaustion test is adaptive (it parks gated
//! fetches until the first 429) rather than hardcoding the slot count.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, Clock, IdGenerator, InsertMediaInput, MediaClass, MediaStore,
    RemoteCacheRecord,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::storage::FsBlobStore;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};
use pinetree::streaming::hub::hub_and_sink;

const MAX_FILE_SIZE: u64 = 1 << 20;
const CACHE_CAP: u64 = 512 << 20;

/// Minimal PNG body: sniffs `image/png`, hence class Preview.
fn png_bytes(tag: u8) -> Vec<u8> {
    let mut body = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    body.extend_from_slice(&[tag, 1, 2, 3, 4, 5, 6, 7]);
    body
}

fn text_bytes() -> Vec<u8> {
    b"hello world, refetched text".to_vec()
}

/// Mutable canned origin: per-path bodies plus a gate that parks accepted
/// connections before responding (slot-exhaustion test). The gate starts
/// open; each accepted connection counts before waiting on it.
struct Origin {
    addr: SocketAddr,
    bodies: Arc<Mutex<HashMap<String, (String, Vec<u8>)>>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
    connections: Arc<AtomicUsize>,
}

impl Origin {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind origin");
        let addr = listener.local_addr().expect("origin addr");
        let bodies: Arc<Mutex<HashMap<String, (String, Vec<u8>)>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let gate = Arc::new((Mutex::new(true), Condvar::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let worker_bodies = bodies.clone();
        let worker_gate = gate.clone();
        let worker_connections = connections.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let bodies = worker_bodies.clone();
                let gate = worker_gate.clone();
                let connections = worker_connections.clone();
                std::thread::spawn(move || handle_origin(stream, bodies, gate, connections));
            }
        });
        Self {
            addr,
            bodies,
            gate,
            connections,
        }
    }

    fn set_body(&self, path: &str, content_type: &str, body: Vec<u8>) {
        self.bodies
            .lock()
            .unwrap()
            .insert(path.to_string(), (content_type.to_string(), body));
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn close_gate(&self) {
        *self.gate.0.lock().unwrap() = false;
    }

    fn open_gate(&self) {
        *self.gate.0.lock().unwrap() = true;
        self.gate.1.notify_all();
    }

    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

fn handle_origin(
    mut stream: TcpStream,
    bodies: Arc<Mutex<HashMap<String, (String, Vec<u8>)>>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
    connections: Arc<AtomicUsize>,
) {
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                head.extend_from_slice(&chunk[..n]);
                if head.windows(4).any(|w| w == b"\r\n\r\n") || head.len() > 16_384 {
                    break;
                }
            }
            Err(_) => return,
        }
    }
    let path = String::from_utf8_lossy(&head)
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/")
        .to_string();
    connections.fetch_add(1, Ordering::SeqCst);
    let (lock, cvar) = &*gate;
    let mut open = lock.lock().unwrap();
    while !*open {
        let (guard, timeout) = cvar.wait_timeout(open, Duration::from_secs(90)).unwrap();
        open = guard;
        if timeout.timed_out() {
            break;
        }
    }
    drop(open);
    let bodies = bodies.lock().unwrap();
    if let Some((content_type, body)) = bodies.get(&path) {
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(body);
    } else {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    }
    let _ = stream.flush();
}

struct Harness {
    base: String,
    client: reqwest::Client,
    services: Arc<AppServices>,
    cache: Arc<FsBlobStore>,
}

async fn start() -> (Harness, Origin) {
    let root = std::env::temp_dir().join(format!(
        "pinetree-files-serve-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).expect("create test dir");
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store =
        RedbStore::open(root.join("t.redb"), clock.clone(), ids.clone()).expect("open store");
    store.set_cache_cap(CACHE_CAP).expect("set cache cap");
    let durable = Arc::new(FsBlobStore::new(root.join("blobs_durable")).expect("durable pool"));
    let cache = Arc::new(FsBlobStore::new(root.join("blobs_cache")).expect("cache pool"));
    let (hub, streaming) = hub_and_sink(1024);
    let services = Arc::new(AppServices {
        clock: clock.clone(),
        ids: ids.clone(),
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
        blobs: durable,
        cache_blobs: cache.clone(),
        notifications: store.clone(),
        auth: store.clone(),
        streaming: streaming.clone(),
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
        max_file_size: MAX_FILE_SIZE,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: CACHE_CAP,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    });
    let worker = Arc::new(JobWorker {
        clock: clock.clone(),
        ids: ids.clone(),
        jobs: store.clone(),
        notifications: store.clone(),
        streaming,
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 30_000,
        base_backoff_ms: 1_000,
        transcode_config: TranscodeConfig {
            enabled: false,
            ..TranscodeConfig::default()
        },
        email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
    });
    let state = ApiState {
        services: services.clone(),
        worker,
        rate_limiter: Arc::new(RateLimiter::with_misskey_defaults(clock.clone())),
        upload_slots: Arc::new(UploadSlots::with_defaults()),
        hub,
        trusted_proxies: Arc::new(Vec::new()),
        featured_cache: Arc::new(pinetree::application::commands::pins::FeaturedCache::default()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            build_router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .expect("serve test router");
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("http client");
    (
        Harness {
            base: format!("http://{addr}"),
            client,
            services,
            cache,
        },
        Origin::start(),
    )
}

/// Live cache row the way the fetch path leaves it: bytes in the pool,
/// metadata plus one charged `(source_url, original)` index row.
fn insert_live_row(harness: &Harness, source_url: &str, bytes: &[u8]) -> String {
    let blob_key = sha256_hex(bytes);
    harness.cache.put(&blob_key, bytes).expect("stage bytes");
    let meta = harness
        .services
        .media
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: bytes.len() as u64,
            hash: md5_hex(bytes),
            blob_key: blob_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: format!("https://pinetree.local/files/pending"),
            thumbnail_url: None,
            comment: None,
            source_url: Some(source_url.to_string()),
            max_file_size: MAX_FILE_SIZE,
        })
        .expect("insert metadata");
    let now = harness.services.clock.now_ms();
    harness
        .services
        .media
        .remote_cache_insert(&RemoteCacheRecord {
            source_url: source_url.to_string(),
            variant: "original".to_string(),
            media_id: meta.id.clone(),
            blob_key,
            size_bytes: bytes.len() as u64,
            class: MediaClass::Preview,
            fetched_at_ms: now,
            last_access_ms: now,
            access_count: 1,
            generation: 0,
            bytes_absent: false,
        })
        .expect("insert index row");
    meta.id
}

/// Cold row: metadata with no bytes behind it and no index row, so the
/// next `GET` must refetch from the origin.
fn insert_cold_row(harness: &Harness, source_url: &str, key_tag: u64) -> String {
    harness
        .services
        .media
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "application/octet-stream".to_string(),
            size_bytes: 0,
            hash: format!("{key_tag:032}"),
            blob_key: format!("{key_tag:064}"),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: format!("https://pinetree.local/files/pending"),
            thumbnail_url: None,
            comment: None,
            source_url: Some(source_url.to_string()),
            max_file_size: MAX_FILE_SIZE,
        })
        .expect("insert cold metadata")
        .id
}

#[tokio::test]
async fn file_hit_touches_cache_record() {
    let (harness, origin) = start().await;
    let bytes = png_bytes(7);
    let url = origin.url("/hit.png");
    let id = insert_live_row(&harness, &url, &bytes);
    let before = harness
        .services
        .media
        .remote_cache_lookup(&url, "original")
        .expect("lookup")
        .expect("index row");

    let response = harness
        .client
        .get(format!("{}/files/{id}", harness.base))
        .send()
        .await
        .expect("GET file");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("content type"),
        "image/png"
    );
    assert_eq!(
        response
            .headers()
            .get("x-content-type-options")
            .expect("nosniff"),
        "nosniff"
    );
    let body = response.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), bytes.as_slice());

    let after = harness
        .services
        .media
        .remote_cache_lookup(&url, "original")
        .expect("lookup")
        .expect("index row");
    assert_eq!(
        after.access_count,
        before.access_count + 1,
        "hit must advance the access record"
    );
    assert_eq!(
        after.generation,
        before.generation + 1,
        "hit must move the generation past any listed snapshot"
    );
    assert!(
        after.last_access_ms >= before.last_access_ms,
        "hit must not move LRU backwards"
    );
}

#[tokio::test]
async fn refetch_with_changed_mime_serves_new_type() {
    let (harness, origin) = start().await;
    let first = png_bytes(11);
    origin.set_body("/swap.bin", "image/png", first.clone());
    let url = origin.url("/swap.bin");
    let cold_id = insert_cold_row(&harness, &url, 41);

    // Cold fetch: the served type comes from the fetched bytes, not the
    // stale pre-fetch row (`application/octet-stream`).
    let response = harness
        .client
        .get(format!("{}/files/{cold_id}", harness.base))
        .send()
        .await
        .expect("GET cold file");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("content type"),
        "image/png"
    );
    let body = response.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), first.as_slice());

    let fresh = harness
        .services
        .media
        .find_by_source_url(&url)
        .expect("find by source")
        .pop()
        .expect("fresh row")
        .id;
    let rec = harness
        .services
        .media
        .remote_cache_lookup(&url, "original")
        .expect("lookup")
        .expect("index row");
    let first_key = rec.blob_key.clone();

    // Origin changes content; cached bytes are dropped so the next `GET`
    // must refetch. The response must carry the NEW type, not the
    // pre-refetch `image/png`.
    let second = text_bytes();
    origin.set_body("/swap.bin", "text/plain", second.clone());
    harness.cache.delete(&first_key).expect("drop bytes");

    let response = harness
        .client
        .get(format!("{}/files/{fresh}", harness.base))
        .send()
        .await
        .expect("GET refetched file");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("content type"),
        "text/plain",
        "refetch response must use refreshed metadata"
    );
    let body = response.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), second.as_slice());

    let meta = harness
        .services
        .media
        .get(&fresh)
        .expect("get meta")
        .expect("row");
    assert_eq!(meta.mime, "text/plain");
    let rec = harness
        .services
        .media
        .remote_cache_lookup(&url, "original")
        .expect("lookup")
        .expect("index row");
    assert_ne!(rec.blob_key, first_key);
}

#[tokio::test]
async fn proxy_serves_allowlisted_media_and_refuses_active_content() {
    let (harness, origin) = start().await;
    let png = png_bytes(13);
    origin.set_body("/p.png", "image/png", png.clone());
    origin.set_body(
        "/evil.html",
        "text/html",
        b"<html><script>alert(1)</script></html>".to_vec(),
    );

    let response = harness
        .client
        .get(format!("{}/proxy", harness.base))
        .query(&[("url", origin.url("/p.png"))])
        .header("User-Agent", "pinetree-slice3-test")
        .send()
        .await
        .expect("proxy png");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("content type"),
        "image/png"
    );
    assert_eq!(
        response
            .headers()
            .get("x-content-type-options")
            .expect("nosniff"),
        "nosniff"
    );
    let body = response.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), png.as_slice());

    // The inline-safe allowlist is unchanged: active content is refused.
    // Wave 7 oracle collapse: wrong-type refusal is the same plain 404 as
    // SSRF-blocked/dead origins (no longer distinguishable 400).
    let response = harness
        .client
        .get(format!("{}/proxy", harness.base))
        .query(&[("url", origin.url("/evil.html"))])
        .header("User-Agent", "pinetree-slice3-test")
        .send()
        .await
        .expect("proxy html");
    assert_eq!(response.status().as_u16(), 404);

    // Missing User-Agent is still refused.
    let response = harness
        .client
        .get(format!("{}/proxy", harness.base))
        .query(&[("url", origin.url("/p.png"))])
        .send()
        .await
        .expect("proxy without UA");
    assert_eq!(response.status().as_u16(), 400);

    // Proxy-loop protection is unchanged.
    let response = harness
        .client
        .get(format!("{}/proxy", harness.base))
        .query(&[("url", origin.url("/p.png"))])
        .header("User-Agent", "misskey/2026.10 test")
        .send()
        .await
        .expect("proxy loop");
    assert_eq!(response.status().as_u16(), 403);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slot_exhausted_cold_fetch_is_retryable_without_reservation_leak() {
    let (harness, origin) = start().await;
    origin.close_gate();
    // One cold row per parked attempt plus the rejected probe.
    let mut ids = Vec::new();
    for tag in 0u64..33 {
        let url = origin.url(&format!("/slot{tag}.bin"));
        origin.set_body(
            &format!("/slot{tag}.bin"),
            "image/png",
            png_bytes(tag as u8),
        );
        ids.push((
            url,
            insert_cold_row(&harness, &origin.url(&format!("/slot{tag}.bin")), 100 + tag),
        ));
    }

    // Park gated fetches until the first immediate 429: no slot count is
    // hardcoded, so this observes whatever bound the server enforces.
    let mut parked = Vec::new();
    let mut parked_count = 0usize;
    let mut rejected = None;
    for (_, id) in &ids {
        let task = tokio::spawn({
            let client = harness.client.clone();
            let base = harness.base.clone();
            let id = id.clone();
            async move { client.get(format!("{base}/files/{id}")).send().await }
        });
        let mut observed_park = false;
        for _ in 0..100 {
            if origin.connection_count() == parked_count + 1 {
                observed_park = true;
                break;
            }
            if task.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if task.is_finished() {
            let response = task.await.expect("join probe").expect("probe response");
            assert_eq!(
                response.status().as_u16(),
                429,
                "slot exhaustion must fail closed retryable"
            );
            rejected = Some(response);
            break;
        }
        assert!(observed_park, "parked fetch must reach the gated origin");
        parked.push(task);
        parked_count += 1;
    }
    let rejected = rejected.expect("slot limit must trip within 33 attempts");
    assert!(parked_count >= 1, "at least one fetch must park first");
    assert!(
        rejected.headers().get("retry-after").is_some(),
        "exhaustion must carry a Retry-After hint"
    );
    let value: serde_json::Value = rejected.json().await.expect("error body");
    assert_eq!(value["code"], "RATE_LIMIT_EXCEEDED");
    // The rejected probe never reached the origin: exhaustion happens
    // before any fetch or reservation exists.
    assert_eq!(origin.connection_count(), parked_count);

    // Proxy origin fetches share the same limit.
    let proxy = harness
        .client
        .get(format!("{}/proxy", harness.base))
        .query(&[("url", origin.url("/slot0.bin"))])
        .header("User-Agent", "pinetree-slice3-test")
        .send()
        .await
        .expect("proxy while exhausted");
    assert_eq!(proxy.status().as_u16(), 429);

    // While parked, every in-flight fetch holds exactly one max-size
    // reservation: nothing reserved twice, nothing released early.
    let stats = harness.services.media.cache_stats().expect("cache stats");
    assert_eq!(stats.in_flight_bytes, parked_count as u64 * MAX_FILE_SIZE);

    // Release the origin: every parked fetch completes, and every
    // reservation is released exactly once (slice 1 guard).
    origin.open_gate();
    for task in parked {
        let response = task.await.expect("join parked").expect("parked response");
        assert_eq!(response.status().as_u16(), 200);
    }
    let stats = harness.services.media.cache_stats().expect("cache stats");
    assert_eq!(
        stats.in_flight_bytes, 0,
        "completed fetches must not leak reservations"
    );
}
