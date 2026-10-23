//! Wave 7 file-serving residuals (adversarial-3 §Wave 7).
//!
//! Black-box through the real router; mirrors the
//! `files_http_cache_hit.rs` harness shape without touching that file.
//! Pins: id validation (no 500/redirect on attacker bytes), short
//! `Cache-Control` on `/files` hits, cold dead-origin 404 (never 500),
//! proxy oracle collapse (wrong-type and dead-origin share one plain 404),
//! proxy URL cap, proxy disk-spill happy path, and hits serving while all
//! 8 cold slots are parked.

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
fn wave7_png_bytes(tag: u8) -> Vec<u8> {
    let mut body = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    body.extend_from_slice(&[tag, 1, 2, 3, 4, 5, 6, 7]);
    body
}

/// Mutable canned origin plus a gate that parks accepted connections.
struct Wave7Origin {
    addr: SocketAddr,
    bodies: Arc<Mutex<HashMap<String, (String, Vec<u8>)>>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
    connections: Arc<AtomicUsize>,
}

impl Wave7Origin {
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
                std::thread::spawn(move || {
                    wave7_handle_origin(stream, bodies, gate, connections);
                });
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

fn wave7_handle_origin(
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

struct Wave7Harness {
    base: String,
    client: reqwest::Client,
    services: Arc<AppServices>,
    cache: Arc<FsBlobStore>,
}

async fn wave7_start() -> (Wave7Harness, Wave7Origin) {
    let root = std::env::temp_dir().join(format!(
        "pinetree-wave7-serve-{}-{}",
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
        Wave7Harness {
            base: format!("http://{addr}"),
            client,
            services,
            cache,
        },
        Wave7Origin::start(),
    )
}

/// Live cache row the way the fetch path leaves it: bytes in the pool,
/// metadata plus one charged `(source_url, original)` index row.
fn wave7_insert_live_row(harness: &Wave7Harness, source_url: &str, bytes: &[u8]) -> String {
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
            url: "https://pinetree.local/files/pending".to_string(),
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
fn wave7_insert_cold_row(harness: &Wave7Harness, source_url: &str, key_tag: u64) -> String {
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
            url: "https://pinetree.local/files/pending".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some(source_url.to_string()),
            max_file_size: MAX_FILE_SIZE,
        })
        .expect("insert cold metadata")
        .id
}

#[tokio::test]
async fn wave7_invalid_file_id_is_plain_404() {
    // Non-id bytes (injection shapes) never reach the DB: plain 404,
    // never 500 and never a redirect. Multi-segment shapes route to the
    // named-file handler and are covered there.
    let (harness, _origin) = wave7_start().await;
    for bad in ["bad!id", "ab+cd", "ab%20cd"] {
        let response = harness
            .client
            .get(format!("{}/files/{bad}", harness.base))
            .send()
            .await
            .expect("GET file");
        assert_eq!(
            response.status().as_u16(),
            404,
            "non-id {bad:?} must be plain 404"
        );
    }
    // Overlong ids are rejected the same way.
    let long = "a".repeat(33);
    let response = harness
        .client
        .get(format!("{}/files/{long}", harness.base))
        .send()
        .await
        .expect("GET file");
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn wave7_named_redirect_validates_id() {
    let (harness, _origin) = wave7_start().await;
    let no_redirect = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(60))
        .build()
        .expect("no-redirect client");

    // Attacker bytes in the named-file path: plain 404, not a `Location`
    // reflecting the input.
    let response = no_redirect
        .get(format!("{}/files/bad!id/evil.png", harness.base))
        .send()
        .await
        .expect("GET named file");
    assert_eq!(response.status().as_u16(), 404);
    assert!(response.headers().get("location").is_none());

    // A valid id still redirects to the canonical path.
    let bytes = wave7_png_bytes(77);
    let url = "http://127.0.0.1:1/wave7-named.png";
    let id = wave7_insert_live_row(&harness, url, &bytes);
    let response = no_redirect
        .get(format!("{}/files/{id}/anything.png", harness.base))
        .send()
        .await
        .expect("GET named file");
    assert_eq!(response.status().as_u16(), 301);
    assert_eq!(
        response.headers().get("location").expect("location"),
        &format!("/files/{id}")
    );
}

#[tokio::test]
async fn wave7_files_hit_sends_short_cache_control() {
    // Anonymous repeat loads are client-cacheable briefly; every server
    // hit still touches (generation bump preserved).
    let (harness, _origin) = wave7_start().await;
    let bytes = wave7_png_bytes(78);
    let url = "http://127.0.0.1:1/wave7-cc.png";
    let id = wave7_insert_live_row(&harness, url, &bytes);
    let before = harness
        .services
        .media
        .remote_cache_lookup(url, "original")
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
            .get("cache-control")
            .expect("cache-control"),
        "public, max-age=60"
    );
    let body = response.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), bytes.as_slice());

    let after = harness
        .services
        .media
        .remote_cache_lookup(url, "original")
        .expect("lookup")
        .expect("index row");
    assert_eq!(after.access_count, before.access_count + 1);
    assert_eq!(after.generation, before.generation + 1);
}

#[tokio::test]
async fn wave7_cold_dead_origin_is_plain_404() {
    // A cold row whose origin refuses connections: the fetch fails with
    // `Validation`, which the cold path maps to plain 404 — never 500.
    // Port 1 on loopback refuses fast with no server needed.
    let (harness, _origin) = wave7_start().await;
    let url = "http://127.0.0.1:1/wave7-dead.png";
    let id = wave7_insert_cold_row(&harness, url, 4242);
    let response = harness
        .client
        .get(format!("{}/files/{id}", harness.base))
        .send()
        .await
        .expect("GET cold file");
    assert_eq!(
        response.status().as_u16(),
        404,
        "dead origin on cold GET must be plain 404, not 500"
    );
}

#[tokio::test]
async fn wave7_proxy_oracle_collapsed_to_single_404() {
    // SSRF-blocked/dead origin and wrong-type used to differ (404 vs 400),
    // letting callers port-scan image hosts. Both are one plain 404 now
    // with identical empty bodies; the allowlist itself is unchanged.
    let (harness, origin) = wave7_start().await;
    let png = wave7_png_bytes(79);
    origin.set_body("/w7.png", "image/png", png.clone());
    origin.set_body(
        "/w7-evil.html",
        "text/html",
        b"<html><script>alert(1)</script></html>".to_vec(),
    );

    // Happy path still streams the bytes (now via a disk spill).
    let response = harness
        .client
        .get(format!("{}/proxy", harness.base))
        .query(&[("url", origin.url("/w7.png"))])
        .header("User-Agent", "pinetree-wave7-test")
        .send()
        .await
        .expect("proxy png");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response.headers().get("content-type").expect("type"),
        "image/png"
    );
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .expect("cache-control"),
        "max-age=31536000, immutable"
    );
    let body = response.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), png.as_slice());

    // Wrong-type refusal: plain 404, not 400.
    let wrong_type = harness
        .client
        .get(format!("{}/proxy", harness.base))
        .query(&[("url", origin.url("/w7-evil.html"))])
        .header("User-Agent", "pinetree-wave7-test")
        .send()
        .await
        .expect("proxy html");
    assert_eq!(wrong_type.status().as_u16(), 404);
    let wrong_body = wrong_type.bytes().await.expect("read body");

    // Dead origin: the same plain 404 with the same body.
    let dead = harness
        .client
        .get(format!("{}/proxy", harness.base))
        .query(&[("url", "http://127.0.0.1:1/wave7-nodead.png")])
        .header("User-Agent", "pinetree-wave7-test")
        .send()
        .await
        .expect("proxy dead");
    assert_eq!(dead.status().as_u16(), 404);
    let dead_body = dead.bytes().await.expect("read body");
    assert_eq!(
        wrong_body.as_ref(),
        dead_body.as_ref(),
        "refusals must be indistinguishable"
    );
}

#[tokio::test]
async fn wave7_proxy_url_over_cap_is_404() {
    // Proxy URLs share the 1024-char cap with the fetch core; longer URLs
    // are the same plain 404 as every other refusal.
    let (harness, _origin) = wave7_start().await;
    let long_url = format!("http://127.0.0.1:1/{}", "a".repeat(2000));
    assert!(long_url.chars().count() > 1024);
    let response = harness
        .client
        .get(format!("{}/proxy", harness.base))
        .query(&[("url", long_url)])
        .header("User-Agent", "pinetree-wave7-test")
        .send()
        .await
        .expect("proxy long url");
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wave7_hit_serves_while_cold_slots_exhausted() {
    // All 8 cold slots parked on a gated origin: a cache hit (separate
    // hit pool) still serves 200 instead of queueing behind the cold
    // bound or failing.
    let (harness, origin) = wave7_start().await;
    let bytes = wave7_png_bytes(80);
    let hit_url = origin.url("/wave7-hit.png");
    let hit_id = wave7_insert_live_row(&harness, &hit_url, &bytes);

    origin.close_gate();
    let mut ids = Vec::new();
    for tag in 0u64..12 {
        let url = origin.url(&format!("/wave7-slot{tag}.bin"));
        origin.set_body(
            &format!("/wave7-slot{tag}.bin"),
            "image/png",
            wave7_png_bytes(tag as u8),
        );
        ids.push(wave7_insert_cold_row(&harness, &url, 9000 + tag));
    }
    let mut parked = Vec::new();
    let mut parked_count = 0usize;
    for id in &ids {
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
                "cold slot exhaustion must fail closed retryable"
            );
            assert!(response.headers().get("retry-after").is_some());
            break;
        }
        assert!(observed_park, "parked fetch must reach the gated origin");
        parked.push(task);
        parked_count += 1;
    }
    assert!(
        parked_count >= 1,
        "at least one fetch must park before the hit probe"
    );

    // The hit serves while cold slots are full.
    let response = harness
        .client
        .get(format!("{}/files/{hit_id}", harness.base))
        .send()
        .await
        .expect("GET hit while cold slots full");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .expect("cache-control"),
        "public, max-age=60"
    );
    let body = response.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), bytes.as_slice());

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
