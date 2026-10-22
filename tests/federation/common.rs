//! Shared harness for the `federation` integration tests (see
//! `tests/federation.rs`): live two-node boot, HTTP helpers, signed-POST/GET
//! helpers, and drain/poll synchronization. No `#[test]` lives here.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use serde_json::Value;

use pinetree::activitypub::signatures::sign_request_headers;
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{ActivityDedup, Clock, NoopStreamingSink};
use pinetree::domain::id::SystemClock;
use pinetree::domain::note::Note;
use pinetree::federation::delivery::{
    MAX_AP_JSON_BYTES, SafeHttpClient, SafeHttpConfig, SafeHttpResponse, format_http_date,
    parse_http_url,
};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::storage::FsBlobStore;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

pub(crate) static NODE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One live federation node: services, worker, router state, and the
/// public `http://127.0.0.1:{port}` base URL (which doubles as `public_url`).
pub(crate) struct FedNode {
    pub(crate) tag: &'static str,
    pub(crate) services: Arc<AppServices>,
    pub(crate) worker: Arc<JobWorker>,
    pub(crate) state: ApiState,
    pub(crate) base_url: String,
    pub(crate) host: String,
    pub(crate) addr: SocketAddr,
    pub(crate) serve_handle: Option<tokio::task::JoinHandle<()>>,
    // Dedicated runtime per node (instead of the shared test runtime):
    // production nodes never share a runtime, and sharing one makes a
    // blocking network call in any node's handler able to starve the
    // others' scheduling. Holding the runtime keeps it alive.
    #[allow(dead_code)]
    pub(crate) runtime: Option<Arc<tokio::runtime::Runtime>>,
}

/// Federation routes (now wired in `build_router` in `src/`).
fn federation_router(state: ApiState) -> Router {
    build_router(state)
}

async fn wait_ready(client: &SafeHttpClient, base_url: &str, tag: &str) {
    let start = Instant::now();
    loop {
        match client.send("GET", &format!("{base_url}/healthz"), &[], None, 1024) {
            Ok(resp) if resp.status == 200 => return,
            _ => {
                assert!(
                    start.elapsed() < Duration::from_secs(15),
                    "{tag} server did not become ready"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

pub(crate) fn boot_node(tag: &'static str) -> FedNode {
    // Boot on a raw OS thread: building a runtime panics inside a
    // runtime context, and the callers run on the shared test runtime.
    // Each node gets a DEDICATED runtime (production nodes never share
    // one): a blocking network call in one node's handler must not be
    // able to starve another node's scheduling. The runtime is stored on
    // the node to keep it alive.
    std::thread::Builder::new()
        .name(format!("fed-boot-{tag}"))
        .spawn(move || boot_node_inner(tag))
        .expect("spawn fed boot thread")
        .join()
        .expect("fed boot thread")
}

fn boot_node_inner(tag: &'static str) -> FedNode {
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name(format!("fed-{tag}"))
            .enable_all()
            .build()
            .expect("build fed runtime"),
    );
    let n = NODE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-fed-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create fed temp dir");

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("bind fed listener");
    let addr = listener.local_addr().expect("listener addr");
    let base_url = format!("http://{addr}");
    let host = base_url
        .split("://")
        .nth(1)
        .expect("base url host")
        .to_string();

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone())
        .expect("open fed store");
    let blobs = Arc::new(FsBlobStore::new(dir.join("blobs")).expect("open blob store"));
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
        blobs: blobs.clone(),
        notifications: store.clone(),
        auth: store.clone(),
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
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            // Generous ceiling (mirrors `test_client`): dev-profile RSA
            // keygen + parallel test binaries can stall loopback responses
            // for seconds; production defaults stay tight.
            timeout: Duration::from_secs(300),
            total_timeout: Duration::from_secs(300),
            ..Default::default()
        }),
        public_url: base_url.clone(),
        max_file_size: 10_000_000,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs.clone(),
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: pinetree::config::RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    });
    let worker = Arc::new(JobWorker {
        clock: store.clock().clone(),
        ids: store.ids().clone(),
        jobs: store.clone(),
        notifications: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 30_000,
        base_backoff_ms: 50,
        transcode_config: TranscodeConfig::default(),
        email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
    });
    let rate_limiter = Arc::new(RateLimiter::with_misskey_defaults(clock.clone()));
    // Warm the instance actor keypair now: pure-Rust RSA-2048 keygen takes
    // ~9 s in the dev/test profile, and any first signed fetch inside a
    // request handler would blow the HTTP client's read timeout.
    services.ensure_system_actor().expect("system actor");
    let state = ApiState {
        services: services.clone(),
        worker: worker.clone(),
        rate_limiter,
        upload_slots: Arc::new(UploadSlots::with_defaults()),
        hub: pinetree::streaming::hub::Hub::new(1024),
        trusted_proxies: Arc::new(Vec::new()),
        featured_cache: std::sync::Arc::new(
            pinetree::application::commands::pins::FeaturedCache::default(),
        ),
    };
    let app = federation_router(state.clone());
    let serve_handle = rt.spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    let node = FedNode {
        tag,
        services,
        worker,
        state,
        base_url,
        host,
        addr,
        serve_handle: Some(serve_handle),
        runtime: Some(rt.clone()),
    };
    let client = test_client();
    let base = node.base_url.clone();
    rt.block_on(wait_ready(&client, &base, node.tag));
    node
}

impl FedNode {
    pub(crate) fn stop_server(&mut self) {
        if let Some(handle) = self.serve_handle.take() {
            handle.abort();
        }
    }

    /// Release the node: abort the server task and shut the dedicated
    /// runtime down without blocking (dropping a runtime from async
    /// context panics, so this must be explicit).
    fn shutdown(&mut self) {
        self.stop_server();
        if let Some(rt) = self.runtime.take() {
            if let Ok(rt) = Arc::try_unwrap(rt) {
                rt.shutdown_background();
            }
            // Else: another owner exists (should not happen in tests);
            // dropping our Arc is still safe, the runtime lives on.
        }
    }
}

impl Drop for FedNode {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl FedNode {
    pub(crate) fn start_server(&mut self) {
        // Same raw-thread discipline as `boot_node`: `block_on` must not
        // run on the test runtime's workers.
        let rt = self.runtime.clone().expect("node runtime");
        let state = self.state.clone();
        let addr = self.addr;
        let tag = self.tag;
        let base = self.base_url.clone();
        let serve_handle = std::thread::Builder::new()
            .name(format!("fed-rebind-{tag}"))
            .spawn(move || {
                let start = Instant::now();
                let listener = loop {
                    match rt.block_on(tokio::net::TcpListener::bind(addr)) {
                        Ok(l) => break l,
                        Err(_) => {
                            assert!(
                                start.elapsed() < Duration::from_secs(10),
                                "{tag} could not rebind {addr}"
                            );
                            std::thread::sleep(Duration::from_millis(100));
                        }
                    }
                };
                let app = federation_router(state);
                let serve_handle = rt.spawn(async move {
                    let _ = axum::serve(
                        listener,
                        app.into_make_service_with_connect_info::<SocketAddr>(),
                    )
                    .await;
                });
                let client = test_client();
                rt.block_on(wait_ready(&client, &base, tag));
                serve_handle
            })
            .expect("spawn fed rebind thread")
            .join()
            .expect("fed rebind thread");
        self.serve_handle = Some(serve_handle);
    }
}

pub(crate) fn test_client() -> SafeHttpClient {
    SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: true,
        // Generous ceiling: dev-profile RSA keygen on the peer can stall a
        // response for several seconds on first signed exchange.
        timeout: Duration::from_secs(60),
        ..Default::default()
    })
}

pub(crate) fn api_post(
    client: &SafeHttpClient,
    url: &str,
    token: Option<&str>,
    body: &Value,
) -> SafeHttpResponse {
    let bytes = serde_json::to_vec(body).expect("encode api body");
    let bearer = token.map(|t| format!("Bearer {t}"));
    let mut headers: Vec<(&str, &str)> = vec![
        ("Content-Type", "application/json"),
        ("Accept", "application/json"),
    ];
    if let Some(b) = bearer.as_ref() {
        headers.push(("Authorization", b.as_str()));
    }
    client
        .send("POST", url, &headers, Some(&bytes), 1024 * 1024)
        .unwrap_or_else(|e| panic!("POST {url} failed: {e}"))
}

pub(crate) fn api_json(resp: &SafeHttpResponse, what: &str) -> Value {
    assert!(
        (200..300).contains(&resp.status),
        "{what}: HTTP {} body {}",
        resp.status,
        String::from_utf8_lossy(&resp.body)
    );
    serde_json::from_slice(&resp.body).expect("api json body")
}

pub(crate) fn http_get(client: &SafeHttpClient, url: &str, accept: &str) -> SafeHttpResponse {
    client
        .send("GET", url, &[("Accept", accept)], None, MAX_AP_JSON_BYTES)
        .unwrap_or_else(|e| panic!("GET {url} failed: {e}"))
}

/// Drain both workers until quiescent (bounded rounds; transient failures
/// reschedule with backoff and are picked up by later `pump_until` ticks).
pub(crate) fn drain_all(a: &FedNode, b: &FedNode) {
    for _ in 0..30 {
        let da = a.worker.drain(200).expect("drain a");
        let db = b.worker.drain(200).expect("drain b");
        if da == 0 && db == 0 {
            break;
        }
    }
}

/// Drain both workers until `cond` holds (deadline) — delivery chains
/// (Follow → Accept, Create → side effects) need several drain rounds.
pub(crate) async fn pump_until(
    a: &FedNode,
    b: &FedNode,
    desc: &str,
    deadline: Duration,
    cond: &mut impl FnMut() -> bool,
) {
    let start = Instant::now();
    loop {
        drain_all(a, b);
        if cond() {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "timed out waiting for federation: {desc}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `cond` without draining (inbox handling is synchronous, so state
/// committed by a POST is visible as soon as the POST returns).
pub(crate) async fn poll_until(desc: &str, deadline: Duration, cond: &mut impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < deadline, "timed out waiting for: {desc}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// POST a signed ActivityPub activity from `sender` to an inbox URL.
/// `date_ms_override` allows crafting skewed-`Date` requests.
pub(crate) fn signed_post_to_inbox(
    client: &SafeHttpClient,
    sender: &Arc<AppServices>,
    sender_id: &str,
    inbox_url: &str,
    body: &[u8],
    date_ms_override: Option<i64>,
) -> SafeHttpResponse {
    let keypair = sender
        .ensure_actor_keypair(sender_id)
        .expect("sender keypair");
    let base = sender.public_url.trim_end_matches('/').to_string();
    let key_id = format!("{base}/users/{sender_id}#main-key");
    let parsed = parse_http_url(inbox_url).expect("parse inbox url");
    let now_ms = date_ms_override.unwrap_or_else(|| sender.clock.now_ms());
    let date = format_http_date(now_ms);
    let sig = sign_request_headers(
        "POST",
        &parsed.path_and_query,
        &parsed.host_header,
        &date,
        Some(body),
        &key_id,
        &keypair.private_key_pem,
    )
    .expect("sign activity");
    let digest = sig.digest.clone().expect("digest header");
    let headers = [
        ("Content-Type", "application/activity+json"),
        ("Accept", "application/activity+json"),
        ("Date", sig.date.as_str()),
        ("Digest", digest.as_str()),
        ("Signature", sig.signature.as_str()),
    ];
    client
        .send("POST", inbox_url, &headers, Some(body), MAX_AP_JSON_BYTES)
        .unwrap_or_else(|e| panic!("signed POST {inbox_url} failed: {e}"))
}

pub(crate) fn note_uri(node: &FedNode, note: &Note) -> String {
    format!("{}/notes/{}", node.base_url, note.id)
}

/// Inline loopback PNG server (mirrors the `remote_cache` unit-test style).
/// Serves `png` at `/pic.png` (404 elsewhere) for up to `max_hits` requests.
pub(crate) fn spawn_png_server(png: Vec<u8>, max_hits: usize) -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind png server");
    let addr = listener.local_addr().expect("png addr");
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        for _ in 0..max_hits {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
            let mut buf = [0u8; 2048];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            if req.starts_with("GET /pic.png") {
                let hdr = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    png.len()
                );
                let _ = stream.write_all(hdr.as_bytes());
                let _ = stream.write_all(&png);
            } else {
                let _ = stream.write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope",
                );
            }
        }
    });
    addr
}

/// Signed ActivityPub GET from `sender_id` (mirrors `signed_post_to_inbox`).
pub(crate) fn signed_get_to_url(
    client: &SafeHttpClient,
    sender: &Arc<AppServices>,
    sender_id: &str,
    url: &str,
    accept: &str,
) -> SafeHttpResponse {
    let keypair = sender
        .ensure_actor_keypair(sender_id)
        .expect("sender keypair");
    let base = sender.public_url.trim_end_matches('/').to_string();
    let key_id = format!("{base}/users/{sender_id}#main-key");
    let parsed = parse_http_url(url).expect("parse get url");
    let date = format_http_date(sender.clock.now_ms());
    let sig = sign_request_headers(
        "GET",
        &parsed.path_and_query,
        &parsed.host_header,
        &date,
        None,
        &key_id,
        &keypair.private_key_pem,
    )
    .expect("sign get");
    let headers = [
        ("Host", parsed.host_header.as_str()),
        ("Date", sig.date.as_str()),
        ("Signature", sig.signature.as_str()),
        ("Accept", accept),
    ];
    client
        .send("GET", url, &headers, None, MAX_AP_JSON_BYTES)
        .unwrap_or_else(|e| panic!("signed GET {url} failed: {e}"))
}

pub(crate) fn clip_items(resp: &SafeHttpResponse, what: &str) -> Vec<String> {
    clip_items_signed(resp, what, None)
}

/// `clip_items` that re-signs the `first`-page hop as `signer` (a real
/// client signs every page fetch; an unsigned hop degrades to the
/// anonymous view — which is exactly what the gate asserts elsewhere).
pub(crate) fn clip_items_signed(
    resp: &SafeHttpResponse,
    what: &str,
    signer: Option<(&Arc<AppServices>, &str)>,
) -> Vec<String> {
    assert!(
        (200..300).contains(&resp.status),
        "{what}: HTTP {} body {}",
        resp.status,
        String::from_utf8_lossy(&resp.body)
    );
    let doc: Value = serde_json::from_slice(&resp.body).expect("collection json");
    if let Some(items) = doc.get("orderedItems").and_then(|v| v.as_array()) {
        return items
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
    }
    // Index form: follow `first` (with the signer's signature when given).
    let first = doc
        .get("first")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("{what}: no orderedItems/first in {doc}"))
        .to_string();
    first_to_items(&first, what, signer)
}

pub(crate) fn first_to_items(
    first: &str,
    what: &str,
    signer: Option<(&Arc<AppServices>, &str)>,
) -> Vec<String> {
    let client = test_client();
    let resp = match signer {
        Some((services, id)) => {
            signed_get_to_url(&client, services, id, first, "application/activity+json")
        }
        None => http_get(&client, first, "application/activity+json"),
    };
    assert!(
        (200..300).contains(&resp.status),
        "{what}: page HTTP {} body {}",
        resp.status,
        String::from_utf8_lossy(&resp.body)
    );
    let doc: Value = serde_json::from_slice(&resp.body).expect("page json");
    doc.get("orderedItems")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_else(|| panic!("{what}: no orderedItems in {doc}"))
}
