//! Phase 2 Deliverable 3: end-to-end federation between two live `pinetree`
//! HTTP servers (`pinetree-a`, `pinetree-b`) on loopback.
//!
//! Harness choice: in-process `axum::serve` on `127.0.0.1:0` (no subprocess
//! precedent exists for live servers in cargo tests). Each node gets its own
//! `RedbStore` temp dir (`pinetree-fed-{a|b}-{pid}-{n}`), `FsBlobStore`,
//! `SystemClock` (real time, so HTTP `Date` skew checks pass), default
//! `AidxGenerator` ids, `SafeHttpClient` with `allow_private_ips: true`
//! (required for loopback federation), a `JobWorker` (50 ms base backoff so
//! retry phases settle fast), and `RateLimiter::with_misskey_defaults`.
//!
//! The federation route handlers (`webfinger`, `actor`, `notes`, `inbox`)
//! exist in `src/` but are not yet wired into `build_router` (done
//! concurrently), so this file wires them onto the router itself. If route
//! wiring later lands in `src/`, these explicit routes keep working
//! unchanged. Happy-path federation goes through the Misskey HTTP API
//! (`signup`, `users/show` remote resolution, `following/create|delete`,
//! `notes/create`) and `AppServices` + worker drains; raw signed POSTs are
//! used only for auth-negative and dedup-replay tests.
//!
//! Timing: `#[tokio::test(flavor = "multi_thread", worker_threads = 16)]`
//! because `SafeHttpClient::send` blocks on I/O while the servers live on
//! the same runtime. All
//! cross-node assertions use poll-until-deadline helpers, never
//! fixed sleeps (except the short backoff wait in the retry phase).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, HeaderValue};
use axum::routing::{get, post};
use axum::Router;
use serde_json::{Value, json};

use pinetree::activitypub::{actor, objects, webfinger};
use pinetree::activitypub::objects::{
    ParsedAttachment, render_create_or_announce_for_note, render_follow_activity,
};
use pinetree::activitypub::signatures::{
    compute_digest_header, sign_request_headers, verify_http_signature,
};
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, AuthStore, BlobStore, Clock, CreateNoteInput, FollowStore,
    JobQueue, MediaStore, NoopStreamingSink, NoteStore, ReactionStore, TimelineCursor,
    UpdateAccountPatch,
};
use pinetree::domain::id::SystemClock;
use pinetree::domain::note::{Note, Visibility};
use pinetree::federation::delivery::{
    MAX_AP_JSON_BYTES, SafeHttpClient, SafeHttpConfig, SafeHttpResponse,
    fetch_and_upsert_remote_actor, format_http_date, parse_http_url,
};
use pinetree::federation::inbox::{inbox_handler, user_inbox_handler};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::remote_cache::fetch_and_cache_remote_media;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

static NODE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One live federation node: services, worker, router state, and the
/// public `http://127.0.0.1:{port}` base URL (which doubles as `public_url`).
struct FedNode {
    tag: &'static str,
    services: Arc<AppServices>,
    worker: Arc<JobWorker>,
    state: ApiState,
    base_url: String,
    host: String,
    addr: SocketAddr,
    serve_handle: Option<tokio::task::JoinHandle<()>>,
    // Dedicated runtime per node (instead of the shared test runtime):
    // production nodes never share a runtime, and sharing one makes a
    // blocking network call in any node's handler able to starve the
    // others' scheduling. Holding the runtime keeps it alive.
    #[allow(dead_code)]
    runtime: Option<Arc<tokio::runtime::Runtime>>,
}

/// Federation routes (now wired in `build_router` in `src/`).
fn federation_router(state: ApiState) -> Router {
    build_router(state)
}

async fn wait_ready(client: &SafeHttpClient, base_url: &str, tag: &str) {
    let start = Instant::now();
    loop {
        match client.send(
            "GET",
            &format!("{base_url}/healthz"),
            &[],
            None,
            1024,
        ) {
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

fn boot_node(tag: &'static str) -> FedNode {
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
        blobs,
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
        featured_cache: std::sync::Arc::new(pinetree::application::commands::pins::FeaturedCache::default()),
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
    fn stop_server(&mut self) {
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
    fn start_server(&mut self) {
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

fn test_client() -> SafeHttpClient {
    SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: true,
        // Generous ceiling: dev-profile RSA keygen on the peer can stall a
        // response for several seconds on first signed exchange.
        timeout: Duration::from_secs(60),
        ..Default::default()
    })
}

fn api_post(
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

fn api_json(resp: &SafeHttpResponse, what: &str) -> Value {
    assert!(
        (200..300).contains(&resp.status),
        "{what}: HTTP {} body {}",
        resp.status,
        String::from_utf8_lossy(&resp.body)
    );
    serde_json::from_slice(&resp.body).expect("api json body")
}

fn http_get(client: &SafeHttpClient, url: &str, accept: &str) -> SafeHttpResponse {
    client
        .send("GET", url, &[("Accept", accept)], None, MAX_AP_JSON_BYTES)
        .unwrap_or_else(|e| panic!("GET {url} failed: {e}"))
}

/// Drain both workers until quiescent (bounded rounds; transient failures
/// reschedule with backoff and are picked up by later `pump_until` ticks).
fn drain_all(a: &FedNode, b: &FedNode) {
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
async fn pump_until(
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
async fn poll_until(desc: &str, deadline: Duration, cond: &mut impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < deadline,
            "timed out waiting for: {desc}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// POST a signed ActivityPub activity from `sender` to an inbox URL.
/// `date_ms_override` allows crafting skewed-`Date` requests.
fn signed_post_to_inbox(
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

fn note_uri(node: &FedNode, note: &Note) -> String {
    format!("{}/notes/{}", node.base_url, note.id)
}

/// Inline loopback PNG server (mirrors the `remote_cache` unit-test style).
/// Serves `png` at `/pic.png` (404 elsewhere) for up to `max_hits` requests.
fn spawn_png_server(png: Vec<u8>, max_hits: usize) -> SocketAddr {
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

// ---------------------------------------------------------------------------
// Test 1: full two-node matrix (one shared pair keeps RSA-2048 key
// generation — pure Rust, seconds per key — to ~4 keys total).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn federation_two_node_end_to_end() {
    let a = boot_node("a");
    let b = boot_node("b");
    let client = test_client();
    let phase_deadline = Duration::from_secs(25);

    // --- Misskey signup on both nodes -------------------------------------
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", a.base_url),
            None,
            &json!({"username": "alice", "password": "s3cret-pw-a"}),
        ),
        "A signup alice",
    );
    let alice_id = v["id"].as_str().expect("alice id").to_string();
    let alice_token = v["token"].as_str().expect("alice token").to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", b.base_url),
            None,
            &json!({"username": "bob", "password": "s3cret-pw-b"}),
        ),
        "B signup bob",
    );
    let bob_id = v["id"].as_str().expect("bob id").to_string();
    let bob_token = v["token"].as_str().expect("bob token").to_string();

    let alice_uri = format!("{}/users/{}", a.base_url, alice_id);
    let bob_uri = format!("{}/users/{}", b.base_url, bob_id);

    // Warm user keypairs now (same ~9 s dev-profile keygen cost as above);
    // delivery signs with these inside worker drains.
    a.services.ensure_actor_keypair(&alice_id).expect("alice key");
    b.services.ensure_actor_keypair(&bob_id).expect("bob key");

    // --- 1. webfinger / host-meta / nodeinfo -------------------------------
    for resource in [
        format!("acct:alice@{}", a.host),
        format!("alice@{}", a.host),
        format!("{}/users/{}", a.base_url, alice_id),
        format!("{}/@alice", a.base_url),
    ] {
        let url = format!("{}/.well-known/webfinger?resource={resource}", a.base_url);
        let resp = http_get(&client, &url, "application/jrd+json");
        assert_eq!(resp.status, 200, "webfinger {resource}");
        let jrd: Value = serde_json::from_slice(&resp.body).expect("jrd json");
        let self_href = jrd["links"]
            .as_array()
            .expect("jrd links")
            .iter()
            .find(|l| l["rel"] == "self")
            .and_then(|l| l["href"].as_str())
            .expect("self href");
        assert_eq!(self_href, alice_uri, "webfinger {resource}");
    }
    let hm = http_get(&client, &format!("{}/.well-known/host-meta", a.base_url), "*/*");
    assert_eq!(hm.status, 200);
    assert!(
        String::from_utf8_lossy(&hm.body).contains(".well-known/webfinger?resource={uri}"),
        "host-meta carries the webfinger template"
    );
    let hmj = http_get(
        &client,
        &format!("{}/.well-known/host-meta.json", a.base_url),
        "application/json",
    );
    assert_eq!(hmj.status, 200);
    let hmj_doc: Value = serde_json::from_slice(&hmj.body).expect("host-meta.json");
    assert_eq!(
        hmj_doc["links"][0]["template"],
        format!("{}/.well-known/webfinger?resource={{uri}}", a.base_url)
    );
    let wkn = http_get(
        &client,
        &format!("{}/.well-known/nodeinfo", a.base_url),
        "application/json",
    );
    assert_eq!(wkn.status, 200);
    let wkn_doc: Value = serde_json::from_slice(&wkn.body).expect("nodeinfo links");
    let rels: Vec<&str> = wkn_doc["links"]
        .as_array()
        .expect("nodeinfo links array")
        .iter()
        .filter_map(|l| l["rel"].as_str())
        .collect();
    assert!(rels.contains(&"http://nodeinfo.diaspora.software/ns/schema/2.0"));
    assert!(rels.contains(&"http://nodeinfo.diaspora.software/ns/schema/2.1"));
    for version in ["2.0", "2.1"] {
        let ni = http_get(
            &client,
            &format!("{}/nodeinfo/{version}", a.base_url),
            "application/json",
        );
        assert_eq!(ni.status, 200, "nodeinfo {version}");
        let doc: Value = serde_json::from_slice(&ni.body).expect("nodeinfo doc");
        assert_eq!(doc["version"], version);
        assert_eq!(doc["protocols"], json!(["activitypub"]));
    }

    // --- actor documents + key rotation + signature unit checks ------------
    let actor_resp = http_get(&client, &alice_uri, "application/activity+json");
    assert_eq!(actor_resp.status, 200);
    let actor_doc: Value = serde_json::from_slice(&actor_resp.body).expect("actor json");
    assert_eq!(actor_doc["id"], alice_uri);
    assert!(
        actor_doc["publicKey"]["id"]
            .as_str()
            .expect("key id")
            .ends_with("#main-key")
    );
    assert!(
        actor_doc["publicKey"]["publicKeyPem"]
            .as_str()
            .expect("pem")
            .contains("BEGIN PUBLIC KEY")
    );
    let pk = http_get(
        &client,
        &format!("{alice_uri}/publickey"),
        "application/activity+json",
    );
    assert_eq!(pk.status, 200);
    let pk_doc: Value = serde_json::from_slice(&pk.body).expect("publickey json");
    assert_eq!(pk_doc["id"], format!("{alice_uri}#main-key"));
    for coll in ["outbox", "followers", "following", "collections/featured"] {
        let r = http_get(
            &client,
            &format!("{alice_uri}/{coll}"),
            "application/activity+json",
        );
        assert_eq!(r.status, 200, "actor {coll}");
        let doc: Value = serde_json::from_slice(&r.body).expect("collection json");
        assert_eq!(doc["type"], "OrderedCollection", "actor {coll}");
    }
    let at = http_get(
        &client,
        &format!("{}/@alice", a.base_url),
        "application/activity+json",
    );
    assert_eq!(at.status, 200);
    let at_doc: Value = serde_json::from_slice(&at.body).expect("@alice json");
    assert_eq!(at_doc["id"], alice_uri);

    // Key rotation changes the served PEM.
    let kp_before = a
        .services
        .ensure_actor_keypair(&alice_id)
        .expect("ensure keypair");
    let kp_after = a
        .services
        .rotate_actor_keypair(&alice_id)
        .expect("rotate keypair");
    assert_ne!(kp_before.public_key_pem, kp_after.public_key_pem);
    let stored = a
        .services
        .auth
        .get_actor_keypair(&alice_id)
        .expect("load keypair")
        .expect("stored keypair");
    assert_eq!(stored.public_key_pem, kp_after.public_key_pem);
    let actor_resp = http_get(&client, &alice_uri, "application/activity+json");
    let actor_doc: Value = serde_json::from_slice(&actor_resp.body).expect("actor json");
    assert_eq!(
        actor_doc["publicKey"]["publicKeyPem"], kp_after.public_key_pem,
        "rotated key is served"
    );

    // Sign/verify round-trip at the HTTP-signature layer (unit-level).
    let now_ms = a.services.clock.now_ms();
    let key_id = format!("{alice_uri}#main-key");
    let probe_body = br#"{"type":"Probe"}"#;
    let sig = sign_request_headers(
        "POST",
        "/inbox",
        "127.0.0.1:9",
        &format_http_date(now_ms),
        Some(probe_body),
        &key_id,
        &kp_after.private_key_pem,
    )
    .expect("sign probe");
    let digest = sig.digest.clone().expect("digest");
    assert_eq!(compute_digest_header(probe_body), digest);
    let mut hm_headers = HeaderMap::new();
    hm_headers.insert("host", HeaderValue::from_str("127.0.0.1:9").unwrap());
    hm_headers.insert("date", HeaderValue::from_str(&sig.date).unwrap());
    hm_headers.insert("digest", HeaderValue::from_str(&digest).unwrap());
    hm_headers.insert("signature", HeaderValue::from_str(&sig.signature).unwrap());
    let parsed = verify_http_signature(
        "POST",
        "/inbox",
        &hm_headers,
        Some(probe_body.as_slice()),
        &kp_after.public_key_pem,
        Some(now_ms),
        300,
    )
    .expect("verify probe");
    assert_eq!(parsed.key_id, key_id);

    // SSRF: strict client rejects loopback before any I/O.
    let strict = SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: false,
        ..Default::default()
    });
    let ssrf_err = strict
        .send("GET", "http://127.0.0.1:9/.well-known/webfinger", &[], None, 1024)
        .unwrap_err();
    assert!(ssrf_err.to_string().contains("SSRF"), "unexpected: {ssrf_err}");

    // --- inbox auth negatives (raw signed POSTs to B) ----------------------
    let b_inbox = format!("{}/inbox", b.base_url);
    let follow_probe =
        render_follow_activity(&a.base_url, "probe-f1", &alice_uri, &bob_uri);
    let follow_bytes = serde_json::to_vec(&follow_probe).expect("follow bytes");
    let b_followers = || {
        b.services
            .follows
            .followers_of(&bob_id, None, 1000)
            .expect("followers")
            .len()
    };
    let followers_before = b_followers();
    // Tampered body (digest mismatch): sign the original bytes, then send
    // altered bytes under the same signature.
    let tamper_key = a
        .services
        .ensure_actor_keypair(&alice_id)
        .expect("alice key");
    let tamper_parsed = parse_http_url(&b_inbox).expect("inbox url");
    let tamper_sig = sign_request_headers(
        "POST",
        &tamper_parsed.path_and_query,
        &tamper_parsed.host_header,
        &format_http_date(a.services.clock.now_ms()),
        Some(&follow_bytes),
        &format!("{alice_uri}#main-key"),
        &tamper_key.private_key_pem,
    )
    .expect("sign");
    let tamper_digest = tamper_sig.digest.clone().expect("digest");
    let mut tampered = follow_bytes.clone();
    tampered.extend_from_slice(b" ");
    let r = client
        .send(
            "POST",
            &b_inbox,
            &[
                ("Content-Type", "application/activity+json"),
                ("Date", tamper_sig.date.as_str()),
                ("Digest", tamper_digest.as_str()),
                ("Signature", tamper_sig.signature.as_str()),
            ],
            Some(&tampered),
            MAX_AP_JSON_BYTES,
        )
        .expect("tamper POST");
    assert_eq!(r.status, 401, "tampered body must be rejected");
    // Bad signature (corrupted signature parameter).
    let kp = a
        .services
        .ensure_actor_keypair(&alice_id)
        .expect("alice key");
    let parsed_url = parse_http_url(&b_inbox).expect("inbox url");
    let good = sign_request_headers(
        "POST",
        &parsed_url.path_and_query,
        &parsed_url.host_header,
        &format_http_date(a.services.clock.now_ms()),
        Some(&follow_bytes),
        &format!("{alice_uri}#main-key"),
        &kp.private_key_pem,
    )
    .expect("sign");
    let bad_sig = good.signature.replacen("signature=\"", "signature=\"X", 1);
    let digest = good.digest.clone().expect("digest");
    let r = client
        .send(
            "POST",
            &b_inbox,
            &[
                ("Content-Type", "application/activity+json"),
                ("Date", good.date.as_str()),
                ("Digest", digest.as_str()),
                ("Signature", bad_sig.as_str()),
            ],
            Some(&follow_bytes),
            MAX_AP_JSON_BYTES,
        )
        .expect("bad-sig POST");
    assert_eq!(r.status, 401, "bad signature must be rejected");
    // Skewed Date (>300 s).
    let skewed = a.services.clock.now_ms() - 600_000;
    let r = signed_post_to_inbox(
        &client,
        &a.services,
        &alice_id,
        &b_inbox,
        &follow_bytes,
        Some(skewed),
    );
    assert_eq!(r.status, 401, "skewed Date must be rejected");
    // Oversize body (>64 KiB).
    let big = vec![b'{'; 66_000];
    let r = signed_post_to_inbox(&client, &a.services, &alice_id, &b_inbox, &big, None);
    assert_eq!(r.status, 413, "oversize body must be rejected");
    assert_eq!(b_followers(), followers_before, "negatives change no state");

    // --- 3. follow both directions over the Misskey API ---------------------
    // Remote resolution via `users/show {username, host}` (WebFinger-backed).
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/users/show", a.base_url),
            Some(&alice_token),
            &json!({"username": "bob", "host": b.host}),
        ),
        "A resolves bob",
    );
    let bob_on_a = v["id"].as_str().expect("remote bob id").to_string();
    let r = api_post(
        &client,
        &format!("{}/api/following/create", a.base_url),
        Some(&alice_token),
        &json!({"userId": bob_on_a}),
    );
    assert!((200..300).contains(&r.status), "A follow bob: {}", r.status);
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/users/show", b.base_url),
            Some(&bob_token),
            &json!({"username": "alice", "host": a.host}),
        ),
        "B resolves alice",
    );
    let alice_on_b = v["id"].as_str().expect("remote alice id").to_string();
    let r = api_post(
        &client,
        &format!("{}/api/following/create", b.base_url),
        Some(&bob_token),
        &json!({"userId": alice_on_b}),
    );
    assert!((200..300).contains(&r.status), "B follow alice: {}", r.status);

    pump_until(&a, &b, "B sees alice as follower", phase_deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup alice")
            .map(|acc| {
                b.services
                    .follows
                    .is_following(&acc.id, &bob_id)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    })
    .await;
    pump_until(&a, &b, "A sees bob as follower", phase_deadline, &mut || {
        a.services
            .accounts
            .find_by_uri(&bob_uri)
            .expect("lookup bob")
            .map(|acc| {
                a.services
                    .follows
                    .is_following(&acc.id, &alice_id)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    })
    .await;
    assert!(
        a.services
            .follows
            .is_following(&alice_id, &bob_on_a)
            .expect("A edge")
    );
    assert!(
        b.services
            .follows
            .is_following(&bob_id, &alice_on_b)
            .expect("B edge")
    );
    // Direct resolver round-trip: the remote actor refetches to the same id.
    let (refetched, _) =
        fetch_and_upsert_remote_actor(&a.services, &a.services.federation_http, &bob_uri)
            .expect("refetch bob");
    assert_eq!(refetched.id, bob_on_a);

    // --- 4. note visibility matrix A -> B ------------------------------------
    // One note via the Misskey HTTP API, the rest at AppServices level.
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/notes/create", a.base_url),
            Some(&alice_token),
            &json!({"text": "matrix http public #fediverse", "visibility": "public"}),
        ),
        "HTTP notes/create",
    );
    let http_note_id = v["createdNote"]["id"]
        .as_str()
        .expect("created note id")
        .to_string();
    let mut matrix: Vec<(Note, Visibility)> = Vec::new();
    let http_note = a
        .services
        .notes
        .get_note(&http_note_id)
        .expect("get http note")
        .expect("http note stored");
    matrix.push((http_note, Visibility::Public));
    let mk = |text: &str, visibility: Visibility, cw: Option<&str>| {
        let mut input = CreateNoteInput::simple_public(&alice_id, text);
        input.visibility = visibility;
        input.cw = cw.map(str::to_string);
        a.services.create_note(input).expect("matrix note")
    };
    let home = mk("matrix home", Visibility::Home, None);
    let followers = mk("matrix followers", Visibility::Followers, None);
    let mut spec_input = CreateNoteInput::simple_public(&alice_id, "matrix specified");
    spec_input.visibility = Visibility::Specified;
    spec_input.visible_users = vec![bob_on_a.clone()];
    let specified = a.services.create_note(spec_input).expect("specified note");
    let cw = mk("matrix cw body", Visibility::Public, Some("matrix-cw"));
    let mention = mk(
        &format!("@bob@{} matrix mention #fediverse", b.host),
        Visibility::Public,
        None,
    );
    assert!(
        mention.mentions.contains(&bob_on_a),
        "A resolves the remote @bob mention to the cached account"
    );
    matrix.push((home, Visibility::Home));
    matrix.push((followers, Visibility::Followers));
    matrix.push((specified, Visibility::Specified));
    matrix.push((cw, Visibility::Public));
    matrix.push((mention, Visibility::Public));
    // Reply last so its parent is already cached on B at ingest time.
    let parent_id = matrix[0].0.id.clone();
    let mut reply_input = CreateNoteInput::simple_public(&alice_id, "matrix reply");
    reply_input.reply_to = Some(parent_id);
    let reply = a.services.create_note(reply_input).expect("reply note");
    matrix.push((reply, Visibility::Public));

    for (note, visibility) in &matrix {
        let uri = note_uri(&a, note);
        let uri_clone = uri.clone();
        pump_until(&a, &b, &format!("B caches {}", note.id), phase_deadline, &mut || {
            b.services.notes.find_by_uri(&uri_clone).expect("find").is_some()
        })
        .await;
    }
    for (note, visibility) in &matrix {
        let uri = note_uri(&a, note);
        let cached = b
            .services
            .notes
            .find_by_uri(&uri)
            .expect("find cached")
            .unwrap_or_else(|| panic!("B cached {}", note.id));
        assert_eq!(cached.visibility, *visibility, "visibility for {}", note.id);
        assert_eq!(cached.text, note.text, "text for {}", note.id);
        assert_eq!(cached.cw, note.cw, "cw for {}", note.id);
        assert!(!cached.tags.iter().any(|t| t.is_empty()));
    }
    // Spot-checks: tags, reply linkage, remote mention, specified audience.
    let http_cached = b
        .services
        .notes
        .find_by_uri(&note_uri(&a, &matrix[0].0))
        .expect("find")
        .expect("http note cached");
    assert!(http_cached.tags.contains(&"fediverse".to_string()));
    let parent_cached_id = http_cached.id.clone();
    let reply_cached = b
        .services
        .notes
        .find_by_uri(&note_uri(&a, &matrix[6].0))
        .expect("find")
        .expect("reply cached");
    assert_eq!(reply_cached.reply_id.as_deref(), Some(parent_cached_id.as_str()));
    let mention_cached = b
        .services
        .notes
        .find_by_uri(&note_uri(&a, &matrix[5].0))
        .expect("find")
        .expect("mention cached");
    assert!(
        mention_cached.text.as_deref().unwrap_or("").contains("@bob@"),
        "mention text preserved"
    );
    assert!(!mention_cached.mentions.is_empty(), "mention ids preserved");
    assert!(mention_cached.tags.contains(&"fediverse".to_string()));
    let spec_cached = b
        .services
        .notes
        .find_by_uri(&note_uri(&a, &matrix[3].0))
        .expect("find")
        .expect("specified cached");
    assert!(!spec_cached.visible_user_ids.is_empty());
    let cw_cached = b
        .services
        .notes
        .find_by_uri(&note_uri(&a, &matrix[4].0))
        .expect("find")
        .expect("cw cached");
    assert_eq!(cw_cached.cw.as_deref(), Some("matrix-cw"));

    // Served Note JSON carries no `_misskey_content`.
    let note_doc = http_get(
        &client,
        &format!("{}/notes/{}", a.base_url, http_note_id),
        "application/activity+json",
    );
    assert_eq!(note_doc.status, 200);
    let note_body = String::from_utf8_lossy(&note_doc.body).to_string();
    assert!(!note_body.contains("_misskey_content"));
    assert!(note_body.contains("\"Note\""));
    let note_act = http_get(
        &client,
        &format!("{}/notes/{}/activity", a.base_url, http_note_id),
        "application/activity+json",
    );
    assert_eq!(note_act.status, 200);
    let act_doc: Value = serde_json::from_slice(&note_act.body).expect("activity json");
    assert_eq!(act_doc["type"], "Create");

    // --- 6a. announce (renote) + undo, like + unlike --------------------------
    // B posts the target; alice already follows bob so it fans out to A.
    let b_target = b
        .services
        .create_note(CreateNoteInput::simple_public(&bob_id, "renote and like target"))
        .expect("b target");
    let b_target_uri = note_uri(&b, &b_target);
    let b_target_uri_clone = b_target_uri.clone();
    pump_until(&a, &b, "A caches B target", phase_deadline, &mut || {
        a.services
            .notes
            .find_by_uri(&b_target_uri_clone)
            .expect("find")
            .is_some()
    })
    .await;
    let a_copy_id = a
        .services
        .notes
        .find_by_uri(&b_target_uri)
        .expect("find")
        .expect("A copy")
        .id
        .clone();

    let mut renote_input = CreateNoteInput::simple_public(&alice_id, "placeholder");
    renote_input.text = None;
    renote_input.renote_of = Some(a_copy_id.clone());
    let renote = a.services.create_note(renote_input).expect("renote");
    pump_until(&a, &b, "B sees the Announce renote", phase_deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup")
            .map(|acc| {
                b.services
                    .notes
                    .notes_by_author(&acc.id, &TimelineCursor::with_limit(1000))
                    .expect("by author")
                    .iter()
                    .any(|n| n.renote_id.as_deref() == Some(b_target.id.as_str()))
            })
            .unwrap_or(false)
    })
    .await;
    a.services
        .delete_note(&alice_id, &renote.id)
        .expect("delete renote");
    pump_until(&a, &b, "Undo(Announce) removes B renote", phase_deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup")
            .map(|acc| {
                b.services
                    .notes
                    .notes_by_author(&acc.id, &TimelineCursor::with_limit(1000))
                    .expect("by author")
                    .iter()
                    .all(|n| n.renote_id.as_deref() != Some(b_target.id.as_str()))
            })
            .unwrap_or(false)
    })
    .await;

    a.services
        .react(&alice_id, &a_copy_id, "👍", false)
        .expect("react");
    pump_until(&a, &b, "Like lands with reaction", phase_deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup")
            .and_then(|acc| {
                b.services
                    .reactions
                    .get_reaction(&acc.id, &b_target.id)
                    .expect("get reaction")
            })
            .map(|r| r.reaction == "👍")
            .unwrap_or(false)
    })
    .await;
    a.services.unreact(&alice_id, &a_copy_id).expect("unreact");
    pump_until(&a, &b, "Undo(Like) removes reaction", phase_deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup")
            .map(|acc| {
                b.services
                    .reactions
                    .get_reaction(&acc.id, &b_target.id)
                    .expect("get reaction")
                    .is_none()
            })
            .unwrap_or(false)
    })
    .await;

    // --- 7a. profile update propagates ---------------------------------------
    a.services
        .update_profile(
            &alice_id,
            UpdateAccountPatch {
                display_name: Some(Some("Alice Updated".to_string())),
                ..Default::default()
            },
        )
        .expect("update profile");
    pump_until(&a, &b, "B caches new display name", phase_deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup")
            .and_then(|acc| acc.display_name)
            == Some("Alice Updated".to_string())
    })
    .await;

    // --- 5b. attachment through Create (inline PNG server) --------------------
    let png: Vec<u8> = vec![
        0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3, 4, 5, 6, 7, 8,
    ];
    let png_addr = spawn_png_server(png.clone(), 8);
    let media_note_uri = format!("{}/notes/media-1", a.base_url);
    let media_activity_id = format!("{}/activities/media-create-1", a.base_url);
    let media_note_obj = json!({
        "id": media_note_uri,
        "type": "Note",
        "attributedTo": alice_uri,
        "content": "media phase note",
        "to": ["https://www.w3.org/ns/activitystreams#Public"],
        "cc": [format!("{alice_uri}/followers")],
        "attachment": [{
            "type": "Document",
            "mediaType": "image/png",
            "url": format!("http://{png_addr}/pic.png"),
            "name": "pic",
            "sensitive": false,
        }],
    });
    let media_activity = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": media_activity_id,
        "type": "Create",
        "actor": alice_uri,
        "object": media_note_obj,
    });
    let media_bytes = serde_json::to_vec(&media_activity).expect("media bytes");
    let r = signed_post_to_inbox(&client, &a.services, &alice_id, &b_inbox, &media_bytes, None);
    assert_eq!(r.status, 202, "media Create accepted");
    poll_until("B caches media note", phase_deadline, &mut || {
        b.services.notes.find_by_uri(&media_note_uri).expect("find").is_some()
    })
    .await;
    // Slice 7 (D-d): the attachment is fetched by the durable
    // `RemoteMediaFetch` worker job, not inline on the inbox request, so
    // drain until the note carries the file.
    pump_until(&a, &b, "B attaches remote media via worker", phase_deadline, &mut || {
        b.services
            .notes
            .find_by_uri(&media_note_uri)
            .expect("find")
            .is_some_and(|n| n.file_ids.len() == 1)
    })
    .await;
    let media_cached = b
        .services
        .notes
        .find_by_uri(&media_note_uri)
        .expect("find")
        .expect("media cached");
    assert_eq!(media_cached.file_ids.len(), 1, "attachment cached through Create");
    let meta = b
        .services
        .media
        .get(&media_cached.file_ids[0])
        .expect("media meta")
        .expect("meta stored");
    assert_eq!(meta.owner_id, None, "remote media is ownerless");
    assert!(meta.is_link, "remote media is a link");
    assert_eq!(
        b.services.blobs.get(&meta.blob_key).expect("blob").expect("blob bytes"),
        png,
        "cached bytes match the source"
    );

    // --- 7b. note delete propagates; A serves a Tombstone --------------------
    let doomed = a
        .services
        .create_note(CreateNoteInput::simple_public(&alice_id, "delete me"))
        .expect("doomed note");
    let doomed_uri = note_uri(&a, &doomed);
    let doomed_uri_clone = doomed_uri.clone();
    pump_until(&a, &b, "B caches doomed note", phase_deadline, &mut || {
        b.services
            .notes
            .find_by_uri(&doomed_uri_clone)
            .expect("find")
            .is_some()
    })
    .await;
    a.services.delete_note(&alice_id, &doomed.id).expect("delete note");
    pump_until(&a, &b, "Delete removes B copy", phase_deadline, &mut || {
        match b.services.notes.find_by_uri(&doomed_uri).expect("find") {
            None => true,
            Some(n) => n.is_deleted,
        }
    })
    .await;
    let tomb = http_get(
        &client,
        &format!("{}/notes/{}", a.base_url, doomed.id),
        "application/activity+json",
    );
    assert_eq!(tomb.status, 410, "deleted note serves Gone");
    assert!(
        String::from_utf8_lossy(&tomb.body).contains("Tombstone"),
        "tombstone document served"
    );

    // --- unfollow (moved after B->A phases so fan-out keeps working) ---------
    let r = api_post(
        &client,
        &format!("{}/api/following/delete", a.base_url),
        Some(&alice_token),
        &json!({"userId": bob_on_a}),
    );
    assert!((200..300).contains(&r.status), "A unfollow: {}", r.status);
    pump_until(&a, &b, "Undo removes B edge", phase_deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup")
            .map(|acc| {
                !b.services
                    .follows
                    .is_following(&acc.id, &bob_id)
                    .unwrap_or(true)
            })
            .unwrap_or(false)
    })
    .await;
    assert!(
        !a.services
            .follows
            .is_following(&alice_id, &bob_on_a)
            .expect("A edge gone")
    );

    // --- 8. dedup replay: same bytes twice, no duplicate state ---------------
    let dedup_follow =
        render_follow_activity(&a.base_url, "dedup-f1", &alice_uri, &bob_uri);
    let dedup_bytes = serde_json::to_vec(&dedup_follow).expect("dedup bytes");
    let r = signed_post_to_inbox(&client, &a.services, &alice_id, &b_inbox, &dedup_bytes, None);
    assert_eq!(r.status, 202, "first Follow accepted");
    let r = signed_post_to_inbox(&client, &a.services, &alice_id, &b_inbox, &dedup_bytes, None);
    assert_eq!(r.status, 202, "replayed Follow accepted");
    poll_until("B has exactly one edge", phase_deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup")
            .map(|acc| {
                b.services
                    .follows
                    .followers_of(&bob_id, None, 1000)
                    .expect("followers")
                    .iter()
                    .filter(|f| f.follower_id == acc.id)
                    .count() == 1
            })
            .unwrap_or(false)
    })
    .await;

    // Same for Create: fresh local note, manual double-POST before any drain.
    let dedup_note = a
        .services
        .create_note(CreateNoteInput::simple_public(&alice_id, "dedup create"))
        .expect("dedup note");
    let dedup_uri = note_uri(&a, &dedup_note);
    let dedup_activity = render_create_or_announce_for_note(&a.services, &dedup_note)
        .expect("render dedup Create");
    let dedup_create_bytes = serde_json::to_vec(&dedup_activity).expect("dedup Create bytes");
    let r = signed_post_to_inbox(
        &client,
        &a.services,
        &alice_id,
        &b_inbox,
        &dedup_create_bytes,
        None,
    );
    assert_eq!(r.status, 202, "first Create accepted");
    let r = signed_post_to_inbox(
        &client,
        &a.services,
        &alice_id,
        &b_inbox,
        &dedup_create_bytes,
        None,
    );
    assert_eq!(r.status, 202, "replayed Create accepted");
    let mut single_copy = || {
        b.services
            .accounts
            .find_by_uri(&alice_uri)
            .expect("lookup")
            .map(|acc| {
                b.services
                    .notes
                    .notes_by_author(&acc.id, &TimelineCursor::with_limit(1000))
                    .expect("by author")
                    .iter()
                    .filter(|n| {
                        n.uri.as_deref() == Some(dedup_uri.as_str())
                    })
                    .count() == 1
            })
            .unwrap_or(false)
    };
    poll_until("B has exactly one copy", phase_deadline, &mut single_copy).await;
    // Settling A's own queued NoteCreated delivery must not duplicate either.
    drain_all(&a, &b);
    assert!(single_copy(), "worker redelivery does not duplicate");

    // --- 9. retry recovery: B down, A commits, B back, exactly-once --------
    let mut b = b;
    b.stop_server();
    let retry_note = a
        .services
        .create_note(CreateNoteInput::simple_public(&alice_id, "retry note"))
        .expect("retry note");
    let retry_uri = note_uri(&a, &retry_note);
    a.worker.drain(200).expect("drain while B down");
    assert!(
        a.services.jobs.pending_count().expect("pending") >= 1,
        "failed delivery stays queued while B is down"
    );
    b.start_server();
    tokio::time::sleep(Duration::from_millis(600)).await; // pass the 50 ms backoff
    let retry_uri_clone = retry_uri.clone();
    pump_until(&a, &b, "retry note delivered", phase_deadline, &mut || {
        b.services
            .notes
            .find_by_uri(&retry_uri_clone)
            .expect("find")
            .is_some()
    })
    .await;
    let copies = b
        .services
        .accounts
        .find_by_uri(&alice_uri)
        .expect("lookup")
        .map(|acc| {
            b.services
                .notes
                .notes_by_author(&acc.id, &TimelineCursor::with_limit(1000))
                .expect("by author")
                .iter()
                .filter(|n| n.uri.as_deref() == Some(retry_uri.as_str()))
                .count()
        })
        .expect("alice cached");
    assert_eq!(copies, 1, "retry delivers exactly once");
}

// ---------------------------------------------------------------------------
// Test 2: remote media fetch/cache at the service layer (no federation
// servers needed — the attachment URL points at an inline PNG server).
// ---------------------------------------------------------------------------

#[test]
fn federation_remote_media_cache() {
    let n = NODE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-fed-media-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("media temp dir");
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone())
        .expect("open media store");
    let blobs = Arc::new(FsBlobStore::new(dir.join("blobs")).expect("blob store"));
    let services = AppServices {
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
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "http://127.0.0.1:9".to_string(),
        max_file_size: 4096,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
    };
    let client = SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: true,
        ..Default::default()
    });

    // Strict client rejects loopback before fetching.
    let strict = SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: false,
        ..Default::default()
    });
    let blocked = ParsedAttachment {
        url: "http://127.0.0.1:9/secret.png".to_string(),
        media_type: Some("image/png".to_string()),
        name: Some("secret.png".to_string()),
        is_sensitive: false,
        blurhash: None,
        width: None,
        height: None,
    };
    assert!(fetch_and_cache_remote_media(&services, &strict, &blocked).is_err());

    let png: Vec<u8> = vec![
        0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 9, 9, 9, 9,
    ];
    let addr = spawn_png_server(png.clone(), 4);
    let good = ParsedAttachment {
        url: format!("http://{addr}/pic.png"),
        media_type: Some("image/png".to_string()),
        name: Some("Alt text".to_string()),
        is_sensitive: true,
        blurhash: None,
        width: Some(64),
        height: Some(64),
    };
    let meta = fetch_and_cache_remote_media(&services, &client, &good).expect("cache png");
    assert_eq!(meta.owner_id, None);
    assert!(meta.is_link);
    assert!(meta.is_sensitive);
    assert_eq!(meta.mime, "image/png");
    assert_eq!(meta.size_bytes, png.len() as u64);
    assert_eq!(blobs.get(&meta.blob_key).expect("blob").expect("bytes"), png);

    // Missing object rejected (404 from the inline server).
    let missing = ParsedAttachment {
        url: format!("http://{addr}/missing.png"),
        media_type: None,
        name: None,
        is_sensitive: false,
        blurhash: None,
        width: None,
        height: None,
    };
    assert!(fetch_and_cache_remote_media(&services, &client, &missing).is_err());
}



// ---------------------------------------------------------------------------
// Batch P gate 4: account move between two live pinetrees.
// bob@B moves to carol@A; dave@A (bob's follower) refollows carol.
// Covers: alias handshake, Move delivery, follower migration, replay
// convergence, forged-Move rejection.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn federation_account_move() {
    let a = boot_node("move-a");
    let b = boot_node("move-b");
    let client = test_client();
    let deadline = Duration::from_secs(25);

    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", a.base_url),
            None,
            &json!({"username": "carol", "password": "s3cret-pw"}),
        ),
        "A signup carol",
    );
    let carol_id = v["id"].as_str().expect("carol id").to_string();
    let carol_token = v["token"].as_str().expect("carol token").to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", a.base_url),
            None,
            &json!({"username": "dave", "password": "s3cret-pw"}),
        ),
        "A signup dave",
    );
    let dave_id = v["id"].as_str().expect("dave id").to_string();
    let dave_token = v["token"].as_str().expect("dave token").to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", b.base_url),
            None,
            &json!({"username": "bob", "password": "s3cret-pw"}),
        ),
        "B signup bob",
    );
    let bob_id = v["id"].as_str().expect("bob id").to_string();
    let bob_token = v["token"].as_str().expect("bob token").to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", b.base_url),
            None,
            &json!({"username": "mallory", "password": "s3cret-pw"}),
        ),
        "B signup mallory",
    );
    let mallory_id = v["id"].as_str().expect("mallory id").to_string();

    let carol_uri = format!("{}/users/{}", a.base_url, carol_id);
    let bob_uri = format!("{}/users/{}", b.base_url, bob_id);
    let mallory_uri = format!("{}/users/{}", b.base_url, mallory_id);

    a.services.ensure_actor_keypair(&carol_id).expect("carol key");
    a.services.ensure_actor_keypair(&dave_id).expect("dave key");
    b.services.ensure_actor_keypair(&bob_id).expect("bob key");
    b.services.ensure_actor_keypair(&mallory_id).expect("mallory key");

    // Dave@A resolves + follows bob@B (remote follow handshake).
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/users/show", a.base_url),
            Some(&dave_token),
            &json!({"username": "bob", "host": b.host}),
        ),
        "A resolves bob",
    );
    let bob_on_a = v["id"].as_str().expect("remote bob id").to_string();
    let r = api_post(
        &client,
        &format!("{}/api/following/create", a.base_url),
        Some(&dave_token),
        &json!({"userId": bob_on_a}),
    );
    assert!((200..300).contains(&r.status), "dave follow bob: {}", r.status);
    pump_until(&a, &b, "B sees dave as follower", deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&format!("{}/users/{}", a.base_url, dave_id))
            .expect("lookup dave")
            .map(|acc| {
                b.services
                    .follows
                    .is_following(&acc.id, &bob_id)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    })
    .await;

    // Alias handshake: carol lists bob's URI as alsoKnownAs.
    let r = api_post(
        &client,
        &format!("{}/api/i/update", a.base_url),
        Some(&carol_token),
        &json!({"alsoKnownAs": [bob_uri]}),
    );
    assert!((200..300).contains(&r.status), "carol alias: {}", r.status);

    // Bob moves to carol@A.
    let r = api_post(
        &client,
        &format!("{}/api/i/move", b.base_url),
        Some(&bob_token),
        &json!({"moveToAccount": format!("carol@{}", a.host)}),
    );
    assert!((200..300).contains(&r.status), "i/move: {} {}", r.status, String::from_utf8_lossy(&r.body));
    let moved: Value = serde_json::from_slice(&r.body).expect("move json");
    assert_eq!(moved["movedTo"], json!(carol_uri), "movedTo packs destination");

    // The Move reaches A: dave unfollows bob and follows carol.
    pump_until(&a, &b, "dave refollows carol", deadline, &mut || {
        a.services
            .follows
            .is_following(&dave_id, &carol_id)
            .unwrap_or(false)
    })
    .await;
    assert!(
        !a.services
            .follows
            .is_following(&dave_id, &bob_on_a)
            .unwrap_or(true),
        "stale dave→bob edge dropped"
    );
    let bob_on_b = b
        .services
        .accounts
        .find_by_id(&bob_id)
        .expect("bob row")
        .expect("bob present");
    assert_eq!(bob_on_b.moved_to_uri.as_deref(), Some(carol_uri.as_str()));

    // Replay convergence: the same Move delivered again changes nothing
    // (no duplicate edges, still exactly one dave→carol edge).
    let replay = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/moves/{}/replay-1", b.base_url, bob_id),
        "type": "Move",
        "actor": bob_uri,
        "object": bob_uri,
        "target": carol_uri,
    });
    let body = serde_json::to_vec(&replay).expect("encode move");
    let inbox_a = format!("{}/inbox", a.base_url);
    let r1 = signed_post_to_inbox(&client, &b.services, &bob_id, &inbox_a, &body, None);
    assert!((200..300).contains(&r1.status), "replay 1: {}", r1.status);
    let r2 = signed_post_to_inbox(&client, &b.services, &bob_id, &inbox_a, &body, None);
    assert!((200..300).contains(&r2.status), "replay 2: {}", r2.status);
    assert!(
        a.services
            .follows
            .is_following(&dave_id, &carol_id)
            .unwrap_or(false),
        "replay keeps the migrated edge"
    );

    // Forgery: mallory signs a Move claiming BOB moved → 401, no state.
    let forged = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/moves/{}/forged-1", b.base_url, mallory_id),
        "type": "Move",
        "actor": mallory_uri,
        "object": bob_uri,
        "target": mallory_uri,
    });
    let body = serde_json::to_vec(&forged).expect("encode forged");
    let r = signed_post_to_inbox(&client, &b.services, &mallory_id, &inbox_a, &body, None);
    assert_eq!(r.status, 401, "forged move rejected");
    let bob_on_a_row = a
        .services
        .accounts
        .find_by_id(&bob_on_a)
        .expect("bob row")
        .expect("bob present");
    // The legitimate Move already recorded carol; the forgery must not
    // overwrite it with mallory.
    assert_eq!(
        bob_on_a_row.moved_to_uri.as_deref(),
        Some(carol_uri.as_str()),
        "forgery changes nothing on the receiver"
    );
}

// ---------------------------------------------------------------------------
// Federated clips (handoff gate 5): two-local-pinetree clip federation.
// Listed collection resolves with per-item gating; unlisted works by
// capability URI and stays unadvertised; remote-approved refuses strangers
// (403) and 404s unknown/unsigned fetchers; TTL revalidation + Add/Remove
// converge; switches/flags off ⇒ zero remote footprint.
// ---------------------------------------------------------------------------

/// Signed ActivityPub GET from `sender_id` (mirrors `signed_post_to_inbox`).
fn signed_get_to_url(
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

fn clip_items(resp: &SafeHttpResponse, what: &str) -> Vec<String> {
    clip_items_signed(resp, what, None)
}

/// `clip_items` that re-signs the `first`-page hop as `signer` (a real
/// client signs every page fetch; an unsigned hop degrades to the
/// anonymous view — which is exactly what the gate asserts elsewhere).
fn clip_items_signed(
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

fn first_to_items(
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

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn federation_clips_listed_unlisted_approved_and_sync() {
    use pinetree::activitypub::federated_collections::{
        remote_clip_cached_uris, resolve_remote_clip,
    };

    let a = boot_node("clips-a");
    let b = boot_node("clips-b");
    let client = test_client();
    let deadline = Duration::from_secs(25);

    // Signup: alice@A, bob + mallory@B.
    let v = api_json(
        &api_post(&client, &format!("{}/api/signup", a.base_url), None, &json!({"username": "alice", "password": "s3cret-pw"})),
        "A signup alice",
    );
    let alice_id = v["id"].as_str().unwrap().to_string();
    let alice_token = v["token"].as_str().unwrap().to_string();
    let v = api_json(
        &api_post(&client, &format!("{}/api/signup", b.base_url), None, &json!({"username": "bob", "password": "s3cret-pw"})),
        "B signup bob",
    );
    let bob_id = v["id"].as_str().unwrap().to_string();
    let bob_token = v["token"].as_str().unwrap().to_string();
    let v = api_json(
        &api_post(&client, &format!("{}/api/signup", b.base_url), None, &json!({"username": "mallory", "password": "s3cret-pw"})),
        "B signup mallory",
    );
    let mallory_id = v["id"].as_str().unwrap().to_string();
    let mallory_token = v["token"].as_str().unwrap().to_string();
    a.services.ensure_actor_keypair(&alice_id).expect("alice key");
    b.services.ensure_actor_keypair(&bob_id).expect("bob key");
    b.services.ensure_actor_keypair(&mallory_id).expect("mallory key");
    let alice_uri = format!("{}/users/{}", a.base_url, alice_id);
    let bob_uri = format!("{}/users/{}", b.base_url, bob_id);

    // Federation switches ON on A (B serves nothing in this test).
    a.services
        .update_instance_settings(&alice_id, None, None, Some(true), None, a.services.clock.now_ms())
        .expect("federateClips on");

    // Alice: public note + followers-only note + listed federated clip.
    let n1 = api_json(
        &api_post(&client, &format!("{}/api/notes/create", a.base_url), Some(&alice_token), &json!({"text": "clip me"})),
        "A note n1",
    )["createdNote"]["id"].as_str().unwrap().to_string();
    let n2 = api_json(
        &api_post(&client, &format!("{}/api/notes/create", a.base_url), Some(&alice_token), &json!({"text": "followers only", "visibility": "followers"})),
        "A note n2",
    )["createdNote"]["id"].as_str().unwrap().to_string();
    let n1_uri = format!("{}/notes/{}", a.base_url, n1);
    let n2_uri = format!("{}/notes/{}", a.base_url, n2);
    let clip = api_json(
        &api_post(&client, &format!("{}/api/clips/create", a.base_url), Some(&alice_token),
            &json!({"name": "x", "isPublic": true, "visibility": "listed", "federate": true})),
        "A clip create",
    );
    let clip_id = clip["id"].as_str().unwrap().to_string();
    let clip_uri = format!("{}/clips/{}", a.base_url, clip_id);
    for note_id in [&n1, &n2] {
        let r = api_post(&client, &format!("{}/api/clips/add-note", a.base_url), Some(&alice_token),
            &json!({"clipId": clip_id, "noteId": note_id}));
        assert_eq!(r.status, 204, "add-note {note_id}: {}", String::from_utf8_lossy(&r.body));
    }

    // Bob@B follows alice@A (per-item gating needs the follow edge on A).
    let v = api_json(
        &api_post(&client, &format!("{}/api/users/show", b.base_url), Some(&bob_token), &json!({"username": "alice", "host": a.host})),
        "B resolves alice",
    );
    let alice_on_b = v["id"].as_str().unwrap().to_string();
    let r = api_post(&client, &format!("{}/api/following/create", b.base_url), Some(&bob_token), &json!({"userId": alice_on_b}));
    assert!((200..300).contains(&r.status), "bob follow alice: {}", r.status);
    pump_until(&a, &b, "A sees bob as follower", deadline, &mut || {
        a.services.accounts.find_by_uri(&bob_uri).expect("lookup bob")
            .map(|acc| a.services.follows.is_following(&acc.id, &alice_id).unwrap_or(false))
            .unwrap_or(false)
    })
    .await;

    // Anonymous fetch: listed collection serves, but only the public item.
    let items = clip_items(
        &http_get(&client, &clip_uri, "application/activity+json"),
        "anon listed",
    );
    assert_eq!(items, vec![n1_uri.clone()], "anonymous sees public items only");
    // Signed fetch as bob (follower): per-item gate admits the followers note.
    let items = clip_items_signed(
        &signed_get_to_url(&client, &b.services, &bob_id, &clip_uri, "application/activity+json"),
        "bob listed",
        Some((&b.services, &bob_id)),
    );
    assert!(items.contains(&n1_uri), "bob sees {items:?}");
    assert!(items.contains(&n2_uri), "follower sees followers item: {items:?}");
    // Signed fetch as mallory (stranger): followers item withheld.
    let r = signed_get_to_url(&client, &b.services, &mallory_id, &clip_uri, "application/activity+json");
    let items = clip_items(&r, "mallory listed");
    assert_eq!(items, vec![n1_uri.clone()], "stranger sees public only: {items:?}");

    // Advertisement: actor doc references the clips index; index lists it.
    let actor_doc: Value = serde_json::from_slice(
        &http_get(&client, &alice_uri, "application/activity+json").body,
    ).expect("actor json");
    let index_url = actor_doc["clips"].as_str().expect("actor clips ref").to_string();
    assert_eq!(index_url, format!("{}/users/{}/clips", a.base_url, alice_id));
    let index_items = clip_items(
        &http_get(&client, &index_url, "application/activity+json"),
        "clips index",
    );
    assert_eq!(index_items, vec![clip_uri.clone()]);

    // Unlisted clip: capability URI serves, index stays silent.
    let unlisted = api_json(
        &api_post(&client, &format!("{}/api/clips/create", a.base_url), Some(&alice_token),
            &json!({"name": "u", "isPublic": false, "visibility": "unlisted", "federate": true})),
        "A unlisted create",
    );
    let unlisted_id = unlisted["id"].as_str().unwrap().to_string();
    let unlisted_uri = format!("{}/clips/{}", a.base_url, unlisted_id);
    let r = api_post(&client, &format!("{}/api/clips/add-note", a.base_url), Some(&alice_token),
        &json!({"clipId": unlisted_id, "noteId": n1}));
    assert_eq!(r.status, 204);
    let items = clip_items(&http_get(&client, &unlisted_uri, "application/activity+json"), "anon unlisted");
    assert_eq!(items, vec![n1_uri.clone()], "bearer capability serves");
    let index_items = clip_items(&http_get(&client, &index_url, "application/activity+json"), "index hides unlisted");
    assert_eq!(index_items, vec![clip_uri.clone()], "unlisted never advertised");

    // Remote-approved clip: stranger signed → 403; unknown/unsigned → 404.
    let approved = api_json(
        &api_post(&client, &format!("{}/api/clips/create", a.base_url), Some(&alice_token),
            &json!({"name": "r", "isPublic": false, "visibility": "remote_approved", "federate": true, "approvedActorUris": [bob_uri]})),
        "A approved create",
    );
    let approved_uri = format!("{}/clips/{}", a.base_url, approved["id"].as_str().unwrap());
    let r = signed_get_to_url(&client, &b.services, &mallory_id, &approved_uri, "application/activity+json");
    assert_eq!(r.status, 403, "stranger refused: {}", String::from_utf8_lossy(&r.body));
    let r = http_get(&client, &approved_uri, "application/activity+json");
    assert_eq!(r.status, 404, "unsigned gets no existence signal");
    let r = clip_items_signed(
        &signed_get_to_url(&client, &b.services, &bob_id, &approved_uri, "application/activity+json"),
        "approver",
        Some((&b.services, &bob_id)),
    );
    assert!(r.is_empty(), "approver served, zero members: {r:?}");
    // Private clip: 404 for everyone remote.
    let private = api_json(
        &api_post(&client, &format!("{}/api/clips/create", a.base_url), Some(&alice_token),
            &json!({"name": "p", "federate": true})),
        "A private create",
    );
    let private_uri = format!("{}/clips/{}", a.base_url, private["id"].as_str().unwrap());
    let r = signed_get_to_url(&client, &b.services, &bob_id, &private_uri, "application/activity+json");
    assert_eq!(r.status, 404, "private never leaves");
    let _ = mallory_token;

    // Membership sync: B primes its cache, then Add/Remove converge.
    // Alice follows bob so bob's public note reaches A for clipping.
    let v = api_json(
        &api_post(&client, &format!("{}/api/users/show", a.base_url), Some(&alice_token), &json!({"username": "bob", "host": b.host})),
        "A resolves bob",
    );
    let bob_on_a = v["id"].as_str().unwrap().to_string();
    let r = api_post(&client, &format!("{}/api/following/create", a.base_url), Some(&alice_token), &json!({"userId": bob_on_a}));
    assert!((200..300).contains(&r.status), "alice follow bob: {}", r.status);
    pump_until(&a, &b, "B sees alice as follower", deadline, &mut || {
        b.services.accounts.find_by_uri(&alice_uri).expect("lookup alice")
            .map(|acc| b.services.follows.is_following(&acc.id, &bob_id).unwrap_or(false))
            .unwrap_or(false)
    })
    .await;
    // B primes the cache (anonymous pull: public items only).
    let pulled = resolve_remote_clip(&b.services, &clip_uri).expect("prime pull");
    assert_eq!(pulled, vec![n1_uri.clone()]);
    assert!(remote_clip_cached_uris().contains(&clip_uri));
    // Bob posts; alice (follower) ingests; alice clips it.
    let m1_b = api_json(
        &api_post(&client, &format!("{}/api/notes/create", b.base_url), Some(&bob_token), &json!({"text": "clip this too"})),
        "B note m1",
    )["createdNote"]["id"].as_str().unwrap().to_string();
    let m1_uri = format!("{}/notes/{}", b.base_url, m1_b);
    pump_until(&a, &b, "A ingests m1", deadline, &mut || {
        a.services.notes.find_by_uri(&m1_uri).expect("lookup m1").is_some()
    })
    .await;
    let m1_on_a = a.services.notes.find_by_uri(&m1_uri).expect("m1 row").unwrap().id.clone();
    let r = api_post(&client, &format!("{}/api/clips/add-note", a.base_url), Some(&alice_token),
        &json!({"clipId": clip_id, "noteId": m1_on_a}));
    assert_eq!(r.status, 204, "clip remote note: {}", String::from_utf8_lossy(&r.body));
    // The Add reaches B's inbox and evicts the cache; the re-pull converges.
    pump_until(&a, &b, "B cache evicted by Add", deadline, &mut || {
        drain_all(&a, &b);
        !remote_clip_cached_uris().contains(&clip_uri)
    })
    .await;
    let pulled = resolve_remote_clip(&b.services, &clip_uri).expect("re-pull");
    assert!(pulled.contains(&n1_uri) && pulled.contains(&m1_uri), "Add converged: {pulled:?}");
    // Remove converges the same way.
    let r = api_post(&client, &format!("{}/api/clips/remove-note", a.base_url), Some(&alice_token),
        &json!({"clipId": clip_id, "noteId": m1_on_a}));
    assert_eq!(r.status, 204);
    pump_until(&a, &b, "B cache evicted by Remove", deadline, &mut || {
        drain_all(&a, &b);
        !remote_clip_cached_uris().contains(&clip_uri)
    })
    .await;
    let pulled = resolve_remote_clip(&b.services, &clip_uri).expect("re-pull 2");
    assert_eq!(pulled, vec![n1_uri.clone()], "Remove converged: {pulled:?}");

    // Switch off ⇒ zero footprint: routes 404, advertisement gone.
    a.services
        .update_instance_settings(&alice_id, None, None, Some(false), None, a.services.clock.now_ms())
        .expect("federateClips off");
    let r = http_get(&client, &clip_uri, "application/activity+json");
    assert_eq!(r.status, 404, "switch off 404s collections");
    let r = http_get(&client, &index_url, "application/activity+json");
    assert_eq!(r.status, 404, "switch off 404s the index");
    let actor_doc: Value = serde_json::from_slice(
        &http_get(&client, &alice_uri, "application/activity+json").body,
    ).expect("actor json 2");
    assert!(actor_doc.get("clips").is_none(), "no advertisement when off");
    // Evict B's cached resolution first: the switch-off must break the
    // NETWORK path (a warm cache legitimately serves without fetching).
    pinetree::activitypub::federated_collections::evict_remote_clip_cache(&clip_uri);
    assert!(resolve_remote_clip(&b.services, &clip_uri).is_err(), "no resolution when off");
}

// ---------------------------------------------------------------------------
// Federated pages (handoff gate 6): two-local-pinetree page federation.
// Article round-trip (title/summary/content/eye-catch), Update(Article)
// converges, scripted page federates as static-only (no script bytes in
// ANY federated payload), page-like round-trip, inbound Article fixture
// renders sanitized.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn federation_pages_article_round_trip_update_and_likes() {
    use pinetree::application::ports::PageStore;

    let a = boot_node("pages-a");
    let b = boot_node("pages-b");
    let client = test_client();
    let deadline = Duration::from_secs(25);

    let v = api_json(
        &api_post(&client, &format!("{}/api/signup", a.base_url), None, &json!({"username": "alice", "password": "s3cret-pw"})),
        "A signup alice",
    );
    let alice_id = v["id"].as_str().unwrap().to_string();
    let alice_token = v["token"].as_str().unwrap().to_string();
    let v = api_json(
        &api_post(&client, &format!("{}/api/signup", b.base_url), None, &json!({"username": "bob", "password": "s3cret-pw"})),
        "B signup bob",
    );
    let bob_id = v["id"].as_str().unwrap().to_string();
    let bob_token = v["token"].as_str().unwrap().to_string();
    a.services.ensure_actor_keypair(&alice_id).expect("alice key");
    b.services.ensure_actor_keypair(&bob_id).expect("bob key");
    let alice_uri = format!("{}/users/{}", a.base_url, alice_id);
    let bob_uri = format!("{}/users/{}", b.base_url, bob_id);

    // Federation switches ON on A.
    a.services
        .update_instance_settings(&alice_id, None, None, None, Some(true), a.services.clock.now_ms())
        .expect("federatePages on");

    // Bob follows alice BEFORE the page exists (fan-out resolves at send).
    let v = api_json(
        &api_post(&client, &format!("{}/api/users/show", b.base_url), Some(&bob_token), &json!({"username": "alice", "host": a.host})),
        "B resolves alice",
    );
    let alice_on_b = v["id"].as_str().unwrap().to_string();
    let r = api_post(&client, &format!("{}/api/following/create", b.base_url), Some(&bob_token), &json!({"userId": alice_on_b}));
    assert!((200..300).contains(&r.status), "bob follow alice: {}", r.status);
    pump_until(&a, &b, "A sees bob as follower", deadline, &mut || {
        a.services.accounts.find_by_uri(&bob_uri).expect("lookup bob")
            .map(|acc| a.services.follows.is_following(&acc.id, &alice_id).unwrap_or(false))
            .unwrap_or(false)
    })
    .await;

    // Alice creates a scripted federated page.
    let page = api_json(
        &api_post(&client, &format!("{}/api/pages/create", a.base_url), Some(&alice_token),
            &json!({
                "title": "Hello",
                "name": "hello",
                "content": [{ "type": "text", "text": "federated body" }],
                "variables": [],
                "script": "let s3cret = `run()`;",
                "federate": true,
            })),
        "A page create",
    );
    let page_id = page["id"].as_str().unwrap().to_string();
    let article_uri = format!("{}/pages/{}", a.base_url, page_id);

    // B receives Create(Article) → materializes a remote page (not a
    // note): title/content fidelity, script never stored.
    pump_until(&a, &b, "B ingests the Article", deadline, &mut || {
        drain_all(&a, &b);
        b.services.pages.get_page_by_uri(&article_uri).expect("lookup article").is_some()
    })
    .await;
    let stored = b.services.pages.get_page_by_uri(&article_uri).expect("article page").unwrap();
    assert_eq!(stored.title, "Hello");
    assert_eq!(stored.script, "", "remote script never stored");
    assert_eq!(stored.uri.as_deref(), Some(article_uri.as_str()));
    assert!(stored.owner_id != alice_id, "remote-owned row");
    let content = serde_json::to_string(&stored.content).unwrap();
    assert!(content.contains("federated body"), "body survived: {content:?}");
    assert!(!content.contains("s3cret") && !content.contains("run()`"), "no script: {content:?}");
    // No note row shadows the page.
    assert!(b.services.notes.find_by_uri(&article_uri).expect("note lookup").is_none());

    // The served Article itself carries no script bytes anywhere.
    let article: Value = serde_json::from_slice(
        &http_get(&client, &article_uri, "application/activity+json").body,
    ).expect("article json");
    assert_eq!(article["type"], "Article");
    assert_eq!(article["name"], "Hello");
    assert!(article["content"].as_str().unwrap().contains("federated body"));
    let wire = serde_json::to_string(&article).unwrap();
    assert!(!wire.contains("s3cret") && !wire.contains("run()`"), "wire has no script");

    // Update converges: alice edits → Update(Article) → B's page updates.
    let r = api_post(&client, &format!("{}/api/pages/update", a.base_url), Some(&alice_token),
        &json!({"pageId": page_id, "title": "Hello 2", "content": [{ "type": "text", "text": "updated body" }]}));
    assert_eq!(r.status, 204, "page update: {}", String::from_utf8_lossy(&r.body));
    pump_until(&a, &b, "B page updated", deadline, &mut || {
        drain_all(&a, &b);
        b.services.pages.get_page_by_uri(&article_uri).expect("lookup").map(|p| {
            p.title == "Hello 2"
                && serde_json::to_string(&p.content).unwrap().contains("updated body")
        }).unwrap_or(false)
    })
    .await;

    // Page-like round-trip: bob's Like(page URI) → A's inbox → count bumps.
    let like = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/likes/bob-1", b.base_url),
        "type": "Like",
        "actor": bob_uri,
        "object": article_uri,
    });
    let body = serde_json::to_vec(&like).unwrap();
    let r = signed_post_to_inbox(&client, &b.services, &bob_id, &format!("{}/inbox", a.base_url), &body, None);
    assert!((200..300).contains(&r.status), "like POST: {}", r.status);
    let count = a.services.pages.get_page(&page_id).expect("page").unwrap().liked_count;
    assert_eq!(count, 1, "page like recorded");
    // Replay converges (no double count).
    let r = signed_post_to_inbox(&client, &b.services, &bob_id, &format!("{}/inbox", a.base_url), &body, None);
    assert!((200..300).contains(&r.status));
    let count = a.services.pages.get_page(&page_id).expect("page").unwrap().liked_count;
    assert_eq!(count, 1, "replay converges");
    // Unknown-page likes are ignored, not errors.
    let like_unknown = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/likes/bob-2", b.base_url),
        "type": "Like",
        "actor": bob_uri,
        "object": format!("{}/pages/aaaaaaaaaaaaaaaa", a.base_url),
    });
    let body = serde_json::to_vec(&like_unknown).unwrap();
    let r = signed_post_to_inbox(&client, &b.services, &bob_id, &format!("{}/inbox", a.base_url), &body, None);
    assert!((200..300).contains(&r.status));

    // Inbound Article fixture with hostile markup materializes as a
    // sanitized page (never a note, never executable).
    let evil = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/articles/evil-1", b.base_url),
        "type": "Create",
        "actor": bob_uri,
        "object": {
            "id": format!("{}/articles/evil-1", b.base_url),
            "type": "Article",
            "attributedTo": bob_uri,
            "name": "evil",
            "content": "<p>hi</p><script>alert(1)</script><img src=x onerror=alert(2)>",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
        },
    });
    let body = serde_json::to_vec(&evil).unwrap();
    let r = signed_post_to_inbox(&client, &b.services, &bob_id, &format!("{}/inbox", a.base_url), &body, None);
    assert!((200..300).contains(&r.status), "evil article POST: {}", r.status);
    let stored = a
        .services
        .pages
        .get_page_by_uri(&format!("{}/articles/evil-1", b.base_url))
        .expect("evil lookup")
        .expect("evil stored as page");
    assert_eq!(stored.script, "");
    let content = serde_json::to_string(&stored.content).unwrap();
    assert!(!content.contains("<script") && !content.contains("onerror"), "sanitized: {content:?}");
    assert!(content.contains("hi"), "readable content kept: {content:?}");
    assert!(a.services.notes.find_by_uri(&format!("{}/articles/evil-1", b.base_url)).expect("evil note lookup").is_none(),
        "no shadow note row");
}
