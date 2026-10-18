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

async fn boot_node(tag: &'static str) -> FedNode {
    let n = NODE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-fed-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create fed temp dir");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
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
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
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
    let serve_handle = tokio::spawn(async move {
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
    };
    let client = test_client();
    let base = node.base_url.clone();
    wait_ready(&client, &base, node.tag).await;
    node
}

impl FedNode {
    fn stop_server(&mut self) {
        if let Some(handle) = self.serve_handle.take() {
            handle.abort();
        }
    }

    async fn start_server(&mut self) {
        let start = Instant::now();
        let listener = loop {
            match tokio::net::TcpListener::bind(self.addr).await {
                Ok(l) => break l,
                Err(_) => {
                    assert!(
                        start.elapsed() < Duration::from_secs(10),
                        "{} could not rebind {}",
                        self.tag,
                        self.addr
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        let app = federation_router(self.state.clone());
        self.serve_handle = Some(tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await;
        }));
        let client = test_client();
        let base = self.base_url.clone();
        let tag = self.tag;
        wait_ready(&client, &base, tag).await;
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
    let a = boot_node("a").await;
    let b = boot_node("b").await;
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
    b.start_server().await;
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


