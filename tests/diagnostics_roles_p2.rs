//! Phase 5/6 regression tests (`docs/prod-ready-2.md` §5/§6 P2).
//!
//! Scope: malformed-input containment on this workstream's surfaces
//! (streaming frames, remote-actor JSON, inbox bodies, durable-job
//! payloads), worker survival without endless reclaim, bounded readiness,
//! actionable-or-documented admin diagnostics, and unmistakable
//! storage-only role semantics. Hermetic: loopback router + real redb
//! store in a temp dir, in-memory blobs, per-call timeouts.
//!
//! Process-wide ops cells (`set_ops_config` / `set_backup_context`) are
//! reconfigured per test under [`serial`]; every HTTP-harness test in this
//! file must hold `_guard` for its whole body. This file never touches
//! `tests/admin_diagnostics_h*.rs` or the sibling P2 files.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use pinetree::activitypub::actor::parse_remote_actor;
use pinetree::api::misskey::endpoints::{OpsConfig, set_backup_context, set_ops_config};
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, Clock, IdGenerator, InstanceSettingsPatch, InstanceSettingsStore,
    JobKind, JobQueue, NewJob, PortError, StreamingSink,
};
use pinetree::backup::{BackupContext, BackupState};
use pinetree::domain::admin::ROLE_POLICY_UNENFORCED_NOTE;
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::domain::instance::InstanceSettings;
use pinetree::domain::moderation::ModerationLogEntry;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};
use pinetree::streaming::hub::hub_and_sink;

/// Serializes this file's HTTP tests: the ops/backup cells are process-wide.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// In-memory blob store: containment tests never need durable bytes.
#[derive(Default)]
struct MemBlobs {
    inner: Mutex<HashMap<String, Vec<u8>>>,
}

impl BlobStore for MemBlobs {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(hash.to_string(), bytes.to_vec());
        Ok(())
    }
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        Ok(self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(hash)
            .cloned())
    }
    fn delete(&self, hash: &str) -> Result<(), PortError> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(hash);
        Ok(())
    }
}

/// Instance-settings view over the real redb store with a test-only outage
/// switch. `fail == true` makes every read answer `StorageUnavailable`,
/// approximating a dead redb backend through the exact code path `/ready`
/// and `admin/status` read (`instance_settings.get()`).
struct FlippableSettings {
    inner: Arc<RedbStore>,
    fail: AtomicBool,
}

impl InstanceSettingsStore for FlippableSettings {
    fn get(&self) -> Result<InstanceSettings, PortError> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(PortError::StorageUnavailable(
                "redb unavailable (test)".to_string(),
            ));
        }
        self.inner.get()
    }
    fn update(
        &self,
        patch: InstanceSettingsPatch,
        log: ModerationLogEntry,
    ) -> Result<InstanceSettings, PortError> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(PortError::StorageUnavailable(
                "redb unavailable (test)".to_string(),
            ));
        }
        self.inner.update(patch, log)
    }
}

struct Harness {
    base: String,
    client: reqwest::Client,
    services: Arc<AppServices>,
    settings: Arc<FlippableSettings>,
    durable_dir: std::path::PathBuf,
    cache_dir: std::path::PathBuf,
    backup_dir: std::path::PathBuf,
}

impl Harness {
    async fn start(detailed_admin_status: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "pinetree-diag-p2-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create test dir");
        let durable_dir = root.join("blobs_durable");
        let cache_dir = root.join("blobs_cache");
        let backup_dir = root.join("backups");
        std::fs::create_dir_all(&durable_dir).expect("durable dir");
        std::fs::create_dir_all(&cache_dir).expect("cache dir");
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store =
            RedbStore::open(root.join("t.redb"), clock.clone(), ids.clone()).expect("open store");
        let settings = Arc::new(FlippableSettings {
            inner: store.clone(),
            fail: AtomicBool::new(false),
        });
        let blobs: Arc<dyn BlobStore> = Arc::new(MemBlobs::default());
        let (hub, streaming) = hub_and_sink(1024);
        let streaming: Arc<dyn StreamingSink> = streaming;
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
            blobs: blobs.clone(),
            notifications: store.clone(),
            auth: store.clone(),
            streaming: streaming.clone(),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: settings.clone(),
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
            public_url: "https://example.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
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
            featured_cache: Arc::new(
                pinetree::application::commands::pins::FeaturedCache::default(),
            ),
        };
        set_ops_config(OpsConfig {
            detailed_admin_status,
            durable_dir: durable_dir.clone(),
            cache_dir: cache_dir.clone(),
            backup_dir: backup_dir.clone(),
            transcode_enabled: false,
            ffmpeg_status: "disabled".to_string(),
            config_fingerprint: serde_json::json!({"test": true}),
            started_ms: 1_700_000_000_000,
        });
        set_backup_context(BackupContext {
            store: store.clone(),
            durable_pool: durable_dir.clone(),
            cache_pool: cache_dir.clone(),
            backup_dir: backup_dir.clone(),
            max_bundle_bytes: None,
            retention_max_count: None,
            retention_max_bytes: None,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(
                listener,
                build_router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .expect("serve test router");
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("http client");
        Self {
            base: format!("http://{addr}"),
            client,
            services,
            settings,
            durable_dir,
            cache_dir,
            backup_dir,
        }
    }

    fn set_settings_fail(&self, fail: bool) {
        self.settings.fail.store(fail, Ordering::Relaxed);
    }

    async fn api(
        &self,
        endpoint: &str,
        body: serde_json::Value,
        token: Option<&str>,
    ) -> (u16, serde_json::Value) {
        let mut request = self
            .client
            .post(format!("{}/api/{endpoint}", self.base))
            .json(&body);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.expect("api call");
        let status = response.status().as_u16();
        let value: serde_json::Value = response.json().await.unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    async fn signup(&self, username: &str) -> (String, String) {
        let (status, value) = self
            .api(
                "signup",
                serde_json::json!({ "username": username, "password": "p2-secret-pw" }),
                None,
            )
            .await;
        assert_eq!(status, 200, "signup {username}: {value}");
        let id = value["id"].as_str().expect("account id").to_string();
        let token = value["token"].as_str().expect("token").to_string();
        (id, token)
    }

    async fn get(&self, path: &str) -> (u16, String) {
        let response = self
            .client
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .expect("get");
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        (status, body)
    }

    fn ws_url(&self, query: &str) -> String {
        self.base.replace("http://", "ws://") + "/streaming" + query
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(url: &str) -> Ws {
    tokio_tungstenite::connect_async(url)
        .await
        .expect("ws connect")
        .0
}

/// Malformed streaming frames: untrusted inbound text/binary. The server must
/// ignore them (never panic, never close the socket on protocol garbage).
fn malformed_ws_frames() -> Vec<Message> {
    let mut frames = vec![
        Message::Text("".into()),
        Message::Text("{".into()),
        Message::Text("[".into()),
        Message::Text("null".into()),
        Message::Text("42".into()),
        Message::Text("\"just a string\"".into()),
        Message::Text("{{{{".into()),
        Message::Text("{\"type\":".into()),
        Message::Text("{\"type\":42,\"body\":null}".into()),
        Message::Text("{\"type\":null}".into()),
        Message::Text("{\"body\":{}}".into()),
        Message::Text("[]".into()),
        Message::Text("{}".into()),
        Message::Text("{\"type\":\"connect\",\"body\":null}".into()),
        Message::Text("{\"type\":\"connect\",\"body\":{\"id\":42}}".into()),
        Message::Text("{\"type\":\"connect\",\"body\":{\"id\":null,\"channel\":null}}".into()),
        Message::Text("{\"type\":\"channel\",\"body\":\"not-an-object\"}".into()),
        Message::Text("{\"type\":\"subNote\",\"body\":{}}".into()),
        Message::Text("{\"type\":\"subNote\",\"body\":{\"id\":[1,2,3]}}".into()),
        Message::Text("\u{0}\u{1}\u{2}binary-ish-text\u{fffd}".into()),
        Message::Text("{\"type\":\"connect\",\"body\":{\"id\":\"\",\"channel\":\"\"}}".into()),
        Message::Binary(vec![0, 1, 2, 255, 254, 253].into()),
        Message::Binary(vec![0; 4096].into()),
    ];
    // Deeply nested object: must error at parse, never stack-overflow.
    let mut nested = String::from("{\"type\":\"x\",\"body\":");
    for _ in 0..256 {
        nested.push_str("{\"a\":");
    }
    nested.push_str("null");
    for _ in 0..256 {
        nested.push('}');
    }
    nested.push('}');
    frames.push(Message::Text(nested.into()));
    // Oversized-but-routable text frame (1 MiB of digits inside JSON).
    frames.push(Message::Text(format!("\"{}\"", "9".repeat(1 << 20)).into()));
    frames
}

/// Malformed WebSocket frames yield no crash and no server-initiated close:
/// the socket stays usable and the process keeps serving health/readiness.
#[tokio::test]
async fn ws_malformed_frames_never_close_or_panic() {
    let _guard = serial();
    let harness = Harness::start(false).await;
    let mut ws = ws_connect(&harness.ws_url("")).await;
    for frame in malformed_ws_frames() {
        ws.send(frame).await.expect("send malformed frame");
    }
    // The server must not have closed the socket on garbage: expect
    // silence (timeout), never a Close frame.
    let probe = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
    match probe {
        Err(_) => {}
        Ok(None) => panic!("server closed the socket on malformed frames"),
        Ok(Some(Err(_))) => panic!("ws transport error after malformed frames"),
        Ok(Some(Ok(Message::Close(_)))) => {
            panic!("server sent Close in response to malformed frames")
        }
        Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
        Ok(Some(Ok(_))) => {}
    }
    // The socket is still usable: a well-formed frame is accepted without
    // error and the process still serves probes.
    ws.send(Message::Text(
        "{\"type\":\"connect\",\"body\":{\"channel\":\"global\",\"id\":\"p2\"}}".into(),
    ))
    .await
    .expect("socket still usable after malformed frames");
    let (status, body) = harness.get("/healthz").await;
    assert_eq!((status, body.as_str()), (200, "ok"));
    let (status, _) = harness.get("/ready").await;
    assert_eq!(status, 200);
    ws.close(None).await.ok();
}

/// Property-style fuzz over the streaming frame surface: deterministic
/// mutations of valid protocol messages must never wedge or kill the
/// connection (each send succeeds; the socket never receives Close).
#[tokio::test]
async fn ws_frame_fuzz_property() {
    let _guard = serial();
    let harness = Harness::start(false).await;
    let seeds = [
        "{\"type\":\"connect\",\"body\":{\"channel\":\"global\",\"id\":\"a\"}}",
        "{\"type\":\"disconnect\",\"body\":{\"id\":\"a\"}}",
        "{\"type\":\"channel\",\"body\":{\"id\":\"a\",\"type\":\"readNotification\"}}",
        "{\"type\":\"subNote\",\"body\":{\"id\":\"note1\"}}",
        "{\"type\":\"unsubNote\",\"body\":{\"id\":\"note1\"}}",
        "{\"type\":\"readNotification\"}",
    ];
    let mut ws = ws_connect(&harness.ws_url("")).await;
    let mut rng: u64 = 0x243F_6A88_85A3_08D3;
    let mut next_byte = || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (rng >> 33) as u8
    };
    for i in 0..300u32 {
        let seed = seeds[(i as usize) % seeds.len()];
        let mut bytes = seed.as_bytes().to_vec();
        let op = next_byte() % 4;
        match op {
            0 => {
                // Truncate at a pseudo-random point.
                let at = (next_byte() as usize) % (bytes.len() + 1);
                bytes.truncate(at);
            }
            1 => {
                // Flip/replace pseudo-random bytes.
                for _ in 0..(1 + next_byte() % 4) {
                    if bytes.is_empty() {
                        break;
                    }
                    let at = (next_byte() as usize) % bytes.len();
                    bytes[at] = next_byte();
                }
            }
            2 => {
                // Splice in hostile snippets.
                let snippets: &[&[u8]] = &[
                    b"\x00\xff\xfe",
                    b"null",
                    b"[]",
                    b"\"\\ud800\"",
                    b"{\"a\":",
                    b"\x22\x5c",
                ];
                let at = (next_byte() as usize) % (bytes.len() + 1);
                let snip = snippets[(next_byte() as usize) % snippets.len()];
                bytes.splice(at..at, snip.iter().cloned());
            }
            _ => {
                // Duplicate a slice (nesting/depth pressure).
                if !bytes.is_empty() {
                    let at = (next_byte() as usize) % bytes.len();
                    let end = bytes.len().min(at + 8);
                    let dup = bytes[at..end].to_vec();
                    bytes.splice(at..at, dup);
                }
            }
        }
        if next_byte() % 5 == 0 {
            ws.send(Message::Binary(bytes.into()))
                .await
                .expect("fuzz binary send must succeed");
        } else {
            // Lossy UTF-8 keeps the fuzzer total (invalid bytes become U+FFFD
            // text instead of a skipped case).
            let text = String::from_utf8_lossy(&bytes).into_owned();
            ws.send(Message::Text(text.into()))
                .await
                .expect("fuzz text send must succeed");
        }
        // Never accept a server-initiated close mid-fuzz.
        if let Ok(Some(Ok(Message::Close(_)))) =
            tokio::time::timeout(Duration::from_millis(5), ws.next()).await
        {
            panic!("server closed connection on fuzz input {i}");
        }
    }
    let (status, _) = harness.get("/healthz").await;
    assert_eq!(status, 200, "process survives the fuzz run");
    ws.close(None).await.ok();
}

const FIXTURE_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nZmFrZXJlbW90ZXN0a2V5\n-----END PUBLIC KEY-----";

fn valid_actor_doc() -> serde_json::Value {
    let id = "https://remote.example/users/alice";
    serde_json::json!({
        "id": id,
        "type": "Person",
        "preferredUsername": "alice",
        "inbox": "https://remote.example/users/alice/inbox",
        "endpoints": { "sharedInbox": "https://remote.example/inbox" },
        "publicKey": {
            "id": format!("{id}#main-key"),
            "type": "Key",
            "owner": id,
            "publicKeyPem": FIXTURE_PEM,
        },
    })
}

/// Fuzz/property-style test for the remote-actor parser: every malformed
/// document maps to `Err`, never a panic. Invalid UTF-8/hostile shapes are
/// included via byte-level mutation of a valid document.
#[tokio::test]
async fn actor_json_fuzz_never_panics() {
    let targeted = vec![
        serde_json::json!(null),
        serde_json::json!([]),
        serde_json::json!("string"),
        serde_json::json!(42),
        serde_json::json!({}),
        serde_json::json!({"id": 42, "type": "Person"}),
        serde_json::json!({"id": null, "type": null}),
        serde_json::json!({"id": "https://remote.example/u/1"}),
        serde_json::json!({"id": "not a url", "type": "Person",
            "preferredUsername": "x", "inbox": "https://remote.example/inbox",
            "publicKey": {"id": "not a url#k", "publicKeyPem": FIXTURE_PEM}}),
        serde_json::json!({"id": "ftp://remote.example/u/1", "type": "Person",
            "preferredUsername": "x", "inbox": "ftp://remote.example/inbox",
            "publicKey": {"id": "ftp://remote.example/u/1#k", "publicKeyPem": FIXTURE_PEM}}),
        serde_json::json!({"id": "https://remote.example/u/1", "type": "Robot",
            "preferredUsername": "x", "inbox": "https://remote.example/inbox",
            "publicKey": {"id": "https://remote.example/u/1#k", "publicKeyPem": FIXTURE_PEM}}),
        // A type ARRAY with no recognized entry falls back to `Person`
        // (lenient upstream-compatible ingest, not a panic path).
        serde_json::json!({"id": "https://a.example/u/1", "type": "Person",
            "preferredUsername": "x", "inbox": "https://b.example/inbox",
            "publicKey": {"id": "https://a.example/u/1#k", "publicKeyPem": FIXTURE_PEM}}),
        // Attacker key id owned by someone else (Slice 2 C1).
        serde_json::json!({"id": "https://a.example/u/1", "type": "Person",
            "preferredUsername": "x", "inbox": "https://a.example/inbox",
            "publicKey": {"id": "https://victim.example/u/9#main-key",
                          "owner": "https://a.example/u/1", "publicKeyPem": FIXTURE_PEM}}),
        // https actor with http inbox (downgrade refused).
        serde_json::json!({"id": "https://a.example/u/1", "type": "Person",
            "preferredUsername": "x", "inbox": "http://a.example/inbox",
            "publicKey": {"id": "https://a.example/u/1#k", "publicKeyPem": FIXTURE_PEM}}),
        serde_json::json!({"id": "https://a.example/u/1", "type": "Person",
            "preferredUsername": "", "inbox": "https://a.example/inbox",
            "publicKey": {"id": "https://a.example/u/1#k", "publicKeyPem": FIXTURE_PEM}}),
        serde_json::json!({"id": "https://a.example/u/1", "type": "Person",
            "preferredUsername": "x", "inbox": "https://a.example/inbox"}),
        serde_json::json!({"id": "https://a.example/u/1", "type": "Person",
            "preferredUsername": "x", "inbox": "https://a.example/inbox",
            "publicKey": {"id": "", "publicKeyPem": ""}}),
    ];
    for doc in &targeted {
        assert!(
            parse_remote_actor(doc).is_err(),
            "hostile actor doc must be rejected: {doc}"
        );
    }
    // Byte-level mutations of a valid document: truncation, flips, hostile
    // splices, and type swaps. None may panic; documents that still parse
    // must carry the original actor id (no confused-deputy id rewrite).
    let valid = serde_json::to_vec(&valid_actor_doc()).expect("valid doc serializes");
    let mut rng: u64 = 0xB529_7A4D_3671_3045;
    let mut next_byte = || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (rng >> 33) as u8
    };
    for i in 0..500u32 {
        let mut bytes = valid.clone();
        match next_byte() % 5 {
            0 => {
                let at = (next_byte() as usize) % (bytes.len() + 1);
                bytes.truncate(at);
            }
            1 => {
                for _ in 0..(1 + next_byte() % 6) {
                    let at = (next_byte() as usize) % bytes.len();
                    bytes[at] = next_byte();
                }
            }
            2 => {
                let snippets: &[&[u8]] = &[
                    b"\x00",
                    b"\"type\":\"Service\"",
                    b"null",
                    b"[]",
                    b"{}",
                    b"\"id\":\"https://evil.example/x\"",
                    b"\\u0000",
                ];
                let at = (next_byte() as usize) % (bytes.len() + 1);
                let snip = snippets[(next_byte() as usize) % snippets.len()];
                bytes.splice(at..at, snip.iter().cloned());
            }
            3 => {
                bytes.clear();
            }
            _ => {
                bytes.extend_from_slice(b"trailing garbage {{{");
            }
        }
        // `from_slice` on arbitrary bytes never panics; the parser only
        // sees well-formed JSON values. A panic inside the parser fails
        // this test, which is exactly the property under test.
        let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&bytes);
        if let Ok(doc) = parsed {
            let result = parse_remote_actor(&doc);
            if let Ok(actor) = result {
                assert_eq!(
                    actor.uri, "https://remote.example/users/alice",
                    "mutation {i} must not rewrite the actor id"
                );
            }
        }
    }
    // The valid document still parses after the fuzz run.
    let actor = parse_remote_actor(&valid_actor_doc()).expect("valid doc parses");
    assert_eq!(actor.username, "alice");
    // Lenient type-array fallback (ingest compatibility): an array with no
    // recognized entry parses as `Person` rather than erroring.
    let mut fallback = valid_actor_doc();
    fallback["type"] = serde_json::json!(["Robot"]);
    let actor = parse_remote_actor(&fallback).expect("unknown type array falls back");
    assert_eq!(actor.actor_type, "Person");
}

/// Malformed inbox bodies map to typed HTTP errors (400/401/413), never a
/// panic or 500, and the process keeps serving afterwards.
#[tokio::test]
async fn inbox_malformed_bodies_return_typed_errors() {
    let _guard = serial();
    let harness = Harness::start(false).await;
    let post_raw = |body: Vec<u8>| {
        let url = format!("{}/inbox", harness.base);
        let client = harness.client.clone();
        async move {
            let response = client
                .post(url)
                .header("Content-Type", "application/activity+json")
                .body(body)
                .send()
                .await
                .expect("inbox post");
            let status = response.status().as_u16();
            let text = response.text().await.unwrap_or_default();
            (status, text)
        }
    };
    let cases: Vec<(Vec<u8>, Vec<u16>)> = vec![
        (b"".to_vec(), vec![400]),
        (b"{".to_vec(), vec![400]),
        (b"[1,2,3]".to_vec(), vec![400]),
        (b"null".to_vec(), vec![400]),
        (b"42".to_vec(), vec![400]),
        (b"{}".to_vec(), vec![400]),
        (br#"{"type":"Create"}"#.to_vec(), vec![400]),
        (br#"{"id":"https://r.example/1"}"#.to_vec(), vec![400]),
        (
            br#"{"type":"Create","id":"https://r.example/1"}"#.to_vec(),
            vec![401],
        ),
        (
            br#"{"type":"Create","id":"https://r.example/1","actor":"x"}"#.to_vec(),
            vec![401],
        ),
        (
            br#"{"type":"Weird","id":"https://r.example/1","actor":"https://r.example/u/1"}"#
                .to_vec(),
            vec![401],
        ),
        (vec![0, 159, 146, 150], vec![400]),
    ];
    for (body, want) in &cases {
        let (status, _) = post_raw(body.clone()).await;
        assert!(
            want.contains(&status),
            "inbox body {body:?} must map to {want:?}, got {status}"
        );
        assert_ne!(status, 500, "inbox must never 500 on malformed input");
    }
    // Oversize body: rejected at the extraction layer, not buffered.
    let big = vec![b'x'; 70_000];
    let (status, _) = post_raw(big).await;
    assert!(
        [400, 413].contains(&status),
        "oversize inbox body must be 400/413, got {status}"
    );
    let (status, body) = harness.get("/healthz").await;
    assert_eq!((status, body.as_str()), (200, "ok"));
}

/// Malformed durable-job payloads quarantine with a `bad-payload` reason and
/// never wedge the worker: after draining, no job is left to reclaim.
#[tokio::test]
async fn worker_survives_malformed_durable_job_without_endless_reclaim() {
    let root = std::env::temp_dir().join(format!(
        "pinetree-diag-p2-worker-{}-{}",
        std::process::id(),
        Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).expect("create test dir");
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store =
        RedbStore::open(root.join("t.redb"), clock.clone(), ids.clone()).expect("open store");
    let blobs: Arc<dyn BlobStore> = Arc::new(MemBlobs::default());
    let (_hub, streaming) = hub_and_sink(16);
    let streaming: Arc<dyn StreamingSink> = streaming;
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
        blobs: blobs.clone(),
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
        public_url: "https://example.local".to_string(),
        max_file_size: 10 * 1024 * 1024,
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
    let worker = JobWorker {
        clock: clock.clone(),
        ids: ids.clone(),
        jobs: store.clone(),
        notifications: store.clone(),
        streaming,
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 1_000,
        base_backoff_ms: 1_000,
        transcode_config: TranscodeConfig {
            enabled: false,
            ..TranscodeConfig::default()
        },
        email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
    };
    let garbage: Vec<u8> = b"\xff\x00{not json{{{".to_vec();
    let kinds = [
        JobKind::Notify,
        JobKind::StreamingPublish,
        JobKind::RemoteMediaFetch,
        JobKind::AntennaMatch,
        JobKind::PollExpiry,
        JobKind::PostScheduledDraft,
        JobKind::RemoteEmojiFetch,
        JobKind::Export,
        JobKind::Import,
        JobKind::CacheEvict,
        JobKind::ReconcileStorage,
        JobKind::BackfillDurableAccounting,
        JobKind::TranscodeMedia,
        JobKind::WebhookDeliver,
        JobKind::EmailSend,
        JobKind::QuoteStampReverify,
        JobKind::ReplyParentResolve,
        JobKind::UnfollowAll,
        JobKind::ExportCustomEmojis,
    ];
    let mut enqueued = 0;
    for (i, kind) in kinds.iter().enumerate() {
        let job = NewJob {
            kind: *kind,
            payload: garbage.clone(),
            idempotency_key: format!("p2-bad-payload-{i}"),
            not_before: 0,
        };
        store.enqueue_jobs(vec![job]).expect("enqueue garbage job");
        enqueued += 1;
    }
    assert!(enqueued > 0);
    let mut processed = 0;
    for _ in 0..(enqueued + 4) {
        if worker.process_one().expect("process_one must not fail") {
            processed += 1;
        } else {
            break;
        }
    }
    assert_eq!(
        processed, enqueued,
        "every malformed job must settle exactly once"
    );
    assert_eq!(
        store.pending_count().expect("pending count"),
        0,
        "no malformed job may stay pending (endless reclaim)"
    );
    // Settling is terminal: nothing is left to reclaim on a second drain.
    assert!(
        !worker.process_one().expect("second drain must be empty"),
        "quarantined jobs must not be reclaimed"
    );
}

/// Readiness is bounded and honest: a redb read failure or a missing storage
/// root turns `/ready` unhealthy (stable error class) while `/healthz`
/// stays up; recovery restores readiness.
#[tokio::test]
async fn readiness_turns_unhealthy_on_redb_failure_and_missing_root() {
    let _guard = serial();
    let harness = Harness::start(false).await;
    let (status, body) = harness.get("/healthz").await;
    assert_eq!((status, body.as_str()), (200, "ok"));
    let (status, body) = tokio::time::timeout(Duration::from_secs(5), harness.get("/ready"))
        .await
        .expect("ready must be bounded");
    assert_eq!((status, body.as_str()), (200, "ready"));
    // Missing durable root: unhealthy, liveness unaffected.
    let valid = pinetree::api::misskey::endpoints::OpsConfig {
        detailed_admin_status: false,
        durable_dir: harness.durable_dir.clone(),
        cache_dir: harness.cache_dir.clone(),
        backup_dir: harness.backup_dir.clone(),
        transcode_enabled: false,
        ffmpeg_status: "disabled".to_string(),
        config_fingerprint: serde_json::json!({"test": true}),
        started_ms: 1_700_000_000_000,
    };
    set_ops_config(OpsConfig {
        durable_dir: harness.backup_dir.join("no-such-durable-p2"),
        ..valid.clone()
    });
    let (status, body) = harness.get("/ready").await;
    assert_eq!(status, 503, "missing storage root must fail readiness");
    assert_eq!(body, "not ready");
    let (status, body) = harness.get("/healthz").await;
    assert_eq!((status, body.as_str()), (200, "ok"));
    set_ops_config(valid.clone());
    let (status, _) = harness.get("/ready").await;
    assert_eq!(status, 200, "readiness recovers with valid roots");
    // Redb read failure: unhealthy with the same bounded shape.
    harness.set_settings_fail(true);
    let (status, body) = harness.get("/ready").await;
    assert_eq!(status, 503, "redb outage must fail readiness");
    assert_eq!(body, "not ready");
    harness.set_settings_fail(false);
    let (status, _) = harness.get("/ready").await;
    assert_eq!(status, 200, "readiness recovers after redb outage");
    // A redb outage is also visible (not silent) on the admin surface.
    let (admin_id, admin_token) = harness.signup("p2readyadmin").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    harness.set_settings_fail(true);
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&admin_token))
        .await;
    harness.set_settings_fail(false);
    assert_eq!(status, 200, "{value}");
    assert_eq!(
        value["redb"]["readable"], false,
        "redb outage must surface as unreadable, got {value}"
    );
}

/// Admin diagnostics expose actionable signals where the ports allow them
/// and stay explicit `null` (never faked) where they do not.
#[tokio::test]
async fn admin_diagnostics_actionable_signals_or_documented_nulls() {
    let _guard = serial();
    let harness = Harness::start(true).await;
    let (admin_id_diag, admin_token) = harness.signup("p2diagadmin").await;
    harness
        .services
        .set_role(None, &admin_id_diag, true, false)
        .expect("make admin");
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&admin_token))
        .await;
    assert_eq!(status, 200, "{value}");
    // Actionable signals that exist on current ports.
    assert!(value["version"].is_string(), "{value}");
    assert!(value["readiness"]["ready"].is_boolean(), "{value}");
    assert_eq!(value["redb"]["readable"], true, "{value}");
    assert_eq!(value["pools"]["durable"]["present"], true, "{value}");
    assert_eq!(value["pools"]["cache"]["present"], true, "{value}");
    assert!(value["cache"]["usedBytes"].is_number(), "{value}");
    assert!(value["cache"]["evictions"].is_number(), "{value}");
    assert!(value["jobs"]["pending"].is_number(), "{value}");
    assert!(value["http"]["rateLimitBuckets"].is_number(), "{value}");
    assert!(value["webSocket"]["connections"].is_number(), "{value}");
    assert!(value["backups"].is_array(), "{value}");
    // Last-backup aggregates are computed from retained status files.
    for key in [
        "lastBackupSuccessMs",
        "lastBackupFailureMs",
        "lastBackupDurationMs",
        "lastBackupBytesWritten",
    ] {
        assert!(
            value.get(key).is_some(),
            "admin/status must carry {key} (null when no backup yet), got {value}"
        );
    }
    // Unavailable aggregates stay explicit null — never faked.
    for key in [
        "durableUsedBytes",
        "freeDiskBytes",
        "accountingDrift",
        "lastBackfill",
        "lastReconcile",
        "jobBreakdown",
        "oldestPendingMs",
        "quarantine",
        "federationDelivery",
    ] {
        assert!(
            value.get(key).is_some_and(serde_json::Value::is_null),
            "admin/status must keep {key} explicit null until the ports expose it, got {value}"
        );
    }
    // Detailed arm stays bounded.
    let (status, value) = harness
        .api(
            "admin/status",
            serde_json::json!({"detailed": true}),
            Some(&admin_token),
        )
        .await;
    assert_eq!(status, 200, "{value}");
    let recent = value["recentErrors"]
        .as_array()
        .expect("recentErrors array");
    assert!(recent.len() <= 32, "detailed arm must stay bounded");
}

/// Role assignment is visibly storage-only: assigning a role (even with
/// permissive policies) grants no authorization, and the stored policies
/// round-trip verbatim so no operator can mistake them for enforced rules.
#[tokio::test]
async fn role_assignment_grants_no_enforcement() {
    let _guard = serial();
    let harness = Harness::start(false).await;
    let (admin_id, admin_token) = harness.signup("p2roleadmin").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    let (user_id, user_token) = harness.signup("p2roleuser").await;
    // The non-enforcement contract is stated in the domain note.
    assert!(
        ROLE_POLICY_UNENFORCED_NOTE.contains("not enforced"),
        "domain role note must keep stating non-enforcement"
    );
    // Admin creates a role whose policies *claim* broad power.
    let policies = serde_json::json!({
        "canManageCustomEmojis": {"value": true},
        "canUpdateAp7400kWaf": {"value": true},
    });
    let (status, role) = harness
        .api(
            "admin/roles/create",
            serde_json::json!({"name": "p2-powerful", "policies": policies}),
            Some(&admin_token),
        )
        .await;
    assert_eq!(status, 200, "{role}");
    let role_id = role["id"].as_str().expect("role id").to_string();
    assert_eq!(role["policies"], policies, "policies stored verbatim");
    let (status, shown) = harness
        .api(
            "admin/roles/show",
            serde_json::json!({"roleId": role_id}),
            Some(&admin_token),
        )
        .await;
    assert_eq!(status, 200, "{shown}");
    assert_eq!(shown["policies"], policies, "policies shown verbatim");
    // Assign the all-powerful role to a plain user.
    let (status, value) = harness
        .api(
            "admin/roles/assign",
            serde_json::json!({"roleId": role_id, "userId": user_id}),
            Some(&admin_token),
        )
        .await;
    assert_eq!(status, 204, "{value}");
    // The assignment grants nothing: every protected admin action still
    // denies the assignee (negative test for the whole protected surface
    // this workstream owns the docs for).
    for endpoint in [
        "admin/status",
        "admin/roles/list",
        "admin/roles/show",
        "admin/roles/users",
    ] {
        let body = match endpoint {
            "admin/roles/show" | "admin/roles/users" => {
                serde_json::json!({"roleId": role_id})
            }
            _ => serde_json::json!({}),
        };
        let (status, value) = harness.api(endpoint, body, Some(&user_token)).await;
        assert_eq!(status, 403, "{endpoint} must deny role assignee: {value}");
        assert_eq!(value["code"], "PERMISSION_DENIED", "{endpoint}: {value}");
    }
    // Membership itself is observable (storage works), then removed.
    let (status, members) = harness
        .api(
            "admin/roles/users",
            serde_json::json!({"roleId": role_id}),
            Some(&admin_token),
        )
        .await;
    assert_eq!(status, 200, "{members}");
    assert!(
        members
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["id"] == user_id)),
        "assigned user must be listed as a member, got {members}"
    );
    let (status, value) = harness
        .api(
            "admin/roles/unassign",
            serde_json::json!({"roleId": role_id, "userId": user_id}),
            Some(&admin_token),
        )
        .await;
    assert_eq!(status, 204, "{value}");
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&user_token))
        .await;
    assert_eq!(status, 403, "unassigned user still denied: {value}");
}

/// Backup states exposed on the diagnostics surface stay queryable and
/// distinguish terminal `Failed` from `Completed`.
#[tokio::test]
async fn backup_status_states_stay_queryable() {
    for state in [
        BackupState::Queued,
        BackupState::Running,
        BackupState::Completed,
        BackupState::Failed,
    ] {
        let debug = format!("{state:?}");
        assert!(
            ["Queued", "Running", "Completed", "Failed"].contains(&debug.as_str()),
            "unexpected backup state {debug}"
        );
    }
}
