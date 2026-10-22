//! Batch G ingress limits (prod-ready.md §8): the 64 KiB ActivityPub inbox
//! cap is enforced at body-extraction time (per-route `DefaultBodyLimit`),
//! Misskey JSON keeps its 1 MiB cap, and multipart uploads stay on the
//! bounded-streaming + semaphore-gated path.
//!
//! Hermetic: every test drives a loopback server over raw TCP (full control
//! of framing: content-length, chunked, dishonest lengths) with per-op
//! timeouts, so a regression fails loudly instead of hanging. No external
//! network and no real federation signatures: under-cap requests are
//! asserted on the pre-auth gates (400/401), oversized ones on 413.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

use pinetree::api::misskey::endpoints::{MAX_JSON_BODY_BYTES, UPLOAD_OVERHEAD_BYTES};
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{BlobStore, Clock, IdGenerator, PortError};
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::inbox::MAX_INBOX_BODY_BYTES;
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};
use pinetree::streaming::hub::hub_and_sink;

/// In-memory blob store: ingress tests never need durable bytes.
#[derive(Default)]
struct MemBlobs {
    inner: Mutex<HashMap<String, Vec<u8>>>,
}

impl BlobStore for MemBlobs {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        self.inner
            .lock()
            .expect("blob lock")
            .insert(hash.to_string(), bytes.to_vec());
        Ok(())
    }
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        Ok(self.inner.lock().expect("blob lock").get(hash).cloned())
    }
    fn delete(&self, hash: &str) -> Result<(), PortError> {
        self.inner.lock().expect("blob lock").remove(hash);
        Ok(())
    }
}

struct TestServer {
    addr: std::net::SocketAddr,
}

impl TestServer {
    async fn start(tag: &str, max_file_size: u64) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinetree-ingress-g1-{}-{tag}-{u}",
            std::process::id(),
            u = Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("t.redb"), clock.clone(), ids.clone())
            .expect("open test store");
        let blobs: Arc<dyn BlobStore> = Arc::new(MemBlobs::default());
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
            blobs: blobs.clone(),
            notifications: store.clone(),
            auth: store.clone(),
            streaming: streaming.clone(),
            dedup: store.clone(),
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
            max_file_size,
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
            transcode_config: TranscodeConfig::default(),
            email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
        });
        // No worker spawn: every assertion below resolves before dispatch.
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
        let listener = TcpListener::bind("127.0.0.1:0")
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
        Self { addr }
    }

    /// Send one raw HTTP/1.1 request and return `(status, body)`. `head` is
    /// the request line plus headers ending in the blank line; `body` is
    /// written verbatim so framing (content-length, chunked, lies) is fully
    /// ours. No FIN is sent: the server answers from the framed request and
    /// the reply is read framed (head + `Content-Length`, EOF-terminated).
    /// `Connection: close` is deliberately NOT requested.
    async fn raw(&self, head: &str, body: &[u8]) -> (u16, Vec<u8>) {
        let (mut rd, _wr) = self.connect_write(head, body).await;
        Self::read_framed(&mut rd).await
    }

    /// Like [`Self::raw`], but half-closes the write side after the body so
    /// a TRUNCATED delivery reads as EOF server-side instead of hanging it.
    /// Only for short-body cases: the FIN races not-yet-flushed app
    /// responses on loopback, so honest-length requests must use [`Self::raw`].
    async fn raw_fin(&self, head: &str, body: &[u8]) -> (u16, Vec<u8>) {
        let (mut rd, mut wr) = self.connect_write(head, body).await;
        wr.shutdown().await.expect("shutdown write half");
        Self::read_framed(&mut rd).await
    }

    async fn connect_write(
        &self,
        head: &str,
        body: &[u8],
    ) -> (
        tokio::net::tcp::OwnedReadHalf,
        tokio::net::tcp::OwnedWriteHalf,
    ) {
        let stream = tokio::time::timeout(Duration::from_secs(15), TcpStream::connect(self.addr))
            .await
            .expect("connect times out")
            .expect("connect test server");
        // Split first: `TcpStream::shutdown()` closes both halves.
        let (rd, mut wr) = stream.into_split();
        tokio::time::timeout(Duration::from_secs(15), wr.write_all(head.as_bytes()))
            .await
            .expect("write head times out")
            .expect("write head");
        tokio::time::timeout(Duration::from_secs(15), wr.write_all(body))
            .await
            .expect("write body times out")
            .expect("write body");
        (rd, wr)
    }

    async fn read_framed(rd: &mut tokio::net::tcp::OwnedReadHalf) -> (u16, Vec<u8>) {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 65536];
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut head_len: Option<usize> = None;
        let mut want_body: Option<usize> = None;
        loop {
            if let (Some(hl), Some(want)) = (head_len, want_body)
                && buf.len() >= hl + want
            {
                break;
            }
            if buf.len() > 8 * 1024 * 1024 {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                panic!("read response times out");
            }
            let n = tokio::time::timeout(remaining, rd.read(&mut tmp))
                .await
                .expect("read times out")
                .expect("read response");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if head_len.is_none()
                && let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n")
            {
                let hl = i + 4;
                head_len = Some(hl);
                want_body = Some(response_content_length(&buf[..hl]).unwrap_or(0));
            }
        }
        split_response(&buf)
    }
}

fn response_content_length(head: &[u8]) -> Option<usize> {
    let text = String::from_utf8_lossy(head);
    for line in text.lines().skip(1) {
        let line = line.trim();
        if line.is_empty() {
            break;
        }
        if let Some(value) = line.split_once(':')
            && value.0.trim().eq_ignore_ascii_case("content-length")
        {
            return value.1.trim().parse().ok();
        }
    }
    None
}

fn split_response(raw: &[u8]) -> (u16, Vec<u8>) {
    let status: u16 = String::from_utf8_lossy(raw)
        .lines()
        .next()
        .expect("empty response")
        .split_whitespace()
        .nth(1)
        .expect("no status code")
        .parse()
        .expect("status parses");
    let sep = b"\r\n\r\n";
    let body = raw
        .windows(sep.len())
        .position(|window| window == sep)
        .map(|i| raw[i + sep.len()..].to_vec())
        .unwrap_or_default();
    (status, body)
}

const AP_JSON: &str = "application/activity+json";

/// `POST` head with an explicit `Content-Length` (the value may be a lie;
/// that is the point of the dishonest-length tests).
fn head_with_length(path: &str, content_type: &str, declared: usize) -> String {
    // No `Connection: close`: with an immediate client FIN it makes hyper
    // drop not-yet-flushed app responses. Framed reads + server close on
    // rejections give us everything the assertions need.
    format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: {content_type}\r\n\
         Content-Length: {declared}\r\n\r\n"
    )
}

fn head_chunked(path: &str, content_type: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: {content_type}\r\n\
         Transfer-Encoding: chunked\r\n\r\n"
    )
}

/// One-chunk-per-entry chunked framing for the given parts (no length header
/// anywhere: the server must enforce the cap on actual streamed bytes).
fn chunked(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for part in parts {
        out.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"0\r\n\r\n");
    out
}

const MULTIPART_BOUNDARY: &str = "g1boundary";

fn multipart_upload(file: &[u8]) -> (String, Vec<u8>) {
    let content_type = format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}");
    let mut body = Vec::new();
    body.extend_from_slice(
        format!(
            "--{MULTIPART_BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; \
             filename=\"big.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(file);
    body.extend_from_slice(format!("\r\n--{MULTIPART_BOUNDARY}--\r\n").as_bytes());
    (content_type, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbox_content_length_over_cap_is_413() {
    let server = TestServer::start("cl-over", 10 * 1024 * 1024).await;
    let body = vec![b'x'; MAX_INBOX_BODY_BYTES + 1024];
    let (status, _) = server
        .raw(&head_with_length("/inbox", AP_JSON, body.len()), &body)
        .await;
    assert_eq!(status, 413, "content-length over 64 KiB must be 413");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbox_chunked_body_without_length_is_413() {
    let server = TestServer::start("chunked", 10 * 1024 * 1024).await;
    let big = vec![b'y'; MAX_INBOX_BODY_BYTES + 512];
    let frame = chunked(&[&big[..MAX_INBOX_BODY_BYTES], &big[MAX_INBOX_BODY_BYTES..]]);
    let (status, _) = server.raw(&head_chunked("/inbox", AP_JSON), &frame).await;
    assert_eq!(
        status, 413,
        "chunked body over 64 KiB with no length must be 413"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbox_dishonest_lengths_follow_framing_without_hang() {
    let server = TestServer::start("dishonest", 10 * 1024 * 1024).await;
    // Lie 1: claims 100 bytes but streams 100 plus 66 KiB more. Framing
    // delivers the first 100 (not JSON) → 400; the extra bytes cannot
    // smuggle an oversized activity past extraction on this request.
    let mut smuggled = vec![b'x'; 100];
    smuggled.extend_from_slice(&vec![b'y'; MAX_INBOX_BODY_BYTES + 1024]);
    let (status, _) = server
        .raw(&head_with_length("/inbox", AP_JSON, 100), &smuggled)
        .await;
    assert_eq!(status, 400, "small-framed prefix is not JSON");
    // Lie 2: claims 66 KiB but delivers 100 bytes then EOF. The truncated
    // body is a 400, never a hang and never a bypass. This is the one case
    // that needs a real FIN (otherwise the server waits for the rest).
    let short = vec![b'x'; 100];
    let (status, _) = server
        .raw_fin(
            &head_with_length("/inbox", AP_JSON, MAX_INBOX_BODY_BYTES + 1024),
            &short,
        )
        .await;
    assert_eq!(status, 400, "truncated body is a 400");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbox_invalid_json_under_cap_keeps_400() {
    let server = TestServer::start("bad-json", 10 * 1024 * 1024).await;
    let body = b"{not json".to_vec();
    let (status, _) = server
        .raw(&head_with_length("/inbox", AP_JSON, body.len()), &body)
        .await;
    assert_eq!(status, 400, "under-cap garbage keeps inbox 400 semantics");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbox_small_unsigned_activity_reaches_auth_gate() {
    let server = TestServer::start("auth-gate", 10 * 1024 * 1024).await;
    let body = br#"{"type":"Follow","id":"https://remote.example/acts/1","actor":"https://remote.example/users/a"}"#.to_vec();
    let (status, _) = server
        .raw(&head_with_length("/inbox", AP_JSON, body.len()), &body)
        .await;
    assert_eq!(
        status, 401,
        "under-cap well-formed activity must reach the signature gate"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbox_boundary_at_cap_passes_but_one_more_byte_is_413() {
    let server = TestServer::start("boundary", 10 * 1024 * 1024).await;
    let at_cap = vec![b'x'; MAX_INBOX_BODY_BYTES];
    let (status, _) = server
        .raw(&head_with_length("/inbox", AP_JSON, at_cap.len()), &at_cap)
        .await;
    assert_ne!(status, 413, "exactly 64 KiB must pass extraction");
    assert_eq!(status, 400, "exactly 64 KiB of non-JSON is a 400");
    let over = vec![b'x'; MAX_INBOX_BODY_BYTES + 1];
    let (status, _) = server
        .raw(&head_with_length("/inbox", AP_JSON, over.len()), &over)
        .await;
    assert_eq!(status, 413, "64 KiB + 1 byte must be 413");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbox_concurrent_oversized_requests_all_413() {
    // Bounded memory comes from the extraction layer (`Limited` aborts past
    // the cap, so no request ever buffers its full body); this burst proves
    // the behavior holds under concurrency on both inbox routes, including
    // the personal route whose account lookup must not run first.
    let server = Arc::new(TestServer::start("burst", 10 * 1024 * 1024).await);
    let mut handles = Vec::new();
    for i in 0..32 {
        let owned = server.clone();
        let path = if i % 2 == 0 {
            "/inbox".to_string()
        } else {
            "/users/does-not-exist/inbox".to_string()
        };
        handles.push(tokio::spawn(async move {
            let body = vec![b'z'; MAX_INBOX_BODY_BYTES + 777];
            owned
                .raw(&head_with_length(&path, AP_JSON, body.len()), &body)
                .await
                .0
        }));
    }
    for handle in handles {
        assert_eq!(
            handle.await.expect("request task"),
            413,
            "every concurrent oversized inbox request must be 413"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn misskey_json_over_1mib_keeps_misskey_413() {
    let server = TestServer::start("json-over", 10 * 1024 * 1024).await;
    let pad = "x".repeat(MAX_JSON_BODY_BYTES + 64);
    let body = format!(r#"{{"pad":"{pad}"}}"#).into_bytes();
    assert!(
        body.len() < 2 * 1024 * 1024,
        "test body must stay under the 2 MiB framework default"
    );
    let (status, raw_body) = server
        .raw(
            &head_with_length("/api/meta", "application/json", body.len()),
            &body,
        )
        .await;
    assert_eq!(status, 413, "JSON over 1 MiB must be 413");
    let parsed: serde_json::Value =
        serde_json::from_slice(&raw_body).expect("413 carries a Misskey error body");
    assert_eq!(
        parsed["code"], "MAX_FILE_SIZE_EXCEEDED",
        "oversized JSON keeps the Misskey error code"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn misskey_json_over_framework_default_still_413() {
    let server = TestServer::start("json-huge", 10 * 1024 * 1024).await;
    let body = format!(r#"{{"pad":"{}"}}"#, "x".repeat(2_200_000)).into_bytes();
    let (status, _) = server
        .raw(
            &head_with_length("/api/meta", "application/json", body.len()),
            &body,
        )
        .await;
    assert_eq!(status, 413, "JSON over the 2 MiB default must still be 413");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn misskey_small_meta_keeps_200() {
    let server = TestServer::start("json-ok", 10 * 1024 * 1024).await;
    let body = br#"{}"#.to_vec();
    let (status, raw_body) = server
        .raw(
            &head_with_length("/api/meta", "application/json", body.len()),
            &body,
        )
        .await;
    assert_eq!(status, 200, "small meta call keeps Misskey semantics");
    assert!(
        serde_json::from_slice::<serde_json::Value>(&raw_body).is_ok(),
        "meta answers JSON"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_oversized_multipart_is_413_before_auth() {
    // `max_file_size = 4096`, so the streaming cap is 4096 +
    // `UPLOAD_OVERHEAD_BYTES`; the 1.2 MiB file field crosses it without
    // any credential presented, proving the bounded read precedes auth.
    let server = TestServer::start("upload-over", 4096).await;
    let (content_type, body) = multipart_upload(&vec![b'a'; 1_200_000]);
    assert!(
        body.len() > 4096 + UPLOAD_OVERHEAD_BYTES,
        "test upload must cross the streaming cap"
    );
    let (status, _) = server
        .raw(
            &head_with_length("/api/drive/files/create", &content_type, body.len()),
            &body,
        )
        .await;
    assert_eq!(status, 413, "oversized multipart must be 413");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_small_multipart_reaches_auth_gate() {
    let server = TestServer::start("upload-auth", 4096).await;
    let (content_type, body) = multipart_upload(b"tiny");
    let (status, _) = server
        .raw(
            &head_with_length("/api/drive/files/create", &content_type, body.len()),
            &body,
        )
        .await;
    assert_eq!(
        status, 401,
        "under-cap credential-less upload must reach the auth gate"
    );
}

const INBOX_SRC: &str = include_str!("../src/federation/inbox/mod.rs");
const INBOX_COMMON_SRC: &str = include_str!("../src/federation/inbox/common.rs");
const INBOX_CREATE_SRC: &str = include_str!("../src/federation/inbox/create.rs");
const INBOX_UPDATE_SRC: &str = include_str!("../src/federation/inbox/update.rs");
const INBOX_DELETE_SRC: &str = include_str!("../src/federation/inbox/delete.rs");
const INBOX_LIKE_SRC: &str = include_str!("../src/federation/inbox/like.rs");
const INBOX_ANNOUNCE_SRC: &str = include_str!("../src/federation/inbox/announce.rs");
const INBOX_FOLLOW_SRC: &str = include_str!("../src/federation/inbox/follow.rs");
const INBOX_FLAG_SRC: &str = include_str!("../src/federation/inbox/flag.rs");
const INBOX_VOTE_SRC: &str = include_str!("../src/federation/inbox/vote.rs");
const ROUTER_SRC: &str = include_str!("../src/api/misskey/endpoints/mod.rs");
const FILES_SRC: &str = include_str!("../src/api/misskey/endpoints/files_http.rs");
const RESPONSE_SRC: &str = include_str!("../src/api/misskey/response/upload_slots.rs");
const ACTOR_SRC: &str = include_str!("../src/activitypub/actor.rs");
const ACTOR_REMOTE_SRC: &str = include_str!("../src/activitypub/actor/remote_actor.rs");
const ACTOR_DOCUMENT_SRC: &str = include_str!("../src/activitypub/actor/actor_document.rs");
const ACTOR_COLLECTIONS_SRC: &str = include_str!("../src/activitypub/actor/collections.rs");
const ACTOR_OUTBOX_SRC: &str = include_str!("../src/activitypub/actor/outbox.rs");
const WEBFINGER_SRC: &str = include_str!("../src/activitypub/webfinger.rs");
const WEBFINGER_RESPONSES_SRC: &str = include_str!("../src/activitypub/webfinger/responses.rs");
const WEBFINGER_HOST_META_SRC: &str = include_str!("../src/activitypub/webfinger/host_meta.rs");
const WEBFINGER_FINGER_SRC: &str = include_str!("../src/activitypub/webfinger/finger.rs");
const WEBFINGER_NODEINFO_SRC: &str = include_str!("../src/activitypub/webfinger/nodeinfo.rs");

#[test]
fn inbox_cap_value_and_router_wiring() {
    assert_eq!(
        MAX_INBOX_BODY_BYTES,
        64 * 1024,
        "inbox business cap stays 64 KiB"
    );
    assert!(
        INBOX_SRC.contains("DefaultBodyLimit::max(MAX_INBOX_BODY_BYTES)"),
        "inbox.rs owns the extraction-layer cap constructor"
    );
    assert!(
        ROUTER_SRC.contains("fed_inbox::inbox_body_limit()"),
        "both inbox routes wire the per-route limit"
    );
    for (name, src) in [("inbox.rs", INBOX_SRC), ("endpoints/mod.rs", ROUTER_SRC)] {
        // Skip comment lines: the "must not disable" contract text itself
        // names the forbidden call.
        let code: String = src
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !code.contains("DefaultBodyLimit::disable()"),
            "{name} must not disable framework defaults"
        );
    }
}

#[test]
fn misskey_json_and_upload_caps_unchanged() {
    assert_eq!(
        MAX_JSON_BODY_BYTES, 1_048_576,
        "Misskey JSON cap stays 1 MiB"
    );
    assert_eq!(
        UPLOAD_OVERHEAD_BYTES, 1_048_576,
        "multipart framing overhead stays 1 MiB"
    );
    assert!(
        ROUTER_SRC.contains("if body.len() > MAX_JSON_BODY_BYTES"),
        "JSON dispatcher keeps its 1 MiB check"
    );
    assert!(
        FILES_SRC.contains("read_capped_body(body, upload_cap_bytes("),
        "uploads keep the bounded streaming read"
    );
    assert!(
        FILES_SRC.contains(".upload_slots") && FILES_SRC.contains(".acquire(&client_ip"),
        "uploads keep the semaphore gate"
    );
    assert!(
        RESPONSE_SRC.contains("MAX_CONCURRENT_UPLOADS_PER_IP")
            && RESPONSE_SRC.contains("MAX_CONCURRENT_UPLOADS_GLOBAL")
            && RESPONSE_SRC.contains("MAX_UPLOAD_SLOT_IPS")
            && RESPONSE_SRC.contains("try_acquire_owned"),
        "Batch D upload-slot bounds stay in place"
    );
}

#[test]
fn no_unbounded_body_collect_on_untrusted_routes() {
    // Every `to_bytes(..., usize::MAX)` in these files must live inside the
    // test module (after the first `#[cfg(test)]`); production code on
    // untrusted routes only collects through capped extractors / `Limited`.
    for (name, src) in [
        ("inbox/mod.rs", INBOX_SRC),
        ("inbox/common.rs", INBOX_COMMON_SRC),
        ("inbox/create.rs", INBOX_CREATE_SRC),
        ("inbox/update.rs", INBOX_UPDATE_SRC),
        ("inbox/delete.rs", INBOX_DELETE_SRC),
        ("inbox/like.rs", INBOX_LIKE_SRC),
        ("inbox/announce.rs", INBOX_ANNOUNCE_SRC),
        ("inbox/follow.rs", INBOX_FOLLOW_SRC),
        ("inbox/flag.rs", INBOX_FLAG_SRC),
        ("inbox/vote.rs", INBOX_VOTE_SRC),
        ("endpoints/mod.rs", ROUTER_SRC),
        ("endpoints/files_http.rs", FILES_SRC),
        ("actor/mod.rs", ACTOR_SRC),
        ("actor/remote_actor.rs", ACTOR_REMOTE_SRC),
        ("actor/actor_document.rs", ACTOR_DOCUMENT_SRC),
        ("actor/collections.rs", ACTOR_COLLECTIONS_SRC),
        ("actor/outbox.rs", ACTOR_OUTBOX_SRC),
        ("webfinger/mod.rs", WEBFINGER_SRC),
        ("webfinger/responses.rs", WEBFINGER_RESPONSES_SRC),
        ("webfinger/host_meta.rs", WEBFINGER_HOST_META_SRC),
        ("webfinger/finger.rs", WEBFINGER_FINGER_SRC),
        ("webfinger/nodeinfo.rs", WEBFINGER_NODEINFO_SRC),
    ] {
        let test_offset = src.find("#[cfg(test)]").unwrap_or(src.len());
        let (production, _) = src.split_at(test_offset);
        for (index, line) in production.lines().enumerate() {
            assert!(
                !(line.contains("to_bytes") && line.contains("usize::MAX")),
                "{name} production line {} collects an unbounded body: {line}",
                index + 1
            );
        }
    }
}
