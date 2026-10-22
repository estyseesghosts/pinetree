//! WP4 streaming-complete tests (gaps.md §28): new channels, new `main`
//! events, `reacted.emoji`, `userList` include flags, `pollEnded` compat.
//!
//! Covered against a live server + real WebSocket clients (same framing as
//! `tests/streaming.rs`): `hashtag` (tag match + bad-`q` refusal),
//! `roleTimeline` (connects, silent without a role model), `drive`
//! (fileCreated/fileUpdated/fileDeleted), `serverStats`/`queueStats`
//! (snapshot `stats` + `requestLog` → `statsLog`), `admin` (moderator-only,
//! `newAbuseUserReport`), `clip` (`updated`/`deleted`, null bodies),
//! `userList` `withFiles`/`withRenotes` filtering, `reacted.emoji`
//! (`null` for unicode, `{name, url}` for custom emoji), `pollEnded` as a
//! compat `noteUpdated` frame, every new `main` event wire name, and the
//! `reversi|channel|chatUser|chatRoom` `UNSUPPORTED_CHANNEL` decision.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::events::{MainEventKind, MainMessage, StreamingEvent};
use pinetree::application::ports::{BlobStore, Clock, IdGenerator, InsertMediaInput, PortError};
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};
use pinetree::streaming::hub::hub_and_sink;

#[derive(Default)]
struct MemBlobs {
    inner: Mutex<HashMap<String, Vec<u8>>>,
}

impl BlobStore for MemBlobs {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        self.inner
            .lock()
            .unwrap()
            .insert(hash.to_string(), bytes.to_vec());
        Ok(())
    }
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        Ok(self.inner.lock().unwrap().get(hash).cloned())
    }
    fn delete(&self, hash: &str) -> Result<(), PortError> {
        self.inner.lock().unwrap().remove(hash);
        Ok(())
    }
}

/// Minimal 1x1 PNG (only the 8-byte magic matters to the sniffer).
const TINY_PNG: &[u8] = &[
    0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 13, b'I', b'H', b'D', b'R', 0, 0, 0,
    1, 0, 0, 0, 1, 8, 6, 0, 0, 0, 0x1f, 0x15, 0xc4, 0x89, 0, 0, 0, 13, b'I', b'D', b'A', b'T',
    0x78, 0x9c, 0x63, 0x60, 0x60, 0x60, 0x00, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4,
    0, 0, 0, 0, b'I', b'E', b'N', b'D', 0xae, 0x42, 0x60, 0x82,
];

struct LiveServer {
    base: String,
    http: reqwest::Client,
    services: Arc<AppServices>,
}

impl LiveServer {
    async fn start() -> Self {
        let dir =
            std::env::temp_dir().join(format!("pinetree-gaps-stream-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("t.redb"), clock.clone(), ids.clone()).unwrap();
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
            streaming: streaming.clone(),
            dedup: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://stream.test".to_string(),
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
            transcode_config: TranscodeConfig::default(),
            email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
        });
        let _bg = worker.clone().spawn(Duration::from_millis(10));
        std::mem::forget(_bg);
        let state = ApiState {
            services: services.clone(),
            worker,
            rate_limiter: Arc::new(RateLimiter::with_misskey_defaults(clock.clone())),
            upload_slots: Arc::new(UploadSlots::with_defaults()),
            hub,
            trusted_proxies: Arc::new(Vec::new()),
            featured_cache: std::sync::Arc::new(
                pinetree::application::commands::pins::FeaturedCache::default(),
            ),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                build_router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        Self {
            base: format!("http://{addr}"),
            http: reqwest::Client::new(),
            services,
        }
    }

    fn ws_url(&self, query: &str) -> String {
        self.base.replace("http://", "ws://") + "/streaming" + query
    }

    async fn api(&self, endpoint: &str, body: Value, token: Option<&str>) -> (u16, Value) {
        let mut map = body.as_object().cloned().unwrap_or_default();
        if let Some(token) = token {
            map.insert("i".to_string(), Value::String(token.to_string()));
        }
        let res = self
            .http
            .post(format!("{}/api/{endpoint}", self.base))
            .json(&map)
            .send()
            .await
            .unwrap();
        let status = res.status().as_u16();
        let value: Value = res.json().await.unwrap_or(Value::Null);
        (status, value)
    }

    async fn signup(&self, username: &str) -> (String, String) {
        let res = self
            .http
            .post(format!("{}/signup", self.base))
            .json(&serde_json::json!({ "username": username, "password": "pw-pw-pw-pw" }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let value: Value = res.json().await.unwrap();
        (
            value["id"].as_str().unwrap().to_string(),
            value["token"].as_str().unwrap().to_string(),
        )
    }

    /// Insert a drive file row directly (metadata only; the packer only
    /// reads metadata, so no blob bytes are needed).
    fn make_file(&self, owner: Option<&str>, name: &str) -> String {
        let blob_key = pinetree::media::ingest::sha256_hex(TINY_PNG);
        self.services.blobs.put(&blob_key, TINY_PNG).unwrap();
        self.services
            .media
            .insert_metadata(InsertMediaInput {
                id: None,
                owner_id: owner.map(str::to_string),
                name: name.to_string(),
                mime: "image/png".to_string(),
                size_bytes: TINY_PNG.len() as u64,
                hash: pinetree::media::ingest::md5_hex(TINY_PNG),
                blob_key,
                blurhash: None,
                properties: MediaProperties::default(),
                is_sensitive: false,
                is_link: false,
                url: format!("https://stream.test/files/{name}"),
                thumbnail_url: None,
                comment: None,
                source_url: None,
                max_file_size: 10 * 1024 * 1024,
            })
            .unwrap()
            .id
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(url: &str) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("ws connect");
    ws
}

async fn send(ws: &mut Ws, msg: Value) {
    ws.send(Message::Text(msg.to_string().into()))
        .await
        .unwrap();
}

async fn recv(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for stream frame")
            .expect("stream closed")
            .expect("ws error");
        match msg {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

async fn recv_nothing(ws: &mut Ws) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(400);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return;
        }
        let res = tokio::time::timeout(left, ws.next()).await;
        match res {
            Err(_) => return,
            Ok(None) => return,
            Ok(Some(Err(_))) => return,
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            Ok(other) => panic!("expected silence, got {other:?}"),
        }
    }
}

fn channel_body(frame: &Value) -> (&str, &str, &Value) {
    assert_eq!(frame["type"], "channel", "not a channel frame: {frame}");
    let id = frame["body"]["id"].as_str().unwrap();
    let event = frame["body"]["type"].as_str().unwrap();
    let body = &frame["body"]["body"];
    (id, event, body)
}

fn publish(services: &AppServices, event: &StreamingEvent) {
    services
        .streaming
        .publish("streaming", &serde_json::to_vec(event).unwrap())
        .unwrap();
}

/// Let the server process prior `connect`/`subNote` frames before a direct
/// hub publish lands (direct publishes skip the HTTP+worker delay that API
/// flows get for free; without this the event can arrive before the
/// subscription exists and is dropped).
async fn settle() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

/// Next `channel` frame with the given id+event, skipping unrelated frames
/// (bounded; panics when the event never arrives).
async fn recv_channel(ws: &mut Ws, id: &str, event: &str) -> Value {
    for _ in 0..8 {
        let frame = recv(ws).await;
        if frame["type"] == "channel" && frame["body"]["id"] == id && frame["body"]["type"] == event
        {
            return frame;
        }
    }
    panic!("missing channel event {event} on {id}");
}

/// Next top-level `noteUpdated` frame with the given id+type, skipping
/// unrelated frames (e.g. `emojiAdded` broadcasts, bounded).
async fn recv_note_updated(ws: &mut Ws, note_id: &str, event: &str) -> Value {
    for _ in 0..8 {
        let frame = recv(ws).await;
        if frame["type"] == "noteUpdated"
            && frame["body"]["id"] == note_id
            && frame["body"]["type"] == event
        {
            return frame;
        }
    }
    panic!("missing noteUpdated {event} for {note_id}");
}

#[tokio::test]
async fn hashtag_channel_matches_tags_and_rejects_bad_q() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;

    let mut rust_ws = connect(&srv.ws_url("")).await;
    send(
        &mut rust_ws,
        serde_json::json!({
            "type": "connect",
            "body": { "channel": "hashtag", "id": "r", "params": { "q": [["rust"]] } },
        }),
    )
    .await;
    // AND-of-ORs: needs both tags.
    let mut both_ws = connect(&srv.ws_url("")).await;
    send(
        &mut both_ws,
        serde_json::json!({
            "type": "connect",
            "body": { "channel": "hashtag", "id": "b", "params": { "q": [["rust", "async"]] } },
        }),
    )
    .await;
    // Malformed `q` (not an array of string arrays): silently refused —
    // never an `UNSUPPORTED_CHANNEL` (reserved for unknown names).
    let mut bad_ws = connect(&srv.ws_url("")).await;
    send(
        &mut bad_ws,
        serde_json::json!({
            "type": "connect",
            "body": { "channel": "hashtag", "id": "x", "params": { "q": "rust" } },
        }),
    )
    .await;

    let (status, created) = srv
        .api(
            "notes/create",
            serde_json::json!({ "text": "hello #rust world" }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();

    let frame = recv(&mut rust_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("r", "note"));
    assert_eq!(body["id"], note_id);
    // Single-tag note does not satisfy the two-tag AND.
    recv_nothing(&mut both_ws).await;
    recv_nothing(&mut bad_ws).await;
}

#[tokio::test]
async fn decision_channels_stay_unsupported() {
    let srv = LiveServer::start().await;
    let mut ws = connect(&srv.ws_url("")).await;
    // reversi/*, channel, chatUser, chatRoom stay UNSUPPORTED_CHANNEL by
    // decision (no channels/chat/Reversi model — see module docs).
    for (i, channel) in ["reversi", "reversiGame", "channel", "chatUser", "chatRoom"]
        .iter()
        .enumerate()
    {
        let id = format!("u{i}");
        send(
            &mut ws,
            serde_json::json!({
                "type": "connect", "body": { "channel": channel, "id": id },
            }),
        )
        .await;
        let frame = recv(&mut ws).await;
        assert_eq!(frame["type"], "error", "{channel}");
        assert_eq!(frame["body"]["code"], "UNSUPPORTED_CHANNEL", "{channel}");
        assert_eq!(frame["body"]["channel"], *channel);
    }
}

#[tokio::test]
async fn role_timeline_connects_but_stays_silent_without_roles() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;

    let mut ws = connect(&srv.ws_url("")).await;
    send(&mut ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "roleTimeline", "id": "r", "params": { "roleId": "admin" }, "pong": true },
    })).await;
    // The channel is valid (pong ack), but pinetree has no role model, so
    // no producer publishes into it — silence, not an error.
    let frame = recv(&mut ws).await;
    assert_eq!(frame["type"], "connected");

    let (status, _) = srv
        .api(
            "notes/create",
            serde_json::json!({ "text": "public" }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    recv_nothing(&mut ws).await;
}

#[tokio::test]
async fn drive_channel_delivers_file_events_to_owner() {
    let srv = LiveServer::start().await;
    let (alice_id, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    let mut alice_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(
        &mut alice_ws,
        serde_json::json!({
            "type": "connect", "body": { "channel": "drive", "id": "d" },
        }),
    )
    .await;
    // Anonymous `drive`: a credential channel, silently ignored.
    let mut anon_ws = connect(&srv.ws_url("")).await;
    send(
        &mut anon_ws,
        serde_json::json!({
            "type": "connect", "body": { "channel": "drive", "id": "a", "pong": true },
        }),
    )
    .await;
    recv_nothing(&mut anon_ws).await;

    let file_id = srv.make_file(Some(&alice_id), "pic.png");
    settle().await;
    publish(
        &srv.services,
        &StreamingEvent::DriveFileCreated {
            user_id: alice_id.clone(),
            file_id: file_id.clone(),
        },
    );
    let frame = recv(&mut alice_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("d", "fileCreated"));
    assert_eq!(body["id"], file_id);
    recv_nothing(&mut anon_ws).await;

    publish(
        &srv.services,
        &StreamingEvent::DriveFileUpdated {
            user_id: alice_id.clone(),
            file_id: file_id.clone(),
        },
    );
    let frame = recv(&mut alice_ws).await;
    let (_, event, _) = channel_body(&frame);
    assert_eq!(event, "fileUpdated");

    // Blob rows for another user never reach this socket.
    let bob_file = srv.make_file(Some("someone-else"), "other.png");
    let _ = bob_token;
    publish(
        &srv.services,
        &StreamingEvent::DriveFileCreated {
            user_id: "someone-else".to_string(),
            file_id: bob_file.clone(),
        },
    );
    recv_nothing(&mut alice_ws).await;

    publish(
        &srv.services,
        &StreamingEvent::DriveFileDeleted {
            user_id: alice_id.clone(),
            file_id: file_id.clone(),
        },
    );
    let frame = recv(&mut alice_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("d", "fileDeleted"));
    assert_eq!(body, &Value::String(file_id));
}

#[tokio::test]
async fn stats_channels_emit_snapshot_and_stats_log() {
    let srv = LiveServer::start().await;

    let mut ws = connect(&srv.ws_url("")).await;
    send(
        &mut ws,
        serde_json::json!({
            "type": "connect", "body": { "channel": "serverStats", "id": "s" },
        }),
    )
    .await;
    let frame = recv(&mut ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("s", "stats"));
    assert!(body.get("machine").is_some(), "{body}");
    assert!(body.get("cpu").is_some(), "{body}");
    assert!(body.get("mem").is_some(), "{body}");

    // `requestLog` on the channel answers one `statsLog` sample.
    send(&mut ws, serde_json::json!({
        "type": "ch", "body": { "id": "s", "type": "requestLog", "body": { "id": "log1", "length": 10 } },
    })).await;
    let frame = recv(&mut ws).await;
    let (id, event, _) = channel_body(&frame);
    assert_eq!((id, event), ("s", "statsLog"));

    let mut qw = connect(&srv.ws_url("")).await;
    send(
        &mut qw,
        serde_json::json!({
            "type": "connect", "body": { "channel": "queueStats", "id": "q" },
        }),
    )
    .await;
    let frame = recv(&mut qw).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("q", "stats"));
    assert!(
        body.get("waiting").and_then(Value::as_u64).is_some(),
        "{body}"
    );
    send(&mut qw, serde_json::json!({
        "type": "ch", "body": { "id": "q", "type": "requestLog", "body": { "id": "log2", "length": 5 } },
    })).await;
    let frame = recv(&mut qw).await;
    let (id, event, _) = channel_body(&frame);
    assert_eq!((id, event), ("q", "statsLog"));
}

#[tokio::test]
async fn admin_channel_needs_moderator_and_reports_abuse() {
    let srv = LiveServer::start().await;
    let (alice_id, alice_token) = srv.signup("alice").await;
    let (bob_id, bob_token) = srv.signup("bob").await;
    let (mod_id, mod_token) = srv.signup("carmod").await;
    srv.services
        .set_role(None, &mod_id, false, true)
        .expect("set role");

    // Non-moderator: silently refused even with pong (no `connected`).
    let mut alice_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(
        &mut alice_ws,
        serde_json::json!({
            "type": "connect", "body": { "channel": "admin", "id": "a", "pong": true },
        }),
    )
    .await;
    recv_nothing(&mut alice_ws).await;

    let mut mod_ws = connect(&srv.ws_url(&format!("?i={mod_token}"))).await;
    send(
        &mut mod_ws,
        serde_json::json!({
            "type": "connect", "body": { "channel": "admin", "id": "a", "pong": true },
        }),
    )
    .await;
    let frame = recv(&mut mod_ws).await;
    assert_eq!(frame["type"], "connected");

    // Bob reports Alice; the moderator socket gets `newAbuseUserReport`.
    let (status, report) = srv
        .api(
            "users/report-abuse",
            serde_json::json!({ "userId": alice_id, "comment": "spam" }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 200, "{report}");
    let report_id = report["id"].as_str().unwrap().to_string();
    let _ = bob_id;
    settle().await;
    publish(
        &srv.services,
        &StreamingEvent::AdminReport {
            report_id: report_id.clone(),
        },
    );
    let frame = recv(&mut mod_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("a", "newAbuseUserReport"));
    assert_eq!(body["id"], report_id);
    assert_eq!(body["comment"], "spam");
    recv_nothing(&mut alice_ws).await;
}

#[tokio::test]
async fn clip_channel_delivers_updated_deleted_with_null_body() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;

    let (status, clip) = srv
        .api(
            "clips/create",
            serde_json::json!({ "name": "pub", "isPublic": true }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200, "{clip}");
    let clip_id = clip["id"].as_str().unwrap().to_string();

    // Anonymous subscribers may watch public clips (upstream parity).
    let mut ws = connect(&srv.ws_url("")).await;
    send(
        &mut ws,
        serde_json::json!({
            "type": "connect",
            "body": { "channel": "clip", "id": "c", "params": { "clipId": clip_id }, "pong": true },
        }),
    )
    .await;
    let frame = recv(&mut ws).await;
    assert_eq!(frame["type"], "connected");

    // Gone clips refuse silently (no `connected`, no error).
    let mut gone_ws = connect(&srv.ws_url("")).await;
    send(&mut gone_ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "clip", "id": "g", "params": { "clipId": "aaaaaaaaaaaaaaaa" }, "pong": true },
    })).await;
    recv_nothing(&mut gone_ws).await;

    settle().await;
    publish(
        &srv.services,
        &StreamingEvent::ClipUpdated {
            clip_id: clip_id.clone(),
        },
    );
    let frame = recv(&mut ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("c", "updated"));
    assert_eq!(body, &Value::Null);
    publish(
        &srv.services,
        &StreamingEvent::ClipDeleted {
            clip_id: clip_id.clone(),
        },
    );
    let frame = recv(&mut ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("c", "deleted"));
    assert_eq!(body, &Value::Null);
}

#[tokio::test]
async fn userlist_channel_applies_with_files_and_with_renotes() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (bob_id, bob_token) = srv.signup("bob").await;

    let (status, list) = srv
        .api(
            "users/lists/create",
            serde_json::json!({ "name": "l" }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let list_id = list["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api(
            "users/lists/push",
            serde_json::json!({ "listId": list_id, "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);

    // `withFiles: true` drops file-less notes.
    let mut files_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(&mut files_ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "userList", "id": "f", "params": { "listId": list_id, "withFiles": true } },
    })).await;
    // `withRenotes: false` drops pure renotes.
    let mut renotes_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(&mut renotes_ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "userList", "id": "r", "params": { "listId": list_id, "withRenotes": false } },
    })).await;
    // The earlier `push` surfaces once per socket as `userAdded` (worker
    // timing); drain it before asserting note flow.
    let frame = recv_channel(&mut files_ws, "f", "userAdded").await;
    assert_eq!(frame["body"]["body"]["id"], bob_id);
    let frame = recv_channel(&mut renotes_ws, "r", "userAdded").await;
    assert_eq!(frame["body"]["body"]["id"], bob_id);

    let (status, created) = srv
        .api(
            "notes/create",
            serde_json::json!({ "text": "plain list note" }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 200);
    let plain_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    // The plain note is file-less (skipped by `f`) but a valid non-renote
    // (delivered to `r`); the earlier `push` may surface as `userAdded`
    // first depending on worker timing, so match by event.
    let frame = recv_channel(&mut renotes_ws, "r", "note").await;
    assert_eq!(frame["body"]["body"]["id"], plain_id);
    recv_nothing(&mut files_ws).await;

    // A file note passes `withFiles` but a pure renote does not pass
    // `withRenotes: false`; the renote still passes `withFiles: true`
    // (it carries no files... so it must NOT pass either — assert both).
    let file_id = srv.make_file(Some(&bob_id), "attach.png");
    let (status, created) = srv
        .api(
            "notes/create",
            serde_json::json!({ "text": "file note", "fileIds": [file_id] }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 200, "{created}");
    let file_note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let frame = recv_channel(&mut files_ws, "f", "note").await;
    assert_eq!(frame["body"]["body"]["id"], file_note_id);
    // The file note is also a valid non-renote for `r`; consume it there.
    let frame = recv_channel(&mut renotes_ws, "r", "note").await;
    assert_eq!(frame["body"]["body"]["id"], file_note_id);

    let (status, _) = srv
        .api(
            "notes/create",
            serde_json::json!({ "renoteId": file_note_id }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 200);
    recv_nothing(&mut renotes_ws).await;
    recv_nothing(&mut files_ws).await;
}

#[tokio::test]
async fn reacted_carries_emoji_detail() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    let (status, created) = srv
        .api(
            "notes/create",
            serde_json::json!({ "text": "react me" }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();

    let mut ws = connect(&srv.ws_url(&format!("?i={bob_token}"))).await;
    send(
        &mut ws,
        serde_json::json!({ "type": "subNote", "body": { "id": note_id } }),
    )
    .await;

    // Unicode reaction: `emoji` is null (upstream parity).
    let (status, _) = srv
        .api(
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": "❤" }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 204);
    // The emoji broadcast (`emojiAdded`, fanned to every connection) may
    // land before the `unreacted` frame — match by event.
    let frame = recv_note_updated(&mut ws, &note_id, "reacted").await;
    assert_eq!(frame["body"]["body"]["emoji"], Value::Null);

    // Custom emoji reaction: `emoji` is `{name, url}`.
    let file_id = srv.make_file(None, "blobcat.png");
    let emoji = srv
        .services
        .create_emoji(
            "mod1",
            "blobcat",
            &file_id,
            None,
            Vec::new(),
            None,
            false,
            false,
            Vec::new(),
        )
        .unwrap();
    let (status, _) = srv
        .api(
            "notes/reactions/delete",
            serde_json::json!({ "noteId": note_id }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 204);
    let frame = recv_note_updated(&mut ws, &note_id, "unreacted").await;
    let _ = frame;
    let (status, _) = srv
        .api(
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": ":blobcat:" }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 204, "custom emoji reaction");
    let frame = recv_note_updated(&mut ws, &note_id, "reacted").await;
    assert_eq!(frame["body"]["body"]["reaction"], ":blobcat:");
    assert_eq!(frame["body"]["body"]["emoji"]["name"], "blobcat");
    assert!(
        frame["body"]["body"]["emoji"]["url"]
            .as_str()
            .is_some_and(|u| !u.is_empty()),
        "{frame}"
    );
    let _ = emoji;
}

#[tokio::test]
async fn poll_ended_stays_a_compat_note_updated_frame() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;

    let (status, created) = srv
        .api(
            "notes/create",
            serde_json::json!({ "text": "poll note" }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();

    let mut ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(
        &mut ws,
        serde_json::json!({ "type": "subNote", "body": { "id": note_id } }),
    )
    .await;
    settle().await;
    publish(
        &srv.services,
        &StreamingEvent::PollEnded {
            note_id: note_id.clone(),
        },
    );
    // The hub still emits `pollEnded` as a `noteUpdated` frame (compat
    // extra); the real notification is a worker change (see WP4 report).
    let frame = recv_note_updated(&mut ws, &note_id, "pollEnded").await;
    assert_eq!(frame["body"]["id"], note_id);
}

#[tokio::test]
async fn main_channel_forwards_all_new_event_kinds() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (status, me) = srv
        .api("i", serde_json::json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let me_id = me["id"].as_str().unwrap().to_string();

    let mut ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(
        &mut ws,
        serde_json::json!({
            "type": "connect", "body": { "channel": "main", "id": "m" },
        }),
    )
    .await;
    settle().await;

    // Every WP4 `MainEventKind` (Misskey `MainEventTypes` parity) is
    // forwarded on `main` with its upstream wire name.
    let cases: &[(MainEventKind, &str)] = &[
        (MainEventKind::UnreadNotification, "unreadNotification"),
        (MainEventKind::UnreadAntenna, "unreadAntenna"),
        (MainEventKind::ReadAntenna, "readAntenna"),
        (MainEventKind::ReadAllAnnouncements, "readAllAnnouncements"),
        (MainEventKind::AnnouncementCreated, "announcementCreated"),
        (MainEventKind::MyTokenRegenerated, "myTokenRegenerated"),
        (MainEventKind::Signin, "signin"),
        (MainEventKind::RegistryUpdated, "registryUpdated"),
        (MainEventKind::DriveFileCreated, "driveFileCreated"),
        (MainEventKind::UrlUploadFinished, "urlUploadFinished"),
        (MainEventKind::PageEvent, "pageEvent"),
    ];
    for (kind, wire) in cases {
        srv.services
            .streaming
            .push_main(MainMessage {
                user_id: me_id.clone(),
                kind: *kind,
                body: serde_json::json!({ "marker": wire }),
            })
            .unwrap();
    }
    let mut seen = Vec::new();
    for _ in 0..cases.len() {
        let frame = recv(&mut ws).await;
        let (id, event, _) = channel_body(&frame);
        assert_eq!(id, "m");
        seen.push(event.to_string());
    }
    for (_, wire) in cases {
        assert!(
            seen.contains(&wire.to_string()),
            "missing {wire} in {seen:?}"
        );
    }
}
