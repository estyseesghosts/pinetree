//! Phase 3 streaming contract tests (profile §2): a live server plus real
//! WebSocket clients (`tokio-tungstenite`, same framing as misskey-js).
//!
//! Covered: upgrade auth (anonymous/bad-token/scoped-without-`read:account`),
//! `connect`/`disconnect`, `main` follow/followed/notification/meUpdated,
//! timeline channels, note capture (`subNote` aliases, reacted/unreacted,
//! deleted), `readNotification`, `UNSUPPORTED_CHANNEL`, and visibility
//! gating of streamed notes.

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
use pinetree::application::ports::{
    ApiTokenRecord, AuthStore, BlobStore, Clock, IdGenerator, PortError,
};
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};
use pinetree::persistence::redb::RedbStore;
use pinetree::streaming::hub::hub_and_sink;

#[derive(Default)]
struct MemBlobs {
    inner: Mutex<HashMap<String, Vec<u8>>>,
}

impl BlobStore for MemBlobs {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        self.inner.lock().unwrap().insert(hash.to_string(), bytes.to_vec());
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

struct LiveServer {
    base: String,
    http: reqwest::Client,
    services: Arc<AppServices>,
}

impl LiveServer {
    async fn start() -> Self {
        let dir = std::env::temp_dir().join(format!("pinetree-stream-test-{}", uuid::Uuid::new_v4()));
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
        // Keep the background worker alive for the test lifetime without
        // awaiting it (detached tasks survive `JoinHandle` drops, but hold
        // the handle so the intent is explicit).
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
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(url: &str) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(url).await.expect("ws connect");
    ws
}

async fn send(ws: &mut Ws, msg: Value) {
    ws.send(Message::Text(msg.to_string().into())).await.unwrap();
}

async fn recv(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for stream frame")
            .expect("stream closed")
            .expect("ws error");
        match msg {
            // The server pings every 60 s (immediately on connect too):
            // keep-alive frames are not protocol frames.
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

#[tokio::test]
async fn anonymous_global_timeline_streams_public_notes() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;

    let mut ws = connect(&srv.ws_url("")).await;
    send(&mut ws, serde_json::json!({
        "type": "connect", "body": { "channel": "globalTimeline", "id": "g" },
    })).await;

    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "hello stream" }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();

    let frame = recv(&mut ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("g", "note"));
    assert_eq!(body["id"], note_id);
    assert_eq!(body["text"], "hello stream");
}

#[tokio::test]
async fn bad_and_scoped_tokens_are_rejected_at_upgrade() {
    let srv = LiveServer::start().await;
    let (alice_id, _) = srv.signup("alice").await;

    // Unknown token → 401.
    let err = tokio_tungstenite::connect_async(srv.ws_url("?i=nope")).await.unwrap_err();
    assert!(err.to_string().contains("401"), "expected 401, got {err}");

    // Scoped app token without read:account → 401 (Misskey parity).
    let scoped = "scoped-token-0123456789abcdef";
    srv.services
        .auth
        .store_token(ApiTokenRecord {
            id: "tok-streaming-test".to_string(),
            token_hash: pinetree::api::misskey::auth::hash_token(scoped),
            account_id: alice_id,
            permissions: vec!["write:notes".to_string()],
            is_first_party: false,
            name: None,
            description: None,
            icon_url: None,
            created_at_ms: 1,
        })
        .unwrap();
    let err = tokio_tungstenite::connect_async(srv.ws_url(&format!("?i={scoped}")))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("401"), "expected 401, got {err}");
}

#[tokio::test]
async fn credential_channels_need_auth_and_unknown_channels_error() {
    let srv = LiveServer::start().await;
    let mut ws = connect(&srv.ws_url("")).await;

    // Anonymous homeTimeline: silently ignored (Misskey parity) — no frame.
    send(&mut ws, serde_json::json!({
        "type": "connect", "body": { "channel": "homeTimeline", "id": "h" },
    })).await;
    // Unknown channel → explicit UNSUPPORTED_CHANNEL error frame.
    send(&mut ws, serde_json::json!({
        "type": "connect", "body": { "channel": "nope", "id": "u" },
    })).await;
    let frame = recv(&mut ws).await;
    assert_eq!(frame["type"], "error");
    assert_eq!(frame["body"]["code"], "UNSUPPORTED_CHANNEL");
    assert_eq!(frame["body"]["channel"], "nope");
    // Anonymous `userList`: a credential channel, silently ignored (no frame).
    send(&mut ws, serde_json::json!({
        "type": "connect", "body": { "channel": "userList", "id": "l", "params": { "listId": "x" } },
    })).await;
    recv_nothing(&mut ws).await;
    // Anonymous `antenna`: a credential channel, silently ignored (no frame).
    send(&mut ws, serde_json::json!({
        "type": "connect", "body": { "channel": "antenna", "id": "a", "params": { "antennaId": "x" } },
    })).await;
    recv_nothing(&mut ws).await;
}

#[tokio::test]
async fn antenna_channel_streams_matches_to_owner_only() {    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    // Alice owns a match-all antenna (keywords required by the API —
    // like upstream, an empty list is EMPTY_KEYWORD).
    let (status, packed) = srv
        .api(
            "antennas/create",
            serde_json::json!({
                "name": "all", "src": "all", "keywords": [["hello"]],
                "excludeKeywords": [], "users": [],
                "caseSensitive": false, "withReplies": true, "withFile": false,
            }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let antenna_id = packed["id"].as_str().unwrap().to_string();

    // Owner connects: no ack frame, but the channel is live.
    let mut alice_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(&mut alice_ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "antenna", "id": "a", "params": { "antennaId": antenna_id } },
    })).await;
    // Stranger's socket on the same antenna id: silently refused (no frames).
    let mut bob_ws = connect(&srv.ws_url(&format!("?i={bob_token}"))).await;
    send(&mut bob_ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "antenna", "id": "b", "params": { "antennaId": antenna_id } },
    })).await;
    // Unknown antenna id on the owner's socket: silently refused too.
    send(&mut alice_ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "antenna", "id": "c", "params": { "antennaId": "aaaaaaaaaaaaaaaa" } },
    })).await;

    // Bob posts: Alice's antenna channel gets the `note` event, Bob's gets
    // nothing (and Alice's unknown-id channel gets nothing).
    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "hello antennas" }), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let frame = recv(&mut alice_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("a", "note"));
    assert_eq!(body["id"], note_id);
    recv_nothing(&mut bob_ws).await;
}

#[tokio::test]
async fn userlist_channel_streams_notes_and_membership_to_owner() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (bob_id, bob_token) = srv.signup("bob").await;

    // Alice owns a list; owner connects before any members exist.
    let (status, list) = srv
        .api("users/lists/create", serde_json::json!({ "name": "l" }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let list_id = list["id"].as_str().unwrap().to_string();
    let mut alice_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(&mut alice_ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "userList", "id": "l", "params": { "listId": list_id } },
    })).await;
    // Stranger's socket on the same list: silently refused.
    let mut bob_ws = connect(&srv.ws_url(&format!("?i={bob_token}"))).await;
    send(&mut bob_ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "userList", "id": "b", "params": { "listId": list_id } },
    })).await;

    // Push bob → owner gets `userAdded` with the packed member.
    let (status, _) = srv
        .api(
            "users/lists/push",
            serde_json::json!({ "listId": list_id, "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let frame = recv(&mut alice_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("l", "userAdded"));
    assert_eq!(body["id"], bob_id);
    recv_nothing(&mut bob_ws).await;

    // Bob posts → owner's list channel gets the `note` event.
    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "list note" }), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let frame = recv(&mut alice_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("l", "note"));
    assert_eq!(body["id"], note_id);
    recv_nothing(&mut bob_ws).await;

    // Pull bob → owner gets `userRemoved`; his next note stays silent.
    let (status, _) = srv
        .api(
            "users/lists/pull",
            serde_json::json!({ "listId": list_id, "userId": bob_id }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let frame = recv(&mut alice_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("l", "userRemoved"));
    assert_eq!(body["id"], bob_id);
    let (status, _) = srv
        .api("notes/create", serde_json::json!({ "text": "after pull" }), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    recv_nothing(&mut alice_ws).await;
    recv_nothing(&mut bob_ws).await;
}

#[tokio::test]
async fn main_channel_gets_follow_followed_and_notification() {
    let srv = LiveServer::start().await;
    let (alice_id, alice_token) = srv.signup("alice").await;
    let (bob_id, bob_token) = srv.signup("bob").await;

    let mut alice_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    let mut bob_ws = connect(&srv.ws_url(&format!("?i={bob_token}"))).await;
    send(&mut alice_ws, serde_json::json!({
        "type": "connect", "body": { "channel": "main", "id": "m" },
    })).await;
    send(&mut bob_ws, serde_json::json!({
        "type": "connect", "body": { "channel": "main", "id": "m" },
    })).await;

    let (status, _) = srv
        .api("following/create", serde_json::json!({ "userId": bob_id }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);

    // Follower's main: `follow` (packed followee). A `meUpdated` for the
    // followee may arrive first — same txn, either job can win the race.
    let mut saw_follow = false;
    for _ in 0..3 {
        let frame = recv(&mut alice_ws).await;
        let (id, event, body) = channel_body(&frame);
        assert_eq!(id, "m", "{frame}");
        if event == "follow" {
            saw_follow = true;
            assert_eq!(body["id"], bob_id);
            break;
        }
    }
    assert!(saw_follow);

    // Followee's main: followed + notification (+ maybe meUpdated), any order.
    let mut saw_followed = false;
    let mut saw_notification = false;
    for _ in 0..3 {
        let frame = recv(&mut bob_ws).await;
        let (id, event, body) = channel_body(&frame);
        assert_eq!(id, "m");
        match event {
            "followed" => {
                saw_followed = true;
                assert_eq!(body["id"], alice_id);
            }
            "notification" => {
                saw_notification = true;
                assert_eq!(body["type"], "follow");
            }
            "meUpdated" => {}
            other => panic!("unexpected main event {other}: {frame}"),
        }
        if saw_followed && saw_notification {
            break;
        }
    }
    assert!(saw_followed && saw_notification);
}

#[tokio::test]
async fn note_capture_gets_reacted_unreacted_deleted() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "capture me" }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();

    let mut ws = connect(&srv.ws_url(&format!("?i={bob_token}"))).await;
    send(&mut ws, serde_json::json!({
        "type": "subNote", "body": { "id": note_id },
    })).await;

    let (status, _) = srv
        .api(
            "notes/reactions/create",
            serde_json::json!({ "noteId": note_id, "reaction": "❤" }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 204);
    let frame = recv(&mut ws).await;
    assert_eq!(frame["type"], "noteUpdated");
    assert_eq!(frame["body"]["id"], note_id);
    assert_eq!(frame["body"]["type"], "reacted");
    assert_eq!(frame["body"]["body"]["reaction"], "❤");

    let (status, _) = srv
        .api("notes/reactions/delete", serde_json::json!({ "noteId": note_id }), Some(&bob_token))
        .await;
    assert_eq!(status, 204);
    let frame = recv(&mut ws).await;
    assert_eq!(frame["body"]["type"], "unreacted");

    let (status, _) = srv
        .api("notes/delete", serde_json::json!({ "noteId": note_id }), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let frame = recv(&mut ws).await;
    assert_eq!(frame["body"]["type"], "deleted");
    assert!(frame["body"]["body"]["deletedAt"].is_string());
}

#[tokio::test]
async fn home_timeline_only_streams_followed_authors() {
    let srv = LiveServer::start().await;
    let (alice_id, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;
    let (_, carol_token) = srv.signup("carol").await;

    let mut ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(&mut ws, serde_json::json!({
        "type": "connect", "body": { "channel": "homeTimeline", "id": "h" },
    })).await;

    // Bob (not followed): no frame.
    let (status, _) = srv
        .api("notes/create", serde_json::json!({ "text": "stranger" }), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    recv_nothing(&mut ws).await;

    // Follow Carol, then her note arrives.
    let (status, carol) = srv.api("i", serde_json::json!({}), Some(&carol_token)).await;
    assert_eq!(status, 200);
    let carol_id = carol["id"].as_str().unwrap().to_string();
    assert_ne!(carol_id, alice_id);
    let (status, _) = srv
        .api("following/create", serde_json::json!({ "userId": carol_id }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);

    // Drain the follow notification frame from Alice's main? Alice has no
    // main channel here — only homeTimeline, which never gets follow events.
    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "friend post" }), Some(&carol_token))
        .await;
    assert_eq!(status, 200);
    let frame = recv(&mut ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("h", "note"));
    assert_eq!(body["id"], created["createdNote"]["id"]);
}

#[tokio::test]
async fn local_and_hybrid_channels_deliver() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    // Anonymous localTimeline: public local notes arrive…
    let mut anon = connect(&srv.ws_url("")).await;
    send(&mut anon, serde_json::json!({
        "type": "connect", "body": { "channel": "localTimeline", "id": "l" },
    })).await;
    // …but home-visibility notes and replies (default withReplies=false) do not.
    let (status, _) = srv
        .api("notes/create", serde_json::json!({ "text": "home only", "visibility": "home" }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "local hello" }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("notes/create", serde_json::json!({ "text": "a reply", "replyId": created["createdNote"]["id"] }), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    let frame = recv(&mut anon).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("l", "note"));
    assert_eq!(body["text"], "local hello");
    recv_nothing(&mut anon).await;

    // withReplies=true re-admits replies (param plumbing).
    let mut anon2 = connect(&srv.ws_url("")).await;
    send(&mut anon2, serde_json::json!({
        "type": "connect",
        "body": { "channel": "localTimeline", "id": "l", "params": { "withReplies": true } },
    })).await;
    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "another reply", "replyId": created["createdNote"]["id"] }), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    let frame = recv(&mut anon2).await;
    let (_, _, body) = channel_body(&frame);
    assert_eq!(body["id"], created["createdNote"]["id"]);

    // Hybrid (authed): followed author's home note + local public notes.
    let (status, bob) = srv.api("i", serde_json::json!({}), Some(&bob_token)).await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("following/create", serde_json::json!({ "userId": bob["id"] }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let mut alice_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(&mut alice_ws, serde_json::json!({
        "type": "connect", "body": { "channel": "hybridTimeline", "id": "hy" },
    })).await;
    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "bob home note", "visibility": "home" }), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    let frame = recv(&mut alice_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("hy", "note"));
    assert_eq!(body["id"], created["createdNote"]["id"]);
}

#[tokio::test]
async fn disconnect_and_unsubscribe_go_silent() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    let mut ws = connect(&srv.ws_url(&format!("?i={bob_token}"))).await;
    send(&mut ws, serde_json::json!({
        "type": "connect", "body": { "channel": "globalTimeline", "id": "g" },
    })).await;
    send(&mut ws, serde_json::json!({ "type": "disconnect", "body": { "id": "g" } })).await;
    let (status, _) = srv
        .api("notes/create", serde_json::json!({ "text": "after disconnect" }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    recv_nothing(&mut ws).await;

    // Capture via the `s` alias, release via `un`: reactions stay silent.
    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "capture me" }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    send(&mut ws, serde_json::json!({ "type": "s", "body": { "id": note_id } })).await;
    send(&mut ws, serde_json::json!({ "type": "un", "body": { "id": note_id } })).await;
    let (status, _) = srv
        .api("notes/reactions/create", serde_json::json!({ "noteId": note_id, "reaction": "❤" }), Some(&bob_token))
        .await;
    assert_eq!(status, 204);
    recv_nothing(&mut ws).await;
}

#[tokio::test]
async fn specified_capture_gate() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (bob_id, bob_token) = srv.signup("bob").await;
    let (_, carol_token) = srv.signup("carol").await;

    let (status, created) = srv
        .api("notes/create", serde_json::json!({
            "text": "for bob eyes only",
            "visibility": "specified",
            "visibleUserIds": [bob_id],
        }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();

    let mut bob_ws = connect(&srv.ws_url(&format!("?i={bob_token}"))).await;
    let mut carol_ws = connect(&srv.ws_url(&format!("?i={carol_token}"))).await;
    send(&mut bob_ws, serde_json::json!({ "type": "subNote", "body": { "id": note_id } })).await;
    send(&mut carol_ws, serde_json::json!({ "type": "subNote", "body": { "id": note_id } })).await;
    // Settle captures before the reaction lands.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (status, _) = srv
        .api("notes/reactions/create", serde_json::json!({ "noteId": note_id, "reaction": "❤" }), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let frame = recv(&mut bob_ws).await;
    assert_eq!(frame["type"], "noteUpdated");
    assert_eq!(frame["body"]["type"], "reacted");
    recv_nothing(&mut carol_ws).await;
}

#[tokio::test]
async fn bearer_header_auth_and_pong_connected() {
    use tokio_tungstenite::tungstenite::http::Request;
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;

    // Authorization-header auth (non-browser clients, RFC 6750 §2.1).
    // A custom Request must carry the full WS handshake headers itself.
    let host = srv.base.trim_start_matches("http://").to_string();
    let req = Request::builder()
        .uri(srv.ws_url(""))
        .header("Host", host)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .header("Authorization", format!("Bearer {alice_token}"))
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.expect("ws connect");
    // pong:true → connected frame (Misskey parity).
    send(&mut ws, serde_json::json!({
        "type": "connect",
        "body": { "channel": "main", "id": "m", "pong": true },
    })).await;
    let frame = recv(&mut ws).await;
    assert_eq!(frame["type"], "connected");
    assert_eq!(frame["body"]["id"], "m");
}

#[tokio::test]
async fn main_reply_renote_unfollow_events() {
    let srv = LiveServer::start().await;
    let (alice_id, alice_token) = srv.signup("alice").await;
    let (bob_id, bob_token) = srv.signup("bob").await;

    let mut alice_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(&mut alice_ws, serde_json::json!({
        "type": "connect", "body": { "channel": "main", "id": "m" },
    })).await;

    let (status, created) = srv
        .api("notes/create", serde_json::json!({ "text": "alice root" }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let root_id = created["createdNote"]["id"].as_str().unwrap().to_string();

    let (status, _) = srv
        .api("notes/create", serde_json::json!({ "text": "bob reply", "replyId": root_id }), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    let mut saw_reply = false;
    for _ in 0..4 {
        let frame = recv(&mut alice_ws).await;
        let (id, event, body) = channel_body(&frame);
        assert_eq!(id, "m");
        if event == "reply" && body["replyId"] == root_id {
            saw_reply = true;
            break;
        }
    }
    assert!(saw_reply, "no reply event on main");

    let (status, _) = srv
        .api("notes/create", serde_json::json!({ "renoteId": root_id }), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    let mut saw_renote = false;
    for _ in 0..4 {
        let frame = recv(&mut alice_ws).await;
        let (id, event, body) = channel_body(&frame);
        assert_eq!(id, "m");
        if event == "renote" && body["renoteId"] == root_id {
            saw_renote = true;
            break;
        }
    }
    assert!(saw_renote, "no renote event on main");

    // Follow then unfollow: the follower's main eventually sees unfollow.
    let (status, _) = srv
        .api("following/create", serde_json::json!({ "userId": bob_id }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("following/delete", serde_json::json!({ "userId": bob_id }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let mut saw_unfollow = false;
    for _ in 0..6 {
        let frame = recv(&mut alice_ws).await;
        let (id, event, body) = channel_body(&frame);
        assert_eq!(id, "m");
        if event == "unfollow" && body["id"] == bob_id {
            saw_unfollow = true;
            break;
        }
        let _ = alice_id;
    }
    assert!(saw_unfollow, "no unfollow event on main");
}

#[tokio::test]
async fn followers_only_notes_skip_non_followers_on_global() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    let mut ws = connect(&srv.ws_url(&format!("?i={bob_token}"))).await;
    send(&mut ws, serde_json::json!({
        "type": "connect", "body": { "channel": "globalTimeline", "id": "g" },
    })).await;

    let (status, _) = srv
        .api(
            "notes/create",
            serde_json::json!({ "text": "inner circle", "visibility": "followers" }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    recv_nothing(&mut ws).await;
}

#[tokio::test]
async fn me_updated_and_read_notification_round_trip() {
    let srv = LiveServer::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    let mut ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(&mut ws, serde_json::json!({
        "type": "connect", "body": { "channel": "main", "id": "m" },
    })).await;

    // Bob follows Alice → followed + notification on Alice's main.
    let (status, alice) = srv.api("i", serde_json::json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "following/create",
            serde_json::json!({ "userId": alice["id"] }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 200);
    // Consume the follow frames (followed + notification + meUpdated in
    // any order — following commits a MeUpdated for the followee too).
    for _ in 0..3 {
        let frame = recv(&mut ws).await;
        let (id, _, _) = channel_body(&frame);
        assert_eq!(id, "m");
    }

    // i/update → meUpdated with the new name.
    let (status, _) = srv
        .api("i/update", serde_json::json!({ "name": "Alice Live" }), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let mut seen = false;
    for _ in 0..3 {
        let frame = recv(&mut ws).await;
        let (id, event, body) = channel_body(&frame);
        assert_eq!(id, "m");
        if event == "meUpdated" && body["name"] == "Alice Live" {
            seen = true;
            break;
        }
    }
    assert!(seen, "no meUpdated carrying the new name");

    // readNotification → readAllNotifications + notifications marked read.
    send(&mut ws, serde_json::json!({ "type": "readNotification" })).await;
    let frame = recv(&mut ws).await;
    let (id, event, _) = channel_body(&frame);
    assert_eq!((id, event), ("m", "readAllNotifications"));
    let (status, notifs) = srv.api("i/notifications", serde_json::json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert!(notifs.as_array().unwrap().iter().all(|n| n["isRead"] == true));
}

/// Batch M WP3: suspending an account closes ITS open `/streaming` sockets
/// (and nobody else's). The suspend transaction queues one `UserSuspended`
/// streaming job; the worker fans it out through the hub and each
/// connection whose viewer matches drops its socket.
#[tokio::test]
async fn wp3_suspend_closes_open_streams_for_that_account_only() {
    let srv = LiveServer::start().await;
    let (alice_id, alice_token) = srv.signup("alice").await;
    let (_bob_id, bob_token) = srv.signup("bob").await;
    let (mod_id, mod_token) = srv.signup("mod").await;
    srv.services
        .set_role(None, &mod_id, false, true)
        .expect("set role");

    let mut alice_ws = connect(&srv.ws_url(&format!("?i={alice_token}"))).await;
    send(&mut alice_ws, serde_json::json!({
        "type": "connect", "body": { "channel": "main", "id": "m" },
    })).await;
    let mut bob_ws = connect(&srv.ws_url(&format!("?i={bob_token}"))).await;
    send(&mut bob_ws, serde_json::json!({
        "type": "connect", "body": { "channel": "globalTimeline", "id": "g" },
    })).await;

    let (status, value) = srv
        .api(
            "admin/suspend-user",
            serde_json::json!({ "userId": alice_id }),
            Some(&mod_token),
        )
        .await;
    assert_eq!(status, 204, "{value}");

    // Alice's socket closes (the worker runs the stream-close job).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, alice_ws.next()).await {
            Err(_) => panic!("suspended account's stream stayed open"),
            Ok(None) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(Message::Close(_)))) => break,
            Ok(Some(Ok(_))) => continue, // pings / stray frames
        }
    }

    // Bob's socket stays open and keeps streaming.
    let (status, created) = srv
        .api(
            "notes/create",
            serde_json::json!({ "text": "still streaming" }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 200);
    let note_id = created["createdNote"]["id"].as_str().unwrap().to_string();
    let frame = recv(&mut bob_ws).await;
    let (id, event, body) = channel_body(&frame);
    assert_eq!((id, event), ("g", "note"));
    assert_eq!(body["id"], note_id);
}
