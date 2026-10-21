//! Custom-emoji streaming broadcasts + zip transfer (Gap 2 + Gap 3).
//!
//! Self-contained: this file owns its live-server harness (copied from
//! `tests/streaming.rs`, which this workstream must not edit).
//!
//! - Streaming: two channel-less WebSocket connections both receive
//!   `emojiAdded` / `emojiUpdated` / `emojiDeleted` frames when the worker
//!   expands a `StreamingPublish(EmojiBroadcast)` job (broadcast, not
//!   channel-scoped — Misskey `onBroadcastMessage` parity).
//! - Transfer (op-level): export builds a stored-compression zip in the
//!   caller's drive plus an `exportCompleted` notification with
//!   `exportedEntity: "customEmoji"`; import applies `meta.json` rows,
//!   replaces name collisions, and rejects zip-bombs / oversized entries /
//!   non-image MIME / traversal filenames / over-count archives.

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::commands::emoji_transfer::{
    handle_custom_emoji_transfer, TRANSFER_MAX_ENTRIES,
};
use pinetree::application::events::{EmojiBroadcastOp, StreamingEvent};
use pinetree::application::ports::{
    BlobStore, Clock, CustomEmojiTransferPayload, IdGenerator, JobKind, NewJob, PortError,
};
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::domain::notification::NotificationKind;
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
        let dir = std::env::temp_dir().join(format!("pinetree-emoji-xfer-test-{}", uuid::Uuid::new_v4()));
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
            cache_blobs: blobs.clone(),
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
            public_url: "https://emoji.test".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
            default_like: None,
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

async fn recv(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
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

/// Minimal bytes the shared sniffer calls `image/png` (magic only).
fn png_bytes(tag: &[u8]) -> Vec<u8> {
    let mut out = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    out.extend_from_slice(tag);
    out
}

fn publish_broadcast(services: &Arc<AppServices>, op: EmojiBroadcastOp, emojis: Vec<pinetree::domain::emoji::CustomEmoji>) {
    let event = StreamingEvent::EmojiBroadcast { op, emojis };
    let job = NewJob::new_json(
        JobKind::StreamingPublish,
        &event,
        format!("test-emoji-broadcast-{}", services.ids.generate()),
        services.clock.now_ms(),
    )
    .unwrap();
    services.jobs.enqueue_jobs(vec![job]).unwrap();
}

fn make_mod(services: &Arc<AppServices>) -> String {
    let acc = services.create_account("mod", None).unwrap();
    services.set_role(None, &acc.id, false, true).unwrap();
    acc.id
}

#[tokio::test]
async fn emoji_broadcast_reaches_every_connection_without_channels() {
    let srv = LiveServer::start().await;
    let mod_id = make_mod(&srv.services);

    // Two connections, NO channels subscribed (broadcast is not channel-scoped).
    let mut ws1 = connect(&srv.ws_url("")).await;
    let mut ws2 = connect(&srv.ws_url("")).await;

    // `admin/emoji/add` fans out `emojiAdded` with `{emoji}` post-commit.
    let file = srv
        .services
        .upload_media(&mod_id, "a.png".to_string(), &png_bytes(b"a"), None, false, false)
        .unwrap();
    let emoji = srv
        .services
        .create_emoji(&mod_id, "blobcat", &file.id, None, vec![], None, false, false, vec![])
        .unwrap();
    for ws in [&mut ws1, &mut ws2] {
        let frame = recv(ws).await;
        assert_eq!(frame["type"], "emojiAdded", "{frame}");
        assert_eq!(frame["body"]["emoji"]["name"], "blobcat", "{frame}");
        assert_eq!(frame["body"]["emoji"]["id"], emoji.id, "{frame}");
    }

    // `admin/emoji/update` fans out `emojiUpdated` with `{emojis: [...]}`.
    let updated = srv
        .services
        .update_emoji(
            &mod_id,
            Some(&emoji.id),
            None,
            None,
            Some(Some("cats".to_string())),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    assert_eq!(updated.category.as_deref(), Some("cats"));
    for ws in [&mut ws1, &mut ws2] {
        let frame = recv(ws).await;
        assert_eq!(frame["type"], "emojiUpdated", "{frame}");
        assert_eq!(frame["body"]["emojis"][0]["name"], "blobcat", "{frame}");
        assert_eq!(frame["body"]["emojis"][0]["category"], "cats", "{frame}");
    }

    // `admin/emoji/delete` fans out `emojiDeleted` with the pre-delete
    // snapshot, even though the row is gone when the worker packs it.
    let snap = srv.services.emojis.get_emoji_by_name("blobcat").unwrap().unwrap();
    srv.services.delete_emoji(&mod_id, &snap.id).unwrap();
    assert!(srv.services.emojis.get_emoji_by_name("blobcat").unwrap().is_none());
    for ws in [&mut ws1, &mut ws2] {
        let frame = recv(ws).await;
        assert_eq!(frame["type"], "emojiDeleted", "{frame}");
        assert_eq!(frame["body"]["emojis"][0]["name"], "blobcat", "{frame}");
        assert_eq!(frame["body"]["emojis"][0]["id"], snap.id, "{frame}");
    }

    // `Added`/`Updated` skip snapshots deleted between commit and run: a
    // hand-enqueued `Added` for the deleted row packs to `{"emoji": null}`
    // (the specified empty-pack arm) on both connections, while a live
    // emoji still broadcasts its full body.
    publish_broadcast(&srv.services, EmojiBroadcastOp::Added, vec![snap]);
    for ws in [&mut ws1, &mut ws2] {
        let frame = recv(ws).await;
        assert_eq!(frame["type"], "emojiAdded", "{frame}");
        assert!(frame["body"]["emoji"].is_null(), "{frame}");
    }
    let file2 = srv
        .services
        .upload_media(&mod_id, "b.png".to_string(), &png_bytes(b"b"), None, false, false)
        .unwrap();
    srv.services
        .create_emoji(&mod_id, "tada", &file2.id, None, vec![], None, false, false, vec![])
        .unwrap();
    for ws in [&mut ws1, &mut ws2] {
        let frame = recv(ws).await;
        assert_eq!(frame["type"], "emojiAdded", "{frame}");
        assert_eq!(frame["body"]["emoji"]["name"], "tada", "{frame}");
    }
}

fn drive_zip_bytes(services: &Arc<AppServices>, owner_id: &str, zip_name: &str) -> (Value, Vec<u8>) {
    let rows = services.media.list_by_owner(owner_id).unwrap();
    let meta = rows
        .iter()
        .find(|m| m.name == zip_name)
        .unwrap_or_else(|| panic!("no drive file {zip_name} in {:?}", rows.iter().map(|m| &m.name).collect::<Vec<_>>()));
    let bytes = services.blobs.get(&meta.blob_key).unwrap().unwrap();
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&bytes[..])).unwrap();
    let mut buf = String::new();
    use std::io::Read as _;
    archive.by_name("meta.json").unwrap().read_to_string(&mut buf).unwrap();
    (serde_json::from_str(&buf).unwrap(), bytes)
}

#[tokio::test]
async fn export_builds_stored_zip_and_export_completed_notify() {
    let srv = LiveServer::start().await;
    let (alice_id, _) = srv.signup("alice").await;
    let mod_id = make_mod(&srv.services);

    let file_a = srv
        .services
        .upload_media(&alice_id, "a.png".to_string(), &png_bytes(b"aaa"), None, false, false)
        .unwrap();
    srv.services
        .create_emoji(&mod_id, "alpha", &file_a.id, None, vec![], None, false, false, vec![])
        .unwrap();
    // An emoji whose media row is gone exports as `downloaded: false`.
    let file_b = srv
        .services
        .upload_media(&alice_id, "b.png".to_string(), &png_bytes(b"bbb"), None, false, false)
        .unwrap();
    let beta = srv
        .services
        .create_emoji(&mod_id, "beta", &file_b.id, None, vec![], None, false, false, vec![])
        .unwrap();
    srv.services.media.delete_metadata(&file_b.id).unwrap();
    assert_eq!(beta.shortcode, "beta");

    let payload = CustomEmojiTransferPayload { user_id: alice_id.clone(), file_id: None };
    let job = NewJob::new_json(
        JobKind::ExportCustomEmojis,
        &payload,
        format!("test-export-{}", srv.services.ids.generate()),
        srv.services.clock.now_ms(),
    )
    .unwrap();
    srv.services.jobs.enqueue_jobs(vec![job]).unwrap();

    // The background worker runs export → drive store → exportCompleted.
    let deadline = Instant::now() + Duration::from_secs(10);
    let notify = loop {
        let notifs = srv.services.notifications.list_notifications(&alice_id, 100).unwrap();
        if let Some(n) = notifs.iter().find(|n| matches!(n.kind, NotificationKind::ExportCompleted)) {
            break n.clone();
        }
        if Instant::now() > deadline {
            panic!("no exportCompleted notification");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(notify.exported_entity.as_deref(), Some("customEmoji"));
    let file_id = notify.file_id.clone().expect("exportCompleted carries fileId");
    let stored = srv.services.media.list_by_owner(&alice_id).unwrap();
    let stored = stored.iter().find(|m| m.id == file_id).expect("drive file exists");
    assert!(stored.name.starts_with("custom-emojis-"), "{}", stored.name);
    assert!(stored.name.ends_with(".zip"), "{}", stored.name);
    let zip_name = stored.name.clone();

    let (meta, _) = drive_zip_bytes(&srv.services, &alice_id, &zip_name);
    assert_eq!(meta["metaVersion"], 2);
    assert_eq!(meta["host"], "emoji.test");
    assert!(meta["exportedAt"].is_string());
    let rows = meta["emojis"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{meta}");
    let alpha = rows.iter().find(|r| r["emoji"]["name"] == "alpha").unwrap();
    assert_eq!(alpha["downloaded"], true);
    assert_eq!(alpha["fileName"], "alpha.png");
    let beta_row = rows.iter().find(|r| r["emoji"]["name"] == "beta").unwrap();
    assert_eq!(beta_row["downloaded"], false);

    // The archive uses stored (level-0) compression, and only reachable
    // images are stored as entries.
    let rows2 = srv.services.media.list_by_owner(&alice_id).unwrap();
    let stored = rows2.iter().find(|m| m.name == zip_name).unwrap();
    let raw = srv.services.blobs.get(&stored.blob_key).unwrap().unwrap();
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&raw[..])).unwrap();
    let names: Vec<String> = (0..archive.len()).map(|i| archive.name_for_index(i).unwrap().to_string()).collect();
    assert!(names.contains(&"meta.json".to_string()), "{names:?}");
    assert!(names.contains(&"alpha.png".to_string()), "{names:?}");
    assert!(!names.iter().any(|n| n.contains("beta")), "{names:?}");
    assert_eq!(
        archive.by_name("alpha.png").unwrap().compression(),
        zip::CompressionMethod::Stored
    );
}

fn build_zip(meta: &Value, entries: &[(&str, zip::CompressionMethod, Vec<u8>)]) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let opts = zip::write::SimpleFileOptions::default();
    writer.start_file("meta.json", opts).unwrap();
    writer.write_all(&serde_json::to_vec(meta).unwrap()).unwrap();
    for (name, method, bytes) in entries {
        writer
            .start_file(*name, zip::write::SimpleFileOptions::default().compression_method(*method))
            .unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn upload_zip(services: &Arc<AppServices>, owner: &str, name: &str, bytes: &[u8]) -> String {
    services
        .upload_media(owner, name.to_string(), bytes, None, false, true)
        .unwrap()
        .id
}

fn import_payload(user_id: &str, file_id: &str) -> Vec<u8> {
    serde_json::to_vec(&CustomEmojiTransferPayload {
        user_id: user_id.to_string(),
        file_id: Some(file_id.to_string()),
    })
    .unwrap()
}

fn meta_row(file_name: &str, downloaded: bool, name: &str) -> Value {
    serde_json::json!({
        "fileName": file_name,
        "downloaded": downloaded,
        "emoji": { "name": name },
    })
}

#[tokio::test]
async fn import_applies_rows_and_replaces_collisions() {
    let srv = LiveServer::start().await;
    let (alice_id, _) = srv.signup("alice").await;

    // Collision: a live row with the same name but different bytes.
    let old_file = srv
        .services
        .upload_media(&alice_id, "old.png".to_string(), &png_bytes(b"old"), None, false, false)
        .unwrap();
    let before = srv
        .services
        .create_emoji(&alice_id, "repl", &old_file.id, None, vec![], None, false, false, vec![])
        .unwrap();

    let new_png = png_bytes(b"new-bytes-here");
    let meta = serde_json::json!({
        "metaVersion": 2, "host": "remote.test", "exportedAt": "2026-10-20T00:00:00.000Z",
        "emojis": [meta_row("repl.png", true, "repl")],
    });
    let zip = build_zip(&meta, &[("repl.png", zip::CompressionMethod::Stored, new_png.clone())]);
    let zip_id = upload_zip(&srv.services, &alice_id, "import.zip", &zip);
    let out = handle_custom_emoji_transfer(
        &srv.services,
        &JobKind::ImportCustomEmojis,
        &import_payload(&alice_id, &zip_id),
    )
    .unwrap();
    assert!(out.is_none());

    let after = srv.services.emojis.get_emoji_by_name("repl").unwrap().unwrap();
    assert_ne!(after.id, before.id, "delete-then-add replaces the row");
    let media = srv.services.media.get(after.media_id.as_deref().unwrap()).unwrap().unwrap();
    assert_eq!(srv.services.blobs.get(&media.blob_key).unwrap().unwrap(), new_png);
}

#[tokio::test]
async fn import_skips_bad_rows_but_applies_good_ones() {
    let srv = LiveServer::start().await;
    let (alice_id, _) = srv.signup("alice").await;

    let good_png = png_bytes(b"good");
    let meta = serde_json::json!({
        "metaVersion": 2, "host": "remote.test", "exportedAt": "2026-10-20T00:00:00.000Z",
        "emojis": [
            meta_row("good.png", true, "good"),
            // Non-image bytes.
            meta_row("text.png", true, "notimage"),
            // Traversal fileName (fails the `^[a-zA-Z0-9_]+(\.[a-zA-Z0-9]+)*$` rule).
            meta_row("../evil.png", true, "evil"),
            // Invalid emoji name.
            meta_row("bad-name.png", true, "has-dash"),
            // Not downloaded.
            meta_row("skipped.png", true, "skippeddl"),
            // Referenced but absent from the archive.
            meta_row("ghost.png", true, "ghost"),
        ],
    });
    // Tweak: the not-downloaded row must actually say downloaded:false.
    let mut meta = meta;
    meta["emojis"][4]["downloaded"] = Value::Bool(false);
    let zip = build_zip(
        &meta,
        &[
            ("good.png", zip::CompressionMethod::Stored, good_png.clone()),
            ("text.png", zip::CompressionMethod::Stored, b"hello world".to_vec()),
            ("../evil.png", zip::CompressionMethod::Stored, png_bytes(b"evil")),
            ("bad-name.png", zip::CompressionMethod::Stored, png_bytes(b"bad")),
            ("skipped.png", zip::CompressionMethod::Stored, png_bytes(b"sk")),
        ],
    );
    let zip_id = upload_zip(&srv.services, &alice_id, "mixed.zip", &zip);
    handle_custom_emoji_transfer(
        &srv.services,
        &JobKind::ImportCustomEmojis,
        &import_payload(&alice_id, &zip_id),
    )
    .unwrap();

    assert!(srv.services.emojis.get_emoji_by_name("good").unwrap().is_some());
    for missing in ["notimage", "evil", "has-dash", "skippeddl", "ghost"] {
        assert!(
            srv.services.emojis.get_emoji_by_name(missing).unwrap().is_none(),
            "{missing} must not be imported"
        );
    }
}

#[tokio::test]
async fn import_rejects_archive_level_problems() {
    let srv = LiveServer::start().await;
    let (alice_id, _) = srv.signup("alice").await;

    // Missing meta.json fails the job.
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer.start_file("stray.png", zip::write::SimpleFileOptions::default()).unwrap();
    writer.write_all(&png_bytes(b"x")).unwrap();
    let no_meta: Vec<u8> = writer.finish().unwrap().into_inner();
    let no_meta_id = upload_zip(&srv.services, &alice_id, "no-meta.zip", &no_meta);
    assert!(matches!(
        handle_custom_emoji_transfer(
            &srv.services,
            &JobKind::ImportCustomEmojis,
            &import_payload(&alice_id, &no_meta_id),
        ),
        Err(PortError::Validation(_))
    ));

    // Not a zip at all.
    let junk_id = upload_zip(&srv.services, &alice_id, "junk.zip", b"not a zip");
    assert!(matches!(
        handle_custom_emoji_transfer(
            &srv.services,
            &JobKind::ImportCustomEmojis,
            &import_payload(&alice_id, &junk_id),
        ),
        Err(PortError::Validation(_))
    ));

    // Entry count over the cap (zip-bomb shape: many tiny entries).
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer.start_file("meta.json", zip::write::SimpleFileOptions::default()).unwrap();
    writer.write_all(br#"{"metaVersion":2,"emojis":[]}"#).unwrap();
    for i in 0..=TRANSFER_MAX_ENTRIES {
        writer.start_file(format!("f{i}.png"), zip::write::SimpleFileOptions::default()).unwrap();
        writer.write_all(b"x").unwrap();
    }
    let many: Vec<u8> = writer.finish().unwrap().into_inner();
    let many_id = upload_zip(&srv.services, &alice_id, "many.zip", &many);
    assert!(matches!(
        handle_custom_emoji_transfer(
            &srv.services,
            &JobKind::ImportCustomEmojis,
            &import_payload(&alice_id, &many_id),
        ),
        Err(PortError::Validation(_))
    ));

    // Missing fileId in the payload is a bad payload (quarantine).
    let no_file = serde_json::to_vec(&CustomEmojiTransferPayload { user_id: alice_id.clone(), file_id: None }).unwrap();
    assert!(matches!(
        handle_custom_emoji_transfer(&srv.services, &JobKind::ImportCustomEmojis, &no_file),
        Err(PortError::Validation(_))
    ));

    // Gone file / gone user converge quietly.
    assert!(
        handle_custom_emoji_transfer(
            &srv.services,
            &JobKind::ImportCustomEmojis,
            &import_payload(&alice_id, "aaaaaaaaaaaaaaaa"),
        )
        .unwrap()
        .is_none()
    );
    assert!(
        handle_custom_emoji_transfer(
            &srv.services,
            &JobKind::ExportCustomEmojis,
            &serde_json::to_vec(&CustomEmojiTransferPayload { user_id: "bbbbbbbbbbbbbbbb".to_string(), file_id: None }).unwrap(),
        )
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn import_skips_huge_declared_entry_but_applies_the_rest() {
    let srv = LiveServer::start().await;
    let (alice_id, _) = srv.signup("alice").await;

    // 33 MiB of zeros deflates to almost nothing on disk, but the declared
    // uncompressed size exceeds the per-entry cap — it must be skipped via
    // `ZipFile::size()` WITHOUT being read into memory.
    let huge = vec![0u8; 33 * 1024 * 1024];
    let good_png = png_bytes(b"survivor");
    let meta = serde_json::json!({
        "metaVersion": 2, "host": "remote.test", "exportedAt": "2026-10-20T00:00:00.000Z",
        "emojis": [
            meta_row("huge.png", true, "huge"),
            meta_row("survivor.png", true, "survivor"),
        ],
    });
    let zip = build_zip(
        &meta,
        &[
            ("huge.png", zip::CompressionMethod::Deflated, huge),
            ("survivor.png", zip::CompressionMethod::Stored, good_png),
        ],
    );
    assert!(zip.len() < 1024 * 1024, "fixture must stay small on disk");
    let zip_id = upload_zip(&srv.services, &alice_id, "huge.zip", &zip);
    handle_custom_emoji_transfer(
        &srv.services,
        &JobKind::ImportCustomEmojis,
        &import_payload(&alice_id, &zip_id),
    )
    .unwrap();
    assert!(srv.services.emojis.get_emoji_by_name("huge").unwrap().is_none());
    assert!(srv.services.emojis.get_emoji_by_name("survivor").unwrap().is_some());
}
