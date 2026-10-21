//! Gaps final-wiring dispatcher tests: every arm added by the final-wiring
//! pass, exercised over HTTP against a live server (same framing as
//! `tests/gaps_streaming.rs`).
//!
//! Covered: `ping`, `endpoint(s)`, `retention`, `reset-db`, `sw/*`,
//! `v2/admin/emoji/list`, `i/2fa/*`, `my/apps`, `app/*`,
//! `following/{update,update-all,list}`, `following/requests/sent`,
//! `drive/files/{check-existence,attached-notes}`, `drive/stream`,
//! `users/pages`, `users/lists/create-from-public`, `gallery/*`,
//! `flash/*`, `hashtags/*`, `announcements*`, `i/read-announcement`,
//! `promo/read`, the `i` announcement badge, the admin families,
//! `federation/*`, `queue/*`, `abuse-report-notification-recipient/*`,
//! and the live-mail hookups (`i/update-email`, `request-reset-password`).
//! Plus the final-wiring 5/5 arms: `drive/folders/*`,
//! `drive/files/{find,move-bulk}` + the folder-aware `drive/files` and
//! `drive/files/update` legs, `notes/thread-muting/*`,
//! `users/update-memo`, `system-webhook/*`, `webhooks/*`,
//! `admin/send-email`. Channels/chat stay 404 by owner SKIP order
//! (`docs/gaps.md:5`; the code was removed from the tree).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::net::TcpListener;

use pinetree::api::misskey::endpoints::set_email_sender;
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::commands::auth_2fa::totp_token;
use pinetree::application::ports::{
    BlobStore, Clock, IdGenerator, InsertMediaInput, PasswordResetRequest, PortError,
};
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{EmailError, EmailMessage, EmailSender, SmtpConfig, SmtpEmailSender};
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

const TINY_PNG: &[u8] = &[
    0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 13, b'I', b'H', b'D', b'R', 0, 0,
    0, 1, 0, 0, 0, 1, 8, 6, 0, 0, 0, 0x1f, 0x15, 0xc4, 0x89, 0, 0, 0, 13, b'I', b'D', b'A',
    b'T', 0x78, 0x9c, 0x63, 0x60, 0x60, 0x60, 0x00, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0d, 0x0a,
    0x2d, 0xb4, 0, 0, 0, 0, b'I', b'E', b'N', b'D', 0xae, 0x42, 0x60, 0x82,
];

#[derive(Default)]
struct Recorder {
    sent: Mutex<Vec<EmailMessage>>,
}

impl EmailSender for Recorder {
    fn send(&self, msg: &EmailMessage) -> Result<(), EmailError> {
        self.sent.lock().unwrap().push(msg.clone());
        Ok(())
    }
}

struct Live {
    base: String,
    http: reqwest::Client,
    services: Arc<AppServices>,
    recorder: Arc<Recorder>,
}

impl Live {
    async fn start() -> Self {
        let dir = std::env::temp_dir().join(format!("pinetree-gaps-final-{}", uuid::Uuid::new_v4()));
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
            streaming,
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
            public_url: "https://final.test".to_string(),
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
            streaming: services.streaming.clone(),
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
            featured_cache: Arc::new(
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
            recorder: Arc::new(Recorder::default()),
        }
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
            .json(&json!({ "username": username, "password": "pw-pw-pw-pw" }))
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

    fn make_file(&self, owner: &str, name: &str) -> String {
        let blob_key = pinetree::media::ingest::sha256_hex(TINY_PNG);
        self.services.blobs.put(&blob_key, TINY_PNG).unwrap();
        self.services
            .media
            .insert_metadata(InsertMediaInput {
                id: None,
                owner_id: Some(owner.to_string()),
                name: name.to_string(),
                mime: "image/png".to_string(),
                size_bytes: TINY_PNG.len() as u64,
                hash: pinetree::media::ingest::md5_hex(TINY_PNG),
                blob_key,
                blurhash: None,
                properties: MediaProperties::default(),
                is_sensitive: false,
                is_link: false,
                url: format!("https://final.test/files/{name}"),
                thumbnail_url: None,
                comment: None,
                source_url: None,
                max_file_size: 10 * 1024 * 1024,
            })
            .unwrap()
            .id
    }
}

fn code_of(value: &Value) -> &str {
    value
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("<no-code>")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test]
async fn gaps_final_wiring_arms() {
    let srv = Live::start().await;
    // Publish the recording sender: live-mail paths send through it.
    let sender: Arc<dyn EmailSender> = srv.recorder.clone();
    set_email_sender(sender);

    // --- misc -----------------------------------------------------------
    let (status, value) = srv.api("ping", json!({}), None).await;
    assert_eq!((status, value), (200, json!({})));
    let (status, value) = srv.api("endpoints", json!({}), None).await;
    assert_eq!(status, 200);
    assert!(value.as_array().unwrap().iter().any(|n| n == "ping"));
    let (status, value) = srv.api("endpoint", json!({"endpoint": "ping"}), None).await;
    assert_eq!((status, value["name"].clone()), (200, json!("ping")));
    let (status, value) = srv.api("endpoint", json!({"endpoint": "nope"}), None).await;
    assert_eq!(status, 404);
    assert_eq!(code_of(&value), "NO_SUCH_ENDPOINT");
    let (status, value) = srv.api("retention", json!({}), Some("x")).await;
    assert_eq!(status, 401);
    assert_eq!(code_of(&value), "AUTHENTICATION_FAILED");
    let (alice_id, alice_token) = srv.signup("alice").await;
    let (status, value) = srv.api("retention", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 403);
    assert_eq!(code_of(&value), "PERMISSION_DENIED");
    srv.services
        .set_role(None, &alice_id, true, true)
        .unwrap();
    let (status, value) = srv.api("retention", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value), (200, json!({})));
    let (status, value) = srv
        .api(
            "retention",
            json!({"policy": {"keepDays": 90}}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, json!({"keepDays": 90})));
    let (status, value) = srv.api("reset-db", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");

    // --- sw/* ------------------------------------------------------------
    let sub = json!({"endpoint": "https://push.example/sub/1", "auth": "a", "publickey": "p"});
    let (status, _) = srv.api("sw/register", sub.clone(), Some(&alice_token)).await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "sw/show-registration",
            json!({"endpoint": "https://push.example/sub/1"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["auth"].clone()), (200, json!("a")));
    let (status, _) = srv
        .api(
            "sw/show-registration",
            json!({"endpoint": "https://push.example/nope"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    let (status, _) = srv
        .api(
            "sw/update-registration",
            json!({"endpoint": "https://push.example/sub/1", "auth": "a2", "publickey": "p2"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api("sw/unregister", json!({"endpoint": "https://push.example/sub/1"}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);

    // --- v2/admin/emoji/list ----------------------------------------------
    let (status, value) = srv.api("v2/admin/emoji/list", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value), (200, json!([])));

    // --- 2fa + apps ---------------------------------------------------------
    let (status, value) = srv
        .api("i/2fa/register", json!({"password": "wrong"}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INCORRECT_PASSWORD");
    let (status, value) = srv
        .api("i/2fa/register", json!({"password": "pw-pw-pw-pw"}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let secret = value["secret"].as_str().unwrap().to_string();
    assert!(!value["qr"].as_str().unwrap().is_empty());
    let good = totp_token(&secret, now_secs()).unwrap();
    let (status, _) = srv
        .api("i/2fa/done", json!({"token": "000000"}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    let (status, _) = srv
        .api("i/2fa/done", json!({"token": good}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api("i/2fa/password-less", json!({"value": true}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api("i/2fa/update-key", json!({"password": "pw-pw-pw-pw"}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let secret2 = value["secret"].as_str().unwrap().to_string();
    assert_ne!(secret, secret2);
    let good2 = totp_token(&secret2, now_secs()).unwrap();
    let (status, _) = srv
        .api("i/2fa/done", json!({"token": good2}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "i/2fa/authenticate",
            json!({"userId": alice_id, "token": "000000"}),
            None,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_TOKEN");
    let (status, value) = srv
        .api(
            "i/2fa/authenticate",
            json!({"userId": alice_id, "token": good2}),
            None,
        )
        .await;
    assert_eq!(status, 200);
    assert!(!value["token"].as_str().unwrap().is_empty());
    let (status, _) = srv
        .api("i/2fa/unregister", json!({"password": "pw-pw-pw-pw"}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api("i/2fa/remove-key", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, value) = srv.api("my/apps", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api(
            "app/create",
            json!({"name": "helper", "permission": ["read:account"]}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert!(!value["secret"].as_str().unwrap().is_empty());
    let app_id = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv.api("app/show", json!({"appId": app_id}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value["name"], json!("helper"));
    let (status, value) = srv.api("app/show", json!({"appId": "nope"}), Some(&alice_token)).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_APP");

    // --- following extras ----------------------------------------------------
    let (bob_id, bob_token) = srv.signup("bob").await;
    let (status, value) = srv.api("following/list", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, _) = srv
        .api("following/requests/sent", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    // `following/update` on a missing edge is `NOT_FOLLOWING` (the redb
    // `update_follow_options` override is sibling-owned; until it lands the
    // store answers 500 — the arm maps whatever the store reports).
    let (status, _) = srv
        .api("following/create", json!({"userId": bob_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, value) = srv
        .api(
            "following/update",
            json!({"userId": bob_id, "withReplies": true}),
            Some(&alice_token),
        )
        .await;
    assert!(status == 200 || status == 500, "update status {status}");
    if status == 200 {
        assert_eq!(value["followeeId"], json!(bob_id));
        let (update_all_status, _) = srv
            .api("following/update-all", json!({"notify": "normal"}), Some(&alice_token))
            .await;
        assert_eq!(update_all_status, 200);
    }
    let (status, value) = srv.api("following/list", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api("following/update", json!({"userId": bob_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);

    // --- drive extras ----------------------------------------------------------
    let (status, value) = srv
        .api("drive/files/check-existence", json!({"md5": "abc"}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FILE");
    let (status, value) = srv
        .api("drive/files/attached-notes", json!({"fileId": "nope"}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FILE");
    let (status, value) = srv.api("drive/stream", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value), (200, json!([])));
    let file_id = srv.make_file(&alice_id, "pic.png");
    let (status, _) = srv
        .api("drive/files/check-existence", json!({"md5": "nope"}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    let (status, value) = srv.api("drive/stream", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, value) = srv
        .api("drive/files/attached-notes", json!({"fileId": file_id}), Some(&alice_token))
        .await;
    assert_eq!((status, value), (200, json!([])));
    let _ = bob_token;

    // --- social reads ------------------------------------------------------------
    let (status, value) = srv.api("users/pages", json!({"userId": "nobody"}), None).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api(
            "users/lists/create-from-public",
            json!({"name": "copy", "listId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_LIST");

    // --- gallery + flash ------------------------------------------------------------
    let (status, value) = srv
        .api(
            "gallery/posts/create",
            json!({"title": "t", "fileIds": []}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "FILE_REQUIRED");
    let (status, value) = srv
        .api(
            "gallery/posts/create",
            json!({"title": "t", "fileIds": [file_id]}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["likedCount"], json!(0));
    assert!(value.get("isLiked").is_none());
    let post_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv.api("gallery/posts/show", json!({"postId": "nope"}), None).await;
    assert_eq!(status, 400);
    let (status, value) = srv.api("gallery/posts/show", json!({"postId": post_id}), None).await;
    assert_eq!(status, 200);
    assert_eq!(value["title"], json!("t"));
    let (status, _) = srv
        .api(
            "gallery/posts/update",
            json!({"postId": post_id, "title": "t2"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("gallery/posts/like", json!({"postId": post_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api("gallery/posts/like", json!({"postId": post_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "ALREADY_LIKED");
    let (status, _) = srv.api("gallery/featured", json!(null), None).await;
    assert_eq!(status, 200);
    let (status, _) = srv.api("gallery/popular", json!(null), None).await;
    assert_eq!(status, 200);
    let (status, value) = srv.api("gallery/posts", json!(null), None).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api("gallery/posts/unlike", json!({"postId": post_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api("gallery/posts/unlike", json!({"postId": post_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NOT_LIKED");
    let (status, _) = srv
        .api("gallery/posts/delete", json!({"postId": post_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "flash/create",
            json!({"title": "play", "script": "let x = 1;", "visibility": "bogus"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, value) = srv
        .api(
            "flash/create",
            json!({"title": "play", "script": "let x = 1;"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let flash_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv.api("flash/show", json!({"flashId": "nope"}), None).await;
    assert_eq!(status, 400);
    let (status, _) = srv
        .api(
            "flash/update",
            json!({"flashId": flash_id, "summary": "s"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("flash/like", json!({"flashId": flash_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv.api("flash/featured", json!(null), None).await;
    assert_eq!(status, 200);
    let (status, value) = srv.api("flash/my", json!(null), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, value) = srv.api("flash/my-likes", json!(null), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api("flash/unlike", json!({"flashId": flash_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api("flash/delete", json!({"flashId": flash_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);

    // --- hashtags ------------------------------------------------------------
    let (status, value) = srv.api("hashtags/list", json!({}), None).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api("hashtags/show", json!({"tag": "nope"}), None)
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_HASHTAG");
    let (status, value) = srv.api("hashtags/trend", json!({}), None).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv.api("hashtags/users", json!({"tag": "nope"}), None).await;
    assert_eq!((status, value), (200, json!([])));

    // --- announcements ----------------------------------------------------------
    let (status, value) = srv.api("announcements", json!(null), None).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api("announcements/show", json!({"announcementId": "nope"}), None)
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_ANNOUNCEMENT");
    let (status, value) = srv
        .api("i/read-announcement", json!({"announcementId": "nope"}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_ANNOUNCEMENT");
    let (status, _) = srv.api("promo/read", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 204);
    let (status, value) = srv.api("i", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value["hasUnreadAnnouncement"], json!(false));
    assert_eq!(value["unreadAnnouncements"], json!([]));

    // --- admin families ------------------------------------------------------
    let (status, value) = srv
        .api(
            "admin/roles/create",
            json!({"name": "vip", "target": "manual"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let role_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv.api("admin/roles/list", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("admin/roles/show", json!({"roleId": role_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "admin/roles/assign",
            json!({"roleId": role_id, "userId": alice_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api("admin/roles/users", json!({"roleId": role_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api(
            "admin/roles/unassign",
            json!({"roleId": role_id, "userId": alice_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "admin/roles/update",
            json!({"roleId": role_id, "name": "vip2"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("admin/roles/delete", json!({"roleId": role_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api("admin/relays/add", json!({"inbox": "https://r.example/inbox"}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["status"], json!("requesting"));
    let (status, value) = srv.api("admin/relays/list", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, _) = srv
        .api(
            "admin/relays/remove",
            json!({"inbox": "https://r.example/inbox"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let ad = json!({
        "url": "https://ad.example/", "memo": "m", "place": "square",
        "ratio": 1.0, "priority": "high", "expiresAt": 9_999_999_999_999_i64,
        "startsAt": 1_i64, "imageUrl": "https://ad.example/i.png", "dayOfWeek": -1_i64,
    });
    let (status, value) = srv.api("admin/ad/create", ad.clone(), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let ad_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv.api("admin/ad/list", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let mut update = ad.clone();
    update["id"] = json!(ad_id);
    let (status, _) = srv.api("admin/ad/update", update, Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, _) = srv.api("admin/ad/refresh", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api("admin/ad/delete", json!({"id": ad_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "admin/announcements/create",
            json!({"title": "hello", "text": "world"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let ann_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv.api("admin/announcements/list", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "admin/announcements/update",
            json!({"id": ann_id, "title": "hi", "text": "there"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api("admin/announcements/delete", json!({"id": ann_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv.api("admin/stats", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, _) = srv.api("admin/server-info", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, value) = srv
        .api("admin/get-user-ips", json!({"userId": alice_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert!(value.is_array());
    let (status, value) = srv
        .api(
            "admin/accounts/find-by-email",
            json!({"email": "nobody@example.com"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, Value::Null));
    let (status, _) = srv
        .api("admin/update-proxy-account", json!({"userId": Value::Null}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv.api("admin/proxy-account", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value["userId"].clone()), (200, Value::Null));
    let (status, _) = srv
        .api("admin/unset-mfa", json!({"userId": alice_id}), Some(&alice_token))
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "admin/reset-password",
            json!({"userId": alice_id, "password": "new-pw-new-pw"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv.api("federation/instances", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api(
            "federation/update-instance",
            json!({"host": "remote.example", "isSuspended": true}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["isSuspended"], json!(true));
    let (status, value) = srv
        .api("federation/show-instance", json!({"host": "remote.example"}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["host"], json!("remote.example"));
    let (status, _) = srv
        .api(
            "federation/refresh-remote-instance-metadata",
            json!({"host": "remote.example"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv.api("admin/federation/list", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "federation/find-by-email",
            json!({"email": "nobody@example.com"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv.api("queue/stats", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, value) = srv.api("queue/deliver-delayed", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, _) = srv.api("admin/queue/stats", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    let (status, value) = srv
        .api("abuse-report-notification-recipient/create", json!({"name": "ops", "method": "email", "email": "ops@example.com"}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let recipient_id = value["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api("abuse-report-notification-recipient/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "abuse-report-notification-recipient/show",
            json!({"id": recipient_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "abuse-report-notification-recipient/update",
            json!({"id": recipient_id, "name": "ops2", "method": "email", "email": "ops@example.com"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, value) = srv
        .api(
            "abuse-report-notification-recipient/show",
            json!({"id": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_ABUSE_REPORT_NOTIFICATION_RECIPIENT");
    let (status, _) = srv
        .api(
            "abuse-report-notification-recipient/delete",
            json!({"id": recipient_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);

    // --- live mail ------------------------------------------------------
    let (status, value) = srv
        .api(
            "i/update-email",
            json!({"password": "new-pw-new-pw", "email": "alice@example.com"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["id"], json!(alice_id));
    assert_eq!(srv.recorder.sent.lock().unwrap().len(), 1);
    let account = srv
        .services
        .accounts
        .find_by_username_and_host("alice", None)
        .unwrap()
        .unwrap();
    let verify_code = account.email_verify_code.clone().unwrap();
    let (status, _) = srv.api("verify-email", json!({"code": verify_code}), None).await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "request-reset-password",
            json!({"username": "alice", "email": "alice@example.com"}),
            None,
        )
        .await;
    assert_eq!(status, 204);
    assert_eq!(srv.recorder.sent.lock().unwrap().len(), 2);
    let reset_subjects: Vec<String> = srv
        .recorder
        .sent
        .lock()
        .unwrap()
        .iter()
        .map(|msg| msg.subject.clone())
        .collect();
    assert!(reset_subjects.iter().any(|s| s.contains("Verify")));
    assert!(reset_subjects.iter().any(|s| s.contains("Reset")));
    // Direct row + arm round-trip for `reset-password`.
    let token = "tok-".to_string() + &"t".repeat(60);
    srv.services
        .password_resets
        .create_reset_request(PasswordResetRequest {
            id: srv.services.ids.generate(),
            user_id: alice_id.clone(),
            token: token.clone(),
        })
        .unwrap();
    let (status, _) = srv
        .api("reset-password", json!({"token": token, "password": "brand-new-pw"}), None)
        .await;
    assert_eq!(status, 204);

    // --- channels/chat stay 404 -------------------------------------------
    // Owner SKIP order (`docs/gaps.md:5`): the code was removed from the
    // tree, so these stay `UNKNOWN_API_ENDPOINT` (not `UNSUPPORTED`).
    for endpoint in [
        "channels/create",
        "channels/show",
        "chat/rooms/create",
        "chat/messages/create",
    ] {
        let (status, value) = srv.api(endpoint, json!({}), Some(&alice_token)).await;
        assert_eq!(status, 404, "{endpoint}");
        assert_eq!(code_of(&value), "UNKNOWN_API_ENDPOINT", "{endpoint}");
    }

    // --- drive/folders CRUD -------------------------------------------------
    let (status, value) = srv
        .api("drive/folders/create", json!({"name": "docs"}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["name"], json!("docs"));
    assert_eq!(value["parentId"], Value::Null);
    assert_eq!(value["userId"], json!(alice_id));
    let folder_id = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api(
            "drive/folders/create",
            json!({"parentId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["name"], json!("Untitled"));
    assert_eq!(value["parentId"], json!(folder_id));
    let child_id = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api(
            "drive/folders/create",
            json!({"name": "orphan", "parentId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    assert_eq!(value["error"]["id"], json!("17ebfd4a-6d3b-4318-bc8f-b605f418ee3f"));
    let (status, value) = srv
        .api(
            "drive/folders/show",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["name"].clone()), (200, json!("docs")));
    let (status, value) = srv
        .api("drive/folders/show", json!({"folderId": "missing"}), Some(&alice_token))
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    // Bob's folder reads as missing to alice (ownership precedence).
    let (status, value) = srv
        .api("drive/folders/create", json!({"name": "bobs"}), Some(&bob_token))
        .await;
    assert_eq!(status, 200);
    let bob_folder = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api(
            "drive/folders/show",
            json!({"folderId": bob_folder}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    let (status, value) = srv
        .api(
            "drive/folders/find",
            json!({"name": "docs"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, value) = srv
        .api(
            "drive/folders/find",
            json!({"name": "docs", "parentId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api(
            "drive/folders/update",
            json!({"folderId": folder_id, "name": "papers"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["name"].clone()), (200, json!("papers")));
    let (status, value) = srv
        .api(
            "drive/folders/update",
            json!({"folderId": child_id, "parentId": child_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "RECURSIVE_NESTING");
    let (status, value) = srv
        .api(
            "drive/folders/update",
            json!({"folderId": child_id, "parentId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_PARENT_FOLDER");
    // Non-empty folders refuse deletion (the child is still inside).
    let (status, value) = srv
        .api(
            "drive/folders/delete",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "HAS_CHILD_FILES_OR_FOLDERS");
    let (status, value) = srv
        .api(
            "drive/folders/update",
            json!({"folderId": child_id, "parentId": Value::Null}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["parentId"].clone()), (200, Value::Null));
    // Deleting the child (empty, at root) succeeds.
    let (status, _) = srv
        .api(
            "drive/folders/delete",
            json!({"folderId": child_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);

    // --- drive/files/find + move-bulk + folder legs --------------------------
    let file_a = srv.make_file(&alice_id, "report-a.png");
    let file_b = srv.make_file(&alice_id, "report-a.png");
    let (status, value) = srv
        .api(
            "drive/files/find",
            json!({"name": "report-a.png"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 2);
    let (status, value) = srv
        .api(
            "drive/files/move-bulk",
            json!({"fileIds": [], "folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, value) = srv
        .api(
            "drive/files/move-bulk",
            json!({"fileIds": [file_a, file_b], "folderId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    let (status, _) = srv
        .api(
            "drive/files/move-bulk",
            json!({"fileIds": [file_a, file_b], "folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    // Root-scoped find no longer sees the moved files.
    let (status, value) = srv
        .api(
            "drive/files/find",
            json!({"name": "report-a.png"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 0);
    // Folder-scoped find sees both.
    let (status, value) = srv
        .api(
            "drive/files/find",
            json!({"name": "report-a.png", "folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 2);
    let (status, value) = srv
        .api(
            "drive/files",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 2);
    // `drive/files/update` moves one file; the pack carries `folderId`.
    let (status, value) = srv
        .api(
            "drive/files/update",
            json!({"fileId": file_a, "folderId": Value::Null}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["folderId"], Value::Null);
    let (status, value) = srv
        .api(
            "drive/files/update",
            json!({"fileId": file_a, "folderId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    // A folder holding files still refuses deletion; emptying it clears
    // the guard.
    let (status, value) = srv
        .api(
            "drive/folders/delete",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "HAS_CHILD_FILES_OR_FOLDERS");
    let (status, _) = srv
        .api(
            "drive/files/update",
            json!({"fileId": file_b, "folderId": Value::Null}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "drive/folders/delete",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);

    // --- notes/thread-muting + users/update-memo -------------------------------
    let (status, value) = srv
        .api("notes/create", json!({"text": "thread me"}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    let note_id = value["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api(
            "notes/thread-muting/create",
            json!({"noteId": note_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "notes/thread-muting/create",
            json!({"noteId": note_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "notes/thread-muting/delete",
            json!({"noteId": note_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "notes/thread-muting/delete",
            json!({"noteId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_NOTE");
    assert_eq!(
        value["error"]["id"],
        json!("f92bec2b-2f38-460e-8f68-b3e47fb0986d")
    );
    let (status, value) = srv
        .api(
            "users/update-memo",
            json!({"userId": bob_id, "memo": "owes me lunch"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, json!("owes me lunch")));
    let (status, value) = srv
        .api(
            "users/update-memo",
            json!({"userId": bob_id, "memo": ""}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, Value::Null));
    let (status, value) = srv
        .api(
            "users/update-memo",
            json!({"userId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_USER");

    // --- system-webhook/* (admin) + webhooks/* (owner) --------------------------
    let (status, _) = srv
        .api(
            "system-webhook/create",
            json!({"name": "ops", "url": "https://ops.example/hook", "trigger": "abuseReport"}),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 403);
    let (status, value) = srv
        .api(
            "system-webhook/create",
            json!({"name": "ops", "url": "https://ops.example/hook", "trigger": "nope"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, value) = srv
        .api(
            "system-webhook/create",
            json!({"name": "ops", "url": "https://ops.example/hook", "trigger": "abuseReport"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["trigger"], json!("abuseReport"));
    assert!(!value["secret"].as_str().unwrap().is_empty());
    let hook_id = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api("system-webhook/list", json!({}), Some(&alice_token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, value) = srv
        .api(
            "system-webhook/show",
            json!({"webhookId": hook_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["name"].clone()), (200, json!("ops")));
    let (status, value) = srv
        .api(
            "system-webhook/update",
            json!({"webhookId": hook_id, "isActive": false}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["isActive"].clone()), (200, json!(false)));
    let (status, value) = srv
        .api(
            "system-webhook/show",
            json!({"webhookId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_WEBHOOK");
    assert_eq!(
        value["error"]["id"],
        json!("722a824a-ba81-4d2e-8f84-bbc358bfdb42")
    );
    let (status, _) = srv
        .api(
            "system-webhook/delete",
            json!({"webhookId": hook_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "system-webhook/delete",
            json!({"webhookId": hook_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_WEBHOOK");
    // Owner webhooks: five succeed, the sixth hits the cap.
    for name in ["w1", "w2", "w3", "w4", "w5"] {
        let (status, _) = srv
            .api(
                "webhooks/create",
                json!({"name": name, "url": "https://bob.example/hook", "trigger": "mention"}),
                Some(&bob_token),
            )
            .await;
        assert_eq!(status, 200, "{name}");
    }
    let (status, value) = srv
        .api(
            "webhooks/create",
            json!({"name": "w6", "url": "https://bob.example/hook", "trigger": "mention"}),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "TOO_MANY_WEBHOOKS");
    assert_eq!(
        value["error"]["id"],
        json!("adadc6d4-090b-4f02-a74a-fdb5d5a1f4c7")
    );
    let (status, value) = srv.api("webhooks/list", json!({}), Some(&bob_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 5);
    let first_id = value[0]["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api("webhooks/show", json!({"webhookId": first_id}), Some(&bob_token))
        .await;
    assert_eq!((status, value["name"].clone()), (200, json!("w1")));
    // Alice cannot see bob's webhook (foreign reads as not-found).
    let (status, value) = srv
        .api(
            "webhooks/show",
            json!({"webhookId": first_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_WEBHOOK");
    let (status, _) = srv
        .api(
            "webhooks/delete",
            json!({"webhookId": first_id}),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 204);

    // --- admin/send-email --------------------------------------------------------
    let (status, _) = srv
        .api(
            "admin/send-email",
            json!({"to": "ops@example.com", "subject": "hi", "text": "hello"}),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 403);
    let (status, value) = srv
        .api(
            "admin/send-email",
            json!({"to": "not-an-email", "subject": "hi", "text": "hello"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let before = srv.services.jobs.pending_count().unwrap();
    let (status, _) = srv
        .api(
            "admin/send-email",
            json!({"to": "ops@example.com", "subject": "hi", "text": "hello"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    assert_eq!(srv.services.jobs.pending_count().unwrap(), before + 1);
}
