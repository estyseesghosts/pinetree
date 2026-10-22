//! Shared live-server harness for the final-wiring dispatcher tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::net::TcpListener;

use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{BlobStore, Clock, IdGenerator, InsertMediaInput, PortError};
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{
    EmailError, EmailMessage, EmailSender, SmtpConfig, SmtpEmailSender,
};
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
    0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 13, b'I', b'H', b'D', b'R', 0, 0, 0,
    1, 0, 0, 0, 1, 8, 6, 0, 0, 0, 0x1f, 0x15, 0xc4, 0x89, 0, 0, 0, 13, b'I', b'D', b'A', b'T',
    0x78, 0x9c, 0x63, 0x60, 0x60, 0x60, 0x00, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4,
    0, 0, 0, 0, b'I', b'E', b'N', b'D', 0xae, 0x42, 0x60, 0x82,
];

#[derive(Default)]
pub(crate) struct Recorder {
    pub(crate) sent: Mutex<Vec<EmailMessage>>,
}

impl EmailSender for Recorder {
    fn send(&self, msg: &EmailMessage) -> Result<(), EmailError> {
        self.sent.lock().unwrap().push(msg.clone());
        Ok(())
    }
}

pub(crate) struct Live {
    pub(crate) base: String,
    pub(crate) http: reqwest::Client,
    pub(crate) services: Arc<AppServices>,
    pub(crate) recorder: Arc<Recorder>,
}

impl Live {
    pub(crate) async fn start() -> Self {
        let dir =
            std::env::temp_dir().join(format!("pinetree-gaps-final-{}", uuid::Uuid::new_v4()));
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

    pub(crate) async fn api(
        &self,
        endpoint: &str,
        body: Value,
        token: Option<&str>,
    ) -> (u16, Value) {
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

    pub(crate) async fn signup(&self, username: &str) -> (String, String) {
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

    pub(crate) fn make_file(&self, owner: &str, name: &str) -> String {
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

pub(crate) fn code_of(value: &Value) -> &str {
    value
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("<no-code>")
}

pub(crate) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
