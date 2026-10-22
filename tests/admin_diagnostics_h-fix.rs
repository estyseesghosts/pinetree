//! Batch H fix verification (prod-ready.md §9 H4/H5): moderator-without-admin
//! stays denied on both status arms, a missing durable root fails readiness
//! while liveness stays up, a redb outage marks the store unreadable with a
//! stable error class, and the detailed arm stays bounded.
//!
//! Hermetic: each test boots a loopback router (real redb store in a temp
//! dir, in-memory blobs) and drives it over HTTP with per-call timeouts.
//! Process-wide ops cells (`set_ops_config` / `set_backup_context`) are
//! reconfigured per test under a serializing lock — tests in this file must
//! hold `_guard` for their whole body. This file never touches
//! `tests/admin_diagnostics_h1.rs`; it only adds the moderator, durable,
//! redb-outage, and bounded-detail cases the H-fix owns.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use uuid::Uuid;

use pinetree::api::misskey::endpoints::{OpsConfig, set_backup_context, set_ops_config};
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, Clock, IdGenerator, InstanceSettingsPatch, InstanceSettingsStore,
    JobKind, NewJob, PortError, StreamingSink,
};
use pinetree::backup::{BackupContext, BackupState};
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

/// Serializes this file's tests: the ops/backup cells are process-wide.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// In-memory blob store: diagnostics tests never need durable bytes.
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
/// switch. `fail == true` makes every `get`/`update` answer
/// `StorageUnavailable`, approximating a dead redb backend through the exact
/// code path `/ready` and `admin/status` read (`instance_settings.get()`).
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
    async fn start(detailed_admin_status: bool, ffmpeg_status: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "pinetree-admin-hfix-{}-{}",
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
            ffmpeg_status: ffmpeg_status.to_string(),
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
                serde_json::json!({ "username": username, "password": "hfix-secret-pw" }),
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
}

/// Moderator-but-not-admin is denied on both status arms even with the
/// detailed flag on; a real admin still passes both arms on the same router.
#[tokio::test]
async fn moderator_without_admin_is_denied_both_arms() {
    let _guard = serial();
    let harness = Harness::start(true, "disabled").await;
    let (mod_id, mod_token) = harness.signup("hfixmod").await;
    harness
        .services
        .set_role(None, &mod_id, false, true)
        .expect("make moderator");
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&mod_token))
        .await;
    assert_eq!(status, 403, "{value}");
    assert_eq!(value["code"], "PERMISSION_DENIED");
    let (status, value) = harness
        .api(
            "admin/status",
            serde_json::json!({"detailed": true}),
            Some(&mod_token),
        )
        .await;
    assert_eq!(status, 403, "{value}");
    assert_eq!(value["code"], "PERMISSION_DENIED");
    // The admin path on the same router still works (basic + detailed).
    let (admin_id, admin_token) = harness.signup("hfixadmin").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&admin_token))
        .await;
    assert_eq!(status, 200, "{value}");
    let (status, value) = harness
        .api(
            "admin/status",
            serde_json::json!({"detailed": true}),
            Some(&admin_token),
        )
        .await;
    assert_eq!(status, 200, "{value}");
    assert!(value["recentErrors"].is_array(), "{value}");
}

/// A missing durable pool root fails readiness (503, stable error class)
/// while liveness stays up; the admin surface reports the probe class.
#[tokio::test]
async fn durable_root_missing_fails_ready_not_liveness() {
    let _guard = serial();
    let harness = Harness::start(false, "disabled").await;
    let (admin_id, token) = harness.signup("hfixdurable").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    let (status, body) = harness.get("/healthz").await;
    assert_eq!(status, 200);
    assert_eq!(body, "ok");
    let (status, body) = harness.get("/ready").await;
    assert_eq!(status, 200);
    assert_eq!(body, "ready");
    set_ops_config(OpsConfig {
        detailed_admin_status: false,
        durable_dir: harness.backup_dir.join("no-such-durable"),
        cache_dir: harness.cache_dir.clone(),
        backup_dir: harness.backup_dir.clone(),
        transcode_enabled: false,
        ffmpeg_status: "disabled".to_string(),
        config_fingerprint: serde_json::Value::Null,
        started_ms: 1_700_000_000_000,
    });
    let (status, body) = harness.get("/ready").await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body, "not ready");
    let (status, _) = harness.get("/healthz").await;
    assert_eq!(status, 200);
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["readiness"]["ready"], false);
    assert_eq!(
        value["readiness"]["lastProbeErrorClass"],
        "storage-root-missing"
    );
    set_ops_config(OpsConfig {
        detailed_admin_status: false,
        durable_dir: harness.durable_dir.clone(),
        cache_dir: harness.cache_dir.clone(),
        backup_dir: harness.backup_dir.clone(),
        transcode_enabled: false,
        ffmpeg_status: "disabled".to_string(),
        config_fingerprint: serde_json::Value::Null,
        started_ms: 1_700_000_000_000,
    });
    let (status, _) = harness.get("/ready").await;
    assert_eq!(status, 200);
}

/// A redb outage (every settings read fails) answers 503 on `/ready` with
/// the stable `storage-unavailable` class while `/healthz` stays 200; the
/// admin surface marks redb unreadable instead of failing.
#[tokio::test]
async fn redb_unavailable_marks_store_unreadable() {
    let _guard = serial();
    let harness = Harness::start(false, "disabled").await;
    let (admin_id, token) = harness.signup("hfixredb").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["redb"]["readable"], true);
    harness.set_settings_fail(true);
    let (status, body) = harness.get("/ready").await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body, "not ready");
    let (status, _) = harness.get("/healthz").await;
    assert_eq!(status, 200);
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["redb"]["readable"], false);
    assert_eq!(value["readiness"]["ready"], false);
    assert_eq!(
        value["readiness"]["lastProbeErrorClass"],
        "storage-unavailable"
    );
    harness.set_settings_fail(false);
    let (status, _) = harness.get("/ready").await;
    assert_eq!(status, 200);
}

/// The status schema stays stable and bounded: presence numbers are real,
/// unknown aggregates stay explicit nulls, the detailed arm is capped, and
/// no secret leaks into either arm.
#[tokio::test]
async fn status_shape_stays_bounded_with_websocket() {
    let _guard = serial();
    let harness = Harness::start(true, "disabled").await;
    let (admin_id, token) = harness.signup("hfixshape").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    harness
        .services
        .jobs
        .enqueue_jobs(vec![NewJob {
            kind: JobKind::Notify,
            payload: Vec::new(),
            idempotency_key: "hfix-job-1".to_string(),
            not_before: 0,
        }])
        .expect("enqueue");
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 200, "{value}");
    assert!(value["webSocket"]["connections"].is_number(), "{value}");
    assert!(value["webSocket"]["onlineAccounts"].is_number(), "{value}");
    assert!(value["http"]["rateLimitBuckets"].is_number(), "{value}");
    assert_eq!(value["jobs"]["pending"], 1);
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
        assert!(value[key].is_null(), "{key} must stay honest null: {value}");
    }
    let (status, detailed) = harness
        .api(
            "admin/status",
            serde_json::json!({"detailed": true}),
            Some(&token),
        )
        .await;
    assert_eq!(status, 200, "{detailed}");
    let recent = detailed["recentErrors"].as_array().expect("array");
    // `STATUS_DETAIL_CAP` (32): the detailed arm never grows past it.
    assert!(recent.len() <= 32, "len={}: {detailed}", recent.len());
    assert!(detailed["errorCounts"].is_object(), "{detailed}");
    assert!(detailed["startupProbes"].is_object(), "{detailed}");
    for body in [&value, &detailed] {
        let text = serde_json::to_string(body).expect("serialize");
        for secret in [
            "hfix-secret-pw",
            "password",
            "private",
            "secret",
            "cookie",
            "authorization",
            "smtp",
        ] {
            assert!(
                !text.to_ascii_lowercase().contains(secret),
                "status leaks {secret:?}"
            );
        }
        assert!(
            !text
                .to_ascii_lowercase()
                .contains(&token.to_ascii_lowercase()),
            "status leaks the bearer token"
        );
    }
    assert_eq!(
        serde_json::to_value(BackupState::Queued).expect("serialize"),
        serde_json::json!("queued")
    );
}
