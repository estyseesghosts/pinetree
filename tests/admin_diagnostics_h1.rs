//! Batch H admin diagnostics (prod-ready.md §9 H4/H6): the authenticated
//! admin status + backup surface is schema-stable, redacted, flag-gated,
//! and honest about unknown values; readiness fails while liveness stays
//! up when a critical storage root disappears.
//!
//! Hermetic: every test boots a loopback router (real redb store in a temp
//! dir, in-memory blobs) and drives it over HTTP with per-call timeouts.
//! Process-wide ops cells (`set_ops_config` / `set_backup_context`) are
//! reconfigured per test under a serializing lock — tests in this file
//! must hold `_guard` for their whole body.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use uuid::Uuid;

use pinetree::api::misskey::endpoints::{OpsConfig, set_backup_context, set_ops_config};
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, Clock, IdGenerator, JobKind, NewJob, PortError, StreamingSink,
};
use pinetree::backup::{BackupContext, BackupState};
use pinetree::domain::id::{AidxGenerator, SystemClock};
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

struct Harness {
    base: String,
    client: reqwest::Client,
    services: Arc<AppServices>,
    durable_dir: std::path::PathBuf,
    cache_dir: std::path::PathBuf,
    backup_dir: std::path::PathBuf,
}

impl Harness {
    async fn start(detailed_admin_status: bool, ffmpeg_status: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "pinetree-admin-h1-{}-{}",
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
            durable_dir,
            cache_dir,
            backup_dir,
        }
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
                serde_json::json!({ "username": username, "password": "h1-secret-pw" }),
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

#[tokio::test]
async fn anon_is_denied_admin_status() {
    let _guard = serial();
    let harness = Harness::start(false, "disabled").await;
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), None)
        .await;
    assert_eq!(status, 401, "{value}");
    let (status, _) = harness
        .api("admin/backups", serde_json::json!({}), None)
        .await;
    assert_eq!(status, 401);
    let (status, _) = harness
        .api("admin/backup", serde_json::json!({}), None)
        .await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn non_admin_is_denied_admin_status() {
    let _guard = serial();
    let harness = Harness::start(false, "disabled").await;
    let (_id, token) = harness.signup("h1bob").await;
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 403, "{value}");
    let (status, _) = harness
        .api("admin/backup", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 403);
}

#[tokio::test]
async fn flag_off_hides_detailed_fields() {
    let _guard = serial();
    let harness = Harness::start(false, "disabled").await;
    let (admin_id, token) = harness.signup("h1alice").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    // Basic status works with the flag off.
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 200, "{value}");
    assert!(value["version"].is_string(), "{value}");
    assert_eq!(value["safeMode"]["detailedAdminStatus"], false);
    assert!(value["readiness"]["ready"].is_boolean(), "{value}");
    assert_eq!(value["redb"]["readable"], true);
    // ...but the expensive/private fields are absent, not merely empty.
    for key in [
        "recentErrors",
        "errorCounts",
        "configFingerprint",
        "startupProbes",
    ] {
        assert!(value.get(key).is_none(), "flag-off leaks {key}: {value}");
    }
    // Explicit detail request is refused, not downgraded.
    let (status, _) = harness
        .api(
            "admin/status",
            serde_json::json!({"detailed": true}),
            Some(&token),
        )
        .await;
    assert_eq!(status, 403);
}

#[tokio::test]
async fn flag_on_still_requires_admin() {
    let _guard = serial();
    let harness = Harness::start(true, "disabled").await;
    let (status, _) = harness
        .api("admin/status", serde_json::json!({"detailed": true}), None)
        .await;
    assert_eq!(status, 401);
    let (_id, user_token) = harness.signup("h1carol").await;
    let (status, _) = harness
        .api(
            "admin/status",
            serde_json::json!({"detailed": true}),
            Some(&user_token),
        )
        .await;
    assert_eq!(status, 403);
    let (admin_id, admin_token) = harness.signup("h1dave").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    let (status, value) = harness
        .api(
            "admin/status",
            serde_json::json!({"detailed": true}),
            Some(&admin_token),
        )
        .await;
    assert_eq!(status, 200, "{value}");
    assert!(value["recentErrors"].is_array(), "{value}");
    assert!(value["errorCounts"].is_object(), "{value}");
    assert_eq!(value["configFingerprint"]["test"], true);
    assert!(value["startupProbes"]["transcode"].is_string(), "{value}");
}

#[tokio::test]
async fn basic_status_carries_no_secrets() {
    let _guard = serial();
    let harness = Harness::start(false, "disabled").await;
    let (admin_id, token) = harness.signup("h1erin").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 200, "{value}");
    let text = serde_json::to_string(&value).expect("serialize");
    for secret in [
        "h1-secret-pw",
        &token,
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
    // Unknown-by-design values are explicit nulls, and transcode reports
    // the disabled toolchain line (never a binary dump).
    assert!(value["durableUsedBytes"].is_null(), "{value}");
    assert!(value["freeDiskBytes"].is_null(), "{value}");
    assert_eq!(value["transcode"]["enabled"], false);
    assert_eq!(value["transcode"]["status"], "disabled");
}

#[tokio::test]
async fn admin_backup_roundtrip_is_accepted_fast() {
    let _guard = serial();
    let harness = Harness::start(false, "disabled").await;
    let (admin_id, token) = harness.signup("h1frank").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    // Unknown job id first (no setup needed).
    let (status, _) = harness
        .api(
            "admin/backup-status",
            serde_json::json!({"jobId": "bk-nope-1"}),
            Some(&token),
        )
        .await;
    assert_eq!(status, 400);
    // Request → 202 + job id (accepted-fast; no worker drain running here).
    let (status, value) = harness
        .api("admin/backup", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 202, "{value}");
    let job_id = value["jobId"].as_str().expect("job id").to_string();
    // Status shows the queued record with no secrets.
    let (status, value) = harness
        .api(
            "admin/backup-status",
            serde_json::json!({"jobId": job_id}),
            Some(&token),
        )
        .await;
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["state"], "queued");
    assert!(value["failureRedacted"].is_null(), "{value}");
    // List contains it (bounded snapshot).
    let (status, value) = harness
        .api("admin/backups", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 200, "{value}");
    let items = value.as_array().expect("array");
    assert!(items.iter().any(|item| item["jobId"] == job_id), "{value}");
}

#[tokio::test]
async fn incomplete_backup_is_failed_not_final() {
    let _guard = serial();
    let harness = Harness::start(false, "disabled").await;
    let (admin_id, token) = harness.signup("h1grace").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    // Simulate a crash between staging and publish: a `running` status
    // file plus an abandoned `.partial-*` bundle, then the startup sweep.
    let job_id = "bk-h1-crashed-1";
    std::fs::create_dir_all(&harness.backup_dir).expect("backup dir");
    std::fs::write(
        harness.backup_dir.join(format!("{job_id}.status.json")),
        serde_json::json!({
            "job_id": job_id,
            "state": "running",
            "requested_ms": 1_700_000_000_000u64,
            "started_ms": 1_700_000_000_001u64,
            "finished_ms": None::<u64>,
            "attempts": 1u32,
            "output_file": None::<String>,
            "bytes_written": None::<u64>,
            "tables_exported": 0u64,
            "media_files": 0u64,
            "gate_wait_ms": 0u64,
            "failure_redacted": None::<String>,
        })
        .to_string(),
    )
    .expect("write stale status");
    std::fs::write(
        harness.backup_dir.join(".partial-bk-h1-crashed-1.zip"),
        b"junk",
    )
    .expect("write partial");
    let report =
        pinetree::backup::cleanup_abandoned_partials(&harness.backup_dir).expect("startup cleanup");
    assert_eq!(report.partials_removed, 1);
    assert_eq!(report.stale_running_failed, 1);
    // The admin surface reports it failed with a redacted reason — never
    // as a completed backup.
    let (status, value) = harness
        .api(
            "admin/backup-status",
            serde_json::json!({"jobId": job_id}),
            Some(&token),
        )
        .await;
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["state"], "failed");
    assert_eq!(value["failureRedacted"], "abandoned-restart");
}

#[tokio::test]
async fn backlog_and_toolchain_show_in_status() {
    let _guard = serial();
    let harness = Harness::start(false, "ffmpeg version 7.1-test").await;
    let (admin_id, token) = harness.signup("h1heidi").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    harness
        .services
        .jobs
        .enqueue_jobs(vec![
            NewJob {
                kind: JobKind::FederateDeliver,
                payload: Vec::new(),
                idempotency_key: "h1-job-1".to_string(),
                not_before: 0,
            },
            NewJob {
                kind: JobKind::Notify,
                payload: Vec::new(),
                idempotency_key: "h1-job-2".to_string(),
                not_before: 0,
            },
        ])
        .expect("enqueue");
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["jobs"]["pending"], 2);
    // No per-kind/quarantine port yet: explicit null, never a guess.
    assert!(value["jobBreakdown"].is_null(), "{value}");
    assert!(value["quarantine"].is_null(), "{value}");
    assert_eq!(value["transcode"]["status"], "ffmpeg version 7.1-test");
}

#[tokio::test]
async fn readiness_fails_while_liveness_stays_up() {
    let _guard = serial();
    let harness = Harness::start(false, "disabled").await;
    // Healthy first: both probes green.
    let (status, _) = harness.get("/healthz").await;
    assert_eq!(status, 200);
    let (status, _) = harness.get("/ready").await;
    assert_eq!(status, 200);
    // Simulate a lost cache pool: readiness must go non-2xx while the
    // process/event loop (liveness) is unaffected.
    set_ops_config(OpsConfig {
        detailed_admin_status: false,
        durable_dir: harness.durable_dir.clone(),
        cache_dir: harness.backup_dir.join("no-such-pool"),
        backup_dir: harness.backup_dir.clone(),
        transcode_enabled: false,
        ffmpeg_status: "disabled".to_string(),
        config_fingerprint: serde_json::Value::Null,
        started_ms: 1_700_000_000_000,
    });
    let (status, _) = harness.get("/ready").await;
    assert_eq!(status, 503);
    let (status, _) = harness.get("/healthz").await;
    assert_eq!(status, 200);
    // The admin surface exposes the probe error class (no paths).
    let (admin_id, token) = harness.signup("h1ivan").await;
    harness
        .services
        .set_role(None, &admin_id, true, false)
        .expect("make admin");
    let (status, value) = harness
        .api("admin/status", serde_json::json!({}), Some(&token))
        .await;
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["readiness"]["ready"], false);
    assert_eq!(
        value["readiness"]["lastProbeErrorClass"],
        "storage-root-missing"
    );
    // Restore the healthy roots for the next test in this binary.
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

#[test]
fn backup_state_serializes_lowercase() {
    assert_eq!(
        serde_json::to_value(BackupState::Queued).expect("serialize"),
        serde_json::json!("queued")
    );
}
