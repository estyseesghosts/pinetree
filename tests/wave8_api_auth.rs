//! Wave 8 API/auth residuals (adversarial-3 §Wave 8, Agent API-Auth).
//!
//! Pins the Wave 8 contract fixes at the HTTP and service layers:
//!
//! - **Anonymous list fallback** (`users/lists/show`, `get-memberships`):
//!   the owner-view / public-view branch matches the upstream lookup
//!   (`show.ts`, `get-memberships.ts`: `{id, userId}` when `forPublic`
//!   is off and a caller is present, `{id, isPublic}` otherwise).
//!   Anonymous callers see public lists either way; a non-owner never
//!   sees a list through `forPublic: false`, even a public one.
//! - **Sign-in oracle** (`signin-flow` vs legacy `signin`): the flow
//!   endpoint keeps the upstream disclosure posture (404 unknown / 403
//!   suspended BEFORE the password check — parity requires the oracle);
//!   the legacy endpoint keeps the hardened posture (password first,
//!   existence-hiding). Both are intentional; these tests pin the
//!   chosen statuses so the pair cannot silently flip.
//! - **Deleted-account 2FA** (`i/2fa/authenticate`): a pre-deletion
//!   challenge replayed after deletion answers `NO_SUCH_2FA` (400),
//!   indistinguishable from a TOTP-less account — never 403.
//! - **`set_2fa_passwordless(false)`** with no (verified) secret is a
//!   no-op success.
//! - **Rotate-then-confirm** (`update-key` → `done`/`key-done`): the
//!   pending secret lives under a separate key, so the verified secret
//!   keeps working until the new key is confirmed (upstream
//!   `twoFactorTempSecret` parity).
//! - **Expired-challenge prune**: a failed registry remove is logged
//!   and the challenge still reports expired.
//! - **Shared note side effects**: local `create_note` and inbound
//!   `handle_inbound_create_note` build the reply/renote/mention triple
//!   through one application operation (same notifiees/kinds; only the
//!   idempotency-key suffixes differ per path).
//! - **Scheduler streaming**: `PostScheduledDraft` packs the
//!   `main`-channel `notification` body worker-side (the application
//!   layer returns the stored row as the pack input).
//!
//! Each behavior-changing test fails on the old code (noted per test);
//! the layering moves are proven by compilation (no `crate::api` import
//! remains in the application files) with behavior preserved here.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::commands::auth_2fa::totp_token;
use pinetree::application::commands::helpers::NoteSideEffectMode;
use pinetree::application::commands::pins::FeaturedCache;
use pinetree::application::events::{MainMessage, NotifyJobPayload};
use pinetree::application::ports::{
    BlobStore, Clock, CreateDraftInput, CreateNoteInput, IdGenerator, JobKind, NoopStreamingSink,
    PortError, StreamingSink,
};
use pinetree::domain::id::{AidxGenerator, ManualClock, SystemClock};
use pinetree::domain::note::Visibility;
use pinetree::domain::notification::NotificationKind;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::storage::FsBlobStore;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};
use pinetree::streaming::hub::hub_and_sink;

static N: AtomicU64 = AtomicU64::new(0);

fn test_services() -> (AppServices, Arc<ManualClock>) {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-wave8-auth-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock = Arc::new(ManualClock::new(1_760_400_000_000));
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
    let services = AppServices {
        clock: clock.clone() as Arc<dyn Clock>,
        ids,
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
        streaming: Arc::new(NoopStreamingSink),
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
    };
    (services, clock)
}

fn make_user(services: &AppServices, username: &str, password: &str) -> String {
    let hash = pinetree::api::misskey::auth::hash_password(password);
    services.create_account(username, Some(&hash)).unwrap().id
}

fn code_of(err: PortError) -> String {
    match err {
        PortError::Validation(msg) | PortError::Conflict(msg) | PortError::NotFound(msg) => {
            msg.split_whitespace().next().unwrap_or("").to_string()
        }
        PortError::StorageUnavailable(msg) => format!("UNAVAILABLE:{msg}"),
    }
}

fn issue_challenge(services: &AppServices, account_id: &str) -> String {
    services.issue_totp_challenge(account_id).unwrap()
}

fn enable_totp(services: &AppServices, account_id: &str, password: &str, now_ms: i64) -> String {
    let enrol = services.register_2fa(account_id, password).unwrap();
    let token = totp_token(&enrol.secret, now_ms.div_euclid(1000) as u64).unwrap();
    services.done_2fa(account_id, &token).unwrap();
    enrol.secret
}

// --- Live HTTP harness (endpoint-level pins) -------------------------------

struct Live {
    base: String,
    http: reqwest::Client,
    services: Arc<AppServices>,
}

impl Live {
    async fn start() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinetree-wave8-live-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("t.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
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
            public_url: "https://wave8.test".to_string(),
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
        let state = ApiState {
            services: services.clone(),
            worker,
            rate_limiter: Arc::new(RateLimiter::with_misskey_defaults(clock.clone())),
            upload_slots: Arc::new(UploadSlots::with_defaults()),
            hub,
            trusted_proxies: Arc::new(Vec::new()),
            featured_cache: Arc::new(FeaturedCache::default()),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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

    async fn api(
        &self,
        endpoint: &str,
        body: serde_json::Value,
        token: Option<&str>,
    ) -> (u16, serde_json::Value) {
        let mut map = body.as_object().cloned().unwrap_or_default();
        if let Some(token) = token {
            map.insert(
                "i".to_string(),
                serde_json::Value::String(token.to_string()),
            );
        }
        let res = self
            .http
            .post(format!("{}/api/{endpoint}", self.base))
            .json(&map)
            .send()
            .await
            .unwrap();
        let status = res.status().as_u16();
        let value: serde_json::Value = res.json().await.unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    async fn signup(&self, username: &str) -> (String, String) {
        let res = self
            .http
            .post(format!("{}/signup", self.base))
            .json(&serde_json::json!({ "username": username, "password": "s3cret-pw" }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let value: serde_json::Value = res.json().await.unwrap();
        (
            value["id"].as_str().unwrap().to_string(),
            value["token"].as_str().unwrap().to_string(),
        )
    }
}

/// Anonymous + `forPublic: false` matches upstream: public lists stay
/// visible, private lists stay hidden. Would fail if the public branch
/// were dropped for anonymous callers (or if anonymous saw private
/// lists); also pins the owner-only rule for authenticated non-owners.
#[tokio::test]
async fn lists_show_owner_public_matrix() {
    let srv = Live::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    // One public and one private list owned by alice.
    let (status, public) = srv
        .api(
            "users/lists/create",
            serde_json::json!({ "name": "pub" }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200, "{public}");
    let public_id = public["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api(
            "users/lists/update",
            serde_json::json!({ "listId": public_id, "isPublic": true }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, private) = srv
        .api(
            "users/lists/create",
            serde_json::json!({ "name": "priv" }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200, "{private}");
    let private_id = private["id"].as_str().unwrap().to_string();

    // Owner sees both with `forPublic: false` (owner view).
    for list_id in [&public_id, &private_id] {
        let (status, shown) = srv
            .api(
                "users/lists/show",
                serde_json::json!({ "listId": list_id, "forPublic": false }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(status, 200, "{shown}");
        assert_eq!(shown["id"], serde_json::json!(list_id));
    }
    // Owner sees the public one with `forPublic: true` (public view).
    let (status, shown) = srv
        .api(
            "users/lists/show",
            serde_json::json!({ "listId": public_id, "forPublic": true }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200, "{shown}");

    // Non-owner with `forPublic: false` sees NEITHER — even the public
    // list (upstream owner-only rule for the non-public view).
    for list_id in [&public_id, &private_id] {
        let (status, value) = srv
            .api(
                "users/lists/show",
                serde_json::json!({ "listId": list_id, "forPublic": false }),
                Some(&bob_token),
            )
            .await;
        assert_eq!(status, 400, "{value}");
        assert_eq!(value["error"]["code"], "NO_SUCH_LIST", "{value}");
    }
    // Non-owner with `forPublic: true` sees the public list only.
    let (status, shown) = srv
        .api(
            "users/lists/show",
            serde_json::json!({ "listId": public_id, "forPublic": true }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 200, "{shown}");
    let (status, value) = srv
        .api(
            "users/lists/show",
            serde_json::json!({ "listId": private_id, "forPublic": true }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 400, "{value}");

    // Anonymous: public list visible either way (upstream `{id,
    // isPublic}` lookup), private list never visible.
    for for_public in [false, true] {
        let (status, shown) = srv
            .api(
                "users/lists/show",
                serde_json::json!({ "listId": public_id, "forPublic": for_public }),
                None,
            )
            .await;
        assert_eq!(status, 200, "forPublic={for_public}: {shown}");
    }
    for for_public in [false, true] {
        let (status, value) = srv
            .api(
                "users/lists/show",
                serde_json::json!({ "listId": private_id, "forPublic": for_public }),
                None,
            )
            .await;
        assert_eq!(status, 400, "forPublic={for_public}: {value}");
        assert_eq!(value["error"]["code"], "NO_SUCH_LIST", "{value}");
    }
}

/// Same owner/public matrix for `users/lists/get-memberships`: the gate
/// is the same upstream lookup, so the statuses must match `show`.
#[tokio::test]
async fn lists_get_memberships_owner_public_matrix() {
    let srv = Live::start().await;
    let (_, alice_token) = srv.signup("alice").await;
    let (_, bob_token) = srv.signup("bob").await;

    let (status, public) = srv
        .api(
            "users/lists/create",
            serde_json::json!({ "name": "pub" }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200, "{public}");
    let public_id = public["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api(
            "users/lists/update",
            serde_json::json!({ "listId": public_id, "isPublic": true }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);

    // Owner, non-owner without `forPublic` (404), public view, anonymous.
    let (status, members) = srv
        .api(
            "users/lists/get-memberships",
            serde_json::json!({ "listId": public_id, "forPublic": false }),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200, "{members}");
    let (status, value) = srv
        .api(
            "users/lists/get-memberships",
            serde_json::json!({ "listId": public_id, "forPublic": false }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 400, "{value}");
    assert_eq!(value["error"]["code"], "NO_SUCH_LIST", "{value}");
    let (status, _) = srv
        .api(
            "users/lists/get-memberships",
            serde_json::json!({ "listId": public_id, "forPublic": true }),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 200);
    for for_public in [false, true] {
        let (status, _) = srv
            .api(
                "users/lists/get-memberships",
                serde_json::json!({ "listId": public_id, "forPublic": for_public }),
                None,
            )
            .await;
        assert_eq!(status, 200, "forPublic={for_public}");
    }
}

/// `signin-flow` keeps the upstream oracle: unknown → 404 before any
/// password work, suspended → 403 even with a WRONG password (the
/// password is never checked for gated accounts). Fails if the
/// pre-password gates are ever moved after the password check.
#[tokio::test]
async fn signin_flow_oracle_matches_upstream() {
    let srv = Live::start().await;
    // Unknown username: 404 without any password involved.
    let (status, value) = srv
        .api(
            "signin-flow",
            serde_json::json!({ "username": "ghost", "password": "whatever" }),
            None,
        )
        .await;
    assert_eq!(status, 404, "{value}");
    assert_eq!(value["error"]["code"], "NO_SUCH_USER", "{value}");

    let (alice_id, _) = srv.signup("alice").await;
    srv.services.suspend_user(&alice_id, &alice_id).unwrap();
    // Suspended + WRONG password still answers 403 SUSPENDED (the
    // password is never verified for gated accounts).
    let (status, value) = srv
        .api(
            "signin-flow",
            serde_json::json!({ "username": "alice", "password": "wrong-pw" }),
            None,
        )
        .await;
    assert_eq!(status, 403, "{value}");
    assert_eq!(value["error"]["code"], "USER_SUSPENDED", "{value}");

    // Deleted handles read as unknown (404).
    let (bob_id, _) = srv.signup("bob").await;
    srv.services.delete_account(None, &bob_id, None).unwrap();
    let (status, value) = srv
        .api(
            "signin-flow",
            serde_json::json!({ "username": "bob", "password": "s3cret-pw" }),
            None,
        )
        .await;
    assert_eq!(status, 404, "{value}");
    assert_eq!(value["error"]["code"], "NO_SUCH_USER", "{value}");
}

/// Legacy `signin` keeps the hardened posture: the password verifies
/// first, so unknown, wrong-password, suspended-with-wrong-password,
/// and deleted handles all answer alike (401). Only a CORRECT password
/// on a suspended account reveals the suspension (403). Fails if the
/// gates are ever moved before the password check.
#[tokio::test]
async fn legacy_signin_hides_existence() {
    let srv = Live::start().await;
    let (status, value) = srv
        .api(
            "signin",
            serde_json::json!({ "username": "ghost", "password": "whatever" }),
            None,
        )
        .await;
    assert_eq!(status, 401, "{value}");
    assert_eq!(value["error"]["code"], "AUTHENTICATION_FAILED", "{value}");

    let (alice_id, _) = srv.signup("alice").await;
    srv.services.suspend_user(&alice_id, &alice_id).unwrap();
    // Progressive auth backoff (1 s after the ghost failure above) applies
    // per IP: wait it out so this assertion tests posture, not throttling.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    // Suspended + WRONG password: indistinguishable from unknown (401).
    let (status, value) = srv
        .api(
            "signin",
            serde_json::json!({ "username": "alice", "password": "wrong-pw" }),
            None,
        )
        .await;
    assert_eq!(status, 401, "{value}");
    assert_eq!(value["error"]["code"], "AUTHENTICATION_FAILED", "{value}");
    // Two failures now back off the IP key for 2 s: wait before the
    // correct-password attempt (which must reach the handler).
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    // Suspended + CORRECT password: the suspension is revealed (403).
    let (status, value) = srv
        .api(
            "signin",
            serde_json::json!({ "username": "alice", "password": "s3cret-pw" }),
            None,
        )
        .await;
    assert_eq!(status, 403, "{value}");
    assert_eq!(value["error"]["code"], "USER_SUSPENDED", "{value}");

    // Deleted + correct password: indistinguishable from unknown (401).
    let (bob_id, _) = srv.signup("bob").await;
    srv.services.delete_account(None, &bob_id, None).unwrap();
    let (status, value) = srv
        .api(
            "signin",
            serde_json::json!({ "username": "bob", "password": "s3cret-pw" }),
            None,
        )
        .await;
    assert_eq!(status, 401, "{value}");
    assert_eq!(value["error"]["code"], "AUTHENTICATION_FAILED", "{value}");
}

/// Replaying a pre-deletion challenge after the account is deleted
/// answers `NO_SUCH_2FA` — exactly like an account that never had 2FA —
/// never 403. Fails on the old code, which answered `USER_SUSPENDED`
/// (storage marks deleted rows suspended too).
#[test]
fn deleted_2fa_challenge_replay_is_no_such_2fa() {
    let (services, clock) = test_services();
    let alice = make_user(&services, "alice", "s3cret-pw");
    let now_ms = clock.now_ms();
    let secret = enable_totp(&services, &alice, "s3cret-pw", now_ms);
    let challenge = issue_challenge(&services, &alice);
    let token = totp_token(&secret, now_ms.div_euclid(1000) as u64).unwrap();
    services.delete_account(None, &alice, None).unwrap();
    // Correct challenge + correct token, but the account is gone.
    let err = services
        .authenticate_2fa(&alice, &challenge, &token)
        .unwrap_err();
    assert_eq!(code_of(err), "NO_SUCH_2FA");

    // A deleted account that never had 2FA answers identically.
    let bob = make_user(&services, "bob", "s3cret-pw");
    services.delete_account(None, &bob, None).unwrap();
    let err = services
        .authenticate_2fa(&bob, &issue_challenge(&services, &bob), "123456")
        .unwrap_err();
    assert_eq!(code_of(err), "NO_SUCH_2FA");
}

/// `set_2fa_passwordless(false)` with no verified secret is a no-op
/// success (already effectively disabled). Fails on the old code, which
/// answered `NO_SUCH_2FA`.
#[test]
fn passwordless_false_without_secret_is_noop_success() {
    let (services, _) = test_services();
    let alice = make_user(&services, "alice", "s3cret-pw");
    // No secret at all.
    services.set_2fa_passwordless(&alice, false).unwrap();
    // Pending (never confirmed) secret only: still a no-op success.
    services.register_2fa(&alice, "s3cret-pw").unwrap();
    services.set_2fa_passwordless(&alice, false).unwrap();
    // Enabling stays refused.
    assert_eq!(
        code_of(services.set_2fa_passwordless(&alice, true).unwrap_err()),
        "INVALID_PARAM"
    );
}

/// Rotation stages a pending secret: the verified secret keeps working
/// until the new key is confirmed, and the pending key does not verify
/// before then. Fails on the old code, which clobbered the verified row
/// (old token answered `NO_SUCH_2FA` immediately after rotate).
#[test]
fn rotate_keeps_old_secret_until_confirm() {
    let (services, clock) = test_services();
    let bob = make_user(&services, "bob", "s3cret-pw");
    let now_ms = clock.now_ms();
    let old_secret = enable_totp(&services, &bob, "s3cret-pw", now_ms);
    let old_token = totp_token(&old_secret, now_ms.div_euclid(1000) as u64).unwrap();

    let enrol = services.rotate_2fa_key(&bob, "s3cret-pw").unwrap();
    assert_ne!(enrol.secret, old_secret);
    // Old secret still verifies under a fresh challenge.
    services
        .authenticate_2fa(&bob, &issue_challenge(&services, &bob), &old_token)
        .unwrap();
    // Pending secret does not verify before confirmation.
    let new_token = totp_token(&enrol.secret, now_ms.div_euclid(1000) as u64).unwrap();
    assert_eq!(
        code_of(
            services
                .authenticate_2fa(&bob, &issue_challenge(&services, &bob), &new_token)
                .unwrap_err()
        ),
        "INVALID_TOKEN"
    );
    // Confirming the new key promotes it: new works, old stops.
    services.done_2fa(&bob, &new_token).unwrap();
    // The confirmation consumed this step's code (replay protection), so
    // advance past the TOTP step before re-authenticating.
    clock.advance_ms(60_000);
    let now_ms = clock.now_ms();
    let new_token = totp_token(&enrol.secret, now_ms.div_euclid(1000) as u64).unwrap();
    services
        .authenticate_2fa(&bob, &issue_challenge(&services, &bob), &new_token)
        .unwrap();
    assert_eq!(
        code_of(
            services
                .authenticate_2fa(&bob, &issue_challenge(&services, &bob), &old_token)
                .unwrap_err()
        ),
        "INVALID_TOKEN"
    );
}

/// The password-gated confirm path (`key-done`) promotes the same
/// pending row: register → key-done → authenticate works.
#[test]
fn key_done_promotes_pending_secret() {
    let (services, clock) = test_services();
    let bob = make_user(&services, "bob", "s3cret-pw");
    let now_ms = clock.now_ms();
    let enrol = services.register_2fa(&bob, "s3cret-pw").unwrap();
    let token = totp_token(&enrol.secret, now_ms.div_euclid(1000) as u64).unwrap();
    services.confirm_2fa_key(&bob, "s3cret-pw", &token).unwrap();
    services
        .authenticate_2fa(&bob, &issue_challenge(&services, &bob), &token)
        .unwrap();
}

/// An expired challenge still reports expired when the prune remove
/// itself fails (the failure is logged, never masks the expiry).
#[test]
fn expired_challenge_prune_still_reports_expired() {
    let (services, clock) = test_services();
    let alice = make_user(&services, "alice", "s3cret-pw");
    let now_ms = clock.now_ms();
    let secret = enable_totp(&services, &alice, "s3cret-pw", now_ms);
    let challenge = issue_challenge(&services, &alice);
    // Past the 5-minute TTL the challenge is expired.
    clock.advance_ms(5 * 60 * 1000 + 1);
    // Wrong token against the expired challenge: still expired (the
    // expired row is pruned on read; a prune failure would be logged and
    // the expiry still reported).
    let err = services
        .authenticate_2fa(&alice, &challenge, "000000")
        .unwrap_err();
    assert_eq!(code_of(err), "INVALID_TOKEN");
    // A live token against the expired challenge: expired too.
    let token = totp_token(&secret, clock.now_ms().div_euclid(1000) as u64).unwrap();
    let err = services
        .authenticate_2fa(&alice, &challenge, &token)
        .unwrap_err();
    assert_eq!(code_of(err), "INVALID_TOKEN");
}

/// Account deletion verifies the password through the domain verifier
/// (moved down from the HTTP adapter): wrong password is refused, the
/// right one deletes.
#[test]
fn delete_account_password_verifies_through_domain() {
    let (services, _) = test_services();
    let password = "correct-horse";
    let hash = pinetree::domain::password::hash_password(password);
    assert!(pinetree::domain::password::verify_password(password, &hash));
    assert!(!pinetree::domain::password::verify_password("wrong", &hash));
    let alice = services.create_account("alice", Some(&hash)).unwrap().id;
    let err = services
        .delete_account(Some(&alice), &alice, Some("wrong"))
        .unwrap_err();
    assert_eq!(code_of(err), "INCORRECT_PASSWORD");
    services
        .delete_account(Some(&alice), &alice, Some(password))
        .unwrap();
}

/// Local `create_note` and inbound `handle_inbound_create_note` build
/// the reply/renote/mention triple through one application operation:
/// same notifiees and kinds, only the idempotency-key suffixes differ
/// per path (`reply` vs `inbound-reply`, …).
#[test]
fn note_interaction_jobs_shared_by_local_and_inbound() {
    let (services, _) = test_services();
    let alice = make_user(&services, "alice", "pw");
    let bob = make_user(&services, "bob", "pw");
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice, "hello"))
        .unwrap();

    let summarize = |jobs: Vec<pinetree::application::ports::NewJob>| {
        let mut out: Vec<(String, NotificationKind, String)> = jobs
            .into_iter()
            .filter(|job| job.kind == JobKind::Notify)
            .map(|job| {
                let payload: NotifyJobPayload = serde_json::from_slice(&job.payload).unwrap();
                (payload.notifiee_id, payload.kind, job.idempotency_key)
            })
            .collect();
        out.sort_by(|a, b| (&a.0, &a.2).cmp(&(&b.0, &b.2)));
        out
    };

    // A reply that also mentions the author collapses to one `Reply`.
    let local = summarize(
        services
            .note_interaction_jobs(
                &bob,
                Some(&note.id),
                None,
                Some("@alice hey"),
                None,
                false,
                NoteSideEffectMode::Local,
            )
            .unwrap()
            .jobs,
    );
    let inbound = summarize(
        services
            .note_interaction_jobs(
                &bob,
                Some(&note.id),
                None,
                Some("@alice hey"),
                None,
                false,
                NoteSideEffectMode::Inbound,
            )
            .unwrap()
            .jobs,
    );
    assert_eq!(local.len(), 1);
    assert_eq!(inbound.len(), 1);
    assert_eq!(local[0].0, alice);
    assert_eq!(local[0].1, NotificationKind::Reply);
    assert_eq!(inbound[0].0, alice);
    assert_eq!(inbound[0].1, NotificationKind::Reply);
    assert!(
        local[0].2.contains(":reply:"),
        "local suffix preserved: {}",
        local[0].2
    );
    assert!(
        inbound[0].2.contains("inbound-reply"),
        "inbound suffix preserved: {}",
        inbound[0].2
    );
}

#[derive(Default)]
struct RecordingSink {
    mains: Mutex<Vec<MainMessage>>,
}

impl StreamingSink for RecordingSink {
    fn publish(&self, _event_type: &str, _payload: &[u8]) -> Result<(), PortError> {
        Ok(())
    }
    fn push_main(&self, msg: MainMessage) -> Result<(), PortError> {
        self.mains.lock().unwrap().push(msg);
        Ok(())
    }
}

/// The scheduler path returns the stored notification as the pack input
/// and the worker packs the `main`-channel body: a due draft posts, the
/// draft is gone, and the streamed hint carries the packed posted note.
#[test]
fn scheduled_post_streams_packed_notification_via_worker() {
    let (mut services, clock) = test_services();
    let recording = Arc::new(RecordingSink::default());
    services.streaming = recording.clone() as Arc<dyn StreamingSink>;
    let alice = make_user(&services, "alice", "pw");
    let now = clock.now_ms();
    let draft = services
        .create_draft(
            &alice,
            CreateDraftInput {
                text: Some("scheduled body".to_string()),
                visibility: Visibility::Public,
                scheduled_at_ms: Some(now + 60_000),
                is_actually_scheduled: true,
                ..Default::default()
            },
        )
        .unwrap();
    let services = Arc::new(services);
    let worker = JobWorker {
        clock: services.clock.clone(),
        ids: services.ids.clone(),
        jobs: services.jobs.clone(),
        notifications: services.notifications.clone(),
        streaming: recording.clone() as Arc<dyn StreamingSink>,
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 30_000,
        base_backoff_ms: 1_000,
        transcode_config: TranscodeConfig::default(),
        email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
    };
    worker.drain(100).unwrap();
    assert!(services.notes.get_draft(&draft.id).unwrap().is_some());

    clock.advance_ms(61_000);
    worker.drain(100).unwrap();
    assert!(services.notes.get_draft(&draft.id).unwrap().is_none());
    let mains = recording.mains.lock().unwrap();
    let notification = mains
        .iter()
        .find(|msg| msg.kind == pinetree::application::events::MainEventKind::Notification)
        .expect("scheduler notification hint streamed");
    assert_eq!(notification.user_id, alice);
    assert_eq!(notification.body["type"], "scheduledNotePosted");
    assert!(notification.body.get("note").is_some());
    assert_eq!(
        notification.body["note"]["text"],
        serde_json::json!("scheduled body")
    );
}
