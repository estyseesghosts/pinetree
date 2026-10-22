//! TOTP challenge consume race (audit follow-up WP2 §4).
//!
//! `authenticate_2fa` must validate the challenge, enforce the
//! cross-challenge replay guard, record the TOTP step, and retire the
//! challenge in one durable transaction, so two concurrent logins with
//! the same code mint at most one session. These tests drive the command
//! against the real redb backend with `std::thread::scope` (no channels
//! or in-test locks: the loser's `Err` is the assertion) and never print
//! secrets, challenges, or tokens.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use pinetree::application::commands::AppServices;
use pinetree::application::commands::auth_2fa::totp_token;
use pinetree::application::ports::BlobStore;
use pinetree::application::ports::Clock;
use pinetree::application::ports::IdGenerator;
use pinetree::application::ports::NoopStreamingSink;
use pinetree::application::ports::PortError;
use pinetree::application::ports::RegistryItem;
use pinetree::application::ports::RegistryStore;
use pinetree::domain::admin::SCOPE_2FA;
use pinetree::domain::id::AidxGenerator;
use pinetree::domain::id::ManualClock;
use pinetree::federation::delivery::SafeHttpClient;
use pinetree::federation::delivery::SafeHttpConfig;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

struct Parts {
    dir: PathBuf,
    clock: Arc<ManualClock>,
    ids: Arc<dyn IdGenerator>,
    store: Arc<RedbStore>,
}

fn boot() -> Parts {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir: PathBuf = std::env::temp_dir().join(format!(
        "pinetree-totp-race-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock = Arc::new(ManualClock::new(1_760_400_000_000));
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
    Parts {
        dir,
        clock,
        ids,
        store,
    }
}

fn assemble(parts: &Parts, registry: Arc<dyn RegistryStore>) -> AppServices {
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(parts.dir.join("blobs")).unwrap());
    AppServices {
        clock: parts.clock.clone() as Arc<dyn Clock>,
        ids: parts.ids.clone(),
        accounts: parts.store.clone(),
        notes: parts.store.clone(),
        follows: parts.store.clone(),
        blocks: parts.store.clone(),
        mutes: parts.store.clone(),
        reactions: parts.store.clone(),
        favorites: parts.store.clone(),
        emojis: parts.store.clone(),
        antennas: parts.store.clone(),
        antenna_notes: parts.store.clone(),
        polls: parts.store.clone(),
        jobs: parts.store.clone(),
        media: parts.store.clone(),
        blobs: blobs.clone(),
        notifications: parts.store.clone(),
        auth: parts.store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        dedup: parts.store.clone(),
        host_caps: parts.store.clone(),
        moderation: parts.store.clone(),
        instance_settings: parts.store.clone(),
        invites: parts.store.clone(),
        abuse_reports: parts.store.clone(),
        lists: parts.store.clone(),
        signin: parts.store.clone(),
        password_resets: parts.store.clone(),
        registry,
        clips: parts.store.clone(),
        pages: parts.store.clone(),
        folders: parts.store.clone(),
        system_webhooks: parts.store.clone(),
        user_webhooks: parts.store.clone(),
        abuse_recipients: parts.store.clone(),
        memos: parts.store.clone(),
        thread_mutes: parts.store.clone(),
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
    }
}

fn test_services() -> (AppServices, Arc<ManualClock>) {
    let parts = boot();
    let registry: Arc<dyn RegistryStore> = parts.store.clone();
    let services = assemble(&parts, registry);
    (services, parts.clock)
}

fn make_user(services: &AppServices, username: &str, password: &str) -> String {
    let hash = pinetree::api::misskey::auth::hash_password(password);
    let account = services.create_account(username, Some(&hash)).unwrap();
    account.id
}

fn now_secs(clock: &ManualClock) -> u64 {
    clock.now_ms().div_euclid(1000) as u64
}

/// Register + confirm a TOTP secret, returning it (never logged).
fn enrol_verified(
    services: &AppServices,
    account_id: &str,
    password: &str,
    clock: &ManualClock,
) -> String {
    let enrol = services.register_2fa(account_id, password).unwrap();
    let token = totp_token(&enrol.secret, now_secs(clock)).unwrap();
    services.done_2fa(account_id, &token).unwrap();
    enrol.secret
}

fn scope_2fa() -> Vec<String> {
    vec![SCOPE_2FA.to_string()]
}

fn code_of(err: &PortError) -> String {
    match err {
        PortError::Validation(message)
        | PortError::Conflict(message)
        | PortError::NotFound(message) => {
            message.split_whitespace().next().unwrap_or("").to_string()
        }
        PortError::StorageUnavailable(_) => "UNAVAILABLE".to_string(),
    }
}

#[test]
fn same_challenge_race_mints_once() {
    let (services, clock) = test_services();
    let account_id = make_user(&services, "racealice", "correct-horse");
    let secret = enrol_verified(&services, &account_id, "correct-horse", &clock);
    let challenge = services.issue_totp_challenge(&account_id).unwrap();
    let token = totp_token(&secret, now_secs(&clock)).unwrap();

    // Pin both threads on a barrier so the two consumes overlap; redb
    // serializes the write transactions and the atomic consume lets
    // exactly one commit. No in-test lock guards the calls: the loser's
    // `Err` is the assertion.
    let barrier = Barrier::new(2);
    let (first, second) = std::thread::scope(|task| {
        let left = task.spawn(|| {
            barrier.wait();
            services.authenticate_2fa(&account_id, &challenge, &token)
        });
        let right = task.spawn(|| {
            barrier.wait();
            services.authenticate_2fa(&account_id, &challenge, &token)
        });
        (
            left.join().expect("left racer must finish"),
            right.join().expect("right racer must finish"),
        )
    });
    let oks = [&first, &second]
        .iter()
        .filter(|result| result.is_ok())
        .count();
    assert_eq!(
        oks, 1,
        "same challenge and code raced: exactly one login succeeds"
    );
    for result in [&first, &second] {
        if let Err(err) = result {
            assert_eq!(code_of(err), "INVALID_TOKEN");
        }
    }
    // The consumed challenge retires: a follow-up that would mint is
    // rejected, so only one session could ever come out of the race.
    let again = services.authenticate_2fa(&account_id, &challenge, &token);
    assert!(again.is_err(), "consumed challenge must not mint again");
    let err = again.unwrap_err();
    assert_eq!(code_of(&err), "INVALID_TOKEN");
}

#[test]
fn different_challenges_same_step_mints_once() {
    let (services, clock) = test_services();
    let account_id = make_user(&services, "racebob", "correct-horse");
    let secret = enrol_verified(&services, &account_id, "correct-horse", &clock);
    // Issuing a second challenge replaces the first: only one row is
    // live per account, so the stale challenge must lose even though
    // both threads present the same valid TOTP step.
    let stale = services.issue_totp_challenge(&account_id).unwrap();
    let live = services.issue_totp_challenge(&account_id).unwrap();
    assert!(
        !stale.is_empty() && !live.is_empty() && stale != live,
        "a fresh challenge must rotate the live row"
    );
    let token = totp_token(&secret, now_secs(&clock)).unwrap();

    let barrier = Barrier::new(2);
    let (stale_result, live_result) = std::thread::scope(|task| {
        let stale_racer = task.spawn(|| {
            barrier.wait();
            services.authenticate_2fa(&account_id, &stale, &token)
        });
        let live_racer = task.spawn(|| {
            barrier.wait();
            services.authenticate_2fa(&account_id, &live, &token)
        });
        (
            stale_racer.join().expect("stale racer must finish"),
            live_racer.join().expect("live racer must finish"),
        )
    });
    let oks = [&stale_result, &live_result]
        .iter()
        .filter(|result| result.is_ok())
        .count();
    assert_eq!(
        oks, 1,
        "same step under two challenges: exactly one login succeeds"
    );
    assert!(
        stale_result.is_err(),
        "the replaced challenge must not authenticate"
    );
    assert!(
        live_result.is_ok(),
        "the live challenge authenticates exactly once"
    );
    // The consumed step is recorded: the same code under a fresh
    // challenge is a cross-challenge replay, not a second mint.
    let fresh = services.issue_totp_challenge(&account_id).unwrap();
    let replay = services.authenticate_2fa(&account_id, &fresh, &token);
    assert!(replay.is_err(), "same step must not mint twice");
    let err = replay.unwrap_err();
    assert_eq!(code_of(&err), "INVALID_TOKEN");
    if let PortError::Validation(message) = &err {
        assert!(
            message.contains("token already used"),
            "the replay guard fires across challenges"
        );
    } else {
        panic!("replay must report the consumed step");
    }
}

#[test]
fn sequential_replay_of_same_challenge_fails() {
    let (services, clock) = test_services();
    let account_id = make_user(&services, "racecarol", "correct-horse");
    let secret = enrol_verified(&services, &account_id, "correct-horse", &clock);
    let challenge = services.issue_totp_challenge(&account_id).unwrap();
    let token = totp_token(&secret, now_secs(&clock)).unwrap();
    services
        .authenticate_2fa(&account_id, &challenge, &token)
        .unwrap();
    let replay = services.authenticate_2fa(&account_id, &challenge, &token);
    assert!(replay.is_err(), "consumed challenge must not mint again");
    let err = replay.unwrap_err();
    assert_eq!(code_of(&err), "INVALID_TOKEN");
}

#[test]
fn expired_challenge_fails_and_is_pruned() {
    let (services, clock) = test_services();
    let account_id = make_user(&services, "racedave", "correct-horse");
    let secret = enrol_verified(&services, &account_id, "correct-horse", &clock);
    let challenge = services.issue_totp_challenge(&account_id).unwrap();
    // Move past the five-minute challenge TTL, then mint a code that is
    // valid at the new clock: the failure must be expiry, not the code.
    clock.advance_ms(10 * 60 * 1000);
    let token = totp_token(&secret, now_secs(&clock)).unwrap();
    let err = services
        .authenticate_2fa(&account_id, &challenge, &token)
        .unwrap_err();
    assert_eq!(code_of(&err), "INVALID_TOKEN");
    let row = services
        .registry
        .registry_get(&account_id, None, &scope_2fa(), "totp_challenge")
        .unwrap();
    assert!(row.is_none(), "the expired challenge is pruned on use");
}

/// Registry double that delegates every read/write to the real store but
/// fails the atomic consume: proves a store error surfaces instead of
/// minting a session. No lock is needed — every method is a straight
/// delegation except the injected failure.
struct FailConsumeRegistry {
    inner: Arc<RedbStore>,
}

impl RegistryStore for FailConsumeRegistry {
    fn registry_set(&self, item: RegistryItem) -> Result<(), PortError> {
        self.inner.registry_set(item)
    }

    fn registry_get(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
        key: &str,
    ) -> Result<Option<RegistryItem>, PortError> {
        self.inner.registry_get(user_id, domain, scope, key)
    }

    fn registry_list(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
    ) -> Result<Vec<RegistryItem>, PortError> {
        self.inner.registry_list(user_id, domain, scope)
    }

    fn registry_remove(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
        key: &str,
    ) -> Result<(), PortError> {
        self.inner.registry_remove(user_id, domain, scope, key)
    }

    fn registry_scopes(
        &self,
        user_id: &str,
    ) -> Result<Vec<(Option<String>, Vec<String>)>, PortError> {
        self.inner.registry_scopes(user_id)
    }

    fn consume_totp_login(
        &self,
        _account_id: &str,
        _challenge: &str,
        _matched_step: u64,
        _now_ms: i64,
    ) -> Result<(), PortError> {
        Err(PortError::StorageUnavailable(
            "injected consume failure".to_string(),
        ))
    }
}

#[test]
fn consume_store_error_is_not_success() {
    let parts = boot();
    let registry: Arc<dyn RegistryStore> = Arc::new(FailConsumeRegistry {
        inner: parts.store.clone(),
    });
    let services = assemble(&parts, registry);
    let clock = parts.clock;
    let account_id = make_user(&services, "raceerin", "correct-horse");
    let secret = enrol_verified(&services, &account_id, "correct-horse", &clock);
    let challenge = services.issue_totp_challenge(&account_id).unwrap();
    let token = totp_token(&secret, now_secs(&clock)).unwrap();
    let err = services
        .authenticate_2fa(&account_id, &challenge, &token)
        .unwrap_err();
    assert!(
        matches!(err, PortError::StorageUnavailable(_)),
        "a consume failure must surface as a store error"
    );
    // The failed consume commits nothing: the challenge row is intact.
    let row = services
        .registry
        .registry_get(&account_id, None, &scope_2fa(), "totp_challenge")
        .unwrap();
    assert!(
        row.is_some(),
        "a failed consume must not retire the challenge"
    );
}
