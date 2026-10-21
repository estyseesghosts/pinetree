//! WP2-part-C auth tests (service level over the real redb backend):
//! TOTP enrol/verify, the app registry, and the WebAuthn decision.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::commands::auth_2fa::{base32_decode, base32_encode, totp_token};
use pinetree::application::ports::{BlobStore, Clock, IdGenerator, NoopStreamingSink, PortError};
use pinetree::domain::admin::{PASSKEY_UNSUPPORTED_REASON, SECURITY_KEY_UNSUPPORTED_REASON};
use pinetree::domain::id::{AidxGenerator, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services() -> (AppServices, Arc<ManualClock>) {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-gaps-auth-{}-{}",
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
    let account = services.create_account(username, Some(&hash)).unwrap();
    account.id
}

fn code_of(err: PortError) -> String {
    match err {
        PortError::Validation(msg) | PortError::Conflict(msg) | PortError::NotFound(msg) => {
            msg.split_whitespace().next().unwrap_or("").to_string()
        }
        PortError::StorageUnavailable(msg) => format!("UNAVAILABLE:{msg}"),
    }
}

#[test]
fn totp_register_and_done_flow() {
    let (services, clock) = test_services();
    let alice = make_user(&services, "alice", "correct-horse");

    // Wrong password is INCORRECT_PASSWORD (verbatim Misskey code).
    let err = services.register_2fa(&alice, "wrong").unwrap_err();
    assert_eq!(code_of(err), "INCORRECT_PASSWORD");

    // Register mints a pending secret with an otpauth URL.
    let enrol = services.register_2fa(&alice, "correct-horse").unwrap();
    assert!(enrol.url.starts_with("otpauth://totp/"));
    assert_eq!(enrol.qr, enrol.url);
    assert!(!enrol.secret.is_empty());

    // TOTP at the current clock verifies; a wrong token does not.
    let now_secs = clock.now_ms().div_euclid(1000) as u64;
    let token = totp_token(&enrol.secret, now_secs).unwrap();
    assert!(token.len() == 6 && token.bytes().all(|b| b.is_ascii_digit()));
    let bad = if token == "000000" { "000001" } else { "000000" };
    assert_eq!(
        code_of(services.done_2fa(&alice, bad).unwrap_err()),
        "INVALID_TOKEN"
    );
    services.done_2fa(&alice, &token).unwrap();

    // Re-registering over a verified secret is refused.
    assert_eq!(
        code_of(services.register_2fa(&alice, "correct-horse").unwrap_err()),
        "INVALID_PARAM"
    );

    // authenticate_2fa accepts the live token.
    services.authenticate_2fa(&alice, &token).unwrap();
    assert_eq!(
        code_of(services.authenticate_2fa(&alice, bad).unwrap_err()),
        "INVALID_TOKEN"
    );
}

#[test]
fn totp_key_done_passwordless_unregister_rotate() {
    let (services, clock) = test_services();
    let bob = make_user(&services, "bob", "s3cret!");

    // key-done needs the password AND a valid token.
    let enrol = services.register_2fa(&bob, "s3cret!").unwrap();
    let err = services.confirm_2fa_key(&bob, "nope", "000000").unwrap_err();
    assert_eq!(code_of(err), "INCORRECT_PASSWORD");
    let now_secs = clock.now_ms().div_euclid(1000) as u64;
    let token = totp_token(&enrol.secret, now_secs).unwrap();
    services.confirm_2fa_key(&bob, "s3cret!", &token).unwrap();

    // passwordless toggle persists.
    services.set_2fa_passwordless(&bob, true).unwrap();

    // rotate swaps the secret; the old token no longer verifies, and the
    // rotated row is pending until re-confirmed via done.
    let enrol2 = services.rotate_2fa_key(&bob, "s3cret!").unwrap();
    assert_ne!(enrol.secret, enrol2.secret);
    assert_eq!(
        code_of(services.authenticate_2fa(&bob, &token).unwrap_err()),
        "NO_SUCH_2FA"
    );
    let token2 = totp_token(&enrol2.secret, now_secs).unwrap();
    services.done_2fa(&bob, &token2).unwrap();
    services.authenticate_2fa(&bob, &token2).unwrap();

    // unregister drops the row; further use is NO_SUCH_2FA.
    services.unregister_2fa(&bob, "s3cret!").unwrap();
    assert_eq!(
        code_of(services.authenticate_2fa(&bob, &token).unwrap_err()),
        "NO_SUCH_2FA"
    );
    assert_eq!(
        code_of(services.unregister_2fa(&bob, "s3cret!").unwrap_err()),
        "NO_SUCH_2FA"
    );
}

#[test]
fn totp_rfc6238_vector_and_base32_roundtrip() {
    // RFC 6238 Appendix B SHA-1 vector: secret "12345678901234567890",
    // T=59s -> 94287082 (8 digits) -> 6-digit 287082.
    let secret_b32 = base32_encode(b"12345678901234567890");
    assert_eq!(secret_b32, "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ");
    assert_eq!(base32_decode(&secret_b32).unwrap(), b"12345678901234567890");
    assert_eq!(totp_token(&secret_b32, 59).unwrap(), "287082");
    assert_eq!(totp_token(&secret_b32, 1111111109).unwrap(), "081804");
}

#[test]
fn webauthn_stays_400_with_documented_reason() {
    let (services, _) = test_services();
    let carol = make_user(&services, "carol", "pw123456");
    let err = services.remove_2fa_security_key(&carol, "cred-1").unwrap_err();
    let msg = match err {
        PortError::Validation(m) => m,
        other => panic!("unexpected: {other:?}"),
    };
    assert!(msg.starts_with("INVALID_PARAM"), "{msg}");
    assert!(msg.contains(SECURITY_KEY_UNSUPPORTED_REASON));
    assert!(!PASSKEY_UNSUPPORTED_REASON.is_empty());
}

#[test]
fn app_registry_create_show_my_apps() {
    let (services, _) = test_services();
    let dave = make_user(&services, "dave", "pw123456");

    let app = services
        .create_app(
            &dave,
            "my client",
            Some("test client".to_string()),
            Some("https://client.example/cb"),
            vec!["read:account".to_string()],
        )
        .unwrap();
    assert_eq!(app.name, "my client");
    assert!(!app.secret.is_empty());

    // Owner-scoped show works; unknown id is verbatim NO_SUCH_APP.
    let shown = services.show_app(&dave, &app.id).unwrap();
    assert_eq!(shown.secret, app.secret);
    assert_eq!(
        code_of(services.show_app(&dave, "nope").unwrap_err()),
        "NO_SUCH_APP"
    );

    // my/apps lists the caller's rows.
    let mine = services.list_my_apps(&dave).unwrap();
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].id, app.id);

    // Bad callback URL is refused.
    assert_eq!(
        code_of(
            services
                .create_app(&dave, "bad", None, Some("ftp://x"), vec![])
                .unwrap_err()
        ),
        "INVALID_PARAM"
    );
}

#[test]
fn admin_unset_2fa_clears_without_password() {
    let (services, clock) = test_services();
    let erin = make_user(&services, "erin", "pw123456");
    let enrol = services.register_2fa(&erin, "pw123456").unwrap();
    let now_secs = clock.now_ms().div_euclid(1000) as u64;
    services
        .done_2fa(&erin, &totp_token(&enrol.secret, now_secs).unwrap())
        .unwrap();
    services.admin_unset_2fa(&erin).unwrap();
    assert_eq!(
        code_of(
            services
                .authenticate_2fa(&erin, &totp_token(&enrol.secret, now_secs).unwrap())
                .unwrap_err()
        ),
        "NO_SUCH_2FA"
    );
    // Missing row is a no-op success.
    services.admin_unset_2fa(&erin).unwrap();
}
