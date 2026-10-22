//! Shared fixtures for the `federation_sig_mode_e5` signature-mode tests
//! (see `tests/federation_sig_mode_e5.rs`): static keypairs, header builders,
//! the process-wide mode cell, and the hermetic `AppServices` harness. No
//! `#[test]` lives here.

use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use serde_json::json;

use pinetree::activitypub::signatures;
use pinetree::application::commands::AppServices;
use pinetree::application::ports::ActivityDedup;
use pinetree::application::ports::Clock;
use pinetree::application::ports::NoopStreamingSink;
use pinetree::config::RemoteCacheMode;
use pinetree::config::Rfc9421Mode;
use pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT;
use pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT;
use pinetree::domain::id::SystemClock;
use pinetree::domain::pinning::DEFAULT_PIN_LIMIT;
use pinetree::federation::delivery;
use pinetree::federation::delivery::SafeHttpClient;
use pinetree::federation::delivery::SafeHttpConfig;
use pinetree::federation::inbox;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Remote signer under test (key owner for every cached-key assertion).
pub(crate) const REMOTE_ACTOR: &str = "https://remote.example/users/alice";
/// Key id bound to [`REMOTE_ACTOR`] in the pre-seeded key cache.
pub(crate) const REMOTE_KEY_ID: &str = "https://remote.example/users/alice#main-key";
/// A second actor used only for mismatch/authority-negative tests.
pub(crate) const OTHER_ACTOR: &str = "https://remote.example/users/mallory";
/// Second actor's key id (signed with the runtime-generated key).
pub(crate) const OTHER_KEY_ID: &str = "https://remote.example/users/mallory#main-key";
/// Shared-inbox request target exercised by every inbox test.
pub(crate) const INBOX_PATH: &str = "/inbox";
/// `Host` header carried by every signed test request.
pub(crate) const REQUEST_HOST: &str = "local.test";
/// `public_url` of the harness services (drives `@scheme` reconstruction).
pub(crate) const REQUEST_PUBLIC_URL: &str = "https://local.test";

/// Static 2048-bit fixture pair (the Node-crypto interop pair from the
/// `signatures.rs` unit tests, copied so this integration binary pays no
/// key-generation cost for the common case).
pub(crate) const FIXTURE_PRIVATE_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0HwE4w8IWM74o\n\
G5kVUz5SVn3z7nKJpNEUlhMzM0e0/4nWOmGJN7pVEstNj3aZUz6nzOIjPuGSWHu1\n\
r9j7sesR86SwRkb7t29KikYl/1gFeYJ5g0Y6MASNguHH4jzwJCzDde+7+efNckM5\n\
B7TuFaa8XsTUDKKac5WLduNA2urIJDflT2EMlkVQB9zygd5iOp27s3+PLRVi35T6\n\
8JCh3xrP4W5+Kntu/+FKdUEs5RvrXN3jiSwN/WumIDdDrXuhiHn/XxTmPJxpNvF5\n\
ZT+I18wdU3Q31tnw6ksJGVVaXhE3m5iHFuOui8qzBZaUdfyQL/ueSnc1cejpVjkU\n\
RW2hxUd5AgMBAAECggEASXhMG0deJ4g7jEiVjKlDxnoZLiKY/aQI1IjSlBPar+17\n\
37I4jqv4iTL9UkmuaqQV7oQXyaCgif0lmNp9e0z7QXtMNcq0sYAFGTaFnoIk+Taz\n\
DbWKM7naYY59YfbaPGnFCIVWoCozbw2odd664lJajINrbcMpoC59VUHNOGsTUUcl\n\
Zu6XJb41vJR984iBWVl3H4IBQ0fq8omwSdDfnVesTUFAI5+UWbVsFSkR2GxeafeW\n\
wCcxb/HZGmHdZA0r+wcE5DHlvKWEuNyTLeaL+5Ls4i4tF1u5M4VObR6p8pOXCuDr\n\
bhlrMC29ppcytPA6gp3rfYjQB3KGSCez85QOZEoiMQKBgQDfFkkAYRDJXOFmiKMI\n\
5Es9iEKNDRtnJGZHsY7EUaY0KjwARnf0B107GOcpDP/rAvDcE1AgrH8WWp2U/F7c\n\
Tn0ivFoC71mcgqDW8CpxWm6gFfJxIDcQHab4pCePZTSokCGvQHkQ8SCF9CiYPF8e\n\
N36OYIfCcqzw10S61wdoneWpywKBgQDOsfNbnEM4MqP3slJ4xfZEMJiCmn9A0XLN\n\
FKlg64WwZYuPf0tVGLUsFk7v2bxKjPdIB/CN0k/bmBgbTsCVNaxN2JQgCWVN1Lkn\n\
RK74TDk1U54AoEEg5wFDboKxwila4O+QzEv9Kvc4wKfP6m3UKwcsjCHOJ8IeKNdr\n\
g6/sV7h7SwKBgQDMcmLyuwIkWD+ltSEc/APMX30GQXXUn7IwkBPT7TQRaIdWhT5X\n\
YQxbJaZro6NCII7WUcjEOPG7bc4vBN52rmPWGqGvQOVyHvECKlXrTkJF6fcnH6v2\n\
1AX6po4ux0RsGrMUJlUNZr40pR8y7VkKmzcbinHAjVHa4p31US1T/ijE4wKBgBsK\n\
kOoFUY4Cogz/2WZPj95v9SPxDoDuZfSKpXY2mfDv+Ag4ASUK7HjuGjmq5MVkF93m\n\
nAQR5by280BvVyauNPKiSQ6/6+74y5hllfaKEln1rJAsZxjk3/dwd8zFuB9Zmj35\n\
XF+sxVc5bTr4UpIVZwTe+HU+IllLwpMgUV28RDhtAoGBAKGCggYfQ1Q54vmljH1c\n\
me9QQFofDzsoq8Gj1xuhDFFNmHqW1z0DQkkf1gdKgx9E/uE4ceXTLE9z313cof8v\n\
mzqBLgLPPqB2yu9VRXgr43i6mJ3jiW6NK+tPIj7U7GNdGRd8LAmjP0W/8S6CFoYk\n\
/Xx9/OtP1rd1WHkDZbdd3gi9\n\
-----END PRIVATE KEY-----\n";
pub(crate) const FIXTURE_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtB8BOMPCFjO+KBuZFVM+\n\
UlZ98+5yiaTRFJYTMzNHtP+J1jphiTe6VRLLTY92mVM+p8ziIz7hklh7ta/Y+7Hr\n\
EfOksEZG+7dvSopGJf9YBXmCeYNGOjAEjYLhx+I88CQsw3Xvu/nnzXJDOQe07hWm\n\
vF7E1AyimnOVi3bjQNrqyCQ35U9hDJZFUAfc8oHeYjqdu7N/jy0VYt+U+vCQod8a\n\
z+Fufip7bv/hSnVBLOUb61zd44ksDf1rpiA3Q617oYh5/18U5jycaTbxeWU/iNfM\n\
HVN0N9bZ8OpLCRlVWl4RN5uYhxbjrovKswWWlHX8kC/7nkp3NXHo6VY5FEVtocVH\n\
eQIDAQAB\n\
-----END PUBLIC KEY-----\n";

/// Second 2048-bit signer, generated once per test-binary run (rotation and
/// mismatch tests need two distinct keys; everything else uses the static
/// fixture above).
pub(crate) fn second_keypair() -> (String, String) {
    static KEYS: OnceLock<(String, String)> = OnceLock::new();
    KEYS.get_or_init(|| {
        let (private_key, public_key) =
            signatures::generate_rsa_keypair_2048().expect("generate second Batch E keypair");
        (
            signatures::private_key_to_pkcs8_pem(&private_key),
            signatures::public_key_to_spki_pem(&public_key),
        )
    })
    .clone()
}

/// Fixed reference time for the pure-crypto tests (deterministic: no wall
/// clock involved).
pub(crate) fn fixed_now_ms() -> i64 {
    signatures::parse_http_date("Wed, 14 Oct 2026 12:00:00 GMT").expect("fixture date parses")
}

/// `Date` header matching [`fixed_now_ms`].
pub(crate) fn fixed_date_header() -> String {
    delivery::format_http_date(fixed_now_ms())
}

/// `created` seconds matching [`fixed_now_ms`].
pub(crate) fn fixed_created_secs() -> i64 {
    fixed_now_ms().div_euclid(1000)
}

pub(crate) fn header_map(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        let key: HeaderName = name.parse().expect("test header name parses");
        let header_value: HeaderValue = value.parse().expect("test header value parses");
        map.insert(key, header_value);
    }
    map
}

/// Legacy-signed POST headers covering `(request-target) date host digest`.
pub(crate) fn legacy_post_headers(
    path: &str,
    host: &str,
    date: &str,
    body: &[u8],
    key_id: &str,
    private_pem: &str,
) -> HeaderMap {
    let signed =
        signatures::sign_request_headers("POST", path, host, date, Some(body), key_id, private_pem)
            .expect("legacy test signing works");
    let digest = signed
        .digest
        .as_deref()
        .expect("POST signing emits a digest");
    header_map(&[
        ("date", date),
        ("host", host),
        ("digest", digest),
        ("signature", signed.signature.as_str()),
    ])
}

/// RFC 9421-signed POST headers covering `@method @authority @path`
/// plus `content-digest`.
pub(crate) fn rfc9421_post_headers(
    path: &str,
    scheme: &str,
    host: &str,
    body: &[u8],
    key_id: &str,
    private_pem: &str,
    created_secs: i64,
) -> HeaderMap {
    let signed = signatures::sign_rfc9421_headers(
        "POST",
        path,
        scheme,
        host,
        Some(body),
        key_id,
        private_pem,
        created_secs,
    )
    .expect("RFC 9421 test signing works");
    let digest = signed
        .content_digest
        .as_deref()
        .expect("POST signing emits content-digest");
    header_map(&[
        ("host", host),
        ("content-digest", digest),
        ("signature-input", signed.signature_input.as_str()),
        ("signature", signed.signature.as_str()),
    ])
}

/// Dual-signed POST headers: the RFC 9421 dictionary travels in `signature`
/// while the legacy value travels in `authorization` (both slots are
/// consulted by each parser, so either order verifies).
#[allow(clippy::too_many_arguments)]
pub(crate) fn dual_post_headers(
    path: &str,
    scheme: &str,
    host: &str,
    date: &str,
    body: &[u8],
    key_id: &str,
    private_pem: &str,
    created_secs: i64,
) -> HeaderMap {
    let legacy =
        signatures::sign_request_headers("POST", path, host, date, Some(body), key_id, private_pem)
            .expect("legacy test signing works");
    let rfc9421 = signatures::sign_rfc9421_headers(
        "POST",
        path,
        scheme,
        host,
        Some(body),
        key_id,
        private_pem,
        created_secs,
    )
    .expect("RFC 9421 test signing works");
    let legacy_digest = legacy
        .digest
        .as_deref()
        .expect("POST signing emits a digest");
    let rfc9421_digest = rfc9421
        .content_digest
        .as_deref()
        .expect("POST signing emits content-digest");
    let authorization = format!("Signature {}", legacy.signature);
    header_map(&[
        ("date", date),
        ("host", host),
        ("digest", legacy_digest),
        ("content-digest", rfc9421_digest),
        ("signature-input", rfc9421.signature_input.as_str()),
        ("signature", rfc9421.signature.as_str()),
        ("authorization", authorization.as_str()),
    ])
}

/// Fixed POST body shared by the pure-crypto tests.
pub(crate) fn post_body() -> Vec<u8> {
    br#"{"type":"Follow","id":"https://remote.example/acts/1"}"#.to_vec()
}

// ---------------------------------------------------------------------------
// Process-wide mode handling (serialized: the mode cell is global)
// ---------------------------------------------------------------------------

/// Serializes every test that touches the process-wide signature-mode cell.
/// A Tokio mutex (not `std`): the guard is held across `.await` while the
/// mode is in force, which is exactly what a Tokio mutex is built for (and
/// what `clippy::await_holding_lock` accepts).
static MODE_MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

pub(crate) fn mode_mutex() -> &'static tokio::sync::Mutex<()> {
    MODE_MUTEX.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Run an async test body with the process-wide mode set, restoring
/// `permissive` afterwards (even on panic, via the drop guard; the Tokio
/// mutex itself never poisons).
pub(crate) async fn with_sig_mode<R>(mode: Rfc9421Mode, test: impl Future<Output = R>) -> R {
    let _held = mode_mutex().lock().await;
    struct RestoreMode;
    impl Drop for RestoreMode {
        fn drop(&mut self) {
            signatures::set_rfc9421_mode(Rfc9421Mode::Permissive);
        }
    }
    let _restore = RestoreMode;
    signatures::set_rfc9421_mode(mode);
    test.await
}

// ---------------------------------------------------------------------------
// Harness: local services with a pre-seeded remote key (no network)
// ---------------------------------------------------------------------------

/// Build `AppServices` on a fresh temp-dir store (deleted first, so reruns
/// are repeatable) with the harness `public_url`. No listener, no worker:
/// the tests call `inbox_handler` and the fetch gate directly.
pub(crate) fn boot_services(directory_tag: &str) -> Arc<AppServices> {
    let directory = std::env::temp_dir().join(format!("pinetree-sigmode-e5-{directory_tag}"));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("create Batch E temp dir");
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let store = RedbStore::open_with_default_ids(directory.join("store.redb"), clock.clone())
        .expect("open Batch E store");
    let blobs = Arc::new(FsBlobStore::new(directory.join("blobs")).expect("open blob store"));
    Arc::new(AppServices {
        clock: clock.clone(),
        ids: store.ids().clone(),
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
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            // Hermetic: nothing under test should fetch. Short timeouts fail
            // fast (instead of hanging) if that invariant ever breaks.
            allow_private_ips: false,
            timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(5),
            ..Default::default()
        }),
        public_url: REQUEST_PUBLIC_URL.to_string(),
        max_file_size: 10_000_000,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs.clone(),
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    })
}

/// Seed the remote-key cache so verification resolves locally (cache hit =
/// no fetch). The key id stays bound to [`REMOTE_ACTOR`]: the inbox compares
/// `activity.actor` against this cached owner, never against key-id text.
pub(crate) fn seed_fixture_key(services: &AppServices) {
    services
        .cache_remote_public_key(REMOTE_KEY_ID, REMOTE_ACTOR, None, FIXTURE_PUBLIC_PEM)
        .expect("cache fixture remote key");
}

/// Activity the inbox accepts and ignores without fetching (unknown types
/// authenticate, then dispatch returns 202 with no storage or network work
/// beyond signature verification).
pub(crate) fn ignored_activity(id: &str, actor: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"type": "Ignore", "id": id, "actor": actor}))
        .expect("activity serializes")
}

/// `Create` whose object lives on a foreign host (trips the
/// object-authority gate after successful verification).
pub(crate) fn foreign_object_create(id: &str, actor: &str) -> Vec<u8> {
    serde_json::to_vec(
        &json!({"type": "Create", "id": id, "actor": actor, "object": "https://other.example/notes/1"}),
    )
    .expect("activity serializes")
}

pub(crate) async fn post_shared_inbox(
    services: &Arc<AppServices>,
    headers: HeaderMap,
    body: Vec<u8>,
) -> StatusCode {
    inbox::inbox_handler(State(services.clone()), headers, Bytes::from(body))
        .await
        .status()
}
