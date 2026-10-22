//! Shared test helpers for endpoint tests (split from `endpoints/mod.rs`).
//!
//! Pure move, no behavior change.

#![allow(unused_imports)]
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use crate::api::misskey::auth::{generate_random_token, hash_token, verify_password};
use crate::api::misskey::response::{RateLimiter, UploadSlots};
use crate::application::ports::{
    ActivityDedup, ApiTokenRecord, BlobStore, NoopStreamingSink, PortError, ReactionStore,
    StreamingSink,
};
use crate::domain::id::{AidxGenerator, Clock, ManualClock};
use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use crate::federation::retry::DestinationTracker;
use crate::persistence::redb::RedbStore;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE};

use super::{ApiState, api_dispatch, api_dispatch_blocking, build_router};
use crate::application::commands::AppServices;
use crate::jobs::worker::JobWorker;
use crate::media::transcode::TranscodeConfig;
use crate::services::email::{SmtpConfig, SmtpEmailSender};
use crate::streaming::hub::Hub;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;

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

pub(crate) fn test_state() -> (ApiState, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(1_700_000_000_000));
    let ids: Arc<dyn crate::application::ports::IdGenerator> =
        Arc::new(AidxGenerator::with_node_id(clock.clone(), "test", 0));
    let dir = std::env::temp_dir().join(format!("pinetree-api-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = RedbStore::open(dir.join("t.redb"), clock.clone(), ids.clone()).unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(MemBlobs::default());
    let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
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
        note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs.clone(),
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: crate::config::RemoteCacheMode::OnDemand,
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
    let rate_limiter = Arc::new(RateLimiter::with_misskey_defaults(clock.clone()));
    let upload_slots = Arc::new(UploadSlots::with_defaults());
    (
        ApiState {
            services,
            worker,
            rate_limiter,
            upload_slots,
            hub: Hub::new(1024),
            trusted_proxies: Arc::new(Vec::new()),
            featured_cache: Arc::new(crate::application::commands::pins::FeaturedCache::default()),
        },
        clock,
    )
}

pub(crate) async fn read_json(response: Response) -> (StatusCode, Value, HeaderMap) {
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value, headers)
}

/// Direct-call peer used by every handler test unless a test says otherwise.
pub(crate) fn test_peer() -> ConnectInfo<SocketAddr> {
    ConnectInfo("127.0.0.1:9".parse().unwrap())
}

pub(crate) fn peer_at(ip: &str) -> ConnectInfo<SocketAddr> {
    ConnectInfo(format!("{ip}:9").parse().unwrap())
}

pub(crate) async fn call(
    state: &ApiState,
    endpoint: &str,
    body: Value,
    token: Option<&str>,
) -> (StatusCode, Value) {
    call_from(state, endpoint, body, token, test_peer()).await
}

pub(crate) async fn call_from(
    state: &ApiState,
    endpoint: &str,
    body: Value,
    token: Option<&str>,
    peer: ConnectInfo<SocketAddr>,
) -> (StatusCode, Value) {
    let mut headers = HeaderMap::new();
    if let Some(token) = token {
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {token}").parse::<HeaderValue>().unwrap(),
        );
    }
    let bytes = Bytes::from(serde_json::to_vec(&body).unwrap());
    let response = match api_dispatch(
        State(state.clone()),
        peer,
        Path(endpoint.to_string()),
        headers,
        bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(err) => err.into_response(),
    };
    let (status, value, _) = read_json(response).await;
    (status, value)
}

pub(crate) async fn signup_user(state: &ApiState, username: &str) -> (String, String) {
    let (status, value) = call(
        state,
        "signup",
        serde_json::json!({ "username": username, "password": "s3cret-pw" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "signup {username}: {value}");
    let token = value["token"].as_str().unwrap().to_string();
    let id = value["id"].as_str().unwrap().to_string();
    (id, token)
}

/// Pre-slice-9 `salt:sha256` hash, built the way the legacy hasher did.
pub(crate) fn legacy_password_hash(salt_hex: &str, password: &str) -> String {
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    sha2::Digest::update(&mut hasher, salt_hex.as_bytes());
    sha2::Digest::update(&mut hasher, b":");
    sha2::Digest::update(&mut hasher, password.as_bytes());
    format!("{salt_hex}:{}", hex::encode(sha2::Digest::finalize(hasher)))
}

/// Streaming chunk source that counts every byte the server pulls.
/// Slice 6 (H1): proves an oversized upload aborts early instead of
/// buffering the whole body.
pub(crate) struct CountingChunks {
    pub(crate) remaining: usize,
    pub(crate) chunk: usize,
    pub(crate) yielded: Arc<AtomicUsize>,
}

impl futures_core::Stream for CountingChunks {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return std::task::Poll::Ready(None);
        }
        let n = self.chunk.min(self.remaining);
        self.remaining -= n;
        self.yielded
            .fetch_add(n, std::sync::atomic::Ordering::SeqCst);
        std::task::Poll::Ready(Some(Ok(Bytes::from(vec![0x78; n]))))
    }
}

pub(crate) async fn create_note(state: &ApiState, body: Value, token: &str) -> String {
    let (status, value) = call(state, "notes/create", body, Some(token)).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    value["createdNote"]["id"].as_str().unwrap().to_string()
}

pub(crate) fn note_ids(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap().to_string())
        .collect()
}

pub(crate) async fn set_ff_visibility(
    state: &ApiState,
    token: &str,
    followers: &str,
    following: &str,
) {
    let (status, value) = call(
        state,
        "i/update",
        serde_json::json!({
            "followersVisibility": followers,
            "followingVisibility": following,
        }),
        Some(token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "i/update visibility: {value}");
}

pub(crate) async fn ff_lists(
    state: &ApiState,
    target: &str,
    token: Option<&str>,
) -> ((StatusCode, Value), (StatusCode, Value)) {
    let followers = call(
        state,
        "users/followers",
        serde_json::json!({ "userId": target }),
        token,
    )
    .await;
    let following = call(
        state,
        "users/following",
        serde_json::json!({ "userId": target }),
        token,
    )
    .await;
    (followers, following)
}

pub(crate) fn assert_forbidden(status: StatusCode, value: &Value) {
    // Upstream `FORBIDDEN` is `kind: client` → HTTP 400, not 403.
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "FORBIDDEN");
    assert_eq!(value["error"]["code"], "FORBIDDEN");
    assert_eq!(value["error"]["kind"], "client");
    assert_eq!(value["message"], "Forbidden.");
}

/// Stub inbox on 127.0.0.1 that records each POST body verbatim (WP3
/// fan-out assertions). Answers `202 Accepted`, serves `max_requests`.
pub(crate) fn capture_inbox(max_requests: usize) -> (String, Arc<Mutex<Vec<String>>>) {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind capture inbox");
    let addr = listener.local_addr().expect("capture inbox addr");
    let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = bodies.clone();
    std::thread::spawn(move || {
        for _ in 0..max_requests {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf: Vec<u8> = Vec::new();
            let mut chunk = [0u8; 4096];
            let mut body_start: Option<usize> = None;
            let mut content_len = 0usize;
            loop {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&chunk[..n]);
                        if body_start.is_none() {
                            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                let head =
                                    String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
                                content_len = head
                                    .lines()
                                    .filter_map(|l| l.split_once(':'))
                                    .find(|(k, _)| k.trim() == "content-length")
                                    .and_then(|(_, v)| v.trim().parse::<usize>().ok())
                                    .unwrap_or(0);
                                body_start = Some(pos + 4);
                            }
                        }
                        if let Some(start) = body_start {
                            if buf.len() >= start + content_len {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            if let Some(start) = body_start {
                let end = (start + content_len).min(buf.len());
                sink.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf[start..end]).to_string());
            }
            let _ = stream.write_all(
                b"HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            );
            let _ = stream.flush();
        }
    });
    (format!("http://{addr}/inbox"), bodies)
}

/// Read one export result file's bytes through the notification's
/// `fileId` (media metadata + blob store, like the import worker does).
pub(crate) async fn read_export_bytes(state: &ApiState, file_id: &str) -> Vec<u8> {
    let meta = state
        .services
        .media
        .get(file_id)
        .unwrap()
        .expect("export file metadata");
    state
        .services
        .blobs
        .get(&meta.blob_key)
        .unwrap()
        .expect("export file bytes")
}

/// The newest `exportCompleted` notifications for `token`
/// (`markAsRead: false` so repeated reads in one test stay stable).
pub(crate) async fn export_completed_notifications(state: &ApiState, token: &str) -> Value {
    let (status, notes) = call(
        state,
        "i/notifications",
        serde_json::json!({ "markAsRead": false, "includeTypes": ["exportCompleted"] }),
        Some(token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notes}");
    notes
}

/// Create a remote account in the store (§13 import/export fixture).
pub(crate) fn remote_account(services: &AppServices, username: &str, host: &str) -> String {
    use crate::application::ports::CreateAccountInput;
    services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: username.to_string(),
                host: Some(host.to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://{host}/users/{username}")),
                inbox: Some(format!("https://{host}/inbox")),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .unwrap()
        .id
}

/// Batch Q Phase 1: mint a third-party token scoped to `permissions`.
pub(crate) async fn scoped_token(
    state: &ApiState,
    account_id: &str,
    permissions: Vec<&str>,
) -> String {
    let raw = generate_random_token();
    state
        .services
        .auth
        .store_token(ApiTokenRecord {
            id: state.services.ids.generate(),
            token_hash: hash_token(&raw),
            account_id: account_id.to_string(),
            permissions: permissions.into_iter().map(str::to_string).collect(),
            is_first_party: false,
            name: None,
            description: None,
            icon_url: None,
            created_at_ms: state.services.clock.now_ms(),
        })
        .unwrap();
    raw
}
