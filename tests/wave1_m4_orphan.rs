//! Wave 1 M4: pooled-but-never-indexed bytes charge exactly once.
//!
//! Fails on old code: a crash orphan (bytes pooled, index insert never
//! ran) fetched with `was_absent == false` and empty alias set charged
//! `false`, so the row carried `size = actual` with `total += 0`. The
//! later eviction then hid the drift.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, CacheEvictionOutcome, MediaClass, MediaStore, NoopStreamingSink,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest;
use pinetree::media::remote_cache::fetch_on_first_request;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn harness() -> (AppServices, Arc<FsBlobStore>, Arc<RedbStore>) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-m4-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    let media: Arc<dyn MediaStore> = store.clone();
    let services = AppServices {
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
        media,
        blobs: durable,
        cache_blobs: cache.clone(),
        notifications: store.clone(),
        auth: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
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
        public_url: "https://pinetree.local".to_string(),
        max_file_size: 4096,
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
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    };
    (services, cache, store)
}

fn serve(body: Vec<u8>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for _ in 0..4 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).unwrap_or(0);
            let _ = n;
            let hdr = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(hdr.as_bytes());
            let _ = stream.write_all(&body);
            let _ = stream.flush();
        }
    });
    addr
}

#[test]
fn crash_orphan_bytes_charge_once_and_evict_debits_same() {
    let (services, cache, store) = harness();
    // Crash orphan: bytes pooled, never indexed, never charged.
    let body = vec![
        0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 9, 9, 9, 9, 1, 2, 3, 4,
    ];
    let key = ingest::sha256_hex(&body);
    cache.put(&key, &body).unwrap();
    assert_eq!(store.cache_stats().unwrap().total_bytes, 0);
    assert!(store.cache_entries_for_blob_key(&key).unwrap().is_empty());

    let addr = serve(body.clone());
    let url = format!("http://{addr}/orphan.png");
    let meta = fetch_on_first_request(&services, &url, "original").unwrap();
    assert_eq!(meta.blob_key, key);
    assert_eq!(meta.size_bytes, body.len() as u64);
    // Exactly once: the orphan commit is the charge owner.
    assert_eq!(
        store.cache_stats().unwrap().total_bytes,
        body.len() as u64,
        "orphan bytes indexed by this commit must charge actual once"
    );

    // A later eviction debits that same amount (no saturating paper-over).
    let outcome = store
        .commit_cache_eviction(
            &key,
            body.len() as u64,
            MediaClass::Preview,
            &[(url.clone(), "original".to_string())],
        )
        .unwrap();
    assert!(matches!(
        outcome,
        CacheEvictionOutcome::Evicted { debited_bytes } if debited_bytes == body.len() as u64
    ));
    assert_eq!(store.cache_stats().unwrap().total_bytes, 0);
}
