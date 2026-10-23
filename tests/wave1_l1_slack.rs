//! Wave 1 L1: class slack counts in-flight reservations (fail closed).
//!
//! Fails on old code: `max - total` (ignoring `in_flight`) lets a fetch
//! borrow class room that in-flight reservations already consumed, so two
//! concurrent class reservers jointly exceed the cap plus slack.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, MediaStore, NoopStreamingSink, PortError,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::remote_cache::fetch_on_first_request;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;
use std::io::{Read, Write};
use std::net::TcpListener;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn harness(max_bytes: u64, preview_cap: u64, max_file_size: u64) -> AppServices {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-l1-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    let media: Arc<dyn MediaStore> = store.clone();
    AppServices {
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
        cache_blobs: cache,
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
        max_file_size,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: max_bytes,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: preview_cap,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    }
}

fn serve(body: Vec<u8>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for _ in 0..4 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf).unwrap_or(0);
            let hdr = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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
fn class_recheck_counts_in_flight_and_fails_closed() {
    // max 300, Preview cap 50, file cap 200. After the fetch reserves 200,
    // slack is 300 - 0 - 200 = 100, allowance 150 < actual 200, so the
    // real-class recheck must fail closed. Old code used slack 300 and
    // allowed it (allowance 350).
    let services = harness(300, 50, 200);
    let mut body = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    body.resize(200, 0xAA);
    let addr = serve(body.clone());
    let url = format!("http://{addr}/big-preview.png");
    let err = fetch_on_first_request(&services, &url, "original")
        .expect_err("class borrow past in-flight slack must fail closed");
    assert!(
        matches!(err, PortError::Validation(ref m) if m == "CACHE_CAPACITY_EXCEEDED"),
        "unexpected: {err:?}"
    );
    let stats = services.media.cache_stats().unwrap();
    assert_eq!(stats.total_bytes, 0);
    assert_eq!(stats.in_flight_bytes, 0);
    assert!(
        services
            .media
            .remote_cache_lookup(&url, "original")
            .unwrap()
            .is_none()
    );
}

#[test]
fn concurrent_class_reservers_cannot_jointly_exceed_cap_plus_slack() {
    // A prior in-flight reservation consumes the borrow room: with max 300,
    // cap 50, and 100 bytes already in flight, a 200-byte Preview fetch
    // (allowance now 50 + (300 - 0 - 300) = 50) must fail closed. Old code
    // ignored the 300 in flight (100 prior + 200 own) and allowed it.
    let services = harness(300, 50, 200);
    assert!(services.media.reserve_cache_bytes(100).unwrap());
    let mut body = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    body.resize(200, 0xBB);
    let addr = serve(body);
    let url = format!("http://{addr}/second-preview.png");
    let err = fetch_on_first_request(&services, &url, "original")
        .expect_err("second class reserver must not over-borrow");
    assert!(
        matches!(err, PortError::Validation(ref m) if m == "CACHE_CAPACITY_EXCEEDED"),
        "unexpected: {err:?}"
    );
    // Prior reservation untouched; fetch released its own.
    assert_eq!(services.media.cache_stats().unwrap().in_flight_bytes, 100);
    services.media.release_cache_reservation(100).unwrap();
    assert_eq!(services.media.cache_stats().unwrap().in_flight_bytes, 0);
}
