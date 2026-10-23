//! Wave 5 (Agent negative-cache): failed remote actor/key fetches must not
//! amplify one inbound POST into unbounded outbound fetches.
//!
//! - `DEFAULT_MAX_DNS_IN_FLIGHT` stays 32 (the DNS pool bound is unchanged).
//! - N sequential resolves of one dead key id produce exactly one outbound
//!   fetch; the rest fail fast from the bounded TTL negative cache.
//! - N concurrent resolves of one dead key id share a single in-flight
//!   fetch (singleflight); followers wait instead of duplicating the GET.
//! - After the backoff TTL expires, the next resolve refetches exactly
//!   once, then backs off again.
//!
//! Fails on old code: every miss performs a signed fetch, so N sequential
//! identical failures cost N outbound fetches and N concurrent refreshes
//! cost N fetches (each holding a `spawn_blocking` thread plus a DNS slot).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{ActivityDedup, NoopStreamingSink};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::federation::delivery::{
    DEFAULT_MAX_DNS_IN_FLIGHT, SafeHttpClient, SafeHttpConfig, resolve_remote_public_key,
};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
/// Backoff for a single consecutive failure is 60s (see `stamp_retry_due`).
const FIRST_BACKOFF_MS: i64 = 61_000;
/// Per-connection stall so concurrent refreshes overlap in flight.
const STUB_DELAY: Duration = Duration::from_millis(300);

fn open_services(mclock: Arc<ManualClock>) -> Arc<AppServices> {
    let dir = std::env::temp_dir().join(format!("pinetree-wave5-negcache-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = mclock;
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
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
        blobs: durable.clone(),
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
        max_file_size: 10_000_000,
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
    })
}

/// Stub origin that counts every inbound connection and always answers 500
/// (so every actor fetch through it fails). Each connection stalls briefly
/// first, so concurrent refreshes overlap in flight unless singleflight
/// shares them.
fn failing_stub() -> (Arc<AtomicUsize>, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    std::thread::spawn(move || {
        for stream in listener.incoming().take(64) {
            let Ok(mut stream) = stream else {
                break;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(STUB_DELAY);
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            let body = b"nope";
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            );
            let _ = stream.write_all(body);
        }
    });
    (hits, base)
}

#[test]
fn wave5_dns_pool_bound_stays_32() {
    assert_eq!(
        DEFAULT_MAX_DNS_IN_FLIGHT, 32,
        "the DNS pool bound must stay 32 while the negative cache lands"
    );
}

#[test]
fn wave5_failed_key_ids_fetch_once_then_back_off() {
    let (hits, base) = failing_stub();
    let mclock = Arc::new(ManualClock::new(BASE_MS));
    let services = open_services(mclock.clone());

    let key_a = format!("{base}/users/negcache-a#main-key");
    let key_b = format!("{base}/users/negcache-b#main-key");

    // N sequential resolves of one dead key id: exactly one outbound fetch,
    // every attempt still an error.
    for _ in 0..10 {
        assert!(
            resolve_remote_public_key(&services, &services.federation_http, &key_a, false).is_err(),
            "dead key must stay an error"
        );
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "10 sequential failures must cost one outbound fetch"
    );

    // N concurrent resolves of a fresh dead key id: singleflight shares one
    // in-flight fetch instead of paying for N.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let services = Arc::clone(&services);
        let key_b = key_b.clone();
        handles.push(std::thread::spawn(move || {
            resolve_remote_public_key(&services, &services.federation_http, &key_b, false).is_err()
        }));
    }
    for handle in handles {
        assert!(
            handle.join().expect("worker finishes"),
            "dead key must stay an error"
        );
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "8 concurrent failures must share one outbound fetch"
    );

    // Cache hits: further attempts fetch nothing.
    for _ in 0..5 {
        assert!(
            resolve_remote_public_key(&services, &services.federation_http, &key_b, false).is_err()
        );
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "negative-cache hits must not refetch"
    );

    // TTL expiry refetches exactly once, then backs off again.
    mclock.advance_ms(FIRST_BACKOFF_MS);
    assert!(
        resolve_remote_public_key(&services, &services.federation_http, &key_b, false).is_err()
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        3,
        "post-TTL resolve must refetch once"
    );
    for _ in 0..3 {
        assert!(
            resolve_remote_public_key(&services, &services.federation_http, &key_b, false).is_err()
        );
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        3,
        "second failure must back off again without refetching"
    );
}
