//! Wave 1 M3: old-key abandon holds the old stripe across check-and-delete.
//!
//! Fails on old code: `fetch.rs` held the lifecycle + new-key lock only,
//! so a concurrent refetch reinstalling `K_old` landed between the
//! `refcount == 0 && entries.is_empty()` check and the byte delete. The
//! delete then removed live bytes while the fresh index row survived.
//! Fixed code acquires `(new, old)` in stripe order before the commit, so
//! the reinstall blocks until the abandon finishes and both bytes and the
//! index survive.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{ActivityDedup, BlobStore, MediaStore, NoopStreamingSink};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest;
use pinetree::media::remote_cache::fetch_on_first_request;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// `BlobStore` wrapper that sleeps before every cache-pool delete, opening
/// the abandon window deterministically: the abandoning fetch checks
/// (empty), sleeps inside the delete, and only then removes bytes. A
/// concurrent reinstall landing in the window loses bytes on old code
/// (no old-key lock) and blocks-then-survives on fixed code.
struct SleepDeleteBlobs {
    inner: Arc<FsBlobStore>,
    sleep_ms: u64,
}

impl BlobStore for SleepDeleteBlobs {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        self.inner.put(hash, bytes)
    }
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        self.inner.get(hash)
    }
    fn delete(&self, hash: &str) -> Result<(), PortError> {
        std::thread::sleep(Duration::from_millis(self.sleep_ms));
        self.inner.delete(hash)
    }
    fn byte_len(&self, hash: &str) -> Result<Option<u64>, PortError> {
        self.inner.byte_len(hash)
    }
    fn import_file(&self, hash: &str, src: &std::path::Path) -> Result<(), PortError> {
        self.inner.import_file(hash, src)
    }
}

use pinetree::domain::id::PortError;

fn png_body(len: usize, seed: u8) -> Vec<u8> {
    let mut body = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    body.resize(len.max(8), seed);
    body.truncate(len);
    if body.len() > 8 {
        body[8] = seed;
    }
    body
}

#[test]
fn abandon_while_concurrent_refetch_reinstalls_keeps_bytes_and_index() {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-m3-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    let real_cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    let sleeping: Arc<dyn BlobStore> = Arc::new(SleepDeleteBlobs {
        inner: real_cache.clone(),
        sleep_ms: 500,
    });
    let media: Arc<dyn MediaStore> = store.clone();
    let services = Arc::new(AppServices {
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
        cache_blobs: sleeping,
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
    });

    let old_body = png_body(16, 0xA1);
    let new_body = png_body(12, 0xB2);
    let old_key = ingest::sha256_hex(&old_body);
    let new_key = ingest::sha256_hex(&new_body);
    assert_ne!(old_key, new_key);
    let version = Arc::new(AtomicU64::new(0));
    let old_clone = old_body.clone();
    let new_clone = new_body.clone();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_version = version.clone();
    std::thread::spawn(move || {
        for _ in 0..16 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).into_owned();
            let path = req
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_string();
            // No per-path server delay: ordering is controlled client-side
            // (thread B starts its fetch 100 ms after A) so the
            // single-threaded accept loop cannot reorder the downloads.
            let body: &[u8] = if path == "/u1.png" {
                let v = server_version.load(Ordering::SeqCst);
                if v == 0 { &old_clone } else { &new_clone }
            } else if path == "/u2.png" {
                &old_clone
            } else {
                let _ = stream.write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
                continue;
            };
            let hdr = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(hdr.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
        }
    });

    // Phase 1: U1 serves K_old, charged once. Then drop the bytes so the
    // phase-2 fetch misses the pool and refetches (content changed).
    let u1 = format!("http://{addr}/u1.png");
    let u2 = format!("http://{addr}/u2.png");
    fetch_on_first_request(&services, &u1, "original").unwrap();
    assert_eq!(store.cache_stats().unwrap().total_bytes, 16);
    real_cache.delete(&old_key).unwrap();
    assert_eq!(
        real_cache.byte_len(&old_key).unwrap(),
        None,
        "phase-2 precondition: old bytes dropped so U1 refetches"
    );

    // Phase 2: U1 changes content (abandons K_old), U2 reinstalls K_old.
    // B starts 100 ms after A so A checks empty and sleeps inside the
    // delete window first; B's reinstall then lands mid-window (old code
    // deletes it) or blocks on the old stripe until A finishes (fixed).
    version.store(1, Ordering::SeqCst);
    std::thread::scope(|scope| {
        let a = scope.spawn(|| fetch_on_first_request(&services, &u1, "original"));
        let b = scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(100));
            fetch_on_first_request(&services, &u2, "original")
        });
        let meta_a = a.join().unwrap().unwrap();
        let meta_b = b.join().unwrap().unwrap();
        assert_eq!(meta_a.blob_key, new_key);
        assert_eq!(meta_b.blob_key, old_key);
    });

    // Both bytes and both index rows survive; accounting charges both keys.
    assert_eq!(
        real_cache.byte_len(&new_key).unwrap(),
        Some(new_body.len() as u64)
    );
    assert_eq!(
        real_cache.byte_len(&old_key).unwrap(),
        Some(old_body.len() as u64),
        "abandon must not delete concurrently reinstalled bytes"
    );
    let rec_u1 = store
        .remote_cache_lookup(&u1, "original")
        .unwrap()
        .expect("u1 index survives");
    let rec_u2 = store
        .remote_cache_lookup(&u2, "original")
        .unwrap()
        .expect("reinstalled u2 index survives");
    assert_eq!(rec_u1.blob_key, new_key);
    assert_eq!(rec_u2.blob_key, old_key);
    assert!(!rec_u2.bytes_absent);
    assert_eq!(
        store.cache_stats().unwrap().total_bytes,
        new_body.len() as u64 + old_body.len() as u64
    );
}
