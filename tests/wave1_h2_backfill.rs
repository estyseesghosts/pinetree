//! Wave 1 H2: open-time storage backfill rebuilds per physical key.
//!
//! Fails on old code: a two-alias shared blob backfills to `2 * size`,
//! evicted rows count as live, and `CACHE_BLOB_CLASS` stays empty (later
//! evictions guess the class).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::ports::{InsertMediaInput, MediaClass, MediaStore, RemoteCacheRecord};
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::MediaProperties;
use pinetree::media::ingest;
use pinetree::persistence::redb::RedbStore;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn open_store() -> (Arc<RedbStore>, std::path::PathBuf) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-h2-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    (store, dir)
}

#[test]
fn backfill_charges_shared_blob_once_and_records_class() {
    let (store, _dir) = open_store();
    let shared = vec![
        0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 7, 7, 7, 7, 7, 7, 7, 7,
    ];
    let size = shared.len() as u64;
    assert_eq!(size, 16);
    let key = ingest::sha256_hex(&shared);

    for (id, url) in [
        ("midh2shared000a", "https://cdn.example/h2-a.png"),
        ("midh2shared000b", "https://cdn.example/h2-b.png"),
    ] {
        store
            .insert_metadata(InsertMediaInput {
                id: Some(id.to_string()),
                owner_id: None,
                name: "remote.bin".to_string(),
                mime: "image/png".to_string(),
                size_bytes: size,
                hash: ingest::md5_hex(&shared),
                blob_key: key.clone(),
                blurhash: None,
                properties: MediaProperties::default(),
                is_sensitive: false,
                is_link: true,
                url: format!("https://pinetree.local/files/{id}"),
                thumbnail_url: None,
                comment: None,
                source_url: Some(url.to_string()),
                max_file_size: 10_000_000,
            })
            .unwrap();
    }
    // One evicted row (different key): backfill must skip it.
    let evicted_bytes = vec![b'e'; 8];
    let evicted_key = ingest::sha256_hex(&evicted_bytes);
    let evicted_meta = store
        .insert_metadata(InsertMediaInput {
            id: Some("midh2evicted001".to_string()),
            owner_id: None,
            name: "remote.bin".to_string(),
            mime: "image/png".to_string(),
            size_bytes: evicted_bytes.len() as u64,
            hash: ingest::md5_hex(&evicted_bytes),
            blob_key: evicted_key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: "https://pinetree.local/files/midh2evicted001".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: Some("https://cdn.example/h2-evicted.png".to_string()),
            max_file_size: 10_000_000,
        })
        .unwrap();
    store
        .remote_cache_insert(&RemoteCacheRecord {
            source_url: "https://cdn.example/h2-evicted.png".to_string(),
            variant: "original".to_string(),
            media_id: evicted_meta.id.clone(),
            blob_key: evicted_key.clone(),
            size_bytes: evicted_bytes.len() as u64,
            class: MediaClass::Preview,
            fetched_at_ms: 1,
            last_access_ms: 1,
            access_count: 1,
            bytes_absent: false,
            generation: 0,
        })
        .unwrap();
    store
        .cache_accounting_apply(8, MediaClass::Preview, 8, 0, 1, 8, 0, 0)
        .unwrap();
    store
        .commit_cache_eviction(
            &evicted_key,
            8,
            MediaClass::Preview,
            &[(
                "https://cdn.example/h2-evicted.png".to_string(),
                "original".to_string(),
            )],
        )
        .unwrap();

    // Simulate a pre-backfill database: derived state cleared, media kept.
    store.clear_storage_accounting_for_test().unwrap();
    store.run_storage_accounting_backfill().unwrap();

    let stats = store.cache_stats().unwrap();
    assert_eq!(
        stats.total_bytes, size,
        "shared blob must backfill to one charge, not 2x"
    );
    assert_eq!(stats.preview_bytes, size);
    // Evicted row contributes nothing.
    assert!(stats.total_bytes < size + 8);

    // Both aliases re-indexed live.
    for url in [
        "https://cdn.example/h2-a.png",
        "https://cdn.example/h2-b.png",
    ] {
        let rec = store
            .remote_cache_lookup(url, "original")
            .unwrap()
            .expect("alias re-indexed");
        assert_eq!(rec.blob_key, key);
        assert_eq!(rec.size_bytes, size);
        assert!(!rec.bytes_absent);
    }
    // Evicted alias stays tombstoned.
    let tomb = store
        .remote_cache_lookup("https://cdn.example/h2-evicted.png", "original")
        .unwrap()
        .expect("evicted alias re-indexed as tombstone");
    assert!(tomb.bytes_absent);
    assert_eq!(tomb.size_bytes, 0);

    // Authoritative class recorded: evict with a wrong caller class still
    // debits Preview.
    let before = store.cache_stats().unwrap();
    let outcome = store
        .commit_cache_eviction(
            &key,
            size,
            MediaClass::Full,
            &[
                (
                    "https://cdn.example/h2-a.png".to_string(),
                    "original".to_string(),
                ),
                (
                    "https://cdn.example/h2-b.png".to_string(),
                    "original".to_string(),
                ),
            ],
        )
        .unwrap();
    assert!(matches!(
        outcome,
        pinetree::application::ports::CacheEvictionOutcome::Evicted { .. }
    ));
    let after = store.cache_stats().unwrap();
    assert_eq!(after.preview_bytes, before.preview_bytes - size);
    assert_eq!(after.full_bytes, before.full_bytes);
    assert_eq!(after.total_bytes, before.total_bytes - size);
}
