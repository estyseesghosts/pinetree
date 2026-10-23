//! Wave 1 H1: dangling reconciler must not debit a shared key while a live
//! charged alias remains.
//!
//! Fails on old code: deleting one alias's media row and reconciling
//! debited the whole physical key (`total_bytes` dropped to 0) while the
//! sibling stayed charged.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    ActivityDedup, BlobStore, InsertMediaInput, MediaClass, MediaStore, NoopStreamingSink,
    RemoteCacheRecord,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::{MediaMeta, MediaProperties};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest;
use pinetree::media::reconcile::reconcile_media;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

fn open_backend() -> (
    Arc<RedbStore>,
    Arc<FsBlobStore>,
    Arc<FsBlobStore>,
    Arc<dyn Clock>,
) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-h1-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(BASE_MS));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    (store, cache, durable, clock)
}

fn services_for(
    store: &Arc<RedbStore>,
    cache: &Arc<FsBlobStore>,
    durable: &Arc<FsBlobStore>,
    clock: &Arc<dyn Clock>,
) -> Arc<AppServices> {
    let media: Arc<dyn MediaStore> = store.clone();
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
        media,
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

fn row_bytes(len: usize, seed: u8) -> Vec<u8> {
    let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    bytes.resize(len.max(8), seed);
    bytes.truncate(len);
    if bytes.len() > 8 {
        bytes[8] = seed;
    }
    bytes
}

#[test]
fn dangling_single_alias_removal_keeps_shared_charge() {
    let (store, cache, durable, clock) = open_backend();
    let services = services_for(&store, &cache, &durable, &clock);
    let shared = row_bytes(32, 21);
    let key = ingest::sha256_hex(&shared);
    cache.put(&key, &shared).unwrap();

    let mut metas: Vec<MediaMeta> = Vec::new();
    for (id, url) in [
        ("midh1shared000a", "https://cdn.example/h1-a.png"),
        ("midh1shared000b", "https://cdn.example/h1-b.png"),
    ] {
        let meta = store
            .insert_metadata(InsertMediaInput {
                id: Some(id.to_string()),
                owner_id: None,
                name: "remote.bin".to_string(),
                mime: "image/png".to_string(),
                size_bytes: shared.len() as u64,
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
        store
            .remote_cache_insert(&RemoteCacheRecord {
                source_url: url.to_string(),
                variant: "original".to_string(),
                media_id: meta.id.clone(),
                blob_key: key.clone(),
                size_bytes: shared.len() as u64,
                class: MediaClass::Preview,
                fetched_at_ms: BASE_MS,
                last_access_ms: BASE_MS,
                access_count: 1,
                bytes_absent: false,
                generation: 0,
            })
            .unwrap();
        metas.push(meta);
    }
    // Fetch-path shared-blob rule: charge once per physical key.
    store
        .cache_accounting_apply(32, MediaClass::Preview, 32, 0, 1, 32, 0, 0)
        .unwrap();
    assert_eq!(store.cache_stats().unwrap().total_bytes, 32);

    // Delete ONE alias's media row: the other alias still charges the key.
    store.delete_metadata(&metas[0].id).unwrap();
    let report = reconcile_media(&services).unwrap();
    assert_eq!(report.orphans_removed, 0);
    assert_eq!(
        store.cache_stats().unwrap().total_bytes,
        32,
        "shared key with a live charged alias must not debit"
    );
    let remaining = store
        .remote_cache_lookup("https://cdn.example/h1-b.png", "original")
        .unwrap()
        .expect("live sibling index row survives");
    assert!(!remaining.bytes_absent);
    assert_eq!(remaining.size_bytes, 32);
    assert_eq!(cache.byte_len(&key).unwrap(), Some(32));

    // Evicting the last alias debits exactly once.
    store.delete_metadata(&metas[1].id).unwrap();
    let report = reconcile_media(&services).unwrap();
    assert_eq!(report.orphans_removed, 1);
    assert_eq!(store.cache_stats().unwrap().total_bytes, 0);
    assert!(cache.byte_len(&key).unwrap().is_none());
}
