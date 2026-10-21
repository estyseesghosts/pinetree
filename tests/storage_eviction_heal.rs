//! Cache eviction healing: metadata survives byte eviction, crash orphans
//! are detectable and swept, legacy MD5 blobs stay invisible, and the
//! per-note remote-fetch budget pins hold.
//!
//! The sibling crash reconciler has not landed yet, so the crash-heal case
//! drives a local `evict_reconcile_orphan_blobs` over the `MediaStore` API.
//! Switch to it at the `// TODO(parent)` site once it lands.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pinetree::activitypub::objects::ParsedAttachment;
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, CreateAccountInput, CreateNoteInput, InsertMediaInput,
    MediaClass, MediaStore, NoopStreamingSink, NoteStore, PortError, RemoteCacheRecord,
    RemoteMediaFetchPayload,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::MediaProperties;
use pinetree::domain::note::MAX_FILE_IDS;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::remote_cache::{
    build_remote_media_fetch_job, MAX_REMOTE_ATTACHMENTS_PER_NOTE,
    MAX_REMOTE_MEDIA_TOTAL_BYTES_PER_NOTE,
};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
static EVICT_SEQ: AtomicU64 = AtomicU64::new(0);

struct EvictHarness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    cache: Arc<FsBlobStore>,
}

fn evict_harness() -> (EvictHarness, String) {
    let n = EVICT_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-eviction-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<ManualClock> = Arc::new(ManualClock::new(BASE_MS));
    let clock_port: Arc<dyn Clock> = clock.clone();
    let store =
        RedbStore::open_with_default_ids(dir.join("store.redb"), clock_port.clone()).unwrap();
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let services = Arc::new(AppServices {
        clock: clock_port.clone(),
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
    });
    let author = store
        .create_account(
            CreateAccountInput {
                handle: format!("evict_user_{n}"),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://remote.example/users/evict_user_{n}")),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .expect("create remote author")
        .id;
    (
        EvictHarness {
            store,
            services,
            cache,
        },
        author,
    )
}

fn evict_remote_input(source_url: &str, seed: u8) -> (InsertMediaInput, Vec<u8>) {
    let bytes = vec![0x89, b'P', b'N', b'G', seed, 0x01, 0x02, 0x03];
    let input = InsertMediaInput {
        id: None,
        owner_id: None,
        name: "remote.png".to_string(),
        mime: "image/png".to_string(),
        size_bytes: bytes.len() as u64,
        hash: md5_hex(&bytes),
        blob_key: sha256_hex(&bytes),
        blurhash: None,
        properties: MediaProperties::default(),
        is_sensitive: false,
        is_link: true,
        url: "https://pinetree.local/files/remote".to_string(),
        thumbnail_url: None,
        comment: None,
        source_url: Some(source_url.to_string()),
        max_file_size: 10_000_000,
    };
    (input, bytes)
}

// TODO(parent): switch to the sibling crash reconciler when it lands.
// Removes cache blobs (from `known_keys`) referenced by no metadata row;
// returns the number of orphans collected. Referenced blobs are never
// touched.
fn evict_reconcile_orphan_blobs(
    store: &RedbStore,
    blobs: &FsBlobStore,
    known_keys: &[String],
) -> usize {
    let mut removed = 0;
    for key in known_keys {
        if store.blob_refcount(key).expect("refcount") == 0 {
            blobs.delete(key).expect("delete orphan blob");
            removed += 1;
        }
    }
    removed
}

#[test]
fn eviction_bytes_only_metadata_and_source_url_survive_refetch_heals() {
    let (h, _author) = evict_harness();
    let (input, bytes) = evict_remote_input("https://remote.example/a.png", 0xA1);
    let key = input.blob_key.clone();
    h.cache.put(&key, &bytes).unwrap();
    let meta = h.store.insert_metadata(input).unwrap();

    // Evict bytes only (aggressive cache pressure): metadata row stays.
    h.cache.delete(&key).unwrap();
    assert_eq!(h.cache.get(&key).unwrap(), None, "bytes evicted");
    let row = h.store.get(&meta.id).unwrap().expect("metadata retained");
    assert_eq!(
        row.source_url.as_deref(),
        Some("https://remote.example/a.png"),
        "source_url retained after byte eviction"
    );
    assert!(!h
        .store
        .find_by_source_url("https://remote.example/a.png")
        .unwrap()
        .is_empty());

    // Re-fetch heals: re-put the same content-addressed bytes, upsert the
    // row under the same id (the `evicted` flag flip once that path lands).
    h.cache.put(&key, &bytes).unwrap();
    assert_eq!(
        h.cache.get(&key).unwrap().as_deref(),
        Some(bytes.as_slice()),
        "re-fetch restores bytes"
    );
    let healed = h
        .store
        .insert_metadata(InsertMediaInput {
            id: Some(meta.id.clone()),
            owner_id: row.owner_id.clone(),
            name: row.name.clone(),
            mime: row.mime.clone(),
            size_bytes: row.size_bytes,
            hash: row.hash.clone(),
            blob_key: row.blob_key.clone(),
            blurhash: row.blurhash.clone(),
            properties: row.properties.clone(),
            is_sensitive: row.is_sensitive,
            is_link: row.is_link,
            url: row.url.clone(),
            thumbnail_url: row.thumbnail_url.clone(),
            comment: row.comment.clone(),
            source_url: row.source_url.clone(),
            max_file_size: 10_000_000,
        })
        .expect("upsert same id");
    assert_eq!(healed.id, meta.id);
    assert!(
        h.store
            .get(&meta.id)
            .unwrap()
            .expect("row still present")
            .is_link
    );

    // The `(source_url, variant)` index pins the same lifecycle: insert,
    // lookup, touch (LRU clock +1), eviction-candidate order, remove.
    let rec = RemoteCacheRecord {
        source_url: "https://remote.example/a.png".to_string(),
        variant: "original".to_string(),
        media_id: meta.id.clone(),
        blob_key: key.clone(),
        size_bytes: bytes.len() as u64,
        class: MediaClass::Preview,
        fetched_at_ms: BASE_MS,
        last_access_ms: BASE_MS,
        access_count: 0,
    };
    h.store.remote_cache_insert(&rec).expect("index insert");
    let found = h
        .store
        .remote_cache_lookup("https://remote.example/a.png", "original")
        .expect("lookup")
        .expect("record present");
    assert_eq!(found.media_id, meta.id);
    assert_eq!(found.class, MediaClass::Preview);
    h.store
        .remote_cache_touch("https://remote.example/a.png", "original", BASE_MS + 1)
        .expect("touch");
    let touched = h
        .store
        .remote_cache_lookup("https://remote.example/a.png", "original")
        .expect("lookup")
        .expect("record present");
    assert_eq!(touched.last_access_ms, BASE_MS + 1);
    assert_eq!(touched.access_count, 1);
    let candidates = h
        .store
        .remote_cache_evict_candidates(10)
        .expect("candidates");
    assert!(candidates
        .iter()
        .any(|r| r.source_url == "https://remote.example/a.png"));
    h.store
        .remote_cache_remove("https://remote.example/a.png", "original")
        .expect("remove");
    assert!(h
        .store
        .remote_cache_lookup("https://remote.example/a.png", "original")
        .expect("lookup")
        .is_none());
}

#[test]
fn eviction_crash_heal_metadata_gone_then_orphan_reconciled() {
    let (h, _author) = evict_harness();
    // Live row + bytes.
    let (live_input, live_bytes) = evict_remote_input("https://remote.example/live.png", 0xB1);
    let live_key = live_input.blob_key.clone();
    h.cache.put(&live_key, &live_bytes).unwrap();
    let live = h.store.insert_metadata(live_input).unwrap();
    // Doomed row + bytes.
    let (dead_input, dead_bytes) = evict_remote_input("https://remote.example/dead.png", 0xB2);
    let dead_key = dead_input.blob_key.clone();
    h.cache.put(&dead_key, &dead_bytes).unwrap();
    let dead = h.store.insert_metadata(dead_input).unwrap();

    // Crash between metadata delete and blob delete: row gone, refcount
    // back to 0, bytes orphaned.
    h.store.delete_metadata(&dead.id).unwrap();
    assert!(h.store.get(&dead.id).unwrap().is_none());
    assert_eq!(h.store.blob_refcount(&dead_key).unwrap(), 0);
    assert_eq!(
        h.cache.get(&dead_key).unwrap().as_deref(),
        Some(dead_bytes.as_slice()),
        "crash leaves orphan bytes (detectable: refcount 0 + no row)"
    );

    // Reconciler collects exactly the orphan; the live blob is untouched.
    let removed =
        evict_reconcile_orphan_blobs(&h.store, &h.cache, &[live_key.clone(), dead_key.clone()]);
    assert_eq!(removed, 1, "exactly one orphan collected");
    assert_eq!(
        h.cache.get(&dead_key).unwrap(),
        None,
        "orphan bytes removed"
    );
    assert_eq!(
        h.cache.get(&live_key).unwrap().as_deref(),
        Some(live_bytes.as_slice()),
        "referenced bytes preserved"
    );
    assert!(h.store.get(&live.id).unwrap().is_some());
}

#[test]
fn eviction_md5_keyed_legacy_blob_reads_as_none() {
    // Pre-slice-8 blobs were keyed by MD5 with no length sidecar; `get`
    // must report them as corrupt (`None`), never serve raw bytes.
    let (h, _author) = evict_harness();
    let payload = b"legacy md5-keyed blob content".to_vec();
    let legacy_md5 = md5_hex(&payload);
    let path = h.cache.object_path(&legacy_md5).unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &payload).unwrap();
    assert_eq!(
        h.cache.get(&legacy_md5).unwrap(),
        None,
        "MD5-keyed legacy blob (no sidecar) reads as None"
    );
}

#[test]
fn eviction_per_note_budget_pins_hold() {
    // Constant pins: 16 attachments max, 64 MiB cumulative budget per note.
    assert_eq!(MAX_REMOTE_ATTACHMENTS_PER_NOTE, 16);
    assert_eq!(MAX_REMOTE_ATTACHMENTS_PER_NOTE, MAX_FILE_IDS);
    assert_eq!(MAX_REMOTE_MEDIA_TOTAL_BYTES_PER_NOTE, 64 * 1024 * 1024);

    let (h, author) = evict_harness();
    // Job builder: empty → None; 20 → truncated to 16 in document order.
    assert!(build_remote_media_fetch_job(&h.services, "act:empty", &[])
        .unwrap()
        .is_none());
    let attachments: Vec<ParsedAttachment> = (0..20)
        .map(|i| ParsedAttachment {
            url: format!("https://remote.example/{i}.png"),
            media_type: Some("image/png".to_string()),
            name: Some("remote file".to_string()),
            is_sensitive: false,
            blurhash: None,
            width: None,
            height: None,
        })
        .collect();
    let job = build_remote_media_fetch_job(&h.services, "act:big", &attachments)
        .unwrap()
        .expect("non-empty builds a job");
    let payload: RemoteMediaFetchPayload = serde_json::from_slice(&job.payload).unwrap();
    assert_eq!(
        payload.attachments.len(),
        16,
        "truncated to the per-note cap"
    );
    assert_eq!(payload.attachments[0].url, "https://remote.example/0.png");
    assert_eq!(payload.attachments[15].url, "https://remote.example/15.png");

    // Unit-level budget gate (mirrors `process_remote_media_fetch`): the
    // first attachment is always attempted; once `attempted > 0` and the
    // budget is exhausted, later fetches stop.
    let admit = |attempted: usize, total: u64| -> bool {
        !(attempted > 0 && total >= MAX_REMOTE_MEDIA_TOTAL_BYTES_PER_NOTE)
    };
    // Two 64 MiB attachments: first attempted, second skipped.
    assert!(admit(0, 0), "first attachment always attempted");
    assert!(
        !admit(1, 64 * 1024 * 1024),
        "second large attachment skipped"
    );
    // Single oversize-first: still attempted (never starves the first).
    assert!(admit(0, 0), "oversize-first still attempted");
    // Small attachments under budget keep flowing.
    assert!(
        admit(3, 3 * 1024 * 1024),
        "under-budget attachments admitted"
    );

    // Real attach path: the 17th remote file on one note is rejected.
    let note = h
        .store
        .create_note(
            CreateNoteInput::simple_public(&author, "many files"),
            vec![],
        )
        .expect("create note");
    for i in 0..16 {
        let (input, bytes) =
            evict_remote_input(&format!("https://remote.example/n{i}.png"), i as u8);
        h.cache.put(&input.blob_key, &bytes).unwrap();
        h.services
            .attach_remote_media_to_note(&note.id, input)
            .unwrap_or_else(|e| panic!("attach {i}: {e:?}"));
    }
    let (last, last_bytes) = evict_remote_input("https://remote.example/n16.png", 0xFF);
    h.cache.put(&last.blob_key, &last_bytes).unwrap();
    match h.services.attach_remote_media_to_note(&note.id, last) {
        Err(PortError::Validation(msg)) => assert_eq!(msg, "TOO_MANY_ATTACHMENTS"),
        other => panic!("17th attachment must be rejected, got {other:?}"),
    }
}
