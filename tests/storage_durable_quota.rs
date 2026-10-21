//! Per-user durable drive quota (`drive_capacity_per_user_bytes`, 1 GiB
//! default): enforced in-transaction by `upload_media` (pre-check plus the
//! atomic `insert_metadata_with_durable_quota`), with dedup free,
//! grandfathered reads/deletes unaffected, and cross-owner shared blobs
//! surviving a sharer's delete via the refcount-aware `delete_media`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, CreateAccountInput, InsertMediaInput, MediaStore,
    NoopStreamingSink, PortError,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::MediaProperties;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
static QUOTA_SEQ: AtomicU64 = AtomicU64::new(0);

struct QuotaHarness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    blobs: Arc<FsBlobStore>,
}

fn quota_harness(drive_quota_bytes: u64) -> (QuotaHarness, String) {
    let n = QUOTA_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-quota-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<ManualClock> = Arc::new(ManualClock::new(BASE_MS));
    let clock_port: Arc<dyn Clock> = clock.clone();
    let store =
        RedbStore::open_with_default_ids(dir.join("store.redb"), clock_port.clone()).unwrap();
    let blobs = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    let cache_blobs = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
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
        blobs: blobs.clone(),
        cache_blobs,
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
        drive_capacity_per_user_bytes: drive_quota_bytes,
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
    let owner = store
        .create_account(CreateAccountInput::local(format!("quota_user_{n}")), None)
        .expect("create owner")
        .id;
    (
        QuotaHarness {
            store,
            services,
            blobs,
        },
        owner,
    )
}

fn quota_bytes(seed: u8, len: usize) -> Vec<u8> {
    // PNG-magic prefix so the shared sniffer accepts the bytes; trailing
    // seed byte keeps distinct payloads distinct.
    let mut v = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    v.resize(len.max(8), seed);
    v[len - 1] = seed;
    v
}

fn quota_expect_exceeded(err: PortError) {
    match err {
        PortError::Validation(msg) => assert_eq!(msg, "DRIVE_CAPACITY_EXCEEDED"),
        other => panic!("expected DRIVE_CAPACITY_EXCEEDED, got {other:?}"),
    }
}

fn quota_input(owner: &str, name: &str, bytes: &[u8], max_file_size: u64) -> InsertMediaInput {
    InsertMediaInput {
        id: None,
        owner_id: Some(owner.to_string()),
        name: name.to_string(),
        mime: "image/png".to_string(),
        size_bytes: bytes.len() as u64,
        hash: md5_hex(bytes),
        blob_key: sha256_hex(bytes),
        blurhash: None,
        properties: MediaProperties::default(),
        is_sensitive: false,
        is_link: false,
        url: format!("https://pinetree.local/files/{name}"),
        thumbnail_url: None,
        comment: None,
        source_url: None,
        max_file_size,
    }
}

#[test]
fn quota_over_cap_rejected_with_no_row_and_no_bytes() {
    let (h, owner) = quota_harness(64);
    let big = quota_bytes(0xA1, 128);

    // Application path: pre-check fails before any blob write.
    let err = h
        .services
        .upload_media(&owner, "big.png".to_string(), &big, None, false, false)
        .expect_err("over-quota upload rejected");
    quota_expect_exceeded(err);
    assert!(
        h.store.list_by_owner(&owner).unwrap().is_empty(),
        "rejected upload leaves no metadata row"
    );
    assert_eq!(
        h.blobs.get(&sha256_hex(&big)).unwrap(),
        None,
        "rejected upload leaves no blob bytes"
    );
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), 0);

    // Store path: the in-transaction check is authoritative on a lost race.
    let err = h
        .store
        .insert_metadata_with_durable_quota(
            quota_input(&owner, "big2.png", &big, h.services.max_file_size),
            64,
        )
        .expect_err("over-quota insert rejected");
    quota_expect_exceeded(err);
    assert!(h.store.list_by_owner(&owner).unwrap().is_empty());
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), 0);
}

#[test]
fn quota_upload_media_default_path_smoke() {
    // 1 GiB default quota: an ordinary upload succeeds end to end.
    let (h, owner) = quota_harness(1 << 30);
    let bytes = quota_bytes(0xB2, 32);
    let meta = h
        .services
        .upload_media(&owner, "smoke.png".to_string(), &bytes, None, false, false)
        .expect("default-path upload succeeds");
    assert_eq!(meta.size_bytes, bytes.len() as u64);
    assert_eq!(
        h.services
            .get_media_for(&owner, &meta.id)
            .expect("read back")
            .id,
        meta.id
    );
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), bytes.len() as u64);
    assert_eq!(h.store.blob_refcount(&meta.blob_key).unwrap(), 1);
}

#[test]
fn quota_dedup_hit_does_not_charge_twice() {
    let (h, owner) = quota_harness(100);
    let bytes = quota_bytes(0xC3, 60);
    let first = h
        .services
        .upload_media(&owner, "one.png".to_string(), &bytes, None, false, false)
        .expect("first upload");
    let second = h
        .services
        .upload_media(
            &owner,
            "one-again.png".to_string(),
            &bytes,
            None,
            false,
            false,
        )
        .expect("dedup hit");
    assert_eq!(first.id, second.id, "same owner + same bytes → same row");
    assert_eq!(h.store.list_by_owner(&owner).unwrap().len(), 1);
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), 60);
    assert_eq!(h.store.blob_refcount(&first.blob_key).unwrap(), 1);
    // The 60 bytes are still charged once: 60 + 50 > 100 must fail.
    let other = quota_bytes(0xC4, 50);
    let err = h
        .services
        .upload_media(&owner, "other.png".to_string(), &other, None, false, false)
        .expect_err("over quota");
    quota_expect_exceeded(err);
}

#[test]
fn quota_grandfathered_user_can_read_and_delete_then_upload() {
    let (h, owner) = quota_harness(64);
    // Rows written before the quota existed (plain `insert_metadata`
    // bypasses the quota check): the user is over quota from the start.
    let over1 = quota_bytes(0xD1, 48);
    let over2 = quota_bytes(0xD2, 48);
    for (i, b) in [over1.clone(), over2.clone()].into_iter().enumerate() {
        h.blobs.put(&sha256_hex(&b), &b).unwrap();
        h.store
            .insert_metadata(quota_input(
                &owner,
                &format!("legacy-{i}.png"),
                &b,
                h.services.max_file_size,
            ))
            .unwrap();
    }
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), 96);

    // Over-quota user can still read their files (no quota check on reads).
    let rows = h.store.list_by_owner(&owner).unwrap();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        h.services
            .get_media_for(&owner, &row.id)
            .expect("grandfathered read works");
    }
    // And delete: the refcount-aware path frees bytes …
    h.services
        .delete_media(&owner, &rows[0].id)
        .expect("grandfathered delete works");
    assert_eq!(
        h.blobs.get(&rows[0].blob_key).unwrap(),
        None,
        "deleted row frees its bytes"
    );
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), 48);
    // … so a subsequent upload under the freed budget succeeds.
    let small = quota_bytes(0xD3, 16);
    h.services
        .upload_media(&owner, "fresh.png".to_string(), &small, None, false, false)
        .expect("upload after free");
    assert_eq!(h.store.durable_bytes(&owner).unwrap(), 48 + 16);
}

#[test]
fn quota_cross_owner_identical_bytes_owner_delete_preserves_other() {
    let (h, alice) = quota_harness(1 << 30);
    let bob = h
        .store
        .create_account(CreateAccountInput::local("quota_bob_shared"), None)
        .expect("create bob")
        .id;
    let bytes = quota_bytes(0xE5, 40);

    let a_meta = h
        .services
        .upload_media(&alice, "shared.png".to_string(), &bytes, None, false, false)
        .expect("alice upload");
    let b_meta = h
        .services
        .upload_media(&bob, "shared.png".to_string(), &bytes, None, false, false)
        .expect("bob upload");
    assert_ne!(a_meta.id, b_meta.id, "dedup is per-owner: distinct rows");
    assert_eq!(
        a_meta.blob_key, b_meta.blob_key,
        "identical bytes share one blob"
    );
    assert_eq!(h.store.blob_refcount(&a_meta.blob_key).unwrap(), 2);

    h.services
        .delete_media(&alice, &a_meta.id)
        .expect("alice delete");
    assert!(
        h.store.get(&a_meta.id).unwrap().is_none(),
        "alice row is gone"
    );
    let b_row = h.store.get(&b_meta.id).unwrap().expect("bob row survives");
    assert_eq!(b_row.owner_id.as_deref(), Some(bob.as_str()));
    assert_eq!(
        h.blobs.get(&b_meta.blob_key).unwrap().as_deref(),
        Some(bytes.as_slice()),
        "shared bytes survive while bob references them"
    );
    assert_eq!(h.store.blob_refcount(&b_meta.blob_key).unwrap(), 1);
}
