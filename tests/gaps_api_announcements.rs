//! WP2-part-B: `announcements*` + `i/read-announcement` + `promo/read`
//! application-layer contract (real redb backend for accounts, atomic mem
//! backend for announcement rows).
//!
//! Covers create/list/show, read marking with badge clearing, the
//! `NO_SUCH_ANNOUNCEMENT` miss, and idempotent promo reads.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::announcements::{AnnouncementStore, MemAnnouncementStore};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    BlobStore, Clock, IdGenerator, NoopStreamingSink, PortError,
};
use pinetree::domain::id::{AidxGenerator, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services(prefix: &str) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-gaps-announce-{}-{prefix}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
    AppServices {
        clock,
        ids,
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
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone(),
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
        public_url: "https://example.local".to_string(),
        max_file_size: 10 * 1024 * 1024,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs.clone(),
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: pinetree::config::RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    }
}

#[test]
fn announcements_list_show_and_badge_clears() {
    let services = test_services("badge");
    let store = MemAnnouncementStore::new();
    let alice = services.create_account("alice", None).unwrap();

    assert!(services.announcements_list(&store, Some(&alice.id)).unwrap().is_empty());
    assert_eq!(services.announcements_unread_count(&store, &alice.id).unwrap(), 0);

    // Validation: blank title / blank text rejected.
    assert!(services
        .announcement_create(&store, "  ", "body", None, false, false)
        .is_err());
    assert!(services
        .announcement_create(&store, "title", "", None, false, false)
        .is_err());

    let first = services
        .announcement_create(&store, "welcome", "hello everyone", None, true, true)
        .unwrap();
    let second = services
        .announcement_create(&store, "update", "new things", None, false, false)
        .unwrap();

    // Newest-first with unread flags.
    let list = services.announcements_list(&store, Some(&alice.id)).unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].announcement.id, second.id);
    assert!(!list[0].is_read);
    // Anonymous sees the same rows, never read.
    assert_eq!(services.announcements_list(&store, None).unwrap().len(), 2);

    // Show + miss.
    let shown = services
        .announcement_show(&store, &first.id, Some(&alice.id))
        .unwrap();
    assert!(!shown.is_read);
    assert_eq!(
        services.announcement_show(&store, "missing", Some(&alice.id)).unwrap_err(),
        PortError::NotFound("NO_SUCH_ANNOUNCEMENT".to_string())
    );
    assert_eq!(
        services.announcement_read(&store, &alice.id, "missing").unwrap_err(),
        PortError::NotFound("NO_SUCH_ANNOUNCEMENT".to_string())
    );

    // Badge: 2 unread -> read one -> 1 -> read other -> 0 (idempotent).
    assert_eq!(services.announcements_unread_count(&store, &alice.id).unwrap(), 2);
    services.announcement_read(&store, &alice.id, &first.id).unwrap();
    services.announcement_read(&store, &alice.id, &first.id).unwrap();
    assert_eq!(services.announcements_unread_count(&store, &alice.id).unwrap(), 1);
    services.announcement_read(&store, &alice.id, &second.id).unwrap();
    assert_eq!(services.announcements_unread_count(&store, &alice.id).unwrap(), 0);

    let list = services.announcements_list(&store, Some(&alice.id)).unwrap();
    assert!(list.iter().all(|v| v.is_read));
    assert!(store.drain_jobs().is_empty());
}

#[test]
fn promo_read_is_idempotent_and_per_user() {
    let services = test_services("promo");
    let store = MemAnnouncementStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    assert!(!store.promo_is_read(&alice.id).unwrap());
    services.promo_read(&store, &alice.id).unwrap();
    services.promo_read(&store, &alice.id).unwrap();
    assert!(store.promo_is_read(&alice.id).unwrap());
    // Per-user: bob is unaffected.
    assert!(!store.promo_is_read(&bob.id).unwrap());
}
