//! WP2-part-B: `gallery/*` + `flash/*` application-layer contract (real
//! redb backend for accounts, atomic mem backend for gallery rows).
//!
//! Covers gallery post CRUD (+ validation + owner gates), like/unlike with
//! `ALREADY_LIKED`/`NOT_LIKED`, list/featured/popular rankings, and flash
//! CRUD + like/unlike + my/my-likes/featured.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::gallery::IdCursor;
use pinetree::application::commands::gallery::{GalleryStore, MemGalleryStore};
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
        "pinetree-gaps-gallery-{}-{prefix}-{}",
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

fn files(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("file{i}")).collect()
}

#[test]
fn gallery_post_crud_with_owner_gate() {
    let services = test_services("post-crud");
    let store = MemGalleryStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    // Files are required; duplicates rejected.
    assert!(services
        .gallery_post_create(&store, &alice.id, Some("art"), None, Vec::new(), false)
        .is_err());
    assert!(services
        .gallery_post_create(
            &store, &alice.id, Some("art"), None,
            vec!["f".to_string(), "f".to_string()], false,
        )
        .is_err());

    let post = services
        .gallery_post_create(&store, &alice.id, Some("art"), Some("mine"), files(2), false)
        .unwrap();
    assert_eq!(post.liked_count, 0);

    let updated = services
        .gallery_post_update(
            &store, &alice.id, &post.id,
            Some(Some("art v2")), None, None, Some(true),
        )
        .unwrap();
    assert_eq!(updated.title.as_deref(), Some("art v2"));
    assert!(updated.is_sensitive);

    assert_eq!(
        services
            .gallery_post_update(&store, &bob.id, &post.id, Some(Some("x")), None, None, None)
            .unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string())
    );
    assert_eq!(
        services.gallery_post_delete(&store, &bob.id, &post.id).unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string())
    );
    assert_eq!(
        services.gallery_post_show(&store, "missing").unwrap_err(),
        PortError::NotFound("NO_SUCH_POST".to_string())
    );

    services.gallery_post_delete(&store, &alice.id, &post.id).unwrap();
    assert_eq!(
        services.gallery_post_show(&store, &post.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_POST".to_string())
    );
    assert!(store.drain_jobs().is_empty());
}

#[test]
fn gallery_likes_and_rankings() {
    let services = test_services("post-likes");
    let store = MemGalleryStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let carol = services.create_account("carol", None).unwrap();

    let a = services
        .gallery_post_create(&store, &alice.id, Some("a"), None, files(1), false)
        .unwrap();
    let b = services
        .gallery_post_create(&store, &bob.id, Some("b"), None, files(1), false)
        .unwrap();

    services.gallery_post_like(&store, &carol.id, &a.id).unwrap();
    assert_eq!(
        services.gallery_post_like(&store, &carol.id, &a.id).unwrap_err(),
        PortError::Conflict("ALREADY_LIKED".to_string())
    );
    services.gallery_post_like(&store, &bob.id, &a.id).unwrap();
    services.gallery_post_like(&store, &carol.id, &b.id).unwrap();

    assert_eq!(services.gallery_post_show(&store, &a.id).unwrap().liked_count, 2);

    // Double-unlike errors; unknown post errors.
    assert_eq!(
        services.gallery_post_unlike(&store, &alice.id, &a.id).unwrap_err(),
        PortError::Conflict("NOT_LIKED".to_string())
    );
    assert_eq!(
        services.gallery_post_unlike(&store, &carol.id, "missing").unwrap_err(),
        PortError::NotFound("NO_SUCH_POST".to_string())
    );

    let featured = services.gallery_featured(&store, &IdCursor::with_limit(10)).unwrap();
    assert_eq!(featured.len(), 2);
    assert_eq!(featured[0].id, a.id, "most-liked ranks first");

    let popular = services.gallery_popular(&store, &IdCursor::with_limit(10)).unwrap();
    assert_eq!(popular.len(), 2);

    let listed = services.gallery_posts_list(&store, &IdCursor::with_limit(10)).unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].id, b.id, "newest-first");

    // Unliking drops the counter; delete cascades likes.
    services.gallery_post_unlike(&store, &carol.id, &a.id).unwrap();
    assert_eq!(services.gallery_post_show(&store, &a.id).unwrap().liked_count, 1);
    services.gallery_post_delete(&store, &alice.id, &a.id).unwrap();
    assert_eq!(
        services.gallery_post_like(&store, &bob.id, &a.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_POST".to_string())
    );
}

#[test]
fn flash_crud_likes_and_lists() {
    let services = test_services("flash");
    let store = MemGalleryStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    assert!(services
        .flash_create(&store, &alice.id, "  ", None, "script", "public")
        .is_err());
    assert!(services
        .flash_create(&store, &alice.id, "game", None, "script", "everyone")
        .is_err());

    let flash = services
        .flash_create(&store, &alice.id, "game", Some("fun"), "print(1)", "home")
        .unwrap();
    assert_eq!(services.flash_show(&store, &flash.id).unwrap(), flash);
    assert_eq!(
        services.flash_show(&store, "missing").unwrap_err(),
        PortError::NotFound("NO_SUCH_FLASH".to_string())
    );

    // Non-owner mutation refused.
    assert_eq!(
        services
            .flash_update(&store, &bob.id, &flash.id, Some("x"), None, None, None)
            .unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string())
    );
    assert_eq!(
        services.flash_delete(&store, &bob.id, &flash.id).unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string())
    );

    let updated = services
        .flash_update(
            &store, &alice.id, &flash.id,
            Some("game v2"), Some(None), Some("print(2)"), Some("public"),
        )
        .unwrap();
    assert_eq!(updated.title, "game v2");
    assert!(updated.summary.is_none());

    // Likes.
    services.flash_like(&store, &bob.id, &flash.id).unwrap();
    assert_eq!(
        services.flash_like(&store, &bob.id, &flash.id).unwrap_err(),
        PortError::Conflict("ALREADY_LIKED".to_string())
    );
    assert_eq!(
        services.flash_unlike(&store, &alice.id, &flash.id).unwrap_err(),
        PortError::Conflict("NOT_LIKED".to_string())
    );
    let liked = services
        .flash_my_likes(&store, &bob.id, &IdCursor::with_limit(10))
        .unwrap();
    assert_eq!(liked.len(), 1);
    assert_eq!(liked[0].id, flash.id);

    let mine = services.flash_my(&store, &alice.id, &IdCursor::with_limit(10)).unwrap();
    assert_eq!(mine.len(), 1);
    let featured = services.flash_featured(&store, &IdCursor::with_limit(10)).unwrap();
    assert_eq!(featured.len(), 1);

    services.flash_unlike(&store, &bob.id, &flash.id).unwrap();
    assert!(services
        .flash_my_likes(&store, &bob.id, &IdCursor::with_limit(10))
        .unwrap()
        .is_empty());

    services.flash_delete(&store, &alice.id, &flash.id).unwrap();
    assert!(services.flash_my(&store, &alice.id, &IdCursor::with_limit(10)).unwrap().is_empty());
}
