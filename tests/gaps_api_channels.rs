//! WP2-part-B: `channels/*` application-layer contract (real redb backend
//! for accounts, atomic mem backend for channel rows).
//!
//! Covers create/show/update/delete (+ validation + owner gates), follow +
//! unfollow (idempotent), followed/owned/featured/search listings,
//! favorite/unfavorite/my-favorites, timeline emptiness, and delete cascade.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::channels::{ChannelStore, IdCursor, MemChannelStore};
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
        "pinetree-gaps-channels-{}-{prefix}-{}",
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
fn channel_crud_roundtrip() {
    let services = test_services("crud");
    let store = MemChannelStore::new();
    let alice = services.create_account("alice", None).unwrap();

    let channel = services
        .channel_create(&store, &alice.id, "general", Some("lobby"), None, None)
        .unwrap();
    assert_eq!(channel.owner_id.as_deref(), Some(alice.id.as_str()));
    assert_eq!(channel.name, "general");

    let shown = services.channel_show(&store, &channel.id).unwrap();
    assert_eq!(shown, channel);

    let updated = services
        .channel_update(
            &store,
            &alice.id,
            &channel.id,
            Some("general-2"),
            None,
            None,
            Some(Some("#ff0000")),
            Some(true),
            None,
        )
        .unwrap();
    assert_eq!(updated.name, "general-2");
    assert!(updated.is_sensitive);

    services
        .channel_delete(&store, &alice.id, &channel.id)
        .unwrap();
    assert_eq!(
        services.channel_show(&store, &channel.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_CHANNEL".to_string())
    );
    assert!(store.drain_jobs().is_empty());
}

#[test]
fn channel_create_validates_name_and_unknown_is_no_such_channel() {
    let services = test_services("validation");
    let store = MemChannelStore::new();
    let alice = services.create_account("alice", None).unwrap();

    assert!(services
        .channel_create(&store, &alice.id, "  ", None, None, None)
        .is_err());
    assert_eq!(
        services.channel_show(&store, "nope").unwrap_err(),
        PortError::NotFound("NO_SUCH_CHANNEL".to_string())
    );
    assert_eq!(
        services
            .channel_update(&store, &alice.id, "nope", Some("x"), None, None, None, None, None)
            .unwrap_err(),
        PortError::NotFound("NO_SUCH_CHANNEL".to_string())
    );
}

#[test]
fn channel_update_delete_are_owner_only() {
    let services = test_services("owner-gate");
    let store = MemChannelStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let channel = services
        .channel_create(&store, &alice.id, "general", None, None, None)
        .unwrap();

    assert_eq!(
        services
            .channel_update(&store, &bob.id, &channel.id, Some("hijack"), None, None, None, None, None)
            .unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string())
    );
    assert_eq!(
        services
            .channel_delete(&store, &bob.id, &channel.id)
            .unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string())
    );
    // The channel survives both refused mutations.
    assert_eq!(
        services.channel_show(&store, &channel.id).unwrap().name,
        "general"
    );
}

#[test]
fn channel_follow_unfollow_are_idempotent() {
    let services = test_services("follow");
    let store = MemChannelStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let channel = services
        .channel_create(&store, &alice.id, "general", None, None, None)
        .unwrap();

    services.channel_follow(&store, &bob.id, &channel.id).unwrap();
    services.channel_follow(&store, &bob.id, &channel.id).unwrap();
    assert!(store.is_following_channel(&channel.id, &bob.id).unwrap());

    let followed = services
        .channels_followed(&store, &bob.id, &IdCursor::with_limit(10))
        .unwrap();
    assert_eq!(followed.len(), 1);

    services.channel_unfollow(&store, &bob.id, &channel.id).unwrap();
    services.channel_unfollow(&store, &bob.id, &channel.id).unwrap();
    assert!(!store.is_following_channel(&channel.id, &bob.id).unwrap());
}

#[test]
fn channel_listings_search_featured_and_timeline() {
    let services = test_services("listings");
    let store = MemChannelStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    let rust = services
        .channel_create(&store, &alice.id, "rustaceans", Some("rust chat"), None, None)
        .unwrap();
    let _other = services
        .channel_create(&store, &alice.id, "cooking", None, None, None)
        .unwrap();
    services.channel_follow(&store, &bob.id, &rust.id).unwrap();

    let owned = services
        .channels_owned(&store, &alice.id, &IdCursor::with_limit(10))
        .unwrap();
    assert_eq!(owned.len(), 2);

    let found = services
        .channels_search(&store, &rust.id, &IdCursor::with_limit(10))
        .unwrap();
    assert!(found.is_empty(), "search matches name/description, not id");
    let found = services
        .channels_search(&store, "rust", &IdCursor::with_limit(10))
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, rust.id);

    let featured = services
        .channels_featured(&store, &IdCursor::with_limit(10))
        .unwrap();
    assert_eq!(featured.len(), 2);
    assert_eq!(featured[0].id, rust.id, "followed channel ranks first");

    // No channel posting surface exists: existing channels read empty.
    assert!(services
        .channel_timeline(&store, &rust.id, &IdCursor::with_limit(10))
        .unwrap()
        .is_empty());
    assert_eq!(
        services
            .channel_timeline(&store, "missing", &IdCursor::with_limit(10))
            .unwrap_err(),
        PortError::NotFound("NO_SUCH_CHANNEL".to_string())
    );
}

#[test]
fn channel_favorites_and_delete_cascade() {
    let services = test_services("favorites");
    let store = MemChannelStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let channel = services
        .channel_create(&store, &alice.id, "general", None, None, None)
        .unwrap();

    services.channel_favorite(&store, &bob.id, &channel.id).unwrap();
    services.channel_favorite(&store, &bob.id, &channel.id).unwrap();
    let mine = services
        .channels_my_favorites(&store, &bob.id, &IdCursor::with_limit(10))
        .unwrap();
    assert_eq!(mine.len(), 1);
    services.channel_unfavorite(&store, &bob.id, &channel.id).unwrap();
    services.channel_unfavorite(&store, &bob.id, &channel.id).unwrap();
    assert!(services
        .channels_my_favorites(&store, &bob.id, &IdCursor::with_limit(10))
        .unwrap()
        .is_empty());

    // Follows + favorites cascade on delete.
    services.channel_follow(&store, &bob.id, &channel.id).unwrap();
    services.channel_favorite(&store, &bob.id, &channel.id).unwrap();
    services
        .channel_delete(&store, &alice.id, &channel.id)
        .unwrap();
    assert!(!store.is_following_channel(&channel.id, &bob.id).unwrap());
    assert!(services
        .channels_followed(&store, &bob.id, &IdCursor::with_limit(10))
        .unwrap()
        .is_empty());
    assert!(services
        .channels_my_favorites(&store, &bob.id, &IdCursor::with_limit(10))
        .unwrap()
        .is_empty());
}
