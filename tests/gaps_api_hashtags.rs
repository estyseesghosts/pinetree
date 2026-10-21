//! WP2-part-B: `hashtags/*` application-layer contract (real redb backend,
//! derived entirely from existing note/account stores — no new storage).
//!
//! Covers list/search/show/trend/users, the `NO_SUCH_HASHTAG` miss, and
//! visibility filtering of tag aggregates.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    BlobStore, Clock, CreateNoteInput, IdGenerator, NoopStreamingSink, PortError, TimelineCursor,
};
use pinetree::domain::id::{AidxGenerator, ManualClock};
use pinetree::domain::note::Visibility;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services(prefix: &str) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-gaps-hashtags-{}-{prefix}-{}",
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

fn public_note(services: &AppServices, author_id: &str, text: &str) {
    services
        .create_note(CreateNoteInput::simple_public(author_id, text))
        .unwrap();
}

#[test]
fn hashtag_list_search_show_trend_users() {
    let services = test_services("basic");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    public_note(&services, &alice.id, "hello #rust world");
    public_note(&services, &alice.id, "again #rust and #misskey");
    public_note(&services, &bob.id, "#misskey from bob");

    let list = services.hashtags_list(None, 10).unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].tag, "misskey");
    assert_eq!(list[0].mentioned_notes_count, 2);
    assert_eq!(list[1].tag, "rust");

    let search = services.hashtags_search(None, "rus", 10).unwrap();
    assert_eq!(search.len(), 1);
    assert_eq!(search[0].tag, "rust");
    assert!(services.hashtags_search(None, "zzz", 10).unwrap().is_empty());

    let show = services.hashtag_show(None, "Rust").unwrap();
    assert_eq!(show.tag, "rust");
    assert_eq!(show.mentioned_notes_count, 2);
    assert_eq!(show.mentioned_users_count, 1);
    assert_eq!(show.mentioned_local_users_count, 1);
    assert_eq!(show.mentioned_remote_users_count, 0);

    assert_eq!(
        services.hashtag_show(None, "neverused").unwrap_err(),
        PortError::NotFound("NO_SUCH_HASHTAG".to_string())
    );

    let trend = services.hashtags_trend(None).unwrap();
    assert!(!trend.is_empty());
    assert!(trend.len() <= 10);

    let users = services.hashtag_users(None, "misskey", 10).unwrap();
    assert_eq!(users.len(), 2);

    // Case-insensitive lookup matches the same tag.
    assert_eq!(
        services.hashtag_show(None, "MISSKEY").unwrap().mentioned_notes_count,
        2
    );
}

#[test]
fn hashtag_aggregates_respect_visibility() {
    let services = test_services("visibility");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    public_note(&services, &alice.id, "open #gated");
    let mut followers_only = CreateNoteInput::simple_public(&alice.id, "closed #gated");
    followers_only.visibility = Visibility::Followers;
    services.create_note(followers_only).unwrap();

    // Anonymous sees only the public note.
    let anon = services.hashtag_show(None, "gated").unwrap();
    assert_eq!(anon.mentioned_notes_count, 1);

    // The author sees both.
    let author = services.hashtag_show(Some(&alice.id), "gated").unwrap();
    assert_eq!(author.mentioned_notes_count, 2);

    // A non-follower stranger sees only the public one.
    let stranger = services.hashtag_show(Some(&bob.id), "gated").unwrap();
    assert_eq!(stranger.mentioned_notes_count, 1);

    // After following, both are visible.
    services.follow(&bob.id, &alice.id, false).unwrap();
    let follower = services.hashtag_show(Some(&bob.id), "gated").unwrap();
    assert_eq!(follower.mentioned_notes_count, 2);
}

#[test]
fn hashtag_list_respects_cursor_limit() {
    let services = test_services("limit");
    let alice = services.create_account("alice", None).unwrap();
    public_note(&services, &alice.id, "#one #two #three");
    let list = services.hashtags_list(None, 2).unwrap();
    assert_eq!(list.len(), 2);
    // Empty query in search matches everything (substring of every tag).
    let search = services.hashtags_search(None, "", 1).unwrap();
    assert_eq!(search.len(), 1);
    let _ = TimelineCursor::with_limit(10);
}
