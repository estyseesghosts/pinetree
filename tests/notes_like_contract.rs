//! `notes/like` contract tests (op level): Sharkey/Catodon parity.
//!
//! Ground truth: `notes/like {noteId, override?}` in both references is a
//! thin wrapper over `ReactionService.create` with
//! `override ?? serverSettings.defaultLike` — NOT a separate data model and
//! NOT a Misskey favourite (local-only bookmark, no federation). Pinetree:
//! `override ??` reactor's `defaultReaction ??` instance `default_like`
//! config `??` ❤, with the Sharkey toggle (same-emoji re-press unlikes,
//! different-emoji press replaces) and federation through the normal
//! reaction `Like` path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{BlobStore, Clock, IdGenerator, PortError};
use pinetree::application::ports::{CreateNoteInput, NoopStreamingSink};
use pinetree::domain::id::{AidxGenerator, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services(prefix: &str) -> AppServices {
    test_services_with_like(prefix, None)
}

fn test_services_with_like(prefix: &str, default_like: Option<&str>) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-like-test-{}-{prefix}-{}",
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
        cache_blobs: blobs.clone(),
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
        default_like: default_like.map(str::to_string),
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

fn make_user(services: &AppServices, username: &str) -> String {
    services.create_account(username, None).unwrap().id
}

fn make_note(services: &AppServices, author_id: &str) -> String {
    services
        .create_note(CreateNoteInput {
            no_extract_mentions: true,
            ..CreateNoteInput::simple_public(author_id, "like me")
        })
        .unwrap()
        .id
}

fn stored_reaction(services: &AppServices, user_id: &str, note_id: &str) -> Option<String> {
    services
        .reactions
        .get_reaction(user_id, note_id)
        .unwrap()
        .map(|row| row.reaction)
}

#[test]
fn like_defaults_to_bare_heart() {
    let services = test_services("heart");
    let author = make_user(&services, "author");
    let user = make_user(&services, "user");
    let note = make_note(&services, &author);
    assert_eq!(services.like_note(&user, &note, None).unwrap(), true);
    assert_eq!(stored_reaction(&services, &user, &note).as_deref(), Some("❤"));
}

#[test]
fn like_prefers_override_then_user_default_then_instance() {
    // Instance default applies when nothing else is set.
    let services = test_services_with_like("instance", Some("👍"));
    let author = make_user(&services, "author");
    let user = make_user(&services, "user");
    let note = make_note(&services, &author);
    services.like_note(&user, &note, None).unwrap();
    assert_eq!(stored_reaction(&services, &user, &note).as_deref(), Some("👍"));

    // Explicit override wins over the instance default.
    let note2 = make_note(&services, &author);
    services.like_note(&user, &note2, Some("🎉")).unwrap();
    assert_eq!(stored_reaction(&services, &user, &note2).as_deref(), Some("🎉"));

    // User defaultReaction wins over the instance default.
    let user2 = make_user(&services, "user2");
    services
        .update_profile(
            &user2,
            pinetree::application::ports::UpdateAccountPatch {
                default_reaction: Some(Some("⭐".to_string())),
                ..Default::default()
            },
        )
        .unwrap();
    let note3 = make_note(&services, &author);
    services.like_note(&user2, &note3, None).unwrap();
    assert_eq!(stored_reaction(&services, &user2, &note3).as_deref(), Some("⭐"));
}

#[test]
fn relike_with_same_emoji_toggles_off() {
    let services = test_services("toggle");
    let author = make_user(&services, "author");
    let user = make_user(&services, "user");
    let note = make_note(&services, &author);
    assert_eq!(services.like_note(&user, &note, Some("👍")).unwrap(), true);
    assert_eq!(stored_reaction(&services, &user, &note).as_deref(), Some("👍"));
    // Same emoji again → unliked, never ALREADY_REACTED.
    assert_eq!(services.like_note(&user, &note, Some("👍")).unwrap(), false);
    assert_eq!(stored_reaction(&services, &user, &note), None);
}

#[test]
fn like_with_different_emoji_replaces() {
    let services = test_services("replace");
    let author = make_user(&services, "author");
    let user = make_user(&services, "user");
    let note = make_note(&services, &author);
    services.like_note(&user, &note, Some("👍")).unwrap();
    assert_eq!(services.like_note(&user, &note, Some("🎉")).unwrap(), true);
    assert_eq!(stored_reaction(&services, &user, &note).as_deref(), Some("🎉"));
}

#[test]
fn like_with_unknown_custom_emoji_falls_back_to_heart() {
    let services = test_services("fallback");
    let author = make_user(&services, "author");
    let user = make_user(&services, "user");
    let note = make_note(&services, &author);
    services.like_note(&user, &note, Some(":nope:")).unwrap();
    assert_eq!(stored_reaction(&services, &user, &note).as_deref(), Some("❤"));
}

#[test]
fn like_on_missing_note_is_no_such_note() {
    let services = test_services("missing");
    let user = make_user(&services, "user");
    let err = services.like_note(&user, "n00000000000000", None).unwrap_err();
    assert!(matches!(err, PortError::NotFound(ref code) if code == "NO_SUCH_NOTE"), "got {err:?}");
}
