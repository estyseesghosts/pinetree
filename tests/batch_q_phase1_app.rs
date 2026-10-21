//! Batch Q Phase 1 application-layer fixes (integration tests over the
//! real redb backend):
//! - FIX 2: `notes/delete` moderator bypass (`delete_note`).
//! - FIX 3a: `users/show` suspended-user 404 with moderator bypass
//!   (`users_show_by_id_as` / `users_show_by_username_as`).
//! - FIX 3b: `notes/show` anonymous `CONTENT_RESTRICTED_BY_USER` gate.
//! - FIX 7: home/hybrid timeline `includeMyRenotes` /
//!   `includeRenotedMyNotes` / `includeLocalRenotes` flags.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    BlobStore, Clock, CreateAccountInput, CreateNoteInput, IdGenerator, NoopStreamingSink,
    PortError, TimelineCursor, UpdateAccountPatch,
};
use pinetree::domain::id::{AidxGenerator, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services(prefix: &str) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-batchq-test-{}-{prefix}-{}",
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

/// A pure (bare) reshare of `target_id` by `author_id`.
fn pure_renote(services: &AppServices, author_id: &str, target_id: &str) -> String {
    let mut input = CreateNoteInput::simple_public(author_id, "placeholder");
    input.text = None;
    input.renote_of = Some(target_id.to_string());
    services.create_note(input).unwrap().id
}

fn remote_account(services: &AppServices, handle: &str, host: &str) -> String {
    services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: handle.to_string(),
                host: Some(host.to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://{host}/users/{handle}")),
                inbox: Some(format!("https://{host}/inbox")),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .unwrap()
        .id
}

// --- FIX 2 ---------------------------------------------------------------

#[test]
fn moderator_can_delete_anothers_note() {
    let services = test_services("mod-delete");
    let alice = services.create_account("alice", None).unwrap();
    let moderator = services.create_account("mod", None).unwrap();
    services
        .set_role(None, &moderator.id, false, true)
        .unwrap();
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "hello"))
        .unwrap();

    services.delete_note(&moderator.id, &note.id).unwrap();

    let err = services
        .notes_show(&note.id, Some(&moderator.id))
        .unwrap_err();
    assert_eq!(err, PortError::NotFound("NO_SUCH_NOTE".to_string()));
}

#[test]
fn non_owner_non_moderator_delete_is_access_denied_exact_code() {
    let services = test_services("mod-delete-denied");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "hello"))
        .unwrap();

    let err = services.delete_note(&bob.id, &note.id).unwrap_err();
    assert_eq!(err, PortError::Validation("ACCESS_DENIED".to_string()));

    // The note survives the refused delete.
    services
        .notes_show(&note.id, Some(&bob.id))
        .expect("refused delete must not remove the note");
}

#[test]
fn author_can_still_delete_own_note() {
    let services = test_services("author-delete");
    let alice = services.create_account("alice", None).unwrap();
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "hello"))
        .unwrap();
    services.delete_note(&alice.id, &note.id).unwrap();
}

// --- FIX 3a --------------------------------------------------------------

#[test]
fn users_show_suspended_hidden_from_non_moderator_visible_to_moderator() {
    let services = test_services("users-show-suspended");
    let bob = services.create_account("bob", None).unwrap();
    let carol = services.create_account("carol", None).unwrap();
    let moderator = services.create_account("mod", None).unwrap();
    services
        .set_role(None, &moderator.id, false, true)
        .unwrap();
    services
        .suspend_user(&moderator.id, &carol.id)
        .unwrap();

    // Anonymous and ordinary viewers read the suspended account as missing.
    for viewer in [None, Some(bob.id.as_str())] {
        assert_eq!(
            services.users_show_by_id_as(&carol.id, viewer).unwrap_err(),
            PortError::NotFound("NO_SUCH_USER".to_string()),
            "suspended user hidden from {viewer:?}"
        );
        assert_eq!(
            services
                .users_show_by_username_as("carol", None, viewer)
                .unwrap_err(),
            PortError::NotFound("NO_SUCH_USER".to_string()),
            "suspended user hidden from {viewer:?} (username path)"
        );
    }

    // Moderators still see the suspended account on both paths.
    let by_id = services
        .users_show_by_id_as(&carol.id, Some(&moderator.id))
        .unwrap();
    assert_eq!(by_id.id, carol.id);
    let by_name = services
        .users_show_by_username_as("carol", None, Some(&moderator.id))
        .unwrap();
    assert_eq!(by_name.id, carol.id);

    // Non-suspended accounts are unaffected on every path.
    assert_eq!(
        services
            .users_show_by_id_as(&bob.id, None)
            .unwrap()
            .id,
        bob.id
    );
    assert_eq!(
        services
            .users_show_by_username_as("bob", None, Some(&bob.id))
            .unwrap()
            .id,
        bob.id
    );
}

// --- FIX 3b --------------------------------------------------------------

#[test]
fn notes_show_anonymous_require_signin_author_is_content_restricted() {
    let services = test_services("notes-show-gated");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    services
        .update_profile(
            &alice.id,
            UpdateAccountPatch {
                require_signin_to_view_contents: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "gated hello"))
        .unwrap();

    // Anonymous: exact upstream code string.
    assert_eq!(
        services.notes_show(&note.id, None).unwrap_err(),
        PortError::Validation("CONTENT_RESTRICTED_BY_USER".to_string())
    );

    // Signed-in viewers (and the author) are unaffected.
    services
        .notes_show(&note.id, Some(&bob.id))
        .expect("signed-in viewer must see the note");
    services
        .notes_show(&note.id, Some(&alice.id))
        .expect("author must see her own note");
}

#[test]
fn notes_show_anonymous_open_author_unaffected() {
    let services = test_services("notes-show-open");
    let alice = services.create_account("alice", None).unwrap();
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "open hello"))
        .unwrap();
    services
        .notes_show(&note.id, None)
        .expect("anonymous viewer must see an ungated note");
}

// --- FIX 7: home timeline -------------------------------------------------

#[test]
fn home_timeline_include_my_renotes_false_excludes_own_renotes() {
    let services = test_services("home-my-renotes");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    services.follow(&bob.id, &alice.id, false).unwrap();
    let root = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "root"))
        .unwrap();
    let own_renote = pure_renote(&services, &bob.id, &root.id);

    let cursor = TimelineCursor::with_limit(20);
    let default: Vec<String> = services
        .home_timeline(&bob.id, &cursor, false, true)
        .unwrap()
        .into_iter()
        .map(|n| n.id)
        .collect();
    assert!(
        default.contains(&own_renote),
        "default (flags omitted) includes own renotes: {default:?}"
    );

    let excluded: Vec<String> = services
        .home_timeline_with_includes(&bob.id, &cursor, false, true, false, true, true)
        .unwrap()
        .into_iter()
        .map(|n| n.id)
        .collect();
    assert!(
        !excluded.contains(&own_renote),
        "includeMyRenotes=false excludes own renotes: {excluded:?}"
    );
    assert!(
        excluded.contains(&root.id),
        "the renoted root itself stays: {excluded:?}"
    );
}

#[test]
fn home_timeline_include_renoted_my_notes_false_excludes_renotes_of_me() {
    let services = test_services("home-renoted-mine");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    services.follow(&bob.id, &alice.id, false).unwrap();
    let mine = services
        .create_note(CreateNoteInput::simple_public(&bob.id, "mine"))
        .unwrap();
    let renote_of_mine = pure_renote(&services, &alice.id, &mine.id);

    let cursor = TimelineCursor::with_limit(20);
    let default: Vec<String> = services
        .home_timeline(&bob.id, &cursor, false, true)
        .unwrap()
        .into_iter()
        .map(|n| n.id)
        .collect();
    assert!(
        default.contains(&renote_of_mine),
        "default includes renotes of my notes: {default:?}"
    );

    let excluded: Vec<String> = services
        .home_timeline_with_includes(&bob.id, &cursor, false, true, true, false, true)
        .unwrap()
        .into_iter()
        .map(|n| n.id)
        .collect();
    assert!(
        !excluded.contains(&renote_of_mine),
        "includeRenotedMyNotes=false excludes them: {excluded:?}"
    );
}

#[test]
fn home_timeline_include_local_renotes_false_keeps_only_remote_target_renotes() {
    let services = test_services("home-local-renotes");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    services.follow(&bob.id, &alice.id, false).unwrap();
    let local_root = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "local root"))
        .unwrap();
    let local_renote = pure_renote(&services, &alice.id, &local_root.id);

    let rina = remote_account(&services, "rina", "remote.example");
    let remote_root = services
        .create_note(CreateNoteInput::simple_public(&rina, "remote root"))
        .unwrap();
    let remote_renote = pure_renote(&services, &alice.id, &remote_root.id);

    let cursor = TimelineCursor::with_limit(20);
    let default: Vec<String> = services
        .home_timeline(&bob.id, &cursor, false, true)
        .unwrap()
        .into_iter()
        .map(|n| n.id)
        .collect();
    assert!(default.contains(&local_renote), "default keeps local-target renote");
    assert!(default.contains(&remote_renote), "default keeps remote-target renote");

    let filtered: Vec<String> = services
        .home_timeline_with_includes(&bob.id, &cursor, false, true, true, true, false)
        .unwrap()
        .into_iter()
        .map(|n| n.id)
        .collect();
    assert!(
        !filtered.contains(&local_renote),
        "includeLocalRenotes=false excludes local-target renotes: {filtered:?}"
    );
    assert!(
        filtered.contains(&remote_renote),
        "includeLocalRenotes=false keeps remote-target renotes: {filtered:?}"
    );
}

// --- FIX 7: hybrid timeline -----------------------------------------------

#[test]
fn hybrid_timeline_include_my_renotes_false_excludes_own_renotes() {
    let services = test_services("hybrid-my-renotes");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    services.follow(&bob.id, &alice.id, false).unwrap();
    let root = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "root"))
        .unwrap();
    let own_renote = pure_renote(&services, &bob.id, &root.id);

    let cursor = TimelineCursor::with_limit(20);
    let default: Vec<String> = services
        .hybrid_timeline(&bob.id, &cursor, false, true, false)
        .unwrap()
        .into_iter()
        .map(|n| n.id)
        .collect();
    assert!(
        default.contains(&own_renote),
        "default hybrid includes own renotes: {default:?}"
    );

    // The flag must hold even though the renote is local-public (reachable
    // via the local arm): upstream applies the flags to the combined query.
    let excluded: Vec<String> = services
        .hybrid_timeline_with_includes(&bob.id, &cursor, false, true, false, false, true, true)
        .unwrap()
        .into_iter()
        .map(|n| n.id)
        .collect();
    assert!(
        !excluded.contains(&own_renote),
        "hybrid includeMyRenotes=false excludes own renotes: {excluded:?}"
    );
    assert!(
        excluded.contains(&root.id),
        "the renoted root itself stays: {excluded:?}"
    );
}
