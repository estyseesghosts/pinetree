//! WP2-part-B: `chat/*` application-layer contract (real redb backend for
//! accounts, atomic mem backend for chat rows).
//!
//! Covers rooms (create/show/update/delete/join/leave/members/mute),
//! invitations (invite/inbox/outbox/accept/decline), messages
//! (create/show/update/delete/read), history paging, and unread counts.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::channels::IdCursor;
use pinetree::application::commands::chat::MemChatStore;
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
        "pinetree-gaps-chat-{}-{prefix}-{}",
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
fn room_create_show_update_delete_with_owner_gate() {
    let services = test_services("room-crud");
    let store = MemChatStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    let room = services
        .chat_room_create(&store, &alice.id, Some("lobby"), Some("hang out"), None)
        .unwrap();
    assert_eq!(room.owner_id, alice.id);

    // Non-members read the room as missing.
    assert_eq!(
        services.chat_room_show(&store, &bob.id, &room.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string())
    );

    let updated = services
        .chat_room_update(
            &store,
            &alice.id,
            &room.id,
            Some(Some("lobby-2")),
            Some(None),
        )
        .unwrap();
    assert_eq!(updated.name.as_deref(), Some("lobby-2"));
    assert!(updated.description.is_none());

    assert_eq!(
        services
            .chat_room_update(&store, &bob.id, &room.id, Some(Some("x")), None)
            .unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string()),
        "non-members never reach the owner check"
    );

    services.chat_room_join(&store, &bob.id, &room.id).unwrap();
    assert_eq!(
        services
            .chat_room_update(&store, &bob.id, &room.id, Some(Some("x")), None)
            .unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string())
    );
    assert_eq!(
        services
            .chat_room_delete(&store, &bob.id, &room.id)
            .unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string())
    );

    services.chat_room_delete(&store, &alice.id, &room.id).unwrap();
    assert_eq!(
        services.chat_room_show(&store, &alice.id, &room.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string())
    );
    assert!(store.drain_jobs().is_empty());
}

#[test]
fn room_create_invites_target_and_validates() {
    let services = test_services("room-create-11");
    let store = MemChatStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    // Unknown target.
    assert_eq!(
        services
            .chat_room_create(&store, &alice.id, None, None, Some("missing"))
            .unwrap_err(),
        PortError::NotFound("NO_SUCH_USER".to_string())
    );
    // 1:1 with yourself.
    assert!(services
        .chat_room_create(&store, &alice.id, None, None, Some(&alice.id))
        .is_err());
    // Blank name.
    assert!(services
        .chat_room_create(&store, &alice.id, Some("  "), None, None)
        .is_err());

    let room = services
        .chat_room_create(&store, &alice.id, None, None, Some(&bob.id))
        .unwrap();
    // Owner is in; the target holds an invitation.
    assert!(services.chat_room_show(&store, &alice.id, &room.id).is_ok());
    assert_eq!(
        services.chat_room_show(&store, &bob.id, &room.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string())
    );
    let inbox = services.chat_invitations_inbox(&store, &bob.id).unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].room_id, room.id);
    let outbox = services.chat_invitations_outbox(&store, &alice.id).unwrap();
    assert_eq!(outbox.len(), 1);

    // Accept joins + consumes the invitation.
    let joined = services.chat_invitation_accept(&store, &bob.id, &alice.id).unwrap();
    assert_eq!(joined.id, room.id);
    assert!(services.chat_room_show(&store, &bob.id, &room.id).is_ok());
    assert!(services.chat_invitations_inbox(&store, &bob.id).unwrap().is_empty());

    // Nothing left to accept or decline.
    assert_eq!(
        services.chat_invitation_accept(&store, &bob.id, &alice.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_INVITATION".to_string())
    );
    assert_eq!(
        services.chat_invitation_decline(&store, &bob.id, &alice.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_INVITATION".to_string())
    );
}

#[test]
fn invite_decline_and_join_consumes_invitation() {
    let services = test_services("invite-decline");
    let store = MemChatStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let room = services
        .chat_room_create(&store, &alice.id, Some("group"), None, None)
        .unwrap();

    services.chat_room_invite(&store, &alice.id, &room.id, &bob.id).unwrap();
    // Idempotent re-invite returns the same row.
    let again = services.chat_room_invite(&store, &alice.id, &room.id, &bob.id).unwrap();
    assert_eq!(services.chat_invitations_inbox(&store, &bob.id).unwrap().len(), 1);

    services.chat_invitation_decline(&store, &bob.id, &alice.id).unwrap();
    assert!(services.chat_invitations_inbox(&store, &bob.id).unwrap().is_empty());

    // Re-invite then join directly: the invitation is consumed.
    services.chat_room_invite(&store, &alice.id, &room.id, &bob.id).unwrap();
    assert_eq!(again.room_id, room.id);
    services.chat_room_join(&store, &bob.id, &room.id).unwrap();
    services.chat_room_join(&store, &bob.id, &room.id).unwrap();
    assert!(services.chat_invitations_inbox(&store, &bob.id).unwrap().is_empty());

    let members = services.chat_room_members(&store, &alice.id, &room.id).unwrap();
    assert_eq!(members.len(), 2);
}

#[test]
fn leave_transfers_ownership_and_last_leave_deletes() {
    let services = test_services("leave");
    let store = MemChatStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let room = services
        .chat_room_create(&store, &alice.id, Some("group"), None, None)
        .unwrap();
    services.chat_room_join(&store, &bob.id, &room.id).unwrap();

    // Non-members cannot leave.
    let carol = services.create_account("carol", None).unwrap();
    assert_eq!(
        services.chat_room_leave(&store, &carol.id, &room.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string())
    );

    // Owner leaves: ownership passes to the remaining member.
    services.chat_room_leave(&store, &alice.id, &room.id).unwrap();
    let room = services.chat_room_show(&store, &bob.id, &room.id).unwrap();
    assert_eq!(room.owner_id, bob.id);

    // Last member out deletes the room.
    services.chat_room_leave(&store, &bob.id, &room.id).unwrap();
    assert_eq!(
        services.chat_room_show(&store, &bob.id, &room.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string())
    );
}

#[test]
fn mute_flag_is_member_scoped_and_idempotent() {
    let services = test_services("mute");
    let store = MemChatStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let room = services
        .chat_room_create(&store, &alice.id, Some("group"), None, None)
        .unwrap();
    services.chat_room_join(&store, &bob.id, &room.id).unwrap();

    services.chat_room_set_muted(&store, &bob.id, &room.id, true).unwrap();
    services.chat_room_set_muted(&store, &bob.id, &room.id, true).unwrap();
    let members = services.chat_room_members(&store, &alice.id, &room.id).unwrap();
    let bob_row = members.iter().find(|m| m.user_id == bob.id).unwrap();
    assert!(bob_row.muted);
    services.chat_room_set_muted(&store, &bob.id, &room.id, false).unwrap();

    let carol = services.create_account("carol", None).unwrap();
    assert_eq!(
        services.chat_room_set_muted(&store, &carol.id, &room.id, true).unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string())
    );
}

#[test]
fn messages_crud_read_and_unread() {
    let services = test_services("messages");
    let store = MemChatStore::new();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let carol = services.create_account("carol", None).unwrap();
    let room = services
        .chat_room_create(&store, &alice.id, Some("group"), None, None)
        .unwrap();
    services.chat_room_join(&store, &bob.id, &room.id).unwrap();

    // Empty body is rejected; outsiders cannot post.
    assert!(services
        .chat_message_create(&store, &alice.id, &room.id, Some("  "), None)
        .is_err());
    assert_eq!(
        services
            .chat_message_create(&store, &carol.id, &room.id, Some("hi"), None)
            .unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string())
    );

    let first = services
        .chat_message_create(&store, &alice.id, &room.id, Some("hello"), None)
        .unwrap();
    let second = services
        .chat_message_create(&store, &bob.id, &room.id, Some("hey"), Some("file1"))
        .unwrap();
    assert_eq!(second.file_id.as_deref(), Some("file1"));

    assert_eq!(
        services.chat_message_show(&store, &carol.id, &first.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string())
    );
    assert_eq!(
        services.chat_message_show(&store, &alice.id, &first.id).unwrap().text.as_deref(),
        Some("hello")
    );

    // Non-authors cannot edit; authors can (still requiring a body).
    assert_eq!(
        services
            .chat_message_update(&store, &bob.id, &first.id, Some("hijack"), None)
            .unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string())
    );
    let edited = services
        .chat_message_update(&store, &alice.id, &first.id, Some("hello!"), None)
        .unwrap();
    assert_eq!(edited.text.as_deref(), Some("hello!"));
    assert!(edited.updated_at_ms.is_some());
    assert!(services
        .chat_message_update(&store, &alice.id, &first.id, Some(""), Some(None))
        .is_err());

    // Delete: author or room owner.
    assert_eq!(
        services.chat_message_delete(&store, &carol.id, &first.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_ROOM".to_string())
    );
    assert_eq!(
        services.chat_message_delete(&store, &bob.id, &first.id).unwrap_err(),
        PortError::Validation("ACCESS_DENIED".to_string()),
        "bob is neither author nor owner"
    );
    services.chat_message_delete(&store, &alice.id, &second.id).unwrap();
    assert_eq!(
        services.chat_message_show(&store, &alice.id, &second.id).unwrap_err(),
        PortError::NotFound("NO_SUCH_MESSAGE".to_string())
    );

    // History: newest-first, room filter, cursor paging.
    let third = services
        .chat_message_create(&store, &alice.id, &room.id, Some("third"), None)
        .unwrap();
    let history = services
        .chat_history(&store, &alice.id, Some(&room.id), &IdCursor::with_limit(10))
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id, third.id);
    assert_eq!(history[1].id, first.id);
    let page = services
        .chat_history(
            &store,
            &alice.id,
            Some(&room.id),
            &IdCursor { since_id: None, until_id: Some(third.id.clone()), limit: 10 },
        )
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id, first.id);

    // Unread: bob's message to alice... second was deleted, so only alice's
    // live messages count for bob; alice's own messages never count.
    assert_eq!(services.chat_unread_count(&store, &bob.id).unwrap(), 2);
    services.chat_message_read(&store, &bob.id, &first.id).unwrap();
    assert_eq!(services.chat_unread_count(&store, &bob.id).unwrap(), 1);
    services.chat_message_read(&store, &bob.id, &third.id).unwrap();
    assert_eq!(services.chat_unread_count(&store, &bob.id).unwrap(), 0);
    // Read watermarks are monotonic: re-reading an older message is a no-op.
    services.chat_message_read(&store, &bob.id, &first.id).unwrap();
    assert_eq!(services.chat_unread_count(&store, &bob.id).unwrap(), 0);
    assert_eq!(services.chat_unread_count(&store, &alice.id).unwrap(), 0);
}
