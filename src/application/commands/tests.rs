use super::AppServices;

use crate::activitypub::signatures::{rsa_sha256_sign, rsa_sha256_verify};
use crate::application::ports::{
    ActivityDedup, AuthStore, BlobStore, Clock, IdGenerator, NoopStreamingSink,
};
use crate::domain::id::{AidxGenerator, ManualClock};
use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use crate::media::storage::FsBlobStore;
use crate::persistence::redb::RedbStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn test_services(prefix: &str) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-cmd-test-{}-{prefix}-{}",
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
        public_url: "https://example.local".to_string(),
        max_file_size: 10 * 1024 * 1024,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs.clone(),
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: crate::config::RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    }
}

/// Every remote server fetching a new actor at once must be served the same key: concurrent first
/// calls to `ensure_actor_keypair` may each generate a keypair, but only one may be stored and returned.
#[test]
fn concurrent_ensure_actor_keypair_yields_one_key() {
    let services = test_services("keys-concurrent");
    let alice = services.create_account("alice", None).unwrap();
    let keys: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| services.ensure_actor_keypair(&alice.id).unwrap()))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(keys.windows(2).all(|w| w[0] == w[1]), "callers saw different keys");
    let stored = services.auth.get_actor_keypair(&alice.id).unwrap().unwrap();
    assert_eq!(stored, keys[0], "the stored key is the one every caller got");
}

/// Concurrent first fetches of any actor each provision the instance `system_actor`; none may fail.
#[test]
fn concurrent_ensure_system_actor_all_succeed() {
    let services = test_services("system-actor-concurrent");
    let results: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8).map(|_| scope.spawn(|| services.ensure_system_actor())).collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    for r in &results {
        assert!(r.is_ok(), "{r:?}");
    }
    let first = results[0].as_ref().unwrap();
    assert!(results.iter().all(|r| r.as_ref().unwrap().0.id == first.0.id && r.as_ref().unwrap().1 == first.1));
}

#[test]
fn actor_keypair_generation_rotation_and_remote_key_cache() {
    let services = test_services("keys");
    let alice = services.create_account("alice", None).unwrap();

    // Initial lazy generation creates and persists an RSA-2048 keypair.
    let kp1 = services.ensure_actor_keypair(&alice.id).unwrap();
    let kp1_again = services.ensure_actor_keypair(&alice.id).unwrap();
    assert_eq!(kp1, kp1_again, "ensure_actor_keypair is idempotent");
    assert!(kp1.public_key_pem.contains("BEGIN PUBLIC KEY"));
    assert!(kp1.private_key_pem.contains("BEGIN PRIVATE KEY"));

    let sig = rsa_sha256_sign(&kp1.private_key_pem, b"federation payload").unwrap();
    assert!(
        rsa_sha256_verify(&kp1.public_key_pem, b"federation payload", &sig).unwrap()
    );

    // Key rotation replaces the persisted keypair.
    let kp2 = services.rotate_actor_keypair(&alice.id).unwrap();
    assert_ne!(kp1.public_key_pem, kp2.public_key_pem);
    let fetched = services.auth.get_actor_keypair(&alice.id).unwrap().unwrap();
    assert_eq!(fetched.public_key_pem, kp2.public_key_pem);

    // System actor provisioning is idempotent and yields a valid keypair.
    let (sys1, sys_kp1) = services.ensure_system_actor().unwrap();
    let (sys2, sys_kp2) = services.ensure_system_actor().unwrap();
    assert_eq!(sys1.id, sys2.id);
    assert_eq!(sys_kp1, sys_kp2);
    assert!(sys1.is_bot);

    // Remote public key cache stores and retrieves by key_id.
    let cached = services
        .cache_remote_public_key(
            "https://remote.example/users/bob#main-key",
            "https://remote.example/users/bob",
            None,
            &kp1.public_key_pem,
        )
        .unwrap();
    let loaded = services
        .auth
        .get_remote_public_key("https://remote.example/users/bob#main-key")
        .unwrap()
        .unwrap();
    assert_eq!(loaded, cached);
}

// --- Slice 8 (D6) content-addressing tests (written before the fix) ---

// --- Slice 7 (H2) local-upload sniff tests (written before the fix) ---

#[test]
fn slice8_distinct_uploads_never_share_blob_storage() {
    // Two distinct blobs get distinct SHA-256 keys and both stay
    // retrievable under their own key.
    let services = test_services("slice8-distinct");
    let alice = services.create_account("alice", None).unwrap();
    let a = services
        .upload_media(&alice.id, "a.txt".to_string(), b"payload A", None, false, true)
        .unwrap();
    let b = services
        .upload_media(&alice.id, "b.txt".to_string(), b"payload B", None, false, true)
        .unwrap();
    assert_ne!(a.blob_key, b.blob_key, "distinct bytes → distinct blob keys");
    assert_ne!(a.hash, b.hash);
    assert_eq!(
        services.blobs.get(&a.blob_key).unwrap().as_deref(),
        Some(b"payload A".as_slice())
    );
    assert_eq!(
        services.blobs.get(&b.blob_key).unwrap().as_deref(),
        Some(b"payload B".as_slice())
    );
}

#[test]
fn slice8_owner_dedup_still_works_and_force_bypasses() {
    // Per-owner dedup still keys on the md5: same bytes twice → same row,
    // single blob write. `force = true` bypasses the dedup but lands on
    // the same blob key.
    let services = test_services("slice8-dedup");
    let alice = services.create_account("alice", None).unwrap();
    let bytes = b"slice8 dedup payload";
    let first = services
        .upload_media(&alice.id, "a.bin".to_string(), bytes, None, false, false)
        .unwrap();
    let second = services
        .upload_media(&alice.id, "a.bin".to_string(), bytes, None, false, false)
        .unwrap();
    assert_eq!(first.id, second.id, "same bytes → same row (md5 dedup)");
    // The blob lives under the SHA-256 key, never under the md5.
    assert_ne!(first.blob_key, first.hash);
    assert_eq!(
        services.blobs.get(&first.blob_key).unwrap().as_deref(),
        Some(bytes.as_slice())
    );
    assert_eq!(
        services.blobs.get(&first.hash).unwrap(),
        None,
        "md5 is not a blob key"
    );
    let forced = services
        .upload_media(&alice.id, "a.bin".to_string(), bytes, None, false, true)
        .unwrap();
    assert_ne!(forced.id, first.id, "force bypasses the dedup");
    assert_eq!(forced.blob_key, first.blob_key, "same bytes → same blob key");
    assert_eq!(forced.hash, first.hash, "md5 unchanged for the same bytes");
}

#[test]
fn slice8_empty_upload_pins_known_md5_and_sha256_vectors() {
    // The empty input has universally-known digests — md5("") and
    // sha256("") are also the values the pre-existing fixtures carry, so
    // no constant here is trusted blindly. Pins the packer `md5` value
    // absolutely across the blob-key migration.
    let services = test_services("slice8-vectors");
    let alice = services.create_account("alice", None).unwrap();
    let meta = services
        .upload_media(&alice.id, "empty.bin".to_string(), b"", None, false, true)
        .unwrap();
    assert_eq!(meta.hash, "d41d8cd98f00b204e9800998ecf8427e");
    assert_eq!(
        meta.blob_key,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        services.blobs.get(&meta.blob_key).unwrap().as_deref(),
        Some(b"".as_slice())
    );
}

#[test]
fn local_non_utf8_upload_claiming_html_is_not_html() {
    // A local upload of non-UTF-8 bytes must never be stored as the
    // caller-claimed `text/html` (the old duplicate fell back to the
    // caller MIME for non-UTF-8 bytes).
    let services = test_services("slice7-local-sniff");
    let alice = services.create_account("alice", None).unwrap();
    let bytes = vec![0xFF, 0xFE, 0x00, 0x01, 0x02, 0x03];
    let meta = services
        .upload_media(&alice.id, "evil.html".to_string(), &bytes, None, false, true)
        .unwrap();
    assert_eq!(meta.mime, "application/octet-stream");
    assert_ne!(meta.mime, "text/html");
}

#[test]
fn local_upload_sniff_beats_declared_type() {
    let services = test_services("slice7-local-declared");
    let alice = services.create_account("alice", None).unwrap();
    // Real PNG bytes are stored as PNG however they arrive...
    let mut png = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    png.extend_from_slice(&[0x00, 0x01, 0x02, 0x03]);
    let meta = services
        .upload_media(&alice.id, "x.bin".to_string(), &png, None, false, true)
        .unwrap();
    assert_eq!(meta.mime, "image/png");
    // ...and markup bytes never become text, even for a `.txt` name.
    let svg = b"<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>".to_vec();
    let meta = services
        .upload_media(&alice.id, "x.txt".to_string(), &svg, None, false, true)
        .unwrap();
    assert_eq!(meta.mime, "application/octet-stream");
    // Plain UTF-8 still stores as text.
    let meta = services
        .upload_media(&alice.id, "hi.txt".to_string(), b"hello world", None, false, true)
        .unwrap();
    assert_eq!(meta.mime, "text/plain");
}

// --- §0B full profile store -------------------------------------------------

use crate::application::ports::{PortError as ProfilePortError, UpdateAccountPatch as ProfilePatch};
use crate::domain::account::{AvatarDecoration as ProfileDeco, ChatScope as ProfileChatScope, FollowVisibility as ProfileVisibility, MutedWord as ProfileMuteWord, NotificationRecieveConfig as ProfileNotifConfig};

#[test]
fn profile_store_round_trip_and_defaults() {
    let services = test_services("profile-0b");
    let alice = services.create_account("alice", None).unwrap();
    // Upstream column defaults on fresh accounts.
    assert!(alice.public_reactions);
    assert!(alice.prevent_ai_learning);
    assert!(alice.inject_featured_note);
    assert!(alice.receive_announcement_email);
    assert_eq!(alice.chat_scope, ProfileChatScope::Mutual);
    assert_eq!(
        alice.email_notification_types,
        vec!["follow".to_string(), "receiveFollowRequest".to_string()]
    );
    assert!(!alice.reject_quotes);
    assert_eq!(alice.default_reaction, None);

    let mut notif = std::collections::BTreeMap::new();
    notif.insert(
        "mention".to_string(),
        ProfileNotifConfig { kind: "following".to_string(), user_list_id: None },
    );
    let updated = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                display_name: Some(Some("  Alice  ".to_string())),
                description: Some(Some("bio".to_string())),
                followed_message: Some(Some("hi".to_string())),
                location: Some(None),
                birthday: Some(Some("1990-02-28".to_string())),
                lang: Some(Some("en".to_string())),
                fields: Some(vec![crate::domain::account::ProfileField {
                    name: "x".to_string(),
                    value: "y".to_string(),
                }]),
                is_locked: Some(true),
                hide_online_status: Some(true),
                public_reactions: Some(false),
                careful_bot: Some(true),
                auto_accept_followed: Some(true),
                follow_approval_local_seconds: Some(Some(60)),
                follow_approval_remote_seconds: Some(None),
                no_crawle: Some(true),
                prevent_ai_learning: Some(false),
                require_signin_to_view_contents: Some(true),
                make_notes_followers_only_before: Some(Some(42)),
                make_notes_hidden_before: Some(None),
                inject_featured_note: Some(false),
                receive_announcement_email: Some(false),
                always_mark_nsfw: Some(true),
                auto_sensitive: Some(true),
                following_visibility: Some(ProfileVisibility::Followers),
                followers_visibility: Some(ProfileVisibility::Private),
                chat_scope: Some(ProfileChatScope::None),
                muted_words: Some(vec![ProfileMuteWord::And(vec![
                    "a".to_string(),
                    "b".to_string(),
                ])]),
                hard_muted_words: Some(vec![ProfileMuteWord::Single("/z/i".to_string())]),
                muted_instances: Some(vec!["spam.example".to_string()]),
                notification_recieve_config: Some(notif),
                email_notification_types: Some(vec!["mention".to_string()]),
                reject_quotes: Some(true),
                default_reaction: Some(Some("❤".to_string())),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(updated.display_name.as_deref(), Some("Alice"));
    assert_eq!(updated.followed_message.as_deref(), Some("hi"));
    assert_eq!(updated.location, None);
    assert_eq!(updated.chat_scope, ProfileChatScope::None);
    assert_eq!(updated.following_visibility, ProfileVisibility::Followers);
    assert_eq!(updated.followers_visibility, ProfileVisibility::Private);
    assert!(updated.enable_word_mute);
    assert_eq!(updated.muted_words.len(), 1);
    assert!(updated.notification_recieve_config.contains_key("mention"));
    assert!(updated.reject_quotes);
    assert_eq!(updated.default_reaction.as_deref(), Some("❤"));
    assert!(updated.verified_links.is_empty());
    assert!(updated.validate().is_ok());
}

#[test]
fn profile_name_trim_and_prohibited_words() {
    let mut services = test_services("profile-words");
    services.prohibited_words = vec!["/spam/i".to_string(), "bad word".to_string()];
    let alice = services.create_account("alice", None).unwrap();

    // Regex-form keyword matches case-insensitively.
    let err = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                display_name: Some(Some("I love SPAM posts".to_string())),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(
        err,
        ProfilePortError::Validation("YOUR_NAME_CONTAINS_PROHIBITED_WORDS".to_string())
    );
    // Space-separated keywords require all words present.
    let err = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                display_name: Some(Some("a bad word here".to_string())),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(
        err,
        ProfilePortError::Validation("YOUR_NAME_CONTAINS_PROHIBITED_WORDS".to_string())
    );
    // Blank names clear to null; surrounding whitespace trims.
    let updated = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                display_name: Some(Some("   ".to_string())),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(updated.display_name, None);
    let updated = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                display_name: Some(Some("  fine  ".to_string())),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(updated.display_name.as_deref(), Some("fine"));
}

#[test]
fn profile_word_mute_validation() {
    let services = test_services("profile-mute");
    let alice = services.create_account("alice", None).unwrap();

    for bad in ["/(/", "plain", "/unclosed"] {
        let err = services
            .update_profile(
                &alice.id,
                ProfilePatch {
                    muted_words: Some(vec![ProfileMuteWord::Single(bad.to_string())]),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(err, ProfilePortError::Validation("INVALID_REGEXP".to_string()), "{bad}");
    }
    let huge = format!("/{}/", "a".repeat(199));
    let err = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                hard_muted_words: Some(vec![ProfileMuteWord::Single(huge)]),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(
        err,
        ProfilePortError::Validation("TOO_MANY_MUTED_WORDS".to_string())
    );
    // Two decorations exceed the default avatarDecorationLimit of 1.
    let err = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                avatar_decorations: Some(vec![
                    ProfileDeco { id: "a".to_string(), angle: None, flip_h: None, offset_x: None, offset_y: None },
                    ProfileDeco { id: "b".to_string(), angle: None, flip_h: None, offset_x: None, offset_y: None },
                ]),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(
        err,
        ProfilePortError::Validation("RESTRICTED_BY_ROLE".to_string())
    );
}

#[test]
fn profile_also_known_as_resolution() {
    let services = test_services("profile-aka");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    // Self-reference is forbidden.
    let err = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                also_known_as: Some(Some(vec!["alice".to_string()])),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(
        err,
        ProfilePortError::Validation("FORBIDDEN_TO_SET_YOURSELF".to_string())
    );
    // Blank lines are NO_SUCH_USER.
    let err = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                also_known_as: Some(Some(vec!["  ".to_string()])),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(err, ProfilePortError::NotFound("NO_SUCH_USER".to_string()));
    // Unresolvable entries are skipped; `bob` resolves to his actor URI.
    let updated = services
        .update_profile(
            &alice.id,
            ProfilePatch {
                also_known_as: Some(Some(vec![
                    "ghost@nowhere.example".to_string(),
                    "bob".to_string(),
                ])),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        updated.also_known_as,
        Some(vec![format!("https://example.local/users/{}", bob.id)])
    );
}

#[test]
fn profile_unlock_accepts_pending_requests() {
    let services = test_services("profile-unlock");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    services
        .update_profile(
            &bob.id,
            ProfilePatch { is_locked: Some(true), ..Default::default() },
        )
        .unwrap();
    services.follow(&alice.id, &bob.id, false).unwrap();
    assert!(services.follows.get_follow_request(&alice.id, &bob.id).unwrap().is_some());

    services
        .update_profile(
            &bob.id,
            ProfilePatch { is_locked: Some(false), ..Default::default() },
        )
        .unwrap();
    assert!(services.follows.get_follow_request(&alice.id, &bob.id).unwrap().is_none());
    assert!(services.follows.is_following(&alice.id, &bob.id).unwrap());
}
