use super::*;

fn local_account() -> Account {
    Account::new_local("aid00000000000001".to_string(), "alice".to_string()).unwrap()
}

fn remote_account() -> Account {
    Account {
        id: "aid00000000000002".to_string(),
        username: "bob".to_string(),
        username_lower: "bob".to_string(),
        host: Some("remote.example".to_string()),
        display_name: None,
        description: None,
        location: None,
        birthday: None,
        uri: Some("https://remote.example/users/bob".to_string()),
        inbox: Some("https://remote.example/users/bob/inbox".to_string()),
        shared_inbox: None,
        followers_count: 0,
        following_count: 0,
        notes_count: 0,
        avatar_id: None,
        banner_id: None,
        is_locked: false,
        is_bot: false,
        is_suspended: false,
        is_admin: false,
        is_moderator: false,
        is_explorable: false,
        is_deleted: false,
        moderator_note: None,
        is_cat: false,
        hide_online_status: false,
        avatar_decorations: Vec::new(),
        also_known_as: None,
        require_signin_to_view_contents: false,
        make_notes_followers_only_before: None,
        make_notes_hidden_before: None,
        chat_scope: ChatScope::default(),
        followed_message: None,
        lang: None,
        fields: Vec::new(),
        verified_links: Vec::new(),
        public_reactions: true,
        following_visibility: FollowVisibility::default(),
        followers_visibility: FollowVisibility::default(),
        auto_accept_followed: false,
        follow_approval_local_seconds: None,
        follow_approval_remote_seconds: None,
        no_crawle: false,
        prevent_ai_learning: true,
        always_mark_nsfw: false,
        auto_sensitive: false,
        careful_bot: false,
        inject_featured_note: true,
        receive_announcement_email: true,
        pinned_page_id: None,
        enable_word_mute: false,
        muted_words: Vec::new(),
        hard_muted_words: Vec::new(),
        muted_instances: Vec::new(),
        notification_recieve_config: std::collections::BTreeMap::new(),
        email_notification_types: default_email_notification_types(),
        reject_quotes: false,
        default_reaction: None,
        show_remote_emojis: false,
        email: None,
        email_verified: false,
        email_verify_code: None,
        moved_to_uri: None,
        moved_at_ms: None,
    }
}

#[test]
fn new_local_sets_defaults() {
    let a = local_account();
    assert_eq!(a.username_lower, "alice");
    assert_eq!(a.host, None);
    assert!(a.is_explorable);
    assert!(!a.is_locked && !a.is_bot && !a.is_suspended && !a.is_deleted && !a.is_cat);
    assert!(!a.is_admin && !a.is_moderator && !a.has_moderator_powers());
    assert_eq!(
        (a.followers_count, a.following_count, a.notes_count),
        (0, 0, 0)
    );
    assert!(a.validate().is_ok());
}

#[test]
fn new_local_lowercases_username() {
    let a = Account::new_local("aid00000000000003".to_string(), "Alice_1".to_string()).unwrap();
    assert_eq!(a.username, "Alice_1");
    assert_eq!(a.username_lower, "alice_1");
    assert!(a.validate().is_ok());
}

#[test]
fn local_username_bounds() {
    for good in ["a".to_string(), "A_09".to_string(), "x".repeat(20)] {
        assert!(
            validate_local_username(&good).is_ok(),
            "expected ok: {good}"
        );
    }
    for bad in [
        "".to_string(),
        "x".repeat(21),
        "has space".to_string(),
        "hy-phen".to_string(),
        "dot.name".to_string(),
        "caf\u{e9}".to_string(),
    ] {
        assert!(
            validate_local_username(&bad).is_err(),
            "expected err: {bad}"
        );
    }
}

#[test]
fn remote_username_bounds() {
    for good in [
        "a".to_string(),
        "user.name-1_x".to_string(),
        "x".repeat(128),
        "0".to_string(),
    ] {
        assert!(
            validate_remote_username(&good).is_ok(),
            "expected ok: {good}"
        );
    }
    for bad in [
        "".to_string(),
        "x".repeat(129),
        ".leading".to_string(),
        "trailing.".to_string(),
        "-leading".to_string(),
        "trailing-".to_string(),
        "has space".to_string(),
    ] {
        assert!(
            validate_remote_username(&bad).is_err(),
            "expected err: {bad}"
        );
    }
    assert!(validate_remote_username("_").is_ok());
    assert!(validate_remote_username("a_b").is_ok());
}

#[test]
fn description_and_location_bounds() {
    let mut a = local_account();
    a.description = Some("d".repeat(1500));
    assert!(a.validate().is_ok());
    a.description = Some("d".repeat(1501));
    assert!(a.validate().is_err());

    let mut a = local_account();
    a.location = Some("l".repeat(50));
    assert!(a.validate().is_ok());
    a.location = Some("l".repeat(51));
    assert!(a.validate().is_err());
}

#[test]
fn birthday_bounds() {
    assert!(validate_birthday("1990-02-28").is_ok());
    assert!(validate_birthday("2000-02-29").is_ok()); // leap day
    assert!(validate_birthday("1990-02-30").is_err()); // no such date
    assert!(validate_birthday("1990-13-01").is_err());
    assert!(validate_birthday("not-a-date").is_err());
    assert!(validate_birthday("1990-2-8").is_err()); // not zero-padded
    assert!(validate_birthday("").is_err());

    let mut a = local_account();
    a.birthday = Some("1990-02-30".to_string());
    assert!(a.validate().is_err());
    a.birthday = Some("1990-02-28".to_string());
    assert!(a.validate().is_ok());
}

#[test]
fn remote_uri_requirement() {
    assert!(remote_account().validate().is_ok());

    let mut no_uri = remote_account();
    no_uri.uri = None;
    assert!(no_uri.validate().is_err());

    let mut no_inbox = remote_account();
    no_inbox.inbox = None;
    assert!(no_inbox.validate().is_err());

    let mut local_with_uri = local_account();
    local_with_uri.uri = Some("https://example.local/users/x".to_string());
    assert!(local_with_uri.validate().is_err());
}

#[test]
fn actor_url_forms() {
    let local = local_account();
    assert_eq!(
        local.actor_url("https://example.local/"),
        "https://example.local/users/aid00000000000001"
    );
    let remote = remote_account();
    assert_eq!(
        remote.actor_url("https://example.local"),
        "https://remote.example/users/bob"
    );
}

#[test]
fn profile_store_defaults_match_upstream_columns() {
    // `MiUser` / `MiUserProfile` column defaults (verified against the
    // Misskey 2026.10.1-alpha.0 checkout: `models/User.ts`,
    // `models/UserProfile.ts`).
    let a = local_account();
    assert!(a.public_reactions);
    assert!(a.prevent_ai_learning);
    assert!(a.inject_featured_note);
    assert!(a.receive_announcement_email);
    assert_eq!(a.chat_scope, ChatScope::Mutual);
    assert_eq!(a.following_visibility, FollowVisibility::Public);
    assert_eq!(a.followers_visibility, FollowVisibility::Public);
    assert_eq!(
        a.email_notification_types,
        vec!["follow".to_string(), "receiveFollowRequest".to_string()]
    );
    assert!(!a.hide_online_status);
    assert!(!a.careful_bot);
    assert!(!a.auto_accept_followed);
    assert!(!a.no_crawle);
    assert!(!a.always_mark_nsfw);
    assert!(!a.auto_sensitive);
    assert!(!a.require_signin_to_view_contents);
    assert!(!a.enable_word_mute);
    assert!(!a.reject_quotes);
    assert!(!a.is_admin);
    assert!(!a.is_moderator);
    assert_eq!(a.default_reaction, None);
    assert!(a.validate().is_ok());
    // Serde round-trip preserves the defaults (old rows without the new
    // keys decode via `#[serde(default)]`).
    let json = serde_json::to_string(&a).unwrap();
    let back: Account = serde_json::from_str(&json).unwrap();
    assert_eq!(back, a);
    // Rows written before §0B (no profile keys) decode to the upstream
    // column defaults instead of failing.
    let legacy = serde_json::json!({
        "id": a.id,
        "username": a.username,
        "username_lower": "alice",
        "host": null,
        "display_name": null,
        "description": null,
        "location": null,
        "birthday": null,
        "uri": null,
        "inbox": null,
        "shared_inbox": null,
        "followers_count": 0,
        "following_count": 0,
        "notes_count": 0,
        "avatar_id": null,
        "banner_id": null,
        "is_locked": false,
        "is_bot": false,
        "is_suspended": false,
        "is_explorable": true,
        "is_deleted": false,
        "is_cat": false,
    });
    let decoded: Account = serde_json::from_value(legacy).unwrap();
    assert!(decoded.public_reactions);
    assert!(decoded.prevent_ai_learning);
    assert!(decoded.inject_featured_note);
    assert!(decoded.receive_announcement_email);
    assert_eq!(decoded.chat_scope, ChatScope::Mutual);
    // Rows written before Batch M carry no role keys: both default off.
    assert!(!decoded.is_admin && !decoded.is_moderator);
    assert!(decoded.validate().is_ok());
}

#[test]
fn profile_store_bounds() {
    let mut a = local_account();
    a.followed_message = Some("".to_string());
    assert!(a.validate().is_err());
    a.followed_message = Some("m".repeat(256));
    assert!(a.validate().is_ok());
    a.followed_message = Some("m".repeat(257));
    assert!(a.validate().is_err());

    let mut a = local_account();
    a.fields = vec![
        ProfileField {
            name: "n".to_string(),
            value: "v".to_string()
        };
        17
    ];
    assert!(a.validate().is_err());

    let mut a = local_account();
    a.avatar_decorations = vec![
        AvatarDecoration {
            id: "d".to_string(),
            angle: Some(0.6),
            flip_h: None,
            offset_x: None,
            offset_y: None,
        };
        1
    ];
    assert!(a.validate().is_err());

    let mut a = local_account();
    a.follow_approval_local_seconds = Some(-1);
    assert!(a.validate().is_err());
    a.follow_approval_local_seconds = Some(FOLLOW_APPROVAL_MAX_SECONDS + 1);
    assert!(a.validate().is_err());
    a.follow_approval_local_seconds = Some(3600);
    assert!(a.validate().is_ok());

    let mut a = local_account();
    a.default_reaction = Some("".to_string());
    assert!(a.validate().is_err());
    a.default_reaction = Some("r".repeat(261));
    assert!(a.validate().is_err());
    a.default_reaction = Some("❤".to_string());
    assert!(a.validate().is_ok());

    let mut a = local_account();
    a.also_known_as = Some(vec!["u".to_string(); 11]);
    assert!(a.validate().is_err());

    let bad = NotificationRecieveConfig {
        kind: "sometimes".to_string(),
        user_list_id: None,
    };
    assert!(bad.validate().is_err());
    let bad_list = NotificationRecieveConfig {
        kind: "list".to_string(),
        user_list_id: None,
    };
    assert!(bad_list.validate().is_err());
    let good = NotificationRecieveConfig {
        kind: "mutualFollow".to_string(),
        user_list_id: None,
    };
    assert!(good.validate().is_ok());
}

#[test]
fn roles_default_off_and_admin_implies_moderator() {
    let mut a = local_account();
    assert!(!a.has_moderator_powers());
    a.is_moderator = true;
    assert!(a.has_moderator_powers());
    a.is_moderator = false;
    a.is_admin = true;
    assert!(a.has_moderator_powers());
    assert!(a.validate().is_ok());
}

#[test]
fn rename_validates_shape_availability_and_liveness() {
    let a = local_account();
    assert!(super::validate_rename(&a, "bob_2", false).is_ok());
    assert!(super::validate_rename(&a, "has space", false).is_err());
    assert_eq!(
        super::validate_rename(&a, "bob_2", true),
        Err(crate::domain::id::DomainError::Conflict(
            "USERNAME_TAKEN".to_string()
        ))
    );
    let mut gone = local_account();
    gone.is_suspended = true;
    assert!(super::validate_rename(&gone, "bob_2", false).is_err());
    gone.is_suspended = false;
    gone.is_deleted = true;
    assert!(super::validate_rename(&gone, "bob_2", false).is_err());
}

#[test]
fn move_sets_once_and_never_clears() {
    let a = local_account();
    let (uri, at) = super::validate_move(&a, "https://new.example/users/alice", 123).unwrap();
    assert_eq!(uri, "https://new.example/users/alice");
    assert_eq!(at, 123);
    assert!(super::validate_move(&a, "gopher://x", 123).is_err());
    let mut moved = local_account();
    moved.moved_to_uri = Some("https://new.example/users/alice".to_string());
    moved.moved_at_ms = Some(123);
    // `prohibitMoved`: a second move is refused even to a new target.
    assert!(super::validate_move(&moved, "https://other.example/u", 456).is_err());
    let mut remote = remote_account();
    remote.moved_to_uri = None;
    assert!(super::validate_move(&remote, "https://other.example/u", 1).is_err());
}

#[test]
fn pinned_page_and_verified_links_loop() {
    assert!(super::validate_pinned_page_update(None).is_ok());
    assert!(super::validate_pinned_page_update(Some("page1")).is_ok());
    assert!(super::validate_pinned_page_update(Some("  ")).is_err());
    // Every profile update resets to empty; re-verify is armed until a
    // verification timestamp covers the update.
    assert!(super::reset_verified_links_for_reverify().is_empty());
    assert!(super::needs_link_reverify(None, 100));
    assert!(super::needs_link_reverify(Some(50), 100));
    assert!(!super::needs_link_reverify(Some(100), 100));
    assert!(!super::needs_link_reverify(Some(150), 100));
}

#[test]
fn signin_input_and_password_policy() {
    assert!(super::validate_signin_input("alice", "s3cret!!").is_ok());
    assert!(super::validate_signin_input("  ", "s3cret!!").is_err());
    assert!(super::validate_signin_input("alice", "").is_err());
    assert!(super::password_meets_policy("s3cret!!").is_ok());
    assert!(super::password_meets_policy("short").is_err());
    assert!(super::password_meets_policy(&"x".repeat(129)).is_err());
}
