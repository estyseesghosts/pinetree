use super::*;

fn valid_note() -> Note {
    Note {
        id: "nid00000000000001".to_string(),
        author_id: "aid00000000000001".to_string(),
        text: Some("hello".to_string()),
        cw: None,
        visibility: Visibility::Public,
        visible_user_ids: Vec::new(),
        local_only: false,
        reply_id: None,
        renote_id: None,
        reply_user_id: None,
        file_ids: Vec::new(),
        mentions: Vec::new(),
        mentioned_remote_users: "[]".to_string(),
        tags: Vec::new(),
        emojis: Vec::new(),
        reactions: BTreeMap::new(),
        renote_count: 0,
        replies_count: 0,
        reaction_acceptance: None,
        uri: None,
        url: None,
        user_host: None,
        is_deleted: false,
        reply_parent_uri: None,
        quote: None,
        updated_at_ms: None,
        process_errors: None,
    }
}

#[test]
fn visibility_ordering() {
    assert!(Visibility::Public < Visibility::Home);
    assert!(Visibility::Home < Visibility::Followers);
    assert!(Visibility::Followers < Visibility::Specified);
}

#[test]
fn valid_note_passes_both_validators() {
    let n = valid_note();
    assert!(n.validate_api_create().is_ok());
    assert!(n.validate_storage().is_ok());
}

#[test]
fn text_bounds() {
    let mut n = valid_note();
    n.text = Some("t".repeat(3000));
    assert!(n.validate_api_create().is_ok());

    n.text = Some("t".repeat(3001));
    assert!(n.validate_api_create().is_err());
    assert!(n.validate_storage().is_err());

    // Blank text without renote/files is rejected; None without them too.
    n.text = Some("   ".to_string());
    assert!(n.validate_api_create().is_err());
    n.text = None;
    assert!(n.validate_api_create().is_err());

    // Blank text rides along on a renote; files stand in for text.
    n.text = Some("   ".to_string());
    n.renote_id = Some("nid00000000000002".to_string());
    assert!(n.validate_api_create().is_ok());
    n.renote_id = None;
    n.text = None;
    n.file_ids = vec!["mid00000000000001".to_string()];
    assert!(n.validate_api_create().is_ok());

    // Empty string text is never valid.
    n.text = Some(String::new());
    assert!(n.validate_api_create().is_err());
}

#[test]
fn poll_stands_in_for_text() {
    let mut n = valid_note();
    n.text = None;
    assert!(n.validate_api_create().is_err());
    assert!(n.validate_api_create_with_poll(true).is_ok());
    assert!(n.validate_storage_with_poll(true).is_ok());
    n.text = Some("   ".to_string());
    assert!(n.validate_api_create().is_err());
    assert!(n.validate_api_create_with_poll(true).is_ok());
}

#[test]
fn cw_bounds() {
    let mut n = valid_note();
    n.cw = Some("c".repeat(100));
    assert!(n.validate_api_create().is_ok());
    n.cw = Some("c".repeat(101));
    assert!(n.validate_api_create().is_err());
    // Storage carries up to 512.
    assert!(n.validate_storage().is_ok());
    n.cw = Some("c".repeat(512));
    assert!(n.validate_storage().is_ok());
    n.cw = Some("c".repeat(513));
    assert!(n.validate_storage().is_err());
    // Empty cw is rejected at API create.
    n.cw = Some(String::new());
    assert!(n.validate_api_create().is_err());
}

#[test]
fn specified_requires_visible_user_ids() {
    let mut n = valid_note();
    n.visibility = Visibility::Specified;
    assert!(n.validate_api_create().is_err());
    n.visible_user_ids = vec!["aid00000000000009".to_string()];
    assert!(n.validate_api_create().is_ok());
}

/// Slice B: `is_reply` covers resolved and pending replies, but a root
/// is neither.
#[test]
fn is_reply_covers_resolved_and_pending() {
    let root = valid_note();
    assert!(!root.is_reply());
    assert!(!root.has_pending_reply_parent());

    let mut resolved = valid_note();
    resolved.reply_id = Some("nid00000000000002".to_string());
    assert!(resolved.is_reply());
    assert!(!resolved.has_pending_reply_parent());

    let mut pending = valid_note();
    pending.reply_parent_uri = Some("https://remote.example/notes/1".to_string());
    assert!(pending.is_reply());
    assert!(pending.has_pending_reply_parent());
}

/// Slice B: a stored note is either linked or pending, never both.
#[test]
fn resolved_and_pending_parent_conflict() {
    let mut n = valid_note();
    n.reply_id = Some("nid00000000000002".to_string());
    n.reply_parent_uri = Some("https://remote.example/notes/1".to_string());
    assert!(n.validate_api_create().is_err());
    assert!(n.validate_storage().is_err());

    // Pending alone validates; overlong pending URIs do not.
    n.reply_id = None;
    assert!(n.validate_storage().is_ok());
    n.reply_parent_uri = Some(format!("https://remote.example/{}", "x".repeat(2048)));
    assert!(n.validate_storage().is_err());
}

#[test]
fn file_id_bounds() {
    let mut n = valid_note();
    n.text = None;
    n.file_ids = (0..16).map(|i| format!("mid{i:012}")).collect();
    assert!(n.validate_api_create().is_ok());
    n.file_ids.push("mid00000000000099".to_string());
    assert!(n.validate_api_create().is_err());

    n.file_ids = vec!["mid1".to_string(), "mid1".to_string()];
    assert!(n.validate_api_create().is_err());
}

#[test]
fn tag_emoji_uri_bounds() {
    let mut n = valid_note();
    n.tags = vec!["t".repeat(128)];
    n.emojis = vec!["e".repeat(128)];
    assert!(n.validate_api_create().is_ok());
    n.tags = vec!["t".repeat(129)];
    assert!(n.validate_api_create().is_err());
    n.tags = Vec::new();
    n.emojis = vec!["e".repeat(129)];
    assert!(n.validate_api_create().is_err());

    let mut n = valid_note();
    n.uri = Some("u".repeat(513));
    assert!(n.validate_api_create().is_err());
    n.uri = None;
    n.user_host = Some("h".repeat(129));
    assert!(n.validate_api_create().is_err());
}

#[test]
fn extract_helpers() {
    assert_eq!(
        extract_mentions("hello @alice and @bob_1!"),
        vec!["alice".to_string(), "bob_1".to_string()]
    );
    assert_eq!(extract_mentions("no mentions here"), Vec::<String>::new());
    assert_eq!(extract_mentions("mail a@b alone"), Vec::<String>::new());
    assert_eq!(
        extract_hashtags("#rust #hello_world done"),
        vec!["rust".to_string(), "hello_world".to_string()]
    );
    assert_eq!(
        extract_hashtags(&format!("#ok #{}", "t".repeat(129))),
        vec!["ok".to_string()]
    );
}

#[test]
fn edit_requires_live_owner_note() {
    let mut n = valid_note();
    assert!(can_edit_note(&n, "aid00000000000001").is_ok());
    assert!(can_edit_note(&n, "aid00000000000009").is_err());
    n.is_deleted = true;
    assert!(can_edit_note(&n, "aid00000000000001").is_err());
}

#[test]
fn delete_allows_owner_or_moderator_on_live_notes() {
    let mut n = valid_note();
    assert!(can_delete_note(&n, "aid00000000000001", false).is_ok());
    assert!(can_delete_note(&n, "aid00000000000009", true).is_ok());
    assert!(can_delete_note(&n, "aid00000000000009", false).is_err());
    n.is_deleted = true;
    assert!(can_delete_note(&n, "aid00000000000001", false).is_err());
    assert!(tombstone_transition(&valid_note()).is_ok());
    assert!(tombstone_transition(&n).is_err());
}

#[test]
fn sanitize_mfm_strips_dangerous_subset_only() {
    // Plain MFM passes through.
    let plain = "**bold** @alice #tag :emoji: https://example.local/x";
    assert_eq!(sanitize_mfm(plain), plain);
    // Script blocks drop (fail-closed on unterminated).
    assert_eq!(sanitize_mfm("a<script>alert(1)</script>b"), "ab");
    assert_eq!(sanitize_mfm("a<SCRIPT SRC=x>evil</SCRIPT>b"), "ab");
    assert_eq!(sanitize_mfm("a<script>alert(1)"), "a");
    // Event handlers drop; other attributes stay.
    assert_eq!(
        sanitize_mfm("<b title=\"hi\" onclick=\"evil()\">x</b>"),
        "<b title=\"hi\">x</b>"
    );
    // Dangerous schemes neutralize (any case).
    assert_eq!(
        sanitize_mfm("[x](JAVASCRIPT:alert(1))"),
        "[x](blocked:alert(1))"
    );
    assert_eq!(
        sanitize_mfm("![x](data:text/html,evil)"),
        "![x](blocked:,evil)"
    );
    // Null bytes never survive; sanitize is idempotent.
    let once = sanitize_mfm("a\0<script>x</script>b");
    assert!(!once.contains('\0'));
    assert_eq!(sanitize_mfm(&once), once);
}

#[test]
fn hashtag_trend_counts_and_ranks() {
    let mut a = valid_note();
    a.tags = vec!["Rust".to_string(), "rust".to_string(), "async".to_string()];
    let mut b = valid_note();
    b.tags = vec!["rust".to_string()];
    let mut deleted = valid_note();
    deleted.is_deleted = true;
    deleted.tags = vec!["rust".to_string()];
    let counts = aggregate_hashtag_counts([&a, &b, &deleted]);
    // Per-note dedup: `a` contributes one `rust` despite two spellings.
    assert_eq!(counts.get("rust"), Some(&2));
    assert_eq!(counts.get("async"), Some(&1));
    let ranked = rank_hashtag_trends(&counts, 1);
    assert_eq!(ranked, vec![("rust".to_string(), 2)]);
    let ranked_all = rank_hashtag_trends(&counts, 0);
    assert_eq!(
        ranked_all,
        vec![("rust".to_string(), 2), ("async".to_string(), 1)]
    );
}
