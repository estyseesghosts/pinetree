use super::common::*;

/// J2 (Batch N WS1): the local shortcode is unique case-insensitively
/// (account-handle rule, C2). A second create with different case fails
/// `Conflict(DUPLICATE_NAME)`; a rename onto a taken name fails
/// `Conflict(SAME_NAME_EMOJI_EXISTS)`; delete frees the name. Remote rows
/// are unique per (host, lower shortcode) and upserts never clear the
/// cached `media_id`.
fn case_j2_emoji_uniqueness<S: ConformanceStore>(store: &S) {
    use pinetree::domain::emoji::{CustomEmoji, RemoteEmoji};
    use pinetree::domain::moderation::{ModerationAction, ModerationLogEntry};
    let log = |id: &str| {
        ModerationLogEntry::new(
            id.to_string(),
            Some("mod".to_string()),
            ModerationAction::AddCustomEmoji,
            None,
            serde_json::json!({}),
            1_700_000_000_000,
        )
        .unwrap()
    };
    let emoji = |id: &str, shortcode: &str| CustomEmoji {
        id: id.to_string(),
        shortcode: shortcode.to_string(),
        category: None,
        media_id: None,
        aliases: Vec::new(),
        license: None,
        is_sensitive: false,
        local_only: false,
        role_ids: Vec::new(),
        created_at_ms: 1_700_000_000_000,
    };
    store
        .create_emoji(emoji("e1", "BlobCat"), log("l1"), Vec::new())
        .expect("first emoji lands");
    let err = store
        .create_emoji(emoji("e2", "blobcat"), log("l2"), Vec::new())
        .expect_err("case-insensitive duplicate conflicts");
    assert!(
        matches!(err, PortError::Conflict(ref code) if code == "DUPLICATE_NAME"),
        "got {err:?}"
    );
    // Same name + same case also conflicts.
    let err = store
        .create_emoji(emoji("e3", "BlobCat"), log("l3"), Vec::new())
        .expect_err("exact duplicate conflicts");
    assert!(
        matches!(err, PortError::Conflict(ref code) if code == "DUPLICATE_NAME"),
        "got {err:?}"
    );
    // Lookup is case-insensitive too.
    assert!(
        store
            .get_emoji_by_name("BLOBCAT")
            .expect("lookup")
            .is_some()
    );
    // A second emoji renames onto the taken name → SAME_NAME_EMOJI_EXISTS.
    store
        .create_emoji(emoji("e4", "free"), log("l4"), Vec::new())
        .expect("second emoji lands");
    let mut renamed = emoji("e4", "BLOBCAT");
    renamed.category = Some("c".to_string());
    let err = store
        .update_emoji(renamed, log("l5"), Vec::new())
        .expect_err("rename onto taken name conflicts");
    assert!(
        matches!(err, PortError::Conflict(ref code) if code == "SAME_NAME_EMOJI_EXISTS"),
        "got {err:?}"
    );
    // Delete frees the name for reuse.
    assert!(
        store
            .delete_emoji("e1", log("l6"), Vec::new())
            .expect("delete")
    );
    store
        .create_emoji(emoji("e5", "blobcat"), log("l7"), Vec::new())
        .expect("name reusable after delete");

    // Remote rows: unique per (host, lower shortcode), case-insensitive.
    let remote = RemoteEmoji::new("R.Example", "Party", "https://r.example/p.png", 1).unwrap();
    store.upsert_remote_emoji(remote).expect("remote lands");
    let again = RemoteEmoji::new("r.example", "party", "https://r.example/p2.png", 2).unwrap();
    let merged = store.upsert_remote_emoji(again).expect("re-upsert");
    assert_eq!(merged.remote_url, "https://r.example/p2.png");
    assert_eq!(
        merged.first_seen_ms, 1,
        "first-seen is stable across upserts"
    );
    // A different host is a different row.
    let other = RemoteEmoji::new("other.example", "party", "https://o.example/p.png", 3).unwrap();
    store.upsert_remote_emoji(other).expect("other host lands");
    // Caching a local copy survives later upserts.
    store
        .set_remote_emoji_media("r.example", "party", "mid1")
        .expect("cache media");
    let recached = RemoteEmoji::new("r.example", "PARTY", "https://r.example/p3.png", 4).unwrap();
    let merged = store
        .upsert_remote_emoji(recached)
        .expect("upsert keeps cache");
    assert_eq!(merged.media_id.as_deref(), Some("mid1"));
    assert_eq!(merged.remote_url, "https://r.example/p3.png");
}

#[test]
fn j2_emoji_uniqueness() {
    let factory = RedbFactory::fresh("j2");
    let store = factory.open(test_clock());
    case_j2_emoji_uniqueness(&*store);
}

/// J3 (emoji completion; Group J — custom emoji): `list_remote_emojis`
/// pages remote rows newest-first by `first_seen_ms`, filters by host
/// (case-insensitive) and shortcode query (plain or `:name:` tokens), and
/// honors `since_date`/`until_date` + `limit`. `since_id`/`until_id` are
/// accepted and ignored (remote rows have no time-ordered ids).
fn case_j3_remote_emoji_list<S: ConformanceStore>(store: &S) {
    use pinetree::domain::emoji::RemoteEmoji;
    let cursor = |limit: usize| TimelineCursor {
        limit,
        ..Default::default()
    };
    for (host, name, seen) in [
        ("r.example", "party", 10),
        ("r.example", "blobcat", 20),
        ("other.example", "party", 30),
    ] {
        store
            .upsert_remote_emoji(
                RemoteEmoji::new(host, name, &format!("https://{host}/{name}.png"), seen).unwrap(),
            )
            .expect("remote lands");
    }
    // No filters: newest-first by first_seen_ms.
    let all = store
        .list_remote_emojis(None, None, &cursor(10))
        .expect("list");
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].shortcode, "party");
    assert_eq!(all[0].host, "other.example");
    assert_eq!(all[2].host, "r.example");
    assert_eq!(all[2].shortcode, "party");
    // Host filter is case-insensitive.
    let mine = store
        .list_remote_emojis(None, Some("R.EXAMPLE"), &cursor(10))
        .expect("host filter");
    assert_eq!(mine.len(), 2);
    assert!(mine.iter().all(|e| e.host == "r.example"));
    // Query matches the shortcode (plain or `:name:` token).
    let q = store
        .list_remote_emojis(Some("blob"), None, &cursor(10))
        .expect("query");
    assert_eq!(q.len(), 1);
    assert_eq!(q[0].shortcode, "blobcat");
    let tok = store
        .list_remote_emojis(Some(":party:"), None, &cursor(10))
        .expect("token");
    assert_eq!(tok.len(), 2);
    // Dates bound first_seen_ms; limit truncates.
    let dated = store
        .list_remote_emojis(
            None,
            None,
            &TimelineCursor {
                since_date: Some(15),
                until_date: Some(25),
                ..cursor(10)
            },
        )
        .expect("dates");
    assert_eq!(dated.len(), 1);
    assert_eq!(dated[0].shortcode, "blobcat");
    let capped = store
        .list_remote_emojis(None, None, &cursor(2))
        .expect("limit");
    assert_eq!(capped.len(), 2);
    // Unknown host matches nothing.
    let none = store
        .list_remote_emojis(None, Some("nope.example"), &cursor(10))
        .expect("unknown host");
    assert!(none.is_empty());
}

#[test]
fn j3_remote_emoji_list() {
    let factory = RedbFactory::fresh("j3");
    let store = factory.open(test_clock());
    case_j3_remote_emoji_list(&*store);
}
