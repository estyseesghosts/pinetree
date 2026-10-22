use super::common::*;

/// O1 (Batch N WS4; Group O — antennas): per-antenna feeds append
/// newest-first, trim to the newest 200 in the same transaction, drop
/// single rows idempotently, and cascade on antenna delete.
fn case_o1_antenna_feed_trim<S: ConformanceStore>(store: &S) {
    use pinetree::domain::antenna::{Antenna, AntennaSrc};
    let owner = create_user(store, "k1_owner");
    let antenna = Antenna {
        id: "ant00000000000001".to_string(),
        owner_id: owner.id.clone(),
        name: "k".to_string(),
        src: AntennaSrc::All,
        user_list_id: None,
        // Non-empty: the store enforces the upstream EMPTY_KEYWORD rule.
        keywords: vec![vec!["x".to_string()]],
        exclude_keywords: Vec::new(),
        users: Vec::new(),
        case_sensitive: false,
        with_replies: true,
        with_file: false,
        local_only: false,
        exclude_bots: false,
        exclude_notes_in_sensitive_channel: false,
        is_active: true,
        last_used_at_ms: 0,
        created_at_ms: 0,
    };
    store.create_antenna(antenna, Vec::new()).expect("create");
    // Owner cap accounting sees the row.
    assert_eq!(store.antennas_by_owner(&owner.id).expect("list").len(), 1);
    // 205 sequential note ids (aidx-ordered): the feed keeps the newest 200.
    for i in 0..205 {
        store
            .append_antenna_note("ant00000000000001", &format!("nid{i:020}"))
            .expect("append");
    }
    // Reads page at most 100 (upstream `notes.ts` parity): two pages cover
    // the trimmed 200, newest-first, oldest kept is #5.
    let page = |limit: usize, until_id: Option<String>| {
        store
            .antenna_notes(
                "ant00000000000001",
                &pinetree::application::ports::TimelineCursor {
                    limit,
                    until_id,
                    ..Default::default()
                },
            )
            .expect("read")
    };
    let page1 = page(100, None);
    assert_eq!(page1.len(), 100);
    assert_eq!(page1[0], "nid00000000000000000204", "newest first");
    let page2 = page(100, page1.last().cloned());
    assert_eq!(page2.len(), 100);
    assert_eq!(page2[99], "nid00000000000000000005", "oldest kept is #5");
    assert!(
        !page1.contains(&"nid00000000000000000004".to_string())
            && !page2.contains(&"nid00000000000000000004".to_string()),
        "overflow head trimmed"
    );
    // Single-row removal is idempotent.
    store
        .remove_antenna_note("ant00000000000001", "nid00000000000000000204")
        .expect("remove");
    store
        .remove_antenna_note("ant00000000000001", "nid00000000000000000204")
        .expect("second remove converges");
    let ids = page(100, None);
    assert_eq!(ids.len(), 100, "a full page still reads after one removal");
    assert_eq!(ids[0], "nid00000000000000000203");
    // Delete cascades the feed rows.
    assert!(
        store
            .delete_antenna(&owner.id, "ant00000000000001")
            .expect("delete")
    );
    assert!(
        !store
            .delete_antenna(&owner.id, "ant00000000000001")
            .expect("second delete")
    );
    let ids = page(100, None);
    assert!(ids.is_empty(), "cascade cleared the feed");
}

#[test]
fn o1_antenna_feed_trim_and_cascade() {
    let factory = RedbFactory::fresh("o1");
    let store = factory.open(test_clock());
    case_o1_antenna_feed_trim(&*store);
}
