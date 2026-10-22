use super::common::*;

// ---------------------------------------------------------------------------
// Group N — notification order (`docs/persistence-conformance.md`)
// ---------------------------------------------------------------------------

/// N1: `i/notifications` order is newest-first across kinds, and `sinceId`/`untilId`
/// paging walks that same order. Ids mirror production (`notify:{kind}:{suffix}`), so
/// a key order that starts with the kind would group rows by kind and fail here.
#[test]
fn n1_notifications_newest_first_across_kinds() {
    use pinetree::domain::notification::{Notification, NotificationKind};

    let factory = RedbFactory::fresh("n1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let me = remote_account(&*store, "n1_me");
    let who = remote_account(&*store, "n1_who");

    // Oldest first. Kinds interleave so a kind-grouped order is visibly wrong.
    let rows = [
        ("inbound-reply", NotificationKind::Reply),
        ("inbound-reaction", NotificationKind::Reaction),
        ("inbound-mention", NotificationKind::Mention),
        ("inbound-follow", NotificationKind::Follow),
        ("inbound-reply", NotificationKind::Reply),
        ("inbound-mention", NotificationKind::Mention),
    ];
    let mut ids = Vec::new();
    for (i, (label, kind)) in rows.iter().enumerate() {
        let id = format!("notify:{label}:{:016}", i + 1);
        store
            .insert_notification(Notification {
                clip_id: None,
                page_id: None,
                id: id.clone(),
                notifiee_id: me.id.clone(),
                notifier_id: who.id.clone(),
                kind: kind.clone(),
                note_id: None,
                reaction: None,
                draft_id: None,
                file_id: None,
                exported_entity: None,
                is_read: false,
            })
            .expect("insert notification");
        ids.push(id);
    }
    let newest_first: Vec<String> = ids.iter().rev().cloned().collect();

    let all = store
        .list_notifications_filtered(&me.id, &TimelineCursor::with_limit(20), &[], &[])
        .expect("filtered list");
    let got: Vec<String> = all.iter().map(|n| n.id.clone()).collect();
    assert_eq!(got, newest_first, "newest-first across kinds");

    let plain = store.list_notifications(&me.id, 20).expect("list");
    let got: Vec<String> = plain.iter().map(|n| n.id.clone()).collect();
    assert_eq!(got, newest_first, "unfiltered list uses the same order");

    // Page 2 via untilId = the third-newest id, limit 2 → the next two rows.
    let cursor = TimelineCursor {
        until_id: Some(newest_first[2].clone()),
        limit: 2,
        ..Default::default()
    };
    let page = store
        .list_notifications_filtered(&me.id, &cursor, &[], &[])
        .expect("paged list");
    let got: Vec<String> = page.iter().map(|n| n.id.clone()).collect();
    assert_eq!(
        got,
        newest_first[3..5].to_vec(),
        "untilId pages across kinds"
    );

    // sinceId = the third-newest id → only the two newer rows.
    let cursor = TimelineCursor {
        since_id: Some(newest_first[2].clone()),
        limit: 20,
        ..Default::default()
    };
    let newer = store
        .list_notifications_filtered(&me.id, &cursor, &[], &[])
        .expect("since list");
    let got: Vec<String> = newer.iter().map(|n| n.id.clone()).collect();
    assert_eq!(
        got,
        newest_first[..2].to_vec(),
        "sinceId returns only newer rows"
    );
}
