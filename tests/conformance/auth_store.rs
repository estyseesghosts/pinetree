use super::common::*;

/// V1 (Batch O WP9): signin history is newest-first, cursor-paged, and
/// bounded per account (oldest-first trim in the same transaction).
#[test]
fn v1_signin_history_bounded_and_paged() {
    let factory = RedbFactory::fresh("v1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let mut ids_made = Vec::new();
    for i in 0..(MAX_SIGNIN_ROWS_PER_USER + 5) {
        clock.advance_ms(1);
        let id = format!("s1row{:06}", i);
        ids_made.push(id.clone());
        store
            .record_signin(SigninRecord {
                id,
                account_id: "s1alice".to_string(),
                ip: "10.0.0.1".to_string(),
                headers: "{}".to_string(),
                success: i % 2 == 0,
                created_at_ms: clock.now_ms(),
            })
            .expect("record");
    }
    let rows = store
        .list_signins("s1alice", 200, None, None, None, None)
        .expect("list");
    assert_eq!(rows.len(), MAX_SIGNIN_ROWS_PER_USER, "trimmed to the cap");
    assert_eq!(
        rows[0].id,
        ids_made.last().unwrap().as_str(),
        "newest first"
    );
    assert!(
        rows.windows(2).all(|w| w[0].id > w[1].id),
        "strictly newest-first"
    );
    // A foreign account sees nothing.
    let rows = store
        .list_signins("s1bob", 100, None, None, None, None)
        .expect("list");
    assert!(rows.is_empty());
    // Cursor paging converges: untilId excludes the head row.
    let head = store
        .list_signins("s1alice", 100, None, None, None, None)
        .expect("list");
    let page = store
        .list_signins("s1alice", 1, None, Some(&head[0].id), None, None)
        .expect("page");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id, head[1].id);
    // Date cursors bound the window.
    let split = head[50].created_at_ms;
    let newer = store
        .list_signins("s1alice", 200, None, None, Some(split), None)
        .expect("since");
    assert!(newer.iter().all(|r| r.created_at_ms > split));
    let older = store
        .list_signins("s1alice", 200, None, None, None, Some(split))
        .expect("until");
    assert!(older.iter().all(|r| r.created_at_ms < split));
}

/// W1 (Batch P): password-reset rows are single-use and id-indexed.
/// Create → find by token → delete by id → both lookups miss; deleting a
/// missing id is a silent success (consume-on-expiry parity).
fn case_w1_reset_request_lifecycle<S: PasswordResetStore>(store: &S) {
    assert!(
        store
            .find_reset_request("tok-absent")
            .expect("find")
            .is_none()
    );
    store
        .create_reset_request(PasswordResetRequest {
            id: "rid00000000000001".to_string(),
            user_id: "alice".to_string(),
            token: "tok-alice-1".to_string(),
        })
        .expect("create");
    let found = store
        .find_reset_request("tok-alice-1")
        .expect("find")
        .expect("row present");
    assert_eq!(found.user_id, "alice");
    assert_eq!(found.id, "rid00000000000001");
    store
        .delete_reset_request("rid00000000000001")
        .expect("delete");
    assert!(
        store
            .find_reset_request("tok-alice-1")
            .expect("find")
            .is_none()
    );
    // Idempotent consume: missing ids are not errors.
    store
        .delete_reset_request("rid00000000000001")
        .expect("re-delete");
    store
        .delete_reset_request("rid-never-existed")
        .expect("missing");
}

#[test]
fn w1_reset_request_lifecycle() {
    let factory = RedbFactory::fresh("w1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_w1_reset_request_lifecycle(&*store);
}
