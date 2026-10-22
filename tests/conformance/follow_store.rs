use super::common::*;

// ---------------------------------------------------------------------------
// Group F: pending follow requests (Phase 2.6 slice 4, decision D-a)
// ---------------------------------------------------------------------------

fn counts<S: AccountStore>(store: &S, id: &str) -> (u64, u64) {
    let a = store.find_by_id(id).expect("find").expect("account");
    (a.followers_count as u64, a.following_count as u64)
}

/// F1: a pending request is not a follow edge — no `is_following`, not in
/// `followers_of`/`following_of`, counters untouched.
fn case_f1_request_is_not_an_edge<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "f1_alice");
    let bob = create_user(store, "f1_bob");
    let req = store
        .request_follow(&alice.id, &bob.id, true, None, vec![])
        .expect("request");
    assert_eq!(req.follower_id, alice.id);
    assert!(!store.is_following(&alice.id, &bob.id).unwrap());
    assert!(store.followers_of(&bob.id, None, 100).unwrap().is_empty());
    assert!(store.following_of(&alice.id, None, 100).unwrap().is_empty());
    assert_eq!(counts(store, &bob.id), (0, 0));
    assert_eq!(counts(store, &alice.id), (0, 0));
    assert!(
        store
            .get_follow_request(&alice.id, &bob.id)
            .unwrap()
            .is_some()
    );
    let listed = store.follow_requests_to(&bob.id, None, 100).unwrap();
    assert_eq!(listed.len(), 1);
    assert!(matches!(
        store.request_follow(&alice.id, &bob.id, true, None, vec![]),
        Err(PortError::Conflict(_))
    ));
}

#[test]
fn f1_request_is_not_an_edge() {
    let factory = RedbFactory::fresh("f1");
    let store = factory.open(test_clock());
    case_f1_request_is_not_an_edge(&*store);
}

/// F2: accept flips state atomically with its jobs; the edge and counters
/// appear, the request disappears.
fn case_f2_accept<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "f2_alice");
    let bob = create_user(store, "f2_bob");
    assert!(matches!(
        store.accept_follow_request(&alice.id, &bob.id, vec![]),
        Err(PortError::NotFound(_))
    ));
    store
        .request_follow(&alice.id, &bob.id, true, None, vec![])
        .expect("request");
    let before = store.pending_count().unwrap();
    store
        .accept_follow_request(&alice.id, &bob.id, vec![streaming_job(clock, "f2:accept")])
        .expect("accept");
    assert_eq!(
        store.pending_count().unwrap(),
        before + 1,
        "job committed with flip"
    );
    assert!(store.is_following(&alice.id, &bob.id).unwrap());
    assert_eq!(store.followers_of(&bob.id, None, 100).unwrap().len(), 1);
    assert_eq!(counts(store, &bob.id), (1, 0));
    assert_eq!(counts(store, &alice.id), (0, 1));
    assert!(
        store
            .get_follow_request(&alice.id, &bob.id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn f2_accept() {
    let factory = RedbFactory::fresh("f2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_f2_accept(&*store, &clock);
}

/// F3: reject/cancel (`remove_follow_request`) and `unfollow` both clean up a
/// pending request; an inbound-style `follow` (remote Accept) supersedes it.
fn case_f3_cleanup<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "f3_alice");
    let bob = create_user(store, "f3_bob");
    store
        .request_follow(&alice.id, &bob.id, false, None, vec![])
        .unwrap();
    store
        .remove_follow_request(&alice.id, &bob.id, vec![])
        .expect("remove");
    assert!(
        store
            .get_follow_request(&alice.id, &bob.id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .follow_requests_to(&bob.id, None, 10)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store.remove_follow_request(&alice.id, &bob.id, vec![]),
        Err(PortError::NotFound(_))
    ));
    // Undo(Follow) of a pending request.
    store
        .request_follow(&alice.id, &bob.id, false, None, vec![])
        .unwrap();
    store
        .unfollow(&alice.id, &bob.id, vec![])
        .expect("unfollow pending");
    assert!(
        store
            .get_follow_request(&alice.id, &bob.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(counts(store, &bob.id), (0, 0));
    assert!(matches!(
        store.unfollow(&alice.id, &bob.id, vec![]),
        Err(PortError::Validation(_))
    ));
    // Remote Accept path: a plain follow supersedes the request.
    store
        .request_follow(&alice.id, &bob.id, false, None, vec![])
        .unwrap();
    store
        .follow(&alice.id, &bob.id, false, None, vec![])
        .unwrap();
    assert!(
        store
            .get_follow_request(&alice.id, &bob.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(counts(store, &bob.id), (1, 0));
}

#[test]
fn f3_cleanup() {
    let factory = RedbFactory::fresh("f3");
    let store = factory.open(test_clock());
    case_f3_cleanup(&*store);
}

/// F4: inbound Follow to a locked account is deduplicated like any activity.
fn case_f4_request_activity_dedup<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "f4_alice");
    let bob = create_user(store, "f4_bob");
    let first = store
        .record_follow_request_activity("activity:f4:1", &alice.id, &bob.id, false, None, vec![])
        .unwrap();
    assert!(first.is_some());
    let dup = store
        .record_follow_request_activity("activity:f4:1", &alice.id, &bob.id, false, None, vec![])
        .unwrap();
    assert!(dup.is_none());
    assert!(!store.is_following(&alice.id, &bob.id).unwrap());
}

#[test]
fn f4_request_activity_dedup() {
    let factory = RedbFactory::fresh("f4");
    let store = factory.open(test_clock());
    case_f4_request_activity_dedup(&*store);
}

/// F5 (service level): locked followee — pending follower cannot read
/// followers-only notes and gets no fan-out edge; accept grants both; reject
/// and cancel clean up; notification kind is `receiveFollowRequest`.
#[test]
fn f5_locked_account_service_flow() {
    use pinetree::application::commands::follows::FollowOutcome;
    use pinetree::application::ports::UpdateAccountPatch;
    use pinetree::domain::notification::NotificationKind;
    let factory = RedbFactory::fresh("f5");
    let clock = test_clock();
    let (store, svc, _worker) = open_worker_stack(&factory, clock, Arc::new(NoopStreamingSink));
    let alice = create_user(&*store, "f5_alice");
    let bob = create_user(&*store, "f5_bob");
    let carol = create_user(&*store, "f5_carol");
    store
        .update_account(
            &bob.id,
            UpdateAccountPatch {
                is_locked: Some(true),
                ..Default::default()
            },
            Vec::new(),
        )
        .unwrap();
    let mut input = CreateNoteInput::simple_public(&bob.id, "followers only");
    input.visibility = Visibility::Followers;
    let note = svc.create_note(input).expect("note");

    assert!(matches!(
        svc.follow(&alice.id, &bob.id, true).unwrap(),
        FollowOutcome::Requested(_)
    ));
    assert!(matches!(
        svc.notes_show(&note.id, Some(&alice.id)),
        Err(PortError::NotFound(_))
    ));
    assert!(store.followers_of(&bob.id, None, 100).unwrap().is_empty());
    let kinds: Vec<_> = store.list_notifications(&bob.id, 10).unwrap();
    let _ = kinds; // notifications are written by the worker; asserted below
    _worker.drain(16).unwrap();
    let notifs = store.list_notifications(&bob.id, 10).unwrap();
    assert!(
        notifs
            .iter()
            .any(|n| n.kind == NotificationKind::ReceiveFollowRequest)
    );
    assert!(!notifs.iter().any(|n| n.kind == NotificationKind::Follow));

    svc.accept_follow_request(&bob.id, &alice.id)
        .expect("accept");
    assert!(svc.notes_show(&note.id, Some(&alice.id)).is_ok());
    assert_eq!(store.followers_of(&bob.id, None, 100).unwrap().len(), 1);

    // Reject and cancel.
    svc.follow(&carol.id, &bob.id, true).unwrap();
    svc.reject_follow_request(&bob.id, &carol.id)
        .expect("reject");
    assert!(svc.notes_show(&note.id, Some(&carol.id)).is_err());
    assert!(
        store
            .get_follow_request(&carol.id, &bob.id)
            .unwrap()
            .is_none()
    );
    svc.follow(&carol.id, &bob.id, true).unwrap();
    svc.cancel_follow_request(&carol.id, &bob.id)
        .expect("cancel");
    assert!(
        store
            .get_follow_request(&carol.id, &bob.id)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        svc.cancel_follow_request(&carol.id, &bob.id),
        Err(PortError::NotFound(_))
    ));
}
