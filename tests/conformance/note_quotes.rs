use super::common::*;

// ---------------------------------------------------------------------------
// Group Q — quote posts (docs/quote-posts.md): domain record, authorization
// transitions, stamp persistence, dedup-atomic happy paths, crash
// consistency. Generic over ConformanceStore so Phase 4 runs these against
// FoundationDB unchanged.
// ---------------------------------------------------------------------------

fn quote_input<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
    author_id: &str,
    target: &Note,
    key: &str,
) -> Note {
    let mut input = CreateNoteInput::simple_public(author_id, "quoting commentary");
    input.renote_of = Some(target.id.clone());
    input.quote = Some(quote_pending("https://remote.example/notes/1"));
    store
        .create_note(input, vec![streaming_job(clock, key)])
        .expect("create quote note")
}

fn case_q1_quote_create_persists<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q1:target");

    // Pure renote (no text/CW/files, no quote record) stays a pure renote:
    // never misclassified as a quote (migration safety for old rows).
    let mut pure = CreateNoteInput::simple_public(&alice.id, "x");
    pure.text = None;
    pure.renote_of = Some(target.id.clone());
    let pure_note = store.create_note(pure, vec![]).expect("pure renote");
    assert!(
        pure_note.quote.is_none(),
        "pure renote must not gain a quote record"
    );
    assert!(!pure_note.is_quote());

    // Quote: relation + resolved local target id converge in storage.
    let quote = quote_input(store, clock, &alice.id, &target, "q1:quote");
    assert!(quote.is_quote());
    let q = quote.quote.as_ref().expect("quote record present");
    assert_eq!(q.target_uri, "https://remote.example/notes/1");
    assert_eq!(q.target_note_id.as_deref(), Some(target.id.as_str()));
    assert_eq!(q.authorization, QuoteAuthorizationState::Pending);

    // Reopen-equivalent read: the row round-trips with its quote intact.
    let reread = store
        .get_note(&quote.id)
        .expect("reread")
        .expect("quote found");
    assert_eq!(reread.quote, quote.quote);

    // Migration default: a row written before quotes existed (no `quote`
    // key at all) decodes as a non-quote, never a misclassified quote.
    let legacy_json = serde_json::json!({
        "id": "nid00000000000001", "author_id": alice.id, "text": "old",
        "cw": null, "visibility": "public", "visible_user_ids": [],
        "local_only": false, "reply_id": null, "renote_id": target.id,
        "reply_user_id": null, "file_ids": [], "mentions": [],
        "mentioned_remote_users": "[]", "tags": [], "emojis": [],
        "reactions": {}, "renote_count": 0, "replies_count": 0,
        "reaction_acceptance": null, "uri": null, "url": null,
        "user_host": null, "is_deleted": false,
    });
    let legacy: Note =
        serde_json::from_value(legacy_json).expect("legacy row without quote key decodes");
    assert!(legacy.quote.is_none(), "legacy rows default to non-quote");
}

#[test]
fn q1_quote_create_persists_and_pure_renote_stays_pure() {
    let factory = RedbFactory::fresh("q1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q1_quote_create_persists(&*store, &clock);
}

fn case_q2_quote_transitions<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q2:target");
    let quote = quote_input(store, clock, &alice.id, &target, "q2:quote");

    // Missing note and non-quote notes are NotFound, never Validation.
    assert!(matches!(
        store.set_quote_authorization(
            "nid-missing",
            QuoteAuthorizationState::Approved,
            None,
            0,
            vec![]
        ),
        Err(PortError::NotFound(_))
    ));
    assert!(matches!(
        store.set_quote_authorization(
            &target.id,
            QuoteAuthorizationState::Approved,
            None,
            0,
            vec![]
        ),
        Err(PortError::NotFound(_))
    ));

    // Approved requires a stamp: no false approval.
    assert!(is_validation(
        &store
            .set_quote_authorization(
                &quote.id,
                QuoteAuthorizationState::Approved,
                None,
                0,
                vec![]
            )
            .expect_err("approved without stamp must fail")
    ));

    // Pending -> Approved (with stamp) + jobs commit together.
    let approved = store
        .set_quote_authorization(
            &quote.id,
            QuoteAuthorizationState::Approved,
            Some("https://remote.example/stamps/1".to_string()),
            clock.now_ms(),
            vec![streaming_job(clock, "q2:approved-job")],
        )
        .expect("approve with stamp");
    assert_eq!(
        approved.quote.as_ref().unwrap().authorization,
        QuoteAuthorizationState::Approved
    );
    assert_eq!(
        approved
            .quote
            .as_ref()
            .unwrap()
            .authorization_uri
            .as_deref(),
        Some("https://remote.example/stamps/1")
    );
    let mut saw_approval_job = false;
    for _ in 0..8 {
        let Some(job) = store.claim_next(60_000).expect("claim") else {
            break;
        };
        if job.idempotency_key == "q2:approved-job" {
            saw_approval_job = true;
        }
        store.complete(&job.id).expect("complete");
    }
    assert!(
        saw_approval_job,
        "approval job committed with the transition"
    );

    // Same-state repeat is a no-op success (idempotent replay).
    let again = store
        .set_quote_authorization(
            &quote.id,
            QuoteAuthorizationState::Approved,
            None,
            0,
            vec![],
        )
        .expect("same-state repeat ok");
    assert_eq!(
        again.quote.as_ref().unwrap().authorization_uri.as_deref(),
        Some("https://remote.example/stamps/1"),
        "replay without a stamp URI retains the stored stamp"
    );

    // Illegal transitions are rejected: Approved -> Rejected, Approved -> Unknown.
    assert!(is_validation(
        &store
            .set_quote_authorization(
                &quote.id,
                QuoteAuthorizationState::Rejected,
                None,
                0,
                vec![]
            )
            .expect_err("approved->rejected is illegal")
    ));
    // Approved -> Revoked is the valid revocation path.
    let revoked = store
        .set_quote_authorization(&quote.id, QuoteAuthorizationState::Revoked, None, 0, vec![])
        .expect("approved->revoked");
    assert_eq!(
        revoked.quote.as_ref().unwrap().authorization,
        QuoteAuthorizationState::Revoked
    );
    // The dead stamp URI is retained so duplicate Deletes still match.
    assert_eq!(
        revoked.quote.as_ref().unwrap().authorization_uri.as_deref(),
        Some("https://remote.example/stamps/1")
    );
}

#[test]
fn q2_quote_authorization_transitions() {
    let factory = RedbFactory::fresh("q2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q2_quote_transitions(&*store, &clock);
}

fn case_q3_stamps_and_lookups<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q3:target");
    let quote = quote_input(store, clock, &alice.id, &target, "q3:quote");

    // Unknown lookups are None, never errors.
    assert!(
        store
            .find_note_by_quote_request("https://x.example/req-0")
            .expect("lookup")
            .is_none()
    );
    assert!(
        store
            .find_note_by_quote_stamp("https://x.example/stamp-0")
            .expect("lookup")
            .is_none()
    );
    assert!(
        store
            .get_quote_stamp("https://x.example/stamp-0")
            .expect("lookup")
            .is_none()
    );

    // Request lookup finds our pending quote.
    let found = store
        .find_note_by_quote_request("https://local.example/activities/req-1")
        .expect("lookup")
        .expect("quote found by request URI");
    assert_eq!(found.id, quote.id);

    // Stamp round-trip + stamp lookup after approval.
    let stamp = QuoteStamp {
        id: "https://local.example/users/bob/stamps/s1".to_string(),
        author_id: bob.id.clone(),
        interacting_object_uri: "https://remote.example/notes/q1".to_string(),
        interaction_target_note_id: target.id.clone(),
        request_activity_uri: "https://remote.example/activities/req-9".to_string(),
        revoked: false,
        created_at_ms: clock.now_ms(),
    };
    store
        .save_quote_stamp(stamp.clone(), vec![])
        .expect("save stamp");
    assert_eq!(
        store.get_quote_stamp(&stamp.id).expect("get").unwrap(),
        stamp
    );

    store
        .set_quote_authorization(
            &quote.id,
            QuoteAuthorizationState::Approved,
            Some(stamp.id.clone()),
            clock.now_ms(),
            vec![],
        )
        .expect("approve");
    let by_stamp = store
        .find_note_by_quote_stamp(&stamp.id)
        .expect("lookup")
        .expect("quote found by stamp URI");
    assert_eq!(by_stamp.id, quote.id);
}

#[test]
fn q3_quote_stamps_and_lookups() {
    let factory = RedbFactory::fresh("q3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q3_stamps_and_lookups(&*store, &clock);
}

fn case_q4_request_dedup<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q4:target");

    let stamp = QuoteStamp {
        id: "https://local.example/users/bob/stamps/s1".to_string(),
        author_id: bob.id.clone(),
        interacting_object_uri: "https://remote.example/notes/q1".to_string(),
        interaction_target_note_id: target.id.clone(),
        request_activity_uri: "https://remote.example/activities/req-1".to_string(),
        revoked: false,
        created_at_ms: clock.now_ms(),
    };
    // Accept path: stamp + Accept job commit atomically under the request id.
    let first = store
        .record_quote_request_accept(
            "https://remote.example/activities/req-1",
            stamp.clone(),
            vec![streaming_job(clock, "q4:accept-job")],
        )
        .expect("accept")
        .expect("first delivery applies");
    assert_eq!(first.id, stamp.id);
    // Replay: no duplicate stamp write, no duplicate job.
    let replay = store
        .record_quote_request_accept(
            "https://remote.example/activities/req-1",
            stamp,
            vec![streaming_job(clock, "q4:accept-job-dup")],
        )
        .expect("replay accepted");
    assert!(replay.is_none(), "replayed QuoteRequest applies nothing");
    assert_eq!(
        store.pending_count().expect("pending"),
        2,
        "target note job + exactly one accept job (no duplicate)"
    );

    // Reject path: true then false on replay, jobs likewise deduplicated.
    assert!(
        store
            .record_quote_request_reject(
                "https://remote.example/activities/req-2",
                vec![streaming_job(clock, "q4:reject-job")]
            )
            .expect("reject")
    );
    assert!(
        !store
            .record_quote_request_reject(
                "https://remote.example/activities/req-2",
                vec![streaming_job(clock, "q4:reject-job-dup")]
            )
            .expect("reject replay")
    );
}

#[test]
fn q4_quote_request_accept_reject_atomicity() {
    let factory = RedbFactory::fresh("q4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q4_request_dedup(&*store, &clock);
}

fn case_q5_decision_and_revocation<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "alice");
    let bob = create_user(store, "bob");
    let target = note_with_job(store, clock, &bob.id, "original", "q5:target");
    let quote = quote_input(store, clock, &alice.id, &target, "q5:quote");

    // Accept our request: Pending -> Approved with the verified stamp.
    let applied = store
        .record_quote_decision_accept(
            "https://remote.example/activities/accept-1",
            &quote.id,
            "https://remote.example/stamps/7",
            clock.now_ms(),
            vec![streaming_job(clock, "q5:accept-job")],
        )
        .expect("decision accept")
        .expect("first Accept applies");
    assert_eq!(
        applied.quote.as_ref().unwrap().authorization,
        QuoteAuthorizationState::Approved
    );
    assert!(
        store
            .record_quote_decision_accept(
                "https://remote.example/activities/accept-1",
                &quote.id,
                "https://remote.example/stamps/7",
                clock.now_ms(),
                vec![],
            )
            .expect("replay")
            .is_none()
    );

    // Second quote: Reject retains the relation + projection (renote + text
    // intact, state Rejected).
    let quote2 = quote_input(store, clock, &alice.id, &target, "q5:quote2");
    let rejected = store
        .record_quote_decision_reject(
            "https://remote.example/activities/reject-1",
            &quote2.id,
            vec![],
        )
        .expect("decision reject")
        .expect("Reject applies");
    assert_eq!(
        rejected.quote.as_ref().unwrap().authorization,
        QuoteAuthorizationState::Rejected
    );
    assert_eq!(rejected.renote_id.as_deref(), Some(target.id.as_str()));
    assert!(
        rejected.text.is_some(),
        "rejected quote keeps its commentary"
    );

    // Revocation: Approved -> Revoked with forwarding jobs committed together.
    // Seed a stamp row so revocation also flips the stamp's revoked flag.
    let seed = QuoteStamp {
        id: "https://remote.example/stamps/7".to_string(),
        author_id: bob.id.clone(),
        interacting_object_uri: "https://remote.example/notes/q5".to_string(),
        interaction_target_note_id: target.id.clone(),
        request_activity_uri: "https://remote.example/activities/req-q5".to_string(),
        revoked: false,
        created_at_ms: clock.now_ms(),
    };
    store.save_quote_stamp(seed, vec![]).expect("seed stamp");
    let revoked = store
        .record_quote_stamp_delete(
            "https://remote.example/activities/delete-stamp-1",
            &quote.id,
            vec![streaming_job(clock, "q5:revoke-job")],
        )
        .expect("stamp delete")
        .expect("revocation applies");
    assert_eq!(
        revoked.quote.as_ref().unwrap().authorization,
        QuoteAuthorizationState::Revoked
    );
    // The backing stamp row is revoked in the same transaction: it no longer
    // dereferences as valid.
    let stamp_row = store
        .get_quote_stamp("https://remote.example/stamps/7")
        .expect("get stamp")
        .expect("stamp row retained");
    assert!(
        stamp_row.revoked,
        "stamp row revoked with the note transition"
    );
    // Revocation is not terminal: a fresh stamp can re-approve.
    let reapproved = store
        .set_quote_authorization(
            &quote.id,
            QuoteAuthorizationState::Approved,
            Some("https://remote.example/stamps/8".to_string()),
            clock.now_ms(),
            vec![],
        )
        .expect("revoked->approved with fresh stamp");
    assert_eq!(
        reapproved.quote.as_ref().unwrap().authorization,
        QuoteAuthorizationState::Approved
    );
    assert!(
        store
            .record_quote_stamp_delete(
                "https://remote.example/activities/delete-stamp-1",
                &quote.id,
                vec![]
            )
            .expect("replay")
            .is_none()
    );
}

#[test]
fn q5_quote_decision_and_revocation() {
    let factory = RedbFactory::fresh("q5");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_q5_decision_and_revocation(&*store, &clock);
}

fn child_q6() -> ! {
    let (clock, store) = child_store();
    let target = list_all(&*store)
        .into_iter()
        .find(|n| !n.is_quote())
        .expect("target note exists");
    let quoter = store
        .find_by_handle("alice")
        .expect("find alice")
        .expect("alice exists");
    let mut input = CreateNoteInput::simple_public(&quoter.id, "crash quote");
    input.renote_of = Some(target.id.clone());
    input.quote = Some(quote_pending("https://remote.example/notes/9"));
    store
        .create_note(input, vec![streaming_job(&clock, "q6:job")])
        .expect("child create quote");
    drop(store);
    drop(clock);
    ready_and_park();
}

/// Q6: quote create-then-crash — note + quote state + outbox job survive a
/// SIGKILL right after commit (crash-recovery consistency).
#[test]
fn q6_quote_crash_consistency() {
    if child_mode_is("q6") {
        child_q6();
    }
    let _guard = lock_crash_harness();
    let factory = RedbFactory::fresh("q6");
    let (alice_id, target_id) = {
        let clock = test_clock();
        let store = factory.open(clock.clone());
        let alice = create_user(&*store, "alice");
        let bob = create_user(&*store, "bob");
        let target = note_with_job(&*store, &clock, &bob.id, "original", "q6:target");
        (alice.id.clone(), target.id.clone())
    };
    run_crash_child(
        "q6",
        "note_quotes::q6_quote_crash_consistency",
        factory.path(),
    );

    let clock2 = test_clock();
    let store = factory.reopen(clock2.clone());
    let quotes: Vec<Note> = list_all(&*store)
        .into_iter()
        .filter(|n| n.author_id == alice_id && n.is_quote())
        .collect();
    assert_eq!(quotes.len(), 1, "exactly one quote survived the crash");
    let quote = &quotes[0];
    assert_eq!(quote.renote_id.as_deref(), Some(target_id.as_str()));
    assert_eq!(
        quote.quote.as_ref().unwrap().authorization,
        QuoteAuthorizationState::Pending
    );
    let claimed: Vec<_> = (0..8)
        .filter_map(|_| store.claim_next(60_000).expect("claim"))
        .collect();
    assert!(
        claimed.iter().any(|j| j.idempotency_key == "q6:job"
            && String::from_utf8_lossy(&j.payload).contains(&quote.id)),
        "the quote's outbox job survived alongside it"
    );
}
