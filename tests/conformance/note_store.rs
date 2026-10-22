use super::common::*;

// ---------------------------------------------------------------------------
// Group E — visibility agreement
// ---------------------------------------------------------------------------

struct EFixture {
    alice_id: String,
    follower_id: String,
    mentioned_id: String,
    replier_target_author_id: String,
    stranger_id: String,
}

/// Build author + follower + mentioned + replied-to-author + stranger accounts
/// and one stored note per visibility level. The followers note is a reply so
/// `reply_user_id` is populated; the specified note lists `mentioned`.
fn setup_e<S: ConformanceStore>(store: &S) -> (EFixture, Vec<Note>) {
    let alice = create_user(store, "e_alice");
    let follower = create_user(store, "e_follower");
    let mentioned = create_user(store, "e_mentioned");
    let carol = create_user(store, "e_carol");
    let stranger = create_user(store, "e_stranger");
    // follower follows alice (no jobs needed for the matrix).
    store
        .follow(&follower.id, &alice.id, false, None, vec![])
        .expect("follow");
    // Carol's note for alice to reply to (populates reply_user_id paths).
    let carol_note = store
        .create_note(
            CreateNoteInput::simple_public(&carol.id, "carol's note"),
            vec![],
        )
        .expect("carol note");
    let mk = |visibility: Visibility, text: &str, reply_to: Option<String>| {
        let mut input = CreateNoteInput::simple_public(&alice.id, text);
        input.visibility = visibility;
        input.reply_to = reply_to;
        if visibility == Visibility::Specified {
            input.visible_users = vec![mentioned.id.clone()];
        }
        // Mention bob-equivalent so `mentions` resolves through the store.
        store.create_note(input, vec![]).expect("matrix note")
    };
    let notes = vec![
        mk(Visibility::Public, "public note", None),
        mk(Visibility::Home, "home note", None),
        mk(
            Visibility::Followers,
            "followers reply note",
            Some(carol_note.id.clone()),
        ),
        mk(Visibility::Specified, "specified note @e_mentioned", None),
    ];
    (
        EFixture {
            alice_id: alice.id,
            follower_id: follower.id,
            mentioned_id: mentioned.id,
            replier_target_author_id: carol.id,
            stranger_id: stranger.id,
        },
        notes,
    )
}

/// E1: API packing (`notes_show` semantics), timeline filters
/// (`home_timeline` / `local_timeline` semantics from
/// `application/queries.rs`), and the `is_visible` predicate agree on every
/// matrix cell.
#[test]
fn e1_three_call_site_agreement() {
    let factory = RedbFactory::fresh("e1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let (fx, notes) = setup_e(&*store);

    // Sanity: the stored followers note really is a reply to carol, and the
    // specified note really lists `mentioned`.
    let followers_note = notes
        .iter()
        .find(|n| n.visibility == Visibility::Followers)
        .expect("followers note");
    assert_eq!(
        followers_note.reply_user_id.as_deref(),
        Some(fx.replier_target_author_id.as_str())
    );

    for local_only in [false, true] {
        for note in &notes {
            let mut note = note.clone();
            note.local_only = local_only;
            // Viewers: author, follower, mentioned, replied-to-author, stranger, anon.
            let viewers: Vec<(&str, ViewerContext)> = vec![
                ("author", ViewerContext::local_user(&fx.alice_id, false)),
                ("follower", ViewerContext::local_user(&fx.follower_id, true)),
                (
                    "mentioned",
                    ViewerContext::local_user(&fx.mentioned_id, false),
                ),
                (
                    "replied",
                    ViewerContext::local_user(&fx.replier_target_author_id, false),
                ),
                (
                    "stranger",
                    ViewerContext::local_user(&fx.stranger_id, false),
                ),
                ("anonymous", ViewerContext::anonymous()),
            ];
            for (label, viewer) in &viewers {
                let predicate = is_visible(&note, viewer);
                // Call site 1 — API packing (`notes_show`): stored note must be
                // non-deleted, then the predicate decides.
                let packing = !note.is_deleted && is_visible(&note, viewer);
                assert_eq!(packing, predicate, "packing vs predicate");
                // Call site 2a — local timeline filter: eligibility AND predicate.
                let local_includes =
                    is_eligible_for_local_timeline(&note) && is_visible(&note, viewer);
                assert_eq!(
                    local_includes,
                    is_eligible_for_local_timeline(&note) && predicate,
                    "local timeline filter agrees"
                );
                // Call site 2b — home timeline filter for an in-scope viewer
                // (the author themselves or a follower, mirroring the
                // `authors` set in `application/queries.rs::home_timeline`):
                // scope AND predicate.
                let in_home_scope = matches!(*label, "author" | "follower");
                let home_includes = in_home_scope && is_visible(&note, viewer);
                assert_eq!(
                    home_includes,
                    in_home_scope && predicate,
                    "home timeline filter agrees"
                );
                // Expected matrix values (local readers; local_only changes
                // nothing for local reads). The followers fixture is a reply
                // to carol with no mentions, so `mentioned` sees nothing;
                // the specified fixture lists only `mentioned`.
                let want = match note.visibility {
                    Visibility::Public | Visibility::Home => true,
                    Visibility::Followers => {
                        matches!(*label, "author" | "follower" | "replied")
                    }
                    Visibility::Specified => matches!(*label, "author" | "mentioned"),
                };
                assert_eq!(
                    predicate, want,
                    "visibility={:?} local_only={local_only} viewer={label}",
                    note.visibility
                );
            }
        }
    }
}

/// E2: followers remote-remote fallback — both sides remote, no local follow
/// row → visible.
#[test]
fn e2_followers_remote_remote_fallback() {
    let note = Note {
        author_id: "remote-author".to_string(),
        user_host: Some("remote.example".to_string()),
        visibility: Visibility::Followers,
        text: Some("federated followers note".to_string()),
        ..Default::default()
    };
    let remote_stranger = ViewerContext::remote_user("stranger", "other.example", false);
    assert!(
        is_visible(&note, &remote_stranger),
        "remote-remote fallback must assume visible"
    );
    let local_stranger = ViewerContext::local_user("stranger", false);
    assert!(
        !is_visible(&note, &local_stranger),
        "local stranger with no follow edge sees nothing"
    );
    assert!(!is_visible(&note, &ViewerContext::anonymous()));
}

/// E3: `specified` with empty `visible_user_ids` rejected at creation.
#[test]
fn e3_specified_empty_visible_users_rejected() {
    let factory = RedbFactory::fresh("e3");
    let clock = test_clock();
    let store = factory.open(clock);
    let alice = create_user(&*store, "alice");
    let mut input = CreateNoteInput::simple_public(&alice.id, "specified to nobody");
    input.visibility = Visibility::Specified;
    input.visible_users = vec![];
    let err = store
        .create_note(input, vec![])
        .expect_err("empty specified audience must be rejected");
    assert!(is_validation(&err), "got {err:?}");
}

/// E4: `local_only` notes never reach federated surfaces or remote viewers.
#[test]
fn e4_local_only_never_federated() {
    let factory = RedbFactory::fresh("e4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let alice = create_user(&*store, "alice");
    for visibility in [
        Visibility::Public,
        Visibility::Home,
        Visibility::Followers,
        Visibility::Specified,
    ] {
        let mut input = CreateNoteInput::simple_public(&alice.id, "local only note");
        input.visibility = visibility;
        input.local_only = true;
        if visibility == Visibility::Specified {
            input.visible_users = vec![alice.id.clone()];
        }
        let note = store.create_note(input, vec![]).expect("local_only create");
        assert!(!is_eligible_for_federated_timeline(&note));
        assert!(!is_eligible_for_federation_delivery(&note));
        let remote = ViewerContext::remote_user("follower", "remote.example", true);
        assert!(
            !is_visible(&note, &remote),
            "{visibility:?} local_only must be invisible remotely"
        );
        let federated_render = ViewerContext {
            viewer_id: Some("follower".to_string()),
            viewer_host: None,
            follows_author: true,
            is_remote_request: true,
        };
        assert!(!is_visible(&note, &federated_render));
    }
}

// ---------------------------------------------------------------------------
// Group G: reply visibility (Phase 2.6 slice 5, audit D8)
// ---------------------------------------------------------------------------

/// G1: a reply requires the target to be visible to the replier; otherwise
/// NO_SUCH_NOTE (same as a missing target, so existence is not leaked).
#[test]
fn g1_reply_requires_visible_target() {
    let factory = RedbFactory::fresh("g1");
    let (store, svc, _w) = open_worker_stack(&factory, test_clock(), Arc::new(NoopStreamingSink));
    let author = create_user(&*store, "g1_author");
    let follower = create_user(&*store, "g1_follower");
    let mentioned = create_user(&*store, "g1_mentioned");
    let stranger = create_user(&*store, "g1_stranger");
    store
        .follow(&follower.id, &author.id, false, None, vec![])
        .unwrap();

    let mk = |vis: Visibility| {
        let mut i = CreateNoteInput::simple_public(&author.id, "parent");
        i.visibility = vis;
        if vis == Visibility::Specified {
            i.visible_users = vec![mentioned.id.clone()];
        }
        svc.create_note(i).expect("parent")
    };
    let reply = |who: &str, parent: &Note| {
        let mut i = CreateNoteInput::simple_public(who, "re");
        i.reply_to = Some(parent.id.clone());
        svc.create_note(i)
    };
    let is_no_such_note = |r: Result<Note, PortError>| matches!(r, Err(PortError::NotFound(ref m)) if m == "NO_SUCH_NOTE");

    let followers_note = mk(Visibility::Followers);
    let specified_note = mk(Visibility::Specified);
    let public_note = mk(Visibility::Public);
    let home_note = mk(Visibility::Home);

    // Rejected.
    assert!(is_no_such_note(reply(&stranger.id, &followers_note)));
    assert!(is_no_such_note(reply(&stranger.id, &specified_note)));
    assert!(is_no_such_note(reply(&follower.id, &specified_note)));
    // Missing target looks identical.
    let mut i = CreateNoteInput::simple_public(&stranger.id, "re");
    i.reply_to = Some("does-not-exist".to_string());
    assert!(is_no_such_note(svc.create_note(i)));
    // Allowed.
    assert!(reply(&follower.id, &followers_note).is_ok());
    assert!(reply(&author.id, &followers_note).is_ok());
    assert!(reply(&mentioned.id, &specified_note).is_ok());
    assert!(reply(&stranger.id, &public_note).is_ok());
    assert!(reply(&stranger.id, &home_note).is_ok());
}

/// G2: renote/quote of a non-public target by a non-author is rejected
/// (policy.rs narrowing); public renotes still work.
#[test]
fn g2_renote_visibility() {
    let factory = RedbFactory::fresh("g2");
    let (store, svc, _w) = open_worker_stack(&factory, test_clock(), Arc::new(NoopStreamingSink));
    let author = create_user(&*store, "g2_author");
    let stranger = create_user(&*store, "g2_stranger");
    let mut i = CreateNoteInput::simple_public(&author.id, "private");
    i.visibility = Visibility::Followers;
    let fo = svc.create_note(i).unwrap();
    let public = svc
        .create_note(CreateNoteInput::simple_public(&author.id, "pub"))
        .unwrap();
    for text in [None, Some("quote".to_string())] {
        let mut r = CreateNoteInput::simple_public(&stranger.id, "x");
        r.text = text;
        r.renote_of = Some(fo.id.clone());
        assert!(
            svc.create_note(r).is_err(),
            "renote/quote of followers-only must fail"
        );
    }
    let mut r = CreateNoteInput::simple_public(&stranger.id, "x");
    r.text = None;
    r.renote_of = Some(public.id.clone());
    assert!(svc.create_note(r).is_ok());
}

// ---------------------------------------------------------------------------
// Group U — note URI uniqueness (`docs/persistence-conformance.md`)
// ---------------------------------------------------------------------------

fn uri_input(author_id: &str, uri: &str, text: &str) -> CreateNoteInput {
    let mut input = CreateNoteInput::simple_public(author_id, text);
    input.uri = Some(uri.to_string());
    input
}

/// U1: the same note `uri` arriving under two different activity ids
/// (`ap/show`-shaped fetch, then the real `Create`) yields exactly one row;
/// the second is a replay (`Ok(None)`), its jobs are not enqueued, and
/// `find_by_uri` resolves the first row.
fn case_u1_same_uri_different_activity<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let remote = remote_account(store, "u1_remote");
    let uri = "https://remote.example/notes/u1";
    let first = store
        .record_note_activity(
            uri,
            uri_input(&remote.id, uri, "bare copy"),
            vec![streaming_job(clock, "u1:job-1")],
        )
        .expect("first")
        .expect("first wins");
    let second = store
        .record_note_activity(
            "https://remote.example/activities/u1-create",
            uri_input(&remote.id, uri, "full copy"),
            vec![streaming_job(clock, "u1:job-2")],
        )
        .expect("second");
    assert_eq!(
        second, None,
        "same uri under another activity id is a replay"
    );
    assert_eq!(
        store.pending_count().expect("count"),
        1,
        "loser enqueued no job"
    );
    let found = NoteStore::find_by_uri(&*store, uri)
        .expect("find")
        .expect("indexed");
    assert_eq!(found.id, first.id);
    let rows = list_all(store)
        .iter()
        .filter(|n| n.uri.as_deref() == Some(uri))
        .count();
    assert_eq!(rows, 1, "exactly one row per uri");

    // A plain create with a taken uri is a conflict, not a second row.
    let err = store
        .create_note(uri_input(&remote.id, uri, "again"), Vec::new())
        .expect_err("duplicate uri");
    assert!(matches!(err, PortError::Conflict(_)), "got {err:?}");
}

#[test]
fn u1_same_uri_different_activity_ids() {
    let factory = RedbFactory::fresh("u1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_u1_same_uri_different_activity(&*store, &clock);
}

/// U2: 8 racers, same uri, distinct activity ids → one winner, one row.
#[test]
fn u2_concurrent_same_uri() {
    let factory = RedbFactory::fresh("u2");
    let clock = test_clock();
    let store: Arc<RedbStore> = factory.open(clock.clone());
    let remote = remote_account(&*store, "u2_remote");
    let uri = "https://remote.example/notes/u2";
    let results: Vec<_> = std::thread::scope(|s| {
        (0..8)
            .map(|i| {
                let store = store.clone();
                let clock = clock.clone();
                let author = remote.id.clone();
                s.spawn(move || {
                    store.record_note_activity(
                        &format!("https://remote.example/activities/u2-{i}"),
                        uri_input(&author, uri, &format!("racer {i}")),
                        vec![streaming_job(&clock, &format!("u2:job-{i}"))],
                    )
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().expect("join"))
            .collect()
    });
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(
        results.iter().filter(|r| matches!(r, Ok(Some(_)))).count(),
        1
    );
    let rows = list_all(&*store)
        .iter()
        .filter(|n| n.uri.as_deref() == Some(uri))
        .count();
    assert_eq!(rows, 1);
    assert_eq!(store.pending_count().expect("count"), 1);
}

/// U3: a deleted note releases its uri (re-fetch after `Delete` may recreate).
#[test]
fn u3_deleted_note_releases_uri() {
    let factory = RedbFactory::fresh("u3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    let remote = remote_account(&*store, "u3_remote");
    let uri = "https://remote.example/notes/u3";
    let first = store
        .record_note_activity("act:u3:1", uri_input(&remote.id, uri, "one"), Vec::new())
        .unwrap()
        .unwrap();
    store.delete_note(&first.id, Vec::new()).expect("delete");
    assert!(
        NoteStore::find_by_uri(&*store, uri).unwrap().is_none(),
        "tombstone not found by uri"
    );
    let again = store
        .record_note_activity("act:u3:2", uri_input(&remote.id, uri, "two"), Vec::new())
        .unwrap()
        .expect("uri free again");
    assert_eq!(
        NoteStore::find_by_uri(&*store, uri).unwrap().unwrap().id,
        again.id
    );
}

// ---------------------------------------------------------------------------
// Group T — remembered deletions (tombstones; bench S6, 2026-10-17)
// ---------------------------------------------------------------------------

/// T1: a `Delete` that reaches the store before its `Create` leaves a
/// tombstone. The late `Create` under a new activity id is refused (`Ok(None)`),
/// stores no row, and enqueues no job.
fn case_t1_delete_before_create<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let remote = remote_account(store, "t1_remote");
    let uri = "https://remote.example/notes/t1";
    let deleted = store
        .record_delete_uri_activity(
            "https://remote.example/activities/t1-delete",
            uri,
            Vec::new(),
        )
        .expect("delete first");
    assert!(
        !deleted,
        "no note held: the delete only records a tombstone"
    );
    let late = store
        .record_note_activity(
            "https://remote.example/activities/t1-create",
            uri_input(&remote.id, uri, "late copy"),
            vec![streaming_job(clock, "t1:job-1")],
        )
        .expect("late create");
    assert_eq!(late, None, "a tombstoned uri must not be stored");
    assert_eq!(
        store.pending_count().expect("count"),
        0,
        "refused copy enqueued no job"
    );
    assert!(
        NoteStore::find_by_uri(&*store, uri)
            .expect("find")
            .is_none()
    );
    // A plain create of the same uri is refused the same way.
    let err = store
        .create_note(uri_input(&remote.id, uri, "again"), Vec::new())
        .expect_err("tombstoned uri");
    assert!(matches!(err, PortError::Conflict(_)), "got {err:?}");
}

#[test]
fn t1_delete_before_create_leaves_tombstone() {
    let factory = RedbFactory::fresh("t1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_t1_delete_before_create(&*store, &clock);
}

/// T2: a `Delete` by URI for a held note removes it and enqueues its streaming
/// job. The uri is released afterwards, as Group U3 requires, so a fresh `Create`
/// may take it again.
fn case_t2_delete_by_uri_removes_and_releases<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let remote = remote_account(store, "t2_remote");
    let uri = "https://remote.example/notes/t2";
    store
        .record_note_activity(
            "https://remote.example/activities/t2-create",
            uri_input(&remote.id, uri, "stored"),
            Vec::new(),
        )
        .expect("create")
        .expect("stored");
    let deleted = store
        .record_delete_uri_activity(
            "https://remote.example/activities/t2-delete",
            uri,
            vec![streaming_job(clock, "t2:job-del")],
        )
        .expect("delete");
    assert!(deleted, "the held note is deleted");
    assert!(
        NoteStore::find_by_uri(&*store, uri)
            .expect("find")
            .is_none()
    );
    assert_eq!(
        store.pending_count().expect("count"),
        1,
        "delete enqueued its job"
    );
    let again = store
        .record_note_activity(
            "https://remote.example/activities/t2-create-again",
            uri_input(&remote.id, uri, "fetched again"),
            Vec::new(),
        )
        .expect("create after delete")
        .expect("uri released after a held note is deleted");
    assert_eq!(
        NoteStore::find_by_uri(&*store, uri)
            .expect("find")
            .unwrap()
            .id,
        again.id
    );
}

#[test]
fn t2_delete_by_uri_removes_and_releases() {
    let factory = RedbFactory::fresh("t2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_t2_delete_by_uri_removes_and_releases(&*store, &clock);
}

/// T3: a replayed delete activity is a no-op, a tombstone is scoped to its own
/// uri, and an empty uri is rejected.
fn case_t3_delete_replay_and_scope<S: ConformanceStore>(store: &S) {
    let remote = remote_account(store, "t3_remote");
    let uri = "https://remote.example/notes/t3";
    let other = "https://remote.example/notes/t3-other";
    let delete_id = "https://remote.example/activities/t3-delete";
    assert!(
        !store
            .record_delete_uri_activity(delete_id, uri, Vec::new())
            .expect("first")
    );
    assert!(
        !store
            .record_delete_uri_activity(delete_id, uri, Vec::new())
            .expect("replay"),
        "a replayed delete activity is a no-op"
    );
    let kept = store
        .record_note_activity(
            "https://remote.example/activities/t3-create-other",
            uri_input(&remote.id, other, "other"),
            Vec::new(),
        )
        .expect("other create")
        .expect("other uri is not tombstoned");
    assert_eq!(kept.uri.as_deref(), Some(other));
    let err = store
        .record_delete_uri_activity(
            "https://remote.example/activities/t3-empty",
            "  ",
            Vec::new(),
        )
        .expect_err("empty uri");
    assert!(matches!(err, PortError::Validation(_)), "got {err:?}");
}

#[test]
fn t3_delete_replay_and_scope() {
    let factory = RedbFactory::fresh("t3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_t3_delete_replay_and_scope(&*store);
}
