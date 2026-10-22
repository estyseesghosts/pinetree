use super::common::*;

// ---------------------------------------------------------------------------
// Group S — drafts + scheduled arming (§5, `docs/misskey-parity-plan.md`)
// ---------------------------------------------------------------------------

fn draft_row(id: &str, user_id: &str, scheduled_at_ms: Option<i64>) -> NoteDraft {
    NoteDraft {
        id: id.to_string(),
        user_id: user_id.to_string(),
        text: Some("draft".to_string()),
        cw: None,
        visibility: Visibility::Public,
        visible_user_ids: Vec::new(),
        local_only: false,
        reply_id: None,
        renote_id: None,
        file_ids: Vec::new(),
        hashtag: None,
        reaction_acceptance: None,
        has_poll: false,
        poll_choices: Vec::new(),
        poll_multiple: false,
        poll_expires_at_ms: None,
        poll_expired_after_ms: None,
        scheduled_at_ms,
        is_actually_scheduled: scheduled_at_ms.is_some(),
    }
}

fn scheduled_draft_job(key: &str, draft_id: &str, at_ms: i64) -> NewJob {
    NewJob::new_json(
        JobKind::PostScheduledDraft,
        &ScheduledDraftPayload {
            draft_id: draft_id.to_string(),
            scheduled_at_ms: at_ms,
        },
        key,
        at_ms,
    )
    .expect("scheduled fixture is a valid payload")
}

fn draft_cursor(limit: usize) -> TimelineCursor {
    TimelineCursor {
        since_id: None,
        until_id: None,
        since_date: None,
        until_date: None,
        limit,
    }
}

/// S1: draft CRUD through the ports — create/get/update/delete, counts,
/// newest-first paging, and the `scheduled` tri-state filter.
fn case_s1_draft_crud<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "s1_alice");
    assert_eq!(store.count_drafts(&alice.id).unwrap(), 0);
    assert_eq!(store.count_scheduled_drafts(&alice.id).unwrap(), 0);
    let d1 = store
        .create_draft(draft_row("s1_d1", &alice.id, None), vec![])
        .unwrap();
    assert_eq!(d1.user_id, alice.id);
    let d2 = store
        .create_draft(
            draft_row("s1_d2", &alice.id, Some(BASE_MS + 60_000)),
            vec![],
        )
        .unwrap();
    assert_eq!(store.count_drafts(&alice.id).unwrap(), 2);
    assert_eq!(store.count_scheduled_drafts(&alice.id).unwrap(), 1);

    // Newest-first; `scheduled` tri-state.
    let all = store
        .drafts_by_user(&alice.id, &draft_cursor(10), None)
        .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].id, d2.id, "newest first");
    assert_eq!(
        store
            .drafts_by_user(&alice.id, &draft_cursor(10), Some(true))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .drafts_by_user(&alice.id, &draft_cursor(10), Some(false))
            .unwrap()
            .len(),
        1
    );

    // Update replaces the row; missing rows are NotFound.
    let mut merged = d1.clone();
    merged.text = Some("edited".to_string());
    let updated = store.update_draft(merged, vec![]).unwrap();
    assert_eq!(updated.text.as_deref(), Some("edited"));
    assert!(matches!(
        store.update_draft(draft_row("s1_nope", &alice.id, None), vec![]),
        Err(PortError::NotFound(msg)) if msg == "NO_SUCH_NOTE_DRAFT"
    ));

    // Delete drops the row + index; missing rows are a no-op success.
    let before = store.pending_count().unwrap();
    store
        .delete_draft(&d1.id, vec![streaming_job(&test_clock(), "s1-del-job")])
        .unwrap();
    assert!(store.get_draft(&d1.id).unwrap().is_none());
    assert_eq!(store.count_drafts(&alice.id).unwrap(), 1);
    assert_eq!(store.pending_count().unwrap(), before + 1);
    store.delete_draft(&d1.id, vec![]).unwrap();
}

#[test]
fn s1_draft_crud() {
    let factory = RedbFactory::fresh("v1");
    let store = factory.open(test_clock());
    case_s1_draft_crud(&*store);
}

/// S2: scheduled arming lifecycle — `create_draft` arms a `PostScheduledDraft`
/// job (`not_before` = `scheduled_at`), and `remove_scheduled_draft_jobs`
/// clears every arming for the draft (update/delete `clearSchedule` parity),
/// including repeated clears.
fn case_s2_scheduled_arming<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "s2_alice");
    let at = BASE_MS + 60_000;
    store
        .create_draft(
            draft_row("s2_d1", &alice.id, Some(at)),
            vec![scheduled_draft_job("s2-arm", "s2_d1", at)],
        )
        .unwrap();
    assert_eq!(store.pending_count().unwrap(), 1);
    // Not due before `scheduled_at`: the armed job is not claimable yet.
    assert!(store.claim_next(30_000).unwrap().is_none());
    clock.advance_ms(61_000);
    let claimed = store.claim_next(30_000).unwrap().expect("due job claims");
    assert_eq!(claimed.kind, JobKind::PostScheduledDraft);
    assert_eq!(claimed.not_before, at);
    let payload: ScheduledDraftPayload = serde_json::from_slice(&claimed.payload).unwrap();
    assert_eq!(payload.draft_id, "s2_d1");
    assert_eq!(payload.scheduled_at_ms, at);
    store.complete(&claimed.id).unwrap();

    // Two armings for one draft (update re-arms after clearing): the clear
    // removes both, and repeats are no-ops.
    store
        .create_draft(
            draft_row("s2_d2", &alice.id, Some(at)),
            vec![
                scheduled_draft_job("s2-arm-a", "s2_d2", at),
                scheduled_draft_job("s2-arm-b", "s2_d2", at + 1),
            ],
        )
        .unwrap();
    // Clearing an unrelated draft removes nothing.
    assert_eq!(store.remove_scheduled_draft_jobs("s2_nope").unwrap(), 0);
    assert_eq!(store.remove_scheduled_draft_jobs("s2_d2").unwrap(), 2);
    assert_eq!(store.remove_scheduled_draft_jobs("s2_d2").unwrap(), 0);
    assert_eq!(store.pending_count().unwrap(), 0);
}

#[test]
fn s2_scheduled_arming() {
    let factory = RedbFactory::fresh("s2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_s2_scheduled_arming(&*store, &clock);
}

// ---------------------------------------------------------------------------
// Group P — pinnings (§7, `docs/misskey-parity-plan.md`)
// ---------------------------------------------------------------------------

fn pinning_row(id: &str, user_id: &str, note_id: &str) -> UserNotePining {
    UserNotePining {
        id: id.to_string(),
        user_id: user_id.to_string(),
        note_id: note_id.to_string(),
    }
}

/// P1: pinning CRUD through the ports — unique `(user, note)` pairs
/// (`ALREADY_PINNED` on dup), oldest-first listing, no-op remove.
fn case_p1_pinning_crud<S: ConformanceStore>(store: &S) {
    let alice = create_user(store, "p1_alice");
    assert!(store.pinnings_for_user(&alice.id).unwrap().is_empty());
    store
        .add_pinning(pinning_row("p1_b", &alice.id, "n2"), vec![])
        .unwrap();
    store
        .add_pinning(pinning_row("p1_a", &alice.id, "n1"), vec![])
        .unwrap();
    assert!(matches!(
        store.add_pinning(pinning_row("p1_dup", &alice.id, "n1"), vec![]),
        Err(PortError::Conflict(msg)) if msg == "ALREADY_PINNED"
    ));
    // Oldest pin first (by pinning id).
    let listed = store.pinnings_for_user(&alice.id).unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].id, "p1_a");
    assert_eq!(listed[1].id, "p1_b");
    // Remove drops the pair; repeats are no-op successes.
    store.remove_pinning(&alice.id, "n1", vec![]).unwrap();
    let listed = store.pinnings_for_user(&alice.id).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].note_id, "n2");
    store.remove_pinning(&alice.id, "n1", vec![]).unwrap();
    store
        .remove_pinning(&alice.id, "n_missing", vec![])
        .unwrap();
}

#[test]
fn p1_pinning_crud() {
    let factory = RedbFactory::fresh("p1");
    let store = factory.open(test_clock());
    case_p1_pinning_crud(&*store);
}

/// P2: pinning + jobs commit atomically.
fn case_p2_pinning_atomicity<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "p2_alice");
    let before = store.pending_count().unwrap();
    store
        .add_pinning(
            pinning_row("p2_a", &alice.id, "n1"),
            vec![streaming_job(clock, "p2-pin-job")],
        )
        .unwrap();
    assert_eq!(store.pending_count().unwrap(), before + 1);
    store
        .remove_pinning(&alice.id, "n1", vec![streaming_job(clock, "p2-unpin-job")])
        .unwrap();
    assert_eq!(store.pending_count().unwrap(), before + 2);
    assert!(store.pinnings_for_user(&alice.id).unwrap().is_empty());
}

#[test]
fn p2_pinning_atomicity() {
    let factory = RedbFactory::fresh("p2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_p2_pinning_atomicity(&*store, &clock);
}

// ---------------------------------------------------------------------------
// Group E — note edits (§8) + quote-disable storage (§3,
// `docs/misskey-parity-plan.md`; backend-generic, Phase 4 runs these
// against FoundationDB unchanged)
// ---------------------------------------------------------------------------

/// Minimal edit input: text-only change, everything else carried over from
/// the stored note. Callers set `poll` explicitly (`None` removes the
/// stored poll) and `cw`/`renote_of` when the case needs them.
fn edit_text_input(editor_id: &str, note_id: &str, text: &str) -> EditNoteInput {
    EditNoteInput {
        editor_id: editor_id.to_string(),
        note_id: note_id.to_string(),
        text: Some(text.to_string()),
        cw: None,
        renote_of: None,
        file_ids: Vec::new(),
        poll: None,
        reaction_acceptance: None,
        visible_users: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        uri_base: "https://example.local".to_string(),
    }
}

fn edit_poll_input(expires_at_ms: i64) -> PollInput {
    PollInput {
        choices: vec!["yes".to_string(), "no".to_string()],
        multiple: false,
        expires_at_ms: Some(expires_at_ms),
    }
}

/// Note with a poll through the storage ports (`has_poll` must agree with
/// `poll`, per the create-path invariant).
fn note_with_poll<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
    author_id: &str,
    text: &str,
    key: &str,
) -> Note {
    let mut input = CreateNoteInput::simple_public(author_id, text);
    input.has_poll = true;
    input.poll = Some(edit_poll_input(BASE_MS + 3_600_000));
    store
        .create_note(input, vec![streaming_job(clock, key)])
        .expect("create_note with poll")
}

fn set_reject_quotes<S: ConformanceStore>(store: &S, account_id: &str, reject: bool) {
    store
        .update_account(
            account_id,
            pinetree::application::ports::UpdateAccountPatch {
                reject_quotes: Some(reject),
                ..Default::default()
            },
            Vec::new(),
        )
        .expect("update reject_quotes");
}

/// Quote-shaped create input through the storage ports: commentary text +
/// `renote_of` (an `is_quote` shape) with an explicit quote relation, as
/// the application layer builds it for local quotes.
fn quote_create_input(author_id: &str, target: &Note) -> CreateNoteInput {
    let mut input = CreateNoteInput::simple_public(author_id, "commentary");
    input.renote_of = Some(target.id.clone());
    input.quote = Some(quote_pending("https://remote.example/notes/e6-target"));
    input
}

/// E1: edit lifecycle — an effective text+cw edit updates the row, stamps
/// `updated_at_ms`, commits the jobs, and writes one history row.
/// Author mismatch is `Validation(YOU_ARE_NOT_THE_AUTHOR)`; unknown and
/// deleted notes are `NotFound(NO_SUCH_NOTE)`. Reply-parent immutability
/// is structural (`EditNoteInput` carries no reply field): a reply note's
/// `reply_id` survives an effective edit.
fn case_e1_edit_lifecycle<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "e1_alice");
    let bob = create_user(store, "e1_bob");
    let note = note_with_poll(store, clock, &alice.id, "v1", "e1:create");
    assert!(note.updated_at_ms.is_none());

    let mut edit = edit_text_input(&alice.id, &note.id, "v2");
    edit.cw = Some("spoiler".to_string());
    edit.poll = Some(edit_poll_input(BASE_MS + 3_600_000));
    let before = store.pending_count().unwrap();
    let edited = store
        .apply_note_edit(edit, vec![streaming_job(clock, "e1:edit")])
        .expect("effective edit");
    assert_eq!(edited.text.as_deref(), Some("v2"));
    assert_eq!(edited.cw.as_deref(), Some("spoiler"));
    assert!(edited.updated_at_ms.is_some());
    assert_eq!(
        store.pending_count().unwrap(),
        before + 1,
        "edit + jobs commit together"
    );
    let reread = store.get_note(&note.id).unwrap().expect("note still here");
    assert_eq!(reread.text.as_deref(), Some("v2"));
    assert_eq!(reread.updated_at_ms, edited.updated_at_ms);
    // The preserved poll survives the edit untouched with zeroed tallies.
    let poll = store.get_poll(&note.id).unwrap().expect("poll kept");
    assert_eq!(poll.choices, vec!["yes".to_string(), "no".to_string()]);
    assert_eq!(poll.votes, vec![0, 0]);

    let edits = store.note_edits(&note.id).unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].old_text.as_deref(), Some("v1"));
    assert_eq!(edits[0].new_text.as_deref(), Some("v2"));
    assert_eq!(edits[0].cw.as_deref(), Some("spoiler"));

    // Author mismatch → YOU_ARE_NOT_THE_AUTHOR (storage-level re-check).
    let err = store
        .apply_note_edit(edit_text_input(&bob.id, &note.id, "hijack"), vec![])
        .expect_err("non-author edit must fail");
    assert!(
        matches!(err, PortError::Validation(ref msg) if msg == "YOU_ARE_NOT_THE_AUTHOR"),
        "got {err:?}"
    );
    // Unknown and deleted notes → NO_SUCH_NOTE.
    let err = store
        .apply_note_edit(edit_text_input(&alice.id, "missing", "x"), vec![])
        .expect_err("unknown note must fail");
    assert!(
        matches!(err, PortError::NotFound(ref msg) if msg == "NO_SUCH_NOTE"),
        "got {err:?}"
    );
    store.delete_note(&note.id, vec![]).unwrap();
    let err = store
        .apply_note_edit(edit_text_input(&alice.id, &note.id, "x"), vec![])
        .expect_err("deleted note must fail");
    assert!(
        matches!(err, PortError::NotFound(ref msg) if msg == "NO_SUCH_NOTE"),
        "got {err:?}"
    );

    // Reply-parent immutability: a reply edit keeps its original parent.
    let parent = note_with_job(store, clock, &bob.id, "parent", "e1:parent");
    let mut reply = CreateNoteInput::simple_public(&alice.id, "reply v1");
    reply.reply_to = Some(parent.id.clone());
    let reply = store.create_note(reply, vec![]).expect("reply");
    let edited_reply = store
        .apply_note_edit(edit_text_input(&alice.id, &reply.id, "reply v2"), vec![])
        .expect("reply edit");
    assert_eq!(
        edited_reply.reply_id.as_deref(),
        Some(parent.id.as_str()),
        "reply parent is immutable"
    );
}

#[test]
fn note_edit_lifecycle() {
    let factory = RedbFactory::fresh("ed1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e1_edit_lifecycle(&*store, &clock);
}

/// E2: no-op edit — identical effective text/cw/poll/files/renote/
/// acceptance returns the ORIGINAL note (`updated_at_ms` stays `None`),
/// writes no history row, and enqueues no jobs (`pending_count`
/// unchanged, even when jobs are passed).
fn case_e2_edit_noop<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "e2_alice");
    let note = note_with_poll(store, clock, &alice.id, "same", "e2:create");
    let mut noop = edit_text_input(&alice.id, &note.id, "same");
    noop.poll = Some(edit_poll_input(BASE_MS + 3_600_000));
    let before = store.pending_count().unwrap();
    let returned = store
        .apply_note_edit(noop, vec![streaming_job(clock, "e2:noop")])
        .expect("no-op edit succeeds");
    assert!(
        returned.updated_at_ms.is_none(),
        "no-op returns the original note"
    );
    assert_eq!(returned.text.as_deref(), Some("same"));
    let reread = store.get_note(&note.id).unwrap().expect("note");
    assert!(reread.updated_at_ms.is_none());
    assert!(
        store.note_edits(&note.id).unwrap().is_empty(),
        "no history row on no-op"
    );
    assert_eq!(
        store.pending_count().unwrap(),
        before,
        "no jobs enqueued on no-op"
    );
}

#[test]
fn note_edit_noop_writes_nothing() {
    let factory = RedbFactory::fresh("ed2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e2_edit_noop(&*store, &clock);
}

/// E3: poll replace resets votes — a different poll definition swaps the
/// row with zeroed tallies, drops every `POLL_VOTES` row for the note
/// (re-vote succeeds, tallies restart), writes one history row, and
/// commits the passed jobs (the poll-expiry re-arm among them).
fn case_e3_edit_poll_replace_resets_votes<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "e3_alice");
    let bob = create_user(store, "e3_bob");
    let note = note_with_poll(store, clock, &alice.id, "pick", "e3:create");
    store
        .cast_vote(
            PollVote {
                id: "e3-vote-1".to_string(),
                note_id: note.id.clone(),
                user_id: bob.id.clone(),
                choice: 0,
            },
            vec![],
        )
        .expect("first vote");
    assert_eq!(store.get_poll(&note.id).unwrap().unwrap().votes, vec![1, 0]);

    let replacement = PollInput {
        choices: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        multiple: false,
        expires_at_ms: Some(BASE_MS + 7_200_000),
    };
    let mut edit = edit_text_input(&alice.id, &note.id, "pick");
    edit.poll = Some(replacement.clone());
    let rearm = NewJob::new_json(
        JobKind::PollExpiry,
        &PollExpiryPayload {
            note_id: note.id.clone(),
        },
        "e3:rearm",
        BASE_MS + 7_200_000,
    )
    .expect("rearm payload");
    let before = store.pending_count().unwrap();
    store
        .apply_note_edit(edit, vec![streaming_job(clock, "e3:edit"), rearm])
        .expect("poll replace");
    assert_eq!(
        store.pending_count().unwrap(),
        before + 2,
        "edit jobs committed"
    );
    let poll = store.get_poll(&note.id).unwrap().expect("replaced poll");
    assert_eq!(
        poll.choices,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(poll.votes, vec![0, 0, 0], "tallies restart zeroed");
    assert!(
        store
            .votes_by_user_for_note(&bob.id, &note.id)
            .unwrap()
            .is_empty(),
        "old vote rows dropped"
    );
    // Re-vote succeeds (the old vote no longer blocks); tallies restart.
    store
        .cast_vote(
            PollVote {
                id: "e3-vote-2".to_string(),
                note_id: note.id.clone(),
                user_id: bob.id.clone(),
                choice: 0,
            },
            vec![],
        )
        .expect("re-vote after reset");
    assert_eq!(
        store.get_poll(&note.id).unwrap().unwrap().votes,
        vec![1, 0, 0]
    );
    assert_eq!(
        store.note_edits(&note.id).unwrap().len(),
        1,
        "one history row"
    );
}

#[test]
fn note_edit_poll_replace_resets_votes() {
    let factory = RedbFactory::fresh("ed3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e3_edit_poll_replace_resets_votes(&*store, &clock);
}

/// E4: history chaining — two effective edits yield two rows oldest-first;
/// row2's `old_text` is row1's `new_text`; row1's `old_date_ms` is the
/// note creation time (parsed from the time-ordered note id, which the
/// factory's id generator stamps from the same clock) and row2's
/// `old_date_ms` is row1's edit time.
fn case_e4_edit_history_chaining<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "e4_alice");
    let t0 = clock.now_ms();
    let note = note_with_job(store, clock, &alice.id, "v1", "e4:create");
    clock.advance_ms(1_000);
    store
        .apply_note_edit(edit_text_input(&alice.id, &note.id, "v2"), vec![])
        .expect("first edit");
    clock.advance_ms(1_000);
    store
        .apply_note_edit(edit_text_input(&alice.id, &note.id, "v3"), vec![])
        .expect("second edit");
    let rows = store.note_edits(&note.id).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].old_text.as_deref(), Some("v1"));
    assert_eq!(rows[0].new_text.as_deref(), Some("v2"));
    assert_eq!(rows[1].old_text.as_deref(), Some("v2"));
    assert_eq!(rows[1].new_text.as_deref(), Some("v3"));
    assert_eq!(
        rows[0].old_date_ms, t0,
        "first old_date is the creation time"
    );
    assert_eq!(
        rows[1].old_date_ms, rows[0].updated_at_ms,
        "second old_date chains from the first edit"
    );
    assert!(rows[0].old_date_ms < rows[0].updated_at_ms);
    assert!(rows[0].updated_at_ms < rows[1].updated_at_ms);
}

#[test]
fn note_edit_history_chaining() {
    let factory = RedbFactory::fresh("ed4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e4_edit_history_chaining(&*store, &clock);
}

/// E5: inbound dedup — `record_note_edit_activity` commits the edit,
/// history row, and jobs once; replaying the same activity id converges
/// to `None` with no second history row and no extra jobs.
fn case_e5_inbound_edit_dedup<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "e5_alice");
    let note = note_with_job(store, clock, &alice.id, "v1", "e5:create");
    let before = store.pending_count().unwrap();
    let first = store
        .record_note_edit_activity(
            "e5:upd-1",
            edit_text_input(&alice.id, &note.id, "v2"),
            vec![streaming_job(clock, "e5:edit")],
        )
        .expect("first delivery applies")
        .expect("first delivery is not a replay");
    assert_eq!(first.text.as_deref(), Some("v2"));
    assert_eq!(store.note_edits(&note.id).unwrap().len(), 1);
    assert_eq!(store.pending_count().unwrap(), before + 1);
    let replay = store
        .record_note_edit_activity(
            "e5:upd-1",
            edit_text_input(&alice.id, &note.id, "v3"),
            vec![streaming_job(clock, "e5:replay")],
        )
        .expect("replay converges");
    assert!(replay.is_none(), "replay applies nothing");
    assert_eq!(
        store.get_note(&note.id).unwrap().unwrap().text.as_deref(),
        Some("v2"),
        "replay changes nothing"
    );
    assert_eq!(
        store.note_edits(&note.id).unwrap().len(),
        1,
        "no second history row"
    );
    assert_eq!(
        store.pending_count().unwrap(),
        before + 1,
        "no extra jobs on replay"
    );
}

#[test]
fn note_edit_inbound_dedup() {
    let factory = RedbFactory::fresh("ed5");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e5_inbound_edit_dedup(&*store, &clock);
}

/// E6: quote-disable storage (§3) — the QUOTER's own `reject_quotes` flag
/// gates quote creation (Sharkey `checkQuotePermissions` parity): a
/// remote quoter is stripped to a plain note (`quoteUnavailable`, no
/// `renote_count` bump); a local quoter fails with
/// `QUOTE_DISABLED_FOR_USER`.
fn case_e6_quote_disable_storage<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let bob = create_user(store, "e6_bob");
    let target = note_with_job(store, clock, &bob.id, "quotable", "e6:target");

    // (a) Remote quoter with the flag: stripped to a plain note.
    let carol = remote_account(store, "e6_carol");
    set_reject_quotes(store, &carol.id, true);
    let stripped = store
        .create_note(
            quote_create_input(&carol.id, &target),
            vec![streaming_job(clock, "e6:strip")],
        )
        .expect("remote strip stores a plain note");
    assert!(stripped.renote_id.is_none(), "renote detached");
    assert!(stripped.quote.is_none(), "quote relation dropped");
    assert_eq!(
        stripped.process_errors,
        Some(vec!["quoteUnavailable".to_string()])
    );
    assert_eq!(
        stripped.text.as_deref(),
        Some("commentary"),
        "commentary kept"
    );
    let target_after = store.get_note(&target.id).unwrap().expect("target");
    assert_eq!(
        target_after.renote_count, 0,
        "no renote_count bump on strip"
    );

    // (b) Local quoter with the flag: hard failure.
    let dave = create_user(store, "e6_dave");
    set_reject_quotes(store, &dave.id, true);
    let err = store
        .create_note(quote_create_input(&dave.id, &target), vec![])
        .expect_err("local quote with the flag must fail");
    assert!(
        matches!(err, PortError::Validation(ref msg) if msg == "QUOTE_DISABLED_FOR_USER"),
        "got {err:?}"
    );

    // (c) Control: remote quoter without the flag stores the quote normally.
    let erin = remote_account(store, "e6_erin");
    let quote = store
        .create_note(
            quote_create_input(&erin.id, &target),
            vec![streaming_job(clock, "e6:control")],
        )
        .expect("control quote stores");
    assert_eq!(quote.renote_id.as_deref(), Some(target.id.as_str()));
    let relation = quote.quote.as_ref().expect("quote relation kept");
    assert_eq!(relation.target_note_id.as_deref(), Some(target.id.as_str()));
    let target_after = store.get_note(&target.id).unwrap().expect("target");
    assert_eq!(target_after.renote_count, 1, "control bumps renote_count");
}

#[test]
fn quote_disable_storage() {
    let factory = RedbFactory::fresh("ed6");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_e6_quote_disable_storage(&*store, &clock);
}
