use super::common::*;

/// J1 (Batch N WS2): the favorite is unique per (account, note). A second
/// insert for the same pair fails `Conflict(ALREADY_FAVORITED)`; a
/// different account or a different note succeeds.
fn case_j1_favorite_uniqueness<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "j1_alice");
    let bob = create_user(store, "j1_bob");
    let note = note_with_job(store, clock, &alice.id, "fav me", "j1:n1");
    let other = note_with_job(store, clock, &alice.id, "fav me too", "j1:n2");
    let fav = NoteFavorite {
        id: "j1fav000000000001".to_string(),
        user_id: bob.id.clone(),
        note_id: note.id.clone(),
    };
    store
        .favorite_note(fav.clone(), Vec::new())
        .expect("first favorite lands");
    let err = store
        .favorite_note(
            NoteFavorite {
                id: "j1fav000000000002".to_string(),
                user_id: bob.id.clone(),
                note_id: note.id.clone(),
            },
            Vec::new(),
        )
        .expect_err("duplicate favorite conflicts");
    assert!(is_conflict(&err), "got {err:?}");
    // Same note, different account: fine.
    store
        .favorite_note(
            NoteFavorite {
                id: "j1fav000000000003".to_string(),
                user_id: alice.id.clone(),
                note_id: note.id.clone(),
            },
            Vec::new(),
        )
        .expect("other account may favorite");
    // Same account, different note: fine.
    store
        .favorite_note(
            NoteFavorite {
                id: "j1fav000000000004".to_string(),
                user_id: bob.id.clone(),
                note_id: other.id.clone(),
            },
            Vec::new(),
        )
        .expect("other note may be favorited");
    assert!(store.is_favorited(&bob.id, &note.id).expect("lookup"));
    assert!(!store.is_favorited(&bob.id, "missing").expect("lookup"));
}

/// J3 (Batch N WS2): deleting a favorite removes the row but leaves the
/// note; deleting again reports `false` (the endpoint maps it to
/// `NOT_FAVORITED`).
fn case_j3_favorite_delete_leaves_note<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    let alice = create_user(store, "j3_alice");
    let note = note_with_job(store, clock, &alice.id, "still here", "j3:n1");
    store
        .favorite_note(
            NoteFavorite {
                id: "j3fav000000000001".to_string(),
                user_id: alice.id.clone(),
                note_id: note.id.clone(),
            },
            Vec::new(),
        )
        .expect("favorite lands");
    assert!(
        store
            .unfavorite_note(&alice.id, &note.id)
            .expect("unfavorite")
    );
    assert!(
        !store
            .unfavorite_note(&alice.id, &note.id)
            .expect("second delete")
    );
    assert!(
        store.get_note(&note.id).expect("lookup").is_some(),
        "the note survives the unfavorite"
    );
}

#[test]
fn j1_favorite_uniqueness() {
    let factory = RedbFactory::fresh("j1");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_j1_favorite_uniqueness(&*store, &clock);
}

#[test]
fn j3_favorite_delete_leaves_note() {
    let factory = RedbFactory::fresh("j3");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_j3_favorite_delete_leaves_note(&*store, &clock);
}
