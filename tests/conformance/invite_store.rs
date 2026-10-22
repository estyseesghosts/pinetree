use super::common::*;

// ---------------------------------------------------------------------------
// Group I — invite codes and the signup claim (Batch M WP2;
// `docs/implementer-guide-batch-m.md`)
// ---------------------------------------------------------------------------

fn invite_input(
    code: &str,
    created_by: Option<&str>,
    expires_at_ms: Option<i64>,
) -> CreateInviteInput {
    CreateInviteInput {
        code: code.to_string(),
        created_by: created_by.map(str::to_string),
        expires_at_ms,
    }
}

fn consume_input(code: &str, handle: &str) -> ConsumeInviteInput {
    ConsumeInviteInput {
        code: code.to_string(),
        account: CreateAccountInput::local(handle),
        password_hash: Some(format!("hash-for-{handle}")),
    }
}

/// I1: one invite code creates at most one account even under N retrying
/// claims — exactly one `consume_and_create_account` succeeds (the
/// unique-index row is dropped with the claim).
fn case_i1_invite_single_use<S: ConformanceStore>(store: &S) {
    store
        .create_invites(&[invite_input("ICODE0001AAAA", None, None)], None)
        .expect("create invite");
    let mut successes = 0usize;
    for attempt in 0..5 {
        match store.consume_and_create_account(consume_input(
            "ICODE0001AAAA",
            &format!("i1_claimant{attempt}"),
        )) {
            Ok((account, ticket)) => {
                successes += 1;
                assert_eq!(
                    ticket.used_by.as_deref(),
                    Some(account.id.as_str()),
                    "the winning claim marks the code used for its account"
                );
            }
            Err(PortError::NotFound(_)) => {}
            Err(err) => panic!("attempt {attempt} failed unexpectedly: {err:?}"),
        }
    }
    assert_eq!(successes, 1, "exactly one of 5 claims succeeds");
    assert!(
        store
            .find_invite_by_code("ICODE0001AAAA")
            .expect("lookup")
            .is_none(),
        "a consumed code is gone from the unique index"
    );
    assert!(
        store
            .find_by_handle("i1_claimant0")
            .expect("lookup")
            .is_some(),
        "the first (winning) claim created the account"
    );
    assert!(
        store
            .find_by_handle("i1_claimant1")
            .expect("lookup")
            .is_none(),
        "losing claims created nothing"
    );
}

/// I2: the claim and the account create are ONE transaction — a failing
/// account insert (duplicate handle) aborts the claim entirely: the code
/// stays unused and claimable, and no account row appears.
fn case_i2_consume_create_atomic<S: ConformanceStore>(store: &S) {
    create_user(store, "i2_taken");
    store
        .create_invites(&[invite_input("ICODE0002AAAA", None, None)], None)
        .expect("create invite");
    let err = store
        .consume_and_create_account(consume_input("ICODE0002AAAA", "i2_taken"))
        .expect_err("duplicate handle refuses the consume");
    assert!(
        matches!(err, PortError::Conflict(_)),
        "duplicate handle is a conflict, got {err:?}"
    );
    let ticket = store
        .find_invite_by_code("ICODE0002AAAA")
        .expect("lookup")
        .expect("the code survived the failed claim");
    assert!(!ticket.is_used(), "the failed claim consumed nothing");
    // A valid claim in the same state now succeeds.
    let (account, ticket) = store
        .consume_and_create_account(consume_input("ICODE0002AAAA", "i2_fresh"))
        .expect("claim after the failed attempt");
    assert!(ticket.is_used());
    assert!(store.find_by_id(&account.id).expect("lookup").is_some());
    assert!(
        store.find_by_handle("i2_taken").expect("lookup").is_some(),
        "the pre-existing account is untouched"
    );
}

/// I4: the happy path of the claim — a valid unexpired code creates the
/// account, marks the code used (used_by/used_at), and drops the unique
/// index row; missing, used, and expired codes all refuse the same way
/// (upstream's signup gate answers 400 for all three).
fn case_i4_consume_happy_path<S: ConformanceStore>(store: &S, clock: &ManualClock) {
    store
        .create_invites(
            &[invite_input("ICODE0004AAAA", None, Some(BASE_MS + 60_000))],
            None,
        )
        .expect("create invite");
    let (account, ticket) = store
        .consume_and_create_account(consume_input("ICODE0004AAAA", "i4_newbie"))
        .expect("consume a valid invite");
    assert_eq!(ticket.used_by.as_deref(), Some(account.id.as_str()));
    assert_eq!(ticket.used_at_ms, Some(clock.now_ms()));
    assert!(store.find_by_handle("i4_newbie").expect("lookup").is_some());
    assert!(
        store
            .find_invite_by_code("ICODE0004AAAA")
            .expect("lookup")
            .is_none()
    );
    // Second consume of the same code: refused.
    assert!(matches!(
        store.consume_and_create_account(consume_input("ICODE0004AAAA", "i4_ghost")),
        Err(PortError::NotFound(_))
    ));
    assert!(store.find_by_handle("i4_ghost").expect("lookup").is_none());
    // Expired code: refused the same way, nothing created.
    store
        .create_invites(
            &[invite_input("ICODE0004EXPD", None, Some(BASE_MS - 1))],
            None,
        )
        .expect("create expired invite");
    assert!(matches!(
        store.consume_and_create_account(consume_input("ICODE0004EXPD", "i4_late")),
        Err(PortError::NotFound(_))
    ));
    assert!(store.find_by_handle("i4_late").expect("lookup").is_none());
}

#[test]
fn i1_invite_single_use() {
    let factory = RedbFactory::fresh("i1");
    let clock = test_clock();
    let store = factory.open(clock);
    case_i1_invite_single_use(&*store);
}

#[test]
fn i2_consume_create_atomic() {
    let factory = RedbFactory::fresh("i2");
    let clock = test_clock();
    let store = factory.open(clock);
    case_i2_consume_create_atomic(&*store);
}

#[test]
fn i4_consume_happy_path() {
    let factory = RedbFactory::fresh("i4");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_i4_consume_happy_path(&*store, &clock);
}
