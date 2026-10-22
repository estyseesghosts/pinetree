use super::common::*;

/// I3: `InstanceSettings.registration` (upstream `disableRegistration`)
/// persists through the settings store and commits with its
/// `updateServerSettings` row. The gate itself is application logic
/// (`AppServices::signup`), asserted by the endpoint tests; a passing
/// backend must store the policy exactly.
fn case_i3_registration_policy<S: ConformanceStore>(store: &S) {
    let close = InstanceSettingsPatch {
        registration: Some(RegistrationPolicy::Closed),
        ..Default::default()
    };
    let updated = store
        .update(
            close,
            log_entry(
                "i3log000000000a",
                None,
                ModerationAction::UpdateServerSettings,
                None,
                serde_json::json!({ "before": {"disableRegistration": false}, "after": {"disableRegistration": true} }),
            ),
        )
        .expect("close registration");
    assert!(updated.registration.disable_registration());
    assert!(
        InstanceSettingsStore::get(store)
            .expect("get")
            .registration
            .disable_registration()
    );
    let open = InstanceSettingsPatch {
        registration: Some(RegistrationPolicy::Open),
        ..Default::default()
    };
    store
        .update(
            open,
            log_entry(
                "i3log000000000b",
                None,
                ModerationAction::UpdateServerSettings,
                None,
                serde_json::json!({ "before": {"disableRegistration": true}, "after": {"disableRegistration": false} }),
            ),
        )
        .expect("reopen registration");
    assert!(
        !InstanceSettingsStore::get(store)
            .expect("get")
            .registration
            .disable_registration()
    );
    assert_eq!(log_rows(store).len(), 2, "each write kept its row");
}

#[test]
fn i3_registration_policy() {
    let factory = RedbFactory::fresh("i3");
    let clock = test_clock();
    let store = factory.open(clock);
    case_i3_registration_policy(&*store);
}

/// L1+L2: blocked_hosts normalization through the store (lowercase, dedup,
/// empty-drop) and update atomicity (settings + moderation-log row in one
/// transaction).
fn case_l_blocked_hosts<S: ConformanceStore>(store: &S) {
    let patch = InstanceSettingsPatch {
        blocked_hosts: Some(vec![
            " Example.COM ".to_string(),
            "example.com".to_string(),
            "".to_string(),
            "  ".to_string(),
            "Sub.Example.COM".to_string(),
        ]),
        ..Default::default()
    };
    let updated = store
        .update(
            patch,
            log_entry(
                "l1log000000000a",
                None,
                ModerationAction::UpdateServerSettings,
                None,
                serde_json::json!({}),
            ),
        )
        .expect("update blocked hosts");
    assert_eq!(
        updated.blocked_hosts,
        vec!["example.com".to_string(), "sub.example.com".to_string()]
    );
    assert_eq!(
        InstanceSettingsStore::get(store)
            .expect("get")
            .blocked_hosts,
        vec!["example.com".to_string(), "sub.example.com".to_string()]
    );
    assert_eq!(log_rows(store).len(), 1, "update committed its log row");

    // Unblocking (empty list) restores with no data loss at storage level.
    let unblock = InstanceSettingsPatch {
        blocked_hosts: Some(Vec::new()),
        ..Default::default()
    };
    store
        .update(
            unblock,
            log_entry(
                "l1log000000000b",
                None,
                ModerationAction::UpdateServerSettings,
                None,
                serde_json::json!({}),
            ),
        )
        .expect("unblock");
    assert!(
        InstanceSettingsStore::get(store)
            .expect("get")
            .blocked_hosts
            .is_empty()
    );
}

#[test]
fn l_blocked_hosts() {
    let factory = RedbFactory::fresh("l");
    let clock = test_clock();
    let store = factory.open(clock);
    case_l_blocked_hosts(&*store);
}
