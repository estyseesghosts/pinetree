use super::common::*;

/// W2 (Batch P): registry upserts, per-(domain, scope) reads, and domain
/// isolation. Set overwrites in place; remove of a missing key is silent;
/// null-domain and token-domain rows never see each other; scopes lists
/// distinct (domain, scope) pairs.
fn case_w2_registry_store<S: RegistryStore>(store: &S, clock: &ManualClock) {
    let item =
        |domain: Option<&str>, scope: &[&str], key: &str, value: serde_json::Value| RegistryItem {
            id: format!("reg-{key}"),
            user_id: "alice".to_string(),
            domain: domain.map(str::to_string),
            scope: scope.iter().map(|s| s.to_string()).collect(),
            key: key.to_string(),
            value,
            updated_at_ms: clock.now_ms(),
        };
    let scope: Vec<String> = vec!["client".to_string()];
    // Misses first.
    assert!(
        store
            .registry_get("alice", None, &scope, "theme")
            .expect("get")
            .is_none()
    );
    store
        .registry_set(item(None, &["client"], "theme", serde_json::json!("dark")))
        .expect("set");
    store
        .registry_set(item(None, &["client"], "font", serde_json::json!(14)))
        .expect("set");
    store
        .registry_set(item(None, &["other"], "theme", serde_json::json!("light")))
        .expect("set");
    store
        .registry_set(item(
            Some("apptoken1"),
            &["client"],
            "theme",
            serde_json::json!("blue"),
        ))
        .expect("set");
    // Other users see nothing.
    assert!(
        store
            .registry_get("mallory", None, &scope, "theme")
            .expect("get")
            .is_none()
    );
    // Domain isolation.
    let row = store
        .registry_get("alice", None, &scope, "theme")
        .expect("get")
        .expect("row");
    assert_eq!(row.value, serde_json::json!("dark"));
    let row = store
        .registry_get("alice", Some("apptoken1"), &scope, "theme")
        .expect("get")
        .expect("row");
    assert_eq!(row.value, serde_json::json!("blue"));
    // Scope listing.
    let mut keys: Vec<String> = store
        .registry_list("alice", None, &scope)
        .expect("list")
        .into_iter()
        .map(|item| item.key)
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["font".to_string(), "theme".to_string()]);
    // Overwrite in place (one row, new value).
    store
        .registry_set(item(
            None,
            &["client"],
            "theme",
            serde_json::json!("darker"),
        ))
        .expect("overwrite");
    let rows = store.registry_list("alice", None, &scope).expect("list");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter().find(|r| r.key == "theme").expect("theme").value,
        serde_json::json!("darker")
    );
    // Scopes-with-domain pairs.
    let mut pairs = store.registry_scopes("alice").expect("scopes");
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            (None, vec!["client".to_string()]),
            (None, vec!["other".to_string()]),
            (Some("apptoken1".to_string()), vec!["client".to_string()]),
        ]
    );
    // Remove converges; missing removes are silent.
    store
        .registry_remove("alice", None, &scope, "theme")
        .expect("remove");
    assert!(
        store
            .registry_get("alice", None, &scope, "theme")
            .expect("get")
            .is_none()
    );
    store
        .registry_remove("alice", None, &scope, "theme")
        .expect("re-remove");
    store
        .registry_remove("alice", None, &scope, "never")
        .expect("missing");
    // Scoped rows of other scopes/domains survive.
    assert!(
        store
            .registry_get("alice", None, &scope, "font")
            .expect("get")
            .is_some()
    );
    assert!(
        store
            .registry_get("alice", Some("apptoken1"), &scope, "theme")
            .expect("get")
            .is_some()
    );
}

#[test]
fn w2_registry_store_isolation() {
    let factory = RedbFactory::fresh("w2");
    let clock = test_clock();
    let store = factory.open(clock.clone());
    case_w2_registry_store(&*store, &clock);
}
