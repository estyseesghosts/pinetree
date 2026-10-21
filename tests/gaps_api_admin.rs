//! WP2-part-C admin/misc tests (service level over the real redb backend):
//! relays, announcements, ads, roles, abuse recipients, proxy account,
//! stats/server-info/queue, reset-password, user IPs, find-by-email,
//! federation instances, push subscriptions, retention, endpoint(s).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::commands::misc_endpoints::{ENDPOINT_NAMES, ping};
use pinetree::application::ports::{
    BlobStore, Clock, CreateAccountInput, IdGenerator, NoopStreamingSink, PortError, SigninRecord,
    TimelineCursor, UpdateAccountPatch,
};
use pinetree::domain::admin::{QUEUE_CONTROL_UNSUPPORTED_REASON, RESET_DB_REFUSED_REASON};
use pinetree::domain::id::{AidxGenerator, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services() -> (AppServices, Arc<ManualClock>) {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-gaps-admin-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock = Arc::new(ManualClock::new(1_760_400_000_000));
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
    let services = AppServices {
        clock: clock.clone() as Arc<dyn Clock>,
        ids: ids.clone(),
        accounts: store.clone(),
        notes: store.clone(),
        follows: store.clone(),
        blocks: store.clone(),
        mutes: store.clone(),
        reactions: store.clone(),
        favorites: store.clone(),
        emojis: store.clone(),
        antennas: store.clone(),
        antenna_notes: store.clone(),
        polls: store.clone(),
        jobs: store.clone(),
        media: store.clone(),
        blobs: blobs.clone(),
        notifications: store.clone(),
        auth: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone(),
        host_caps: store.clone(),
        moderation: store.clone(),
        instance_settings: store.clone(),
        invites: store.clone(),
        abuse_reports: store.clone(),
        lists: store.clone(),
        signin: store.clone(),
        password_resets: store.clone(),
        registry: store.clone(),
        clips: store.clone(),
        pages: store.clone(),
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "https://example.local".to_string(),
        max_file_size: 10 * 1024 * 1024,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs.clone(),
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: pinetree::config::RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    };
    (services, clock)
}

fn make_user(services: &AppServices, username: &str) -> String {
    let hash = pinetree::api::misskey::auth::hash_password("pw123456");
    services.create_account(username, Some(&hash)).unwrap().id
}

fn code_of(err: PortError) -> String {
    match err {
        PortError::Validation(msg) | PortError::Conflict(msg) | PortError::NotFound(msg) => {
            msg.split_whitespace().next().unwrap_or("").to_string()
        }
        PortError::StorageUnavailable(msg) => format!("UNAVAILABLE:{msg}"),
    }
}

#[test]
fn relays_crud() {
    let (services, _) = test_services();
    assert!(services.relay_list().unwrap().is_empty());
    let relay = services.relay_add("https://relay.example/inbox").unwrap();
    assert_eq!(relay.status, "requesting");
    // Idempotent re-add returns the same row.
    let again = services.relay_add("https://relay.example/inbox").unwrap();
    assert_eq!(again.inbox, relay.inbox);
    assert_eq!(services.relay_list().unwrap().len(), 1);
    assert_eq!(
        code_of(services.relay_add("not-a-url").unwrap_err()),
        "INVALID_PARAM"
    );
    services.relay_remove("https://relay.example/inbox").unwrap();
    assert!(services.relay_list().unwrap().is_empty());
    // Missing remove is a no-op.
    services.relay_remove("https://relay.example/inbox").unwrap();
}

#[test]
fn admin_announcements_crud() {
    let (services, _) = test_services();
    let created = services
        .admin_announcement_create("Hello", "world", None)
        .unwrap();
    assert_eq!(services.admin_announcement_list().unwrap().len(), 1);
    let updated = services
        .admin_announcement_update(&created.id, "Hi", "there", None)
        .unwrap();
    assert_eq!(updated.title, "Hi");
    assert_eq!(updated.created_at_ms, created.created_at_ms);
    services.admin_announcement_delete(&created.id).unwrap();
    assert!(services.admin_announcement_list().unwrap().is_empty());
    assert_eq!(
        code_of(
            services
                .admin_announcement_update("missing", "t", "x", None)
                .unwrap_err()
        ),
        "NO_SUCH_ANNOUNCEMENT"
    );
    assert_eq!(
        code_of(services.admin_announcement_create("", "x", None).unwrap_err()),
        "INVALID_PARAM"
    );
}

#[test]
fn ads_crud_and_refresh() {
    let (services, clock) = test_services();
    let now = clock.now_ms();
    let live = services
        .ad_create(
            "https://ads.example/1",
            "memo",
            "square",
            1.0,
            "high",
            now + 100_000,
            now,
            "https://ads.example/1.png",
            6,
        )
        .unwrap();
    services
        .ad_create(
            "https://ads.example/2",
            "memo",
            "square",
            1.0,
            "low",
            now - 1,
            now - 1000,
            "https://ads.example/2.png",
            6,
        )
        .unwrap();
    assert_eq!(services.ad_list().unwrap().len(), 2);
    let updated = services
        .ad_update(
            &live.id,
            "https://ads.example/1b",
            "memo2",
            "horizontal",
            2.0,
            "high",
            now + 200_000,
            now,
            "https://ads.example/1b.png",
            6,
        )
        .unwrap();
    assert_eq!(updated.place, "horizontal");
    // refresh prunes only the expired row.
    assert_eq!(services.ad_refresh().unwrap(), 1);
    assert_eq!(services.ad_list().unwrap().len(), 1);
    services.ad_delete(&live.id).unwrap();
    assert!(services.ad_list().unwrap().is_empty());
    assert_eq!(
        code_of(
            services
                .ad_update("missing", "https://x.example", "m", "square", 1.0, "low", 2, 1, "https://x.example/i.png", 6)
                .unwrap_err()
        ),
        "NO_SUCH_AD"
    );
}

#[test]
fn roles_crud_and_membership() {
    let (services, _) = test_services();
    let alice = make_user(&services, "alice");
    let role = services
        .role_create(
            "mods",
            "moderators",
            None,
            None,
            "manual",
            serde_json::json!({}),
            true,
            false,
            true,
            1,
            serde_json::json!({}),
        )
        .unwrap();
    assert_eq!(services.role_list().unwrap().len(), 1);
    assert_eq!(services.role_show(&role.id).unwrap().name, "mods");
    assert_eq!(
        code_of(services.role_show("missing").unwrap_err()),
        "NO_SUCH_ROLE"
    );

    // assign is idempotent and needs an existing account.
    services.role_assign(&role.id, &alice).unwrap();
    services.role_assign(&role.id, &alice).unwrap();
    assert_eq!(
        code_of(services.role_assign(&role.id, "ghost").unwrap_err()),
        "NO_SUCH_USER"
    );
    let members = services.role_users(&role.id, 10, 0).unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].id, alice);
    services.role_unassign(&role.id, &alice).unwrap();
    assert!(services.role_users(&role.id, 10, 0).unwrap().is_empty());
    // unassign is a no-op for non-members.
    services.role_unassign(&role.id, &alice).unwrap();

    let updated = services
        .role_update(
            &role.id,
            "mods2",
            "d",
            None,
            None,
            "manual",
            serde_json::json!({}),
            false,
            true,
            false,
            2,
            serde_json::json!({"x": 1}),
        )
        .unwrap();
    assert_eq!(updated.name, "mods2");
    // Policies stored verbatim (not enforced — TODOS P2).
    assert_eq!(updated.policies, serde_json::json!({"x": 1}));

    services.role_delete(&role.id).unwrap();
    assert!(services.role_list().unwrap().is_empty());
    // delete is a no-op for missing ids.
    services.role_delete(&role.id).unwrap();
    assert_eq!(code_of(services.role_create("", "d", None, None, "manual", serde_json::json!({}), false, false, false, 0, serde_json::json!({})).unwrap_err()), "INVALID_PARAM");
}

#[test]
fn abuse_recipients_crud() {
    let (services, _) = test_services();
    let alice = make_user(&services, "alice");
    let email_recipient = services
        .abuse_recipient_create(true, "mailbox", "email", Some(alice.clone()), Some("abuse@example.local".to_string()), None)
        .unwrap();
    assert_eq!(services.abuse_recipient_list().unwrap().len(), 1);
    let shown = services.abuse_recipient_show(&email_recipient.id).unwrap();
    assert_eq!(shown.user_id, Some(alice));
    let updated = services
        .abuse_recipient_update(
            &email_recipient.id,
            false,
            "mailbox2",
            "email",
            None,
            Some("b@example.local".to_string()),
            None,
        )
        .unwrap();
    assert!(!updated.is_active);
    services.abuse_recipient_delete(&email_recipient.id).unwrap();
    assert!(services.abuse_recipient_list().unwrap().is_empty());
    assert_eq!(
        code_of(services.abuse_recipient_show(&email_recipient.id).unwrap_err()),
        "NO_SUCH_ABUSE_REPORT_NOTIFICATION_RECIPIENT"
    );
    // method validation.
    assert_eq!(
        code_of(
            services
                .abuse_recipient_create(true, "x", "sms", None, None, None)
                .unwrap_err()
        ),
        "INVALID_PARAM"
    );
}

#[test]
fn proxy_account_get_set_clear() {
    let (services, _) = test_services();
    let alice = make_user(&services, "alice");
    assert_eq!(services.proxy_account_get().unwrap(), None);
    services.proxy_account_set(Some(&alice)).unwrap();
    assert_eq!(services.proxy_account_get().unwrap(), Some(alice.clone()));
    services.proxy_account_set(None).unwrap();
    assert_eq!(services.proxy_account_get().unwrap(), None);
    assert_eq!(
        code_of(services.proxy_account_set(Some("ghost")).unwrap_err()),
        "NO_SUCH_USER"
    );
    // Remote accounts are refused as proxy.
    let remote = services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: "bob".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/bob".to_string()),
                inbox: Some("https://remote.example/users/bob/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .unwrap();
    assert_eq!(
        code_of(services.proxy_account_set(Some(&remote.id)).unwrap_err()),
        "INVALID_PARAM"
    );
}

#[test]
fn stats_server_info_queue_shapes() {
    let (services, clock) = test_services();
    make_user(&services, "alice");
    let stats = services.admin_stats_overview().unwrap();
    assert_eq!(stats.get("usersCount").and_then(|v| v.as_u64()), Some(1));
    assert!(stats.get("pendingJobs").is_some());
    assert!(stats.get("knownRemoteHosts").is_some());

    let info = services.admin_server_info();
    assert!(info.get("machine").is_some());
    assert!(info.get("cpu").is_some());

    let queue = services.queue_stats().unwrap();
    assert_eq!(
        queue.pointer("/deliver/waiting").and_then(|v| v.as_u64()),
        Some(0)
    );
    let err = services.queue_delayed_unsupported().unwrap_err();
    assert!(code_of(err) == "INVALID_PARAM");
    assert!(QUEUE_CONTROL_UNSUPPORTED_REASON.contains("unified queue"));

    // reset-db is a loud refusal, never a silent no-op.
    let err = services.refuse_reset_db().unwrap_err();
    let msg = match err {
        PortError::Validation(m) => m,
        other => panic!("unexpected: {other:?}"),
    };
    assert!(msg.contains(RESET_DB_REFUSED_REASON));

    // ping answers an empty object.
    assert_eq!(ping(), serde_json::json!({}));
    let _ = clock;
}

#[test]
fn reset_password_and_user_ips_and_find_by_email() {
    let (services, clock) = test_services();
    let alice = make_user(&services, "alice");
    let new_hash = pinetree::api::misskey::auth::hash_password("brand-new-password");
    services.admin_reset_password(&alice, &new_hash).unwrap();
    let stored = services.auth.get_password_hash(&alice).unwrap().unwrap();
    assert!(pinetree::api::misskey::auth::verify_password("brand-new-password", &stored));
    assert_eq!(
        code_of(services.admin_reset_password("ghost", &new_hash).unwrap_err()),
        "NO_SUCH_USER"
    );

    // Signin rows back get_user_ips.
    services
        .signin
        .record_signin(SigninRecord {
            id: services.ids.generate(),
            account_id: alice.clone(),
            ip: "10.0.0.7".to_string(),
            headers: "{}".to_string(),
            success: true,
            created_at_ms: clock.now_ms(),
        })
        .unwrap();
    let ips = services.get_user_ips(&alice, 10).unwrap();
    assert_eq!(ips.len(), 1);
    assert_eq!(ips[0].get("ip").and_then(|v| v.as_str()), Some("10.0.0.7"));
    assert_eq!(
        code_of(services.get_user_ips("ghost", 10).unwrap_err()),
        "NO_SUCH_USER"
    );

    // find_account_by_email: exact case-insensitive match.
    assert_eq!(services.find_account_by_email("a@example.local").unwrap(), None);
    services
        .accounts
        .update_account(
            &alice,
            UpdateAccountPatch {
                email: Some(Some("Alice@Example.Local".to_string())),
                ..Default::default()
            },
            Vec::new(),
        )
        .unwrap();
    let found = services.find_account_by_email("alice@example.local").unwrap().unwrap();
    assert_eq!(found.id, alice);
}

#[test]
fn federation_instances_update_refresh() {
    let (services, _) = test_services();
    make_user(&services, "local");
    // One cached remote account.
    services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: "bob".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/bob".to_string()),
                inbox: Some("https://remote.example/users/bob/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .unwrap();
    let instances = services.federation_instances().unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].get("host").and_then(|v| v.as_str()), Some("remote.example"));
    assert_eq!(instances[0].get("usersCount").and_then(|v| v.as_u64()), Some(1));

    let updated = services
        .federation_update_instance("remote.example", Some(true), None, None, Some("spam".to_string()))
        .unwrap();
    assert!(updated.is_suspended);
    let shown = services.federation_show_instance("REMOTE.EXAMPLE").unwrap().unwrap();
    assert_eq!(shown.get("isSuspended").and_then(|v| v.as_bool()), Some(true));
    assert!(services.federation_show_instance("unknown.example").unwrap().is_none());

    // refresh touches the override (204 semantics); unknown hosts are no-ops.
    services.federation_refresh_metadata("remote.example").unwrap();
    services.federation_refresh_metadata("unknown.example").unwrap();
}

#[test]
fn push_subscriptions_crud() {
    let (services, _) = test_services();
    let alice = make_user(&services, "alice");
    let endpoint = "https://push.example/sub/1";
    let sub = services.sw_register(&alice, endpoint, "auth-key", "pub-key").unwrap();
    assert_eq!(sub.endpoint, endpoint);
    assert_eq!(services.sw_show(&alice, endpoint).unwrap().auth, "auth-key");
    let updated = services.sw_update(&alice, endpoint, "auth2", "pub2").unwrap();
    assert_eq!(updated.auth, "auth2");
    services.sw_unregister(&alice, endpoint).unwrap();
    assert_eq!(
        code_of(services.sw_show(&alice, endpoint).unwrap_err()),
        "NO_SUCH_SUBSCRIPTION"
    );
    // missing unregister is a no-op.
    services.sw_unregister(&alice, endpoint).unwrap();
    assert_eq!(
        code_of(services.sw_register(&alice, "http://insecure.example/x", "a", "p").unwrap_err()),
        "INVALID_PARAM"
    );
}

#[test]
fn retention_roundtrip_and_endpoints_inventory() {
    let (services, _) = test_services();
    assert_eq!(services.retention_get().unwrap(), serde_json::json!({}));
    let policy = serde_json::json!({"keepDays": 90});
    assert_eq!(services.retention_set(policy.clone()).unwrap(), policy);
    assert_eq!(services.retention_get().unwrap(), policy);
    assert_eq!(
        code_of(services.retention_set(serde_json::json!([])).unwrap_err()),
        "INVALID_PARAM"
    );

    // endpoint(s) inventory covers every WP2-part-C arm; unknown names 404.
    for name in [
        "i/2fa/register",
        "i/2fa/remove-key",
        "i/2fa/authenticate",
        "my/apps",
        "app/create",
        "app/show",
        "ping",
        "retention",
        "reset-db",
        "sw/register",
        "v2/admin/emoji/list",
        "endpoint",
        "endpoints",
        "admin/roles/assign",
        "admin/queue/stats",
        "admin/relays/add",
        "admin/ad/refresh",
        "admin/announcements/delete",
        "admin/federation/update-instance",
        "admin/stats",
        "admin/get-user-ips",
        "admin/accounts/find-by-email",
        "admin/server-info",
        "admin/update-proxy-account",
        "admin/unset-mfa",
        "admin/reset-password",
        "federation/instances",
        "federation/find-by-email",
        "federation/get-user-ips",
        "queue/stats",
        "abuse-report-notification-recipient/create",
    ] {
        assert!(ENDPOINT_NAMES.contains(&name), "{name}");
        assert_eq!(
            services.endpoint_describe(name).unwrap().get("name").and_then(|v| v.as_str()),
            Some(name)
        );
    }
    assert_eq!(
        code_of(services.endpoint_describe("drive/folders/create").unwrap_err()),
        "NO_SUCH_ENDPOINT"
    );

    // v2 emoji list: empty store answers empty pages on both branches.
    let cursor = TimelineCursor {
        since_id: None,
        until_id: None,
        since_date: None,
        until_date: None,
        limit: 10,
    };
    let page = services.list_emojis_v2(None, None, &cursor).unwrap();
    assert!(matches!(page, pinetree::application::commands::misc_endpoints::EmojisV2Page::Local(ref rows) if rows.is_empty()));
    let page = services.list_emojis_v2(None, Some("remote.example"), &cursor).unwrap();
    assert!(matches!(page, pinetree::application::commands::misc_endpoints::EmojisV2Page::Remote(ref rows) if rows.is_empty()));
}
