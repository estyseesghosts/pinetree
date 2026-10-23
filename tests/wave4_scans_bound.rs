//! Wave 4 (Agent scans and counters) liveness: hot paths must do bounded
//! work — maintained counters instead of full-table decodes, cursor-paged
//! sweeps instead of unbounded `Vec`s, index reads instead of 10k scans,
//! and a singleflight + budget gate around NodeInfo probes.
//!
//! - `TimelineCursor::with_limit` clamps to `MAX_LIST_LIMIT`; the redb
//!   `list_all_notes` enforces the same bound for literal cursors.
//! - `count_local_notes`, `count_local_accounts`, and `pending_count` are
//!   O(1) maintained counters (create/delete/suspend/settle/quarantine
//!   transitions keep them exact).
//! - NodeInfo renders from counters and memoizes the payload briefly.
//! - Shared-inbox and follower sweeps page via resume cursors;
//!   renote/reply fan-out pages indexes; reactors resolve through a bounded
//!   id list.
//! - `NodeinfoProbeGate` singleflights one host and sheds excess probes.
//!
//! Fails on old code: `with_limit(10_000)` admitted 10_000 (no clamp),
//! `list_all_notes` with 1_050 stored notes decoded all 1_050 rows,
//! `nodeinfo.rs` / `recipients.rs` production code contained
//! `list_all_notes` scans (pinned by the source-shape asserts below), and
//! the page/index/counter/gate APIs did not exist.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pinetree::activitypub::webfinger::{cached_nodeinfo, render_nodeinfo};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, CreateAccountInput, CreateNoteInput, JobKind, JobQueue, NewJob,
    NoopStreamingSink, NoteStore, ReactionStore, TimelineCursor,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::host_caps::{
    NODEINFO_PROBE_BUDGET_PER_MIN, NODEINFO_PROBE_MAX_IN_FLIGHT, NodeinfoProbeGate,
};
use pinetree::federation::outbox::{
    INTERACTED_AUDIENCE_CAP, SUSPEND_SWEEP_PAGE_ROWS, collect_follower_id_page,
    collect_known_shared_inboxes, collect_note_interacted_ids, collect_shared_inbox_page,
};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
static SEQ: AtomicUsize = AtomicUsize::new(0);

fn open_backend() -> (
    Arc<RedbStore>,
    Arc<FsBlobStore>,
    Arc<FsBlobStore>,
    Arc<ManualClock>,
) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-w4scans-{n}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock = Arc::new(ManualClock::new(BASE_MS));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    (store, cache, durable, clock)
}

fn services_for(
    store: &Arc<RedbStore>,
    cache: &Arc<FsBlobStore>,
    durable: &Arc<FsBlobStore>,
    clock: &Arc<ManualClock>,
) -> Arc<AppServices> {
    let clock_dyn: Arc<dyn Clock> = clock.clone();
    Arc::new(AppServices {
        clock: clock_dyn,
        ids: store.ids().clone(),
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
        blobs: durable.clone(),
        cache_blobs: cache.clone(),
        notifications: store.clone(),
        auth: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
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
        folders: store.clone(),
        system_webhooks: store.clone(),
        user_webhooks: store.clone(),
        abuse_recipients: store.clone(),
        memos: store.clone(),
        thread_mutes: store.clone(),
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "https://pinetree.local".to_string(),
        max_file_size: 10_000_000,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    })
}

fn remote_account(store: &Arc<RedbStore>, tag: &str, host: &str, inbox: Option<&str>) {
    store
        .create_account(
            CreateAccountInput {
                handle: tag.to_string(),
                host: Some(host.to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://{host}/users/{tag}")),
                inbox: Some(
                    inbox
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("https://{host}/users/{tag}/inbox")),
                ),
                shared_inbox: Some(format!("https://{host}/shared")),
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
                also_known_as: None,
                moved_to_uri: None,
                moved_at_ms: None,
            },
            None,
        )
        .unwrap_or_else(|e| panic!("remote account {tag}: {e:?}"));
}

fn production_source(path: &str) -> String {
    let src = std::fs::read_to_string(path).unwrap();
    let test_offset = src.find("#[cfg(test)]").unwrap_or(src.len());
    let (production, _) = src.split_at(test_offset);
    production
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn wave4_cursor_limits_clamp_at_the_port() {
    assert_eq!(
        TimelineCursor::with_limit(10_000).limit,
        TimelineCursor::MAX_LIST_LIMIT,
        "a 10k request must clamp to the port bound, not decode 10k rows"
    );
    assert_eq!(
        TimelineCursor::with_limit(0).limit,
        0,
        "0 keeps backend default"
    );
    assert_eq!(TimelineCursor::clamp_limit(50), 50);
    assert_eq!(
        TimelineCursor::clamp_limit(usize::MAX),
        TimelineCursor::MAX_LIST_LIMIT
    );
    assert!(
        TimelineCursor::MAX_LIST_LIMIT <= 1_000,
        "bound must be small enough to matter, got {}",
        TimelineCursor::MAX_LIST_LIMIT
    );
}

#[test]
fn wave4_list_all_notes_never_decodes_more_than_one_page() {
    let (store, cache, durable, clock) = open_backend();
    let services = services_for(&store, &cache, &durable, &clock);
    let author = services.create_account("w4scanwriter", None).unwrap();
    for i in 0..1_050 {
        let mut input =
            CreateNoteInput::simple_public(&author.id, format!("bounded scan note {i}"));
        input.uri_base = String::new();
        store.create_note(input, Vec::new()).unwrap();
    }
    // A literal giant limit clamps at the backend: one page, not 1_050 rows.
    let page = store
        .list_all_notes(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        page.len(),
        TimelineCursor::MAX_LIST_LIMIT,
        "old code decoded all 1_050 rows for a 10k limit"
    );
    // Depth comes from paging, not bigger limits.
    let mut cursor = TimelineCursor::with_limit(500);
    let first = store.list_all_notes(&cursor).unwrap();
    assert_eq!(first.len(), 500);
    cursor.until_id = first.last().map(|n| n.id.clone());
    let second = store.list_all_notes(&cursor).unwrap();
    assert_eq!(second.len(), 500);
    assert!(
        second
            .iter()
            .all(|n| Some(&n.id) < first.last().map(|f| &f.id)),
        "second page must sit strictly below the first"
    );
}

#[test]
fn wave4_local_counters_track_create_delete_suspend() {
    let (store, cache, durable, clock) = open_backend();
    let services = services_for(&store, &cache, &durable, &clock);
    assert_eq!(store.count_local_notes().unwrap(), 0);
    assert_eq!(services.accounts.count_local_accounts().unwrap(), 0);

    let alice = services.create_account("w4alice", None).unwrap();
    let _bob = services.create_account("w4bob", None).unwrap();
    remote_account(&store, "w4remote", "remote.example", None);
    assert_eq!(services.accounts.count_local_accounts().unwrap(), 2);

    let n1 = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "one"))
        .unwrap();
    let n2 = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "two"))
        .unwrap();
    let remote = store
        .find_by_username_and_host("w4remote", Some("remote.example"))
        .unwrap()
        .unwrap();
    let mut rinput = CreateNoteInput::simple_public(&remote.id, "remote note");
    rinput.uri_base = "https://remote.example".to_string();
    services.create_note(rinput).unwrap();
    assert_eq!(store.count_local_notes().unwrap(), 2);
    assert_eq!(store.count_notes().unwrap(), 3);

    // Delete debits exactly once; double delete is NotFound, not a debit.
    services.delete_note(&alice.id, &n1.id).unwrap();
    assert_eq!(store.count_local_notes().unwrap(), 1);
    assert!(services.delete_note(&alice.id, &n1.id).is_err());
    assert_eq!(store.count_local_notes().unwrap(), 1);

    // Suspend / unsuspend / delete move the user counter with the same
    // predicate NodeInfo renders (local, live, unsuspended).
    let moderator = services.create_account("w4mod", None).unwrap();
    services.set_role(None, &moderator.id, false, true).unwrap();
    assert_eq!(services.accounts.count_local_accounts().unwrap(), 3);
    services.suspend_user(&moderator.id, &alice.id).unwrap();
    assert_eq!(services.accounts.count_local_accounts().unwrap(), 2);
    // Repeat suspends are no-change: no double debit.
    services.suspend_user(&moderator.id, &alice.id).unwrap();
    assert_eq!(services.accounts.count_local_accounts().unwrap(), 2);
    services.unsuspend_user(&moderator.id, &alice.id).unwrap();
    assert_eq!(services.accounts.count_local_accounts().unwrap(), 3);
    services
        .delete_account(Some(moderator.id.as_str()), &alice.id, None)
        .unwrap();
    assert_eq!(services.accounts.count_local_accounts().unwrap(), 2);
    // Deleting an already-deleted account must not debit again.
    services
        .delete_account(Some(moderator.id.as_str()), &alice.id, None)
        .unwrap();
    assert_eq!(services.accounts.count_local_accounts().unwrap(), 2);

    // The second note still counts (account deletion tombstones notes
    // author-side; the counter only moves on note delete).
    assert_eq!(store.count_local_notes().unwrap(), 1);
    services.delete_note(&alice.id, &n2.id).unwrap();
    assert_eq!(store.count_local_notes().unwrap(), 0);
}

#[test]
fn wave4_nodeinfo_renders_from_counters_and_caches_briefly() {
    let (store, cache, durable, clock) = open_backend();
    let services = services_for(&store, &cache, &durable, &clock);
    let alice = services.create_account("w4nialice", None).unwrap();
    let remote_tag = "w4niremote";
    remote_account(&store, remote_tag, "niremote.example", None);
    for i in 0..3 {
        services
            .create_note(CreateNoteInput::simple_public(
                &alice.id,
                format!("local {i}"),
            ))
            .unwrap();
    }
    let remote = store
        .find_by_username_and_host(remote_tag, Some("niremote.example"))
        .unwrap()
        .unwrap();
    let mut rinput = CreateNoteInput::simple_public(&remote.id, "remote");
    rinput.uri_base = "https://niremote.example".to_string();
    services.create_note(rinput).unwrap();

    let doc = render_nodeinfo(&services, "2.1");
    assert_eq!(doc["usage"]["users"]["total"], 1);
    assert_eq!(doc["usage"]["localPosts"], 3);

    // Cached path agrees while the clock holds still, and refreshes past
    // the TTL window.
    let first = cached_nodeinfo(&services, "2.0");
    assert_eq!(first["usage"]["localPosts"], 3);
    services
        .create_note(CreateNoteInput::simple_public(&alice.id, "fourth"))
        .unwrap();
    let stale = cached_nodeinfo(&services, "2.0");
    assert_eq!(
        stale["usage"]["localPosts"], 3,
        "within the TTL window the memoized payload is served"
    );
    clock.advance_ms(61_000);
    let fresh = cached_nodeinfo(&services, "2.0");
    assert_eq!(fresh["usage"]["localPosts"], 4);

    // Structural pin: NodeInfo production code must not scan tables.
    let src = production_source("src/activitypub/webfinger/nodeinfo.rs");
    assert!(
        src.contains("count_local_notes"),
        "NodeInfo must read the note counter"
    );
    assert!(
        src.contains("count_local_accounts"),
        "NodeInfo must read the account counter"
    );
    assert!(
        !src.contains("list_all_notes"),
        "old NodeInfo decoded up to 10_000 note rows per scrape"
    );
    assert!(
        !src.contains("search_accounts"),
        "old NodeInfo decoded up to 10_000 account rows per scrape"
    );
}

#[test]
fn wave4_pending_count_is_a_counter_not_a_decode() {
    let (store, _cache, _durable, clock) = open_backend();
    let now = clock.now_ms();
    assert_eq!(store.pending_count().unwrap(), 0);
    let payload = |key: &str| {
        NewJob::new_json(JobKind::Notify, &serde_json::json!({"k": key}), key, now).unwrap()
    };
    store
        .enqueue_jobs(vec![payload("w4a"), payload("w4b"), payload("w4c")])
        .unwrap();
    assert_eq!(store.pending_count().unwrap(), 3);

    // Claim does not settle; complete and permanent-fail do.
    let claimed = store.claim_next(60_000).unwrap().unwrap();
    assert_eq!(store.pending_count().unwrap(), 3);
    store.complete(&claimed.id).unwrap();
    assert_eq!(store.pending_count().unwrap(), 2);

    let claimed = store.claim_next(60_000).unwrap().unwrap();
    store.fail_permanent(&claimed.id, "w4-gone").unwrap();
    assert_eq!(store.pending_count().unwrap(), 1);
    // Repeating a terminal settle is a no-op: the counter never double-debits.
    store.fail_permanent(&claimed.id, "w4-gone").unwrap();
    store.complete(&claimed.id).unwrap();
    assert_eq!(store.pending_count().unwrap(), 1);

    // Exhaustion quarantines out of the pending set but keeps the row.
    let last = store.claim_next(60_000).unwrap().unwrap();
    for _ in 0..8 {
        store.fail(&last.id, now + 1_000).unwrap();
    }
    assert_eq!(store.pending_count().unwrap(), 0);
    let row = store.get_job(&last.id).unwrap().unwrap();
    assert!(row.failed, "quarantined rows stay inspectable");

    // Followup settle replaces the parent with its children exactly.
    store.enqueue_jobs(vec![payload("w4parent")]).unwrap();
    assert_eq!(store.pending_count().unwrap(), 1);
    let parent = store.claim_next(60_000).unwrap().unwrap();
    store
        .complete_with_followups(&parent.id, vec![payload("w4child-a"), payload("w4child-b")])
        .unwrap();
    assert_eq!(store.pending_count().unwrap(), 2);

    // Structural pin: no full decode behind pending_count.
    let src = production_source("src/persistence/redb/job_queue.rs");
    assert!(
        src.contains("counter_get"),
        "pending_count must read the counter"
    );
}

#[test]
fn wave4_renote_index_serves_fanout_without_full_scan() {
    let (store, cache, durable, clock) = open_backend();
    let services = services_for(&store, &cache, &durable, &clock);
    let author = services.create_account("w4rtauthor", None).unwrap();
    let replier = services.create_account("w4rtreplier", None).unwrap();
    let renoter = services.create_account("w4rtrenoter", None).unwrap();
    let reactor_remote = "w4rtreactor";
    remote_account(&store, reactor_remote, "rtreact.example", None);
    let reactor = store
        .find_by_username_and_host(reactor_remote, Some("rtreact.example"))
        .unwrap()
        .unwrap();

    let target = services
        .create_note(CreateNoteInput::simple_public(&author.id, "renote target"))
        .unwrap();
    let mut reply_input = CreateNoteInput::simple_public(&replier.id, "a reply");
    reply_input.reply_to = Some(target.id.clone());
    services.create_note(reply_input).unwrap();
    let mut renote_input = CreateNoteInput::simple_public(&renoter.id, "");
    renote_input.text = None;
    renote_input.renote_of = Some(target.id.clone());
    let renote = services.create_note(renote_input).unwrap();
    store
        .react(&reactor.id, &target.id, "❤", "w4react1", false, Vec::new())
        .unwrap();

    // Index read: newest-first, live only, cursor-paged.
    let found = store
        .notes_by_renote(&target.id, &TimelineCursor::with_limit(100))
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, renote.id);

    // The interacted audience resolves through indexes + bounded id list.
    let interacted = collect_note_interacted_ids(&services, &target).unwrap();
    assert!(interacted.contains(&replier.id), "replier included");
    assert!(interacted.contains(&renoter.id), "renoter included");
    assert!(interacted.contains(&reactor.id), "reactor included");
    assert!(!interacted.contains(&author.id), "author excluded");
    assert!(
        interacted.len() <= INTERACTED_AUDIENCE_CAP,
        "audience stays bounded"
    );

    // Delete drops the edge; the audience converges without the scan.
    services.delete_note(&renoter.id, &renote.id).unwrap();
    let found = store
        .notes_by_renote(&target.id, &TimelineCursor::with_limit(100))
        .unwrap();
    assert!(found.is_empty());
    let interacted = collect_note_interacted_ids(&services, &target).unwrap();
    assert!(!interacted.contains(&renoter.id));

    // Structural pin: fan-out audience code must not scan notes/reactions.
    let src = production_source("src/federation/outbox/recipients.rs");
    assert!(
        src.contains("notes_by_renote"),
        "renote fan-out must page the index"
    );
    assert!(
        src.contains("reactor_ids_for_note"),
        "reactor fan-out must use the bounded id list"
    );
    assert!(
        !src.contains("list_all_notes"),
        "old fan-out scanned up to 10_000 notes per edited note"
    );
    assert!(
        !src.contains("reactions_for_note"),
        "old fan-out loaded every reaction row per edited note"
    );
}

#[test]
fn wave4_sweep_and_follower_expansion_page_by_cursor() {
    let (store, cache, durable, clock) = open_backend();
    let services = services_for(&store, &cache, &durable, &clock);
    let author = services.create_account("w4swauthor", None).unwrap();
    // 30 remote accounts across 6 shared inboxes plus 2 distinct inboxes.
    for i in 0..30 {
        let tag = format!("w4swpeer{i:02}");
        let host = format!("w4sw{}.example", i % 6);
        store
            .create_account(
                CreateAccountInput {
                    handle: tag.clone(),
                    host: Some(host.clone()),
                    display_name: None,
                    description: None,
                    location: None,
                    birthday: None,
                    uri: Some(format!("https://{host}/users/{tag}")),
                    inbox: Some(format!("https://{host}/users/{tag}/inbox")),
                    shared_inbox: Some(format!("https://{host}/shared")),
                    is_bot: false,
                    is_cat: false,
                    is_locked: false,
                    preserved_usernames: Vec::new(),
                    also_known_as: None,
                    moved_to_uri: None,
                    moved_at_ms: None,
                },
                None,
            )
            .unwrap();
    }
    remote_account(
        &store,
        "w4swsolo",
        "w4swsolo.example",
        Some("https://w4swsolo.example/in"),
    );

    // Follower pages chain to the full set with no truncation.
    for i in 0..7 {
        let tag = format!("w4swfan{i}");
        let fan = services.create_account(&tag, None).unwrap();
        services
            .follows
            .follow(&fan.id, &author.id, false, None, Vec::new())
            .unwrap();
    }
    let mut walked = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) =
            collect_follower_id_page(&services, &author.id, cursor.as_deref(), 3).unwrap();
        assert!(page.len() <= 3, "pages stay bounded");
        walked.extend(page);
        match next {
            None => break,
            Some(after) => cursor = Some(after),
        }
    }
    assert_eq!(walked.len(), 7, "every follower visited exactly once");

    // Shared-inbox pages chain and match the whole-set helper.
    let mut pages: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut page_count = 0;
    loop {
        let (page, next) =
            collect_shared_inbox_page(&services, cursor.as_deref(), SUSPEND_SWEEP_PAGE_ROWS)
                .unwrap();
        assert!(page.len() <= SUSPEND_SWEEP_PAGE_ROWS);
        // In-page dedupe: 30 peers collapse to 6 shared inboxes + 1 solo.
        let mut deduped = page.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(page.len(), deduped.len());
        page_count += 1;
        pages.extend(page);
        match next {
            None => break,
            Some(after) => cursor = Some(after),
        }
    }
    assert_eq!(page_count, 1, "31 accounts fit one 500-row page");
    let mut pages_sorted = pages.clone();
    pages_sorted.sort();
    let mut whole = collect_known_shared_inboxes(&services).unwrap();
    whole.sort();
    assert_eq!(pages_sorted, whole, "paged walk covers the whole set");
    assert_eq!(whole.len(), 7, "6 shared + 1 solo inbox");
}

#[test]
fn wave4_nodeinfo_probe_gate_singleflights_and_budgets() {
    // Concurrent probes for one host share a single fetch.
    let gate = Arc::new(NodeinfoProbeGate::new(
        NODEINFO_PROBE_MAX_IN_FLIGHT,
        NODEINFO_PROBE_BUDGET_PER_MIN,
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let gate_ref = gate.clone();
        let calls_ref = calls.clone();
        handles.push(std::thread::spawn(move || {
            gate_ref.probe("single.example", BASE_MS, || {
                calls_ref.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(100));
                Some("misskey".to_string())
            })
        }));
    }
    let results: Vec<Option<String>> = handles
        .into_iter()
        .map(|h| h.join().expect("probe thread"))
        .collect();
    assert!(
        results.iter().all(|r| r.as_deref() == Some("misskey")),
        "every waiter shares the leader result"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "one fetch per host");

    // A zero budget sheds the probe without fetching (uncached failure).
    let starved = NodeinfoProbeGate::new(8, 0);
    let fetched = Arc::new(AtomicUsize::new(0));
    let fetched_ref = fetched.clone();
    let outcome = starved.probe("shed.example", BASE_MS, || {
        fetched_ref.fetch_add(1, Ordering::SeqCst);
        Some("mastodon".to_string())
    });
    assert_eq!(outcome, None);
    assert_eq!(
        fetched.load(Ordering::SeqCst),
        0,
        "no fetch past the budget"
    );

    // In-flight saturation sheds other hosts without fetching.
    let narrow = Arc::new(NodeinfoProbeGate::new(1, 60));
    let release = Arc::new(std::sync::Barrier::new(2));
    let release_ref = release.clone();
    let narrow_ref = narrow.clone();
    let leader = std::thread::spawn(move || {
        narrow_ref.probe("busy.example", BASE_MS, || {
            release_ref.wait();
            Some("akkoma".to_string())
        })
    });
    // Wait until the leader is inside its fetch.
    std::thread::sleep(std::time::Duration::from_millis(50));
    let other_fetched = Arc::new(AtomicUsize::new(0));
    let other_ref = other_fetched.clone();
    let shed = narrow.probe("other.example", BASE_MS, || {
        other_ref.fetch_add(1, Ordering::SeqCst);
        Some("pleroma".to_string())
    });
    assert_eq!(shed, None, "saturated gate sheds without fetching");
    assert_eq!(other_fetched.load(Ordering::SeqCst), 0);
    release.wait();
    assert_eq!(leader.join().expect("leader").as_deref(), Some("akkoma"));

    // The per-minute budget refills in the next window.
    let metered = NodeinfoProbeGate::new(8, 1);
    assert_eq!(
        metered
            .probe("w1.example", BASE_MS, || Some("a".to_string()))
            .as_deref(),
        Some("a")
    );
    assert_eq!(
        metered.probe("w2.example", BASE_MS, || Some("b".to_string())),
        None
    );
    assert_eq!(
        metered
            .probe("w2.example", BASE_MS + 60_000, || Some("b".to_string()))
            .as_deref(),
        Some("b"),
        "new window refills the budget"
    );
}
