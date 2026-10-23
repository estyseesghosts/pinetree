//! Wave 4 (Agent fan-out) liveness: a 100-inbox activity job must split into
//! one-inbox follow-ups instead of holding the only worker for 100
//! sequential blocking POSTs (which would starve `Notify`,
//! `RemoteMediaFetch`, and `PollExpiry`).
//!
//! - `FANOUT_INLINE_MAX` is 10–20 (was 100): `fanout_is_split(100)` is true.
//! - A 100-remote-shared-inbox `SuspendBroadcast` job executed through
//!   `execute_federate_deliver` returns `Split` with exactly 100 follow-ups
//!   (derived keys `{job_key}:{inbox}`) and finishes well under
//!   `FANOUT_JOB_DEADLINE_MS` with zero inline POSTs.
//!
//! Fails on old code: with `FANOUT_INLINE_MAX == 100`, 100 inboxes are not
//! split, so the job attempts 100 inline signed POSTs; every send fails
//! closed (unroutable hosts) and the outcome is `Delivered` with zero
//! follow-ups after paying for 100 round trips.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, CreateAccountInput, JobKind, NoopStreamingSink,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::outbox::{
    FANOUT_INLINE_CONCURRENCY, FANOUT_INLINE_MAX, FANOUT_JOB_DEADLINE_MS, FederateDeliverPayload,
    execute_federate_deliver, fanout_is_split,
};
use pinetree::federation::retry::{DeliveryOutcome, DestinationTracker};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

const BASE_MS: i64 = 1_760_400_000_000;
const INBOXES: usize = 100;
static SEQ: AtomicU64 = AtomicU64::new(0);

fn open_backend() -> (
    Arc<RedbStore>,
    Arc<FsBlobStore>,
    Arc<FsBlobStore>,
    Arc<dyn Clock>,
) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-w4fanout-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(BASE_MS));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    (store, cache, durable, clock)
}

fn services_for(
    store: &Arc<RedbStore>,
    cache: &Arc<FsBlobStore>,
    durable: &Arc<FsBlobStore>,
    clock: &Arc<dyn Clock>,
) -> Arc<AppServices> {
    Arc::new(AppServices {
        clock: clock.clone(),
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

#[test]
fn wave4_inline_threshold_is_ten_to_twenty_and_splits_a_hundred_inboxes() {
    assert!(
        (10..=20).contains(&FANOUT_INLINE_MAX),
        "inline threshold must be 10-20, got {FANOUT_INLINE_MAX}"
    );
    assert!(
        FANOUT_INLINE_CONCURRENCY >= 2 && FANOUT_INLINE_CONCURRENCY <= 8,
        "inline wave width must stay a small cap, got {FANOUT_INLINE_CONCURRENCY}"
    );
    assert!(
        FANOUT_JOB_DEADLINE_MS > 0,
        "inline phase needs a positive wall-clock deadline"
    );
    assert!(!fanout_is_split(0), "empty fan-out never splits");
    assert!(
        !fanout_is_split(FANOUT_INLINE_MAX),
        "exactly-at-threshold stays inline"
    );
    assert!(
        fanout_is_split(FANOUT_INLINE_MAX + 1),
        "one past the threshold splits"
    );
    assert!(
        fanout_is_split(INBOXES),
        "a 100-inbox job must split, not run 100 inline POSTs"
    );
}

#[test]
fn wave4_hundred_inbox_suspend_broadcast_splits_under_deadline() {
    let (store, cache, durable, clock) = open_backend();
    let services = services_for(&store, &cache, &durable, &clock);

    let author = services.create_account("w4author", None).unwrap();
    for i in 0..INBOXES {
        let tag = format!("w4peer{i:03}");
        store
            .create_account(
                CreateAccountInput {
                    handle: tag.clone(),
                    host: Some("w4fanout.example".to_string()),
                    display_name: None,
                    description: None,
                    location: None,
                    birthday: None,
                    uri: Some(format!("https://w4fanout.example/users/{tag}")),
                    inbox: Some(format!("https://w4fanout.example/users/{tag}/inbox")),
                    shared_inbox: Some(format!("https://w4fanout.example/shared-inbox-{i:03}")),
                    is_bot: false,
                    is_cat: false,
                    is_locked: false,
                    also_known_as: None,
                    moved_to_uri: None,
                    moved_at_ms: None,
                    preserved_usernames: Vec::new(),
                },
                None,
            )
            .unwrap_or_else(|e| panic!("remote account {tag}: {e:?}"));
    }

    let payload = FederateDeliverPayload::SuspendBroadcast {
        account_id: author.id.clone(),
        activity_id: "https://pinetree.local/activities/w4fanout".to_string(),
        undo: false,
        inbox_override: None,
        after: None,
    };
    let job = payload
        .into_job("wave4-fanout", clock.now_ms())
        .expect("suspend job");
    assert_eq!(job.kind, JobKind::FederateDeliver);

    let tracker = DestinationTracker::new();
    let started = Instant::now();
    let outcome = execute_federate_deliver(
        &services,
        &services.federation_http,
        &tracker,
        "wave4-fanout",
        &job.payload,
    )
    .expect("execute");
    let elapsed_ms = started.elapsed().as_millis();

    let followups = match outcome {
        DeliveryOutcome::Split { followups } => followups,
        other => panic!("100-inbox job must split without inline POSTs, got: {other:?}"),
    };
    assert_eq!(
        followups.len(),
        INBOXES,
        "split must carry one follow-up per inbox"
    );
    assert!(
        elapsed_ms < FANOUT_JOB_DEADLINE_MS as u128,
        "split of {INBOXES} inboxes must finish under the {FANOUT_JOB_DEADLINE_MS}ms job deadline, took {elapsed_ms}ms"
    );
    for job in &followups {
        assert_eq!(job.kind, JobKind::FederateDeliver);
        let decoded: FederateDeliverPayload =
            serde_json::from_slice(&job.payload).expect("follow-up payload round-trips");
        match decoded {
            FederateDeliverPayload::SuspendBroadcast {
                account_id,
                inbox_override: Some(inbox),
                ..
            } => {
                assert_eq!(account_id, author.id);
                assert_eq!(job.idempotency_key, format!("wave4-fanout:{inbox}"));
            }
            other => panic!("follow-up must stay a one-inbox sweep, got: {other:?}"),
        }
    }
}
