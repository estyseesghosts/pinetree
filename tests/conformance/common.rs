pub use std::collections::HashSet;

pub use std::path::{Path, PathBuf};

pub use std::process::{Child, Command, Stdio};

pub use std::sync::Arc;

pub use std::sync::atomic::{AtomicU64, Ordering};

pub use std::time::{Duration, Instant};

pub use pinetree::application::commands::AppServices;

pub use pinetree::application::ports::{
    AccountStore, ActivityDedup, AntennaNoteStore, AntennaStore, BlobStore, BlockStore, ClipStore,
    ConsumeInviteInput, CreateAccountInput, CreateInviteInput, CreateNoteInput, EditNoteInput,
    EmojiStore, FavoriteStore, FollowStore, InsertMediaInput, InstanceSettingsPatch,
    InstanceSettingsStore, InviteStore, JobKind, JobQueue, MAX_JOB_ATTEMPTS, MAX_JOB_PAYLOAD_BYTES,
    MAX_JOBS_PER_TXN, MAX_SIGNIN_ROWS_PER_USER, MediaStore, ModerationLogFilter,
    ModerationLogStore, MuteStore, NewJob, NoopStreamingSink, NoteStore, NotificationStore,
    PageStore, PasswordResetRequest, PasswordResetStore, PollExpiryPayload, PollStore, PortError,
    ReactionStore, RegistryItem, RegistryStore, ReplyParentResolvePayload, ScheduledDraftPayload,
    SigninRecord, SigninStore, StreamingSink, TimelineCursor, UpdateAccountPatch, UserListStore,
};

pub use pinetree::domain::clip::{
    CLIP_LIMIT_PER_OWNER, CLIP_NOTES_PER_CLIP, Clip, ClipFavorite, ClipFollow, ClipNote,
};

pub use pinetree::domain::collection::CollectionTier;

pub use pinetree::domain::draft::NoteDraft;

pub use pinetree::domain::favorite::NoteFavorite;

pub use pinetree::domain::id::{Clock, ManualClock};

pub use pinetree::domain::instance::RegistrationPolicy;

pub use pinetree::domain::moderation::{ModerationAction, ModerationLogEntry};

pub use pinetree::domain::note::{Note, Visibility};

pub use pinetree::domain::page::{Page, PageFont, PageLike};

pub use pinetree::domain::pinning::UserNotePining;

pub use pinetree::domain::policy::{
    ViewerContext, is_eligible_for_federated_timeline, is_eligible_for_federation_delivery,
    is_eligible_for_local_timeline, is_visible,
};

pub use pinetree::domain::poll::{PollInput, PollVote};

pub use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};

pub use pinetree::federation::outbox::FederateDeliverPayload;

pub use pinetree::federation::retry::DestinationTracker;

pub use pinetree::jobs::worker::JobWorker;

pub use pinetree::media::storage::FsBlobStore;

pub use pinetree::media::transcode::TranscodeConfig;

pub use pinetree::persistence::redb::RedbStore;

pub use pinetree::services::email::{SmtpConfig, SmtpEmailSender};

// ---------------------------------------------------------------------------
// Factory abstraction (Phase 4 reuses the generic cases with an FDB factory)
// ---------------------------------------------------------------------------

/// Every port the conformance suite drives.
pub trait ConformanceStore:
    AccountStore
    + NoteStore
    + JobQueue
    + FollowStore
    + ActivityDedup
    + MediaStore
    + BlockStore
    + MuteStore
    + ReactionStore
    + FavoriteStore
    + EmojiStore
    + AntennaStore
    + AntennaNoteStore
    + PollStore
    + ModerationLogStore
    + InstanceSettingsStore
    + InviteStore
{
}

impl<T> ConformanceStore for T where
    T: AccountStore
        + NoteStore
        + JobQueue
        + FollowStore
        + ActivityDedup
        + MediaStore
        + BlockStore
        + MuteStore
        + ReactionStore
        + FavoriteStore
        + EmojiStore
        + AntennaStore
        + AntennaNoteStore
        + PollStore
        + ModerationLogStore
        + InstanceSettingsStore
        + InviteStore
{
}

/// Opens fresh and reopened store handles for one isolated case.
pub trait StoreFactory {
    type Store: ConformanceStore;

    fn open(&self, clock: Arc<ManualClock>) -> Arc<Self::Store>;
    fn reopen(&self, clock: Arc<ManualClock>) -> Arc<Self::Store>;
}

/// redb factory: one temp dir + file per case.
pub struct RedbFactory {
    path: PathBuf,
}

impl RedbFactory {
    pub fn fresh(prefix: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-conf-{}-{}-{n}",
            std::process::id(),
            prefix
        ));
        std::fs::create_dir_all(&dir).expect("create conformance temp dir");
        Self {
            path: dir.join("store.redb"),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl StoreFactory for RedbFactory {
    type Store = RedbStore;

    fn open(&self, clock: Arc<ManualClock>) -> Arc<RedbStore> {
        RedbStore::open_with_default_ids(&self.path, clock as Arc<dyn Clock>)
            .expect("open redb conformance store")
    }

    fn reopen(&self, clock: Arc<ManualClock>) -> Arc<RedbStore> {
        RedbStore::open_with_default_ids(&self.path, clock as Arc<dyn Clock>)
            .expect("reopen redb conformance store")
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

pub const BASE_MS: i64 = 1_760_400_000_000;

pub fn test_clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(BASE_MS))
}

pub fn streaming_job(clock: &ManualClock, key: &str) -> NewJob {
    NewJob {
        kind: JobKind::StreamingPublish,
        payload: br#"{"note_id":"__PENDING_NOTE_ID__"}"#.to_vec(),
        idempotency_key: key.to_string(),
        not_before: clock.now_ms(),
    }
}

pub fn federate_job(clock: &ManualClock, key: &str) -> NewJob {
    // Minimal well-formed Follow payload (parseable by the delivery path;
    // never executed in the suites that use this fixture).
    let payload = FederateDeliverPayload::Follow {
        follow_id: format!("{key}:follow"),
        follower_id: format!("{key}:follower"),
        followee_id: format!("{key}:followee"),
        inbox_url: "https://example.local/inbox".to_string(),
    };
    NewJob::new_json(JobKind::FederateDeliver, &payload, key, clock.now_ms())
        .expect("federate fixture is a valid payload")
}

pub fn create_user<S: AccountStore>(store: &S, handle: &str) -> pinetree::domain::account::Account {
    store
        .create_account(CreateAccountInput::local(handle), None)
        .unwrap_or_else(|e| panic!("create_account({handle}) failed: {e:?}"))
}

pub fn note_with_job<S: ConformanceStore>(
    store: &S,
    clock: &ManualClock,
    author_id: &str,
    text: &str,
    key: &str,
) -> Note {
    store
        .create_note(
            CreateNoteInput::simple_public(author_id, text),
            vec![streaming_job(clock, key)],
        )
        .expect("create_note with one job")
}

pub fn list_all<S: NoteStore>(store: &S) -> Vec<Note> {
    store
        .list_all_notes(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })
        .expect("list_all_notes")
}

pub fn is_conflict(e: &PortError) -> bool {
    matches!(e, PortError::Conflict(_))
}

pub fn is_validation(e: &PortError) -> bool {
    matches!(e, PortError::Validation(_))
}

// ---------------- Subprocess crash harness ----------------

pub fn child_mode_is(kind: &str) -> bool {
    std::env::var("PINETREE_CONFORMANCE_CHILD")
        .map(|v| v == kind)
        .unwrap_or(false)
}

pub fn child_db_path() -> PathBuf {
    PathBuf::from(std::env::var("PINETREE_DB_PATH").expect("PINETREE_DB_PATH must be set"))
}

pub fn child_store() -> (Arc<ManualClock>, Arc<RedbStore>) {
    let clock = test_clock();
    let store = RedbStore::open_with_default_ids(child_db_path(), clock.clone() as Arc<dyn Clock>)
        .expect("child open store");
    (clock, store)
}

pub fn ready_and_park() -> ! {
    if let Ok(ready_path) = std::env::var("PINETREE_READY_FILE") {
        let _ = std::fs::write(ready_path, b"READY");
    }
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}

static CHILD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub fn lock_crash_harness() -> std::sync::MutexGuard<'static, ()> {
    CHILD_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn run_crash_child(kind: &str, test_name: &str, db_path: &Path) {
    let ready_file = db_path.with_extension(format!("{kind}.ready"));
    let _ = std::fs::remove_file(&ready_file);
    let mut child = Command::new(std::env::current_exe().expect("current_exe"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("PINETREE_CONFORMANCE_CHILD", kind)
        .env("PINETREE_DB_PATH", db_path)
        .env("PINETREE_READY_FILE", &ready_file)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn conformance child");
    let deadline = Instant::now() + Duration::from_secs(15);
    while !ready_file.exists() {
        if let Some(status) = child.try_wait().expect("try_wait child") {
            panic!("conformance child {kind} exited early with {status}");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("conformance child {kind} timed out waiting for READY file");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    kill_child(&mut child);
}

pub fn kill_child(child: &mut Child) {
    child.kill().expect("SIGKILL conformance child");
    let _ = child.wait();
    std::thread::sleep(Duration::from_millis(100));
}

/// Full single-node stack over a redb factory path with a controllable clock
/// and a caller-chosen streaming sink, mirroring
/// `tests/crash_recovery.rs::open_stack` plus a worker.
pub fn open_worker_stack(
    factory: &RedbFactory,
    clock: Arc<ManualClock>,
    streaming: Arc<dyn StreamingSink>,
) -> (Arc<RedbStore>, Arc<AppServices>, JobWorker) {
    let store = factory.open(clock.clone());
    let blobs = Arc::new(
        FsBlobStore::new(factory.path().parent().expect("temp parent").join("blobs"))
            .expect("open blob store"),
    );
    let clock_port: Arc<dyn Clock> = clock.clone();
    let services = Arc::new(AppServices {
        clock: clock_port.clone(),
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
        blobs: blobs.clone(),
        notifications: store.clone(),
        auth: store.clone(),
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
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "https://example.local".to_string(),
        max_file_size: 10_000_000,
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
    });
    let worker = JobWorker {
        clock: clock_port,
        ids: store.ids().clone(),
        jobs: store.clone(),
        notifications: store.clone(),
        streaming,
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 60_000,
        base_backoff_ms: 10,
        transcode_config: TranscodeConfig::default(),
        email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
    };
    (store, services, worker)
}

/// Remote account whose preferred inbox is `inbox`.
pub fn slice10_remote_account<S: AccountStore>(
    store: &S,
    handle: &str,
    inbox: &str,
) -> pinetree::domain::account::Account {
    store
        .create_account(
            CreateAccountInput {
                handle: handle.to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://remote.example/users/{handle}")),
                inbox: Some(inbox.to_string()),
                shared_inbox: None,
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
        .unwrap_or_else(|e| panic!("remote account {handle}: {e:?}"))
}

pub use pinetree::domain::quote::{QuoteAuthorizationState, QuoteReference, QuoteStamp};

/// Remote (federated) account with a resolvable actor URI.
pub fn remote_account<S: AccountStore>(
    store: &S,
    handle: &str,
) -> pinetree::domain::account::Account {
    store
        .create_account(
            CreateAccountInput {
                handle: handle.to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://remote.example/users/{handle}")),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
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
        .unwrap_or_else(|e| panic!("remote account {handle}: {e:?}"))
}

pub use pinetree::domain::user_list::{
    USER_LIST_LIMIT, USER_LIST_MEMBER_LIMIT, UserList, UserListMembership,
};

// ---------------------------------------------------------------------------
// Shared store fixtures (used by 2+ store files; moved here by the P2 split)
// ---------------------------------------------------------------------------

/// Batch M log fixture. Ids are harness-supplied (the port accepts
/// caller-built rows; ids are generated by the application in production).
pub fn log_entry(
    id: &str,
    actor: Option<&str>,
    action: ModerationAction,
    target: Option<&str>,
    details: serde_json::Value,
) -> ModerationLogEntry {
    ModerationLogEntry::new(
        id.to_string(),
        actor.map(str::to_string),
        action,
        target.map(str::to_string),
        details,
        BASE_MS,
    )
    .expect("valid log entry")
}

/// An intentionally invalid row (`details` must be a JSON object): passed
/// into a compound method to make the log append fail INSIDE the action's
/// transaction, proving the state change and the row commit together or
/// not at all (H1/H2/H4).
pub fn invalid_log_entry(id: &str, action: ModerationAction) -> ModerationLogEntry {
    ModerationLogEntry {
        id: id.to_string(),
        actor_account: None,
        action,
        target: None,
        details: serde_json::json!("not an object"),
        created_at_ms: BASE_MS,
    }
}

pub fn log_rows<S: ModerationLogStore>(store: &S) -> Vec<ModerationLogEntry> {
    store
        .list_logs(
            &ModerationLogFilter::default(),
            &TimelineCursor {
                limit: 1_000,
                ..Default::default()
            },
        )
        .expect("list_logs")
}

pub fn quote_pending(target_uri: &str) -> QuoteReference {
    QuoteReference {
        target_uri: target_uri.to_string(),
        target_note_id: None,
        authorization: QuoteAuthorizationState::Pending,
        authorization_uri: None,
        request_activity_uri: Some("https://local.example/activities/req-1".to_string()),
        last_verified_at: None,
    }
}
