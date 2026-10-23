//! Wave 6 actor-upsert atomicity: the remote-actor handshake
//! (`alsoKnownAs`, `movedTo`) commits in the creating transaction.
//!
//! Old code did `create_account` (txn 1, no handshake) then a second
//! `update_account` (txn 2, handshake only). A crash between the two left
//! a row without the move columns, so the `i/move` handshake never
//! completed. The fix folds the columns into the first `CreateAccountInput`
//! (create path) and keeps the refresh path on a single `update_account`.
//! This test counts store calls: a fresh upsert with handshake must do
//! exactly one `create_account` and zero `update_account`s, and the
//! returned + re-read rows must carry both profile and move columns.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pinetree::activitypub::actor::parse_remote_actor;
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, CreateAccountInput, NoopStreamingSink,
    UpdateAccountPatch,
};
use pinetree::domain::account::Account;
use pinetree::domain::id::{AidxGenerator, ManualClock};
use pinetree::domain::moderation::ModerationLogEntry;
use pinetree::federation::delivery::{
    SafeHttpClient, SafeHttpConfig, upsert_remote_actor_from_doc,
};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;
use serde_json::{Value, json};

const FIXTURE_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nZmFrZXJlbW90ZXN0a2V5\n-----END PUBLIC KEY-----";

struct CountingAccounts {
    inner: Arc<RedbStore>,
    creates: AtomicUsize,
    updates: AtomicUsize,
}

impl CountingAccounts {
    fn counts(&self) -> (usize, usize) {
        (
            self.creates.load(Ordering::SeqCst),
            self.updates.load(Ordering::SeqCst),
        )
    }
}

impl AccountStore for CountingAccounts {
    fn create_account(
        &self,
        input: CreateAccountInput,
        log: Option<&ModerationLogEntry>,
    ) -> Result<Account, pinetree::application::ports::PortError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        self.inner.create_account(input, log)
    }

    fn find_by_id(
        &self,
        id: &str,
    ) -> Result<Option<Account>, pinetree::application::ports::PortError> {
        self.inner.find_by_id(id)
    }

    fn find_by_handle(
        &self,
        h: &str,
    ) -> Result<Option<Account>, pinetree::application::ports::PortError> {
        self.inner.find_by_handle(h)
    }

    fn find_by_username_and_host(
        &self,
        u: &str,
        h: Option<&str>,
    ) -> Result<Option<Account>, pinetree::application::ports::PortError> {
        self.inner.find_by_username_and_host(u, h)
    }

    fn is_username_available(
        &self,
        u: &str,
        p: &[String],
    ) -> Result<bool, pinetree::application::ports::PortError> {
        self.inner.is_username_available(u, p)
    }

    fn update_account(
        &self,
        id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<pinetree::application::ports::NewJob>,
    ) -> Result<Account, pinetree::application::ports::PortError> {
        self.updates.fetch_add(1, Ordering::SeqCst);
        self.inner.update_account(id, patch, jobs)
    }

    fn rename(
        &self,
        id: &str,
        h: &str,
    ) -> Result<Account, pinetree::application::ports::PortError> {
        self.inner.rename(id, h)
    }

    fn get_owner(&self, id: &str) -> Result<String, pinetree::application::ports::PortError> {
        self.inner.get_owner(id)
    }

    fn set_owner(
        &self,
        id: &str,
        node: &str,
        proof: &str,
    ) -> Result<(), pinetree::application::ports::PortError> {
        self.inner.set_owner(id, node, proof)
    }

    fn search_accounts(
        &self,
        q: &str,
        o: usize,
        l: usize,
    ) -> Result<Vec<Account>, pinetree::application::ports::PortError> {
        self.inner.search_accounts(q, o, l)
    }

    fn all_accounts(
        &self,
        after: Option<&str>,
        max_rows: usize,
    ) -> Result<(Vec<Account>, Option<String>), pinetree::application::ports::PortError> {
        self.inner.all_accounts(after, max_rows)
    }

    fn remote_shared_inboxes(
        &self,
        after: Option<&str>,
        max_rows: usize,
    ) -> Result<(Vec<String>, Option<String>), pinetree::application::ports::PortError> {
        self.inner.remote_shared_inboxes(after, max_rows)
    }

    fn find_by_uri(
        &self,
        uri: &str,
    ) -> Result<Option<Account>, pinetree::application::ports::PortError> {
        self.inner.find_by_uri(uri)
    }

    fn find_by_email_verify_code(
        &self,
        code: &str,
    ) -> Result<Option<Account>, pinetree::application::ports::PortError> {
        self.inner.find_by_email_verify_code(code)
    }

    fn count_accounts(&self) -> Result<u64, pinetree::application::ports::PortError> {
        self.inner.count_accounts()
    }

    fn count_local_accounts(&self) -> Result<u64, pinetree::application::ports::PortError> {
        self.inner.count_local_accounts()
    }
}

fn actor_doc(id: &str, username: &str) -> Value {
    json!({
        "id": id,
        "type": "Person",
        "preferredUsername": username,
        "inbox": format!("{id}/inbox"),
        "publicKey": {
            "id": format!("{id}#main-key"),
            "type": "Key",
            "owner": id,
            "publicKeyPem": FIXTURE_PEM,
        },
    })
}

fn test_services() -> (AppServices, Arc<CountingAccounts>) {
    use std::sync::atomic::AtomicU64;

    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-wave6-actor-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn pinetree::application::ports::Clock> =
        Arc::new(ManualClock::new(1_760_400_000_000));
    let ids: Arc<dyn pinetree::application::ports::IdGenerator> =
        Arc::new(AidxGenerator::new(clock.clone()));
    let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
    let counting = Arc::new(CountingAccounts {
        inner: store.clone(),
        creates: AtomicUsize::new(0),
        updates: AtomicUsize::new(0),
    });
    let services = AppServices {
        clock,
        ids,
        accounts: counting.clone(),
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
    (services, counting)
}

#[test]
fn wave6_actor_upsert_single_commit_seeds_handshake() {
    let (services, counting) = test_services();
    let id = "https://remote.example/users/mover";
    let mut doc = actor_doc(id, "mover");
    doc["alsoKnownAs"] = json!(["https://local.example/users/9"]);
    doc["movedTo"] = json!("https://remote.example/users/mover-new");
    // Sanity: the fixture really carries the handshake.
    let parsed = parse_remote_actor(&doc).expect("fixture parses");
    assert_eq!(
        parsed.also_known_as,
        Some(vec!["https://local.example/users/9".to_string()])
    );
    assert_eq!(
        parsed.moved_to_uri,
        Some("https://remote.example/users/mover-new".to_string())
    );

    let (account, _) =
        upsert_remote_actor_from_doc(&services, id, &doc).expect("fresh upsert succeeds");

    // One commit: exactly one create, zero follow-up updates. The old
    // create-then-update code did (1, 1) here.
    assert_eq!(
        counting.counts(),
        (1, 0),
        "fresh upsert with handshake must be a single create commit"
    );

    // Profile and move columns land together in that commit.
    assert_eq!(
        account.also_known_as,
        Some(vec!["https://local.example/users/9".to_string()])
    );
    assert_eq!(
        account.moved_to_uri,
        Some("https://remote.example/users/mover-new".to_string())
    );
    assert!(
        account.moved_at_ms.is_some(),
        "moved_to_uri must be stamped in the same commit"
    );

    // The stored row (not just the returned struct) has both.
    let stored = services
        .accounts
        .find_by_uri(id)
        .expect("lookup")
        .expect("row exists");
    assert_eq!(stored.also_known_as, account.also_known_as);
    assert_eq!(stored.moved_to_uri, account.moved_to_uri);
    assert_eq!(stored.moved_at_ms, account.moved_at_ms);
}

#[test]
fn wave6_actor_upsert_no_handshake_still_single_commit() {
    let (services, counting) = test_services();
    let id = "https://remote.example/users/plain";
    let doc = actor_doc(id, "plain");

    let (account, _) =
        upsert_remote_actor_from_doc(&services, id, &doc).expect("plain upsert succeeds");
    assert_eq!(counting.counts(), (1, 0));
    assert_eq!(account.also_known_as, None);
    assert_eq!(account.moved_to_uri, None);
    assert_eq!(account.moved_at_ms, None);
}

#[test]
fn wave6_actor_upsert_refresh_stays_single_update() {
    let (services, counting) = test_services();
    let id = "https://remote.example/users/re";
    let mut doc = actor_doc(id, "reuser");
    doc["alsoKnownAs"] = json!(["https://local.example/users/1"]);
    doc["movedTo"] = json!("https://remote.example/users/re-new");
    upsert_remote_actor_from_doc(&services, id, &doc).expect("seed");
    assert_eq!(counting.counts(), (1, 0));

    // Refresh with a new display name and rotated handshake: still one txn.
    let mut doc2 = actor_doc(id, "reuser");
    doc2["name"] = json!("New Name");
    doc2["alsoKnownAs"] = json!(["https://local.example/users/2"]);
    doc2["movedTo"] = json!("https://remote.example/users/re-new2");
    let (account, _) =
        upsert_remote_actor_from_doc(&services, id, &doc2).expect("refresh succeeds");
    assert_eq!(
        counting.counts(),
        (1, 1),
        "refresh must be a single update commit"
    );
    assert_eq!(account.display_name.as_deref(), Some("New Name"));
    assert_eq!(
        account.also_known_as,
        Some(vec!["https://local.example/users/2".to_string()])
    );
    assert_eq!(
        account.moved_to_uri,
        Some("https://remote.example/users/re-new2".to_string())
    );
    assert!(account.moved_at_ms.is_some());
}
