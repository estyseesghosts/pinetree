use super::account_store::{ACCOUNT_OWNERS, ACCOUNTS_BY_HANDLE, ACCOUNTS_BY_URI, USED_USERNAMES};
use super::announcement_store::{ANNOUNCE_READS, ANNOUNCEMENTS, PROMO_READS};
use super::antenna_store::{ANTENNA_NOTES, ANTENNAS, ANTENNAS_BY_OWNER};
use super::auth_store::{
    ACTOR_KEYPAIRS, API_TOKENS, API_TOKENS_BY_ID, LEGACY_SESSIONS, MIAUTH_SESSIONS,
    PASSWORD_RESET_IDS, PASSWORD_RESETS, REMOTE_PUBLIC_KEYS, SIGNINS, SIGNINS_BY_USER,
};
use super::block_store::{
    BLOCKINGS_FORWARD, BLOCKINGS_REVERSE, MUTINGS_FORWARD, RENOTE_MUTINGS_FORWARD,
};
use super::clip_store::{
    CLIP_FAVORITES, CLIP_FOLLOWS, CLIP_NOTES, CLIP_TOMBSTONES, CLIPS, CLIPS_BY_OWNER,
};
use super::codec::{encode_json, map_redb_err};
use super::drive_folder_store::{
    DRIVE_FILE_FOLDERS, DRIVE_FILES_IN_FOLDER, DRIVE_FOLDERS, DRIVE_FOLDERS_BY_NAME,
    DRIVE_FOLDERS_BY_OWNER_PARENT,
};
use super::emoji_store::{EMOJIS, EMOJIS_BY_NAME, REMOTE_EMOJIS};
use super::favorite_store::FAVORITES;
use super::follow_store::{FOLLOW_REQ_FORWARD, FOLLOW_REQ_REVERSE};
use super::gallery_store::{FLASH_LIKES, FLASHES, GALLERY_POST_LIKES, GALLERY_POSTS};
use super::host_caps_store::HOST_SOFTWARE;
use super::instance_settings_store::INSTANCE_SETTINGS;
use super::invite_store::{INVITE_CODES, INVITES};
use super::job_queue::{JOBS, JOBS_DUE_INDEX};
use super::media_store::{CACHE_ACCOUNTING, REMOTE_CACHE};
use super::moderation_store::{
    ABUSE_REPORT_RECIPIENTS, ABUSE_REPORTS, MODERATION_LOGS, USER_MEMOS,
};
use super::note_store::{
    NOTE_DRAFTS, NOTE_DRAFTS_BY_USER, NOTE_EDITS, NOTE_EDITS_BY_NOTE, NOTES_BY_AUTHOR,
    NOTES_BY_QUOTE_REQUEST, NOTES_BY_QUOTE_STAMP, NOTES_BY_REPLY, NOTES_BY_REPLY_PENDING, PINNINGS,
};
use super::note_thread_mute_store::THREAD_MUTES;
use super::notification_store::{NOTIFICATIONS, NOTIFICATIONS_BY_USER};
use super::page_store::{PAGE_LIKES, PAGES, PAGES_BY_OWNER, PAGES_BY_OWNER_NAME, PAGES_BY_URI};
use super::reaction_store::{REACTIONS, REACTIONS_BY_NOTE};
use super::registry_store::REGISTRY;
use super::user_list_store::{
    USER_LIST_FAVORITES, USER_LIST_MEMBERSHIPS, USER_LISTS, USER_LISTS_BY_OWNER,
};
use super::webhook_store::{
    SYSTEM_WEBHOOKS, SYSTEM_WEBHOOKS_BY_TRIGGER, USER_WEBHOOKS, USER_WEBHOOKS_BY_OWNER,
    USER_WEBHOOKS_BY_OWNER_TRIGGER,
};
use crate::application::ports::{Clock, IdGenerator, Job, NewJob, PortError, RegistryStore};
use crate::domain::id::AidxGenerator;
use redb::{Database, ReadableTableMetadata, TableDefinition, WriteTransaction};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[path = "store_backfill.rs"]
mod store_backfill;

pub(crate) const ACCOUNTS: TableDefinition<&str, &[u8]> = TableDefinition::new("accounts");
pub(crate) const HANDLE_INDEX: TableDefinition<&str, &str> = TableDefinition::new("handle_index");
pub(crate) const NOTES: TableDefinition<&str, &[u8]> = TableDefinition::new("notes");
pub(crate) const FOLLOWS_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follows_forward");
pub(crate) const FOLLOWS_REVERSE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follows_reverse");
/// Polls by note id (`docs/misskey-parity-plan.md` §4; Misskey `MiPoll`).
pub(crate) const POLLS: TableDefinition<&str, &[u8]> = TableDefinition::new("polls");
/// Poll votes keyed `(user_id, note_id, choice)` (`UNIQUE(user,note,choice)`
/// parity with Misskey `MiPollVote`).
pub(crate) const POLL_VOTES: TableDefinition<(&str, &str, i64), &[u8]> =
    TableDefinition::new("poll_votes");
pub(crate) const ACTIVITY_DEDUP: TableDefinition<&str, u8> = TableDefinition::new("activity_dedup");
pub(crate) const MEDIA_META: TableDefinition<&str, &[u8]> = TableDefinition::new("media_meta");
/// Storage overhaul: live metadata references per blob key (0 ⇒ bytes may
/// be deleted). Metadata + accounting commit in-txn; byte deletion happens
/// outside the txn with reconciler sweep.
pub(crate) const BLOB_REFCOUNT: TableDefinition<&str, u64> = TableDefinition::new("blob_refcount");
/// Durable (local-upload) bytes charged per owner (quota enforcement).
pub(crate) const DURABLE_BYTES_BY_OWNER: TableDefinition<&str, u64> =
    TableDefinition::new("durable_bytes_by_owner");
pub(crate) const CREDENTIALS: TableDefinition<&str, &str> = TableDefinition::new("credentials");
/// Locally authored `QuoteAuthorization` stamps, keyed by full stamp IRI.
pub(crate) const QUOTE_STAMPS: TableDefinition<&str, &[u8]> = TableDefinition::new("quote_stamps");
/// Unique index of live notes by ActivityPub `uri` → note id. Written in the
/// same transaction as the note row; this is the storage-enforced
/// uniqueness rule for note URIs (`docs/persistence-conformance.md`, "Note
/// URI uniqueness") and the O(1) path behind `NoteStore::find_by_uri`.
/// Rebuildable derived state; backfilled at open for older databases.
pub(crate) const NOTES_BY_URI: TableDefinition<&str, &str> = TableDefinition::new("notes_by_uri");
/// Remembered deletions: a remote note URI whose `Delete` arrived while no note
/// was held (the `Delete` overtook its `Create`). A `Create` or fetch of a
/// tombstoned URI is refused so the late copy cannot resurrect the note. Keyed
/// by URI only; the value is a marker. Written only by
/// `record_delete_uri_activity`; deleting a held note does not write one (Group U3).
pub(crate) const TOMBSTONES_BY_URI: TableDefinition<&str, u8> =
    TableDefinition::new("tombstones_by_uri");
/// Schema/migration markers (`docs/replies.md` §4): single-row string
/// keys (done markers, resume cursors) for versioned backfills.
pub(crate) const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
/// META marker: `REACTIONS_BY_NOTE` has been built from `REACTIONS`.
pub(crate) const REACTIONS_BY_NOTE_DONE_KEY: &str = "migration:reactions_by_note:v1";
/// META marker: legacy `API_TOKENS` rows (minted before Batch O token ids)
/// have been assigned ids and indexed in `API_TOKENS_BY_ID`.
pub(crate) const TOKEN_ID_DONE_KEY: &str = "migration:api_token_ids:v1";
/// `NOTES_BY_REPLY` backfill version marker: present with value
/// [`REPLY_INDEX_DONE_VALUE`] once every stored note has been indexed.
pub(crate) const REPLY_INDEX_DONE_KEY: &str = "migration:notes_by_reply:v1";
const REPLY_INDEX_DONE_VALUE: &str = "done";
/// Resume cursor for an interrupted backfill: the last scanned note id.
pub(crate) const REPLY_INDEX_PROGRESS_KEY: &str = "migration:notes_by_reply:v1:progress";
/// `NOTES_BY_URI` backfill done marker (single transaction at open).
pub(crate) const URI_INDEX_DONE_KEY: &str = "migration:notes_by_uri:v1";
/// META marker: `NOTIFICATIONS_BY_USER` has been built from `NOTIFICATIONS`.
pub(crate) const NOTIFICATION_ORDER_DONE_KEY: &str = "migration:notifications_by_user_ordered:v1";
/// META marker: blob refcounts, durable owner totals, and the remote-cache
/// index have been rebuilt from `MEDIA_META` (storage overhaul backfill).
pub(crate) const STORAGE_ACCOUNTING_DONE_KEY: &str = "migration:storage_accounting:v1";
/// Resume cursor for an interrupted accounting rebuild: the last media id
/// whose counters were committed. Batches partition the table by id, and
/// each batch commits its counter deltas together with the advanced cursor,
/// so a restart resumes without double-counting.
pub(crate) const STORAGE_ACCOUNTING_PROGRESS_KEY: &str = "migration:storage_accounting:v1:progress";
/// Media rows rebuilt per accounting-backfill batch. Bounds one read batch
/// and one write transaction (counter deltas + progress + maybe the done
/// marker), mirroring `REPLY_BACKFILL_BATCH` discipline.
const STORAGE_ACCOUNTING_BATCH: usize = 500;
/// META marker: `ACCOUNTS_BY_URI` + `ACCOUNTS_BY_HANDLE` have been built
/// from `ACCOUNTS` (WP5 federation backfill, single transaction at open).
pub(crate) const ACCOUNT_URI_INDEX_DONE_KEY: &str = "migration:accounts_by_uri:v1";
/// META marker: `NOTES_BY_QUOTE_REQUEST` + `NOTES_BY_QUOTE_STAMP` have been
/// built from `NOTES` (WP5 federation backfill, single transaction at open).
pub(crate) const QUOTE_INDEX_DONE_KEY: &str = "migration:notes_by_quote:v1";

pub struct RedbStore {
    pub(crate) db: Database,
    path: PathBuf,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) ids: Arc<dyn IdGenerator>,
    pub(crate) local_node_id: String,
    abort_before_commit: AtomicBool,
}

impl RedbStore {
    pub fn open(
        path: impl AsRef<Path>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdGenerator>,
    ) -> Result<Arc<Self>, PortError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    PortError::StorageUnavailable(format!("create db parent dir: {e}"))
                })?;
            }
        }
        let db = Database::create(&path)
            .map_err(|e| PortError::StorageUnavailable(format!("open redb: {e}")))?;

        let store = Arc::new(Self {
            db,
            path,
            clock,
            ids,
            local_node_id: "local".to_string(),
            abort_before_commit: AtomicBool::new(false),
        });
        store.init_tables()?;
        // Slice A migration (`docs/replies.md` §4): backfill the reply
        // adjacency index for databases created before it existed. Runs to
        // completion here — before this handle serves any request — in
        // bounded crash-resumable batches (see `run_reply_index_backfill`).
        store.run_reply_index_backfill()?;
        store.run_uri_index_backfill()?;
        store.run_notification_order_backfill()?;
        store.run_reactions_by_note_backfill()?;
        store.run_token_id_backfill()?;
        store.run_storage_accounting_backfill()?;
        store.run_account_uri_index_backfill()?;
        store.run_quote_index_backfill()?;
        Ok(store)
    }

    pub fn open_with_default_ids(
        path: impl AsRef<Path>,
        clock: Arc<dyn Clock>,
    ) -> Result<Arc<Self>, PortError> {
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        Self::open(path, clock, ids)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    pub fn ids(&self) -> &Arc<dyn IdGenerator> {
        &self.ids
    }

    /// Clone this store as the [`RegistryStore`] handle backing the admin
    /// global collections: `application::commands::admin_ext` persists
    /// relays, ads, announcements, roles, abuse recipients, the proxy
    /// account, and federation overrides under the `~instance` user id via
    /// this registry. No dedicated admin tables exist by design (see
    /// `crate::domain::admin`); the registry already serves every need,
    /// so this helper is the whole admin wiring.
    pub fn registry_handle(self: &Arc<Self>) -> Arc<dyn RegistryStore> {
        self.clone()
    }

    pub fn set_abort_before_commit(&self, enabled: bool) {
        self.abort_before_commit.store(enabled, Ordering::SeqCst);
    }

    fn init_tables(&self) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        let _ = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
        let _ = write_txn.open_table(USED_USERNAMES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(ACCOUNT_OWNERS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(NOTES).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTES_BY_AUTHOR)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(FOLLOWS_FORWARD)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(FOLLOWS_REVERSE)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(BLOCKINGS_FORWARD)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(BLOCKINGS_REVERSE)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(MUTINGS_FORWARD)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(RENOTE_MUTINGS_FORWARD)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(FOLLOW_REQ_FORWARD)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(FOLLOW_REQ_REVERSE)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(REACTIONS_BY_NOTE)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTE_DRAFTS_BY_USER)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(PINNINGS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(FAVORITES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(EMOJIS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(EMOJIS_BY_NAME).map_err(map_redb_err)?;
        let _ = write_txn.open_table(REMOTE_EMOJIS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(ANTENNAS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(ANTENNAS_BY_OWNER)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(ANTENNA_NOTES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(USER_LISTS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(USER_LISTS_BY_OWNER)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(USER_LIST_MEMBERSHIPS)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(USER_LIST_FAVORITES)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(CLIPS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(CLIPS_BY_OWNER).map_err(map_redb_err)?;
        let _ = write_txn.open_table(CLIP_NOTES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(CLIP_FAVORITES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(CLIP_FOLLOWS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(CLIP_TOMBSTONES)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(PAGES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(PAGES_BY_OWNER).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(PAGES_BY_OWNER_NAME)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(PAGES_BY_URI).map_err(map_redb_err)?;
        let _ = write_txn.open_table(PAGE_LIKES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(POLLS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(POLL_VOTES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(JOBS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(JOBS_DUE_INDEX).map_err(map_redb_err)?;
        let _ = write_txn.open_table(ACTIVITY_DEDUP).map_err(map_redb_err)?;
        let _ = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let _ = write_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(DURABLE_BYTES_BY_OWNER)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(CACHE_ACCOUNTING)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTIFICATIONS_BY_USER)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(CREDENTIALS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(API_TOKENS_BY_ID)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(SIGNINS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(SIGNINS_BY_USER)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(MIAUTH_SESSIONS)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(LEGACY_SESSIONS)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(PASSWORD_RESETS)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(PASSWORD_RESET_IDS)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(REGISTRY).map_err(map_redb_err)?;
        let _ = write_txn.open_table(ACTOR_KEYPAIRS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(REMOTE_PUBLIC_KEYS)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(QUOTE_STAMPS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(NOTES_BY_REPLY).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTES_BY_REPLY_PENDING)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(TOMBSTONES_BY_URI)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(NOTE_EDITS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTE_EDITS_BY_NOTE)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(HOST_SOFTWARE).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(MODERATION_LOGS)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(INSTANCE_SETTINGS)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(INVITES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(DRIVE_FOLDERS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(DRIVE_FOLDERS_BY_OWNER_PARENT)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(DRIVE_FOLDERS_BY_NAME)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(DRIVE_FILE_FOLDERS)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(DRIVE_FILES_IN_FOLDER)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(SYSTEM_WEBHOOKS)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(SYSTEM_WEBHOOKS_BY_TRIGGER)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(USER_WEBHOOKS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(USER_WEBHOOKS_BY_OWNER)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(USER_WEBHOOKS_BY_OWNER_TRIGGER)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(ABUSE_REPORT_RECIPIENTS)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(ACCOUNTS_BY_URI)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(ACCOUNTS_BY_HANDLE)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTES_BY_QUOTE_REQUEST)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTES_BY_QUOTE_STAMP)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(USER_MEMOS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(THREAD_MUTES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(GALLERY_POSTS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(GALLERY_POST_LIKES)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(FLASHES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(FLASH_LIKES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(ANNOUNCEMENTS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(ANNOUNCE_READS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(PROMO_READS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(META).map_err(map_redb_err)?;
        write_txn.commit().map_err(map_redb_err)?;
        Ok(())
    }

    pub(crate) fn begin_write(&self) -> Result<WriteTransaction, PortError> {
        self.db.begin_write().map_err(map_redb_err)
    }

    pub(crate) fn commit_write(&self, write_txn: WriteTransaction) -> Result<(), PortError> {
        if self.abort_before_commit.swap(false, Ordering::SeqCst) {
            let _ = write_txn.abort();
            return Err(PortError::StorageUnavailable(
                "injected crash before commit".to_string(),
            ));
        }
        write_txn.commit().map_err(map_redb_err)
    }

    pub(crate) fn enqueue_jobs_in_txn(
        &self,
        write_txn: &WriteTransaction,
        jobs: Vec<NewJob>,
        note_id_substitution: Option<&str>,
    ) -> Result<Vec<Job>, PortError> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let mut jobs_table = write_txn.open_table(JOBS).map_err(map_redb_err)?;
        let mut due_table = write_txn.open_table(JOBS_DUE_INDEX).map_err(map_redb_err)?;
        let mut out = Vec::with_capacity(jobs.len());
        for mut new_job in jobs {
            new_job.validate()?;
            if let Some(nid) = note_id_substitution {
                if let Ok(s) = std::str::from_utf8(&new_job.payload) {
                    if s.contains("__PENDING_NOTE_ID__") {
                        new_job.payload = s.replace("__PENDING_NOTE_ID__", nid).into_bytes();
                    }
                }
            }
            let job_id = self.ids.generate();
            let job = Job {
                id: job_id.clone(),
                kind: new_job.kind,
                payload: new_job.payload,
                idempotency_key: new_job.idempotency_key,
                not_before: new_job.not_before,
                attempts: 0,
                lease_expires: None,
                failed: false,
                fail_reason: None,
            };
            let encoded = encode_json(&job)?;
            jobs_table
                .insert(job.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            due_table
                .insert((job.not_before, job.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
            out.push(job);
        }
        Ok(out)
    }
}
