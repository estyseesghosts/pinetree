use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use crate::application::ports::{
    AccountStore, ActivityDedup, ActorKeypairRecord, ApiTokenRecord, AuthStore, BlockStore, Clock,
    ConsumeInviteInput, CreateAccountInput, CreateInviteInput, CreateNoteInput, EditNoteInput,
    FollowStore, HostCapsStore, HostSoftware,
    IdGenerator, InsertMediaInput, InstanceSettingsPatch, InstanceSettingsStore, InviteListFilter,
    InviteSort, InviteStore, InviteTypeFilter,
    Job, JobQueue, LegacyAuthSessionRecord, MediaStore, MiAuthSessionRecord, ModerationLogFilter,
    ModerationLogStore, MuteStore, NewJob,
    NoteStore, NotificationStore, PollChoiceCount, PollStore, PortError, ReactionStore,
    RemotePublicKeyRecord, TimelineCursor, UpdateAccountPatch, MAX_JOBS_PER_TXN, MAX_JOB_ATTEMPTS,
};
use crate::domain::account::{validate_local_username, Account};
use crate::domain::blocking::Blocking;
use crate::domain::draft::NoteDraft;
use crate::domain::follow::{Follow, FollowNotify, FollowRequest};
use crate::domain::instance::{InstanceSettings, InviteCode};
use crate::domain::moderation::ModerationLogEntry;
use crate::domain::muting::{validate_mute_expiry, validate_mute_params, Muting, RenoteMuting};
use crate::domain::id::{parse_aidx_time, AidxGenerator};
use crate::domain::media::MediaMeta;
use crate::domain::note::{
    extract_hashtags, is_quote_content, Note, NoteEdit, MAX_CW_STORAGE_CHARS,
    MAX_FILE_IDS, MAX_NOTE_TEXT_CHARS,
};
use crate::domain::notification::{Notification, NotificationKind};
use crate::domain::pinning::UserNotePining;
use crate::domain::policy::{narrow_on_create, NarrowingInput};
use crate::domain::quote::{QuoteAuthorizationState, QuoteReference, QuoteStamp};
use crate::domain::poll::{Poll, PollVote};
use crate::domain::reaction::Reaction;

const ACCOUNTS: TableDefinition<&str, &[u8]> = TableDefinition::new("accounts");
const HANDLE_INDEX: TableDefinition<&str, &str> = TableDefinition::new("handle_index");
const USED_USERNAMES: TableDefinition<&str, u8> = TableDefinition::new("used_usernames");
const ACCOUNT_OWNERS: TableDefinition<&str, &str> = TableDefinition::new("account_owners");
const NOTES: TableDefinition<&str, &[u8]> = TableDefinition::new("notes");
const NOTES_BY_AUTHOR: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("notes_by_author");
const FOLLOWS_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follows_forward");
const FOLLOWS_REVERSE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follows_reverse");
/// Blocks, keyed `(blocker_id, blockee_id)` with a reverse index keyed
/// `(blockee_id, blocker_id)` (§0A). Values are JSON [`Blocking`] rows.
const BLOCKINGS_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("blockings_forward");
const BLOCKINGS_REVERSE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("blockings_reverse");
/// Mutes, keyed `(muter_id, mutee_id)` (§0A). Values are JSON [`Muting`]
/// rows (which carry the optional expiry).
const MUTINGS_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("mutings_forward");
/// Renote-mutes, keyed `(muter_id, mutee_id)` (§0A). Values are JSON
/// [`RenoteMuting`] rows.
const RENOTE_MUTINGS_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("renote_mutings_forward");
const FOLLOW_REQ_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follow_requests_forward");
const FOLLOW_REQ_REVERSE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follow_requests_reverse");
const REACTIONS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("reactions");
/// Note drafts by id (`docs/misskey-parity-plan.md` §5; Misskey
/// `note_draft`). Values are JSON [`NoteDraft`] rows.
const NOTE_DRAFTS: TableDefinition<&str, &[u8]> = TableDefinition::new("note_drafts");
/// Draft index keyed `(user_id, draft_id)` (aidx draft ids are
/// time-ordered, so reverse ranges read newest-first).
const NOTE_DRAFTS_BY_USER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("note_drafts_by_user");
/// Note pinnings keyed `(user_id, note_id)` (`UNIQUE(user,note)`;
/// `docs/misskey-parity-plan.md` §7; Misskey `user_note_pining`).
/// Values are JSON [`UserNotePining`] rows (which carry the pinning id
/// used for oldest-first ordering).
const PINNINGS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("pinnings");
/// Polls by note id (`docs/misskey-parity-plan.md` §4; Misskey `MiPoll`).
const POLLS: TableDefinition<&str, &[u8]> = TableDefinition::new("polls");
/// Poll votes keyed `(user_id, note_id, choice)` (`UNIQUE(user,note,choice)`
/// parity with Misskey `MiPollVote`).
const POLL_VOTES: TableDefinition<(&str, &str, i64), &[u8]> =
    TableDefinition::new("poll_votes");
const JOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("jobs");
const JOBS_DUE_INDEX: TableDefinition<(i64, &str), u8> = TableDefinition::new("jobs_due_index");
const ACTIVITY_DEDUP: TableDefinition<&str, u8> = TableDefinition::new("activity_dedup");
const MEDIA_META: TableDefinition<&str, &[u8]> = TableDefinition::new("media_meta");
const NOTIFICATIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("notifications");
/// `(notifiee, order key) -> notification id`. The order key is the time-ordered
/// suffix of the id (see [`notification_order_key`]). The full id cannot be the key:
/// it starts with the kind (`notify:inbound-reply:…`), so ordering by it groups rows
/// by kind. Renamed from `notifications_by_user`; old rows are backfilled at open.
const NOTIFICATIONS_BY_USER: TableDefinition<(&str, &str), &str> =
    TableDefinition::new("notifications_by_user_ordered");
const CREDENTIALS: TableDefinition<&str, &str> = TableDefinition::new("credentials");
const API_TOKENS: TableDefinition<&str, &[u8]> = TableDefinition::new("api_tokens");
const MIAUTH_SESSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("miauth_sessions");
const LEGACY_SESSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("legacy_sessions");
const ACTOR_KEYPAIRS: TableDefinition<&str, &[u8]> = TableDefinition::new("actor_keypairs");
const REMOTE_PUBLIC_KEYS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("remote_public_keys");
/// Locally authored `QuoteAuthorization` stamps, keyed by full stamp IRI.
const QUOTE_STAMPS: TableDefinition<&str, &[u8]> = TableDefinition::new("quote_stamps");
/// Direct reply edges, keyed by `(parent_note_id, child_note_id)`
/// (`docs/replies.md` Slice A). `Note.reply_id` stays authoritative; this
/// is rebuildable derived state, backfilled by a versioned migration for
/// databases created before the index existed.
const NOTES_BY_REPLY: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("notes_by_reply");
/// Unresolved reply parents, keyed by `(parent_uri, child_note_id)`
/// (`docs/replies.md` Slice B). One row per note waiting on an `inReplyTo`
/// URI; removed atomically when the child links (or is deleted).
const NOTES_BY_REPLY_PENDING: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("notes_by_reply_pending");
/// Unique index of live notes by ActivityPub `uri` → note id. Written in the
/// same transaction as the note row; this is the storage-enforced
/// uniqueness rule for note URIs (`docs/persistence-conformance.md`, "Note
/// URI uniqueness") and the O(1) path behind `NoteStore::find_by_uri`.
/// Rebuildable derived state; backfilled at open for older databases.
const NOTES_BY_URI: TableDefinition<&str, &str> = TableDefinition::new("notes_by_uri");
/// Remembered deletions: a remote note URI whose `Delete` arrived while no note
/// was held (the `Delete` overtook its `Create`). A `Create` or fetch of a
/// tombstoned URI is refused so the late copy cannot resurrect the note. Keyed
/// by URI only; the value is a marker. Written only by
/// `record_delete_uri_activity`; deleting a held note does not write one (Group U3).
const TOMBSTONES_BY_URI: TableDefinition<&str, u8> = TableDefinition::new("tombstones_by_uri");
/// Schema/migration markers (`docs/replies.md` §4): single-row string
/// keys (done markers, resume cursors) for versioned backfills.
const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
/// Note edit history, keyed by edit row id (§8 `docs/misskey-parity-plan.md`;
/// Sharkey `MiNoteEdit` parity).
const NOTE_EDITS: TableDefinition<&str, &[u8]> = TableDefinition::new("note_edits");
/// Edit rows by `(note_id, edit_id)`, oldest-first by construction (edit ids
/// are time-ordered), driving `NoteStore::note_edits`.
const NOTE_EDITS_BY_NOTE: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("note_edits_by_note");
/// Cached per-host ActivityPub software names (§6 `docs/misskey-parity-plan.md`;
/// `HostCapsStore` parity): lowercased host → JSON [`HostSoftware`] row.
const HOST_SOFTWARE: TableDefinition<&str, &[u8]> = TableDefinition::new("host_software");
/// Moderation-log rows (Batch M §3.2; Misskey `ModerationLogs`), keyed by
/// row id (aidx, time-ordered → reverse range reads newest-first).
const MODERATION_LOGS: TableDefinition<&str, &[u8]> = TableDefinition::new("moderation_logs");
/// Instance settings singleton row (Batch M §3.3; `InstanceSettingsStore`),
/// keyed by `"settings"`. Absent row = [`InstanceSettings::default()`].
const INSTANCE_SETTINGS: TableDefinition<&str, &[u8]> = TableDefinition::new("instance_settings");
/// Invite codes (Batch M WP2; upstream `RegistrationTicket`), keyed by
/// ticket id (aidx, time-ordered → reverse range reads newest-first).
const INVITES: TableDefinition<&str, &[u8]> = TableDefinition::new("invites");
/// Unique invite-code index (upstream `RegistrationTicket.code` unique
/// column): code → ticket id. A row present here means the code is
/// unconsumed; `consume_and_create_account` removes it atomically.
const INVITE_CODES: TableDefinition<&str, &str> = TableDefinition::new("invite_codes");

/// `NOTES_BY_REPLY` backfill version marker: present with value
/// [`REPLY_INDEX_DONE_VALUE`] once every stored note has been indexed.
const REPLY_INDEX_DONE_KEY: &str = "migration:notes_by_reply:v1";
const REPLY_INDEX_DONE_VALUE: &str = "done";
/// Resume cursor for an interrupted backfill: the last scanned note id.
const REPLY_INDEX_PROGRESS_KEY: &str = "migration:notes_by_reply:v1:progress";
/// `NOTES_BY_URI` backfill done marker (single transaction at open).
const URI_INDEX_DONE_KEY: &str = "migration:notes_by_uri:v1";
/// META marker: `NOTIFICATIONS_BY_USER` has been built from `NOTIFICATIONS`.
const NOTIFICATION_ORDER_DONE_KEY: &str = "migration:notifications_by_user_ordered:v1";

pub struct RedbStore {
    db: Database,
    path: PathBuf,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    local_node_id: String,
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
        let _ = write_txn.open_table(NOTES_BY_AUTHOR).map_err(map_redb_err)?;
        let _ = write_txn.open_table(FOLLOWS_FORWARD).map_err(map_redb_err)?;
        let _ = write_txn.open_table(FOLLOWS_REVERSE).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(BLOCKINGS_FORWARD)
            .map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(BLOCKINGS_REVERSE)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(MUTINGS_FORWARD).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(RENOTE_MUTINGS_FORWARD)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(FOLLOW_REQ_FORWARD).map_err(map_redb_err)?;
        let _ = write_txn.open_table(FOLLOW_REQ_REVERSE).map_err(map_redb_err)?;
        let _ = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTE_DRAFTS_BY_USER)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(PINNINGS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(POLLS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(POLL_VOTES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(JOBS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(JOBS_DUE_INDEX).map_err(map_redb_err)?;
        let _ = write_txn.open_table(ACTIVITY_DEDUP).map_err(map_redb_err)?;
        let _ = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let _ = write_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTIFICATIONS_BY_USER)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(CREDENTIALS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(MIAUTH_SESSIONS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(LEGACY_SESSIONS).map_err(map_redb_err)?;
        let _ = write_txn.open_table(ACTOR_KEYPAIRS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(REMOTE_PUBLIC_KEYS)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(QUOTE_STAMPS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(NOTES_BY_REPLY)
            .map_err(map_redb_err)?;
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
        let _ = write_txn.open_table(MODERATION_LOGS).map_err(map_redb_err)?;
        let _ = write_txn
            .open_table(INSTANCE_SETTINGS)
            .map_err(map_redb_err)?;
        let _ = write_txn.open_table(INVITES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
        let _ = write_txn.open_table(META).map_err(map_redb_err)?;
        write_txn.commit().map_err(map_redb_err)?;
        Ok(())
    }

    fn begin_write(&self) -> Result<WriteTransaction, PortError> {
        self.db.begin_write().map_err(map_redb_err)
    }

    fn commit_write(&self, write_txn: WriteTransaction) -> Result<(), PortError> {
        if self.abort_before_commit.swap(false, Ordering::SeqCst) {
            let _ = write_txn.abort();
            return Err(PortError::StorageUnavailable(
                "injected crash before commit".to_string(),
            ));
        }
        write_txn.commit().map_err(map_redb_err)
    }

    // --- Slice A reply-index migration (`docs/replies.md` §4) ---------------

    /// Notes scanned per backfill batch. Small enough to keep every batch
    /// transaction far under the FDB ~10 MB shape this schema must also fit.
    const REPLY_BACKFILL_BATCH: usize = 500;

    /// Run the `NOTES_BY_REPLY` backfill to completion in bounded batches.
    /// Each batch reads at most `REPLY_BACKFILL_BATCH` note rows, inserts
    /// their `(parent, child)` index rows, and persists the resume cursor in
    /// the SAME write transaction; the done marker commits only with the
    /// final batch. Crash-resumable (progress survives; replays re-insert
    /// idempotently) and a no-op when the marker is already set.
    pub fn run_reply_index_backfill(&self) -> Result<(), PortError> {
        loop {
            if self.reply_index_backfill_batch(Self::REPLY_BACKFILL_BATCH)? {
                return Ok(());
            }
        }
    }

    /// One backfill batch. Returns `true` when the migration is complete
    /// (done marker observed or just written).
    fn reply_index_backfill_batch(&self, batch_limit: usize) -> Result<bool, PortError> {
        if self.meta_get(REPLY_INDEX_DONE_KEY)?.as_deref() == Some(REPLY_INDEX_DONE_VALUE) {
            return Ok(true);
        }
        let progress = self
            .meta_get(REPLY_INDEX_PROGRESS_KEY)?
            .unwrap_or_default();
        // Bounded read: at most `batch_limit` note rows past the cursor.
        let scanned: Vec<(String, Option<String>)> = {
            let read_txn = self.db.begin_read().map_err(map_redb_err)?;
            let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut out = Vec::new();
            let range = notes_table
                .range(progress.as_str()..)
                .map_err(map_redb_err)?;
            for entry in range {
                let (k, v) = entry.map_err(map_redb_err)?;
                let note_id: String = k.value().to_string();
                if note_id.as_str() <= progress.as_str() {
                    continue;
                }
                let note: Note = decode_json(v.value())?;
                out.push((note_id, note.reply_id));
                if out.len() >= batch_limit.max(1) {
                    break;
                }
            }
            out
        };
        let exhausted = scanned.len() < batch_limit.max(1);
        let write_txn = self.begin_write()?;
        {
            let mut index_table = write_txn
                .open_table(NOTES_BY_REPLY)
                .map_err(map_redb_err)?;
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            for (child_id, parent_opt) in &scanned {
                if let Some(parent_id) = parent_opt {
                    index_table
                        .insert((parent_id.as_str(), child_id.as_str()), &1u8)
                        .map_err(map_redb_err)?;
                }
            }
            if let Some((last_id, _)) = scanned.last() {
                meta_table
                    .insert(REPLY_INDEX_PROGRESS_KEY, last_id.as_str())
                    .map_err(map_redb_err)?;
            }
            if exhausted {
                meta_table
                    .insert(REPLY_INDEX_DONE_KEY, REPLY_INDEX_DONE_VALUE)
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(exhausted)
    }

    /// Build `NOTES_BY_URI` for databases created before the index existed.
    /// One pass; when several live rows share a `uri` (duplicates created
    /// before uniqueness was enforced) the richest row wins (resolved reply
    /// link or pending parent, then attachments), ties going to the oldest.
    /// The losing rows stay in `NOTES` but are unreachable by uri.
    pub fn run_uri_index_backfill(&self) -> Result<(), PortError> {
        if self.meta_get(URI_INDEX_DONE_KEY)?.as_deref() == Some(REPLY_INDEX_DONE_VALUE) {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut by_uri = write_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
            let mut best: std::collections::HashMap<String, (u8, String)> =
                std::collections::HashMap::new();
            for entry in notes_table.iter().map_err(map_redb_err)? {
                let (k, v) = entry.map_err(map_redb_err)?;
                let note: Note = decode_json(v.value())?;
                let Some(uri) = note.uri.as_deref().map(str::trim).filter(|u| !u.is_empty())
                else {
                    continue;
                };
                if note.is_deleted {
                    continue;
                }
                let rank = u8::from(note.reply_id.is_some() || note.reply_parent_uri.is_some())
                    + u8::from(!note.file_ids.is_empty());
                let id = k.value().to_string();
                match best.get(uri) {
                    Some((r, _)) if *r >= rank => {}
                    _ => {
                        best.insert(uri.to_string(), (rank, id));
                    }
                }
            }
            for (uri, (_, id)) in &best {
                by_uri
                    .insert(uri.as_str(), id.as_str())
                    .map_err(map_redb_err)?;
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            meta_table
                .insert(URI_INDEX_DONE_KEY, REPLY_INDEX_DONE_VALUE)
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    /// Build `NOTIFICATIONS_BY_USER` (order-keyed) from `NOTIFICATIONS` once, for
    /// databases written before the index was keyed by the id's time-ordered
    /// suffix. Idempotent; the META marker makes later opens a no-op.
    pub fn run_notification_order_backfill(&self) -> Result<(), PortError> {
        if self.meta_get(NOTIFICATION_ORDER_DONE_KEY)?.as_deref() == Some("done") {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let notif_table = write_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
            let mut by_user = write_txn
                .open_table(NOTIFICATIONS_BY_USER)
                .map_err(map_redb_err)?;
            for entry in notif_table.iter().map_err(map_redb_err)? {
                let (k, v) = entry.map_err(map_redb_err)?;
                let stored: Notification = decode_json(v.value())?;
                let nid = k.value();
                by_user
                    .insert(
                        (stored.notifiee_id.as_str(), notification_order_key(nid)),
                        nid,
                    )
                    .map_err(map_redb_err)?;
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            meta_table
                .insert(NOTIFICATION_ORDER_DONE_KEY, "done")
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn meta_get(&self, key: &str) -> Result<Option<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let meta_table = read_txn.open_table(META).map_err(map_redb_err)?;
        match meta_table.get(key).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(guard.value().to_string())),
            None => Ok(None),
        }
    }

    /// Test hook: drop every `NOTES_BY_REPLY` row and clear the migration
    /// marker/cursor, simulating a database created before Slice A. The
    /// next `run_reply_index_backfill` (or reopen) must rebuild the index
    /// from stored notes without touching note rows, counters, or jobs.
    pub fn remove_reply_index_for_test(&self) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut index_table = write_txn
                .open_table(NOTES_BY_REPLY)
                .map_err(map_redb_err)?;
            let keys: Vec<(String, String)> = {
                let mut out = Vec::new();
                for entry in index_table.iter().map_err(map_redb_err)? {
                    let (k, _) = entry.map_err(map_redb_err)?;
                    let (parent, child) = k.value();
                    out.push((parent.to_string(), child.to_string()));
                }
                out
            };
            for (parent, child) in &keys {
                index_table
                    .remove((parent.as_str(), child.as_str()))
                    .map_err(map_redb_err)?;
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            let _ = meta_table.remove(REPLY_INDEX_DONE_KEY);
            let _ = meta_table.remove(REPLY_INDEX_PROGRESS_KEY);
        }
        self.commit_write(write_txn)
    }

    fn handle_key(username_lower: &str, host: Option<&str>) -> String {
        match host {
            Some(h) if !h.is_empty() => {
                format!("remote:{}:{}", h.to_ascii_lowercase(), username_lower)
            }
            _ => format!("local:{username_lower}"),
        }
    }

    fn enqueue_jobs_in_txn(
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

    /// Mark `job_id` done inside an open write transaction (no-op when the
    /// job is missing). Table guards are scoped to this call so the caller
    /// can open the same tables again later in the transaction.
    fn complete_in_txn(&self, write_txn: &WriteTransaction, job_id: &str) -> Result<(), PortError> {
        let mut jobs_table = write_txn.open_table(JOBS).map_err(map_redb_err)?;
        let mut due_table = write_txn.open_table(JOBS_DUE_INDEX).map_err(map_redb_err)?;
        let job_opt: Option<Job> = match jobs_table.get(job_id).map_err(map_redb_err)? {
            Some(jg) => Some(decode_json(jg.value())?),
            None => None,
        };
        if let Some(job) = job_opt {
            jobs_table.remove(job_id).map_err(map_redb_err)?;
            let _ = due_table.remove((job.not_before, job_id));
            if let Some(exp) = job.lease_expires {
                let _ = due_table.remove((exp, job_id));
            }
        }
        Ok(())
    }

    fn create_note_in_txn(
        &self,
        write_txn: &WriteTransaction,
        mut input: CreateNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        // `has_poll` must agree with `poll` (the application layer sets the
        // flag from the input;storage enforces the invariant).
        if input.has_poll != input.poll.is_some() {
            return Err(PortError::Validation(
                "has_poll must agree with poll".to_string(),
            ));
        }
        // Validate the poll params inside the transaction too (the
        // application layer validates against the same clock first; this
        // keeps the invariant storage-side for every caller, including
        // inbound federation).
        if let Some(poll_input) = input.poll.as_ref() {
            poll_input.validate(self.clock.now_ms()).map_err(|e| match e {
                crate::domain::id::DomainError::Validation(msg) => {
                    PortError::Validation(msg)
                }
                crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
                crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
                crate::domain::id::DomainError::StorageUnavailable(msg) => {
                    PortError::StorageUnavailable(msg)
                }
            })?;
        }
        if input.has_channel {
            return Err(PortError::Validation(
                "channels are unsupported (`UNSUPPORTED_FIELD`)".to_string(),
            ));
        }
        for job in &jobs {
            job.validate()?;
        }

        // Check prohibited words
        if let Some(text) = &input.text {
            let lower = text.to_lowercase();
            for word in &input.prohibited_words {
                let w = word.trim().to_lowercase();
                if !w.is_empty() && lower.contains(&w) {
                    return Err(PortError::Validation(
                        "CONTAINS_PROHIBITED_WORDS".to_string(),
                    ));
                }
            }
        }
        if let Some(cw) = &input.cw {
            let lower = cw.to_lowercase();
            for word in &input.prohibited_words {
                let w = word.trim().to_lowercase();
                if !w.is_empty() && lower.contains(&w) {
                    return Err(PortError::Validation(
                        "CONTAINS_PROHIBITED_WORDS".to_string(),
                    ));
                }
            }
        }

        let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let handle_table = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
        let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
        let mut by_author_table = write_txn
            .open_table(NOTES_BY_AUTHOR)
            .map_err(map_redb_err)?;
        let mut by_reply_table = write_txn
            .open_table(NOTES_BY_REPLY)
            .map_err(map_redb_err)?;
        let mut by_reply_pending_table = write_txn
            .open_table(NOTES_BY_REPLY_PENDING)
            .map_err(map_redb_err)?;
        let mut by_uri_table = write_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
        let media_table = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;

        let mut author: Account = match accounts_table
            .get(input.author_id.as_str())
            .map_err(map_redb_err)?
        {
            Some(g) => decode_json(g.value())?,
            None => return Err(PortError::NotFound("author account not found".to_string())),
        };
        if author.is_deleted || author.is_suspended {
            return Err(PortError::Validation(
                "author is deleted or suspended".to_string(),
            ));
        }

        // Validate reply_to and renote_of targets. `reply_to` and
        // `reply_parent_uri` together are conflicting parents (Slice B):
        // the note is either linked or pending, never both.
        if input.reply_to.is_some() && input.reply_parent_uri.is_some() {
            return Err(PortError::Validation(
                "reply has both a resolved parent and a pending parent URI".to_string(),
            ));
        }
        // Normalize the pending parent URI: blank strings carry no
        // relationship and decode as no pending parent.
        let reply_parent_uri = input
            .reply_parent_uri
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let mut reply_target: Option<Note> = None;
        if let Some(reply_id) = &input.reply_to {
            let n: Note = match notes_table.get(reply_id.as_str()).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_REPLY_TARGET".to_string())),
            };
            if n.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_REPLY_TARGET".to_string()));
            }
            reply_target = Some(n);
        }

        let mut renote_target: Option<Note> = None;
        if let Some(renote_id) = &input.renote_of {
            let n: Note = match notes_table.get(renote_id.as_str()).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string())),
            };
            if n.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()));
            }
            renote_target = Some(n);
        }

        // Validate visible_users exist
        for uid in &input.visible_users {
            if accounts_table
                .get(uid.as_str())
                .map_err(map_redb_err)?
                .is_none()
            {
                return Err(PortError::Validation(format!(
                    "visible user {uid} does not exist"
                )));
            }
        }

        // Validate file_ids exist and belong to author (or are unowned remote cache files for a remote author)
        for fid in &input.file_ids {
            let media: MediaMeta = match media_table.get(fid.as_str()).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NO_SUCH_FILE".to_string())),
            };
            let owner_ok = media.owner_id.as_deref() == Some(input.author_id.as_str())
                || (author.host.is_some() && media.owner_id.is_none());
            if !owner_ok {
                return Err(PortError::Validation("NO_SUCH_FILE".to_string()));
            }
        }

        // Sharkey `isQuote` parity (`NoteCreateService.isQuote`, synced with
        // `misc/is-renote.ts`): text/cw presence (not blankness), a resolved
        // reply link, a poll, or attached files turn a renote into a quote.
        // `reply_parent_uri` (unresolved inbound parent) is NOT commentary.
        let is_quote = input.renote_of.is_some()
            && crate::domain::note::is_quote_content(
                input.text.as_deref(),
                input.cw.as_deref(),
                input.reply_to.is_some(),
                input.poll.is_some(),
                input.file_ids.len(),
            );
        let narrowed = narrow_on_create(NarrowingInput {
            requested_visibility: input.visibility,
            requested_local_only: input.local_only,
            requested_visible_user_ids: input.visible_users.clone(),
            author_id: &input.author_id,
            text: input.text.as_deref(),
            cw: input.cw.as_deref(),
            renote_target: renote_target.as_ref(),
            reply_target: reply_target.as_ref(),
            renote_target_blocks_author: {
                // §0A: real block lookup (was hardcoded `false`). Pure
                // renotes of an author who blocks the replier are rejected
                // with `YOU_HAVE_BEEN_BLOCKED` (Misskey
                // `NoteCreateService` parity, enforced here so the local
                // and inbound create paths share one check atomically).
                // Gated on `!is_quote` (Sharkey `isRenote && !isQuote`
                // parity): quotes skip the pure-renote block gate and fall
                // through to the §3 permission check below instead.
                match renote_target.as_ref() {
                    Some(target)
                        if !is_quote && target.author_id != input.author_id =>
                    {
                        self.blocking_exists_in_txn(
                            write_txn,
                            &target.author_id,
                            &input.author_id,
                        )?
                    }
                    _ => false,
                }
            },
            sensitive_words: &input.sensitive_words,
            can_public_note: true,
        })?;

        // §0A: replying to an author who blocks the replier is rejected
        // with `YOU_HAVE_BEEN_BLOCKED` (Misskey `NoteCreateService`
        // reply-blocking parity), in the same transaction as the insert.
        if let Some(target) = reply_target.as_ref() {
            if target.author_id != input.author_id
                && self.blocking_exists_in_txn(write_txn, &target.author_id, &input.author_id)?
            {
                return Err(PortError::Validation(
                    "YOU_HAVE_BEEN_BLOCKED".to_string(),
                ));
            }
        }

        // §3 (Sharkey `NoteCreateService.checkQuotePermissions` parity):
        // the QUOTER's own `rejectQuotes` flag gates quote creation — it is
        // a per-user moderation switch (admin `admin/reject-quotes` +
        // self-service `i/update {rejectQuotes}`), not the target author's.
        // Local quoters fail closed with `QUOTE_DISABLED_FOR_USER`; remote
        // quoters (inbound federation, which runs this same transaction via
        // `record_note_activity`) are stripped to a plain note with a
        // `quoteUnavailable` diagnostic. Position: after renote-narrowing,
        // before the pure-renote block gate — the inputs (quote shape +
        // author flag) are narrowing-independent, and the narrow gate above
        // is pure-renote-only, so upstream error precedence holds exactly.
        let mut process_errors: Option<Vec<String>> = None;
        if is_quote && author.reject_quotes {
            if author.host.is_none() {
                return Err(PortError::Validation(
                    "QUOTE_DISABLED_FOR_USER".to_string(),
                ));
            }
            input.renote_of = None;
            process_errors = Some(vec!["quoteUnavailable".to_string()]);
        }
        // A stripped quote leaves no renote behind: drop the loaded target
        // so no `renote_count` bump or quote record is written below.
        // (`renote_target` is `Some` only when `renote_of` was set, so this
        // is a no-op for ordinary notes.)
        let renote_target = if input.renote_of.is_none() {
            None
        } else {
            renote_target
        };
        // The strip above also drops any inbound quote relation: the stored
        // note is an ordinary plain note carrying only the diagnostic.
        if input.renote_of.is_none() {
            input.quote = None;
        }

        // Extract mentions & tags
        let mut mentions = input.extra_mentions;
        if !input.no_extract_mentions {
            if let Some(text) = &input.text {
                for (uname, host_opt) in crate::domain::note::extract_all_mentions(text) {
                    let key = Self::handle_key(&uname.to_ascii_lowercase(), host_opt.as_deref());
                    if let Some(uid_guard) =
                        handle_table.get(key.as_str()).map_err(map_redb_err)?
                    {
                        let uid = uid_guard.value().to_string();
                        if !mentions.contains(&uid) {
                            mentions.push(uid);
                        }
                    }
                }
            }
        }

        let mut tags = input.extra_tags;
        if !input.no_extract_hashtags {
            if let Some(text) = &input.text {
                for t in extract_hashtags(text) {
                    if !tags.contains(&t) {
                        tags.push(t);
                    }
                }
            }
        }

        let note_uri = input
            .uri
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .map(str::to_string);
        if let Some(uri) = note_uri.as_deref() {
            // Tombstones don't hold their uri (deleting drops the index row).
            let holder = by_uri_table
                .get(uri)
                .map_err(map_redb_err)?
                .map(|g| g.value().to_string());
            if let Some(holder_id) = holder {
                let live = match notes_table.get(holder_id.as_str()).map_err(map_redb_err)? {
                    Some(g) => !decode_json::<Note>(g.value())?.is_deleted,
                    None => false,
                };
                if live {
                    return Err(PortError::Conflict("DUPLICATE_NOTE_URI".to_string()));
                }
            }
            // A remembered deletion blocks a late copy of the same URI
            // (`Delete` overtook its `Create`, or a fetch replays a deleted note).
            let tombstones = write_txn
                .open_table(TOMBSTONES_BY_URI)
                .map_err(map_redb_err)?;
            if tombstones.get(uri).map_err(map_redb_err)?.is_some() {
                return Err(PortError::Conflict("TOMBSTONED_NOTE_URI".to_string()));
            }
        }
        let note_id = self.ids.generate();
        // Quotes always carry `renote_id` for Misskey API compatibility, so
        // the resolved local target id is the single source of truth: when
        // the application resolved the target but left `target_note_id`
        // empty, fill it from `renote_of` here so both paths converge.
        let mut quote = input.quote;
        if let Some(q) = quote.as_mut() {
            if q.target_note_id.is_none() {
                q.target_note_id = input.renote_of.clone();
            }
            q.validate().map_err(|e| match e {
                crate::domain::id::DomainError::Validation(msg) => {
                    PortError::Validation(format!("invalid quote: {msg}"))
                }
                other => PortError::Validation(format!("invalid quote: {other:?}")),
            })?;
            if let (Some(tid), Some(rid)) = (q.target_note_id.as_deref(), input.renote_of.as_deref()) {
                if tid != rid {
                    return Err(PortError::Validation(
                        "quote target_note_id must match renote_of".to_string(),
                    ));
                }
            }
        }
        let note = Note {
            id: note_id.clone(),
            author_id: input.author_id.clone(),
            text: input.text,
            cw: input.cw,
            visibility: narrowed.visibility,
            visible_user_ids: narrowed.visible_user_ids,
            local_only: narrowed.local_only,
            reply_id: input.reply_to,
            renote_id: input.renote_of,
            reply_user_id: reply_target.as_ref().map(|r| r.author_id.clone()),
            file_ids: input.file_ids,
            mentions,
            mentioned_remote_users: "[]".to_string(),
            tags,
            emojis: Vec::new(),
            reactions: BTreeMap::new(),
            renote_count: 0,
            replies_count: 0,
            reaction_acceptance: input.reaction_acceptance,
            uri: input.uri,
            url: input.url,
            user_host: author.host.clone(),
            is_deleted: false,
            reply_parent_uri,
            quote,
            updated_at_ms: None,
            process_errors,
        };
        if author.host.is_some() {
            note.validate_storage_with_poll(input.poll.is_some())?;
        } else {
            note.validate_api_create_with_poll(input.poll.is_some())?;
        }

        // Update reply_target.replies_count
        if let Some(mut rt) = reply_target {
            rt.replies_count = rt.replies_count.saturating_add(1);
            let rt_bytes = encode_json(&rt)?;
            notes_table
                .insert(rt.id.as_str(), rt_bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        // Update renote_target.renote_count (if different from reply_target)
        if let Some(mut rnt) = renote_target {
            if note.reply_id.as_deref() == Some(rnt.id.as_str()) {
                rnt.replies_count = rnt.replies_count.saturating_add(1);
            }
            rnt.renote_count = rnt.renote_count.saturating_add(1);
            let rnt_bytes = encode_json(&rnt)?;
            notes_table
                .insert(rnt.id.as_str(), rnt_bytes.as_slice())
                .map_err(map_redb_err)?;
        }

        // Update author.notes_count
        author.notes_count = author.notes_count.saturating_add(1);
        let author_bytes = encode_json(&author)?;
        accounts_table
            .insert(author.id.as_str(), author_bytes.as_slice())
            .map_err(map_redb_err)?;

        let note_bytes = encode_json(&note)?;
        notes_table
            .insert(note.id.as_str(), note_bytes.as_slice())
            .map_err(map_redb_err)?;
        by_author_table
            .insert((note.author_id.as_str(), note.id.as_str()), &1u8)
            .map_err(map_redb_err)?;
        if let Some(uri) = note_uri.as_deref() {
            by_uri_table
                .insert(uri, note.id.as_str())
                .map_err(map_redb_err)?;
        }
        // Slice A adjacency index: a resolved reply links its
        // `(parent, child)` row in the same transaction as the note, the
        // parent counter bump, and the durable jobs below. A pending
        // (unresolved) reply instead records its `(parent_uri, child)` row
        // so the resolver can find it when the parent arrives.
        if let Some(parent_id) = note.reply_id.as_deref() {
            by_reply_table
                .insert((parent_id, note.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        } else if let Some(parent_uri) = note.reply_parent_uri.as_deref() {
            by_reply_pending_table
                .insert((parent_uri, note.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        }

        drop(accounts_table);
        drop(handle_table);
        drop(notes_table);
        drop(by_author_table);
        drop(by_reply_table);
        drop(by_reply_pending_table);
        drop(by_uri_table);
        drop(media_table);

        // §4: the poll row commits in the same transaction as the note
        // (Misskey `NoteCreateService` transactional `MiNote` + `MiPoll`
        // insert parity). `votes` starts zeroed with one slot per choice,
        // or seeded from the origin-reported tallies for cached remote
        // polls (the local path always seeds zeros).
        if let Some(poll_input) = input.poll.as_ref() {
            let mut polls_table = write_txn.open_table(POLLS).map_err(map_redb_err)?;
            let votes = match input.poll_initial_votes.as_ref() {
                None => vec![0u64; poll_input.choices.len()],
                Some(seed) => {
                    if seed.len() != poll_input.choices.len() {
                        return Err(PortError::Validation(
                            "poll seed votes must match choices".to_string(),
                        ));
                    }
                    seed.clone()
                }
            };
            let poll = Poll {
                note_id: note.id.clone(),
                choices: poll_input.choices.clone(),
                votes,
                multiple: poll_input.multiple,
                expires_at_ms: poll_input.expires_at_ms,
            };
            let poll_bytes = encode_json(&poll)?;
            polls_table
                .insert(note.id.as_str(), poll_bytes.as_slice())
                .map_err(map_redb_err)?;
        }

        self.enqueue_jobs_in_txn(write_txn, jobs, Some(&note.id))?;
        Ok(note)
    }

    /// `notes/edit` storage path (§8; Sharkey `NoteEditService.edit`
    /// parity). One transaction commits the updated note row, the
    /// `note_edits` history row (effective edits only), the poll
    /// add/remove/replace (votes reset), and `jobs` atomically.
    #[allow(clippy::too_many_lines)]
    fn apply_note_edit_in_txn(
        &self,
        write_txn: &WriteTransaction,
        edit: EditNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
        let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let media_table = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;

        let old: Note = match notes_table
            .get(edit.note_id.as_str())
            .map_err(map_redb_err)?
        {
            Some(g) => decode_json(g.value())?,
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        };
        if old.is_deleted {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        if old.author_id != edit.editor_id {
            return Err(PortError::Validation(
                "YOU_ARE_NOT_THE_AUTHOR".to_string(),
            ));
        }
        let author: Account = match accounts_table
            .get(old.author_id.as_str())
            .map_err(map_redb_err)?
        {
            Some(g) => decode_json(g.value())?,
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        };

        // The new renote target (None = detach; the reply parent is
        // immutable and stays on the original). A pure-renote target is
        // rejected (Sharkey endpoint `cannotReRenote` parity, re-checked
        // here so the gate holds inside the atomic transaction).
        let mut renote_target: Option<Note> = None;
        if let Some(renote_id) = edit.renote_of.as_deref() {
            let target: Note = match notes_table
                .get(renote_id)
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => {
                    return Err(PortError::NotFound(
                        "NO_SUCH_RENOTE_TARGET".to_string(),
                    ))
                }
            };
            if target.is_deleted {
                return Err(PortError::NotFound(
                    "NO_SUCH_RENOTE_TARGET".to_string(),
                ));
            }
            if target.renote_id.is_some() && target.quote.is_none() {
                return Err(PortError::Validation(
                    "CANNOT_RENOTE_TO_A_PURE_RENOTE".to_string(),
                ));
            }
            renote_target = Some(target);
        }

        // Files must exist and be owned (same rule as create).
        for fid in &edit.file_ids {
            let media: MediaMeta = match media_table
                .get(fid.as_str())
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NO_SUCH_FILE".to_string())),
            };
            let owner_ok = media.owner_id.as_deref() == Some(old.author_id.as_str())
                || (author.host.is_some() && media.owner_id.is_none());
            if !owner_ok {
                return Err(PortError::Validation("NO_SUCH_FILE".to_string()));
            }
        }
        // `specified` audiences must resolve (same rule as create).
        for uid in &edit.visible_users {
            if accounts_table
                .get(uid.as_str())
                .map_err(map_redb_err)?
                .is_none()
            {
                return Err(PortError::Validation(format!(
                    "visible user {uid} does not exist"
                )));
            }
        }

        // Effective text/cw: trimmed + truncated to the storage bounds
        // (Sharkey service trim parity; the endpoint pre-gates the local
        // API limits, the inbound path relies on this trim).
        let new_text = trim_to_none(edit.text.as_deref(), MAX_NOTE_TEXT_CHARS);
        let new_cw = trim_to_none(edit.cw.as_deref(), MAX_CW_STORAGE_CHARS);
        let new_has_poll = edit.poll.is_some();

        // Effective quote shape (Sharkey `isQuote` parity — same predicate
        // as the create path).
        let new_is_quote = edit.renote_of.is_some()
            && is_quote_content(
                new_text.as_deref(),
                new_cw.as_deref(),
                old.reply_id.is_some(),
                new_has_poll,
                edit.file_ids.len(),
            );

        // Visibility narrowing over the FORCED original values (Sharkey
        // edit parity: `visibility`/`localOnly` are reset to the original
        // note's, then renote/reply-driven narrowing still applies, so a
        // tighter new target narrows — or rejects — the edit).
        let narrowed = narrow_on_create(NarrowingInput {
            requested_visibility: old.visibility,
            requested_local_only: old.local_only,
            requested_visible_user_ids: edit.visible_users.clone(),
            author_id: &old.author_id,
            text: new_text.as_deref(),
            cw: new_cw.as_deref(),
            renote_target: renote_target.as_ref(),
            reply_target: None,
            renote_target_blocks_author: match renote_target.as_ref() {
                Some(target)
                    if !new_is_quote && target.author_id != old.author_id =>
                {
                    self.blocking_exists_in_txn(
                        write_txn,
                        &target.author_id,
                        &old.author_id,
                    )?
                }
                _ => false,
            },
            sensitive_words: &edit.sensitive_words,
            can_public_note: true,
        })?;
        // Reply-driven §0A gate against the ORIGINAL reply parent (the
        // reply link itself is immutable, but its author may have blocked
        // the editor since the note was created).
        if let Some(reply_id) = old.reply_id.as_deref() {
            if let Some(parent) = notes_table
                .get(reply_id)
                .map_err(map_redb_err)?
                .map(|g| decode_json::<Note>(g.value()))
                .transpose()?
            {
                if !parent.is_deleted
                    && parent.author_id != old.author_id
                    && self.blocking_exists_in_txn(
                        write_txn,
                        &parent.author_id,
                        &old.author_id,
                    )?
                {
                    return Err(PortError::Validation(
                        "YOU_HAVE_BEEN_BLOCKED".to_string(),
                    ));
                }
            }
        }

        // §3 quote-permission check on the effective shape (same position
        // and semantics as the create path: quoter-side flag, local throws,
        // remote strips). On the edit path the strip also detaches the
        // renote and drops the quote relation.
        let mut new_renote_id = edit.renote_of.clone();
        let mut process_errors = old.process_errors.clone();
        if new_is_quote && author.reject_quotes {
            if author.host.is_none() {
                return Err(PortError::Validation(
                    "QUOTE_DISABLED_FOR_USER".to_string(),
                ));
            }
            new_renote_id = None;
            renote_target = None;
            process_errors = Some(vec!["quoteUnavailable".to_string()]);
        }

        // Prohibited words over text/cw/poll choices (Sharkey
        // `checkProhibitedWordsContain` parity — storage-side so the
        // inbound edit path shares the gate atomically).
        {
            let mut haystacks: Vec<&str> = Vec::new();
            if let Some(text) = new_text.as_deref() {
                haystacks.push(text);
            }
            if let Some(cw) = new_cw.as_deref() {
                haystacks.push(cw);
            }
            if contains_any_word(&haystacks, &edit.prohibited_words) {
                return Err(PortError::Validation(
                    "CONTAINS_PROHIBITED_WORDS".to_string(),
                ));
            }
            if let Some(poll) = edit.poll.as_ref() {
                let choices: Vec<&str> =
                    poll.choices.iter().map(String::as_str).collect();
                if contains_any_word(&choices, &edit.prohibited_words) {
                    return Err(PortError::Validation(
                        "CONTAINS_PROHIBITED_WORDS".to_string(),
                    ));
                }
            }
        }

        // Poll change detection against the stored row (add/remove/replace
        // all reset votes; an untouched poll keeps its tallies even past
        // expiry).
        let mut polls_table = write_txn.open_table(POLLS).map_err(map_redb_err)?;
        let old_poll: Option<Poll> = match polls_table
            .get(old.id.as_str())
            .map_err(map_redb_err)?
        {
            Some(g) => Some(decode_json(g.value())?),
            None => None,
        };
        let poll_changed = match (old_poll.as_ref(), edit.poll.as_ref()) {
            (None, None) => false,
            (Some(_), None) | (None, Some(_)) => true,
            (Some(old_poll), Some(new_poll)) => {
                old_poll.choices != new_poll.choices
                    || old_poll.multiple != new_poll.multiple
                    || old_poll.expires_at_ms != new_poll.expires_at_ms
            }
        };
        if let Some(new_poll) = edit.poll.as_ref() {
            if poll_changed {
                new_poll.validate(self.clock.now_ms()).map_err(|e| match e {
                    crate::domain::id::DomainError::Validation(msg) => {
                        PortError::Validation(msg)
                    }
                    crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
                    crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
                    crate::domain::id::DomainError::StorageUnavailable(msg) => {
                        PortError::StorageUnavailable(msg)
                    }
                })?;
            }
        }

        // No-change edits return the original note and write nothing
        // (Sharkey `else { return oldnote; }` parity — no history row, no
        // jobs). Files use the upstream truthy-OR detection (any files on
        // either side count as changed; no deep compare).
        let text_changed = new_text != old.text;
        let cw_changed = new_cw != old.cw;
        let has_poll_changed = old_poll.is_some() != new_has_poll;
        let files_changed = !old.file_ids.is_empty() || !edit.file_ids.is_empty();
        let renote_changed = new_renote_id != old.renote_id;
        let acceptance_changed = edit.reaction_acceptance != old.reaction_acceptance;
        if !text_changed
            && !cw_changed
            && !has_poll_changed
            && !files_changed
            && !poll_changed
            && !renote_changed
            && !acceptance_changed
        {
            return Ok(old);
        }

        // Recompute mentions/tags from the new text (same extraction as
        // create). A `specified` edit merges the visible audience into the
        // mentions (Sharkey service parity).
        let mut mentions: Vec<String> = Vec::new();
        if let Some(text) = new_text.as_deref() {
            let handle_table = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
            for (uname, host_opt) in crate::domain::note::extract_all_mentions(text) {
                let key =
                    Self::handle_key(&uname.to_ascii_lowercase(), host_opt.as_deref());
                if let Some(uid_guard) =
                    handle_table.get(key.as_str()).map_err(map_redb_err)?
                {
                    let uid = uid_guard.value().to_string();
                    if !mentions.contains(&uid) {
                        mentions.push(uid);
                    }
                }
            }
        }
        let mut tags: Vec<String> = Vec::new();
        if let Some(text) = new_text.as_deref() {
            for t in extract_hashtags(text) {
                if !tags.contains(&t) {
                    tags.push(t);
                }
            }
        }
        let visible_user_ids = narrowed.visible_user_ids.clone();
        if narrowed.visibility == crate::domain::note::Visibility::Specified {
            for uid in visible_user_ids.clone() {
                if !mentions.contains(&uid) {
                    mentions.push(uid);
                }
            }
        }

        // Poll mutation with vote reset (votes table rows for this note are
        // dropped; the poll row is replaced or removed; tallies restart
        // zeroed).
        if poll_changed {
            let mut votes_table =
                write_txn.open_table(POLL_VOTES).map_err(map_redb_err)?;
            let stale: Vec<(String, String, i64)> = votes_table
                .iter()
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().and_then(|(k, _)| {
                        let (user, note, choice) = k.value();
                        if note == old.id.as_str() {
                            Some((user.to_string(), note.to_string(), choice))
                        } else {
                            None
                        }
                    })
                })
                .collect();
            for (user, note, choice) in stale {
                votes_table
                    .remove((user.as_str(), note.as_str(), choice))
                    .map_err(map_redb_err)?;
            }
            match edit.poll.as_ref() {
                Some(new_poll) => {
                    let poll = Poll {
                        note_id: old.id.clone(),
                        choices: new_poll.choices.clone(),
                        votes: vec![0u64; new_poll.choices.len()],
                        multiple: new_poll.multiple,
                        expires_at_ms: new_poll.expires_at_ms,
                    };
                    let poll_bytes = encode_json(&poll)?;
                    polls_table
                        .insert(old.id.as_str(), poll_bytes.as_slice())
                        .map_err(map_redb_err)?;
                }
                None => {
                    polls_table
                        .remove(old.id.as_str())
                        .map_err(map_redb_err)?;
                }
            }
        }

        // Quote relation maintenance on renote change (Sharkey edit parity:
        // no `QuoteRequest` is issued on edit — the relation is just set).
        // An unchanged renote keeps its existing authorization record.
        let new_quote = match (old.renote_id.as_deref(), new_renote_id.as_deref()) {
            (a, b) if a == b => old.quote.clone(),
            (_, None) => None,
            (_, Some(new_id)) => {
                let base = edit.uri_base.trim_end_matches('/');
                let target_uri = renote_target
                    .as_ref()
                    .and_then(|t| t.uri.clone())
                    .unwrap_or_else(|| format!("{base}/notes/{new_id}"));
                Some(QuoteReference::unapproved(target_uri))
            }
        };
        // Keep `target_note_id` in sync with `renote_id` (create-path
        // invariant): a carried-over quote pointing elsewhere is reset to
        // the effective target.
        let mut new_quote = new_quote;
        if let (Some(q), Some(rid)) = (new_quote.as_mut(), new_renote_id.as_deref()) {
            q.target_note_id = Some(rid.to_string());
        }

        let now_ms = self.clock.now_ms();
        let edited = Note {
            id: old.id.clone(),
            author_id: old.author_id.clone(),
            text: new_text.clone(),
            cw: new_cw.clone(),
            visibility: narrowed.visibility,
            visible_user_ids,
            local_only: narrowed.local_only,
            reply_id: old.reply_id.clone(),
            renote_id: new_renote_id,
            reply_user_id: old.reply_user_id.clone(),
            file_ids: edit.file_ids.clone(),
            mentions,
            mentioned_remote_users: old.mentioned_remote_users.clone(),
            tags,
            emojis: Vec::new(),
            reactions: old.reactions.clone(),
            renote_count: old.renote_count,
            replies_count: old.replies_count,
            reaction_acceptance: edit.reaction_acceptance,
            uri: old.uri.clone(),
            url: old.url.clone(),
            user_host: old.user_host.clone(),
            is_deleted: false,
            reply_parent_uri: old.reply_parent_uri.clone(),
            quote: new_quote,
            updated_at_ms: Some(now_ms),
            process_errors,
        };
        edited.validate_storage_with_poll(new_has_poll)?;

        // History row (Sharkey `noteEditRepository.insert` parity):
        // `oldDate` is the previous edit time (note creation time on the
        // first edit, derived from the time-ordered note id).
        let old_date_ms = old.updated_at_ms.unwrap_or_else(|| {
            self.ids
                .parse_time(&old.id)
                .unwrap_or_else(|_| now_ms)
        });
        let history = NoteEdit {
            id: self.ids.generate(),
            note_id: old.id.clone(),
            old_text: old.text.clone(),
            new_text: if text_changed { new_text.clone() } else { None },
            cw: if cw_changed { new_cw.clone() } else { None },
            file_ids: None,
            old_date_ms,
            updated_at_ms: now_ms,
        };
        history.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
            other => PortError::Validation(format!("invalid note edit: {other:?}")),
        })?;
        {
            let mut edits_table =
                write_txn.open_table(NOTE_EDITS).map_err(map_redb_err)?;
            let mut by_note_table = write_txn
                .open_table(NOTE_EDITS_BY_NOTE)
                .map_err(map_redb_err)?;
            let history_bytes = encode_json(&history)?;
            edits_table
                .insert(history.id.as_str(), history_bytes.as_slice())
                .map_err(map_redb_err)?;
            by_note_table
                .insert(
                    (old.id.as_str(), history.id.as_str()),
                    &1u8,
                )
                .map_err(map_redb_err)?;
        }

        let edited_bytes = encode_json(&edited)?;
        notes_table
            .insert(edited.id.as_str(), edited_bytes.as_slice())
            .map_err(map_redb_err)?;
        drop(notes_table);
        drop(accounts_table);
        drop(media_table);
        drop(polls_table);

        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(edited)
    }

    fn delete_note_in_txn(
        &self,
        write_txn: &WriteTransaction,
        id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut by_reply_table = write_txn
                .open_table(NOTES_BY_REPLY)
                .map_err(map_redb_err)?;
            let mut by_reply_pending_table = write_txn
                .open_table(NOTES_BY_REPLY_PENDING)
                .map_err(map_redb_err)?;
            let mut note: Note = match notes_table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                // Already-deleted guard: repeating a delete cannot decrement
                // counters twice.
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            let was_pure_renote = note.renote_id.is_some()
                && note.text.is_none()
                && note.cw.is_none()
                && note.file_ids.is_empty();
            let renote_target_id = note.renote_id.clone();
            let reply_parent_id = note.reply_id.clone();
            let reply_parent_uri = note.reply_parent_uri.clone();

            if let Some(uri) = note.uri.as_deref().map(str::trim).filter(|u| !u.is_empty()) {
                let mut by_uri = write_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
                let owned = by_uri
                    .get(uri)
                    .map_err(map_redb_err)?
                    .is_some_and(|g| g.value() == id);
                if owned {
                    // Deleting a held note releases its uri (Group U3): only a
                    // `Delete` that arrives with no note held leaves a tombstone.
                    by_uri.remove(uri).map_err(map_redb_err)?;
                }
            }
            note.is_deleted = true;
            note.text = None;
            note.cw = None;
            note.file_ids.clear();
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;

            if was_pure_renote {
                if let Some(tid) = renote_target_id {
                    let target_opt: Option<Note> =
                        match notes_table.get(tid.as_str()).map_err(map_redb_err)? {
                            Some(tg) => Some(decode_json(tg.value())?),
                            None => None,
                        };
                    if let Some(mut target) = target_opt {
                        target.renote_count = target.renote_count.saturating_sub(1);
                        let tbytes = encode_json(&target)?;
                        notes_table
                            .insert(target.id.as_str(), tbytes.as_slice())
                            .map_err(map_redb_err)?;
                    }
                }
            }

            // Slice A counter consistency: a first-time delete of any note
            // with a resolved reply parent decrements that parent's
            // `replies_count` (saturating) and drops the `(parent, child)`
            // index row; a pending child drops its `(parent_uri, child)`
            // row. Descendants keep their own `reply_id` (no reparenting).
            // The parent row is updated whether or not it is itself
            // deleted — the counter counts direct replies, not visible ones.
            if let Some(parent_id) = reply_parent_id.as_deref() {
                let _ = by_reply_table
                    .remove((parent_id, note.id.as_str()))
                    .map_err(map_redb_err)?;
                let parent_opt: Option<Note> =
                    match notes_table.get(parent_id).map_err(map_redb_err)? {
                        Some(pg) => Some(decode_json(pg.value())?),
                        None => None,
                    };
                if let Some(mut parent) = parent_opt {
                    parent.replies_count = parent.replies_count.saturating_sub(1);
                    let pbytes = encode_json(&parent)?;
                    notes_table
                        .insert(parent.id.as_str(), pbytes.as_slice())
                        .map_err(map_redb_err)?;
                }
            }
            if let Some(parent_uri) = reply_parent_uri.as_deref() {
                let _ = by_reply_pending_table
                    .remove((parent_uri, note.id.as_str()))
                    .map_err(map_redb_err)?;
            }

            let author_opt: Option<Account> = match accounts_table
                .get(note.author_id.as_str())
                .map_err(map_redb_err)?
            {
                Some(ag) => Some(decode_json(ag.value())?),
                None => None,
            };
            if let Some(mut author) = author_opt {
                author.notes_count = author.notes_count.saturating_sub(1);
                let abytes = encode_json(&author)?;
                accounts_table
                    .insert(author.id.as_str(), abytes.as_slice())
                    .map_err(map_redb_err)?;
            }
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, Some(id))?;
        Ok(())
    }

    /// Remove a pending request (both index rows). Returns whether one existed.
    fn delete_follow_request_rows(
        &self,
        write_txn: &WriteTransaction,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<bool, PortError> {
        let mut fwd = write_txn.open_table(FOLLOW_REQ_FORWARD).map_err(map_redb_err)?;
        let mut rev = write_txn.open_table(FOLLOW_REQ_REVERSE).map_err(map_redb_err)?;
        let existed = fwd
            .remove((follower_id, followee_id))
            .map_err(map_redb_err)?
            .is_some();
        rev.remove((followee_id, follower_id)).map_err(map_redb_err)?;
        Ok(existed)
    }

    /// Read-only block-edge probe inside an open write transaction (§0A).
    fn blocking_exists_in_txn(
        &self,
        write_txn: &WriteTransaction,
        blocker_id: &str,
        blockee_id: &str,
    ) -> Result<bool, PortError> {
        let table = write_txn.open_table(BLOCKINGS_FORWARD).map_err(map_redb_err)?;
        Ok(table
            .get((blocker_id, blockee_id))
            .map_err(map_redb_err)?
            .is_some())
    }

    /// Follow-gate block enforcement inside an open write transaction (§0A;
    /// Misskey `UserFollowingService.follow` parity): the follower blocking
    /// the followee is `BLOCKING` ("You are blocking that user."), the
    /// followee blocking the follower is `BLOCKED` ("You are blocked by
    /// that user.").
    fn reject_when_blocked_in_txn(
        &self,
        write_txn: &WriteTransaction,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<(), PortError> {
        if self.blocking_exists_in_txn(write_txn, follower_id, followee_id)? {
            return Err(PortError::Validation("BLOCKING".to_string()));
        }
        if self.blocking_exists_in_txn(write_txn, followee_id, follower_id)? {
            return Err(PortError::Validation("BLOCKED".to_string()));
        }
        Ok(())
    }

    /// Drop one follow edge (both index rows) and adjust counters, silently
    /// when no edge exists. Shared by `unfollow_in_txn` (which reports
    /// `NOT_FOLLOWING`) and the atomic `block` (which must not fail when
    /// there is nothing to tear down).
    fn drop_follow_edge_in_txn(
        &self,
        write_txn: &WriteTransaction,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<bool, PortError> {
        let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let mut fwd_table = write_txn.open_table(FOLLOWS_FORWARD).map_err(map_redb_err)?;
        let mut rev_table = write_txn.open_table(FOLLOWS_REVERSE).map_err(map_redb_err)?;
        if fwd_table
            .remove((follower_id, followee_id))
            .map_err(map_redb_err)?
            .is_none()
        {
            return Ok(false);
        }
        rev_table
            .remove((followee_id, follower_id))
            .map_err(map_redb_err)?;
        // Decode-then-write: the read guard must drop before the mutable
        // insert (redb borrow rules).
        let follower_bytes: Option<Vec<u8>> = accounts_table
            .get(follower_id)
            .map_err(map_redb_err)?
            .map(|g| g.value().to_vec());
        if let Some(raw) = follower_bytes {
            let mut follower: Account = decode_json(&raw)?;
            follower.following_count = follower.following_count.saturating_sub(1);
            let b = encode_json(&follower)?;
            accounts_table
                .insert(follower.id.as_str(), b.as_slice())
                .map_err(map_redb_err)?;
        }
        let followee_bytes: Option<Vec<u8>> = accounts_table
            .get(followee_id)
            .map_err(map_redb_err)?
            .map(|g| g.value().to_vec());
        if let Some(raw) = followee_bytes {
            let mut followee: Account = decode_json(&raw)?;
            followee.followers_count = followee.followers_count.saturating_sub(1);
            let b = encode_json(&followee)?;
            accounts_table
                .insert(followee.id.as_str(), b.as_slice())
                .map_err(map_redb_err)?;
        }
        Ok(true)
    }

    /// Atomic block insert + bidirectional follow/request teardown inside an
    /// ambient write transaction (§0A; Misskey
    /// `UserBlockingService.block()` parity). Shared by `BlockStore::block`
    /// and the inbound-`Block` dedup record path so both commit state+jobs
    /// atomically with no second logic path.
    fn block_in_txn(
        &self,
        write_txn: &WriteTransaction,
        blocking: &Blocking,
        jobs: Vec<NewJob>,
    ) -> Result<Blocking, PortError> {
        blocking.validate()?;
        for job in &jobs {
            job.validate()?;
        }
        let (blocker_id, blockee_id) = (blocking.blocker_id.as_str(), blocking.blockee_id.as_str());
        {
            let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            for id in [blocker_id, blockee_id] {
                if accounts_table.get(id).map_err(map_redb_err)?.is_none() {
                    return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
                }
            }
            drop(accounts_table);
            let mut fwd_table = write_txn.open_table(BLOCKINGS_FORWARD).map_err(map_redb_err)?;
            if fwd_table
                .get((blocker_id, blockee_id))
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict("ALREADY_BLOCKING".to_string()));
            }
            let bytes = encode_json(&blocking)?;
            fwd_table
                .insert((blocker_id, blockee_id), bytes.as_slice())
                .map_err(map_redb_err)?;
            drop(fwd_table);
            let mut rev_table = write_txn.open_table(BLOCKINGS_REVERSE).map_err(map_redb_err)?;
            rev_table
                .insert((blockee_id, blocker_id), bytes.as_slice())
                .map_err(map_redb_err)?;
            drop(rev_table);
        }
        let _ = self.delete_follow_request_rows(&write_txn, blocker_id, blockee_id)?;
        let _ = self.delete_follow_request_rows(&write_txn, blockee_id, blocker_id)?;
        let _ = self.drop_follow_edge_in_txn(&write_txn, blocker_id, blockee_id)?;
        let _ = self.drop_follow_edge_in_txn(&write_txn, blockee_id, blocker_id)?;
        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(blocking.clone())
    }

    /// Atomic block removal inside an ambient write transaction (§0A).
    /// `NotFound(NOT_BLOCKING)` when no row exists.
    fn unblock_in_txn(
        &self,
        write_txn: &WriteTransaction,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        {
            let mut fwd_table = write_txn.open_table(BLOCKINGS_FORWARD).map_err(map_redb_err)?;
            let mut rev_table = write_txn.open_table(BLOCKINGS_REVERSE).map_err(map_redb_err)?;
            if fwd_table
                .remove((blocker_id, blockee_id))
                .map_err(map_redb_err)?
                .is_none()
            {
                return Err(PortError::NotFound("NOT_BLOCKING".to_string()));
            }
            rev_table
                .remove((blockee_id, blocker_id))
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(())
    }

    /// Newest-first id-cursor paging shared by the block/mute/renote-mute
    /// listers (§0A; Misskey `makePaginationQuery` parity over aidx ids,
    /// which are time-ordered).
    fn apply_id_cursor<T>(
        &self,
        mut rows: Vec<T>,
        cursor: &TimelineCursor,
        id_of: impl Fn(&T) -> &str,
    ) -> Vec<T> {
        rows.sort_by(|a, b| id_of(b).cmp(id_of(a)));
        let mut out = Vec::with_capacity(rows.len().min(cursor.limit.max(1)));
        for row in rows {
            let id = id_of(&row);
            if let Some(since_id) = cursor.since_id.as_deref() {
                if id <= since_id {
                    continue;
                }
            }
            if let Some(until_id) = cursor.until_id.as_deref() {
                if id >= until_id {
                    continue;
                }
            }
            if cursor.since_date.is_some() || cursor.until_date.is_some() {
                if let Ok(ts) = self.ids.parse_time(id) {
                    if let Some(since_date) = cursor.since_date {
                        if ts <= since_date {
                            continue;
                        }
                    }
                    if let Some(until_date) = cursor.until_date {
                        if ts >= until_date {
                            continue;
                        }
                    }
                }
            }
            out.push(row);
            if out.len() >= cursor.limit {
                break;
            }
        }
        out
    }

    fn request_follow_in_txn(
        &self,
        write_txn: &WriteTransaction,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<FollowRequest, PortError> {
        if follower_id == followee_id {
            return Err(PortError::Validation("FOLLOWEE_IS_YOURSELF".to_string()));
        }
        // §0A: block enforcement, both directions (Misskey
        // `UserFollowingService.follow` parity). Runs inside the same
        // transaction as the request insert, so a concurrent block cannot
        // slip through. Covers local and inbound follows alike.
        self.reject_when_blocked_in_txn(write_txn, follower_id, followee_id)?;
        for job in &jobs {
            job.validate()?;
        }
        let req = FollowRequest {
            id: self.ids.generate(),
            follower_id: follower_id.to_string(),
            followee_id: followee_id.to_string(),
            with_replies,
            notify,
        };
        {
            let accounts = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let edges = write_txn.open_table(FOLLOWS_FORWARD).map_err(map_redb_err)?;
            let mut fwd = write_txn.open_table(FOLLOW_REQ_FORWARD).map_err(map_redb_err)?;
            let mut rev = write_txn.open_table(FOLLOW_REQ_REVERSE).map_err(map_redb_err)?;
            for id in [follower_id, followee_id] {
                if accounts.get(id).map_err(map_redb_err)?.is_none() {
                    return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
                }
            }
            if edges
                .get((follower_id, followee_id))
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict("ALREADY_FOLLOWING".to_string()));
            }
            if fwd
                .get((follower_id, followee_id))
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict("ALREADY_REQUESTED".to_string()));
            }
            let bytes = encode_json(&req)?;
            fwd.insert((follower_id, followee_id), bytes.as_slice())
                .map_err(map_redb_err)?;
            rev.insert((followee_id, follower_id), bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(req)
    }

    fn follow_in_txn(
        &self,
        write_txn: &WriteTransaction,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError> {
        if follower_id == followee_id {
            return Err(PortError::Validation("FOLLOWEE_IS_YOURSELF".to_string()));
        }
        // §0A: block enforcement, both directions (see
        // `request_follow_in_txn`).
        self.reject_when_blocked_in_txn(write_txn, follower_id, followee_id)?;
        for job in &jobs {
            job.validate()?;
        }

        let follow = Follow {
            id: self.ids.generate(),
            follower_id: follower_id.to_string(),
            followee_id: followee_id.to_string(),
            with_replies,
            notify,
        };
        follow.validate()?;
        // An edge supersedes any pending request for the same pair.
        self.delete_follow_request_rows(write_txn, follower_id, followee_id)?;

        {
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut fwd_table = write_txn.open_table(FOLLOWS_FORWARD).map_err(map_redb_err)?;
            let mut rev_table = write_txn.open_table(FOLLOWS_REVERSE).map_err(map_redb_err)?;

            if fwd_table
                .get((follower_id, followee_id))
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict("ALREADY_FOLLOWING".to_string()));
            }

            let mut follower: Account =
                match accounts_table.get(follower_id).map_err(map_redb_err)? {
                    Some(g) => decode_json(g.value())?,
                    None => return Err(PortError::NotFound("NO_SUCH_USER".to_string())),
                };
            let mut followee: Account =
                match accounts_table.get(followee_id).map_err(map_redb_err)? {
                    Some(g) => decode_json(g.value())?,
                    None => return Err(PortError::NotFound("NO_SUCH_USER".to_string())),
                };

            follower.following_count = follower.following_count.saturating_add(1);
            followee.followers_count = followee.followers_count.saturating_add(1);

            let follower_bytes = encode_json(&follower)?;
            let followee_bytes = encode_json(&followee)?;
            accounts_table
                .insert(follower.id.as_str(), follower_bytes.as_slice())
                .map_err(map_redb_err)?;
            accounts_table
                .insert(followee.id.as_str(), followee_bytes.as_slice())
                .map_err(map_redb_err)?;

            let follow_bytes = encode_json(&follow)?;
            fwd_table
                .insert((follower_id, followee_id), follow_bytes.as_slice())
                .map_err(map_redb_err)?;
            rev_table
                .insert((followee_id, follower_id), follow_bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(follow)
    }

    fn unfollow_in_txn(
        &self,
        write_txn: &WriteTransaction,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        if follower_id == followee_id {
            return Err(PortError::Validation("FOLLOWEE_IS_YOURSELF".to_string()));
        }
        for job in &jobs {
            job.validate()?;
        }
        // Undo/cancel of a still-pending request removes the request instead.
        let had_request = self.delete_follow_request_rows(write_txn, follower_id, followee_id)?;

        {
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut fwd_table = write_txn.open_table(FOLLOWS_FORWARD).map_err(map_redb_err)?;
            let mut rev_table = write_txn.open_table(FOLLOWS_REVERSE).map_err(map_redb_err)?;

            if had_request
                && fwd_table
                    .get((follower_id, followee_id))
                    .map_err(map_redb_err)?
                    .is_none()
            {
                drop((accounts_table, fwd_table, rev_table));
                self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
                return Ok(());
            }
            if accounts_table
                .get(followee_id)
                .map_err(map_redb_err)?
                .is_none()
            {
                return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
            }
            if fwd_table
                .remove((follower_id, followee_id))
                .map_err(map_redb_err)?
                .is_none()
            {
                return Err(PortError::Validation("NOT_FOLLOWING".to_string()));
            }
            rev_table
                .remove((followee_id, follower_id))
                .map_err(map_redb_err)?;

            let follower_opt: Option<Account> =
                match accounts_table.get(follower_id).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            if let Some(mut follower) = follower_opt {
                follower.following_count = follower.following_count.saturating_sub(1);
                let b = encode_json(&follower)?;
                accounts_table
                    .insert(follower.id.as_str(), b.as_slice())
                    .map_err(map_redb_err)?;
            }

            let followee_opt: Option<Account> =
                match accounts_table.get(followee_id).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            if let Some(mut followee) = followee_opt {
                followee.followers_count = followee.followers_count.saturating_sub(1);
                let b = encode_json(&followee)?;
                accounts_table
                    .insert(followee.id.as_str(), b.as_slice())
                    .map_err(map_redb_err)?;
            }
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(())
    }

    fn react_in_txn(
        &self,
        write_txn: &WriteTransaction,
        user_id: &str,
        note_id: &str,
        reaction_str: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Reaction, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let reaction = Reaction {
            id: self.ids.generate(),
            user_id: user_id.to_string(),
            note_id: note_id.to_string(),
            reaction: reaction_str.to_string(),
        };
        reaction.validate()?;

        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut reactions_table = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;

            let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            // §0A: reacting to an author who blocks the reactor is rejected
            // (Misskey `ReactionService.create` parity: block check first,
            // then visibility). Self-reactions are exempt.
            if note.author_id != user_id
                && self.blocking_exists_in_txn(write_txn, &note.author_id, user_id)?
            {
                return Err(PortError::Validation(
                    "YOU_HAVE_BEEN_BLOCKED".to_string(),
                ));
            }

            let existing_opt: Option<Reaction> = match reactions_table
                .get((user_id, note_id))
                .map_err(map_redb_err)?
            {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };

            if let Some(prev) = existing_opt {
                if !upsert {
                    return Err(PortError::Conflict("ALREADY_REACTED".to_string()));
                }
                if let Some(cnt) = note.reactions.get_mut(&prev.reaction) {
                    *cnt = cnt.saturating_sub(1);
                    if *cnt == 0 {
                        note.reactions.remove(&prev.reaction);
                    }
                }
            }

            *note.reactions.entry(reaction.reaction.clone()).or_insert(0) += 1;
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;

            let reaction_bytes = encode_json(&reaction)?;
            reactions_table
                .insert((user_id, note_id), reaction_bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, Some(note_id))?;
        Ok(reaction)
    }

    fn unreact_in_txn(
        &self,
        write_txn: &WriteTransaction,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut reactions_table = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;

            let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }

            let prev: Reaction = match reactions_table
                .remove((user_id, note_id))
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NOT_REACTED".to_string())),
            };

            if let Some(cnt) = note.reactions.get_mut(&prev.reaction) {
                *cnt = cnt.saturating_sub(1);
                if *cnt == 0 {
                    note.reactions.remove(&prev.reaction);
                }
            }
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, Some(note_id))?;
        Ok(())
    }

    fn update_account_in_txn(
        &self,
        write_txn: &WriteTransaction,
        id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let updated = {
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut acc: Account = match accounts_table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound(format!("account {id} not found"))),
            };
            if let Some(v) = patch.display_name {
                acc.display_name = v;
            }
            if let Some(v) = patch.description {
                acc.description = v;
            }
            if let Some(v) = patch.followed_message {
                acc.followed_message = v;
            }
            if let Some(v) = patch.location {
                acc.location = v;
            }
            if let Some(v) = patch.birthday {
                acc.birthday = v;
            }
            if let Some(v) = patch.lang {
                acc.lang = v;
            }
            if let Some(v) = patch.avatar_id {
                acc.avatar_id = v;
            }
            if let Some(v) = patch.avatar_decorations {
                acc.avatar_decorations = v;
            }
            if let Some(v) = patch.banner_id {
                acc.banner_id = v;
            }
            if let Some(v) = patch.fields {
                acc.fields = v;
            }
            if let Some(v) = patch.is_locked {
                acc.is_locked = v;
            }
            if let Some(v) = patch.is_bot {
                acc.is_bot = v;
            }
            if let Some(v) = patch.is_cat {
                acc.is_cat = v;
            }
            if let Some(v) = patch.is_explorable {
                acc.is_explorable = v;
            }
            if let Some(v) = patch.hide_online_status {
                acc.hide_online_status = v;
            }
            if let Some(v) = patch.public_reactions {
                acc.public_reactions = v;
            }
            if let Some(v) = patch.careful_bot {
                acc.careful_bot = v;
            }
            if let Some(v) = patch.auto_accept_followed {
                acc.auto_accept_followed = v;
            }
            if let Some(v) = patch.follow_approval_local_seconds {
                acc.follow_approval_local_seconds = v;
            }
            if let Some(v) = patch.follow_approval_remote_seconds {
                acc.follow_approval_remote_seconds = v;
            }
            if let Some(v) = patch.no_crawle {
                acc.no_crawle = v;
            }
            if let Some(v) = patch.prevent_ai_learning {
                acc.prevent_ai_learning = v;
            }
            if let Some(v) = patch.require_signin_to_view_contents {
                acc.require_signin_to_view_contents = v;
            }
            if let Some(v) = patch.make_notes_followers_only_before {
                acc.make_notes_followers_only_before = v;
            }
            if let Some(v) = patch.make_notes_hidden_before {
                acc.make_notes_hidden_before = v;
            }
            if let Some(v) = patch.inject_featured_note {
                acc.inject_featured_note = v;
            }
            if let Some(v) = patch.receive_announcement_email {
                acc.receive_announcement_email = v;
            }
            if let Some(v) = patch.always_mark_nsfw {
                acc.always_mark_nsfw = v;
            }
            if let Some(v) = patch.auto_sensitive {
                acc.auto_sensitive = v;
            }
            if let Some(v) = patch.following_visibility {
                acc.following_visibility = v;
            }
            if let Some(v) = patch.followers_visibility {
                acc.followers_visibility = v;
            }
            if let Some(v) = patch.chat_scope {
                acc.chat_scope = v;
            }
            if let Some(v) = patch.pinned_page_id {
                acc.pinned_page_id = v;
            }
            if let Some(v) = patch.muted_words {
                acc.enable_word_mute = !v.is_empty();
                acc.muted_words = v;
            }
            if let Some(v) = patch.hard_muted_words {
                acc.hard_muted_words = v;
            }
            if let Some(v) = patch.muted_instances {
                acc.muted_instances = v;
            }
            if let Some(v) = patch.notification_recieve_config {
                acc.notification_recieve_config = v;
            }
            if let Some(v) = patch.email_notification_types {
                acc.email_notification_types = v;
            }
            if let Some(v) = patch.also_known_as {
                acc.also_known_as = v;
            }
            if let Some(v) = patch.reject_quotes {
                acc.reject_quotes = v;
            }
            if let Some(v) = patch.default_reaction {
                acc.default_reaction = v;
            }
            // `update.ts` resets `verifiedLinks` on every profile update;
            // re-verification happens off-request (see `verifyLink`).
            // Only applied when the caller sets the field (profile updates);
            // remote-actor refreshes leave it untouched.
            if let Some(v) = patch.verified_links {
                acc.verified_links = v;
            }
            acc.validate()?;
            let bytes = encode_json(&acc)?;
            accounts_table
                .insert(acc.id.as_str(), bytes.as_slice())
                .map_err(map_redb_err)?;
            acc
        };
        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(updated)
    }

    /// Quote-state transition inside an ambient write transaction (shared by
    /// `set_quote_authorization` and the `record_quote_*` dedup methods so
    /// state changes and outbox jobs commit atomically).
    fn set_quote_authorization_in_txn(
        &self,
        write_txn: &WriteTransaction,
        note_id: &str,
        state: QuoteAuthorizationState,
        authorization_uri: Option<String>,
        verified_at_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
        let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
            Some(g) => decode_json(g.value())?,
            None => return Err(PortError::NotFound("NO_SUCH_QUOTE".to_string())),
        };
        if note.is_deleted {
            return Err(PortError::NotFound("NO_SUCH_QUOTE".to_string()));
        }
        let Some(quote) = note.quote.as_mut() else {
            return Err(PortError::NotFound("NO_SUCH_QUOTE".to_string()));
        };
        if !quote.authorization.allows_transition(state) {
            return Err(PortError::Validation(format!(
                "illegal quote transition {} -> {}",
                quote.authorization.api_status(),
                state.api_status()
            )));
        }
        quote.authorization = state;
        // A stamp URI is kept only for stamp-backed approval. A repeat
        // `Approved` without a URI (idempotent Accept replay) retains the
        // stored stamp; `Revoked` retains it too so later duplicate
        // `Delete`s still match this quote and converge to no-ops instead
        // of misses. Every other state clears it so an
        // `Approved`-requires-stamp invariant always holds.
        match state {
            QuoteAuthorizationState::Approved => {
                if authorization_uri.is_some() {
                    quote.authorization_uri = authorization_uri;
                }
            }
            QuoteAuthorizationState::Revoked => {}
            _ => quote.authorization_uri = None,
        }
        if matches!(
            state,
            QuoteAuthorizationState::Approved | QuoteAuthorizationState::ApprovedByException
        ) {
            quote.last_verified_at = Some(verified_at_ms);
        }
        quote.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => {
                PortError::Validation(format!("invalid quote: {msg}"))
            }
            other => PortError::Validation(format!("invalid quote: {other:?}")),
        })?;
        let note_bytes = encode_json(&note)?;
        notes_table
            .insert(note.id.as_str(), note_bytes.as_slice())
            .map_err(map_redb_err)?;
        drop(notes_table);
        self.enqueue_jobs_in_txn(write_txn, jobs, Some(&note.id))?;
        Ok(note)
    }

    /// Stamp insert inside an ambient write transaction.
    fn save_quote_stamp_in_txn(
        &self,
        write_txn: &WriteTransaction,
        stamp: &QuoteStamp,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        if stamp.id.trim().is_empty() {
            return Err(PortError::Validation("empty stamp id".to_string()));
        }
        for job in &jobs {
            job.validate()?;
        }
        {
            let mut table = write_txn.open_table(QUOTE_STAMPS).map_err(map_redb_err)?;
            let bytes = encode_json(stamp)?;
            table
                .insert(stamp.id.as_str(), bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(())
    }

    fn check_and_mark_dedup_in_txn(
        &self,
        write_txn: &WriteTransaction,
        activity_id: &str,
    ) -> Result<bool, PortError> {
        if activity_id.trim().is_empty() {
            return Err(PortError::Validation("empty activity_id".to_string()));
        }
        let mut table = write_txn.open_table(ACTIVITY_DEDUP).map_err(map_redb_err)?;
        let exists = table.get(activity_id).map_err(map_redb_err)?.is_some();
        if !exists {
            table.insert(activity_id, &1u8).map_err(map_redb_err)?;
        }
        Ok(exists)
    }
}

fn map_redb_err(err: impl std::fmt::Display) -> PortError {
    PortError::StorageUnavailable(err.to_string())
}

fn encode_json<T: serde::Serialize>(val: &T) -> Result<Vec<u8>, PortError> {
    serde_json::to_vec(val)
        .map_err(|e| PortError::StorageUnavailable(format!("json encode: {e}")))
}

fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, PortError> {
    serde_json::from_slice(bytes)
        .map_err(|e| PortError::StorageUnavailable(format!("json decode: {e}")))
}

/// Trim `raw` and truncate to `max_chars`; empty-after-trim maps to `None`
/// (Sharkey service trim parity for `notes/edit`).
fn trim_to_none(raw: Option<&str>, max_chars: usize) -> Option<String> {
    let trimmed = raw.unwrap_or("").trim();
    if trimmed.is_empty() {
        return None;
    }
    let truncated: String = trimmed.chars().take(max_chars).collect();
    Some(truncated)
}

/// Case-insensitive substring match of any non-empty word in any haystack
/// (prohibited-words check shared by the create and edit transactions).
fn contains_any_word(haystacks: &[&str], words: &[String]) -> bool {
    for text in haystacks {
        let lower = text.to_lowercase();
        for word in words {
            let w = word.trim().to_lowercase();
            if !w.is_empty() && lower.contains(w.as_str()) {
                return true;
            }
        }
    }
    false
}

/// Time-ordered part of a notification id: `notify:{kind}:{suffix}` → `{suffix}`.
/// Ids without a `:` are returned unchanged.
fn notification_order_key(id: &str) -> &str {
    id.rsplit(':').next().unwrap_or(id)
}

// --- Batch M: moderation log + instance settings (§3.2, §3.3) -------------

impl RedbStore {
    /// Append one moderation-log row inside an open write transaction so a
    /// moderation action's state change and its log row commit together
    /// (§2.3; conformance H4).
    fn append_log_in_txn(
        &self,
        write_txn: &WriteTransaction,
        entry: &ModerationLogEntry,
    ) -> Result<(), PortError> {
        entry.validate()?;
        let mut table = write_txn.open_table(MODERATION_LOGS).map_err(map_redb_err)?;
        let bytes = encode_json(entry)?;
        table
            .insert(entry.id.as_str(), bytes.as_slice())
            .map_err(map_redb_err)?;
        Ok(())
    }

    /// Delete every follow request involving `account_id` in either
    /// direction (upstream `postSuspend`). Returns the number of request
    /// pairs removed.
    fn delete_follow_requests_involving_in_txn(
        &self,
        write_txn: &WriteTransaction,
        account_id: &str,
    ) -> Result<u64, PortError> {
        // Collect pairs first: the range scan borrows the table (redb
        // borrow rules), and each pair must then be dropped from both
        // index tables via `delete_follow_request_rows`.
        let mut pairs: Vec<(String, String)> = Vec::new(); // (follower, followee)
        {
            let fwd = write_txn
                .open_table(FOLLOW_REQ_FORWARD)
                .map_err(map_redb_err)?;
            for entry in fwd
                .range((account_id, "")..=(account_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
            {
                let (k, _) = entry.map_err(map_redb_err)?;
                let (follower, followee) = k.value();
                pairs.push((follower.to_string(), followee.to_string()));
            }
        }
        {
            let rev = write_txn
                .open_table(FOLLOW_REQ_REVERSE)
                .map_err(map_redb_err)?;
            for entry in rev
                .range((account_id, "")..=(account_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
            {
                // Reverse keys are (followee, follower).
                let (k, _) = entry.map_err(map_redb_err)?;
                let (followee, follower) = k.value();
                pairs.push((follower.to_string(), followee.to_string()));
            }
        }
        let mut removed = 0u64;
        for (follower, followee) in pairs {
            if self.delete_follow_request_rows(write_txn, &follower, &followee)? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Load + patch + re-validate one account row inside an open write
    /// transaction.
    fn set_account_flags_in_txn<F>(
        &self,
        write_txn: &WriteTransaction,
        account_id: &str,
        apply: F,
    ) -> Result<Account, PortError>
    where
        F: FnOnce(&mut Account),
    {
        let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let mut account: Account = match accounts_table.get(account_id).map_err(map_redb_err)? {
            Some(g) => decode_json(g.value())?,
            None => {
                return Err(PortError::NotFound(format!(
                    "account {account_id} not found"
                )))
            }
        };
        apply(&mut account);
        account.validate()?;
        let bytes = encode_json(&account)?;
        accounts_table
            .insert(account.id.as_str(), bytes.as_slice())
            .map_err(map_redb_err)?;
        Ok(account)
    }
}

impl ModerationLogStore for RedbStore {
    fn set_suspended(
        &self,
        account_id: &str,
        suspended: bool,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        if suspended {
            self.delete_follow_requests_involving_in_txn(&write_txn, account_id)?;
        }
        let updated = self.set_account_flags_in_txn(&write_txn, account_id, |account| {
            account.is_suspended = suspended;
        })?;
        self.append_log_in_txn(&write_txn, &log)?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn set_role(
        &self,
        account_id: &str,
        is_admin: bool,
        is_moderator: bool,
        log: ModerationLogEntry,
    ) -> Result<Account, PortError> {
        let write_txn = self.begin_write()?;
        let updated = self.set_account_flags_in_txn(&write_txn, account_id, |account| {
            account.is_admin = is_admin;
            account.is_moderator = is_moderator;
        })?;
        self.append_log_in_txn(&write_txn, &log)?;
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn update_account_logged(
        &self,
        account_id: &str,
        patch: UpdateAccountPatch,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError> {
        let write_txn = self.begin_write()?;
        let updated = self.update_account_in_txn(&write_txn, account_id, patch, jobs)?;
        self.append_log_in_txn(&write_txn, &log)?;
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn append(&self, entry: ModerationLogEntry) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        self.append_log_in_txn(&write_txn, &entry)?;
        self.commit_write(write_txn)
    }

    fn list_logs(
        &self,
        filter: &ModerationLogFilter,
        cursor: &TimelineCursor,
    ) -> Result<Vec<ModerationLogEntry>, PortError> {
        let limit = if cursor.limit == 0 { 10 } else { cursor.limit };
        // Bounded scan (conformance F2): filtered listings stop after this
        // many rows even when more exist.
        const SCAN_CAP: usize = 10_000;
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MODERATION_LOGS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        let mut scanned = 0usize;
        for entry in table.range(""..).map_err(map_redb_err)?.rev() {
            scanned += 1;
            if scanned > SCAN_CAP {
                break;
            }
            let (_, v) = entry.map_err(map_redb_err)?;
            let stored: ModerationLogEntry = decode_json(v.value())?;
            if !matches_cursor(&stored.id, cursor) {
                continue;
            }
            if let Some(action) = filter.action {
                if stored.action != action {
                    continue;
                }
            }
            if let Some(actor) = &filter.actor {
                if stored.actor_account.as_deref() != Some(actor.as_str()) {
                    continue;
                }
            }
            if let Some(target) = &filter.target {
                if stored.target.as_deref() != Some(target.as_str()) {
                    continue;
                }
            }
            if let Some(search) = &filter.search {
                let needle = search.to_ascii_lowercase();
                let haystack = stored.details.to_string().to_ascii_lowercase();
                if !haystack.contains(&needle) {
                    continue;
                }
            }
            out.push(stored);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

impl InstanceSettingsStore for RedbStore {
    fn get(&self) -> Result<InstanceSettings, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(INSTANCE_SETTINGS)
            .map_err(map_redb_err)?;
        match table.get("settings").map_err(map_redb_err)? {
            Some(g) => decode_json(g.value()),
            None => Ok(InstanceSettings::default()),
        }
    }

    fn update(
        &self,
        patch: InstanceSettingsPatch,
        log: ModerationLogEntry,
    ) -> Result<InstanceSettings, PortError> {
        let write_txn = self.begin_write()?;
        let updated = {
            let current = {
                let table = write_txn
                    .open_table(INSTANCE_SETTINGS)
                    .map_err(map_redb_err)?;
                match table.get("settings").map_err(map_redb_err)? {
                    Some(g) => decode_json(g.value())?,
                    None => InstanceSettings::default(),
                }
            };
            let mut settings = current;
            if let Some(registration) = patch.registration {
                settings.registration = registration;
            }
            if let Some(blocked_hosts) = patch.blocked_hosts {
                settings.blocked_hosts = InstanceSettings::normalize_blocked_hosts(&blocked_hosts);
            }
            if let Some(name) = patch.name {
                settings.name = name;
            }
            if let Some(description) = patch.description {
                settings.description = description;
            }
            if let Some(max_note_text_length) = patch.max_note_text_length {
                settings.max_note_text_length = max_note_text_length;
            }
            settings.validate()?;
            let mut table = write_txn
                .open_table(INSTANCE_SETTINGS)
                .map_err(map_redb_err)?;
            let bytes = encode_json(&settings)?;
            table.insert("settings", bytes.as_slice()).map_err(map_redb_err)?;
            settings
        };
        self.append_log_in_txn(&write_txn, &log)?;
        self.commit_write(write_txn)?;
        Ok(updated)
    }
}

/// Row filter shared by the invite listings (keyset + `admin/invite/list`
/// `type` + creator scope).
fn matches_invite_filter(invite: &InviteCode, filter: &InviteListFilter, now_ms: i64) -> bool {
    if let Some(creator) = &filter.created_by {
        if invite.created_by.as_deref() != Some(creator.as_str()) {
            return false;
        }
    }
    match filter.filter {
        InviteTypeFilter::All => {}
        InviteTypeFilter::Unused => {
            if invite.is_used() {
                return false;
            }
        }
        InviteTypeFilter::Used => {
            if !invite.is_used() {
                return false;
            }
        }
        InviteTypeFilter::Expired => {
            if !invite.is_expired(now_ms) {
                return false;
            }
        }
    }
    if let Some(since_id) = &filter.since_id {
        if invite.id.as_str() <= since_id.as_str() {
            return false;
        }
    }
    if let Some(until_id) = &filter.until_id {
        if invite.id.as_str() >= until_id.as_str() {
            return false;
        }
    }
    if let Some(since_date_ms) = filter.since_date_ms {
        if invite.created_at_ms <= since_date_ms {
            return false;
        }
    }
    if let Some(until_date_ms) = filter.until_date_ms {
        if invite.created_at_ms >= until_date_ms {
            return false;
        }
    }
    true
}

impl InviteStore for RedbStore {
    fn create_invites(
        &self,
        inputs: &[CreateInviteInput],
        log: Option<&ModerationLogEntry>,
    ) -> Result<Vec<InviteCode>, PortError> {
        if inputs.is_empty() || inputs.len() > 100 {
            return Err(PortError::Validation(
                "INVALID_PARAM invite count must be 1..=100".to_string(),
            ));
        }
        let now_ms = self.clock.now_ms();
        let write_txn = self.begin_write()?;
        let mut created = Vec::with_capacity(inputs.len());
        {
            let mut invites_table = write_txn.open_table(INVITES).map_err(map_redb_err)?;
            let mut codes_table = write_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
            for input in inputs {
                let id = self.ids.generate();
                let invite = InviteCode::new(
                    id.clone(),
                    input.code.clone(),
                    input.created_by.clone(),
                    now_ms,
                    input.expires_at_ms,
                )?;
                if codes_table
                    .get(invite.code.as_str())
                    .map_err(map_redb_err)?
                    .is_some()
                {
                    return Err(PortError::Conflict(format!(
                        "invite code {} already exists",
                        invite.code
                    )));
                }
                let bytes = encode_json(&invite)?;
                invites_table
                    .insert(id.as_str(), bytes.as_slice())
                    .map_err(map_redb_err)?;
                codes_table
                    .insert(invite.code.as_str(), id.as_str())
                    .map_err(map_redb_err)?;
                created.push(invite);
            }
        }
        if let Some(entry) = log {
            self.append_log_in_txn(&write_txn, entry)?;
        }
        self.commit_write(write_txn)?;
        Ok(created)
    }

    fn find_invite_by_id(&self, id: &str) -> Result<Option<InviteCode>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(INVITES).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn find_invite_by_code(&self, code: &str) -> Result<Option<InviteCode>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let codes_table = read_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
        let Some(id_guard) = codes_table.get(code).map_err(map_redb_err)? else {
            // Consumed or never issued: the unique-index row is the source
            // of truth for "still claimable".
            return Ok(None);
        };
        let id = id_guard.value().to_string();
        drop(id_guard);
        let table = read_txn.open_table(INVITES).map_err(map_redb_err)?;
        match table.get(id.as_str()).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn list_invites(&self, filter: &InviteListFilter) -> Result<Vec<InviteCode>, PortError> {
        if filter.limit == 0 {
            return Ok(Vec::new());
        }
        // Bounded scan (conformance F2 parity): filtered listings stop
        // after this many rows even when more exist.
        const SCAN_CAP: usize = 10_000;
        let now_ms = self.clock.now_ms();
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(INVITES).map_err(map_redb_err)?;
        let mut out: Vec<InviteCode> = Vec::new();
        let mut scanned = 0usize;
        match filter.sort {
            // Id order == created order (aidx); scan in the requested
            // direction so a bounded scan sees the right end first.
            InviteSort::CreatedAsc => {
                for entry in table.range(""..).map_err(map_redb_err)? {
                    scanned += 1;
                    if scanned > SCAN_CAP {
                        break;
                    }
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let invite: InviteCode = decode_json(v.value())?;
                    if matches_invite_filter(&invite, filter, now_ms) {
                        out.push(invite);
                    }
                }
            }
            InviteSort::CreatedDesc => {
                for entry in table.range(""..).map_err(map_redb_err)?.rev() {
                    scanned += 1;
                    if scanned > SCAN_CAP {
                        break;
                    }
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let invite: InviteCode = decode_json(v.value())?;
                    if matches_invite_filter(&invite, filter, now_ms) {
                        out.push(invite);
                    }
                }
            }
            InviteSort::UsedAsc | InviteSort::UsedDesc => {
                for entry in table.range(""..).map_err(map_redb_err)?.rev() {
                    scanned += 1;
                    if scanned > SCAN_CAP {
                        break;
                    }
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let invite: InviteCode = decode_json(v.value())?;
                    if matches_invite_filter(&invite, filter, now_ms) {
                        out.push(invite);
                    }
                }
                out.sort_by(|a, b| {
                    // `+usedAt` = used_at DESC with unused LAST
                    // (`NULLS LAST`); `-usedAt` = used_at ASC with unused
                    // FIRST (`NULLS FIRST`). Option ordering already puts
                    // None first ascending, so only the descending case
                    // inverts. Id breaks ties deterministically.
                    let by_used = match filter.sort {
                        InviteSort::UsedDesc => b.used_at_ms.cmp(&a.used_at_ms),
                        _ => a.used_at_ms.cmp(&b.used_at_ms),
                    };
                    by_used.then_with(|| a.id.cmp(&b.id))
                });
            }
        }
        Ok(out.into_iter().skip(filter.offset).take(filter.limit).collect())
    }

    fn delete_invite(
        &self,
        id: &str,
        log: Option<&ModerationLogEntry>,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        let code = {
            let mut invites_table = write_txn.open_table(INVITES).map_err(map_redb_err)?;
            let Some(guard) = invites_table.get(id).map_err(map_redb_err)? else {
                return Err(PortError::NotFound("NO_SUCH_INVITE_CODE".to_string()));
            };
            let invite: InviteCode = decode_json(guard.value())?;
            drop(guard);
            invites_table.remove(id).map_err(map_redb_err)?;
            invite.code
        };
        {
            let mut codes_table = write_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
            codes_table.remove(code.as_str()).map_err(map_redb_err)?;
        }
        if let Some(entry) = log {
            self.append_log_in_txn(&write_txn, entry)?;
        }
        self.commit_write(write_txn)
    }

    fn consume_and_create_account(
        &self,
        input: ConsumeInviteInput,
    ) -> Result<(Account, InviteCode), PortError> {
        let account = self.build_account(input.account)?;
        let now_ms = self.clock.now_ms();
        // One transaction: the claim (mark used + drop the unique-index
        // row) and the account creation commit together or not at all
        // (conformance I1/I2). A failure here (bad username, duplicate
        // handle, injected fault) aborts everything below.
        let write_txn = self.begin_write()?;
        let consumed = {
            let mut codes_table = write_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
            let mut invites_table = write_txn.open_table(INVITES).map_err(map_redb_err)?;
            let id = match codes_table.get(input.code.as_str()).map_err(map_redb_err)? {
                Some(guard) => guard.value().to_string(),
                None => return Err(PortError::NotFound("NO_SUCH_INVITE_CODE".to_string())),
            };
            let mut invite: InviteCode = match invites_table.get(id.as_str()).map_err(map_redb_err)? {
                Some(guard) => decode_json(guard.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_INVITE_CODE".to_string())),
            };
            // Missing, used, and expired codes fail the same way upstream's
            // signup gate does (`SignupApiService`: `ticket == null ||
            // ticket.usedById != null` → 400).
            if invite.is_used() || invite.is_expired(now_ms) {
                return Err(PortError::NotFound("NO_SUCH_INVITE_CODE".to_string()));
            }
            invite.consume(&account.id, now_ms)?;
            let bytes = encode_json(&invite)?;
            invites_table
                .insert(id.as_str(), bytes.as_slice())
                .map_err(map_redb_err)?;
            codes_table.remove(input.code.as_str()).map_err(map_redb_err)?;
            invite
        };
        self.create_account_in_txn(&write_txn, &account, None)?;
        if let Some(hash) = &input.password_hash {
            let mut credentials = write_txn.open_table(CREDENTIALS).map_err(map_redb_err)?;
            credentials
                .insert(account.id.as_str(), hash.as_str())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok((account, consumed))
    }
}

fn matches_cursor(note_id: &str, cursor: &TimelineCursor) -> bool {
    if let Some(since_id) = &cursor.since_id {
        if note_id <= since_id.as_str() {
            return false;
        }
    }
    if let Some(until_id) = &cursor.until_id {
        if note_id >= until_id.as_str() {
            return false;
        }
    }
    if cursor.since_date.is_some() || cursor.until_date.is_some() {
        if let Ok(ts) = parse_aidx_time(note_id) {
            if let Some(since_date) = cursor.since_date {
                if ts <= since_date {
                    return false;
                }
            }
            if let Some(until_date) = cursor.until_date {
                if ts >= until_date {
                    return false;
                }
            }
        }
    }
    true
}

impl RedbStore {
    /// Validate + construct one account row from `input` (no writes).
    /// The local-username and preserved-name rules run here so every
    /// creation path (CLI, signup, `admin/accounts/create`, invite claim)
    /// shares them.
    fn build_account(&self, input: CreateAccountInput) -> Result<Account, PortError> {
        let username = input.handle.trim().to_string();
        let username_lower = username.to_ascii_lowercase();
        let is_local = input.host.is_none();

        if is_local {
            validate_local_username(&username)?;
            for reserved in &input.preserved_usernames {
                if reserved.eq_ignore_ascii_case(&username_lower) {
                    return Err(PortError::Conflict(format!(
                        "username {username} is reserved"
                    )));
                }
            }
        }

        let account_id = self.ids.generate();
        let account = Account {
            id: account_id.clone(),
            username,
            username_lower: username_lower.clone(),
            host: input.host.clone(),
            display_name: input.display_name,
            description: input.description,
            location: input.location,
            birthday: input.birthday,
            uri: input.uri,
            inbox: input.inbox,
            shared_inbox: input.shared_inbox,
            followers_count: 0,
            following_count: 0,
            notes_count: 0,
            avatar_id: None,
            banner_id: None,
            is_locked: input.is_locked,
            is_bot: input.is_bot,
            is_suspended: false,
            is_admin: false,
            is_moderator: false,
            is_explorable: true,
            is_deleted: false,
            is_cat: input.is_cat,
            // §0B: fresh accounts start at the upstream column defaults.
            hide_online_status: false,
            avatar_decorations: Vec::new(),
            also_known_as: None,
            require_signin_to_view_contents: false,
            make_notes_followers_only_before: None,
            make_notes_hidden_before: None,
            chat_scope: crate::domain::account::ChatScope::default(),
            followed_message: None,
            lang: None,
            fields: Vec::new(),
            verified_links: Vec::new(),
            public_reactions: true,
            following_visibility: crate::domain::account::FollowVisibility::default(),
            followers_visibility: crate::domain::account::FollowVisibility::default(),
            auto_accept_followed: false,
            follow_approval_local_seconds: None,
            follow_approval_remote_seconds: None,
            no_crawle: false,
            prevent_ai_learning: true,
            always_mark_nsfw: false,
            auto_sensitive: false,
            careful_bot: false,
            inject_featured_note: true,
            receive_announcement_email: true,
            pinned_page_id: None,
            enable_word_mute: false,
            muted_words: Vec::new(),
            hard_muted_words: Vec::new(),
            muted_instances: Vec::new(),
            notification_recieve_config: std::collections::BTreeMap::new(),
            email_notification_types: vec![
                "follow".to_string(),
                "receiveFollowRequest".to_string(),
            ],
            reject_quotes: false,
            default_reaction: None,
        };
        account.validate()?;
        Ok(account)
    }

    /// Account uniqueness checks + row/index inserts (+ optional `log`)
    /// inside an open write transaction: the account row, its handle
    /// index, the owner mapping, and the moderation-log row commit
    /// together (§2.3).
    fn create_account_in_txn(
        &self,
        write_txn: &WriteTransaction,
        account: &Account,
        log: Option<&ModerationLogEntry>,
    ) -> Result<(), PortError> {
        let is_local = account.host.is_none();
        let username_lower = account.username_lower.as_str();
        let handle_key = Self::handle_key(&account.username_lower, account.host.as_deref());
        {
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut handle_table = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
            let used_table = write_txn.open_table(USED_USERNAMES).map_err(map_redb_err)?;
            let mut owners_table = write_txn.open_table(ACCOUNT_OWNERS).map_err(map_redb_err)?;

            if is_local
                && used_table
                    .get(username_lower)
                    .map_err(map_redb_err)?
                    .is_some()
            {
                return Err(PortError::Conflict(format!(
                    "handle {username_lower} was previously used"
                )));
            }
            if handle_table
                .get(handle_key.as_str())
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict(format!(
                    "handle {username_lower} is already taken"
                )));
            }

            let encoded = encode_json(account)?;
            accounts_table
                .insert(account.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            handle_table
                .insert(handle_key.as_str(), account.id.as_str())
                .map_err(map_redb_err)?;
            owners_table
                .insert(account.id.as_str(), self.local_node_id.as_str())
                .map_err(map_redb_err)?;
        }
        if let Some(entry) = log {
            self.append_log_in_txn(write_txn, entry)?;
        }
        Ok(())
    }
}

impl AccountStore for RedbStore {
    fn create_account(
        &self,
        input: CreateAccountInput,
        log: Option<&ModerationLogEntry>,
    ) -> Result<Account, PortError> {
        let account = self.build_account(input)?;
        let write_txn = self.begin_write()?;
        self.create_account_in_txn(&write_txn, &account, log)?;
        self.commit_write(write_txn)?;
        Ok(account)
    }

    fn find_by_id(&self, id: &str) -> Result<Option<Account>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn find_by_handle(&self, normalized_handle: &str) -> Result<Option<Account>, PortError> {
        let cleaned = normalized_handle.trim().trim_start_matches('@');
        self.find_by_username_and_host(cleaned, None)
    }

    fn find_by_username_and_host(
        &self,
        username: &str,
        host: Option<&str>,
    ) -> Result<Option<Account>, PortError> {
        let lower = username.trim().to_ascii_lowercase();
        let key = Self::handle_key(&lower, host);
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let handle_table = read_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
        let accounts_table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let Some(id_guard) = handle_table.get(key.as_str()).map_err(map_redb_err)? else {
            return Ok(None);
        };
        let account_id = id_guard.value().to_string();
        match accounts_table.get(account_id.as_str()).map_err(map_redb_err)? {
            Some(acc_guard) => Ok(Some(decode_json(acc_guard.value())?)),
            None => Ok(None),
        }
    }

    fn is_username_available(
        &self,
        username: &str,
        preserved_usernames: &[String],
    ) -> Result<bool, PortError> {
        validate_local_username(username)?;
        let lower = username.to_ascii_lowercase();
        for p in preserved_usernames {
            if p.eq_ignore_ascii_case(&lower) {
                return Ok(false);
            }
        }
        let key = Self::handle_key(&lower, None);
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let handle_table = read_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
        let used_table = read_txn.open_table(USED_USERNAMES).map_err(map_redb_err)?;
        if handle_table
            .get(key.as_str())
            .map_err(map_redb_err)?
            .is_some()
        {
            return Ok(false);
        }
        if used_table
            .get(lower.as_str())
            .map_err(map_redb_err)?
            .is_some()
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn update_account(&self, id: &str, patch: UpdateAccountPatch, jobs: Vec<NewJob>) -> Result<Account, PortError> {
        let write_txn = self.begin_write()?;
        let updated = self.update_account_in_txn(&write_txn, id, patch, jobs)?;
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn rename(&self, id: &str, new_handle: &str) -> Result<Account, PortError> {
        validate_local_username(new_handle)?;
        let new_lower = new_handle.to_ascii_lowercase();
        let new_key = Self::handle_key(&new_lower, None);

        let write_txn = self.begin_write()?;
        let updated = {
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut handle_table = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
            let mut used_table = write_txn.open_table(USED_USERNAMES).map_err(map_redb_err)?;

            let mut acc: Account = match accounts_table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound(format!("account {id} not found"))),
            };
            if acc.host.is_some() {
                return Err(PortError::Validation(
                    "cannot rename remote account".to_string(),
                ));
            }
            if acc.username_lower == new_lower {
                acc.username = new_handle.to_string();
                let bytes = encode_json(&acc)?;
                accounts_table
                    .insert(acc.id.as_str(), bytes.as_slice())
                    .map_err(map_redb_err)?;
                acc
            } else {
                if used_table
                    .get(new_lower.as_str())
                    .map_err(map_redb_err)?
                    .is_some()
                {
                    return Err(PortError::Conflict(format!(
                        "handle {new_lower} was previously used"
                    )));
                }
                if handle_table
                    .get(new_key.as_str())
                    .map_err(map_redb_err)?
                    .is_some()
                {
                    return Err(PortError::Conflict(format!(
                        "handle {new_lower} is already taken"
                    )));
                }
                let old_lower = acc.username_lower.clone();
                let old_key = Self::handle_key(&old_lower, None);
                handle_table
                    .remove(old_key.as_str())
                    .map_err(map_redb_err)?;
                used_table
                    .insert(old_lower.as_str(), &1u8)
                    .map_err(map_redb_err)?;

                acc.username = new_handle.to_string();
                acc.username_lower = new_lower;
                acc.validate()?;
                let bytes = encode_json(&acc)?;
                accounts_table
                    .insert(acc.id.as_str(), bytes.as_slice())
                    .map_err(map_redb_err)?;
                handle_table
                    .insert(new_key.as_str(), acc.id.as_str())
                    .map_err(map_redb_err)?;
                acc
            }
        };
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn get_owner(&self, id: &str) -> Result<String, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let owners = read_txn.open_table(ACCOUNT_OWNERS).map_err(map_redb_err)?;
        if let Some(guard) = owners.get(id).map_err(map_redb_err)? {
            return Ok(guard.value().to_string());
        }
        let accounts = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        if accounts.get(id).map_err(map_redb_err)?.is_some() {
            return Ok(self.local_node_id.clone());
        }
        Err(PortError::NotFound(format!("account {id} not found")))
    }

    fn set_owner(&self, id: &str, node: &str, migration_proof: &str) -> Result<(), PortError> {
        if migration_proof.trim().is_empty() {
            return Err(PortError::Validation(
                "migration_proof is required".to_string(),
            ));
        }
        let write_txn = self.begin_write()?;
        {
            let accounts = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            if accounts.get(id).map_err(map_redb_err)?.is_none() {
                return Err(PortError::NotFound(format!("account {id} not found")));
            }
            let mut owners = write_txn.open_table(ACCOUNT_OWNERS).map_err(map_redb_err)?;
            owners.insert(id, node).map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn search_accounts(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Account>, PortError> {
        let q = query.trim().to_lowercase();
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let mut matches = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, val) = entry.map_err(map_redb_err)?;
            let acc: Account = decode_json(val.value())?;
            if acc.is_deleted || acc.is_suspended {
                continue;
            }
            let name_match = acc
                .display_name
                .as_ref()
                .map(|n| n.to_lowercase().contains(&q))
                .unwrap_or(false);
            if q.is_empty() || acc.username_lower.contains(&q) || name_match {
                matches.push(acc);
            }
        }
        matches.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(matches.into_iter().skip(offset).take(limit).collect())
    }

    fn find_by_uri(&self, uri: &str) -> Result<Option<Account>, PortError> {
        let target = uri.trim();
        if target.is_empty() {
            return Ok(None);
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, val) = entry.map_err(map_redb_err)?;
            let acc: Account = decode_json(val.value())?;
            if acc.uri.as_deref() == Some(target) {
                return Ok(Some(acc));
            }
        }
        Ok(None)
    }
}

impl NoteStore for RedbStore {
    fn create_note(&self, input: CreateNoteInput, jobs: Vec<NewJob>) -> Result<Note, PortError> {
        let write_txn = self.begin_write()?;
        let note = self.create_note_in_txn(&write_txn, input, jobs)?;
        self.commit_write(write_txn)?;
        Ok(note)
    }

    fn apply_note_edit(&self, edit: EditNoteInput, jobs: Vec<NewJob>) -> Result<Note, PortError> {
        let write_txn = self.begin_write()?;
        let result = self.apply_note_edit_in_txn(&write_txn, edit, jobs);
        match result {
            Ok(note) => self.commit_write(write_txn).map(|_| note),
            Err(e) => {
                let _ = write_txn.abort();
                Err(e)
            }
        }
    }

    fn note_edits(&self, note_id: &str) -> Result<Vec<NoteEdit>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_note = read_txn
            .open_table(NOTE_EDITS_BY_NOTE)
            .map_err(map_redb_err)?;
        let edits_table = read_txn.open_table(NOTE_EDITS).map_err(map_redb_err)?;
        // `(note_id, edit_id)` range reads oldest-first: edit ids are
        // time-ordered, so index order is history order.
        let range = by_note
            .range((note_id, "")..=(note_id, "\u{10ffff}"))
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in range {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, edit_id) = k.value();
            if let Some(g) = edits_table.get(edit_id).map_err(map_redb_err)? {
                out.push(decode_json(g.value())?);
            }
        }
        Ok(out)
    }

    fn get_note(&self, id: &str) -> Result<Option<Note>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => {
                let note: Note = decode_json(guard.value())?;
                if note.is_deleted {
                    Ok(None)
                } else {
                    Ok(Some(note))
                }
            }
            None => Ok(None),
        }
    }

    fn get_note_including_deleted(&self, id: &str) -> Result<Option<Note>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn delete_note(&self, id: &str, jobs: Vec<NewJob>) -> Result<(), PortError> {
        // Single implementation: `delete_note_in_txn` carries the tombstone,
        // counter, and reply-index maintenance (Slice A). Both entry points
        // shared one copy-pasted body before; they must never drift again.
        let write_txn = self.begin_write()?;
        let result = self.delete_note_in_txn(&write_txn, id, jobs);
        match result {
            Ok(()) => self.commit_write(write_txn),
            Err(e) => {
                let _ = write_txn.abort();
                Err(e)
            }
        }
    }

    fn notes_by_author(
        &self,
        author_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        let limit = if cursor.limit == 0 { 10 } else { cursor.limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_author = read_txn.open_table(NOTES_BY_AUTHOR).map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;

        let range = by_author
            .range((author_id, "")..=(author_id, "\u{10ffff}"))
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in range.rev() {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, note_id) = k.value();
            if !matches_cursor(note_id, cursor) {
                continue;
            }
            if let Some(ng) = notes_table.get(note_id).map_err(map_redb_err)? {
                let note: Note = decode_json(ng.value())?;
                if !note.is_deleted {
                    out.push(note);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn list_all_notes(&self, cursor: &TimelineCursor) -> Result<Vec<Note>, PortError> {
        let limit = if cursor.limit == 0 { 100 } else { cursor.limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in notes_table.iter().map_err(map_redb_err)?.rev() {
            let (k, v) = entry.map_err(map_redb_err)?;
            let note_id = k.value();
            if !matches_cursor(note_id, cursor) {
                continue;
            }
            let note: Note = decode_json(v.value())?;
            if !note.is_deleted {
                out.push(note);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn find_by_uri(&self, uri: &str) -> Result<Option<Note>, PortError> {
        let target = uri.trim();
        if target.is_empty() {
            return Ok(None);
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_uri = read_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
        let Some(id) = by_uri.get(target).map_err(map_redb_err)?.map(|g| g.value().to_string())
        else {
            return Ok(None);
        };
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        let found = match notes_table.get(id.as_str()).map_err(map_redb_err)? {
            Some(g) => Some(decode_json::<Note>(g.value())?),
            None => None,
        };
        Ok(found.filter(|n| !n.is_deleted))
    }

    fn notes_by_reply(
        &self,
        reply_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        // `limit` is a candidate scan limit for the application-level
        // visibility scan loop, not the final visible page size.
        let limit = if cursor.limit == 0 { 50 } else { cursor.limit };
        if reply_id.is_empty() {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_reply = read_txn.open_table(NOTES_BY_REPLY).map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;

        let range = by_reply
            .range((reply_id, "")..=(reply_id, "\u{10ffff}"))
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in range.rev() {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, note_id) = k.value();
            if !matches_cursor(note_id, cursor) {
                continue;
            }
            if let Some(ng) = notes_table.get(note_id).map_err(map_redb_err)? {
                let note: Note = decode_json(ng.value())?;
                // Deleted rows are removed at delete time; the guard stays
                // so a torn index can never surface a tombstone.
                if !note.is_deleted {
                    out.push(note);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn pending_reply_children(
        &self,
        parent_uri: &str,
        limit: usize,
    ) -> Result<Vec<Note>, PortError> {
        let target = parent_uri.trim();
        if target.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let pending_table = read_txn
            .open_table(NOTES_BY_REPLY_PENDING)
            .map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;

        let range = pending_table
            .range((target, "")..=(target, "\u{10ffff}"))
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in range {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, note_id) = k.value();
            if let Some(ng) = notes_table.get(note_id).map_err(map_redb_err)? {
                let note: Note = decode_json(ng.value())?;
                // Stale rows (child deleted meanwhile — the delete path
                // removes them, this covers torn races) are skipped, not
                // returned.
                if note.is_deleted || note.reply_id.is_some() {
                    continue;
                }
                if note.reply_parent_uri.as_deref() == Some(target) {
                    out.push(note);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn link_pending_reply(
        &self,
        child_id: &str,
        parent_id: &str,
        parent_canonical_uri: &str,
    ) -> Result<Option<Note>, PortError> {
        if child_id.is_empty() || parent_id.is_empty() || parent_canonical_uri.trim().is_empty() {
            return Err(PortError::Validation(
                "link_pending_reply requires child, parent, and parent URI".to_string(),
            ));
        }
        let expected_uri = parent_canonical_uri.trim();
        let write_txn = self.begin_write()?;
        let linked = {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut by_reply_table = write_txn
                .open_table(NOTES_BY_REPLY)
                .map_err(map_redb_err)?;
            let mut by_reply_pending_table = write_txn
                .open_table(NOTES_BY_REPLY_PENDING)
                .map_err(map_redb_err)?;

            let mut child: Note = match notes_table
                .get(child_id)
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Ok(None),
            };
            if child.is_deleted {
                return Ok(None);
            }
            if let Some(existing) = child.reply_id.as_deref() {
                // Already linked: converge with no counter or index change.
                // A torn pending row/URI left behind by a crashed link is
                // cleaned below (committed with this transaction) so the
                // child never appears pending again.
                if existing == parent_id && child.reply_parent_uri.is_some() {
                    let stale_uri = child.reply_parent_uri.clone().unwrap_or_default();
                    child.reply_parent_uri = None;
                    let child_bytes = encode_json(&child)?;
                    notes_table
                        .insert(child.id.as_str(), child_bytes.as_slice())
                        .map_err(map_redb_err)?;
                    let _ = by_reply_pending_table
                        .remove((stale_uri.as_str(), child.id.as_str()))
                        .map_err(map_redb_err)?;
                    drop(notes_table);
                    drop(by_reply_table);
                    drop(by_reply_pending_table);
                    self.commit_write(write_txn)?;
                }
                return Ok(None);
            }
            let Some(pending_uri) = child.reply_parent_uri.clone() else {
                // Genuine root, or linked concurrently: nothing to do.
                return Ok(None);
            };
            if pending_uri.as_str() != expected_uri {
                return Err(PortError::Validation(
                    "REPLY_PARENT_MISMATCH: child pending URI does not match parent canonical URI"
                        .to_string(),
                ));
            }
            let mut parent: Note = match notes_table
                .get(parent_id)
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Ok(None),
            };
            // Cycle guard: the parent chain above the link target must not
            // reach the child (including the trivial self-link). Deleted
            // ancestors are walked through — they keep their `reply_id`.
            // A deleted parent still links: the steady state matches a
            // delete racing the resolver the other way around.
            {
                let mut current = Some(parent.id.clone());
                let mut depth = 0usize;
                while let Some(id) = current {
                    if id.as_str() == child_id {
                        return Err(PortError::Validation(
                            "REPLY_CYCLE: linking would create a reply cycle".to_string(),
                        ));
                    }
                    depth += 1;
                    if depth > 512 {
                        break;
                    }
                    current = match notes_table.get(id.as_str()).map_err(map_redb_err)? {
                        Some(ag) => {
                            let ancestor: Note = decode_json(ag.value())?;
                            ancestor.reply_id
                        }
                        None => None,
                    };
                }
            }

            // Same parent-driven narrowing as a reply whose parent was
            // cached at ingestion (`domain::policy::narrow_on_create` reply
            // rules): the child's stored visibility is the request, the
            // parent only ever tightens it.
            let renote_target: Option<Note> = match child.renote_id.as_deref() {
                Some(rid) => match notes_table.get(rid).map_err(map_redb_err)? {
                    Some(rg) => Some(decode_json(rg.value())?),
                    None => None,
                },
                None => None,
            };
            let narrowed = narrow_on_create(NarrowingInput {
                requested_visibility: child.visibility,
                requested_local_only: child.local_only,
                requested_visible_user_ids: child.visible_user_ids.clone(),
                author_id: &child.author_id,
                text: child.text.as_deref(),
                cw: child.cw.as_deref(),
                renote_target: renote_target.as_ref(),
                reply_target: Some(&parent),
                renote_target_blocks_author: false,
                sensitive_words: &[],
                can_public_note: true,
            })
            .map_err(|e| PortError::Validation(format!("reply link narrowing: {e:?}")))?;

            child.reply_id = Some(parent.id.clone());
            child.reply_parent_uri = None;
            child.reply_user_id = Some(parent.author_id.clone());
            child.visibility = narrowed.visibility;
            child.local_only = narrowed.local_only;
            child.visible_user_ids = narrowed.visible_user_ids;
            let child_bytes = encode_json(&child)?;
            notes_table
                .insert(child.id.as_str(), child_bytes.as_slice())
                .map_err(map_redb_err)?;

            parent.replies_count = parent.replies_count.saturating_add(1);
            let parent_bytes = encode_json(&parent)?;
            notes_table
                .insert(parent.id.as_str(), parent_bytes.as_slice())
                .map_err(map_redb_err)?;

            by_reply_table
                .insert((parent.id.as_str(), child.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
            let _ = by_reply_pending_table
                .remove((pending_uri.as_str(), child.id.as_str()))
                .map_err(map_redb_err)?;
            child
        };
        self.commit_write(write_txn)?;
        Ok(Some(linked))
    }

    fn set_pending_reply_parent(
        &self,
        child_id: &str,
        parent_uri: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let uri = parent_uri.trim();
        if child_id.is_empty()
            || !(uri.starts_with("http://") || uri.starts_with("https://"))
        {
            return Err(PortError::Validation(
                "set_pending_reply_parent requires a note id and an absolute http(s) parent URI"
                    .to_string(),
            ));
        }
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        // No-op paths commit nothing: every branch below that returns
        // `false` performs reads only, so falling through to the commit
        // with no writes is a harmless empty transaction.
        let changed = {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut by_reply_pending_table = write_txn
                .open_table(NOTES_BY_REPLY_PENDING)
                .map_err(map_redb_err)?;
            let stored: Option<Note> = match notes_table
                .get(child_id)
                .map_err(map_redb_err)?
            {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };
            let mut child = match stored {
                Some(note) => note,
                None => return Ok(false),
            };
            if child.is_deleted || child.reply_id.is_some() {
                return Ok(false);
            }
            if child.reply_parent_uri.as_deref() == Some(uri) {
                return Ok(false);
            }
            if let Some(old_uri) = child.reply_parent_uri.clone() {
                let _ = by_reply_pending_table
                    .remove((old_uri.as_str(), child.id.as_str()))
                    .map_err(map_redb_err)?;
            }
            child.reply_parent_uri = Some(uri.to_string());
            let child_bytes = encode_json(&child)?;
            notes_table
                .insert(child.id.as_str(), child_bytes.as_slice())
                .map_err(map_redb_err)?;
            by_reply_pending_table
                .insert((uri, child.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
            true
        };
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(changed)
    }

    fn set_quote_authorization(
        &self,
        note_id: &str,
        state: QuoteAuthorizationState,
        authorization_uri: Option<String>,
        verified_at_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        let note = self.set_quote_authorization_in_txn(
            &write_txn,
            note_id,
            state,
            authorization_uri,
            verified_at_ms,
            jobs,
        )?;
        self.commit_write(write_txn)?;
        Ok(note)
    }

    fn find_note_by_quote_request(&self, request_uri: &str) -> Result<Option<Note>, PortError> {
        let target = request_uri.trim();
        if target.is_empty() {
            return Ok(None);
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        for entry in notes_table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let note: Note = decode_json(v.value())?;
            if note.is_deleted {
                continue;
            }
            if note
                .quote
                .as_ref()
                .and_then(|q| q.request_activity_uri.as_deref())
                == Some(target)
            {
                return Ok(Some(note));
            }
        }
        Ok(None)
    }

    fn find_note_by_quote_stamp(&self, stamp_uri: &str) -> Result<Option<Note>, PortError> {
        let target = stamp_uri.trim();
        if target.is_empty() {
            return Ok(None);
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        for entry in notes_table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let note: Note = decode_json(v.value())?;
            if note.is_deleted {
                continue;
            }
            if note
                .quote
                .as_ref()
                .and_then(|q| q.authorization_uri.as_deref())
                == Some(target)
            {
                return Ok(Some(note));
            }
        }
        Ok(None)
    }

    fn save_quote_stamp(&self, stamp: QuoteStamp, jobs: Vec<NewJob>) -> Result<(), PortError> {
        if stamp.id.trim().is_empty() {
            return Err(PortError::Validation("empty stamp id".to_string()));
        }
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        self.save_quote_stamp_in_txn(&write_txn, &stamp, jobs)?;
        self.commit_write(write_txn)
    }

    fn get_quote_stamp(&self, stamp_id: &str) -> Result<Option<QuoteStamp>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(QUOTE_STAMPS).map_err(map_redb_err)?;
        match table.get(stamp_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn attach_remote_media(
        &self,
        note_id: &str,
        input: InsertMediaInput,
        jobs: Vec<NewJob>,
    ) -> Result<MediaMeta, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        // Build and validate the row before opening the transaction (so a
        // validation failure never touches storage).
        let id = input.id.clone().unwrap_or_else(|| self.ids.generate());
        let meta = MediaMeta {
            id: id.clone(),
            owner_id: input.owner_id.clone(),
            name: input.name.clone(),
            mime: input.mime.clone(),
            size_bytes: input.size_bytes,
            hash: input.hash.clone(),
            blob_key: input.blob_key.clone(),
            blurhash: input.blurhash.clone(),
            properties: input.properties.clone(),
            is_sensitive: input.is_sensitive,
            is_link: input.is_link,
            url: input.url.clone(),
            thumbnail_url: input.thumbnail_url.clone(),
            comment: input.comment.clone(),
            source_url: input.source_url.clone(),
        };
        meta.validate(input.max_file_size)?;
        let write_txn = self.begin_write()?;
        // NOTES and MEDIA_META are each opened exactly once: opening the
        // same table twice in one write txn errors.
        let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
        let mut media_table = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        // Read the note into an owned value first: the access guard borrows
        // the table, so it must be dropped before any early `abort()`.
        let note_opt: Option<Note> = {
            let guard = notes_table.get(note_id).map_err(map_redb_err)?;
            guard
                .map(|g| decode_json(g.value()))
                .transpose()?
        };
        let mut note: Note = match note_opt {
            Some(n) => n,
            None => {
                drop(notes_table);
                drop(media_table);
                let _ = write_txn.abort();
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
        };
        if note.is_deleted {
            drop(notes_table);
            drop(media_table);
            let _ = write_txn.abort();
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        // Idempotency per (source_url, note): a row for this URL that the
        // note already references is returned as-is — no second insert, so
        // job retries and inbox replays never orphan rows. The scan collects
        // the hit first so the iterator is dropped before any `abort()`.
        let mut idempotent_hit: Option<MediaMeta> = None;
        if let Some(url) = meta.source_url.as_deref() {
            let target = url.trim();
            if !target.is_empty() {
                let attached: std::collections::HashSet<&str> =
                    note.file_ids.iter().map(String::as_str).collect();
                for entry in media_table.iter().map_err(map_redb_err)? {
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let existing: MediaMeta = decode_json(v.value())?;
                    if existing.source_url.as_deref().map(str::trim) == Some(target)
                        && attached.contains(existing.id.as_str())
                    {
                        idempotent_hit = Some(existing);
                        break;
                    }
                }
            }
        }
        if let Some(existing) = idempotent_hit {
            drop(notes_table);
            drop(media_table);
            let _ = write_txn.abort();
            return Ok(existing);
        }
        let author: Account = {
            let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let author_opt: Option<Account> = {
                let guard = accounts_table
                    .get(note.author_id.as_str())
                    .map_err(map_redb_err)?;
                guard
                    .map(|g| decode_json(g.value()))
                    .transpose()?
            };
            drop(accounts_table);
            match author_opt {
                Some(a) => a,
                None => {
                    drop(notes_table);
                    drop(media_table);
                    let _ = write_txn.abort();
                    return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
                }
            }
        };
        // Same ownership rule as `create_note_in_txn`: the row must belong
        // to the author, or be an unowned remote-cache row for a remote
        // author.
        let owner_ok = input.owner_id.as_deref() == Some(note.author_id.as_str())
            || (author.host.is_some() && input.owner_id.is_none());
        if !owner_ok {
            drop(notes_table);
            drop(media_table);
            let _ = write_txn.abort();
            return Err(PortError::Validation("NO_SUCH_FILE".to_string()));
        }
        if note.file_ids.len() >= MAX_FILE_IDS {
            drop(notes_table);
            drop(media_table);
            let _ = write_txn.abort();
            return Err(PortError::Validation(
                "TOO_MANY_ATTACHMENTS".to_string(),
            ));
        }
        // `note.file_ids` uniqueness is a domain invariant; skip ids the
        // note already references (defensive: same outcome as idempotent).
        if !note.file_ids.contains(&id) {
            note.file_ids.push(id);
        }
        let note_bytes = encode_json(&note)?;
        notes_table
            .insert(note.id.as_str(), note_bytes.as_slice())
            .map_err(map_redb_err)?;
        let meta_bytes = encode_json(&meta)?;
        media_table
            .insert(meta.id.as_str(), meta_bytes.as_slice())
            .map_err(map_redb_err)?;
        drop(notes_table);
        drop(media_table);
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(meta)
    }

    // --- Note drafts (§5) -------------------------------------------------

    fn create_draft(&self, draft: NoteDraft, jobs: Vec<NewJob>) -> Result<NoteDraft, PortError> {
        draft
            .validate_shape()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        let write_txn = self.begin_write()?;
        {
            let mut drafts_table = write_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
            let mut by_user_table = write_txn
                .open_table(NOTE_DRAFTS_BY_USER)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&draft)?;
            drafts_table
                .insert(draft.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            by_user_table
                .insert((draft.user_id.as_str(), draft.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(draft)
    }

    fn get_draft(&self, id: &str) -> Result<Option<NoteDraft>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn update_draft(&self, draft: NoteDraft, jobs: Vec<NewJob>) -> Result<NoteDraft, PortError> {
        draft
            .validate_shape()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        // Existence + ownership pre-check in a read txn (the write txn
        // below must not abort while table guards are live).
        let existing: NoteDraft = match self.get_draft(&draft.id)? {
            Some(existing) => existing,
            None => return Err(PortError::NotFound("NO_SUCH_NOTE_DRAFT".to_string())),
        };
        if existing.user_id != draft.user_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        let write_txn = self.begin_write()?;
        {
            let mut drafts_table = write_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
            let encoded = encode_json(&draft)?;
            drafts_table
                .insert(draft.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(draft)
    }

    fn delete_draft(&self, id: &str, jobs: Vec<NewJob>) -> Result<(), PortError> {
        // Resolve the owner first (read txn) so the write txn below never
        // aborts while table guards are live.
        let existing = self.get_draft(id)?;
        let write_txn = self.begin_write()?;
        {
            let mut drafts_table = write_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
            let mut by_user_table = write_txn
                .open_table(NOTE_DRAFTS_BY_USER)
                .map_err(map_redb_err)?;
            if let Some(draft) = existing {
                drafts_table.remove(id).map_err(map_redb_err)?;
                let _ = by_user_table.remove((draft.user_id.as_str(), id));
            }
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn drafts_by_user(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
        scheduled: Option<bool>,
    ) -> Result<Vec<NoteDraft>, PortError> {
        let limit = if cursor.limit == 0 { 30 } else { cursor.limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTE_DRAFTS_BY_USER)
            .map_err(map_redb_err)?;
        let drafts_table = read_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in by_user
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .rev()
        {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, draft_id) = k.value();
            if !matches_cursor(draft_id, cursor) {
                continue;
            }
            if let Some(guard) = drafts_table.get(draft_id).map_err(map_redb_err)? {
                let draft: NoteDraft = decode_json(guard.value())?;
                if let Some(want_scheduled) = scheduled {
                    if draft.is_actually_scheduled != want_scheduled {
                        continue;
                    }
                }
                out.push(draft);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn count_drafts(&self, user_id: &str) -> Result<usize, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTE_DRAFTS_BY_USER)
            .map_err(map_redb_err)?;
        Ok(by_user
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .count())
    }

    fn count_scheduled_drafts(&self, user_id: &str) -> Result<usize, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTE_DRAFTS_BY_USER)
            .map_err(map_redb_err)?;
        let drafts_table = read_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
        let mut count = 0usize;
        for entry in by_user
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, draft_id) = k.value();
            if let Some(guard) = drafts_table.get(draft_id).map_err(map_redb_err)? {
                let draft: NoteDraft = decode_json(guard.value())?;
                if draft.is_actually_scheduled {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    // --- Note pinnings (§7) -----------------------------------------------

    fn add_pinning(
        &self,
        pinning: UserNotePining,
        jobs: Vec<NewJob>,
    ) -> Result<UserNotePining, PortError> {
        pinning
            .validate()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        // Duplicate pre-check in a read txn (the write txn below must not
        // abort while table guards are live). The unique key makes the
        // residual race converge to the same `ALREADY_PINNED`.
        if self
            .pinnings_for_user(&pinning.user_id)?
            .iter()
            .any(|p| p.note_id == pinning.note_id)
        {
            return Err(PortError::Conflict("ALREADY_PINNED".to_string()));
        }
        let write_txn = self.begin_write()?;
        {
            let mut pinnings_table = write_txn.open_table(PINNINGS).map_err(map_redb_err)?;
            if pinnings_table
                .get((pinning.user_id.as_str(), pinning.note_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                drop(pinnings_table);
                let _ = write_txn.abort();
                return Err(PortError::Conflict("ALREADY_PINNED".to_string()));
            }
            let encoded = encode_json(&pinning)?;
            pinnings_table
                .insert(
                    (pinning.user_id.as_str(), pinning.note_id.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(pinning)
    }

    fn remove_pinning(
        &self,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut pinnings_table = write_txn.open_table(PINNINGS).map_err(map_redb_err)?;
            let _ = pinnings_table.remove((user_id, note_id));
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn pinnings_for_user(&self, user_id: &str) -> Result<Vec<UserNotePining>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let pinnings_table = read_txn.open_table(PINNINGS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in pinnings_table
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        // Oldest pin first (pinning-id order; aidx ids are time-ordered),
        // matching the upstream `pins.map(...)` pin order.
        out.sort_by(|a: &UserNotePining, b: &UserNotePining| a.id.cmp(&b.id));
        Ok(out)
    }
}

impl JobQueue for RedbStore {
    fn claim_next(&self, lease_duration_ms: i64) -> Result<Option<Job>, PortError> {
        let now = self.clock.now_ms();
        let write_txn = self.begin_write()?;
        let claimed = {
            let mut jobs_table = write_txn.open_table(JOBS).map_err(map_redb_err)?;
            let mut due_table = write_txn.open_table(JOBS_DUE_INDEX).map_err(map_redb_err)?;

            let mut candidate: Option<(i64, String, Job)> = None;
            for entry in due_table
                .range((i64::MIN, "")..=(now, "\u{10ffff}"))
                .map_err(map_redb_err)?
            {
                let (k, _) = entry.map_err(map_redb_err)?;
                let (due_ts, job_id_str) = k.value();
                let job_id = job_id_str.to_string();
                if let Some(jg) = jobs_table.get(job_id.as_str()).map_err(map_redb_err)? {
                    let job: Job = decode_json(jg.value())?;
                    // Terminal (quarantined) jobs never re-enter the claimable
                    // set even if a stale due-index entry still points at one.
                    if job.failed {
                        continue;
                    }
                    let lease_expired = match job.lease_expires {
                        None => true,
                        Some(exp) => exp <= now,
                    };
                    if job.not_before <= now && lease_expired {
                        candidate = Some((due_ts, job_id, job));
                        break;
                    }
                }
            }

            if let Some((old_due, job_id, mut job)) = candidate {
                let was_leased = job.lease_expires.is_some();
                let new_lease = now + lease_duration_ms.max(1);
                due_table
                    .remove((old_due, job_id.as_str()))
                    .map_err(map_redb_err)?;
                // Reclaiming an expired lease counts as an attempt: a job
                // whose handler panicked/crashed without settling must burn
                // retry budget instead of looping forever. Fresh jobs
                // (`lease_expires == None`) must NOT increment.
                let quarantined = if was_leased {
                    job.attempts = job.attempts.saturating_add(1);
                    if job.attempts >= MAX_JOB_ATTEMPTS {
                        job.failed = true;
                        job.fail_reason = Some("retry-exhausted".to_string());
                        job.lease_expires = None;
                        let encoded = encode_json(&job)?;
                        jobs_table
                            .insert(job.id.as_str(), encoded.as_slice())
                            .map_err(map_redb_err)?;
                        // No new due-index entry: quarantined jobs leave the
                        // claimable set but stay inspectable via `get_job`.
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if quarantined {
                    None
                } else {
                    job.lease_expires = Some(new_lease);
                    let encoded = encode_json(&job)?;
                    jobs_table
                        .insert(job.id.as_str(), encoded.as_slice())
                        .map_err(map_redb_err)?;
                    due_table
                        .insert((new_lease, job.id.as_str()), &1u8)
                        .map_err(map_redb_err)?;
                    Some(job)
                }
            } else {
                None
            }
        };
        self.commit_write(write_txn)?;
        Ok(claimed)
    }

    fn complete(&self, job_id: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        self.complete_in_txn(&write_txn, job_id)?;
        self.commit_write(write_txn)
    }

    fn complete_with_followups(
        &self,
        job_id: &str,
        followups: Vec<NewJob>,
    ) -> Result<(), PortError> {
        if followups.len() > MAX_JOBS_PER_TXN {
            return Err(PortError::Validation(format!(
                "complete_with_followups: {} followups exceed the {MAX_JOBS_PER_TXN} per-txn cap",
                followups.len()
            )));
        }
        // Validate before opening the transaction so a bad followup never
        // leaves a half-applied completion behind.
        for job in &followups {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        // `complete_in_txn` opens and drops its own table guards before the
        // followups open theirs (redb rejects opening a table twice).
        self.complete_in_txn(&write_txn, job_id)?;
        self.enqueue_jobs_in_txn(&write_txn, followups, None)?;
        self.commit_write(write_txn)
    }

    fn enqueue_jobs(&self, jobs: Vec<NewJob>) -> Result<(), PortError> {
        if jobs.len() > MAX_JOBS_PER_TXN {
            return Err(PortError::Validation(format!(
                "enqueue_jobs: {} jobs exceed the {MAX_JOBS_PER_TXN} per-txn cap",
                jobs.len()
            )));
        }
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)
    }

    fn defer(&self, job_id: &str, not_before_ms: i64) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut jobs_table = write_txn.open_table(JOBS).map_err(map_redb_err)?;
            let mut due_table = write_txn.open_table(JOBS_DUE_INDEX).map_err(map_redb_err)?;
            let mut job: Job = match jobs_table.get(job_id).map_err(map_redb_err)? {
                Some(jg) => decode_json(jg.value())?,
                // Missing jobs are a no-op success, like `complete`.
                None => return Ok(()),
            };
            // Terminal state is sticky: a quarantined job is never rescheduled.
            if job.failed {
                return Ok(());
            }
            let _ = due_table.remove((job.not_before, job_id));
            if let Some(exp) = job.lease_expires {
                let _ = due_table.remove((exp, job_id));
            }
            // Release the lease without touching `attempts`: a deferral is
            // not a failed delivery and must not burn retry budget.
            job.lease_expires = None;
            job.not_before = not_before_ms;
            let encoded = encode_json(&job)?;
            jobs_table
                .insert(job_id, encoded.as_slice())
                .map_err(map_redb_err)?;
            due_table
                .insert((not_before_ms, job_id), &1u8)
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn fail(&self, job_id: &str, retry_at_ms: i64) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut jobs_table = write_txn.open_table(JOBS).map_err(map_redb_err)?;
            let mut due_table = write_txn.open_table(JOBS_DUE_INDEX).map_err(map_redb_err)?;
            let mut job: Job = match jobs_table.get(job_id).map_err(map_redb_err)? {
                Some(jg) => decode_json(jg.value())?,
                // Missing jobs are a no-op success, like `complete`.
                None => return Ok(()),
            };
            // Terminal state is sticky: `fail` never re-queues a quarantined job.
            if job.failed {
                return Ok(());
            }
            let _ = due_table.remove((job.not_before, job_id));
            if let Some(exp) = job.lease_expires {
                let _ = due_table.remove((exp, job_id));
            }
            job.attempts = job.attempts.saturating_add(1);
            job.lease_expires = None;
            if job.attempts >= MAX_JOB_ATTEMPTS {
                // Retry budget exhausted: auto-quarantine instead of
                // rescheduling forever. Same reason string the worker uses
                // when it quarantines for exhaustion directly.
                job.failed = true;
                job.fail_reason = Some("retry-exhausted".to_string());
                let encoded = encode_json(&job)?;
                jobs_table
                    .insert(job_id, encoded.as_slice())
                    .map_err(map_redb_err)?;
            } else {
                job.not_before = retry_at_ms;
                let encoded = encode_json(&job)?;
                jobs_table
                    .insert(job_id, encoded.as_slice())
                    .map_err(map_redb_err)?;
                due_table
                    .insert((retry_at_ms, job_id), &1u8)
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)
    }

    fn fail_permanent(&self, job_id: &str, reason: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut jobs_table = write_txn.open_table(JOBS).map_err(map_redb_err)?;
            let mut due_table = write_txn.open_table(JOBS_DUE_INDEX).map_err(map_redb_err)?;
            let mut job: Job = match jobs_table.get(job_id).map_err(map_redb_err)? {
                Some(jg) => decode_json(jg.value())?,
                // Missing jobs are a no-op success, like `complete`.
                None => return Ok(()),
            };
            // Idempotent: keep the first recorded reason.
            if job.failed {
                return Ok(());
            }
            let _ = due_table.remove((job.not_before, job_id));
            if let Some(exp) = job.lease_expires {
                let _ = due_table.remove((exp, job_id));
            }
            job.failed = true;
            job.fail_reason = Some(truncate_fail_reason(reason));
            job.lease_expires = None;
            let encoded = encode_json(&job)?;
            jobs_table
                .insert(job_id, encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn get_job(&self, job_id: &str) -> Result<Option<Job>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(JOBS).map_err(map_redb_err)?;
        match table.get(job_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn pending_count(&self) -> Result<usize, PortError> {
        // Counts actionable (non-terminal) jobs only: quarantined rows stay
        // in the table for inspection via `get_job` but are not pending work,
        // so drains that loop on `pending_count() == 0` still terminate.
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(JOBS).map_err(map_redb_err)?;
        let mut count = 0usize;
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, val) = entry.map_err(map_redb_err)?;
            let job: Job = decode_json(val.value())?;
            if !job.failed {
                count += 1;
            }
        }
        Ok(count)
    }

    fn remove_scheduled_draft_jobs(&self, draft_id: &str) -> Result<usize, PortError> {
        // `clearSchedule` parity (linear scan, like the upstream
        // delayed/waiting/active queue scan): remove every non-terminal
        // `PostScheduledDraft` job armed for `draft_id`, including leased
        // ones (a worker holding a removed job converges quietly at
        // settlement: `complete` on a missing row is a no-op, and the
        // handler re-checks the draft's arming before posting).
        use crate::application::ports::{JobKind, ScheduledDraftPayload};
        let targets: Vec<(String, i64, Option<i64>)> = {
            let read_txn = self.db.begin_read().map_err(map_redb_err)?;
            let table = read_txn.open_table(JOBS).map_err(map_redb_err)?;
            let mut out = Vec::new();
            for entry in table.iter().map_err(map_redb_err)? {
                let (k, v) = entry.map_err(map_redb_err)?;
                let job: Job = decode_json(v.value())?;
                if job.failed || job.kind != JobKind::PostScheduledDraft {
                    continue;
                }
                let payload: ScheduledDraftPayload = match serde_json::from_slice(&job.payload) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                if payload.draft_id == draft_id {
                    out.push((k.value().to_string(), job.not_before, job.lease_expires));
                }
            }
            out
        };
        if targets.is_empty() {
            return Ok(0);
        }
        let write_txn = self.begin_write()?;
        {
            let mut jobs_table = write_txn.open_table(JOBS).map_err(map_redb_err)?;
            let mut due_table = write_txn.open_table(JOBS_DUE_INDEX).map_err(map_redb_err)?;
            for (job_id, not_before, lease_expires) in &targets {
                jobs_table.remove(job_id.as_str()).map_err(map_redb_err)?;
                let _ = due_table.remove((*not_before, job_id.as_str()));
                if let Some(exp) = lease_expires {
                    let _ = due_table.remove((*exp, job_id.as_str()));
                }
            }
        }
        let removed = targets.len();
        self.commit_write(write_txn)?;
        Ok(removed)
    }
}

/// Cap a quarantine reason so an unbounded remote response body can never
/// bloat a job row (reasons stay far under the 64 KiB job payload cap).
fn truncate_fail_reason(reason: &str) -> String {
    const MAX_REASON_CHARS: usize = 512;
    reason.chars().take(MAX_REASON_CHARS).collect()
}

impl FollowStore for RedbStore {
    fn follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError> {
        let write_txn = self.begin_write()?;
        let follow = self.follow_in_txn(
            &write_txn,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        )?;
        self.commit_write(write_txn)?;
        Ok(follow)
    }

    fn unfollow(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        self.unfollow_in_txn(&write_txn, follower_id, followee_id, jobs)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn is_following(&self, follower_id: &str, followee_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let fwd_table = read_txn.open_table(FOLLOWS_FORWARD).map_err(map_redb_err)?;
        Ok(fwd_table
            .get((follower_id, followee_id))
            .map_err(map_redb_err)?
            .is_some())
    }

    fn get_follow(
        &self,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<Option<Follow>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let fwd_table = read_txn.open_table(FOLLOWS_FORWARD).map_err(map_redb_err)?;
        match fwd_table
            .get((follower_id, followee_id))
            .map_err(map_redb_err)?
        {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn followers_of(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Follow>, PortError> {
        let limit = if limit == 0 { 100 } else { limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let rev_table = read_txn.open_table(FOLLOWS_REVERSE).map_err(map_redb_err)?;
        let start = cursor.unwrap_or("");
        let mut out = Vec::new();
        for entry in rev_table
            .range((account_id, start)..=(account_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (k, v) = entry.map_err(map_redb_err)?;
            let (_, follower_id) = k.value();
            if Some(follower_id) == cursor {
                continue;
            }
            out.push(decode_json(v.value())?);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn following_of(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Follow>, PortError> {
        let limit = if limit == 0 { 100 } else { limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let fwd_table = read_txn.open_table(FOLLOWS_FORWARD).map_err(map_redb_err)?;
        let start = cursor.unwrap_or("");
        let mut out = Vec::new();
        for entry in fwd_table
            .range((account_id, start)..=(account_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (k, v) = entry.map_err(map_redb_err)?;
            let (_, followee_id) = k.value();
            if Some(followee_id) == cursor {
                continue;
            }
            out.push(decode_json(v.value())?);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn request_follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<FollowRequest, PortError> {
        let write_txn = self.begin_write()?;
        let req = self.request_follow_in_txn(
            &write_txn,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        )?;
        self.commit_write(write_txn)?;
        Ok(req)
    }

    fn get_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<Option<FollowRequest>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let t = read_txn.open_table(FOLLOW_REQ_FORWARD).map_err(map_redb_err)?;
        match t.get((follower_id, followee_id)).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn follow_requests_to(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<FollowRequest>, PortError> {
        let limit = if limit == 0 { 100 } else { limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let t = read_txn.open_table(FOLLOW_REQ_REVERSE).map_err(map_redb_err)?;
        let start = cursor.unwrap_or("");
        let mut out = Vec::new();
        for entry in t
            .range((account_id, start)..=(account_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (k, v) = entry.map_err(map_redb_err)?;
            if Some(k.value().1) == cursor {
                continue;
            }
            out.push(decode_json(v.value())?);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn accept_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError> {
        let write_txn = self.begin_write()?;
        let req: FollowRequest = {
            let t = write_txn.open_table(FOLLOW_REQ_FORWARD).map_err(map_redb_err)?;
            let got = t.get((follower_id, followee_id)).map_err(map_redb_err)?;
            match got {
                Some(g) => decode_json(g.value())?,
                None => {
                    return Err(PortError::NotFound("NO_FOLLOW_REQUEST".to_string()))
                }
            }
        };
        let follow = self.follow_in_txn(
            &write_txn,
            follower_id,
            followee_id,
            req.with_replies,
            req.notify,
            jobs,
        )?;
        self.commit_write(write_txn)?;
        Ok(follow)
    }

    fn remove_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        if !self.delete_follow_request_rows(&write_txn, follower_id, followee_id)? {
            return Err(PortError::NotFound("NO_FOLLOW_REQUEST".to_string()));
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
    }
}

impl HostCapsStore for RedbStore {
    fn get_host_software(&self, host: &str) -> Result<Option<HostSoftware>, PortError> {
        let key = host.to_ascii_lowercase();
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(HOST_SOFTWARE).map_err(map_redb_err)?;
        match table.get(key.as_str()).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn put_host_software(&self, entry: HostSoftware) -> Result<(), PortError> {
        let key = entry.host.to_ascii_lowercase();
        let bytes = encode_json(&HostSoftware {
            host: key.clone(),
            ..entry
        })?;
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(HOST_SOFTWARE).map_err(map_redb_err)?;
            table.insert(key.as_str(), bytes.as_slice()).map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(())
    }
}

impl BlockStore for RedbStore {
    fn block(&self, blocking: Blocking, jobs: Vec<NewJob>) -> Result<Blocking, PortError> {
        let write_txn = self.begin_write()?;
        let stored = self.block_in_txn(&write_txn, &blocking, jobs)?;
        self.commit_write(write_txn)?;
        Ok(stored)
    }

    fn unblock(
        &self,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        self.unblock_in_txn(&write_txn, blocker_id, blockee_id, jobs)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn get_blocking(
        &self,
        blocker_id: &str,
        blockee_id: &str,
    ) -> Result<Option<Blocking>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(BLOCKINGS_FORWARD).map_err(map_redb_err)?;
        match table.get((blocker_id, blockee_id)).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn is_blocking(&self, blocker_id: &str, blockee_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(BLOCKINGS_FORWARD).map_err(map_redb_err)?;
        Ok(table
            .get((blocker_id, blockee_id))
            .map_err(map_redb_err)?
            .is_some())
    }

    fn blocking_ids_for(&self, blocker_id: &str) -> Result<Vec<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(BLOCKINGS_FORWARD).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let (blocker, _) = k.value();
            if blocker != blocker_id {
                continue;
            }
            let row: Blocking = decode_json(v.value())?;
            out.push(row.blockee_id);
        }
        Ok(out)
    }

    fn blocked_by_ids_for(&self, blockee_id: &str) -> Result<Vec<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(BLOCKINGS_REVERSE).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let (blockee, _) = k.value();
            if blockee != blockee_id {
                continue;
            }
            let row: Blocking = decode_json(v.value())?;
            out.push(row.blocker_id);
        }
        Ok(out)
    }

    fn list_blockings(
        &self,
        blocker_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Blocking>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(BLOCKINGS_FORWARD).map_err(map_redb_err)?;
        let mut rows: Vec<Blocking> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let (blocker, _) = k.value();
            if blocker != blocker_id {
                continue;
            }
            rows.push(decode_json(v.value())?);
        }
        drop(table);
        drop(read_txn);
        Ok(self.apply_id_cursor(rows, cursor, |r| r.id.as_str()))
    }
}

impl MuteStore for RedbStore {
    fn mute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        expires_at_ms: Option<i64>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Muting>, PortError> {
        validate_mute_params(muter_id, mutee_id)?;
        for job in &jobs {
            job.validate()?;
        }
        // Misskey `mute/create` parity: a past `expiresAt` creates no mute
        // and is not an error.
        if let Some(exp) = expires_at_ms {
            if exp <= self.clock.now_ms() {
                return Ok(None);
            }
            validate_mute_expiry(exp, self.clock.now_ms())?;
        }
        let muting = Muting {
            id: self.ids.generate(),
            muter_id: muter_id.to_string(),
            mutee_id: mutee_id.to_string(),
            expires_at_ms,
        };
        muting.validate()?;
        let write_txn = self.begin_write()?;
        {
            let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            for id in [muter_id, mutee_id] {
                if accounts_table.get(id).map_err(map_redb_err)?.is_none() {
                    return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
                }
            }
            drop(accounts_table);
            let mut table = write_txn.open_table(MUTINGS_FORWARD).map_err(map_redb_err)?;
            if table
                .get((muter_id, mutee_id))
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict("ALREADY_MUTING".to_string()));
            }
            let bytes = encode_json(&muting)?;
            table
                .insert((muter_id, mutee_id), bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(Some(muting))
    }

    fn unmute(&self, muter_id: &str, mutee_id: &str, jobs: Vec<NewJob>) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(MUTINGS_FORWARD).map_err(map_redb_err)?;
            if table
                .remove((muter_id, mutee_id))
                .map_err(map_redb_err)?
                .is_none()
            {
                return Err(PortError::NotFound("NOT_MUTING".to_string()));
            }
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn get_muting(
        &self,
        muter_id: &str,
        mutee_id: &str,
    ) -> Result<Option<Muting>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MUTINGS_FORWARD).map_err(map_redb_err)?;
        match table.get((muter_id, mutee_id)).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn is_muting(&self, muter_id: &str, mutee_id: &str) -> Result<bool, PortError> {
        let now = self.clock.now_ms();
        Ok(self
            .get_muting(muter_id, mutee_id)?
            .map(|m| m.is_active_at(now))
            .unwrap_or(false))
    }

    fn muted_ids_for(&self, muter_id: &str) -> Result<Vec<String>, PortError> {
        let now = self.clock.now_ms();
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MUTINGS_FORWARD).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let (muter, _) = k.value();
            if muter != muter_id {
                continue;
            }
            let row: Muting = decode_json(v.value())?;
            if row.is_active_at(now) {
                out.push(row.mutee_id);
            }
        }
        Ok(out)
    }

    fn list_mutings(
        &self,
        muter_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Muting>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MUTINGS_FORWARD).map_err(map_redb_err)?;
        let mut rows: Vec<Muting> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let (muter, _) = k.value();
            if muter != muter_id {
                continue;
            }
            rows.push(decode_json(v.value())?);
        }
        drop(table);
        drop(read_txn);
        Ok(self.apply_id_cursor(rows, cursor, |r| r.id.as_str()))
    }

    fn renote_mute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<RenoteMuting, PortError> {
        validate_mute_params(muter_id, mutee_id)?;
        for job in &jobs {
            job.validate()?;
        }
        let row = RenoteMuting {
            id: self.ids.generate(),
            muter_id: muter_id.to_string(),
            mutee_id: mutee_id.to_string(),
        };
        row.validate()?;
        let write_txn = self.begin_write()?;
        {
            let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            for id in [muter_id, mutee_id] {
                if accounts_table.get(id).map_err(map_redb_err)?.is_none() {
                    return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
                }
            }
            drop(accounts_table);
            let mut table = write_txn
                .open_table(RENOTE_MUTINGS_FORWARD)
                .map_err(map_redb_err)?;
            if table
                .get((muter_id, mutee_id))
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict("ALREADY_MUTING".to_string()));
            }
            let bytes = encode_json(&row)?;
            table
                .insert((muter_id, mutee_id), bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(row)
    }

    fn renote_unmute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(RENOTE_MUTINGS_FORWARD)
                .map_err(map_redb_err)?;
            if table
                .remove((muter_id, mutee_id))
                .map_err(map_redb_err)?
                .is_none()
            {
                return Err(PortError::NotFound("NOT_MUTING".to_string()));
            }
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn get_renote_muting(
        &self,
        muter_id: &str,
        mutee_id: &str,
    ) -> Result<Option<RenoteMuting>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(RENOTE_MUTINGS_FORWARD)
            .map_err(map_redb_err)?;
        match table.get((muter_id, mutee_id)).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn is_renote_muting(&self, muter_id: &str, mutee_id: &str) -> Result<bool, PortError> {
        Ok(self.get_renote_muting(muter_id, mutee_id)?.is_some())
    }

    fn renote_muted_ids_for(&self, muter_id: &str) -> Result<Vec<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(RENOTE_MUTINGS_FORWARD)
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let (muter, _) = k.value();
            if muter != muter_id {
                continue;
            }
            let row: RenoteMuting = decode_json(v.value())?;
            out.push(row.mutee_id);
        }
        Ok(out)
    }

    fn list_renote_mutings(
        &self,
        muter_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<RenoteMuting>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(RENOTE_MUTINGS_FORWARD)
            .map_err(map_redb_err)?;
        let mut rows: Vec<RenoteMuting> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let (muter, _) = k.value();
            if muter != muter_id {
                continue;
            }
            rows.push(decode_json(v.value())?);
        }
        drop(table);
        drop(read_txn);
        Ok(self.apply_id_cursor(rows, cursor, |r| r.id.as_str()))
    }
}

impl ReactionStore for RedbStore {
    fn react(
        &self,
        user_id: &str,
        note_id: &str,
        reaction_str: &str,
        reaction_id: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Reaction, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let reaction = Reaction {
            id: reaction_id.to_string(),
            user_id: user_id.to_string(),
            note_id: note_id.to_string(),
            reaction: reaction_str.to_string(),
        };
        reaction.validate()?;

        let write_txn = self.begin_write()?;
        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut reactions_table = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;

            let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            // §0A: reacting to an author who blocks the reactor is rejected
            // (Misskey `ReactionService.create` parity: block check first,
            // then visibility). Self-reactions are exempt.
            if note.author_id != user_id
                && self.blocking_exists_in_txn(&write_txn, &note.author_id, user_id)?
            {
                return Err(PortError::Validation(
                    "YOU_HAVE_BEEN_BLOCKED".to_string(),
                ));
            }

            let existing_opt: Option<Reaction> = match reactions_table
                .get((user_id, note_id))
                .map_err(map_redb_err)?
            {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };

            if let Some(prev) = existing_opt {
                if !upsert {
                    return Err(PortError::Conflict("ALREADY_REACTED".to_string()));
                }
                if let Some(cnt) = note.reactions.get_mut(&prev.reaction) {
                    *cnt = cnt.saturating_sub(1);
                    if *cnt == 0 {
                        note.reactions.remove(&prev.reaction);
                    }
                }
            }

            *note.reactions.entry(reaction.reaction.clone()).or_insert(0) += 1;
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;

            let reaction_bytes = encode_json(&reaction)?;
            reactions_table
                .insert((user_id, note_id), reaction_bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, Some(note_id))?;
        self.commit_write(write_txn)?;
        Ok(reaction)
    }

    fn unreact(
        &self,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut reactions_table = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;

            let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }

            let prev: Reaction = match reactions_table
                .remove((user_id, note_id))
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NOT_REACTED".to_string())),
            };

            if let Some(cnt) = note.reactions.get_mut(&prev.reaction) {
                *cnt = cnt.saturating_sub(1);
                if *cnt == 0 {
                    note.reactions.remove(&prev.reaction);
                }
            }
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, Some(note_id))?;
        self.commit_write(write_txn)
    }

    fn get_reaction(
        &self,
        user_id: &str,
        note_id: &str,
    ) -> Result<Option<Reaction>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REACTIONS).map_err(map_redb_err)?;
        match table.get((user_id, note_id)).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn reactions_by_user(&self, user_id: &str) -> Result<Vec<Reaction>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REACTIONS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        Ok(out)
    }

    fn reactions_for_note(&self, note_id: &str) -> Result<Vec<Reaction>, PortError> {
        // No by-note index exists (reactions are keyed `(user, note)` for
        // the `users/reactions` read path), so this scans and filters. Reads
        // are not transaction-size limited; the §8 edit fan-out is the only
        // caller and pages its deliveries through the standard split.
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REACTIONS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let reaction: Reaction = decode_json(v.value())?;
            if reaction.note_id == note_id {
                out.push(reaction);
            }
        }
        Ok(out)
    }
}

impl PollStore for RedbStore {
    fn get_poll(&self, note_id: &str) -> Result<Option<Poll>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(POLLS).map_err(map_redb_err)?;
        match table.get(note_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn votes_by_user_for_note(
        &self,
        user_id: &str,
        note_id: &str,
    ) -> Result<Vec<PollVote>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(POLL_VOTES).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table
            .range((user_id, note_id, i64::MIN)..=(user_id, note_id, i64::MAX))
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        out.sort_by_key(|v: &PollVote| v.choice);
        Ok(out)
    }

    fn cast_vote(&self, vote: PollVote, jobs: Vec<NewJob>) -> Result<PollVote, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        if vote.choice > 10_000 {
            return Err(PortError::Validation("INVALID_CHOICE".to_string()));
        }
        let write_txn = self.begin_write()?;
        {
            let notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut polls_table = write_txn.open_table(POLLS).map_err(map_redb_err)?;
            let mut votes_table = write_txn.open_table(POLL_VOTES).map_err(map_redb_err)?;

            let note: Note = match notes_table.get(vote.note_id.as_str()).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            let mut poll: Poll = match polls_table
                .get(vote.note_id.as_str())
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NO_POLL".to_string())),
            };
            // §0A: voting on an author who blocks the voter is rejected
            // (Misskey `PollService.vote` block-check parity). Self-votes
            // are exempt. Re-checked here (not only in the application
            // layer) so the gate holds inside the atomic transaction.
            if note.author_id != vote.user_id
                && self.blocking_exists_in_txn(&write_txn, &note.author_id, &vote.user_id)?
            {
                return Err(PortError::Validation(
                    "YOU_HAVE_BEEN_BLOCKED".to_string(),
                ));
            }
            if poll.is_expired(self.clock.now_ms()) {
                return Err(PortError::Validation("ALREADY_EXPIRED".to_string()));
            }
            if vote.choice >= poll.choices.len() {
                return Err(PortError::Validation("INVALID_CHOICE".to_string()));
            }
            let choice_idx = vote.choice as i64;

            if poll.multiple {
                if votes_table
                    .get((vote.user_id.as_str(), vote.note_id.as_str(), choice_idx))
                    .map_err(map_redb_err)?
                    .is_some()
                {
                    return Err(PortError::Conflict("ALREADY_VOTED".to_string()));
                }
            } else {
                let already_voted = votes_table
                    .range(
                        (
                            vote.user_id.as_str(),
                            vote.note_id.as_str(),
                            i64::MIN,
                        )..=(
                            vote.user_id.as_str(),
                            vote.note_id.as_str(),
                            i64::MAX,
                        ),
                    )
                    .map_err(map_redb_err)?
                    .next()
                    .is_some();
                if already_voted {
                    return Err(PortError::Conflict("ALREADY_VOTED".to_string()));
                }
            }

            // `UPDATE votes[i] += 1` (Misskey 1-based SQL index parity:
            // `votes[$1]` with `index = choice + 1`; ours is 0-based).
            if let Some(slot) = poll.votes.get_mut(vote.choice) {
                *slot = slot.saturating_add(1);
            } else {
                return Err(PortError::Validation("INVALID_CHOICE".to_string()));
            }
            let poll_bytes = encode_json(&poll)?;
            polls_table
                .insert(vote.note_id.as_str(), poll_bytes.as_slice())
                .map_err(map_redb_err)?;
            let vote_bytes = encode_json(&vote)?;
            votes_table
                .insert(
                    (
                        vote.user_id.as_str(),
                        vote.note_id.as_str(),
                        choice_idx,
                    ),
                    vote_bytes.as_slice(),
                )
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, Some(&vote.note_id))?;
        self.commit_write(write_txn)?;
        Ok(vote)
    }

    fn sync_poll_votes(
        &self,
        note_id: &str,
        counts_by_name: &[PollChoiceCount],
        jobs: Vec<NewJob>,
    ) -> Result<Poll, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        let updated = {
            let mut polls_table = write_txn.open_table(POLLS).map_err(map_redb_err)?;
            let mut poll: Poll = match polls_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_POLL".to_string())),
            };
            // Misskey `updateQuestion` parity: match stored choices by name;
            // unknown remote names are ignored, missing names keep counts.
            for (i, choice) in poll.choices.iter().enumerate() {
                if let Some(count) = counts_by_name.iter().find(|c| &c.name == choice) {
                    if let Some(slot) = poll.votes.get_mut(i) {
                        *slot = count.votes;
                    }
                }
            }
            let poll_bytes = encode_json(&poll)?;
            polls_table
                .insert(note_id, poll_bytes.as_slice())
                .map_err(map_redb_err)?;
            poll
        };
        self.enqueue_jobs_in_txn(&write_txn, jobs, Some(note_id))?;
        self.commit_write(write_txn)?;
        Ok(updated)
    }
}

impl ActivityDedup for RedbStore {
    fn is_recorded(&self, activity_id: &str) -> Result<bool, PortError> {
        if activity_id.trim().is_empty() {
            return Err(PortError::Validation("empty activity_id".to_string()));
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACTIVITY_DEDUP).map_err(map_redb_err)?;
        Ok(table.get(activity_id).map_err(map_redb_err)?.is_some())
    }

    fn forget_activity(&self, activity_id: &str) -> Result<(), PortError> {
        if activity_id.trim().is_empty() {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ACTIVITY_DEDUP).map_err(map_redb_err)?;
            table.remove(activity_id).map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn check_and_record(&self, activity_id: &str) -> Result<bool, PortError> {
        if activity_id.trim().is_empty() {
            return Err(PortError::Validation("empty activity_id".to_string()));
        }
        let write_txn = self.begin_write()?;
        let already_seen = {
            let mut table = write_txn.open_table(ACTIVITY_DEDUP).map_err(map_redb_err)?;
            let exists = table.get(activity_id).map_err(map_redb_err)?.is_some();
            if !exists {
                table.insert(activity_id, &1u8).map_err(map_redb_err)?;
            }
            exists
        };
        self.commit_write(write_txn)?;
        Ok(already_seen)
    }

    fn record_note_activity(
        &self,
        activity_id: &str,
        input: CreateNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        if activity_id.trim().is_empty() {
            return Err(PortError::Validation("empty activity_id".to_string()));
        }
        let write_txn = self.begin_write()?;
        let already_seen = {
            let mut table = write_txn.open_table(ACTIVITY_DEDUP).map_err(map_redb_err)?;
            let exists = table.get(activity_id).map_err(map_redb_err)?.is_some();
            if !exists {
                table.insert(activity_id, &1u8).map_err(map_redb_err)?;
            }
            exists
        };
        if already_seen {
            let _ = write_txn.abort();
            return Ok(None);
        }
        // The same note arriving under a different activity id (an `ap/show`
        // fetch, a quote-target cache, a re-wrapped `Create`) is a replay of
        // the note, not a second note.
        match self.create_note_in_txn(&write_txn, input, jobs) {
            Ok(note) => {
                self.commit_write(write_txn)?;
                Ok(Some(note))
            }
            // A tombstoned URI is a deleted note arriving again: same as a replay.
            Err(PortError::Conflict(m))
                if m == "DUPLICATE_NOTE_URI" || m == "TOMBSTONED_NOTE_URI" =>
            {
                let _ = write_txn.abort();
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn record_delete_note_activity(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(false);
        }
        self.delete_note_in_txn(&write_txn, note_id, jobs)?;
        self.commit_write(write_txn)?;
        Ok(true)
    }

    /// Delete by object URI. Deletes the live note holding `object_uri` if
    /// one exists (it may have appeared since the caller's lookup), otherwise
    /// records a tombstone. Both happen in this one write transaction, so a
    /// concurrent `Create` either commits first (and is deleted here) or sees
    /// the tombstone and is refused. `jobs` are only used when a note is
    /// deleted; a tombstone-only outcome has no note to announce, so they are
    /// dropped.
    fn record_delete_uri_activity(
        &self,
        activity_id: &str,
        object_uri: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let uri = object_uri.trim();
        if uri.is_empty() {
            return Err(PortError::Validation("empty object uri".to_string()));
        }
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(false);
        }
        let holder = {
            let by_uri = write_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
            let holder = by_uri
                .get(uri)
                .map_err(map_redb_err)?
                .map(|g| g.value().to_string());
            holder
        };
        let mut deleted = false;
        if let Some(note_id) = holder {
            match self.delete_note_in_txn(&write_txn, &note_id, jobs) {
                Ok(()) => deleted = true,
                // The index pointed at a row that is already deleted: treat as
                // absent and fall through to the tombstone.
                Err(PortError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        if !deleted {
            let mut tombstones = write_txn
                .open_table(TOMBSTONES_BY_URI)
                .map_err(map_redb_err)?;
            tombstones.insert(uri, &1u8).map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(deleted)
    }

    fn record_note_edit_activity(
        &self,
        activity_id: &str,
        edit: EditNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        let note = self.apply_note_edit_in_txn(&write_txn, edit, jobs)?;
        self.commit_write(write_txn)?;
        Ok(Some(note))
    }

    fn record_follow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Follow>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        let follow = self.follow_in_txn(
            &write_txn,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        )?;
        self.commit_write(write_txn)?;
        Ok(Some(follow))
    }

    fn record_follow_request_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<FollowRequest>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        let req = self.request_follow_in_txn(
            &write_txn,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        )?;
        self.commit_write(write_txn)?;
        Ok(Some(req))
    }

    fn record_unfollow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(false);
        }
        self.unfollow_in_txn(&write_txn, follower_id, followee_id, jobs)?;
        self.commit_write(write_txn)?;
        Ok(true)
    }

    fn record_reject_follow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(false);
        }
        match self.unfollow_in_txn(&write_txn, follower_id, followee_id, jobs) {
            Ok(()) => {}
            Err(PortError::Validation(msg)) if msg.contains("NOT_FOLLOWING") => {}
            Err(e) => return Err(e),
        }
        self.commit_write(write_txn)?;
        Ok(true)
    }

    fn record_block_activity(
        &self,
        activity_id: &str,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Blocking>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        let blocking = Blocking {
            id: self.ids.generate(),
            blocker_id: blocker_id.to_string(),
            blockee_id: blockee_id.to_string(),
        };
        let stored = match self.block_in_txn(&write_txn, &blocking, jobs) {
            Ok(stored) => stored,
            // A different activity re-blocking an existing pair converges
            // (Misskey would hit its unique constraint here; swallowing
            // keeps redelivery from storming).
            Err(PortError::Conflict(msg)) if msg == "ALREADY_BLOCKING" => {
                let _ = write_txn.abort();
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        self.commit_write(write_txn)?;
        Ok(Some(stored))
    }

    fn record_unblock_activity(
        &self,
        activity_id: &str,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(false);
        }
        match self.unblock_in_txn(&write_txn, blocker_id, blockee_id, jobs) {
            Ok(()) => {}
            Err(PortError::NotFound(msg)) if msg == "NOT_BLOCKING" => {
                let _ = write_txn.abort();
                return Ok(false);
            }
            Err(e) => return Err(e),
        }
        self.commit_write(write_txn)?;
        Ok(true)
    }

    fn record_react_activity(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        reaction: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Reaction>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        let r = self.react_in_txn(&write_txn, user_id, note_id, reaction, upsert, jobs)?;
        self.commit_write(write_txn)?;
        Ok(Some(r))
    }

    fn record_unreact_activity(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(false);
        }
        self.unreact_in_txn(&write_txn, user_id, note_id, jobs)?;
        self.commit_write(write_txn)?;
        Ok(true)
    }

    fn record_update_account_activity(
        &self,
        activity_id: &str,
        account_id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Account>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        let acc = self.update_account_in_txn(&write_txn, account_id, patch, jobs)?;
        self.commit_write(write_txn)?;
        Ok(Some(acc))
    }

    fn record_quote_request_accept(
        &self,
        activity_id: &str,
        stamp: QuoteStamp,
        jobs: Vec<NewJob>,
    ) -> Result<Option<QuoteStamp>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        self.save_quote_stamp_in_txn(&write_txn, &stamp, jobs)?;
        self.commit_write(write_txn)?;
        Ok(Some(stamp))
    }

    fn record_quote_request_reject(
        &self,
        activity_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(false);
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(true)
    }

    fn record_quote_decision_accept(
        &self,
        activity_id: &str,
        note_id: &str,
        stamp_uri: &str,
        verified_at_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        let note = self.set_quote_authorization_in_txn(
            &write_txn,
            note_id,
            QuoteAuthorizationState::Approved,
            Some(stamp_uri.to_string()),
            verified_at_ms,
            jobs,
        )?;
        self.commit_write(write_txn)?;
        Ok(Some(note))
    }

    fn record_quote_decision_reject(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        // Rejection retains the relation and API projection; only the state
        // flips. `now` is only used for Approved-family timestamps.
        let now = self.clock.now_ms();
        let note = self.set_quote_authorization_in_txn(
            &write_txn,
            note_id,
            QuoteAuthorizationState::Rejected,
            None,
            now,
            jobs,
        )?;
        self.commit_write(write_txn)?;
        Ok(Some(note))
    }

    fn record_quote_stamp_delete(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let write_txn = self.begin_write()?;
        if self.check_and_mark_dedup_in_txn(&write_txn, activity_id)? {
            let _ = write_txn.abort();
            return Ok(None);
        }
        let now = self.clock.now_ms();
        let note = self.set_quote_authorization_in_txn(
            &write_txn,
            note_id,
            QuoteAuthorizationState::Revoked,
            None,
            now,
            jobs,
        )?;
        // Mark the backing stamp row revoked in the same transaction so it
        // stops dereferencing (get_quote_stamp gates on !revoked).
        if let Some(stamp_uri) = note
            .quote
            .as_ref()
            .and_then(|q| q.authorization_uri.as_deref())
            .map(ToOwned::to_owned)
        {
            let stamp_bytes: Option<Vec<u8>> = {
                let stamps = write_txn.open_table(QUOTE_STAMPS).map_err(map_redb_err)?;
                match stamps.get(stamp_uri.as_str()).map_err(map_redb_err)? {
                    Some(g) => Some(g.value().to_vec()),
                    None => None,
                }
            };
            if let Some(raw) = stamp_bytes {
                let mut stamp: QuoteStamp = decode_json(&raw)?;
                stamp.revoked = true;
                let bytes = encode_json(&stamp)?;
                let stamp_id = stamp.id.clone();
                let mut stamps = write_txn.open_table(QUOTE_STAMPS).map_err(map_redb_err)?;
                stamps
                    .insert(stamp_id.as_str(), bytes.as_slice())
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(Some(note))
    }
}

impl MediaStore for RedbStore {
    fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError> {
        let id = input.id.unwrap_or_else(|| self.ids.generate());
        let meta = MediaMeta {
            id,
            owner_id: input.owner_id,
            name: input.name,
            mime: input.mime,
            size_bytes: input.size_bytes,
            hash: input.hash,
            blob_key: input.blob_key,
            blurhash: input.blurhash,
            properties: input.properties,
            is_sensitive: input.is_sensitive,
            is_link: input.is_link,
            url: input.url,
            thumbnail_url: input.thumbnail_url,
            comment: input.comment,
            source_url: input.source_url,
        };
        meta.validate(input.max_file_size)?;

        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
            let encoded = encode_json(&meta)?;
            table
                .insert(meta.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(meta)
    }

    fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn find_by_owner_and_hash(
        &self,
        owner_id: &str,
        hash: &str,
    ) -> Result<Option<MediaMeta>, PortError> {
        // Slice 8 (D6): the dedup key stays the md5 (`MediaMeta.hash`), not
        // the SHA-256 blob address — same bytes → same md5 → existing row.
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let meta: MediaMeta = decode_json(v.value())?;
            if meta.owner_id.as_deref() == Some(owner_id) && meta.hash == hash {
                return Ok(Some(meta));
            }
        }
        Ok(None)
    }

    fn find_by_source_url(&self, source_url: &str) -> Result<Vec<MediaMeta>, PortError> {
        let target = source_url.trim();
        if target.is_empty() {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let meta: MediaMeta = decode_json(v.value())?;
            if meta.source_url.as_deref().map(str::trim) == Some(target) {
                out.push(meta);
            }
        }
        Ok(out)
    }

    fn delete_metadata(&self, id: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
            table.remove(id).map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn list_by_owner(&self, owner_id: &str) -> Result<Vec<MediaMeta>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let meta: MediaMeta = decode_json(v.value())?;
            if meta.owner_id.as_deref() == Some(owner_id) {
                out.push(meta);
            }
        }
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
    }
}

impl NotificationStore for RedbStore {
    fn insert_notification(&self, notification: Notification) -> Result<Notification, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut notif_table = write_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
            let mut by_user = write_txn
                .open_table(NOTIFICATIONS_BY_USER)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&notification)?;
            notif_table
                .insert(notification.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            by_user
                .insert(
                    (
                        notification.notifiee_id.as_str(),
                        notification_order_key(&notification.id),
                    ),
                    notification.id.as_str(),
                )
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(notification)
    }

    fn list_notifications(
        &self,
        notifiee_id: &str,
        limit: usize,
    ) -> Result<Vec<Notification>, PortError> {
        let limit = if limit == 0 { 20 } else { limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let notif_table = read_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTIFICATIONS_BY_USER)
            .map_err(map_redb_err)?;

        let mut out = Vec::new();
        for entry in by_user
            .range((notifiee_id, "")..=(notifiee_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .rev()
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            let nid = v.value();
            if let Some(g) = notif_table.get(nid).map_err(map_redb_err)? {
                out.push(decode_json(g.value())?);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn list_notifications_filtered(
        &self,
        notifiee_id: &str,
        cursor: &TimelineCursor,
        include: &[NotificationKind],
        exclude: &[NotificationKind],
    ) -> Result<Vec<Notification>, PortError> {
        let limit = if cursor.limit == 0 { 10 } else { cursor.limit };
        // Cursor ids are full notification ids; compare them on the order key.
        let aid_cursor = TimelineCursor {
            since_id: cursor.since_id.as_deref().map(|s| notification_order_key(s).to_string()),
            until_id: cursor.until_id.as_deref().map(|s| notification_order_key(s).to_string()),
            ..cursor.clone()
        };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let notif_table = read_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTIFICATIONS_BY_USER)
            .map_err(map_redb_err)?;

        let mut out = Vec::new();
        for entry in by_user
            .range((notifiee_id, "")..=(notifiee_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .rev()
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            let nid = v.value();
            if !matches_cursor(notification_order_key(nid), &aid_cursor) {
                continue;
            }
            let Some(g) = notif_table.get(nid).map_err(map_redb_err)? else {
                continue;
            };
            let stored: Notification = decode_json(g.value())?;
            if !include.is_empty() && !include.contains(&stored.kind) {
                continue;
            }
            if exclude.contains(&stored.kind) {
                continue;
            }
            out.push(stored);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn mark_all_read(&self, notifiee_id: &str) -> Result<u64, PortError> {
        let write_txn = self.begin_write()?;
        let flipped = {
            let mut notif_table = write_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
            let by_user = write_txn
                .open_table(NOTIFICATIONS_BY_USER)
                .map_err(map_redb_err)?;
            let mut flipped: u64 = 0;
            let ids: Vec<String> = by_user
                .range((notifiee_id, "")..=(notifiee_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().map(|(_, v)| v.value().to_string())
                })
                .collect();
            for nid in ids {
                let raw: Option<Vec<u8>> = notif_table
                    .get(nid.as_str())
                    .map_err(map_redb_err)?
                    .map(|g| g.value().to_vec());
                let Some(raw) = raw else { continue };
                let mut stored: Notification = decode_json(&raw)?;
                if stored.is_read {
                    continue;
                }
                stored.is_read = true;
                let encoded = encode_json(&stored)?;
                notif_table
                    .insert(nid.as_str(), encoded.as_slice())
                    .map_err(map_redb_err)?;
                flipped += 1;
            }
            flipped
        };
        self.commit_write(write_txn)?;
        Ok(flipped)
    }
}

impl AuthStore for RedbStore {
    fn set_password_hash(&self, account_id: &str, password_hash: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(CREDENTIALS).map_err(map_redb_err)?;
            table
                .insert(account_id, password_hash)
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn get_password_hash(&self, account_id: &str) -> Result<Option<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CREDENTIALS).map_err(map_redb_err)?;
        match table.get(account_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(g.value().to_string())),
            None => Ok(None),
        }
    }

    fn store_token(&self, record: ApiTokenRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
            let encoded = encode_json(&record)?;
            table
                .insert(record.token_hash.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn get_token_by_hash(&self, token_hash: &str) -> Result<Option<ApiTokenRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
        match table.get(token_hash).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn delete_token_by_hash(&self, token_hash: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
            table.remove(token_hash).map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn save_miauth_session(&self, session: MiAuthSessionRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(MIAUTH_SESSIONS)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&session)?;
            table
                .insert(session.session.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn get_miauth_session(&self, session_id: &str) -> Result<Option<MiAuthSessionRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MIAUTH_SESSIONS).map_err(map_redb_err)?;
        match table.get(session_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn consume_miauth_session(
        &self,
        session_id: &str,
    ) -> Result<Option<MiAuthSessionRecord>, PortError> {
        let write_txn = self.begin_write()?;
        let consumed = {
            let mut table = write_txn
                .open_table(MIAUTH_SESSIONS)
                .map_err(map_redb_err)?;
            let sess_opt: Option<MiAuthSessionRecord> =
                match table.get(session_id).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            match sess_opt {
                Some(mut sess)
                    if !sess.fetched && sess.token.is_some() && sess.account_id.is_some() =>
                {
                    sess.fetched = true;
                    let encoded = encode_json(&sess)?;
                    table
                        .insert(session_id, encoded.as_slice())
                        .map_err(map_redb_err)?;
                    Some(sess)
                }
                _ => None,
            }
        };
        self.commit_write(write_txn)?;
        Ok(consumed)
    }

    fn save_legacy_session(&self, session: LegacyAuthSessionRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(LEGACY_SESSIONS)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&session)?;
            table
                .insert(session.token.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn get_legacy_session(&self, token: &str) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(LEGACY_SESSIONS).map_err(map_redb_err)?;
        match table.get(token).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn take_legacy_session(
        &self,
        token: &str,
    ) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut table = write_txn
                .open_table(LEGACY_SESSIONS)
                .map_err(map_redb_err)?;
            let existing: Option<LegacyAuthSessionRecord> =
                match table.get(token).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            match existing {
                Some(s) if s.account_id.is_some() && s.access_token.is_some() => {
                    table.remove(token).map_err(map_redb_err)?;
                    Some(s)
                }
                Some(s) => Some(s),
                None => None,
            }
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn get_actor_keypair(
        &self,
        account_id: &str,
    ) -> Result<Option<ActorKeypairRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACTOR_KEYPAIRS).map_err(map_redb_err)?;
        match table.get(account_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn save_actor_keypair(&self, record: ActorKeypairRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ACTOR_KEYPAIRS).map_err(map_redb_err)?;
            let encoded = encode_json(&record)?;
            table
                .insert(record.account_id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn get_remote_public_key(
        &self,
        key_id: &str,
    ) -> Result<Option<RemotePublicKeyRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(REMOTE_PUBLIC_KEYS)
            .map_err(map_redb_err)?;
        match table.get(key_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn save_remote_public_key(&self, record: RemotePublicKeyRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(REMOTE_PUBLIC_KEYS)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&record)?;
            table
                .insert(record.key_id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }
}
