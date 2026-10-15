use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use crate::application::ports::{
    AccountStore, ActivityDedup, ActorKeypairRecord, ApiTokenRecord, AuthStore, Clock,
    CreateAccountInput, CreateNoteInput, FollowStore, IdGenerator, InsertMediaInput, Job,
    JobQueue, LegacyAuthSessionRecord, MediaStore, MiAuthSessionRecord, NewJob, NoteStore,
    NotificationStore, PortError, ReactionStore, RemotePublicKeyRecord, TimelineCursor,
    UpdateAccountPatch, MAX_JOB_ATTEMPTS,
};
use crate::domain::account::{validate_local_username, Account};
use crate::domain::follow::{Follow, FollowNotify, FollowRequest};
use crate::domain::id::{parse_aidx_time, AidxGenerator};
use crate::domain::media::MediaMeta;
use crate::domain::note::{extract_hashtags, extract_mentions, Note, MAX_FILE_IDS};
use crate::domain::notification::Notification;
use crate::domain::policy::{narrow_on_create, NarrowingInput};
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
const FOLLOW_REQ_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follow_requests_forward");
const FOLLOW_REQ_REVERSE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follow_requests_reverse");
const REACTIONS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("reactions");
const JOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("jobs");
const JOBS_DUE_INDEX: TableDefinition<(i64, &str), u8> = TableDefinition::new("jobs_due_index");
const ACTIVITY_DEDUP: TableDefinition<&str, u8> = TableDefinition::new("activity_dedup");
const MEDIA_META: TableDefinition<&str, &[u8]> = TableDefinition::new("media_meta");
const NOTIFICATIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("notifications");
const NOTIFICATIONS_BY_USER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("notifications_by_user");
const CREDENTIALS: TableDefinition<&str, &str> = TableDefinition::new("credentials");
const API_TOKENS: TableDefinition<&str, &[u8]> = TableDefinition::new("api_tokens");
const MIAUTH_SESSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("miauth_sessions");
const LEGACY_SESSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("legacy_sessions");
const ACTOR_KEYPAIRS: TableDefinition<&str, &[u8]> = TableDefinition::new("actor_keypairs");
const REMOTE_PUBLIC_KEYS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("remote_public_keys");

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
        let _ = write_txn.open_table(FOLLOW_REQ_FORWARD).map_err(map_redb_err)?;
        let _ = write_txn.open_table(FOLLOW_REQ_REVERSE).map_err(map_redb_err)?;
        let _ = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;
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

    fn create_note_in_txn(
        &self,
        write_txn: &WriteTransaction,
        input: CreateNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        if input.has_poll {
            return Err(PortError::Validation(
                "polls are unsupported in Phase 1".to_string(),
            ));
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

        // Validate reply_to and renote_of targets
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

        let narrowed = narrow_on_create(NarrowingInput {
            requested_visibility: input.visibility,
            requested_local_only: input.local_only,
            requested_visible_user_ids: input.visible_users.clone(),
            author_id: &input.author_id,
            text: input.text.as_deref(),
            cw: input.cw.as_deref(),
            renote_target: renote_target.as_ref(),
            reply_target: reply_target.as_ref(),
            renote_target_blocks_author: false,
            sensitive_words: &input.sensitive_words,
            can_public_note: true,
        })?;

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

        let note_id = self.ids.generate();
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
        };
        if author.host.is_some() {
            note.validate_storage()?;
        } else {
            note.validate_api_create()?;
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

        drop(accounts_table);
        drop(handle_table);
        drop(notes_table);
        drop(by_author_table);
        drop(media_table);

        self.enqueue_jobs_in_txn(write_txn, jobs, Some(&note.id))?;
        Ok(note)
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
            let mut note: Note = match notes_table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            let was_pure_renote = note.renote_id.is_some()
                && note.text.is_none()
                && note.cw.is_none()
                && note.file_ids.is_empty();
            let renote_target_id = note.renote_id.clone();

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
            if let Some(v) = patch.location {
                acc.location = v;
            }
            if let Some(v) = patch.birthday {
                acc.birthday = v;
            }
            if let Some(v) = patch.avatar_id {
                acc.avatar_id = v;
            }
            if let Some(v) = patch.banner_id {
                acc.banner_id = v;
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

impl AccountStore for RedbStore {
    fn create_account(&self, input: CreateAccountInput) -> Result<Account, PortError> {
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
            is_explorable: true,
            is_deleted: false,
            is_cat: input.is_cat,
        };
        account.validate()?;

        let handle_key = Self::handle_key(&username_lower, account.host.as_deref());
        let write_txn = self.begin_write()?;
        {
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut handle_table = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
            let used_table = write_txn.open_table(USED_USERNAMES).map_err(map_redb_err)?;
            let mut owners_table = write_txn.open_table(ACCOUNT_OWNERS).map_err(map_redb_err)?;

            if is_local
                && used_table
                    .get(username_lower.as_str())
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

            let encoded = encode_json(&account)?;
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

    fn update_account(&self, id: &str, patch: UpdateAccountPatch) -> Result<Account, PortError> {
        let write_txn = self.begin_write()?;
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
            if let Some(v) = patch.location {
                acc.location = v;
            }
            if let Some(v) = patch.birthday {
                acc.birthday = v;
            }
            if let Some(v) = patch.avatar_id {
                acc.avatar_id = v;
            }
            if let Some(v) = patch.banner_id {
                acc.banner_id = v;
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
            acc.validate()?;
            let bytes = encode_json(&acc)?;
            accounts_table
                .insert(acc.id.as_str(), bytes.as_slice())
                .map_err(map_redb_err)?;
            acc
        };
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
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut note: Note = match notes_table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            let was_pure_renote = note.renote_id.is_some()
                && note.text.is_none()
                && note.cw.is_none()
                && note.file_ids.is_empty();
            let renote_target_id = note.renote_id.clone();

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
        self.enqueue_jobs_in_txn(&write_txn, jobs, Some(id))?;
        self.commit_write(write_txn)
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
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        for entry in notes_table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let note: Note = decode_json(v.value())?;
            if !note.is_deleted && note.uri.as_deref() == Some(target) {
                return Ok(Some(note));
            }
        }
        Ok(None)
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
        {
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

impl ReactionStore for RedbStore {
    fn react(
        &self,
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
        let note = self.create_note_in_txn(&write_txn, input, jobs)?;
        self.commit_write(write_txn)?;
        Ok(Some(note))
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
                    (notification.notifiee_id.as_str(), notification.id.as_str()),
                    &1u8,
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
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, nid) = k.value();
            if let Some(g) = notif_table.get(nid).map_err(map_redb_err)? {
                out.push(decode_json(g.value())?);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
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
