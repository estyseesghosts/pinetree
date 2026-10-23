use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::{ACTIVITY_DEDUP, NOTES_BY_URI, QUOTE_STAMPS, RedbStore, TOMBSTONES_BY_URI};
use crate::application::ports::{
    ActivityDedup, CreateNoteInput, EditNoteInput, NewJob, PortError, UpdateAccountPatch,
};
use crate::domain::account::Account;
use crate::domain::blocking::Blocking;
use crate::domain::follow::{Follow, FollowNotify, FollowRequest};
use crate::domain::note::Note;
use crate::domain::quote::{QuoteAuthorizationState, QuoteStamp};
use crate::domain::reaction::Reaction;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, WriteTransaction};

impl RedbStore {
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
                // The failed `create_note_in_txn` performed only reads
                // before the URI check, so the only mutation in this
                // transaction is the new activity's dedup mark. Commit it
                // so the next delivery of this activity is `already_seen`
                // instead of another full insert attempt (replay flood).
                self.commit_write(write_txn)?;
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
