use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::{ACCOUNTS, RedbStore};
use crate::application::ports::{BlockStore, MuteStore, NewJob, PortError, TimelineCursor};
use crate::domain::blocking::Blocking;
use crate::domain::muting::{Muting, RenoteMuting, validate_mute_expiry, validate_mute_params};
use redb::{
    ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction,
};

/// Blocks, keyed `(blocker_id, blockee_id)` with a reverse index keyed
/// `(blockee_id, blocker_id)` (§0A). Values are JSON [`Blocking`] rows.
pub(crate) const BLOCKINGS_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("blockings_forward");
pub(crate) const BLOCKINGS_REVERSE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("blockings_reverse");
/// Mutes, keyed `(muter_id, mutee_id)` (§0A). Values are JSON [`Muting`]
/// rows (which carry the optional expiry).
pub(crate) const MUTINGS_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("mutings_forward");
/// Renote-mutes, keyed `(muter_id, mutee_id)` (§0A). Values are JSON
/// [`RenoteMuting`] rows.
pub(crate) const RENOTE_MUTINGS_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("renote_mutings_forward");
impl RedbStore {
    /// Read-only block-edge probe inside an open write transaction (§0A).
    pub(crate) fn blocking_exists_in_txn(
        &self,
        write_txn: &WriteTransaction,
        blocker_id: &str,
        blockee_id: &str,
    ) -> Result<bool, PortError> {
        let table = write_txn
            .open_table(BLOCKINGS_FORWARD)
            .map_err(map_redb_err)?;
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
    pub(crate) fn reject_when_blocked_in_txn(
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

    /// Atomic block insert + bidirectional follow/request teardown inside an
    /// ambient write transaction (§0A; Misskey
    /// `UserBlockingService.block()` parity). Shared by `BlockStore::block`
    /// and the inbound-`Block` dedup record path so both commit state+jobs
    /// atomically with no second logic path.
    pub(crate) fn block_in_txn(
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
            let mut fwd_table = write_txn
                .open_table(BLOCKINGS_FORWARD)
                .map_err(map_redb_err)?;
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
            let mut rev_table = write_txn
                .open_table(BLOCKINGS_REVERSE)
                .map_err(map_redb_err)?;
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
    pub(crate) fn unblock_in_txn(
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
            let mut fwd_table = write_txn
                .open_table(BLOCKINGS_FORWARD)
                .map_err(map_redb_err)?;
            let mut rev_table = write_txn
                .open_table(BLOCKINGS_REVERSE)
                .map_err(map_redb_err)?;
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
        let table = read_txn
            .open_table(BLOCKINGS_FORWARD)
            .map_err(map_redb_err)?;
        match table.get((blocker_id, blockee_id)).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn is_blocking(&self, blocker_id: &str, blockee_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(BLOCKINGS_FORWARD)
            .map_err(map_redb_err)?;
        Ok(table
            .get((blocker_id, blockee_id))
            .map_err(map_redb_err)?
            .is_some())
    }

    fn blocking_ids_for(&self, blocker_id: &str) -> Result<Vec<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(BLOCKINGS_FORWARD)
            .map_err(map_redb_err)?;
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
        let table = read_txn
            .open_table(BLOCKINGS_REVERSE)
            .map_err(map_redb_err)?;
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
        let table = read_txn
            .open_table(BLOCKINGS_FORWARD)
            .map_err(map_redb_err)?;
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
            let mut table = write_txn
                .open_table(MUTINGS_FORWARD)
                .map_err(map_redb_err)?;
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
            let mut table = write_txn
                .open_table(MUTINGS_FORWARD)
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

    fn get_muting(&self, muter_id: &str, mutee_id: &str) -> Result<Option<Muting>, PortError> {
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
