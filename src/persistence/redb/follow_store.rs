use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::{ACCOUNTS, FOLLOWS_FORWARD, FOLLOWS_REVERSE, RedbStore};
use crate::application::ports::{FollowStore, NewJob, PortError};
use crate::domain::account::Account;
use crate::domain::follow::{Follow, FollowNotify, FollowRequest};
use redb::{
    ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction,
};

pub(crate) const FOLLOW_REQ_FORWARD: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follow_requests_forward");
pub(crate) const FOLLOW_REQ_REVERSE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("follow_requests_reverse");
impl RedbStore {
    /// Remove a pending request (both index rows). Returns whether one existed.
    pub(crate) fn delete_follow_request_rows(
        &self,
        write_txn: &WriteTransaction,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<bool, PortError> {
        let mut fwd = write_txn
            .open_table(FOLLOW_REQ_FORWARD)
            .map_err(map_redb_err)?;
        let mut rev = write_txn
            .open_table(FOLLOW_REQ_REVERSE)
            .map_err(map_redb_err)?;
        let existed = fwd
            .remove((follower_id, followee_id))
            .map_err(map_redb_err)?
            .is_some();
        rev.remove((followee_id, follower_id))
            .map_err(map_redb_err)?;
        Ok(existed)
    }

    /// Drop one follow edge (both index rows) and adjust counters, silently
    /// when no edge exists. Shared by `unfollow_in_txn` (which reports
    /// `NOT_FOLLOWING`) and the atomic `block` (which must not fail when
    /// there is nothing to tear down).
    pub(crate) fn drop_follow_edge_in_txn(
        &self,
        write_txn: &WriteTransaction,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<bool, PortError> {
        let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let mut fwd_table = write_txn
            .open_table(FOLLOWS_FORWARD)
            .map_err(map_redb_err)?;
        let mut rev_table = write_txn
            .open_table(FOLLOWS_REVERSE)
            .map_err(map_redb_err)?;
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

    pub(crate) fn request_follow_in_txn(
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
            let edges = write_txn
                .open_table(FOLLOWS_FORWARD)
                .map_err(map_redb_err)?;
            let mut fwd = write_txn
                .open_table(FOLLOW_REQ_FORWARD)
                .map_err(map_redb_err)?;
            let mut rev = write_txn
                .open_table(FOLLOW_REQ_REVERSE)
                .map_err(map_redb_err)?;
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

    pub(crate) fn follow_in_txn(
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
            let mut fwd_table = write_txn
                .open_table(FOLLOWS_FORWARD)
                .map_err(map_redb_err)?;
            let mut rev_table = write_txn
                .open_table(FOLLOWS_REVERSE)
                .map_err(map_redb_err)?;

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

    pub(crate) fn unfollow_in_txn(
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
            let mut fwd_table = write_txn
                .open_table(FOLLOWS_FORWARD)
                .map_err(map_redb_err)?;
            let mut rev_table = write_txn
                .open_table(FOLLOWS_REVERSE)
                .map_err(map_redb_err)?;

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
        let t = read_txn
            .open_table(FOLLOW_REQ_FORWARD)
            .map_err(map_redb_err)?;
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
        let t = read_txn
            .open_table(FOLLOW_REQ_REVERSE)
            .map_err(map_redb_err)?;
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
            let t = write_txn
                .open_table(FOLLOW_REQ_FORWARD)
                .map_err(map_redb_err)?;
            let got = t.get((follower_id, followee_id)).map_err(map_redb_err)?;
            match got {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_FOLLOW_REQUEST".to_string())),
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
