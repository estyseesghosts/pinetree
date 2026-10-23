use super::codec::{decode_json, encode_json, map_redb_err};
use super::follow_store::{FOLLOW_REQ_FORWARD, FOLLOW_REQ_REVERSE};
use super::note_store::matches_cursor;
use super::store::{
    ACCOUNTS, ACTIVITY_DEDUP, COUNTER_ACCOUNTS_LOCAL, FOLLOWS_FORWARD, FOLLOWS_REVERSE, RedbStore,
};
use crate::application::ports::{
    AbuseReportFilter, AbuseReportRecipientStore, AbuseReportStore, ModerationLogFilter,
    ModerationLogStore, NewJob, PortError, TimelineCursor, UpdateAccountPatch, UserMemoStore,
};
use crate::domain::account::Account;
use crate::domain::moderation::{AbuseReport, AbuseReportResolution, ModerationLogEntry};
use crate::domain::webhook::AbuseReportRecipient;
use redb::{
    ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction,
};

/// User memos `(owner_id, target_id)` → memo text bytes (gaps WP2a;
/// `users/update-memo`). `None`/empty clears the row (no empty rows).
pub(crate) const USER_MEMOS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("user_memos");
/// Moderation-log rows (Batch M §3.2; Misskey `ModerationLogs`), keyed by
/// row id (aidx, time-ordered → reverse range reads newest-first).
pub(crate) const MODERATION_LOGS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("moderation_logs");
pub(crate) const ABUSE_REPORTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("abuse_reports");
/// Abuse-report notification recipients by id (WP3;
/// `abuse-report/notification-recipient/*` parity). Values are JSON
/// [`AbuseReportRecipient`] rows.
pub(crate) const ABUSE_REPORT_RECIPIENTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("abuse_report_recipients");
impl RedbStore {
    /// Append one moderation-log row inside an open write transaction so a
    /// moderation action's state change and its log row commit together
    /// (§2.3; conformance H4).
    pub(crate) fn append_log_in_txn(
        &self,
        write_txn: &WriteTransaction,
        entry: &ModerationLogEntry,
    ) -> Result<(), PortError> {
        entry.validate()?;
        let mut table = write_txn
            .open_table(MODERATION_LOGS)
            .map_err(map_redb_err)?;
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
                )));
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
        // Wave 4 scans: suspend/unsuspend flips the NodeInfo `users.total`
        // predicate (local, live, unsuspended). The delta commits in the
        // same transaction; no-change transitions bump nothing.
        let before: Option<Account> = {
            let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            match accounts_table.get(account_id).map_err(map_redb_err)? {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            }
        };
        let updated = self.set_account_flags_in_txn(&write_txn, account_id, |account| {
            account.is_suspended = suspended;
        })?;
        if let Some(prev) = before {
            let counted_before = prev.host.is_none() && !prev.is_deleted && !prev.is_suspended;
            let counted_after =
                updated.host.is_none() && !updated.is_deleted && !updated.is_suspended;
            match (counted_before, counted_after) {
                (true, false) => {
                    self.counter_add_in_txn(&write_txn, COUNTER_ACCOUNTS_LOCAL, -1)?;
                }
                (false, true) => {
                    self.counter_add_in_txn(&write_txn, COUNTER_ACCOUNTS_LOCAL, 1)?;
                }
                _ => {}
            }
        }
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

    fn delete_account(
        &self,
        account_id: &str,
        log: Option<ModerationLogEntry>,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        if let Some(l) = &log {
            l.validate()?;
        }
        let write_txn = self.begin_write()?;
        // Wave 4 scans: deletion drops the row from the NodeInfo
        // `users.total` predicate when it was counted (local, live,
        // unsuspended). Read before flagging; the delete itself is
        // idempotent so only a counted row debits.
        let counted_before: bool = {
            let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            match accounts_table.get(account_id).map_err(map_redb_err)? {
                Some(g) => {
                    let prev: Account = decode_json(g.value())?;
                    prev.host.is_none() && !prev.is_deleted && !prev.is_suspended
                }
                None => false,
            }
        };
        self.set_account_flags_in_txn(&write_txn, account_id, |account| {
            account.is_deleted = true;
            account.is_suspended = true;
        })?;
        if counted_before {
            self.counter_add_in_txn(&write_txn, COUNTER_ACCOUNTS_LOCAL, -1)?;
        }
        self.delete_follow_requests_involving_in_txn(&write_txn, account_id)?;
        // Remove follow edges in both directions (account as follower or followee).
        let mut keys: Vec<(String, String)> = Vec::new();
        {
            let fwd = write_txn
                .open_table(FOLLOWS_FORWARD)
                .map_err(map_redb_err)?;
            for entry in fwd
                .range((account_id, "")..=(account_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
            {
                let (k, _) = entry.map_err(map_redb_err)?;
                let (f, fe) = k.value();
                keys.push((f.to_string(), fe.to_string()));
            }
        }
        {
            let rev = write_txn
                .open_table(FOLLOWS_REVERSE)
                .map_err(map_redb_err)?;
            for entry in rev
                .range((account_id, "")..=(account_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
            {
                let (k, _) = entry.map_err(map_redb_err)?;
                let (fe, f) = k.value();
                keys.push((f.to_string(), fe.to_string()));
            }
        }
        for (f, fe) in keys {
            let mut fwd = write_txn
                .open_table(FOLLOWS_FORWARD)
                .map_err(map_redb_err)?;
            let _ = fwd
                .remove((f.as_str(), fe.as_str()))
                .map_err(map_redb_err)?;
            let mut rev = write_txn
                .open_table(FOLLOWS_REVERSE)
                .map_err(map_redb_err)?;
            let _ = rev
                .remove((fe.as_str(), f.as_str()))
                .map_err(map_redb_err)?;
        }
        if let Some(l) = log {
            self.append_log_in_txn(&write_txn, &l)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
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

impl AbuseReportStore for RedbStore {
    fn create(&self, report: AbuseReport) -> Result<AbuseReport, PortError> {
        report.validate()?;
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
            let bytes = encode_json(&report)?;
            table
                .insert(report.id.as_str(), bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(report)
    }

    fn count_for_target(&self, account_id: &str) -> Result<usize, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
        let mut count = 0usize;
        for entry in table.range(""..).map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let report: AbuseReport = decode_json(v.value())?;
            if report.target_account == account_id {
                count += 1;
            }
        }
        Ok(count)
    }

    fn get(&self, id: &str) -> Result<Option<AbuseReport>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn list(
        &self,
        filter: &AbuseReportFilter,
        cursor: &TimelineCursor,
    ) -> Result<Vec<AbuseReport>, PortError> {
        let limit = if cursor.limit == 0 { 10 } else { cursor.limit };
        const SCAN_CAP: usize = 10_000;
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        let mut scanned = 0usize;
        for entry in table.range(""..).map_err(map_redb_err)?.rev() {
            scanned += 1;
            if scanned > SCAN_CAP {
                break;
            }
            let (_, v) = entry.map_err(map_redb_err)?;
            let report: AbuseReport = decode_json(v.value())?;
            if !matches_cursor(&report.id, cursor) {
                continue;
            }
            // state filter
            match filter.state.as_deref() {
                Some("resolved") if !report.resolved => continue,
                Some("unresolved") if report.resolved => continue,
                _ => {}
            }
            // reporter origin: remote if reporter account host != None. We
            // cannot resolve host here without account store; pinetree stores
            // reporter as account id only and the domain report has no host.
            // DIVERGENCE: origin filters are applied at the API layer via
            // account lookup; storage keeps all rows. For now accept all.
            let _ = (&filter.reporter_origin, &filter.target_origin);
            out.push(report);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn resolve(
        &self,
        id: &str,
        resolved_as: Option<AbuseReportResolution>,
        assignee: Option<String>,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<AbuseReport, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        let mut report: AbuseReport = {
            let table = write_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
            match table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_ABUSE_REPORT".to_string())),
            }
        };
        report.resolved = true;
        report.resolved_as = resolved_as;
        report.assignee = assignee;
        report.validate()?;
        {
            let mut table = write_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
            let bytes = encode_json(&report)?;
            table.insert(id, bytes.as_slice()).map_err(map_redb_err)?;
        }
        self.append_log_in_txn(&write_txn, &log)?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(report)
    }

    fn set_note(
        &self,
        id: &str,
        note: String,
        log: Option<ModerationLogEntry>,
        jobs: Vec<NewJob>,
    ) -> Result<AbuseReport, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        let mut report: AbuseReport = {
            let table = write_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
            match table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_ABUSE_REPORT".to_string())),
            }
        };
        report.moderation_note = note;
        report.validate()?;
        {
            let mut table = write_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
            let bytes = encode_json(&report)?;
            table.insert(id, bytes.as_slice()).map_err(map_redb_err)?;
        }
        if let Some(entry) = log {
            self.append_log_in_txn(&write_txn, &entry)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(report)
    }

    fn mark_forwarded(
        &self,
        id: &str,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<AbuseReport, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        let mut report: AbuseReport = {
            let table = write_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
            match table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_ABUSE_REPORT".to_string())),
            }
        };
        if report.forwarded {
            return Err(PortError::Conflict("already forwarded".to_string()));
        }
        report.forwarded = true;
        report.validate()?;
        {
            let mut table = write_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
            let bytes = encode_json(&report)?;
            table.insert(id, bytes.as_slice()).map_err(map_redb_err)?;
        }
        self.append_log_in_txn(&write_txn, &log)?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(report)
    }

    fn create_flag_report(
        &self,
        activity_id: &str,
        report: AbuseReport,
    ) -> Result<Option<AbuseReport>, PortError> {
        report.validate()?;
        let write_txn = self.begin_write()?;
        let inserted = {
            let mut dedup = write_txn.open_table(ACTIVITY_DEDUP).map_err(map_redb_err)?;
            if dedup.get(activity_id).map_err(map_redb_err)?.is_some() {
                None
            } else {
                dedup.insert(activity_id, 1u8).map_err(map_redb_err)?;
                drop(dedup);
                let mut table = write_txn.open_table(ABUSE_REPORTS).map_err(map_redb_err)?;
                let bytes = encode_json(&report)?;
                table
                    .insert(report.id.as_str(), bytes.as_slice())
                    .map_err(map_redb_err)?;
                Some(report)
            }
        };
        self.commit_write(write_txn)?;
        Ok(inserted)
    }
}

impl AbuseReportRecipientStore for RedbStore {
    fn insert_recipient(
        &self,
        row: AbuseReportRecipient,
    ) -> Result<AbuseReportRecipient, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut recipients = write_txn
                .open_table(ABUSE_REPORT_RECIPIENTS)
                .map_err(map_redb_err)?;
            if recipients
                .get(row.id.as_str())
                .map_err(map_redb_err)?
                .is_some()
            {
                drop(recipients);
                let _ = write_txn.abort();
                return Err(PortError::Conflict(format!(
                    "abuse-report recipient {} already exists",
                    row.id
                )));
            }
            let encoded = encode_json(&row)?;
            recipients
                .insert(row.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(row)
    }

    fn get_recipient(&self, id: &str) -> Result<Option<AbuseReportRecipient>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let recipients = read_txn
            .open_table(ABUSE_REPORT_RECIPIENTS)
            .map_err(map_redb_err)?;
        match recipients.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn list_recipients(&self) -> Result<Vec<AbuseReportRecipient>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let recipients = read_txn
            .open_table(ABUSE_REPORT_RECIPIENTS)
            .map_err(map_redb_err)?;
        let mut out: Vec<AbuseReportRecipient> = Vec::new();
        for entry in recipients.iter().map_err(map_redb_err)? {
            let (_, value) = entry.map_err(map_redb_err)?;
            out.push(decode_json(value.value())?);
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    fn put_recipient(&self, row: AbuseReportRecipient) -> Result<AbuseReportRecipient, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut recipients = write_txn
                .open_table(ABUSE_REPORT_RECIPIENTS)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&row)?;
            recipients
                .insert(row.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(row)
    }

    fn delete_recipient(&self, id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut recipients = write_txn
                .open_table(ABUSE_REPORT_RECIPIENTS)
                .map_err(map_redb_err)?;
            recipients.remove(id).map_err(map_redb_err)?.is_some()
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn active_recipient_emails(&self, limit: usize) -> Result<Vec<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let recipients = read_txn
            .open_table(ABUSE_REPORT_RECIPIENTS)
            .map_err(map_redb_err)?;
        let mut active: Vec<(String, String)> = Vec::new();
        for entry in recipients.iter().map_err(map_redb_err)? {
            let (_, value) = entry.map_err(map_redb_err)?;
            let row: AbuseReportRecipient = decode_json(value.value())?;
            if row.is_active {
                active.push((row.id.clone(), row.email.clone()));
            }
        }
        active.sort();
        active.truncate(limit);
        Ok(active.into_iter().map(|(_, email)| email).collect())
    }
}

impl UserMemoStore for RedbStore {
    fn set_memo(
        &self,
        owner_id: &str,
        target_id: &str,
        memo: Option<String>,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(USER_MEMOS).map_err(map_redb_err)?;
            match memo {
                Some(text) if !text.is_empty() => {
                    table
                        .insert((owner_id, target_id), text.as_bytes())
                        .map_err(map_redb_err)?;
                }
                _ => {
                    table.remove((owner_id, target_id)).map_err(map_redb_err)?;
                }
            }
        }
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn get_memo(&self, owner_id: &str, target_id: &str) -> Result<Option<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(USER_MEMOS).map_err(map_redb_err)?;
        match table.get((owner_id, target_id)).map_err(map_redb_err)? {
            Some(value) => Ok(Some(String::from_utf8(value.value().to_vec()).map_err(
                |err| PortError::StorageUnavailable(format!("memo not utf-8: {err}")),
            )?)),
            None => Ok(None),
        }
    }
}
