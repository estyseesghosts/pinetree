use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::ports::{
    Job, JobQueue, MAX_JOB_ATTEMPTS, MAX_JOBS_PER_TXN, NewJob, PortError,
};
use redb::{
    ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction,
};

pub(crate) const JOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("jobs");
pub(crate) const JOBS_DUE_INDEX: TableDefinition<(i64, &str), u8> =
    TableDefinition::new("jobs_due_index");
impl RedbStore {
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
