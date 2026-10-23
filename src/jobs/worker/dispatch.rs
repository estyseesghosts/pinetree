use super::JobWorker;
use crate::application::ports::{
    Job, JobKind, MAX_JOB_ATTEMPTS, MAX_JOBS_PER_TXN, NewJob, PortError,
};

/// How a claimed job's record is settled after its handler runs.
pub(crate) enum Disposition {
    /// Handler succeeded: remove the row.
    Done,
    /// Handler failed retryably: reschedule with backoff, quarantining when
    /// the retry budget (`MAX_JOB_ATTEMPTS`) is exhausted. When
    /// `retry_after_ms` is set (a server's `Retry-After`), the job is not
    /// rescheduled earlier than that.
    Retry { retry_after_ms: Option<i64> },
    /// Fan-out finished: complete the job and enqueue these per-inbox
    /// follow-ups. The batches commit in separate transactions ahead of the
    /// completion; every batch enqueue is idempotent on `idempotency_key`,
    /// so a crash after batch K of N followed by a lease-expiry reclaim
    /// replays the same keys and converges on one row per key instead of
    /// duplicating per-inbox POSTs. The last batch commits atomically with
    /// the completion.
    Fanout(Vec<NewJob>),
    /// Destination in cooldown: reschedule at this time WITHOUT counting an
    /// attempt.
    Defer(i64),
    /// Handler failed terminally: quarantine with the recorded reason.
    Quarantine(String),
}

/// Split follow-up jobs into full batches (each at most `MAX_JOBS_PER_TXN`)
/// that are enqueued on their own, plus the final batch that is committed
/// atomically with the completion of the activity job. The final batch is
/// never empty when the input is not.
pub fn split_followup_batches(mut followups: Vec<NewJob>) -> (Vec<Vec<NewJob>>, Vec<NewJob>) {
    let mut batches = Vec::new();
    while followups.len() > MAX_JOBS_PER_TXN {
        let rest = followups.split_off(MAX_JOBS_PER_TXN);
        batches.push(std::mem::replace(&mut followups, rest));
    }
    (batches, followups)
}

fn panic_payload_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

impl JobWorker {
    pub fn process_one(&self) -> Result<bool, PortError> {
        let claimed = self.jobs.claim_next(self.lease_duration_ms)?;
        let job = match claimed {
            None => return Ok(false),
            Some(job) => job,
        };
        // Panic containment: a panicking handler must quarantine the job
        // (burning no extra attempt beyond the lease-reclaim count) instead
        // of unwinding the worker and looping forever on the same job.
        let dispatch: Result<Disposition, PortError> = match std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| self.dispatch_job(&job)),
        ) {
            Ok(inner) => inner,
            Err(payload) => Ok(Disposition::Quarantine(format!(
                "panic: {}",
                panic_payload_message(&payload)
            ))),
        };
        // Storage errors from the settlement calls below propagate to the
        // caller, as before (drain/spawn behavior unchanged).
        match dispatch? {
            Disposition::Done => {
                self.jobs.complete(&job.id)?;
            }
            Disposition::Retry { retry_after_ms } => {
                if job.attempts.saturating_add(1) >= MAX_JOB_ATTEMPTS {
                    // Retry budget exhausted for any kind: quarantine with a
                    // recorded reason instead of retrying forever.
                    self.quarantine(&job, "retry-exhausted")?;
                } else {
                    let shift = job.attempts.min(10);
                    let backoff = self.base_backoff_ms.saturating_mul(1_i64 << shift);
                    let mut retry_at = self.clock.now_ms().saturating_add(backoff);
                    if let Some(earliest) = retry_after_ms {
                        retry_at = retry_at.max(earliest);
                    }
                    self.jobs.fail(&job.id, retry_at)?;
                }
            }
            Disposition::Fanout(followups) => {
                // Each batch enqueue is idempotent on `idempotency_key`: a
                // crash between batches leaves the parent claimable, and the
                // reclaim replays the same batches without inserting a second
                // row per key. The final batch commits with the completion.
                let (batches, last) = split_followup_batches(followups);
                for batch in batches {
                    self.jobs.enqueue_jobs(batch)?;
                }
                self.jobs.complete_with_followups(&job.id, last)?;
            }
            Disposition::Defer(until) => {
                self.jobs.defer(&job.id, until)?;
            }
            Disposition::Quarantine(reason) => {
                self.quarantine(&job, &reason)?;
            }
        }
        Ok(true)
    }

    fn dispatch_job(&self, job: &Job) -> Result<Disposition, PortError> {
        match job.kind {
            JobKind::Notify => match self.handle_notify(&job.payload, &job.idempotency_key) {
                Ok(()) => Ok(Disposition::Done),
                // Unparseable payloads can never succeed on retry.
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            JobKind::StreamingPublish => match self.handle_streaming_publish(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                // Unparseable payloads can never succeed on retry.
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            JobKind::FederateDeliver => self.dispatch_federate(job),
            // No materialized-timeline fan-out exists (fan-out-on-read);
            // the kind is reserved so enqueued rows complete instead of
            // wedging the worker.
            JobKind::TimelineMaterialize => Ok(Disposition::Done),
            // Slice 7 (H2+C3, decision D-d): remote media fetched
            // off-request. Bad payloads quarantine; per-attachment fetch
            // failures are best-effort skips inside the processor, so only
            // storage faults retry the whole job (already-attached URLs are
            // re-skipped without network I/O).
            JobKind::RemoteMediaFetch => match self.handle_remote_media_fetch(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Quote-post authorization: re-verify an offered stamp whose
            // first fetch failed transiently. Bad payloads quarantine;
            // transient fetch failures retry with backoff (quarantined past
            // the budget); invalid stamp content completes quietly — a later
            // re-delivered Accept (new activity id) can still be processed.
            // Quote-post authorization: re-verify an offered stamp whose
            // first fetch failed transiently. Bad payloads quarantine;
            // transient fetch failures retry with backoff (quarantined past
            // the budget); invalid stamp content completes quietly — a later
            // re-delivered Accept (new activity id) can still be processed.
            JobKind::QuoteStampReverify => match self.handle_quote_stamp_reverify(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Branched replies (`docs/replies.md` Slice B): resolve one
            // pending reply-parent URI off-request. Bad payloads quarantine;
            // an unavailable parent retries with backoff (quarantined past
            // the budget with the pending entry kept); a full batch returns
            // a continuation job for the same URI via `Fanout`.
            JobKind::ReplyParentResolve => {
                match crate::application::commands::inbound::replies::handle_reply_parent_resolve(
                    &self.services,
                    &job.payload,
                ) {
                    Ok(None) => Ok(Disposition::Done),
                    Ok(Some(continuation)) => Ok(Disposition::Fanout(vec![continuation])),
                    Err(PortError::Validation(reason)) => {
                        Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                    }
                    Err(_) => Ok(Disposition::Retry {
                        retry_after_ms: None,
                    }),
                }
            }
            // Antenna fan-in (Batch N WS4): match one committed note into
            // every active antenna. Bad payloads quarantine; a gone note
            // converges quietly; only storage faults retry.
            JobKind::AntennaMatch => match self.handle_antenna_match(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Poll expiry (§4): publish the `pollEnded` streaming event
            // once the poll has expired, then enqueue the `pollEnded`
            // `Notify` job for the note author. Bad payloads quarantine;
            // a gone note/poll or a not-yet-expired poll completes quietly
            // (a re-armed job covers clock skew). Only storage faults
            // retry.
            JobKind::PollExpiry => match self.run_poll_expiry(&job.payload) {
                Ok(_) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Scheduled drafts (§5): post the draft through the full
            // note-create path once its time arrives. Bad payloads
            // quarantine; a gone/unscheduled/rescheduled draft converges
            // quietly (Done — the re-armed job covers it when one exists).
            // Only storage faults retry. A failed post is itself a
            // successful run (`ScheduledNotePostFailed` is emitted inside).
            JobKind::PostScheduledDraft => match self.handle_post_scheduled_draft(&job.payload) {
                Ok(_) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Remote custom-emoji lazy fetch (Batch N WP8b): download one
            // known remote icon off-request. Bad payloads quarantine; a
            // gone/already-cached row or an unusable fetch converges quietly
            // (Done — the packed URL keeps pointing at the remote host).
            // Only storage faults retry.
            JobKind::RemoteEmojiFetch => match self.handle_remote_emoji_fetch(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Profile export (§13): build the file, store it in the caller's
            // drive, and fan out the `exportCompleted` Notify follow-up —
            // committed atomically with the export job's completion, so the
            // notification can never be lost or duplicated. Bad payloads
            // quarantine; a gone user converges quietly (no follow-up);
            // only storage faults retry.
            JobKind::Export => match self.handle_export(&job.payload) {
                Ok(None) => Ok(Disposition::Done),
                Ok(Some(notify)) => Ok(Disposition::Fanout(vec![notify])),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Profile import (§13): apply the caller's drive-file rows
            // through the interactive application ops. Bad payloads
            // quarantine; gone users/files converge quietly; per-row
            // failures skip the row inside the handler. Only storage
            // faults retry.
            JobKind::Import => match self.handle_import(&job.payload) {
                Ok(_) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Silent unfollow sweep (Batch M WP3): remove one bounded batch
            // of the account's outgoing follows (row + its `Undo(Follow)`
            // job commit together); a full batch returns a continuation.
            // Bad payloads quarantine; per-row conflicts converge as skips;
            // only storage faults retry the job.
            JobKind::UnfollowAll => match self.services.handle_unfollow_all(&job.payload) {
                Ok(None) => Ok(Disposition::Done),
                Ok(Some(continuation)) => Ok(Disposition::Fanout(vec![continuation])),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Custom-emoji export/import (emoji completion workstream; real
            // handlers in `application::commands::emoji_transfer`, owned by
            // the transfer subagent — these arms only keep the dispatcher
            // exhaustive until they land).
            JobKind::ExportCustomEmojis | JobKind::ImportCustomEmojis => {
                match crate::application::commands::emoji_transfer::handle_custom_emoji_transfer(
                    &self.services,
                    &job.kind,
                    &job.payload,
                ) {
                    Ok(None) => Ok(Disposition::Done),
                    Ok(Some(notify)) => Ok(Disposition::Fanout(vec![notify])),
                    Err(PortError::Validation(reason)) => {
                        Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                    }
                    Err(_) => Ok(Disposition::Retry {
                        retry_after_ms: None,
                    }),
                }
            }
            // Storage overhaul: free cache bytes under pressure (bytes only,
            // LRU+TTL-first, metadata stays for re-fetch). Bad payloads
            // quarantine; only storage faults retry.
            JobKind::CacheEvict => match self.handle_cache_evict(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Storage overhaul: one bounded reconciler run (orphan sweep,
            // missing-bytes heal, accounting-drift detect/repair, legacy
            // flag, sidecar heal). The run pages newest-first and returns a
            // continuation cursor when the table exceeds the cap; the cursor
            // is enqueued durably so the chain walks monotonically to the
            // oldest row (never "complete" after one page). Bad payloads
            // quarantine; storage faults retry.
            JobKind::ReconcileStorage => Ok(self.handle_reconcile_storage(&job.payload)),
            // Storage overhaul: recompute durable owner bytes + blob
            // refcounts in bounded page runs (post-upgrade backfill). Each
            // run scans at most BACKFILL_PAGES_PER_RUN pages and either
            // persists exact sums (walk ended — the only path that writes
            // counters) or enqueues a durable continuation carrying the
            // staged prefix sums plus the resume cursor. Counters are never
            // overwritten from a partial scan. Bad payloads quarantine;
            // storage faults retry.
            JobKind::BackfillDurableAccounting => {
                Ok(self.handle_backfill_durable_accounting(&job.payload))
            }
            // Post-commit ffmpeg transcode (gaps WP1): finish processing
            // staged bytes outside any transaction. Bad payloads
            // quarantine; deterministic input failures (bad decode, ffmpeg
            // error, wall/output/RSS breach) quarantine with the ffmpeg
            // reason; only storage faults retry.
            JobKind::TranscodeMedia => match self.dispatch_transcode_media(&job.payload) {
                Ok(disposition) => Ok(disposition),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Durable webhook delivery (gaps WP3): signed POST reusing the
            // federation outbox's cooldown/backoff discipline
            // (`classify_inbox_status` + `DestinationTracker`). Bad
            // payloads quarantine; transport outcomes map like
            // `FederateDeliver` (Done/Retry/Defer/Fanout).
            JobKind::WebhookDeliver => match self.dispatch_webhook_deliver(&job.payload) {
                Ok(disposition) => Ok(disposition),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
            // Durable email send (gaps WP3): render-and-send one message
            // through the configured transport. Bad payloads and terminal
            // send failures (unconfigured transport, invalid message,
            // auth) quarantine; connection/rejection failures retry with
            // backoff.
            JobKind::EmailSend => match self.handle_email_send(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry {
                    retry_after_ms: None,
                }),
            },
        }
    }

    fn quarantine(&self, job: &Job, reason: &str) -> Result<(), PortError> {
        eprintln!(
            "quarantining job {} ({:?}) after {} attempts: {reason}",
            job.id, job.kind, job.attempts
        );
        self.jobs.fail_permanent(&job.id, reason)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn follow_up(n: usize) -> NewJob {
        NewJob {
            kind: JobKind::FederateDeliver,
            payload: Vec::new(),
            idempotency_key: format!("k:{n}"),
            not_before: 0,
        }
    }

    #[test]
    fn slice10_followup_batches_respect_the_per_txn_cap() {
        let (batches, last) = split_followup_batches((0..1_200).map(follow_up).collect());
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![500, 500]
        );
        assert_eq!(last.len(), 200);
        assert_eq!(last[0].idempotency_key, "k:1000", "order is preserved");

        let (batches, last) = split_followup_batches((0..500).map(follow_up).collect());
        assert!(
            batches.is_empty(),
            "exactly one full batch commits with completion"
        );
        assert_eq!(last.len(), 500);

        let (batches, last) = split_followup_batches((0..501).map(follow_up).collect());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 500);
        assert_eq!(last.len(), 1);

        let (batches, last) = split_followup_batches(Vec::new());
        assert!(batches.is_empty() && last.is_empty());
    }

    #[test]
    fn slice10_every_batch_is_within_the_cap() {
        let (batches, last) = split_followup_batches((0..2_500).map(follow_up).collect());
        assert!(batches.iter().all(|b| b.len() <= MAX_JOBS_PER_TXN));
        assert!(last.len() <= MAX_JOBS_PER_TXN);
        let total: usize = batches.iter().map(Vec::len).sum::<usize>() + last.len();
        assert_eq!(total, 2_500, "no follow-up dropped or duplicated");
    }
}
