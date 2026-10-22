use super::JobWorker;
use super::dispatch::Disposition;
use crate::application::ports::{CacheEvictPayload, JobKind, NewJob, PortError};

impl JobWorker {
    /// Run one `CacheEvict` job: free cache bytes under pressure (bytes
    /// only, LRU+TTL-first, metadata stays for re-fetch). Parse failures
    /// and unusable budgets quarantine; only storage faults retry (via the
    /// dispatcher's standard wrapper).
    pub(crate) fn handle_cache_evict(&self, payload: &[u8]) -> Result<(), PortError> {
        let parsed = serde_json::from_slice::<CacheEvictPayload>(payload)
            .map_err(|e| PortError::Validation(format!("cache-evict invalid: {e}")))?;
        crate::media::remote_cache::ensure_cache_budget(
            &self.services,
            parsed.need_bytes,
            parsed.class,
        )
    }

    // Storage overhaul: one bounded reconciler run (orphan sweep,
    // missing-bytes heal, accounting-drift detect/repair, legacy
    // flag, sidecar heal). The run pages newest-first and returns a
    // continuation cursor when the table exceeds the cap; the cursor
    // is enqueued durably so the chain walks monotonically to the
    // oldest row (never "complete" after one page). Bad payloads
    // quarantine; storage faults retry.
    pub(crate) fn handle_reconcile_storage(&self, payload: &[u8]) -> Disposition {
        match serde_json::from_slice::<crate::media::reconciler::ReconcileStorageJobPayload>(
            payload,
        ) {
            Err(e) => {
                Disposition::Quarantine(format!("bad-payload: reconcile-storage invalid: {e}"))
            }
            Ok(payload) => {
                match crate::media::reconciler::reconcile_media_paged(
                    &self.services,
                    payload.after.as_deref(),
                    payload
                        .scan_cap
                        .unwrap_or(crate::media::reconciler::RECONCILE_SCAN_CAP),
                ) {
                    Ok((_, Some(next_after))) => {
                        let cont = crate::media::reconciler::ReconcileStorageJobPayload {
                            scan_cap: payload.scan_cap,
                            after: Some(next_after.clone()),
                        };
                        match NewJob::new_json(
                            JobKind::ReconcileStorage,
                            &cont,
                            format!("storage-reconcile:{next_after}"),
                            self.clock.now_ms(),
                        ) {
                            Ok(job) => Disposition::Fanout(vec![job]),
                            Err(e) => Disposition::Quarantine(format!(
                                "bad-payload: reconcile continuation invalid: {e}"
                            )),
                        }
                    }
                    Ok((_, None)) => Disposition::Done,
                    Err(PortError::Validation(reason)) => {
                        Disposition::Quarantine(format!("bad-payload: {reason}"))
                    }
                    Err(_) => Disposition::Retry {
                        retry_after_ms: None,
                    },
                }
            }
        }
    }

    // Storage overhaul: recompute durable owner bytes + blob
    // refcounts in bounded page runs (post-upgrade backfill). Each
    // run scans at most BACKFILL_PAGES_PER_RUN pages and either
    // persists exact sums (walk ended — the only path that writes
    // counters) or enqueues a durable continuation carrying the
    // staged prefix sums plus the resume cursor. Counters are never
    // overwritten from a partial scan. Bad payloads quarantine;
    // storage faults retry.
    pub(crate) fn handle_backfill_durable_accounting(&self, payload: &[u8]) -> Disposition {
        match serde_json::from_slice::<crate::media::reconciler::BackfillDurableJobPayload>(payload)
        {
            Err(e) => {
                Disposition::Quarantine(format!("bad-payload: backfill-durable invalid: {e}"))
            }
            Ok(payload) => {
                match crate::media::reconciler::backfill_durable_step(
                    &self.services,
                    payload.staged,
                    payload.after,
                    payload
                        .batch_size
                        .unwrap_or(crate::media::reconciler::BACKFILL_PAGE_SIZE),
                    crate::media::reconciler::BACKFILL_PAGES_PER_RUN,
                ) {
                    Ok(crate::media::reconciler::BackfillStep::Continue { staged, next_after }) => {
                        let cont = crate::media::reconciler::BackfillDurableJobPayload {
                            after: Some(next_after.clone()),
                            batch_size: payload.batch_size,
                            staged,
                        };
                        match NewJob::new_json(
                            JobKind::BackfillDurableAccounting,
                            &cont,
                            format!("storage-backfill:{next_after}"),
                            self.clock.now_ms(),
                        ) {
                            Ok(job) => Disposition::Fanout(vec![job]),
                            Err(e) => Disposition::Quarantine(format!(
                                "bad-payload: backfill continuation invalid: {e}"
                            )),
                        }
                    }
                    Ok(crate::media::reconciler::BackfillStep::Complete { staged, .. }) => {
                        match crate::media::reconciler::apply_backfill_sums(&self.services, &staged)
                        {
                            Ok(()) => Disposition::Done,
                            Err(PortError::Validation(reason)) => {
                                Disposition::Quarantine(format!("bad-payload: {reason}"))
                            }
                            Err(_) => Disposition::Retry {
                                retry_after_ms: None,
                            },
                        }
                    }
                    Err(PortError::Validation(reason)) => {
                        Disposition::Quarantine(format!("bad-payload: {reason}"))
                    }
                    Err(_) => Disposition::Retry {
                        retry_after_ms: None,
                    },
                }
            }
        }
    }
}
