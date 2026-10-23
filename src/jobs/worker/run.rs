use super::JobWorker;
use crate::application::ports::PortError;
use crate::federation::outbox::FANOUT_JOB_DEADLINE_MS;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

// ===== Batch F backup drain (append-only region) ==========================
// Batch C owns the `ReconcileStorage` / `BackfillDurableAccounting` arms
// above; this region only drains backup jobs. Backup requests are durable
// status files in the backup dir (see `crate::backup`); the drain claims
// the oldest queued request and runs it synchronously with bounded retries.
// `JobKind` lives in `application::ports` (owned by the parent), so backup
// intentionally reuses no `JobKind` variant: the parent wires this drain
// into the worker tick / maintenance loop (see the returned snippet).

impl JobWorker {
    /// Drain up to `max_steps` queued backup jobs. Returns completed jobs.
    /// A transient failure keeps the request queued (bounded attempts in
    /// the status record); anything terminal fails it with a redacted
    /// reason. Stops early when the queue is empty.
    pub fn drain_pending_backups(
        &self,
        ctx: &crate::backup::BackupContext,
        max_steps: usize,
    ) -> Result<usize, PortError> {
        let _ = self.clock.now_ms();
        let mut done = 0;
        for _ in 0..max_steps {
            match crate::backup::run_next_backup(ctx) {
                Ok(None) => break,
                Ok(Some(_)) => done += 1,
                Err(e) => return Err(e.to_port_error()),
            }
        }
        Ok(done)
    }
}

impl JobWorker {
    /// Wall-clock budget one claimed job may spend inside `process_one`
    /// before the worker flags it as an overrun.
    ///
    /// The actionable deadline is enforced where the time is spent:
    /// `fan_out` bounds inline delivery to `FANOUT_INLINE_MAX` inboxes in
    /// waves of `FANOUT_INLINE_CONCURRENCY` concurrent POSTs under
    /// `FANOUT_JOB_DEADLINE_MS`, splitting anything larger into one-inbox
    /// follow-ups. This guard makes any residual overrun (a hung socket, a
    /// slow store) observable and guarantees the loop still settles the
    /// job and moves on to the next one — which may be a `Notify`,
    /// `RemoteMediaFetch`, or `PollExpiry` — instead of wedging behind a
    /// single long job.
    fn note_job_wall_time(&self, elapsed: std::time::Duration) {
        if elapsed.as_millis() > FANOUT_JOB_DEADLINE_MS.max(0) as u128 {
            eprintln!(
                "worker job exceeded wall budget: took {}ms (budget {FANOUT_JOB_DEADLINE_MS}ms)",
                elapsed.as_millis()
            );
        }
    }

    pub fn drain(&self, max_steps: usize) -> Result<usize, PortError> {
        let mut done = 0;
        for _ in 0..max_steps {
            // Resilient: a panic escaping `process_one` (e.g. a storage
            // panic outside the contained handler dispatch) must not unwind
            // the drain caller. The lease is left to expire — and expiry now
            // counts as an attempt — so the job is retried/quarantined
            // instead of wedging the loop.
            let started = Instant::now();
            let step =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.process_one()));
            self.note_job_wall_time(started.elapsed());
            match step {
                Ok(Ok(true)) => done += 1,
                Ok(Ok(false)) => break,
                Ok(Err(e)) => return Err(e),
                Err(_) => continue,
            }
        }
        Ok(done)
    }

    pub fn spawn(
        self: Arc<Self>,
        poll_interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        self.spawn_until(poll_interval, Arc::new(AtomicBool::new(false)))
    }

    /// Spawn the worker loop until `stop` is set.
    ///
    /// `spawn` is the never-stopping variant (tests, embedding). A server
    /// that owns a shutdown signal uses `spawn_until` so the loop parks
    /// between polls instead of racing the process exit: a job claimed but
    /// not settled keeps its lease, the lease expires, and the next process
    /// to start reclaims it. No work is lost either way, but the shutdown
    /// path stops taking new jobs while the HTTP server drains.
    pub fn spawn_until(
        self: Arc<Self>,
        poll_interval: std::time::Duration,
        stop: Arc<AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let this = Arc::clone(&self);
                // `process_one` already quarantines handler panics via
                // `catch_unwind`; this outer catch only guards panics that
                // escape settlement/storage so the background loop never dies
                // silently. Either way the lease is settled (quarantine) or
                // left to expire (which counts as an attempt).
                let worked = tokio::task::spawn_blocking(move || {
                    let started = Instant::now();
                    let out = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        this.process_one()
                    })) {
                        Ok(Ok(v)) => v,
                        Ok(Err(_)) => false,
                        Err(_) => false,
                    };
                    this.note_job_wall_time(started.elapsed());
                    out
                })
                .await
                .unwrap_or(false);
                if !worked {
                    tokio::time::sleep(poll_interval).await;
                }
            }
        })
    }
}
