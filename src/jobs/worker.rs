use std::sync::Arc;

use crate::application::commands::AppServices;
use crate::application::events::NotifyJobPayload;
use crate::application::ports::{
    Clock, IdGenerator, Job, JobKind, JobQueue, NotificationStore, PortError, StreamingSink,
    MAX_JOB_ATTEMPTS,
};
use crate::domain::notification::Notification;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

/// In-process durable-job worker (Phase 1, single node).
pub struct JobWorker {
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    pub jobs: Arc<dyn JobQueue>,
    pub notifications: Arc<dyn NotificationStore>,
    pub streaming: Arc<dyn StreamingSink>,
    pub services: Arc<AppServices>,
    pub delivery_tracker: Arc<DestinationTracker>,
    pub lease_duration_ms: i64,
    pub base_backoff_ms: i64,
}

/// How a claimed job's record is settled after its handler runs.
enum Disposition {
    /// Handler succeeded: remove the row.
    Done,
    /// Handler failed retryably: reschedule with backoff, quarantining when
    /// the retry budget (`MAX_JOB_ATTEMPTS`) is exhausted.
    Retry,
    /// Handler failed terminally: quarantine with the recorded reason.
    Quarantine(String),
}

impl JobWorker {
    pub fn process_one(&self) -> Result<bool, PortError> {
        let claimed = self.jobs.claim_next(self.lease_duration_ms)?;
        let job = match claimed {
            None => return Ok(false),
            Some(job) => job,
        };
        let disposition: Result<Disposition, PortError> = match job.kind {
            JobKind::Notify => match self.handle_notify(&job.payload, &job.idempotency_key) {
                Ok(()) => Ok(Disposition::Done),
                // Unparseable payloads can never succeed on retry.
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry),
            },
            JobKind::StreamingPublish => match self.streaming.publish("streaming", &job.payload)
            {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry),
            },
            JobKind::FederateDeliver => self.dispatch_federate(&job),
            JobKind::TimelineMaterialize => Ok(Disposition::Done),
        };
        // Storage errors from the settlement calls below propagate to the
        // caller, as before (drain/spawn behavior unchanged).
        match disposition? {
            Disposition::Done => {
                self.jobs.complete(&job.id)?;
            }
            Disposition::Retry => {
                if job.attempts + 1 >= MAX_JOB_ATTEMPTS {
                    // Retry budget exhausted for any kind: quarantine with a
                    // recorded reason instead of retrying forever.
                    self.quarantine(&job, "retry-exhausted")?;
                } else {
                    let shift = job.attempts.min(10);
                    let backoff = self.base_backoff_ms.saturating_mul(1_i64 << shift);
                    let retry_at = self.clock.now_ms().saturating_add(backoff);
                    self.jobs.fail(&job.id, retry_at)?;
                }
            }
            Disposition::Quarantine(reason) => {
                self.quarantine(&job, &reason)?;
            }
        }
        Ok(true)
    }

    fn quarantine(&self, job: &Job, reason: &str) -> Result<(), PortError> {
        eprintln!(
            "quarantining job {} ({:?}) after {} attempts: {reason}",
            job.id, job.kind, job.attempts
        );
        self.jobs.fail_permanent(&job.id, reason)
    }

    fn handle_notify(&self, payload: &[u8], idempotency_key: &str) -> Result<(), PortError> {
        let notify: NotifyJobPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("notify payload invalid: {e}")))?;
        let id = if idempotency_key.trim().is_empty() {
            self.ids.generate()
        } else {
            idempotency_key.to_string()
        };
        let notification = Notification {
            id,
            notifiee_id: notify.notifiee_id,
            notifier_id: notify.notifier_id,
            kind: notify.kind,
            note_id: notify.note_id,
            reaction: notify.reaction,
            is_read: false,
        };
        match self.notifications.insert_notification(notification) {
            Ok(_) => Ok(()),
            // Already inserted under this idempotency key: treat as success
            // so the job completes instead of retrying forever.
            Err(PortError::Conflict(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn dispatch_federate(&self, job: &Job) -> Result<Disposition, PortError> {
        let outcome = match crate::federation::outbox::execute_federate_deliver(
            &self.services,
            &self.services.federation_http,
            &self.delivery_tracker,
            &job.payload,
        ) {
            Ok(outcome) => outcome,
            // Unparseable payloads can never succeed on retry.
            Err(PortError::Validation(reason)) => {
                return Ok(Disposition::Quarantine(format!("bad-payload: {reason}")));
            }
            Err(e) => return Err(e),
        };
        match outcome {
            DeliveryOutcome::Delivered => Ok(Disposition::Done),
            DeliveryOutcome::PermanentFailure(reason) => Ok(Disposition::Quarantine(format!(
                "permanent-failure: {reason}"
            ))),
            // `fail()` reschedules with backoff via `process_one`, which
            // quarantines once the retry budget is exhausted.
            DeliveryOutcome::TransientFailure(_) => Ok(Disposition::Retry),
        }
    }

    pub fn drain(&self, max_steps: usize) -> Result<usize, PortError> {
        let mut done = 0;
        for _ in 0..max_steps {
            if !self.process_one()? {
                break;
            }
            done += 1;
        }
        Ok(done)
    }

    pub fn spawn(
        self: Arc<Self>,
        poll_interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let this = Arc::clone(&self);
                let worked = tokio::task::spawn_blocking(move || this.process_one().unwrap_or(false))
                    .await
                    .unwrap_or(false);
                if !worked {
                    tokio::time::sleep(poll_interval).await;
                }
            }
        })
    }
}
