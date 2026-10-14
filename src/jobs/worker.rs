use std::sync::Arc;

use crate::application::commands::AppServices;
use crate::application::events::NotifyJobPayload;
use crate::application::ports::{
    Clock, IdGenerator, Job, JobKind, JobQueue, NotificationStore, PortError, StreamingSink,
};
use crate::domain::notification::Notification;
use crate::federation::retry::{
    DeliveryOutcome, DestinationTracker, MAX_DELIVERY_ATTEMPTS,
};

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

impl JobWorker {
    pub fn process_one(&self) -> Result<bool, PortError> {
        let claimed = self.jobs.claim_next(self.lease_duration_ms)?;
        let job = match claimed {
            None => return Ok(false),
            Some(job) => job,
        };
        let outcome: Result<(), PortError> = match job.kind {
            JobKind::Notify => self.handle_notify(&job.payload, &job.idempotency_key),
            JobKind::StreamingPublish => self.streaming.publish("streaming", &job.payload),
            JobKind::FederateDeliver => self.handle_federate_deliver(&job),
            JobKind::TimelineMaterialize => Ok(()),
        };
        match outcome {
            Ok(()) => {
                self.jobs.complete(&job.id)?;
                Ok(true)
            }
            Err(_) => {
                let shift = job.attempts.min(10);
                let backoff = self.base_backoff_ms.saturating_mul(1_i64 << shift);
                let retry_at = self.clock.now_ms().saturating_add(backoff);
                self.jobs.fail(&job.id, retry_at)?;
                Ok(true)
            }
        }
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

    fn handle_federate_deliver(&self, job: &Job) -> Result<(), PortError> {
        let outcome = crate::federation::outbox::execute_federate_deliver(
            &self.services,
            &self.services.federation_http,
            &self.delivery_tracker,
            &job.payload,
        )?;
        match outcome {
            DeliveryOutcome::Delivered | DeliveryOutcome::PermanentFailure(_) => Ok(()),
            DeliveryOutcome::TransientFailure(_) => {
                if job.attempts + 1 >= MAX_DELIVERY_ATTEMPTS {
                    // Attempts exhausted: complete instead of retrying forever.
                    Ok(())
                } else {
                    // `fail()` reschedules with backoff via `process_one`.
                    Err(PortError::StorageUnavailable(
                        "transient federate deliver failure".into(),
                    ))
                }
            }
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
