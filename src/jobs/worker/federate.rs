use super::JobWorker;
use super::dispatch::Disposition;
use crate::application::ports::{Job, PortError};
use crate::federation::retry::DeliveryOutcome;

impl JobWorker {
    pub(crate) fn dispatch_federate(&self, job: &Job) -> Result<Disposition, PortError> {
        let outcome = match crate::federation::outbox::execute_federate_deliver(
            &self.services,
            &self.services.federation_http,
            &self.delivery_tracker,
            &job.idempotency_key,
            &job.payload,
        ) {
            Ok(outcome) => outcome,
            // Unparseable payloads can never succeed on retry.
            Err(PortError::Validation(reason)) => {
                crate::fed_debug!(
                    "out job key={:?} quarantine reason=bad-payload: {reason}",
                    job.idempotency_key
                );
                return Ok(Disposition::Quarantine(format!("bad-payload: {reason}")));
            }
            Err(e) => return Err(e),
        };
        crate::fed_debug!("out job key={:?} outcome={outcome:?}", job.idempotency_key);
        Ok(match outcome {
            DeliveryOutcome::Delivered => Disposition::Done,
            DeliveryOutcome::PermanentFailure(reason) => {
                Disposition::Quarantine(format!("permanent-failure: {reason}"))
            }
            // `fail()` reschedules with backoff via `process_one`, which
            // quarantines once the retry budget is exhausted.
            DeliveryOutcome::TransientFailure { retry_after_ms, .. } => {
                Disposition::Retry { retry_after_ms }
            }
            DeliveryOutcome::Deferred { until_ms } => Disposition::Defer(until_ms),
            DeliveryOutcome::Split { followups } => Disposition::Fanout(followups),
        })
    }
}
