use super::JobWorker;
use super::dispatch::Disposition;
use crate::application::ports::PortError;
use crate::federation::retry::DeliveryOutcome;

impl JobWorker {
    /// Run one `WebhookDeliver` job (gaps WP3): cooldown-aware signed
    /// POST reusing the federation outbox's classification/backoff
    /// discipline. Outcomes map like `FederateDeliver`
    /// (Done/Retry/Defer/Fanout); only the payload parse can fail here
    /// (the executor reports transport outcomes as values, never errors).
    pub(crate) fn dispatch_webhook_deliver(
        &self,
        payload: &[u8],
    ) -> Result<Disposition, PortError> {
        use crate::application::commands::webhooks::{
            WebhookDeliverPayload, execute_webhook_deliver,
        };
        let parsed: WebhookDeliverPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("webhook-deliver payload invalid: {e}")))?;
        let outcome = execute_webhook_deliver(
            &self.services.federation_http,
            &self.delivery_tracker,
            self.base_backoff_ms,
            self.clock.now_ms(),
            &parsed,
        );
        crate::fed_debug!("out webhook job url={:?} outcome={outcome:?}", parsed.url);
        Ok(match outcome {
            DeliveryOutcome::Delivered => Disposition::Done,
            DeliveryOutcome::PermanentFailure(reason) => {
                Disposition::Quarantine(format!("permanent-failure: {reason}"))
            }
            DeliveryOutcome::TransientFailure { retry_after_ms, .. } => {
                Disposition::Retry { retry_after_ms }
            }
            DeliveryOutcome::Deferred { until_ms } => Disposition::Defer(until_ms),
            DeliveryOutcome::Split { followups } => Disposition::Fanout(followups),
        })
    }
}
