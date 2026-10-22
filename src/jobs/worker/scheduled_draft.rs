use super::JobWorker;
use crate::application::ports::PortError;

impl JobWorker {
    /// Run one `PostScheduledDraft` job (§5): post the draft when its
    /// scheduled time has arrived. Gone, unscheduled, rescheduled, and
    /// not-yet-due drafts converge quietly; only storage faults retry.
    pub(crate) fn handle_post_scheduled_draft(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::ScheduledDraftPayload;
        let parsed: ScheduledDraftPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("scheduled-draft payload invalid: {e}")))?;
        self.services
            .post_scheduled_draft(&parsed.draft_id, parsed.scheduled_at_ms)
            .map(|_| ())
    }
}
