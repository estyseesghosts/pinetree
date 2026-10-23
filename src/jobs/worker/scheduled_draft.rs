use super::JobWorker;
use crate::api::misskey::packers::pack_notification;
use crate::application::events::{MainEventKind, MainMessage};
use crate::application::ports::{PortError, ScheduledDraftPayload};

impl JobWorker {
    /// Run one `PostScheduledDraft` job (§5): post the draft when its
    /// scheduled time has arrived. Gone, unscheduled, rescheduled, and
    /// not-yet-due drafts converge quietly; only storage faults retry.
    /// The application layer returns the inserted notification row as a
    /// pack input; the `main`-channel body is packed here (adapter-side,
    /// mirroring `push_notify_mains` for scheduler kinds: the single
    /// `notification` hint only — no unread nudge, no side channels).
    pub(crate) fn handle_post_scheduled_draft(&self, payload: &[u8]) -> Result<(), PortError> {
        let parsed: ScheduledDraftPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("scheduled-draft payload invalid: {e}")))?;
        let (_outcome, notification) = self
            .services
            .post_scheduled_draft(&parsed.draft_id, parsed.scheduled_at_ms)?;
        if let Some(stored) = notification {
            let body = pack_notification(&stored, &self.services);
            self.services.streaming.push_main(MainMessage {
                user_id: stored.notifiee_id.clone(),
                kind: MainEventKind::Notification,
                body,
            })?;
        }
        Ok(())
    }
}
