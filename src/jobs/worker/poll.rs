use super::JobWorker;
use crate::application::events::NotifyJobPayload;
use crate::application::ports::{JobKind, NewJob, PortError};
use crate::domain::notification::NotificationKind;

impl JobWorker {
    /// Run one `PollExpiry` job (§4): publish the `pollEnded` streaming
    /// event when the poll has expired, then enqueue the `pollEnded`
    /// `Notify` job for the note author (Misskey
    /// `EndedPollNotificationProcessorService` parity, author-only —
    /// pinetree has no voter fan-out yet). Gone notes/polls and premature
    /// runs complete quietly. Only storage faults retry.
    pub(crate) fn run_poll_expiry(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::PollExpiryPayload;
        let parsed: PollExpiryPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("poll-expiry payload invalid: {e}")))?;
        if !self.services.handle_poll_expiry(&parsed.note_id)? {
            return Ok(());
        }
        let Some(note) = self.services.notes.get_note(&parsed.note_id)? else {
            return Ok(());
        };
        let notify = NotifyJobPayload {
            notifiee_id: note.author_id.clone(),
            notifier_id: note.author_id.clone(),
            kind: NotificationKind::PollEnded,
            note_id: Some(note.id.clone()),
            reaction: None,
            file_id: None,
            exported_entity: None,
            clip_id: None,
            page_id: None,
        };
        let job = NewJob::new_json(
            JobKind::Notify,
            &notify,
            format!("notify:poll-ended:{}", note.id),
            self.clock.now_ms(),
        )?;
        self.jobs.enqueue_jobs(vec![job])
    }
}
