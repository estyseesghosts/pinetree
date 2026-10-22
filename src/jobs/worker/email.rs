use super::JobWorker;
use crate::application::ports::PortError;

impl JobWorker {
    /// Run one `EmailSend` job (gaps WP3): render-and-send one message
    /// through the configured transport. Terminal failures (bad payload
    /// shape, unconfigured transport, invalid message, auth rejection)
    /// quarantine; connection and content rejections propagate for the
    /// caller to retry with backoff.
    pub(crate) fn handle_email_send(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::services::email::{EmailError, EmailSendPayload, execute_email_send};
        let parsed: EmailSendPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("email-send payload invalid: {e}")))?;
        execute_email_send(self.email_sender.as_ref(), &parsed).map_err(|err| match err {
            EmailError::NotConfigured | EmailError::InvalidMessage(_) | EmailError::Auth(_) => {
                PortError::Validation(format!("email send failed: {err}"))
            }
            EmailError::Connection(_) | EmailError::Rejected(_) => {
                PortError::StorageUnavailable(format!("email send failed: {err}"))
            }
        })
    }
}
