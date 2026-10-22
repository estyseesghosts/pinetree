use super::JobWorker;
use crate::application::ports::PortError;

impl JobWorker {
    /// Run one `RemoteEmojiFetch` job (Batch N WP8b): fetch-and-cache one
    /// known remote emoji. Gone/already-cached rows and unusable fetches
    /// converge quietly inside; only storage faults retry.
    pub(crate) fn handle_remote_emoji_fetch(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::RemoteEmojiFetchPayload;
        let parsed: RemoteEmojiFetchPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("remote-emoji payload invalid: {e}")))?;
        self.services
            .fetch_remote_emoji(&parsed.host, &parsed.shortcode)
    }
}
