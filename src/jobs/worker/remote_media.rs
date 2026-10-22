use super::JobWorker;
use crate::application::ports::{PortError, RemoteMediaFetchPayload};

impl JobWorker {
    /// Slice 7: run one `RemoteMediaFetch` job. A note deleted between
    /// enqueue and run maps to success (nothing to attach to).
    pub(crate) fn handle_remote_media_fetch(&self, payload: &[u8]) -> Result<(), PortError> {
        let parsed: RemoteMediaFetchPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("remote-media payload invalid: {e}")))?;
        match crate::media::remote_cache::process_remote_media_fetch(&self.services, &parsed) {
            Ok(()) => Ok(()),
            Err(PortError::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
