use super::JobWorker;
use super::dispatch::Disposition;
use crate::application::ports::PortError;

impl JobWorker {
    /// Run one `TranscodeMedia` job (gaps WP1): finish processing the
    /// staged bytes and commit the derived result. A missing row or an
    /// already-converged row completes quietly; deterministic input
    /// failures quarantine with the ffmpeg reason (surfaced as `422`
    /// once `MediaState::Failed` lands); only missing/unreadable storage
    /// (or a vanishing toolchain) propagates for the caller to retry.
    pub(crate) fn dispatch_transcode_media(
        &self,
        payload: &[u8],
    ) -> Result<Disposition, PortError> {
        use crate::jobs::transcode::{TranscodeJobPayload, TranscodeOutcome, run_transcode_job};
        let file_id = TranscodeJobPayload::file_id_from_bytes(payload)?;
        match run_transcode_job(
            self.services.media.as_ref(),
            self.services.blobs.as_ref(),
            &file_id,
            &self.transcode_config,
        ) {
            Ok(TranscodeOutcome::Noop) | Ok(TranscodeOutcome::Ready(_)) => Ok(Disposition::Done),
            Ok(TranscodeOutcome::FailedDeterministic(reason)) => {
                Ok(Disposition::Quarantine(reason))
            }
            Err(err) => Err(err),
        }
    }
}
