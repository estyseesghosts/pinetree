use super::JobWorker;
use crate::application::ports::PortError;

impl JobWorker {
    /// Run one `Export` job (§13): build the file and store it in the
    /// caller's drive. Returns the `exportCompleted` `Notify` follow-up for
    /// the dispatcher to commit atomically with this job's completion, or
    /// `None` when the user is gone (converges quietly).
    pub(crate) fn handle_export(
        &self,
        payload: &[u8],
    ) -> Result<Option<crate::application::ports::NewJob>, PortError> {
        use crate::application::ports::ExportJobPayload;
        let parsed: ExportJobPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("export payload invalid: {e}")))?;
        self.services.run_export_job(&parsed)
    }
}
