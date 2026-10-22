use super::JobWorker;
use crate::application::ports::PortError;

impl JobWorker {
    /// Run one `Import` job (§13): apply the caller's drive-file rows.
    /// Gone users/files converge quietly; only storage faults retry.
    pub(crate) fn handle_import(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::ImportJobPayload;
        let parsed: ImportJobPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("import payload invalid: {e}")))?;
        self.services.run_import_job(&parsed)
    }
}
