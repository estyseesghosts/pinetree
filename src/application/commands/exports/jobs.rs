use crate::application::commands::AppServices;

use super::common::{IMPORT_MAX_FILE_BYTES, export_file_name, import_entity_tag};
use crate::application::events::NotifyJobPayload;
use crate::application::ports::{
    ExportEntity, ExportJobPayload, ImportEntity, ImportJobPayload, JobKind, NewJob, PortError,
};
use crate::domain::notification::NotificationKind;

impl AppServices {
    /// Enqueue one export job for `user_id` (endpoint path; the rate limit
    /// in the endpoint arm is the 1/hr gate).
    pub fn enqueue_export(
        &self,
        user_id: &str,
        entity: ExportEntity,
        exclude_muting: bool,
        exclude_inactive: bool,
    ) -> Result<String, PortError> {
        self.accounts.get_owner(user_id)?;
        let export_id = self.ids.generate();
        let payload = ExportJobPayload {
            export_id: export_id.clone(),
            user_id: user_id.to_string(),
            entity,
            exclude_muting,
            exclude_inactive,
        };
        let entity_tag = entity.exported_entity();
        self.jobs.enqueue_jobs(vec![NewJob::new_json(
            JobKind::Export,
            &payload,
            format!("export:{entity_tag}:{export_id}"),
            self.now_ms(),
        )?])?;
        Ok(export_id)
    }

    /// Enqueue one import job for `user_id` (endpoint path; guards ran in
    /// [`Self::check_import_file`] first).
    pub fn enqueue_import(
        &self,
        user_id: &str,
        entity: ImportEntity,
        file_id: &str,
        with_replies: Option<bool>,
    ) -> Result<String, PortError> {
        self.accounts.get_owner(user_id)?;
        let import_id = self.ids.generate();
        let payload = ImportJobPayload {
            import_id: import_id.clone(),
            user_id: user_id.to_string(),
            entity,
            file_id: file_id.to_string(),
            with_replies,
        };
        self.jobs.enqueue_jobs(vec![NewJob::new_json(
            JobKind::Import,
            &payload,
            format!("import:{import_id}"),
            self.now_ms(),
        )?])?;
        Ok(import_id)
    }

    /// Misskey `import-*.ts:71-82` guard shape: the file must exist AND
    /// belong to the caller (`NO_SUCH_FILE`), must be non-empty
    /// (`EMPTY_FILE`), and must fit the 64KiB cap (`TOO_BIG_FILE`). The
    /// endpoint maps these code strings to the per-endpoint verbatim UUIDs
    /// (`ApiError::import_*`). The commented-out upstream `type` check
    /// (`UNEXPECTED_FILE_TYPE`) stays unimplemented, like upstream.
    pub fn check_import_file(
        &self,
        user_id: &str,
        file_id: &str,
    ) -> Result<crate::domain::media::MediaMeta, PortError> {
        self.accounts.get_owner(user_id)?;
        let meta = match self.media.get(file_id)? {
            Some(meta) if meta.owner_id.as_deref() == Some(user_id) => meta,
            _ => return Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        };
        if meta.size_bytes == 0 {
            return Err(PortError::Validation("EMPTY_FILE".to_string()));
        }
        if meta.size_bytes > IMPORT_MAX_FILE_BYTES {
            return Err(PortError::Validation("TOO_BIG_FILE".to_string()));
        }
        Ok(meta)
    }

    /// Notifier-less delivery gate for `exportCompleted` (Misskey
    /// `NotificationService.ts:97-104` parity: with no `notifierId` only a
    /// `never` receive-config drops the notification; self-notify and mute
    /// checks do not apply). A missing notifiee fails open.
    pub fn export_completed_allowed(&self, notifiee_id: &str) -> Result<bool, PortError> {
        let Some(notifiee) = self.accounts.find_by_id(notifiee_id)? else {
            return Ok(true);
        };
        let Some(config) = notifiee
            .notification_recieve_config
            .get(NotificationKind::ExportCompleted.wire_type())
        else {
            return Ok(true);
        };
        Ok(config.kind.as_str() != "never")
    }

    /// Run one [`JobKind::Export`] job (worker path). Returns the
    /// `exportCompleted` `Notify` follow-up for the worker to commit
    /// atomically with the export job's completion (`Fanout`), or `None`
    /// when the user is gone (converges quietly, like upstream).
    pub fn run_export_job(&self, payload: &ExportJobPayload) -> Result<Option<NewJob>, PortError> {
        let Some(user) = self.accounts.find_by_id(&payload.user_id)? else {
            return Ok(None);
        };
        let bytes = self.build_export_bytes(&user.id, payload)?;
        let name = export_file_name(payload.entity, self.now_ms());
        // Content-deduped (`force == false`, unlike upstream `force: true`):
        // identical bytes return the existing row, so a crashed-and-retried
        // export converges on one file instead of orphaning a duplicate.
        let file = self.upload_media(&user.id, name, &bytes, None, false, false)?;
        let notify = NotifyJobPayload {
            notifiee_id: user.id.clone(),
            notifier_id: user.id.clone(),
            kind: NotificationKind::ExportCompleted,
            note_id: None,
            reaction: None,
            file_id: Some(file.id),
            exported_entity: Some(payload.entity.exported_entity().to_string()),
            clip_id: None,
            page_id: None,
        };
        Ok(Some(NewJob::new_json(
            JobKind::Notify,
            &notify,
            format!("export-completed:{}", payload.export_id),
            self.now_ms(),
        )?))
    }

    /// Run one [`JobKind::Import`] job (worker path). A gone user or file
    /// converges quietly (upstream returns early too). Per-row failures
    /// (unresolvable, self, `ALREADY_*`, block gates) skip the row and count
    /// it; only storage faults fail the job so it retries with backoff.
    pub fn run_import_job(&self, payload: &ImportJobPayload) -> Result<(), PortError> {
        let Some(user) = self.accounts.find_by_id(&payload.user_id)? else {
            return Ok(());
        };
        let Some(meta) = self.media.get(&payload.file_id)? else {
            return Ok(());
        };
        if meta.owner_id.as_deref() != Some(user.id.as_str()) {
            return Ok(());
        }
        let Some(bytes) = self.blobs.get(&meta.blob_key)? else {
            return Ok(());
        };
        match payload.entity {
            ImportEntity::Following | ImportEntity::Blocking | ImportEntity::Muting => {
                let text = String::from_utf8_lossy(&bytes);
                let (applied, skipped) = self.apply_relation_import(
                    &user.id,
                    payload.entity,
                    &text,
                    payload.with_replies,
                )?;
                eprintln!(
                    "import-{} for {}: applied={applied} skipped={skipped}",
                    import_entity_tag(payload.entity),
                    user.id,
                );
            }
            // User lists now have a subsystem (Batch O WP16): parse + apply
            // rows through `apply_user_list_import` (find-or-create by name,
            // local-only resolution, dup/foreign skips).
            ImportEntity::UserLists => {
                let text = String::from_utf8_lossy(&bytes);
                let (applied, skipped) = self.apply_user_list_import(&user.id, &text)?;
                eprintln!(
                    "import-user-lists for {}: applied={applied} skipped={skipped}",
                    user.id,
                );
            }
            // Antenna subsystem now exists (Batch N WS4): parse + validate
            // rows with the upstream `exportedAntennaSchema` rules, skip
            // empty-keyword rows, rewrite `src=list`+`userListAccts` to
            // `src=users` (upstream parity), and apply through
            // `create_antenna` (same op as the interactive endpoint).
            ImportEntity::Antennas => {
                let (applied, skipped) = self.apply_antenna_import(&user.id, &bytes)?;
                eprintln!(
                    "import-antennas for {}: applied={applied} skipped={skipped}",
                    user.id,
                );
            }
            // pinetree-only clip import (no upstream endpoint exists):
            // consume our own `export-clips` format — find-or-create each
            // clip by name, attach resolvable + visible notes.
            ImportEntity::Clips => {
                let text = String::from_utf8_lossy(&bytes);
                let (applied, skipped) = self.apply_clip_import(&user.id, &text)?;
                eprintln!(
                    "import-clips for {}: applied={applied} skipped={skipped}",
                    user.id,
                );
            }
        }
        Ok(())
    }
}
