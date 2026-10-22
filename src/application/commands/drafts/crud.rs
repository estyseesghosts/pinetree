use crate::application::commands::AppServices;

use crate::application::ports::{CreateDraftInput, PortError, TimelineCursor, UpdateDraftPatch};
use crate::domain::draft::{NoteDraft, validate_draft_poll_expiry, validate_draft_schedule};
use crate::domain::id::DomainError;

pub(crate) fn domain_to_port(err: DomainError) -> PortError {
    match err {
        DomainError::Validation(msg) => PortError::Validation(msg),
        DomainError::Conflict(msg) => PortError::Conflict(msg),
        DomainError::NotFound(msg) => PortError::NotFound(msg),
        DomainError::StorageUnavailable(msg) => PortError::StorageUnavailable(msg),
    }
}

impl AppServices {
    pub fn create_draft(
        &self,
        author_id: &str,
        input: CreateDraftInput,
    ) -> Result<NoteDraft, PortError> {
        self.accounts.get_owner(author_id)?;
        if self.notes.count_drafts(author_id)? >= self.note_draft_limit {
            return Err(PortError::Validation("TOO_MANY_DRAFTS".to_string()));
        }
        if input.is_actually_scheduled
            && self.notes.count_scheduled_drafts(author_id)? >= self.scheduled_note_limit
        {
            return Err(PortError::Validation(
                "TOO_MANY_SCHEDULED_NOTES".to_string(),
            ));
        }
        let now = self.now_ms();
        validate_draft_schedule(input.scheduled_at_ms, input.is_actually_scheduled, now)
            .map_err(domain_to_port)?;
        validate_draft_poll_expiry(input.poll_expires_at_ms, input.has_poll, now)
            .map_err(domain_to_port)?;
        self.check_draft_targets(
            author_id,
            input.reply_to.as_deref(),
            input.renote_of.as_deref(),
            &input.file_ids,
        )?;
        let draft = NoteDraft {
            id: self.ids.generate(),
            user_id: author_id.to_string(),
            text: input.text,
            cw: input.cw,
            visibility: input.visibility,
            visible_user_ids: input.visible_users,
            local_only: input.local_only,
            reply_id: input.reply_to,
            renote_id: input.renote_of,
            file_ids: input.file_ids,
            hashtag: input.hashtag,
            reaction_acceptance: input.reaction_acceptance,
            has_poll: input.has_poll,
            poll_choices: input.poll_choices,
            poll_multiple: input.poll_multiple,
            poll_expires_at_ms: input.poll_expires_at_ms,
            poll_expired_after_ms: input.poll_expired_after_ms,
            scheduled_at_ms: input.scheduled_at_ms,
            is_actually_scheduled: input.is_actually_scheduled,
        };
        draft.validate_shape().map_err(domain_to_port)?;
        let mut jobs = Vec::new();
        if draft.is_actually_scheduled {
            if let Some(at) = draft.scheduled_at_ms {
                jobs.push(self.scheduled_draft_job(&draft.id, at)?);
            }
        }
        self.notes.create_draft(draft, jobs)
    }

    /// Update one draft (Misskey `NoteDraftService.update` parity):
    /// the patch is validated as a *partial* shape (like upstream
    /// `validate(me, data)` receiving the partial `data`), so
    /// `isActuallyScheduled: true` without `scheduledAt` in the same patch
    /// is `SCHEDULED_AT_REQUIRED` even when the stored row already has a
    /// time. The old arming is cleared and the merged row re-armed when
    /// still scheduled (upstream `clearSchedule` + `schedule` parity).
    pub fn update_draft(
        &self,
        author_id: &str,
        draft_id: &str,
        patch: UpdateDraftPatch,
    ) -> Result<NoteDraft, PortError> {
        self.accounts.get_owner(author_id)?;
        let stored = match self.notes.get_draft(draft_id)? {
            Some(d) => d,
            None => return Err(PortError::NotFound("NO_SUCH_NOTE_DRAFT".to_string())),
        };
        if stored.user_id != author_id {
            return Err(PortError::NotFound("NO_SUCH_NOTE_DRAFT".to_string()));
        }
        if !stored.is_actually_scheduled && patch.is_actually_scheduled == Some(true) {
            if self.notes.count_scheduled_drafts(author_id)? >= self.scheduled_note_limit {
                return Err(PortError::Validation(
                    "TOO_MANY_SCHEDULED_NOTES".to_string(),
                ));
            }
        }
        // Partial-shape validation on the patch (upstream parity): schedule
        // rules see the patch's own `scheduledAt`/`isActuallyScheduled`
        // (`None` patch field = absent, like `undefined` upstream).
        let patch_scheduled_at = match patch.scheduled_at_ms {
            None => None,
            Some(inner) => inner,
        };
        let patch_is_scheduled = patch.is_actually_scheduled.unwrap_or(false);
        if patch.is_actually_scheduled.is_some() || patch.scheduled_at_ms.is_some() {
            validate_draft_schedule(patch_scheduled_at, patch_is_scheduled, self.now_ms())
                .map_err(domain_to_port)?;
        }
        if let Some(expires_at) = patch.poll_expires_at_ms {
            validate_draft_poll_expiry(expires_at, true, self.now_ms()).map_err(domain_to_port)?;
        }
        // Referential checks only for patch-supplied targets/files.
        let reply_to = match &patch.reply_to {
            None => stored.reply_id.clone(),
            Some(inner) => inner.clone(),
        };
        let renote_of = match &patch.renote_of {
            None => stored.renote_id.clone(),
            Some(inner) => inner.clone(),
        };
        let file_ids = patch
            .file_ids
            .clone()
            .unwrap_or_else(|| stored.file_ids.clone());
        self.check_draft_targets(
            author_id,
            reply_to.as_deref(),
            renote_of.as_deref(),
            &file_ids,
        )?;
        let merged = NoteDraft {
            id: stored.id.clone(),
            user_id: stored.user_id.clone(),
            text: patch.text.unwrap_or(stored.text),
            cw: patch.cw.unwrap_or(stored.cw),
            visibility: patch.visibility.unwrap_or(stored.visibility),
            visible_user_ids: patch.visible_users.unwrap_or(stored.visible_user_ids),
            local_only: patch.local_only.unwrap_or(stored.local_only),
            reply_id: reply_to,
            renote_id: renote_of,
            file_ids,
            hashtag: patch.hashtag.unwrap_or(stored.hashtag),
            reaction_acceptance: patch
                .reaction_acceptance
                .unwrap_or(stored.reaction_acceptance),
            // Upstream `update` never toggles `hasPoll` (the endpoint does
            // not forward it): poll content patches apply onto the stored
            // flag. Documented wart, see the endpoint manifest.
            has_poll: stored.has_poll,
            poll_choices: patch.poll_choices.unwrap_or(stored.poll_choices),
            poll_multiple: patch.poll_multiple.unwrap_or(stored.poll_multiple),
            poll_expires_at_ms: patch
                .poll_expires_at_ms
                .unwrap_or(stored.poll_expires_at_ms),
            poll_expired_after_ms: patch
                .poll_expired_after_ms
                .unwrap_or(stored.poll_expired_after_ms),
            scheduled_at_ms: patch.scheduled_at_ms.unwrap_or(stored.scheduled_at_ms),
            is_actually_scheduled: patch
                .is_actually_scheduled
                .unwrap_or(stored.is_actually_scheduled),
        };
        merged.validate_shape().map_err(domain_to_port)?;
        // `clearSchedule` parity: drop every arming for this draft (the
        // store scan covers delayed, waiting, and leased jobs, like the
        // upstream delayed/waiting/active scan), then re-arm from the
        // merged row when still scheduled. Both the clear and the new
        // arming commit in the same storage transaction as the row update.
        self.jobs.remove_scheduled_draft_jobs(&merged.id)?;
        let mut jobs = Vec::new();
        if merged.is_actually_scheduled {
            if let Some(at) = merged.scheduled_at_ms {
                jobs.push(self.scheduled_draft_job(&merged.id, at)?);
            }
        }
        self.notes.update_draft(merged, jobs)
    }

    /// Delete one draft (Misskey `NoteDraftService.delete` parity) and
    /// clear its arming (`clearSchedule` parity).
    pub fn delete_draft(&self, author_id: &str, draft_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(author_id)?;
        let stored = match self.notes.get_draft(draft_id)? {
            Some(d) => d,
            None => return Err(PortError::NotFound("NO_SUCH_NOTE_DRAFT".to_string())),
        };
        if stored.user_id != author_id {
            // The endpoint maps foreign drafts to `ACCESS_DENIED`
            // (unreachable through this op, which filters by owner first —
            // kept for the exact upstream error surface).
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        self.jobs.remove_scheduled_draft_jobs(&stored.id)?;
        self.notes.delete_draft(&stored.id, Vec::new())
    }

    /// List one account's drafts, newest-first (`notes/drafts/list`
    /// parity: `scheduled` tri-state filter + standard cursor paging).
    pub fn list_drafts(
        &self,
        author_id: &str,
        cursor: &TimelineCursor,
        scheduled: Option<bool>,
    ) -> Result<Vec<NoteDraft>, PortError> {
        self.accounts.get_owner(author_id)?;
        self.notes.drafts_by_user(author_id, cursor, scheduled)
    }

    /// Count one account's drafts (`notes/drafts/count` parity).
    pub fn count_drafts(&self, author_id: &str) -> Result<usize, PortError> {
        self.accounts.get_owner(author_id)?;
        self.notes.count_drafts(author_id)
    }
}
