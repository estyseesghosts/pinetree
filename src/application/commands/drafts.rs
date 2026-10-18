//! Note drafts + scheduling (`docs/misskey-parity-plan.md` §5; Misskey
//! `NoteDraftService.ts:60-338` parity). Misskey API + ActivityPub adapters
//! call these same ops; the scheduled-post worker calls
//! [`AppServices::post_scheduled_draft`].

use super::AppServices;

use crate::application::ports::{
    CreateDraftInput, CreateNoteInput, JobKind, NewJob, PortError, ScheduledDraftPayload,
    TimelineCursor, UpdateDraftPatch,
};
use crate::domain::draft::{NoteDraft, validate_draft_poll_expiry, validate_draft_schedule};
use crate::domain::id::DomainError;
use crate::domain::notification::NotificationKind;
use crate::domain::poll::PollInput;

fn domain_to_port(err: DomainError) -> PortError {
    match err {
        DomainError::Validation(msg) => PortError::Validation(msg),
        DomainError::Conflict(msg) => PortError::Conflict(msg),
        DomainError::NotFound(msg) => PortError::NotFound(msg),
        DomainError::StorageUnavailable(msg) => PortError::StorageUnavailable(msg),
    }
}

/// Outcome of one `PostScheduledDraft` job (worker settlement + tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostScheduledOutcome {
    /// The draft posted: carries the new note id.
    Posted,
    /// Converged quietly (missing draft, unscheduled, rescheduled, or the
    /// scheduled time lies in the future).
    Skipped,
    /// The post failed: the draft is kept and `ScheduledNotePostFailed`
    /// was emitted.
    Failed,
}

impl AppServices {
    /// Create one draft (Misskey `NoteDraftService.create` parity).
    /// Order: draft-count limit → scheduled-count limit → shape + schedule
    /// + poll-expiry validation → referential checks → store + arm the
    /// `PostScheduledDraft` job (`not_before = scheduled_at`) in one
    /// storage transaction.
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
            return Err(PortError::Validation("TOO_MANY_SCHEDULED_NOTES".to_string()));
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
                return Err(PortError::Validation("TOO_MANY_SCHEDULED_NOTES".to_string()));
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
        let file_ids = patch.file_ids.clone().unwrap_or_else(|| stored.file_ids.clone());
        self.check_draft_targets(author_id, reply_to.as_deref(), renote_of.as_deref(), &file_ids)?;
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

    /// Run one `PostScheduledDraft` job (Misskey
    /// `PostScheduledNoteProcessorService.process` parity): post the draft
    /// through the full note-create path (which re-validates targets,
    /// files, polls, and prohibited words at post time), delete the draft
    /// on success, and emit `ScheduledNotePosted` / `ScheduledNotePostFailed`.
    /// Converges quietly when the draft is gone, unscheduled, re-armed for
    /// a different time, or not due yet.
    pub fn post_scheduled_draft(
        &self,
        draft_id: &str,
        scheduled_at_ms: i64,
    ) -> Result<PostScheduledOutcome, PortError> {
        let Some(draft) = self.notes.get_draft(draft_id)? else {
            return Ok(PostScheduledOutcome::Skipped);
        };
        if !draft.is_actually_scheduled || draft.scheduled_at_ms != Some(scheduled_at_ms) {
            return Ok(PostScheduledOutcome::Skipped);
        }
        if scheduled_at_ms > self.now_ms() {
            return Ok(PostScheduledOutcome::Skipped);
        }
        let input = CreateNoteInput {
            author_id: draft.user_id.clone(),
            text: draft.text.clone(),
            cw: draft.cw.clone(),
            visibility: draft.visibility,
            local_only: draft.local_only,
            reply_to: draft.reply_id.clone(),
            renote_of: draft.renote_id.clone(),
            reply_parent_uri: None,
            file_ids: draft.file_ids.clone(),
            visible_users: draft.visible_user_ids.clone(),
            reaction_acceptance: draft.reaction_acceptance,
            no_extract_mentions: false,
            no_extract_hashtags: false,
            no_extract_emojis: false,
            has_poll: draft.has_poll,
            poll: draft_poll_input(&draft, self.now_ms()),
            poll_initial_votes: None,
            has_channel: false,
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            uri: None,
            url: None,
            extra_mentions: Vec::new(),
            extra_tags: Vec::new(),
            quote: None,
        };
        match self.create_note(input) {
            Ok(note) => {
                // Post succeeded: drop the draft (its arming already fired)
                // and notify.
                self.notes.delete_draft(&draft.id, Vec::new())?;
                self.emit_scheduled_notification(
                    &draft.user_id,
                    NotificationKind::ScheduledNotePosted,
                    Some(note.id),
                    None,
                )?;
                Ok(PostScheduledOutcome::Posted)
            }
            Err(_) => {
                // Post failed: the draft is kept (upstream parity) and the
                // failure is notified with the draft attached.
                self.emit_scheduled_notification(
                    &draft.user_id,
                    NotificationKind::ScheduledNotePostFailed,
                    None,
                    Some(draft.id),
                )?;
                Ok(PostScheduledOutcome::Failed)
            }
        }
    }

    /// Referential checks shared by draft `create`/`update` (Misskey
    /// `NoteDraftService.validate` parity, minus channel internals which
    /// do not exist here): every file must exist and be owned by the
    /// author (`NO_SUCH_FILE`); reply/renote targets must exist and be
    /// live (`NO_SUCH_REPLY_TARGET` / `NO_SUCH_RENOTE_TARGET`); a target
    /// whose author blocks the drafter is `YOU_HAVE_BEEN_BLOCKED`.
    /// Pure-renote and visibility-narrowing subtleties are deferred to
    /// post time, when the full `create_note` path re-validates.
    fn check_draft_targets(
        &self,
        author_id: &str,
        reply_to: Option<&str>,
        renote_of: Option<&str>,
        file_ids: &[String],
    ) -> Result<(), PortError> {
        for fid in file_ids {
            let owned = match self.media.get(fid)? {
                Some(meta) => meta.owner_id.as_deref() == Some(author_id),
                None => false,
            };
            if !owned {
                return Err(PortError::Validation("NO_SUCH_FILE".to_string()));
            }
        }
        if let Some(reply_id) = reply_to {
            let target = match self.notes.get_note(reply_id)? {
                Some(note) => note,
                None => return Err(PortError::NotFound("NO_SUCH_REPLY_TARGET".to_string())),
            };
            if target.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_REPLY_TARGET".to_string()));
            }
            if target.author_id != author_id
                && self.blocks.is_blocking(&target.author_id, author_id)?
            {
                return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
            }
        }
        if let Some(renote_id) = renote_of {
            let target = match self.notes.get_note(renote_id)? {
                Some(note) => note,
                None => return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string())),
            };
            if target.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()));
            }
            if target.author_id != author_id
                && self.blocks.is_blocking(&target.author_id, author_id)?
            {
                return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
            }
        }
        Ok(())
    }

    /// Build the delayed `PostScheduledDraft` job (`not_before =
    /// scheduled_at`; Misskey `schedule()` delay parity). The payload
    /// carries the armed time so a rescheduled draft converges quietly.
    fn scheduled_draft_job(&self, draft_id: &str, scheduled_at_ms: i64) -> Result<NewJob, PortError> {
        NewJob::new_json(
            JobKind::PostScheduledDraft,
            &ScheduledDraftPayload {
                draft_id: draft_id.to_string(),
                scheduled_at_ms,
            },
            format!("scheduled-draft:{draft_id}:{scheduled_at_ms}"),
            scheduled_at_ms,
        )
    }

    /// Insert a scheduling notification directly (the worker is already
    /// inside a job: no extra queue hop) and fan the `main`-channel
    /// `notification` hint out through the streaming sink.
    fn emit_scheduled_notification(
        &self,
        user_id: &str,
        kind: NotificationKind,
        note_id: Option<String>,
        draft_id: Option<String>,
    ) -> Result<(), PortError> {
        let stored = self.notifications.insert_notification(
            crate::domain::notification::Notification {
                id: self.ids.generate(),
                notifiee_id: user_id.to_string(),
                notifier_id: user_id.to_string(),
                kind,
                note_id,
                reaction: None,
                draft_id,
                file_id: None,
                exported_entity: None,
                is_read: false,
            },
        )?;
        let packed = crate::api::misskey::packers::pack_notification(&stored, self);
        self.streaming.push_main(
            crate::application::events::MainMessage {
                user_id: user_id.to_string(),
                kind: crate::application::events::MainEventKind::Notification,
                body: packed,
            },
        )
    }
}

/// Draft poll columns → [`PollInput`] for the post-time create path.
/// `expiredAfter` (a duration from post time) wins over `expiresAt` (an
/// absolute instant), exactly like the post-time `expiredAfter`-wins rule
/// in `notes/create` (Misskey `PostScheduledNoteProcessorService`
/// `pollExpiredAfter ? now + after : pollExpiresAt` parity).
fn draft_poll_input(draft: &NoteDraft, now_ms: i64) -> Option<PollInput> {
    if !draft.has_poll {
        return None;
    }
    let expires_at_ms = match draft.poll_expired_after_ms {
        Some(after) if after >= 1 => Some(now_ms.saturating_add(after)),
        _ => draft.poll_expires_at_ms,
    };
    Some(PollInput {
        choices: draft.poll_choices.clone(),
        multiple: draft.poll_multiple,
        expires_at_ms,
    })
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::application::ports::{ActivityDedup, BlobStore, Clock, IdGenerator, NoopStreamingSink};
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::domain::note::Visibility;
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::jobs::worker::JobWorker;
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    fn draft_services(prefix: &str) -> (AppServices, Arc<ManualClock>) {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-draft-test-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<ManualClock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store =
            RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        let services = AppServices {
            clock: clock.clone() as Arc<dyn Clock>,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://example.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        };
        (services, clock)
    }

    fn worker_for(services: &Arc<AppServices>) -> JobWorker {
        JobWorker {
            clock: services.clock.clone(),
            ids: services.ids.clone(),
            jobs: services.jobs.clone(),
            notifications: services.notifications.clone(),
            streaming: services.streaming.clone(),
            services: services.clone(),
            delivery_tracker: Arc::new(crate::federation::retry::DestinationTracker::new()),
            lease_duration_ms: 30_000,
            base_backoff_ms: 1_000,
        }
    }

    fn port_code(err: PortError) -> String {
        match err {
            PortError::Validation(m) | PortError::Conflict(m) | PortError::NotFound(m) => m,
            PortError::StorageUnavailable(m) => panic!("storage fault: {m}"),
        }
    }

    fn simple_draft() -> CreateDraftInput {
        CreateDraftInput {
            text: Some("draft body".to_string()),
            visibility: Visibility::Public,
            ..Default::default()
        }
    }

    fn scheduled_input(at_ms: i64) -> CreateDraftInput {
        CreateDraftInput {
            text: Some("scheduled body".to_string()),
            visibility: Visibility::Public,
            scheduled_at_ms: Some(at_ms),
            is_actually_scheduled: true,
            ..Default::default()
        }
    }

    #[test]
    fn draft_crud_count_and_list_filters() {
        let (services, _) = draft_services("crud");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();

        assert_eq!(services.count_drafts(&alice.id).unwrap(), 0);
        let d1 = services.create_draft(&alice.id, simple_draft()).unwrap();
        assert_eq!(d1.user_id, alice.id);
        assert!(!d1.is_actually_scheduled);
        let now = services.clock.now_ms();
        let d2 = services
            .create_draft(&alice.id, scheduled_input(now + 60_000))
            .unwrap();
        assert_eq!(services.count_drafts(&alice.id).unwrap(), 2);
        assert_eq!(services.count_drafts(&bob.id).unwrap(), 0);

        // List newest-first; `scheduled` tri-state filters.
        let all = services
            .list_drafts(&alice.id, &TimelineCursor::with_limit(10), None)
            .unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, d2.id, "newest first");
        let scheduled_only = services
            .list_drafts(&alice.id, &TimelineCursor::with_limit(10), Some(true))
            .unwrap();
        assert_eq!(scheduled_only.len(), 1);
        assert_eq!(scheduled_only[0].id, d2.id);
        let unscheduled_only = services
            .list_drafts(&alice.id, &TimelineCursor::with_limit(10), Some(false))
            .unwrap();
        assert_eq!(unscheduled_only.len(), 1);
        assert_eq!(unscheduled_only[0].id, d1.id);

        // Update merges the patch over the stored row.
        let updated = services
            .update_draft(
                &alice.id,
                &d1.id,
                UpdateDraftPatch {
                    text: Some(Some("edited".to_string())),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated.text.as_deref(), Some("edited"));
        assert_eq!(updated.user_id, alice.id);

        // Foreign drafts are invisible to the updater.
        let err = services
            .update_draft(
                &bob.id,
                &d1.id,
                UpdateDraftPatch {
                    text: Some(Some("hijack".to_string())),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(port_code(err), "NO_SUCH_NOTE_DRAFT");

        // Delete + count.
        services.delete_draft(&alice.id, &d1.id).unwrap();
        assert_eq!(services.count_drafts(&alice.id).unwrap(), 1);
        services.delete_draft(&alice.id, &d2.id).unwrap();
        assert_eq!(services.count_drafts(&alice.id).unwrap(), 0);
        let err = services.delete_draft(&alice.id, &d1.id).unwrap_err();
        assert_eq!(port_code(err), "NO_SUCH_NOTE_DRAFT");
    }

    #[test]
    fn draft_schedule_gates_carry_exact_upstream_codes() {
        let (services, clock) = draft_services("schedule");
        let alice = services.create_account("alice", None).unwrap();
        let now = clock.now_ms();

        // Scheduled without a time.
        let err = services
            .create_draft(
                &alice.id,
                CreateDraftInput {
                    is_actually_scheduled: true,
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(port_code(err), "SCHEDULED_AT_REQUIRED");

        // Non-future times (past and exactly-now).
        for at in [now - 1, now] {
            let err = services
                .create_draft(&alice.id, scheduled_input(at))
                .unwrap_err();
            assert_eq!(port_code(err), "SCHEDULED_AT_MUST_BE_IN_FUTURE");
        }

        // One scheduled draft fits the default `scheduledNoteLimit` of 1;
        // the second is rejected with the exact upstream code.
        services
            .create_draft(&alice.id, scheduled_input(now + 60_000))
            .unwrap();
        let err = services
            .create_draft(&alice.id, scheduled_input(now + 120_000))
            .unwrap_err();
        assert_eq!(port_code(err), "TOO_MANY_SCHEDULED_NOTES");

        // Unscheduled drafts are unaffected by the scheduled cap.
        let alice_plain = services.create_draft(&alice.id, simple_draft()).unwrap();

        // Update: flipping `isActuallyScheduled` without a time in the
        // same patch is REQUIRED even when the row already has one (the
        // patch is validated as a partial, upstream parity). A fresh
        // account avoids the scheduled cap tripping first (upstream
        // checks the cap before validating, same as here).
        let bob = services.create_account("bob", None).unwrap();
        let bob_plain = services.create_draft(&bob.id, simple_draft()).unwrap();
        let err = services
            .update_draft(
                &bob.id,
                &bob_plain.id,
                UpdateDraftPatch {
                    is_actually_scheduled: Some(true),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(port_code(err), "SCHEDULED_AT_REQUIRED");

        // Update: scheduling past the cap is rejected too.
        let err = services
            .update_draft(
                &alice.id,
                &alice_plain.id,
                UpdateDraftPatch {
                    scheduled_at_ms: Some(Some(now + 30_000)),
                    is_actually_scheduled: Some(true),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(port_code(err), "TOO_MANY_SCHEDULED_NOTES");
    }

    #[test]
    fn draft_limit_and_referential_gates() {
        let (mut services, _) = draft_services("limits");
        services.note_draft_limit = 2;
        let alice = services.create_account("alice", None).unwrap();
        services.create_draft(&alice.id, simple_draft()).unwrap();
        services.create_draft(&alice.id, simple_draft()).unwrap();
        let err = services.create_draft(&alice.id, simple_draft()).unwrap_err();
        assert_eq!(port_code(err), "TOO_MANY_DRAFTS");
        // Lift the cap for the referential checks below (they would
        // otherwise trip the draft limit first).
        services.note_draft_limit = 100;

        // Unknown files / targets are rejected with the note-create idents.
        let err = services
            .create_draft(
                &alice.id,
                CreateDraftInput {
                    file_ids: vec!["mid00000000000001".to_string()],
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(port_code(err), "NO_SUCH_FILE");
        let err = services
            .create_draft(
                &alice.id,
                CreateDraftInput {
                    reply_to: Some("nid00000000000001".to_string()),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(port_code(err), "NO_SUCH_REPLY_TARGET");
        let err = services
            .create_draft(
                &alice.id,
                CreateDraftInput {
                    renote_of: Some("nid00000000000001".to_string()),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(port_code(err), "NO_SUCH_RENOTE_TARGET");

        // Blocked by the reply target's author.
        let bob = services.create_account("bob", None).unwrap();
        let target = crate::application::ports::CreateNoteInput::simple_public(&bob.id, "hi");
        let target = services.create_note(target).unwrap();
        services.block(&bob.id, &alice.id).unwrap();
        let err = services
            .create_draft(
                &alice.id,
                CreateDraftInput {
                    reply_to: Some(target.id),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(port_code(err), "YOU_HAVE_BEEN_BLOCKED");
    }

    #[test]
    fn scheduled_post_fires_through_the_worker() {
        let (services, clock) = draft_services("fire");
        let alice = services.create_account("alice", None).unwrap();
        let now = clock.now_ms();
        let draft = services
            .create_draft(&alice.id, scheduled_input(now + 60_000))
            .unwrap();
        let services = Arc::new(services);
        let worker = worker_for(&services);

        // Not due yet: the arming job is not claimable and nothing posts.
        worker.drain(100).unwrap();
        assert!(services.notes.get_draft(&draft.id).unwrap().is_some());

        // Past the scheduled time the worker posts the note, deletes the
        // draft, and emits `ScheduledNotePosted` carrying the note.
        clock.advance_ms(61_000);
        worker.drain(100).unwrap();
        assert!(services.notes.get_draft(&draft.id).unwrap().is_none());
        let notes = services
            .notes
            .notes_by_author(&alice.id, &TimelineCursor::with_limit(10))
            .unwrap();
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].text.as_deref(), Some("scheduled body"));
        let notifs = services
            .notifications
            .list_notifications_filtered(
                &alice.id,
                &TimelineCursor::with_limit(10),
                &[NotificationKind::ScheduledNotePosted],
                &[],
            )
            .unwrap();
        assert_eq!(notifs.len(), 1);
        assert_eq!(notifs[0].note_id, Some(notes[0].id.clone()));
        // The kind round-trips through serde as the upstream camelCase type.
        let kind_json = serde_json::to_value(&notifs[0].kind).unwrap();
        assert_eq!(kind_json, serde_json::json!("scheduledNotePosted"));
    }

    #[test]
    fn scheduled_post_failure_keeps_the_draft_and_notifies() {
        let (services, clock) = draft_services("fail");
        let alice = services.create_account("alice", None).unwrap();
        let now = clock.now_ms();
        // A draft replying to a note that is deleted before post time:
        // the post-time `create_note` rejects it, so the draft is kept
        // and `ScheduledNotePostFailed` carries the draft.
        let target =
            crate::application::ports::CreateNoteInput::simple_public(&alice.id, "target");
        let target = services.create_note(target).unwrap();
        let draft = services
            .create_draft(
                &alice.id,
                CreateDraftInput {
                    text: Some("reply draft".to_string()),
                    visibility: Visibility::Public,
                    reply_to: Some(target.id.clone()),
                    scheduled_at_ms: Some(now + 60_000),
                    is_actually_scheduled: true,
                    ..Default::default()
                },
            )
            .unwrap();
        services.delete_note(&alice.id, &target.id).unwrap();
        let services = Arc::new(services);
        let worker = worker_for(&services);
        clock.advance_ms(61_000);
        worker.drain(100).unwrap();
        assert!(services.notes.get_draft(&draft.id).unwrap().is_some());
        let notifs = services
            .notifications
            .list_notifications_filtered(
                &alice.id,
                &TimelineCursor::with_limit(10),
                &[NotificationKind::ScheduledNotePostFailed],
                &[],
            )
            .unwrap();
        assert_eq!(notifs.len(), 1);
        assert_eq!(notifs[0].draft_id, Some(draft.id.clone()));
        // The failed kind filters through the same exclude machinery.
        let excluded = services
            .notifications
            .list_notifications_filtered(
                &alice.id,
                &TimelineCursor::with_limit(10),
                &[],
                &[NotificationKind::ScheduledNotePostFailed],
            )
            .unwrap();
        assert!(excluded.is_empty());
    }

    #[test]
    fn draft_update_reschedules_and_delete_clears() {
        let (services, clock) = draft_services("resched");
        let alice = services.create_account("alice", None).unwrap();
        let now = clock.now_ms();
        let draft = services
            .create_draft(&alice.id, scheduled_input(now + 60_000))
            .unwrap();
        // Move the arming to +120s.
        let merged = services
            .update_draft(
                &alice.id,
                &draft.id,
                UpdateDraftPatch {
                    scheduled_at_ms: Some(Some(now + 120_000)),
                    is_actually_scheduled: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(merged.scheduled_at_ms, Some(now + 120_000));
        let services = Arc::new(services);
        let worker = worker_for(&services);
        // The stale +60s arming is gone: nothing posts at +61s.
        clock.advance_ms(61_000);
        worker.drain(100).unwrap();
        assert!(services.notes.get_draft(&draft.id).unwrap().is_some());
        assert!(
            services
                .notes
                .notes_by_author(&alice.id, &TimelineCursor::with_limit(10))
                .unwrap()
                .is_empty()
        );
        // The re-armed +120s job posts exactly once.
        clock.advance_ms(60_000);
        worker.drain(100).unwrap();
        assert!(services.notes.get_draft(&draft.id).unwrap().is_none());
        assert_eq!(
            services
                .notes
                .notes_by_author(&alice.id, &TimelineCursor::with_limit(10))
                .unwrap()
                .len(),
            1
        );

        // Delete clears the arming: a deleted draft never posts.
        let clock_now = clock.now_ms();
        let doomed = services
            .create_draft(&alice.id, scheduled_input(clock_now + 60_000))
            .unwrap();
        services.delete_draft(&alice.id, &doomed.id).unwrap();
        clock.advance_ms(61_000);
        worker.drain(100).unwrap();
        assert_eq!(
            services
                .notes
                .notes_by_author(&alice.id, &TimelineCursor::with_limit(10))
                .unwrap()
                .len(),
            1,
            "only the earlier note exists"
        );
    }
}
