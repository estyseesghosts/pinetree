use crate::application::commands::AppServices;

use crate::application::ports::{
    CreateDraftInput, CreateNoteInput, JobKind, NewJob, PortError, ScheduledDraftPayload,
};
use crate::domain::draft::NoteDraft;
use crate::domain::notification::{Notification, NotificationKind};
use crate::domain::poll::PollInput;

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
    /// Post one due scheduled draft. Returns the settlement plus, when a
    /// notification row was inserted, the stored row itself (the pack
    /// input): the caller — the `PostScheduledDraft` worker, an adapter —
    /// packs the `main`-channel body worker-side (mirroring
    /// `push_notify_mains`), so the application layer never touches wire
    /// shapes.
    pub fn post_scheduled_draft(
        &self,
        draft_id: &str,
        scheduled_at_ms: i64,
    ) -> Result<(PostScheduledOutcome, Option<Notification>), PortError> {
        let Some(draft) = self.notes.get_draft(draft_id)? else {
            return Ok((PostScheduledOutcome::Skipped, None));
        };
        if !draft.is_actually_scheduled || draft.scheduled_at_ms != Some(scheduled_at_ms) {
            return Ok((PostScheduledOutcome::Skipped, None));
        }
        if scheduled_at_ms > self.now_ms() {
            return Ok((PostScheduledOutcome::Skipped, None));
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
            uri_base: String::new(),
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
                let stored = self.insert_scheduled_notification(
                    &draft.user_id,
                    NotificationKind::ScheduledNotePosted,
                    Some(note.id),
                    None,
                )?;
                Ok((PostScheduledOutcome::Posted, Some(stored)))
            }
            Err(_) => {
                // Post failed: the draft is kept (upstream parity) and the
                // failure is notified with the draft attached.
                let stored = self.insert_scheduled_notification(
                    &draft.user_id,
                    NotificationKind::ScheduledNotePostFailed,
                    None,
                    Some(draft.id),
                )?;
                Ok((PostScheduledOutcome::Failed, Some(stored)))
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
    pub(crate) fn check_draft_targets(
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
    pub(crate) fn scheduled_draft_job(
        &self,
        draft_id: &str,
        scheduled_at_ms: i64,
    ) -> Result<NewJob, PortError> {
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
    /// inside a job: no extra queue hop) and return the stored row. The
    /// caller packs the `main`-channel body worker-side, so this stays
    /// clear of wire shapes.
    fn insert_scheduled_notification(
        &self,
        user_id: &str,
        kind: NotificationKind,
        note_id: Option<String>,
        draft_id: Option<String>,
    ) -> Result<Notification, PortError> {
        self.notifications.insert_notification(Notification {
            id: self.ids.generate(),
            notifiee_id: user_id.to_string(),
            notifier_id: user_id.to_string(),
            kind,
            note_id,
            reaction: None,
            draft_id,
            file_id: None,
            exported_entity: None,
            clip_id: None,
            page_id: None,
            is_read: false,
        })
    }
    pub fn create_scheduled_note(
        &self,
        author_id: &str,
        mut input: CreateDraftInput,
        scheduled_at_ms: i64,
    ) -> Result<NoteDraft, PortError> {
        self.accounts.get_owner(author_id)?;
        if self.notes.count_scheduled_drafts(author_id)? >= self.scheduled_note_limit {
            return Err(PortError::Validation("SCHEDULE_NOTE_MAX".to_string()));
        }
        // Upstream validates the schedule time before anything else.
        if scheduled_at_ms <= self.now_ms() {
            return Err(PortError::Validation(
                "CANNOT_CREATE_ALREADY_EXPIRED_SCHEDULE".to_string(),
            ));
        }
        // Poll expiry is judged against the scheduled time, not now.
        if input.has_poll {
            if let Some(expires_at) = input.poll_expires_at_ms {
                if expires_at < scheduled_at_ms {
                    return Err(PortError::Validation(
                        "CANNOT_CREATE_ALREADY_EXPIRED_POLL".to_string(),
                    ));
                }
            }
        }
        // Pure-renote and visibility gates (verbatim Sharkey codes; the
        // draft path defers these to post time).
        if let Some(renote_id) = input.renote_of.as_deref() {
            let target = match self.notes.get_note(renote_id)? {
                Some(note) if !note.is_deleted => note,
                _ => return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string())),
            };
            if Self::is_pure_renote_shape(&target, &self.polls.get_poll(&target.id)?) {
                return Err(PortError::Validation("SHARKEY_CANNOT_RERENOTE".to_string()));
            }
            if target.author_id != author_id
                && matches!(
                    target.visibility,
                    crate::domain::note::Visibility::Followers
                        | crate::domain::note::Visibility::Specified
                )
            {
                return Err(PortError::Validation(
                    "CANNOT_RENOTE_DUE_TO_VISIBILITY".to_string(),
                ));
            }
            if target.author_id != author_id
                && self.blocks.is_blocking(&target.author_id, author_id)?
            {
                return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
            }
        }
        if let Some(reply_id) = input.reply_to.as_deref() {
            let target = match self.notes.get_note(reply_id)? {
                Some(note) if !note.is_deleted => note,
                _ => return Err(PortError::NotFound("NO_SUCH_REPLY_TARGET".to_string())),
            };
            if Self::is_pure_renote_shape(&target, &self.polls.get_poll(&target.id)?) {
                return Err(PortError::Validation(
                    "CANNOT_REPLY_TO_A_PURE_RENOTE".to_string(),
                ));
            }
            if target.author_id != author_id
                && self.blocks.is_blocking(&target.author_id, author_id)?
            {
                return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
            }
        }
        input.is_actually_scheduled = true;
        input.scheduled_at_ms = Some(scheduled_at_ms);
        self.create_draft(author_id, input)
    }

    /// Pure-renote shape for the schedule shim's up-front gates: a renote
    /// carrying no commentary (text, CW, reply, files, or poll).
    fn is_pure_renote_shape(
        note: &crate::domain::note::Note,
        poll: &Option<crate::domain::poll::Poll>,
    ) -> bool {
        note.renote_id.is_some()
            && note.text.is_none()
            && note.cw.is_none()
            && note.reply_id.is_none()
            && note.file_ids.is_empty()
            && poll.is_none()
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
