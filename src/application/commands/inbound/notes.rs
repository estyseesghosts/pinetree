use std::collections::HashSet;

use super::super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::{CreateNoteInput, NewJob, PortError};
use crate::domain::note::{Note, extract_mentions};
use crate::domain::notification::NotificationKind;
use crate::domain::reaction::{Reaction, coerce_reaction};

impl AppServices {
    /// Record an inbound `Create(Note)` with best-effort reply/renote/mention
    /// notifications plus a streaming job. Replay returns `Ok(None)`.
    /// `extra_jobs` (e.g. a `RemoteMediaFetch` job built by the inbox) are
    /// committed atomically with the note, so a replay enqueues nothing.
    pub fn handle_inbound_create_note(
        &self,
        activity_id: &str,
        input: CreateNoteInput,
        extra_jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let mut jobs = Vec::new();
        // One `Notify` per user (mirrors the local `create_note` dedup):
        // a remote note that is both a reply and a mention must not emit
        // two `notification` frames.
        let mut already_notified: HashSet<String> = HashSet::new();
        if let Some(reply_id) = input.reply_to.as_deref() {
            if let Ok(Some(target)) = self.notes.get_note(reply_id) {
                if target.author_id != input.author_id {
                    already_notified.insert(target.author_id.clone());
                    if let Ok(job) = self.notify_job(
                        &target.author_id,
                        &input.author_id,
                        NotificationKind::Reply,
                        Some("__PENDING_NOTE_ID__".to_string()),
                        None,
                        "inbound-reply",
                    ) {
                        jobs.push(job);
                    }
                }
            }
        }
        if let Some(renote_id) = input.renote_of.as_deref() {
            if let Ok(Some(target)) = self.notes.get_note(renote_id) {
                if target.author_id != input.author_id
                    && !already_notified.contains(&target.author_id)
                {
                    already_notified.insert(target.author_id.clone());
                    if let Ok(job) = self.notify_job(
                        &target.author_id,
                        &input.author_id,
                        NotificationKind::Renote,
                        Some("__PENDING_NOTE_ID__".to_string()),
                        None,
                        "inbound-renote",
                    ) {
                        jobs.push(job);
                    }
                }
            }
        }
        if !input.no_extract_mentions {
            let mut text = input.text.clone().unwrap_or_default();
            if let Some(cw) = input.cw.as_ref() {
                text.push('\n');
                text.push_str(cw);
            }
            let mut seen: HashSet<String> = HashSet::new();
            for username in extract_mentions(&text) {
                let lower = username.to_lowercase();
                if !seen.insert(lower.clone()) {
                    continue;
                }
                if let Ok(Some(mentioned)) = self.accounts.find_by_handle(&lower) {
                    if mentioned.id != input.author_id
                        && !already_notified.contains(&mentioned.id)
                    {
                        already_notified.insert(mentioned.id.clone());
                        if let Ok(job) = self.notify_job(
                            &mentioned.id,
                            &input.author_id,
                            NotificationKind::Mention,
                            Some("__PENDING_NOTE_ID__".to_string()),
                            None,
                            "inbound-mention",
                        ) {
                            jobs.push(job);
                        }
                    }
                }
            }
        }
        jobs.push(self.streaming_job(
            &StreamingEvent::NoteCreated {
                note_id: "__PENDING_NOTE_ID__".to_string(),
            },
            format!(
                "inbound-note-create:{}:{}",
                input.author_id,
                self.ids.generate()
            ),
        )?);
        jobs.extend(extra_jobs);
        let mut input_with_words = input;
        input_with_words.prohibited_words = self.prohibited_words.clone();
        input_with_words.sensitive_words = self.sensitive_words.clone();
        self.dedup
            .record_note_activity(activity_id, input_with_words, jobs)
    }

    /// Record an inbound `Delete(Tombstone)` / `Undo(Announce)`. A missing
    /// note maps to `Ok(false)`.
    pub fn handle_inbound_delete_note(
        &self,
        activity_id: &str,
        note_id: &str,
    ) -> Result<bool, PortError> {
        let job = self.streaming_job(
            &StreamingEvent::NoteDeleted {
                note_id: note_id.to_string(),
            },
            format!(
                "inbound-note-delete:{note_id}:{}",
                self.ids.generate()
            ),
        )?;
        match self
            .dedup
            .record_delete_note_activity(activity_id, note_id, vec![job])
        {
            Ok(v) => Ok(v),
            Err(PortError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Like`. A commit race surfacing as `Conflict` maps
    /// to `Ok(None)`.
    pub fn handle_inbound_like(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        reaction: &str,
    ) -> Result<Option<Reaction>, PortError> {
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        let coerced = coerce_reaction(reaction, note.reaction_acceptance, false);
        let mut jobs = Vec::new();
        jobs.push(self.streaming_job(
            &StreamingEvent::NoteReacted {
                note_id: note_id.to_string(),
                user_id: user_id.to_string(),
                reaction: coerced.clone(),
            },
            format!(
                "inbound-react:{user_id}:{note_id}:{}",
                self.ids.generate()
            ),
        )?);
        if note.author_id != user_id {
            jobs.push(self.notify_job(
                &note.author_id,
                user_id,
                NotificationKind::Reaction,
                Some(note_id.to_string()),
                Some(coerced.clone()),
                "inbound-reaction",
            )?);
        }
        match self.dedup.record_react_activity(
            activity_id,
            user_id,
            note_id,
            &coerced,
            true,
            jobs,
        ) {
            Ok(v) => Ok(v),
            Err(PortError::Conflict(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Undo(Like)`. A missing reaction surfaces as
    /// `Validation(NOT_REACTED)` and maps to `Ok(false)`.
    pub fn handle_inbound_unlike(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
    ) -> Result<bool, PortError> {
        let reaction = self
            .reactions
            .get_reaction(user_id, note_id)?
            .map(|r| r.reaction)
            .unwrap_or_default();
        let job = self.streaming_job(
            &StreamingEvent::NoteUnreacted {
                note_id: note_id.to_string(),
                user_id: user_id.to_string(),
                reaction,
            },
            format!(
                "inbound-unreact:{user_id}:{note_id}:{}",
                self.ids.generate()
            ),
        )?;
        match self
            .dedup
            .record_unreact_activity(activity_id, user_id, note_id, vec![job])
        {
            Ok(v) => Ok(v),
            Err(PortError::Validation(msg)) if msg.contains("NOT_REACTED") => Ok(false),
            Err(e) => Err(e),
        }
    }
}
