use std::collections::HashSet;

use super::super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::{CreateNoteInput, EditNoteInput, NewJob, PortError};
use crate::domain::note::{Note, extract_mentions};
use crate::domain::notification::NotificationKind;
use crate::domain::poll::PollInput;
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
        // Instance block (WP5): drop before any state write or dedup record.
        if self.inbound_host_blocked(&input.author_id) {
            return Ok(None);
        }
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
                    if mentioned.id != input.author_id && !already_notified.contains(&mentioned.id)
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
        // Batch N WS4: antenna fan-in for federated notes too (same txn).
        jobs.push(self.antenna_match_job("__PENDING_NOTE_ID__")?);
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
            format!("inbound-note-delete:{note_id}:{}", self.ids.generate()),
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

    /// Record an inbound `Delete` for a note URI this node does not hold
    /// (bench S6, 2026-10-17). Writes a tombstone so a later `Create` or fetch
    /// of the URI cannot resurrect the note. The caller authorises the deleter
    /// first. If the note appeared between the caller's lookup and this write,
    /// it is deleted in the same transaction. That path emits no streaming
    /// event, because the caller does not know the note id in advance.
    pub fn handle_inbound_tombstone_uri(
        &self,
        activity_id: &str,
        object_uri: &str,
    ) -> Result<bool, PortError> {
        match self
            .dedup
            .record_delete_uri_activity(activity_id, object_uri, Vec::new())
        {
            Ok(v) => Ok(v),
            Err(PortError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Update(Note|Article|Page)` (§8; Sharkey
    /// `ApNoteService.updateNote` parity): apply a remote author's edit to
    /// the cached local copy through the same storage path as local edits.
    /// `renote_of` is the inbox-resolved quote-target note id (`None` =
    /// detach/absent — the reply link is always kept, the audience/files
    /// are never touched by federation input). Unknown notes, locally
    /// authored copies, suspended authors, and attribution mismatches
    /// converge to `Ok(None)` (fail-closed); a deletion race maps to
    /// `Ok(None)`. Replays converge to `Ok(None)` via the dedup mark.
    pub fn handle_inbound_update_note(
        &self,
        activity_id: &str,
        note_uri: &str,
        actor_uri: &str,
        text: Option<String>,
        cw: Option<String>,
        renote_of: Option<String>,
        poll: Option<PollInput>,
    ) -> Result<Option<Note>, PortError> {
        let Some(cached) = self.notes.find_by_uri(note_uri)? else {
            return Ok(None);
        };
        if cached.is_deleted {
            return Ok(None);
        }
        let Some(author) = self.accounts.find_by_id(&cached.author_id)? else {
            return Ok(None);
        };
        // Never rewrite a local note from federation input (same rule as
        // the `Update(Person)` actor path).
        if author.is_local() {
            return Ok(None);
        }
        if author.is_suspended {
            return Ok(None);
        }
        // Authority: the signer must be the cached copy's author (Misskey
        // `attribution === user.uri` parity).
        let base = self.public_url.trim_end_matches('/');
        let owner_uri = author.uri.clone().unwrap_or_else(|| author.actor_url(base));
        if owner_uri != actor_uri {
            return Ok(None);
        }
        let old_poll = self.polls.get_poll(&cached.id)?;
        let input = EditNoteInput {
            editor_id: author.id.clone(),
            note_id: cached.id.clone(),
            text,
            cw,
            renote_of,
            // Files and the audience are never touched by federation input
            // (Sharkey resolves attachments off-request; pinetree keeps the
            // cached values — documented divergence).
            file_ids: cached.file_ids.clone(),
            poll,
            reaction_acceptance: cached.reaction_acceptance,
            visible_users: cached.visible_user_ids.clone(),
            prohibited_words: self.prohibited_words.clone(),
            sensitive_words: self.sensitive_words.clone(),
            uri_base: base.to_string(),
        };
        let jobs =
            self.edit_side_effect_jobs(&cached, &author.id, &input.poll, old_poll.as_ref(), false)?;
        match self
            .dedup
            .record_note_edit_activity(activity_id, input, jobs)
        {
            Ok(note) => Ok(note),
            Err(PortError::NotFound(msg)) if msg == "NO_SUCH_NOTE" => Ok(None),
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
        if self.inbound_host_blocked(user_id) {
            return Ok(None);
        }
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
            format!("inbound-react:{user_id}:{note_id}:{}", self.ids.generate()),
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
        match self
            .dedup
            .record_react_activity(activity_id, user_id, note_id, &coerced, true, jobs)
        {
            Ok(v) => Ok(v),
            Err(PortError::Conflict(_)) => Ok(None),
            // §0A: the note author blocks the reactor — skip instead of
            // failing the inbox delivery.
            Err(PortError::Validation(msg)) if msg == "YOU_HAVE_BEEN_BLOCKED" => Ok(None),
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
