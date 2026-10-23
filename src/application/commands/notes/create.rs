use crate::application::commands::AppServices;
use crate::application::commands::helpers::NoteSideEffectMode;

use crate::application::events::StreamingEvent;
use crate::application::ports::{CreateNoteInput, EditNoteInput, PortError};
use crate::domain::note::Note;
use crate::domain::notification::NotificationKind;

/// Local custom-emoji reaction shape (`:name:` or `:name@.:`, Misskey
/// `isCustomEmojiRegexp` parity): returns the lowercased shortcode the
/// local registry is keyed by. `:name@other-host:` shapes never originate
/// from local users and are left alone (pre-existing divergence).
pub(crate) fn parse_local_custom_emoji(reaction: &str) -> Option<String> {
    let inner = reaction.strip_prefix(':')?.strip_suffix(':')?;
    if inner.is_empty() {
        return None;
    }
    let name = inner.strip_suffix("@.").unwrap_or(inner);
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '+' || c == '-')
    {
        return None;
    }
    Some(name.to_ascii_lowercase())
}

impl AppServices {
    #[allow(clippy::too_many_lines)]
    pub fn create_note(&self, mut input: CreateNoteInput) -> Result<Note, PortError> {
        self.accounts.get_owner(&input.author_id)?;
        input.uri_base = self.public_url.trim_end_matches('/').to_string();

        // `reply_parent_uri` is inbox-only (`docs/replies.md` Slice B):
        // local replies resolve their target synchronously (or fail with
        // `NO_SUCH_NOTE` below), so a locally authored note is never
        // pending on an unresolved parent.
        if input.reply_parent_uri.is_some() {
            return Err(PortError::Validation(
                "reply_parent_uri is set only by inbound federation".to_string(),
            ));
        }
        // `poll_initial_votes` is inbox-only (§4): local polls always start
        // zeroed; only cached remote polls seed origin-reported tallies.
        if input.poll_initial_votes.is_some() {
            return Err(PortError::Validation(
                "poll_initial_votes is set only by inbound federation".to_string(),
            ));
        }

        // Resolve reply/renote targets and build the reply, renote, and
        // mention `Notify` jobs through the shared application operation
        // (same triple the inbound federation path builds).
        let effects = self.note_interaction_jobs(
            &input.author_id,
            input.reply_to.as_deref(),
            input.renote_of.as_deref(),
            input.text.as_deref(),
            input.cw.as_deref(),
            input.no_extract_mentions,
            NoteSideEffectMode::Local,
        )?;
        let renote_target = effects.renote_target;
        let mut jobs = effects.jobs;

        jobs.push(self.streaming_job(
            &StreamingEvent::NoteCreated {
                note_id: "__PENDING_NOTE_ID__".to_string(),
            },
            format!("note-create:{}:{}", input.author_id, self.ids.generate()),
        )?);

        // Batch N WS4: one bounded `antenna_match` job per created note,
        // committed in the same transaction (`__PENDING_NOTE_ID__` is
        // substituted with the committed id in-txn, same mechanism).
        jobs.push(self.antenna_match_job("__PENDING_NOTE_ID__")?);

        if !input.local_only {
            // MVP fan-out: the worker resolves recipient inboxes at delivery
            // time, so an empty inbox list completes as a Delivered no-op.
            // `__PENDING_NOTE_ID__` is substituted with the committed note id
            // inside the storage transaction (generic string replace in
            // `enqueue_jobs_in_txn`, which also covers structured payloads).
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::NoteCreated {
                    note_id: "__PENDING_NOTE_ID__".into(),
                    author_id: input.author_id.clone(),
                    inbox_override: None,
                }
                .into_job(
                    format!("federate:note:{}:{}", input.author_id, self.ids.generate()),
                    self.now_ms(),
                )?,
            );
        }

        // Quote detection: a renote carrying own commentary (non-blank text,
        // CW, files, or a poll — Misskey `isQuote` parity) is a quote,
        // semantically distinct from a pure renote. The quote relation +
        // the `QuoteRequest` delivery job commit in the same transaction as
        // the note below.
        let is_quote = input.renote_of.is_some()
            && crate::domain::note::is_quote_content(
                input.text.as_deref(),
                input.cw.as_deref(),
                input.reply_to.is_some(),
                input.poll.is_some(),
                input.file_ids.len(),
            );
        if is_quote {
            if let Some(target) = renote_target.as_ref() {
                let base = self.public_url.trim_end_matches('/').to_string();
                let target_uri = target
                    .uri
                    .clone()
                    .unwrap_or_else(|| format!("{base}/notes/{}", target.id));
                let target_author = self.accounts.find_by_id(&target.author_id)?;
                let self_quote = target.author_id == input.author_id;
                let mentioned = target.mentions.iter().any(|m| m == &input.author_id);
                let quote = if self_quote || mentioned {
                    // FEP-044f exceptions: self-quotes and quotes by an
                    // author mentioned in the original post need no stamp.
                    crate::domain::quote::QuoteReference {
                        target_uri,
                        target_note_id: Some(target.id.clone()),
                        authorization:
                            crate::domain::quote::QuoteAuthorizationState::ApprovedByException,
                        authorization_uri: None,
                        request_activity_uri: None,
                        last_verified_at: Some(self.now_ms()),
                    }
                } else {
                    match target_author {
                        Some(author) if author.is_local() => {
                            // Same-server quote: the server is authority for
                            // both sides, so its policy decision is final.
                            // Public/unlisted non-local-only targets
                            // auto-approve (MVP default); anything else stays
                            // Pending (tighter targets are already rejected
                            // by narrowing, so this is a safe fallback,
                            // never approval).
                            let approved = matches!(
                                target.visibility,
                                crate::domain::note::Visibility::Public
                                    | crate::domain::note::Visibility::Home
                            ) && !target.local_only;
                            crate::domain::quote::QuoteReference {
                                target_uri,
                                target_note_id: Some(target.id.clone()),
                                authorization: if approved {
                                    crate::domain::quote::QuoteAuthorizationState::ApprovedByException
                                } else {
                                    crate::domain::quote::QuoteAuthorizationState::Pending
                                },
                                authorization_uri: None,
                                request_activity_uri: None,
                                last_verified_at: if approved { Some(self.now_ms()) } else { None },
                            }
                        }
                        Some(author) => {
                            // Remote author: Pending + QuoteRequest to their
                            // inbox (same transaction as the note). The
                            // request URI is stored only when a delivery job
                            // was actually enqueued (never a dangling ID).
                            let request_row_id = self.ids.generate();
                            let request_uri = format!("{base}/activities/{request_row_id}");
                            let mut enqueued = false;
                            if !input.local_only {
                                if let Some(inbox) =
                                    crate::federation::outbox::preferred_inbox(&author)
                                {
                                    jobs.push(
                                        crate::federation::outbox::FederateDeliverPayload::QuoteRequest {
                                            request_id: request_row_id,
                                            quote_note_id: "__PENDING_NOTE_ID__".into(),
                                            quoter_account_id: input.author_id.clone(),
                                            quoted_uri: target_uri.clone(),
                                            target_actor_uri: author.actor_url(&base),
                                            inbox_url: inbox,
                                        }
                                        .into_job(
                                            format!(
                                                "federate:quote-request:{}:{}",
                                                input.author_id,
                                                self.ids.generate()
                                            ),
                                            self.now_ms(),
                                        )?,
                                    );
                                    enqueued = true;
                                }
                            }
                            crate::domain::quote::QuoteReference::pending(
                                target_uri,
                                Some(target.id.clone()),
                                enqueued.then(|| request_uri),
                            )
                        }
                        None => crate::domain::quote::QuoteReference::pending(
                            target_uri,
                            Some(target.id.clone()),
                            None,
                        ),
                    }
                };
                input.quote = Some(quote);
            }
        }

        let mut input_with_instance_words = input;
        input_with_instance_words.prohibited_words = self.prohibited_words.clone();
        input_with_instance_words.sensitive_words = self.sensitive_words.clone();
        // Polls (§4): normalize the flag, validate against the server
        // clock, and arm the delayed expiry job (committed atomically with
        // the note via the `__PENDING_NOTE_ID__` substitution).
        input_with_instance_words.has_poll = input_with_instance_words.poll.is_some();
        if let Some(poll) = input_with_instance_words.poll.clone() {
            poll.validate(self.now_ms()).map_err(|e| match e {
                crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
                crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
                crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
                crate::domain::id::DomainError::StorageUnavailable(msg) => {
                    PortError::StorageUnavailable(msg)
                }
            })?;
            if let Some(expiry_job) = self.poll_expiry_job("__PENDING_NOTE_ID__", &poll)? {
                jobs.push(expiry_job);
            }
        }
        self.notes.create_note(input_with_instance_words, jobs)
    }

    pub fn delete_note(&self, actor_id: &str, note_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(actor_id)?;
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        if note.author_id != actor_id && !self.viewer_is_moderator(Some(actor_id))? {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        let base = self.public_url.trim_end_matches('/');
        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::NoteDeleted {
                note_id: note_id.to_string(),
            },
            format!("note-delete:{note_id}:{}", self.ids.generate()),
        )?];
        if !note.local_only {
            let note_uri = note
                .uri
                .clone()
                .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
            // A pure renote (no text/CW/files) federates as Undo(Announce)
            // of the renote target rather than Delete(Tombstone).
            let was_announce_of_uri = if note.renote_id.is_some()
                && note.text.is_none()
                && note.cw.is_none()
                && note.file_ids.is_empty()
            {
                match &note.renote_id {
                    Some(renote_id) => match self.notes.get_note(renote_id)? {
                        Some(target) => Some(
                            target
                                .uri
                                .clone()
                                .unwrap_or_else(|| format!("{base}/notes/{}", target.id)),
                        ),
                        None => None,
                    },
                    None => None,
                }
            } else {
                None
            };
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::NoteDeleted {
                    note_id: note.id.clone(),
                    author_id: actor_id.to_string(),
                    note_uri,
                    was_announce_of_uri,
                    inbox_override: None,
                }
                .into_job(
                    format!("federate:delete:{note_id}:{}", self.ids.generate()),
                    self.now_ms(),
                )?,
            );
        }
        self.notes.delete_note(note_id, jobs)
    }

    /// Edit a note (§8; Sharkey `NoteEditService.edit` parity). Author-only
    /// (`NOT_LOCAL_USER` when the editor is remote —
    /// `apply_note_edit_in_txn` re-checks authorship atomically);
    /// `reply_id`, `visibility`, and `local_only` are forced back to the
    /// original note's values inside storage. Side effects reuse the create
    /// path: a note-stream `updated` event, an `edited` notification to the
    /// reply author, and an `Update(Note)` federated deliver (followers +
    /// mentioned/replied/renoted/reacted remote users). A no-change edit
    /// returns the original note and enqueues nothing.
    pub fn edit_note(&self, mut input: EditNoteInput) -> Result<Note, PortError> {
        self.accounts.get_owner(&input.editor_id)?;
        let editor = self
            .accounts
            .find_by_id(&input.editor_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        if editor.host.is_some() {
            return Err(PortError::Validation("NOT_LOCAL_USER".to_string()));
        }
        let old = match self.notes.get_note(&input.note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        if old.author_id != input.editor_id {
            return Err(PortError::Validation("YOU_ARE_NOT_THE_AUTHOR".to_string()));
        }
        input.prohibited_words = self.prohibited_words.clone();
        input.sensitive_words = self.sensitive_words.clone();
        input.uri_base = self.public_url.trim_end_matches('/').to_string();
        let old_poll = self.polls.get_poll(&old.id)?;
        let jobs = self.edit_side_effect_jobs(
            &old,
            &input.editor_id,
            &input.poll,
            old_poll.as_ref(),
            true,
        )?;
        self.notes.apply_note_edit(input, jobs)
    }

    /// Jobs shared by the local and inbound edit paths: the `updated`
    /// streaming event, the `edited` notification to the reply author, and
    /// (local path only) the `Update(Note)` federated deliver plus a
    /// re-armed poll-expiry job when the edit leaves an expiring poll. The
    /// old poll row decides the re-arm: an unchanged poll keeps its
    /// existing arming; a removed poll lets the old job converge quietly.
    pub(crate) fn edit_side_effect_jobs(
        &self,
        old: &Note,
        editor_id: &str,
        new_poll: &Option<crate::domain::poll::PollInput>,
        old_poll: Option<&crate::domain::poll::Poll>,
        federate: bool,
    ) -> Result<Vec<crate::application::ports::NewJob>, PortError> {
        let mut jobs = Vec::new();
        jobs.push(self.streaming_job(
            &StreamingEvent::NoteUpdated {
                note_id: old.id.clone(),
            },
            format!("note-edit:{}:{}", old.id, self.ids.generate()),
        )?);
        // `edited` goes to the reply author only (Sharkey
        // `postNoteEdited` parity — new mentions and renote targets are not
        // notified on edit). The `Notify` worker gate drops self-notifies,
        // mutes, and receive-config denials.
        if let Some(reply_id) = old.reply_id.as_deref() {
            if let Some(parent) = self.notes.get_note(reply_id)? {
                if parent.author_id != editor_id {
                    jobs.push(self.notify_job(
                        &parent.author_id,
                        editor_id,
                        NotificationKind::Edited,
                        Some(old.id.clone()),
                        None,
                        "edit",
                    )?);
                }
            }
        }
        if federate && !old.local_only {
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::NoteUpdated {
                    note_id: old.id.clone(),
                    author_id: old.author_id.clone(),
                    inbox_override: None,
                }
                .into_job(
                    format!("federate:note-update:{}:{}", old.id, self.ids.generate()),
                    self.now_ms(),
                )?,
            );
        }
        let poll_changed = match (old_poll, new_poll.as_ref()) {
            (None, None) => false,
            (Some(_), None) | (None, Some(_)) => true,
            (Some(old_poll), Some(new_poll)) => {
                old_poll.choices != new_poll.choices
                    || old_poll.multiple != new_poll.multiple
                    || old_poll.expires_at_ms != new_poll.expires_at_ms
            }
        };
        if poll_changed {
            if let Some(poll) = new_poll.as_ref() {
                // The storage transaction re-validates; a `None` here only
                // means the poll never expires (no arming needed).
                if let Some(expiry_job) = self.poll_expiry_job(&old.id, poll)? {
                    jobs.push(expiry_job);
                }
            }
        }
        Ok(jobs)
    }

    pub fn unrenote(&self, actor_id: &str, note_id: &str) -> Result<usize, PortError> {
        self.accounts.get_owner(actor_id)?;
        let target = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        // All of the caller's live renotes of the target (pure renotes and
        // quotes alike, mirroring Misskey `notes/unrenote`).
        let own_renotes: Vec<Note> = self
            .notes
            .list_all_notes(&crate::application::ports::TimelineCursor::with_limit(
                10_000,
            ))?
            .into_iter()
            .filter(|note| {
                note.author_id == actor_id
                    && !note.is_deleted
                    && note.renote_id.as_deref() == Some(target.id.as_str())
            })
            .collect();
        let count = own_renotes.len();
        for renote in own_renotes {
            // The existing delete path keeps streaming + federation jobs.
            self.delete_note(actor_id, &renote.id)?;
        }
        Ok(count)
    }
}
