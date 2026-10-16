use std::collections::HashSet;

use super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::{CreateNoteInput, PortError};
use crate::domain::note::{Note, extract_mentions};
use crate::domain::notification::NotificationKind;
use crate::domain::policy::{ViewerContext, is_visible};
use crate::domain::reaction::{Reaction, coerce_reaction};

impl AppServices {
    #[allow(clippy::too_many_lines)]
    pub fn create_note(&self, mut input: CreateNoteInput) -> Result<Note, PortError> {
        self.accounts.get_owner(&input.author_id)?;

        // `reply_parent_uri` is inbox-only (`docs/replies.md` Slice B):
        // local replies resolve their target synchronously (or fail with
        // `NO_SUCH_NOTE` below), so a locally authored note is never
        // pending on an unresolved parent.
        if input.reply_parent_uri.is_some() {
            return Err(PortError::Validation(
                "reply_parent_uri is set only by inbound federation".to_string(),
            ));
        }

        // Resolve reply/renote targets so we know who to notify.
        let reply_target = match input.reply_to.as_deref() {
            None => None,
            Some(id) => match self.notes.get_note(id)? {
                // Missing and invisible targets are indistinguishable so a
                // reply cannot probe for the existence of hidden notes (D8).
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
                Some(note) => {
                    let follows_author = note.author_id == input.author_id
                        || self.follows.is_following(&input.author_id, &note.author_id)?;
                    let viewer = ViewerContext::local_user(&input.author_id, follows_author);
                    if note.is_deleted || !is_visible(&note, &viewer) {
                        return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
                    }
                    Some(note)
                }
            },
        };
        let renote_target = match input.renote_of.as_deref() {
            None => None,
            Some(id) => match self.notes.get_note(id)? {
                None => {
                    return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()));
                }
                Some(note) if note.is_deleted => {
                    return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()));
                }
                Some(note) => Some(note),
            },
        };

        let mut jobs = Vec::new();
        let mut already_notified: HashSet<String> = HashSet::new();

        if let Some(target) = reply_target.as_ref() {
            if target.author_id != input.author_id {
                already_notified.insert(target.author_id.clone());
                jobs.push(self.notify_job(
                    &target.author_id,
                    &input.author_id,
                    NotificationKind::Reply,
                    Some("__PENDING_NOTE_ID__".to_string()),
                    None,
                    "reply",
                )?);
            }
        }
        if let Some(target) = renote_target.as_ref() {
            if target.author_id != input.author_id
                && !already_notified.contains(&target.author_id)
            {
                already_notified.insert(target.author_id.clone());
                jobs.push(self.notify_job(
                    &target.author_id,
                    &input.author_id,
                    NotificationKind::Renote,
                    Some("__PENDING_NOTE_ID__".to_string()),
                    None,
                    "renote",
                )?);
            }
        }

        // `@username` mentions (local only) become Mention notifications.
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
                if let Some(mentioned) = self.accounts.find_by_handle(&lower)? {
                    if mentioned.id != input.author_id
                        && !already_notified.contains(&mentioned.id)
                    {
                        already_notified.insert(mentioned.id.clone());
                        jobs.push(self.notify_job(
                            &mentioned.id,
                            &input.author_id,
                            NotificationKind::Mention,
                            Some("__PENDING_NOTE_ID__".to_string()),
                            None,
                            "mention",
                        )?);
                    }
                }
            }
        }

        jobs.push(self.streaming_job(
            &StreamingEvent::NoteCreated {
                note_id: "__PENDING_NOTE_ID__".to_string(),
            },
            format!("note-create:{}:{}", input.author_id, self.ids.generate()),
        )?);

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
                format!(
                    "federate:note:{}:{}",
                    input.author_id,
                    self.ids.generate()
                ),
                self.now_ms(),
            )?,
        );
        }

        // Quote detection: a renote carrying own commentary (non-blank text,
        // CW, or files) is a quote, semantically distinct from a pure
        // renote. The quote relation + the `QuoteRequest` delivery job
        // commit in the same transaction as the note below.
        let has_commentary = input
            .text
            .as_deref()
            .map(|t| !t.trim().is_empty())
            .unwrap_or(false)
            || input.cw.is_some()
            || !input.file_ids.is_empty();
        let is_quote = input.renote_of.is_some() && has_commentary;
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
                                last_verified_at: if approved {
                                    Some(self.now_ms())
                                } else {
                                    None
                                },
                            }
                        }
                        Some(author) => {
                            // Remote author: Pending + QuoteRequest to their
                            // inbox (same transaction as the note). The
                            // request URI is stored only when a delivery job
                            // was actually enqueued (never a dangling ID).
                            let request_row_id = self.ids.generate();
                            let request_uri =
                                format!("{base}/activities/{request_row_id}");
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
        self.notes
            .create_note(input_with_instance_words, jobs)
    }

    pub fn delete_note(&self, actor_id: &str, note_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(actor_id)?;
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        if note.author_id != actor_id {
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
            let was_announce_of_uri =
                if note.renote_id.is_some() && note.text.is_none() && note.cw.is_none() && note
                    .file_ids
                    .is_empty()
                {
                    match &note.renote_id {
                        Some(renote_id) => match self.notes.get_note(renote_id)? {
                            Some(target) => Some(target.uri.clone().unwrap_or_else(|| {
                                format!("{base}/notes/{}", target.id)
                            })),
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
            .list_all_notes(&crate::application::ports::TimelineCursor::with_limit(10_000))?
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

    pub fn react(
        &self,
        user_id: &str,
        note_id: &str,
        reaction: &str,
        upsert: bool,
    ) -> Result<Reaction, PortError> {
        self.accounts.get_owner(user_id)?;
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        let follows_author = if user_id == note.author_id {
            true
        } else {
            self.follows.is_following(user_id, &note.author_id)?
        };
        let viewer = ViewerContext::local_user(user_id, follows_author);
        if !is_visible(&note, &viewer) {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        if note.renote_id.is_some()
            && note.text.is_none()
            && note.cw.is_none()
            && note.file_ids.is_empty()
        {
            return Err(PortError::Validation(
                "CANNOT_REACT_TO_RENOTE".to_string(),
            ));
        }
        let coerced = coerce_reaction(reaction, note.reaction_acceptance, false);
        let mut jobs = Vec::new();
        jobs.push(self.streaming_job(
            &StreamingEvent::NoteReacted {
                note_id: note_id.to_string(),
                user_id: user_id.to_string(),
                reaction: coerced.clone(),
            },
            format!("react:{user_id}:{note_id}:{}", self.ids.generate()),
        )?);
        if note.author_id != user_id {
            jobs.push(self.notify_job(
                &note.author_id,
                user_id,
                NotificationKind::Reaction,
                Some(note_id.to_string()),
                Some(coerced.clone()),
                "reaction",
            )?);
        }
        // Federate the Like when the note author is remote.
        if let Some(author) = self.accounts.find_by_id(&note.author_id)? {
            if !author.is_local() {
                if let Some(inbox_url) =
                    crate::federation::outbox::preferred_inbox(&author)
                {
                    let base = self.public_url.trim_end_matches('/');
                    let reaction_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::Like {
                            reaction_id: reaction_id.clone(),
                            actor_id: user_id.to_string(),
                            target_note_uri: note.uri.clone().unwrap_or_else(|| {
                                format!("{base}/notes/{note_id}")
                            }),
                            reaction: coerced.clone(),
                            inbox_url,
                        }
                        .into_job(
                            format!("federate:like:{user_id}:{note_id}:{reaction_id}"),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        self.reactions
            .react(user_id, note_id, &coerced, upsert, jobs)
    }

    pub fn unreact(&self, user_id: &str, note_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let existing = self.reactions.get_reaction(user_id, note_id)?;
        let reaction = existing
            .as_ref()
            .map(|r| r.reaction.clone())
            .unwrap_or_default();
        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::NoteUnreacted {
                note_id: note_id.to_string(),
                user_id: user_id.to_string(),
                reaction,
            },
            format!("unreact:{user_id}:{note_id}:{}", self.ids.generate()),
        )?];
        // Federate Undo(Like) when the note author is remote.
        if let Some(note) = self.notes.get_note(note_id)? {
            if let Some(author) = self.accounts.find_by_id(&note.author_id)? {
                if !author.is_local() {
                    if let Some(inbox_url) =
                        crate::federation::outbox::preferred_inbox(&author)
                    {
                        let base = self.public_url.trim_end_matches('/');
                        let reaction_id = existing
                            .as_ref()
                            .map(|r| r.id.clone())
                            .unwrap_or_else(|| self.ids.generate());
                        let undo_id = self.ids.generate();
                        // Best-effort: when no reaction row exists (storage
                        // reports NOT_REACTED) fall back to the Like-only
                        // heart for the Undo(Like) object.
                        let reaction_str = existing
                            .as_ref()
                            .map(|r| r.reaction.clone())
                            .unwrap_or_else(|| "❤".to_string());
                        jobs.push(
                            crate::federation::outbox::FederateDeliverPayload::UndoLike {
                                undo_id: undo_id.clone(),
                                reaction_id,
                                actor_id: user_id.to_string(),
                                target_note_uri: note.uri.clone().unwrap_or_else(|| {
                                    format!("{base}/notes/{note_id}")
                                }),
                                reaction: reaction_str,
                                inbox_url,
                            }
                            .into_job(
                                format!("federate:unlike:{user_id}:{note_id}:{undo_id}"),
                                self.now_ms(),
                            )?,
                        );
                    }
                }
            }
        }
        self.reactions.unreact(user_id, note_id, jobs)
    }
}
