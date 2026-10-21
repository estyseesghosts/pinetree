use std::collections::HashSet;

use super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::{CreateNoteInput, EditNoteInput, PortError};
use crate::domain::note::{Note, extract_mentions};
use crate::domain::notification::NotificationKind;
use crate::domain::policy::{ViewerContext, is_visible};
use crate::domain::reaction::{FALLBACK_REACTION, Reaction, coerce_reaction, normalize_reaction};

/// Local custom-emoji reaction shape (`:name:` or `:name@.:`, Misskey
/// `isCustomEmojiRegexp` parity): returns the lowercased shortcode the
/// local registry is keyed by. `:name@other-host:` shapes never originate
/// from local users and are left alone (pre-existing divergence).
fn parse_local_custom_emoji(reaction: &str) -> Option<String> {
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
        // Polls (§4): normalize the flag, validate against the server
        // clock, and arm the delayed expiry job (committed atomically with
        // the note via the `__PENDING_NOTE_ID__` substitution).
        input_with_instance_words.has_poll = input_with_instance_words.poll.is_some();
        if let Some(poll) = input_with_instance_words.poll.clone() {
            poll.validate(self.now_ms()).map_err(|e| match e {
                crate::domain::id::DomainError::Validation(msg) => {
                    PortError::Validation(msg)
                }
                crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
                crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
                crate::domain::id::DomainError::StorageUnavailable(msg) => {
                    PortError::StorageUnavailable(msg)
                }
            })?;
            if let Some(expiry_job) =
                self.poll_expiry_job("__PENDING_NOTE_ID__", &poll)?
            {
                jobs.push(expiry_job);
            }
        }
        self.notes
            .create_note(input_with_instance_words, jobs)
    }

    pub fn delete_note(&self, actor_id: &str, note_id: &str) -> Result<(), PortError> {        self.accounts.get_owner(actor_id)?;
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
            return Err(PortError::Validation(
                "YOU_ARE_NOT_THE_AUTHOR".to_string(),
            ));
        }
        input.prohibited_words = self.prohibited_words.clone();
        input.sensitive_words = self.sensitive_words.clone();
        input.uri_base = self.public_url.trim_end_matches('/').to_string();
        let old_poll = self.polls.get_poll(&old.id)?;
        let jobs = self.edit_side_effect_jobs(&old, &input.editor_id, &input.poll, old_poll.as_ref(), true)?;
        self.notes.apply_note_edit(input, jobs)
    }

    /// Jobs shared by the local and inbound edit paths: the `updated`
    /// streaming event, the `edited` notification to the reply author, and
    /// (local path only) the `Update(Note)` federated deliver plus a
    /// re-armed poll-expiry job when the edit leaves an expiring poll. The
    /// old poll row decides the re-arm: an unchanged poll keeps its
    /// existing arming; a removed poll lets the old job converge quietly.
    pub(super) fn edit_side_effect_jobs(
        &self,        old: &Note,
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

    /// Heart-click entry point (§6 DIVERGENCE, decided): `None` (absent/null
    /// `reaction` param) resolves server-side to the override/`defaultReaction`/
    /// instance-`default_like`/❤ chain ([`Self::resolve_like_emoji`]), then runs
    /// the exact `react` pipeline below — so a stored default goes through
    /// `normalize_reaction` too (`"❤️"` stored → `"❤"` applied; custom
    /// `":name:"` defaults pass through the registry gate) and
    /// `coerce_reaction` still wins on like-only notes. Explicit reactions
    /// behave byte-identically to [`AppServices::react`].
    pub fn react_with_default(
        &self,
        user_id: &str,
        note_id: &str,
        reaction: Option<&str>,
        upsert: bool,
    ) -> Result<Reaction, PortError> {
        let resolved;
        let reaction = match reaction {
            Some(r) => r,
            None => {
                resolved = self.resolve_like_emoji(user_id, None)?;
                &resolved
            }
        };
        self.react(user_id, note_id, reaction, upsert)
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
        // §0A: block check first, then visibility (Misskey
        // `ReactionService.create` parity). The storage transaction
        // re-checks atomically; this keeps the error precedence identical
        // on the local path.
        if note.author_id != user_id && self.blocks.is_blocking(&note.author_id, user_id)? {
            return Err(PortError::Validation(
                "YOU_HAVE_BEEN_BLOCKED".to_string(),
            ));
        }
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
        let coerced = self.resolve_reaction_for_note(user_id, &note, reaction)?;
        // One id for the reaction row and the federated Like, so `unreact`'s
        // Undo(Like) names the activity the remote server received.
        let reaction_id = self.ids.generate();
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
            .react(user_id, note_id, &coerced, &reaction_id, upsert, jobs)
    }

    /// Full reaction-string resolution for one note (§6 wire compat +
    /// custom-emoji gate): `likeOnly` coercion, legacy/FE0F normalization,
    /// then [`Self::resolve_custom_emoji_reaction`]. Shared by [`Self::react`]
    /// and [`Self::like_note`] so both store byte-identical strings.
    fn resolve_reaction_for_note(
        &self,
        user_id: &str,
        note: &Note,
        reaction: &str,
    ) -> Result<String, PortError> {
        let _ = user_id;
        let coerced = coerce_reaction(reaction, note.reaction_acceptance, false);
        // §6 wire compat (Misskey `ReactionService` parity): `likeOnly`
        // already forced the bare-heart fallback above; every other
        // reaction is normalized (legacies map, FE0F strip unless ZWJ).
        let coerced = if coerced == reaction {
            normalize_reaction(Some(reaction))
        } else {
            coerced
        };
        // Custom-emoji permission gate (Misskey `ReactionService.create`
        // parity for the local `:name:` / `:name@.:` shapes): unknown names,
        // role-gated rows (no role subsystem — permission unprovable),
        // `localOnly` on remote notes, and sensitive rows on
        // `nonSensitive*` notes all fall back to ❤ (never an error).
        // `:name@other-host:` shapes never originate locally; they keep the
        // pre-existing verbatim divergence.
        self.resolve_custom_emoji_reaction(&coerced, note)
    }

    /// Resolve the implicit like emoji: explicit `override` first, then the
    /// reactor's `defaultReaction`, then the instance `default_like` config
    /// key, then the bare heart (Sharkey/Catodon `?? serverSettings.defaultLike`
    /// parity, with the pinetree per-user setting taking precedence over the
    /// instance one). The result still passes through the reaction gate in
    /// [`Self::react`], so unusable custom names fall back to ❤ there.
    fn resolve_like_emoji(
        &self,
        user_id: &str,
        override_emoji: Option<&str>,
    ) -> Result<String, PortError> {
        if let Some(r) = override_emoji {
            return Ok(r.to_string());
        }
        if let Some(account) = self.accounts.find_by_id(user_id)? {
            if let Some(default) = account.default_reaction {
                return Ok(default);
            }
        }
        Ok(self.default_like.clone().unwrap_or_else(|| FALLBACK_REACTION.to_string()))
    }

    /// `notes/like` (Sharkey/Catodon parity): react with
    /// [`Self::resolve_like_emoji`]. Sharkey toggle semantics: re-pressing
    /// with the reaction that is already stored removes it (`Ok(false)`);
    /// pressing with a different emoji replaces the stored one through the
    /// upsert path (`Ok(true)`). Never returns `ALREADY_REACTED`.
    pub fn like_note(
        &self,
        user_id: &str,
        note_id: &str,
        override_emoji: Option<&str>,
    ) -> Result<bool, PortError> {
        match self.react_with_default(user_id, note_id, override_emoji, false) {
            Ok(_) => Ok(true),
            Err(PortError::Conflict(code)) if code == "ALREADY_REACTED" => {
                let note = match self.notes.get_note(note_id)? {
                    Some(note) if !note.is_deleted => note,
                    _ => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
                };
                let requested = self.resolve_like_emoji(user_id, override_emoji)?;
                let requested =
                    self.resolve_reaction_for_note(user_id, &note, &requested)?;
                let existing = self
                    .reactions
                    .get_reaction(user_id, note_id)?
                    .map(|row| row.reaction);
                if existing.as_deref() == Some(requested.as_str()) {
                    self.unreact(user_id, note_id)?;
                    Ok(false)
                } else {
                    self.react_with_default(user_id, note_id, override_emoji, true)?;
                    Ok(true)
                }
            }
            Err(other) => Err(other),
        }
    }

    /// Misskey `ReactionService.create` custom-emoji branch parity (local
    /// `:name:` / `:name@.:` shapes only): resolve against the local emoji
    /// registry, falling back to [`FALLBACK_REACTION`] on unknown names,
    /// role-gated rows (pinetree has no role subsystem, so the permission
    /// is unprovable), `localOnly` rows on remote notes, and sensitive rows
    /// on `nonSensitive*` notes. Never errors.
    fn resolve_custom_emoji_reaction(
        &self,
        reaction: &str,
        note: &Note,
    ) -> Result<String, PortError> {
        let name = match parse_local_custom_emoji(reaction) {
            Some(name) => name,
            None => return Ok(reaction.to_string()),
        };
        let Some(emoji) = self.emojis.get_emoji_by_name(&name)? else {
            return Ok(FALLBACK_REACTION.to_string());
        };
        if !emoji.role_ids.is_empty() {
            return Ok(FALLBACK_REACTION.to_string());
        }
        if emoji.local_only {
            let author_local = self
                .accounts
                .find_by_id(&note.author_id)?
                .is_some_and(|a| a.is_local());
            if !author_local {
                return Ok(FALLBACK_REACTION.to_string());
            }
        }
        if emoji.is_sensitive
            && matches!(
                note.reaction_acceptance,
                Some(
                    crate::domain::note::ReactionAcceptance::NonSensitiveOnly
                        | crate::domain::note::ReactionAcceptance::NonSensitiveOnlyForLocalLikeOnlyForRemote
                )
            )
        {
            return Ok(FALLBACK_REACTION.to_string());
        }
        // Canonicalize `:name@.:` to `:name:` (upstream parity).
        Ok(format!(":{name}:"))
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

    /// `notes/thread-muting/create {noteId*}`: mute the note's thread for
    /// the caller (gaps WP2a; idempotent — repeats stay 204). The note must
    /// exist and be live (`NO_SUCH_NOTE` on miss/deleted). Thread identity
    /// is the note itself (single-node: no reply-tree walk).
    pub fn mute_note_thread(&self, user_id: &str, note_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        match self.notes.get_note(note_id)? {
            Some(note) if !note.is_deleted => {}
            _ => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        }
        self.thread_mutes.mute_thread(user_id, note_id)?;
        Ok(())
    }

    /// `notes/thread-muting/delete {noteId*}`: unmute the note's thread
    /// (idempotent twin of [`Self::mute_note_thread`]).
    pub fn unmute_note_thread(&self, user_id: &str, note_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        match self.notes.get_note(note_id)? {
            Some(note) if !note.is_deleted => {}
            _ => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        }
        self.thread_mutes.unmute_thread(user_id, note_id)?;
        Ok(())
    }
}

#[cfg(test)]
mod federation_tests {
    use super::AppServices;
    use crate::application::commands::tests::test_services;
    use crate::application::ports::{CreateAccountInput, CreateNoteInput, JobKind};
    use crate::federation::outbox::FederateDeliverPayload;

    fn remote_account(services: &AppServices, handle: &str) -> String {
        services
            .accounts
            .create_account(CreateAccountInput {
                handle: handle.to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://remote.example/users/{handle}")),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .unwrap()
            .id
    }

    fn federate_jobs(services: &AppServices) -> Vec<FederateDeliverPayload> {
        let mut out = Vec::new();
        while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
            if job.kind == JobKind::FederateDeliver {
                out.push(serde_json::from_slice(&job.payload).unwrap());
            }
            services.jobs.complete(&job.id).unwrap();
        }
        out
    }

    #[test]
    fn undo_like_names_the_like_that_was_delivered() {
        // Regression (bench run 2, S2 residue): `unreact` built the Undo(Like)
        // from the reaction *row* id, but `react` had federated the Like under
        // a different id. Akkoma/Pleroma resolves an Undo's object by id and
        // silently drops it when unknown, so the remote favourite survived.
        let services = test_services("notes-undo-like");
        let alice = services.create_account("alice", None).unwrap().id;
        let rina = remote_account(&services, "rina");
        let note = services
            .handle_inbound_create_note(
                "https://remote.example/activities/1",
                CreateNoteInput {
                    uri: Some("https://remote.example/notes/1".to_string()),
                    ..CreateNoteInput::simple_public(&rina, "hello")
                },
                Vec::new(),
            )
            .unwrap()
            .expect("remote note stored");

        services.react(&alice, &note.id, "🎉", false).unwrap();
        let like_id = match federate_jobs(&services).as_slice() {
            [FederateDeliverPayload::Like { reaction_id, .. }] => reaction_id.clone(),
            other => panic!("expected one Like, got {other:?}"),
        };

        services.unreact(&alice, &note.id).unwrap();
        match federate_jobs(&services).as_slice() {
            [FederateDeliverPayload::UndoLike { reaction_id, .. }] => {
                assert_eq!(
                    reaction_id, &like_id,
                    "Undo(Like) must reference the Like id that was delivered"
                );
            }
            other => panic!("expected one UndoLike, got {other:?}"),
        }
    }
}
