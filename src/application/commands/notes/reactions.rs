use crate::application::commands::AppServices;

use super::create::parse_local_custom_emoji;
use crate::application::events::StreamingEvent;
use crate::application::ports::PortError;
use crate::domain::note::Note;
use crate::domain::notification::NotificationKind;
use crate::domain::policy::{ViewerContext, is_visible};
use crate::domain::reaction::{FALLBACK_REACTION, Reaction, coerce_reaction, normalize_reaction};

impl AppServices {
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
            return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
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
            return Err(PortError::Validation("CANNOT_REACT_TO_RENOTE".to_string()));
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
                if let Some(inbox_url) = crate::federation::outbox::preferred_inbox(&author) {
                    let base = self.public_url.trim_end_matches('/');
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::Like {
                            reaction_id: reaction_id.clone(),
                            actor_id: user_id.to_string(),
                            target_note_uri: note
                                .uri
                                .clone()
                                .unwrap_or_else(|| format!("{base}/notes/{note_id}")),
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
        Ok(self
            .default_like
            .clone()
            .unwrap_or_else(|| FALLBACK_REACTION.to_string()))
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
                let requested = self.resolve_reaction_for_note(user_id, &note, &requested)?;
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
                    if let Some(inbox_url) = crate::federation::outbox::preferred_inbox(&author) {
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
                                target_note_uri: note
                                    .uri
                                    .clone()
                                    .unwrap_or_else(|| format!("{base}/notes/{note_id}")),
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
