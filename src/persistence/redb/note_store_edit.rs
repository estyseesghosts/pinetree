use super::super::codec::{decode_json, encode_json, map_redb_err, trim_to_none};
use super::super::store::{
    ACCOUNTS, HANDLE_INDEX, MEDIA_META, NOTES, POLL_VOTES, POLLS, QUOTE_STAMPS, RedbStore,
};
use super::{NOTE_EDITS, NOTE_EDITS_BY_NOTE, contains_any_word};
use crate::application::ports::{EditNoteInput, NewJob, PortError};
use crate::domain::account::Account;
use crate::domain::media::MediaMeta;
use crate::domain::note::{
    MAX_CW_STORAGE_CHARS, MAX_NOTE_TEXT_CHARS, Note, NoteEdit, extract_hashtags, is_quote_content,
};
use crate::domain::policy::{NarrowingInput, narrow_on_create};
use crate::domain::poll::Poll;
use crate::domain::quote::{QuoteAuthorizationState, QuoteReference, QuoteStamp};
use redb::{ReadableTable, ReadableTableMetadata, WriteTransaction};

impl RedbStore {
    /// `notes/edit` storage path (§8; Sharkey `NoteEditService.edit`
    /// parity). One transaction commits the updated note row, the
    /// `note_edits` history row (effective edits only), the poll
    /// add/remove/replace (votes reset), and `jobs` atomically.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn apply_note_edit_in_txn(
        &self,
        write_txn: &WriteTransaction,
        edit: EditNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
        let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let media_table = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;

        let old: Note = match notes_table
            .get(edit.note_id.as_str())
            .map_err(map_redb_err)?
        {
            Some(g) => decode_json(g.value())?,
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        };
        if old.is_deleted {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        if old.author_id != edit.editor_id {
            return Err(PortError::Validation("YOU_ARE_NOT_THE_AUTHOR".to_string()));
        }
        let author: Account = match accounts_table
            .get(old.author_id.as_str())
            .map_err(map_redb_err)?
        {
            Some(g) => decode_json(g.value())?,
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        };

        // The new renote target (None = detach; the reply parent is
        // immutable and stays on the original). A pure-renote target is
        // rejected (Sharkey endpoint `cannotReRenote` parity, re-checked
        // here so the gate holds inside the atomic transaction).
        let mut renote_target: Option<Note> = None;
        if let Some(renote_id) = edit.renote_of.as_deref() {
            let target: Note = match notes_table.get(renote_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string())),
            };
            if target.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()));
            }
            if target.renote_id.is_some() && target.quote.is_none() {
                return Err(PortError::Validation(
                    "CANNOT_RENOTE_TO_A_PURE_RENOTE".to_string(),
                ));
            }
            renote_target = Some(target);
        }

        // Files must exist and be owned (same rule as create).
        for fid in &edit.file_ids {
            let media: MediaMeta = match media_table.get(fid.as_str()).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NO_SUCH_FILE".to_string())),
            };
            let owner_ok = media.owner_id.as_deref() == Some(old.author_id.as_str())
                || (author.host.is_some() && media.owner_id.is_none());
            if !owner_ok {
                return Err(PortError::Validation("NO_SUCH_FILE".to_string()));
            }
        }
        // `specified` audiences must resolve (same rule as create).
        for uid in &edit.visible_users {
            if accounts_table
                .get(uid.as_str())
                .map_err(map_redb_err)?
                .is_none()
            {
                return Err(PortError::Validation(format!(
                    "visible user {uid} does not exist"
                )));
            }
        }

        // Effective text/cw: trimmed + truncated to the storage bounds
        // (Sharkey service trim parity; the endpoint pre-gates the local
        // API limits, the inbound path relies on this trim).
        let new_text = trim_to_none(edit.text.as_deref(), MAX_NOTE_TEXT_CHARS);
        let new_cw = trim_to_none(edit.cw.as_deref(), MAX_CW_STORAGE_CHARS);
        let new_has_poll = edit.poll.is_some();

        // Effective quote shape (Sharkey `isQuote` parity — same predicate
        // as the create path).
        let new_is_quote = edit.renote_of.is_some()
            && is_quote_content(
                new_text.as_deref(),
                new_cw.as_deref(),
                old.reply_id.is_some(),
                new_has_poll,
                edit.file_ids.len(),
            );

        // Visibility narrowing over the FORCED original values (Sharkey
        // edit parity: `visibility`/`localOnly` are reset to the original
        // note's, then renote/reply-driven narrowing still applies, so a
        // tighter new target narrows — or rejects — the edit).
        let narrowed = narrow_on_create(NarrowingInput {
            requested_visibility: old.visibility,
            requested_local_only: old.local_only,
            requested_visible_user_ids: edit.visible_users.clone(),
            author_id: &old.author_id,
            text: new_text.as_deref(),
            cw: new_cw.as_deref(),
            renote_target: renote_target.as_ref(),
            reply_target: None,
            renote_target_blocks_author: match renote_target.as_ref() {
                Some(target) if !new_is_quote && target.author_id != old.author_id => {
                    self.blocking_exists_in_txn(write_txn, &target.author_id, &old.author_id)?
                }
                _ => false,
            },
            sensitive_words: &edit.sensitive_words,
            can_public_note: true,
        })?;
        // Reply-driven §0A gate against the ORIGINAL reply parent (the
        // reply link itself is immutable, but its author may have blocked
        // the editor since the note was created).
        if let Some(reply_id) = old.reply_id.as_deref() {
            if let Some(parent) = notes_table
                .get(reply_id)
                .map_err(map_redb_err)?
                .map(|g| decode_json::<Note>(g.value()))
                .transpose()?
            {
                if !parent.is_deleted
                    && parent.author_id != old.author_id
                    && self.blocking_exists_in_txn(write_txn, &parent.author_id, &old.author_id)?
                {
                    return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
                }
            }
        }

        // §3 quote-permission check on the effective shape (same position
        // and semantics as the create path: quoter-side flag, local throws,
        // remote strips). On the edit path the strip also detaches the
        // renote and drops the quote relation.
        let mut new_renote_id = edit.renote_of.clone();
        let mut process_errors = old.process_errors.clone();
        if new_is_quote && author.reject_quotes {
            if author.host.is_none() {
                return Err(PortError::Validation("QUOTE_DISABLED_FOR_USER".to_string()));
            }
            new_renote_id = None;
            renote_target = None;
            process_errors = Some(vec!["quoteUnavailable".to_string()]);
        }

        // Prohibited words over text/cw/poll choices (Sharkey
        // `checkProhibitedWordsContain` parity — storage-side so the
        // inbound edit path shares the gate atomically).
        {
            let mut haystacks: Vec<&str> = Vec::new();
            if let Some(text) = new_text.as_deref() {
                haystacks.push(text);
            }
            if let Some(cw) = new_cw.as_deref() {
                haystacks.push(cw);
            }
            if contains_any_word(&haystacks, &edit.prohibited_words) {
                return Err(PortError::Validation(
                    "CONTAINS_PROHIBITED_WORDS".to_string(),
                ));
            }
            if let Some(poll) = edit.poll.as_ref() {
                let choices: Vec<&str> = poll.choices.iter().map(String::as_str).collect();
                if contains_any_word(&choices, &edit.prohibited_words) {
                    return Err(PortError::Validation(
                        "CONTAINS_PROHIBITED_WORDS".to_string(),
                    ));
                }
            }
        }

        // Poll change detection against the stored row (add/remove/replace
        // all reset votes; an untouched poll keeps its tallies even past
        // expiry).
        let mut polls_table = write_txn.open_table(POLLS).map_err(map_redb_err)?;
        let old_poll: Option<Poll> = match polls_table.get(old.id.as_str()).map_err(map_redb_err)? {
            Some(g) => Some(decode_json(g.value())?),
            None => None,
        };
        let poll_changed = match (old_poll.as_ref(), edit.poll.as_ref()) {
            (None, None) => false,
            (Some(_), None) | (None, Some(_)) => true,
            (Some(old_poll), Some(new_poll)) => {
                old_poll.choices != new_poll.choices
                    || old_poll.multiple != new_poll.multiple
                    || old_poll.expires_at_ms != new_poll.expires_at_ms
            }
        };
        if let Some(new_poll) = edit.poll.as_ref() {
            if poll_changed {
                new_poll
                    .validate(self.clock.now_ms())
                    .map_err(|e| match e {
                        crate::domain::id::DomainError::Validation(msg) => {
                            PortError::Validation(msg)
                        }
                        crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
                        crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
                        crate::domain::id::DomainError::StorageUnavailable(msg) => {
                            PortError::StorageUnavailable(msg)
                        }
                    })?;
            }
        }

        // No-change edits return the original note and write nothing
        // (Sharkey `else { return oldnote; }` parity — no history row, no
        // jobs). Files use the upstream truthy-OR detection (any files on
        // either side count as changed; no deep compare).
        let text_changed = new_text != old.text;
        let cw_changed = new_cw != old.cw;
        let has_poll_changed = old_poll.is_some() != new_has_poll;
        let files_changed = !old.file_ids.is_empty() || !edit.file_ids.is_empty();
        let renote_changed = new_renote_id != old.renote_id;
        let acceptance_changed = edit.reaction_acceptance != old.reaction_acceptance;
        if !text_changed
            && !cw_changed
            && !has_poll_changed
            && !files_changed
            && !poll_changed
            && !renote_changed
            && !acceptance_changed
        {
            return Ok(old);
        }

        // Recompute mentions/tags from the new text (same extraction as
        // create). A `specified` edit merges the visible audience into the
        // mentions (Sharkey service parity).
        let mut mentions: Vec<String> = Vec::new();
        if let Some(text) = new_text.as_deref() {
            let handle_table = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
            for (uname, host_opt) in crate::domain::note::extract_all_mentions(text) {
                let key = Self::handle_key(&uname.to_ascii_lowercase(), host_opt.as_deref());
                if let Some(uid_guard) = handle_table.get(key.as_str()).map_err(map_redb_err)? {
                    let uid = uid_guard.value().to_string();
                    if !mentions.contains(&uid) {
                        mentions.push(uid);
                    }
                }
            }
        }
        let mut tags: Vec<String> = Vec::new();
        if let Some(text) = new_text.as_deref() {
            for t in extract_hashtags(text) {
                if !tags.contains(&t) {
                    tags.push(t);
                }
            }
        }
        let visible_user_ids = narrowed.visible_user_ids.clone();
        if narrowed.visibility == crate::domain::note::Visibility::Specified {
            for uid in visible_user_ids.clone() {
                if !mentions.contains(&uid) {
                    mentions.push(uid);
                }
            }
        }

        // Poll mutation with vote reset (votes table rows for this note are
        // dropped; the poll row is replaced or removed; tallies restart
        // zeroed).
        if poll_changed {
            let mut votes_table = write_txn.open_table(POLL_VOTES).map_err(map_redb_err)?;
            let stale: Vec<(String, String, i64)> = votes_table
                .iter()
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().and_then(|(k, _)| {
                        let (user, note, choice) = k.value();
                        if note == old.id.as_str() {
                            Some((user.to_string(), note.to_string(), choice))
                        } else {
                            None
                        }
                    })
                })
                .collect();
            for (user, note, choice) in stale {
                votes_table
                    .remove((user.as_str(), note.as_str(), choice))
                    .map_err(map_redb_err)?;
            }
            match edit.poll.as_ref() {
                Some(new_poll) => {
                    let poll = Poll {
                        note_id: old.id.clone(),
                        choices: new_poll.choices.clone(),
                        votes: vec![0u64; new_poll.choices.len()],
                        multiple: new_poll.multiple,
                        expires_at_ms: new_poll.expires_at_ms,
                    };
                    let poll_bytes = encode_json(&poll)?;
                    polls_table
                        .insert(old.id.as_str(), poll_bytes.as_slice())
                        .map_err(map_redb_err)?;
                }
                None => {
                    polls_table.remove(old.id.as_str()).map_err(map_redb_err)?;
                }
            }
        }

        // Quote relation maintenance on renote change (Sharkey edit parity:
        // no `QuoteRequest` is issued on edit — the relation is just set).
        // An unchanged renote keeps its existing authorization record.
        let new_quote = match (old.renote_id.as_deref(), new_renote_id.as_deref()) {
            (a, b) if a == b => old.quote.clone(),
            (_, None) => None,
            (_, Some(new_id)) => {
                let base = edit.uri_base.trim_end_matches('/');
                let target_uri = renote_target
                    .as_ref()
                    .and_then(|t| t.uri.clone())
                    .unwrap_or_else(|| format!("{base}/notes/{new_id}"));
                Some(QuoteReference::unapproved(target_uri))
            }
        };
        // Keep `target_note_id` in sync with `renote_id` (create-path
        // invariant): a carried-over quote pointing elsewhere is reset to
        // the effective target.
        let mut new_quote = new_quote;
        if let (Some(q), Some(rid)) = (new_quote.as_mut(), new_renote_id.as_deref()) {
            q.target_note_id = Some(rid.to_string());
        }

        let now_ms = self.clock.now_ms();
        let edited = Note {
            id: old.id.clone(),
            author_id: old.author_id.clone(),
            text: new_text.clone(),
            cw: new_cw.clone(),
            visibility: narrowed.visibility,
            visible_user_ids,
            local_only: narrowed.local_only,
            reply_id: old.reply_id.clone(),
            renote_id: new_renote_id,
            reply_user_id: old.reply_user_id.clone(),
            file_ids: edit.file_ids.clone(),
            mentions,
            mentioned_remote_users: old.mentioned_remote_users.clone(),
            tags,
            emojis: Vec::new(),
            reactions: old.reactions.clone(),
            renote_count: old.renote_count,
            replies_count: old.replies_count,
            reaction_acceptance: edit.reaction_acceptance,
            uri: old.uri.clone(),
            url: old.url.clone(),
            user_host: old.user_host.clone(),
            is_deleted: false,
            reply_parent_uri: old.reply_parent_uri.clone(),
            quote: new_quote,
            updated_at_ms: Some(now_ms),
            process_errors,
        };
        edited.validate_storage_with_poll(new_has_poll)?;

        // History row (Sharkey `noteEditRepository.insert` parity):
        // `oldDate` is the previous edit time (note creation time on the
        // first edit, derived from the time-ordered note id).
        let old_date_ms = old
            .updated_at_ms
            .unwrap_or_else(|| self.ids.parse_time(&old.id).unwrap_or_else(|_| now_ms));
        let history = NoteEdit {
            id: self.ids.generate(),
            note_id: old.id.clone(),
            old_text: old.text.clone(),
            new_text: if text_changed { new_text.clone() } else { None },
            cw: if cw_changed { new_cw.clone() } else { None },
            file_ids: None,
            old_date_ms,
            updated_at_ms: now_ms,
        };
        history.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
            other => PortError::Validation(format!("invalid note edit: {other:?}")),
        })?;
        {
            let mut edits_table = write_txn.open_table(NOTE_EDITS).map_err(map_redb_err)?;
            let mut by_note_table = write_txn
                .open_table(NOTE_EDITS_BY_NOTE)
                .map_err(map_redb_err)?;
            let history_bytes = encode_json(&history)?;
            edits_table
                .insert(history.id.as_str(), history_bytes.as_slice())
                .map_err(map_redb_err)?;
            by_note_table
                .insert((old.id.as_str(), history.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        }

        let edited_bytes = encode_json(&edited)?;
        notes_table
            .insert(edited.id.as_str(), edited_bytes.as_slice())
            .map_err(map_redb_err)?;
        // WP5 federation: the quote relation may have detached (renote
        // cleared) or reset (renote swapped) — sync both index rows in
        // this same transaction. A carried-over quote keeps its rows.
        if old.quote != edited.quote {
            self.remove_quote_indexes_in_txn(&write_txn, &edited.id, old.quote.as_ref())?;
            self.insert_quote_indexes_in_txn(&write_txn, &edited)?;
        }
        drop(notes_table);
        drop(accounts_table);
        drop(media_table);
        drop(polls_table);

        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(edited)
    }

    /// Quote-state transition inside an ambient write transaction (shared by
    /// `set_quote_authorization` and the `record_quote_*` dedup methods so
    /// state changes and outbox jobs commit atomically).
    pub(crate) fn set_quote_authorization_in_txn(
        &self,
        write_txn: &WriteTransaction,
        note_id: &str,
        state: QuoteAuthorizationState,
        authorization_uri: Option<String>,
        verified_at_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
        let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
            Some(g) => decode_json(g.value())?,
            None => return Err(PortError::NotFound("NO_SUCH_QUOTE".to_string())),
        };
        if note.is_deleted {
            return Err(PortError::NotFound("NO_SUCH_QUOTE".to_string()));
        }
        let old_quote = note.quote.clone();
        let Some(quote) = note.quote.as_mut() else {
            return Err(PortError::NotFound("NO_SUCH_QUOTE".to_string()));
        };
        if !quote.authorization.allows_transition(state) {
            return Err(PortError::Validation(format!(
                "illegal quote transition {} -> {}",
                quote.authorization.api_status(),
                state.api_status()
            )));
        }
        quote.authorization = state;
        // A stamp URI is kept only for stamp-backed approval. A repeat
        // `Approved` without a URI (idempotent Accept replay) retains the
        // stored stamp; `Revoked` retains it too so later duplicate
        // `Delete`s still match this quote and converge to no-ops instead
        // of misses. Every other state clears it so an
        // `Approved`-requires-stamp invariant always holds.
        match state {
            QuoteAuthorizationState::Approved => {
                if authorization_uri.is_some() {
                    quote.authorization_uri = authorization_uri;
                }
            }
            QuoteAuthorizationState::Revoked => {}
            _ => quote.authorization_uri = None,
        }
        if matches!(
            state,
            QuoteAuthorizationState::Approved | QuoteAuthorizationState::ApprovedByException
        ) {
            quote.last_verified_at = Some(verified_at_ms);
        }
        quote.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => {
                PortError::Validation(format!("invalid quote: {msg}"))
            }
            other => PortError::Validation(format!("invalid quote: {other:?}")),
        })?;
        let note_bytes = encode_json(&note)?;
        notes_table
            .insert(note.id.as_str(), note_bytes.as_slice())
            .map_err(map_redb_err)?;
        drop(notes_table);
        // WP5 federation: stamp transitions move the authorization row in
        // this same transaction (request rows never change here).
        if old_quote != note.quote {
            self.remove_quote_indexes_in_txn(&write_txn, &note.id, old_quote.as_ref())?;
            self.insert_quote_indexes_in_txn(&write_txn, &note)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, Some(&note.id))?;
        Ok(note)
    }

    /// Stamp insert inside an ambient write transaction.
    pub(crate) fn save_quote_stamp_in_txn(
        &self,
        write_txn: &WriteTransaction,
        stamp: &QuoteStamp,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        if stamp.id.trim().is_empty() {
            return Err(PortError::Validation("empty stamp id".to_string()));
        }
        for job in &jobs {
            job.validate()?;
        }
        {
            let mut table = write_txn.open_table(QUOTE_STAMPS).map_err(map_redb_err)?;
            let bytes = encode_json(stamp)?;
            table
                .insert(stamp.id.as_str(), bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(())
    }
}
