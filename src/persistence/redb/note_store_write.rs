use super::super::codec::{decode_json, encode_json, map_redb_err};
use super::super::store::{
    ACCOUNTS, HANDLE_INDEX, MEDIA_META, NOTES, NOTES_BY_URI, POLLS, RedbStore, TOMBSTONES_BY_URI,
};
use super::{
    NOTES_BY_AUTHOR, NOTES_BY_QUOTE_REQUEST, NOTES_BY_QUOTE_STAMP, NOTES_BY_REPLY,
    NOTES_BY_REPLY_PENDING,
};
use crate::application::ports::{CreateNoteInput, NewJob, PortError};
use crate::domain::account::Account;
use crate::domain::media::MediaMeta;
use crate::domain::note::{Note, extract_hashtags};
use crate::domain::policy::{NarrowingInput, narrow_on_create};
use crate::domain::poll::Poll;
use crate::domain::quote::QuoteReference;
use redb::{ReadableTable, ReadableTableMetadata, WriteTransaction};
use std::collections::BTreeMap;

impl RedbStore {
    /// Insert the quote-relation index rows for a live note
    /// (`NOTES_BY_QUOTE_REQUEST` + `NOTES_BY_QUOTE_STAMP`) inside the
    /// ambient write transaction. Trims like every other URI index; empty
    /// or missing URIs index nothing.
    pub(crate) fn insert_quote_indexes_in_txn(
        &self,
        write_txn: &WriteTransaction,
        note: &Note,
    ) -> Result<(), PortError> {
        let Some(quote) = note.quote.as_ref() else {
            return Ok(());
        };
        if let Some(uri) = quote
            .request_activity_uri
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
        {
            let mut by_request = write_txn
                .open_table(NOTES_BY_QUOTE_REQUEST)
                .map_err(map_redb_err)?;
            by_request
                .insert(uri, note.id.as_str())
                .map_err(map_redb_err)?;
        }
        if let Some(uri) = quote
            .authorization_uri
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
        {
            let mut by_stamp = write_txn
                .open_table(NOTES_BY_QUOTE_STAMP)
                .map_err(map_redb_err)?;
            by_stamp
                .insert(uri, note.id.as_str())
                .map_err(map_redb_err)?;
        }
        Ok(())
    }

    /// Drop the quote-relation index rows a note held, but only when the
    /// row still names this note (a newer note may have claimed the key).
    /// Same ownership guard as the `NOTES_BY_URI` removal.
    pub(crate) fn remove_quote_indexes_in_txn(
        &self,
        write_txn: &WriteTransaction,
        note_id: &str,
        quote: Option<&QuoteReference>,
    ) -> Result<(), PortError> {
        let Some(quote) = quote else {
            return Ok(());
        };
        if let Some(uri) = quote
            .request_activity_uri
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
        {
            let mut by_request = write_txn
                .open_table(NOTES_BY_QUOTE_REQUEST)
                .map_err(map_redb_err)?;
            let owned = by_request
                .get(uri)
                .map_err(map_redb_err)?
                .is_some_and(|guard| guard.value() == note_id);
            if owned {
                by_request.remove(uri).map_err(map_redb_err)?;
            }
        }
        if let Some(uri) = quote
            .authorization_uri
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
        {
            let mut by_stamp = write_txn
                .open_table(NOTES_BY_QUOTE_STAMP)
                .map_err(map_redb_err)?;
            let owned = by_stamp
                .get(uri)
                .map_err(map_redb_err)?
                .is_some_and(|guard| guard.value() == note_id);
            if owned {
                by_stamp.remove(uri).map_err(map_redb_err)?;
            }
        }
        Ok(())
    }

    pub(crate) fn create_note_in_txn(
        &self,
        write_txn: &WriteTransaction,
        mut input: CreateNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        // `has_poll` must agree with `poll` (the application layer sets the
        // flag from the input;storage enforces the invariant).
        if input.has_poll != input.poll.is_some() {
            return Err(PortError::Validation(
                "has_poll must agree with poll".to_string(),
            ));
        }
        // Validate the poll params inside the transaction too (the
        // application layer validates against the same clock first; this
        // keeps the invariant storage-side for every caller, including
        // inbound federation).
        if let Some(poll_input) = input.poll.as_ref() {
            poll_input
                .validate(self.clock.now_ms())
                .map_err(|e| match e {
                    crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
                    crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
                    crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
                    crate::domain::id::DomainError::StorageUnavailable(msg) => {
                        PortError::StorageUnavailable(msg)
                    }
                })?;
        }
        if input.has_channel {
            return Err(PortError::Validation(
                "channels are unsupported (`UNSUPPORTED_FIELD`)".to_string(),
            ));
        }
        for job in &jobs {
            job.validate()?;
        }

        // Check prohibited words
        if let Some(text) = &input.text {
            let lower = text.to_lowercase();
            for word in &input.prohibited_words {
                let w = word.trim().to_lowercase();
                if !w.is_empty() && lower.contains(&w) {
                    return Err(PortError::Validation(
                        "CONTAINS_PROHIBITED_WORDS".to_string(),
                    ));
                }
            }
        }
        if let Some(cw) = &input.cw {
            let lower = cw.to_lowercase();
            for word in &input.prohibited_words {
                let w = word.trim().to_lowercase();
                if !w.is_empty() && lower.contains(&w) {
                    return Err(PortError::Validation(
                        "CONTAINS_PROHIBITED_WORDS".to_string(),
                    ));
                }
            }
        }

        let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let handle_table = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
        let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
        let mut by_author_table = write_txn
            .open_table(NOTES_BY_AUTHOR)
            .map_err(map_redb_err)?;
        let mut by_reply_table = write_txn.open_table(NOTES_BY_REPLY).map_err(map_redb_err)?;
        let mut by_reply_pending_table = write_txn
            .open_table(NOTES_BY_REPLY_PENDING)
            .map_err(map_redb_err)?;
        let mut by_uri_table = write_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
        let media_table = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;

        let mut author: Account = match accounts_table
            .get(input.author_id.as_str())
            .map_err(map_redb_err)?
        {
            Some(g) => decode_json(g.value())?,
            None => return Err(PortError::NotFound("author account not found".to_string())),
        };
        if author.is_deleted || author.is_suspended {
            return Err(PortError::Validation(
                "author is deleted or suspended".to_string(),
            ));
        }

        // Validate reply_to and renote_of targets. `reply_to` and
        // `reply_parent_uri` together are conflicting parents (Slice B):
        // the note is either linked or pending, never both.
        if input.reply_to.is_some() && input.reply_parent_uri.is_some() {
            return Err(PortError::Validation(
                "reply has both a resolved parent and a pending parent URI".to_string(),
            ));
        }
        // Normalize the pending parent URI: blank strings carry no
        // relationship and decode as no pending parent.
        let reply_parent_uri = input
            .reply_parent_uri
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let mut reply_target: Option<Note> = None;
        if let Some(reply_id) = &input.reply_to {
            let n: Note = match notes_table.get(reply_id.as_str()).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_REPLY_TARGET".to_string())),
            };
            if n.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_REPLY_TARGET".to_string()));
            }
            reply_target = Some(n);
        }

        let mut renote_target: Option<Note> = None;
        if let Some(renote_id) = &input.renote_of {
            let n: Note = match notes_table.get(renote_id.as_str()).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string())),
            };
            if n.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()));
            }
            renote_target = Some(n);
        }

        // Validate visible_users exist
        for uid in &input.visible_users {
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

        // Validate file_ids exist and belong to author (or are unowned remote cache files for a remote author)
        for fid in &input.file_ids {
            let media: MediaMeta = match media_table.get(fid.as_str()).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NO_SUCH_FILE".to_string())),
            };
            let owner_ok = media.owner_id.as_deref() == Some(input.author_id.as_str())
                || (author.host.is_some() && media.owner_id.is_none());
            if !owner_ok {
                return Err(PortError::Validation("NO_SUCH_FILE".to_string()));
            }
        }

        // Sharkey `isQuote` parity (`NoteCreateService.isQuote`, synced with
        // `misc/is-renote.ts`): text/cw presence (not blankness), a resolved
        // reply link, a poll, or attached files turn a renote into a quote.
        // `reply_parent_uri` (unresolved inbound parent) is NOT commentary.
        let is_quote = input.renote_of.is_some()
            && crate::domain::note::is_quote_content(
                input.text.as_deref(),
                input.cw.as_deref(),
                input.reply_to.is_some(),
                input.poll.is_some(),
                input.file_ids.len(),
            );
        let narrowed = narrow_on_create(NarrowingInput {
            requested_visibility: input.visibility,
            requested_local_only: input.local_only,
            requested_visible_user_ids: input.visible_users.clone(),
            author_id: &input.author_id,
            text: input.text.as_deref(),
            cw: input.cw.as_deref(),
            renote_target: renote_target.as_ref(),
            reply_target: reply_target.as_ref(),
            renote_target_blocks_author: {
                // §0A: real block lookup (was hardcoded `false`). Pure
                // renotes of an author who blocks the replier are rejected
                // with `YOU_HAVE_BEEN_BLOCKED` (Misskey
                // `NoteCreateService` parity, enforced here so the local
                // and inbound create paths share one check atomically).
                // Gated on `!is_quote` (Sharkey `isRenote && !isQuote`
                // parity): quotes skip the pure-renote block gate and fall
                // through to the §3 permission check below instead.
                match renote_target.as_ref() {
                    Some(target) if !is_quote && target.author_id != input.author_id => {
                        self.blocking_exists_in_txn(write_txn, &target.author_id, &input.author_id)?
                    }
                    _ => false,
                }
            },
            sensitive_words: &input.sensitive_words,
            can_public_note: true,
        })?;

        // §0A: replying to an author who blocks the replier is rejected
        // with `YOU_HAVE_BEEN_BLOCKED` (Misskey `NoteCreateService`
        // reply-blocking parity), in the same transaction as the insert.
        if let Some(target) = reply_target.as_ref() {
            if target.author_id != input.author_id
                && self.blocking_exists_in_txn(write_txn, &target.author_id, &input.author_id)?
            {
                return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
            }
        }

        // §3 (Sharkey `NoteCreateService.checkQuotePermissions` parity):
        // the QUOTER's own `rejectQuotes` flag gates quote creation — it is
        // a per-user moderation switch (admin `admin/reject-quotes` +
        // self-service `i/update {rejectQuotes}`), not the target author's.
        // Local quoters fail closed with `QUOTE_DISABLED_FOR_USER`; remote
        // quoters (inbound federation, which runs this same transaction via
        // `record_note_activity`) are stripped to a plain note with a
        // `quoteUnavailable` diagnostic. Position: after renote-narrowing,
        // before the pure-renote block gate — the inputs (quote shape +
        // author flag) are narrowing-independent, and the narrow gate above
        // is pure-renote-only, so upstream error precedence holds exactly.
        let mut process_errors: Option<Vec<String>> = None;
        if is_quote && author.reject_quotes {
            if author.host.is_none() {
                return Err(PortError::Validation("QUOTE_DISABLED_FOR_USER".to_string()));
            }
            input.renote_of = None;
            process_errors = Some(vec!["quoteUnavailable".to_string()]);
        }
        // A stripped quote leaves no renote behind: drop the loaded target
        // so no `renote_count` bump or quote record is written below.
        // (`renote_target` is `Some` only when `renote_of` was set, so this
        // is a no-op for ordinary notes.)
        let renote_target = if input.renote_of.is_none() {
            None
        } else {
            renote_target
        };
        // The strip above also drops any inbound quote relation: the stored
        // note is an ordinary plain note carrying only the diagnostic.
        if input.renote_of.is_none() {
            input.quote = None;
        }

        // Extract mentions & tags
        let mut mentions = input.extra_mentions;
        if !input.no_extract_mentions {
            if let Some(text) = &input.text {
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
        }

        let mut tags = input.extra_tags;
        if !input.no_extract_hashtags {
            if let Some(text) = &input.text {
                for t in extract_hashtags(text) {
                    if !tags.contains(&t) {
                        tags.push(t);
                    }
                }
            }
        }

        let note_uri = input
            .uri
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .map(str::to_string);
        if let Some(uri) = note_uri.as_deref() {
            // Tombstones don't hold their uri (deleting drops the index row).
            let holder = by_uri_table
                .get(uri)
                .map_err(map_redb_err)?
                .map(|g| g.value().to_string());
            if let Some(holder_id) = holder {
                let live = match notes_table.get(holder_id.as_str()).map_err(map_redb_err)? {
                    Some(g) => !decode_json::<Note>(g.value())?.is_deleted,
                    None => false,
                };
                if live {
                    return Err(PortError::Conflict("DUPLICATE_NOTE_URI".to_string()));
                }
            }
            // A remembered deletion blocks a late copy of the same URI
            // (`Delete` overtook its `Create`, or a fetch replays a deleted note).
            let tombstones = write_txn
                .open_table(TOMBSTONES_BY_URI)
                .map_err(map_redb_err)?;
            if tombstones.get(uri).map_err(map_redb_err)?.is_some() {
                return Err(PortError::Conflict("TOMBSTONED_NOTE_URI".to_string()));
            }
        }
        let note_id = self.ids.generate();
        // Local notes without an explicit URI get the canonical
        // `{uri_base}/notes/{id}` (the id is fresh, so no conflict check).
        let note_uri = note_uri.or_else(|| {
            let base = input.uri_base.trim_end_matches('/');
            (!base.is_empty()).then(|| format!("{base}/notes/{note_id}"))
        });
        // Quotes always carry `renote_id` for Misskey API compatibility, so
        // the resolved local target id is the single source of truth: when
        // the application resolved the target but left `target_note_id`
        // empty, fill it from `renote_of` here so both paths converge.
        let mut quote = input.quote;
        if let Some(q) = quote.as_mut() {
            if q.target_note_id.is_none() {
                q.target_note_id = input.renote_of.clone();
            }
            q.validate().map_err(|e| match e {
                crate::domain::id::DomainError::Validation(msg) => {
                    PortError::Validation(format!("invalid quote: {msg}"))
                }
                other => PortError::Validation(format!("invalid quote: {other:?}")),
            })?;
            if let (Some(tid), Some(rid)) =
                (q.target_note_id.as_deref(), input.renote_of.as_deref())
            {
                if tid != rid {
                    return Err(PortError::Validation(
                        "quote target_note_id must match renote_of".to_string(),
                    ));
                }
            }
        }
        let note = Note {
            id: note_id.clone(),
            author_id: input.author_id.clone(),
            text: input.text,
            cw: input.cw,
            visibility: narrowed.visibility,
            visible_user_ids: narrowed.visible_user_ids,
            local_only: narrowed.local_only,
            reply_id: input.reply_to,
            renote_id: input.renote_of,
            reply_user_id: reply_target.as_ref().map(|r| r.author_id.clone()),
            file_ids: input.file_ids,
            mentions,
            mentioned_remote_users: "[]".to_string(),
            tags,
            emojis: Vec::new(),
            reactions: BTreeMap::new(),
            renote_count: 0,
            replies_count: 0,
            reaction_acceptance: input.reaction_acceptance,
            uri: input.uri.or_else(|| note_uri.clone()),
            url: input.url,
            user_host: author.host.clone(),
            is_deleted: false,
            reply_parent_uri,
            quote,
            updated_at_ms: None,
            process_errors,
        };
        if author.host.is_some() {
            note.validate_storage_with_poll(input.poll.is_some())?;
        } else {
            note.validate_api_create_with_poll(input.poll.is_some())?;
        }

        // Update reply_target.replies_count
        if let Some(mut rt) = reply_target {
            rt.replies_count = rt.replies_count.saturating_add(1);
            let rt_bytes = encode_json(&rt)?;
            notes_table
                .insert(rt.id.as_str(), rt_bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        // Update renote_target.renote_count (if different from reply_target)
        if let Some(mut rnt) = renote_target {
            if note.reply_id.as_deref() == Some(rnt.id.as_str()) {
                rnt.replies_count = rnt.replies_count.saturating_add(1);
            }
            rnt.renote_count = rnt.renote_count.saturating_add(1);
            let rnt_bytes = encode_json(&rnt)?;
            notes_table
                .insert(rnt.id.as_str(), rnt_bytes.as_slice())
                .map_err(map_redb_err)?;
        }

        // Update author.notes_count
        author.notes_count = author.notes_count.saturating_add(1);
        let author_bytes = encode_json(&author)?;
        accounts_table
            .insert(author.id.as_str(), author_bytes.as_slice())
            .map_err(map_redb_err)?;

        let note_bytes = encode_json(&note)?;
        notes_table
            .insert(note.id.as_str(), note_bytes.as_slice())
            .map_err(map_redb_err)?;
        by_author_table
            .insert((note.author_id.as_str(), note.id.as_str()), &1u8)
            .map_err(map_redb_err)?;
        if let Some(uri) = note_uri.as_deref() {
            by_uri_table
                .insert(uri, note.id.as_str())
                .map_err(map_redb_err)?;
        }
        // WP5 federation: the quote-relation rows commit in the same
        // transaction as the note row, the counter bumps, and the jobs.
        self.insert_quote_indexes_in_txn(&write_txn, &note)?;
        // Slice A adjacency index: a resolved reply links its
        // `(parent, child)` row in the same transaction as the note, the
        // parent counter bump, and the durable jobs below. A pending
        // (unresolved) reply instead records its `(parent_uri, child)` row
        // so the resolver can find it when the parent arrives.
        if let Some(parent_id) = note.reply_id.as_deref() {
            by_reply_table
                .insert((parent_id, note.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        } else if let Some(parent_uri) = note.reply_parent_uri.as_deref() {
            by_reply_pending_table
                .insert((parent_uri, note.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        }

        drop(accounts_table);
        drop(handle_table);
        drop(notes_table);
        drop(by_author_table);
        drop(by_reply_table);
        drop(by_reply_pending_table);
        drop(by_uri_table);
        drop(media_table);

        // §4: the poll row commits in the same transaction as the note
        // (Misskey `NoteCreateService` transactional `MiNote` + `MiPoll`
        // insert parity). `votes` starts zeroed with one slot per choice,
        // or seeded from the origin-reported tallies for cached remote
        // polls (the local path always seeds zeros).
        if let Some(poll_input) = input.poll.as_ref() {
            let mut polls_table = write_txn.open_table(POLLS).map_err(map_redb_err)?;
            let votes = match input.poll_initial_votes.as_ref() {
                None => vec![0u64; poll_input.choices.len()],
                Some(seed) => {
                    if seed.len() != poll_input.choices.len() {
                        return Err(PortError::Validation(
                            "poll seed votes must match choices".to_string(),
                        ));
                    }
                    seed.clone()
                }
            };
            let poll = Poll {
                note_id: note.id.clone(),
                choices: poll_input.choices.clone(),
                votes,
                multiple: poll_input.multiple,
                expires_at_ms: poll_input.expires_at_ms,
            };
            let poll_bytes = encode_json(&poll)?;
            polls_table
                .insert(note.id.as_str(), poll_bytes.as_slice())
                .map_err(map_redb_err)?;
        }

        self.enqueue_jobs_in_txn(write_txn, jobs, Some(&note.id))?;
        Ok(note)
    }

    pub(crate) fn delete_note_in_txn(
        &self,
        write_txn: &WriteTransaction,
        id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut by_reply_table = write_txn.open_table(NOTES_BY_REPLY).map_err(map_redb_err)?;
            let mut by_reply_pending_table = write_txn
                .open_table(NOTES_BY_REPLY_PENDING)
                .map_err(map_redb_err)?;
            let mut note: Note = match notes_table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                // Already-deleted guard: repeating a delete cannot decrement
                // counters twice.
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            let was_pure_renote = note.renote_id.is_some()
                && note.text.is_none()
                && note.cw.is_none()
                && note.file_ids.is_empty();
            let renote_target_id = note.renote_id.clone();
            let reply_parent_id = note.reply_id.clone();
            let reply_parent_uri = note.reply_parent_uri.clone();

            if let Some(uri) = note.uri.as_deref().map(str::trim).filter(|u| !u.is_empty()) {
                let mut by_uri = write_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
                let owned = by_uri
                    .get(uri)
                    .map_err(map_redb_err)?
                    .is_some_and(|g| g.value() == id);
                if owned {
                    // Deleting a held note releases its uri (Group U3): only a
                    // `Delete` that arrives with no note held leaves a tombstone.
                    by_uri.remove(uri).map_err(map_redb_err)?;
                }
            }
            // WP5 federation: a deleted note releases its quote-relation
            // rows too (both lookups skip deleted notes).
            self.remove_quote_indexes_in_txn(&write_txn, &note.id, note.quote.as_ref())?;
            note.is_deleted = true;
            note.text = None;
            note.cw = None;
            note.file_ids.clear();
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;

            if was_pure_renote {
                if let Some(tid) = renote_target_id {
                    let target_opt: Option<Note> =
                        match notes_table.get(tid.as_str()).map_err(map_redb_err)? {
                            Some(tg) => Some(decode_json(tg.value())?),
                            None => None,
                        };
                    if let Some(mut target) = target_opt {
                        target.renote_count = target.renote_count.saturating_sub(1);
                        let tbytes = encode_json(&target)?;
                        notes_table
                            .insert(target.id.as_str(), tbytes.as_slice())
                            .map_err(map_redb_err)?;
                    }
                }
            }

            // Slice A counter consistency: a first-time delete of any note
            // with a resolved reply parent decrements that parent's
            // `replies_count` (saturating) and drops the `(parent, child)`
            // index row; a pending child drops its `(parent_uri, child)`
            // row. Descendants keep their own `reply_id` (no reparenting).
            // The parent row is updated whether or not it is itself
            // deleted — the counter counts direct replies, not visible ones.
            if let Some(parent_id) = reply_parent_id.as_deref() {
                let _ = by_reply_table
                    .remove((parent_id, note.id.as_str()))
                    .map_err(map_redb_err)?;
                let parent_opt: Option<Note> =
                    match notes_table.get(parent_id).map_err(map_redb_err)? {
                        Some(pg) => Some(decode_json(pg.value())?),
                        None => None,
                    };
                if let Some(mut parent) = parent_opt {
                    parent.replies_count = parent.replies_count.saturating_sub(1);
                    let pbytes = encode_json(&parent)?;
                    notes_table
                        .insert(parent.id.as_str(), pbytes.as_slice())
                        .map_err(map_redb_err)?;
                }
            }
            if let Some(parent_uri) = reply_parent_uri.as_deref() {
                let _ = by_reply_pending_table
                    .remove((parent_uri, note.id.as_str()))
                    .map_err(map_redb_err)?;
            }

            let author_opt: Option<Account> = match accounts_table
                .get(note.author_id.as_str())
                .map_err(map_redb_err)?
            {
                Some(ag) => Some(decode_json(ag.value())?),
                None => None,
            };
            if let Some(mut author) = author_opt {
                author.notes_count = author.notes_count.saturating_sub(1);
                let abytes = encode_json(&author)?;
                accounts_table
                    .insert(author.id.as_str(), abytes.as_slice())
                    .map_err(map_redb_err)?;
            }
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, Some(id))?;
        Ok(())
    }
}
