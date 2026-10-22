use super::super::codec::{decode_json, encode_json, map_redb_err};
use super::super::store::{
    ACCOUNTS, BLOB_REFCOUNT, DURABLE_BYTES_BY_OWNER, MEDIA_META, NOTES, NOTES_BY_URI, QUOTE_STAMPS,
    RedbStore,
};
use super::{
    NOTE_DRAFTS, NOTE_DRAFTS_BY_USER, NOTE_EDITS, NOTE_EDITS_BY_NOTE, NOTES_BY_AUTHOR,
    NOTES_BY_QUOTE_REQUEST, NOTES_BY_QUOTE_STAMP, NOTES_BY_REPLY, NOTES_BY_REPLY_PENDING, PINNINGS,
    matches_cursor,
};
use crate::application::ports::{
    CreateNoteInput, EditNoteInput, InsertMediaInput, MediaClass, NewJob, NoteStore, PortError,
    TimelineCursor,
};
use crate::domain::account::Account;
use crate::domain::draft::NoteDraft;
use crate::domain::media::MediaMeta;
use crate::domain::note::{MAX_FILE_IDS, Note, NoteEdit};
use crate::domain::pinning::UserNotePining;
use crate::domain::policy::{NarrowingInput, narrow_on_create};
use crate::domain::quote::{QuoteAuthorizationState, QuoteStamp};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

impl NoteStore for RedbStore {
    fn create_note(&self, input: CreateNoteInput, jobs: Vec<NewJob>) -> Result<Note, PortError> {
        let write_txn = self.begin_write()?;
        let note = self.create_note_in_txn(&write_txn, input, jobs)?;
        self.commit_write(write_txn)?;
        Ok(note)
    }

    fn apply_note_edit(&self, edit: EditNoteInput, jobs: Vec<NewJob>) -> Result<Note, PortError> {
        let write_txn = self.begin_write()?;
        let result = self.apply_note_edit_in_txn(&write_txn, edit, jobs);
        match result {
            Ok(note) => self.commit_write(write_txn).map(|_| note),
            Err(e) => {
                let _ = write_txn.abort();
                Err(e)
            }
        }
    }

    fn note_edits(&self, note_id: &str) -> Result<Vec<NoteEdit>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_note = read_txn
            .open_table(NOTE_EDITS_BY_NOTE)
            .map_err(map_redb_err)?;
        let edits_table = read_txn.open_table(NOTE_EDITS).map_err(map_redb_err)?;
        // `(note_id, edit_id)` range reads oldest-first: edit ids are
        // time-ordered, so index order is history order.
        let range = by_note
            .range((note_id, "")..=(note_id, "\u{10ffff}"))
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in range {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, edit_id) = k.value();
            if let Some(g) = edits_table.get(edit_id).map_err(map_redb_err)? {
                out.push(decode_json(g.value())?);
            }
        }
        Ok(out)
    }

    fn get_note(&self, id: &str) -> Result<Option<Note>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => {
                let note: Note = decode_json(guard.value())?;
                if note.is_deleted {
                    Ok(None)
                } else {
                    Ok(Some(note))
                }
            }
            None => Ok(None),
        }
    }

    fn get_note_including_deleted(&self, id: &str) -> Result<Option<Note>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn delete_note(&self, id: &str, jobs: Vec<NewJob>) -> Result<(), PortError> {
        // Single implementation: `delete_note_in_txn` carries the tombstone,
        // counter, and reply-index maintenance (Slice A). Both entry points
        // shared one copy-pasted body before; they must never drift again.
        let write_txn = self.begin_write()?;
        let result = self.delete_note_in_txn(&write_txn, id, jobs);
        match result {
            Ok(()) => self.commit_write(write_txn),
            Err(e) => {
                let _ = write_txn.abort();
                Err(e)
            }
        }
    }

    fn notes_by_author(
        &self,
        author_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        let limit = if cursor.limit == 0 { 10 } else { cursor.limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_author = read_txn.open_table(NOTES_BY_AUTHOR).map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;

        let range = by_author
            .range((author_id, "")..=(author_id, "\u{10ffff}"))
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in range.rev() {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, note_id) = k.value();
            if !matches_cursor(note_id, cursor) {
                continue;
            }
            if let Some(ng) = notes_table.get(note_id).map_err(map_redb_err)? {
                let note: Note = decode_json(ng.value())?;
                if !note.is_deleted {
                    out.push(note);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn list_all_notes(&self, cursor: &TimelineCursor) -> Result<Vec<Note>, PortError> {
        let limit = if cursor.limit == 0 { 100 } else { cursor.limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in notes_table.iter().map_err(map_redb_err)?.rev() {
            let (k, v) = entry.map_err(map_redb_err)?;
            let note_id = k.value();
            if !matches_cursor(note_id, cursor) {
                continue;
            }
            let note: Note = decode_json(v.value())?;
            if !note.is_deleted {
                out.push(note);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn count_notes(&self) -> Result<u64, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        Ok(notes_table.len().map_err(map_redb_err)?)
    }

    fn count_local_notes(&self) -> Result<u64, PortError> {
        // One accounts pass builds the local-author set; one notes pass
        // counts live rows by those authors. Both tables are small on a
        // single node; FDB inherits the same bounded-shape discipline via
        // its own adapter (this is a read, not a transaction).
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let accounts_table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let mut local: std::collections::HashSet<String> = std::collections::HashSet::new();
        for entry in accounts_table.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let acc: Account = decode_json(v.value())?;
            if acc.is_local() {
                local.insert(k.value().to_string());
            }
        }
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        let mut count = 0u64;
        for entry in notes_table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let note: Note = decode_json(v.value())?;
            if !note.is_deleted && local.contains(&note.author_id) {
                count += 1;
            }
        }
        Ok(count)
    }

    fn find_by_uri(&self, uri: &str) -> Result<Option<Note>, PortError> {
        let target = uri.trim();
        if target.is_empty() {
            return Ok(None);
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_uri = read_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
        let Some(id) = by_uri
            .get(target)
            .map_err(map_redb_err)?
            .map(|g| g.value().to_string())
        else {
            return Ok(None);
        };
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        let found = match notes_table.get(id.as_str()).map_err(map_redb_err)? {
            Some(g) => Some(decode_json::<Note>(g.value())?),
            None => None,
        };
        Ok(found.filter(|n| !n.is_deleted))
    }

    fn notes_by_reply(
        &self,
        reply_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        // `limit` is a candidate scan limit for the application-level
        // visibility scan loop, not the final visible page size.
        let limit = if cursor.limit == 0 { 50 } else { cursor.limit };
        if reply_id.is_empty() {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_reply = read_txn.open_table(NOTES_BY_REPLY).map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;

        let range = by_reply
            .range((reply_id, "")..=(reply_id, "\u{10ffff}"))
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in range.rev() {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, note_id) = k.value();
            if !matches_cursor(note_id, cursor) {
                continue;
            }
            if let Some(ng) = notes_table.get(note_id).map_err(map_redb_err)? {
                let note: Note = decode_json(ng.value())?;
                // Deleted rows are removed at delete time; the guard stays
                // so a torn index can never surface a tombstone.
                if !note.is_deleted {
                    out.push(note);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn pending_reply_children(
        &self,
        parent_uri: &str,
        limit: usize,
    ) -> Result<Vec<Note>, PortError> {
        let target = parent_uri.trim();
        if target.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let pending_table = read_txn
            .open_table(NOTES_BY_REPLY_PENDING)
            .map_err(map_redb_err)?;
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;

        let range = pending_table
            .range((target, "")..=(target, "\u{10ffff}"))
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in range {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, note_id) = k.value();
            if let Some(ng) = notes_table.get(note_id).map_err(map_redb_err)? {
                let note: Note = decode_json(ng.value())?;
                // Stale rows (child deleted meanwhile — the delete path
                // removes them, this covers torn races) are skipped, not
                // returned.
                if note.is_deleted || note.reply_id.is_some() {
                    continue;
                }
                if note.reply_parent_uri.as_deref() == Some(target) {
                    out.push(note);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn link_pending_reply(
        &self,
        child_id: &str,
        parent_id: &str,
        parent_canonical_uri: &str,
    ) -> Result<Option<Note>, PortError> {
        if child_id.is_empty() || parent_id.is_empty() || parent_canonical_uri.trim().is_empty() {
            return Err(PortError::Validation(
                "link_pending_reply requires child, parent, and parent URI".to_string(),
            ));
        }
        let expected_uri = parent_canonical_uri.trim();
        let write_txn = self.begin_write()?;
        let linked = {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut by_reply_table = write_txn.open_table(NOTES_BY_REPLY).map_err(map_redb_err)?;
            let mut by_reply_pending_table = write_txn
                .open_table(NOTES_BY_REPLY_PENDING)
                .map_err(map_redb_err)?;

            let mut child: Note = match notes_table.get(child_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Ok(None),
            };
            if child.is_deleted {
                return Ok(None);
            }
            if let Some(existing) = child.reply_id.as_deref() {
                // Already linked: converge with no counter or index change.
                // A torn pending row/URI left behind by a crashed link is
                // cleaned below (committed with this transaction) so the
                // child never appears pending again.
                if existing == parent_id && child.reply_parent_uri.is_some() {
                    let stale_uri = child.reply_parent_uri.clone().unwrap_or_default();
                    child.reply_parent_uri = None;
                    let child_bytes = encode_json(&child)?;
                    notes_table
                        .insert(child.id.as_str(), child_bytes.as_slice())
                        .map_err(map_redb_err)?;
                    let _ = by_reply_pending_table
                        .remove((stale_uri.as_str(), child.id.as_str()))
                        .map_err(map_redb_err)?;
                    drop(notes_table);
                    drop(by_reply_table);
                    drop(by_reply_pending_table);
                    self.commit_write(write_txn)?;
                }
                return Ok(None);
            }
            let Some(pending_uri) = child.reply_parent_uri.clone() else {
                // Genuine root, or linked concurrently: nothing to do.
                return Ok(None);
            };
            if pending_uri.as_str() != expected_uri {
                return Err(PortError::Validation(
                    "REPLY_PARENT_MISMATCH: child pending URI does not match parent canonical URI"
                        .to_string(),
                ));
            }
            let mut parent: Note = match notes_table.get(parent_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Ok(None),
            };
            // Cycle guard: the parent chain above the link target must not
            // reach the child (including the trivial self-link). Deleted
            // ancestors are walked through — they keep their `reply_id`.
            // A deleted parent still links: the steady state matches a
            // delete racing the resolver the other way around.
            {
                let mut current = Some(parent.id.clone());
                let mut depth = 0usize;
                while let Some(id) = current {
                    if id.as_str() == child_id {
                        return Err(PortError::Validation(
                            "REPLY_CYCLE: linking would create a reply cycle".to_string(),
                        ));
                    }
                    depth += 1;
                    if depth > 512 {
                        break;
                    }
                    current = match notes_table.get(id.as_str()).map_err(map_redb_err)? {
                        Some(ag) => {
                            let ancestor: Note = decode_json(ag.value())?;
                            ancestor.reply_id
                        }
                        None => None,
                    };
                }
            }

            // Same parent-driven narrowing as a reply whose parent was
            // cached at ingestion (`domain::policy::narrow_on_create` reply
            // rules): the child's stored visibility is the request, the
            // parent only ever tightens it.
            let renote_target: Option<Note> = match child.renote_id.as_deref() {
                Some(rid) => match notes_table.get(rid).map_err(map_redb_err)? {
                    Some(rg) => Some(decode_json(rg.value())?),
                    None => None,
                },
                None => None,
            };
            let narrowed = narrow_on_create(NarrowingInput {
                requested_visibility: child.visibility,
                requested_local_only: child.local_only,
                requested_visible_user_ids: child.visible_user_ids.clone(),
                author_id: &child.author_id,
                text: child.text.as_deref(),
                cw: child.cw.as_deref(),
                renote_target: renote_target.as_ref(),
                reply_target: Some(&parent),
                renote_target_blocks_author: false,
                sensitive_words: &[],
                can_public_note: true,
            })
            .map_err(|e| PortError::Validation(format!("reply link narrowing: {e:?}")))?;

            child.reply_id = Some(parent.id.clone());
            child.reply_parent_uri = None;
            child.reply_user_id = Some(parent.author_id.clone());
            child.visibility = narrowed.visibility;
            child.local_only = narrowed.local_only;
            child.visible_user_ids = narrowed.visible_user_ids;
            let child_bytes = encode_json(&child)?;
            notes_table
                .insert(child.id.as_str(), child_bytes.as_slice())
                .map_err(map_redb_err)?;

            parent.replies_count = parent.replies_count.saturating_add(1);
            let parent_bytes = encode_json(&parent)?;
            notes_table
                .insert(parent.id.as_str(), parent_bytes.as_slice())
                .map_err(map_redb_err)?;

            by_reply_table
                .insert((parent.id.as_str(), child.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
            let _ = by_reply_pending_table
                .remove((pending_uri.as_str(), child.id.as_str()))
                .map_err(map_redb_err)?;
            child
        };
        self.commit_write(write_txn)?;
        Ok(Some(linked))
    }

    fn set_pending_reply_parent(
        &self,
        child_id: &str,
        parent_uri: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let uri = parent_uri.trim();
        if child_id.is_empty() || !(uri.starts_with("http://") || uri.starts_with("https://")) {
            return Err(PortError::Validation(
                "set_pending_reply_parent requires a note id and an absolute http(s) parent URI"
                    .to_string(),
            ));
        }
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        // No-op paths commit nothing: every branch below that returns
        // `false` performs reads only, so falling through to the commit
        // with no writes is a harmless empty transaction.
        let changed = {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut by_reply_pending_table = write_txn
                .open_table(NOTES_BY_REPLY_PENDING)
                .map_err(map_redb_err)?;
            let stored: Option<Note> = match notes_table.get(child_id).map_err(map_redb_err)? {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };
            let mut child = match stored {
                Some(note) => note,
                None => return Ok(false),
            };
            if child.is_deleted || child.reply_id.is_some() {
                return Ok(false);
            }
            if child.reply_parent_uri.as_deref() == Some(uri) {
                return Ok(false);
            }
            if let Some(old_uri) = child.reply_parent_uri.clone() {
                let _ = by_reply_pending_table
                    .remove((old_uri.as_str(), child.id.as_str()))
                    .map_err(map_redb_err)?;
            }
            child.reply_parent_uri = Some(uri.to_string());
            let child_bytes = encode_json(&child)?;
            notes_table
                .insert(child.id.as_str(), child_bytes.as_slice())
                .map_err(map_redb_err)?;
            by_reply_pending_table
                .insert((uri, child.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
            true
        };
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(changed)
    }

    fn set_quote_authorization(
        &self,
        note_id: &str,
        state: QuoteAuthorizationState,
        authorization_uri: Option<String>,
        verified_at_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<Note, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        let note = self.set_quote_authorization_in_txn(
            &write_txn,
            note_id,
            state,
            authorization_uri,
            verified_at_ms,
            jobs,
        )?;
        self.commit_write(write_txn)?;
        Ok(note)
    }

    fn find_note_by_quote_request(&self, request_uri: &str) -> Result<Option<Note>, PortError> {
        let target = request_uri.trim();
        if target.is_empty() {
            return Ok(None);
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_request = read_txn
            .open_table(NOTES_BY_QUOTE_REQUEST)
            .map_err(map_redb_err)?;
        let Some(id) = by_request
            .get(target)
            .map_err(map_redb_err)?
            .map(|guard| guard.value().to_string())
        else {
            return Ok(None);
        };
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        let found = match notes_table.get(id.as_str()).map_err(map_redb_err)? {
            Some(guard) => Some(decode_json::<Note>(guard.value())?),
            None => None,
        };
        Ok(found.filter(|note| !note.is_deleted))
    }

    fn find_note_by_quote_stamp(&self, stamp_uri: &str) -> Result<Option<Note>, PortError> {
        let target = stamp_uri.trim();
        if target.is_empty() {
            return Ok(None);
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_stamp = read_txn
            .open_table(NOTES_BY_QUOTE_STAMP)
            .map_err(map_redb_err)?;
        let Some(id) = by_stamp
            .get(target)
            .map_err(map_redb_err)?
            .map(|guard| guard.value().to_string())
        else {
            return Ok(None);
        };
        let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
        let found = match notes_table.get(id.as_str()).map_err(map_redb_err)? {
            Some(guard) => Some(decode_json::<Note>(guard.value())?),
            None => None,
        };
        Ok(found.filter(|note| !note.is_deleted))
    }

    fn save_quote_stamp(&self, stamp: QuoteStamp, jobs: Vec<NewJob>) -> Result<(), PortError> {
        if stamp.id.trim().is_empty() {
            return Err(PortError::Validation("empty stamp id".to_string()));
        }
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        self.save_quote_stamp_in_txn(&write_txn, &stamp, jobs)?;
        self.commit_write(write_txn)
    }

    fn get_quote_stamp(&self, stamp_id: &str) -> Result<Option<QuoteStamp>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(QUOTE_STAMPS).map_err(map_redb_err)?;
        match table.get(stamp_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn attach_remote_media(
        &self,
        note_id: &str,
        input: InsertMediaInput,
        jobs: Vec<NewJob>,
    ) -> Result<MediaMeta, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        // Build and validate the row before opening the transaction (so a
        // validation failure never touches storage).
        let id = input.id.clone().unwrap_or_else(|| self.ids.generate());
        let meta = MediaMeta {
            id: id.clone(),
            owner_id: input.owner_id.clone(),
            name: input.name.clone(),
            mime: input.mime.clone(),
            size_bytes: input.size_bytes,
            hash: input.hash.clone(),
            blob_key: input.blob_key.clone(),
            blurhash: input.blurhash.clone(),
            properties: input.properties.clone(),
            is_sensitive: input.is_sensitive,
            is_link: input.is_link,
            url: input.url.clone(),
            thumbnail_url: input.thumbnail_url.clone(),
            comment: input.comment.clone(),
            source_url: input.source_url.clone(),
            variant: "original".to_string(),
            media_class: MediaClass::classify_mime(input.mime.as_str())
                .as_str()
                .to_string(),
            evicted: false,
        };
        meta.validate(input.max_file_size)?;
        let write_txn = self.begin_write()?;
        // NOTES and MEDIA_META are each opened exactly once: opening the
        // same table twice in one write txn errors.
        let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
        let mut media_table = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        // Read the note into an owned value first: the access guard borrows
        // the table, so it must be dropped before any early `abort()`.
        let note_opt: Option<Note> = {
            let guard = notes_table.get(note_id).map_err(map_redb_err)?;
            guard.map(|g| decode_json(g.value())).transpose()?
        };
        let mut note: Note = match note_opt {
            Some(n) => n,
            None => {
                drop(notes_table);
                drop(media_table);
                let _ = write_txn.abort();
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
        };
        if note.is_deleted {
            drop(notes_table);
            drop(media_table);
            let _ = write_txn.abort();
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        // Idempotency per (source_url, note): a row for this URL that the
        // note already references is returned as-is — no second insert, so
        // job retries and inbox replays never orphan rows. The scan collects
        // the hit first so the iterator is dropped before any `abort()`.
        let mut idempotent_hit: Option<MediaMeta> = None;
        if let Some(url) = meta.source_url.as_deref() {
            let target = url.trim();
            if !target.is_empty() {
                let attached: std::collections::HashSet<&str> =
                    note.file_ids.iter().map(String::as_str).collect();
                for entry in media_table.iter().map_err(map_redb_err)? {
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let existing: MediaMeta = decode_json(v.value())?;
                    if existing.source_url.as_deref().map(str::trim) == Some(target)
                        && attached.contains(existing.id.as_str())
                    {
                        idempotent_hit = Some(existing);
                        break;
                    }
                }
            }
        }
        if let Some(existing) = idempotent_hit {
            drop(notes_table);
            drop(media_table);
            let _ = write_txn.abort();
            return Ok(existing);
        }
        let author: Account = {
            let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let author_opt: Option<Account> = {
                let guard = accounts_table
                    .get(note.author_id.as_str())
                    .map_err(map_redb_err)?;
                guard.map(|g| decode_json(g.value())).transpose()?
            };
            drop(accounts_table);
            match author_opt {
                Some(a) => a,
                None => {
                    drop(notes_table);
                    drop(media_table);
                    let _ = write_txn.abort();
                    return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
                }
            }
        };
        // Same ownership rule as `create_note_in_txn`: the row must belong
        // to the author, or be an unowned remote-cache row for a remote
        // author.
        let owner_ok = input.owner_id.as_deref() == Some(note.author_id.as_str())
            || (author.host.is_some() && input.owner_id.is_none());
        if !owner_ok {
            drop(notes_table);
            drop(media_table);
            let _ = write_txn.abort();
            return Err(PortError::Validation("NO_SUCH_FILE".to_string()));
        }
        if note.file_ids.len() >= MAX_FILE_IDS {
            drop(notes_table);
            drop(media_table);
            let _ = write_txn.abort();
            return Err(PortError::Validation("TOO_MANY_ATTACHMENTS".to_string()));
        }
        // `note.file_ids` uniqueness is a domain invariant; skip ids the
        // note already references (defensive: same outcome as idempotent).
        if !note.file_ids.contains(&id) {
            note.file_ids.push(id);
        }
        let note_bytes = encode_json(&note)?;
        notes_table
            .insert(note.id.as_str(), note_bytes.as_slice())
            .map_err(map_redb_err)?;
        // Storage-overhaul accounting for the attach path: the row is new
        // (or a same-id rewrite), so refcount and durable totals move here
        // — `insert_metadata` is bypassed on this path.
        let old_attached: Option<MediaMeta> =
            match media_table.get(meta.id.as_str()).map_err(map_redb_err)? {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };
        let meta_bytes = encode_json(&meta)?;
        media_table
            .insert(meta.id.as_str(), meta_bytes.as_slice())
            .map_err(map_redb_err)?;
        {
            let mut refcounts = write_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
            if let Some(old) = old_attached.as_ref() {
                if old.blob_key != meta.blob_key {
                    let left = refcounts
                        .get(old.blob_key.as_str())
                        .map_err(map_redb_err)?
                        .map(|g| g.value())
                        .unwrap_or(0)
                        .saturating_sub(1);
                    if left == 0 {
                        refcounts
                            .remove(old.blob_key.as_str())
                            .map_err(map_redb_err)?;
                    } else {
                        refcounts
                            .insert(old.blob_key.as_str(), left)
                            .map_err(map_redb_err)?;
                    }
                }
            }
            if old_attached
                .as_ref()
                .is_none_or(|o| o.blob_key != meta.blob_key)
            {
                let count = refcounts
                    .get(meta.blob_key.as_str())
                    .map_err(map_redb_err)?
                    .map(|g| g.value())
                    .unwrap_or(0);
                refcounts
                    .insert(meta.blob_key.as_str(), count.saturating_add(1))
                    .map_err(map_redb_err)?;
            }
            if old_attached.as_ref().map(|o| o.owner_id.as_deref())
                != Some(meta.owner_id.as_deref())
                || old_attached.as_ref().map(|o| o.size_bytes) != Some(meta.size_bytes)
            {
                if let Some(old) = old_attached.as_ref() {
                    if let Some(owner) = old.owner_id.as_deref() {
                        let mut durable = write_txn
                            .open_table(DURABLE_BYTES_BY_OWNER)
                            .map_err(map_redb_err)?;
                        let total = durable
                            .get(owner)
                            .map_err(map_redb_err)?
                            .map(|g| g.value())
                            .unwrap_or(0);
                        durable
                            .insert(owner, total.saturating_sub(old.size_bytes))
                            .map_err(map_redb_err)?;
                    }
                }
                if let Some(owner) = meta.owner_id.as_deref() {
                    let mut durable = write_txn
                        .open_table(DURABLE_BYTES_BY_OWNER)
                        .map_err(map_redb_err)?;
                    let total = durable
                        .get(owner)
                        .map_err(map_redb_err)?
                        .map(|g| g.value())
                        .unwrap_or(0);
                    durable
                        .insert(owner, total.saturating_add(meta.size_bytes))
                        .map_err(map_redb_err)?;
                }
            }
        }
        drop(notes_table);
        drop(media_table);
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(meta)
    }

    // --- Note drafts (§5) -------------------------------------------------

    fn create_draft(&self, draft: NoteDraft, jobs: Vec<NewJob>) -> Result<NoteDraft, PortError> {
        draft
            .validate_shape()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        let write_txn = self.begin_write()?;
        {
            let mut drafts_table = write_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
            let mut by_user_table = write_txn
                .open_table(NOTE_DRAFTS_BY_USER)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&draft)?;
            drafts_table
                .insert(draft.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            by_user_table
                .insert((draft.user_id.as_str(), draft.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(draft)
    }

    fn get_draft(&self, id: &str) -> Result<Option<NoteDraft>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn update_draft(&self, draft: NoteDraft, jobs: Vec<NewJob>) -> Result<NoteDraft, PortError> {
        draft
            .validate_shape()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        // Existence + ownership pre-check in a read txn (the write txn
        // below must not abort while table guards are live).
        let existing: NoteDraft = match self.get_draft(&draft.id)? {
            Some(existing) => existing,
            None => return Err(PortError::NotFound("NO_SUCH_NOTE_DRAFT".to_string())),
        };
        if existing.user_id != draft.user_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        let write_txn = self.begin_write()?;
        {
            let mut drafts_table = write_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
            let encoded = encode_json(&draft)?;
            drafts_table
                .insert(draft.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(draft)
    }

    fn delete_draft(&self, id: &str, jobs: Vec<NewJob>) -> Result<(), PortError> {
        // Resolve the owner first (read txn) so the write txn below never
        // aborts while table guards are live.
        let existing = self.get_draft(id)?;
        let write_txn = self.begin_write()?;
        {
            let mut drafts_table = write_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
            let mut by_user_table = write_txn
                .open_table(NOTE_DRAFTS_BY_USER)
                .map_err(map_redb_err)?;
            if let Some(draft) = existing {
                drafts_table.remove(id).map_err(map_redb_err)?;
                let _ = by_user_table.remove((draft.user_id.as_str(), id));
            }
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn drafts_by_user(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
        scheduled: Option<bool>,
    ) -> Result<Vec<NoteDraft>, PortError> {
        let limit = if cursor.limit == 0 { 30 } else { cursor.limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTE_DRAFTS_BY_USER)
            .map_err(map_redb_err)?;
        let drafts_table = read_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in by_user
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .rev()
        {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, draft_id) = k.value();
            if !matches_cursor(draft_id, cursor) {
                continue;
            }
            if let Some(guard) = drafts_table.get(draft_id).map_err(map_redb_err)? {
                let draft: NoteDraft = decode_json(guard.value())?;
                if let Some(want_scheduled) = scheduled {
                    if draft.is_actually_scheduled != want_scheduled {
                        continue;
                    }
                }
                out.push(draft);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn count_drafts(&self, user_id: &str) -> Result<usize, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTE_DRAFTS_BY_USER)
            .map_err(map_redb_err)?;
        Ok(by_user
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .count())
    }

    fn count_scheduled_drafts(&self, user_id: &str) -> Result<usize, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTE_DRAFTS_BY_USER)
            .map_err(map_redb_err)?;
        let drafts_table = read_txn.open_table(NOTE_DRAFTS).map_err(map_redb_err)?;
        let mut count = 0usize;
        for entry in by_user
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, draft_id) = k.value();
            if let Some(guard) = drafts_table.get(draft_id).map_err(map_redb_err)? {
                let draft: NoteDraft = decode_json(guard.value())?;
                if draft.is_actually_scheduled {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    // --- Note pinnings (§7) -----------------------------------------------

    fn add_pinning(
        &self,
        pinning: UserNotePining,
        jobs: Vec<NewJob>,
    ) -> Result<UserNotePining, PortError> {
        pinning
            .validate()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        // Duplicate pre-check in a read txn (the write txn below must not
        // abort while table guards are live). The unique key makes the
        // residual race converge to the same `ALREADY_PINNED`.
        if self
            .pinnings_for_user(&pinning.user_id)?
            .iter()
            .any(|p| p.note_id == pinning.note_id)
        {
            return Err(PortError::Conflict("ALREADY_PINNED".to_string()));
        }
        let write_txn = self.begin_write()?;
        {
            let mut pinnings_table = write_txn.open_table(PINNINGS).map_err(map_redb_err)?;
            if pinnings_table
                .get((pinning.user_id.as_str(), pinning.note_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                drop(pinnings_table);
                let _ = write_txn.abort();
                return Err(PortError::Conflict("ALREADY_PINNED".to_string()));
            }
            let encoded = encode_json(&pinning)?;
            pinnings_table
                .insert(
                    (pinning.user_id.as_str(), pinning.note_id.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(pinning)
    }

    fn remove_pinning(
        &self,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut pinnings_table = write_txn.open_table(PINNINGS).map_err(map_redb_err)?;
            let _ = pinnings_table.remove((user_id, note_id));
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn pinnings_for_user(&self, user_id: &str) -> Result<Vec<UserNotePining>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let pinnings_table = read_txn.open_table(PINNINGS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in pinnings_table
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        // Oldest pin first (pinning-id order; aidx ids are time-ordered),
        // matching the upstream `pins.map(...)` pin order.
        out.sort_by(|a: &UserNotePining, b: &UserNotePining| a.id.cmp(&b.id));
        Ok(out)
    }
}
