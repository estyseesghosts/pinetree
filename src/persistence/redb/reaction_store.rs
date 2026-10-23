use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::{NOTES, RedbStore};
use crate::application::ports::{NewJob, PortError, ReactionStore};
use crate::domain::note::Note;
use crate::domain::reaction::Reaction;
use redb::{
    ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction,
};

pub(crate) const REACTIONS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("reactions");
/// Reactions by `(note_id, reaction_id)` (`notes/reactions` read path;
/// reaction ids are time-ordered so the range reads oldest-first and the
/// app reverses to newest-first). Written in the same transaction as the
/// primary `REACTIONS` row; rebuildable derived state, backfilled at open
/// for older databases.
pub(crate) const REACTIONS_BY_NOTE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("reactions_by_note");
impl RedbStore {
    pub(crate) fn react_in_txn(
        &self,
        write_txn: &WriteTransaction,
        user_id: &str,
        note_id: &str,
        reaction_str: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Reaction, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let reaction = Reaction {
            id: self.ids.generate(),
            user_id: user_id.to_string(),
            note_id: note_id.to_string(),
            reaction: reaction_str.to_string(),
        };
        reaction.validate()?;

        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut reactions_table = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;
            let mut by_note_table = write_txn
                .open_table(REACTIONS_BY_NOTE)
                .map_err(map_redb_err)?;

            let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            // §0A: reacting to an author who blocks the reactor is rejected
            // (Misskey `ReactionService.create` parity: block check first,
            // then visibility). Self-reactions are exempt.
            if note.author_id != user_id
                && self.blocking_exists_in_txn(write_txn, &note.author_id, user_id)?
            {
                return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
            }

            let existing_opt: Option<Reaction> = match reactions_table
                .get((user_id, note_id))
                .map_err(map_redb_err)?
            {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };

            if let Some(prev) = &existing_opt {
                if !upsert {
                    return Err(PortError::Conflict("ALREADY_REACTED".to_string()));
                }
                if let Some(cnt) = note.reactions.get_mut(&prev.reaction) {
                    *cnt = cnt.saturating_sub(1);
                    if *cnt == 0 {
                        note.reactions.remove(&prev.reaction);
                    }
                }
            }

            *note.reactions.entry(reaction.reaction.clone()).or_insert(0) += 1;
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;

            let reaction_bytes = encode_json(&reaction)?;
            reactions_table
                .insert((user_id, note_id), reaction_bytes.as_slice())
                .map_err(map_redb_err)?;
            // Upsert mints a fresh reaction id: drop the stale index row.
            if let Some(prev) = &existing_opt {
                let _ = by_note_table.remove((prev.note_id.as_str(), prev.id.as_str()));
            }
            by_note_table
                .insert(
                    (reaction.note_id.as_str(), reaction.id.as_str()),
                    reaction_bytes.as_slice(),
                )
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, Some(note_id))?;
        Ok(reaction)
    }

    pub(crate) fn unreact_in_txn(
        &self,
        write_txn: &WriteTransaction,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut reactions_table = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;
            let mut by_note_table = write_txn
                .open_table(REACTIONS_BY_NOTE)
                .map_err(map_redb_err)?;

            let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }

            let prev: Reaction = match reactions_table
                .remove((user_id, note_id))
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NOT_REACTED".to_string())),
            };

            if let Some(cnt) = note.reactions.get_mut(&prev.reaction) {
                *cnt = cnt.saturating_sub(1);
                if *cnt == 0 {
                    note.reactions.remove(&prev.reaction);
                }
            }
            let _ = by_note_table.remove((prev.note_id.as_str(), prev.id.as_str()));
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(write_txn, jobs, Some(note_id))?;
        Ok(())
    }
}
impl ReactionStore for RedbStore {
    fn react(
        &self,
        user_id: &str,
        note_id: &str,
        reaction_str: &str,
        reaction_id: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Reaction, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let reaction = Reaction {
            id: reaction_id.to_string(),
            user_id: user_id.to_string(),
            note_id: note_id.to_string(),
            reaction: reaction_str.to_string(),
        };
        reaction.validate()?;

        let write_txn = self.begin_write()?;
        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut reactions_table = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;
            let mut by_note_table = write_txn
                .open_table(REACTIONS_BY_NOTE)
                .map_err(map_redb_err)?;

            let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            // §0A: reacting to an author who blocks the reactor is rejected
            // (Misskey `ReactionService.create` parity: block check first,
            // then visibility). Self-reactions are exempt.
            if note.author_id != user_id
                && self.blocking_exists_in_txn(&write_txn, &note.author_id, user_id)?
            {
                return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
            }

            let existing_opt: Option<Reaction> = match reactions_table
                .get((user_id, note_id))
                .map_err(map_redb_err)?
            {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };

            if let Some(prev) = &existing_opt {
                if !upsert {
                    return Err(PortError::Conflict("ALREADY_REACTED".to_string()));
                }
                if let Some(cnt) = note.reactions.get_mut(&prev.reaction) {
                    *cnt = cnt.saturating_sub(1);
                    if *cnt == 0 {
                        note.reactions.remove(&prev.reaction);
                    }
                }
            }

            *note.reactions.entry(reaction.reaction.clone()).or_insert(0) += 1;
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;

            let reaction_bytes = encode_json(&reaction)?;
            reactions_table
                .insert((user_id, note_id), reaction_bytes.as_slice())
                .map_err(map_redb_err)?;
            if let Some(prev) = &existing_opt {
                let _ = by_note_table.remove((prev.note_id.as_str(), prev.id.as_str()));
            }
            by_note_table
                .insert(
                    (reaction.note_id.as_str(), reaction.id.as_str()),
                    reaction_bytes.as_slice(),
                )
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, Some(note_id))?;
        self.commit_write(write_txn)?;
        Ok(reaction)
    }

    fn unreact(&self, user_id: &str, note_id: &str, jobs: Vec<NewJob>) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        {
            let mut notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut reactions_table = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;
            let mut by_note_table = write_txn
                .open_table(REACTIONS_BY_NOTE)
                .map_err(map_redb_err)?;

            let mut note: Note = match notes_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }

            let prev: Reaction = match reactions_table
                .remove((user_id, note_id))
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NOT_REACTED".to_string())),
            };

            if let Some(cnt) = note.reactions.get_mut(&prev.reaction) {
                *cnt = cnt.saturating_sub(1);
                if *cnt == 0 {
                    note.reactions.remove(&prev.reaction);
                }
            }
            let _ = by_note_table.remove((prev.note_id.as_str(), prev.id.as_str()));
            let note_bytes = encode_json(&note)?;
            notes_table
                .insert(note.id.as_str(), note_bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, Some(note_id))?;
        self.commit_write(write_txn)
    }

    fn get_reaction(&self, user_id: &str, note_id: &str) -> Result<Option<Reaction>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REACTIONS).map_err(map_redb_err)?;
        match table.get((user_id, note_id)).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn reactions_by_user(&self, user_id: &str) -> Result<Vec<Reaction>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REACTIONS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        Ok(out)
    }

    fn reactions_for_note(&self, note_id: &str) -> Result<Vec<Reaction>, PortError> {
        // `REACTIONS_BY_NOTE` range scan: bounded to one note's rows
        // (oldest-first by time-ordered reaction id; the `notes/reactions`
        // query reverses to newest-first). Backfilled at open for older DBs.
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(REACTIONS_BY_NOTE)
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table
            .range((note_id, "")..=(note_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            let reaction: Reaction = decode_json(v.value())?;
            out.push(reaction);
        }
        Ok(out)
    }

    fn reactor_ids_for_note(&self, note_id: &str, limit: usize) -> Result<Vec<String>, PortError> {
        // Wave 4 liveness: the range is lazy, so at most `limit` rows are
        // decoded no matter how viral the note is (the full-row loader
        // above stays for the `notes/reactions` read path).
        if note_id.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(REACTIONS_BY_NOTE)
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table
            .range((note_id, "")..=(note_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .take(limit)
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            let reaction: Reaction = decode_json(v.value())?;
            out.push(reaction.user_id);
        }
        Ok(out)
    }

    fn count_reactions(&self) -> Result<u64, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REACTIONS).map_err(map_redb_err)?;
        Ok(table.len().map_err(map_redb_err)?)
    }
}
