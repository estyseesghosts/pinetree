use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::{NOTES, POLL_VOTES, POLLS, RedbStore};
use crate::application::ports::{NewJob, PollChoiceCount, PollStore, PortError};
use crate::domain::note::Note;
use crate::domain::poll::{Poll, PollVote};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

impl PollStore for RedbStore {
    fn get_poll(&self, note_id: &str) -> Result<Option<Poll>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(POLLS).map_err(map_redb_err)?;
        match table.get(note_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn votes_by_user_for_note(
        &self,
        user_id: &str,
        note_id: &str,
    ) -> Result<Vec<PollVote>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(POLL_VOTES).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table
            .range((user_id, note_id, i64::MIN)..=(user_id, note_id, i64::MAX))
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        out.sort_by_key(|v: &PollVote| v.choice);
        Ok(out)
    }

    fn cast_vote(&self, vote: PollVote, jobs: Vec<NewJob>) -> Result<PollVote, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        if vote.choice > 10_000 {
            return Err(PortError::Validation("INVALID_CHOICE".to_string()));
        }
        let write_txn = self.begin_write()?;
        {
            let notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut polls_table = write_txn.open_table(POLLS).map_err(map_redb_err)?;
            let mut votes_table = write_txn.open_table(POLL_VOTES).map_err(map_redb_err)?;

            let note: Note = match notes_table
                .get(vote.note_id.as_str())
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            };
            if note.is_deleted {
                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
            }
            let mut poll: Poll = match polls_table
                .get(vote.note_id.as_str())
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::Validation("NO_POLL".to_string())),
            };
            // §0A: voting on an author who blocks the voter is rejected
            // (Misskey `PollService.vote` block-check parity). Self-votes
            // are exempt. Re-checked here (not only in the application
            // layer) so the gate holds inside the atomic transaction.
            if note.author_id != vote.user_id
                && self.blocking_exists_in_txn(&write_txn, &note.author_id, &vote.user_id)?
            {
                return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
            }
            if poll.is_expired(self.clock.now_ms()) {
                return Err(PortError::Validation("ALREADY_EXPIRED".to_string()));
            }
            if vote.choice >= poll.choices.len() {
                return Err(PortError::Validation("INVALID_CHOICE".to_string()));
            }
            let choice_idx = vote.choice as i64;

            if poll.multiple {
                if votes_table
                    .get((vote.user_id.as_str(), vote.note_id.as_str(), choice_idx))
                    .map_err(map_redb_err)?
                    .is_some()
                {
                    return Err(PortError::Conflict("ALREADY_VOTED".to_string()));
                }
            } else {
                let already_voted = votes_table
                    .range(
                        (vote.user_id.as_str(), vote.note_id.as_str(), i64::MIN)
                            ..=(vote.user_id.as_str(), vote.note_id.as_str(), i64::MAX),
                    )
                    .map_err(map_redb_err)?
                    .next()
                    .is_some();
                if already_voted {
                    return Err(PortError::Conflict("ALREADY_VOTED".to_string()));
                }
            }

            // `UPDATE votes[i] += 1` (Misskey 1-based SQL index parity:
            // `votes[$1]` with `index = choice + 1`; ours is 0-based).
            if let Some(slot) = poll.votes.get_mut(vote.choice) {
                *slot = slot.saturating_add(1);
            } else {
                return Err(PortError::Validation("INVALID_CHOICE".to_string()));
            }
            let poll_bytes = encode_json(&poll)?;
            polls_table
                .insert(vote.note_id.as_str(), poll_bytes.as_slice())
                .map_err(map_redb_err)?;
            let vote_bytes = encode_json(&vote)?;
            votes_table
                .insert(
                    (vote.user_id.as_str(), vote.note_id.as_str(), choice_idx),
                    vote_bytes.as_slice(),
                )
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, Some(&vote.note_id))?;
        self.commit_write(write_txn)?;
        Ok(vote)
    }

    fn sync_poll_votes(
        &self,
        note_id: &str,
        counts_by_name: &[PollChoiceCount],
        jobs: Vec<NewJob>,
    ) -> Result<Poll, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        let updated = {
            let mut polls_table = write_txn.open_table(POLLS).map_err(map_redb_err)?;
            let mut poll: Poll = match polls_table.get(note_id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound("NO_POLL".to_string())),
            };
            // Misskey `updateQuestion` parity: match stored choices by name;
            // unknown remote names are ignored, missing names keep counts.
            for (i, choice) in poll.choices.iter().enumerate() {
                if let Some(count) = counts_by_name.iter().find(|c| &c.name == choice) {
                    if let Some(slot) = poll.votes.get_mut(i) {
                        *slot = count.votes;
                    }
                }
            }
            let poll_bytes = encode_json(&poll)?;
            polls_table
                .insert(note_id, poll_bytes.as_slice())
                .map_err(map_redb_err)?;
            poll
        };
        self.enqueue_jobs_in_txn(&write_txn, jobs, Some(note_id))?;
        self.commit_write(write_txn)?;
        Ok(updated)
    }
}
