use super::job_queue::NewJob;
use crate::domain::id::PortError;
use crate::domain::reaction::Reaction;

pub trait ReactionStore: Send + Sync {
    /// `reaction_id` becomes the stored row id. The application also federates
    /// the Like under this id, so a later `Undo(Like)` (built from the row id)
    /// names the activity the remote server actually received.
    fn react(
        &self,
        user_id: &str,
        note_id: &str,
        reaction: &str,
        reaction_id: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Reaction, PortError>;
    fn unreact(&self, user_id: &str, note_id: &str, jobs: Vec<NewJob>) -> Result<(), PortError>;
    fn get_reaction(&self, user_id: &str, note_id: &str) -> Result<Option<Reaction>, PortError>;
    /// Every reaction row by `user_id` (`users/reactions` read path;
    /// Misskey `NoteReactionsRepository` query parity). Backends return
    /// rows in any order; the application op sorts newest-first and pages.
    fn reactions_by_user(&self, user_id: &str) -> Result<Vec<Reaction>, PortError>;
    /// Every reaction row on `note_id` (§8 edit fan-out: remote reactors
    /// join the `Update(Note)` audience). Backends may scan; rows in any
    /// order.
    fn reactions_for_note(&self, note_id: &str) -> Result<Vec<Reaction>, PortError> {
        let _ = note_id;
        Err(PortError::StorageUnavailable(
            "ReactionStore::reactions_for_note not implemented".to_string(),
        ))
    }
    /// Distinct reactor user ids for `note_id`, oldest-first, truncated to
    /// `limit` (Wave 4 liveness: edit fan-out needs the audience, not the
    /// full reaction rows). Backends serve it from the by-note index and
    /// decode at most `limit` rows. The default truncates
    /// `reactions_for_note` for test doubles.
    fn reactor_ids_for_note(&self, note_id: &str, limit: usize) -> Result<Vec<String>, PortError> {
        if note_id.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for reaction in self.reactions_for_note(note_id)? {
            if !out.contains(&reaction.user_id) {
                out.push(reaction.user_id);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }
    /// Total stored reaction rows (Batch P `stats`).
    fn count_reactions(&self) -> Result<u64, PortError> {
        Err(PortError::StorageUnavailable(
            "ReactionStore::count_reactions not implemented".to_string(),
        ))
    }
}
