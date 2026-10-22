use super::job_queue::NewJob;
use crate::domain::id::PortError;
use crate::domain::poll::{Poll, PollVote};
use serde::{Deserialize, Serialize};

/// Polls and poll votes (`docs/misskey-parity-plan.md` §4; Misskey `MiPoll`
/// + `MiPollVote`). Poll rows are created atomically with their note in
/// `NoteStore::create_note`; this trait covers reads, voting, and inbound
/// `Question` vote-count sync. One storage transaction commits the vote +
/// counter bump + jobs atomically; FDB inherits this path (no second path).
pub trait PollStore: Send + Sync {
    /// The poll attached to `note_id`, if any.
    fn get_poll(&self, note_id: &str) -> Result<Option<Poll>, PortError>;
    /// Every vote by `user_id` on `note_id` (drives `isVoted` packing and
    /// duplicate-vote detection). Empty when the user never voted.
    fn votes_by_user_for_note(
        &self,
        user_id: &str,
        note_id: &str,
    ) -> Result<Vec<PollVote>, PortError>;
    /// Atomically record one vote and bump `votes[choice]` (Misskey
    /// `PollService.vote` parity). The backend re-checks, in order:
    /// note exists and is live (`NotFound(NO_SUCH_NOTE)`), poll exists
    /// (`Validation(NO_POLL)`), voter not blocked by the author
    /// (`Validation(YOU_HAVE_BEEN_BLOCKED)`; self-votes exempt), poll live
    /// (`Validation(ALREADY_EXPIRED)`), choice in range
    /// (`Validation(INVALID_CHOICE)`), no duplicate (`Conflict(ALREADY_VOTED)`
    /// — for `multiple` polls only the same choice conflicts). `jobs`
    /// commit in the same transaction.
    fn cast_vote(&self, vote: PollVote, jobs: Vec<NewJob>) -> Result<PollVote, PortError>;
    /// Overwrite a poll's vote counts by choice name (inbound `Question`
    /// update, Misskey `ApQuestionService.updateQuestion` parity): counts
    /// for stored choices present in `counts_by_name` are replaced, unknown
    /// names are ignored, stored choices missing from the update keep their
    /// counts. `NotFound(NO_POLL)` when the note has no poll.
    fn sync_poll_votes(
        &self,
        note_id: &str,
        counts_by_name: &[PollChoiceCount],
        jobs: Vec<NewJob>,
    ) -> Result<Poll, PortError>;
}

/// One choice's remotely reported vote count (inbound `Question` sync input;
/// mirrors the ActivityPub `oneOf`/`anyOf` entry shape in contract-stable
/// form so the application layer never imports ActivityPub types).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollChoiceCount {
    pub name: String,
    pub votes: u64,
}
