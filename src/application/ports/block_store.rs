use super::job_queue::NewJob;
use super::note_store::TimelineCursor;
use crate::domain::blocking::Blocking;
use crate::domain::id::PortError;
use crate::domain::muting::{Muting, RenoteMuting};

/// Directed blocks (`docs/misskey-parity-plan.md` §0A; Misskey
/// `MiBlocking{blocker,blockee}` unique).
///
/// `block` is the atomic unit: it validates the pair, rejects duplicates
/// with `Conflict(ALREADY_BLOCKING)`, removes both follow edges and both
/// pending follow requests (adjusting counters), inserts the block row, and
/// enqueues `jobs` — all in ONE storage transaction. The caller builds the
/// [`Blocking`] row (including its id) so the federated `Block` activity id
/// (`{url}/blocks/{id}`, Misskey `ApRendererService.renderBlock` parity) is
/// identical to the stored row id — and to the id a later `Undo(Block)`
/// references. Backends implement the whole method; the application op only
/// builds jobs (federated `Block` for local→remote, streaming hints). The
/// same method serves local `blocking/create` and inbound `Block`
/// (remote→local never federates, so no loop is possible).
pub trait BlockStore: Send + Sync {
    fn block(&self, blocking: Blocking, jobs: Vec<NewJob>) -> Result<Blocking, PortError>;
    /// Remove the block. `NotFound(NOT_BLOCKING)` when no row exists
    /// (Misskey `unblock` warns and returns; the endpoint checks first, so
    /// the application op surfaces the error).
    fn unblock(
        &self,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    fn get_blocking(
        &self,
        blocker_id: &str,
        blockee_id: &str,
    ) -> Result<Option<Blocking>, PortError>;
    /// True when `blocker_id` blocks `blockee_id`.
    fn is_blocking(&self, blocker_id: &str, blockee_id: &str) -> Result<bool, PortError>;
    /// Ids `blocker_id` blocks (read-time feed filter input).
    fn blocking_ids_for(&self, blocker_id: &str) -> Result<Vec<String>, PortError>;
    /// Ids blocking `blockee_id` (read-time feed filter input).
    fn blocked_by_ids_for(&self, blockee_id: &str) -> Result<Vec<String>, PortError>;
    /// Blocks created by `blocker_id`, newest-first, cursor-paged.
    fn list_blockings(
        &self,
        blocker_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Blocking>, PortError>;
}

/// Mutes and renote-mutes (`docs/misskey-parity-plan.md` §0A; Misskey
/// `MiMuting{muter,mutee,expiresAt?}` and `MiRenoteMuting{muter,mutee}`).
///
/// Mutes never touch follow edges. `is_muting`/`muted_ids_for` report only
/// *active* mutes (`expires_at_ms` absent or in the future); `get_muting`
/// reports row existence (so `unmute` of an expired-but-present row still
/// resolves to `NOT_MUTING`-free deletion, mirroring Misskey's endpoint
/// existence check).
pub trait MuteStore: Send + Sync {
    /// `Ok(None)` when `expires_at_ms` is past (Misskey `mute/create`
    /// returns early without creating); `Conflict(ALREADY_MUTING)` on dup.
    fn mute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        expires_at_ms: Option<i64>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Muting>, PortError>;
    /// `NotFound(NOT_MUTING)` when no row exists.
    fn unmute(&self, muter_id: &str, mutee_id: &str, jobs: Vec<NewJob>) -> Result<(), PortError>;
    fn get_muting(&self, muter_id: &str, mutee_id: &str) -> Result<Option<Muting>, PortError>;
    /// True when an *active* mute row exists.
    fn is_muting(&self, muter_id: &str, mutee_id: &str) -> Result<bool, PortError>;
    /// Active mutee ids for `muter_id` (read-time feed filter input).
    fn muted_ids_for(&self, muter_id: &str) -> Result<Vec<String>, PortError>;
    /// Mute rows by `muter_id` (including expired), newest-first,
    /// cursor-paged (drives `mute/list`).
    fn list_mutings(
        &self,
        muter_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Muting>, PortError>;
    /// `Conflict(ALREADY_MUTING)` on dup.
    fn renote_mute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<RenoteMuting, PortError>;
    /// `NotFound(NOT_MUTING)` when no row exists.
    fn renote_unmute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    fn get_renote_muting(
        &self,
        muter_id: &str,
        mutee_id: &str,
    ) -> Result<Option<RenoteMuting>, PortError>;
    fn is_renote_muting(&self, muter_id: &str, mutee_id: &str) -> Result<bool, PortError>;
    /// Mutee ids whose pure renotes `muter_id` hides.
    fn renote_muted_ids_for(&self, muter_id: &str) -> Result<Vec<String>, PortError>;
    fn list_renote_mutings(
        &self,
        muter_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<RenoteMuting>, PortError>;
}
