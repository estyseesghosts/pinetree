use crate::domain::id::PortError;

/// Durable `Accept(Follow)` activity index (`Undo(Accept)` by-ID support).
///
/// A bare-IRI `Undo{object: <accept-id>}` carries no embedded `Follow`, so
/// the pair it revokes resolves only through a previously recorded
/// `Accept`. The inbox records every embedded `Accept(Follow)` here; the
/// `Undo` path consults it. Bounded by the backend (the process-local map
/// caps at `ACTIVITY_INDEX_CAP`; the durable table evicts oldest-first
/// past the same bound).
pub trait ActivityIndex: Send + Sync {
    /// Record an `Accept(Follow)` grant. No-op on empty ids.
    fn record_accept(
        &self,
        accept_id: &str,
        follower_actor_uri: &str,
        followee_actor_uri: &str,
    ) -> Result<(), PortError>;
    /// Look up a previously recorded `Accept(Follow)` by activity id.
    fn lookup_accept(&self, accept_id: &str) -> Result<Option<(String, String)>, PortError>;
    /// Forget one entry (used after a successful by-ID undo so a replayed
    /// `Undo` cannot unfollow a later re-follow: the next embedded
    /// `Accept` re-seeds when the follow is re-granted).
    fn forget_accept(&self, accept_id: &str) -> Result<(), PortError>;
}
