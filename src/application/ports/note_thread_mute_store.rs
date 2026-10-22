use crate::domain::id::PortError;

/// Per-user thread mutes (`notes/thread-muting/*` parity: hiding one
/// reply-tree for the muting user only; never touches follow edges).
pub trait NoteThreadMuteStore: Send + Sync {
    /// Mute `note_id`'s thread for `user_id`. Returns true when the row
    /// was newly added (idempotent: repeats return false).
    fn mute_thread(&self, user_id: &str, note_id: &str) -> Result<bool, PortError>;
    /// Unmute `note_id`'s thread for `user_id`. Returns true when a row
    /// existed (idempotent: repeats return false).
    fn unmute_thread(&self, user_id: &str, note_id: &str) -> Result<bool, PortError>;
    /// True when `user_id` muted `note_id`'s thread.
    fn is_thread_muted(&self, user_id: &str, note_id: &str) -> Result<bool, PortError>;
}

// ---------------------------------------------------------------------------
// Gaps WP3: webhooks + abuse-report recipients (moved verbatim from
// `src/application/commands/webhooks.rs`; the command module keeps its
// copies working until the parent wires the redb tables and the
// `AppServices` fields).
// ---------------------------------------------------------------------------
