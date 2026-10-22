use super::job_queue::NewJob;
use crate::domain::id::PortError;

/// Clips (Misskey `MiClip`/`MiClipNote`/`MiClipFavorite` parity + pinetree
/// federation columns). Count checks + inserts commit atomically in ONE
/// port call (same discipline as `UserListStore`); FDB inherits this path.
pub trait ClipStore: Send + Sync {
    /// Insert one clip; `Conflict(TOO_MANY_CLIPS)` when the owner already
    /// holds [`crate::domain::clip::CLIP_LIMIT_PER_OWNER`] clips. Count
    /// check + insert commit atomically.
    fn create_clip(
        &self,
        clip: crate::domain::clip::Clip,
    ) -> Result<crate::domain::clip::Clip, PortError>;
    /// Owner-scoped rewrite. `None` on miss (endpoint maps to
    /// per-endpoint `NO_SUCH_CLIP`).
    fn update_clip(
        &self,
        clip: crate::domain::clip::Clip,
        jobs: Vec<NewJob>,
    ) -> Result<Option<crate::domain::clip::Clip>, PortError>;
    /// Owner-scoped delete with clip-note + favorite + follow cascade, one
    /// transaction. Returns true when a row existed.
    fn delete_clip(
        &self,
        owner_id: &str,
        clip_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    fn get_clip(&self, clip_id: &str) -> Result<Option<crate::domain::clip::Clip>, PortError>;
    /// Owner's clips, newest-first (bounded by the per-owner cap).
    fn clips_by_owner(&self, owner_id: &str) -> Result<Vec<crate::domain::clip::Clip>, PortError>;
    /// Add one `(clip, note)` pair; `Conflict(ALREADY_CLIPPED)` on a
    /// duplicate pair, `Conflict(TOO_MANY_CLIP_NOTES)` past
    /// [`crate::domain::clip::CLIP_NOTES_PER_CLIP`] members. Cap check +
    /// insert + `last_clipped_at` touch + `jobs` commit atomically.
    fn add_clip_note(
        &self,
        row: crate::domain::clip::ClipNote,
        now_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::clip::ClipNote, PortError>;
    /// Remove one pair (idempotent) with `jobs`, one transaction.
    /// Returns true when a row existed.
    fn remove_clip_note(
        &self,
        clip_id: &str,
        note_id: &str,
        now_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    /// Newest-first note ids of one clip (bounded by the per-clip cap).
    fn clip_note_ids(&self, clip_id: &str) -> Result<Vec<String>, PortError>;
    fn count_clip_notes(&self, clip_id: &str) -> Result<u64, PortError>;
    /// Clip ids containing `note_id` (`notes/clips` parity: which of the
    /// caller's clips hold this note). Defaults to empty so in-memory test
    /// doubles keep compiling; durable backends must override it.
    fn clip_ids_for_note(&self, note_id: &str) -> Result<Vec<String>, PortError> {
        let _ = note_id;
        Ok(Vec::new())
    }
    /// Favorite one clip (idempotent insert). Returns true when the row
    /// was newly added.
    fn favorite_clip(&self, fav: crate::domain::clip::ClipFavorite) -> Result<bool, PortError>;
    /// Unfavorite (idempotent). Returns true when a row existed.
    fn unfavorite_clip(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError>;
    fn is_clip_favorited(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError>;
    fn clip_favorite_count(&self, clip_id: &str) -> Result<u64, PortError>;
    /// Newest-first favorites of one user (clip ids bounded by the
    /// per-owner clip cap × small fan-out; favorites of deleted clips are
    /// skipped by the caller).
    fn clip_favorites_by_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<crate::domain::clip::ClipFavorite>, PortError>;
    /// Follow one clip (idempotent insert). Returns true when newly added.
    fn follow_clip(&self, follow: crate::domain::clip::ClipFollow) -> Result<bool, PortError>;
    /// Unfollow (idempotent). Returns true when a row existed.
    fn unfollow_clip(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError>;
    fn is_following_clip(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError>;
    /// Newest-first follower rows of one clip, bounded by `limit`.
    fn followers_of_clip(
        &self,
        clip_id: &str,
        limit: usize,
    ) -> Result<Vec<crate::domain::clip::ClipFollow>, PortError>;
    /// Clips one user follows (for timelines/lookup), newest-first.
    fn followed_clips_of(
        &self,
        user_id: &str,
    ) -> Result<Vec<crate::domain::clip::ClipFollow>, PortError>;
    /// Record a clip tombstone (`uri → deleted_at_ms`); tiny rows, kept
    /// forever in v1 (documented). Idempotent.
    fn record_clip_tombstone(&self, clip_uri: &str, deleted_at_ms: i64) -> Result<(), PortError>;
    fn clip_tombstone(&self, clip_uri: &str) -> Result<Option<i64>, PortError>;
}
