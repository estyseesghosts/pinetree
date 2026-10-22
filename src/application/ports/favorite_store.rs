use super::job_queue::NewJob;
use super::note_store::TimelineCursor;
use crate::domain::id::PortError;

/// Note favorites (`notes/favorites/*`, `i/favorites`, `notes/state`;
/// Misskey `MiNoteFavorite` parity: unique over (user, note)).
pub trait FavoriteStore: Send + Sync {
    fn favorite_note(
        &self,
        favorite: crate::domain::favorite::NoteFavorite,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::favorite::NoteFavorite, PortError>;
    fn unfavorite_note(&self, user_id: &str, note_id: &str) -> Result<bool, PortError>;
    fn is_favorited(&self, user_id: &str, note_id: &str) -> Result<bool, PortError>;
    /// Favorites of `user_id`, newest-first (favorite-id order), cursor-paged.
    fn favorites_by_user(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<crate::domain::favorite::NoteFavorite>, PortError>;
}
