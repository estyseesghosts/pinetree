//! Note favorites (`notes/favorites/*`, `i/favorites`, `notes/state`).
//!
//! Misskey parity: `notes/favorites/create.ts` (errors `NO_SUCH_NOTE`,
//! `ALREADY_FAVORITED`), `delete.ts` (`NO_SUCH_NOTE`, `NOT_FAVORITED`),
//! `i/favorites.ts` (cursor list), `notes/state.ts`
//! (`{isFavorited, isMutedThread}`). Thread-muting stays out (guide §8
//! keeps it out); `notes/state` answers `isMutedThread: false` always.

use super::AppServices;

use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::favorite::NoteFavorite;
use crate::domain::policy::{ViewerContext, is_visible};

impl AppServices {
    fn visible_note_or_no_such(
        &self,
        viewer_id: &str,
        note_id: &str,
    ) -> Result<crate::domain::note::Note, PortError> {
        let note = match self.notes.get_note(note_id)? {
            Some(n) if !n.is_deleted => n,
            _ => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        };
        let follows_author =
            viewer_id == note.author_id || self.follows.is_following(viewer_id, &note.author_id)?;
        let viewer = ViewerContext::local_user(viewer_id, follows_author);
        if !is_visible(&note, &viewer) {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        Ok(note)
    }

    /// Favorite a note (create path checks visibility → `NO_SUCH_NOTE`;
    /// duplicates → `ALREADY_FAVORITED` from the store).
    pub fn favorite_note(&self, user_id: &str, note_id: &str) -> Result<NoteFavorite, PortError> {
        self.accounts.get_owner(user_id)?;
        let note = self.visible_note_or_no_such(user_id, note_id)?;
        let fav = NoteFavorite {
            id: self.ids.generate(),
            user_id: user_id.to_string(),
            note_id: note.id,
        };
        fav.validate()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        self.favorites.favorite_note(fav, Vec::new())
    }

    /// Unfavorite a note (delete path checks existence only, no visibility
    /// gate — upstream `delete.ts` asymmetry; missing row → `NOT_FAVORITED`).
    pub fn unfavorite_note(&self, user_id: &str, note_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        match self.notes.get_note(note_id)? {
            Some(n) if !n.is_deleted => {}
            _ => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        }
        if !self.favorites.unfavorite_note(user_id, note_id)? {
            return Err(PortError::Validation("NOT_FAVORITED".to_string()));
        }
        Ok(())
    }

    pub fn favorites_for_user(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<NoteFavorite>, PortError> {
        self.accounts.get_owner(user_id)?;
        self.favorites.favorites_by_user(user_id, cursor)
    }
}
