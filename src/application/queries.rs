use std::collections::{HashMap, HashSet};

use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::Account;
use crate::domain::note::Note;
use crate::domain::policy::{ViewerContext, is_eligible_for_local_timeline, is_visible};

impl AppServices {
    fn viewer_ctx(
        &self,
        viewer_id: Option<&str>,
        author_id: &str,
        follows_cache: &HashMap<String, bool>,
    ) -> ViewerContext {
        match viewer_id {
            None => ViewerContext::anonymous(),
            Some(viewer) => {
                let follows_author = if viewer == author_id {
                    true
                } else {
                    follows_cache.get(author_id).copied().unwrap_or(false)
                };
                ViewerContext::local_user(viewer, follows_author)
            }
        }
    }

    /// Follow set of `viewer_id` for visibility checks. Failures propagate;
    /// callers that cannot tolerate that should pre-filter instead.
    fn follow_set(&self, viewer_id: &str) -> Result<HashSet<String>, PortError> {
        let following = self.follows.following_of(viewer_id, None, 10_000)?;
        Ok(following.into_iter().map(|f| f.followee_id).collect())
    }

    fn follows_cache(
        &self,
        viewer_id: Option<&str>,
    ) -> Result<HashMap<String, bool>, PortError> {
        match viewer_id {
            None => Ok(HashMap::new()),
            Some(viewer) => {
                let set = self.follow_set(viewer)?;
                Ok(set.into_iter().map(|id| (id, true)).collect())
            }
        }
    }

    fn note_time_ms(&self, note: &Note) -> i64 {
        self.ids.parse_time(&note.id).unwrap_or(0)
    }

    fn apply_cursor(&self, notes: Vec<Note>, cursor: &TimelineCursor) -> Vec<Note> {
        let mut out: Vec<Note> = notes
            .into_iter()
            .filter(|note| {
                if let Some(since_id) = cursor.since_id.as_deref() {
                    if note.id.as_str() <= since_id {
                        return false;
                    }
                }
                if let Some(until_id) = cursor.until_id.as_deref() {
                    if note.id.as_str() >= until_id {
                        return false;
                    }
                }
                if let Some(since_date) = cursor.since_date {
                    if self.note_time_ms(note) <= since_date {
                        return false;
                    }
                }
                if let Some(until_date) = cursor.until_date {
                    if self.note_time_ms(note) >= until_date {
                        return false;
                    }
                }
                true
            })
            .collect();
        out.sort_by(|a, b| b.id.cmp(&a.id));
        out.truncate(cursor.limit);
        out
    }

    fn is_pure_renote(note: &Note) -> bool {
        note.renote_id.is_some()
            && note.text.is_none()
            && note.cw.is_none()
            && note.file_ids.is_empty()
    }

    pub fn notes_show(
        &self,
        note_id: &str,
        viewer_id: Option<&str>,
    ) -> Result<Note, PortError> {
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        let viewer = match viewer_id {
            None => ViewerContext::anonymous(),
            Some(viewer) => {
                let follows_author = if viewer == note.author_id {
                    true
                } else {
                    self.follows.is_following(viewer, &note.author_id)?
                };
                ViewerContext::local_user(viewer, follows_author)
            }
        };
        if !is_visible(&note, &viewer) {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        Ok(note)
    }

    pub fn users_show_by_id(&self, user_id: &str) -> Result<Account, PortError> {
        match self.accounts.find_by_id(user_id)? {
            None => Err(PortError::NotFound("NO_SUCH_USER".to_string())),
            Some(account) => Ok(account),
        }
    }

    pub fn users_show_by_username(
        &self,
        username: &str,
        host: Option<&str>,
    ) -> Result<Account, PortError> {
        match self
            .accounts
            .find_by_username_and_host(username, host)?
        {
            None => Err(PortError::NotFound("NO_SUCH_USER".to_string())),
            Some(account) => Ok(account),
        }
    }

    pub fn home_timeline(
        &self,
        viewer_id: &str,
        cursor: &TimelineCursor,
        with_files: bool,
        with_renotes: bool,
    ) -> Result<Vec<Note>, PortError> {
        let following = self.follow_set(viewer_id)?;
        let mut authors = following;
        authors.insert(viewer_id.to_string());

        let cache: HashMap<String, bool> =
            authors.iter().cloned().map(|id| (id, true)).collect();
        let all = self.notes.list_all_notes(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })?;
        let filtered: Vec<Note> = all
            .into_iter()
            .filter(|note| {
                let in_scope = authors.contains(&note.author_id)
                    || note.visible_user_ids.iter().any(|id| id == viewer_id)
                    || note.mentions.iter().any(|id| id == viewer_id);
                if !in_scope {
                    return false;
                }
                let viewer = self.viewer_ctx(Some(viewer_id), &note.author_id, &cache);
                if !is_visible(note, &viewer) {
                    return false;
                }
                if with_files && note.file_ids.is_empty() {
                    return false;
                }
                if !with_renotes && Self::is_pure_renote(note) {
                    return false;
                }
                true
            })
            .collect();
        Ok(self.apply_cursor(filtered, cursor))
    }

    pub fn local_timeline(
        &self,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
        with_files: bool,
        with_renotes: bool,
        with_replies: bool,
    ) -> Result<Vec<Note>, PortError> {
        let cache = self.follows_cache(viewer_id)?;
        let all = self.notes.list_all_notes(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })?;
        let filtered: Vec<Note> = all
            .into_iter()
            .filter(|note| {
                if !is_eligible_for_local_timeline(note) {
                    return false;
                }
                let viewer = self.viewer_ctx(viewer_id, &note.author_id, &cache);
                if !is_visible(note, &viewer) {
                    return false;
                }
                if with_files && note.file_ids.is_empty() {
                    return false;
                }
                if !with_renotes && Self::is_pure_renote(note) {
                    return false;
                }
                if !with_replies && note.reply_id.is_some() {
                    return false;
                }
                true
            })
            .collect();
        Ok(self.apply_cursor(filtered, cursor))
    }

    pub fn hybrid_timeline(
        &self,
        viewer_id: &str,
        cursor: &TimelineCursor,
        with_files: bool,
        with_renotes: bool,
        with_replies: bool,
    ) -> Result<Vec<Note>, PortError> {
        // Fetch generous windows from both sources, then merge.
        let wide = TimelineCursor {
            since_id: cursor.since_id.clone(),
            until_id: cursor.until_id.clone(),
            since_date: cursor.since_date,
            until_date: cursor.until_date,
            limit: 10_000,
        };
        let home = self.home_timeline(viewer_id, &wide, with_files, with_renotes)?;
        let local = self.local_timeline(Some(viewer_id), &wide, with_files, with_renotes, with_replies)?;
        let mut seen: HashSet<String> = HashSet::new();
        let mut merged: Vec<Note> = Vec::new();
        for note in home.into_iter().chain(local) {
            // Home notes are not subject to the reply filter; apply it here
            // for notes that only came from the home side.
            if !with_replies && note.reply_id.is_some() {
                continue;
            }
            if seen.insert(note.id.clone()) {
                merged.push(note);
            }
        }
        merged.sort_by(|a, b| b.id.cmp(&a.id));
        merged.truncate(cursor.limit);
        Ok(merged)
    }
}
