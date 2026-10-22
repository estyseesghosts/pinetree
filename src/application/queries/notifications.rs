use crate::application::commands::AppServices;
use crate::application::events::{MainEventKind, MainMessage};
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::note::Note;
use crate::domain::policy::is_visible;

impl AppServices {
    pub fn mentions_timeline(
        &self,
        viewer_id: &str,
        following_only: bool,
        visibility: Option<crate::domain::note::Visibility>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        let cache = self.follows_cache(Some(viewer_id))?;
        let authors: Option<std::collections::HashSet<String>> = if following_only {
            let mut set = self.follow_set(viewer_id)?;
            set.insert(viewer_id.to_string());
            Some(set)
        } else {
            None
        };
        let all = self.all_notes_bounded()?;
        let mut scoped = Vec::new();
        for note in all {
            if !note.mentions.iter().any(|id| id == viewer_id)
                && !note.visible_user_ids.iter().any(|id| id == viewer_id)
            {
                continue;
            }
            if let Some(authors) = authors.as_ref() {
                if !authors.contains(&note.author_id) {
                    continue;
                }
            }
            if let Some(want) = visibility {
                if note.visibility != want {
                    continue;
                }
            }
            let viewer = self.viewer_ctx(Some(viewer_id), &note.author_id, &cache);
            if !is_visible(&note, &viewer) {
                continue;
            }
            scoped.push(note);
        }
        Ok(self.apply_cursor(self.apply_feed_filter(Some(viewer_id), scoped)?, cursor))
    }

    /// `users/recommendation` (Misskey `users/recommendation.ts`
    /// documented-simplified scoring, DIVERGENCE noted in the manifest):
    /// unlocked + explorable LOCAL accounts, excluding self, already-followed,
    /// muted/blocked (either direction), suspended, and deleted, ordered by
    /// `followers_count` descending. Upstream additionally requires
    /// `updatedAt` within 7 days; pinetree stores no per-account activity
    /// timestamp, so recency is not filtered (documented).
    pub fn mark_all_notifications_read(&self, account_id: &str) -> Result<u64, PortError> {
        self.accounts.get_owner(account_id)?;
        let flipped = self.notifications.mark_all_read(account_id)?;
        self.streaming.push_main(MainMessage {
            user_id: account_id.to_string(),
            kind: MainEventKind::ReadAllNotifications,
            body: serde_json::Value::Null,
        })?;
        Ok(flipped)
    }
}
