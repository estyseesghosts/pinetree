use std::collections::{HashMap, HashSet};

use crate::application::commands::AppServices;
use crate::application::events::{MainEventKind, MainMessage};
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::Account;
use crate::domain::follow::Follow;
use crate::domain::note::Note;
use crate::domain::policy::{
    ViewerContext, is_eligible_for_federated_timeline, is_eligible_for_local_timeline, is_visible,
};

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

    /// Federated (global) timeline, mirroring Misskey
    /// `notes/global-timeline`: all non-`local_only` `public` notes, local
    /// and remote (visibility-policy §5, `is_eligible_for_federated_timeline`).
    /// No auth required; no reply filter — Misskey's global timeline has no
    /// `withReplies` parameter and carries replies.
    pub fn global_timeline(
        &self,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
        with_files: bool,
        with_renotes: bool,
    ) -> Result<Vec<Note>, PortError> {
        let cache = self.follows_cache(viewer_id)?;
        let all = self.notes.list_all_notes(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })?;
        let filtered: Vec<Note> = all
            .into_iter()
            .filter(|note| {
                if !is_eligible_for_federated_timeline(note) {
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

    /// Phase 3 streaming: per-note eligibility for one timeline channel,
    /// mirroring the REST timeline filters in this file exactly (same
    /// visibility + eligibility + flag logic, without cursor/pagination).
    /// `home`/`hybrid` require an authenticated viewer.
    pub fn note_eligible_for_channel(
        &self,
        channel: TimelineChannel,
        viewer_id: Option<&str>,
        note: &Note,
        params: &TimelineChannelParams,
    ) -> Result<bool, PortError> {
        if params.with_files && note.file_ids.is_empty() {
            return Ok(false);
        }
        if !params.with_renotes && Self::is_pure_renote(note) {
            return Ok(false);
        }
        match channel {
            TimelineChannel::Home => {
                let viewer = match viewer_id {
                    Some(v) => v,
                    None => return Ok(false),
                };
                let following = self.follow_set(viewer)?;
                let in_scope = note.author_id == viewer
                    || following.contains(&note.author_id)
                    || note.visible_user_ids.iter().any(|id| id == viewer)
                    || note.mentions.iter().any(|id| id == viewer);
                if !in_scope {
                    return Ok(false);
                }
                let cache: HashMap<String, bool> =
                    following.into_iter().map(|id| (id, true)).collect();
                let ctx = self.viewer_ctx(Some(viewer), &note.author_id, &cache);
                Ok(is_visible(note, &ctx))
            }
            TimelineChannel::Local => {
                if !is_eligible_for_local_timeline(note) {
                    return Ok(false);
                }
                if !params.with_replies && note.reply_id.is_some() {
                    return Ok(false);
                }
                let cache = self.follows_cache(viewer_id)?;
                let ctx = self.viewer_ctx(viewer_id, &note.author_id, &cache);
                Ok(is_visible(note, &ctx))
            }
            TimelineChannel::Hybrid => {
                let viewer = match viewer_id {
                    Some(v) => v,
                    None => return Ok(false),
                };
                let home = self.note_eligible_for_channel(
                    TimelineChannel::Home,
                    Some(viewer),
                    note,
                    params,
                )?;
                if home {
                    return Ok(!(!params.with_replies && note.reply_id.is_some()));
                }
                self.note_eligible_for_channel(
                    TimelineChannel::Local,
                    Some(viewer),
                    note,
                    params,
                )
            }
            TimelineChannel::Global => {
                if !is_eligible_for_federated_timeline(note) {
                    return Ok(false);
                }
                let cache = self.follows_cache(viewer_id)?;
                let ctx = self.viewer_ctx(viewer_id, &note.author_id, &cache);
                Ok(is_visible(note, &ctx))
            }
        }
    }

    /// Phase 3 streaming: the light visibility gate for note-capture
    /// (`noteUpdated`) events, mirroring Misskey `Connection` exactly:
    /// `specified` requires the viewer in `visible_user_ids` (or authorship);
    /// `followers` requires authorship or a follow edge. Anonymous viewers
    /// only pass `public`/`home` notes.
    pub fn capture_gate(&self, viewer_id: Option<&str>, note_id: &str) -> Result<bool, PortError> {
        let note = match self.notes.get_note(note_id)? {
            Some(note) => note,
            None => return Ok(false),
        };
        match note.visibility {
            crate::domain::note::Visibility::Specified => {
                let Some(viewer) = viewer_id else {
                    return Ok(false);
                };
                Ok(note.author_id == viewer
                    || note.visible_user_ids.iter().any(|id| id == viewer))
            }
            crate::domain::note::Visibility::Followers => {
                let Some(viewer) = viewer_id else {
                    return Ok(false);
                };
                if note.author_id == viewer {
                    return Ok(true);
                }
                Ok(self.follows.is_following(viewer, &note.author_id)?)
            }
            _ => Ok(true),
        }
    }

    // --- Phase 3 note relations / search -----------------------------------

    /// One bounded scan + visibility filter + cursor page, shared by the
    /// Phase 3 relation endpoints. `keep` pre-selects candidates; visibility
    /// (`is_visible`) always applies on top.
    fn visible_page(
        &self,
        viewer_id: Option<&str>,
        notes: Vec<Note>,
        cursor: &TimelineCursor,
        keep: impl Fn(&Note) -> bool,
    ) -> Result<Vec<Note>, PortError> {
        let cache = self.follows_cache(viewer_id)?;
        let filtered: Vec<Note> = notes
            .into_iter()
            .filter(|note| {
                if !keep(note) {
                    return false;
                }
                let viewer = self.viewer_ctx(viewer_id, &note.author_id, &cache);
                is_visible(note, &viewer)
            })
            .collect();
        Ok(self.apply_cursor(filtered, cursor))
    }

    fn all_notes_bounded(&self) -> Result<Vec<Note>, PortError> {
        self.notes.list_all_notes(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })
    }

    /// `users/notes`: one author's notes, newest-first, visibility-filtered.
    /// An unknown `user_id` yields `[]` (Misskey `users/notes` has no
    /// `NO_SUCH_USER` error).
    pub fn user_notes(
        &self,
        user_id: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
        with_replies: bool,
        with_renotes: bool,
        with_files: bool,
    ) -> Result<Vec<Note>, PortError> {
        let all = self.all_notes_bounded()?;
        self.visible_page(viewer_id, all, cursor, |note| {
            if note.author_id.as_str() != user_id {
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
    }

    /// `notes/replies`: direct replies (`reply_id == note_id`). Like Misskey,
    /// an unknown `note_id` yields `[]` (no existence check).
    pub fn note_replies(
        &self,
        note_id: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        let all = self.all_notes_bounded()?;
        self.visible_page(viewer_id, all, cursor, |note| {
            note.reply_id.as_deref() == Some(note_id)
        })
    }

    /// `notes/renotes`: every renote of the note, quotes included. Like
    /// Misskey, an unknown `note_id` is `NO_SUCH_NOTE`.
    pub fn note_renotes(
        &self,
        note_id: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        if self.notes.get_note(note_id)?.is_none() {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        let all = self.all_notes_bounded()?;
        self.visible_page(viewer_id, all, cursor, |note| {
            note.renote_id.as_deref() == Some(note_id)
        })
    }

    /// `notes/children`: replies plus quote-renotes (renotes carrying text
    /// or files, mirroring Misskey's `text IS NOT NULL OR fileIds != '{}'`;
    /// we have no polls so the `hasPoll` disjunct does not apply). Like
    /// Misskey, an unknown `note_id` yields `[]`.
    pub fn note_children(
        &self,
        note_id: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        let all = self.all_notes_bounded()?;
        self.visible_page(viewer_id, all, cursor, |note| {
            if note.reply_id.as_deref() == Some(note_id) {
                return true;
            }
            note.renote_id.as_deref() == Some(note_id)
                && (note.text.is_some() || !note.file_ids.is_empty())
        })
    }

    /// `notes/conversation`: the ancestor chain above the note via
    /// `replyId`, oldest-first. The target itself goes through `notes_show`
    /// (unknown or invisible-to-viewer is `NO_SUCH_NOTE`); ancestors are
    /// walked newest-to-oldest, invisible ones skipped but walked through,
    /// then `offset`/`limit` apply over the visible chain. The walk stops at
    /// a missing note or a depth guard (cycles must never hang the request).
    pub fn note_conversation(
        &self,
        note_id: &str,
        viewer_id: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Note>, PortError> {
        let target = self.notes_show(note_id, viewer_id)?;
        let cache = self.follows_cache(viewer_id)?;
        let mut chain: Vec<Note> = Vec::new();
        let mut current = target.reply_id.clone();
        let mut depth = 0usize;
        while let Some(id) = current {
            depth += 1;
            if depth > 512 {
                break;
            }
            let Some(parent) = self.notes.get_note(&id)? else {
                break;
            };
            let viewer = self.viewer_ctx(viewer_id, &parent.author_id, &cache);
            if is_visible(&parent, &viewer) {
                chain.push(parent.clone());
            }
            // No early exit on `limit`: oldest-first ordering takes from the
            // far end of the chain, so the whole (depth-guarded) walk is
            // needed before `offset`/`limit` apply.
            current = parent.reply_id.clone();
        }
        chain.reverse();
        Ok(chain.into_iter().skip(offset).take(limit).collect())
    }

    /// `notes/search`: case-insensitive substring match over `text`+`cw`,
    /// visibility-filtered. The corpus is local notes only (profile §3);
    /// `host == "."` is the same local-only scope, while any other `host`
    /// value selects cached authors with exactly that host.
    pub fn search_notes(
        &self,
        query: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
        user_id: Option<&str>,
        host: Option<&str>,
    ) -> Result<Vec<Note>, PortError> {
        let needle = query.to_lowercase();
        let all = self.all_notes_bounded()?;
        let cache = self.follows_cache(viewer_id)?;
        let mut scoped: Vec<Note> = Vec::new();
        for note in all {
            if let Some(uid) = user_id {
                if note.author_id.as_str() != uid {
                    continue;
                }
            }
            let author = match self.accounts.find_by_id(&note.author_id)? {
                Some(account) => account,
                None => continue,
            };
            match host {
                None | Some(".") => {
                    if !author.is_local() {
                        continue;
                    }
                }
                Some(h) => {
                    if author.host.as_deref() != Some(h) {
                        continue;
                    }
                }
            }
            let mut haystack = note.text.clone().unwrap_or_default();
            if let Some(cw) = note.cw.as_ref() {
                haystack.push('\n');
                haystack.push_str(cw);
            }
            if !haystack.to_lowercase().contains(&needle) {
                continue;
            }
            let viewer = self.viewer_ctx(viewer_id, &note.author_id, &cache);
            if !is_visible(&note, &viewer) {
                continue;
            }
            scoped.push(note);
        }
        Ok(self.apply_cursor(scoped, cursor))
    }

    /// `notes/search-by-tag`: notes carrying `tag` (case-insensitive exact
    /// match on one extracted tag), visibility-filtered, newest-first.
    pub fn notes_with_tag(
        &self,
        tag: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        let needle = tag.to_lowercase();
        let all = self.all_notes_bounded()?;
        self.visible_page(viewer_id, all, cursor, |note| {
            note.tags.iter().any(|t| t.to_lowercase() == needle)
        })
    }

    // --- Phase 3 follow lists ----------------------------------------------

    fn follow_matches_cursor(&self, id: &str, cursor: &TimelineCursor) -> bool {
        if let Some(since_id) = cursor.since_id.as_deref() {
            if id <= since_id {
                return false;
            }
        }
        if let Some(until_id) = cursor.until_id.as_deref() {
            if id >= until_id {
                return false;
            }
        }
        if cursor.since_date.is_some() || cursor.until_date.is_some() {
            if let Ok(ts) = self.ids.parse_time(id) {
                if let Some(since_date) = cursor.since_date {
                    if ts <= since_date {
                        return false;
                    }
                }
                if let Some(until_date) = cursor.until_date {
                    if ts >= until_date {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn apply_follow_cursor(
        &self,
        follows: Vec<Follow>,
        cursor: &TimelineCursor,
    ) -> Vec<Follow> {
        let mut out: Vec<Follow> = follows
            .into_iter()
            .filter(|f| self.follow_matches_cursor(&f.id, cursor))
            .collect();
        out.sort_by(|a, b| b.id.cmp(&a.id));
        out.truncate(cursor.limit);
        out
    }

    /// `users/followers`: follow edges where `user_id` is the followee,
    /// newest-first with full cursor paging.
    pub fn user_followers(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Follow>, PortError> {
        let all = self.follows.followers_of(user_id, None, 10_000)?;
        Ok(self.apply_follow_cursor(all, cursor))
    }

    /// `users/following`: follow edges where `user_id` is the follower,
    /// newest-first with full cursor paging.
    pub fn user_following(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Follow>, PortError> {
        let all = self.follows.following_of(user_id, None, 10_000)?;
        Ok(self.apply_follow_cursor(all, cursor))
    }

    /// Mark every notification of `account_id` read and fan out the
    /// `readAllNotifications` main event. Shared by `i/notifications`
    /// (`markAsRead`) and `notifications/mark-all-as-read`. (A write op
    /// co-located with the read queries to avoid a new module for one
    /// method.)
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

/// Phase 3 streaming timeline channel (profile §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelineChannel {
    Home,
    Local,
    Hybrid,
    Global,
}

/// Streaming channel filter flags (Misskey defaults).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineChannelParams {
    pub with_files: bool,
    pub with_renotes: bool,
    pub with_replies: bool,
}

impl Default for TimelineChannelParams {
    fn default() -> Self {
        Self {
            with_files: false,
            with_renotes: true,
            with_replies: false,
        }
    }
}
