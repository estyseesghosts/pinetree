use std::collections::HashMap;

use super::filters::REPLY_SCAN_BATCH;
use super::filters::ReactionWithNote;
use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::Account;
use crate::domain::instance::UgcVisibilityForVisitor;
use crate::domain::note::Note;
use crate::domain::policy::{ViewerContext, is_visible};
use crate::domain::reaction::Reaction;

impl AppServices {
    pub fn notes_show(&self, note_id: &str, viewer_id: Option<&str>) -> Result<Note, PortError> {
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        // Fail-closed pending withhold first (`visibility.rs`: an
        // unresolved `reply_parent_uri` reads as missing on every surface).
        // It precedes the content gates below so a pending note never
        // leaks existence through a gate error (indistinguishability).
        if note.reply_parent_uri.is_some() {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        // Batch Q Phase 1 FIX 3b (Misskey `notes/show.ts:69-79` parity):
        // an anonymous viewer reading an author with
        // `requireSigninToViewContents` gets `CONTENT_RESTRICTED_BY_USER`
        // (exact upstream code; the API layer maps it to upstream's
        // message/id). Checked before visibility, mirroring upstream order
        // (their gates run before `pack`). The server-wide
        // `ugcVisibilityForVisitor` gate follows: `none` restricts every
        // anonymous read; `local` restricts anonymous reads of remote
        // authors' notes (pinetree default is `all`; admins opt in).
        if viewer_id.is_none() {
            let author = self.accounts.find_by_id(&note.author_id)?;
            if let Some(author) = author {
                if author.require_signin_to_view_contents {
                    return Err(PortError::Validation(
                        "CONTENT_RESTRICTED_BY_USER".to_string(),
                    ));
                }
                let ugc = self
                    .instance_settings
                    .get()
                    .map(|s| s.ugc_visibility_for_visitor)
                    .unwrap_or_default();
                match ugc {
                    UgcVisibilityForVisitor::None => {
                        return Err(PortError::Validation(
                            "CONTENT_RESTRICTED_BY_SERVER".to_string(),
                        ));
                    }
                    UgcVisibilityForVisitor::Local if author.host.is_some() => {
                        return Err(PortError::Validation(
                            "CONTENT_RESTRICTED_BY_SERVER".to_string(),
                        ));
                    }
                    _ => {}
                }
            }
        }
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
        // §0A: blocked/muted/suspended/host-blocked authors (and their
        // replies/renotes) read as missing, like invisible notes.
        let filter = self.feed_filter(viewer_id)?;
        let mut account_cache: HashMap<String, Option<Account>> = HashMap::new();
        let mut note_cache: HashMap<String, Option<Note>> = HashMap::new();
        if self.feed_filter_hides(&filter, &note, &mut account_cache, &mut note_cache)? {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        Ok(note)
    }

    /// `users/show` single-user read with the suspension gate (Batch Q
    /// Phase 1 FIX 3a; Misskey `users/show.ts:174` parity): a suspended
    /// account reads as `NO_SUCH_USER` unless the viewer has moderator
    /// powers (`Account::has_moderator_powers`, admin implied). Anonymous
    /// (`None`) viewers never bypass. The un-gated `users_show_by_id` /
    /// `users_show_by_username` below keep their exact behavior for the
    /// existing internal callers; the `users/show` endpoint should call
    /// these `_as` variants with the caller's id.
    pub fn user_notes(
        &self,
        user_id: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
        with_replies: bool,
        with_renotes: bool,
        with_files: bool,
    ) -> Result<Vec<Note>, PortError> {
        self.users_show_by_id(user_id)?;
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
            if !with_replies && note.is_reply() {
                return false;
            }
            true
        })
    }

    /// `notes/user-list-timeline` (Batch O WP16): owner-only pull timeline.
    /// Merges members' newest notes (bounded: member cap × per-member page)
    /// and applies the per-membership reply rule plus the caller's include
    /// flags, then the shared visibility + mute/block page. Mirrors the
    /// upstream `getFromDb` shape (no write fan-out, DIVERGENCE 3).
    #[allow(clippy::too_many_arguments)]
    pub fn note_replies(
        &self,
        note_id: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        let page_size = match cursor.limit {
            0 => 10,
            n if n > 100 => 100,
            n => n,
        };
        let cache = self.follows_cache(viewer_id)?;
        let feed_filter = self.feed_filter(viewer_id)?;
        let mut filter_accounts: HashMap<String, Option<Account>> = HashMap::new();
        let mut filter_notes: HashMap<String, Option<Note>> = HashMap::new();
        let mut visible: Vec<Note> = Vec::new();
        let mut scan = TimelineCursor {
            since_id: cursor.since_id.clone(),
            until_id: cursor.until_id.clone(),
            since_date: cursor.since_date,
            until_date: cursor.until_date,
            limit: REPLY_SCAN_BATCH,
        };
        loop {
            let window_before = scan.until_id.clone();
            let batch = self.notes.notes_by_reply(note_id, &scan)?;
            if batch.is_empty() {
                break;
            }
            let fetched = batch.len();
            for note in batch {
                // Advance past every scanned child — visible or not — so a
                // run of hidden rows cannot stall the scan.
                scan.until_id = Some(note.id.clone());
                let viewer = self.viewer_ctx(viewer_id, &note.author_id, &cache);
                if !is_visible(&note, &viewer) {
                    continue;
                }
                // §0A: blocked/muted/suspended authors filter like
                // invisible notes.
                if self.feed_filter_hides(
                    &feed_filter,
                    &note,
                    &mut filter_accounts,
                    &mut filter_notes,
                )? {
                    continue;
                }
                visible.push(note);
                if visible.len() >= page_size {
                    break;
                }
            }
            if visible.len() >= page_size {
                break;
            }
            if fetched < REPLY_SCAN_BATCH {
                // Short batch: the parent range is exhausted.
                break;
            }
            // Defensive: the contract promises strictly-newest-first rows
            // inside the exclusive `(sinceId, untilId)` window. If a backend
            // ever returns a row that does not advance the window, stop
            // rather than looping forever.
            if !Self::scan_window_advanced(window_before.as_deref(), scan.until_id.as_deref()) {
                break;
            }
        }
        Ok(self.apply_cursor(visible, cursor))
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

    /// `notes/children`: replies plus quote-renotes (renotes carrying text,
    /// files, or a poll — mirroring Misskey's `text IS NOT NULL OR
    /// fileIds != '{}' OR hasPoll`). Like Misskey, an unknown `note_id`
    /// yields `[]`.
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
                && (note.text.is_some()
                    || !note.file_ids.is_empty()
                    || self.polls.get_poll(&note.id).ok().flatten().is_some())
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
        let feed_filter = self.feed_filter(viewer_id)?;
        let mut filter_accounts: HashMap<String, Option<Account>> = HashMap::new();
        let mut filter_notes: HashMap<String, Option<Note>> = HashMap::new();
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
            if is_visible(&parent, &viewer)
                && !self.feed_filter_hides(
                    &feed_filter,
                    &parent,
                    &mut filter_accounts,
                    &mut filter_notes,
                )?
            {
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
    /// visibility-filtered. Misskey parity: no `host` filter means the
    /// global scope (all cached hosts); `host == "."` is local-only, any
    /// other `host` value selects cached authors with exactly that host.
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
                None => {}
                Some(".") => {
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
        Ok(self.apply_cursor(self.apply_feed_filter(viewer_id, scoped)?, cursor))
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

    // --- Batch P ----------------------------------------------------------

    /// `notes/mentions` (Misskey `notes/mentions.ts` parity): notes
    /// mentioning the viewer (`mentions`) or directly addressed to them
    /// (`visibleUserIds`, i.e. specified-visibility notes). `following`
    /// restricts authors to the viewer's followees + self (upstream
    /// `following` branch); `visibility` narrows to one visibility value.
    /// Standard visibility + mute/block/thread filtering applies on top.
    pub fn recommended_notes(
        &self,
        viewer_id: Option<&str>,
        limit: usize,
        offset: usize,
        with_renotes: bool,
        with_sensitive: bool,
    ) -> Result<Vec<crate::domain::note::Note>, PortError> {
        let suggested: Vec<Account> = match viewer_id {
            Some(viewer) => self.recommended_users(viewer, 50, 0)?,
            None => Vec::new(),
        };
        let cache = self.follows_cache(viewer_id)?;
        let all = self.all_notes_bounded()?;
        let mut scoped = Vec::new();
        for note in all {
            // Anonymous (or suggestion-poor) callers fall back to every
            // visible note; credentialed callers see suggested authors'
            // notes plus their own recent notes.
            let in_scope = if suggested.is_empty() {
                true
            } else {
                suggested.iter().any(|a| a.id == note.author_id)
                    || Some(note.author_id.as_str()) == viewer_id
            };
            if !in_scope {
                continue;
            }
            if !with_renotes && Self::is_pure_renote(&note) {
                continue;
            }
            // Sharkey `withSensitive=false` parity (soft): drop notes with
            // sensitive-flagged attachments (media rows are read per
            // candidate; missing rows fail open).
            if !with_sensitive && !note.file_ids.is_empty() {
                let mut sensitive = false;
                for file_id in &note.file_ids {
                    if let Ok(Some(meta)) = self.media.get(file_id) {
                        if meta.is_sensitive {
                            sensitive = true;
                            break;
                        }
                    }
                }
                if sensitive {
                    continue;
                }
            }
            let viewer = self.viewer_ctx(viewer_id, &note.author_id, &cache);
            if !is_visible(&note, &viewer) {
                continue;
            }
            scoped.push(note);
        }
        scoped.sort_by(|a, b| b.id.cmp(&a.id));
        // Offset-then-limit over the ranked page (the feed is a
        // server-ranked queue, not an id cursor — Sharkey parity on params).
        let filtered = self.apply_feed_filter(viewer_id, scoped)?;
        Ok(filtered.into_iter().skip(offset).take(limit).collect())
    }

    /// `stats` (Misskey `stats.ts` parity on shape; values from our stores):
    /// `(notes, local_notes, users, local_users, reactions, instances,
    /// drive_local_bytes, drive_remote_bytes)`. `instances` counts distinct
    /// remote hosts among cached accounts (upstream counts its instances
    /// table). Drive sums walk media rows once, attributing by owner
    /// locality (upstream hardcodes both to 0 — pinetree reports real sums,
    /// DIVERGENCE).
    #[allow(clippy::type_complexity)]
    pub fn user_reactions(
        &self,
        user_id: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<ReactionWithNote>, PortError> {
        let target = self.users_show_by_id(user_id)?;
        if !self.viewer_is_moderator(viewer_id)? {
            if !target.is_local() {
                return Err(PortError::Validation("IS_REMOTE_USER".to_string()));
            }
            if viewer_id != Some(target.id.as_str()) && !target.public_reactions {
                return Err(PortError::Validation("REACTIONS_NOT_PUBLIC".to_string()));
            }
            if let Some(viewer) = viewer_id {
                if self.blocks.is_blocking(&target.id, viewer)? {
                    return Ok(Vec::new());
                }
            }
        }
        let reactions = self.reactions.reactions_by_user(&target.id)?;
        let follows = self.follows_cache(viewer_id)?;
        let filter = self.feed_filter(viewer_id)?;
        let mut account_cache: HashMap<String, Option<Account>> = HashMap::new();
        let mut kept: Vec<ReactionWithNote> = Vec::new();
        for reaction in reactions {
            // Upstream `leftJoin`s the note; a reaction whose note is gone
            // cannot be packed, so it is skipped (documented DIVERGENCE
            // from the join shape, invisible on the wire).
            let Some(note) = self.notes.get_note(&reaction.note_id)? else {
                continue;
            };
            if note.is_deleted {
                continue;
            }
            let viewer = self.viewer_ctx(viewer_id, &note.author_id, &follows);
            if !is_visible(&note, &viewer) {
                continue;
            }
            let mut involved: Vec<String> = vec![note.author_id.clone()];
            if let Some(reply_user_id) = note.reply_user_id.as_deref() {
                involved.push(reply_user_id.to_string());
            }
            if let Some(renote_id) = note.renote_id.as_deref() {
                if let Some(renote_target) = self.notes.get_note(renote_id)? {
                    involved.push(renote_target.author_id);
                }
            }
            let involved_refs: Vec<&str> = involved.iter().map(String::as_str).collect();
            if self.suspended_or_host_blocked(&involved_refs, &mut account_cache)? {
                continue;
            }
            if note.author_id != target.id
                && involved_refs.iter().any(|id| {
                    filter.muted.contains(*id)
                        || filter.blocking.contains(*id)
                        || filter.blocked_by.contains(*id)
                })
            {
                continue;
            }
            kept.push(ReactionWithNote { reaction, note });
        }
        kept.retain(|row| self.follow_matches_cursor(&row.reaction.id, cursor));
        kept.sort_by(|a, b| b.reaction.id.cmp(&a.reaction.id));
        kept.truncate(cursor.limit);
        Ok(kept)
    }

    /// `notes/reactions` (list reactors; Misskey `notes/reactions.ts`): the
    /// note must be visible to the viewer (`NO_SUCH_NOTE` otherwise), rows
    /// are newest-first by reaction id with the usual id/date cursor, and an
    /// optional `reaction_type` filter (`:name@.:` is the local spelling of
    /// `:name:`). Rows whose reactor account is gone are skipped.
    pub fn note_reactions(
        &self,
        note_id: &str,
        viewer_id: Option<&str>,
        reaction_type: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<(Reaction, Account)>, PortError> {
        let note = self.notes_show(note_id, viewer_id)?;
        let wanted = reaction_type
            .filter(|t| !t.is_empty())
            .map(|t| match t.strip_suffix("@.:") {
                Some(name) => format!("{name}:"),
                None => t.to_string(),
            });
        let mut rows: Vec<Reaction> = self
            .reactions
            .reactions_for_note(&note.id)?
            .into_iter()
            .filter(|r| wanted.as_deref().map_or(true, |w| r.reaction == w))
            .filter(|r| self.follow_matches_cursor(&r.id, cursor))
            .collect();
        rows.sort_by(|a, b| b.id.cmp(&a.id));
        let mut out = Vec::new();
        for reaction in rows {
            if out.len() >= cursor.limit {
                break;
            }
            if let Some(account) = self.accounts.find_by_id(&reaction.user_id)? {
                out.push((reaction, account));
            }
        }
        Ok(out)
    }
}
