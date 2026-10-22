use std::collections::{HashMap, HashSet};

use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::Account;
use crate::domain::follow::Follow;
use crate::domain::note::Note;
use crate::domain::policy::{ViewerContext, is_visible};
use crate::domain::reaction::Reaction;

/// Candidates fetched per `NoteStore::notes_by_reply` call inside the
/// `note_replies` scan loop (`docs/replies.md` §3.3): a bounded batch that
/// keeps each storage round-trip small while refilling visible pages past
/// hidden rows.
pub(crate) const REPLY_SCAN_BATCH: usize = 50;

/// §0A read-time filter sets for one feed reader (Misskey `QueryService`
/// `generate*FilteringQuery` parity, in-memory form). Built once per read
/// via [`AppServices::feed_filter`]; [`AppServices::feed_filter_hides`]
/// applies it per note.
pub struct FeedFilter {
    pub viewer_id: Option<String>,
    /// Active mutee ids (the viewer muted them).
    pub muted: HashSet<String>,
    /// Ids the viewer blocks.
    pub blocking: HashSet<String>,
    /// Ids blocking the viewer.
    pub blocked_by: HashSet<String>,
    /// Ids whose pure renotes the viewer hides.
    pub renote_muted: HashSet<String>,
    /// Lowercased hosts the viewer muted (`mutedInstances`, §12; Misskey
    /// `generateMutedUserQueryForNotes` instance parity). Local authors (no
    /// host) never match. Empty for anonymous viewers.
    pub muted_instances: Vec<String>,
    /// Viewer-compiled hard word mutes (Batch N WS8; `hardMutedWords`).
    /// Empty for anonymous viewers or viewers with no hard mutes (the
    /// common case — the hard-mute arm of `feed_filter_hides` is then a
    /// single `is_empty` check, zero per-note regex cost).
    pub hard_mutes: crate::domain::word_mute::CompiledWordMute,
}

impl AppServices {
    pub(crate) fn viewer_ctx(
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
    pub(crate) fn follow_set(&self, viewer_id: &str) -> Result<HashSet<String>, PortError> {
        let following = self.follows.following_of(viewer_id, None, 10_000)?;
        Ok(following.into_iter().map(|f| f.followee_id).collect())
    }

    pub(crate) fn follows_cache(
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

    /// §0A read-time filter sets for one viewer (Misskey `QueryService`
    /// `generate*FilteringQuery` parity, in-memory form). `None` viewer =
    /// anonymous: only suspended/host-blocked authors filter.
    pub fn feed_filter(&self, viewer_id: Option<&str>) -> Result<FeedFilter, PortError> {
        let mut filter = FeedFilter {
            viewer_id: viewer_id.map(str::to_string),
            muted: HashSet::new(),
            blocking: HashSet::new(),
            blocked_by: HashSet::new(),
            renote_muted: HashSet::new(),
            muted_instances: Vec::new(),
            hard_mutes: crate::domain::word_mute::CompiledWordMute::default(),
        };
        if let Some(viewer) = viewer_id {
            filter.muted = self.mutes.muted_ids_for(viewer)?.into_iter().collect();
            filter.renote_muted = self
                .mutes
                .renote_muted_ids_for(viewer)?
                .into_iter()
                .collect();
            filter.blocking = self.blocks.blocking_ids_for(viewer)?.into_iter().collect();
            filter.blocked_by = self
                .blocks
                .blocked_by_ids_for(viewer)?
                .into_iter()
                .collect();
            // §12 `mutedInstances` + Batch N WS8 hard word mutes: the
            // viewer's own lists (lowercased hosts; missing viewers fail
            // open to empty lists).
            if let Some(account) = self.accounts.find_by_id(viewer)? {
                filter.muted_instances = account
                    .muted_instances
                    .into_iter()
                    .map(|host| host.to_lowercase())
                    .collect();
                filter.hard_mutes =
                    crate::domain::word_mute::CompiledWordMute::compile(&account.hard_muted_words);
            }
        }
        Ok(filter)
    }

    /// Instance-level blocked hosts (§0A `generateBlockedHostQueryForNote`
    /// parity). Reads the store (Batch M WP5); entries are already
    /// normalized (lowercased, deduped, empties dropped).
    pub fn blocked_hosts(&self) -> Vec<String> {
        self.instance_settings
            .get()
            .map(|s| s.blocked_hosts)
            .unwrap_or_default()
    }

    /// True when `account_id` sits on a blocked host (or subdomain).
    /// Used by inbound handlers to drop before any state write or dedup
    /// record (Batch M WP5). Fail-open on lookup errors.
    pub fn inbound_host_blocked(&self, account_id: &str) -> bool {
        match self.accounts.find_by_id(account_id) {
            Ok(Some(acc)) => {
                if let Some(host) = acc.host.as_deref() {
                    crate::domain::instance::host_is_blocked(&self.blocked_hosts(), host)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// True when `note` must be hidden from the filter's viewer (§0A;
    /// Misskey `generateBaseNoteFilteringQuery` parity):
    /// - the viewer muted / blocks / is blocked by the note author, the
    ///   reply author, or the renote author (an unresolvable renote target
    ///   behaves like upstream's NULL `renoteUserId` and does not hide);
    /// - the note is a pure renote (`renoteId` set, `text` null — the exact
    ///   upstream `generateMutedUserRenotesQueryForNotes` shape) by a
    ///   renote-muted author;
    /// - the viewer muted the instance of any of those authors (§12;
    ///   Misskey `generateMutedUserQueryForNotes` instance parity —
    ///   local authors have no host and never match);
    /// - any of those authors is suspended or on a blocked host (applies
    ///   anonymously too).
    ///
    /// `account_cache` memoizes author lookups across a single read so a
    /// feed scan does one storage read per distinct author; `note_cache`
    /// does the same for reply/renote bodies (hard word mutes).
    pub fn feed_filter_hides(
        &self,
        filter: &FeedFilter,
        note: &Note,
        account_cache: &mut HashMap<String, Option<Account>>,
        note_cache: &mut HashMap<String, Option<Note>>,
    ) -> Result<bool, PortError> {
        // The renote author's id comes from the target row; a missing
        // target behaves like upstream's NULL `renoteUserId` (no hiding).
        let renote_author: Option<String> = match note.renote_id.as_deref() {
            Some(renote_id) => self
                .notes
                .get_note(renote_id)?
                .map(|target| target.author_id),
            None => None,
        };
        let mut involved: Vec<&str> = vec![note.author_id.as_str()];
        if let Some(reply_user_id) = note.reply_user_id.as_deref() {
            involved.push(reply_user_id);
        }
        if let Some(author) = renote_author.as_deref() {
            involved.push(author);
        }
        if let Some(viewer) = filter.viewer_id.as_deref() {
            // Renote-mutes hide only pure renotes (quotes stay visible).
            if note.renote_id.is_some()
                && note.text.is_none()
                && filter.renote_muted.contains(note.author_id.as_str())
            {
                return Ok(true);
            }
            for author_id in &involved {
                if *author_id == viewer {
                    continue;
                }
                if filter.muted.contains(*author_id)
                    || filter.blocking.contains(*author_id)
                    || filter.blocked_by.contains(*author_id)
                {
                    return Ok(true);
                }
            }
            // §12 `mutedInstances`: hide notes whose author, reply author,
            // or renote author sits on a muted instance. Hosts resolve
            // through the shared author cache (one storage read per
            // distinct author per feed scan).
            if !filter.muted_instances.is_empty()
                && self.any_involved_host_muted(&involved, filter, account_cache)?
            {
                return Ok(true);
            }
        }
        // Suspended / host-blocked authors (anonymous viewers included).
        if self.suspended_or_host_blocked(&involved, account_cache)? {
            return Ok(true);
        }
        // Hard word mutes (Batch N WS8): transitive match over the note,
        // its reply, and its renote (self-notes exempt inside the matcher).
        // Last on purpose — reply/renote bodies cost storage reads, so
        // already-hidden notes never pay for them.
        if !filter.hard_mutes.is_empty() && self.note_hard_muted(filter, note, note_cache)? {
            return Ok(true);
        }
        Ok(false)
    }

    /// Fetch one note through a per-read cache (shared across a page/scan
    /// so repeated reply/renote targets read storage once).
    fn cached_note(
        &self,
        note_cache: &mut HashMap<String, Option<Note>>,
        note_id: &str,
    ) -> Result<Option<Note>, PortError> {
        if let Some(hit) = note_cache.get(note_id) {
            return Ok(hit.clone());
        }
        let note = self.notes.get_note(note_id)?;
        note_cache.insert(note_id.to_string(), note.clone());
        Ok(note)
    }

    /// Transitive hard-mute check (`checkNoteWordMute` parity): the note
    /// itself, then its reply, then its renote — first match hides.
    /// Pending (unresolved-parent) replies carry no body and are skipped.
    fn note_hard_muted(
        &self,
        filter: &FeedFilter,
        note: &Note,
        note_cache: &mut HashMap<String, Option<Note>>,
    ) -> Result<bool, PortError> {
        let Some(viewer) = filter.viewer_id.as_deref() else {
            return Ok(false);
        };
        if filter.hard_mutes.matches_note(
            note.cw.as_deref(),
            note.text.as_deref(),
            &note.author_id,
            viewer,
        ) {
            return Ok(true);
        }
        if let Some(reply_id) = note.reply_id.as_deref() {
            if let Some(reply) = self.cached_note(note_cache, reply_id)? {
                if !reply.is_deleted
                    && filter.hard_mutes.matches_note(
                        reply.cw.as_deref(),
                        reply.text.as_deref(),
                        &reply.author_id,
                        viewer,
                    )
                {
                    return Ok(true);
                }
            }
        }
        if let Some(renote_id) = note.renote_id.as_deref() {
            if let Some(target) = self.cached_note(note_cache, renote_id)? {
                if !target.is_deleted
                    && filter.hard_mutes.matches_note(
                        target.cw.as_deref(),
                        target.text.as_deref(),
                        &target.author_id,
                        viewer,
                    )
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// True when any of `involved` authors resolves to an account whose
    /// host (lowercased) is in the viewer's `muted_instances` list (§12;
    /// Misskey `generateMutedUserQueryForNotes` instance parity: exact host
    /// match over note/reply/renote authors, local authors never match).
    /// Shares `account_cache` with the suspended/host-blocked check so one
    /// read serves both.
    fn any_involved_host_muted(
        &self,
        involved: &[&str],
        filter: &FeedFilter,
        account_cache: &mut HashMap<String, Option<Account>>,
    ) -> Result<bool, PortError> {
        for author_id in involved {
            let cached = match account_cache.get(*author_id) {
                Some(hit) => hit.clone(),
                None => {
                    let account = self.accounts.find_by_id(author_id)?;
                    account_cache.insert((*author_id).to_string(), account.clone());
                    account
                }
            };
            if let Some(host) = cached.and_then(|account| account.host) {
                let host = host.to_lowercase();
                if filter.muted_instances.iter().any(|m| m == &host) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Suspended or blocked-host authors among `involved` (Misskey
    /// `generateSuspendedUserQueryForNote` + `generateBlockedHostQueryForNote`
    /// parity; applies to anonymous viewers too). Shared by
    /// [`AppServices::feed_filter_hides`] and the `users/reactions` read
    /// path (which needs this without the mute/block/renote-mute rules).
    pub(crate) fn suspended_or_host_blocked(
        &self,
        involved: &[&str],
        account_cache: &mut HashMap<String, Option<Account>>,
    ) -> Result<bool, PortError> {
        let blocked_hosts = self.blocked_hosts();
        for author_id in involved {
            let cached = match account_cache.get(*author_id) {
                Some(hit) => hit.clone(),
                None => {
                    let account = self.accounts.find_by_id(author_id)?;
                    account_cache.insert((*author_id).to_string(), account.clone());
                    account
                }
            };
            let Some(account) = cached else {
                continue;
            };
            if account.is_suspended {
                return Ok(true);
            }
            if let Some(host) = account.host.as_deref() {
                if crate::domain::instance::host_is_blocked(&blocked_hosts, host) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Apply the §0A read-time filter to one already-visibility-filtered
    /// page of candidates. Kept out of the inline `filter` closures above
    /// so storage errors propagate with `?` instead of being swallowed.
    pub(crate) fn apply_feed_filter(
        &self,
        viewer_id: Option<&str>,
        notes: Vec<Note>,
    ) -> Result<Vec<Note>, PortError> {
        let filter = self.feed_filter(viewer_id)?;
        let mut account_cache: HashMap<String, Option<Account>> = HashMap::new();
        let mut note_cache: HashMap<String, Option<Note>> = HashMap::new();
        let mut out = Vec::with_capacity(notes.len());
        for note in notes {
            if self.feed_filter_hides(&filter, &note, &mut account_cache, &mut note_cache)? {
                continue;
            }
            out.push(note);
        }
        Ok(out)
    }

    fn note_time_ms(&self, note: &Note) -> i64 {
        self.ids.parse_time(&note.id).unwrap_or(0)
    }

    pub(crate) fn apply_cursor(&self, notes: Vec<Note>, cursor: &TimelineCursor) -> Vec<Note> {
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

    pub(crate) fn is_pure_renote(note: &Note) -> bool {
        note.renote_id.is_some()
            && note.text.is_none()
            && note.cw.is_none()
            && note.file_ids.is_empty()
    }

    /// §12 self-reply exception (Misskey `replyId IS NULL OR (replyId NOT
    /// NULL AND replyUserId = userId)` parity): `with_replies=false` drops
    /// replies but keeps self-replies (a reply to one's own note). Pending
    /// (unresolved-parent) replies carry no `reply_user_id` and never count
    /// as self-replies — `is_visible` already withholds them everywhere.
    pub(crate) fn keeps_reply(note: &Note, with_replies: bool) -> bool {
        with_replies
            || !note.is_reply()
            || note.reply_user_id.as_deref() == Some(note.author_id.as_str())
    }

    pub(crate) fn renote_excluded_by_include_flags(
        &self,
        note: &Note,
        viewer_id: &str,
        include_my_renotes: bool,
        include_renoted_my_notes: bool,
        include_local_renotes: bool,
    ) -> bool {
        let Some(target_id) = note.renote_id.as_deref() else {
            return false;
        };
        let has_content = note.text.as_deref().is_some_and(|t| !t.is_empty())
            || !note.file_ids.is_empty()
            || self.polls.get_poll(&note.id).ok().flatten().is_some();
        if has_content {
            return false;
        }
        if !include_my_renotes && note.author_id == viewer_id {
            return true;
        }
        if !include_renoted_my_notes || !include_local_renotes {
            if let Ok(Some(target)) = self.notes.get_note(target_id) {
                if !include_renoted_my_notes && target.author_id == viewer_id {
                    return true;
                }
                if !include_local_renotes {
                    let target_author_host = self
                        .accounts
                        .find_by_id(&target.author_id)
                        .ok()
                        .flatten()
                        .and_then(|a| a.host);
                    if target_author_host.is_none() {
                        return true;
                    }
                }
            }
        }
        false
    }

    pub(crate) fn visible_page(
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
        Ok(self.apply_cursor(self.apply_feed_filter(viewer_id, filtered)?, cursor))
    }

    pub(crate) fn all_notes_bounded(&self) -> Result<Vec<Note>, PortError> {
        self.notes.list_all_notes(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })
    }

    /// True when the `note_replies` scan window moved strictly downward:
    /// the new exclusive `untilId` sits below the previous bound, so the
    /// next candidate batch cannot repeat rows.
    pub(crate) fn scan_window_advanced(previous: Option<&str>, current: Option<&str>) -> bool {
        match (previous, current) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(prev), Some(cur)) => cur < prev,
        }
    }

    /// `users/notes`: one author's notes, newest-first, visibility-filtered.
    /// Unknown `user_id` is `NO_SUCH_USER` (Misskey parity).
    pub(crate) fn follow_matches_cursor(&self, id: &str, cursor: &TimelineCursor) -> bool {
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

    pub(crate) fn apply_follow_cursor(
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
}

/// Which follow list a §1 visibility gate applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowList {
    Followers,
    Following,
}

/// One `users/reactions` row: the reaction plus its reacted-to note.
#[derive(Debug, Clone)]
pub struct ReactionWithNote {
    pub reaction: Reaction,
    pub note: Note,
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
