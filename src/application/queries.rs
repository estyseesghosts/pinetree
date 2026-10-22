use std::collections::{HashMap, HashSet};

use crate::application::commands::AppServices;
use crate::application::events::{MainEventKind, MainMessage};
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::{Account, FollowVisibility};
use crate::domain::follow::Follow;
use crate::domain::instance::UgcVisibilityForVisitor;
use crate::domain::note::Note;
use crate::domain::reaction::Reaction;
use crate::domain::policy::{
    ViewerContext, is_eligible_for_federated_timeline, is_eligible_for_local_timeline, is_visible,
};

/// Candidates fetched per `NoteStore::notes_by_reply` call inside the
/// `note_replies` scan loop (`docs/replies.md` §3.3): a bounded batch that
/// keeps each storage round-trip small while refilling visible pages past
/// hidden rows.
const REPLY_SCAN_BATCH: usize = 50;

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
    fn follow_set(&self, viewer_id: &str) -> Result<HashSet<String>, PortError> {
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
            filter.blocking = self
                .blocks
                .blocking_ids_for(viewer)?
                .into_iter()
                .collect();
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
        self.instance_settings.get().map(|s| s.blocked_hosts).unwrap_or_default()
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
        if !filter.hard_mutes.is_empty()
            && self.note_hard_muted(filter, note, note_cache)?
        {
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
    fn suspended_or_host_blocked(
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
    fn apply_feed_filter(
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

    /// §12 self-reply exception (Misskey `replyId IS NULL OR (replyId NOT
    /// NULL AND replyUserId = userId)` parity): `with_replies=false` drops
    /// replies but keeps self-replies (a reply to one's own note). Pending
    /// (unresolved-parent) replies carry no `reply_user_id` and never count
    /// as self-replies — `is_visible` already withholds them everywhere.
    fn keeps_reply(note: &Note, with_replies: bool) -> bool {
        with_replies
            || !note.is_reply()
            || note.reply_user_id.as_deref() == Some(note.author_id.as_str())
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
    pub fn users_show_by_id_as(
        &self,
        user_id: &str,
        viewer_id: Option<&str>,
    ) -> Result<Account, PortError> {
        let account = match self.accounts.find_by_id(user_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_USER".to_string())),
            Some(account) => account,
        };
        if account.is_suspended && !self.viewer_is_moderator(viewer_id)? {
            return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
        }
        Ok(account)
    }

    /// Username/host sibling of [`AppServices::users_show_by_id_as`]
    /// (same suspension gate + moderator bypass).
    pub fn users_show_by_username_as(
        &self,
        username: &str,
        host: Option<&str>,
        viewer_id: Option<&str>,
    ) -> Result<Account, PortError> {
        let account = match self
            .accounts
            .find_by_username_and_host(username, host)?
        {
            None => return Err(PortError::NotFound("NO_SUCH_USER".to_string())),
            Some(account) => account,
        };
        if account.is_suspended && !self.viewer_is_moderator(viewer_id)? {
            return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
        }
        Ok(account)
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

    /// Batch Q Phase 1 FIX 7: the `includeMyRenotes` /
    /// `includeRenotedMyNotes` / `includeLocalRenotes` exclusion shared by
    /// `home_timeline` and `hybrid_timeline` (Misskey `notes/timeline.ts`
    /// `getFromDb :220-248` + `notes/hybrid-timeline.ts :257-285` parity,
    /// same bare-reshare rule as the user-list path above: a renote
    /// carrying own text/files — or a poll — is content, not a bare
    /// reshare, and survives every flag). Fail-open like the user-list
    /// closure (which cannot propagate storage errors): an unreadable
    /// renote target or account never excludes.
    fn renote_excluded_by_include_flags(
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

    pub fn home_timeline(
        &self,
        viewer_id: &str,
        cursor: &TimelineCursor,
        with_files: bool,
        with_renotes: bool,
    ) -> Result<Vec<Note>, PortError> {
        self.home_timeline_with_includes(
            viewer_id,
            cursor,
            with_files,
            with_renotes,
            true,
            true,
            true,
        )
    }

    /// `home_timeline` with the upstream `includeMyRenotes` /
    /// `includeRenotedMyNotes` / `includeLocalRenotes` flags (all default
    /// `true` in the upstream paramDef). The old `home_timeline` delegates
    /// here so existing callers keep their behavior.
    #[allow(clippy::too_many_arguments)]
    pub fn home_timeline_with_includes(
        &self,
        viewer_id: &str,
        cursor: &TimelineCursor,
        with_files: bool,
        with_renotes: bool,
        include_my_renotes: bool,
        include_renoted_my_notes: bool,
        include_local_renotes: bool,
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
                if self.renote_excluded_by_include_flags(
                    note,
                    viewer_id,
                    include_my_renotes,
                    include_renoted_my_notes,
                    include_local_renotes,
                ) {
                    return false;
                }
                true
            })
            .collect();
        Ok(self.apply_cursor(self.apply_feed_filter(Some(viewer_id), filtered)?, cursor))
    }

    /// `hybrid_timeline` with the upstream `includeMyRenotes` /
    /// `includeRenotedMyNotes` / `includeLocalRenotes` flags (all default
    /// `true` in the upstream paramDef). The old `hybrid_timeline`
    /// delegates here so existing callers keep their behavior.
    #[allow(clippy::too_many_arguments)]
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
                // §12: `with_replies=false` drops replies but keeps
                // self-replies (Misskey local-timeline parity).
                if !Self::keeps_reply(note, with_replies) {
                    return false;
                }
                true
            })
            .collect();
        Ok(self.apply_cursor(self.apply_feed_filter(viewer_id, filtered)?, cursor))
    }

    /// Federated (global) timeline, mirroring Misskey
    /// `notes/global-timeline`: all non-`local_only` `public` notes, local
    /// and remote (visibility-policy §5, `is_eligible_for_federated_timeline`).
    /// No auth required; no `withReplies` param — the timeline carries
    /// replies, including non-self ones (Misskey parity). The upstream
    /// `channelId IS NULL` conjunct holds trivially: channels are rejected
    /// at creation (`UNSUPPORTED_FIELD`), so no stored note has a channel.
    /// §0A/§12 read filters apply below via `apply_feed_filter`.
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
        Ok(self.apply_cursor(self.apply_feed_filter(viewer_id, filtered)?, cursor))
    }

    pub fn hybrid_timeline(
        &self,
        viewer_id: &str,
        cursor: &TimelineCursor,
        with_files: bool,
        with_renotes: bool,
        with_replies: bool,
    ) -> Result<Vec<Note>, PortError> {
        self.hybrid_timeline_with_includes(
            viewer_id,
            cursor,
            with_files,
            with_renotes,
            with_replies,
            true,
            true,
            true,
        )
    }

    /// `hybrid_timeline` with the upstream `includeMyRenotes` /
    /// `includeRenotedMyNotes` / `includeLocalRenotes` flags (all default
    /// `true` in the upstream paramDef). The old `hybrid_timeline`
    /// delegates here so existing callers keep their behavior.
    #[allow(clippy::too_many_arguments)]
    pub fn hybrid_timeline_with_includes(
        &self,
        viewer_id: &str,
        cursor: &TimelineCursor,
        with_files: bool,
        with_renotes: bool,
        with_replies: bool,
        include_my_renotes: bool,
        include_renoted_my_notes: bool,
        include_local_renotes: bool,
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
            // Home notes carry replies by design (§12 DIVERGENCE: home has
            // no `withReplies` param); the hybrid `with_replies` filter
            // applies here to the merged set with the same self-reply
            // exception as the local path (Misskey hybrid parity). Pending
            // (unresolved-parent) replies count as replies, never roots.
            if !Self::keeps_reply(&note, with_replies) {
                continue;
            }
            // Batch Q Phase 1 FIX 7: upstream `hybrid-timeline.ts`
            // `getFromDb :257-285` applies the include flags to the
            // *combined* (home ∪ local-public) query, so they filter the
            // merged set here — not just the home arm (a viewer-owned bare
            // reshare is local-public and would otherwise leak in via the
            // local side).
            if self.renote_excluded_by_include_flags(
                &note,
                viewer_id,
                include_my_renotes,
                include_renoted_my_notes,
                include_local_renotes,
            ) {
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
    /// `home`/`hybrid` require an authenticated viewer. On top of the
    /// channel rules, the §0A/§12 read filters apply (§12 streaming parity:
    /// blocked/muted/suspended/host-blocked/mutedInstance authors — and
    /// their replies/renotes — never stream, exactly like the REST reads).
    pub fn note_eligible_for_channel(
        &self,
        channel: TimelineChannel,
        viewer_id: Option<&str>,
        note: &Note,
        params: &TimelineChannelParams,
    ) -> Result<bool, PortError> {
        if !self.note_base_eligible_for_channel(channel, viewer_id, note, params)? {
            return Ok(false);
        }
        let filter = self.feed_filter(viewer_id)?;
        let mut account_cache: HashMap<String, Option<Account>> = HashMap::new();
        let mut note_cache: HashMap<String, Option<Note>> = HashMap::new();
        Ok(!self.feed_filter_hides(&filter, note, &mut account_cache, &mut note_cache)?)
    }

    /// Channel rules shared by [`AppServices::note_eligible_for_channel`]
    /// (visibility + eligibility + flag logic, no feed filtering). The
    /// hybrid arm recurses here so the §0A filter applies exactly once, on
    /// the merged verdict.
    fn note_base_eligible_for_channel(
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
                // §12 self-reply exception, mirroring `local_timeline`.
                if !Self::keeps_reply(note, params.with_replies) {
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
                let home = self.note_base_eligible_for_channel(
                    TimelineChannel::Home,
                    Some(viewer),
                    note,
                    params,
                )?;
                // Home notes carry replies by design; the hybrid
                // `with_replies` filter applies with the self-reply
                // exception, mirroring `hybrid_timeline`.
                if home {
                    return Ok(Self::keeps_reply(note, params.with_replies));
                }
                self.note_base_eligible_for_channel(
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
    /// only pass `public`/`home` notes. On top of the visibility rules, the
    /// §0A/§12 read filters apply (§12 streaming parity: a captured note by
    /// a blocked/muted/suspended/host-blocked/mutedInstance author — or
    /// replying/renoting one — never emits, exactly like the REST reads).
    pub fn capture_gate(&self, viewer_id: Option<&str>, note_id: &str) -> Result<bool, PortError> {
        let note = match self.notes.get_note(note_id)? {
            Some(note) => note,
            None => return Ok(false),
        };
        let visible = match note.visibility {
            crate::domain::note::Visibility::Specified => {
                let Some(viewer) = viewer_id else {
                    return Ok(false);
                };
                note.author_id == viewer
                    || note.visible_user_ids.iter().any(|id| id == viewer)
            }
            crate::domain::note::Visibility::Followers => {
                let Some(viewer) = viewer_id else {
                    return Ok(false);
                };
                if note.author_id == viewer {
                    true
                } else {
                    self.follows.is_following(viewer, &note.author_id)?
                }
            }
            _ => true,
        };
        if !visible {
            return Ok(false);
        }
        let filter = self.feed_filter(viewer_id)?;
        let mut account_cache: HashMap<String, Option<Account>> = HashMap::new();
        let mut note_cache: HashMap<String, Option<Note>> = HashMap::new();
        Ok(!self.feed_filter_hides(&filter, &note, &mut account_cache, &mut note_cache)?)
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
        Ok(self.apply_cursor(self.apply_feed_filter(viewer_id, filtered)?, cursor))
    }

    fn all_notes_bounded(&self) -> Result<Vec<Note>, PortError> {
        self.notes.list_all_notes(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })
    }

    /// True when the `note_replies` scan window moved strictly downward:
    /// the new exclusive `untilId` sits below the previous bound, so the
    /// next candidate batch cannot repeat rows.
    fn scan_window_advanced(previous: Option<&str>, current: Option<&str>) -> bool {
        match (previous, current) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(prev), Some(cur)) => cur < prev,
        }
    }

    /// `users/notes`: one author's notes, newest-first, visibility-filtered.
    /// Unknown `user_id` is `NO_SUCH_USER` (Misskey parity).
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
    pub fn user_list_timeline(
        &self,
        list_id: &str,
        viewer_id: &str,
        cursor: &TimelineCursor,
        include_my_renotes: bool,
        include_renoted_my_notes: bool,
        include_local_renotes: bool,
        with_renotes: bool,
        with_files: bool,
    ) -> Result<Vec<Note>, PortError> {
        let list = match self.lists.get_user_list(list_id)? {
            Some(list) if list.owner_id == viewer_id => list,
            _ => return Err(PortError::NotFound("NO_SUCH_LIST".to_string())),
        };
        let memberships = self.lists.members_of_list(&list.id)?;
        let with_replies: std::collections::HashMap<&str, bool> = memberships
            .iter()
            .map(|m| (m.member_id.as_str(), m.with_replies))
            .collect();
        // Bounded candidate set: newest page per member.
        let mut all: Vec<Note> = Vec::new();
        for member in &memberships {
            let page = self.notes.notes_by_author(
                &member.member_id,
                &TimelineCursor {
                    limit: 100,
                    ..Default::default()
                },
            )?;
            for note in page {
                // Per-membership reply rule (upstream `getFromDb` bracket):
                // roots, self-replies, and replies to the owner always
                // pass; other replies need `withReplies`.
                let reply_ok = match note.reply_id.as_deref() {
                    None => true,
                    Some(_) => {
                        let target = note.reply_user_id.as_deref().unwrap_or("");
                        target.is_empty()
                            || target == note.author_id
                            || target == list.owner_id
                            || with_replies.get(note.author_id.as_str()).copied().unwrap_or(false)
                    }
                };
                if reply_ok {
                    all.push(note);
                }
            }
        }
        // Newest-first merge (authors are unique per note: no duplicates).
        all.sort_by(|a, b| b.id.cmp(&a.id));
        let viewer_id_opt = Some(viewer_id);
        self.visible_page(viewer_id_opt, all, cursor, |note| {
            if with_files && note.file_ids.is_empty() {
                return false;
            }
            // Upstream brackets: a renote with own text/files (or a poll)
            // is content, not a bare reshare, and survives every flag.
            let renote_target = note.renote_id.as_deref();
            if let Some(target_id) = renote_target {
                let has_content =
                    note.text.as_deref().is_some_and(|t| !t.is_empty()) || !note.file_ids.is_empty();
                if !has_content && self.polls.get_poll(&note.id).ok().flatten().is_none() {
                    if !with_renotes {
                        return false;
                    }
                    if !include_my_renotes && note.author_id == viewer_id {
                        return false;
                    }
                    if !include_renoted_my_notes || !include_local_renotes {
                        if let Ok(Some(target)) = self.notes.get_note(target_id) {
                            let target_author_host = self
                                .accounts
                                .find_by_id(&target.author_id)
                                .ok()
                                .flatten()
                                .and_then(|a| a.host);
                            if !include_renoted_my_notes && target.author_id == viewer_id {
                                return false;
                            }
                            if !include_local_renotes && target_author_host.is_none() {
                                return false;
                            }
                        }
                    }
                }
            }
            true
        })
    }

    /// `notes/replies`: direct replies (`reply_id == note_id`). Like Misskey,
    /// an unknown `note_id` yields `[]` (no existence check).
    ///
    /// Slice A (`docs/replies.md` §3.3): candidates come from the indexed
    /// `NoteStore::notes_by_reply` in bounded batches — never a global
    /// scan — and visibility applies per candidate. When hidden rows leave
    /// the visible page short, the scan advances the internal exclusive
    /// `untilId` past the last scanned child and fetches the next batch,
    /// keeping the caller's original `sinceId`/date bounds intact. The loop
    /// always terminates: each iteration either fills the page, exhausts
    /// the parent range, or advances `untilId` strictly downward (a backend
    /// that fails to advance ends the scan defensively).
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
                if self.feed_filter_hides(&feed_filter, &note, &mut filter_accounts, &mut filter_notes)? {
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
            if !Self::scan_window_advanced(
                window_before.as_deref(),
                scan.until_id.as_deref(),
            ) {
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
                && !self.feed_filter_hides(&feed_filter, &parent, &mut filter_accounts, &mut filter_notes)?
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
        Ok(self.apply_cursor(
            self.apply_feed_filter(viewer_id, scoped)?,
            cursor,
        ))
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
    pub fn recommended_users(
        &self,
        viewer_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Account>, PortError> {
        let following = self.follow_set(viewer_id)?;
        let muted = self
            .mutes
            .muted_ids_for(viewer_id)
            .unwrap_or_default();
        let mut out = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let (page, next) = self.accounts.all_accounts(after.as_deref(), 500)?;
            if page.is_empty() {
                break;
            }
            for acc in page {
                after = Some(acc.id.clone());
                if !acc.is_local()
                    || acc.id == viewer_id
                    || acc.is_locked
                    || !acc.is_explorable
                    || acc.is_suspended
                    || acc.is_deleted
                    || following.contains(&acc.id)
                    || muted.iter().any(|id| id == &acc.id)
                {
                    continue;
                }
                if self.blocks.is_blocking(viewer_id, &acc.id).unwrap_or(false)
                    || self.blocks.is_blocking(&acc.id, viewer_id).unwrap_or(false)
                {
                    continue;
                }
                out.push(acc);
            }
            if next.is_none() {
                break;
            }
        }
        out.sort_by(|a, b| {
            b.followers_count
                .cmp(&a.followers_count)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(out.into_iter().skip(offset).take(limit).collect())
    }

    /// `notes/recommendations` (Batch P; Sharkey `notes/recommendations.ts`
    /// SHAPE parity — `Note[]`, `limit`/`offset` — over a
    /// documented-simplified scorer, DIVERGENCE): newest visible notes by
    /// the [`AppServices::recommended_users`] scorer's top accounts.
    /// Sharkey's `RecommendationService` (per-user queue, language lanes,
    /// re-rank) has no pinetree equivalent and is an explicit non-goal;
    /// `lang`/`refresh` are accepted-and-ignored, `withSensitive` drops
    /// sensitive/file-flagged notes when false, `withRenotes` drops pure
    /// renotes when false. Anonymous callers get the same public-only page.
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
        Ok(filtered
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect())
    }

    /// `stats` (Misskey `stats.ts` parity on shape; values from our stores):
    /// `(notes, local_notes, users, local_users, reactions, instances,
    /// drive_local_bytes, drive_remote_bytes)`. `instances` counts distinct
    /// remote hosts among cached accounts (upstream counts its instances
    /// table). Drive sums walk media rows once, attributing by owner
    /// locality (upstream hardcodes both to 0 — pinetree reports real sums,
    /// DIVERGENCE).
    #[allow(clippy::type_complexity)]
    pub fn instance_stats(&self) -> Result<(u64, u64, u64, u64, u64, u64, u64, u64), PortError> {
        let notes = self.notes.count_notes()?;
        let local_notes = self.notes.count_local_notes()?;
        let reactions = self.reactions.count_reactions()?;
        let mut users = 0u64;
        let mut local_users = 0u64;
        let mut hosts = std::collections::HashSet::new();
        let mut local_ids = std::collections::HashSet::new();
        let mut after: Option<String> = None;
        loop {
            let (page, next) = self.accounts.all_accounts(after.as_deref(), 500)?;
            if page.is_empty() {
                break;
            }
            for acc in page {
                after = Some(acc.id.clone());
                users += 1;
                if acc.is_local() {
                    local_users += 1;
                    local_ids.insert(acc.id.clone());
                } else if let Some(host) = acc.host.clone() {
                    hosts.insert(host);
                }
            }
            if next.is_none() {
                break;
            }
        }
        let mut drive_local = 0u64;
        let mut drive_remote = 0u64;
        let mut media_cursor = TimelineCursor {
            limit: 500,
            ..Default::default()
        };
        loop {
            let page = self.media.list_all_media(&media_cursor)?;
            if page.is_empty() {
                break;
            }
            for meta in &page {
                let is_local = meta
                    .owner_id
                    .as_deref()
                    .map(|owner| local_ids.contains(owner))
                    .unwrap_or(false);
                if is_local {
                    drive_local += meta.size_bytes;
                } else {
                    drive_remote += meta.size_bytes;
                }
            }
            // Empty-page termination only (Batch C/H): a short page means
            // concurrent deletes, not end-of-scan, and an exact-multiple
            // dataset needs the trailing empty read.
            media_cursor.until_id = page.last().map(|m| m.id.clone());
        }
        Ok((
            notes,
            local_notes,
            users,
            local_users,
            reactions,
            hosts.len() as u64,
            drive_local,
            drive_remote,
        ))
    }

    // --- Phase 3 follow lists ----------------------------------------------

    /// Moderator bypass for the §§1–2 visibility gates (Misskey
    /// `RoleService.isModerator` parity): an admin implies moderator
    /// (Batch M §3.1, `Account::has_moderator_powers`).
    pub fn viewer_is_moderator(&self, viewer_id: Option<&str>) -> Result<bool, PortError> {
        let Some(viewer_id) = viewer_id else {
            return Ok(false);
        };
        Ok(self
            .accounts
            .find_by_id(viewer_id)?
            .map(|account| account.has_moderator_powers())
            .unwrap_or(false))
    }

    /// §1 visibility gate shared by the Misskey `users/followers` +
    /// `users/following` endpoints and the ActivityPub followers/following
    /// collection handlers (Misskey `users/followers.ts:115-135` +
    /// `users/following.ts:123-143` parity):
    /// - `public` → everyone;
    /// - moderators bypass (see [`AppServices::viewer_is_moderator`]);
    /// - `private` → only the list owner, else `FORBIDDEN`;
    /// - `followers` → the owner or someone following the owner, else
    ///   `FORBIDDEN` (anonymous requesters are denied).
    pub fn check_follow_list_visible(
        &self,
        target: &Account,
        viewer_id: Option<&str>,
        list: FollowList,
    ) -> Result<(), PortError> {
        let visibility = match list {
            FollowList::Followers => target.followers_visibility,
            FollowList::Following => target.following_visibility,
        };
        if visibility == FollowVisibility::Public {
            return Ok(());
        }
        if self.viewer_is_moderator(viewer_id)? {
            return Ok(());
        }
        match visibility {
            FollowVisibility::Public => Ok(()),
            FollowVisibility::Private => {
                if viewer_id == Some(target.id.as_str()) {
                    Ok(())
                } else {
                    Err(PortError::Validation("FORBIDDEN".to_string()))
                }
            }
            FollowVisibility::Followers => match viewer_id {
                Some(viewer) if viewer == target.id => Ok(()),
                Some(viewer) if self.follows.is_following(viewer, &target.id)? => Ok(()),
                _ => Err(PortError::Validation("FORBIDDEN".to_string())),
            },
        }
    }

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
    /// newest-first with full cursor paging. Unknown `user_id` is
    /// `NO_SUCH_USER`; the §1 visibility gate applies (`FORBIDDEN`).
    pub fn user_followers(
        &self,
        user_id: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Follow>, PortError> {
        let target = self.users_show_by_id(user_id)?;
        self.check_follow_list_visible(&target, viewer_id, FollowList::Followers)?;
        let all = self.follows.followers_of(user_id, None, 10_000)?;
        Ok(self.apply_follow_cursor(all, cursor))
    }

    /// `users/following`: follow edges where `user_id` is the follower,
    /// newest-first with full cursor paging. Unknown `user_id` is
    /// `NO_SUCH_USER`; the §1 visibility gate applies (`FORBIDDEN`).
    pub fn user_following(
        &self,
        user_id: &str,
        viewer_id: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Follow>, PortError> {
        let target = self.users_show_by_id(user_id)?;
        self.check_follow_list_visible(&target, viewer_id, FollowList::Following)?;
        let all = self.follows.following_of(user_id, None, 10_000)?;
        Ok(self.apply_follow_cursor(all, cursor))
    }

    /// `users/reactions`: reactions made by `user_id`, newest-first with
    /// full cursor paging (Misskey `users/reactions.ts:77-121` parity).
    ///
    /// Gates (moderators bypass all three via
    /// [`AppServices::viewer_is_moderator`]):
    /// - remote target → `IS_REMOTE_USER`;
    /// - `!publicReactions && requester != target` → `REACTIONS_NOT_PUBLIC`;
    /// - target blocks the requester → silent `[]`.
    /// Unknown `user_id` is `NO_SUCH_USER`.
    ///
    /// Read filters mirror the endpoint's SQL + post-filter split: note
    /// visibility plus suspended/blocked-host authors always apply; the
    /// blocked/muted post-filter (`isUserRelated` parity over the note
    /// author, reply author, and renote author) is skipped for reactions
    /// to the target's own notes. Unlike
    /// [`AppServices::feed_filter_hides`], renote-mutes do not apply here
    /// (upstream runs no renote-mute query on this endpoint).
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
                return Err(PortError::Validation(
                    "REACTIONS_NOT_PUBLIC".to_string(),
                ));
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
            let involved_refs: Vec<&str> =
                involved.iter().map(String::as_str).collect();
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
        let wanted = reaction_type.filter(|t| !t.is_empty()).map(|t| {
            match t.strip_suffix("@.:") {
                Some(name) => format!("{name}:"),
                None => t.to_string(),
            }
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

    /// §12 self-reply helper for tests: post `text` as `author_id`, replying
    /// to `reply_to` when set.
    #[cfg(test)]
    fn post_note(
        &self,
        author_id: &str,
        text: &str,
        reply_to: Option<&str>,
    ) -> Result<Note, PortError> {
        let mut input =
            crate::application::ports::CreateNoteInput::simple_public(author_id, text);
        input.reply_to = reply_to.map(str::to_string);
        self.create_note(input)
    }
}

#[cfg(test)]
mod timeline_tests {
    use super::*;
    use std::sync::Arc;

    use crate::application::ports::{
        AccountStore, CreateAccountInput, CreateNoteInput, NoopStreamingSink, UpdateAccountPatch,
    };
    use crate::domain::account::MutedWord as ProfileMuteWord;
    use crate::domain::note::Visibility;

    fn accounts() -> (AppServices, String, String) {
        let services = query_services("queries-tl");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        (services, alice.id, bob.id)
    }

    /// Redb-backed services for the timeline tests (same harness shape as
    /// the command modules; `commands::tests` is private to that subtree).
    fn query_services(prefix: &str) -> AppServices {
        use std::sync::atomic::{AtomicU64, Ordering};

        use crate::application::ports::{ActivityDedup, BlobStore, Clock, IdGenerator};
        use crate::domain::id::{AidxGenerator, ManualClock};
        use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
        use crate::media::storage::FsBlobStore;
        use crate::persistence::redb::RedbStore;

        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-query-test-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            favorites: store.clone(),
            emojis: store.clone(),
            antennas: store.clone(),
            antenna_notes: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs: blobs.clone(),
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            lists: store.clone(),
            signin: store.clone(),
            password_resets: store.clone(),
            registry: store.clone(),
            clips: store.clone(),
            pages: store.clone(),
            folders: store.clone(),
            system_webhooks: store.clone(),
            user_webhooks: store.clone(),
            abuse_recipients: store.clone(),
            memos: store.clone(),
            thread_mutes: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://example.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
            default_like: None,
        cache_blobs: blobs.clone(),
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: crate::config::RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
        }
    }

    fn remote_account(services: &AppServices, handle: &str, host: &str) -> String {
        services
            .accounts
            .create_account(CreateAccountInput {
                handle: handle.to_string(),
                host: Some(host.to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://{host}/users/{handle}")),
                inbox: Some(format!("https://{host}/inbox")),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .unwrap()
            .id
    }

    fn reply_input(author_id: &str, text: &str, reply_to: &str) -> CreateNoteInput {
        let mut input = CreateNoteInput::simple_public(author_id, text);
        input.reply_to = Some(reply_to.to_string());
        input
    }

    fn ids(notes: Vec<Note>) -> Vec<String> {
        notes.into_iter().map(|n| n.id).collect()
    }

    fn channel_params(with_replies: bool) -> TimelineChannelParams {
        TimelineChannelParams {
            with_files: false,
            with_renotes: true,
            with_replies,
        }
    }

    /// `AccountStore` that reports one account as suspended while delegating
    /// everything else to the inner store. Suspension has no public write
    /// path yet (no moderation infra), so read-path filtering is covered
    /// through this stub instead of fabricated rows.
    struct SuspendOne {
        inner: Arc<dyn AccountStore>,
        suspended_id: String,
    }

    impl AccountStore for SuspendOne {
        fn create_account(
            &self,
            input: CreateAccountInput,
            log: Option<&crate::domain::moderation::ModerationLogEntry>,
        ) -> Result<Account, PortError> {
            self.inner.create_account(input, log)
        }
        fn find_by_id(&self, id: &str) -> Result<Option<Account>, PortError> {
            let mut account = self.inner.find_by_id(id)?;
            if id == self.suspended_id {
                if let Some(account) = account.as_mut() {
                    account.is_suspended = true;
                }
            }
            Ok(account)
        }
        fn find_by_handle(&self, h: &str) -> Result<Option<Account>, PortError> {
            self.inner.find_by_handle(h)
        }
        fn find_by_username_and_host(
            &self,
            username: &str,
            host: Option<&str>,
        ) -> Result<Option<Account>, PortError> {
            self.inner.find_by_username_and_host(username, host)
        }
        fn is_username_available(
            &self,
            username: &str,
            preserved: &[String],
        ) -> Result<bool, PortError> {
            self.inner.is_username_available(username, preserved)
        }
        fn update_account(
            &self,
            id: &str,
            patch: UpdateAccountPatch,
            jobs: Vec<crate::application::ports::NewJob>,
        ) -> Result<Account, PortError> {
            self.inner.update_account(id, patch, jobs)
        }
        fn rename(&self, id: &str, new_handle: &str) -> Result<Account, PortError> {
            self.inner.rename(id, new_handle)
        }
        fn get_owner(&self, id: &str) -> Result<String, PortError> {
            self.inner.get_owner(id)
        }
        fn set_owner(&self, id: &str, node: &str, proof: &str) -> Result<(), PortError> {
            self.inner.set_owner(id, node, proof)
        }
        fn search_accounts(
            &self,
            query: &str,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<Account>, PortError> {
            self.inner.search_accounts(query, offset, limit)
        }
        fn all_accounts(&self, after: Option<&str>, max_rows: usize) -> Result<(Vec<Account>, Option<String>), PortError> {
            self.inner.all_accounts(after, max_rows)
        }
        fn remote_shared_inboxes(
            &self,
            after: Option<&str>,
            max_rows: usize,
        ) -> Result<(Vec<String>, Option<String>), PortError> {
            self.inner.remote_shared_inboxes(after, max_rows)
        }
    }

    /// Same services but with `suspended_id` reading back suspended.
    fn with_suspended(services: &AppServices, suspended_id: &str) -> AppServices {
        AppServices {
            accounts: Arc::new(SuspendOne {
                inner: services.accounts.clone(),
                suspended_id: suspended_id.to_string(),
            }),
            ..services.clone()
        }
    }

    #[test]
    fn home_timeline_keeps_replies_from_followed_users() {
        // §12 DIVERGENCE: home has no `withReplies` param and includes
        // replies from followed users (Misskey parity would exclude every
        // reply except self-replies-to-self, i.e. `replyUserId == me`).
        let (services, alice, bob) = accounts();
        let carol = services.create_account("carol", None).unwrap().id;
        services.follow(&bob, &alice, false).unwrap();
        let root = services.post_note(&carol, "carol root", None).unwrap();
        let reply = services
            .create_note(reply_input(&alice, "alice replies", &root.id))
            .unwrap();
        assert_ne!(reply.reply_user_id.as_deref(), Some(alice.as_str()));
        let cursor = TimelineCursor::with_limit(10);
        let home = ids(services.home_timeline(&bob, &cursor, false, true).unwrap());
        assert!(home.contains(&reply.id), "followed author's reply stays on home");
        assert!(!home.contains(&root.id), "unfollowed root stays out of home scope");
    }

    #[test]
    fn local_timeline_self_reply_exception() {
        let (services, alice, bob) = accounts();
        let root = services.post_note(&alice, "root", None).unwrap();
        let other_reply = services
            .create_note(reply_input(&bob, "bob replies", &root.id))
            .unwrap();
        assert_ne!(other_reply.reply_user_id.as_deref(), Some(bob.as_str()));
        let self_reply = services
            .create_note(reply_input(&alice, "alice self-reply", &root.id))
            .unwrap();
        assert_eq!(self_reply.reply_user_id.as_deref(), Some(alice.as_str()));
        let cursor = TimelineCursor::with_limit(10);
        // `with_replies=false` drops other-authored replies, keeps roots
        // and self-replies (Misskey local-timeline parity).
        let no_replies = ids(
            services
                .local_timeline(Some(&alice), &cursor, false, true, false)
                .unwrap(),
        );
        assert!(no_replies.contains(&root.id));
        assert!(no_replies.contains(&self_reply.id));
        assert!(!no_replies.contains(&other_reply.id));
        // `with_replies=true` carries everything.
        let with_replies = ids(
            services
                .local_timeline(Some(&alice), &cursor, false, true, true)
                .unwrap(),
        );
        assert!(with_replies.contains(&root.id));
        assert!(with_replies.contains(&self_reply.id));
        assert!(with_replies.contains(&other_reply.id));
    }

    #[test]
    fn hybrid_timeline_self_reply_exception() {
        let (services, alice, bob) = accounts();
        let carol = services.create_account("carol", None).unwrap().id;
        services.follow(&bob, &alice, false).unwrap();
        let root = services.post_note(&alice, "root", None).unwrap();
        let self_reply = services
            .create_note(reply_input(&alice, "self-reply", &root.id))
            .unwrap();
        // A stranger's reply: local-side only, dropped without `withReplies`.
        let stranger_reply = services
            .create_note(reply_input(&carol, "carol replies", &root.id))
            .unwrap();
        let cursor = TimelineCursor::with_limit(10);
        let merged = ids(
            services
                .hybrid_timeline(&bob, &cursor, false, true, false)
                .unwrap(),
        );
        assert!(merged.contains(&root.id));
        assert!(merged.contains(&self_reply.id), "self-reply survives {merged:?}");
        assert!(!merged.contains(&stranger_reply.id));
        let merged_all = ids(
            services
                .hybrid_timeline(&bob, &cursor, false, true, true)
                .unwrap(),
        );
        assert!(merged_all.contains(&stranger_reply.id));
    }

    #[test]
    fn global_timeline_is_public_only_and_carries_replies() {
        let (services, alice, bob) = accounts();
        let root = services.post_note(&alice, "public root", None).unwrap();
        let self_reply = services
            .create_note(reply_input(&alice, "self-reply", &root.id))
            .unwrap();
        let other_reply = services
            .create_note(reply_input(&bob, "bob replies", &root.id))
            .unwrap();
        // Followers-only and local-only notes never federate.
        let mut followers_input = CreateNoteInput::simple_public(&alice, "followers");
        followers_input.visibility = Visibility::Followers;
        let followers_note = services.create_note(followers_input).unwrap();
        let mut local_input = CreateNoteInput::simple_public(&alice, "local only");
        local_input.local_only = true;
        let local_note = services.create_note(local_input).unwrap();
        // Remote public notes federate.
        let rina = remote_account(&services, "rina", "remote.example");
        let remote_note = services.post_note(&rina, "remote hello", None).unwrap();
        assert_eq!(remote_note.user_host.as_deref(), Some("remote.example"));
        let cursor = TimelineCursor::with_limit(20);
        let global = ids(services.global_timeline(None, &cursor, false, true).unwrap());
        assert!(global.contains(&root.id));
        assert!(global.contains(&self_reply.id), "global carries self-replies");
        assert!(global.contains(&other_reply.id), "global carries replies");
        assert!(global.contains(&remote_note.id), "global carries remote public notes");
        assert!(!global.contains(&followers_note.id));
        assert!(!global.contains(&local_note.id));
    }

    #[test]
    fn filters_hide_authors_across_every_surface() {
        let (services, alice, bob) = accounts();
        let carol = services.create_account("carol", None).unwrap().id;
        let dave = services.create_account("dave", None).unwrap().id;
        // bob: blocked by alice. carol: muted by alice (mute keeps the
        // follow edge, so home scope still includes her notes). dave:
        // renote-muted by alice. rina: remote, on alice's muted instance.
        services.follow(&alice, &carol, false).unwrap();
        let blocked_note = services.post_note(&bob, "blocked says hi", None).unwrap();
        let muted_note = services.post_note(&carol, "muted says hi", None).unwrap();
        let target = services.post_note(&alice, "renote target", None).unwrap();
        let mut pure_input = CreateNoteInput::simple_public(&dave, "x");
        pure_input.text = None;
        pure_input.renote_of = Some(target.id.clone());
        let pure_renote = services.create_note(pure_input).unwrap();
        // A quote (renote + text) by the same renote-muted author stays
        // visible: renote-mutes hide pure renotes only.
        let mut quote_input = CreateNoteInput::simple_public(&dave, "quoting");
        quote_input.renote_of = Some(target.id.clone());
        let quote = services.create_note(quote_input).unwrap();
        let rina = remote_account(&services, "rina", "remote.example");
        let remote_note = services.post_note(&rina, "remote hello", None).unwrap();
        services.block(&alice, &bob).unwrap();
        services.mute(&alice, &carol, None).unwrap();
        services.renote_mute(&alice, &dave).unwrap();
        services
            .update_profile(
                &alice,
                UpdateAccountPatch {
                    muted_instances: Some(vec!["remote.example".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();
        let suspended = with_suspended(&services, &bob);

        let cursor = TimelineCursor::with_limit(20);
        for (label, author_note) in [
            ("blocked", &blocked_note),
            ("muted", &muted_note),
            ("renote-muted pure renote", &pure_renote),
            ("muted-instance remote", &remote_note),
        ] {
            // Local + global timelines hide the note for the viewer.
            for (timeline, notes) in [
                (
                    "local",
                    ids(services.local_timeline(Some(&alice), &cursor, false, true, true).unwrap()),
                ),
                (
                    "global",
                    ids(services.global_timeline(Some(&alice), &cursor, false, true).unwrap()),
                ),
            ] {
                assert!(
                    !notes.contains(&author_note.id),
                    "{label} note hidden on {timeline}"
                );
            }
            // `users/notes` hides it too.
            let listed = ids(
                services
                    .user_notes(&author_note.author_id, Some(&alice), &cursor, true, true, false)
                    .unwrap(),
            );
            assert!(!listed.contains(&author_note.id), "{label} note hidden on users/notes");
            // Streaming eligibility agrees on every channel.
            for channel in [
                TimelineChannel::Local,
                TimelineChannel::Global,
                TimelineChannel::Hybrid,
            ] {
                // Hybrid needs an authenticated viewer; alice qualifies
                // regardless of follow edges for local/global-side notes.
                assert!(
                    !services
                        .note_eligible_for_channel(
                            channel,
                            Some(&alice),
                            author_note,
                            &channel_params(true)
                        )
                        .unwrap(),
                    "{label} note ineligible on {channel:?}"
                );
            }
            // Capture agrees as well (the note is public, so only the
            // §0A filter can reject it).
            assert!(
                !services.capture_gate(Some(&alice), &author_note.id).unwrap(),
                "{label} note uncapturable"
            );
        }
        // Renote-mutes hide ONLY pure renotes: dave's quote stays visible
        // on every surface.
        let local = ids(
            services
                .local_timeline(Some(&alice), &cursor, false, true, true)
                .unwrap(),
        );
        assert!(local.contains(&quote.id), "quotes survive renote-mutes");
        // Viewer-scoped filters do not apply anonymously — except
        // suspension, which hides for everyone. (Remote notes are never
        // local-timeline eligible, so the muted-instance control reads the
        // anonymous global timeline.)
        let anon = ids(
            services
                .local_timeline(None, &cursor, false, true, true)
                .unwrap(),
        );
        assert!(anon.contains(&blocked_note.id), "blocks are viewer-scoped");
        assert!(anon.contains(&muted_note.id), "mutes are viewer-scoped");
        let anon_global = ids(services.global_timeline(None, &cursor, false, true).unwrap());
        assert!(
            anon_global.contains(&remote_note.id),
            "mutedInstances are viewer-scoped"
        );
        // Suspension hides for every viewer, including anonymous ones.
        for viewer in [Some(alice.as_str()), None] {
            let notes = ids(
                suspended
                    .local_timeline(viewer, &cursor, false, true, true)
                    .unwrap(),
            );
            assert!(
                !notes.contains(&blocked_note.id),
                "suspended author hidden for {viewer:?}"
            );
            assert!(
                !suspended.capture_gate(viewer, &blocked_note.id).unwrap(),
                "suspended author uncapturable for {viewer:?}"
            );
            let listed = suspended
                .user_notes(&bob, viewer, &cursor, true, true, false)
                .unwrap();
            assert!(listed.is_empty(), "suspended author hidden on users/notes");
        }
        // The suspended author's followers note is hidden even though
        // visibility alone would pass for the author themself... (control:
        // a non-suspended viewer-facing read still filters the row).
        assert!(
            suspended
                .notes_show(&blocked_note.id, Some(&alice))
                .unwrap_err()
                == PortError::NotFound("NO_SUCH_NOTE".to_string())
        );
    }

    #[test]
    fn word_mutes_server_enforced_hard_drops_soft_keeps() {
        // Batch N WS8 (owner-approved DIVERGENCE from upstream, which
        // enforces word mutes only in the frontend): hard mutes drop the
        // note on every read surface; soft mutes do NOT drop (they pack
        // `isSoftMuted` when the note has no CW of its own — see the
        // packer tests). Matching is Misskey-exact (transitive,
        // self-exempt, case-sensitive AND-groups, `/regex/flags` bare
        // strings, CW-first haystack).
        let (services, alice, bob) = accounts();
        services
            .update_profile(
                &alice,
                UpdateAccountPatch {
                    muted_words: Some(vec![ProfileMuteWord::Single("/spoiler/i".to_string())]),
                    hard_muted_words: Some(vec![
                        ProfileMuteWord::Single("/secret/i".to_string()),
                        ProfileMuteWord::And(vec!["foo".to_string(), "bar".to_string()]),
                    ]),
                    ..Default::default()
                },
            )
            .unwrap();
        let plain = services.post_note(&bob, "just a note", None).unwrap();
        let soft_hit = services.post_note(&bob, "a spoiler here", None).unwrap();
        let hard_regex = services.post_note(&bob, "a secret here", None).unwrap();
        let hard_and = services.post_note(&bob, "foo and bar", None).unwrap();
        let partial_and = services.post_note(&bob, "only foo", None).unwrap();
        // A CW of its own does not save a hard-mute hit (drop), but keeps
        // the soft badge off (packed `isSoftMuted` requires no own CW).
        let mut cw_input = CreateNoteInput::simple_public(&bob, "a spoiler body");
        cw_input.cw = Some("cw here".to_string());
        let cw_soft = services.create_note(cw_input).unwrap();
        // Reply-transitive: bob replies to alice's note with a secret.
        let alice_root = services.post_note(&alice, "alice root", None).unwrap();
        let transitive = services.post_note(&bob, "holds a SECRET", Some(&alice_root.id)).unwrap();
        // Self-notes never match: alice's own secret stays visible to her.
        let own = services.post_note(&alice, "my own secret", None).unwrap();

        let cursor = TimelineCursor::with_limit(30);
        let local = ids(
            services
                .local_timeline(Some(&alice), &cursor, false, true, true)
                .unwrap(),
        );
        assert!(local.contains(&plain.id), "plain survives");
        assert!(local.contains(&soft_hit.id), "soft never drops");
        assert!(local.contains(&cw_soft.id), "soft with CW never drops");
        assert!(!local.contains(&hard_regex.id), "hard regex drops");
        assert!(!local.contains(&hard_and.id), "hard AND-group drops");
        assert!(local.contains(&partial_and.id), "partial AND-group survives");
        assert!(!local.contains(&transitive.id), "reply-transitive hard drops");
        assert!(local.contains(&own.id), "self-notes exempt");

        // notes/show fails closed on hard hits (like invisible notes).
        services.notes_show(&hard_regex.id, Some(&alice)).unwrap_err();
        services.notes_show(&soft_hit.id, Some(&alice)).unwrap();
        // Streaming gates agree.
        assert!(
            !services
                .note_eligible_for_channel(
                    TimelineChannel::Local,
                    Some(&alice),
                    &hard_regex,
                    &channel_params(true)
                )
                .unwrap(),
            "hard drops gate streaming"
        );
        assert!(
            services
                .note_eligible_for_channel(
                    TimelineChannel::Local,
                    Some(&alice),
                    &soft_hit,
                    &channel_params(true)
                )
                .unwrap(),
            "soft never gates streaming"
        );
        assert!(!services.capture_gate(Some(&alice), &hard_and.id).unwrap());
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
