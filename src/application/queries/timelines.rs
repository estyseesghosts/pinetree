use std::collections::{HashMap, HashSet};

use super::filters::{TimelineChannel, TimelineChannelParams};
use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::Account;
use crate::domain::note::Note;
use crate::domain::policy::{
    is_eligible_for_federated_timeline, is_eligible_for_local_timeline, is_visible,
};

impl AppServices {
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

        let cache: HashMap<String, bool> = authors.iter().cloned().map(|id| (id, true)).collect();
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
        let local = self.local_timeline(
            Some(viewer_id),
            &wide,
            with_files,
            with_renotes,
            with_replies,
        )?;
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
                note.author_id == viewer || note.visible_user_ids.iter().any(|id| id == viewer)
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
}
