//! `hashtags/*` application ops (WP2-part-B).
//!
//! Ships list / search / show / trend / users (5 endpoints).
//! `notes/search-by-tag` stays the answer for tag timelines (upstream has
//! no hashtag-timeline endpoint either).
//!
//! No new storage: everything derives from the existing note + account
//! stores via a bounded recent-notes scan (`list_all_notes`, limit 10 000,
//! same bound as the other timeline queries) with standard visibility
//! filtering (`domain::visibility::is_visible` + the crate-local
//! `viewer_ctx`/`follows_cache` helpers). Counts therefore reflect recent
//! notes, not all-time totals — documented DIVERGENCE (upstream keeps
//! `UsedForNote`/tag counters; a durable tag index is parent work).
//!
//! Error contract: `hashtags/show` on a never-used tag →
//! `NotFound("NO_SUCH_HASHTAG")` ("No such hashtag.", upstream verbatim
//! code/message, pinetree-fixed UUID). All other endpoints return empty
//! collections instead of errors.

use std::collections::{HashMap, HashSet};

use super::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::visibility::is_visible;

/// Scan bound for tag aggregation (matches `all_notes_bounded`).
const HASHTAG_SCAN_LIMIT: usize = 10_000;
/// `hashtags/trend` result size.
pub const HASHTAG_TREND_LIMIT: usize = 10;

/// `hashtags/show` view (upstream shape subset: tag + usage counts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashtagView {
    pub tag: String,
    /// Notes carrying the tag visible to the viewer (bounded scan).
    pub mentioned_notes_count: u64,
    /// Distinct visible authors (local + remote).
    pub mentioned_users_count: u64,
    /// Distinct visible local authors.
    pub mentioned_local_users_count: u64,
    /// Distinct visible remote authors.
    pub mentioned_remote_users_count: u64,
}

/// `hashtags/list` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashtagEntry {
    pub tag: String,
    pub mentioned_notes_count: u64,
}

impl AppServices {
    /// One bounded recent-notes scan with viewer visibility applied.
    /// Returns `(note_tags_lowercased, author_id)` pairs.
    fn visible_tag_uses(
        &self,
        viewer_id: Option<&str>,
    ) -> Result<Vec<(String, String)>, PortError> {
        let all = self.notes.list_all_notes(&TimelineCursor {
            limit: HASHTAG_SCAN_LIMIT,
            ..Default::default()
        })?;
        let cache = self.follows_cache(viewer_id)?;
        let mut out = Vec::new();
        for note in &all {
            let viewer = self.viewer_ctx(viewer_id, &note.author_id, &cache);
            if !is_visible(note, &viewer) {
                continue;
            }
            for tag in &note.tags {
                out.push((tag.to_lowercase(), note.author_id.clone()));
            }
        }
        Ok(out)
    }

    fn aggregate(uses: &[(String, String)]) -> HashMap<String, (u64, HashSet<String>)> {
        let mut map: HashMap<String, (u64, HashSet<String>)> = HashMap::new();
        for (tag, author) in uses {
            let entry = map.entry(tag.clone()).or_insert((0, HashSet::new()));
            entry.0 += 1;
            entry.1.insert(author.clone());
        }
        map
    }

    /// `hashtags/list` (all tags in the bounded window, most-used first).
    pub fn hashtags_list(
        &self,
        viewer_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HashtagEntry>, PortError> {
        let uses = self.visible_tag_uses(viewer_id)?;
        let mut entries: Vec<HashtagEntry> = Self::aggregate(&uses)
            .into_iter()
            .map(|(tag, (count, _))| HashtagEntry {
                tag,
                mentioned_notes_count: count,
            })
            .collect();
        entries.sort_by(|a, b| {
            b.mentioned_notes_count
                .cmp(&a.mentioned_notes_count)
                .then(a.tag.cmp(&b.tag))
        });
        entries.truncate(limit.clamp(1, 100));
        Ok(entries)
    }

    /// `hashtags/search` (case-insensitive substring, most-used first).
    pub fn hashtags_search(
        &self,
        viewer_id: Option<&str>,
        query: &str,
        limit: usize,
    ) -> Result<Vec<HashtagEntry>, PortError> {
        let needle = query.to_lowercase();
        let uses = self.visible_tag_uses(viewer_id)?;
        let mut entries: Vec<HashtagEntry> = Self::aggregate(&uses)
            .into_iter()
            .filter(|(tag, _)| tag.contains(&needle))
            .map(|(tag, (count, _))| HashtagEntry {
                tag,
                mentioned_notes_count: count,
            })
            .collect();
        entries.sort_by(|a, b| {
            b.mentioned_notes_count
                .cmp(&a.mentioned_notes_count)
                .then(a.tag.cmp(&b.tag))
        });
        entries.truncate(limit.clamp(1, 100));
        Ok(entries)
    }

    /// `hashtags/show`.
    pub fn hashtag_show(
        &self,
        viewer_id: Option<&str>,
        tag: &str,
    ) -> Result<HashtagView, PortError> {
        let needle = tag.to_lowercase();
        let uses = self.visible_tag_uses(viewer_id)?;
        let mut notes = 0u64;
        let mut authors: HashSet<String> = HashSet::new();
        for (t, author) in &uses {
            if *t == needle {
                notes += 1;
                authors.insert(author.clone());
            }
        }
        if notes == 0 {
            return Err(PortError::NotFound("NO_SUCH_HASHTAG".to_string()));
        }
        let mut local = 0u64;
        let mut remote = 0u64;
        for author_id in &authors {
            match self.accounts.find_by_id(author_id)? {
                Some(account) if account.is_local() => local += 1,
                Some(_) => remote += 1,
                None => {}
            }
        }
        Ok(HashtagView {
            tag: needle,
            mentioned_notes_count: notes,
            mentioned_users_count: authors.len() as u64,
            mentioned_local_users_count: local,
            mentioned_remote_users_count: remote,
        })
    }

    /// `hashtags/trend` (top tags in the bounded window, most-used first).
    pub fn hashtags_trend(
        &self,
        viewer_id: Option<&str>,
    ) -> Result<Vec<HashtagEntry>, PortError> {
        self.hashtags_list(viewer_id, HASHTAG_TREND_LIMIT)
    }

    /// `hashtags/users` (distinct recent visible authors of `tag`).
    pub fn hashtag_users(
        &self,
        viewer_id: Option<&str>,
        tag: &str,
        limit: usize,
    ) -> Result<Vec<crate::domain::account::Account>, PortError> {
        let needle = tag.to_lowercase();
        let uses = self.visible_tag_uses(viewer_id)?;
        let mut authors: HashSet<String> = HashSet::new();
        for (t, author) in &uses {
            if *t == needle {
                authors.insert(author.clone());
            }
        }
        let mut out = Vec::new();
        let mut authors: Vec<String> = authors.into_iter().collect();
        authors.sort();
        for author_id in authors {
            if let Some(account) = self.accounts.find_by_id(&author_id)? {
                out.push(account);
            }
            if out.len() >= limit.clamp(1, 100) {
                break;
            }
        }
        Ok(out)
    }
}
