use super::filters::FollowList;
use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::{Account, FollowVisibility};
use crate::domain::follow::Follow;
use crate::domain::note::Note;

impl AppServices {
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
        let account = match self.accounts.find_by_username_and_host(username, host)? {
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
        match self.accounts.find_by_username_and_host(username, host)? {
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
                            || with_replies
                                .get(note.author_id.as_str())
                                .copied()
                                .unwrap_or(false)
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
                let has_content = note.text.as_deref().is_some_and(|t| !t.is_empty())
                    || !note.file_ids.is_empty();
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
    pub fn recommended_users(
        &self,
        viewer_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Account>, PortError> {
        let following = self.follow_set(viewer_id)?;
        let muted = self.mutes.muted_ids_for(viewer_id).unwrap_or_default();
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
}
