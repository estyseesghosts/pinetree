//! Small social/read surfaces over existing stores (WP2 part A, second half).
//!
//! These need no new ports, so they ship as real `AppServices` ops with
//! integration tests: `following/list`, `users/lists/create-from-public`
//! and `users/pages`. Everything else in this WP2a partition that needs a
//! new store (`following/update|update-all`, `following/requests/sent`,
//! `notes/thread-muting/*`, `users/update-memo`, `users/flashs`,
//! `users/achievements`, birthday/frequently-replied reads,
//! `notes/polls/recommendation`, `notes/clips`, `notes/show-partial-bulk`)
//! is a proposed snippet in the WP2 report — the ops are specified there
//! against proposed port methods the parent owns.
//!
//! Upstream reference (Misskey `develop`):
//! `following/list.ts`, `following/requests/sent.ts`,
//! `users/lists/create-from-public.ts`, `users/pages.ts`.

use super::AppServices;

use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::follow::Follow;
use crate::domain::note::Visibility;
use crate::domain::page::Page;
use crate::domain::user_list::UserList;

impl AppServices {
    /// `following/list {notification = false, ...cursor, limit 1..=100 = 10}`:
    /// the caller's own outgoing follows, id-ascending (store order),
    /// `notification` keeps only edges with a notify preference, cursor
    /// bounds apply in memory (single-node MVP; mirrors the
    /// `following_of(.., None, 10_000)` precedent in `queries.rs`).
    pub fn following_list(
        &self,
        caller_id: &str,
        notify_only: bool,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Follow>, PortError> {
        self.accounts.get_owner(caller_id)?;
        let mut out = Vec::new();
        for edge in self.follows.following_of(caller_id, None, 10_000)? {
            if notify_only && edge.notify.is_none() {
                continue;
            }
            if let Some(since_id) = cursor.since_id.as_deref() {
                if edge.id.as_str() <= since_id {
                    continue;
                }
            }
            if let Some(until_id) = cursor.until_id.as_deref() {
                if edge.id.as_str() >= until_id {
                    continue;
                }
            }
            if cursor.since_date.is_some_and(|s| {
                self.ids
                    .parse_time(&edge.id)
                    .map(|t| t <= s)
                    .unwrap_or(true)
            }) {
                continue;
            }
            if cursor.until_date.is_some_and(|u| {
                self.ids
                    .parse_time(&edge.id)
                    .map(|t| t >= u)
                    .unwrap_or(true)
            }) {
                continue;
            }
            out.push(edge);
            if out.len() >= cursor.limit {
                break;
            }
        }
        Ok(out)
    }

    /// `users/lists/create-from-public {name[1..=100], listId}`: copy the
    /// members of another user's **public** list into a new private list.
    /// Verbatim codes: `NO_SUCH_LIST` (source missing or not public),
    /// `TOO_MANY_USERLISTS` (from `create_user_list`), `NO_SUCH_USER`
    /// (a member vanished), `YOU_HAVE_BEEN_BLOCKED` (a member blocks the
    /// caller — aborts like upstream; members added so far stay, upstream
    /// has no cross-member transaction either), `ALREADY_ADDED` /
    /// `TOO_MANY_USERS` (from `add_list_member`). Self-membership skips the
    /// block check (upstream parity).
    pub fn create_list_from_public(
        &self,
        owner_id: &str,
        name: String,
        list_id: &str,
    ) -> Result<UserList, PortError> {
        self.accounts.get_owner(owner_id)?;
        let source = match self.lists.get_user_list(list_id)? {
            Some(list) if list.is_public => list,
            _ => return Err(PortError::NotFound("NO_SUCH_LIST".to_string())),
        };
        let new_list = self.create_user_list(owner_id, name)?;
        for membership in self.lists.members_of_list(&source.id)? {
            let member = self.users_show_by_id(&membership.member_id).map_err(|e| match e {
                PortError::NotFound(_) => PortError::NotFound("NO_SUCH_USER".to_string()),
                other => other,
            })?;
            // Reuse the exact push path (block refusal, proxy follow for
            // remotes, cap errors) so semantics match `users/lists/push`.
            self.push_list_member(owner_id, &new_list.id, &member.id)?;
        }
        Ok(new_list)
    }

    /// `users/pages {userId, ...cursor, limit 1..=100 = 10}`: that user's
    /// **public** pages, newest-first, cursor-paged. Unknown users yield `[]`
    /// (upstream runs the query with no user check — parity).
    pub fn pages_of_user(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Page>, PortError> {
        let mut out = Vec::new();
        for page in self.pages.pages_by_owner(user_id)? {
            if page.visibility != Visibility::Public {
                continue;
            }
            if let Some(since_id) = cursor.since_id.as_deref() {
                if page.id.as_str() <= since_id {
                    continue;
                }
            }
            if let Some(until_id) = cursor.until_id.as_deref() {
                if page.id.as_str() >= until_id {
                    continue;
                }
            }
            out.push(page);
            if out.len() >= cursor.limit {
                break;
            }
        }
        Ok(out)
    }
}
