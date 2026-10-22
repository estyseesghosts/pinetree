use super::job_queue::NewJob;
use crate::domain::id::PortError;

/// User lists (Batch O WP16; pull timelines, remote members, favorites).
pub trait UserListStore: Send + Sync {
    /// Insert one list; `Conflict(TOO_MANY_USERLISTS)` when the owner
    /// already holds [`crate::domain::user_list::USER_LIST_LIMIT`] lists.
    /// Count check + insert commit atomically (conformance K2).
    fn create_user_list(
        &self,
        list: crate::domain::user_list::UserList,
    ) -> Result<crate::domain::user_list::UserList, PortError>;
    /// Owner-scoped rewrite (name / visibility). `None` on miss.
    fn update_user_list(
        &self,
        list: crate::domain::user_list::UserList,
    ) -> Result<Option<crate::domain::user_list::UserList>, PortError>;
    /// Owner-scoped delete with membership + favorite cascade, one
    /// transaction (conformance K3). Returns true when a row existed.
    fn delete_user_list(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError>;
    fn get_user_list(
        &self,
        list_id: &str,
    ) -> Result<Option<crate::domain::user_list::UserList>, PortError>;
    /// Owner's lists, newest-first.
    fn lists_by_owner(
        &self,
        owner_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserList>, PortError>;
    /// Owner's public lists, newest-first (`users/lists/list{userId}`).
    fn public_lists_by_owner(
        &self,
        owner_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserList>, PortError>;
    /// Add one member; `Conflict(ALREADY_ADDED)` on duplicate,
    /// `Conflict(TOO_MANY_USERS)` past
    /// [`crate::domain::user_list::USER_LIST_MEMBER_LIMIT`] members.
    /// Cap check + insert + `jobs` commit atomically (conformance K2).
    fn add_list_member(
        &self,
        member: crate::domain::user_list::UserListMembership,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::user_list::UserListMembership, PortError>;
    /// Remove one member (idempotent) with `jobs`, one transaction.
    /// Returns true when a row existed.
    fn remove_list_member(
        &self,
        list_id: &str,
        member_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    /// Flip `with_replies`. `None` on miss.
    fn update_list_membership(
        &self,
        list_id: &str,
        member_id: &str,
        with_replies: bool,
    ) -> Result<Option<crate::domain::user_list::UserListMembership>, PortError>;
    fn get_list_membership(
        &self,
        list_id: &str,
        member_id: &str,
    ) -> Result<Option<crate::domain::user_list::UserListMembership>, PortError>;
    /// Every membership of one list (bounded by the member cap).
    fn members_of_list(
        &self,
        list_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserListMembership>, PortError>;
    /// Favorite one public list (idempotent insert). Returns true when the
    /// row was newly added.
    fn favorite_list(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError>;
    /// Unfavorite (idempotent). Returns true when a row existed.
    fn unfavorite_list(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError>;
    fn is_list_favorited(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError>;
    fn list_favorite_count(&self, list_id: &str) -> Result<u64, PortError>;
}
