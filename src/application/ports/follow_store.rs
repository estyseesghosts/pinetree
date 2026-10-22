use super::job_queue::NewJob;
use crate::domain::follow::{Follow, FollowNotify, FollowRequest};
use crate::domain::id::PortError;

pub trait FollowStore: Send + Sync {
    fn follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError>;
    fn unfollow(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    fn is_following(&self, follower_id: &str, followee_id: &str) -> Result<bool, PortError>;
    fn get_follow(&self, follower_id: &str, followee_id: &str)
    -> Result<Option<Follow>, PortError>;
    fn followers_of(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Follow>, PortError>;
    fn following_of(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Follow>, PortError>;

    // --- Pending follow requests (Phase 2.6 slice 4, decision D-a). A request
    // is NOT a follow edge: it never affects `is_following`, `followers_of`,
    // counters or visibility until accepted.
    /// Record a pending request. `Conflict(ALREADY_FOLLOWING)` if an edge
    /// exists, `Conflict(ALREADY_REQUESTED)` if a request exists.
    fn request_follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<FollowRequest, PortError> {
        let _ = (follower_id, followee_id, with_replies, notify, jobs);
        Err(PortError::StorageUnavailable(
            "FollowStore::request_follow not implemented".to_string(),
        ))
    }
    fn get_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<Option<FollowRequest>, PortError> {
        let _ = (follower_id, followee_id);
        Ok(None)
    }
    /// Pending requests addressed to `account_id`, ordered by follower id.
    fn follow_requests_to(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<FollowRequest>, PortError> {
        let _ = (account_id, cursor, limit);
        Ok(Vec::new())
    }
    /// Atomically delete the request, create the follow edge (counters
    /// included) and enqueue `jobs`. `NotFound(NO_FOLLOW_REQUEST)` if
    /// there is no request.
    fn accept_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError> {
        let _ = (follower_id, followee_id, jobs);
        Err(PortError::StorageUnavailable(
            "FollowStore::accept_follow_request not implemented".to_string(),
        ))
    }
    /// Atomically delete the request (reject or cancel) and enqueue `jobs`.
    /// `NotFound(NO_FOLLOW_REQUEST)` if there is no request.
    fn remove_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let _ = (follower_id, followee_id, jobs);
        Err(PortError::StorageUnavailable(
            "FollowStore::remove_follow_request not implemented".to_string(),
        ))
    }
    /// Rewrite the `with_replies` flag and notify preference of a live edge
    /// (`following/update` parity: `withReplies`, `notify`).
    /// `NotFound(NOT_FOLLOWING)` when no edge exists. `jobs` commit in the
    /// same transaction (federated `Update(Follow)` fan-out).
    fn update_follow_options(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError> {
        let _ = (follower_id, followee_id, with_replies, notify, jobs);
        Err(PortError::StorageUnavailable(
            "FollowStore::update_follow_options not implemented".to_string(),
        ))
    }
    /// Pending requests sent by `follower_id` (`following/requests/sent`
    /// parity), ordered by followee id. Mirrors [`FollowStore::follow_requests_to`].
    fn follow_requests_from(
        &self,
        follower_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<FollowRequest>, PortError> {
        let _ = (follower_id, cursor, limit);
        Ok(Vec::new())
    }
}
