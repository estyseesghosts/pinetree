use super::super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::PortError;
use crate::domain::follow::Follow;
use crate::domain::notification::NotificationKind;

impl AppServices {
    /// Record an inbound `Follow`, notifying the followee and auto-accepting
    /// when the followee is an unlocked local account and the follower is
    /// remote. A commit race that surfaces as `Conflict` (already following)
    /// is treated as success.
    pub fn handle_inbound_follow(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
    ) -> Result<Option<Follow>, PortError> {
        let mut jobs = Vec::new();
        jobs.push(self.notify_job(
            followee_id,
            follower_id,
            NotificationKind::Follow,
            None,
            None,
            "inbound-follow",
        )?);
        jobs.push(self.streaming_job(
            &StreamingEvent::MeUpdated {
                account_id: followee_id.to_string(),
            },
            format!(
                "inbound-follow:{follower_id}:{followee_id}:{}",
                self.ids.generate()
            ),
        )?);
        // Auto-Accept: the only inbound path that enqueues further federation
        // delivery.
        let base = self.public_url.trim_end_matches('/');
        if let (Some(follower), Some(followee)) = (
            self.accounts.find_by_id(follower_id)?,
            self.accounts.find_by_id(followee_id)?,
        ) {
            if followee.is_local() && !followee.is_locked && !follower.is_local() {
                if let Some(inbox_url) =
                    crate::federation::outbox::preferred_inbox(&follower)
                {
                    let follow_activity = serde_json::json!({
                        "id": activity_id,
                        "type": "Follow",
                        "actor": follower.actor_url(base),
                        "object": followee.actor_url(base),
                    });
                    let response_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::FollowResponse {
                            response_id: response_id.clone(),
                            accept: true,
                            actor_id: followee_id.to_string(),
                            inbox_url,
                            follow_activity,
                        }
                        .into_job(
                            format!("federate:accept:{followee_id}:{response_id}"),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        match self.dedup.record_follow_activity(
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            None,
            jobs,
        ) {
            Ok(v) => Ok(v),
            // Lost a commit race with a duplicate delivery: already following.
            Err(PortError::Conflict(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Undo(Follow)`. A missing follow edge surfaces as
    /// `Validation(NOT_FOLLOWING)` and maps to `Ok(false)`.
    pub fn handle_inbound_unfollow(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<bool, PortError> {
        match self
            .dedup
            .record_unfollow_activity(activity_id, follower_id, followee_id, vec![])
        {
            Ok(v) => Ok(v),
            Err(PortError::Validation(msg)) if msg.contains("NOT_FOLLOWING") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Accept(Follow)`. The Accept's own id dedups; the
    /// underlying follow row insert is idempotent (`Conflict` maps to
    /// `Ok(None)`).
    pub fn handle_inbound_accept_follow(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
    ) -> Result<Option<Follow>, PortError> {
        match self.dedup.record_follow_activity(
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            None,
            vec![],
        ) {
            Ok(v) => Ok(v),
            Err(PortError::Conflict(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Reject(Follow)`: dedup the Reject id, then
    /// best-effort remove the follow edge. Returns `Ok(false)` only when the
    /// Reject itself was already seen.
    ///
    /// NOTE: the trailing `_with_replies` mirrors
    /// `handle_inbound_accept_follow` for call-site symmetry (the inbox
    /// dispatcher passes the Follow's flag through); it has no effect on the
    /// unfollow itself.
    pub fn handle_inbound_reject_follow(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        _with_replies: bool,
    ) -> Result<bool, PortError> {
        if self.dedup.check_and_record(activity_id)? {
            return Ok(false);
        }
        match self.follows.unfollow(follower_id, followee_id, vec![]) {
            Ok(()) => Ok(true),
            Err(PortError::Validation(msg)) if msg.contains("NOT_FOLLOWING") => Ok(true),
            Err(e) => Err(e),
        }
    }
}
