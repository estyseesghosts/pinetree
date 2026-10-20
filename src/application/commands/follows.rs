use super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::PortError;
use crate::domain::follow::{Follow, FollowRequest};
use crate::domain::notification::NotificationKind;

/// Result of a local follow: an immediate edge, or a pending request when the
/// followee is a locked account (Phase 2.6 slice 4).
#[derive(Debug, Clone)]
pub enum FollowOutcome {
    Following(Follow),
    Requested(FollowRequest),
}

impl AppServices {
    pub fn follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
    ) -> Result<FollowOutcome, PortError> {
        self.accounts.get_owner(follower_id)?;
        let pending = self
            .accounts
            .find_by_id(followee_id)?
            .map(|a| a.is_locked)
            .unwrap_or(false);
        let mut jobs = Vec::new();
        jobs.push(self.notify_job(
            followee_id,
            follower_id,
            if pending {
                NotificationKind::ReceiveFollowRequest
            } else {
                NotificationKind::Follow
            },
            None,
            None,
            "follow",
        )?);
        jobs.push(self.streaming_job(
            &StreamingEvent::MeUpdated {
                account_id: followee_id.to_string(),
            },
            format!("follow:{follower_id}:{followee_id}:{}", self.ids.generate()),
        )?);
        // Federate the Follow when either side is remote. The storage follow
        // row id is generated inside the transaction, so the activity carries
        // a fresh id used for the Accept embedding (MVP: it may differ from
        // the storage row id).
        if let (Some(follower), Some(followee)) = (
            self.accounts.find_by_id(follower_id)?,
            self.accounts.find_by_id(followee_id)?,
        ) {
            let remote = if !followee.is_local() {
                Some(&followee)
            } else if !follower.is_local() {
                Some(&follower)
            } else {
                None
            };
            if let Some(remote_account) = remote {
                if let Some(inbox_url) =
                    crate::federation::outbox::preferred_inbox(remote_account)
                {
                    let follow_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::Follow {
                            follow_id: follow_id.clone(),
                            follower_id: follower_id.to_string(),
                            followee_id: followee_id.to_string(),
                            inbox_url,
                        }
                        .into_job(
                            format!(
                                "federate:follow:{follower_id}:{followee_id}:{follow_id}"
                            ),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        if pending {
            return self
                .follows
                .request_follow(follower_id, followee_id, with_replies, None, jobs)
                .map(FollowOutcome::Requested);
        }
        self.follows
            .follow(follower_id, followee_id, with_replies, None, jobs)
            .map(FollowOutcome::Following)
    }

    /// Followee accepts a pending request. The state flip and the AP `Accept`
    /// job (for remote followers) commit in one transaction.
    pub fn accept_follow_request(
        &self,
        followee_id: &str,
        follower_id: &str,
    ) -> Result<Follow, PortError> {
        self.accounts.get_owner(followee_id)?;
        let req = self
            .follows
            .get_follow_request(follower_id, followee_id)?
            .ok_or_else(|| PortError::NotFound("NO_FOLLOW_REQUEST".to_string()))?;
        let mut jobs = Vec::new();
        jobs.push(self.streaming_job(
            &StreamingEvent::MeUpdated {
                account_id: followee_id.to_string(),
            },
            format!("accept-request:{follower_id}:{followee_id}:{}", self.ids.generate()),
        )?);
        // Batch N WS3: the requester learns its request was accepted
        // (Misskey `UserFollowingService.acceptFollowRequest` parity:
        // `createNotification(followerId, followeeId, 'followRequestAccepted')`).
        jobs.push(self.notify_job(
            follower_id,
            followee_id,
            NotificationKind::FollowRequestAccepted,
            None,
            None,
            "follow-request-accepted",
        )?);
        if let (Some(follower), Some(followee)) = (
            self.accounts.find_by_id(follower_id)?,
            self.accounts.find_by_id(followee_id)?,
        ) {
            if !follower.is_local() {
                if let Some(inbox_url) = crate::federation::outbox::preferred_inbox(&follower) {
                    let base = self.public_url.trim_end_matches('/');
                    let response_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::FollowResponse {
                            response_id: response_id.clone(),
                            accept: true,
                            actor_id: followee_id.to_string(),
                            inbox_url,
                            follow_activity: serde_json::json!({
                                "id": format!("{}/follows/{}", follower.actor_url(base), req.id),
                                "type": "Follow",
                                "actor": follower.actor_url(base),
                                "object": followee.actor_url(base),
                            }),
                        }
                        .into_job(
                            format!("federate:accept:{followee_id}:{response_id}"),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        self.follows.accept_follow_request(follower_id, followee_id, jobs)
    }

    /// Followee rejects a pending request (sends AP `Reject` to a remote follower).
    pub fn reject_follow_request(
        &self,
        followee_id: &str,
        follower_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(followee_id)?;
        let req = self
            .follows
            .get_follow_request(follower_id, followee_id)?
            .ok_or_else(|| PortError::NotFound("NO_FOLLOW_REQUEST".to_string()))?;
        let mut jobs = Vec::new();
        if let (Some(follower), Some(followee)) = (
            self.accounts.find_by_id(follower_id)?,
            self.accounts.find_by_id(followee_id)?,
        ) {
            if !follower.is_local() {
                if let Some(inbox_url) = crate::federation::outbox::preferred_inbox(&follower) {
                    let base = self.public_url.trim_end_matches('/');
                    let response_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::FollowResponse {
                            response_id: response_id.clone(),
                            accept: false,
                            actor_id: followee_id.to_string(),
                            inbox_url,
                            follow_activity: serde_json::json!({
                                "id": format!("{}/follows/{}", follower.actor_url(base), req.id),
                                "type": "Follow",
                                "actor": follower.actor_url(base),
                                "object": followee.actor_url(base),
                            }),
                        }
                        .into_job(
                            format!("federate:reject:{followee_id}:{response_id}"),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        self.follows.remove_follow_request(follower_id, followee_id, jobs)
    }

    /// Follower withdraws its own pending request. `unfollow` already removes
    /// a pending request (and federates `Undo(Follow)`), so this delegates.
    pub fn cancel_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<(), PortError> {
        if self
            .follows
            .get_follow_request(follower_id, followee_id)?
            .is_none()
        {
            return Err(PortError::NotFound("NO_FOLLOW_REQUEST".to_string()));
        }
        self.unfollow(follower_id, followee_id)
    }

    pub fn unfollow(&self, follower_id: &str, followee_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(follower_id)?;
        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::Unfollowed {
                follower_id: follower_id.to_string(),
                followee_id: followee_id.to_string(),
            },
            format!("unfollow:{follower_id}:{followee_id}:{}", self.ids.generate()),
        )?];
        if let (Some(follower), Some(followee)) = (
            self.accounts.find_by_id(follower_id)?,
            self.accounts.find_by_id(followee_id)?,
        ) {
            let remote = if !followee.is_local() {
                Some(&followee)
            } else if !follower.is_local() {
                Some(&follower)
            } else {
                None
            };
            if let Some(remote_account) = remote {
                if let Some(inbox_url) =
                    crate::federation::outbox::preferred_inbox(remote_account)
                {
                    let follow_id = self
                        .follows
                        .get_follow(follower_id, followee_id)?
                        .map(|f| f.id)
                        .unwrap_or_else(|| self.ids.generate());
                    let undo_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::Unfollow {
                            undo_id: undo_id.clone(),
                            follow_id,
                            follower_id: follower_id.to_string(),
                            followee_id: followee_id.to_string(),
                            inbox_url,
                        }
                        .into_job(
                            format!("federate:unfollow:{follower_id}:{followee_id}:{undo_id}"),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        self.follows.unfollow(follower_id, followee_id, jobs)
    }
}
