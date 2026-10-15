use super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::PortError;
use crate::domain::follow::Follow;
use crate::domain::notification::NotificationKind;

impl AppServices {
    pub fn follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
    ) -> Result<Follow, PortError> {
        self.accounts.get_owner(follower_id)?;
        let mut jobs = Vec::new();
        jobs.push(self.notify_job(
            followee_id,
            follower_id,
            NotificationKind::Follow,
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
        self.follows
            .follow(follower_id, followee_id, with_replies, None, jobs)
    }

    pub fn unfollow(&self, follower_id: &str, followee_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(follower_id)?;
        let mut jobs = Vec::new();
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
