//! User lists (Batch O WP16; guide `implementer-guide-batch-m.md` WP16).
//!
//! One application operation per Misskey endpoint, shared by the Misskey
//! adapter. Membership changes commit state + stream/federation jobs in one
//! storage transaction (the store enqueues `jobs` in-txn). Remote members
//! enqueue a system-actor `Follow` (upstream `UserListService.addMember`
//! parity); removal leaves the Follow alone. Deleting a list also deletes
//! the owner's antennas sourced from it, so a removed list stops matching.

use super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::PortError;
use crate::domain::user_list::{UserList, UserListMembership};

impl AppServices {
    fn owner_list(&self, owner_id: &str, list_id: &str) -> Result<UserList, PortError> {
        self.accounts.get_owner(owner_id)?;
        match self.lists.get_user_list(list_id)? {
            Some(list) if list.owner_id == owner_id => Ok(list),
            _ => Err(PortError::NotFound("NO_SUCH_LIST".to_string())),
        }
    }

    /// `users/lists/create`: name 1..=100, per-owner cap enforced in the
    /// store transaction (`TOO_MANY_USERLISTS` on race).
    pub fn create_user_list(&self, owner_id: &str, name: String) -> Result<UserList, PortError> {
        self.accounts.get_owner(owner_id)?;
        UserList::validate_name(&name).map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
            crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
            crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
            crate::domain::id::DomainError::StorageUnavailable(msg) => {
                PortError::StorageUnavailable(msg)
            }
        })?;
        let list = UserList {
            id: self.ids.generate(),
            owner_id: owner_id.to_string(),
            name,
            is_public: false,
            created_at_ms: self.now_ms(),
        };
        self.lists.create_user_list(list)
    }

    /// `users/lists/update`: owner-scoped (`NO_SUCH_LIST` on miss or
    /// foreign list); absent fields untouched.
    pub fn update_user_list(
        &self,
        owner_id: &str,
        list_id: &str,
        name: Option<String>,
        is_public: Option<bool>,
    ) -> Result<UserList, PortError> {
        let mut list = self.owner_list(owner_id, list_id)?;
        if let Some(name) = name {
            UserList::validate_name(&name).map_err(|e| match e {
                crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
                crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
                crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
                crate::domain::id::DomainError::StorageUnavailable(msg) => {
                    PortError::StorageUnavailable(msg)
                }
            })?;
            list.name = name;
        }
        if let Some(is_public) = is_public {
            list.is_public = is_public;
        }
        match self.lists.update_user_list(list)? {
            Some(list) => Ok(list),
            None => Err(PortError::NotFound("NO_SUCH_LIST".to_string())),
        }
    }

    /// `users/lists/delete`: owner-scoped cascade (memberships + favorites
    /// in the store transaction) plus the owner's antennas sourced from the
    /// list, so they stop matching.
    pub fn delete_user_list(&self, owner_id: &str, list_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        if !self.lists.delete_user_list(owner_id, list_id)? {
            return Err(PortError::NotFound("NO_SUCH_LIST".to_string()));
        }
        for antenna in self.antennas.antennas_by_owner(owner_id)? {
            if antenna.user_list_id.as_deref() == Some(list_id) {
                self.antennas.delete_antenna(owner_id, &antenna.id)?;
            }
        }
        Ok(())
    }

    /// `users/lists/push`: owner-scoped list, existing user (remote users
    /// resolve like `users/show`), block refusal (skip when self),
    /// per-list cap. Remote members enqueue a system-actor Follow.
    pub fn push_list_member(
        &self,
        owner_id: &str,
        list_id: &str,
        user_id: &str,
    ) -> Result<UserListMembership, PortError> {
        let list = self.owner_list(owner_id, list_id)?;
        let member = self.users_show_by_id(user_id).map_err(|e| match e {
            PortError::NotFound(_) => PortError::NotFound("NO_SUCH_USER".to_string()),
            other => other,
        })?;
        if member.id != owner_id && self.blocks.is_blocking(&member.id, owner_id)? {
            return Err(PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string()));
        }
        let membership = UserListMembership {
            id: self.ids.generate(),
            list_id: list.id.clone(),
            member_id: member.id.clone(),
            with_replies: false,
            created_at_ms: self.now_ms(),
        };
        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::UserListMemberAdded {
                list_id: list.id.clone(),
                member_id: member.id.clone(),
            },
            format!("list-push:{}:{}:{}", list.id, member.id, self.ids.generate()),
        )?];
        if !member.is_local() {
            // Upstream `addMember`: a proxy follow so remote members' notes
            // reach the instance. Removal deliberately leaves it (parity).
            let (proxy, _) = self.ensure_system_actor()?;
            if let Some(inbox_url) = crate::federation::outbox::preferred_inbox(&member) {
                let follow_id = self.ids.generate();
                jobs.push(
                    crate::federation::outbox::FederateDeliverPayload::Follow {
                        follow_id: follow_id.clone(),
                        follower_id: proxy.id,
                        followee_id: member.id.clone(),
                        inbox_url,
                    }
                    .into_job(
                        format!("federate:list-follow:{}:{follow_id}", list.id),
                        self.now_ms(),
                    )?,
                );
            }
        }
        self.lists.add_list_member(membership, jobs)
    }

    /// `users/lists/pull`: owner-scoped list, existing user; removal is
    /// idempotent (upstream `removeMember` deletes without a miss error).
    /// The system-actor Follow from a push is left alone (parity).
    pub fn pull_list_member(
        &self,
        owner_id: &str,
        list_id: &str,
        user_id: &str,
    ) -> Result<(), PortError> {
        let list = self.owner_list(owner_id, list_id)?;
        self.users_show_by_id(user_id).map_err(|e| match e {
            PortError::NotFound(_) => PortError::NotFound("NO_SUCH_USER".to_string()),
            other => other,
        })?;
        let job = self.streaming_job(
            &StreamingEvent::UserListMemberRemoved {
                list_id: list.id.clone(),
                member_id: user_id.to_string(),
            },
            format!("list-pull:{}:{user_id}:{}", list.id, self.ids.generate()),
        )?;
        self.lists.remove_list_member(&list.id, user_id, vec![job])?;
        Ok(())
    }

    /// `users/lists/update-membership`: owner-scoped list + existing user;
    /// a missing membership is a silent no-op (upstream `update` parity).
    pub fn update_list_membership(
        &self,
        owner_id: &str,
        list_id: &str,
        user_id: &str,
        with_replies: bool,
    ) -> Result<(), PortError> {
        let list = self.owner_list(owner_id, list_id)?;
        self.users_show_by_id(user_id).map_err(|e| match e {
            PortError::NotFound(_) => PortError::NotFound("NO_SUCH_USER".to_string()),
            other => other,
        })?;
        self.lists
            .update_list_membership(&list.id, user_id, with_replies)?;
        Ok(())
    }
}
