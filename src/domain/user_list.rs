//! User lists (Batch O WP16; guide `implementer-guide-batch-m.md` WP16).
//!
//! Lists are named sets of accounts with their own pull timeline. The
//! timeline is read at request time (merge members' newest-first, bounded
//! member count) — no write amplification, and adding a member backfills
//! their history (DIVERGENCE 3, manifest §34).

use crate::domain::id::DomainError;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Max lists per owner (upstream `RoleService.userListLimit`, default 10;
/// pinetree enforces and advertises the same value).
pub const USER_LIST_LIMIT: usize = 10;
/// Max members per list (upstream `RoleService.userEachUserListsLimit`,
/// default 50; checked against the adder's policy in `addMember`).
pub const USER_LIST_MEMBER_LIMIT: usize = 50;
/// API name bound (upstream `create.ts` `maxLength: 100`). The column is
/// `varchar(128)` upstream; pinetree enforces 100 at the API like upstream.
pub const USER_LIST_NAME_MAX: usize = 100;

/// One named account set (upstream `MiUserList`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserList {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub is_public: bool,
    pub created_at_ms: i64,
}

impl UserList {
    /// Upstream `create.ts` param rules: name 1..=100 chars.
    pub fn validate_name(name: &str) -> Result<(), DomainError> {
        if name.is_empty() || name.chars().count() > USER_LIST_NAME_MAX {
            return Err(invalid(format!(
                "user list name must be 1..={USER_LIST_NAME_MAX} chars"
            )));
        }
        Ok(())
    }
}

/// One list membership (upstream `MiUserListMembership`), unique on
/// (list, member). `with_replies` drives the pull-timeline reply rule.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserListMembership {
    pub id: String,
    pub list_id: String,
    pub member_id: String,
    pub with_replies: bool,
    pub created_at_ms: i64,
}

/// Upstream `NoteCreateService.ts:1139-1156` reply rule for list fanout,
/// applied at read time here: a reply reaches the list timeline when it has
/// no reply target, is a self-reply, replies to the list owner, or the
/// membership carries `with_replies`.
pub fn list_reply_visible(
    reply_to_user_id: Option<&str>,
    author_id: &str,
    owner_id: &str,
    with_replies: bool,
) -> bool {
    match reply_to_user_id {
        None => true,
        Some(target) => target == author_id || target == owner_id || with_replies,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_rule_matrix() {
        // Plain note, self-reply, reply-to-owner: always visible.
        assert!(list_reply_visible(None, "a", "o", false));
        assert!(list_reply_visible(Some("a"), "a", "o", false));
        assert!(list_reply_visible(Some("o"), "a", "o", false));
        // Third-party reply: gated on with_replies.
        assert!(!list_reply_visible(Some("x"), "a", "o", false));
        assert!(list_reply_visible(Some("x"), "a", "o", true));
    }

    #[test]
    fn name_bounds() {
        assert!(UserList::validate_name("ok").is_ok());
        assert!(UserList::validate_name("").is_err());
        assert!(UserList::validate_name(&"n".repeat(101)).is_err());
        assert!(UserList::validate_name(&"n".repeat(100)).is_ok());
    }
}
