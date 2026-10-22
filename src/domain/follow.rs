//! Directed follow edge: pure data + validation (no I/O).

use crate::domain::id::DomainError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FollowNotify {
    Normal,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Follow {
    pub id: String,
    pub follower_id: String,
    pub followee_id: String,
    pub with_replies: bool,
    pub notify: Option<FollowNotify>,
}

/// Pending follow request (mirrors Misskey's `FollowRequest`). Not a follow
/// edge: it never feeds `follows_author`, fan-out, counters or AP collections.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FollowRequest {
    pub id: String,
    pub follower_id: String,
    pub followee_id: String,
    pub with_replies: bool,
    pub notify: Option<FollowNotify>,
}

impl Follow {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.follower_id.is_empty() || self.followee_id.is_empty() {
            return Err(DomainError::Validation(
                "follower_id and followee_id must be non-empty".to_string(),
            ));
        }
        if self.follower_id == self.followee_id {
            return Err(DomainError::Validation(
                "follower_id and followee_id must differ".to_string(),
            ));
        }
        Ok(())
    }
}

/// Which path a follow attempt takes (WP6 domain layering: locked
/// branching lives here so every adapter shares one rule).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowPath {
    /// Open account: create the edge immediately.
    Direct,
    /// Locked account: queue a pending request instead.
    Request,
}

/// Misskey `FollowingService.follow` branching parity: a locked followee
/// forces the request path; an open followee follows directly.
pub fn decide_follow_path(followee_is_locked: bool) -> FollowPath {
    if followee_is_locked {
        FollowPath::Request
    } else {
        FollowPath::Direct
    }
}

/// Pure rule shared by the application op and storage: request ids must be
/// non-empty and a user cannot request themselves
/// (Misskey `FOLLOWEE_IS_YOURSELF`).
pub fn validate_follow_request_params(
    follower_id: &str,
    followee_id: &str,
) -> Result<(), DomainError> {
    if follower_id.is_empty() || followee_id.is_empty() {
        return Err(DomainError::Validation(
            "follower_id and followee_id must be non-empty".to_string(),
        ));
    }
    if follower_id == followee_id {
        return Err(DomainError::Validation("FOLLOWEE_IS_YOURSELF".to_string()));
    }
    Ok(())
}

impl FollowRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_follow_request_params(&self.follower_id, &self.followee_id)
    }
}

/// Accept path (WP6): the pending request must exist; the built edge keeps
/// the request's `with_replies`/`notify` preferences and the caller's new
/// edge id. Errors `NotFound(NO_FOLLOW_REQUEST)` when `request` is `None`.
pub fn build_follow_from_request(
    request: Option<&FollowRequest>,
    edge_id: String,
) -> Result<Follow, DomainError> {
    let req = request.ok_or_else(|| DomainError::NotFound("NO_FOLLOW_REQUEST".to_string()))?;
    req.validate()?;
    Ok(Follow {
        id: edge_id,
        follower_id: req.follower_id.clone(),
        followee_id: req.followee_id.clone(),
        with_replies: req.with_replies,
        notify: req.notify.clone(),
    })
}

/// Reject/cancel path (WP6): the pending request must exist.
/// Errors `NotFound(NO_FOLLOW_REQUEST)` when `request` is `None`.
pub fn require_follow_request(request: Option<&FollowRequest>) -> Result<(), DomainError> {
    match request {
        Some(req) => req.validate(),
        None => Err(DomainError::NotFound("NO_FOLLOW_REQUEST".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_follow() -> Follow {
        Follow {
            id: "fid00000000000001".to_string(),
            follower_id: "aid00000000000001".to_string(),
            followee_id: "aid00000000000002".to_string(),
            with_replies: false,
            notify: None,
        }
    }

    #[test]
    fn valid_follow_passes() {
        assert!(valid_follow().validate().is_ok());
    }

    #[test]
    fn self_follow_rejected() {
        let mut f = valid_follow();
        f.followee_id = f.follower_id.clone();
        assert!(f.validate().is_err());
    }

    #[test]
    fn empty_ids_rejected() {
        let mut f = valid_follow();
        f.follower_id = String::new();
        assert!(f.validate().is_err());
        let mut f = valid_follow();
        f.followee_id = String::new();
        assert!(f.validate().is_err());
    }

    #[test]
    fn locked_branching_forces_request_path() {
        assert_eq!(super::decide_follow_path(true), super::FollowPath::Request);
        assert_eq!(super::decide_follow_path(false), super::FollowPath::Direct);
    }

    #[test]
    fn request_params_reject_self_and_empties() {
        assert!(super::validate_follow_request_params("a", "b").is_ok());
        assert!(super::validate_follow_request_params("a", "a").is_err());
        assert!(super::validate_follow_request_params("", "b").is_err());
        assert!(super::validate_follow_request_params("a", "").is_err());
    }

    #[test]
    fn accept_builds_edge_from_request() {
        let req = super::FollowRequest {
            id: "rid00000000000001".to_string(),
            follower_id: "aid00000000000001".to_string(),
            followee_id: "aid00000000000002".to_string(),
            with_replies: true,
            notify: None,
        };
        let edge = super::build_follow_from_request(Some(&req), "fid1".to_string()).unwrap();
        assert_eq!(edge.follower_id, req.follower_id);
        assert_eq!(edge.followee_id, req.followee_id);
        assert!(edge.with_replies);
        assert!(super::build_follow_from_request(None, "fid1".to_string()).is_err());
    }

    #[test]
    fn reject_requires_existing_request() {
        let req = super::FollowRequest {
            id: "rid00000000000001".to_string(),
            follower_id: "aid00000000000001".to_string(),
            followee_id: "aid00000000000002".to_string(),
            with_replies: false,
            notify: None,
        };
        assert!(super::require_follow_request(Some(&req)).is_ok());
        assert_eq!(
            super::require_follow_request(None),
            Err(crate::domain::id::DomainError::NotFound(
                "NO_FOLLOW_REQUEST".to_string()
            ))
        );
    }
}
