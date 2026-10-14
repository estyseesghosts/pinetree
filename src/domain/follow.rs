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
}
