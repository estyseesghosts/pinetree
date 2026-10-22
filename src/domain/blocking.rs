//! Directed block edge: pure data + validation (no I/O).
//!
//! Mirrors Misskey's `MiBlocking{blocker,blockee}` (unique pair). A block
//! always tears down both follow edges and both pending follow requests
//! (see `UserBlockingService.block()`); that teardown lives in the
//! application/storage layer, not here.

use crate::domain::id::DomainError;

/// One block: `blocker_id` blocks `blockee_id`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Blocking {
    pub id: String,
    pub blocker_id: String,
    pub blockee_id: String,
}

impl Blocking {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_block_params(&self.blocker_id, &self.blockee_id)
    }
}

/// Pure rule shared by the application op and storage: ids must be
/// non-empty and a user cannot block themselves (Misskey
/// `BLOCKEE_IS_YOURSELF`).
pub fn validate_block_params(blocker_id: &str, blockee_id: &str) -> Result<(), DomainError> {
    if blocker_id.is_empty() || blockee_id.is_empty() {
        return Err(DomainError::Validation(
            "blocker_id and blockee_id must be non-empty".to_string(),
        ));
    }
    if blocker_id == blockee_id {
        return Err(DomainError::Validation("BLOCKEE_IS_YOURSELF".to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_blocking() -> Blocking {
        Blocking {
            id: "bid00000000000001".to_string(),
            blocker_id: "aid00000000000001".to_string(),
            blockee_id: "aid00000000000002".to_string(),
        }
    }

    #[test]
    fn valid_blocking_passes() {
        assert!(valid_blocking().validate().is_ok());
    }

    #[test]
    fn self_block_rejected() {
        let mut b = valid_blocking();
        b.blockee_id = b.blocker_id.clone();
        assert!(b.validate().is_err());
        assert!(validate_block_params("a", "a").is_err());
    }

    #[test]
    fn empty_ids_rejected() {
        assert!(validate_block_params("", "b").is_err());
        assert!(validate_block_params("a", "").is_err());
    }
}
