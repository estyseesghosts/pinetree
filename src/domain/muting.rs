//! Mute edges: pure data + validation (no I/O).
//!
//! Mirrors Misskey's `MiMuting{muter,mutee,expiresAt?}` (unique pair, optional
//! future expiry) and `MiRenoteMuting{muter,mutee}` (unique pair, hides the
//! muted user's pure renotes from the muter's feeds).

use crate::domain::id::DomainError;

/// One mute: `muter_id` mutes `mutee_id` until `expires_at_ms` (`None` =
/// indefinite). An expired mute filters nothing; the read-time filter treats
/// `expires_at_ms <= now` as unmuted (Misskey expires these with a periodic
/// cleaner; pinetree enforces expiry at read time so no second path exists).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Muting {
    pub id: String,
    pub muter_id: String,
    pub mutee_id: String,
    pub expires_at_ms: Option<i64>,
}

impl Muting {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_mute_params(&self.muter_id, &self.mutee_id)?;
        if let Some(expires_at) = self.expires_at_ms {
            validate_mute_expiry(expires_at, 0)?;
        }
        Ok(())
    }

    /// True when this mute hides content at `now_ms`.
    pub fn is_active_at(&self, now_ms: i64) -> bool {
        self.expires_at_ms.map(|exp| exp > now_ms).unwrap_or(true)
    }
}

/// One renote-mute: `muter_id` hides `mutee_id`'s pure renotes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RenoteMuting {
    pub id: String,
    pub muter_id: String,
    pub mutee_id: String,
}

impl RenoteMuting {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_mute_params(&self.muter_id, &self.mutee_id)
    }
}

/// Pure rule shared by mute and renote-mute: ids must be non-empty and a
/// user cannot mute themselves (Misskey `MUTEE_IS_YOURSELF`).
pub fn validate_mute_params(muter_id: &str, mutee_id: &str) -> Result<(), DomainError> {
    if muter_id.is_empty() || mutee_id.is_empty() {
        return Err(DomainError::Validation(
            "muter_id and mutee_id must be non-empty".to_string(),
        ));
    }
    if muter_id == mutee_id {
        return Err(DomainError::Validation("MUTEE_IS_YOURSELF".to_string()));
    }
    Ok(())
}

/// A mute expiry must lie in the future (`expires_at_ms > now_ms`), matching
/// Misskey `mute/create` (`expiresAt` "must lie in the future"; a past value
/// creates no mute).
pub fn validate_mute_expiry(expires_at_ms: i64, now_ms: i64) -> Result<(), DomainError> {
    if expires_at_ms <= now_ms {
        return Err(DomainError::Validation(
            "EXPIRES_AT_MUST_BE_IN_FUTURE".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_mute_rejected() {
        assert!(validate_mute_params("a", "a").is_err());
        assert_eq!(
            validate_mute_params("a", "a"),
            Err(DomainError::Validation("MUTEE_IS_YOURSELF".to_string()))
        );
    }

    #[test]
    fn empty_ids_rejected() {
        assert!(validate_mute_params("", "b").is_err());
        assert!(validate_mute_params("a", "").is_err());
    }

    #[test]
    fn expiry_must_be_future() {
        assert!(validate_mute_expiry(101, 100).is_ok());
        assert!(validate_mute_expiry(100, 100).is_err());
        assert!(validate_mute_expiry(99, 100).is_err());
    }

    #[test]
    fn active_at_respects_expiry() {
        let indefinite = Muting {
            id: "m1".to_string(),
            muter_id: "a".to_string(),
            mutee_id: "b".to_string(),
            expires_at_ms: None,
        };
        assert!(indefinite.is_active_at(1_000_000));
        let timed = Muting {
            expires_at_ms: Some(200),
            ..indefinite.clone()
        };
        assert!(timed.is_active_at(100));
        assert!(!timed.is_active_at(200));
        assert!(!timed.is_active_at(300));
    }
}
