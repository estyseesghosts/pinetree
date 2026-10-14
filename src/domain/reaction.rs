//! Note reaction: pure data + validation (no I/O).

use crate::domain::id::DomainError;
use crate::domain::note::ReactionAcceptance;

pub const FALLBACK_REACTION: &str = "❤️";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Reaction {
    pub id: String,
    pub user_id: String,
    pub note_id: String,
    pub reaction: String,
}

impl Reaction {
    pub fn validate(&self) -> Result<(), DomainError> {
        let n = self.reaction.chars().count();
        if n == 0 || n > 260 {
            return Err(DomainError::Validation(
                "reaction must be 1..=260 chars".to_string(),
            ));
        }
        Ok(())
    }
}

/// Coerces a reaction to the fallback heart when the note's acceptance only
/// allows likes (like-only notes, or like-only-for-remote notes reacting from
/// a remote account). Otherwise returns the reaction unchanged.
pub fn coerce_reaction(
    reaction: &str,
    acceptance: Option<ReactionAcceptance>,
    is_remote: bool,
) -> String {
    match acceptance {
        Some(ReactionAcceptance::LikeOnly) => FALLBACK_REACTION.to_string(),
        Some(ReactionAcceptance::LikeOnlyForRemote) if is_remote => {
            FALLBACK_REACTION.to_string()
        }
        _ => reaction.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reaction(s: &str) -> Reaction {
        Reaction {
            id: "rid00000000000001".to_string(),
            user_id: "aid00000000000001".to_string(),
            note_id: "nid00000000000001".to_string(),
            reaction: s.to_string(),
        }
    }

    #[test]
    fn reaction_bounds() {
        assert!(reaction("👍").validate().is_ok());
        assert!(reaction(":custom_emoji@.:").validate().is_ok());
        assert!(reaction("").validate().is_err());
        assert!(reaction(&"e".repeat(260)).validate().is_ok());
        assert!(reaction(&"e".repeat(261)).validate().is_err());
    }

    #[test]
    fn coerce_reaction_cases() {
        assert_eq!(
            coerce_reaction("👍", Some(ReactionAcceptance::LikeOnly), false),
            FALLBACK_REACTION
        );
        assert_eq!(
            coerce_reaction("👍", Some(ReactionAcceptance::LikeOnly), true),
            FALLBACK_REACTION
        );
        assert_eq!(
            coerce_reaction(
                "👍",
                Some(ReactionAcceptance::LikeOnlyForRemote),
                true
            ),
            FALLBACK_REACTION
        );
        assert_eq!(
            coerce_reaction(
                "👍",
                Some(ReactionAcceptance::LikeOnlyForRemote),
                false
            ),
            "👍"
        );
        assert_eq!(
            coerce_reaction("👍", Some(ReactionAcceptance::NonSensitiveOnly), true),
            "👍"
        );
        assert_eq!(coerce_reaction("👍", None, true), "👍");
    }
}
