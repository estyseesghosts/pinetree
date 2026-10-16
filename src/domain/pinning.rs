//! Note pinning (`docs/misskey-parity-plan.md` §7; Misskey
//! `UserNotePining.ts` + `NotePiningService` parity).
//!
//! One row per pinned note, unique over `(user_id, note_id)` (Misskey
//! `@Index(['userId', 'noteId'], { unique: true })` parity). Only the
//! note's own author may pin it — pinning another account's note answers
//! `NO_SUCH_NOTE`, exactly like the upstream `findOneBy({ id, userId })`
//! lookup.

use crate::domain::id::DomainError;

/// Pin limit default (Misskey `RoleService` `pinLimit: 5` parity).
/// Instance-configurable through [`crate::config::Config`].
pub const DEFAULT_PIN_LIMIT: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserNotePining {
    pub id: String,
    pub user_id: String,
    pub note_id: String,
}

impl UserNotePining {
    /// Storage invariant: both ends of the unique key are non-empty.
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.user_id.trim().is_empty() || self.note_id.trim().is_empty() {
            return Err(DomainError::Validation(
                "pinning requires user_id and note_id".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinning_roundtrips_and_validates() {
        let p = UserNotePining {
            id: "pid00000000000001".to_string(),
            user_id: "aid00000000000001".to_string(),
            note_id: "nid00000000000001".to_string(),
        };
        assert!(p.validate().is_ok());
        let json = serde_json::to_string(&p).expect("serializable");
        let back: UserNotePining = serde_json::from_str(&json).expect("deserializable");
        assert_eq!(p, back);

        let empty = UserNotePining {
            id: "x".to_string(),
            user_id: String::new(),
            note_id: "nid00000000000001".to_string(),
        };
        assert!(empty.validate().is_err());
    }
}
