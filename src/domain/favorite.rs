//! Note favorites (`notes/favorites/*`, `i/favorites`, `notes/state`);
//!
//! One row per (account, note), unique over the pair (Misskey
//! `MiNoteFavorite` `@Unique(['userId', 'noteId'])` parity).

use crate::domain::id::DomainError;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NoteFavorite {
    pub id: String,
    pub user_id: String,
    pub note_id: String,
}

impl NoteFavorite {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.user_id.trim().is_empty() || self.note_id.trim().is_empty() {
            return Err(DomainError::Validation(
                "favorite requires user_id and note_id".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn favorite_roundtrips_and_validates() {
        let f = NoteFavorite {
            id: "fav00000000000001".to_string(),
            user_id: "aid00000000000001".to_string(),
            note_id: "nid00000000000001".to_string(),
        };
        assert!(f.validate().is_ok());
        let json = serde_json::to_string(&f).expect("serializable");
        let back: NoteFavorite = serde_json::from_str(&json).expect("deserializable");
        assert_eq!(f, back);
        let empty = NoteFavorite {
            id: "x".to_string(),
            user_id: String::new(),
            note_id: "nid00000000000001".to_string(),
        };
        assert!(empty.validate().is_err());
    }
}
