//! Polls (`docs/misskey-parity-plan.md` §4): pure data + validation (no I/O).
//!
//! Follows Misskey `notes/create.ts:168-184`, `NoteCreateService.ts:405-470`,
//! `Poll.ts`, `PollVote.ts`: `choices[2..10]` unique, each `1..=50` chars,
//! `multiple`, `expiresAt | expiredAfter` (`expiredAfter` wins, both in ms).

use crate::domain::id::DomainError;

/// A poll attached to one note (`polls` table, keyed by `note_id`).
/// `votes[i]` counts votes for `choices[i]` (Misskey `Poll.votes` parity).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Poll {
    pub note_id: String,
    pub choices: Vec<String>,
    pub votes: Vec<u64>,
    pub multiple: bool,
    /// Expiry as unix millis, `None` = never expires.
    pub expires_at_ms: Option<i64>,
}

/// One vote (`poll_votes` table, `UNIQUE(user, note, choice)`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PollVote {
    pub id: String,
    pub note_id: String,
    pub user_id: String,
    pub choice: usize,
}

/// Validated poll construction input (carried on `CreateNoteInput::poll`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PollInput {
    pub choices: Vec<String>,
    pub multiple: bool,
    pub expires_at_ms: Option<i64>,
}

impl Poll {
    /// True when the poll has expired as of `now_ms` (`None` never expires).
    /// Misskey `polls/vote.ts` parity: `poll.expiresAt < now` rejects votes
    /// (strictly less-than; equality is still votable).
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms.is_some_and(|exp| exp < now_ms)
    }
}

impl PollInput {
    /// Validate Misskey `notes/create.ts` poll params. `now_ms` is the
    /// server clock (expiry must lie in the future).
    ///
    /// Errors carry exact Misskey idents: choice-shape violations are plain
    /// validation messages (the endpoint maps them to `INVALID_PARAM`), an
    /// already-past expiry is `CANNOT_CREATE_ALREADY_EXPIRED_POLL`.
    pub fn validate(&self, now_ms: i64) -> Result<(), DomainError> {
        if self.choices.len() < 2 || self.choices.len() > 10 {
            return Err(DomainError::Validation(
                "poll choices must be 2..=10 items".to_string(),
            ));
        }
        for choice in &self.choices {
            let n = choice.chars().count();
            if n == 0 || n > 50 {
                return Err(DomainError::Validation(
                    "poll choice must be 1..=50 chars".to_string(),
                ));
            }
        }
        let mut seen = std::collections::HashSet::new();
        for choice in &self.choices {
            if !seen.insert(choice) {
                return Err(DomainError::Validation(
                    "poll choices must be unique".to_string(),
                ));
            }
        }
        if let Some(expires_at) = self.expires_at_ms {
            // Misskey `NoteCreateService` parity: expiry must be future time.
            if expires_at <= now_ms {
                return Err(DomainError::Validation(
                    "CANNOT_CREATE_ALREADY_EXPIRED_POLL".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(choices: &[&str], expires_at_ms: Option<i64>) -> PollInput {
        PollInput {
            choices: choices.iter().map(|s| s.to_string()).collect(),
            multiple: false,
            expires_at_ms,
        }
    }

    #[test]
    fn poll_choice_count_bounds() {
        let now = 1_760_400_000_000;
        assert!(input(&["a"], None).validate(now).is_err());
        assert!(input(&["a", "b"], None).validate(now).is_ok());
        let ten: Vec<String> = (0..10).map(|i| format!("c{i}")).collect();
        let ten_refs: Vec<&str> = ten.iter().map(String::as_str).collect();
        assert!(input(&ten_refs, None).validate(now).is_ok());
        let eleven: Vec<String> = (0..11).map(|i| format!("c{i}")).collect();
        let eleven_refs: Vec<&str> = eleven.iter().map(String::as_str).collect();
        assert!(input(&eleven_refs, None).validate(now).is_err());
    }

    #[test]
    fn poll_choice_text_bounds_and_uniqueness() {
        let now = 1_760_400_000_000;
        assert!(input(&["", "b"], None).validate(now).is_err());
        assert!(input(&["a", &"x".repeat(51)], None).validate(now).is_err());
        assert!(input(&["a", &"x".repeat(50)], None).validate(now).is_ok());
        assert!(input(&["a", "a"], None).validate(now).is_err());
    }

    #[test]
    fn poll_already_expired_rejected_with_exact_code() {
        let now = 1_760_400_000_000;
        let err = input(&["a", "b"], Some(now - 1)).validate(now).unwrap_err();
        assert_eq!(
            err.to_string(),
            "validation error: CANNOT_CREATE_ALREADY_EXPIRED_POLL"
        );
        assert!(input(&["a", "b"], Some(now + 1)).validate(now).is_ok());
        // Equality with now is still expired (must be strictly future).
        assert!(input(&["a", "b"], Some(now)).validate(now).is_err());
    }

    #[test]
    fn poll_is_expired_matches_vote_gate() {
        let poll = Poll {
            note_id: "n".to_string(),
            choices: vec!["a".to_string(), "b".to_string()],
            votes: vec![0, 0],
            multiple: false,
            expires_at_ms: Some(100),
        };
        assert!(!poll.is_expired(100));
        assert!(poll.is_expired(101));
        let eternal = Poll {
            expires_at_ms: None,
            ..poll
        };
        assert!(!eternal.is_expired(i64::MAX));
    }
}
