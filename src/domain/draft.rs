//! Note drafts + scheduling (`docs/misskey-parity-plan.md` §5; Misskey
//! `NoteDraft.ts:18-167` parity).
//!
//! A draft mirrors the [`Note`](crate::domain::note::Note) create fields
//! plus the poll columns (`has_poll`, choices, multiple, expiry) and the
//! scheduling columns (`scheduled_at_ms`, `is_actually_scheduled`).
//! Validation is deliberately relaxed compared to notes: empty or missing
//! text/files are allowed (upstream `minLength: 0` on `text`, `minItems: 0`
//! on `fileIds` and poll `choices`). There is no `notes/schedule/*`
//! endpoint upstream — scheduling is `scheduledAt + isActuallyScheduled`
//! on the draft itself.

use crate::domain::id::DomainError;
use crate::domain::note::{ReactionAcceptance, Visibility, MAX_FILE_IDS, MAX_NOTE_TEXT_CHARS};

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Draft scheduling policy defaults (Misskey `RoleService` parity:
/// `noteDraftLimit: 10`, `scheduledNoteLimit: 1`). Instance-configurable
/// through [`crate::config::Config`]; the application layer enforces them.
pub const DEFAULT_NOTE_DRAFT_LIMIT: usize = 10;
/// Draft scheduling policy default (Misskey `scheduledNoteLimit: 1`).
pub const DEFAULT_SCHEDULED_NOTE_LIMIT: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NoteDraft {
    pub id: String,
    pub user_id: String,
    pub text: Option<String>,
    pub cw: Option<String>,
    pub visibility: Visibility,
    pub visible_user_ids: Vec<String>,
    pub local_only: bool,
    pub reply_id: Option<String>,
    pub renote_id: Option<String>,
    pub file_ids: Vec<String>,
    pub hashtag: Option<String>,
    pub reaction_acceptance: Option<ReactionAcceptance>,
    pub has_poll: bool,
    pub poll_choices: Vec<String>,
    pub poll_multiple: bool,
    pub poll_expires_at_ms: Option<i64>,
    pub poll_expired_after_ms: Option<i64>,
    pub scheduled_at_ms: Option<i64>,
    pub is_actually_scheduled: bool,
}

impl NoteDraft {
    /// Shape validation: length/uniqueness bounds only (Misskey
    /// `notes/drafts/create|update.ts` `paramDef` parity). Text may be
    /// empty or absent; poll choices may be empty (relaxed draft rules).
    /// Scheduling clock rules live in
    /// [`validate_draft_schedule`](crate::domain::draft::validate_draft_schedule)
    /// so both `create` and `update` share them.
    pub fn validate_shape(&self) -> Result<(), DomainError> {
        if let Some(text) = &self.text {
            if text.chars().count() > MAX_NOTE_TEXT_CHARS {
                return Err(invalid(format!(
                    "text must be at most {MAX_NOTE_TEXT_CHARS} chars"
                )));
            }
        }
        if let Some(cw) = &self.cw {
            let n = cw.chars().count();
            if n == 0 || n > crate::domain::note::MAX_CW_API_CHARS {
                return Err(invalid(format!(
                    "cw must be 1..={} chars",
                    crate::domain::note::MAX_CW_API_CHARS
                )));
            }
        }
        if let Some(hashtag) = &self.hashtag {
            if hashtag.chars().count() > 200 {
                return Err(invalid("hashtag must be at most 200 chars".to_string()));
            }
        }
        if self.file_ids.len() > MAX_FILE_IDS {
            return Err(invalid(format!(
                "file_ids exceeds {MAX_FILE_IDS} items"
            )));
        }
        {
            let mut seen = std::collections::HashSet::new();
            if !self.file_ids.iter().all(|id| seen.insert(id)) {
                return Err(invalid("file_ids must be unique".to_string()));
            }
        }
        if self.poll_choices.len() > 10 {
            return Err(invalid("poll choices exceeds 10 items".to_string()));
        }
        {
            let mut seen = std::collections::HashSet::new();
            if !self.poll_choices.iter().all(|c| seen.insert(c)) {
                return Err(invalid("poll choices must be unique".to_string()));
            }
        }
        for choice in &self.poll_choices {
            let n = choice.chars().count();
            if n == 0 || n > 50 {
                return Err(invalid("poll choices must be 1..=50 chars".to_string()));
            }
        }
        if let Some(expired_after) = self.poll_expired_after_ms {
            if expired_after < 1 {
                return Err(invalid("poll expiredAfter must be >= 1".to_string()));
            }
        }
        Ok(())
    }
}

/// Scheduling clock rules shared by draft `create` and `update` (Misskey
/// `NoteDraftService.validate` parity). `scheduled_at_ms`/`is_scheduled`
/// are the post-merge values (for `update`, the caller merges the patch
/// over the stored row first, mirroring upstream `validate(me, data)`
/// receiving the merged shape):
/// - scheduled without a time → `SCHEDULED_AT_REQUIRED`;
/// - a non-future time → `SCHEDULED_AT_MUST_BE_IN_FUTURE`.
pub fn validate_draft_schedule(
    scheduled_at_ms: Option<i64>,
    is_actually_scheduled: bool,
    now_ms: i64,
) -> Result<(), DomainError> {
    if is_actually_scheduled {
        let Some(at) = scheduled_at_ms else {
            return Err(invalid("SCHEDULED_AT_REQUIRED".to_string()));
        };
        if at <= now_ms {
            return Err(invalid("SCHEDULED_AT_MUST_BE_IN_FUTURE".to_string()));
        }
    }
    Ok(())
}

/// Poll clock rule shared by draft `create`/`update` (Misskey
/// `NoteDraftService.validate` parity): an already-expired poll carries
/// the exact `notes/create` ident.
pub fn validate_draft_poll_expiry(
    poll_expires_at_ms: Option<i64>,
    has_poll: bool,
    now_ms: i64,
) -> Result<(), DomainError> {
    if has_poll {
        if let Some(expires_at) = poll_expires_at_ms {
            if expires_at <= now_ms {
                return Err(invalid(
                    "CANNOT_CREATE_ALREADY_EXPIRED_POLL".to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draft() -> NoteDraft {
        NoteDraft {
            id: "did00000000000001".to_string(),
            user_id: "aid00000000000001".to_string(),
            text: None,
            cw: None,
            visibility: Visibility::Public,
            visible_user_ids: Vec::new(),
            local_only: false,
            reply_id: None,
            renote_id: None,
            file_ids: Vec::new(),
            hashtag: None,
            reaction_acceptance: None,
            has_poll: false,
            poll_choices: Vec::new(),
            poll_multiple: false,
            poll_expires_at_ms: None,
            poll_expired_after_ms: None,
            scheduled_at_ms: None,
            is_actually_scheduled: false,
        }
    }

    #[test]
    fn relaxed_shape_allows_empty_drafts() {
        // Empty text/files with no poll: valid for drafts (rejected for notes).
        let d = draft();
        assert!(d.validate_shape().is_ok());
        let mut empty_text = draft();
        empty_text.text = Some(String::new());
        assert!(empty_text.validate_shape().is_ok());
        let mut blank = draft();
        blank.text = Some("   ".to_string());
        assert!(blank.validate_shape().is_ok());
    }

    #[test]
    fn shape_bounds_match_upstream_param_defs() {
        let mut d = draft();
        d.text = Some("t".repeat(3001));
        assert!(d.validate_shape().is_err());
        d.text = None;
        d.cw = Some(String::new());
        assert!(d.validate_shape().is_err());
        d.cw = Some("c".repeat(101));
        assert!(d.validate_shape().is_err());
        d.cw = None;
        d.hashtag = Some("h".repeat(201));
        assert!(d.validate_shape().is_err());
        d.hashtag = None;
        d.file_ids = vec!["a".to_string(), "a".to_string()];
        assert!(d.validate_shape().is_err());
        d.file_ids = (0..17).map(|i| format!("f{i}")).collect();
        assert!(d.validate_shape().is_err());
        d.file_ids = Vec::new();
        d.poll_choices = vec!["a".to_string(); 11];
        assert!(d.validate_shape().is_err());
        d.poll_choices = vec!["".to_string(), "b".to_string()];
        assert!(d.validate_shape().is_err());
        d.poll_choices = vec!["a".to_string(), "a".to_string()];
        assert!(d.validate_shape().is_err());
        d.poll_choices = Vec::new();
        d.poll_expired_after_ms = Some(0);
        assert!(d.validate_shape().is_err());
    }

    #[test]
    fn schedule_rules_carry_exact_upstream_codes() {
        // Unscheduled drafts pass regardless of `scheduled_at_ms`.
        assert!(validate_draft_schedule(None, false, 100).is_ok());
        assert!(validate_draft_schedule(Some(50), false, 100).is_ok());
        // Scheduled without a time.
        let err = validate_draft_schedule(None, true, 100).unwrap_err();
        assert_eq!(err, DomainError::Validation("SCHEDULED_AT_REQUIRED".to_string()));
        // Non-future times (past and exactly-now) are rejected.
        for at in [99, 100] {
            let err = validate_draft_schedule(Some(at), true, 100).unwrap_err();
            assert_eq!(
                err,
                DomainError::Validation("SCHEDULED_AT_MUST_BE_IN_FUTURE".to_string())
            );
        }
        assert!(validate_draft_schedule(Some(101), true, 100).is_ok());
    }

    #[test]
    fn draft_poll_expiry_reuses_create_ident() {
        let err = validate_draft_poll_expiry(Some(100), true, 100).unwrap_err();
        assert_eq!(
            err,
            DomainError::Validation("CANNOT_CREATE_ALREADY_EXPIRED_POLL".to_string())
        );
        assert!(validate_draft_poll_expiry(Some(101), true, 100).is_ok());
        // No poll attached: no expiry check.
        assert!(validate_draft_poll_expiry(Some(50), false, 100).is_ok());
    }
}
