//! Quote-post domain types: pure data + validation (no I/O).
//!
//! A *quote* is a note with its own commentary that embeds another note,
//! represented on the Misskey API as `renoteId`/`renote` (same shape as an
//! ordinary renote) but semantically distinguished internally by the presence
//! of a [`QuoteReference`]. Ordinary (pure) renotes have `quote == None`;
//! every recognized structured quote — FEP-044f or legacy — normalizes into
//! `Some(QuoteReference)` at the federation boundary.
//!
//! Wire-format parsing lives in `crate::activitypub::quotes`; storage and
//! adapters must consume only these domain types.

use serde::{Deserialize, Serialize};

use crate::domain::id::DomainError;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Authorization state of a quote relationship.
///
/// Kept separate from quote *visibility* and from whether a client renders
/// the card: rejected/unapproved quotes still render (Keystone policy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QuoteAuthorizationState {
    /// Pinetree authored the quote and is awaiting the target author's decision.
    Pending,
    /// A valid authorization stamp was verified and is not known revoked.
    Approved,
    /// A valid FEP-044f exception applies (self-quote or quoter mentioned by
    /// the original post). Maps to API `approved`.
    ApprovedByException,
    /// Pinetree authored the quote and received a valid `Reject` for its
    /// matching `QuoteRequest`. The relation is retained for rendering.
    Rejected,
    /// A previously valid stamp was validly deleted/revoked.
    Revoked,
    /// A quote relationship is recognized, but no valid stamp or FEP
    /// exception establishes approval. Normal for legacy/non-FEP quotes;
    /// this is NOT an explicit rejection.
    Unapproved,
    /// Validation is currently inconclusive (e.g. transient lookup failure
    /// while checking an expected stamp). A timeout is not a rejection.
    Unknown,
}

impl QuoteAuthorizationState {
    /// Misskey API projection (`quoteAuthorizationStatus` extension).
    /// `ApprovedByException` folds into `approved` to keep the API
    /// intentionally simple.
    pub fn api_status(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved | Self::ApprovedByException => "approved",
            Self::Rejected => "rejected",
            Self::Revoked => "revoked",
            Self::Unapproved => "unapproved",
            Self::Unknown => "unknown",
        }
    }

    /// Whether `next` is a legal authorization transition from `self`.
    /// Same-state repeats are always allowed (idempotent replays).
    pub fn allows_transition(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        match (self, next) {
            (Self::Pending, Self::Approved)
            | (Self::Pending, Self::ApprovedByException)
            | (Self::Pending, Self::Rejected)
            | (Self::Pending, Self::Unapproved)
            | (Self::Pending, Self::Unknown) => true,
            (Self::Unapproved, Self::Approved)
            | (Self::Unapproved, Self::ApprovedByException)
            | (Self::Unapproved, Self::Unknown) => true,
            (Self::Unknown, Self::Approved)
            | (Self::Unknown, Self::ApprovedByException)
            | (Self::Unknown, Self::Unapproved)
            | (Self::Unknown, Self::Pending) => true,
            (Self::Approved, Self::Revoked)
            | (Self::ApprovedByException, Self::Revoked) => true,
            // A fresh stamp after revocation can re-approve (new verification).
            (Self::Revoked, Self::Approved)
            | (Self::Revoked, Self::ApprovedByException)
            | (Self::Revoked, Self::Pending) => true,
            // A fresh decision supersedes a rejection.
            (Self::Rejected, Self::Approved)
            | (Self::Rejected, Self::ApprovedByException) => true,
            _ => false,
        }
    }
}

/// Domain-level quote record. All supported wire formats normalize into this
/// same record; format-specific parsing belongs at the federation boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteReference {
    /// Canonical ActivityPub object ID of the quoted post (never the HTML `url`).
    pub target_uri: String,
    /// Resolved local DB Note ID, when the target is stored locally.
    /// `None` when unresolved — the URI is retained, never discarded.
    #[serde(default)]
    pub target_note_id: Option<String>,
    pub authorization: QuoteAuthorizationState,
    /// Verified `QuoteAuthorization` IRI, if a valid stamp exists.
    #[serde(default)]
    pub authorization_uri: Option<String>,
    /// Locally created `QuoteRequest` IRI, if Pinetree authored the quote.
    #[serde(default)]
    pub request_activity_uri: Option<String>,
    /// Millisecond timestamp of the last successful stamp verification.
    #[serde(default)]
    pub last_verified_at: Option<i64>,
}

impl QuoteReference {
    /// A freshly recognized inbound quote with no authorization evidence.
    /// Legacy-only payloads land here: `unapproved`, never `rejected`.
    pub fn unapproved(target_uri: String) -> Self {
        Self {
            target_uri,
            target_note_id: None,
            authorization: QuoteAuthorizationState::Unapproved,
            authorization_uri: None,
            request_activity_uri: None,
            last_verified_at: None,
        }
    }

    /// A locally authored quote awaiting the target author's decision.
    pub fn pending(
        target_uri: String,
        target_note_id: Option<String>,
        request_activity_uri: Option<String>,
    ) -> Self {
        Self {
            target_uri,
            target_note_id,
            authorization: QuoteAuthorizationState::Pending,
            authorization_uri: None,
            request_activity_uri,
            last_verified_at: None,
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        let uri = self.target_uri.trim();
        if uri.is_empty() {
            return Err(invalid("quote target_uri is required".to_string()));
        }
        if uri.chars().count() > 512 {
            return Err(invalid("quote target_uri exceeds 512 chars".to_string()));
        }
        if !(uri.starts_with("http://") || uri.starts_with("https://")) {
            return Err(invalid(
                "quote target_uri must be an http(s) URI".to_string(),
            ));
        }
        if let Some(auth) = self.authorization_uri.as_deref() {
            let auth = auth.trim();
            if auth.is_empty()
                || !(auth.starts_with("http://") || auth.starts_with("https://"))
            {
                return Err(invalid(
                    "quote authorization_uri must be an http(s) URI".to_string(),
                ));
            }
        }
        if let Some(req) = self.request_activity_uri.as_deref() {
            let req = req.trim();
            if req.is_empty()
                || !(req.starts_with("http://") || req.starts_with("https://"))
            {
                return Err(invalid(
                    "quote request_activity_uri must be an http(s) URI".to_string(),
                ));
            }
        }
        // Approval must carry evidence: a stamp or an exception.
        if matches!(
            self.authorization,
            QuoteAuthorizationState::Approved
        ) && self.authorization_uri.as_deref().map(str::trim) == Some("")
            || matches!(
                self.authorization,
                QuoteAuthorizationState::Approved
            ) && self.authorization_uri.is_none()
        {
            return Err(invalid(
                "approved quotes require an authorization stamp URI".to_string(),
            ));
        }
        Ok(())
    }
}

/// A `QuoteAuthorization` stamp authored by a local user for someone else's
/// quote of their post. Persisted so the stamp IRI stays dereferenceable;
/// revocation flips `revoked` (the row is retained for idempotent replays).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteStamp {
    /// Full stamp IRI (`{base}/users/{author}/stamps/{id}`).
    pub id: String,
    /// Local author of the quoted post (`attributedTo`).
    pub author_id: String,
    /// Quote Note ID being authorized (remote URI when the quoter is remote).
    pub interacting_object_uri: String,
    /// Quoted local Note ID.
    pub interaction_target_note_id: String,
    /// The `QuoteRequest` activity this stamp answers.
    pub request_activity_uri: String,
    pub revoked: bool,
    pub created_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_status_mapping() {
        assert_eq!(QuoteAuthorizationState::Pending.api_status(), "pending");
        assert_eq!(QuoteAuthorizationState::Approved.api_status(), "approved");
        assert_eq!(
            QuoteAuthorizationState::ApprovedByException.api_status(),
            "approved"
        );
        assert_eq!(QuoteAuthorizationState::Rejected.api_status(), "rejected");
        assert_eq!(QuoteAuthorizationState::Revoked.api_status(), "revoked");
        assert_eq!(
            QuoteAuthorizationState::Unapproved.api_status(),
            "unapproved"
        );
        assert_eq!(QuoteAuthorizationState::Unknown.api_status(), "unknown");
    }

    #[test]
    fn transitions() {
        use QuoteAuthorizationState as S;
        // Idempotent repeats always allowed.
        for s in [
            S::Pending,
            S::Approved,
            S::ApprovedByException,
            S::Rejected,
            S::Revoked,
            S::Unapproved,
            S::Unknown,
        ] {
            assert!(s.allows_transition(s), "{s:?} self-transition");
        }
        assert!(S::Pending.allows_transition(S::Approved));
        assert!(S::Pending.allows_transition(S::Rejected));
        assert!(S::Approved.allows_transition(S::Revoked));
        assert!(S::ApprovedByException.allows_transition(S::Revoked));
        assert!(S::Unapproved.allows_transition(S::Approved));
        assert!(S::Rejected.allows_transition(S::Approved));
        // Revocation is not terminal: a fresh stamp can re-approve.
        assert!(S::Revoked.allows_transition(S::Approved));
        assert!(S::Revoked.allows_transition(S::ApprovedByException));
        assert!(!S::Revoked.allows_transition(S::Rejected));
        // No false approval: absence of evidence never approves.
        assert!(!S::Unapproved.allows_transition(S::Rejected));
        assert!(!S::Pending.allows_transition(S::Revoked));
        assert!(!S::Approved.allows_transition(S::Rejected));
        assert!(!S::Rejected.allows_transition(S::Revoked));
    }

    #[test]
    fn quote_reference_validation() {
        let mut q =
            QuoteReference::unapproved("https://remote.example/notes/1".to_string());
        assert!(q.validate().is_ok());
        // request_activity_uri, when present, must be an http(s) URI.
        q.request_activity_uri = Some("not-a-uri".to_string());
        assert!(q.validate().is_err());
        q.request_activity_uri = Some("https://local.example/activities/1".to_string());
        assert!(q.validate().is_ok());
        q.request_activity_uri = None;

        q.target_uri = "gopher://x/y".to_string();
        assert!(q.validate().is_err());

        q.target_uri = "https://remote.example/notes/1".to_string();
        q.authorization = QuoteAuthorizationState::Approved;
        // Approved without a stamp is rejected: no false approval.
        assert!(q.validate().is_err());
        q.authorization_uri = Some("https://remote.example/stamps/1".to_string());
        assert!(q.validate().is_ok());
    }

    #[test]
    fn legacy_constructor_is_unapproved_never_rejected() {
        let q = QuoteReference::unapproved("https://remote.example/notes/9".to_string());
        assert_eq!(q.authorization, QuoteAuthorizationState::Unapproved);
        assert_eq!(q.authorization.api_status(), "unapproved");
        assert!(q.target_note_id.is_none());
    }
}
