use crate::application::commands::AppServices;

use crate::application::ports::{NewJob, PortError};
use crate::domain::note::Note;
use crate::domain::quote::QuoteAuthorizationState;

/// Outcome of an inbound decision (`Accept`/`Reject`) or stamp `Delete`
/// for a quote we authored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuoteDecisionOutcome {
    /// State transitioned (`Approved`, `Rejected`, or `Revoked`).
    Applied(Note),
    /// Replay of an already-seen activity: nothing new.
    Duplicate,
    /// Not actionable (unknown request/stamp, stamp invalid, illegal
    /// transition, already revoked). Transport-accepted, ignored.
    Ignored,
    /// The offered stamp could not be fetched right now (transient): the
    /// quote stays `Pending` and the activity is deliberately NOT recorded,
    /// so a later retry can still be processed. The carried job re-verifies
    /// off-request with worker backoff. Never a rejection.
    StampFetchPending(NewJob),
}

impl AppServices {
    pub fn handle_inbound_quote_decision_accept(
        &self,
        activity_id: &str,
        request_activity_uri: &str,
        stamp_uri: &str,
    ) -> Result<QuoteDecisionOutcome, PortError> {
        let Some(note) = self
            .notes
            .find_note_by_quote_request(request_activity_uri)?
        else {
            return Ok(QuoteDecisionOutcome::Ignored);
        };
        // Only our own quotes are decided here.
        let Some(quoter) = self.accounts.find_by_id(&note.author_id)? else {
            return Ok(QuoteDecisionOutcome::Ignored);
        };
        if !quoter.is_local() {
            return Ok(QuoteDecisionOutcome::Ignored);
        }
        let Some(quote) = note.quote.as_ref() else {
            return Ok(QuoteDecisionOutcome::Ignored);
        };
        if quote.authorization == QuoteAuthorizationState::Approved
            && quote.authorization_uri.as_deref() == Some(stamp_uri.trim())
        {
            // Already approved with this stamp (replay that missed dedup):
            // converge without new work.
            return Ok(QuoteDecisionOutcome::Duplicate);
        }
        let Some(target_id) = quote.target_note_id.clone() else {
            return Ok(QuoteDecisionOutcome::Ignored);
        };
        let target_uri = quote.target_uri.clone();
        let quote_note_uri = note.uri.clone().unwrap_or_else(|| {
            format!(
                "{}/notes/{}",
                self.public_url.trim_end_matches('/'),
                note.id
            )
        });
        // The stamp's authority is the quoted post's author.
        let quoted_author_actor_uri = match self.notes.get_note(&target_id)? {
            Some(target) => match self.accounts.find_by_id(&target.author_id)? {
                Some(acc) => acc.actor_url(self.public_url.trim_end_matches('/')),
                None => return Ok(QuoteDecisionOutcome::Ignored),
            },
            None => return Ok(QuoteDecisionOutcome::Ignored),
        };
        // Non-HTTP stamp URIs are invalid evidence, not a fetchable stamp:
        // ignored, never approval and never a retryable fetch.
        let stamp_trimmed = stamp_uri.trim();
        if !(stamp_trimmed.starts_with("http://") || stamp_trimmed.starts_with("https://")) {
            return Ok(QuoteDecisionOutcome::Ignored);
        }
        let doc = match crate::federation::delivery::fetch_quote_stamp_doc(
            self,
            &self.federation_http,
            stamp_trimmed,
        ) {
            Ok(doc) => doc,
            Err(_) => {
                let retry = NewJob::new_json(
                    crate::application::ports::JobKind::QuoteStampReverify,
                    &crate::application::ports::QuoteStampReverifyPayload {
                        accept_activity_id: activity_id.to_string(),
                        request_uri: request_activity_uri.to_string(),
                        stamp_uri: stamp_trimmed.to_string(),
                    },
                    format!("quote-reverify:{activity_id}"),
                    self.now_ms(),
                )?;
                return Ok(QuoteDecisionOutcome::StampFetchPending(retry));
            }
        };
        if crate::activitypub::quotes::verify_stamp_for_quote(
            &doc,
            stamp_trimmed,
            &quote_note_uri,
            &target_uri,
            &quoted_author_actor_uri,
        )
        .is_err()
        {
            // Invalid stamp content: invalid evidence, never approval — and
            // never `Rejected` either (only an explicit `Reject` rejects).
            return Ok(QuoteDecisionOutcome::Ignored);
        }
        let verified_at = self.now_ms();
        match self.dedup.record_quote_decision_accept(
            activity_id,
            &note.id,
            stamp_uri.trim(),
            verified_at,
            Vec::new(),
        ) {
            Ok(Some(updated)) => Ok(QuoteDecisionOutcome::Applied(updated)),
            Ok(None) => Ok(QuoteDecisionOutcome::Duplicate),
            // The quote moved on already (e.g. rejected/revoked since):
            // accept the transport, change nothing.
            Err(PortError::Validation(_)) | Err(PortError::NotFound(_)) => {
                Ok(QuoteDecisionOutcome::Ignored)
            }
            Err(e) => Err(e),
        }
    }

    /// Apply an inbound `Reject(QuoteRequest)` to our pending quote. The
    /// relation and API projection are retained; only the state flips.
    pub fn handle_inbound_quote_decision_reject(
        &self,
        activity_id: &str,
        request_activity_uri: &str,
    ) -> Result<QuoteDecisionOutcome, PortError> {
        let Some(note) = self
            .notes
            .find_note_by_quote_request(request_activity_uri)?
        else {
            return Ok(QuoteDecisionOutcome::Ignored);
        };
        let Some(quoter) = self.accounts.find_by_id(&note.author_id)? else {
            return Ok(QuoteDecisionOutcome::Ignored);
        };
        if !quoter.is_local() || note.quote.is_none() {
            return Ok(QuoteDecisionOutcome::Ignored);
        }
        match self
            .dedup
            .record_quote_decision_reject(activity_id, &note.id, Vec::new())
        {
            Ok(Some(updated)) => Ok(QuoteDecisionOutcome::Applied(updated)),
            Ok(None) => Ok(QuoteDecisionOutcome::Duplicate),
            Err(PortError::Validation(_)) | Err(PortError::NotFound(_)) => {
                Ok(QuoteDecisionOutcome::Ignored)
            }
            Err(e) => Err(e),
        }
    }

    /// Apply an inbound `Delete(QuoteAuthorization)` for a stamp backing our
    /// quote: `Approved → Revoked`, and the revocation is forwarded to the
    /// quoting note's audience when we own the quote post.
    pub fn handle_inbound_quote_stamp_delete(
        &self,
        activity_id: &str,
        stamp_uri: &str,
    ) -> Result<QuoteDecisionOutcome, PortError> {
        let stamp_uri = stamp_uri.trim();
        // A stamp we authored can only be deleted by us: a remote `Delete`
        // for it is unauthorized (the caller maps this to 401).
        if self.notes.get_quote_stamp(stamp_uri)?.is_some() {
            return Ok(QuoteDecisionOutcome::Ignored);
        }
        let Some(note) = self.notes.find_note_by_quote_stamp(stamp_uri)? else {
            return Ok(QuoteDecisionOutcome::Ignored);
        };
        let Some(quoter) = self.accounts.find_by_id(&note.author_id)? else {
            return Ok(QuoteDecisionOutcome::Ignored);
        };
        if !quoter.is_local() || note.quote.is_none() {
            return Ok(QuoteDecisionOutcome::Ignored);
        }
        if note.quote.as_ref().map(|q| q.authorization) == Some(QuoteAuthorizationState::Revoked) {
            // Already revoked: converge without re-forwarding (breaks
            // Delete-forwarding loops between servers).
            return Ok(QuoteDecisionOutcome::Duplicate);
        }
        let forward_job = self.federate_job(
            &crate::federation::outbox::FederateDeliverPayload::QuoteStampDeleted {
                delete_id: self.ids.generate(),
                actor_id: note.author_id.clone(),
                stamp_uri: stamp_uri.to_string(),
                quote_note_id: note.id.clone(),
                inbox_override: None,
            },
            "federate:quote-stamp-delete",
        )?;
        match self
            .dedup
            .record_quote_stamp_delete(activity_id, &note.id, vec![forward_job])
        {
            Ok(Some(updated)) => Ok(QuoteDecisionOutcome::Applied(updated)),
            Ok(None) => Ok(QuoteDecisionOutcome::Duplicate),
            Err(PortError::Validation(_)) | Err(PortError::NotFound(_)) => {
                Ok(QuoteDecisionOutcome::Ignored)
            }
            Err(e) => Err(e),
        }
    }
}
