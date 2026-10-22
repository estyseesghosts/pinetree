use super::JobWorker;
use crate::application::ports::PortError;

impl JobWorker {
    /// Run one `QuoteStampReverify` job: re-dereference the stamp offered by
    /// an `Accept` whose inbox-time fetch failed transiently, and approve on
    /// success (recorded atomically with the state change). Decided/gone
    /// quotes complete quietly; invalid stamp content completes quietly
    /// (never approval); only transport/storage faults retry.
    pub(crate) fn handle_quote_stamp_reverify(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::QuoteStampReverifyPayload;
        use crate::domain::quote::QuoteAuthorizationState;

        let parsed: QuoteStampReverifyPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("quote-reverify payload invalid: {e}")))?;
        let services = &self.services;
        let Some(note) = services
            .notes
            .find_note_by_quote_request(&parsed.request_uri)?
        else {
            return Ok(());
        };
        let Some(quote) = note.quote.as_ref() else {
            return Ok(());
        };
        if !matches!(
            quote.authorization,
            QuoteAuthorizationState::Pending | QuoteAuthorizationState::Unknown
        ) {
            // Already decided (approved/rejected/revoked): converge.
            return Ok(());
        }
        let base = services.public_url.trim_end_matches('/');
        let quote_note_uri = note
            .uri
            .clone()
            .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
        // Authority inputs mirror the inbox-time check.
        let (target_uri, quoted_author_actor_uri) = match quote.target_note_id.as_deref() {
            Some(tid) => match services.notes.get_note(tid)? {
                Some(target) => {
                    let author_uri = match services.accounts.find_by_id(&target.author_id)? {
                        Some(acc) => acc.actor_url(base),
                        None => return Ok(()),
                    };
                    let canonical = target
                        .uri
                        .unwrap_or_else(|| format!("{base}/notes/{}", target.id));
                    (canonical, author_uri)
                }
                None => return Ok(()),
            },
            None => return Ok(()),
        };
        let doc = match crate::federation::delivery::fetch_quote_stamp_doc(
            services,
            &services.federation_http,
            &parsed.stamp_uri,
        ) {
            Ok(doc) => doc,
            // Transient (network/5xx): retry with worker backoff. The
            // `Retry` disposition quarantines past the attempt budget, so
            // re-verification is rate-limited and never spins forever.
            Err(PortError::Validation(_)) => {
                return Err(PortError::StorageUnavailable(
                    "transient stamp fetch failure".to_string(),
                ));
            }
            Err(e) => return Err(e),
        };
        if crate::activitypub::quotes::verify_stamp_for_quote(
            &doc,
            &parsed.stamp_uri,
            &quote_note_uri,
            &target_uri,
            &quoted_author_actor_uri,
        )
        .is_err()
        {
            // Invalid stamp content: invalid evidence, never approval.
            return Ok(());
        }
        let verified_at = services.clock.now_ms();
        match services.dedup.record_quote_decision_accept(
            &parsed.accept_activity_id,
            &note.id,
            parsed.stamp_uri.trim(),
            verified_at,
            Vec::new(),
        ) {
            Ok(_) => Ok(()),
            // Moved on concurrently (or the note/quote is gone): converge.
            Err(PortError::Validation(_)) | Err(PortError::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
