use crate::application::commands::AppServices;

use crate::application::ports::PortError;
use crate::domain::note::Visibility;
use crate::domain::quote::QuoteStamp;

/// Outcome of an inbound `QuoteRequest` for a local post.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuoteRequestOutcome {
    /// Auto-approved: stamp persisted, `Accept` delivery enqueued.
    Accepted,
    /// Policy-denied: `Reject` delivery enqueued, no stamp created.
    Rejected,
    /// Replay of an already-seen request activity: no new state or jobs.
    Duplicate,
    /// The request is not actionable (unknown/deleted target, target not
    /// ours, instrument mismatch). Accepted at the transport level, ignored.
    Ignored,
}

impl AppServices {
    pub fn handle_inbound_quote_request(
        &self,
        activity_id: &str,
        quoter_account_id: &str,
        quoter_actor_uri: &str,
        quoter_inbox_url: Option<&str>,
        quoted_note_id: &str,
        instrument_uri: &str,
        instrument_quote_target_uri: &str,
        instrument_author_actor_uri: &str,
    ) -> Result<QuoteRequestOutcome, PortError> {
        let base = self.public_url.trim_end_matches('/').to_string();
        let Some(target) = self.notes.get_note(quoted_note_id)? else {
            return Ok(QuoteRequestOutcome::Ignored);
        };
        // The target must be a live local post. (Remote-cached copies are
        // never decided here: only the post's own server issues stamps.)
        if target.is_deleted {
            return Ok(QuoteRequestOutcome::Ignored);
        }
        let Some(target_author) = self.accounts.find_by_id(&target.author_id)? else {
            return Ok(QuoteRequestOutcome::Ignored);
        };
        if !target_author.is_local() {
            return Ok(QuoteRequestOutcome::Ignored);
        }
        let quoted_canonical_uri = target
            .uri
            .clone()
            .unwrap_or_else(|| format!("{base}/notes/{}", target.id));
        // The instrument must quote exactly this post and be the quoter's own.
        if instrument_quote_target_uri.trim() != quoted_canonical_uri
            || instrument_author_actor_uri.trim() != quoter_actor_uri.trim()
        {
            return Ok(QuoteRequestOutcome::Ignored);
        }
        let target_author_uri = target_author.actor_url(&base);

        // Policy: FEP exceptions and public/unlisted auto-approve; anything
        // tighter is answered with Reject only when the requester is
        // entitled to see the target (otherwise silent Ignore, so a quote
        // request can never oracle private-post existence). `manualApproval`
        // is never advertised, so manual review is never offered.
        let self_quote = quoter_actor_uri.trim() == target_author_uri.trim();
        let mentioned = target.mentions.iter().any(|m| m == quoter_account_id);
        let quotable = matches!(target.visibility, Visibility::Public | Visibility::Home)
            && !target.local_only;
        if !(self_quote || mentioned || quotable) {
            let entitled = match target.visibility {
                Visibility::Public | Visibility::Home => true,
                Visibility::Followers => self
                    .follows
                    .is_following(quoter_account_id, &target.author_id)
                    .unwrap_or(false),
                Visibility::Specified => target
                    .visible_user_ids
                    .iter()
                    .any(|u| u == quoter_account_id),
            };
            if !entitled {
                return Ok(QuoteRequestOutcome::Ignored);
            }
            let jobs = match quoter_inbox_url.map(str::trim).filter(|s| !s.is_empty()) {
                Some(inbox) => vec![self.federate_job(
                    &crate::federation::outbox::FederateDeliverPayload::QuoteResponse {
                        response_id: self.ids.generate(),
                        accept: false,
                        decider_account_id: target.author_id.clone(),
                        request_activity_uri: activity_id.to_string(),
                        stamp_uri: None,
                        quoter_actor_uri: quoter_actor_uri.to_string(),
                        inbox_url: inbox.to_string(),
                    },
                    "federate:quote-reject",
                )?],
                None => Vec::new(),
            };
            return match self.dedup.record_quote_request_reject(activity_id, jobs)? {
                true => Ok(QuoteRequestOutcome::Rejected),
                false => Ok(QuoteRequestOutcome::Duplicate),
            };
        }

        let stamp_row_id = self.ids.generate();
        let stamp_id = format!("{base}/users/{}/stamps/{stamp_row_id}", target.author_id);
        let stamp = QuoteStamp {
            id: stamp_id.clone(),
            author_id: target.author_id.clone(),
            interacting_object_uri: instrument_uri.to_string(),
            interaction_target_note_id: target.id.clone(),
            request_activity_uri: activity_id.to_string(),
            revoked: false,
            created_at_ms: self.now_ms(),
        };
        let accept_jobs = match quoter_inbox_url.map(str::trim).filter(|s| !s.is_empty()) {
            Some(inbox) => vec![self.federate_job(
                &crate::federation::outbox::FederateDeliverPayload::QuoteResponse {
                    response_id: self.ids.generate(),
                    accept: true,
                    decider_account_id: target.author_id.clone(),
                    request_activity_uri: activity_id.to_string(),
                    stamp_uri: Some(stamp_id),
                    quoter_actor_uri: quoter_actor_uri.to_string(),
                    inbox_url: inbox.to_string(),
                },
                "federate:quote-accept",
            )?],
            None => Vec::new(),
        };
        match self
            .dedup
            .record_quote_request_accept(activity_id, stamp, accept_jobs)?
        {
            Some(_) => Ok(QuoteRequestOutcome::Accepted),
            None => Ok(QuoteRequestOutcome::Duplicate),
        }
    }
}
