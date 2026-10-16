//! FEP-044f quote authorization workflow (application layer).
//!
//! Two sides:
//!
//! - **Quoter (us quoting them):** local creation enqueues `QuoteRequest`
//!   (see `commands/notes.rs`); inbound `Accept`/`Reject`/stamp-`Delete`
//!   moves the quote `Pending → Approved/Rejected` and `Approved → Revoked`.
//! - **Quoted (them quoting us):** inbound `QuoteRequest` is validated,
//!   decided by policy (auto-approve public/unlisted + FEP exceptions,
//!   reject everything tighter — no `manualApproval` is advertised because
//!   no review workflow exists), and answered with `Accept`+stamp or
//!   `Reject`.
//!
//! Every handler commits state + outbox jobs atomically through the dedup
//! record path, so replays never duplicate state, stamps, or deliveries.
//! Timeouts are never rejections: unverifiable stamps yield `Unknown`.

use super::super::AppServices;

use crate::application::ports::{NewJob, PortError};
use crate::domain::note::{Note, Visibility};
use crate::domain::quote::{QuoteAuthorizationState, QuoteStamp};

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
    /// Decide an inbound `QuoteRequest` for a local post.
    ///
    /// Inputs are pre-validated by the federation boundary (signature, actor
    /// authority, activity dedup key): `quoter_account_id` is the resolved
    /// remote quoter, `quoted_note_id` the local target, `instrument_uri`
    /// the quoter's quote-Note URI, `instrument_quote_target_uri` the target
    /// that Note claims to quote, and `instrument_author_actor_uri` the
    /// actor that authored the instrument. `quoter_inbox_url` is `None`
    /// when the quoter advertises no inbox: the decision is still recorded,
    /// just with no delivery job (never a poison job to an empty URL).
    #[allow(clippy::too_many_arguments)]
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
                Visibility::Specified => target.visible_user_ids.iter().any(|u| u == quoter_account_id),
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
            return match self
                .dedup
                .record_quote_request_reject(activity_id, jobs)?
            {
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

    /// Apply an inbound `Accept(QuoteRequest)` to our pending quote.
    ///
    /// `quoted_author_actor_uri` is the local target author's actor URI
    /// (authority for the stamp); the stamp document is dereferenced and
    /// verified here, before the storage transaction. An `Accept` without a
    /// valid `result` stamp never approves. On transient fetch failure the
    /// returned re-verify job lets the caller schedule a rate-limited retry
    /// (the quote stays `Pending`; nothing is recorded, so retries and
    /// re-deliveries converge) — a timeout is never a rejection.
    pub fn handle_inbound_quote_decision_accept(
        &self,
        activity_id: &str,
        request_activity_uri: &str,
        stamp_uri: &str,
    ) -> Result<QuoteDecisionOutcome, PortError> {
        let Some(note) = self.notes.find_note_by_quote_request(request_activity_uri)? else {
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
        let quote_note_uri = note
            .uri
            .clone()
            .unwrap_or_else(|| format!("{}/notes/{}", self.public_url.trim_end_matches('/'), note.id));
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
        let Some(note) = self.notes.find_note_by_quote_request(request_activity_uri)? else {
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
        if note.quote.as_ref().map(|q| q.authorization)
            == Some(QuoteAuthorizationState::Revoked)
        {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::commands::tests::test_services;
    use crate::application::ports::{
        ActivityDedup, CreateNoteInput, JobKind, JobQueue, NoteStore,
    };
    use crate::federation::delivery::upsert_remote_actor_from_doc;

    const BOB_URI: &str = "https://remote.example/users/bob";
    const BOB_INBOX: &str = "https://remote.example/users/bob/inbox";

    fn remote_bob(services: &AppServices) -> crate::domain::account::Account {
        let doc = serde_json::json!({
            "id": BOB_URI,
            "type": "Person",
            "preferredUsername": "bob",
            "inbox": BOB_INBOX,
            "publicKey": {
                "id": format!("{BOB_URI}#main-key"),
                "owner": BOB_URI,
                "publicKeyPem": "x",
            },
        });
        // The key PEM is irrelevant here (no signatures verified in these
        // tests); upsert only needs the actor shape. A bogus PEM fails key
        // parsing, so borrow a real generated one.
        let (priv_k, pub_k) = crate::activitypub::signatures::generate_rsa_keypair_2048().unwrap();
        let _ = priv_k;
        let pem = crate::activitypub::signatures::public_key_to_spki_pem(&pub_k);
        let mut doc = doc;
        doc["publicKey"]["publicKeyPem"] = serde_json::Value::String(pem);
        upsert_remote_actor_from_doc(services, BOB_URI, &doc)
            .expect("upsert remote bob")
            .0
    }

    fn public_note(services: &AppServices, author: &str, text: &str) -> Note {
        services
            .create_note(CreateNoteInput::simple_public(author, text))
            .expect("create note")
    }

    fn quote_input(author: &str, target: &Note, text: &str) -> CreateNoteInput {
        let mut input = CreateNoteInput::simple_public(author, text);
        input.renote_of = Some(target.id.clone());
        input
    }

    fn federate_payloads(services: &AppServices) -> Vec<crate::federation::outbox::FederateDeliverPayload> {
        let mut out = Vec::new();
        while let Some(job) = services.jobs.claim_next(60_000).expect("claim") {
            if job.kind == JobKind::FederateDeliver {
                if let Ok(p) = serde_json::from_slice::<
                    crate::federation::outbox::FederateDeliverPayload,
                >(&job.payload)
                {
                    out.push(p);
                }
            }
            services.jobs.complete(&job.id).expect("complete");
        }
        out
    }

    #[test]
    fn local_self_quote_is_exception_without_request() {
        let services = test_services("quote-self");
        let alice = services.create_account("alice", None).unwrap();
        let target = public_note(&services, &alice.id, "original");
        let quote = services
            .create_note(quote_input(&alice.id, &target, "my own, quoted"))
            .unwrap();
        let q = quote.quote.as_ref().expect("quote record");
        assert_eq!(q.authorization, QuoteAuthorizationState::ApprovedByException);
        assert_eq!(q.target_note_id.as_deref(), Some(target.id.as_str()));
        assert!(q.request_activity_uri.is_none(), "no handshake needed with self");
        let payloads = federate_payloads(&services);
        assert!(
            !payloads.iter().any(|p| matches!(
                p,
                crate::federation::outbox::FederateDeliverPayload::QuoteRequest { .. }
            )),
            "self-quotes send no QuoteRequest"
        );
    }

    #[test]
    fn local_quote_by_mentioned_author_is_exception() {
        let services = test_services("quote-mention");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = public_note(&services, &bob.id, "hello @alice, thoughts?");
        assert!(target.mentions.contains(&alice.id), "mention extracted");
        let quote = services
            .create_note(quote_input(&alice.id, &target, "quoting bob"))
            .unwrap();
        assert_eq!(
            quote.quote.as_ref().unwrap().authorization,
            QuoteAuthorizationState::ApprovedByException
        );
    }

    #[test]
    fn local_quote_of_local_public_is_server_approved() {
        let services = test_services("quote-local");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = public_note(&services, &bob.id, "public post");
        let quote = services
            .create_note(quote_input(&alice.id, &target, "quoting bob"))
            .unwrap();
        // Same-server policy decision (documented ApprovedByException
        // mapping: no cross-server consent at stake, no stamp exists).
        assert_eq!(
            quote.quote.as_ref().unwrap().authorization,
            QuoteAuthorizationState::ApprovedByException
        );
    }

    #[test]
    fn local_quote_of_local_only_target_stays_pending() {
        let services = test_services("quote-local-only");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let mut t = CreateNoteInput::simple_public(&bob.id, "local only post");
        t.local_only = true;
        let target = services.create_note(t).unwrap();
        let quote = services
            .create_note(quote_input(&alice.id, &target, "quoting"))
            .unwrap();
        assert_eq!(
            quote.quote.as_ref().unwrap().authorization,
            QuoteAuthorizationState::Pending,
            "local-only targets never auto-approve"
        );
    }

    #[test]
    fn quote_of_deleted_target_is_rejected() {
        let services = test_services("quote-deleted");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = public_note(&services, &bob.id, "doomed");
        services.delete_note(&bob.id, &target.id).unwrap();
        let mut input = quote_input(&alice.id, &target, "quoting deleted");
        let err = services.create_note(input).expect_err("deleted target must fail");
        assert!(
            format!("{err:?}").contains("NO_SUCH_RENOTE_TARGET"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn local_pure_renote_sends_no_quote_request() {
        let services = test_services("quote-pure");
        let alice = services.create_account("alice", None).unwrap();
        let bob = remote_bob(&services);
        // Cache bob's post as a remote note (inbound path, no ownership).
        let mut input = CreateNoteInput::simple_public(&bob.id, "remote post");
        input.uri = Some("https://remote.example/notes/1".to_string());
        let target = services
            .handle_inbound_create_note("https://remote.example/activities/c1", input, vec![])
            .unwrap()
            .expect("cached");
        // Pure renote: no text/CW/files and no quote input.
        let mut renote = CreateNoteInput::simple_public(&alice.id, "x");
        renote.text = None;
        renote.renote_of = Some(target.id.clone());
        let note = services.create_note(renote).unwrap();
        assert!(!note.is_quote(), "pure renote is not a quote");
        assert!(federate_payloads(&services).iter().all(|p| !matches!(
            p,
            crate::federation::outbox::FederateDeliverPayload::QuoteRequest { .. }
        )));
    }

    #[test]
    fn local_quote_of_remote_post_is_pending_with_request_job() {
        let services = test_services("quote-remote");
        let alice = services.create_account("alice", None).unwrap();
        let bob = remote_bob(&services);
        let mut input = CreateNoteInput::simple_public(&bob.id, "remote post");
        input.uri = Some("https://remote.example/notes/1".to_string());
        let target = services
            .handle_inbound_create_note("https://remote.example/activities/c1", input, vec![])
            .unwrap()
            .expect("cached");
        // Drain setup jobs so only the quote's jobs remain.
        federate_payloads(&services);

        let quote = services
            .create_note(quote_input(&alice.id, &target, "quoting remote bob"))
            .unwrap();
        let q = quote.quote.as_ref().expect("quote record");
        assert_eq!(q.authorization, QuoteAuthorizationState::Pending);
        let request_uri = q.request_activity_uri.clone().expect("request URI stored");
        assert!(request_uri.starts_with("https://example.local/activities/"));

        let payloads = federate_payloads(&services);
        let req = payloads.iter().find_map(|p| match p {
            crate::federation::outbox::FederateDeliverPayload::QuoteRequest {
                request_id,
                quote_note_id,
                quoted_uri,
                target_actor_uri,
                inbox_url,
                ..
            } => Some((request_id.clone(), quote_note_id.clone(), quoted_uri.clone(), target_actor_uri.clone(), inbox_url.clone())),
            _ => None,
        }).expect("QuoteRequest job enqueued in the note's transaction");
        assert_eq!(req.3, BOB_URI);
        assert_eq!(req.4, BOB_INBOX);
        assert_eq!(req.2, "https://remote.example/notes/1");
        assert_eq!(req.1, quote.id, "__PENDING_NOTE_ID__ substituted");
        assert!(request_uri.ends_with(req.0.as_str()), "stored request URI matches job");
    }

    fn request_fixture(services: &AppServices) -> (Note, crate::domain::account::Account, Note) {
        let bob = services.create_account("bob", None).unwrap();
        let target = public_note(&services, &bob.id, "quotable post");
        let alice_doc = serde_json::json!({
            "id": "https://remote.example/users/alice",
            "type": "Person",
            "preferredUsername": "alice",
            "inbox": "https://remote.example/users/alice/inbox",
            "publicKey": {
                "id": "https://remote.example/users/alice#main-key",
                "owner": "https://remote.example/users/alice",
                "publicKeyPem": "x",
            },
        });
        let (priv_k, pub_k) = crate::activitypub::signatures::generate_rsa_keypair_2048().unwrap();
        let _ = priv_k;
        let mut doc = alice_doc;
        doc["publicKey"]["publicKeyPem"] =
            serde_json::Value::String(crate::activitypub::signatures::public_key_to_spki_pem(&pub_k));
        let alice = upsert_remote_actor_from_doc(services, "https://remote.example/users/alice", &doc)
            .expect("upsert quoter")
            .0;
        // Cache alice's quoting note (remote).
        let mut input = CreateNoteInput::simple_public(&alice.id, "quoting bob");
        input.uri = Some("https://remote.example/notes/q1".to_string());
        let quoting = services
            .handle_inbound_create_note("https://remote.example/activities/cq", input, vec![])
            .unwrap()
            .expect("cached quoting note");
        (target, alice, quoting)
    }

    #[test]
    fn inbound_request_public_auto_accepts_with_stamp() {
        let services = test_services("quote-req-accept");
        let (target, alice, _quoting) = request_fixture(&services);
        let inbox = services.remote_inbox_for_account(&alice).expect("quoter inbox");
        let outcome = services
            .handle_inbound_quote_request(
                "https://remote.example/activities/req-1",
                &alice.id,
                "https://remote.example/users/alice",
                Some(&inbox),
                &target.id,
                "https://remote.example/notes/q1",
                &target.uri.clone().unwrap_or_else(|| format!("https://example.local/notes/{}", target.id)),
                "https://remote.example/users/alice",
            )
            .unwrap();
        assert_eq!(outcome, QuoteRequestOutcome::Accepted);
        // Stamp persisted + Accept delivery enqueued with the stamp result.
        let stamps: Vec<_> = {
            let mut found = Vec::new();
            while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
                if job.kind == JobKind::FederateDeliver {
                    if let Ok(crate::federation::outbox::FederateDeliverPayload::QuoteResponse {
                        accept, stamp_uri, request_activity_uri, ..
                    }) = serde_json::from_slice::<crate::federation::outbox::FederateDeliverPayload>(&job.payload)
                    {
                        assert!(accept);
                        assert_eq!(request_activity_uri, "https://remote.example/activities/req-1");
                        found.push(stamp_uri.expect("accept carries a stamp"));
                    }
                }
                services.jobs.complete(&job.id).unwrap();
            }
            found
        };
        assert_eq!(stamps.len(), 1);
        let stamp = services.notes.get_quote_stamp(&stamps[0]).unwrap().expect("stamp saved");
        assert!(!stamp.revoked);
        assert_eq!(stamp.interacting_object_uri, "https://remote.example/notes/q1");

        // Replay: duplicate, no new jobs.
        let before = services.jobs.pending_count().unwrap();
        let outcome = services
            .handle_inbound_quote_request(
                "https://remote.example/activities/req-1",
                &alice.id,
                "https://remote.example/users/alice",
                Some(&inbox),
                &target.id,
                "https://remote.example/notes/q1",
                &target.uri.clone().unwrap_or_else(|| format!("https://example.local/notes/{}", target.id)),
                "https://remote.example/users/alice",
            )
            .unwrap();
        assert_eq!(outcome, QuoteRequestOutcome::Duplicate);
        assert_eq!(services.jobs.pending_count().unwrap(), before);
    }

    #[test]
    fn inbound_request_private_without_entitlement_is_ignored_not_rejected() {
        let services = test_services("quote-req-reject");
        let bob = services.create_account("bob", None).unwrap();
        let alice = remote_bob(&services);
        // Followers-only post; remote alice is not a follower and not mentioned:
        // silent Ignore (no Reject oracle for private content).
        let mut input = CreateNoteInput::simple_public(&bob.id, "followers post");
        input.visibility = crate::domain::note::Visibility::Followers;
        let target = services.create_note(input).unwrap();
        let inbox = services.remote_inbox_for_account(&alice).expect("inbox");
        let outcome = services
            .handle_inbound_quote_request(
                "https://remote.example/activities/req-2",
                &alice.id,
                BOB_URI,
                Some(&inbox),
                &target.id,
                "https://remote.example/notes/q2",
                &target.uri.clone().unwrap_or_else(|| format!("https://example.local/notes/{}", target.id)),
                BOB_URI,
            )
            .unwrap();
        assert_eq!(outcome, QuoteRequestOutcome::Ignored);
        // No Reject enqueued; no stamp exists anywhere.
        let mut saw_reject = false;
        while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
            if let Ok(crate::federation::outbox::FederateDeliverPayload::QuoteResponse { accept, .. }) =
                serde_json::from_slice::<crate::federation::outbox::FederateDeliverPayload>(&job.payload)
            {
                assert!(!accept);
                saw_reject = true;
            }
            services.jobs.complete(&job.id).unwrap();
        }
        assert!(!saw_reject, "no Reject for unentitled quoter");
    }

    #[test]
    fn inbound_request_follower_gets_reject_without_stamp() {
        let services = test_services("quote-req-reject-entitled");
        let bob = services.create_account("bob", None).unwrap();
        let alice = remote_bob(&services);
        let mut input = CreateNoteInput::simple_public(&bob.id, "followers post");
        input.visibility = crate::domain::note::Visibility::Followers;
        let target = services.create_note(input).unwrap();
        // Entitled follower: Reject is safe (they can already see the post).
        services
            .follows
            .follow(&alice.id, &bob.id, false, None, Vec::new())
            .expect("follow edge");
        let inbox = services.remote_inbox_for_account(&alice).expect("inbox");
        let outcome = services
            .handle_inbound_quote_request(
                "https://remote.example/activities/req-2b",
                &alice.id,
                BOB_URI,
                Some(&inbox),
                &target.id,
                "https://remote.example/notes/q2",
                &target.uri.clone().unwrap_or_else(|| format!("https://example.local/notes/{}", target.id)),
                BOB_URI,
            )
            .unwrap();
        assert_eq!(outcome, QuoteRequestOutcome::Rejected);
    }

    #[test]
    fn inbound_request_instrument_mismatch_ignored() {
        let services = test_services("quote-req-mismatch");
        let (target, alice, _quoting) = request_fixture(&services);
        let outcome = services
            .handle_inbound_quote_request(
                "https://remote.example/activities/req-3",
                &alice.id,
                "https://remote.example/users/alice",
                None,
                &target.id,
                "https://remote.example/notes/q1",
                "https://remote.example/notes/OTHER",
                "https://remote.example/users/alice",
            )
            .unwrap();
        assert_eq!(outcome, QuoteRequestOutcome::Ignored);
    }

    #[test]
    fn decision_reject_retains_relation_and_replay_is_duplicate() {
        let services = test_services("quote-dec-reject");
        let alice = services.create_account("alice", None).unwrap();
        let bob = remote_bob(&services);
        let mut input = CreateNoteInput::simple_public(&bob.id, "remote post");
        input.uri = Some("https://remote.example/notes/1".to_string());
        let target = services
            .handle_inbound_create_note("https://remote.example/activities/c1", input, vec![])
            .unwrap()
            .expect("cached");
        let quote = services.create_note(quote_input(&alice.id, &target, "quoting")).unwrap();
        let request_uri = quote.quote.as_ref().unwrap().request_activity_uri.clone().unwrap();

        let outcome = services
            .handle_inbound_quote_decision_reject("https://remote.example/activities/rej-1", &request_uri)
            .unwrap();
        match outcome {
            QuoteDecisionOutcome::Applied(n) => {
                assert_eq!(n.quote.as_ref().unwrap().authorization, QuoteAuthorizationState::Rejected);
                assert_eq!(n.renote_id.as_deref(), Some(target.id.as_str()));
                assert!(n.text.is_some(), "rejected quote keeps commentary");
            }
            other => panic!("expected Applied, got {other:?}"),
        }
        assert_eq!(
            services
                .handle_inbound_quote_decision_reject("https://remote.example/activities/rej-1", &request_uri)
                .unwrap(),
            QuoteDecisionOutcome::Duplicate
        );
    }

    #[test]
    fn stamp_delete_revokes_forwards_and_converges() {
        let services = test_services("quote-revoke");
        let alice = services.create_account("alice", None).unwrap();
        let bob = remote_bob(&services);
        let mut input = CreateNoteInput::simple_public(&bob.id, "remote post");
        input.uri = Some("https://remote.example/notes/1".to_string());
        let target = services
            .handle_inbound_create_note("https://remote.example/activities/c1", input, vec![])
            .unwrap()
            .expect("cached");
        let quote = services.create_note(quote_input(&alice.id, &target, "quoting")).unwrap();
        let stamp_uri = "https://remote.example/stamps/9".to_string();
        services
            .notes
            .set_quote_authorization(&quote.id, QuoteAuthorizationState::Approved, Some(stamp_uri.clone()), 0, vec![])
            .unwrap();
        federate_payloads(&services);

        let outcome = services
            .handle_inbound_quote_stamp_delete("https://remote.example/activities/del-1", &stamp_uri)
            .unwrap();
        match outcome {
            QuoteDecisionOutcome::Applied(n) => {
                assert_eq!(n.quote.as_ref().unwrap().authorization, QuoteAuthorizationState::Revoked);
            }
            other => panic!("expected Applied, got {other:?}"),
        }
        assert!(federate_payloads(&services).iter().any(|p| matches!(
            p,
            crate::federation::outbox::FederateDeliverPayload::QuoteStampDeleted { .. }
        )), "revocation forwarded to the quote's audience");
        // Already revoked: converge without re-forwarding (no Delete loop).
        assert_eq!(
            services
                .handle_inbound_quote_stamp_delete("https://remote.example/activities/del-2", &stamp_uri)
                .unwrap(),
            QuoteDecisionOutcome::Duplicate
        );
        assert!(federate_payloads(&services).is_empty());
    }

    #[test]
    fn accept_with_malformed_stamp_uri_is_ignored_never_approved() {
        let services = test_services("quote-bad-stamp");
        let alice = services.create_account("alice", None).unwrap();
        let bob = remote_bob(&services);
        let mut input = CreateNoteInput::simple_public(&bob.id, "remote post");
        input.uri = Some("https://remote.example/notes/1".to_string());
        let target = services
            .handle_inbound_create_note("https://remote.example/activities/c1", input, vec![])
            .unwrap()
            .expect("cached");
        let quote = services.create_note(quote_input(&alice.id, &target, "quoting")).unwrap();
        let request_uri = quote.quote.as_ref().unwrap().request_activity_uri.clone().unwrap();
        let outcome = services
            .handle_inbound_quote_decision_accept("https://remote.example/activities/acc-1", &request_uri, "not-a-uri")
            .unwrap();
        assert_eq!(outcome, QuoteDecisionOutcome::Ignored);
        let reread = services.notes.get_note(&quote.id).unwrap().unwrap();
        assert_eq!(reread.quote.as_ref().unwrap().authorization, QuoteAuthorizationState::Pending);
    }
}
