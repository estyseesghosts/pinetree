//! Poll voting (`docs/misskey-parity-plan.md` §4; Misskey
//! `notes/polls/vote.ts` + `PollService.vote` + `ApQuestionService`
//! parities). Misskey API + ActivityPub adapters call these same ops.

use super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::{PollChoiceCount, PollExpiryPayload, PortError};
use crate::domain::policy::{ViewerContext, is_visible};
use crate::domain::poll::{Poll, PollInput, PollVote};

impl AppServices {
    /// Vote on a poll (Misskey `notes/polls/vote.ts` full error set).
    /// Order: note existence → visibility-as-`NO_SUCH_NOTE` → `NO_POLL` →
    /// `YOU_HAVE_BEEN_BLOCKED` (via the §0A `check_poll_vote_block` hook)
    /// → `ALREADY_EXPIRED` → `INVALID_CHOICE` → `ALREADY_VOTED`. Commits
    /// the vote, the counter bump, the `pollVoted` streaming event, and —
    /// for federated notes — the `deliverQuestionUpdate` federation job in
    /// one storage transaction; a vote on a remote note also enqueues the
    /// vote activity (`Create` vote `Note`) to the poll owner's inbox.
    pub fn vote_on_poll(
        &self,
        voter_id: &str,
        note_id: &str,
        choice: usize,
    ) -> Result<PollVote, PortError> {
        self.accounts.get_owner(voter_id)?;
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        if note.is_deleted {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        // Visibility first (invisible notes are indistinguishable from
        // missing ones), then the §0A block hook.
        let follows_author = if voter_id == note.author_id {
            true
        } else {
            self.follows.is_following(voter_id, &note.author_id)?
        };
        let viewer = ViewerContext::local_user(voter_id, follows_author);
        if !is_visible(&note, &viewer) {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        // `check_poll_vote_block` re-loads the note and enforces
        // `YOU_HAVE_BEEN_BLOCKED` (self-votes exempt).
        self.check_poll_vote_block(voter_id, note_id)?;
        let poll = match self.polls.get_poll(note_id)? {
            None => return Err(PortError::Validation("NO_POLL".to_string())),
            Some(poll) => poll,
        };
        let now = self.now_ms();
        if poll.is_expired(now) {
            return Err(PortError::Validation("ALREADY_EXPIRED".to_string()));
        }
        if choice >= poll.choices.len() {
            return Err(PortError::Validation("INVALID_CHOICE".to_string()));
        }
        // Duplicate detection mirrors the storage gate so the error
        // precedence is identical on the local path (storage re-checks
        // atomically; the race converges to the same `ALREADY_VOTED`).
        let existing = self.polls.votes_by_user_for_note(voter_id, note_id)?;
        if poll.multiple {
            if existing.iter().any(|v| v.choice == choice) {
                return Err(PortError::Conflict("ALREADY_VOTED".to_string()));
            }
        } else if !existing.is_empty() {
            return Err(PortError::Conflict("ALREADY_VOTED".to_string()));
        }

        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::PollVoted {
                note_id: note_id.to_string(),
                user_id: voter_id.to_string(),
                choice,
            },
            format!("poll-vote:{voter_id}:{note_id}:{choice}:{}", self.ids.generate()),
        )?];
        // Federated notes fan fresh tallies out as `Update(Question)`
        // (Misskey `deliverQuestionUpdate`); local-only notes never
        // federate. (Execution no-ops for remote-authored notes, whose
        // tallies refresh through `Question` sync instead.)
        if !note.local_only {
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::QuestionUpdated {
                    note_id: note_id.to_string(),
                    author_id: note.author_id.clone(),
                    inbox_override: None,
                }
                .into_job(
                    format!("federate:question:{note_id}:{}", self.ids.generate()),
                    now,
                )?,
            );
        }
        // A vote on a remote note is delivered to the poll owner as a
        // `Create` vote activity (Misskey `renderVote` parity).
        if let Some(author) = self.accounts.find_by_id(&note.author_id)? {
            if !author.is_local() {
                if let Some(inbox_url) =
                    crate::federation::outbox::preferred_inbox(&author)
                {
                    let base = self.public_url.trim_end_matches('/');
                    let vote_row_id = self.ids.generate();
                    let target_uri = note
                        .uri
                        .clone()
                        .unwrap_or_else(|| format!("{base}/notes/{note_id}"));
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::PollVote {
                            vote_id: vote_row_id,
                            voter_id: voter_id.to_string(),
                            target_note_uri: target_uri,
                            choice_name: poll.choices[choice].clone(),
                            inbox_url,
                        }
                        .into_job(
                            format!("federate:vote:{voter_id}:{note_id}:{choice}"),
                            now,
                        )?,
                    );
                }
            }
        }
        let vote = PollVote {
            id: self.ids.generate(),
            note_id: note_id.to_string(),
            user_id: voter_id.to_string(),
            choice,
        };
        self.polls.cast_vote(vote, jobs)
    }

    /// Run one `PollExpiry` job: publish the `pollEnded` streaming event
    /// when the poll has actually expired (Misskey
    /// `EndedPollNotificationProcessorService` parity, minus notifications —
    /// pinetree has no `pollEnded` notification kind yet). Idempotent: a
    /// missing note/poll, or a poll whose expiry moved into the future,
    /// completes quietly.
    pub fn handle_poll_expiry(&self, note_id: &str) -> Result<bool, PortError> {
        let Some(note) = self.notes.get_note(note_id)? else {
            return Ok(false);
        };
        if note.is_deleted {
            return Ok(false);
        }
        let Some(poll) = self.polls.get_poll(note_id)? else {
            return Ok(false);
        };
        if !poll.is_expired(self.now_ms()) {
            return Ok(false);
        }
        self.streaming.publish(
            "streaming",
            &serde_json::to_vec(&StreamingEvent::PollEnded {
                note_id: note_id.to_string(),
            })
            .map_err(|e| {
                PortError::Validation(format!("poll-ended payload invalid: {e}"))
            })?,
        )?;
        Ok(true)
    }

    /// Record a remote vote delivered as `Create(Note)` with `inReplyTo`
    /// pointing at a local poll note and `name` matching a choice
    /// (Misskey `ApNoteService` vote branch parity). Expired polls are
    /// skipped quietly (warn-level upstream); unknown choices are skipped;
    /// duplicates converge to `Ok(false)`. Returns `Ok(true)` when a vote
    /// was recorded (the tally-sync `QuestionUpdated` job commits in the
    /// same transaction).
    pub fn handle_inbound_vote(
        &self,
        activity_id: &str,
        voter_id: &str,
        note_id: &str,
        choice_name: &str,
    ) -> Result<bool, PortError> {
        let Some(note) = self.notes.get_note(note_id)? else {
            return Ok(vote_dropped("no-note", activity_id, note_id));
        };
        if note.is_deleted {
            return Ok(vote_dropped("note-deleted", activity_id, note_id));
        }
        let Some(poll) = self.polls.get_poll(note_id)? else {
            return Ok(vote_dropped("no-poll", activity_id, note_id));
        };
        if poll.is_expired(self.now_ms()) {
            return Ok(vote_dropped("poll-expired", activity_id, note_id));
        }
        let Some(choice) = poll.choices.iter().position(|c| c == choice_name) else {
            return Ok(vote_dropped("unknown-choice", activity_id, note_id));
        };
        // Blocked voters cannot vote (same gate as the local path).
        if note.author_id != voter_id && self.blocks.is_blocking(&note.author_id, voter_id)? {
            return Ok(vote_dropped("voter-blocked", activity_id, note_id));
        }
        let existing = self.polls.votes_by_user_for_note(voter_id, note_id)?;
        if poll.multiple {
            if existing.iter().any(|v| v.choice == choice) {
                return Ok(vote_dropped("already-voted", activity_id, note_id));
            }
        } else if !existing.is_empty() {
            return Ok(vote_dropped("already-voted", activity_id, note_id));
        }
        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::PollVoted {
                note_id: note_id.to_string(),
                user_id: voter_id.to_string(),
                choice,
            },
            format!("inbound-vote:{activity_id}"),
        )?];
        // Tally sync for our followers (Misskey `deliverQuestionUpdate`
        // after `pollService.vote`); skipped for local-only notes, which
        // never federate. Commits atomically with the vote.
        if !note.local_only {
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::QuestionUpdated {
                    note_id: note_id.to_string(),
                    author_id: note.author_id.clone(),
                    inbox_override: None,
                }
                .into_job(
                    format!("federate:question:{note_id}:{activity_id}"),
                    self.now_ms(),
                )?,
            );
        }
        match self.polls.cast_vote(
            PollVote {
                id: self.ids.generate(),
                note_id: note_id.to_string(),
                user_id: voter_id.to_string(),
                choice,
            },
            jobs,
        ) {
            Ok(_) => Ok(true),
            Err(PortError::Conflict(msg)) if msg == "ALREADY_VOTED" => {
                Ok(vote_dropped("already-voted-race", activity_id, note_id))
            }
            Err(PortError::Validation(msg))
                if msg == "ALREADY_EXPIRED" || msg == "INVALID_CHOICE" =>
            {
                Ok(vote_dropped("rejected-by-storage", activity_id, note_id))
            }
            Err(e) => Err(e),
        }
    }

    /// Apply an inbound `Update(Question)` to a cached remote poll
    /// (Misskey `ApQuestionService.updateQuestion` parity): the question
    /// must be attributed to the poll owner (actor match enforced by the
    /// caller), counts update by choice-name match, unknown names are
    /// ignored. Returns the updated poll, or `Ok(None)` when there is
    /// nothing to apply to (unknown note, local note, no poll).
    pub fn handle_inbound_question_update(
        &self,
        activity_id: &str,
        question_id: &str,
        attributed_to_uri: Option<&str>,
        counts: Vec<PollChoiceCount>,
    ) -> Result<Option<Poll>, PortError> {
        let _ = activity_id;
        // Only cached remote notes can be refreshed from federation input;
        // our own polls change through votes, never through `Update`.
        let Some(note) = self.notes.find_by_uri(question_id)? else {
            return Ok(None);
        };
        if note.is_deleted {
            return Ok(None);
        }
        let author = match self.accounts.find_by_id(&note.author_id)? {
            Some(author) => author,
            None => return Ok(None),
        };
        if author.is_local() {
            return Ok(None);
        }
        if self.polls.get_poll(&note.id)?.is_none() {
            return Ok(None);
        }
        // Authority: the question must be attributed to the poll owner
        // (Misskey `attribution === user.uri` parity).
        let base = self.public_url.trim_end_matches('/');
        let owner_uri = author.uri.clone().unwrap_or_else(|| author.actor_url(base));
        match attributed_to_uri {
            Some(attribution) if attribution == owner_uri => {}
            // Unattributed questions inherit the cached owner (same as
            // Misskey's `getOneApId(attributedTo) ?? user.uri` fallback).
            None => {}
            _ => return Ok(None),
        }
        match self.polls.sync_poll_votes(&note.id, &counts, Vec::new()) {
            Ok(poll) => Ok(Some(poll)),
            Err(PortError::NotFound(msg)) if msg == "NO_POLL" => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Build the delayed `PollExpiry` job for a freshly created poll
    /// (`None` when the poll never expires). `not_before` is armed at the
    /// poll's `expires_at` (Misskey `endedPollNotificationQueue` delay
    /// parity). `note_id` may be `"__PENDING_NOTE_ID__"` at enqueue time
    /// (substituted with the committed id inside the storage transaction).
    pub fn poll_expiry_job(
        &self,
        note_id: &str,
        poll: &PollInput,
    ) -> Result<Option<crate::application::ports::NewJob>, PortError> {
        let Some(expires_at) = poll.expires_at_ms else {
            return Ok(None);
        };
        Ok(Some(
            crate::application::ports::NewJob::new_json(
                crate::application::ports::JobKind::PollExpiry,
                &PollExpiryPayload {
                    note_id: note_id.to_string(),
                },
                format!("poll-expiry:{note_id}:{}", self.ids.generate()),
                expires_at,
            )?,
        ))
    }
}

/// An inbound vote that changes nothing is acknowledged (`202`) but never
/// silent: one stderr line per drop with its reason, so a tally that stays
/// at zero can be told apart from a vote that was never processed.
pub(crate) fn vote_dropped(reason: &str, activity_id: &str, note_id: &str) -> bool {
    eprintln!("[inbound-vote] dropped reason={reason} activity={activity_id} note={note_id}");
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::application::ports::{
        ActivityDedup, BlobStore, Clock, IdGenerator, NoopStreamingSink,
    };
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn poll_services(prefix: &str) -> (AppServices, Arc<ManualClock>) {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-poll-test-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<ManualClock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store =
            RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        let services = AppServices {
            clock: clock.clone() as Arc<dyn Clock>,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://example.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        };
        (services, clock)
    }

    fn poll_note(
        services: &AppServices,
        author: &str,
        choices: &[&str],
        multiple: bool,
        expires_at_ms: Option<i64>,
    ) -> crate::domain::note::Note {
        let mut input =
            crate::application::ports::CreateNoteInput::simple_public(author, "pick one");
        input.poll = Some(PollInput {
            choices: choices.iter().map(|s| s.to_string()).collect(),
            multiple,
            expires_at_ms,
        });
        input.has_poll = true;
        services.create_note(input).unwrap()
    }

    fn port_code(err: PortError) -> String {
        match err {
            PortError::Validation(m) | PortError::Conflict(m) | PortError::NotFound(m) => m,
            PortError::StorageUnavailable(m) => panic!("storage fault: {m}"),
        }
    }

    #[test]
    fn poll_create_validation_rejects_bad_shapes() {
        let (services, clock) = poll_services("create-validation");
        let alice = services.create_account("alice", None).unwrap();
        let now = clock.now_ms();

        // Too few / too many choices.
        for choices in [vec!["only"], vec!["c"; 11]] {
            let mut input =
                crate::application::ports::CreateNoteInput::simple_public(&alice.id, "t");
            input.poll = Some(PollInput {
                choices: choices.iter().map(|s| s.to_string()).collect(),
                multiple: false,
                expires_at_ms: None,
            });
            input.has_poll = true;
            assert!(services.create_note(input).is_err());
        }
        // Duplicate choices.
        let mut input =
            crate::application::ports::CreateNoteInput::simple_public(&alice.id, "t");
        input.poll = Some(PollInput {
            choices: vec!["a".to_string(), "a".to_string()],
            multiple: false,
            expires_at_ms: None,
        });
        input.has_poll = true;
        let err = services.create_note(input).unwrap_err();
        assert!(port_code(err).contains("unique"));

        // Already-expired poll carries the exact Misskey ident.
        let mut input =
            crate::application::ports::CreateNoteInput::simple_public(&alice.id, "t");
        input.poll = Some(PollInput {
            choices: vec!["a".to_string(), "b".to_string()],
            multiple: false,
            expires_at_ms: Some(now - 1),
        });
        input.has_poll = true;
        let err = services.create_note(input).unwrap_err();
        assert_eq!(port_code(err), "CANNOT_CREATE_ALREADY_EXPIRED_POLL");

        // A live poll commits with zeroed tallies + an armed expiry job
        // (due at `expires_at`, so the clock must pass it to claim).
        let note = poll_note(&services, &alice.id, &["a", "b"], false, Some(now + 60_000));
        let poll = services.polls.get_poll(&note.id).unwrap().unwrap();
        assert_eq!(poll.votes, vec![0, 0]);
        clock.advance_ms(61_000);
        let mut expiry_job = None;
        while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
            if job.kind == crate::application::ports::JobKind::PollExpiry {
                expiry_job = Some(job);
                break;
            }
        }
        let job = expiry_job.expect("a PollExpiry job must be armed");
        assert_eq!(job.not_before, now + 60_000);
        let payload: PollExpiryPayload = serde_json::from_slice(&job.payload).unwrap();
        assert_eq!(payload.note_id, note.id);
    }

    #[test]
    fn vote_flows_single_and_multiple() {
        let (services, _) = poll_services("vote-flows");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();

        // Single-choice: vote, then duplicate → ALREADY_VOTED.
        let note = poll_note(&services, &alice.id, &["a", "b"], false, None);
        let vote = services.vote_on_poll(&bob.id, &note.id, 0).unwrap();
        assert_eq!(vote.choice, 0);
        let poll = services.polls.get_poll(&note.id).unwrap().unwrap();
        assert_eq!(poll.votes, vec![1, 0]);
        let err = services.vote_on_poll(&bob.id, &note.id, 1).unwrap_err();
        assert_eq!(port_code(err), "ALREADY_VOTED");

        // Multi-choice: distinct choices accumulate, same choice conflicts.
        let multi = poll_note(&services, &alice.id, &["x", "y", "z"], true, None);
        services.vote_on_poll(&bob.id, &multi.id, 0).unwrap();
        services.vote_on_poll(&bob.id, &multi.id, 2).unwrap();
        let poll = services.polls.get_poll(&multi.id).unwrap().unwrap();
        assert_eq!(poll.votes, vec![1, 0, 1]);
        let err = services.vote_on_poll(&bob.id, &multi.id, 0).unwrap_err();
        assert_eq!(port_code(err), "ALREADY_VOTED");
    }

    #[test]
    fn vote_rejects_invalid_choice_expired_blocked_and_missing() {
        let (services, clock) = poll_services("vote-gates");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let now = clock.now_ms();

        // NO_POLL on a plain note.
        let plain =
            crate::application::ports::CreateNoteInput::simple_public(&alice.id, "plain");
        let plain = services.create_note(plain).unwrap();
        let err = services.vote_on_poll(&bob.id, &plain.id, 0).unwrap_err();
        assert_eq!(port_code(err), "NO_POLL");

        // NO_SUCH_NOTE on unknown ids.
        let err = services.vote_on_poll(&bob.id, "nope", 0).unwrap_err();
        assert_eq!(port_code(err), "NO_SUCH_NOTE");

        // INVALID_CHOICE out of range.
        let note = poll_note(&services, &alice.id, &["a", "b"], false, None);
        let err = services.vote_on_poll(&bob.id, &note.id, 7).unwrap_err();
        assert_eq!(port_code(err), "INVALID_CHOICE");

        // ALREADY_EXPIRED after the clock passes (vote first, then expire,
        // then a second voter is rejected and tallies stand).
        let expiring = poll_note(&services, &alice.id, &["a", "b"], false, Some(now + 60_000));
        services.vote_on_poll(&bob.id, &expiring.id, 0).unwrap();
        clock.advance_ms(61_000);
        let carol = services.create_account("carol", None).unwrap();
        let err = services
            .vote_on_poll(&carol.id, &expiring.id, 1)
            .unwrap_err();
        assert_eq!(port_code(err), "ALREADY_EXPIRED");
        assert_eq!(
            services.polls.get_poll(&expiring.id).unwrap().unwrap().votes,
            vec![1, 0]
        );

        // YOU_HAVE_BEEN_BLOCKED via the §0A hook.
        let gated = poll_note(&services, &alice.id, &["a", "b"], false, None);
        services.block(&alice.id, &bob.id).unwrap();
        let err = services.vote_on_poll(&bob.id, &gated.id, 0).unwrap_err();
        assert_eq!(port_code(err), "YOU_HAVE_BEEN_BLOCKED");
        // The author herself can still vote her own poll.
        services.vote_on_poll(&alice.id, &gated.id, 1).unwrap();
    }

    #[test]
    fn poll_pack_shape_has_isvoted_per_requester() {
        let (services, _) = poll_services("pack");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let note = poll_note(&services, &alice.id, &["a", "b"], false, None);
        services.vote_on_poll(&bob.id, &note.id, 1).unwrap();
        let stored = services.notes.get_note(&note.id).unwrap().unwrap();

        let packed =
            crate::api::misskey::packers::pack_note(&stored, Some(&bob.id), &services, 0).unwrap();
        assert_eq!(packed["hasPoll"], serde_json::json!(true));
        assert_eq!(packed["poll"]["multiple"], serde_json::json!(false));
        assert_eq!(packed["poll"]["choices"][0]["text"], serde_json::json!("a"));
        assert_eq!(packed["poll"]["choices"][0]["votes"], serde_json::json!(0));
        assert_eq!(
            packed["poll"]["choices"][0]["isVoted"],
            serde_json::json!(false)
        );
        assert_eq!(packed["poll"]["choices"][1]["votes"], serde_json::json!(1));
        assert_eq!(
            packed["poll"]["choices"][1]["isVoted"],
            serde_json::json!(true)
        );

        // A stranger sees tallies but no votes of their own.
        let carol = services.create_account("carol", None).unwrap();
        let packed =
            crate::api::misskey::packers::pack_note(&stored, Some(&carol.id), &services, 0)
                .unwrap();
        assert_eq!(
            packed["poll"]["choices"][1]["isVoted"],
            serde_json::json!(false)
        );

        // Plain notes pack `hasPoll: false` + `poll: null`.
        let plain =
            crate::application::ports::CreateNoteInput::simple_public(&alice.id, "plain");
        let plain = services.create_note(plain).unwrap();
        let packed =
            crate::api::misskey::packers::pack_note(&plain, Some(&bob.id), &services, 0).unwrap();
        assert_eq!(packed["hasPoll"], serde_json::json!(false));
        assert!(packed["poll"].is_null());
    }

    #[test]
    fn poll_expiry_job_fires_poll_ended() {
        use crate::jobs::worker::JobWorker;
        use crate::streaming::hub::{HubMessage, hub_and_sink};

        let (mut services, clock) = poll_services("expiry");
        let (hub, sink) = hub_and_sink(64);
        services.streaming = sink.clone();
        let services = Arc::new(services);
        let mut rx = hub.subscribe();

        let alice = services.create_account("alice", None).unwrap();
        let now = clock.now_ms();
        let note = poll_note(&services, &alice.id, &["a", "b"], false, Some(now + 60_000));
        // Drain the creation noise (NoteCreated streaming publish).
        let worker = JobWorker {
            clock: services.clock.clone(),
            ids: services.ids.clone(),
            jobs: services.jobs.clone(),
            notifications: services.notifications.clone(),
            streaming: sink.clone(),
            services: services.clone(),
            delivery_tracker: Arc::new(crate::federation::retry::DestinationTracker::new()),
            lease_duration_ms: 30_000,
            base_backoff_ms: 1_000,
        };
        // The expiry job is not due yet: only the creation jobs drain, and
        // no PollEnded event arrives.
        worker.drain(100).unwrap();
        assert!(rx.try_recv().is_ok());
        while rx.try_recv().is_ok() {}

        // Past expiry, a fresh drain publishes `pollEnded` for the note.
        clock.advance_ms(61_000);
        // Re-arm: enqueue a PollExpiry job due now (the armed one is due at
        // `expires_at`, which the manual clock has now passed — claimable).
        worker.drain(100).unwrap();
        let mut saw_ended = false;
        let mut saw_voted = false;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                HubMessage::PollEnded { note_id } if note_id == note.id => saw_ended = true,
                HubMessage::PollVoted { .. } => saw_voted = true,
                _ => {}
            }
        }
        assert!(saw_ended, "expected PollEnded for {}", note.id);
        assert!(!saw_voted);
    }

    #[test]
    fn poll_only_renote_is_a_quote_and_lists_as_child() {
        let (services, _) = poll_services("poll-quote");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target =
            crate::application::ports::CreateNoteInput::simple_public(&alice.id, "original");
        let target = services.create_note(target).unwrap();

        // Renote + poll but no text: a quote (Misskey `isQuote` parity),
        // not a pure renote.
        let mut input =
            crate::application::ports::CreateNoteInput::simple_public(&bob.id, "");
        input.text = None;
        input.renote_of = Some(target.id.clone());
        input.poll = Some(PollInput {
            choices: vec!["a".to_string(), "b".to_string()],
            multiple: false,
            expires_at_ms: None,
        });
        input.has_poll = true;
        let quote = services.create_note(input).unwrap();
        assert!(quote.is_quote());
        assert!(services.polls.get_poll(&quote.id).unwrap().is_some());

        // ... and the poll quote lists under the target's children.
        let children = services
            .note_children(
                &target.id,
                Some(&alice.id),
                &crate::application::ports::TimelineCursor::with_limit(10),
            )
            .unwrap();
        assert!(children.iter().any(|n| n.id == quote.id));
    }

    #[test]
    fn inbound_vote_and_question_sync_converge() {
        let (services, clock) = poll_services("inbound");
        let alice = services.create_account("alice", None).unwrap();
        let now = clock.now_ms();
        let note = poll_note(&services, &alice.id, &["a", "b"], false, Some(now + 600_000));

        // A remote voter id (no local account needed for the op itself).
        let voter = "remote-voter-1";
        assert!(services.handle_inbound_vote("act-1", voter, &note.id, "b").unwrap());
        // Replay of the same choice converges without double-counting.
        assert!(!services.handle_inbound_vote("act-1", voter, &note.id, "b").unwrap());
        assert_eq!(
            services.polls.get_poll(&note.id).unwrap().unwrap().votes,
            vec![0, 1]
        );
        // Unknown choice / expired / missing converge quietly.
        assert!(!services.handle_inbound_vote("act-2", voter, &note.id, "zzz").unwrap());
        assert!(!services.handle_inbound_vote("act-3", voter, "nope", "a").unwrap());

        // Question sync by choice name (Misskey `updateQuestion` parity).
        let counts = vec![
            PollChoiceCount {
                name: "a".to_string(),
                votes: 5,
            },
            PollChoiceCount {
                name: "nope".to_string(),
                votes: 99,
            },
        ];
        // The cached note is local-authored here, so sync is a no-op (only
        // cached *remote* polls refresh from federation input).
        assert!(
            services
                .handle_inbound_question_update("act-9", "https://x/notes/1", None, counts)
                .unwrap()
                .is_none()
        );
    }
}
