//! Blocking / muting application operations (§0A).
//!
//! Local Misskey endpoints (`blocking/*`, `mute/*`, `renote-mute/*`) and the
//! ActivityPub adapters (`Block` / `Undo(Block)`) all funnel through these
//! ops. State + durable jobs commit in ONE storage transaction per op
//! (implemented in the `BlockStore` / `MuteStore` port methods); federation
//! jobs are built here.

use super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::PortError;
use crate::domain::blocking::Blocking;
use crate::domain::muting::{Muting, RenoteMuting};
use crate::domain::note::Note;

impl AppServices {
    /// Block `blockee_id` as `blocker_id` (Misskey
    /// `UserBlockingService.block()` parity): cancels follow requests both
    /// ways, unfollows both ways (atomically in storage), and federates
    /// `Block` when a local user blocks a remote user.
    pub fn block(&self, blocker_id: &str, blockee_id: &str) -> Result<Blocking, PortError> {
        self.accounts.get_owner(blocker_id)?;
        // The federation `Block` activity id is the stored row id (Misskey
        // `renderBlock` parity), so the row is built before the jobs.
        let blocking = Blocking {
            id: self.ids.generate(),
            blocker_id: blocker_id.to_string(),
            blockee_id: blockee_id.to_string(),
        };
        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::MeUpdated {
                account_id: blocker_id.to_string(),
            },
            format!("block:{blocker_id}:{blockee_id}:{}", self.ids.generate()),
        )?];
        if let (Some(blocker), Some(blockee)) = (
            self.accounts.find_by_id(blocker_id)?,
            self.accounts.find_by_id(blockee_id)?,
        ) {
            if blocker.is_local() && !blockee.is_local() {
                if let Some(inbox_url) = crate::federation::outbox::preferred_inbox(&blockee) {
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::Block {
                            block_id: blocking.id.clone(),
                            blocker_id: blocker_id.to_string(),
                            blockee_id: blockee_id.to_string(),
                            inbox_url,
                        }
                        .into_job(
                            format!(
                                "federate:block:{blocker_id}:{blockee_id}:{}",
                                blocking.id
                            ),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        self.blocks.block(blocking, jobs)
    }

    /// Undo a block (Misskey `UserBlockingService.unblock()` parity):
    /// federates `Undo(Block)` for local→remote.
    pub fn unblock(&self, blocker_id: &str, blockee_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(blocker_id)?;
        let existing = self
            .blocks
            .get_blocking(blocker_id, blockee_id)?
            .ok_or_else(|| PortError::NotFound("NOT_BLOCKING".to_string()))?;
        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::MeUpdated {
                account_id: blocker_id.to_string(),
            },
            format!("unblock:{blocker_id}:{blockee_id}:{}", self.ids.generate()),
        )?];
        if let (Some(blocker), Some(blockee)) = (
            self.accounts.find_by_id(blocker_id)?,
            self.accounts.find_by_id(blockee_id)?,
        ) {
            if blocker.is_local() && !blockee.is_local() {
                if let Some(inbox_url) = crate::federation::outbox::preferred_inbox(&blockee) {
                    let undo_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::UndoBlock {
                            undo_id: undo_id.clone(),
                            block_id: existing.id.clone(),
                            blocker_id: blocker_id.to_string(),
                            blockee_id: blockee_id.to_string(),
                            inbox_url,
                        }
                        .into_job(
                            format!("federate:unblock:{blocker_id}:{blockee_id}:{undo_id}"),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        self.blocks.unblock(blocker_id, blockee_id, jobs)
    }

    /// Mute `mutee_id` as `muter_id` (Misskey `UserMutingService.mute()`
    /// parity). `Ok(None)` when `expires_at_ms` lies in the past (the
    /// endpoint returns early without creating, like upstream).
    pub fn mute(
        &self,
        muter_id: &str,
        mutee_id: &str,
        expires_at_ms: Option<i64>,
    ) -> Result<Option<Muting>, PortError> {
        self.accounts.get_owner(muter_id)?;
        self.mutes.mute(muter_id, mutee_id, expires_at_ms, Vec::new())
    }

    pub fn unmute(&self, muter_id: &str, mutee_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(muter_id)?;
        self.mutes.unmute(muter_id, mutee_id, Vec::new())
    }

    pub fn renote_mute(
        &self,
        muter_id: &str,
        mutee_id: &str,
    ) -> Result<RenoteMuting, PortError> {
        self.accounts.get_owner(muter_id)?;
        self.mutes.renote_mute(muter_id, mutee_id, Vec::new())
    }

    pub fn renote_unmute(&self, muter_id: &str, mutee_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(muter_id)?;
        self.mutes.renote_unmute(muter_id, mutee_id, Vec::new())
    }

    /// §4 hook: the Misskey `notes/polls/vote` `YOU_HAVE_BEEN_BLOCKED` gate.
    /// Called by `vote_on_poll` before recording a vote (and re-checked
    /// atomically inside the storage transaction).
    pub fn check_poll_vote_block(
        &self,
        voter_id: &str,
        note_id: &str,
    ) -> Result<Note, PortError> {
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        if note.author_id != voter_id
            && self.blocks.is_blocking(&note.author_id, voter_id)?
        {
            return Err(PortError::Validation(
                "YOU_HAVE_BEEN_BLOCKED".to_string(),
            ));
        }
        Ok(note)
    }

    /// Record an inbound `Block` (remote `blocker_id` blocks local
    /// `blockee_id`; Misskey `ApInboxService.block()` parity). Converges to
    /// `Ok(None)` on replays and on re-blocks of an existing pair.
    pub fn handle_inbound_block(
        &self,
        activity_id: &str,
        blocker_id: &str,
        blockee_id: &str,
    ) -> Result<Option<Blocking>, PortError> {
        let jobs = vec![self.streaming_job(
            &StreamingEvent::MeUpdated {
                account_id: blockee_id.to_string(),
            },
            format!(
                "inbound-block:{blocker_id}:{blockee_id}:{}",
                self.ids.generate()
            ),
        )?];
        match self
            .dedup
            .record_block_activity(activity_id, blocker_id, blockee_id, jobs)
        {
            Ok(v) => Ok(v),
            Err(PortError::Conflict(msg)) if msg == "ALREADY_BLOCKING" => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Undo(Block)` (Misskey `ApInboxService.undoBlock()`
    /// parity). `Ok(false)` on replays and when no block row exists.
    pub fn handle_inbound_unblock(
        &self,
        activity_id: &str,
        blocker_id: &str,
        blockee_id: &str,
    ) -> Result<bool, PortError> {
        let jobs = vec![self.streaming_job(
            &StreamingEvent::MeUpdated {
                account_id: blockee_id.to_string(),
            },
            format!(
                "inbound-unblock:{blocker_id}:{blockee_id}:{}",
                self.ids.generate()
            ),
        )?];
        match self
            .dedup
            .record_unblock_activity(activity_id, blocker_id, blockee_id, jobs)
        {
            Ok(v) => Ok(v),
            Err(PortError::NotFound(msg)) if msg == "NOT_BLOCKING" => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::application::commands::tests::test_services;
    use crate::application::ports::{
        ActivityDedup, BlobStore, CreateNoteInput, NoopStreamingSink, StreamingSink,
        TimelineCursor,
    };
    use crate::domain::id::{AidxGenerator, Clock, ManualClock};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    /// Same as the shared `test_services`, but also hands out the concrete
    /// clock so expiry tests can travel through time.
    fn test_services_with_clock(prefix: &str) -> (AppServices, Arc<ManualClock>) {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-blocks-test-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn crate::application::ports::IdGenerator> =
            Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        let streaming: Arc<dyn StreamingSink> = Arc::new(NoopStreamingSink);
        let services = AppServices {
            clock: clock.clone(),
            ids: ids.clone(),
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            favorites: store.clone(),
            emojis: store.clone(),
            antennas: store.clone(),
            antenna_notes: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: streaming.clone(),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            lists: store.clone(),
            signin: store.clone(),
            password_resets: store.clone(),
            registry: store.clone(),
            clips: store.clone(),
            pages: store.clone(),
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

    fn accounts() -> (AppServices, String, String) {
        let services = test_services("blocks-0a");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        (services, alice.id, bob.id)
    }

    #[test]
    fn block_self_and_unknown() {
        let (services, alice, _) = accounts();
        let err = services.block(&alice, &alice).unwrap_err();
        assert_eq!(err, PortError::Validation("BLOCKEE_IS_YOURSELF".to_string()));
        let err = services.block(&alice, "aid00000000009999").unwrap_err();
        assert_eq!(err, PortError::NotFound("NO_SUCH_USER".to_string()));
    }

    #[test]
    fn block_tears_down_follows_and_requests_both_ways() {
        let (services, alice, bob) = accounts();
        // Follows both ways.
        services.follow(&alice, &bob, false).unwrap();
        services.follow(&bob, &alice, false).unwrap();
        assert!(services.follows.is_following(&alice, &bob).unwrap());
        assert!(services.follows.is_following(&bob, &alice).unwrap());
        // Block: single op removes both edges + enqueues durable jobs.
        let jobs_before = services.jobs.pending_count().unwrap();
        services.block(&alice, &bob).unwrap();
        assert!(!services.follows.is_following(&alice, &bob).unwrap());
        assert!(!services.follows.is_following(&bob, &alice).unwrap());
        assert!(services.blocks.is_blocking(&alice, &bob).unwrap());
        assert!(services.jobs.pending_count().unwrap() > jobs_before);
        // Duplicate → ALREADY_BLOCKING.
        let err = services.block(&alice, &bob).unwrap_err();
        assert_eq!(err, PortError::Conflict("ALREADY_BLOCKING".to_string()));
        // Unblock + missing unblock.
        services.unblock(&alice, &bob).unwrap();
        assert!(!services.blocks.is_blocking(&alice, &bob).unwrap());
        let err = services.unblock(&alice, &bob).unwrap_err();
        assert_eq!(err, PortError::NotFound("NOT_BLOCKING".to_string()));
    }

    #[test]
    fn block_cancels_pending_requests_both_ways() {
        let (services, alice, bob) = accounts();
        services
            .accounts
            .update_account(&bob, crate::application::ports::UpdateAccountPatch {
                is_locked: Some(true),
                ..Default::default()
            }, Vec::new())
            .unwrap();
        services.follow(&alice, &bob, false).unwrap();
        assert!(services.follows.get_follow_request(&alice, &bob).unwrap().is_some());
        services.block(&bob, &alice).unwrap();
        assert!(services.follows.get_follow_request(&alice, &bob).unwrap().is_none());
    }

    #[test]
    fn follow_blocked_errors_both_directions() {
        let (services, alice, bob) = accounts();
        services.block(&alice, &bob).unwrap();
        // Follower blocks followee → BLOCKING.
        let err = services.follow(&alice, &bob, false).unwrap_err();
        assert_eq!(err, PortError::Validation("BLOCKING".to_string()));
        // Followee blocks follower → BLOCKED.
        let err = services.follow(&bob, &alice, false).unwrap_err();
        assert_eq!(err, PortError::Validation("BLOCKED".to_string()));
    }

    #[test]
    fn react_blocked_by_author_errors() {
        let (services, alice, bob) = accounts();
        let note = services
            .create_note(CreateNoteInput::simple_public(&bob, "hi"))
            .unwrap();
        services.react(&alice, &note.id, "❤", false).unwrap();
        services.block(&bob, &alice).unwrap();
        let carol = services.create_account("carol", None).unwrap();
        let note2 = services
            .create_note(CreateNoteInput::simple_public(&bob, "hi again"))
            .unwrap();
        let err = services.react(&alice, &note2.id, "❤", false).unwrap_err();
        assert_eq!(
            err,
            PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string())
        );
        // Self-reaction is exempt.
        services.react(&bob, &note2.id, "❤", false).unwrap();
        let _ = carol;
    }

    #[test]
    fn pure_renote_of_blocker_rejected_quote_allowed() {
        let (services, alice, bob) = accounts();
        let target = services
            .create_note(CreateNoteInput::simple_public(&bob, "target"))
            .unwrap();
        services.block(&bob, &alice).unwrap();
        // Pure renote → YOU_HAVE_BEEN_BLOCKED.
        let mut pure = CreateNoteInput::simple_public(&alice, "x");
        pure.text = None;
        pure.renote_of = Some(target.id.clone());
        let err = services.create_note(pure).unwrap_err();
        assert_eq!(
            err,
            PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string())
        );
        // Quote (text present) is allowed.
        let mut quote = CreateNoteInput::simple_public(&alice, "quoting");
        quote.renote_of = Some(target.id.clone());
        services.create_note(quote).unwrap();
    }

    #[test]
    fn mute_lifecycle_and_expiry() {
        let (services, clock) = test_services_with_clock("mute-expiry");
        let alice = services.create_account("alice", None).unwrap().id;
        let bob = services.create_account("bob", None).unwrap().id;
        // Self-mute rejected.
        let err = services.mute(&alice, &alice, None).unwrap_err();
        assert_eq!(err, PortError::Validation("MUTEE_IS_YOURSELF".to_string()));
        // Past expiry creates nothing and is not an error (upstream parity).
        let past = clock.now_ms() - 1;
        assert_eq!(services.mute(&alice, &bob, Some(past)).unwrap(), None);
        assert!(!services.mutes.is_muting(&alice, &bob).unwrap());
        // Future expiry is active until it passes.
        let future = clock.now_ms() + 60_000;
        assert!(services.mute(&alice, &bob, Some(future)).unwrap().is_some());
        assert!(services.mutes.is_muting(&alice, &bob).unwrap());
        let err = services.mute(&alice, &bob, None).unwrap_err();
        assert_eq!(err, PortError::Conflict("ALREADY_MUTING".to_string()));
        clock.set_ms(future + 1);
        assert!(!services.mutes.is_muting(&alice, &bob).unwrap());
        // Unmute of a missing row → NOT_MUTING.
        services.unmute(&alice, &bob).unwrap();
        let err = services.unmute(&alice, &bob).unwrap_err();
        assert_eq!(err, PortError::NotFound("NOT_MUTING".to_string()));
    }

    #[test]
    fn renote_mute_lifecycle() {
        let (services, alice, bob) = accounts();
        services.renote_mute(&alice, &bob).unwrap();
        assert!(services.mutes.is_renote_muting(&alice, &bob).unwrap());
        let err = services.renote_mute(&alice, &bob).unwrap_err();
        assert_eq!(err, PortError::Conflict("ALREADY_MUTING".to_string()));
        services.renote_unmute(&alice, &bob).unwrap();
        assert!(!services.mutes.is_renote_muting(&alice, &bob).unwrap());
        let err = services.renote_unmute(&alice, &bob).unwrap_err();
        assert_eq!(err, PortError::NotFound("NOT_MUTING".to_string()));
    }

    #[test]
    fn muted_author_filtered_from_home_timeline() {
        let (services, alice, bob) = accounts();
        services.follow(&alice, &bob, false).unwrap();
        services
            .create_note(CreateNoteInput::simple_public(&bob, "hello"))
            .unwrap();
        let cursor = TimelineCursor::with_limit(10);
        assert_eq!(services.home_timeline(&alice, &cursor, false, true).unwrap().len(), 1);
        services.mute(&alice, &bob, None).unwrap();
        assert!(services.home_timeline(&alice, &cursor, false, true).unwrap().is_empty());
    }

    #[test]
    fn blocked_author_filtered_from_local_timeline_and_notes_show() {
        let (services, alice, bob) = accounts();
        let note = services
            .create_note(CreateNoteInput::simple_public(&bob, "hello"))
            .unwrap();
        let cursor = TimelineCursor::with_limit(10);
        assert_eq!(services.local_timeline(Some(&alice), &cursor, false, true, true).unwrap().len(), 1);
        services.notes_show(&note.id, Some(&alice)).unwrap();
        services.block(&alice, &bob).unwrap();
        assert!(services.local_timeline(Some(&alice), &cursor, false, true, true).unwrap().is_empty());
        let err = services.notes_show(&note.id, Some(&alice)).unwrap_err();
        assert_eq!(err, PortError::NotFound("NO_SUCH_NOTE".to_string()));
    }

    #[test]
    fn renote_mute_hides_pure_renotes_only() {
        let (services, alice, bob) = accounts();
        services.follow(&alice, &bob, false).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice, "target"))
            .unwrap();
        let mut pure = CreateNoteInput::simple_public(&bob, "x");
        pure.text = None;
        pure.renote_of = Some(target.id.clone());
        services.create_note(pure).unwrap();
        let mut quote = CreateNoteInput::simple_public(&bob, "quoting");
        quote.renote_of = Some(target.id.clone());
        services.create_note(quote).unwrap();
        let cursor = TimelineCursor::with_limit(10);
        assert_eq!(services.home_timeline(&alice, &cursor, false, true).unwrap().len(), 3);
        services.renote_mute(&alice, &bob).unwrap();
        let notes = services.home_timeline(&alice, &cursor, false, true).unwrap();
        assert_eq!(notes.len(), 2);
        assert!(notes.iter().all(|n| n.text.is_some()));
    }

    #[test]
    fn poll_vote_block_hook() {
        let (services, alice, bob) = accounts();
        let note = services
            .create_note(CreateNoteInput::simple_public(&bob, "with poll"))
            .unwrap();
        // No block: returns the note for §4 to proceed with.
        assert_eq!(services.check_poll_vote_block(&alice, &note.id).unwrap().id, note.id);
        services.block(&bob, &alice).unwrap();
        let err = services.check_poll_vote_block(&alice, &note.id).unwrap_err();
        assert_eq!(
            err,
            PortError::Validation("YOU_HAVE_BEEN_BLOCKED".to_string())
        );
        // Unknown note.
        let err = services.check_poll_vote_block(&alice, "nope").unwrap_err();
        assert_eq!(err, PortError::NotFound("NO_SUCH_NOTE".to_string()));
    }

    #[test]
    fn inbound_block_roundtrip_converges() {
        let (services, alice, bob) = accounts();
        services.follow(&alice, &bob, false).unwrap();
        let first = services.handle_inbound_block("act-1", &bob, &alice).unwrap();
        assert!(first.is_some());
        assert!(services.blocks.is_blocking(&bob, &alice).unwrap());
        assert!(!services.follows.is_following(&alice, &bob).unwrap());
        // Replay converges.
        assert_eq!(services.handle_inbound_block("act-1", &bob, &alice).unwrap(), None);
        // Different activity, same pair converges too.
        assert_eq!(services.handle_inbound_block("act-2", &bob, &alice).unwrap(), None);
        assert!(services.handle_inbound_unblock("act-3", &bob, &alice).unwrap());
        assert!(!services.blocks.is_blocking(&bob, &alice).unwrap());
        // Replay + missing converge to false.
        assert!(!services.handle_inbound_unblock("act-3", &bob, &alice).unwrap());
        assert!(!services.handle_inbound_unblock("act-4", &bob, &alice).unwrap());
    }
}

#[cfg(test)]
mod relation_tests {
    use crate::api::misskey::packers::pack_user_relation;
    use crate::application::commands::tests::test_services;

    #[test]
    fn relation_flags_reflect_block_mute_state() {
        let services = test_services("blocks-relation");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let rel = pack_user_relation(&alice.id, &bob.id, &services).unwrap();
        assert_eq!(rel["isBlocking"], false);
        assert_eq!(rel["isBlocked"], false);
        assert_eq!(rel["isMuted"], false);
        assert_eq!(rel["isRenoteMuted"], false);
        assert_eq!(rel["isFollowing"], false);
        services.block(&alice.id, &bob.id).unwrap();
        services.mute(&alice.id, &bob.id, None).unwrap();
        services.renote_mute(&alice.id, &bob.id).unwrap();
        let rel = pack_user_relation(&alice.id, &bob.id, &services).unwrap();
        assert_eq!(rel["isBlocking"], true);
        assert_eq!(rel["isBlocked"], false);
        assert_eq!(rel["isMuted"], true);
        assert_eq!(rel["isRenoteMuted"], true);
        let rel = pack_user_relation(&bob.id, &alice.id, &services).unwrap();
        assert_eq!(rel["isBlocking"], false);
        assert_eq!(rel["isBlocked"], true);
    }
}

#[cfg(test)]
mod federation_tests {
    use crate::application::commands::tests::test_services;
    use crate::application::ports::{CreateAccountInput, JobKind};
    use crate::federation::outbox::FederateDeliverPayload;

    fn remote_account(services: &AppServices, handle: &str) -> String {
        services
            .accounts
            .create_account(CreateAccountInput {
                handle: handle.to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://remote.example/users/{handle}")),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .unwrap()
            .id
    }

    fn federate_jobs(services: &AppServices) -> Vec<FederateDeliverPayload> {
        let mut out = Vec::new();
        while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
            if job.kind == JobKind::FederateDeliver {
                out.push(serde_json::from_slice(&job.payload).unwrap());
            }
            services.jobs.complete(&job.id).unwrap();
        }
        out
    }

    use super::AppServices;

    #[test]
    fn local_to_remote_block_federates_block_then_undo() {
        let services = test_services("blocks-fed");
        let alice = services.create_account("alice", None).unwrap().id;
        let rina = remote_account(&services, "rina");
        services.block(&alice, &rina).unwrap();
        let jobs = federate_jobs(&services);
        assert_eq!(jobs.len(), 1);
        let block_id = match &jobs[0] {
            FederateDeliverPayload::Block { block_id, blocker_id, blockee_id, inbox_url } => {
                assert_eq!(blocker_id, &alice);
                assert_eq!(blockee_id, &rina);
                assert_eq!(inbox_url, "https://remote.example/inbox");
                block_id.clone()
            }
            other => panic!("expected Block, got {other:?}"),
        };
        services.unblock(&alice, &rina).unwrap();
        let jobs = federate_jobs(&services);
        assert_eq!(jobs.len(), 1);
        match &jobs[0] {
            FederateDeliverPayload::UndoBlock { block_id: inner, blocker_id, blockee_id, .. } => {
                // Undo wraps the original Block id (Misskey renderUndo parity).
                assert_eq!(inner, &block_id);
                assert_eq!(blocker_id, &alice);
                assert_eq!(blockee_id, &rina);
            }
            other => panic!("expected UndoBlock, got {other:?}"),
        }
    }

    #[test]
    fn remote_to_local_block_federates_nothing() {
        let services = test_services("blocks-fed-in");
        let alice = services.create_account("alice", None).unwrap().id;
        let rina = remote_account(&services, "rina");
        services.handle_inbound_block("act-1", &rina, &alice).unwrap();
        assert!(federate_jobs(&services).is_empty());
    }
}
