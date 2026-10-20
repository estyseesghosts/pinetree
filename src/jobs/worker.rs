use std::sync::Arc;

use crate::api::misskey::packers::{
    pack_note, pack_notification, pack_user_detailed_not_me,
};
use crate::application::commands::AppServices;
use crate::application::events::{MainEventKind, MainMessage, NotifyJobPayload, StreamingEvent};
use crate::application::ports::{
    Clock, IdGenerator, Job, JobKind, JobQueue, NewJob, NotificationStore, PortError,
    RemoteMediaFetchPayload, StreamingSink, MAX_JOBS_PER_TXN, MAX_JOB_ATTEMPTS,
};
use crate::domain::notification::{Notification, NotificationKind};
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

/// In-process durable-job worker (Phase 1, single node).
pub struct JobWorker {
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    pub jobs: Arc<dyn JobQueue>,
    pub notifications: Arc<dyn NotificationStore>,
    pub streaming: Arc<dyn StreamingSink>,
    pub services: Arc<AppServices>,
    pub delivery_tracker: Arc<DestinationTracker>,
    pub lease_duration_ms: i64,
    pub base_backoff_ms: i64,
}

/// How a claimed job's record is settled after its handler runs.
enum Disposition {
    /// Handler succeeded: remove the row.
    Done,
    /// Handler failed retryably: reschedule with backoff, quarantining when
    /// the retry budget (`MAX_JOB_ATTEMPTS`) is exhausted. When
    /// `retry_after_ms` is set (a server's `Retry-After`), the job is not
    /// rescheduled earlier than that.
    Retry { retry_after_ms: Option<i64> },
    /// Fan-out finished: complete the job and enqueue these per-inbox
    /// follow-ups. The last batch commits atomically with the completion.
    Fanout(Vec<NewJob>),
    /// Destination in cooldown: reschedule at this time WITHOUT counting an
    /// attempt.
    Defer(i64),
    /// Handler failed terminally: quarantine with the recorded reason.
    Quarantine(String),
}

/// Split follow-up jobs into full batches (each at most `MAX_JOBS_PER_TXN`)
/// that are enqueued on their own, plus the final batch that is committed
/// atomically with the completion of the activity job. The final batch is
/// never empty when the input is not.
pub fn split_followup_batches(mut followups: Vec<NewJob>) -> (Vec<Vec<NewJob>>, Vec<NewJob>) {
    let mut batches = Vec::new();
    while followups.len() > MAX_JOBS_PER_TXN {
        let rest = followups.split_off(MAX_JOBS_PER_TXN);
        batches.push(std::mem::replace(&mut followups, rest));
    }
    (batches, followups)
}

impl JobWorker {
    pub fn process_one(&self) -> Result<bool, PortError> {
        let claimed = self.jobs.claim_next(self.lease_duration_ms)?;
        let job = match claimed {
            None => return Ok(false),
            Some(job) => job,
        };
        // Panic containment: a panicking handler must quarantine the job
        // (burning no extra attempt beyond the lease-reclaim count) instead
        // of unwinding the worker and looping forever on the same job.
        let dispatch: Result<Disposition, PortError> =
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.dispatch_job(&job)
            })) {
                Ok(inner) => inner,
                Err(payload) => Ok(Disposition::Quarantine(format!(
                    "panic: {}",
                    panic_payload_message(&payload)
                ))),
            };
        // Storage errors from the settlement calls below propagate to the
        // caller, as before (drain/spawn behavior unchanged).
        match dispatch? {
            Disposition::Done => {
                self.jobs.complete(&job.id)?;
            }
            Disposition::Retry { retry_after_ms } => {
                if job.attempts.saturating_add(1) >= MAX_JOB_ATTEMPTS {
                    // Retry budget exhausted for any kind: quarantine with a
                    // recorded reason instead of retrying forever.
                    self.quarantine(&job, "retry-exhausted")?;
                } else {
                    let shift = job.attempts.min(10);
                    let backoff = self.base_backoff_ms.saturating_mul(1_i64 << shift);
                    let mut retry_at = self.clock.now_ms().saturating_add(backoff);
                    if let Some(earliest) = retry_after_ms {
                        retry_at = retry_at.max(earliest);
                    }
                    self.jobs.fail(&job.id, retry_at)?;
                }
            }
            Disposition::Fanout(followups) => {
                let (batches, last) = split_followup_batches(followups);
                for batch in batches {
                    self.jobs.enqueue_jobs(batch)?;
                }
                self.jobs.complete_with_followups(&job.id, last)?;
            }
            Disposition::Defer(until) => {
                self.jobs.defer(&job.id, until)?;
            }
            Disposition::Quarantine(reason) => {
                self.quarantine(&job, &reason)?;
            }
        }
        Ok(true)
    }

    fn dispatch_job(&self, job: &Job) -> Result<Disposition, PortError> {
        match job.kind {
            JobKind::Notify => match self.handle_notify(&job.payload, &job.idempotency_key) {
                Ok(()) => Ok(Disposition::Done),
                // Unparseable payloads can never succeed on retry.
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
            },
            JobKind::StreamingPublish => match self.handle_streaming_publish(&job.payload)
            {
                Ok(()) => Ok(Disposition::Done),
                // Unparseable payloads can never succeed on retry.
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
            },
            JobKind::FederateDeliver => self.dispatch_federate(job),
            // No materialized-timeline fan-out exists (fan-out-on-read);
            // the kind is reserved so enqueued rows complete instead of
            // wedging the worker.
            JobKind::TimelineMaterialize => Ok(Disposition::Done),
            // Slice 7 (H2+C3, decision D-d): remote media fetched
            // off-request. Bad payloads quarantine; per-attachment fetch
            // failures are best-effort skips inside the processor, so only
            // storage faults retry the whole job (already-attached URLs are
            // re-skipped without network I/O).
            JobKind::RemoteMediaFetch => match self.handle_remote_media_fetch(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
            },
            // Quote-post authorization: re-verify an offered stamp whose
            // first fetch failed transiently. Bad payloads quarantine;
            // transient fetch failures retry with backoff (quarantined past
            // the budget); invalid stamp content completes quietly — a later
            // re-delivered Accept (new activity id) can still be processed.
            // Quote-post authorization: re-verify an offered stamp whose
            // first fetch failed transiently. Bad payloads quarantine;
            // transient fetch failures retry with backoff (quarantined past
            // the budget); invalid stamp content completes quietly — a later
            // re-delivered Accept (new activity id) can still be processed.
            JobKind::QuoteStampReverify => {
                match self.handle_quote_stamp_reverify(&job.payload) {
                    Ok(()) => Ok(Disposition::Done),
                    Err(PortError::Validation(reason)) => {
                        Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                    }
                    Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
                }
            }
            // Branched replies (`docs/replies.md` Slice B): resolve one
            // pending reply-parent URI off-request. Bad payloads quarantine;
            // an unavailable parent retries with backoff (quarantined past
            // the budget with the pending entry kept); a full batch returns
            // a continuation job for the same URI via `Fanout`.
            JobKind::ReplyParentResolve => {
                match crate::application::commands::inbound::replies::handle_reply_parent_resolve(
                    &self.services,
                    &job.payload,
                ) {
                    Ok(None) => Ok(Disposition::Done),
                    Ok(Some(continuation)) => {
                        Ok(Disposition::Fanout(vec![continuation]))
                    }
                    Err(PortError::Validation(reason)) => {
                        Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                    }
                    Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
                }
            }
            // Antenna fan-in (Batch N WS4): match one committed note into
            // every active antenna. Bad payloads quarantine; a gone note
            // converges quietly; only storage faults retry.
            JobKind::AntennaMatch => match self.handle_antenna_match(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
            },
            // Poll expiry (§4): publish the `pollEnded` streaming event
            // once the poll has expired. Bad payloads quarantine; a gone
            // note/poll or a not-yet-expired poll completes quietly (a
            // re-armed job covers clock skew). Only storage faults retry.
            JobKind::PollExpiry => match self.handle_poll_expiry(&job.payload) {
                Ok(_) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
            },
            // Scheduled drafts (§5): post the draft through the full
            // note-create path once its time arrives. Bad payloads
            // quarantine; a gone/unscheduled/rescheduled draft converges
            // quietly (Done — the re-armed job covers it when one exists).
            // Only storage faults retry. A failed post is itself a
            // successful run (`ScheduledNotePostFailed` is emitted inside).
            JobKind::PostScheduledDraft => match self.handle_post_scheduled_draft(&job.payload) {
                Ok(_) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
            },
            // Remote custom-emoji lazy fetch (Batch N WP8b): download one
            // known remote icon off-request. Bad payloads quarantine; a
            // gone/already-cached row or an unusable fetch converges quietly
            // (Done — the packed URL keeps pointing at the remote host).
            // Only storage faults retry.
            JobKind::RemoteEmojiFetch => match self.handle_remote_emoji_fetch(&job.payload) {
                Ok(()) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
            },
            // Profile export (§13): build the file, store it in the caller's
            // drive, and fan out the `exportCompleted` Notify follow-up —
            // committed atomically with the export job's completion, so the
            // notification can never be lost or duplicated. Bad payloads
            // quarantine; a gone user converges quietly (no follow-up);
            // only storage faults retry.
            JobKind::Export => match self.handle_export(&job.payload) {                Ok(None) => Ok(Disposition::Done),
                Ok(Some(notify)) => Ok(Disposition::Fanout(vec![notify])),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
            },
            // Profile import (§13): apply the caller's drive-file rows
            // through the interactive application ops. Bad payloads
            // quarantine; gone users/files converge quietly; per-row
            // failures skip the row inside the handler. Only storage
            // faults retry.
            JobKind::Import => match self.handle_import(&job.payload) {
                Ok(_) => Ok(Disposition::Done),
                Err(PortError::Validation(reason)) => {
                    Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                }
                Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
            },
            // Silent unfollow sweep (Batch M WP3): remove one bounded batch
            // of the account's outgoing follows (row + its `Undo(Follow)`
            // job commit together); a full batch returns a continuation.
            // Bad payloads quarantine; per-row conflicts converge as skips;
            // only storage faults retry the job.
            JobKind::UnfollowAll => {
                match self.services.handle_unfollow_all(&job.payload) {
                    Ok(None) => Ok(Disposition::Done),
                    Ok(Some(continuation)) => {
                        Ok(Disposition::Fanout(vec![continuation]))
                    }
                    Err(PortError::Validation(reason)) => {
                        Ok(Disposition::Quarantine(format!("bad-payload: {reason}")))
                    }
                    Err(_) => Ok(Disposition::Retry { retry_after_ms: None }),
                }
            }
        }
    }

    fn quarantine(&self, job: &Job, reason: &str) -> Result<(), PortError> {
        eprintln!(
            "quarantining job {} ({:?}) after {} attempts: {reason}",
            job.id, job.kind, job.attempts
        );
        self.jobs.fail_permanent(&job.id, reason)
    }

    fn handle_notify(&self, payload: &[u8], idempotency_key: &str) -> Result<(), PortError> {
        let notify: NotifyJobPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("notify payload invalid: {e}")))?;
        // §11 creation-time receive gate (Misskey `NotificationService`
        // `#createNotificationInternal` parity): self-notify, active mutes,
        // and `notificationRecieveConfig` drop the notification here — the
        // single choke point every local and inbound `Notify` job flows
        // through. Drops converge quietly (job completes, no row, no hints);
        // storage faults still retry. Scheduler-direct inserts bypass this
        // gate like upstream notifier-less notifications do.
        //
        // §13 `exportCompleted` is notifier-less upstream (no `notifierId`,
        // so neither the self-notify nor the mute check applies — only a
        // `never` receive-config drops it). Pinetree rows require a
        // notifier (the exporter), so this kind takes the dedicated
        // notifier-less gate instead. Batch P `test` rides the same gate
        // (upstream `createNotification(user.id, 'test', {})` is equally
        // notifier-less).
        let allowed = if notify.kind == NotificationKind::ExportCompleted
            || notify.kind == NotificationKind::Test
        {
            self.services
                .system_notification_allowed(&notify.notifiee_id, notify.kind)?
        } else if !self.services.notification_delivery_allowed(
            &notify.notifiee_id,
            notify.kind,
            &notify.notifier_id,
        )? {
            return Ok(());
        } else {
            true
        };
        if !allowed {
            return Ok(());
        }
        let id = if idempotency_key.trim().is_empty() {
            self.ids.generate()
        } else {
            idempotency_key.to_string()
        };
        let notification = Notification {
            id,
            notifiee_id: notify.notifiee_id.clone(),
            notifier_id: notify.notifier_id.clone(),
            kind: notify.kind,
            note_id: notify.note_id.clone(),
            reaction: notify.reaction.clone(),
            draft_id: None,
            file_id: notify.file_id.clone(),
            exported_entity: notify.exported_entity.clone(),
            is_read: false,
        };
        let stored = match self.notifications.insert_notification(notification) {
            Ok(stored) => stored,
            // Already inserted under this idempotency key: the live hints
            // below already went out on the first pass, so a replay only
            // completes instead of duplicating them.
            Err(PortError::Conflict(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        // Fresh insert only: fan the `main`-channel hints out through the
        // streaming sink (a no-op when streaming is disabled). Hints are
        // at-most-once per connection by design (see `streaming::hub`).
        self.push_notify_mains(&notify, &stored)
    }

    /// Expand one stored notification into `main`-channel events (Misskey
    /// `NotificationService` + `UserFollowingService` parity): every kind
    /// emits `notification`; Mention/Reply/Renote also emit the packed note
    /// under their own event name; Follow emits `follow` (to the follower,
    /// packed followee) and `followed` (to the followee, packed follower);
    /// ReceiveFollowRequest emits `receiveFollowRequest` (packed follower).
    fn push_notify_mains(
        &self,
        notify: &NotifyJobPayload,
        stored: &Notification,
    ) -> Result<(), PortError> {
        let services = &self.services;
        let notifiee = notify.notifiee_id.as_str();
        let notifier = notify.notifier_id.as_str();
        let packed_notification = pack_notification(stored, services);
        services.streaming.push_main(MainMessage {
            user_id: notifiee.to_string(),
            kind: MainEventKind::Notification,
            body: packed_notification,
        })?;
        let packed_note = |note_id: &str| -> Result<Option<serde_json::Value>, PortError> {
            match services.notes.get_note(note_id)? {
                Some(note) => Ok(Some(pack_note(&note, Some(notifiee), services, 0)?)),
                None => Ok(None),
            }
        };
        let packed_user =
            |account_id: &str, viewer: &str| -> Result<Option<serde_json::Value>, PortError> {
                match services.accounts.find_by_id(account_id)? {
                    Some(account) => Ok(Some(pack_user_detailed_not_me(
                        &account,
                        Some(viewer),
                        services,
                    )?)),
                    None => Ok(None),
                }
            };
        match notify.kind {
            NotificationKind::Mention
            | NotificationKind::Reply
            | NotificationKind::Renote
            | NotificationKind::Edited => {
                let kind = match notify.kind {
                    NotificationKind::Mention => MainEventKind::Mention,
                    NotificationKind::Reply => MainEventKind::Reply,
                    NotificationKind::Edited => MainEventKind::Edited,
                    _ => MainEventKind::Renote,
                };
                if let Some(note_id) = notify.note_id.as_deref() {
                    if let Some(packed) = packed_note(note_id)? {
                        services.streaming.push_main(MainMessage {
                            user_id: notifiee.to_string(),
                            kind,
                            body: packed,
                        })?;
                    }
                }
            }
            NotificationKind::Follow => {
                if let Some(packed) = packed_user(notifiee, notifier)? {
                    services.streaming.push_main(MainMessage {
                        user_id: notifier.to_string(),
                        kind: MainEventKind::Follow,
                        body: packed,
                    })?;
                }
                if let Some(packed) = packed_user(notifier, notifiee)? {
                    services.streaming.push_main(MainMessage {
                        user_id: notifiee.to_string(),
                        kind: MainEventKind::Followed,
                        body: packed,
                    })?;
                }
            }
            NotificationKind::ReceiveFollowRequest => {
                if let Some(packed) = packed_user(notifier, notifiee)? {
                    services.streaming.push_main(MainMessage {
                        user_id: notifiee.to_string(),
                        kind: MainEventKind::ReceiveFollowRequest,
                        body: packed,
                    })?;
                }
            }
            NotificationKind::Reaction => {}
            // Scheduling notifications (§5) fan out as the plain
            // `notification` hint above (no note/user side-channel: the
            // posted note / failed draft rides in the packed body).
            // `exportCompleted` (§13) likewise: the drive file rides in the
            // packed body (`fileId`). `followRequestAccepted` likewise:
            // the packed body carries the acceptor.
            NotificationKind::FollowRequestAccepted
            | NotificationKind::ScheduledNotePosted
            | NotificationKind::ScheduledNotePostFailed
            | NotificationKind::ExportCompleted
            | NotificationKind::Test => {}
        }
        Ok(())
    }

    /// Expand one `StreamingPublish` job. `Unfollowed` needs packed bodies
    /// (worker-side, like notifications); every other payload flows
    /// straight into the sink untouched — including payloads this worker
    /// cannot parse (the sink validates on its own terms, so a legacy or
    /// foreign payload keeps the sink's retry/quarantine behavior instead
    /// of being reclassified here).
    fn handle_streaming_publish(&self, payload: &[u8]) -> Result<(), PortError> {
        if let Ok(StreamingEvent::Unfollowed {
            follower_id,
            followee_id,
        }) = serde_json::from_slice::<StreamingEvent>(payload)
        {
            // Only local followers hold sockets; the followee pack is for
            // the follower's `main` channel (Misskey parity).
            let local_follower = self
                .services
                .accounts
                .find_by_id(&follower_id)?
                .is_some_and(|a| a.is_local());
            if local_follower {
                if let Some(followee) = self.services.accounts.find_by_id(&followee_id)? {
                    let packed = pack_user_detailed_not_me(
                        &followee,
                        Some(&follower_id),
                        &self.services,
                    )?;
                    self.services.streaming.push_main(MainMessage {
                        user_id: follower_id,
                        kind: MainEventKind::Unfollow,
                        body: packed,
                    })?;
                }
            }
            return Ok(());
        }
        self.streaming.publish("streaming", payload)
    }

    /// Slice 7: run one `RemoteMediaFetch` job. A note deleted between
    /// enqueue and run maps to success (nothing to attach to).
    fn handle_remote_media_fetch(&self, payload: &[u8]) -> Result<(), PortError> {
        let parsed: RemoteMediaFetchPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("remote-media payload invalid: {e}")))?;
        match crate::media::remote_cache::process_remote_media_fetch(&self.services, &parsed) {
            Ok(()) => Ok(()),
            Err(PortError::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Run one `RemoteEmojiFetch` job (Batch N WP8b): fetch-and-cache one
    /// known remote emoji. Gone/already-cached rows and unusable fetches
    /// converge quietly inside; only storage faults retry.
    fn handle_remote_emoji_fetch(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::RemoteEmojiFetchPayload;
        let parsed: RemoteEmojiFetchPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("remote-emoji payload invalid: {e}")))?;
        self.services
            .fetch_remote_emoji(&parsed.host, &parsed.shortcode)
    }

    /// Run one `AntennaMatch` job (Batch N WS4): fan one committed note
    /// into matching antenna feeds (+ post-commit stream events). Gone
    /// notes converge quietly; only storage faults retry.
    fn handle_antenna_match(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::AntennaMatchPayload;
        let parsed: AntennaMatchPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("antenna-match payload invalid: {e}")))?;
        self.services.match_antenna_note(&parsed.note_id)
    }

    /// Run one `PollExpiry` job (§4): publish the `pollEnded` streaming
    /// event when the poll has expired. Gone notes/polls and premature
    /// runs complete quietly.
    fn handle_poll_expiry(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::PollExpiryPayload;
        let parsed: PollExpiryPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("poll-expiry payload invalid: {e}")))?;
        self.services
            .handle_poll_expiry(&parsed.note_id)
            .map(|_| ())
    }

    /// Run one `PostScheduledDraft` job (§5): post the draft when its
    /// scheduled time has arrived. Gone, unscheduled, rescheduled, and
    /// not-yet-due drafts converge quietly; only storage faults retry.
    fn handle_post_scheduled_draft(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::ScheduledDraftPayload;
        let parsed: ScheduledDraftPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("scheduled-draft payload invalid: {e}")))?;
        self.services
            .post_scheduled_draft(&parsed.draft_id, parsed.scheduled_at_ms)
            .map(|_| ())
    }

    /// Run one `Export` job (§13): build the file and store it in the
    /// caller's drive. Returns the `exportCompleted` `Notify` follow-up for
    /// the dispatcher to commit atomically with this job's completion, or
    /// `None` when the user is gone (converges quietly).
    fn handle_export(
        &self,
        payload: &[u8],
    ) -> Result<Option<crate::application::ports::NewJob>, PortError> {
        use crate::application::ports::ExportJobPayload;
        let parsed: ExportJobPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("export payload invalid: {e}")))?;
        self.services.run_export_job(&parsed)
    }

    /// Run one `Import` job (§13): apply the caller's drive-file rows.
    /// Gone users/files converge quietly; only storage faults retry.
    fn handle_import(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::ImportJobPayload;
        let parsed: ImportJobPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("import payload invalid: {e}")))?;
        self.services.run_import_job(&parsed)
    }

    /// Run one `QuoteStampReverify` job: re-dereference the stamp offered by
    /// an `Accept` whose inbox-time fetch failed transiently, and approve on
    /// success (recorded atomically with the state change). Decided/gone
    /// quotes complete quietly; invalid stamp content completes quietly
    /// (never approval); only transport/storage faults retry.
    fn handle_quote_stamp_reverify(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::QuoteStampReverifyPayload;
        use crate::domain::quote::QuoteAuthorizationState;

        let parsed: QuoteStampReverifyPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("quote-reverify payload invalid: {e}")))?;
        let services = &self.services;
        let Some(note) = services.notes.find_note_by_quote_request(&parsed.request_uri)? else {
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

    fn dispatch_federate(&self, job: &Job) -> Result<Disposition, PortError> {
        let outcome = match crate::federation::outbox::execute_federate_deliver(
            &self.services,
            &self.services.federation_http,
            &self.delivery_tracker,
            &job.idempotency_key,
            &job.payload,
        ) {
            Ok(outcome) => outcome,
            // Unparseable payloads can never succeed on retry.
            Err(PortError::Validation(reason)) => {
                crate::fed_debug!("out job key={:?} quarantine reason=bad-payload: {reason}", job.idempotency_key);
                return Ok(Disposition::Quarantine(format!("bad-payload: {reason}")));
            }
            Err(e) => return Err(e),
        };
        crate::fed_debug!("out job key={:?} outcome={outcome:?}", job.idempotency_key);
        Ok(match outcome {
            DeliveryOutcome::Delivered => Disposition::Done,
            DeliveryOutcome::PermanentFailure(reason) => {
                Disposition::Quarantine(format!("permanent-failure: {reason}"))
            }
            // `fail()` reschedules with backoff via `process_one`, which
            // quarantines once the retry budget is exhausted.
            DeliveryOutcome::TransientFailure { retry_after_ms, .. } => {
                Disposition::Retry { retry_after_ms }
            }
            DeliveryOutcome::Deferred { until_ms } => Disposition::Defer(until_ms),
            DeliveryOutcome::Split { followups } => Disposition::Fanout(followups),
        })
    }

    pub fn drain(&self, max_steps: usize) -> Result<usize, PortError> {
        let mut done = 0;
        for _ in 0..max_steps {
            // Resilient: a panic escaping `process_one` (e.g. a storage
            // panic outside the contained handler dispatch) must not unwind
            // the drain caller. The lease is left to expire — and expiry now
            // counts as an attempt — so the job is retried/quarantined
            // instead of wedging the loop.
            let step = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.process_one()
            }));
            match step {
                Ok(Ok(true)) => done += 1,
                Ok(Ok(false)) => break,
                Ok(Err(e)) => return Err(e),
                Err(_) => continue,
            }
        }
        Ok(done)
    }

    pub fn spawn(
        self: Arc<Self>,
        poll_interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let this = Arc::clone(&self);
                // `process_one` already quarantines handler panics via
                // `catch_unwind`; this outer catch only guards panics that
                // escape settlement/storage so the background loop never dies
                // silently. Either way the lease is settled (quarantine) or
                // left to expire (which counts as an attempt).
                let worked = tokio::task::spawn_blocking(move || {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        this.process_one()
                    })) {
                        Ok(Ok(v)) => v,
                        Ok(Err(_)) => false,
                        Err(_) => false,
                    }
                })
                .await
                .unwrap_or(false);
                if !worked {
                    tokio::time::sleep(poll_interval).await;
                }
            }
        })
    }
}

fn panic_payload_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod slice10_tests {
    use super::*;

    fn follow_up(n: usize) -> NewJob {
        NewJob {
            kind: JobKind::FederateDeliver,
            payload: Vec::new(),
            idempotency_key: format!("k:{n}"),
            not_before: 0,
        }
    }

    #[test]
    fn slice10_followup_batches_respect_the_per_txn_cap() {
        let (batches, last) = split_followup_batches((0..1_200).map(follow_up).collect());
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), vec![500, 500]);
        assert_eq!(last.len(), 200);
        assert_eq!(last[0].idempotency_key, "k:1000", "order is preserved");

        let (batches, last) = split_followup_batches((0..500).map(follow_up).collect());
        assert!(batches.is_empty(), "exactly one full batch commits with completion");
        assert_eq!(last.len(), 500);

        let (batches, last) = split_followup_batches((0..501).map(follow_up).collect());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 500);
        assert_eq!(last.len(), 1);

        let (batches, last) = split_followup_batches(Vec::new());
        assert!(batches.is_empty() && last.is_empty());
    }

    #[test]
    fn slice10_every_batch_is_within_the_cap() {
        let (batches, last) = split_followup_batches((0..2_500).map(follow_up).collect());
        assert!(batches.iter().all(|b| b.len() <= MAX_JOBS_PER_TXN));
        assert!(last.len() <= MAX_JOBS_PER_TXN);
        let total: usize = batches.iter().map(Vec::len).sum::<usize>() + last.len();
        assert_eq!(total, 2_500, "no follow-up dropped or duplicated");
    }
}
