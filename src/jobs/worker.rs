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
                if job.attempts + 1 >= MAX_JOB_ATTEMPTS {
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
            NotificationKind::Mention | NotificationKind::Reply | NotificationKind::Renote => {
                let kind = match notify.kind {
                    NotificationKind::Mention => MainEventKind::Mention,
                    NotificationKind::Reply => MainEventKind::Reply,
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
                return Ok(Disposition::Quarantine(format!("bad-payload: {reason}")));
            }
            Err(e) => return Err(e),
        };
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
