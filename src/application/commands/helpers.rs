use super::AppServices;

use crate::application::events::{NotifyJobPayload, StreamingEvent};
use crate::application::ports::{JobKind, NewJob, PortError};
use crate::domain::notification::NotificationKind;

impl AppServices {
    pub(super) fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }

    pub(super) fn notify_job(
        &self,
        notifiee_id: &str,
        notifier_id: &str,
        kind: NotificationKind,
        note_id: Option<String>,
        reaction: Option<String>,
        key_suffix: &str,
    ) -> Result<NewJob, PortError> {
        let payload = NotifyJobPayload {
            notifiee_id: notifiee_id.to_string(),
            notifier_id: notifier_id.to_string(),
            kind,
            note_id,
            reaction,
        };
        NewJob::new_json(
            JobKind::Notify,
            &payload,
            format!("notify:{key_suffix}:{}", self.ids.generate()),
            self.now_ms(),
        )
    }

    pub(super) fn streaming_job(
        &self,
        event: &StreamingEvent,
        key: String,
    ) -> Result<NewJob, PortError> {
        NewJob::new_json(JobKind::StreamingPublish, event, key, self.now_ms())
    }

    // ------------------------------------------------------------------
    // Shared federation operations (Phase 2 Slices D1 & D3).
    //
    // These are the inbound/outbound contracts consumed by
    // `src/federation/inbox.rs` and the job worker. Inbound handlers commit
    // state+jobs atomically through the dedup record path and never enqueue
    // further `FederateDeliver` fan-out, except the auto-Accept in
    // `handle_inbound_follow` (prevents federation loops).
    // ------------------------------------------------------------------

    /// Build a `FederateDeliver` job for `payload` with a generated
    /// idempotency key under `key_prefix`.
    pub fn federate_job(
        &self,
        payload: &crate::federation::outbox::FederateDeliverPayload,
        key_prefix: &str,
    ) -> Result<NewJob, PortError> {
        payload.into_job(
            format!("{key_prefix}:{}", self.ids.generate()),
            self.now_ms(),
        )
    }

    /// Preferred remote inbox for `account` (`shared_inbox` first, then
    /// `inbox`; `None` for local accounts).
    pub fn remote_inbox_for_account(
        &self,
        account: &crate::domain::account::Account,
    ) -> Option<String> {
        crate::federation::outbox::preferred_inbox(account)
    }
}
