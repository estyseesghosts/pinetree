//! Durable outbound ActivityPub delivery payloads and job execution
//! (Phase 2 Slice D3; Phase 2.6 Slice 10 delivery reliability).
//!
//! State mutations commit a compact [`FederateDeliverPayload`] inside a
//! [`JobKind::FederateDeliver`] job in the same storage transaction as the
//! domain state change. The background worker resolves destination inboxes
//! (preferring `shared_inbox` when available and deduplicating per inbox URL)
//! and performs signed HTTP POSTs with retry/backoff.
//!
//! Fan-out: a multi-recipient activity job never retries as a whole. Its
//! recipients are walked page by page; small fan-outs are delivered inline and
//! each failed inbox becomes a one-inbox job (`inbox_override`); large fan-outs
//! become one-inbox jobs without any inline send.

mod clips;
mod dispatch;
mod pages;
mod payload;
mod quotes;
mod recipients;

pub use self::dispatch::execute_federate_deliver;
pub use self::payload::{
    FANOUT_INLINE_CONCURRENCY, FANOUT_INLINE_MAX, FANOUT_JOB_DEADLINE_MS, FOLLOWER_PAGE_SIZE,
    FederateDeliverPayload,
};
pub use self::recipients::{
    INTERACTED_AUDIENCE_CAP, SUSPEND_SWEEP_PAGE_ROWS, collect_follower_id_page,
    collect_follower_ids, collect_known_shared_inboxes, collect_note_interacted_ids,
    collect_recipient_inboxes_for_author, collect_shared_inbox_page, fanout_is_split,
    preferred_inbox,
};
