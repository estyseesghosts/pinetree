//! Outbound delivery dispatch: execute jobs, fan-out, and single-inbox send
//! (moved from `src/federation/outbox.rs`).
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

mod deliver_actor;
mod deliver_collections;
mod deliver_notes;
mod deliver_polls;
mod deliver_quotes;
mod deliver_reactions;
mod deliver_social;
mod execute;
mod fanout;

pub use self::execute::execute_federate_deliver;
pub(crate) use self::fanout::{deliver_single, fan_out, per_inbox_job, settle_outcome};
