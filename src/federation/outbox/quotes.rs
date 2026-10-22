//! Quote-stamp revocation fan-out helpers (moved from `src/federation/outbox.rs`).
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

use crate::application::commands::AppServices;
use crate::application::ports::PortError;

/// Extra recipient account ids for quote-stamp revocation fan-out: mirrors
/// the `NoteCreated` audience (mentions, specified users, reply author, and
/// the quoted note's author) so every party that saw the stamp learns of
/// the revocation.
pub(crate) fn quote_note_extras(
    services: &AppServices,
    note: &crate::domain::note::Note,
) -> Result<Vec<String>, PortError> {
    let mut extras = note.mentions.clone();
    for uid in &note.visible_user_ids {
        if !extras.contains(uid) {
            extras.push(uid.clone());
        }
    }
    if let Some(ruid) = &note.reply_user_id {
        if !extras.contains(ruid) {
            extras.push(ruid.clone());
        }
    }
    if let Some(renote_id) = &note.renote_id {
        if let Some(target) = services.notes.get_note(renote_id)? {
            if !extras.contains(&target.author_id) {
                extras.push(target.author_id);
            }
        }
    }
    Ok(extras)
}
