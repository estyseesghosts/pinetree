//! Branched-reply parent resolution (`docs/replies.md` Slice B).
//!
//! Inbound replies whose `inReplyTo` parent is not cached keep the
//! canonical URI in `Note.reply_parent_uri` and stay hidden from ordinary
//! reads (fail-closed) until a `ReplyParentResolve` job links them. This
//! module builds those jobs, executes them for the worker, and offers the
//! opt-in bounded repair for historical notes whose parent URI was
//! discarded before Slice B.

use super::super::AppServices;

use crate::application::ports::{
    JobKind, NewJob, PortError, ReplyParentResolvePayload, TimelineCursor,
};
use crate::federation::delivery::fetch_remote_note;

/// Children linked per resolver run. A full batch enqueues a continuation
/// job for the same URI, so one huge pending set cannot wedge a worker in
/// a single long pass.
pub const REPLY_RESOLVE_BATCH: usize = 100;

/// Upper bound for one opt-in repair run (`repair_unresolved_reply_parents`).
/// The run is sequential and restartable; the bound keeps it from turning
/// into an unbounded crawl of remote origins.
pub const REPLY_REPAIR_MAX_LIMIT: usize = 5000;

/// Build the durable resolver job for one pending parent URI. The
/// idempotency key is URI-keyed (`reply-resolve:{uri}`) so pending-triggered
/// and arrival-triggered jobs for the same parent are recognizable as one
/// logical resolution; duplicates are harmless anyway because linking is
/// idempotent.
pub fn build_reply_parent_resolve_job(
    services: &AppServices,
    parent_uri: &str,
) -> Result<NewJob, PortError> {
    let uri = parent_uri.trim();
    if uri.is_empty() {
        return Err(PortError::Validation(
            "reply resolve job requires a parent URI".to_string(),
        ));
    }
    let payload = ReplyParentResolvePayload {
        parent_uri: uri.to_string(),
    };
    NewJob::new_json(
        JobKind::ReplyParentResolve,
        &payload,
        format!("reply-resolve:{uri}"),
        services.clock.now_ms(),
    )
}

/// Worker entry point for a `ReplyParentResolve` job. Returns the
/// continuation job when more children remain, `None` when done. Payload
/// problems quarantine (never retryable); an unavailable parent surfaces
/// as a storage-style error so the worker retries with backoff.
pub fn handle_reply_parent_resolve(
    services: &AppServices,
    payload: &[u8],
) -> Result<Option<NewJob>, PortError> {
    let parsed: ReplyParentResolvePayload = serde_json::from_slice(payload)
        .map_err(|e| PortError::Validation(format!("reply-resolve payload invalid: {e}")))?;
    match resolve_reply_parent(services, &parsed.parent_uri, REPLY_RESOLVE_BATCH)? {
        ReplyResolveOutcome::Done => Ok(None),
        ReplyResolveOutcome::Continue => {
            Ok(Some(build_reply_parent_resolve_job(
                services,
                parsed.parent_uri.trim(),
            )?))
        }
    }
}

/// Outcome of one bounded resolution pass over a parent URI.
pub enum ReplyResolveOutcome {
    /// No waiters, or every current waiter linked/skipped: nothing left.
    Done,
    /// A full batch was processed and more children may remain: the caller
    /// should enqueue a continuation for the same URI.
    Continue,
}

/// Resolve one pending reply-parent URI: ensure the parent is cached
/// (lookup, else SSRF-guarded fetch-and-cache), then link waiting children
/// in bounded batches through the idempotent
/// `NoteStore::link_pending_reply`.
///
/// - No waiters → `Done` (duplicate/arrival jobs converge quietly).
/// - Parent unresolvable (offline, private, gone) → `StorageUnavailable`
///   so the worker retries with backoff; the pending index entry is kept,
///   and a later parent arrival enqueues a fresh job for the same URI.
/// - Per-child deterministic conflicts (URI mismatch after alias
///   normalization, reply cycle) are logged and skipped — the child's
///   pending entry is kept, but the rest of the batch still links.
pub fn resolve_reply_parent(
    services: &AppServices,
    parent_uri: &str,
    batch_limit: usize,
) -> Result<ReplyResolveOutcome, PortError> {
    let uri = parent_uri.trim();
    if uri.is_empty() {
        return Err(PortError::Validation(
            "reply resolve requires a parent URI".to_string(),
        ));
    }
    if services
        .notes
        .pending_reply_children(uri, 1)?
        .is_empty()
    {
        return Ok(ReplyResolveOutcome::Done);
    }
    let parent_id = match crate::federation::inbox::resolve_note_target_id(services, uri)? {
        Some(id) => id,
        None => {
            return Err(PortError::StorageUnavailable(format!(
                "reply parent unavailable (retryable): {uri}"
            )));
        }
    };
    let parent = match services.notes.get_note_including_deleted(&parent_id)? {
        Some(note) => note,
        None => {
            return Err(PortError::StorageUnavailable(format!(
                "reply parent vanished after resolve: {uri}"
            )));
        }
    };
    let base = services.public_url.trim_end_matches('/');
    let canonical = parent
        .uri
        .clone()
        .unwrap_or_else(|| format!("{base}/notes/{}", parent.id));

    let batch = batch_limit.max(1);
    let mut children = services.notes.pending_reply_children(uri, batch + 1)?;
    if canonical.as_str() != uri {
        // The fetch canonicalized an alias (redirect/equiv-URI): children
        // waiting under the canonical key belong to the same pass.
        for extra in services
            .notes
            .pending_reply_children(&canonical, batch + 1)?
        {
            if !children.iter().any(|c| c.id == extra.id) {
                children.push(extra);
            }
        }
    }
    for child in children.iter().take(batch) {        // Alias normalization: the child's `inReplyTo` named the requested
        // URI while the fetched document canonicalizes to another URI for
        // the same object (HTTP redirect). Re-key the pending URI to the
        // canonical form first — `set_pending_reply_parent` only replaces
        // stale pending data, never a resolved link — then link canonically.
        // A `false` return means the child moved on concurrently: skip it.
        let child_uri = child.reply_parent_uri.clone().unwrap_or_default();
        if child_uri != canonical
            && !services
                .notes
                .set_pending_reply_parent(&child.id, &canonical, Vec::new())?
        {
            continue;
        }
        match services
            .notes
            .link_pending_reply(&child.id, &parent.id, &canonical)
        {
            Ok(_) => {}
            Err(PortError::Validation(reason)) => {
                eprintln!(
                    "reply resolve: skipping child {} (kept pending): {reason}",
                    child.id
                );
            }
            Err(e) => return Err(e),
        }
    }
    // The batch probe fetches `batch + 1`: a longer list proves more
    // children remain under either key.
    if children.len() > batch {
        // Full batch: more children may remain under either key.
        Ok(ReplyResolveOutcome::Continue)
    } else {
        Ok(ReplyResolveOutcome::Done)
    }
}

/// Repair report for one bounded opt-in run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairReport {
    /// Remote root candidates inspected.
    pub scanned: usize,
    /// Notes marked pending (each with an atomically enqueued resolver).
    pub marked: usize,
}

/// Opt-in bounded repair for historical remote notes whose original
/// `inReplyTo` was discarded before Slice B (`docs/replies.md` §6.1).
///
/// Scans at most `limit` of the newest stored notes for remote-authored
/// roots carrying a canonical `uri`, re-fetches each note's OWN origin
/// document through the SSRF-protected path, and — only when that fetched
/// source still declares a resolvable `inReplyTo` — records the pending
/// parent URI plus a resolver job atomically. Genuine roots (no declared
/// parent), unreachable origins, self-links, and already-threaded notes
/// are left untouched; nothing is ever guessed from text.
///
/// Restartable: every mark commits before the next fetch, so re-running
/// skips already-pending notes. Rate shape: strictly sequential fetches
/// with the client's existing timeouts/size/redirect caps, bounded by
/// `limit` (clamped to `REPLY_REPAIR_MAX_LIMIT`).
pub fn repair_unresolved_reply_parents(
    services: &AppServices,
    limit: usize,
) -> Result<RepairReport, PortError> {
    let limit = limit.clamp(1, REPLY_REPAIR_MAX_LIMIT);
    let candidates = services.notes.list_all_notes(&TimelineCursor {
        limit,
        ..Default::default()
    })?;
    let mut report = RepairReport {
        scanned: 0,
        marked: 0,
    };
    for note in &candidates {
        report.scanned += 1;
        // Historical repair candidates only: remotely authored, canonical
        // URI stored, never threaded and not already pending.
        if note.user_host.is_none() {
            continue;
        }
        let Some(own_uri) = note.uri.clone() else {
            continue;
        };
        if note.reply_id.is_some() || note.reply_parent_uri.is_some() {
            continue;
        }
        let parsed =
            match fetch_remote_note(services, &services.federation_http, own_uri.trim()) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
        let Some(target) = parsed.in_reply_to.as_deref().map(str::trim).filter(|s| {
            !s.is_empty() && *s != own_uri.as_str()
        }) else {
            continue;
        };
        let job = build_reply_parent_resolve_job(services, target)?;
        if services
            .notes
            .set_pending_reply_parent(&note.id, target, vec![job])?
        {
            report.marked += 1;
        }
    }
    Ok(report)
}
