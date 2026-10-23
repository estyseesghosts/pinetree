//! Recipient inbox resolution for outbound delivery (moved from `src/federation/outbox.rs`).
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

use std::collections::HashSet;

use crate::application::commands::AppServices;
use crate::application::ports::{JobKind, NewJob, PortError, TimelineCursor};
use crate::domain::account::Account;

use super::dispatch::per_inbox_job;
use super::payload::{FANOUT_INLINE_MAX, FOLLOWER_PAGE_SIZE, FederateDeliverPayload};

/// Prefer a remote account's `shared_inbox` when non-empty, falling back to `inbox`.
pub fn preferred_inbox(account: &Account) -> Option<String> {
    if account.is_local() {
        return None;
    }
    account
        .shared_inbox
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            account
                .inbox
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
        })
}

/// Walk a cursor-paged follower listing to its end. `fetch(cursor, limit)`
/// returns one page that starts strictly after `cursor` (`None` = the first
/// page). A short page ends the walk, so every follower is visited exactly once.
pub fn collect_follower_ids<F>(page_size: usize, mut fetch: F) -> Result<Vec<String>, PortError>
where
    F: FnMut(Option<&str>, usize) -> Result<Vec<String>, PortError>,
{
    let mut all = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = fetch(cursor.as_deref(), page_size)?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        let full = page.len() >= page_size;
        all.extend(page);
        if !full {
            break;
        }
        cursor = Some(last);
    }
    Ok(all)
}

/// Account rows scanned per shared-inbox sweep page (Wave 4 liveness):
/// one bounded `remote_shared_inboxes` round trip. Pages chain via the
/// returned cursor; dispatchers performing a sweep loop
/// [`collect_shared_inbox_page`] and fan out per page instead of
/// materializing every known inbox up front.
pub const SUSPEND_SWEEP_PAGE_ROWS: usize = 500;

/// One bounded page of an author's follower ids (Wave 4 liveness): at most
/// `max_rows` ids strictly after `after` (`None` = from the start), plus
/// the resume cursor — `Some(last)` when the page was full (the next page
/// may be empty), `None` once the table is exhausted.
pub fn collect_follower_id_page(
    services: &AppServices,
    author_id: &str,
    after: Option<&str>,
    max_rows: usize,
) -> Result<(Vec<String>, Option<String>), PortError> {
    let page = services
        .follows
        .followers_of(author_id, after, max_rows.max(1))?;
    let full = page.len() >= max_rows.max(1);
    let ids: Vec<String> = page.into_iter().map(|f| f.follower_id).collect();
    let next = if full { ids.last().cloned() } else { None };
    Ok((ids, next))
}

/// One bounded page of the known remote shared inboxes (Wave 4 liveness):
/// the deduped inboxes seen in at most `max_rows` account rows strictly
/// after `after` (`None` = from the start), plus the resume cursor for the
/// next page (`None` once the accounts table is exhausted).
///
/// Deduplication is per page: the same inbox may reappear across pages
/// when several cached accounts share it. Sweep deliveries sharing one
/// `activity_id` are receiver-deduped, so repeats converge instead of
/// duplicating effects; a dispatcher chaining pages into continuation
/// jobs must carry the account cursor (not the inbox list) forward.
pub fn collect_shared_inbox_page(
    services: &AppServices,
    after: Option<&str>,
    max_rows: usize,
) -> Result<(Vec<String>, Option<String>), PortError> {
    let (page, next) = services
        .accounts
        .remote_shared_inboxes(after, max_rows.max(1))?;
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for inbox in page {
        if seen.insert(inbox.clone()) {
            out.push(inbox);
        }
    }
    Ok((out, next))
}

/// Distinct remote shared inboxes the instance knows (Batch M WP3
/// `Delete(Person)` sweep). Upstream gathers these from the `followings`
/// rows' `followerSharedInbox`/`followeeSharedInbox` snapshots
/// (`postSuspend`/`postUnsuspend`); pinetree reads them from the cached
/// remote accounts (superset of upstream's set — a host known only through
/// a mention still holds a cached actor). Walked in bounded pages via
/// [`collect_shared_inbox_page`] and deduplicated here; the walk ends only
/// at the table end. Dispatchers that must bound per-run memory page the
/// walk themselves and chain pages via the account cursor (carried in a
/// continuation job) instead of calling this whole-set helper.
pub fn collect_known_shared_inboxes(services: &AppServices) -> Result<Vec<String>, PortError> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) =
            collect_shared_inbox_page(services, cursor.as_deref(), SUSPEND_SWEEP_PAGE_ROWS)?;
        for inbox in page {
            if seen.insert(inbox.clone()) {
                out.push(inbox);
            }
        }
        match next {
            None => break,
            Some(after) => cursor = Some(after),
        }
    }
    Ok(out)
}

/// Collect deduplicated remote inbox URLs for an author's followers plus any
/// explicitly mentioned / visible / replied-to / renoted remote accounts.
pub fn collect_recipient_inboxes_for_author(
    services: &AppServices,
    author_id: &str,
    extra_account_ids: &[String],
    include_followers: bool,
) -> Result<Vec<String>, PortError> {
    let mut seen = HashSet::new();
    let mut inboxes = Vec::new();
    let mut add_account = |acc: &Account| {
        if let Some(inbox) = preferred_inbox(acc) {
            if seen.insert(inbox.clone()) {
                inboxes.push(inbox);
            }
        }
    };

    if include_followers {
        let follower_ids = collect_follower_ids(FOLLOWER_PAGE_SIZE, |cursor, limit| {
            services
                .follows
                .followers_of(author_id, cursor, limit)
                .map(|page| {
                    page.into_iter()
                        .map(|f| f.follower_id)
                        .collect::<Vec<String>>()
                })
        })?;
        for follower_id in follower_ids {
            if let Some(acc) = services.accounts.find_by_id(&follower_id)? {
                add_account(&acc);
            }
        }
    }

    for uid in extra_account_ids {
        if let Some(acc) = services.accounts.find_by_id(uid)? {
            add_account(&acc);
        }
    }

    Ok(inboxes)
}

/// Cap on distinct interacted-user ids collected for one edit fan-out
/// (Wave 4 liveness): repliers, renoters, and reactors beyond the cap are
/// best-effort audience the `Update(Note)` skips rather than pages the
/// whole table for. The author's followers (the primary audience) are
/// unaffected — they page through the follower walk, not this cap.
pub const INTERACTED_AUDIENCE_CAP: usize = 2_000;

/// Rows read per interacted-audience page (Wave 4 liveness): replies and
/// renotes page through their indexes in small batches; reactions resolve
/// through the bounded by-note id list.
const INTERACTED_PAGE_ROWS: usize = 100;

/// Account ids that interacted with `note` (§8 edit fan-out: Sharkey
/// `postNoteEdited` delivers the `Update` to remote users who replied to,
/// renoted, or reacted to the note, plus the reply/renote authors). Reads
/// only; the caller pages deliveries through the standard split, and remote
/// filtering happens in `collect_recipient_inboxes_for_author`.
///
/// Wave 4 liveness: repliers page the reply index, renoters page the
/// renote index (never a whole-table scan), and reactors resolve
/// through the bounded by-note id list — the whole walk touches at most
/// [`INTERACTED_AUDIENCE_CAP`] ids.
pub fn collect_note_interacted_ids(
    services: &AppServices,
    note: &crate::domain::note::Note,
) -> Result<Vec<String>, PortError> {
    let mut ids: Vec<String> = Vec::new();
    let author_id = note.author_id.clone();
    let within_cap = |ids: &Vec<String>| ids.len() < INTERACTED_AUDIENCE_CAP;
    let push = |ids: &mut Vec<String>, id: &str| {
        if ids.len() >= INTERACTED_AUDIENCE_CAP {
            return;
        }
        if id != author_id.as_str() && !ids.iter().any(|known| known == id) {
            ids.push(id.to_string());
        }
    };
    if let Some(uid) = note.reply_user_id.as_deref() {
        push(&mut ids, uid);
    }
    if let Some(rid) = note.renote_id.as_deref() {
        if let Some(target) = services.notes.get_note(rid)? {
            push(&mut ids, target.author_id.as_str());
        }
    }
    // Repliers: newest-first index pages until exhausted or capped.
    let mut cursor = TimelineCursor {
        limit: INTERACTED_PAGE_ROWS,
        ..Default::default()
    };
    while within_cap(&ids) {
        let batch = services.notes.notes_by_reply(&note.id, &cursor)?;
        if batch.is_empty() {
            break;
        }
        for reply in &batch {
            push(&mut ids, reply.author_id.as_str());
        }
        if batch.len() < INTERACTED_PAGE_ROWS {
            break;
        }
        cursor.until_id = batch.last().map(|n| n.id.clone());
    }
    // Renoters: renote-index pages until exhausted or capped.
    let mut renote_cursor = TimelineCursor {
        limit: INTERACTED_PAGE_ROWS,
        ..Default::default()
    };
    while within_cap(&ids) {
        let batch = services.notes.notes_by_renote(&note.id, &renote_cursor)?;
        if batch.is_empty() {
            break;
        }
        for renote in &batch {
            push(&mut ids, renote.author_id.as_str());
        }
        if batch.len() < INTERACTED_PAGE_ROWS {
            break;
        }
        renote_cursor.until_id = batch.last().map(|n| n.id.clone());
    }
    // Reactors: bounded id list (no full reaction-row load).
    if within_cap(&ids) {
        let remaining = INTERACTED_AUDIENCE_CAP - ids.len();
        for user_id in services
            .reactions
            .reactor_ids_for_note(&note.id, remaining)?
        {
            push(&mut ids, user_id.as_str());
        }
    }
    Ok(ids)
}

/// Whether a fan-out with `inbox_count` destinations is split into per-inbox
/// jobs instead of being delivered inline.
pub fn fanout_is_split(inbox_count: usize) -> bool {
    inbox_count > FANOUT_INLINE_MAX
}

#[cfg(test)]
mod slice10_tests {
    use super::*;

    fn inbox_payload(inbox: &str) -> FederateDeliverPayload {
        FederateDeliverPayload::ActorUpdated {
            update_id: "upd-1".to_string(),
            account_id: "acc-1".to_string(),
            inbox_override: Some(inbox.to_string()),
        }
    }

    #[test]
    fn slice10_follower_walk_pages_past_ten_thousand_without_truncation() {
        // Larger than the old 10_000 cap would have allowed, and more than
        // three pages at the production page size.
        let all: Vec<String> = (0..10_250).map(|i| format!("follower-{i:06}")).collect();
        let mut calls = 0usize;
        let got = collect_follower_ids(FOLLOWER_PAGE_SIZE, |cursor, limit| {
            calls += 1;
            // Mirror the store: a page starts strictly after the cursor.
            let start = match cursor {
                None => 0,
                Some(c) => all
                    .iter()
                    .position(|id| id == c)
                    .map(|i| i + 1)
                    .expect("cursor is a known follower id"),
            };
            Ok(all.iter().skip(start).take(limit).cloned().collect())
        })
        .expect("walk");
        assert_eq!(got.len(), 10_250, "no follower truncated");
        assert_eq!(got, all, "every follower exactly once, in order");
        assert_eq!(calls, 11, "ten full pages plus one short page");
    }

    #[test]
    fn slice10_follower_walk_handles_exact_page_multiple_and_empty() {
        let all: Vec<String> = (0..2_000).map(|i| format!("f{i}")).collect();
        let got = collect_follower_ids(1_000, |cursor, limit| {
            let start = match cursor {
                None => 0,
                Some(c) => all
                    .iter()
                    .position(|id| id == c)
                    .map(|i| i + 1)
                    .unwrap_or(0),
            };
            Ok(all.iter().skip(start).take(limit).cloned().collect())
        })
        .expect("walk");
        assert_eq!(got.len(), 2_000);

        let empty = collect_follower_ids(1_000, |_, _| Ok(Vec::new())).expect("empty walk");
        assert!(empty.is_empty());
    }

    #[test]
    fn slice10_follower_walk_propagates_storage_errors() {
        let err = collect_follower_ids(10, |_, _| {
            Err(PortError::StorageUnavailable("boom".to_string()))
        })
        .unwrap_err();
        assert!(matches!(err, PortError::StorageUnavailable(_)));
    }

    #[test]
    fn slice10_fanout_split_threshold() {
        assert!(!fanout_is_split(0));
        assert!(!fanout_is_split(FANOUT_INLINE_MAX));
        assert!(fanout_is_split(FANOUT_INLINE_MAX + 1));
    }

    #[test]
    fn slice10_large_fanout_becomes_one_job_per_inbox_with_derived_keys() {
        let inboxes: Vec<String> = (0..2_500)
            .map(|i| format!("https://peer{i}.example/inbox"))
            .collect();
        let jobs: Vec<NewJob> = inboxes
            .iter()
            .map(|inbox| per_inbox_job("act-1", inbox, 42, &inbox_payload).expect("job"))
            .collect();
        assert_eq!(jobs.len(), 2_500);
        assert!(jobs.iter().all(|j| j.kind == JobKind::FederateDeliver));
        assert!(jobs.iter().all(|j| j.not_before == 42));
        assert_eq!(
            jobs[7].idempotency_key, "act-1:https://peer7.example/inbox",
            "re-running the activity job derives the same key"
        );
        let decoded: FederateDeliverPayload =
            serde_json::from_slice(&jobs[7].payload).expect("payload round-trips");
        assert_eq!(decoded, inbox_payload("https://peer7.example/inbox"));
    }
}
