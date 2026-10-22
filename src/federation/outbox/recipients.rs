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
use crate::application::ports::{JobKind, NewJob, PortError};
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

/// Distinct remote shared inboxes the instance knows (Batch M WP3
/// `Delete(Person)` sweep). Upstream gathers these from the `followings`
/// rows' `followerSharedInbox`/`followeeSharedInbox` snapshots
/// (`postSuspend`/`postUnsuspend`); pinetree reads them from the cached
/// remote accounts (superset of upstream's set — a host known only through
/// a mention still holds a cached actor). Walked in bounded pages and
/// deduplicated here; the walk ends only at the table end.
pub fn collect_known_shared_inboxes(services: &AppServices) -> Result<Vec<String>, PortError> {
    const SCAN_PAGE: usize = 1_000;
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) = services
            .accounts
            .remote_shared_inboxes(cursor.as_deref(), SCAN_PAGE)?;
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

/// Account ids that interacted with `note` (§8 edit fan-out: Sharkey
/// `postNoteEdited` delivers the `Update` to remote users who replied to,
/// renoted, or reacted to the note, plus the reply/renote authors). Reads
/// only; the caller pages deliveries through the standard split, and remote
/// filtering happens in `collect_recipient_inboxes_for_author`.
pub(crate) fn collect_note_interacted_ids(
    services: &AppServices,
    note: &crate::domain::note::Note,
) -> Result<Vec<String>, PortError> {
    use crate::application::ports::TimelineCursor;
    let mut ids: Vec<String> = Vec::new();
    let mut push = |id: &str| {
        if id != note.author_id.as_str() && !ids.iter().any(|known| known == id) {
            ids.push(id.to_string());
        }
    };
    if let Some(uid) = note.reply_user_id.as_deref() {
        push(uid);
    }
    if let Some(rid) = note.renote_id.as_deref() {
        if let Some(target) = services.notes.get_note(rid)? {
            push(target.author_id.as_str());
        }
    }
    // Repliers: newest-first pages until exhausted.
    let mut cursor = TimelineCursor {
        limit: 100,
        ..Default::default()
    };
    loop {
        let batch = services.notes.notes_by_reply(&note.id, &cursor)?;
        if batch.is_empty() {
            break;
        }
        for reply in &batch {
            push(reply.author_id.as_str());
        }
        if batch.len() < 100 {
            break;
        }
        cursor.until_id = batch.last().map(|n| n.id.clone());
    }
    // Renoters: bounded scan (same 10k bound as the `notes/renotes` read).
    for candidate in services
        .notes
        .list_all_notes(&TimelineCursor::with_limit(10_000))?
    {
        if candidate.renote_id.as_deref() == Some(note.id.as_str()) {
            push(candidate.author_id.as_str());
        }
    }
    // Reactors.
    for reaction in services.reactions.reactions_for_note(&note.id)? {
        push(reaction.user_id.as_str());
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
