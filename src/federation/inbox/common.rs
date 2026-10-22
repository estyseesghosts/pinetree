//! Inbox shared helpers: account/note resolution, inbound quote resolution,
//! and remote-note ingest (moved from `src/federation/inbox.rs` â€” pure move).

use serde_json::Value;

use crate::activitypub::objects::{ParsedRemoteNote, parse_note_audience};
use crate::application::commands::AppServices;
use crate::application::ports::{CreateNoteInput, PortError};
use crate::domain::account::Account;
use crate::domain::note::{Note, Visibility};
use crate::domain::quote::QuoteReference;
use crate::federation::delivery::{fetch_and_upsert_remote_actor, fetch_remote_note};
use crate::federation::{ReplyCoalescer, StampNegativeCache};
use crate::media::remote_cache::build_remote_media_fetch_job;

/// Build a [`CreateNoteInput`] from a parsed remote note and store it via the
/// inbound (deduping) path. Returns `None` on activity replay.
pub(crate) fn ingest_remote_note(
    services: &AppServices,
    activity_id: &str,
    parsed: &ParsedRemoteNote,
    author: &Account,
    fetch_media: bool,
) -> Result<Option<Note>, PortError> {
    ingest_remote_note_inner(services, activity_id, parsed, author, fetch_media, true)
}

/// Ingest a note fetched on demand (Misskey `ap/show` lookup). Runs the same
/// pipeline as an inbox `Create` (reply link or pending parent, mentions,
/// media job, quote resolution), keyed by the note URI as the activity id.
/// A note already cached under its `uri` returns `Ok(None)` (the storage
/// uniqueness rule); the caller re-reads it.
pub fn ingest_fetched_remote_note(
    services: &AppServices,
    parsed: &ParsedRemoteNote,
    author: &Account,
) -> Result<Option<Note>, PortError> {
    ingest_remote_note(services, &parsed.uri, parsed, author, true)
}

/// Inner ingestion with a quote-target resolution depth guard: a fetched
/// quote target is cached via this same path with `resolve_quote_target =
/// false`, so a malicious chain of quotes cannot recurse without bound.
/// The nested quote URI is still retained (unresolved) on the cached copy.
pub(crate) fn ingest_remote_note_inner(
    services: &AppServices,
    activity_id: &str,
    parsed: &ParsedRemoteNote,
    author: &Account,
    fetch_media: bool,
    resolve_quote_target: bool,
) -> Result<Option<Note>, PortError> {
    let base = services.public_url.trim_end_matches('/').to_string();
    let author_uri = author.actor_url(&base);
    let (visibility, recipient_uris) = parse_note_audience(
        &parsed.to,
        &parsed.cc,
        Some(&format!("{author_uri}/followers")),
    );

    // Slice B (`docs/replies.md` §6): an `inReplyTo` that resolves to a
    // cached note links immediately; a present-but-unresolved one is
    // preserved as a pending parent URI (never flattened into a root).
    // Blank values carry no relationship.
    let (reply_to, reply_parent_uri) = match parsed.in_reply_to.as_deref() {
        Some(uri) => {
            let target = uri.trim();
            if target.is_empty() {
                (None, None)
            } else {
                match find_note_by_uri(services, target)? {
                    Some(note) => (Some(note.id), None),
                    None => (None, Some(target.to_string())),
                }
            }
        }
        None => (None, None),
    };

    // Specified-visibility recipients, best-effort (unresolvable peers are
    // skipped rather than failing the whole note).
    let mut visible_users = Vec::new();
    if visibility == Visibility::Specified {
        for uri in &recipient_uris {
            if let Ok(account) = resolve_remote_account(services, uri) {
                if !visible_users.contains(&account.id) {
                    visible_users.push(account.id);
                }
            }
        }
    }

    // Mentions, best-effort.
    let mut mention_ids = Vec::new();
    for uri in &parsed.mentioned_actor_uris {
        if let Ok(account) = resolve_remote_account(services, uri) {
            if !mention_ids.contains(&account.id) {
                mention_ids.push(account.id);
            }
        }
    }

    // Attachments (slice 7, decision D-d): never fetched inline — the inbox
    // request must not stay open for N×fetch. Instead one durable
    // `RemoteMediaFetch` job is enqueued atomically with the note below;
    // the worker fetches and attaches off-request. The final
    // `record_note_activity` stays the atomic authority, so a replay (which
    // returns `Ok(None)` there) enqueues nothing: zero fetches, zero rows.
    let extra_jobs = if fetch_media {
        match build_remote_media_fetch_job(services, activity_id, &parsed.attachments)? {
            Some(job) => vec![job],
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    let mut extra_jobs = extra_jobs;
    // A pending parent gets its durable resolver job in the same commit as
    // the note below (a replay enqueues nothing — `record_note_activity`
    // stays the atomic authority). No unbounded network I/O happens here;
    // the worker fetches off-request. WP5 coalescing: N waiters for one
    // parent share a single trigger job inside the coalescing window (the
    // pending-index entries are still recorded per note — the job only
    // triggers the worker pass).
    if let Some(pending_uri) = reply_parent_uri.as_deref() {
        if ReplyCoalescer::should_enqueue(pending_uri, services.clock.now_ms()) {
            extra_jobs.push(
                crate::application::commands::inbound::replies::build_reply_parent_resolve_job(
                    services,
                    pending_uri,
                )?,
            );
        }
    }
    // Arrival-triggered resolution: a newly stored canonical URI may unblock
    // children already waiting on it. One cheap pending-index probe per
    // inbound note; the resolver job does the bounded batch work. This path
    // stays UNCONDITIONAL (not coalesced): the parent is already cached, so
    // the trigger links immediately with zero network I/O, and each parent
    // URI arrives once — there is no N-waiter fan-out to coalesce here.
    let own_uri = parsed.uri.trim();
    if !own_uri.is_empty()
        && !services
            .notes
            .pending_reply_children(own_uri, 1)?
            .is_empty()
    {
        extra_jobs.push(
            crate::application::commands::inbound::replies::build_reply_parent_resolve_job(
                services, own_uri,
            )?,
        );
    }
    let file_ids = Vec::new();

    // Structured quote (any of the five carriers): resolve the target to a
    // local note id when possible, and evaluate authorization evidence
    // gathered BEFORE the storage transaction. Unresolvable targets keep
    // their URI (`renote_of: None`, quote retained as unapproved/unknown).
    let (renote_of, quote) = if resolve_quote_target {
        resolve_inbound_quote(services, parsed, &author_uri)?
    } else if let Some(target_uri) = parsed.quote_target_uri.clone() {
        (None, Some(QuoteReference::unapproved(target_uri)))
    } else {
        (None, None)
    };

    // Inbound polls (§4): attach only when the remote Question meets the
    // storage invariants (2..=10 choices, each 1..=50 chars); anything else
    // degrades to a poll-less note rather than rejecting the whole post
    // (Misskey stores whatever arrives — DIVERGENCE, documented: pinetree
    // keeps one invariant for local and remote polls).
    let inbound_poll = parsed.poll.as_ref().filter(|p| {
        p.choices.len() >= 2
            && p.choices.len() <= 10
            && p.choices.iter().all(|c| {
                let n = c.chars().count();
                n >= 1 && n <= 50
            })
    });
    let input = CreateNoteInput {
        author_id: author.id.clone(),
        text: parsed.text.clone(),
        cw: parsed.cw.clone(),
        visibility,
        local_only: false,
        reply_to,
        reply_parent_uri,
        renote_of,
        file_ids,
        visible_users,
        reaction_acceptance: None,
        no_extract_mentions: false,
        no_extract_hashtags: false,
        no_extract_emojis: false,
        has_poll: inbound_poll.is_some(),
        poll: inbound_poll.map(|p| crate::domain::poll::PollInput {
            choices: p.choices.clone(),
            multiple: p.multiple,
            expires_at_ms: p.expires_at_ms,
        }),
        // Seed the cached tallies the origin reported (same transaction).
        poll_initial_votes: inbound_poll.map(|p| p.votes.clone()),
        has_channel: false,
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        uri: Some(parsed.uri.clone()),
        uri_base: String::new(),
        url: parsed.url.clone(),
        extra_mentions: mention_ids,
        extra_tags: parsed.hashtags.clone(),
        quote,
    };
    // WP8b remote-emoji metadata ingest (no fetch): after the drop checks
    // above (blocked-host/suspended paths return before this point), around
    // the note commit. Idempotent upserts — a replay re-upserts harmlessly;
    // only storage faults propagate (malformed rows skip inside).
    if !parsed.emojis.is_empty() {
        if let Some(host) = author.host.as_deref() {
            services.ingest_remote_emoji_tags(host, &parsed.emojis)?;
        }
    }
    services.handle_inbound_create_note(activity_id, input, extra_jobs)
}

/// Resolve an inbound quote target and evaluate its authorization evidence.
///
/// Returns `(renote_of, quote)`: the resolved local target id (if any) plus
/// the domain quote record. Recognition never depends on authorization
/// metadata — legacy-only payloads yield `(id?, unapproved)`. Stamp
/// verification happens here (pre-transaction network I/O); transport
/// failures map to `Unknown` (transient, never rejection), invalid stamp
/// content maps to `Unapproved` (invalid evidence, never `Rejected` — only
/// an explicit `Reject` activity sets `Rejected`).
pub(crate) fn resolve_inbound_quote(
    services: &AppServices,
    parsed: &ParsedRemoteNote,
    quoter_actor_uri: &str,
) -> Result<(Option<String>, Option<QuoteReference>), PortError> {
    use crate::domain::quote::QuoteAuthorizationState::{Approved, ApprovedByException, Unknown};

    let Some(target_uri) = parsed.quote_target_uri.clone() else {
        return Ok((None, None));
    };
    if parsed.quote_has_conflict {
        // Aliases disagreed; canonical `quote` won. Recorded here (server
        // log) since the domain record carries a single target.
        eprintln!(
            "quote carriers conflict on {}: kept {target_uri}",
            parsed.uri
        );
    }
    // Best-effort resolve + cache (origin-checked, SSRF-guarded). Offline
    // or private targets simply stay unresolved; the URI is retained.
    let target_id = resolve_quote_target_id(services, &target_uri)?;
    let mut quote = QuoteReference::unapproved(target_uri.clone());
    quote.target_note_id = target_id.clone();

    // The target author's actor URI + the target's structured mentions feed
    // the FEP-044f exception check (structured identity, never substrings).
    let (target_author_uri, target_mention_uris) = match target_id.as_deref() {
        Some(tid) => match services.notes.get_note(tid)? {
            Some(target) => {
                let base = services.public_url.trim_end_matches('/');
                let author_uri = match services.accounts.find_by_id(&target.author_id)? {
                    Some(acc) => acc.actor_url(base),
                    None => String::new(),
                };
                let mut mentions = Vec::new();
                for mid in &target.mentions {
                    if let Ok(Some(acc)) = services.accounts.find_by_id(mid) {
                        let uri = acc.actor_url(base);
                        if !uri.is_empty() && !mentions.contains(&uri) {
                            mentions.push(uri);
                        }
                    }
                }
                (author_uri, mentions)
            }
            None => (String::new(), Vec::new()),
        },
        None => (String::new(), Vec::new()),
    };

    if !target_author_uri.is_empty()
        && crate::activitypub::quotes::approval_exception(
            quoter_actor_uri,
            &target_author_uri,
            &target_mention_uris,
        )
    {
        quote.authorization = ApprovedByException;
        quote.last_verified_at = Some(services.clock.now_ms());
    } else if let Some(stamp_iri) = parsed.quote_authorization_uri.clone() {
        if target_author_uri.is_empty() {
            // A stamp was offered but its authority cannot be established
            // without the target author: inconclusive, not rejection.
            quote.authorization = Unknown;
        } else {
            // WP5 negative stamp dedup: a recently failed stamp IRI is not
            // re-fetched until its backoff elapses (transport failures map
            // to `Unknown`, never to rejection — same as a fresh failure).
            let now_ms = services.clock.now_ms();
            if !StampNegativeCache::should_fetch(&stamp_iri, now_ms) {
                quote.authorization = Unknown;
            } else {
                let client = &services.federation_http;
                match crate::federation::delivery::fetch_quote_stamp_doc(
                    services, client, &stamp_iri,
                ) {
                    Ok(doc) => {
                        match crate::activitypub::quotes::verify_stamp_for_quote(
                            &doc,
                            &stamp_iri,
                            &parsed.uri,
                            &target_uri,
                            &target_author_uri,
                        ) {
                            Ok(()) => {
                                quote.authorization = Approved;
                                quote.authorization_uri = Some(stamp_iri.clone());
                                quote.last_verified_at = Some(now_ms);
                                StampNegativeCache::record(&stamp_iri, now_ms, true);
                            }
                            Err(_) => {
                                StampNegativeCache::record(&stamp_iri, now_ms, false);
                            }
                        }
                    }
                    Err(_) => {
                        StampNegativeCache::record(&stamp_iri, now_ms, false);
                        quote.authorization = Unknown;
                    }
                }
            }
        }
    }
    if quote.validate().is_err() {
        quote = QuoteReference::unapproved(target_uri);
        quote.target_note_id = target_id.clone();
    }
    Ok((target_id, Some(quote)))
}

/// Resolve a liked/renoted/replied-to target note id: local lookup first,
/// otherwise fetch the remote note (origin-checked) and cache it as a
/// remote note. Returns `None` when the target cannot be resolved.
/// Shared with the Slice B reply resolver (`inbound::replies`), which uses
/// the same lookup-or-fetch-and-cache path for pending reply parents.
pub(crate) fn resolve_note_target_id(
    services: &AppServices,
    target_uri: &str,
) -> Result<Option<String>, PortError> {
    if let Some(note) = find_note_by_uri(services, target_uri)? {
        return Ok(Some(note.id));
    }
    let client = &services.federation_http;
    let parsed = match fetch_remote_note(services, client, target_uri) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(None),
    };
    let author = match resolve_remote_account(services, &parsed.attributed_to) {
        Ok(author) => author,
        Err(_) => return Ok(None),
    };
    // Cache under the note URI as the dedup key so concurrent fetches converge.
    match ingest_remote_note(services, &parsed.uri, &parsed, &author, true)? {
        Some(note) => Ok(Some(note.id)),
        None => Ok(find_note_by_uri(services, target_uri)?.map(|note| note.id)),
    }
}

/// Resolve a quote target the same way, except the cached copy keeps any
/// nested quote as an unresolved URI (one-level depth guard against
/// malicious quote chains; see `ingest_remote_note_inner`).
pub(crate) fn resolve_quote_target_id(
    services: &AppServices,
    target_uri: &str,
) -> Result<Option<String>, PortError> {
    if let Some(note) = find_note_by_uri(services, target_uri)? {
        return Ok(Some(note.id));
    }
    let client = &services.federation_http;
    let parsed = match fetch_remote_note(services, client, target_uri) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(None),
    };
    let author = match resolve_remote_account(services, &parsed.attributed_to) {
        Ok(author) => author,
        Err(_) => return Ok(None),
    };
    match ingest_remote_note_inner(services, &parsed.uri, &parsed, &author, true, false)? {
        Some(note) => Ok(Some(note.id)),
        None => Ok(find_note_by_uri(services, target_uri)?.map(|note| note.id)),
    }
}

/// Find a cached account by actor URI, fetching and upserting the remote actor
/// document when unknown.
pub(crate) fn resolve_remote_account(
    services: &AppServices,
    actor_uri: &str,
) -> Result<Account, PortError> {
    if let Some(account) = services.accounts.find_by_uri(actor_uri)? {
        return Ok(account);
    }
    let client = &services.federation_http;
    fetch_and_upsert_remote_actor(services, client, actor_uri).map(|(account, _)| account)
}

/// Find a local account by actor URI: direct URI lookup, falling back to the
/// `{public_url}/users/{id}` form. Returns `None` when unknown (never fetches).
pub(crate) fn resolve_local_account(
    services: &AppServices,
    uri: &str,
) -> Result<Option<Account>, PortError> {
    // A cached remote copy may carry a uri equal to a local actor URL (e.g.
    // after a self-lookup); only a local account satisfies this resolver.
    if let Some(account) = services.accounts.find_by_uri(uri)? {
        if account.is_local() {
            return Ok(Some(account));
        }
    }
    let base = services.public_url.trim_end_matches('/');
    let prefix = format!("{base}/users/");
    if let Some(rest) = uri.strip_prefix(&prefix) {
        let id = rest.split(['/', '?', '#']).next().unwrap_or("").trim();
        if !id.is_empty() {
            if let Some(account) = services.accounts.find_by_id(id)? {
                if account.is_local() {
                    return Ok(Some(account));
                }
            }
        }
    }
    Ok(None)
}

/// Find a cached note by URI, falling back to the `{public_url}/notes/{id}`
/// form for local notes (which may carry no stored `uri`).
pub(crate) fn find_note_by_uri(
    services: &AppServices,
    uri: &str,
) -> Result<Option<Note>, PortError> {
    if let Some(note) = services.notes.find_by_uri(uri)? {
        return Ok(Some(note));
    }
    let base = services.public_url.trim_end_matches('/');
    let prefix = format!("{base}/notes/");
    if let Some(rest) = uri.strip_prefix(&prefix) {
        let id = rest.split(['/', '?', '#']).next().unwrap_or("").trim();
        if !id.is_empty() {
            return services.notes.get_note(id);
        }
    }
    Ok(None)
}

pub(crate) fn non_empty_str(value: Option<&Value>) -> Option<String> {
    let s = value?.as_str()?.trim();
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Map full FEP type IRIs to their short names (some servers serialize
/// `"type": "https://w3id.org/fep/044f#QuoteRequest"`). Unknown types pass
/// through unchanged (dispatch ignores them).
pub(crate) fn canonical_activity_type(t: &str) -> String {
    let t = t.trim();
    match t {
        crate::activitypub::quotes::FEP044F_QUOTE_REQUEST_TYPE => "QuoteRequest".to_string(),
        crate::activitypub::quotes::FEP044F_QUOTE_AUTHORIZATION_TYPE => {
            "QuoteAuthorization".to_string()
        }
        _ => t.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Responses (empty or short bodies; never expose internals)
// ---------------------------------------------------------------------------
