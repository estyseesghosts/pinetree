//! ActivityPub inbox handlers: `POST /inbox` (shared) and
//! `POST /users/{id}/inbox` (personal) (Phase 2 Slice D2).
//!
//! Every request is HTTP-Signature authenticated before dispatch; all state
//! changes go through the `AppServices::handle_inbound_*` operations, which
//! deduplicate on the activity `id` atomically (replays → `202 Accepted`).
//! Federation input is untrusted: bodies are capped at 64 KiB and every
//! remote fetch goes through the shared [`SafeHttpClient`](crate::federation::delivery::SafeHttpClient)
//! (SSRF guards). Error bodies are empty or short strings; public URLs are
//! built from `public_url`/actor URIs only, never socket peers.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;

use crate::activitypub::actor::{parse_remote_actor, parse_url_host};
use crate::activitypub::objects::{
    ParsedRemoteNote, extract_like_reaction, extract_object_uri, extract_uri_list,
    parse_note_audience, parse_question_poll, parse_remote_note, sanitize_html_to_mfm,
};
use crate::activitypub::signatures::{parse_signature_header, verify_http_signature};
use crate::application::commands::AppServices;
use crate::application::ports::{Clock, CreateNoteInput, PortError, TimelineCursor, UpdateAccountPatch};
use crate::domain::account::Account;
use crate::domain::poll::PollInput;
use crate::domain::note::{Note, Visibility};
use crate::domain::quote::QuoteReference;
use crate::federation::delivery::{
    MAX_AP_JSON_BYTES, fetch_and_upsert_remote_actor, fetch_remote_note, resolve_remote_public_key,
};
use crate::media::remote_cache::build_remote_media_fetch_job;

/// Maximum accepted inbox POST body (64 KiB, same cap as outbound AP JSON).
pub const MAX_INBOX_BODY_BYTES: usize = MAX_AP_JSON_BYTES;
/// Maximum allowed `Date` header skew in either direction (5 minutes).
pub const MAX_CLOCK_SKEW_SECS: i64 = 300;

// ---------------------------------------------------------------------------
// Axum handlers
// ---------------------------------------------------------------------------

/// `POST /inbox` — shared inbox.
pub async fn inbox_handler(
    State(services): State<Arc<AppServices>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    run_blocking(move || process_inbox(&services, None, headers, body)).await
}

/// `POST /users/{id}/inbox` — personal inbox.
pub async fn user_inbox_handler(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    run_blocking(move || process_inbox(&services, Some(id.as_str()), headers, body)).await
}

/// Inbox processing does blocking storage and key-fetch I/O; keep it off the
/// async worker threads.
async fn run_blocking<F: FnOnce() -> Response + Send + 'static>(f: F) -> Response {
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|_| internal_error())
}

fn process_inbox(
    services: &AppServices,
    user_id: Option<&str>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    crate::fed_debug!(
        "in  recv path={} bytes={}",
        match user_id {
            Some(uid) => format!("/users/{uid}/inbox"),
            None => "/inbox".to_string(),
        },
        body.len()
    );
    if body.len() > MAX_INBOX_BODY_BYTES {
        crate::fed_debug!("in  reject reason=payload-too-large bytes={}", body.len());
        return payload_too_large();
    }
    let activity: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            crate::fed_debug!("in  reject reason=bad-json");
            return bad_request();
        }
    };
    let activity_type = match non_empty_str(activity.get("type")) {
        Some(t) => canonical_activity_type(&t),
        None => {
            crate::fed_debug!("in  reject reason=missing-type");
            return bad_request();
        }
    };
    let activity_id = match non_empty_str(activity.get("id")) {
        Some(id) => id,
        None => {
            crate::fed_debug!("in  reject reason=missing-id type={activity_type}");
            return bad_request();
        }
    };

    // Personal inbox: the addressed account must exist and be local.
    // (Lenient addressing: continue to shared processing regardless of
    // whether the activity targets this account.)
    if let Some(uid) = user_id {
        match services.accounts.find_by_id(uid) {
            Ok(Some(acc)) if acc.is_local() => {}
            Ok(_) => {
                crate::fed_debug!("in  reject reason=personal-inbox-not-local user={uid:?}");
                return not_found();
            }
            Err(_) => return internal_error(),
        }
    }

    let path_and_query = match user_id {
        Some(uid) => format!("/users/{uid}/inbox"),
        None => "/inbox".to_string(),
    };
    let signer_actor = match authenticate(services, &path_and_query, &headers, &body) {
        Ok(owner) => owner,
        Err(resp) => {
            crate::fed_debug!("in  reject reason=signature type={activity_type} id={activity_id:?}");
            return resp;
        }
    };

    // Actor authority: the activity actor must be the key owner.
    let actor_uri = match non_empty_str(activity.get("actor")) {
        Some(a) => a,
        None => {
            crate::fed_debug!("in  reject reason=missing-actor type={activity_type} id={activity_id:?}");
            return unauthorized();
        }
    };
    if actor_uri != signer_actor {
        crate::fed_debug!(
            "in  reject reason=actor-not-signer type={activity_type} id={activity_id:?} actor={actor_uri:?} signer={signer_actor:?}"
        );
        return unauthorized();
    }

    // Object authority for types whose objects the signer claims to own.
    if !check_object_authority(&activity_type, &activity, &signer_actor) {
        crate::fed_debug!(
            "in  reject reason=object-authority type={activity_type} id={activity_id:?} actor={actor_uri:?}"
        );
        return unauthorized();
    }

    let resp = dispatch(services, &activity, &activity_id, &activity_type, &actor_uri);
    crate::fed_debug!(
        "in  done type={activity_type} id={activity_id:?} actor={actor_uri:?} status={}",
        resp.status().as_u16()
    );
    resp
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

/// Verify the HTTP signature, resolving the signer `keyId` against the cached
/// remote public key (refreshing once from the network on failure).
/// Returns the key owner's actor URI on success (from the cache, or from the
/// freshly fetched document after the same parse checks are re-applied by
/// the fetch-and-upsert path). Callers must compare `activity.actor` against
/// this owner, never against a string derived from the keyId (Slice 2, C1).
fn authenticate(
    services: &AppServices,
    path_and_query: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<String, Response> {
    // Parse the Signature header first to learn the keyId (no verification yet).
    let raw = headers
        .get("signature")
        .and_then(|v| v.to_str().ok())
        .or_else(|| headers.get("authorization").and_then(|v| v.to_str().ok()));
    let Some(raw) = raw else {
        crate::fed_debug!("in  reject reason=no-signature-header");
        return Err(unauthorized());
    };
    let parsed = match parse_signature_header(raw) {
        Ok(p) => p,
        Err(_) => {
            crate::fed_debug!("in  reject reason=signature-header-unparseable");
            return Err(unauthorized());
        }
    };
    let key_id = parsed.key_id.clone();

    let now_ms = services.clock.now_ms();
    let client = &services.federation_http;
    let verify = |pem: &str| {
        verify_http_signature(
            "POST",
            path_and_query,
            headers,
            Some(body),
            pem,
            Some(now_ms),
            MAX_CLOCK_SKEW_SECS,
        )
    };

    match resolve_remote_public_key(services, client, &key_id, false) {
        Ok((owner, pem)) => match verify(&pem) {
            Ok(_) => Ok(owner),
            Err(_) => {
                // Stale cached key (e.g. rotated): refresh once and re-verify.
                crate::fed_debug!("in  signature-retry keyId={key_id:?} (cached key failed; refreshing)");
                match resolve_remote_public_key(services, client, &key_id, true) {
                    Ok((fresh_owner, fresh_pem)) => match verify(&fresh_pem) {
                        Ok(_) => Ok(fresh_owner),
                        Err(detail) => {
                            crate::fed_debug!("in  reject reason=signature-invalid keyId={key_id:?} detail={detail:?}");
                            Err(unauthorized())
                        }
                    },
                    Err(_) => {
                        crate::fed_debug!("in  reject reason=key-unresolvable keyId={key_id:?}");
                        Err(unauthorized())
                    }
                }
            }
        },
        Err(_) => {
            crate::fed_debug!("in  reject reason=key-unresolvable keyId={key_id:?}");
            Err(unauthorized())
        }
    }
}

// ---------------------------------------------------------------------------
// Authority checks
// ---------------------------------------------------------------------------

fn signer_host(signer_actor: &str) -> Option<String> {
    parse_url_host(signer_actor).map(|(_, host)| host)
}

fn host_of(uri: &str) -> Option<String> {
    parse_url_host(uri).map(|(_, host)| host)
}

/// Object-authority gate. `Follow`/`Accept`/`Reject`/`Undo` objects are the
/// local followee, so actor == signer is sufficient. `Announce`/`Like`
/// targets are routinely third-party or local notes, so no object-host check
/// applies there either; authority is established via actor == signer plus
/// origin-checked target resolution at dispatch. `Create` objects are
/// content the signer claims to own and must live on the signer's host;
/// `Update` objects must be the signer's own actor document. `Delete` and
/// `QuoteRequest` pass through: dispatch re-validates (note deletes keep
/// their author/host check; stamp deletes enforce stamp-author authority;
/// quote requests validate instrument ownership) — a blanket host match
/// here would 401 legitimate revocation forwards and quote requests.
fn check_object_authority(
    activity_type: &str,
    activity: &Value,
    signer_actor: &str,
) -> bool {
    match activity_type {
        "Follow" | "Accept" | "Reject" | "Undo" | "Announce" | "Like" | "Delete" | "Block"
        | "QuoteRequest" => true,
        "Create" => {
            let Some(object_uri) = extract_object_uri(activity.get("object")) else {
                return true; // dispatch ignores activities without an object
            };
            match (host_of(&object_uri), signer_host(signer_actor)) {
                (Some(object_host), Some(signer)) => object_host == signer,
                _ => false,
            }
        }
        "Update" => {
            match activity.get("object") {
                // `Update(Question)` is handled by the §4 poll branch and
                // `Update(Note|Article|Page)` is deferred to §8: dispatch
                // re-validates, so the gate passes both through.
                Some(Value::Object(map))
                    if matches!(
                        map.get("type").and_then(Value::as_str).unwrap_or(""),
                        "Note" | "Article" | "Page" | "Question"
                    ) =>
                {
                    true
                }
                Some(_) => {
                    let Some(object_id) = extract_object_uri(activity.get("object")) else {
                        return true; // dispatch ignores activities without an object
                    };
                    object_id == signer_actor
                }
                None => true,
            }
        }
        // Unknown types are authenticated but ignored at dispatch.
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

fn dispatch(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    activity_type: &str,
    actor_uri: &str,
) -> Response {
    let outcome: Result<Response, PortError> = match activity_type {
        "Follow" => handle_follow(services, activity, activity_id, actor_uri),
        "Accept" => handle_accept(services, activity, activity_id, actor_uri),
        "Reject" => handle_reject(services, activity, activity_id, actor_uri),
        "Undo" => handle_undo(services, activity, activity_id, actor_uri),
        "Block" => handle_block(services, activity, activity_id, actor_uri),
        "Create" => handle_create(services, activity, activity_id, actor_uri),
        "Announce" => handle_announce(services, activity, activity_id, actor_uri),
        "Like" => handle_like(services, activity, activity_id, actor_uri),
        // Akkoma/Pleroma-family emoji reacts (`EmojiReact{content}`; see
        // `docs/misskey-parity-plan.md` §6): same per-(user,note) row as a
        // `Like`, extracted from `content` by the shared helper.
        "EmojiReact" => handle_like(services, activity, activity_id, actor_uri),
        "Delete" => handle_delete(services, activity, activity_id, actor_uri),
        "Update" => handle_update(services, activity, activity_id, actor_uri),
        "QuoteRequest" => handle_quote_request(services, activity, activity_id, actor_uri),
        _ => {
            crate::fed_debug!("in  ignore type={activity_type} id={activity_id:?} reason=unsupported-type");
            Ok(accepted())
        }
    };
    outcome.unwrap_or_else(|_| internal_error())
}

fn handle_follow(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(object_uri) = non_empty_str(activity.get("object")) else {
        return Ok(accepted());
    };
    let follower = resolve_remote_account(services, actor_uri)?;
    if services.inbound_host_blocked(&follower.id) {
        return Ok(accepted());
    }
    let Some(followee) = resolve_local_account(services, &object_uri)? else {
        return Ok(accepted());
    };
    if !followee.is_local() {
        return Ok(accepted());
    }
    services.handle_inbound_follow(activity_id, &follower.id, &followee.id, false)?;
    Ok(accepted())
}

/// Inbound `Block` (Misskey `ApInboxService.block()` parity): the remote
/// actor blocks a local user. Unknown or remote objects are skipped.
fn handle_block(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(object_uri) = non_empty_str(activity.get("object")) else {
        return Ok(accepted());
    };
    let blocker = resolve_remote_account(services, actor_uri)?;
    if services.inbound_host_blocked(&blocker.id) {
        return Ok(accepted());
    }
    let Some(blockee) = resolve_local_account(services, &object_uri)? else {
        return Ok(accepted());
    };
    if !blockee.is_local() {
        return Ok(accepted());
    }
    services.handle_inbound_block(activity_id, &blocker.id, &blockee.id)?;
    Ok(accepted())
}

/// `Accept` routes by object type: `Follow` (existing follow path) or
/// `QuoteRequest` (quote authorization grant). Anything else is ignored.
/// A bare `Accept` without a stamp `result` never approves a quote. A
/// legitimate `Accept(Follow)` carrying a `result` field must never be
/// misrouted: string objects resolve via the quote-request index first.
fn handle_accept(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(obj) = activity.get("object") else {
        return Ok(accepted());
    };
    if let Some(map) = obj.as_object() {
        if is_quote_request_shape(map) {
            return handle_quote_accept(services, activity, activity_id, actor_uri);
        }
        // Embedded Follow object stays on the follow path even with `result`.
        return handle_follow_response(services, activity, activity_id, actor_uri, true);
    }
    if let Some(iri) = obj.as_str().map(str::trim).filter(|s| !s.is_empty()) {
        // Bare-IRI Accept: quote path only when the IRI is a known
        // QuoteRequest AND a stamp `result` is present; otherwise follow.
        if activity.get("result").is_some()
            && services.notes.find_note_by_quote_request(iri)?.is_some()
        {
            return handle_quote_accept(services, activity, activity_id, actor_uri);
        }
    }
    handle_follow_response(services, activity, activity_id, actor_uri, true)
}

/// `Reject` routes the same way. Only an explicit `Reject` of our matching
/// `QuoteRequest` sets `Rejected`; absent stamps never do. A bare-IRI
/// object is looked up as a request id first (normal AP shape), so quote
/// rejects are never lost to the follow path.
fn handle_reject(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(obj) = activity.get("object") else {
        return Ok(accepted());
    };
    if let Some(map) = obj.as_object() {
        if is_quote_request_shape(map) {
            return handle_quote_reject(services, activity, activity_id, actor_uri);
        }
    } else if let Some(iri) = obj.as_str().map(str::trim).filter(|s| !s.is_empty()) {
        if services.notes.find_note_by_quote_request(iri)?.is_some() {
            return handle_quote_reject(services, activity, activity_id, actor_uri);
        }
    }
    handle_follow_response(services, activity, activity_id, actor_uri, false)
}

/// True when an embedded activity object is a `QuoteRequest` (short type
/// name or the full FEP type IRI).
fn is_quote_request_shape(map: &serde_json::Map<String, Value>) -> bool {
    let obj_type = map.get("type").and_then(Value::as_str).unwrap_or("");
    obj_type == "QuoteRequest"
        || obj_type == crate::activitypub::quotes::FEP044F_QUOTE_REQUEST_TYPE
}

fn handle_follow_response(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
    accept: bool,
) -> Result<Response, PortError> {
    let Some(inner) = activity.get("object").and_then(Value::as_object) else {
        return Ok(accepted());
    };
    if inner.get("type").and_then(Value::as_str).unwrap_or("") != "Follow" {
        return Ok(accepted());
    }
    let (Some(inner_actor), Some(inner_object)) = (
        non_empty_str(inner.get("actor")),
        non_empty_str(inner.get("object")),
    ) else {
        return Ok(accepted());
    };
    // The signer must be the followee answering its own follow.
    if inner_object != actor_uri {
        return Ok(unauthorized());
    }
    let Some(local_follower) = resolve_local_account(services, &inner_actor)? else {
        return Ok(accepted());
    };
    if !local_follower.is_local() {
        return Ok(accepted());
    }
    let remote_followee = resolve_remote_account(services, &inner_object)?;
    if accept {
        services.handle_inbound_accept_follow(
            activity_id,
            &local_follower.id,
            &remote_followee.id,
            false,
        )?;
    } else {
        services.handle_inbound_reject_follow(
            activity_id,
            &local_follower.id,
            &remote_followee.id,
            false,
        )?;
    }
    Ok(accepted())
}

/// Inbound FEP-044f `QuoteRequest` for a local post: validate the instrument
/// (ownership + same target), then decide by policy (auto-approve
/// public/unlisted + exceptions, reject tighter) with an atomic
/// stamp+`Accept` / `Reject` commit. Replays are answered without new work.
fn handle_quote_request(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    // Read-only replay gate BEFORE any remote fetch (same benign-race
    // reasoning as `handle_create`).
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    let Some(quoted_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    // The instrument is the quoter's quote Note: inline object or IRI.
    let (instrument_uri, instrument) = match activity.get("instrument") {
        Some(Value::String(uri)) => {
            let uri = uri.trim();
            if uri.is_empty() {
                return Ok(accepted());
            }
            let client = &services.federation_http;
            match fetch_remote_note(services, client, uri) {
                Ok(parsed) => (parsed.uri.clone(), parsed),
                Err(_) => return Ok(accepted()),
            }
        }
        Some(obj @ Value::Object(_)) => match parse_remote_note(obj) {
            Ok(parsed) => (parsed.uri.clone(), parsed),
            Err(_) => return Ok(accepted()),
        },
        _ => return Ok(accepted()),
    };
    // Actor authority: the instrument must be the signer's own Note and must
    // quote exactly the targeted post.
    if instrument.attributed_to != actor_uri {
        return Ok(unauthorized());
    }
    let Some(instrument_target) = instrument.quote_target_uri.clone() else {
        return Ok(accepted());
    };
    if instrument_target != quoted_uri {
        return Ok(accepted());
    }
    let quoter = resolve_remote_account(services, actor_uri)?;
    // The quoted post must be a live local note; only its own server decides.
    let Some(target) = find_note_by_uri(services, &quoted_uri)? else {
        return Ok(accepted());
    };
    let target_author = match services.accounts.find_by_id(&target.author_id)? {
        Some(acc) => acc,
        None => return Ok(accepted()),
    };
    if !target_author.is_local() {
        return Ok(accepted());
    }
    // Enforce the target's actual visibility before deciding: a request for
    // a non-public post from an unauthorized party is rejected, never
    // answered with data. (Policy details live in the application handler;
    // the `Reject` itself reveals nothing beyond the request's own URIs.)
    let inbox = services.remote_inbox_for_account(&quoter);
    let _ = services.handle_inbound_quote_request(
        activity_id,
        &quoter.id,
        actor_uri,
        inbox.as_deref(),
        &target.id,
        &instrument_uri,
        &instrument_target,
        &instrument.attributed_to,
    )?;
    Ok(accepted())
}

/// Inbound `Accept(QuoteRequest)`: the signer must be the quoted post's
/// author, and the `result` stamp must dereference and verify. An `Accept`
/// without a valid stamp never approves.
fn handle_quote_accept(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    let Some(request_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    let Some(stamp_uri) = extract_object_uri(activity.get("result")) else {
        return Ok(accepted());
    };
    // The decider must be the quoted post's author.
    let Some(note) = services.notes.find_note_by_quote_request(&request_uri)? else {
        return Ok(accepted());
    };
    let Some(quote) = note.quote.as_ref() else {
        return Ok(accepted());
    };
    let Some(target_id) = quote.target_note_id.clone() else {
        return Ok(accepted());
    };
    let base = services.public_url.trim_end_matches('/').to_string();
    let target_author_uri = match services.notes.get_note(&target_id)? {
        Some(target) => match services.accounts.find_by_id(&target.author_id)? {
            Some(acc) => acc.actor_url(&base),
            None => return Ok(accepted()),
        },
        None => return Ok(accepted()),
    };
    if target_author_uri != actor_uri {
        return Ok(unauthorized());
    }
    // Transient stamp-fetch failure schedules a rate-limited re-verify job
    // (quote stays Pending); every other outcome is transport-accepted.
    if let crate::application::commands::inbound::quotes::QuoteDecisionOutcome::StampFetchPending(
        retry,
    ) = services.handle_inbound_quote_decision_accept(activity_id, &request_uri, &stamp_uri)?
    {
        services.jobs.enqueue_jobs(vec![retry])?;
    }
    Ok(accepted())
}

/// Inbound `Reject(QuoteRequest)`: only a matching request from the quoted
/// post's author flips our quote to `Rejected` (relation retained).
fn handle_quote_reject(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    // Embedded or bare-IRI object: the request activity IRI either way.
    let request_uri = match activity.get("object") {
        Some(Value::String(s)) if !s.trim().is_empty() => s.trim().to_string(),
        Some(Value::Object(map)) => match map.get("id").and_then(Value::as_str) {
            Some(id) if !id.trim().is_empty() => id.trim().to_string(),
            _ => return Ok(accepted()),
        },
        _ => return Ok(accepted()),
    };
    let Some(note) = services.notes.find_note_by_quote_request(&request_uri)? else {
        return Ok(accepted());
    };
    let Some(quote) = note.quote.as_ref() else {
        return Ok(accepted());
    };
    let Some(target_id) = quote.target_note_id.clone() else {
        return Ok(accepted());
    };
    let base = services.public_url.trim_end_matches('/').to_string();
    let target_author_uri = match services.notes.get_note(&target_id)? {
        Some(target) => match services.accounts.find_by_id(&target.author_id)? {
            Some(acc) => acc.actor_url(&base),
            None => return Ok(accepted()),
        },
        None => return Ok(accepted()),
    };
    if target_author_uri != actor_uri {
        return Ok(unauthorized());
    }
    let _ = services.handle_inbound_quote_decision_reject(activity_id, &request_uri)?;
    Ok(accepted())
}

fn handle_undo(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    match activity.get("object") {
        Some(Value::String(uri)) => {
            // Assume an Announce id: our renote notes store the Announce URI.
            let uri = uri.trim();
            if uri.is_empty() {
                return Ok(accepted());
            }
            let actor_account = resolve_remote_account(services, actor_uri)?;
            if services.inbound_host_blocked(&actor_account.id) {
                return Ok(accepted());
            }
            match find_note_by_uri(services, uri)? {
                Some(note)
                    if note.author_id == actor_account.id && note.renote_id.is_some() =>
                {
                    services.handle_inbound_delete_note(activity_id, &note.id)?;
                }
                _ => {}
            }
            Ok(accepted())
        }
        Some(Value::Object(map)) => {
            match map.get("type").and_then(Value::as_str).unwrap_or("") {
                "Follow" => {
                    let follower = resolve_remote_account(services, actor_uri)?;
                    let Some(inner_object) = non_empty_str(map.get("object")) else {
                        return Ok(accepted());
                    };
                    let Some(followee) = resolve_local_account(services, &inner_object)?
                    else {
                        return Ok(accepted());
                    };
                    if !followee.is_local() {
                        return Ok(accepted());
                    }
                    services.handle_inbound_unfollow(
                        activity_id,
                        &follower.id,
                        &followee.id,
                    )?;
                    // Misskey reuses one Follow id and one Undo id
                    // (`/follows/{follower}/{followee}[/undo]`) per pair;
                    // forget both so a later re-follow / re-unfollow is not
                    // mistaken for a replay (and still gets its Accept).
                    // Replaying an Undo(Follow) is idempotent anyway.
                    if let Some(follow_id) = non_empty_str(map.get("id")) {
                        services.dedup.forget_activity(&follow_id)?;
                    }
                    services.dedup.forget_activity(activity_id)?;
                    Ok(accepted())
                }
                "Like" | "EmojiReact" => {
                    let liker = resolve_remote_account(services, actor_uri)?;
                    if services.inbound_host_blocked(&liker.id) {
                        return Ok(accepted());
                    }
                    let Some(target_uri) = extract_object_uri(map.get("object")) else {
                        return Ok(accepted());
                    };
                    let Some(note) = find_note_by_uri(services, &target_uri)? else {
                        return Ok(accepted());
                    };
                    services.handle_inbound_unlike(activity_id, &liker.id, &note.id)?;
                    Ok(accepted())
                }
                // `Undo(Block)` (Misskey `ApInboxService.undoBlock()`
                // parity): the inner `Block`'s object is the local
                // blockee.
                "Block" => {
                    let blocker = resolve_remote_account(services, actor_uri)?;
                    let Some(blockee_uri) = extract_object_uri(map.get("object")) else {
                        return Ok(accepted());
                    };
                    let Some(blockee) = resolve_local_account(services, &blockee_uri)? else {
                        return Ok(accepted());
                    };
                    if !blockee.is_local() {
                        return Ok(accepted());
                    }
                    services.handle_inbound_unblock(activity_id, &blocker.id, &blockee.id)?;
                    Ok(accepted())
                }
                "Announce" => {
                    let actor_account = resolve_remote_account(services, actor_uri)?;
                    let Some(target_uri) = extract_object_uri(map.get("object")) else {
                        return Ok(accepted());
                    };
                    let Some(target_id) = resolve_note_target_id(services, &target_uri)?
                    else {
                        return Ok(accepted());
                    };
                    // Find the actor's renote of that target.
                    let notes = services
                        .notes
                        .notes_by_author(&actor_account.id, &TimelineCursor::with_limit(100))?;
                    for note in notes {
                        if note.renote_id.as_deref() == Some(target_id.as_str()) {
                            services.handle_inbound_delete_note(activity_id, &note.id)?;
                            break;
                        }
                    }
                    Ok(accepted())
                }
                _ => Ok(accepted()),
            }
        }
        _ => Ok(accepted()),
    }
}

fn handle_create(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    // Slice 7 (H2+C3): read-only replay gate BEFORE any remote fetch, so a
    // replayed Create triggers zero network fetches. Benign race: the final
    // `record_note_activity` stays the atomic authority, so a concurrent
    // first delivery that passes this check simply does the (idempotent)
    // work twice and still converges.
    if services.dedup.is_recorded(activity_id)? {
        crate::fed_debug!("in  drop type=Create id={activity_id:?} reason=replay-already-recorded");
        return Ok(accepted());
    }
    let client = &services.federation_http;
    let parsed: ParsedRemoteNote = match activity.get("object") {
        Some(Value::String(uri)) => {
            let uri = uri.trim();
            if uri.is_empty() {
                crate::fed_debug!("in  drop type=Create id={activity_id:?} reason=empty-object-uri");
                return Ok(accepted());
            }
            match fetch_remote_note(services, client, uri) {
                Ok(parsed) => parsed,
                Err(err) => {
                    crate::fed_debug!(
                        "in  drop type=Create id={activity_id:?} reason=object-fetch-failed uri={uri:?} err={err:?}"
                    );
                    return Ok(accepted());
                }
            }
        }
        Some(obj @ Value::Object(_)) => match parse_remote_note(obj) {
            Ok(parsed) => parsed,
            Err(err) => {
                crate::fed_debug!("in  drop type=Create id={activity_id:?} reason=object-unparseable err={err:?}");
                return Ok(accepted());
            }
        },
        _ => {
            crate::fed_debug!("in  drop type=Create id={activity_id:?} reason=object-missing");
            return Ok(accepted());
        }
    };
    // The note author must be the signer.
    if parsed.attributed_to != actor_uri {
        crate::fed_debug!(
            "in  reject reason=author-not-signer type=Create id={activity_id:?} author={:?} actor={actor_uri:?}",
            parsed.attributed_to
        );
        return Ok(unauthorized());
    }
    let author = resolve_remote_account(services, actor_uri)?;
    if services.inbound_host_blocked(&author.id) {
        return Ok(accepted());
    }
    // §4 remote votes (Misskey `ApNoteService` vote branch parity): a
    // content-less `Create(Note)` carrying `inReplyTo` (the poll note) plus
    // `name` (the choice text) records a vote instead of a note. Misskey
    // returns after the vote attempt unconditionally — vote objects never
    // become notes — so any `inReplyTo` resolving to a poll note
    // short-circuits ingest (unknown choices, expired polls, and
    // duplicates converge inside `handle_inbound_vote`).
    if parsed.text.is_none() && parsed.cw.is_none() {
        if let (Some(choice_name), Some(reply_uri)) =
            (parsed.name.clone(), parsed.in_reply_to.clone())
        {
            if !choice_name.trim().is_empty() {
                match find_note_by_uri(services, reply_uri.trim())? {
                    Some(target) => {
                        if services.polls.get_poll(&target.id)?.is_some() {
                            let applied = services.handle_inbound_vote(
                                activity_id,
                                &author.id,
                                &target.id,
                                choice_name.trim(),
                            )?;
                            if applied {
                                eprintln!(
                                    "[inbound-vote] applied activity={activity_id} note={}",
                                    target.id
                                );
                            }
                            return Ok(accepted());
                        }
                        eprintln!(
                            "[inbound-vote] not-a-poll, ingesting as note activity={activity_id} parent={}",
                            target.id
                        );
                    }
                    None => eprintln!(
                        "[inbound-vote] parent-not-cached, ingesting as note activity={activity_id} parent={}",
                        reply_uri.trim()
                    ),
                }
            }
        }
    }
    // An `Answer` is a vote, never a note: when it did not resolve to a
    // cached poll above, drop it (logged) instead of storing an empty note.
    if is_answer_object(activity) {
        eprintln!("[inbound-vote] answer-without-cached-poll activity={activity_id}");
        return Ok(accepted());
    }
    match ingest_remote_note(services, activity_id, &parsed, &author, true)? {
        Some(note) => crate::fed_debug!(
            "in  applied type=Create id={activity_id:?} uri={:?} note={}",
            parsed.uri,
            note.id
        ),
        None => crate::fed_debug!(
            "in  drop type=Create id={activity_id:?} reason=already-cached-uri uri={:?}",
            parsed.uri
        ),
    }
    Ok(accepted())
}

fn is_answer_object(activity: &Value) -> bool {
    activity
        .get("object")
        .and_then(|o| o.get("type"))
        .and_then(Value::as_str)
        == Some("Answer")
}

fn handle_announce(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(target_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    let announcer = resolve_remote_account(services, actor_uri)?;
    let Some(target_id) = resolve_note_target_id(services, &target_uri)? else {
        return Ok(accepted());
    };

    let base = services.public_url.trim_end_matches('/').to_string();
    let announcer_uri = announcer.actor_url(&base);
    let to = extract_uri_list(activity.get("to"));
    let cc = extract_uri_list(activity.get("cc"));
    let mut visibility = parse_note_audience(
        &to,
        &cc,
        Some(&format!("{announcer_uri}/followers")),
    )
    .0;
    if to.is_empty() && cc.is_empty() {
        visibility = Visibility::Followers;
    }

    let input = CreateNoteInput {
        author_id: announcer.id.clone(),
        text: None,
        cw: None,
        visibility,
        local_only: false,
        reply_to: None,
        reply_parent_uri: None,
        renote_of: Some(target_id),
        file_ids: Vec::new(),
        visible_users: Vec::new(),
        reaction_acceptance: None,
        no_extract_mentions: false,
        no_extract_hashtags: false,
        no_extract_emojis: false,
        has_poll: false,
        poll: None,
        poll_initial_votes: None,
        has_channel: false,
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        uri: Some(activity_id.to_string()),
        uri_base: String::new(),
        url: Some(activity_id.to_string()),
        extra_mentions: Vec::new(),
        extra_tags: Vec::new(),
        quote: None,
    };
    services.handle_inbound_create_note(activity_id, input, Vec::new())?;
    Ok(accepted())
}

fn handle_like(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(target_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    let liker = resolve_remote_account(services, actor_uri)?;
    let Some(target_id) = resolve_note_target_id(services, &target_uri)? else {
        return Ok(accepted());
    };
    // Server-side coercion to the note's acceptance policy happens inside.
    let reaction = extract_like_reaction(activity);
    services.handle_inbound_like(activity_id, &liker.id, &target_id, &reaction)?;
    Ok(accepted())
}

fn handle_delete(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    // Replay gate before any store lookups (mirrors QuoteRequest/Accept/Reject).
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    let Some(object_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    // Path 1: a known note → existing author/host authority + delete.
    if let Some(note) = find_note_by_uri(services, &object_uri)? {
        // Authority: the deleter must be the note's author, or share the note's
        // origin host.
        let base = services.public_url.trim_end_matches('/').to_string();
        let note_uri = note
            .uri
            .clone()
            .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
        let author_uri = match services.accounts.find_by_id(&note.author_id)? {
            Some(author) => Some(author.actor_url(&base)),
            None => None,
        };
        let mut authorized = author_uri.as_deref() == Some(actor_uri);
        if !authorized {
            if let (Some(actor_host), Some(note_host)) = (host_of(actor_uri), host_of(&note_uri))
            {
                authorized = actor_host == note_host;
            }
        }
        if !authorized {
            return Ok(unauthorized());
        }

        services.handle_inbound_delete_note(activity_id, &note.id)?;
        return Ok(accepted());
    }

    // Path 2: a stamp we authored. Only we may delete it; a remote `Delete`
    // for our stamp is unauthorized (revocation of our stamps happens
    // server-side, never via federation input).
    if services.notes.get_quote_stamp(&object_uri)?.is_some() {
        return Ok(unauthorized());
    }

    // Path 3: revocation of a stamp backing a quote we authored.
    // Authority is the STAMP AUTHOR (the quoted post's author actor), not
    // merely anyone on the stamp's host: any actor on the author's host
    // must not be able to revoke another user's stamp. Revocation forwards
    // re-signed by third parties are ignored — a forged `Delete` must never
    // revoke a live quote, and a genuinely deleted stamp is delivered
    // authoritatively by the stamp author's own server. (Forwarding itself
    // is still implemented outbound as gossip.)
    let Some(affected) = services.notes.find_note_by_quote_stamp(&object_uri)? else {
        // Path 4: a `Delete` for a note this node does not hold. Misskey and
        // Catodon can send the `Delete` before the `Create` it races with
        // (bench S6, 2026-10-17). Record a tombstone so the late `Create` is
        // refused. Only the object's own host may tombstone it, the same
        // origin rule as Path 1, and a local URI is never tombstoned from
        // federation input.
        let local_host = host_of(&services.public_url);
        if let (Some(actor_host), Some(object_host)) = (host_of(actor_uri), host_of(&object_uri))
        {
            if actor_host == object_host && Some(&object_host) != local_host.as_ref() {
                services.handle_inbound_tombstone_uri(activity_id, &object_uri)?;
            }
        }
        return Ok(accepted());
    };
    let affected_target_author_uri = affected
        .quote
        .as_ref()
        .and_then(|q| q.target_note_id.clone())
        .and_then(|tid| services.notes.get_note(&tid).ok().flatten())
        .and_then(|target| {
            services
                .accounts
                .find_by_id(&target.author_id)
                .ok()
                .flatten()
                .map(|acc| acc.actor_url(services.public_url.trim_end_matches('/')))
        });
    if affected_target_author_uri.as_deref() != Some(actor_uri) {
        return Ok(accepted());
    }
    let _ = services.handle_inbound_quote_stamp_delete(activity_id, &object_uri)?;
    Ok(accepted())
}

#[allow(clippy::too_many_lines)]
fn handle_update(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let (object_id, embedded_doc) = match activity.get("object") {
        Some(Value::String(uri)) => {
            let uri = uri.trim().to_string();
            if uri.is_empty() {
                return Ok(accepted());
            }
            (uri, None)
        }
        Some(doc @ Value::Object(map)) => {
            let obj_type = map.get("type").and_then(Value::as_str).unwrap_or("");
            // §4 owns ONLY the poll-vote/`Question`-expiry branch of inbound
            // `Update` (this slice). Embedded `Note`/`Article`/`Page`
            // objects are NOT handled here (see below).
            if obj_type == "Question" {
                return handle_update_question(services, doc, activity_id, actor_uri);
            }
            if matches!(obj_type, "Note" | "Article" | "Page") {
                return handle_update_note(services, doc, activity_id, actor_uri);
            }
            let Some(id) = non_empty_str(map.get("id")) else {
                return Ok(accepted());
            };
            (id, Some(doc.clone()))
        }
        _ => return Ok(accepted()),
    };
    // The updated actor must be the signer.
    if object_id != actor_uri {
        return Ok(unauthorized());
    }

    let client = &services.federation_http;
    let (account, parsed) = match embedded_doc {
        Some(doc) => {
            let parsed = match parse_remote_actor(&doc) {
                Ok(parsed) => parsed,
                Err(_) => return Ok(accepted()),
            };
            let account = match services.accounts.find_by_uri(&object_id)? {
                Some(account) => account,
                None => match fetch_and_upsert_remote_actor(services, client, &object_id) {
                    Ok((account, _)) => account,
                    Err(_) => return Ok(accepted()),
                },
            };
            (account, parsed)
        }
        None => match fetch_and_upsert_remote_actor(services, client, &object_id) {
            Ok((account, parsed)) => (account, parsed),
            Err(_) => return Ok(accepted()),
        },
    };
    // Never rewrite a local profile from federation input.
    if account.is_local() {
        return Ok(accepted());
    }

    let description = parsed
        .summary_mfm
        .clone()
        .or_else(|| {
            parsed
                .summary_html
                .as_deref()
                .map(sanitize_html_to_mfm)
        })
        .filter(|s| !s.is_empty());
    let patch = UpdateAccountPatch {
        display_name: Some(parsed.display_name.clone()),
        description: Some(description),
        location: Some(parsed.location.clone()),
        birthday: None,
        avatar_id: None,
        banner_id: None,
        is_locked: Some(parsed.manually_approves_followers),
        is_bot: Some(parsed.is_bot()),
        is_cat: Some(parsed.is_cat),
        is_explorable: Some(parsed.discoverable),
        ..Default::default()
    };
    services.handle_inbound_update_actor(activity_id, &account.id, patch)?;
    Ok(accepted())
}

/// Inbound `Update(Question)` (§4; Misskey `ApQuestionService.updateQuestion`
/// parity): refresh a cached remote poll's tallies from the embedded
/// question document. Authority: the question must be attributed to the
/// cached poll's owner and the signer must be that owner. Unknown notes,
/// locally authored polls, unattributed/misattributed questions, and
/// unparseable options all converge to accept-without-change. Count sync
/// itself is idempotent (overwrite by choice name), so replays converge.
///
/// §8 owns the sibling `Update(Note)`/`Update(Article)`/`Update(Page)`
/// edit branch (`handle_update_note` above, sharing this same inbox
/// dispatcher). This function stays scoped to `Question` objects only.
fn handle_update_question(
    services: &AppServices,
    doc: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(map) = doc.as_object() else {
        return Ok(accepted());
    };
    let Some(question_id) = map
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(accepted());
    };
    // Authority gate first (before any fetch): the signer must own the
    // question it claims to update. `attributedTo` may be a string or an
    // embedded actor object; absent attribution inherits the signer.
    let attributed_to = match map.get("attributedTo") {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Some(Value::Object(m)) => m
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned),
        _ => None,
    };
    if let Some(attribution) = attributed_to.as_deref() {
        if attribution != actor_uri {
            return Ok(accepted());
        }
    }
    let question = match parse_question_poll(map) {
        Some(question) => question,
        None => return Ok(accepted()),
    };
    let counts: Vec<crate::application::ports::PollChoiceCount> = question
        .choices
        .iter()
        .enumerate()
        .map(|(i, name)| crate::application::ports::PollChoiceCount {
            name: name.clone(),
            votes: question.votes.get(i).copied().unwrap_or(0),
        })
        .collect();
    // `handle_inbound_question_update` re-checks authority against the
    // cached poll owner (attribution must equal the owner's actor URI) and
    // ignores locally authored polls, so a forged `Update` can only ever
    // refresh tallies on a remote poll its own owner signed.
    let _ = services.handle_inbound_question_update(
        activity_id,
        question_id,
        attributed_to.as_deref().or(Some(actor_uri)),
        counts,
    )?;
    Ok(accepted())
}

/// Inbound `Update(Note|Article|Page)` (§8; Sharkey
/// `ApInboxService.update` → `ApNoteService.updateNote` parity): apply a
/// remote author's edit to the cached local copy. Embedded documents only
/// (string-URI objects are accepted without fetching — same divergence as
/// the `Question` arm). Authority: the signer must be the note's author
/// (`attributedTo`, defaulting to the signer when absent) and the cached
/// copy's author; locally authored copies, unknown notes, and suspended
/// authors converge to accept-without-change. A missing note is NOT
/// created here (DIVERGENCE from Sharkey, which falls back to create —
/// pinetree's `Create` path already ingests new notes). Poison-pill
/// validation failures accept (no retry storm); storage faults propagate
/// (the delivery retries).
fn handle_update_note(
    services: &AppServices,
    doc: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    // Attribution defaults to the signer when absent (same rule as the
    // `Question` arm and Misskey's `getOneApId ... ?? user.uri` fallback).
    let attributed_to = match doc.get("attributedTo") {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Some(Value::Object(m)) => m
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned),
        _ => None,
    };
    if let Some(attribution) = attributed_to.as_deref() {
        if attribution != actor_uri {
            return Ok(accepted());
        }
    }
    let parsed = match parse_remote_note(doc) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(accepted()),
    };
    if parsed.uri.trim().is_empty() || parsed.attributed_to != actor_uri {
        return Ok(accepted());
    }
    // Quote-target (renote) resolution: a present-but-unresolvable quote
    // detaches (Sharkey `quote ?? null` parity); an unchanged target
    // resolves to the same id (no-change, no history row).
    let renote_of = match parsed.quote_target_uri.as_deref() {
        Some(uri) => find_note_by_uri(services, uri)?.map(|note| note.id),
        None => None,
    };
    // Poll shapes below the storage invariant degrade to poll-less (same
    // divergence as the `Create` ingest path); a parsed-but-invalid poll
    // removes the cached poll (Sharkey `catch → undefined` parity).
    let poll = match parsed.poll.as_ref() {
        Some(p)
            if p.choices.len() >= 2
                && p.choices.len() <= 10
                && p.choices.iter().all(|c| {
                    let n = c.chars().count();
                    n >= 1 && n <= 50
                }) =>
        {
            Some(PollInput {
                choices: p.choices.clone(),
                multiple: p.multiple,
                expires_at_ms: p.expires_at_ms,
            })
        }
        _ => None,
    };
    match services.handle_inbound_update_note(
        activity_id,
        parsed.uri.trim(),
        actor_uri,
        parsed.text.clone(),
        parsed.cw.clone(),
        renote_of,
        poll,
    ) {
        Ok(_) => Ok(accepted()),
        Err(PortError::Validation(_))
        | Err(PortError::NotFound(_))
        | Err(PortError::Conflict(_)) => Ok(accepted()),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Ingestion helpers
// ---------------------------------------------------------------------------

/// Build a [`CreateNoteInput`] from a parsed remote note and store it via the
/// inbound (deduping) path. Returns `None` on activity replay.
fn ingest_remote_note(
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
fn ingest_remote_note_inner(
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
    // the worker fetches off-request.
    if let Some(pending_uri) = reply_parent_uri.as_deref() {
        extra_jobs.push(
            crate::application::commands::inbound::replies::build_reply_parent_resolve_job(
                services,
                pending_uri,
            )?,
        );
    }
    // Arrival-triggered resolution: a newly stored canonical URI may unblock
    // children already waiting on it. One cheap pending-index probe per
    // inbound note; the resolver job does the bounded batch work.
    // Arrival-triggered resolution: a newly stored canonical URI may unblock
    // children already waiting on it. One cheap pending-index probe per
    // inbound note; the resolver job does the bounded batch work.
    let own_uri = parsed.uri.trim();
    if !own_uri.is_empty()
        && !services
            .notes
            .pending_reply_children(own_uri, 1)?
            .is_empty()
    {
        extra_jobs.push(
            crate::application::commands::inbound::replies::build_reply_parent_resolve_job(
                services,
                own_uri,
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
fn resolve_inbound_quote(
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
        eprintln!("quote carriers conflict on {}: kept {target_uri}", parsed.uri);
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
            let client = &services.federation_http;
            match crate::federation::delivery::fetch_quote_stamp_doc(
                services,
                client,
                &stamp_iri,
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
                            quote.authorization_uri = Some(stamp_iri);
                            quote.last_verified_at = Some(services.clock.now_ms());
                        }
                        Err(_) => {}
                    }
                }
                Err(_) => quote.authorization = Unknown,
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
fn resolve_quote_target_id(
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
fn resolve_remote_account(
    services: &AppServices,
    actor_uri: &str,
) -> Result<Account, PortError> {
    if let Some(account) = services.accounts.find_by_uri(actor_uri)? {
        return Ok(account);
    }
    let client = &services.federation_http;
    fetch_and_upsert_remote_actor(services, client, actor_uri)
        .map(|(account, _)| account)
}

/// Find a local account by actor URI: direct URI lookup, falling back to the
/// `{public_url}/users/{id}` form. Returns `None` when unknown (never fetches).
fn resolve_local_account(
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
fn find_note_by_uri(services: &AppServices, uri: &str) -> Result<Option<Note>, PortError> {
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

fn non_empty_str(value: Option<&Value>) -> Option<String> {
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
fn canonical_activity_type(t: &str) -> String {
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

fn accepted() -> Response {
    (StatusCode::ACCEPTED, "").into_response()
}

fn bad_request() -> Response {
    (StatusCode::BAD_REQUEST, "bad request").into_response()
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}

fn payload_too_large() -> Response {
    (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response()
}

fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "").into_response()
}

// ---------------------------------------------------------------------------
// Slice 7 (H2+C3) media-safety regression tests (written before the fix)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod media_safety_tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};

    use axum::http::{HeaderName, HeaderValue};
    use serde_json::json;

    use crate::activitypub::signatures::{
        generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
        sign_request_headers,
    };
    use crate::application::ports::{
        ActivityDedup, BlobStore, Clock, NoopStreamingSink,
    };
    use crate::domain::id::{AidxGenerator, IdGenerator, ManualClock};
    use crate::federation::delivery::{
        format_http_date, upsert_remote_actor_from_doc, SafeHttpClient, SafeHttpConfig,
    };
    use crate::federation::retry::DestinationTracker;
    use crate::jobs::worker::JobWorker;
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    const REMOTE_URI: &str = "https://remote.invalid/users/rocky";

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-inbox-media-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        }
    }

    fn actor_doc(id: &str, username: &str, key_id: &str, pem: &str) -> Value {
        json!({
            "id": id,
            "type": "Person",
            "preferredUsername": username,
            "inbox": format!("{id}/inbox"),
            "publicKey": {"id": key_id, "type": "Key", "owner": id, "publicKeyPem": pem},
        })
    }

    fn signed_inbox_headers(
        services: &AppServices,
        path: &str,
        body: &[u8],
        key_id: &str,
        private_pem: &str,
    ) -> HeaderMap {
        let date = format_http_date(services.clock.now_ms());
        let host = "pinetree.local";
        let signed =
            sign_request_headers("POST", path, host, &date, Some(body), key_id, private_pem)
                .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_str("date").unwrap(),
            HeaderValue::from_str(&date).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("host").unwrap(),
            HeaderValue::from_str(host).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("digest").unwrap(),
            HeaderValue::from_str(&signed.digest.unwrap()).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("signature").unwrap(),
            HeaderValue::from_str(&signed.signature).unwrap(),
        );
        headers
    }

    /// Stub media origin: counts every GET, always serves `png_bytes`.
    fn stub_media_server(hits: Arc<AtomicUsize>, png_bytes: Vec<u8>) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for _ in 0..8 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                hits.fetch_add(1, Ordering::SeqCst);
                let hdr = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    png_bytes.len()
                );
                let _ = stream.write_all(hdr.as_bytes());
                let _ = stream.write_all(&png_bytes);
                let _ = stream.flush();
            }
        });
        addr
    }

    #[test]
    fn replayed_create_triggers_zero_fetches() {
        let services = Arc::new(test_services("replay"));
        let (private_key, public_key) = generate_rsa_keypair_2048().unwrap();
        let (private_pem, public_pem) = (
            private_key_to_pkcs8_pem(&private_key),
            public_key_to_spki_pem(&public_key),
        );
        let key_id = format!("{REMOTE_URI}#main-key");
        let doc = actor_doc(REMOTE_URI, "rocky", &key_id, &public_pem);
        upsert_remote_actor_from_doc(&services, REMOTE_URI, &doc).expect("honest upsert");

        let hits = Arc::new(AtomicUsize::new(0));
        let png = vec![
            0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3, 4, 5, 6, 7, 8,
        ];
        let addr = stub_media_server(hits.clone(), png.clone());
        let note_uri = "https://remote.invalid/notes/1";
        let activity = json!({
            "id": "https://remote.invalid/activities/create-1",
            "type": "Create",
            "actor": REMOTE_URI,
            "object": {
                "id": note_uri,
                "type": "Note",
                "attributedTo": REMOTE_URI,
                "content": "hello with pic",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "attachment": [{
                    "type": "Document",
                    "mediaType": "image/png",
                    "url": format!("http://{addr}/pic.png"),
                    "name": "pic",
                }],
            },
        });
        let body = serde_json::to_vec(&activity).unwrap();

        // First delivery: accepted, note stored WITHOUT files, and the
        // inbox performed zero media fetches (fetch moved to a durable job).
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.clone().into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "inbox must not fetch remote media inline"
        );
        let note = services
            .notes
            .find_by_uri(note_uri)
            .unwrap()
            .expect("note stored on first delivery");
        assert!(note.file_ids.is_empty(), "no inline attach");
        assert_eq!(
            services.jobs.pending_count().unwrap(),
            2,
            "streaming job + RemoteMediaFetch job enqueued atomically"
        );

        // The worker performs exactly one fetch and attaches the sniffed PNG.
        let worker = JobWorker {
            clock: services.clock.clone(),
            ids: services.ids.clone(),
            jobs: services.jobs.clone(),
            notifications: services.notifications.clone(),
            streaming: services.streaming.clone(),
            services: services.clone(),
            delivery_tracker: Arc::new(DestinationTracker::new()),
            lease_duration_ms: 60_000,
            base_backoff_ms: 10,
        };
        assert_eq!(worker.drain(16).unwrap(), 2);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let note = services.notes.find_by_uri(note_uri).unwrap().unwrap();
        assert_eq!(note.file_ids.len(), 1);
        let meta = services
            .media
            .get(&note.file_ids[0])
            .unwrap()
            .expect("attached meta exists");
        assert_eq!(meta.mime, "image/png");
        assert_eq!(
            meta.source_url.as_deref(),
            Some(format!("http://{addr}/pic.png").as_str())
        );

        // Replay of the same Create: accepted, zero fetches, zero new jobs,
        // zero new rows.
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "replay must trigger zero fetches"
        );
        assert_eq!(
            services.jobs.pending_count().unwrap(),
            0,
            "replay enqueues no jobs"
        );
        let note = services.notes.find_by_uri(note_uri).unwrap().unwrap();
        assert_eq!(note.file_ids.len(), 1, "replay attaches nothing new");
    }

    #[test]
    fn replayed_uri_object_create_triggers_zero_note_fetches() {
        // Slice 7 (H2+C3): when the Create carries a URI object, the inbox
        // must fetch the note on first delivery but the read-only
        // `is_recorded` replay gate must skip that fetch on replay. (The
        // embedded-object replay test above cannot cover the gate: without
        // any inline fetch it is trivially fetch-free.)
        let services = Arc::new(test_services("replay-uri"));
        let (private_key, public_key) = generate_rsa_keypair_2048().unwrap();
        let (private_pem, public_pem) = (
            private_key_to_pkcs8_pem(&private_key),
            public_key_to_spki_pem(&public_key),
        );
        let hits = Arc::new(AtomicUsize::new(0));
        // Path-based stub: serves the note JSON at /notes/9, 404 elsewhere.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let actor_uri = format!("http://{addr}/users/rocky");
        let note_uri = format!("http://{addr}/notes/9");
        let served = serde_json::to_vec(&json!({
            "id": note_uri,
            "type": "Note",
            "attributedTo": actor_uri,
            "content": "fetched note",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
        }))
        .unwrap();
        let hits_clone = hits.clone();
        std::thread::spawn(move || {
            for _ in 0..8 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                hits_clone.fetch_add(1, Ordering::SeqCst);
                let request_line = String::from_utf8_lossy(&buf);
                if request_line.starts_with("GET /notes/9") {
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/activity+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        served.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&served);
                } else {
                    let _ = stream.write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                }
                let _ = stream.flush();
            }
        });
        let key_id = format!("{actor_uri}#main-key");
        let doc = actor_doc(&actor_uri, "rocky", &key_id, &public_pem);
        upsert_remote_actor_from_doc(&services, &actor_uri, &doc).expect("honest upsert");

        let activity = json!({
            "id": format!("http://{addr}/activities/create-9"),
            "type": "Create",
            "actor": actor_uri,
            "object": note_uri,
        });
        let body = serde_json::to_vec(&activity).unwrap();

        // First delivery fetches the note exactly once and stores it.
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.clone().into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "first delivery fetches note");
        services
            .notes
            .find_by_uri(&note_uri)
            .unwrap()
            .expect("note stored on first delivery");
        let pending_after_first = services.jobs.pending_count().unwrap();

        // Replay: accepted, but the gate skips the note fetch entirely.
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "replay must trigger zero note fetches"
        );
        assert_eq!(
            services.jobs.pending_count().unwrap(),
            pending_after_first,
            "replay enqueues no jobs"
        );
    }

    #[test]
    fn create_with_too_many_attachments_truncates_to_16() {
        // >16 attachments are truncated (not rejected): the fetch job
        // carries exactly the first 16 in document order.
        let services = test_services("truncate-inbox");
        let (private_key, public_key) = generate_rsa_keypair_2048().unwrap();
        let (private_pem, public_pem) = (
            private_key_to_pkcs8_pem(&private_key),
            public_key_to_spki_pem(&public_key),
        );
        let key_id = format!("{REMOTE_URI}#main-key");
        let doc = actor_doc(REMOTE_URI, "rocky", &key_id, &public_pem);
        upsert_remote_actor_from_doc(&services, REMOTE_URI, &doc).expect("honest upsert");

        let attachments: Vec<Value> = (0..20)
            .map(|i| {
                json!({
                    "type": "Document",
                    "mediaType": "image/png",
                    "url": format!("http://remote.invalid/{i}.png"),
                    "name": "pic",
                })
            })
            .collect();
        let activity = json!({
            "id": "https://remote.invalid/activities/create-many",
            "type": "Create",
            "actor": REMOTE_URI,
            "object": {
                "id": "https://remote.invalid/notes/many",
                "type": "Note",
                "attributedTo": REMOTE_URI,
                "content": "many pics",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "attachment": attachments,
            },
        });
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // Find the fetch job and inspect its payload without running it
        // (the URLs are unroutable; nothing may be fetched in this test).
        let mut saw_fetch = false;
        while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
            if job.kind == crate::application::ports::JobKind::RemoteMediaFetch {
                let payload: crate::application::ports::RemoteMediaFetchPayload =
                    serde_json::from_slice(&job.payload).unwrap();
                assert_eq!(payload.attachments.len(), 16);
                assert_eq!(
                    payload.attachments[0].url,
                    "http://remote.invalid/0.png"
                );
                assert_eq!(
                    payload.attachments[15].url,
                    "http://remote.invalid/15.png"
                );
                saw_fetch = true;
            }
            services.jobs.complete(&job.id).unwrap();
        }
        assert!(saw_fetch, "a truncated fetch job was enqueued");
    }
}

// ---------------------------------------------------------------------------
// Slice 2 (C1 key ownership) regression tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod key_ownership_tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::http::{HeaderName, HeaderValue};
    use serde_json::json;

    use crate::activitypub::signatures::{
        generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
        sign_request_headers,
    };
    use crate::application::ports::{
        ActivityDedup, AuthStore, BlobStore, Clock, IdGenerator, NoopStreamingSink,
    };
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::{
        format_http_date, resolve_remote_public_key, upsert_remote_actor_from_doc, SafeHttpClient,
        SafeHttpConfig,
    };
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    const VICTIM_URI: &str = "https://victim.invalid/users/victim";
    const ATTACKER_URI: &str = "https://attacker.invalid/users/attacker";

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-key-own-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        }
    }

    fn actor_doc(
        id: &str,
        username: &str,
        key_id: &str,
        owner: Option<&str>,
        pem: &str,
    ) -> Value {
        let mut key = json!({"id": key_id, "type": "Key", "publicKeyPem": pem});
        if let Some(o) = owner {
            key["owner"] = json!(o);
        }
        json!({
            "id": id,
            "type": "Person",
            "preferredUsername": username,
            "inbox": format!("{id}/inbox"),
            "publicKey": key,
        })
    }

    fn keypair_pems() -> (String, String) {
        let (priv_key, pub_key) = generate_rsa_keypair_2048().unwrap();
        (
            private_key_to_pkcs8_pem(&priv_key),
            public_key_to_spki_pem(&pub_key),
        )
    }

    fn signed_inbox_headers(
        services: &AppServices,
        path: &str,
        body: &[u8],
        key_id: &str,
        private_pem: &str,
    ) -> HeaderMap {
        let date = format_http_date(services.clock.now_ms());
        let host = "pinetree.local";
        let signed =
            sign_request_headers("POST", path, host, &date, Some(body), key_id, private_pem)
                .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_str("date").unwrap(),
            HeaderValue::from_str(&date).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("host").unwrap(),
            HeaderValue::from_str(host).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("digest").unwrap(),
            HeaderValue::from_str(&signed.digest.unwrap()).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("signature").unwrap(),
            HeaderValue::from_str(&signed.signature).unwrap(),
        );
        headers
    }

    #[test]
    fn parse_accepts_honest_key_shapes() {
        let (_, victim_pub) = keypair_pems();
        for key_id in [
            format!("{VICTIM_URI}#main-key"),
            format!("{VICTIM_URI}/publickey"),
            VICTIM_URI.to_string(),
        ] {
            let doc = actor_doc(VICTIM_URI, "victim", &key_id, Some(VICTIM_URI), &victim_pub);
            let parsed = parse_remote_actor(&doc).expect("honest key shape must parse");
            assert_eq!(parsed.public_key_id, key_id);
            // Bare actor id without `owner` (some implementations omit it).
            let doc = actor_doc(VICTIM_URI, "victim", &key_id, None, &victim_pub);
            parse_remote_actor(&doc).expect("honest key shape without owner must parse");
        }
    }

    #[test]
    fn parse_rejects_cross_origin_key_without_owner() {
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let doc = actor_doc(ATTACKER_URI, "attacker", &victim_key, None, &victim_pub);
        assert!(
            parse_remote_actor(&doc).is_err(),
            "attacker doc advertising a victim key id must be rejected"
        );
    }

    #[test]
    fn parse_rejects_cross_origin_key_with_matching_owner() {
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        // `owner` equals the attacker actor, so only the key-id binding
        // catches this.
        let doc = actor_doc(
            ATTACKER_URI,
            "attacker",
            &victim_key,
            Some(ATTACKER_URI),
            &victim_pub,
        );
        assert!(
            parse_remote_actor(&doc).is_err(),
            "victim key id under an attacker actor must be rejected even with owner == actor"
        );
    }

    #[test]
    fn parse_rejects_owner_pointing_at_victim() {
        let (_, attacker_pub) = keypair_pems();
        let attacker_key = format!("{ATTACKER_URI}#main-key");
        // Key id is honest, but `owner` names someone else.
        let doc = actor_doc(
            ATTACKER_URI,
            "attacker",
            &attacker_key,
            Some(VICTIM_URI),
            &attacker_pub,
        );
        assert!(
            parse_remote_actor(&doc).is_err(),
            "owner mismatch must be rejected"
        );
        // Victim key id + victim owner under the attacker actor: both the
        // owner check and the key-id check fire.
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let doc = actor_doc(
            ATTACKER_URI,
            "attacker",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        assert!(
            parse_remote_actor(&doc).is_err(),
            "cross-origin key with victim owner must be rejected"
        );
    }

    #[test]
    fn upsert_poison_doc_rejected_and_real_key_kept() {
        let services = test_services("poison-upsert");
        let (_, victim_pub) = keypair_pems();
        let (_, attacker_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");

        services
            .cache_remote_public_key(&victim_key, VICTIM_URI, None, &victim_pub)
            .unwrap();

        let poison = actor_doc(ATTACKER_URI, "attacker", &victim_key, None, &attacker_pub);
        assert!(
            upsert_remote_actor_from_doc(&services, ATTACKER_URI, &poison).is_err(),
            "attacker doc advertising the victim key id must not upsert"
        );

        let kept = services
            .auth
            .get_remote_public_key(&victim_key)
            .unwrap()
            .expect("victim key row must survive the poison attempt");
        assert_eq!(kept.actor_uri, VICTIM_URI);
        assert_eq!(kept.public_key_pem, victim_pub.trim());
    }

    #[test]
    fn cache_overwrite_refused_for_different_owner() {
        let services = test_services("overwrite-guard");
        let (_, victim_pub) = keypair_pems();
        let (_, attacker_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");

        services
            .cache_remote_public_key(&victim_key, VICTIM_URI, None, &victim_pub)
            .unwrap();
        let err = services
            .cache_remote_public_key(&victim_key, ATTACKER_URI, None, &attacker_pub)
            .expect_err("overwriting a key bound to another actor must fail");
        assert!(
            matches!(err, PortError::Conflict(_)),
            "overwrite guard must report Conflict, got {err:?}"
        );

        let kept = services
            .auth
            .get_remote_public_key(&victim_key)
            .unwrap()
            .unwrap();
        assert_eq!(kept.actor_uri, VICTIM_URI);
        assert_eq!(kept.public_key_pem, victim_pub.trim());
    }

    #[test]
    fn upsert_honest_key_caches_single_row() {
        let services = test_services("single-row");
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        let (_, parsed) =
            upsert_remote_actor_from_doc(&services, VICTIM_URI, &doc).expect("honest upsert");
        assert_eq!(parsed.public_key_id, victim_key);
        assert!(
            services
                .auth
                .get_remote_public_key(&victim_key)
                .unwrap()
                .is_some(),
            "real key id must be cached"
        );
        assert!(
            services
                .auth
                .get_remote_public_key(&format!("{VICTIM_URI}/publickey"))
                .unwrap()
                .is_none(),
            "no speculative /publickey alias row may be written"
        );
    }

    #[test]
    fn upsert_subpath_key_caches_single_row() {
        let services = test_services("single-row-subpath");
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}/publickey");
        let doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        let (_, parsed) =
            upsert_remote_actor_from_doc(&services, VICTIM_URI, &doc).expect("honest upsert");
        assert_eq!(parsed.public_key_id, victim_key);
        let stored = services
            .auth
            .get_remote_public_key(&victim_key)
            .unwrap()
            .expect("sub-path key id must be cached");
        assert_eq!(stored.actor_uri, VICTIM_URI);
        assert_eq!(stored.public_key_pem, victim_pub.trim());
    }

    #[test]
    fn resolve_returns_owner_and_rejects_fetch_key_mismatch() {
        let services = test_services("resolve-owner");
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        upsert_remote_actor_from_doc(&services, VICTIM_URI, &doc).expect("honest upsert");
        let (owner, pem) =
            resolve_remote_public_key(&services, &services.federation_http, &victim_key, false)
                .expect("cached resolve");
        assert_eq!(owner, VICTIM_URI, "resolve must return the key owner");
        assert_eq!(pem, victim_pub.trim());

        // Fetch path: serve an actor whose advertised key id differs from
        // the requested one; resolve must refuse it.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let served_actor = format!("http://{addr}/users/x");
        let served_doc = actor_doc(
            &served_actor,
            "x",
            &format!("{served_actor}#other-key"),
            Some(&served_actor),
            &victim_pub,
        );
        let served_body = serde_json::to_string(&served_doc).unwrap();
        std::thread::spawn(move || {
            for _ in 0..2 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/activity+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    served_body.len(),
                    served_body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        let requested = format!("{served_actor}#main-key");
        assert!(
            resolve_remote_public_key(&services, &services.federation_http, &requested, true)
                .is_err(),
            "fetched key id must equal the requested key id"
        );
    }

    #[test]
    fn inbox_rejects_forged_victim_activity() {
        let services = test_services("forged-inbox");
        let (attacker_priv, attacker_pub) = keypair_pems();
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");

        // Honest victim row first.
        let victim_doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        upsert_remote_actor_from_doc(&services, VICTIM_URI, &victim_doc).expect("honest upsert");

        // Poison attempt: attacker doc advertising the victim key id.
        let poison = actor_doc(ATTACKER_URI, "attacker", &victim_key, None, &attacker_pub);
        assert!(
            upsert_remote_actor_from_doc(&services, ATTACKER_URI, &poison).is_err(),
            "poison upsert must fail so the cache keeps the victim key"
        );

        // Forged activity: claims victim authorship, carries the victim
        // keyId, but is signed with the attacker key.
        let local = services.create_account("local", None).unwrap();
        let base = services.public_url.trim_end_matches('/').to_string();
        let activity = json!({
            "id": "https://victim.invalid/activities/forged-1",
            "type": "Follow",
            "actor": VICTIM_URI,
            "object": format!("{base}/users/{}", local.id),
        });
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &victim_key, &attacker_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "forged victim activity must be rejected"
        );

        // The victim cache row still belongs to the victim.
        let kept = services
            .auth
            .get_remote_public_key(&victim_key)
            .unwrap()
            .unwrap();
        assert_eq!(kept.actor_uri, VICTIM_URI);
        assert_eq!(kept.public_key_pem, victim_pub.trim());
    }

    #[test]
    fn inbox_accepts_honest_signed_follow() {
        let services = test_services("honest-inbox");
        let (victim_priv, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let victim_doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        upsert_remote_actor_from_doc(&services, VICTIM_URI, &victim_doc).expect("honest upsert");

        let local = services.create_account("local", None).unwrap();
        let base = services.public_url.trim_end_matches('/').to_string();
        let activity = json!({
            "id": "https://victim.invalid/activities/honest-follow-1",
            "type": "Follow",
            "actor": VICTIM_URI,
            "object": format!("{base}/users/{}", local.id),
        });
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &victim_key, &victim_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(
            resp.status(),
            StatusCode::ACCEPTED,
            "honest signed activity must still be accepted"
        );
    }

    #[test]
    fn inbox_block_and_undo_block_roundtrip() {
        let services = test_services("block-inbox");
        let (remote_priv, remote_pub) = keypair_pems();
        let remote_key = format!("{VICTIM_URI}#main-key");
        let remote_doc = actor_doc(VICTIM_URI, "victim", &remote_key, Some(VICTIM_URI), &remote_pub);
        upsert_remote_actor_from_doc(&services, VICTIM_URI, &remote_doc).expect("honest upsert");

        let local = services.create_account("local", None).unwrap();
        let base = services.public_url.trim_end_matches('/').to_string();
        let local_uri = format!("{base}/users/{}", local.id);
        let remote_id = services
            .accounts
            .find_by_uri(VICTIM_URI)
            .unwrap()
            .unwrap()
            .id;

        // Inbound `Block`: the remote actor blocks the local user.
        let block_id = "https://victim.invalid/activities/block-1";
        let activity = json!({
            "id": block_id,
            "type": "Block",
            "actor": VICTIM_URI,
            "object": local_uri,
        });
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &remote_key, &remote_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED, "Block must be accepted");
        assert!(
            services.blocks.is_blocking(&remote_id, &local.id).unwrap(),
            "inbound Block records the block"
        );

        // Replay converges without error.
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &remote_key, &remote_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED, "Block replay must be accepted");

        // Inbound `Undo(Block)` removes it.
        let undo = json!({
            "id": "https://victim.invalid/activities/undo-block-1",
            "type": "Undo",
            "actor": VICTIM_URI,
            "object": {
                "id": block_id,
                "type": "Block",
                "actor": VICTIM_URI,
                "object": local_uri,
            },
        });
        let body = serde_json::to_vec(&undo).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &remote_key, &remote_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED, "Undo(Block) must be accepted");
        assert!(
            !services.blocks.is_blocking(&remote_id, &local.id).unwrap(),
            "inbound Undo(Block) removes the block"
        );
    }

}

/// Slice B end-to-end (`docs/replies.md` §6): a reply to an unknown parent
/// is stored pending (never flattened), withheld from reads, and linked
/// exactly once when the parent later arrives — driven by the durable
/// resolver jobs, with zero network I/O in this hermetic flow.
#[cfg(test)]
mod reply_resolution_tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::http::{HeaderName, HeaderValue};
    use serde_json::json;

    use crate::activitypub::signatures::{
        generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
        sign_request_headers,
    };
    use crate::application::ports::{
        ActivityDedup, BlobStore, Clock, IdGenerator, Job, JobKind, NoopStreamingSink, PortError,
        ReplyParentResolvePayload,
    };
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::{
        format_http_date, upsert_remote_actor_from_doc, SafeHttpClient, SafeHttpConfig,
    };
    use crate::federation::retry::DestinationTracker;
    use crate::jobs::worker::JobWorker;
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    const ROCKY_URI: &str = "https://remote.invalid/users/rocky";
    const PARENT_URI: &str = "https://remote.invalid/notes/parent-1";

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-reply-resolve-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        }
    }

    fn actor_doc(id: &str, username: &str, key_id: &str, pem: &str) -> Value {
        json!({
            "id": id,
            "type": "Person",
            "preferredUsername": username,
            "inbox": format!("{id}/inbox"),
            "publicKey": {"id": key_id, "type": "Key", "owner": id, "publicKeyPem": pem},
        })
    }

    fn signed_inbox_headers(
        services: &AppServices,
        path: &str,
        body: &[u8],
        key_id: &str,
        private_pem: &str,
    ) -> HeaderMap {
        let date = format_http_date(services.clock.now_ms());
        let host = "pinetree.local";
        let signed =
            sign_request_headers("POST", path, host, &date, Some(body), key_id, private_pem)
                .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_str("date").unwrap(),
            HeaderValue::from_str(&date).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("host").unwrap(),
            HeaderValue::from_str(host).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("digest").unwrap(),
            HeaderValue::from_str(&signed.digest.unwrap()).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("signature").unwrap(),
            HeaderValue::from_str(&signed.signature).unwrap(),
        );
        headers
    }

    fn deliver(services: &AppServices, activity: &Value, key_id: &str, private_pem: &str) {
        let body = serde_json::to_vec(activity).unwrap();
        let headers = signed_inbox_headers(services, "/inbox", &body, key_id, private_pem);
        let resp = process_inbox(services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    fn worker_for(services: &Arc<AppServices>) -> JobWorker {
        JobWorker {
            clock: services.clock.clone(),
            ids: services.ids.clone(),
            jobs: services.jobs.clone(),
            notifications: services.notifications.clone(),
            streaming: services.streaming.clone(),
            services: services.clone(),
            delivery_tracker: Arc::new(DestinationTracker::new()),
            lease_duration_ms: 60_000,
            base_backoff_ms: 10,
        }
    }

    /// Claim every pending job, returning them oldest-first. Claimed jobs
    /// stay leased to this caller; consumers `complete` what they inspect.
    fn drain_claims(services: &AppServices) -> Vec<Job> {
        let mut out = Vec::new();
        while let Some(job) = services.jobs.claim_next(60_000).expect("claim") {
            out.push(job);
        }
        out
    }

    #[test]
    fn unknown_parent_stays_pending_then_links_on_arrival() {
        let services = Arc::new(test_services("pending-arrival"));
        let (priv_key, pub_key) = generate_rsa_keypair_2048().unwrap();
        let (private_pem, public_pem) = (
            private_key_to_pkcs8_pem(&priv_key),
            public_key_to_spki_pem(&pub_key),
        );
        let key_id = format!("{ROCKY_URI}#main-key");
        upsert_remote_actor_from_doc(&services, ROCKY_URI, &actor_doc(ROCKY_URI, "rocky", &key_id, &public_pem))
            .expect("honest upsert");

        // 1. Reply to an unknown parent: accepted, stored pending — never a
        // root — with its resolver job committed atomically.
        let reply_uri = "https://remote.invalid/notes/reply-1";
        deliver(
            &services,
            &json!({
                "id": "https://remote.invalid/activities/create-reply-1",
                "type": "Create",
                "actor": ROCKY_URI,
                "object": {
                    "id": reply_uri,
                    "type": "Note",
                    "attributedTo": ROCKY_URI,
                    "content": "a reply to a not-yet-seen note",
                    "inReplyTo": PARENT_URI,
                    "to": ["https://www.w3.org/ns/activitystreams#Public"],
                },
            }),
            &key_id,
            &private_pem,
        );
        let reply = services
            .notes
            .find_by_uri(reply_uri)
            .expect("find reply")
            .expect("reply stored, not dropped");
        assert_eq!(reply.reply_id, None, "no fabricated parent link");
        assert_eq!(
            reply.reply_parent_uri.as_deref(),
            Some(PARENT_URI),
            "unresolved inReplyTo preserved"
        );
        assert!(
            matches!(services.notes_show(&reply.id, None), Err(PortError::NotFound(_))),
            "pending reply withheld from note detail"
        );
        let jobs = drain_claims(&services);
        assert_eq!(jobs.len(), 2, "streaming + resolver committed atomically");
        let resolve_job = jobs
            .iter()
            .find(|j| j.kind == JobKind::ReplyParentResolve)
            .expect("resolver job enqueued for the pending URI");
        let payload: ReplyParentResolvePayload =
            serde_json::from_slice(&resolve_job.payload).expect("resolver payload decodes");
        assert_eq!(payload.parent_uri, PARENT_URI);
        // Simulate the resolver having run while the parent was missing
        // (retry behavior itself is covered in conformance R10): consume
        // both jobs so the arrival below starts clean.
        for job in &jobs {
            services.jobs.complete(&job.id).expect("consume");
        }

        // 2. The parent arrives: accepted, and its arrival enqueues a fresh
        // resolver for the waiting child.
        deliver(
            &services,
            &json!({
                "id": "https://remote.invalid/activities/create-parent-1",
                "type": "Create",
                "actor": ROCKY_URI,
                "object": {
                    "id": PARENT_URI,
                    "type": "Note",
                    "attributedTo": ROCKY_URI,
                    "content": "the parent, arriving late",
                    "to": ["https://www.w3.org/ns/activitystreams#Public"],
                },
            }),
            &key_id,
            &private_pem,
        );
        let parent = services
            .notes
            .find_by_uri(PARENT_URI)
            .expect("find parent")
            .expect("parent stored");
        let arrival_jobs = drain_claims(&services);
        assert!(
            arrival_jobs
                .iter()
                .any(|j| j.kind == JobKind::ReplyParentResolve),
            "parent arrival schedules resolution for waiting children"
        );
        for job in &arrival_jobs {
            services.jobs.complete(&job.id).expect("consume");
        }
        // Re-enqueue one resolver (as the arrival did) and run it through
        // the real worker: the cached parent links with zero network I/O.
        let resolve = crate::application::commands::inbound::replies::build_reply_parent_resolve_job(
            &services, PARENT_URI,
        )
        .expect("resolver job builds");
        services.jobs.enqueue_jobs(vec![resolve]).expect("enqueue");
        let worker = worker_for(&services);
        assert!(worker.process_one().expect("process"), "resolver claimed");
        assert!(
            !worker.process_one().expect("drain"),
            "no work remains after linking"
        );

        // 3. Linked exactly once: reply_id set, pending cleared, counter
        // bumped, index swapped, note visible again.
        let linked = services
            .notes
            .find_by_uri(reply_uri)
            .expect("find linked")
            .expect("reply still stored");
        assert_eq!(linked.reply_id.as_deref(), Some(parent.id.as_str()));
        assert_eq!(linked.reply_parent_uri, None);
        let parent_after = services
            .notes
            .find_by_uri(PARENT_URI)
            .expect("find parent again")
            .expect("parent");
        assert_eq!(parent_after.replies_count, 1, "counted exactly once");
        assert!(
            services
                .notes
                .pending_reply_children(PARENT_URI, 10)
                .expect("pending after link")
                .is_empty(),
            "pending row gone after link"
        );
        assert!(
            services.notes_show(&reply.id, None).is_ok(),
            "linked reply readable again"
        );
    }
}

// ---------------------------------------------------------------------------
// `Update(Note)` round-trip (§8; Sharkey `notes/edit` federation parity)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod update_note_tests {
    use super::*;
    use serde_json::json;

    use crate::activitypub::objects::{render_note_object, render_note_update_activity};
    use crate::application::ports::CreateAccountInput;

    const CAROL_URI: &str = "https://remote.example/users/carol";
    const NOTE_URI: &str = "https://remote.example/notes/n1";

    fn services() -> AppServices {
        crate::application::commands::tests::test_services("update-note")
    }

    fn remote_carol(services: &AppServices) -> Account {
        services
            .accounts
            .create_account(CreateAccountInput {
                handle: "carol".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(CAROL_URI.to_string()),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .expect("remote author")
    }

    /// Cached remote copy with a resolvable `uri`, through the inbound
    /// (deduping) create path.
    fn cached_note(services: &AppServices, author_id: &str, uri: &str, text: &str) -> Note {
        let mut input = CreateNoteInput::simple_public(author_id, text);
        input.uri = Some(uri.to_string());
        services
            .dedup
            .record_note_activity("update-note-tests:seed", input, Vec::new())
            .expect("seed cached copy")
            .expect("first seed applies")
    }

    #[test]
    fn update_note_round_trip_and_replay_converge() {
        let services = services();
        let carol = remote_carol(&services);
        let cached = cached_note(&services, &carol.id, NOTE_URI, "v1");

        // The embedded doc the remote server would send for the edited
        // note (rendered with our own renderer, as the peer would).
        let mut edited = cached.clone();
        edited.text = Some("v2".to_string());
        let doc = render_note_object(&services, &edited, true).expect("render edited note");
        assert_eq!(doc["id"], json!(NOTE_URI));
        assert_eq!(doc["attributedTo"], json!(CAROL_URI));

        let resp = handle_update_note(&services, &doc, "upd-1", CAROL_URI).expect("handle");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let stored = services
            .notes
            .get_note(&cached.id)
            .unwrap()
            .expect("cached note");
        assert_eq!(stored.text.as_deref(), Some("v2"));
        let updated_at = stored.updated_at_ms.expect("edit stamps updated_at");
        assert_eq!(services.notes.note_edits(&cached.id).unwrap().len(), 1);

        // Replay of the same activity id: accepted, converged, no change.
        let resp = handle_update_note(&services, &doc, "upd-1", CAROL_URI).expect("replay");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let stored = services
            .notes
            .get_note(&cached.id)
            .unwrap()
            .expect("cached note");
        assert_eq!(
            stored.updated_at_ms,
            Some(updated_at),
            "replay changes nothing"
        );
        assert_eq!(
            services.notes.note_edits(&cached.id).unwrap().len(),
            1,
            "still one history row after replay"
        );
    }

    #[test]
    fn update_note_authority_gates() {
        let services = services();
        let carol = remote_carol(&services);
        let cached = cached_note(&services, &carol.id, NOTE_URI, "v1");
        let mut edited = cached.clone();
        edited.text = Some("v2".to_string());
        let base_doc = render_note_object(&services, &edited, true).expect("render");

        // (a) Attribution mismatch: `attributedTo` names someone else while
        // the signer is carol → accepted, no change.
        let mut forged = base_doc.clone();
        forged["attributedTo"] = json!("https://remote.example/users/mallory");
        let resp = handle_update_note(&services, &forged, "upd-forged", CAROL_URI)
            .expect("handle forged");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let stored = services
            .notes
            .get_note(&cached.id)
            .unwrap()
            .expect("cached note");
        assert_eq!(
            stored.text.as_deref(),
            Some("v1"),
            "mismatched attribution changes nothing"
        );
        assert!(stored.updated_at_ms.is_none());
        assert!(services.notes.note_edits(&cached.id).unwrap().is_empty());

        // (b) Signer is not the author: `handle_inbound_update_note` with a
        // foreign actor uri converges to `Ok(None)`.
        let none = services
            .handle_inbound_update_note(
                "upd-foreign",
                NOTE_URI,
                "https://remote.example/users/mallory",
                Some("v2".to_string()),
                None,
                None,
                None,
            )
            .expect("foreign signer converges");
        assert!(none.is_none());
        let stored = services
            .notes
            .get_note(&cached.id)
            .unwrap()
            .expect("cached note");
        assert_eq!(stored.text.as_deref(), Some("v1"));

        // (c) Local-author protection: a cached copy owned locally is never
        // rewritten from federation input.
        let local = services.create_account("local", None).expect("local account");
        let local_uri = "https://example.local/notes/local-1".to_string();
        let mut local_input = CreateNoteInput::simple_public(&local.id, "local v1");
        local_input.uri = Some(local_uri.clone());
        let local_note = services
            .dedup
            .record_note_activity("update-note-tests:seed-local", local_input, Vec::new())
            .expect("seed local copy")
            .expect("local seed applies");
        let mut local_edited = local_note.clone();
        local_edited.text = Some("local v2".to_string());
        let local_doc =
            render_note_object(&services, &local_edited, true).expect("render local");
        let base = services.public_url.trim_end_matches('/').to_string();
        let local_actor = local.actor_url(&base);
        let resp =
            handle_update_note(&services, &local_doc, "upd-local", &local_actor).expect("handle");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let stored = services
            .notes
            .get_note(&local_note.id)
            .unwrap()
            .expect("local note");
        assert_eq!(
            stored.text.as_deref(),
            Some("local v1"),
            "local notes are never rewritten from federation"
        );
        assert!(stored.updated_at_ms.is_none());
        assert!(services.notes.note_edits(&local_note.id).unwrap().is_empty());
    }

    #[test]
    fn update_activity_shape_pins() {
        let services = services();
        let carol = remote_carol(&services);
        let cached = cached_note(&services, &carol.id, NOTE_URI, "v1");
        let doc = render_note_object(&services, &cached, true).expect("render");
        let base = services.public_url.trim_end_matches('/').to_string();
        let activity = render_note_update_activity(
            &base,
            "w1",
            CAROL_URI,
            doc.clone(),
            vec![format!("{base}/followers/{}", carol.id)],
            vec!["https://www.w3.org/ns/activitystreams#Public".to_string()],
            services.clock.now_ms(),
        );
        assert_eq!(activity["type"], json!("Update"));
        assert_eq!(activity["object"], doc);
        assert_eq!(activity["actor"], json!(CAROL_URI));
        let id = activity["id"].as_str().expect("activity id");
        assert!(
            id.starts_with(&format!("{base}/activities/")),
            "update id lives under the public base URL, got {id}"
        );
    }
}

// ---------------------------------------------------------------------------
// Inbound `Like` reaction extraction (§6; Misskey wire compat)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod like_reaction_tests {
    use super::*;
    use serde_json::json;

    use crate::application::ports::{CreateAccountInput, ReactionStore};
    use crate::domain::reaction::FALLBACK_REACTION;

    const CAROL_URI: &str = "https://remote.example/users/carol";

    fn services() -> AppServices {
        crate::application::commands::tests::test_services("like-reaction")
    }

    fn remote_carol(services: &AppServices) -> Account {
        services
            .accounts
            .create_account(CreateAccountInput {
                handle: "carol".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(CAROL_URI.to_string()),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .expect("remote liker")
    }

    fn local_note(services: &AppServices, author_id: &str) -> (Note, String) {
        let note = services
            .create_note(CreateNoteInput::simple_public(author_id, "likeable"))
            .expect("local note");
        let base = services.public_url.trim_end_matches('/');
        let uri = format!("{base}/notes/{}", note.id);
        (note, uri)
    }

    fn stored_reaction(services: &AppServices, user_id: &str, note_id: &str) -> String {
        services
            .reactions
            .get_reaction(user_id, note_id)
            .expect("read reaction")
            .expect("reaction stored")
            .reaction
    }

    #[test]
    fn like_without_misskey_reaction_maps_to_fallback() {
        let services = services();
        let alice = services.create_account("alice", None).expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        // Plain Mastodon-favourite `Like`: no `_misskey_reaction`, no
        // `content` → bare-heart FALLBACK.
        let activity = json!({
            "id": "https://remote.example/likes/1",
            "type": "Like",
            "actor": CAROL_URI,
            "object": note_uri,
        });
        let resp = handle_like(&services, &activity, "like-act-1", CAROL_URI).expect("handle");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            stored_reaction(&services, &carol.id, &note.id),
            FALLBACK_REACTION
        );
    }

    #[test]
    fn like_with_misskey_reaction_stores_custom_reaction() {
        let services = services();
        let alice = services.create_account("alice", None).expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        let activity = json!({
            "id": "https://remote.example/likes/2",
            "type": "Like",
            "actor": CAROL_URI,
            "object": note_uri,
            "content": "🎉",
            "_misskey_reaction": "🎉",
        });
        let resp = handle_like(&services, &activity, "like-act-2", CAROL_URI).expect("handle");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(stored_reaction(&services, &carol.id, &note.id), "🎉");
    }

    #[test]
    fn emoji_react_with_unicode_content_stores_emoji() {
        let services = services();
        let alice = services.create_account("alice", None).expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        // Akkoma unicode-emoji shape: `EmojiReact{content}` with no tag.
        let activity = json!({
            "id": "https://akkoma.example/reactions/1",
            "type": "EmojiReact",
            "actor": CAROL_URI,
            "object": note_uri,
            "content": "🎉",
        });
        let resp = dispatch(&services, &activity, "emoji-act-1", "EmojiReact", CAROL_URI);
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(stored_reaction(&services, &carol.id, &note.id), "🎉");
    }

    #[test]
    fn emoji_react_with_tag_falls_back_to_emoji_name() {
        let services = services();
        let alice = services.create_account("alice", None).expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        // Custom-emoji shape with no `content`: fall back to the `Emoji`
        // tag `name`.
        let activity = json!({
            "id": "https://akkoma.example/reactions/2",
            "type": "EmojiReact",
            "actor": CAROL_URI,
            "object": note_uri,
            "tag": [{"type": "Emoji", "name": ":party:", "icon": {"type": "Image", "url": "https://akkoma.example/emoji/party.png"}}],
        });
        let resp = handle_like(&services, &activity, "emoji-act-2", CAROL_URI).expect("handle");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(stored_reaction(&services, &carol.id, &note.id), ":party:");
    }

    #[test]
    fn undo_emoji_react_removes_the_reaction() {
        let services = services();
        let alice = services.create_account("alice", None).expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        let react = json!({
            "id": "https://akkoma.example/reactions/3",
            "type": "EmojiReact",
            "actor": CAROL_URI,
            "object": note_uri,
            "content": "🎉",
        });
        handle_like(&services, &react, "emoji-act-3", CAROL_URI).expect("react");
        assert_eq!(stored_reaction(&services, &carol.id, &note.id), "🎉");

        let undo = json!({
            "id": "https://akkoma.example/reactions/3/undo",
            "type": "Undo",
            "actor": CAROL_URI,
            "object": {
                "id": "https://akkoma.example/reactions/3",
                "type": "EmojiReact",
                "actor": CAROL_URI,
                "object": note_uri,
            },
        });
        let resp = handle_undo(&services, &undo, "emoji-undo-3", CAROL_URI).expect("undo");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert!(services
            .reactions
            .get_reaction(&carol.id, &note.id)
            .expect("read reaction")
            .is_none());
    }
}

#[cfg(test)]
mod delete_tombstone_tests {
    use super::*;
    use serde_json::json;

    use crate::application::ports::{CreateAccountInput, CreateNoteInput};

    const MK_ACTOR: &str = "http://misskey.test/users/asf64vjyfcp90003";
    const MK_NOTE: &str = "http://misskey.test/notes/n1";

    /// Shape check: Misskey renders `Delete { object: Tombstone { id: <note uri> } }`
    /// (`ApRendererService.renderDelete`), and that shape removes an already
    /// stored remote note. This passes; the race where the Delete overtakes its
    /// Create is pinned by `delete_before_create_does_not_resurrect` below.
    #[test]
    fn misskey_tombstone_delete_removes_the_remote_note() {
        let services = crate::application::commands::tests::test_services("delete-tombstone");
        let author = services
            .accounts
            .create_account(CreateAccountInput {
                handle: "mk1".to_string(),
                host: Some("misskey.test".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(MK_ACTOR.to_string()),
                inbox: Some("http://misskey.test/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .expect("remote author");
        let note = services
            .handle_inbound_create_note(
                "http://misskey.test/notes/n1/activity",
                CreateNoteInput {
                    uri: Some(MK_NOTE.to_string()),
                    ..CreateNoteInput::simple_public(&author.id, "PERF|churn|misskey|mk1|T")
                },
                Vec::new(),
            )
            .expect("create")
            .expect("note stored");

        let activity = json!({
            "id": "http://misskey.test/793fb4d9-b03d-480d-b9a6-f9c5e90edace",
            "type": "Delete",
            "actor": MK_ACTOR,
            "object": {
                "id": MK_NOTE,
                "type": "Tombstone",
                "formerType": "Note"
            },
        });
        let resp = handle_delete(
            &services,
            &activity,
            "http://misskey.test/793fb4d9-b03d-480d-b9a6-f9c5e90edace",
            MK_ACTOR,
        )
        .expect("handle");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // `get_note` reports a deleted note as absent.
        assert!(
            services.notes.get_note(&note.id).expect("read").is_none(),
            "Delete(Tombstone) must delete the remote note"
        );
    }

    /// Bench S6 residue (2026-10-17): Misskey and Catodon post then delete a
    /// note within milliseconds. When the Delete is handled while the Create is
    /// still in flight, `handle_delete` finds no note, returns 202, and remembers
    /// nothing; the Create then stores the note and it stays live on pinetree
    /// (`fast_delete_probe`: 2 of 5 notes resident on pt-ks, 1 of 5 on pt-mk; every Delete answered 202).
    /// A Delete that arrives first must keep the late Create from resurrecting it.
    /// Fixed by Path 4 of `handle_delete` (tombstone) and the tombstone check in
    /// `create_note_in_txn`.
    #[test]
    fn delete_before_create_does_not_resurrect() {
        let services = crate::application::commands::tests::test_services("delete-before-create");
        let author = services
            .accounts
            .create_account(CreateAccountInput {
                handle: "mk2".to_string(),
                host: Some("misskey.test".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(MK_ACTOR.to_string()),
                inbox: Some("http://misskey.test/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .expect("remote author");
        let delete_id = "http://misskey.test/late-delete-1";
        let delete = json!({
            "id": delete_id,
            "type": "Delete",
            "actor": MK_ACTOR,
            "object": { "id": MK_NOTE, "type": "Tombstone", "formerType": "Note" },
        });
        handle_delete(&services, &delete, delete_id, MK_ACTOR).expect("delete first");
        services
            .handle_inbound_create_note(
                "http://misskey.test/notes/n1/activity",
                CreateNoteInput {
                    uri: Some(MK_NOTE.to_string()),
                    ..CreateNoteInput::simple_public(&author.id, "PERF|churn|misskey|mk2|T")
                },
                Vec::new(),
            )
            .expect("late create");
        assert!(
            services.notes.find_by_uri(MK_NOTE).expect("read").is_none(),
            "late Create must not resurrect a note whose Delete already arrived"
        );
    }
}

// ---------------------------------------------------------------------------
// Inbound poll votes (P6): payload shapes per software
// ---------------------------------------------------------------------------

#[cfg(test)]
mod inbound_vote_tests {
    use super::*;
    use serde_json::json;

    use crate::application::ports::CreateAccountInput;
    use crate::domain::poll::PollInput;

    const CAROL_URI: &str = "https://remote.example/users/carol";

    fn setup() -> (AppServices, Note, String) {
        let services = crate::application::commands::tests::test_services("inbound-vote");
        let alice = services.create_account("alice", None).expect("alice");
        services
            .accounts
            .create_account(CreateAccountInput {
                handle: "carol".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(CAROL_URI.to_string()),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .expect("carol");
        let mut input = CreateNoteInput::simple_public(&alice.id, "poll?");
        input.poll = Some(PollInput {
            choices: vec!["alpha".to_string(), "beta".to_string()],
            multiple: false,
            expires_at_ms: None,
        });
        input.has_poll = true;
        let note = services.create_note(input).expect("poll note");
        let uri = format!("{}/notes/{}", services.public_url.trim_end_matches('/'), note.id);
        (services, note, uri)
    }

    fn votes(services: &AppServices, note: &Note) -> Vec<u64> {
        services.polls.get_poll(&note.id).unwrap().unwrap().votes
    }

    fn create(object: Value) -> Value {
        json!({ "type": "Create", "actor": CAROL_URI, "object": object })
    }

    /// Misskey/Sharkey/Catodon: a `Note` carrying `name` + `inReplyTo`.
    #[test]
    fn note_shaped_vote_is_tallied() {
        let (services, note, uri) = setup();
        let activity = create(json!({
            "id": "https://remote.example/notes/v1", "type": "Note",
            "attributedTo": CAROL_URI, "inReplyTo": uri, "name": "beta",
            "to": [uri.clone()], "content": null,
        }));
        handle_create(&services, &activity, "https://remote.example/a/v1", CAROL_URI).unwrap();
        assert_eq!(votes(&services, &note), vec![0, 1]);
    }

    /// Pleroma/Akkoma: `Create(Answer{name, inReplyTo})`. Previously the
    /// parser rejected the type and the vote vanished without a trace.
    #[test]
    fn answer_shaped_vote_is_tallied() {
        let (services, note, uri) = setup();
        let activity = create(json!({
            "id": "https://remote.example/answers/1", "type": "Answer",
            "attributedTo": CAROL_URI, "inReplyTo": uri, "name": "alpha",
            "to": [uri.clone()],
        }));
        handle_create(&services, &activity, "https://remote.example/a/v2", CAROL_URI).unwrap();
        assert_eq!(votes(&services, &note), vec![1, 0]);
    }

    /// An `Answer` for an unknown parent is dropped, never stored as a note.
    #[test]
    fn answer_for_unknown_parent_stores_nothing() {
        let (services, _note, _uri) = setup();
        let activity = create(json!({
            "id": "https://remote.example/answers/2", "type": "Answer",
            "attributedTo": CAROL_URI, "inReplyTo": "https://elsewhere.example/notes/x",
            "name": "alpha",
        }));
        handle_create(&services, &activity, "https://remote.example/a/v3", CAROL_URI).unwrap();
        assert!(services.notes.find_by_uri("https://remote.example/answers/2").unwrap().is_none());
    }
}
