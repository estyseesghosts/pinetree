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
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;

use crate::activitypub::actor::parse_url_host;
use crate::activitypub::objects::extract_object_uri;
use crate::activitypub::signatures::{
    VerifyOutcome, apply_sig_mode, parse_first_legacy_signature, parse_signature_input,
    rfc9421_mode, scheme_of_public_url, verify_http_signature, verify_rfc9421_signature,
};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::KeyFetchNegativeCache;
use crate::federation::delivery::{MAX_AP_JSON_BYTES, resolve_remote_public_key};

pub(crate) mod announce;
pub(crate) mod common;
pub(crate) mod create;
pub(crate) mod delete;
pub(crate) mod flag;
pub(crate) mod follow;
pub(crate) mod like;
pub(crate) mod update;
pub(crate) mod vote;

use self::announce::handle_announce;
use self::common::{canonical_activity_type, non_empty_str};
use self::create::handle_create;
use self::delete::handle_delete;
use self::flag::handle_flag;
use self::follow::{
    handle_accept, handle_block, handle_clip_membership, handle_follow, handle_move,
    handle_quote_request, handle_reject, handle_undo,
};
use self::like::handle_like;
use self::update::handle_update;

pub use self::common::ingest_fetched_remote_note;
pub(crate) use self::common::resolve_note_target_id;

/// Maximum accepted inbox POST body (64 KiB, same cap as outbound AP JSON).
pub const MAX_INBOX_BODY_BYTES: usize = MAX_AP_JSON_BYTES;

/// Maximum allowed `Date` header skew in either direction (5 minutes).
pub const MAX_CLOCK_SKEW_SECS: i64 = 300;

/// Extraction-layer cap for both inbox routes (Batch G): the router applies
/// this as a per-route [`DefaultBodyLimit`], so oversized bodies fail with
/// 413 while Axum is still collecting the body — before the handler buffers
/// anything. `process_inbox` keeps its own length check as a backstop for
/// direct (non-router) callers.
pub fn inbox_body_limit() -> DefaultBodyLimit {
    DefaultBodyLimit::max(MAX_INBOX_BODY_BYTES)
}

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

pub(crate) fn process_inbox(
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
            crate::fed_debug!(
                "in  reject reason=signature type={activity_type} id={activity_id:?}"
            );
            return resp;
        }
    };

    // Actor authority: the activity actor must be the key owner.
    let actor_uri = match non_empty_str(activity.get("actor")) {
        Some(a) => a,
        None => {
            crate::fed_debug!(
                "in  reject reason=missing-actor type={activity_type} id={activity_id:?}"
            );
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

    let resp = dispatch(
        services,
        &activity,
        &activity_id,
        &activity_type,
        &actor_uri,
    );
    crate::fed_debug!(
        "in  done type={activity_type} id={activity_id:?} actor={actor_uri:?} status={}",
        resp.status().as_u16()
    );
    resp
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

/// True when the request carries RFC 9421 (`httpbis`) signature fields.
/// Either `Signature-Input` (real RFC 9421 input) or the non-standard
/// `Signature-Agent` marker counts: the former drives verification, the
/// latter only permissive logging (kept for backwards-compatible
/// detection; it never authenticates on its own).
pub fn has_rfc9421_headers(headers: &HeaderMap) -> bool {
    headers.contains_key("signature-input") || headers.contains_key("signature-agent")
}

/// Verify the HTTP signature(s), resolving each signer `keyId` against the
/// cached remote public key (refreshing once from the network on failure).
/// Returns the key owner's actor URI on success (from the cache, or from the
/// freshly fetched document after the same parse checks are re-applied by
/// the fetch-and-upsert path). Callers must compare `activity.actor` against
/// this owner, never against a string derived from the keyId (Slice 2, C1).
///
/// Batch E modes (`federation.rfc9421_mode`, read process-wide): in
/// `permissive` a valid legacy signature authenticates and RFC 9421 fields,
/// when present, are verified rather than merely logged (either valid
/// scheme authenticates dual-signed requests; dual-owner disagreement fails
/// closed). In `enforce` a valid RFC 9421 signature under the supported
/// profile (`signatures` module docs) is required and legacy-only requests
/// are rejected.
fn authenticate(
    services: &AppServices,
    path_and_query: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<String, Response> {
    let mode = rfc9421_mode();
    if has_rfc9421_headers(headers) {
        crate::fed_debug!("in  rfc9421-present mode={mode:?} (verifying per configured mode)");
    }
    let legacy = attempt_legacy_signature(services, path_and_query, headers, body);
    let rfc9421 = attempt_rfc9421_signature(services, path_and_query, headers, body);
    match apply_sig_mode(mode, legacy, rfc9421) {
        Ok(owner) => Ok(owner),
        Err(detail) => {
            crate::fed_debug!(
                "in  reject reason=signature-invalid mode={mode:?} detail={detail:?}"
            );
            Err(unauthorized())
        }
    }
}

/// Resolve `key_id` (cached key, fetch on miss, one refresh on rotation)
/// and run `verify` against the resolved PEM. Returns the key owner's actor
/// URI on success; every failure shape collapses to [`VerifyOutcome::Failed`]
/// with a log-safe detail (the caller maps it to 401).
fn resolve_and_verify(
    services: &AppServices,
    key_id: &str,
    verify: &dyn Fn(&str) -> Result<(), String>,
) -> VerifyOutcome {
    let client = &services.federation_http;
    match resolve_remote_public_key(services, client, key_id, false) {
        Ok((owner, pem)) => match verify(&pem) {
            Ok(()) => VerifyOutcome::Verified(owner),
            Err(_) => {
                // Stale cached key (e.g. rotated): refresh once and re-verify —
                // unless the key fetch just failed (negative cache), in which
                // case the initial miss already paid for the fetch and a
                // forced retry would be pure amplification.
                if !KeyFetchNegativeCache::should_fetch(key_id, services.clock.now_ms()) {
                    crate::fed_debug!(
                        "in  signature-retry-skipped keyId={key_id:?} (negative cache)"
                    );
                    return VerifyOutcome::Failed(format!("key unresolvable: {key_id}"));
                }
                crate::fed_debug!(
                    "in  signature-retry keyId={key_id:?} (cached key failed; refreshing)"
                );
                match resolve_remote_public_key(services, client, key_id, true) {
                    Ok((fresh_owner, fresh_pem)) => match verify(&fresh_pem) {
                        Ok(()) => VerifyOutcome::Verified(fresh_owner),
                        Err(detail) => VerifyOutcome::Failed(detail),
                    },
                    Err(_) => VerifyOutcome::Failed(format!("key unresolvable: {key_id}")),
                }
            }
        },
        Err(_) => VerifyOutcome::Failed(format!("key unresolvable: {key_id}")),
    }
}

/// Legacy draft-cavanaugh attempt: `None` when no `Signature`/`Authorization`
/// header is present, otherwise the verification outcome (unparsable values
/// are `Failed`, never skipped).
fn attempt_legacy_signature(
    services: &AppServices,
    path_and_query: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Option<VerifyOutcome> {
    let parsed = match parse_first_legacy_signature(headers)? {
        Ok(parsed) => parsed,
        Err(err) => {
            return Some(VerifyOutcome::Failed(format!(
                "legacy signature unparsable: {err}"
            )));
        }
    };
    let key_id = parsed.key_id.clone();
    let now_ms = services.clock.now_ms();
    Some(resolve_and_verify(services, &key_id, &|pem| {
        verify_http_signature(
            "POST",
            path_and_query,
            headers,
            Some(body),
            pem,
            Some(now_ms),
            MAX_CLOCK_SKEW_SECS,
        )
        .map(|_| ())
    }))
}

/// RFC 9421 attempt: `None` when no `Signature-Input` header is present,
/// otherwise the verification outcome under the supported profile.
fn attempt_rfc9421_signature(
    services: &AppServices,
    path_and_query: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Option<VerifyOutcome> {
    let raw = headers
        .get("signature-input")
        .and_then(|v| v.to_str().ok())?;
    // Parse first for the keyId (no verification yet); the full check below
    // re-parses, keeping the verifier self-contained.
    let key_id = match parse_signature_input(raw) {
        Ok(parsed) => parsed.key_id,
        Err(err) => {
            return Some(VerifyOutcome::Failed(format!(
                "Signature-Input unparsable: {err}"
            )));
        }
    };
    let scheme = scheme_of_public_url(&services.public_url);
    let now_ms = services.clock.now_ms();
    Some(resolve_and_verify(services, &key_id, &|pem| {
        verify_rfc9421_signature(
            "POST",
            path_and_query,
            scheme,
            headers,
            Some(body),
            pem,
            Some(now_ms),
            MAX_CLOCK_SKEW_SECS,
        )
        .map(|_| ())
    }))
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
fn check_object_authority(activity_type: &str, activity: &Value, signer_actor: &str) -> bool {
    match activity_type {
        "Follow" | "Accept" | "Reject" | "Undo" | "Announce" | "Like" | "Delete" | "Block"
        | "QuoteRequest" => true,
        // `Add`/`Remove` for clip collections pass through: dispatch
        // re-validates (local targets are never mutated remotely; remote
        // targets require the signer to be the collection owner, verified
        // by fetching the collection) — a blanket host match here would
        // 401 legitimate owner-signed membership sync.
        "Add" | "Remove" => true,
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
                // `Update(Note|Article|Page|OrderedCollection)` is deferred
                // to §8 / clip sync: dispatch re-validates, so the gate
                // passes all through.
                Some(Value::Object(map))
                    if matches!(
                        map.get("type").and_then(Value::as_str).unwrap_or(""),
                        "Note" | "Article" | "Page" | "Question" | "OrderedCollection"
                    ) =>
                {
                    true
                }
                // WP5 string-URI `Update(Note)` fetch-and-apply: a string
                // object naming a note passes through — dispatch fetches the
                // document and requires `attributedTo == signer` plus exact
                // authority before applying anything.
                Some(Value::String(_)) => true,
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
        // Clip membership sync (federated clips + pages handoff, Phase 2):
        // prompt cache invalidation; full-state pulls converge. Generic
        // `Add`/`Remove` for any other target (Misskey dispatches these to
        // list/clip membership handlers) are transport-accepted and ignored:
        // pinetree owns no other remotely-mutable collections, and remote
        // actors never mutate local collections — see
        // `handle_clip_membership`. Relay receive+forward (Misskey relay
        // actor + `RelayService.forward`): pinetree runs no relay actor —
        // `fed_policy::relay_decision()` documents the refusal;
        // relay-addressed traffic lands here as accept-and-ignore, never
        // as fan-out.
        "Add" => handle_clip_membership(services, activity, activity_id, actor_uri, true),
        "Remove" => handle_clip_membership(services, activity, activity_id, actor_uri, false),
        // Batch P `i/move` federation half: the mover's old followers learn
        // the new account URI and refollow (Misskey `ApInboxService.move`
        // parity, plus the forged-move rejection the handoff requires).
        "Move" => handle_move(services, activity, activity_id, actor_uri),
        "QuoteRequest" => handle_quote_request(services, activity, activity_id, actor_uri),
        // Inbound `Flag` (Batch N WS5; upstream `ApInboxService.flag`
        // parity): first mentioned local user becomes the report target.
        "Flag" => handle_flag(services, activity, activity_id, actor_uri),
        _ => {
            crate::fed_debug!(
                "in  ignore type={activity_type} id={activity_id:?} reason=unsupported-type"
            );
            Ok(accepted())
        }
    };
    outcome.unwrap_or_else(|_| internal_error())
}

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
