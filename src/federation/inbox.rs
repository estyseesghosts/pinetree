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
    parse_note_audience, parse_remote_note, sanitize_html_to_mfm,
};
use crate::activitypub::signatures::{
    actor_uri_from_key_id, parse_signature_header, verify_http_signature,
};
use crate::application::commands::AppServices;
use crate::application::ports::{Clock, CreateNoteInput, PortError, TimelineCursor, UpdateAccountPatch};
use crate::domain::account::Account;
use crate::domain::note::{Note, Visibility};
use crate::federation::delivery::{
    MAX_AP_JSON_BYTES, fetch_and_upsert_remote_actor, fetch_remote_note, resolve_remote_public_key,
};
use crate::media::remote_cache::fetch_and_cache_remote_media;

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
    if body.len() > MAX_INBOX_BODY_BYTES {
        return payload_too_large();
    }
    let activity: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return bad_request(),
    };
    let activity_type = match non_empty_str(activity.get("type")) {
        Some(t) => t,
        None => return bad_request(),
    };
    let activity_id = match non_empty_str(activity.get("id")) {
        Some(id) => id,
        None => return bad_request(),
    };

    // Personal inbox: the addressed account must exist and be local.
    // (Lenient addressing: continue to shared processing regardless of
    // whether the activity targets this account.)
    if let Some(uid) = user_id {
        match services.accounts.find_by_id(uid) {
            Ok(Some(acc)) if acc.is_local() => {}
            Ok(_) => return not_found(),
            Err(_) => return internal_error(),
        }
    }

    let path_and_query = match user_id {
        Some(uid) => format!("/users/{uid}/inbox"),
        None => "/inbox".to_string(),
    };
    let key_id = match authenticate(services, &path_and_query, &headers, &body) {
        Ok(key_id) => key_id,
        Err(resp) => return resp,
    };
    let signer_actor = actor_uri_from_key_id(&key_id);

    // Actor authority: the activity actor must be the signer.
    let actor_uri = match non_empty_str(activity.get("actor")) {
        Some(a) => a,
        None => return unauthorized(),
    };
    if actor_uri != signer_actor {
        return unauthorized();
    }

    // Object authority for types whose objects the signer claims to own.
    if !check_object_authority(&activity_type, &activity, &signer_actor) {
        return unauthorized();
    }

    dispatch(services, &activity, &activity_id, &activity_type, &actor_uri)
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

/// Verify the HTTP signature, resolving the signer `keyId` against the cached
/// remote public key (refreshing once from the network on failure).
/// Returns the signer `keyId` on success.
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
        return Err(unauthorized());
    };
    let parsed = parse_signature_header(raw).map_err(|_| unauthorized())?;
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
        Ok((_, pem)) => match verify(&pem) {
            Ok(_) => Ok(key_id),
            Err(_) => {
                // Stale cached key (e.g. rotated): refresh once and re-verify.
                match resolve_remote_public_key(services, client, &key_id, true) {
                    Ok((_, fresh_pem)) => {
                        verify(&fresh_pem).map(|_| key_id).map_err(|_| unauthorized())
                    }
                    Err(_) => Err(unauthorized()),
                }
            }
        },
        Err(_) => Err(unauthorized()),
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
/// origin-checked target resolution at dispatch. `Create`/`Delete` objects
/// are content the signer claims to own and must live on the signer's host;
/// `Update` objects must be the signer's own actor document.
fn check_object_authority(
    activity_type: &str,
    activity: &Value,
    signer_actor: &str,
) -> bool {
    match activity_type {
        "Follow" | "Accept" | "Reject" | "Undo" | "Announce" | "Like" => true,
        "Create" | "Delete" => {
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
                // Update(Note) is deferred: dispatch ignores it.
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
        "Accept" => handle_follow_response(services, activity, activity_id, actor_uri, true),
        "Reject" => handle_follow_response(services, activity, activity_id, actor_uri, false),
        "Undo" => handle_undo(services, activity, activity_id, actor_uri),
        "Create" => handle_create(services, activity, activity_id, actor_uri),
        "Announce" => handle_announce(services, activity, activity_id, actor_uri),
        "Like" => handle_like(services, activity, activity_id, actor_uri),
        "Delete" => handle_delete(services, activity, activity_id, actor_uri),
        "Update" => handle_update(services, activity, activity_id, actor_uri),
        _ => Ok(accepted()),
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
    let Some(followee) = resolve_local_account(services, &object_uri)? else {
        return Ok(accepted());
    };
    if !followee.is_local() {
        return Ok(accepted());
    }
    services.handle_inbound_follow(activity_id, &follower.id, &followee.id, false)?;
    Ok(accepted())
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
                "Like" => {
                    let liker = resolve_remote_account(services, actor_uri)?;
                    let Some(target_uri) = extract_object_uri(map.get("object")) else {
                        return Ok(accepted());
                    };
                    let Some(note) = find_note_by_uri(services, &target_uri)? else {
                        return Ok(accepted());
                    };
                    services.handle_inbound_unlike(activity_id, &liker.id, &note.id)?;
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
    let client = &services.federation_http;
    let parsed: ParsedRemoteNote = match activity.get("object") {
        Some(Value::String(uri)) => {
            let uri = uri.trim();
            if uri.is_empty() {
                return Ok(accepted());
            }
            match fetch_remote_note(services, client, uri) {
                Ok(parsed) => parsed,
                Err(_) => return Ok(accepted()),
            }
        }
        Some(obj @ Value::Object(_)) => match parse_remote_note(obj) {
            Ok(parsed) => parsed,
            Err(_) => return Ok(accepted()),
        },
        _ => return Ok(accepted()),
    };
    // The note author must be the signer.
    if parsed.attributed_to != actor_uri {
        return Ok(unauthorized());
    }
    let author = resolve_remote_account(services, actor_uri)?;
    ingest_remote_note(services, activity_id, &parsed, &author, true)?;
    Ok(accepted())
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
        renote_of: Some(target_id),
        file_ids: Vec::new(),
        visible_users: Vec::new(),
        reaction_acceptance: None,
        no_extract_mentions: false,
        no_extract_hashtags: false,
        no_extract_emojis: false,
        has_poll: false,
        has_channel: false,
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        uri: Some(activity_id.to_string()),
        url: Some(activity_id.to_string()),
        extra_mentions: Vec::new(),
        extra_tags: Vec::new(),
    };
    services.handle_inbound_create_note(activity_id, input)?;
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
    let Some(object_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    let Some(note) = find_note_by_uri(services, &object_uri)? else {
        return Ok(accepted());
    };

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
            if matches!(
                obj_type,
                "Note" | "Article" | "Page" | "Question"
            ) {
                // Update(Note) is deferred.
                return Ok(accepted());
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
    };
    services.handle_inbound_update_actor(activity_id, &account.id, patch)?;
    Ok(accepted())
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
    let base = services.public_url.trim_end_matches('/').to_string();
    let author_uri = author.actor_url(&base);
    let (visibility, recipient_uris) = parse_note_audience(
        &parsed.to,
        &parsed.cc,
        Some(&format!("{author_uri}/followers")),
    );

    // Replies to unknown (remote) notes are stored without threading.
    let reply_to = match parsed.in_reply_to.as_deref() {
        Some(uri) => find_note_by_uri(services, uri)?.map(|note| note.id),
        None => None,
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

    // Attachments, best-effort (failures skipped).
    let mut file_ids = Vec::new();
    if fetch_media {
        let client = &services.federation_http;
        for attachment in &parsed.attachments {
            if let Ok(meta) = fetch_and_cache_remote_media(services, client, attachment) {
                file_ids.push(meta.id);
            }
        }
    }

    let input = CreateNoteInput {
        author_id: author.id.clone(),
        text: parsed.text.clone(),
        cw: parsed.cw.clone(),
        visibility,
        local_only: false,
        reply_to,
        renote_of: None,
        file_ids,
        visible_users,
        reaction_acceptance: None,
        no_extract_mentions: false,
        no_extract_hashtags: false,
        no_extract_emojis: false,
        has_poll: false,
        has_channel: false,
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        uri: Some(parsed.uri.clone()),
        url: parsed.url.clone(),
        extra_mentions: mention_ids,
        extra_tags: parsed.hashtags.clone(),
    };
    services.handle_inbound_create_note(activity_id, input)
}

/// Resolve a liked/renoted target note id: local lookup first, otherwise fetch
/// the remote note (origin-checked) and cache it as a remote note.
/// Returns `None` when the target cannot be resolved.
fn resolve_note_target_id(
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
    match ingest_remote_note(services, &parsed.uri, &parsed, &author, false)? {
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
