//! Inbound `Update` handling for actors, notes, questions, and clip sync
//! (moved from `src/federation/inbox.rs`).

use axum::http::StatusCode;
use axum::response::Response;
use serde_json::Value;

use super::common::{find_note_by_uri, non_empty_str};
use super::vote::handle_update_question;
use super::{accepted, unauthorized};
use crate::activitypub::actor::parse_remote_actor;
use crate::activitypub::objects::{ParsedRemoteNote, parse_remote_note, sanitize_html_to_mfm};
use crate::application::commands::AppServices;
use crate::application::ports::{CreateNoteInput, PortError, UpdateAccountPatch};
use crate::domain::account::Account;
use crate::domain::fed_policy;
use crate::domain::note::Note;
use crate::domain::poll::PollInput;
use crate::federation::delivery::{fetch_and_upsert_remote_actor, fetch_remote_note};

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_update(
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
            // WP5 `Update(Note)` string-URI fetch-and-apply (Sharkey parity):
            // a string object that is NOT the actor's own URI names a note
            // whose edited document must be fetched (SSRF/size/redirect
            // guarded inside `fetch_remote_note`) and authority-checked
            // (fetcher == note author, same exact authority) before the
            // shared apply path runs. Unresolvable objects accept without
            // recording so a later retry can succeed.
            if uri != actor_uri {
                return handle_update_note_string(services, &uri, activity_id, actor_uri);
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
            // Clip metadata sync (Phase 2a): `Update(OrderedCollection)`
            // carrying the embedded full collection document.
            if obj_type == "OrderedCollection" {
                return handle_clip_sync_update(services, map, activity_id, actor_uri);
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
        .or_else(|| parsed.summary_html.as_deref().map(sanitize_html_to_mfm))
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

/// Inbound `Update(Note)` by string URI (WP5 §5; Sharkey
/// `ApInboxService.update` → `fetchApObject` parity): dereference the named
/// note over the SSRF/size/redirect-guarded fetcher, then run the shared
/// apply path.
///
/// Authority (all must hold, else accept-without-change):
/// * the fetched document's `attributedTo` is the signer (the signer may
///   only edit its own notes);
/// * the fetched document id stays on the requested URI's exact authority
///   (no cross-host substitution via redirect/alias);
/// * the cached copy exists and is remote-authored (never create here —
///   creation stays on the `Create` path; DIVERGENCE from Sharkey's
///   create-on-update fallback, matching the embedded arm).
pub(crate) fn handle_update_note_string(
    services: &AppServices,
    note_uri: &str,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    // Replay gate before any fetch (same discipline as `Create`).
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    let client = &services.federation_http;
    let parsed = match fetch_remote_note(services, client, note_uri) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(accepted()),
    };
    if parsed.attributed_to != actor_uri {
        return Ok(accepted());
    }
    if !fed_policy::exact_authority_allows(&parsed.uri, note_uri) {
        return Ok(accepted());
    }
    apply_parsed_note_edit(services, activity_id, actor_uri, &parsed)
}

/// Inbound `Update(Note|Article|Page)` (embedded; §8 + Sharkey
/// `ApInboxService.update` → `ApNoteService.updateNote` parity): apply a
/// remote author's edit to the cached local copy. Authority: the signer must
/// be the note's author (`attributedTo`, defaulting to the signer when
/// absent) and the cached copy's author; locally authored copies, unknown
/// notes, and suspended authors converge to accept-without-change. A missing
/// note is NOT created here (DIVERGENCE from Sharkey, which falls back to
/// create — pinetree's `Create` path already ingests new notes).
/// Poison-pill validation failures accept (no retry storm); storage faults
/// propagate (the delivery retries). String-URI objects take the
/// fetch-and-apply arm ([`handle_update_note_string`]).
pub(crate) fn handle_update_note(
    services: &AppServices,
    doc: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    // Federated pages (Phase 3): `Update(Article)` for a materialized
    // remote page refreshes the page row, never the note pipeline. Local
    // `/pages/` URLs are owner-updated locally only — a remote signer is
    // never authoritative for them.
    if doc.get("type").and_then(Value::as_str) == Some("Article") {
        let base = services.public_url.trim_end_matches('/').to_string();
        let doc_id = doc
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if doc_id.starts_with(&format!("{base}/pages/")) {
            return Ok(accepted());
        }
        if !doc_id.is_empty() {
            if let Some(page) = services.pages.get_page_by_uri(doc_id)? {
                let owner_ok = match services.accounts.find_by_id(&page.owner_id)? {
                    Some(owner) => owner.actor_url(&base) == actor_uri,
                    None => false,
                };
                if !owner_ok {
                    return Ok(accepted());
                }
                if services.inbound_host_blocked(&page.owner_id) {
                    return Ok(accepted());
                }
                services.ingest_remote_article(&page.owner_id, doc)?;
                return Ok(accepted());
            }
        }
        // No materialized page for this URI: fall through to the note
        // pipeline (covers non-public Articles, same as Create).
    }
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
    apply_parsed_note_edit(services, activity_id, actor_uri, &parsed)
}

/// Shared edit-apply tail for embedded and fetched `Update(Note)`
/// documents: quote-target re-resolution, poll-shape degradation, and the
/// atomic `handle_inbound_update_note` commit. Poison-pill validation
/// failures accept (no retry storm); storage faults propagate.
pub(crate) fn apply_parsed_note_edit(
    services: &AppServices,
    activity_id: &str,
    actor_uri: &str,
    parsed: &ParsedRemoteNote,
) -> Result<Response, PortError> {
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

/// Inbound `Update(OrderedCollection)` (Phase 2a): clip metadata edits
/// carry the embedded full collection document (one round trip, no
/// refetch). Authority = collection owner, verified by fetching the
/// collection and comparing `attributedTo` (same as Add/Remove). Apply =
/// refresh-or-evict: the embedded document populates the membership
/// cache with its `updated` stamp; replays converge idempotently.
/// Local `/clips/` targets are never mutated remotely.
pub(crate) fn handle_clip_sync_update(
    services: &AppServices,
    doc: &serde_json::Map<String, serde_json::Value>,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    use crate::activitypub::federated_collections::{
        evict_remote_clip_cache, fetch_collection_owner, ingest_remote_clip_doc,
    };
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    let Some(collection_uri) = doc
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|u| !u.is_empty())
    else {
        return Ok(accepted());
    };
    let base = services.public_url.trim_end_matches('/').to_string();
    if collection_uri.starts_with(&format!("{base}/clips/")) {
        return Ok(accepted());
    }
    if !collection_uri.contains("/clips/") {
        return Ok(accepted());
    }
    match fetch_collection_owner(services, collection_uri) {
        Some(owner) if owner == actor_uri => {
            ingest_remote_clip_doc(collection_uri, &Value::Object(doc.clone()));
            services.dedup.check_and_record(activity_id)?;
            Ok(accepted())
        }
        _ => {
            // Authority failure: evict nothing, record nothing (a retry
            // after owner change can still succeed).
            let _ = evict_remote_clip_cache;
            Ok(accepted())
        }
    }
}

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
            .create_account(
                CreateAccountInput {
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
                    also_known_as: None,
                    moved_to_uri: None,
                    moved_at_ms: None,
                },
                None,
            )
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
        let resp =
            handle_update_note(&services, &forged, "upd-forged", CAROL_URI).expect("handle forged");
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
        let local = services
            .create_account("local", None)
            .expect("local account");
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
        let local_doc = render_note_object(&services, &local_edited, true).expect("render local");
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
        assert!(
            services
                .notes
                .note_edits(&local_note.id)
                .unwrap()
                .is_empty()
        );
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
