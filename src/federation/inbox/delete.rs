//! Inbound `Delete` handling and tombstone convergence
//! (moved from `src/federation/inbox.rs`).

use axum::http::StatusCode;
use axum::response::Response;
use serde_json::Value;

use super::common::find_note_by_uri;
use super::{accepted, host_of, unauthorized};
use crate::activitypub::objects::{extract_object_uri, tombstone_inner_id};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::fetch_and_upsert_remote_actor;

pub(crate) fn handle_delete(
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
    // Clip sync (Phase 2a): `Delete(OrderedCollection)` on delete and on
    // tier flips to a more restrictive tier. Receivers evict + prune.
    if let Some(obj) = activity.get("object") {
        let is_collection = obj
            .as_object()
            .and_then(|m| m.get("type"))
            .and_then(Value::as_str)
            == Some("OrderedCollection");
        if is_collection || object_uri.contains("/clips/") {
            return handle_clip_sync_delete(services, obj, &object_uri, activity_id, actor_uri);
        }
    }
    // Federated pages (Phase 3): `Delete(Article-URI)` from the page
    // owner retracts a materialized remote page (cascade drops its
    // likes). Local `/pages/` URLs are owner-deleted locally only.
    // Authority is the page owner throughout; anything else is ignored.
    {
        let base = services.public_url.trim_end_matches('/').to_string();
        if !object_uri.starts_with(&format!("{base}/pages/")) {
            if let Some(page) = services.pages.get_page_by_uri(&object_uri)? {
                let owner_ok = match services.accounts.find_by_id(&page.owner_id)? {
                    Some(owner) => owner.actor_url(&base) == actor_uri,
                    None => false,
                };
                if owner_ok && !services.dedup.check_and_record(activity_id)? {
                    services.pages.delete_page(&page.id, Vec::new())?;
                }
                return Ok(accepted());
            }
            // Not a materialized page: fall through to the note / stamp /
            // tombstone paths below.
        }
    }
    // WP5 Tombstone-typed handling (Misskey dispatch parity): a
    // `Delete` whose embedded object is `Tombstone`-typed names the deleted
    // note in its `id` (the wrapper carries no other fields). This resolves
    // through the same Path 1 below — the branch exists so the shape is
    // recognized explicitly and covered by `tests/gaps_fed_update.rs`
    // rather than by accident of `extract_object_uri`.
    if tombstone_inner_id(activity.get("object")).is_some() {
        crate::fed_debug!("in  tombstone-wrapped delete id={object_uri:?}");
    }
    // Path 0: `Delete(Person)` inbound (Batch Q Phase 1, item 8c) — a
    // remote server announcing one of its accounts is gone. Applied via
    // the moderation tombstone path (handle stays reserved, follow edges
    // and requests torn down both ways), deduped on the activity id.
    // Authority: only the actor itself deletes itself (`actor == object`,
    // hence same host); a LOCAL account URI is never marked from
    // federation input. Embedded non-actor documents (Note/Article/Page)
    // and bare URIs that name no known account fall through to the note /
    // stamp / tombstone paths below byte-for-byte unchanged.
    {
        let object_value = activity.get("object");
        let embedded_is_object = object_value.map(Value::is_object).unwrap_or(false);
        let embedded_type = object_value
            .and_then(|o| o.as_object())
            .and_then(|m| m.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let actor_shaped = !embedded_is_object
            || matches!(
                embedded_type,
                "Person" | "Service" | "Application" | "Group" | "Organization"
            );
        if actor_shaped {
            if let Some(account) = services.accounts.find_by_uri(&object_uri)? {
                // A known account URI is always an actor Delete: it never
                // reaches the note/stamp/tombstone paths (an actor URI must
                // never become a note tombstone).
                if account.is_local() {
                    return Ok(accepted());
                }
                if object_uri != actor_uri {
                    return Ok(accepted());
                }
                // Tombstone first, then record: both sides are idempotent,
                // so a crash between them still converges on retry.
                services
                    .moderation
                    .delete_account(&account.id, None, Vec::new())?;
                services.dedup.check_and_record(activity_id)?;
                return Ok(accepted());
            }
            if embedded_is_object {
                // An embedded actor document for an unknown account:
                // fetch-and-verify it names a remote actor, then tombstone.
                // Unverifiable documents converge without recording so a
                // later retry can succeed.
                match fetch_and_upsert_remote_actor(
                    services,
                    &services.federation_http,
                    &object_uri,
                ) {
                    Ok((account, _)) => {
                        if account.is_local() || object_uri != actor_uri {
                            return Ok(accepted());
                        }
                        services
                            .moderation
                            .delete_account(&account.id, None, Vec::new())?;
                        services.dedup.check_and_record(activity_id)?;
                        return Ok(accepted());
                    }
                    Err(_) => return Ok(accepted()),
                }
            }
        }
    }
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
            if let (Some(actor_host), Some(note_host)) = (host_of(actor_uri), host_of(&note_uri)) {
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
        if let (Some(actor_host), Some(object_host)) = (host_of(actor_uri), host_of(&object_uri)) {
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

/// Inbound `Delete(OrderedCollection)` (Phase 2a): fires on delete and on
/// tier flips to a more restrictive tier. Authority = signer == collection
/// owner (verified like Add/Remove). Receivers evict the membership cache
/// and prune dependent state (remote follow rows for the collection stop
/// receiving fan-out; nothing else is stored remotely). Replays idempotent.
/// A tombstoned remote collection that reappears via full-state pull stays
/// evicted: pulls that 404-or-Tombstone drop the cache entry.
pub(crate) fn handle_clip_sync_delete(
    services: &AppServices,
    object: &Value,
    object_uri: &str,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    use crate::activitypub::federated_collections::{
        evict_remote_clip_cache, fetch_collection_owner,
    };
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    let base = services.public_url.trim_end_matches('/').to_string();
    if object_uri.starts_with(&format!("{base}/clips/")) {
        return Ok(accepted());
    }
    if !object_uri.contains("/clips/") {
        return Ok(accepted());
    }
    // Embedded-document deletes name the collection twice (wrapper id +
    // inner id); both must agree when both are present.
    if let Some(inner) = object
        .as_object()
        .and_then(|m| m.get("id"))
        .and_then(Value::as_str)
    {
        if inner.trim() != object_uri {
            return Ok(accepted());
        }
    }
    match fetch_collection_owner(services, object_uri) {
        Some(owner) if owner == actor_uri => {
            evict_remote_clip_cache(object_uri);
            services.dedup.check_and_record(activity_id)?;
            Ok(accepted())
        }
        // Owner fetch failed (deleted collections 404 their own doc):
        // accept + evict anyway — the collection is gone either way, and
        // holding a stale cached copy is the worse outcome. Forged
        // deletes of *live* collections are the risk; mitigate by
        // re-checking: only evict-without-verify when the fetch itself
        // failed, never when it named a different owner.
        None => {
            evict_remote_clip_cache(object_uri);
            services.dedup.check_and_record(activity_id)?;
            Ok(accepted())
        }
        _ => Ok(accepted()),
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
            .create_account(
                CreateAccountInput {
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
                    also_known_as: None,
                    moved_to_uri: None,
                    moved_at_ms: None,
                },
                None,
            )
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
            .create_account(
                CreateAccountInput {
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
                    also_known_as: None,
                    moved_to_uri: None,
                    moved_at_ms: None,
                },
                None,
            )
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
