use axum::response::Response;
use serde_json::Value;

use super::super::accepted;
use super::super::common::{find_note_by_uri, resolve_note_target_id, resolve_remote_account};
use crate::activitypub::objects::extract_object_uri;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::pinning::UserNotePining;

/// Inbound `Add`/`Remove` for clip collections (federated clips + pages
/// handoff, Phase 2) and featured-collection pin sync (Batch Q Phase 1,
/// item 8a). Semantics:
/// * Target is exactly the signer's own `{actor}/collections/featured`
///   ⇒ pin/unpin handling (see `handle_featured_membership`). Any other
///   `/collections/featured` target (someone else's collection) ⇒ ignore.
/// * Target is a LOCAL `/clips/{id}` URI ⇒ ignore (remote actors never
///   mutate our collections; the owner is local and we never deliver to
///   ourselves). A remote actor claiming ownership of our collection is
///   forged — also ignored, never an error.
/// * Target is remote ⇒ verify the signer IS the collection owner by
///   fetching the collection and comparing `attributedTo` (authority =
///   collection owner). Verified ⇒ evict the cached membership (the next
///   resolve re-pulls full state); unverifiable ⇒ ignore. Eviction is
///   idempotent, so Add/Remove converge even under replay (replays skip
///   via the read-only replay gate, without network I/O).
pub(crate) fn handle_clip_membership(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
    added: bool,
) -> Result<Response, PortError> {
    use crate::activitypub::federated_collections::{
        evict_remote_clip_cache, fetch_collection_owner,
    };
    // Replay gate before any network I/O (same discipline as deletes).
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    let Some(target_uri) = extract_object_uri(activity.get("target")) else {
        return Ok(accepted());
    };
    // Featured-collection pin sync: only the signer's own collection is
    // ever applied; anyone else's converges without effect (same rule as
    // local clip collections below — remote actors never mutate
    // collections they do not own).
    if target_uri == format!("{actor_uri}/collections/featured") {
        return handle_featured_membership(services, activity, activity_id, actor_uri, added);
    }
    if target_uri.ends_with("/collections/featured") {
        return Ok(accepted());
    }
    let base = services.public_url.trim_end_matches('/');
    if target_uri.starts_with(&format!("{base}/clips/")) {
        // Our own collection: never mutated remotely.
        return Ok(accepted());
    }
    if !target_uri.contains("/clips/") {
        return Ok(accepted());
    }
    match fetch_collection_owner(services, &target_uri) {
        Some(owner) if owner == actor_uri => {
            evict_remote_clip_cache(&target_uri);
            Ok(accepted())
        }
        _ => Ok(accepted()),
    }
}

/// Inbound `Add`/`Remove` for a remote actor's featured collection
/// (Batch Q Phase 1, item 8a; Misskey `NotePiningService` featured-sync
/// parity): the collection owner pins/unpins one of its own notes.
///
/// Authority: dispatch only calls this when the target is exactly the
/// signer's own `{actor}/collections/featured`, and the note must belong
/// to the signer (collection owners pin only their own notes). The object
/// may be a bare note URI or an embedded note document — resolution
/// follows the same lookup-or-fetch-and-cache discipline as
/// `Like`/`Announce` targets (origin-checked, SSRF-guarded), so unknown
/// notes materialize before the pin lands.
///
/// Idempotency: duplicate `Add` maps the store `Conflict(ALREADY_PINNED)`
/// to success and duplicate `Remove` is a no-op success upstream-style;
/// the activity id is recorded after the pin op so replays converge via
/// the read-only gate in `handle_clip_membership` without refetching.
/// (Pin and dedup commit in two transactions; both sides are idempotent,
/// so a crash between them still converges on retry.)
pub(crate) fn handle_featured_membership(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
    added: bool,
) -> Result<Response, PortError> {
    // Resolve the signer (cached row or one fetch) and drop blocked hosts
    // BEFORE any state write (blocked-host discipline).
    let actor_account = resolve_remote_account(services, actor_uri)?;
    if services.inbound_host_blocked(&actor_account.id) {
        return Ok(accepted());
    }
    let Some(object_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    let note_id = match find_note_by_uri(services, &object_uri)? {
        Some(note) => note.id,
        None => match resolve_note_target_id(services, &object_uri)? {
            Some(id) => id,
            // Unresolvable targets accept without recording so a later
            // retry can succeed (same discipline as `Move` targets).
            None => return Ok(accepted()),
        },
    };
    let Some(note) = services.notes.get_note(&note_id)? else {
        return Ok(accepted());
    };
    if note.author_id != actor_account.id {
        return Ok(accepted());
    }
    if added {
        let pinning = UserNotePining {
            id: services.ids.generate(),
            user_id: actor_account.id.clone(),
            note_id: note.id,
        };
        match services.notes.add_pinning(pinning, Vec::new()) {
            Ok(_) => {}
            Err(PortError::Conflict(_)) => {}
            Err(e) => return Err(e),
        }
    } else {
        services
            .notes
            .remove_pinning(&actor_account.id, &note.id, Vec::new())?;
    }
    services.dedup.check_and_record(activity_id)?;
    Ok(accepted())
}
