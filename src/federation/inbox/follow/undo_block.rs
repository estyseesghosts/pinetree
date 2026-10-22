use axum::response::Response;
use serde_json::Value;

use super::super::accepted;
use super::super::common::{
    find_note_by_uri, non_empty_str, resolve_local_account, resolve_note_target_id,
    resolve_remote_account,
};
use crate::activitypub::objects::extract_object_uri;
use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::federation::ActivityIdIndex;

/// Inbound `Block` (Misskey `ApInboxService.block()` parity): the remote
/// actor blocks a local user. Unknown or remote objects are skipped.
pub(crate) fn handle_block(
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

pub(crate) fn handle_undo(
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
            // WP5 `Undo(Accept)` by-ID: a bare-IRI object that names a
            // previously seen `Accept(Follow)` revokes that grant. Authority:
            // only the Accepter (the embedded `Follow.object`) may undo it.
            // Unknown ids fall through to the Announce path (which converges
            // quietly when nothing matches).
            if let Some((follower_uri, followee_uri)) = ActivityIdIndex::lookup_accept(uri) {
                if followee_uri != actor_uri {
                    return Ok(accepted());
                }
                let Some(follower) = resolve_local_account(services, &follower_uri)? else {
                    return Ok(accepted());
                };
                if !follower.is_local() {
                    return Ok(accepted());
                }
                if let Ok(followee) = resolve_remote_account(services, &followee_uri) {
                    services.handle_inbound_unfollow(activity_id, &follower.id, &followee.id)?;
                }
                ActivityIdIndex::forget(uri);
                return Ok(accepted());
            }
            let actor_account = resolve_remote_account(services, actor_uri)?;
            if services.inbound_host_blocked(&actor_account.id) {
                return Ok(accepted());
            }
            match find_note_by_uri(services, uri)? {
                Some(note) if note.author_id == actor_account.id && note.renote_id.is_some() => {
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
                    // Clip unfollows: `Undo(Follow{collection URI})`
                    // retracts a clip follow. Forged unfollows of unknown
                    // clips converge quietly.
                    {
                        let base = services.public_url.trim_end_matches('/').to_string();
                        if let Some(clip_id) = inner_object
                            .strip_prefix(&format!("{base}/clips/"))
                            .map(|rest| rest.split(['/', '?', '#']).next().unwrap_or("").trim())
                            .filter(|id| !id.is_empty())
                        {
                            let _ = services.handle_inbound_clip_unfollow(
                                activity_id,
                                &follower.id,
                                clip_id,
                            );
                            // Misskey id-reuse parity (same as account
                            // unfollows below): forget both the Follow id and
                            // this Undo id so a later re-follow is not
                            // mistaken for a replay. Replaying an
                            // Undo(Follow) is idempotent anyway.
                            if let Some(follow_id) = non_empty_str(map.get("id")) {
                                services.dedup.forget_activity(&follow_id)?;
                            }
                            services.dedup.forget_activity(activity_id)?;
                            return Ok(accepted());
                        }
                    }
                    let Some(followee) = resolve_local_account(services, &inner_object)? else {
                        return Ok(accepted());
                    };
                    if !followee.is_local() {
                        return Ok(accepted());
                    }
                    services.handle_inbound_unfollow(activity_id, &follower.id, &followee.id)?;
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
                    let Some(target_id) = resolve_note_target_id(services, &target_uri)? else {
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
                // `Undo(Accept(Follow))` (Batch Q Phase 1, item 8b): the
                // followee revokes a previously granted follow, so the
                // follow edge comes down exactly like an `Undo(Follow)`
                // (same store, same idempotency: a missing edge maps to
                // success). Authority: only the Accepter itself may undo
                // its Accept — the embedded `Accept.actor` and the
                // embedded `Follow.object` must both be the Undo signer,
                // mirroring the `Accept(Follow)` response checks. A bare-IRI
                // object has no activity record to resolve against, so it
                // converges without effect (limitation: by-ID Accept
                // resolution needs an activity store that does not exist).
                "Accept" => {
                    if non_empty_str(map.get("actor")).as_deref() != Some(actor_uri) {
                        return Ok(accepted());
                    }
                    let Some(follow_map) = map.get("object").and_then(Value::as_object) else {
                        return Ok(accepted());
                    };
                    if follow_map.get("type").and_then(Value::as_str).unwrap_or("") != "Follow" {
                        return Ok(accepted());
                    }
                    let (Some(follow_actor), Some(follow_object)) = (
                        non_empty_str(follow_map.get("actor")),
                        non_empty_str(follow_map.get("object")),
                    ) else {
                        return Ok(accepted());
                    };
                    if follow_object != actor_uri {
                        return Ok(accepted());
                    }
                    let Some(follower) = resolve_local_account(services, &follow_actor)? else {
                        return Ok(accepted());
                    };
                    if !follower.is_local() {
                        return Ok(accepted());
                    }
                    let followee = resolve_remote_account(services, &follow_object)?;
                    services.handle_inbound_unfollow(activity_id, &follower.id, &followee.id)?;
                    Ok(accepted())
                }
                _ => Ok(accepted()),
            }
        }
        _ => Ok(accepted()),
    }
}
