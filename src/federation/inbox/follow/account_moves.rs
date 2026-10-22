use axum::response::Response;
use serde_json::Value;

use super::super::common::{non_empty_str, resolve_local_account, resolve_remote_account};
use super::super::{accepted, unauthorized};
use crate::activitypub::objects::extract_object_uri;
use crate::application::commands::AppServices;
use crate::application::ports::{PortError, UpdateAccountPatch};
use crate::federation::delivery::fetch_and_upsert_remote_actor;

/// Inbound `Move` (Batch P; Misskey `ApInboxService.move` parity with one
/// hardening: the mover's authority is verified — `object` must be the
/// signer's own actor URI, so a third-party `Move` claiming someone else
/// moved is rejected with 401 instead of applied).
///
/// On success the mover row records `moved_to_uri`/`moved_at_ms` (and the
/// target joins `also_known_as`, mirroring `moveFromLocal`), then every
/// LOCAL follower unfollows the old account and follows the new one
/// (`postMoveProcess` parity, inline and paged — one transaction per edge,
/// so arbitrarily many followers stay FDB-safe). Replays converge: the
/// activity id is recorded before any edge work, and already-migrated or
/// already-following pairs are skipped.
pub(crate) fn handle_move(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(target_uri) = non_empty_str(activity.get("target")) else {
        return Ok(accepted());
    };
    // Authority: only the old account itself may announce its move.
    if extract_object_uri(activity.get("object")).as_deref() != Some(actor_uri) {
        crate::fed_debug!("in  reject reason=forged-move actor={actor_uri:?}");
        return Ok(unauthorized());
    }
    // Replay gate before any fetch (same advisory pattern as the other
    // handlers: `record` stays atomic below).
    if services.dedup.is_recorded(activity_id)? {
        return Ok(accepted());
    }
    // Resolve the mover (cached row or one fetch) and drop blocked hosts
    // BEFORE recording dedup (blocked-host discipline).
    // Local-first like the target below: a locally-addressed mover (tests,
    // single-node self-delivery) must resolve to the local row, never to a
    // fetched remote duplicate.
    let mover = match resolve_local_account(services, actor_uri)? {
        Some(a) => a,
        None => match services.accounts.find_by_uri(actor_uri)? {
            Some(a) => a,
            None => resolve_remote_account(services, actor_uri)?,
        },
    };
    if services.inbound_host_blocked(&mover.id) {
        return Ok(accepted());
    }
    // Resolve the destination local-first (a local target has no stored
    // `uri`, so `find_by_uri` alone would miss it and the fetch below
    // would upsert a remote duplicate of our own user — migration would
    // follow the duplicate and converge nowhere). Unresolvable targets
    // accept without recording so a later retry can succeed.
    let target = match resolve_local_account(services, &target_uri)? {
        Some(a) => a,
        None => match services.accounts.find_by_uri(&target_uri)? {
            Some(a) => a,
            None => match fetch_and_upsert_remote_actor(
                services,
                &services.federation_http,
                &target_uri,
            ) {
                Ok((account, _)) => account,
                Err(_) => return Ok(accepted()),
            },
        },
    };
    // Already moved elsewhere and now moving again: converge on the latest
    // target (same as a re-delivery — last writer wins, deduped by id).
    let mut also_known = mover.also_known_as.clone().unwrap_or_default();
    if !also_known.iter().any(|u| u == &target_uri) {
        also_known.push(target_uri.clone());
    }
    let now_ms = services.clock.now_ms();
    services.accounts.update_account(
        &mover.id,
        UpdateAccountPatch {
            moved_to_uri: Some(target_uri.clone()),
            moved_at_ms: Some(now_ms),
            also_known_as: Some(Some(also_known)),
            ..Default::default()
        },
        Vec::new(),
    )?;
    services.dedup.check_and_record(activity_id)?;
    // Migrate local followers: unfollow-old then follow-new, paged, one
    // transaction per edge. Remote followers learn via the `Move` they
    // receive from the mover's own server — nothing to do for them here.
    // (A local mover in tests migrates the same way.)
    let mut cursor: Option<String> = None;
    loop {
        let page = services
            .follows
            .followers_of(&mover.id, cursor.as_deref(), 100)?;
        if page.is_empty() {
            break;
        }
        for edge in &page {
            cursor = Some(edge.id.clone());
            let Ok(Some(follower)) = services.accounts.find_by_id(&edge.follower_id) else {
                continue;
            };
            if !follower.is_local() || follower.id == target.id {
                continue;
            }
            if services.follows.is_following(&follower.id, &target.id)? {
                // Already refollowed (replay): still drop the stale old
                // edge so the migration converges.
                let _ = services.unfollow(&follower.id, &mover.id);
                continue;
            }
            let _ = services.unfollow(&follower.id, &mover.id);
            let _ = services.follow(&follower.id, &target.id, false);
        }
        if page.len() < 100 {
            break;
        }
    }
    Ok(accepted())
}
