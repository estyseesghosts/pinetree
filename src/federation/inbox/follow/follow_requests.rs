use axum::response::Response;
use serde_json::Value;

use super::super::accepted;
use super::super::common::{non_empty_str, resolve_local_account, resolve_remote_account};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;

pub(crate) fn handle_follow(
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
    // Clip follows (Phase 2b): `Follow{actor, object: collection URI}` to
    // the clip owner's inbox. Authority = signer == follower (established
    // above); the collection must be ours and readable.
    {
        let base = services.public_url.trim_end_matches('/').to_string();
        if let Some(clip_id) = object_uri
            .strip_prefix(&format!("{base}/clips/"))
            .map(|rest| rest.split(['/', '?', '#']).next().unwrap_or("").trim())
            .filter(|id| !id.is_empty())
        {
            if services.clips.get_clip(clip_id)?.is_some() {
                let _ = services.handle_inbound_clip_follow(activity_id, &follower.id, clip_id);
            }
            return Ok(accepted());
        }
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
