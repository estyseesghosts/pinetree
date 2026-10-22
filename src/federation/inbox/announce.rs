//! Inbound `Announce` handling (moved from `src/federation/inbox.rs`).

use axum::response::Response;
use serde_json::Value;

use super::accepted;
use super::common::{resolve_note_target_id, resolve_remote_account};
use crate::activitypub::objects::{extract_object_uri, extract_uri_list, parse_note_audience};
use crate::application::commands::AppServices;
use crate::application::ports::{CreateNoteInput, PortError};
use crate::domain::note::Visibility;

pub(crate) fn handle_announce(
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
    let mut visibility =
        parse_note_audience(&to, &cc, Some(&format!("{announcer_uri}/followers"))).0;
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
