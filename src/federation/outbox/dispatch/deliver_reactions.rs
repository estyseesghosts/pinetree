use super::deliver_single;
use crate::activitypub::objects::{
    render_emoji_react_activity, render_like_activity_for, render_undo_activity,
};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::fed_policy::ReactionStyle;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::host_caps::{fetch_nodeinfo_software, host_of_url, resolve_reaction_style};
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

pub(crate) fn deliver_like(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    reaction_id: String,
    actor_id: String,
    target_note_uri: String,
    reaction: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
    let actor = match services.accounts.find_by_id(&actor_id)? {
        Some(a) => a,
        None => return Ok(DeliveryOutcome::Delivered),
    };
    // §6 federation split (decided, WP5 three-way): the NodeInfo
    // probe runs here, off-transaction in the delivery worker — never
    // inside a state transaction. Unknown/failed hosts read as
    // Misskey-like. Misskey-family gets `Like` + `_misskey_reaction`,
    // Akkoma/Pleroma gets `EmojiReact` (`content` + `Emoji` tag for
    // custom reactions), Mastodon-family gets a bare `Like`.
    let style = match host_of_url(&inbox_url) {
        Some(host) => resolve_reaction_style(services, &host, services.clock.now_ms(), &|h| {
            fetch_nodeinfo_software(client, h)
        }),
        None => ReactionStyle::MisskeyLike,
    };
    let actor_uri = actor.actor_url(base);
    let activity = match style {
        ReactionStyle::MisskeyLike => render_like_activity_for(
            base,
            &reaction_id,
            &actor_uri,
            &target_note_uri,
            &reaction,
            true,
        ),
        ReactionStyle::EmojiReact => {
            render_emoji_react_activity(base, &reaction_id, &actor_uri, &target_note_uri, &reaction)
        }
        ReactionStyle::PlainLike => render_like_activity_for(
            base,
            &reaction_id,
            &actor_uri,
            &target_note_uri,
            &reaction,
            false,
        ),
    };
    deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
}

pub(crate) fn deliver_undo_like(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    _job_key: &str,
    base: &str,
    undo_id: String,
    reaction_id: String,
    actor_id: String,
    target_note_uri: String,
    reaction: String,
    inbox_url: String,
) -> Result<DeliveryOutcome, PortError> {
    let actor = match services.accounts.find_by_id(&actor_id)? {
        Some(a) => a,
        None => return Ok(DeliveryOutcome::Delivered),
    };
    let actor_uri = actor.actor_url(base);
    // Same per-host decision as `Like` so the undone activity matches
    // the form originally delivered (capability is TTL-stable).
    let style = match host_of_url(&inbox_url) {
        Some(host) => resolve_reaction_style(services, &host, services.clock.now_ms(), &|h| {
            fetch_nodeinfo_software(client, h)
        }),
        None => ReactionStyle::MisskeyLike,
    };
    let inner = match style {
        ReactionStyle::MisskeyLike => render_like_activity_for(
            base,
            &reaction_id,
            &actor_uri,
            &target_note_uri,
            &reaction,
            true,
        ),
        ReactionStyle::EmojiReact => {
            render_emoji_react_activity(base, &reaction_id, &actor_uri, &target_note_uri, &reaction)
        }
        ReactionStyle::PlainLike => render_like_activity_for(
            base,
            &reaction_id,
            &actor_uri,
            &target_note_uri,
            &reaction,
            false,
        ),
    };
    let activity = render_undo_activity(base, &undo_id, &actor_uri, &inner, None, None);
    deliver_single(services, client, tracker, &actor_id, &inbox_url, &activity)
}
