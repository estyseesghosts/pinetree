//! ActivityPub Actor documents, key objects, and actor-scoped collection
//! endpoints (Phase 2 Slices A2 & B1).
//!
//! Canonical actor URLs take the `{public_url}/users/{account_id}` form and
//! never expose node hostnames.

mod actor_document;
mod collections;
mod outbox;
mod remote_actor;

pub use actor_document::{
    get_actor, get_actor_public_key, get_at_profile, render_actor_document,
    render_public_key_document,
};
pub use collections::{get_actor_featured, get_actor_followers, get_actor_following};
pub use outbox::get_actor_outbox;
pub use remote_actor::{ParsedRemoteActor, parse_remote_actor, parse_url_host};
