//! Inbound follow-family handling: `Follow`, `Accept`, `Reject`, `Undo`,
//! `Block`, `Move`, quote authorization, and collection membership
//! (moved from `src/federation/inbox.rs`).

mod account_moves;
mod collection_membership;
mod follow_requests;
mod follow_responses;
#[cfg(test)]
mod key_ownership;
mod quote_requests;
mod undo_block;

pub(crate) use self::account_moves::handle_move;
pub(crate) use self::collection_membership::handle_clip_membership;
pub(crate) use self::follow_requests::handle_follow;
pub(crate) use self::follow_responses::{handle_accept, handle_reject};
pub(crate) use self::quote_requests::handle_quote_request;
pub(crate) use self::undo_block::{handle_block, handle_undo};
