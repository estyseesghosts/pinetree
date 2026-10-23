use super::super::clips::{deliver_clip_delete, deliver_clip_update};
use super::super::pages::{deliver_page_create_or_update, deliver_page_delete};
use super::super::payload::FederateDeliverPayload;
use super::deliver_actor::{
    deliver_actor_updated, deliver_flag, deliver_move_delivered, deliver_suspend_broadcast,
};
use super::deliver_collections::{deliver_clip_membership_changed, deliver_page_liked};
use super::deliver_notes::{deliver_note_created, deliver_note_deleted, deliver_note_updated};
use super::deliver_polls::{deliver_poll_vote, deliver_question_updated};
use super::deliver_quotes::{
    deliver_featured_changed, deliver_quote_request, deliver_quote_response,
    deliver_quote_stamp_deleted,
};
use super::deliver_reactions::{deliver_like, deliver_undo_like};
use super::deliver_social::{
    deliver_block, deliver_follow, deliver_follow_response, deliver_undo_block, deliver_unfollow,
};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

/// Execute one [`FederateDeliverPayload`] job. `job_key` is the job's
/// idempotency key; per-inbox follow-up jobs derive their keys from it.
///
/// A payload with `inbox_override == Some(inbox)` is a single delivery and
/// returns that send's outcome directly (the worker applies job-level retry).
/// A fan-out payload (`inbox_override == None`) resolves the deduplicated
/// inbox list and returns [`DeliveryOutcome::Split`] when any inbox needs a
/// later attempt; see [`fan_out`].
pub fn execute_federate_deliver(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    payload_bytes: &[u8],
) -> Result<DeliveryOutcome, PortError> {
    // Malformed payloads are caller faults, not delivery results: report a
    // parse failure so the worker quarantines the job with a reason instead
    // of retrying (or silently dropping) it.
    let parsed: FederateDeliverPayload = serde_json::from_slice(payload_bytes)
        .map_err(|e| PortError::Validation(format!("federate payload invalid: {e}")))?;

    let base = services.public_url.trim_end_matches('/');

    match parsed {
        FederateDeliverPayload::NoteCreated {
            note_id,
            author_id,
            inbox_override,
        } => deliver_note_created(
            services,
            client,
            tracker,
            job_key,
            base,
            note_id,
            author_id,
            inbox_override,
        ),

        FederateDeliverPayload::NoteDeleted {
            note_id,
            author_id,
            note_uri,
            was_announce_of_uri,
            inbox_override,
        } => deliver_note_deleted(
            services,
            client,
            tracker,
            job_key,
            base,
            note_id,
            author_id,
            note_uri,
            was_announce_of_uri,
            inbox_override,
        ),

        FederateDeliverPayload::Follow {
            follow_id,
            follower_id,
            followee_id,
            inbox_url,
        } => deliver_follow(
            services,
            client,
            tracker,
            job_key,
            base,
            follow_id,
            follower_id,
            followee_id,
            inbox_url,
        ),

        FederateDeliverPayload::Unfollow {
            undo_id,
            follow_id,
            follower_id,
            followee_id,
            inbox_url,
        } => deliver_unfollow(
            services,
            client,
            tracker,
            job_key,
            base,
            undo_id,
            follow_id,
            follower_id,
            followee_id,
            inbox_url,
        ),

        FederateDeliverPayload::FollowResponse {
            response_id,
            accept,
            actor_id,
            inbox_url,
            follow_activity,
        } => deliver_follow_response(
            services,
            client,
            tracker,
            job_key,
            base,
            response_id,
            accept,
            actor_id,
            inbox_url,
            follow_activity,
        ),

        FederateDeliverPayload::Block {
            block_id,
            blocker_id,
            blockee_id,
            inbox_url,
        } => deliver_block(
            services, client, tracker, job_key, base, block_id, blocker_id, blockee_id, inbox_url,
        ),

        FederateDeliverPayload::UndoBlock {
            undo_id,
            block_id,
            blocker_id,
            blockee_id,
            inbox_url,
        } => deliver_undo_block(
            services, client, tracker, job_key, base, undo_id, block_id, blocker_id, blockee_id,
            inbox_url,
        ),

        FederateDeliverPayload::Like {
            reaction_id,
            actor_id,
            target_note_uri,
            reaction,
            inbox_url,
        } => deliver_like(
            services,
            client,
            tracker,
            job_key,
            base,
            reaction_id,
            actor_id,
            target_note_uri,
            reaction,
            inbox_url,
        ),

        FederateDeliverPayload::UndoLike {
            undo_id,
            reaction_id,
            actor_id,
            target_note_uri,
            reaction,
            inbox_url,
        } => deliver_undo_like(
            services,
            client,
            tracker,
            job_key,
            base,
            undo_id,
            reaction_id,
            actor_id,
            target_note_uri,
            reaction,
            inbox_url,
        ),

        FederateDeliverPayload::PollVote {
            vote_id,
            voter_id,
            target_note_uri,
            choice_name,
            inbox_url,
        } => deliver_poll_vote(
            services,
            client,
            tracker,
            job_key,
            base,
            vote_id,
            voter_id,
            target_note_uri,
            choice_name,
            inbox_url,
        ),

        FederateDeliverPayload::QuestionUpdated {
            note_id,
            author_id,
            inbox_override,
        } => deliver_question_updated(
            services,
            client,
            tracker,
            job_key,
            base,
            note_id,
            author_id,
            inbox_override,
        ),

        FederateDeliverPayload::ActorUpdated {
            update_id,
            account_id,
            inbox_override,
        } => deliver_actor_updated(
            services,
            client,
            tracker,
            job_key,
            base,
            update_id,
            account_id,
            inbox_override,
        ),

        FederateDeliverPayload::MoveDelivered {
            activity_id,
            account_id,
            new_actor_uri,
            new_account_id,
            inbox_override,
        } => deliver_move_delivered(
            services,
            client,
            tracker,
            job_key,
            base,
            activity_id,
            account_id,
            new_actor_uri,
            new_account_id,
            inbox_override,
        ),

        FederateDeliverPayload::NoteUpdated {
            note_id,
            author_id,
            inbox_override,
        } => deliver_note_updated(
            services,
            client,
            tracker,
            job_key,
            base,
            note_id,
            author_id,
            inbox_override,
        ),

        FederateDeliverPayload::QuoteRequest {
            request_id,
            quote_note_id,
            quoter_account_id,
            quoted_uri,
            target_actor_uri,
            inbox_url,
        } => deliver_quote_request(
            services,
            client,
            tracker,
            job_key,
            base,
            request_id,
            quote_note_id,
            quoter_account_id,
            quoted_uri,
            target_actor_uri,
            inbox_url,
        ),

        FederateDeliverPayload::QuoteResponse {
            response_id,
            accept,
            decider_account_id,
            request_activity_uri,
            stamp_uri,
            quoter_actor_uri,
            inbox_url,
        } => deliver_quote_response(
            services,
            client,
            tracker,
            job_key,
            base,
            response_id,
            accept,
            decider_account_id,
            request_activity_uri,
            stamp_uri,
            quoter_actor_uri,
            inbox_url,
        ),

        FederateDeliverPayload::QuoteStampDeleted {
            delete_id,
            actor_id,
            stamp_uri,
            quote_note_id,
            inbox_override,
        } => deliver_quote_stamp_deleted(
            services,
            client,
            tracker,
            job_key,
            base,
            delete_id,
            actor_id,
            stamp_uri,
            quote_note_id,
            inbox_override,
        ),

        FederateDeliverPayload::FeaturedChanged {
            actor_id,
            note_id,
            added,
            inbox_override,
        } => deliver_featured_changed(
            services,
            client,
            tracker,
            job_key,
            base,
            actor_id,
            note_id,
            added,
            inbox_override,
        ),

        FederateDeliverPayload::ClipMembershipChanged {
            activity_id,
            actor_id,
            clip_id,
            note_uri,
            added,
            inbox_url,
        } => deliver_clip_membership_changed(
            services,
            client,
            tracker,
            job_key,
            base,
            activity_id,
            actor_id,
            clip_id,
            note_uri,
            added,
            inbox_url,
        ),

        FederateDeliverPayload::PageCreated {
            page_id,
            author_id,
            inbox_override,
        } => deliver_page_create_or_update(
            services,
            client,
            tracker,
            job_key,
            base,
            &page_id,
            &author_id,
            false,
            inbox_override,
        ),

        FederateDeliverPayload::PageUpdated {
            page_id,
            author_id,
            inbox_override,
        } => deliver_page_create_or_update(
            services,
            client,
            tracker,
            job_key,
            base,
            &page_id,
            &author_id,
            true,
            inbox_override,
        ),

        FederateDeliverPayload::PageLiked {
            activity_id,
            actor_id,
            actor_uri,
            page_uri,
            inbox_url,
        } => deliver_page_liked(
            services,
            client,
            tracker,
            job_key,
            base,
            activity_id,
            actor_id,
            actor_uri,
            page_uri,
            inbox_url,
        ),

        FederateDeliverPayload::ClipUpdated {
            clip_id,
            owner_id,
            inbox_override,
        } => deliver_clip_update(
            services,
            client,
            tracker,
            job_key,
            base,
            &clip_id,
            &owner_id,
            inbox_override,
        ),

        FederateDeliverPayload::ClipDeleted {
            clip_id,
            owner_id,
            clip_uri,
            inbox_override,
        } => deliver_clip_delete(
            services,
            client,
            tracker,
            job_key,
            base,
            &clip_id,
            &owner_id,
            &clip_uri,
            inbox_override,
        ),

        FederateDeliverPayload::PageDeleted {
            page_id,
            author_id,
            page_uri,
            inbox_override,
        } => deliver_page_delete(
            services,
            client,
            tracker,
            job_key,
            base,
            &page_id,
            &author_id,
            &page_uri,
            inbox_override,
        ),

        FederateDeliverPayload::SuspendBroadcast {
            account_id,
            activity_id,
            undo,
            inbox_override,
            after,
        } => deliver_suspend_broadcast(
            services,
            client,
            tracker,
            job_key,
            base,
            account_id,
            activity_id,
            undo,
            inbox_override,
            after,
        ),

        FederateDeliverPayload::Flag {
            flag_id,
            actor_id,
            target_account_id,
            content,
            inbox_url,
        } => deliver_flag(
            services,
            client,
            tracker,
            job_key,
            base,
            flag_id,
            actor_id,
            target_account_id,
            content,
            inbox_url,
        ),
    }
}
