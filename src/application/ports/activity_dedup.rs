use super::account_store::UpdateAccountPatch;
use super::job_queue::NewJob;
use super::note_store::{CreateNoteInput, EditNoteInput};
use crate::domain::account::Account;
use crate::domain::blocking::Blocking;
use crate::domain::follow::{Follow, FollowNotify, FollowRequest};
use crate::domain::id::PortError;
use crate::domain::note::Note;
use crate::domain::quote::QuoteStamp;
use crate::domain::reaction::Reaction;

pub trait ActivityDedup: Send + Sync {
    fn check_and_record(&self, activity_id: &str) -> Result<bool, PortError>;
    /// Read-only replay gate (Phase 2.6 slice 7): report whether
    /// `activity_id` was already recorded, without writing. The inbox
    /// calls this BEFORE any remote fetch so replays trigger zero network
    /// I/O. It is advisory only — `record_*_activity` stays the atomic
    /// authority, so the check-then-act race is benign (a concurrent
    /// first delivery simply does the fetch twice; dedup still converges).
    /// Backends must implement the real read; test doubles return `false`.
    fn is_recorded(&self, activity_id: &str) -> Result<bool, PortError>;
    /// Forget a recorded activity id so a later delivery with the same id is
    /// processed again. Used when `Undo(Follow)` retracts a Follow whose id
    /// the sender reuses for every re-follow of the same pair (Misskey's
    /// `/follows/{follower}/{followee}`). No silent default: backends and
    /// test doubles must implement this explicitly.
    fn forget_activity(&self, _activity_id: &str) -> Result<(), PortError> {
        Err(PortError::StorageUnavailable(
            "ActivityDedup::forget_activity not implemented".to_string(),
        ))
    }
    fn record_note_activity(
        &self,
        activity_id: &str,
        input: CreateNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError>;
    fn record_delete_note_activity(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, note_id, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_delete_note_activity not implemented".to_string(),
        ))
    }
    /// Inbound `Delete` addressed by object URI (bench S6, 2026-10-17): the
    /// deleter may reach the store before the note's `Create`. Atomically
    /// deletes the live note at `object_uri` if one is held, else records a
    /// tombstone so a later `record_note_activity` / `create_note` for that
    /// URI is refused (`Ok(None)` / `Conflict`). Returns `true` only when a
    /// note was deleted. Replays of `activity_id` return `false`. Callers
    /// must have authorised the deleter against `object_uri` first.
    fn record_delete_uri_activity(
        &self,
        activity_id: &str,
        object_uri: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, object_uri, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_delete_uri_activity not implemented".to_string(),
        ))
    }
    fn record_follow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Follow>, PortError> {
        let _ = (
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        );
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_follow_activity not implemented".to_string(),
        ))
    }
    /// Dedup + record a pending request from an inbound Follow to a locked
    /// account. `Ok(None)` when the activity id was already seen.
    fn record_follow_request_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<FollowRequest>, PortError> {
        let _ = (
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        );
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_follow_request_activity not implemented".to_string(),
        ))
    }
    fn record_unfollow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, follower_id, followee_id, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_unfollow_activity not implemented".to_string(),
        ))
    }
    fn record_reject_follow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    /// Dedup + record an inbound `Block` (remote `blocker_id` blocks local
    /// `blockee_id`): the same atomic teardown as a local `block` (both
    /// follow edges, both pending requests) plus `jobs`, committed with the
    /// dedup mark in one transaction. `Ok(None)` when the activity id was
    /// already seen. A *different* activity re-blocking an existing pair
    /// converges to `Ok(None)` instead of erroring (no retry storm).
    fn record_block_activity(
        &self,
        activity_id: &str,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Blocking>, PortError> {
        let _ = (activity_id, blocker_id, blockee_id, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_block_activity not implemented".to_string(),
        ))
    }
    /// Dedup + remove the block for an inbound `Undo(Block)`.
    /// `Ok(false)` on replay or when no block row exists (Misskey `unblock`
    /// warns and returns in that case).
    fn record_unblock_activity(
        &self,
        activity_id: &str,
        blocker_id: &str,
        blockee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, blocker_id, blockee_id, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_unblock_activity not implemented".to_string(),
        ))
    }
    fn record_react_activity(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        reaction: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Reaction>, PortError> {
        let _ = (activity_id, user_id, note_id, reaction, upsert, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_react_activity not implemented".to_string(),
        ))
    }
    fn record_unreact_activity(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, user_id, note_id, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_unreact_activity not implemented".to_string(),
        ))
    }
    fn record_update_account_activity(
        &self,
        activity_id: &str,
        account_id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Account>, PortError> {
        let _ = (activity_id, account_id, patch, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_update_account_activity not implemented".to_string(),
        ))
    }
    /// Dedup + persist a locally authored `QuoteAuthorization` stamp and its
    /// `Accept` delivery job. `Ok(None)` when the activity id was already
    /// seen (replay: no duplicate stamp row, no duplicate `Accept`).
    fn record_quote_request_accept(
        &self,
        activity_id: &str,
        stamp: QuoteStamp,
        jobs: Vec<NewJob>,
    ) -> Result<Option<QuoteStamp>, PortError> {
        let _ = (activity_id, stamp, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_quote_request_accept not implemented".to_string(),
        ))
    }
    /// Dedup + enqueue a `Reject(QuoteRequest)` delivery job (no stamp is
    /// created). `Ok(false)` on replay.
    fn record_quote_request_reject(
        &self,
        activity_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_quote_request_reject not implemented".to_string(),
        ))
    }
    /// Dedup + move our pending quote to `Approved` with its verified stamp
    /// (inbound `Accept`). `Ok(None)` on replay.
    fn record_quote_decision_accept(
        &self,
        activity_id: &str,
        note_id: &str,
        stamp_uri: &str,
        verified_at_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let _ = (activity_id, note_id, stamp_uri, verified_at_ms, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_quote_decision_accept not implemented".to_string(),
        ))
    }
    /// Dedup + move our pending quote to `Rejected` (inbound `Reject`).
    /// The relation and API projection are retained. `Ok(None)` on replay.
    fn record_quote_decision_reject(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let _ = (activity_id, note_id, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_quote_decision_reject not implemented".to_string(),
        ))
    }
    /// Dedup + move our approved quote to `Revoked` (valid `Delete` of the
    /// stamp) and enqueue revocation-forwarding jobs. `Ok(None)` on replay.
    fn record_quote_stamp_delete(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let _ = (activity_id, note_id, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_quote_stamp_delete not implemented".to_string(),
        ))
    }
    /// Dedup + apply a federated note edit (inbound `Update(Note)`, §8):
    /// the same [`NoteStore::apply_note_edit`] storage path (no-change
    /// edits converge to the original), committed with the dedup mark in
    /// one transaction. `Ok(None)` on replay. `NotFound(NO_SUCH_NOTE)` when
    /// the cached copy is missing or deleted (callers map it to accept).
    fn record_note_edit_activity(
        &self,
        activity_id: &str,
        edit: EditNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        let _ = (activity_id, edit, jobs);
        Err(PortError::StorageUnavailable(
            "ActivityDedup::record_note_edit_activity not implemented".to_string(),
        ))
    }
}

#[derive(Debug, Default, Clone)]
pub struct NoopActivityDedup;

impl ActivityDedup for NoopActivityDedup {
    fn check_and_record(&self, _activity_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }

    fn is_recorded(&self, _activity_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }

    fn record_note_activity(
        &self,
        _activity_id: &str,
        _input: CreateNoteInput,
        _jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        Ok(None)
    }

    fn record_reject_follow_activity(
        &self,
        _activity_id: &str,
        _follower_id: &str,
        _followee_id: &str,
        _jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        Ok(false)
    }

    fn record_block_activity(
        &self,
        _activity_id: &str,
        _blocker_id: &str,
        _blockee_id: &str,
        _jobs: Vec<NewJob>,
    ) -> Result<Option<crate::domain::blocking::Blocking>, PortError> {
        Ok(None)
    }

    fn record_unblock_activity(
        &self,
        _activity_id: &str,
        _blocker_id: &str,
        _blockee_id: &str,
        _jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        Ok(false)
    }
}
