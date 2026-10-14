# 2026-10-14 — Phase 2 Slice D1 & D3 application & worker wiring (Deliverable 1)

## What changed
- `src/application/commands.rs`
  - `AppServices` gains `dedup: Arc<dyn ActivityDedup>` and
    `federation_http: SafeHttpClient` (imports `ActivityDedup`, `SafeHttpClient`;
    no new architecture — same owner/abstraction).
  - Local ops enqueue `FederateDeliverPayload` jobs atomically in the same
    storage call: `create_note` (NoteCreated, skipped when `local_only`),
    `delete_note` (NoteDeleted with `note_uri` + pure-renote `was_announce_of_uri`
    detection), `follow`/`unfollow` (Follow/Unfollow to the remote side's
    `preferred_inbox`; fresh activity ids since storage row ids are
    transaction-generated — documented MVP mismatch), `react`/`unreact`
    (Like/UndoLike to remote authors), `update_profile` (ActorUpdated via the
    `dedup.record_update_account_activity` atomic path when remote followers
    exist, since `AccountStore::update_account` carries no jobs — workaround
    commented).
  - New shared inbound contract for `src/federation/inbox.rs`: `federate_job`,
    `remote_inbox_for_account`, `handle_inbound_follow` (Notify+Streaming+
    auto-Accept for unlocked local followee / remote follower; Conflict→None),
    `handle_inbound_unfollow` (NOT_FOLLOWING→false), `handle_inbound_create_note`
    (best-effort reply/renote/mention Notify + Streaming), `handle_inbound_delete_note`
    (NotFound→false), `handle_inbound_like` (coerced, upsert; Conflict→None),
    `handle_inbound_unlike` (NOT_REACTED→false), `handle_inbound_update_actor`,
    `handle_inbound_accept_follow` (Conflict→None), `handle_inbound_reject_follow`
    (replay→false, best-effort unfollow). No inbound fan-out except the auto-Accept.
- `src/jobs/worker.rs`: `JobWorker` gains `services` + `delivery_tracker`;
  `FederateDeliver` now runs `execute_federate_deliver`: Delivered/Permanent→complete,
  Transient→`fail()` reschedule until `attempts + 1 >= MAX_DELIVERY_ATTEMPTS` (8).
- Initializers updated: `src/main.rs` (prod defaults), test inits in
  `commands.rs`, `endpoints/mod.rs`, `objects.rs`, `remote_cache.rs`,
  `webfinger.rs`, plus `tests/crash_recovery.rs` (`open_stack` now returns
  `Arc<AppServices>`; `open_worker` takes services).
- No `src/persistence/redb` change needed: `enqueue_jobs_in_txn` already does a
  generic `__PENDING_NOTE_ID__` string replace, covering the structured payload.

## Signature deviation (forced by existing caller)
- `src/federation/inbox.rs` (owned by another agent, must not touch) already calls
  `handle_inbound_reject_follow(activity_id, follower, followee, false)` with a
  4th arg. Implemented as `(activity_id, follower_id, followee_id, _with_replies: bool)`
  (ignored, documented) instead of the 3-arg spec so the crate compiles.

## Verified
- Nothing executed: per workspace rules cargo/fmt/clippy were NOT run.
  `cargo test --lib` (offline) still needs to be run by the caller to confirm
  compilation and that existing tests pass.
- Compatibility notes: conformance tests are store-level (unaffected);
  crash-recovery `pending >= acked.len()` assertions are lower bounds (extra
  federate jobs only increase counts); worker drains of federate jobs with no
  remote followers are Delivered no-ops (no network).

## Open questions
- Follow activity id vs storage row id mismatch (documented in code); Accept
  embedding uses the activity id.
- `MAX_DELIVERY_ATTEMPTS` is 8 in `src/federation/retry.rs`, not 10 as the
  assignment text stated; worker uses the constant.
