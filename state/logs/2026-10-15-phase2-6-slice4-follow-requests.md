# Phase 2.6 slice 4 — Follow request state (C2)

## Decisions
- **D-a:** separate `FollowRequest` store (redb tables `follow_requests_forward/reverse`), mirroring Misskey. `Follow` edges, `followers_of`, counters, `follows_author` and AP collections are untouched, so a pending request cannot leak by construction.
- **D-b:** `following/requests/list|accept|reject|cancel` added now. Error `NO_FOLLOW_REQUEST` (Misskey id `bcde4f8b-…`).
- Locked is decided by `Account.is_locked` for both local and remote followees (remote value comes from `manuallyApprovesFollowers`). Local→unlocked remote stays an optimistic edge as before; local→locked remote is pending until the remote's inbound `Accept` (`follow_in_txn` supersedes the request).

## Changes
- Domain: `FollowRequest`, `NotificationKind::ReceiveFollowRequest`.
- Ports: `FollowStore::{request_follow,get_follow_request,follow_requests_to,accept_follow_request,remove_follow_request}`, `ActivityDedup::record_follow_request_activity`.
- redb: `follow`/`unfollow` now delegate to `follow_in_txn`/`unfollow_in_txn` (previously duplicated); `unfollow` of a pending-only pair removes the request (covers Undo(Follow) and cancel); Reject of a pending request goes through the same path.
- App: `follow()` returns `FollowOutcome`; `accept/reject/cancel_follow_request`; accept/reject enqueue AP `Accept`/`Reject` jobs in the same txn as the state change; inbound Follow to a local locked account = request + `receiveFollowRequest` notification, no auto-Accept.
- API: endpoints above; `hasPendingFollowRequestFromYou/ToYou` packed from real state.
- Docs: superseding notes in `domain-types.md`, `application-ports.md`, `visibility-policy.md`.

## Verification
- Cells F1–F5 in `tests/conformance.rs`. **Process deviation:** the store/service code was written before the tests; the first test run still failed for real (`f3_cleanup`, `f5_*`: `unfollow`/`follow` public store methods duplicated the txn helpers and ignored requests), fixed by delegating to the helpers.
- `cargo test` (full): 179 passed, 0 failed.
- Inbound-Follow-to-locked (remote follower) is covered by code path only at unit level via `record_follow_request_activity` (F4); no HTTP-level inbox test was added.
- Accept/Reject activities for a locally-accepted request embed a synthetic Follow id (`{follower}/follows/{request id}`), not the remote's original activity id (not stored). Misskey/Sharkey match on actor/object; Mastodon-style id matching is an open item.
- **Live smoke NOT run:** `scripts/smoke-phase2.sh` failed pre-flight here (`Sharkey needs Node 22`). `tests/federation_interop.rs` was extended (lock pt3, mk1 follows, pending, list, accept, followers==1, unfollow) and compiles, but is unexecuted.
