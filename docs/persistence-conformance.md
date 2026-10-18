# Persistence conformance tests

**Status:** Frozen (Phase 0 freeze review 2026-10-14; harness green, see §9)
**Sources:** `sns-server-proposal.md` §§2, 5, 9; `docs/application-ports.md`; `docs/visibility-policy.md`.

Both storage backends (redb Phase 1, FoundationDB Phase 4) must pass this exact suite. The suite is backend-agnostic: it drives only the port interfaces from `docs/application-ports.md`, never backend internals. Same cases, same fixtures, both adapters.

## 1. Harness requirements

- Each case runs against a fresh, isolated store instance.
- Time is injected (`Clock`); IDs generated inside the store under test.
- Crash cases kill/restart the backend process (or drop the handle) mid-operation, then assert post-recovery state. Fault-injection hooks, if any, live in the harness, not the ports.
- Concurrency cases run N parallel clients; assertions allow any linearizable outcome but forbid lost updates and duplicates.

## 2. Group A — atomic state + job commit (exit box 2 core)

- **A1 create-then-crash**: `create_note` with one `StreamingPublish` job; crash immediately after commit returns. After recovery: note readable AND job claimable. Neither may exist without the other.
- **A2 crash-before-commit**: crash injected before commit (harness-driven). After recovery: no note, no job. Partial state is failure.
- **A3 multi-job atomicity**: `create_note` with three jobs; all-or-nothing after crash at any point.
- **A4 delete atomicity**: `delete_note` + jobs; tombstone and jobs appear together, note body gone for non-author viewers.
- **A5 follow atomicity**: `follow` + `FederateDeliver` job appear together.

## 3. Group B — job queue semantics

- **B1 lease redelivery**: claim with short lease, never complete, advance clock past expiry → job claimable again with same idempotency key.
- **B2 no double-claim**: job claimed under valid lease is invisible to concurrent `claim_next`.
- **B3 fail/retry**: `fail(job, retry_at)` → unclaimable before `retry_at`, claimable after, `attempts` incremented.
- **B4 complete is terminal**: completed jobs never reappear across restarts.
- **B5 crash-during-handler**: claim, crash before complete → redelivered after lease expiry (at-least-once proof).
- **B6 payload bound**: enqueue path rejects payloads over 64 KiB (`application-ports.md` §6).

## 4. Group C — accounts and handles

- **C1 uniqueness race**: N concurrent `create_account` with same handle (mixed case) → exactly one succeeds, rest `Conflict`.
- **C2 case-insensitivity**: `find_by_handle` resolves any case; `create_account` with differing case collides.
- **C3 rename tombstone**: rename A→B; new account cannot take A (`Conflict`); actor URL unchanged; old handle unresolvable.
- **C4 unknown handle**: `find_by_handle` on free name returns empty (not error).

## 5. Group D — activity dedup

- **D1 first-seen**: `check_and_record(new_id)` → false, then → true.
- **D2 dedup + state atomicity**: inbox-style op (record + state + jobs) crashed mid-way; retry observes already-seen and produces no duplicate state or jobs.
- **D3 concurrent duplicates**: N parallel `check_and_record(same_id)` with state write → exactly one winner writes state.

## 6. Group E — visibility agreement

One predicate, three call sites (`application-ports.md` §10, `visibility-policy.md` intro). Fixture matrix over {public, home, followers, specified} × {author, follower, mentioned, replied-to-author, stranger, anonymous} × {local_only on/off}:

- **E1**: API packing predicate, timeline query filter, and streaming filter agree on every cell.
- **E2**: `followers` remote-remote fallback cell (both remote, no local follow row) → visible.
- **E3**: `specified` with empty `visible_user_ids` rejected at creation (`Validation`).
- **E4**: `local_only` note never appears in any federated-eligible read, regardless of level.

## 7. Group F — bounds and isolation

- **F1 media bytes rejected**: storing blob bytes through any metadata path fails; hashes only.
- **F2 transaction scope**: a single use case touches bounded keys (harness asserts operation completes within backend TX limits; FDB 5 s / 10 MB — the adapter must split, never the application).
- **F3 read-your-commit**: committed state visible to subsequent reads on all nodes/handles (single-node: immediate; multi-node: after commit, no stale routing).

## 8. Exit-box-2 acceptance test

`conformance_exit_box_2`: drive `create_note` (author, text, visibility public) requesting one durable job through the storage interface; crash; recover; assert note + job both present and linked (job payload references the note ID). This single test is the Phase 0 exit evidence for box 2; it must run green against at least one backend or faithful harness before Phase 1.

## 9. Execution status

First green run achieved 2026-10-14: throwaway in-memory harness (`/tmp/opencode/phase0-harness`, std-only Rust, explicitly not product code) implements the ports over maps with commit-point fault injection and passes 15/15 suite cases plus the §8 acceptance test (`cargo test`: groups A1–A4, B1/B3/B4/B6, C1/C3, D1/D3, E1/E3, exit-box-2). Real thread-race cases (C1, D3) included. Evidence in `state/logs/2026-10-14-phase-0-freeze.md`.

Honest scope: this proves suite coherence and interface viability, not a backend. First real-backend green run = redb adapter, Phase 1; both backends must still pass this exact suite before they ship.

## 10. Group Q — quote posts (`docs/quote-posts.md`)

Backend-agnostic, same cases both adapters (FDB inherits):

- **Q1 create persists**: quote relation + resolved `target_note_id` stored with the note; pure renotes stay `quote: None`; legacy rows without a `quote` key decode as non-quotes.
- **Q2 transitions**: `Approved` requires a stamp (no false approval); `Pending→Approved` + jobs commit together; same-state repeats succeed (replay retains the stamp); `Approved→Rejected`/`Approved→Unknown` rejected; `Approved→Revoked` valid.
- **Q3 stamps/lookups**: unknown request/stamp lookups are `None`; request lookup finds the pending quote; stamp round-trips; stamp lookup finds the quote after approval.
- **Q4 request dedup**: `record_quote_request_accept` commits stamp + `Accept` job atomically; replay applies nothing (no duplicate job); reject path `true` then `false` on replay.
- **Q5 decision/revocation**: `Accept` moves `Pending→Approved` with the stamp; replay is `None`; `Reject` retains relation + projection (`renote`, text intact); `record_quote_stamp_delete` moves `Approved→Revoked` with forwarding jobs and flips the backing stamp row `revoked=true` in the same txn; replay is `None`; `Revoked→Approved` with a fresh stamp is legal (revocation is not terminal).
- **Q6 crash consistency**: quote create-then-crash — note + quote state + outbox job survive SIGKILL after commit (redb crash harness; mirrored per backend).

## 11. Group R — branched replies (`docs/replies.md`)

Backend-agnostic, same cases both adapters (FDB inherits once implemented):

- **R1 index+counter**: creating a reply stores the child index entry and increments the parent's `replies_count` in the same atomic commit; non-replies add neither.
- **R2 paging+deleted**: `notes_by_reply` returns newest-first direct children only, honoring cursor bounds; deleted children are excluded without cutting the page short.
- **R3 delete consistency**: deleting a reply decrements the parent's `replies_count` exactly once (saturating) and drops its index rows in the same transaction; descendants keep their `reply_id`.
- **R4 visibility-aware scan**: the `note_replies` page loop skips invisible/deleted candidates and advances until the visible page is full or the range is exhausted (no short pages, no stall on hidden rows).
- **R5 crash-before-commit**: reply create crashed before commit leaves no note, no index entry, no counter increment, no job.
- **R6 link idempotency**: `link_pending_reply` links exactly once across retries (no double counter or index entries), re-narrows visibility, and rejects URI mismatch and cycles as `Validation`.
- **R7 migration backfill+resume**: the versioned backfill indexes pre-existing reply links in bounded batches, resumes from its persisted cursor after an interrupted run, and writes the done marker only on completion.
- **R8 read withholding**: pending-parent notes are invisible to ordinary reads and absent from `notes/replies` until linked; they count as replies under `withReplies=false`.
- **R9 repair**: the opt-in repair path re-queues resolution only for stored remote notes whose fetched source still declares a resolvable `inReplyTo`; genuine roots are untouched.
- **R10 worker resolver**: the `ReplyParentResolve` job fetches the unavailable parent, links waiting children in bounded batches, retries with backoff, and quarantines past budget with the entry kept.
- **R11 children unchanged**: `notes/children` still returns direct replies plus eligible quote-renotes.
- **R12 conversation unchanged**: `notes/conversation` still returns the visible ancestor chain oldest-first with the cycle/depth guard.

Execution status (honest): R1–R12 are green on redb only (`tests/conformance.rs`); the FDB adapter is still a stub and has not run them. The cases are backend-generic, so FDB inherits them when implemented.

## 12. Group G — blocking / muting (`docs/misskey-parity-plan.md` §0A)

Backend-agnostic, same cases both adapters (FDB inherits once implemented):

- **G1 block CRUD**: unique pair, `Conflict(ALREADY_BLOCKING)` on dup, `Validation(BLOCKEE_IS_YOURSELF)` on self, `NotFound(NO_SUCH_USER)` on unknown, directional id lists (`blocking_ids_for` / `blocked_by_ids_for`), newest-first cursor paging (`sinceId`/`untilId`/`sinceDate`/`untilDate`, `limit`), `NotFound(NOT_BLOCKING)` on missing unblock.
- **G2 block atomicity**: one `block` commits the row, both follow edges (counters adjusted), both pending follow requests, and the jobs in a single transaction.
- **G3 enforcement**: storage-level `BLOCKING`/`BLOCKED` on follow/request-follow in both directions, `YOU_HAVE_BEEN_BLOCKED` on react and pure-renote create; quotes pass the block gate.
- **G4 mutes**: mute/renote-mute lifecycle with `ALREADY_MUTING`/`NOT_MUTING`/`MUTEE_IS_YOURSELF`/`NO_SUCH_USER`; past `expiresAt` creates nothing (no error); `is_muting`/`muted_ids_for` report only active (unexpired) rows.

Execution status (honest): G1–G4 are green on redb only (`tests/conformance.rs`); the FDB adapter is still a stub and has not run them. The cases are backend-generic, so FDB inherits them when implemented.

## 13. Group S — drafts + scheduled arming (`docs/misskey-parity-plan.md` §5)

Backend-agnostic, same cases both adapters (FDB inherits once implemented):

- **S1 draft CRUD**: create/get/update/delete through the ports; `get_draft` on unknown ids is `None`; `update_draft` on unknown rows is `NotFound(NO_SUCH_NOTE_DRAFT)`; `delete_draft` on unknown rows is a no-op success; `count_drafts` / `count_scheduled_drafts` track the rows; `drafts_by_user` reads newest-first with the `scheduled` tri-state filter; delete commits the row/index removal and the jobs atomically.
- **S2 scheduled arming**: `create_draft` with a `PostScheduledDraft` job leaves it pending but unclaimable before `not_before` (`scheduled_at`); claiming past due returns the job with the armed time in the payload; `remove_scheduled_draft_jobs` clears every arming for the draft (repeat clears are no-ops returning 0; unrelated drafts untouched).

Execution status (honest): S1–S2 are green on redb only (`tests/conformance.rs`); the FDB adapter is still a stub and has not run them.

## 14. Group P — pinnings (`docs/misskey-parity-plan.md` §7)

Backend-agnostic, same cases both adapters (FDB inherits once implemented):

- **P1 pinning CRUD**: `add_pinning` inserts the pair; duplicates are `Conflict(ALREADY_PINNED)`; `pinnings_for_user` lists oldest-first (pinning-id order); `remove_pinning` drops the pair and repeats/missing pairs are no-op successes.
- **P2 pinning atomicity**: add/remove commit the row change and the jobs in a single transaction (`pending_count` deltas).

Execution status (honest): P1–P2 are green on redb only (`tests/conformance.rs`); the FDB adapter is still a stub and has not run them.

## 15. Group E — note edits (§8) + quote-disable storage (§3)

Backend-agnostic, same cases both adapters (FDB inherits once implemented).

Naming note: this storage group reuses the `E1`–`E6` case labels from `docs/misskey-parity-plan.md` §§3/8; it is unrelated to the Phase-0 visibility Group E in §6 above. Test functions use distinct `note_edit_*` / `quote_disable_*` names.

- **E1 edit lifecycle**: create note (with poll), `apply_note_edit` with changed text+cw → returned + stored note updated, `updated_at_ms` Some, `note_edits(note_id)` has one row with `old_text` = original, `new_text`/`cw` set. Author mismatch → `Validation(YOU_ARE_NOT_THE_AUTHOR)`; unknown/deleted note → `NotFound(NO_SUCH_NOTE)`. Reply-parent immutability is structural (`EditNoteInput` has no reply field) — a reply note's `reply_id` survives an effective edit.
- **E2 no-op**: edit with identical effective text/cw/poll/files/renote/acceptance → returns the ORIGINAL note (`updated_at_ms` stays `None`), writes no history row, enqueues no jobs (`pending_count` unchanged, even when jobs are passed).
- **E3 poll replace resets votes**: create note with poll, add vote rows (`PollStore` vote API), edit with a different poll definition → poll row replaced with zeroed tallies, all `POLL_VOTES` rows for that note dropped (re-vote succeeds; tallies restart), one history row, and the passed jobs (including the poll-expiry re-arm) committed.
- **E4 history chaining**: two effective edits → `note_edits` returns 2 rows oldest-first; row2.`old_text` == row1.`new_text`; row1.`old_date_ms` == creation time (parsed from the time-ordered note id, stamped by the same clock the factory's id generator reads) and row2.`old_date_ms` == row1.`updated_at_ms`.
- **E5 inbound dedup**: `record_note_edit_activity` first call → `Some(note)` + history row + jobs committed; replay same activity id → `None`, no second history row, no extra jobs.
- **E6 quote-disable storage (§3)**: (a) remote author (`host` set) with `reject_quotes=true` (set via `AccountStore::update_account` patch) creates a quote → stored note is PLAIN (`renote_id` None, `quote` None, `process_errors` contains `"quoteUnavailable"`), no `renote_count` bump on target; (b) local author with `reject_quotes=true` → `Validation(QUOTE_DISABLED_FOR_USER)`; (c) remote author with flag false → quote stored normally (control).

Execution status (honest): E1–E6 are green on redb only (`tests/conformance.rs`: `note_edit_lifecycle`, `note_edit_noop_writes_nothing`, `note_edit_poll_replace_resets_votes`, `note_edit_history_chaining`, `note_edit_inbound_dedup`, `quote_disable_storage`); the FDB adapter is still a stub and has not run them.

## Note URI uniqueness (Group U, added 2026-10-17; Draft)

Every backend MUST keep **at most one live note per non-empty `uri`**, enforced
inside the storage transaction (not by caller-side find-then-insert):

- `record_note_activity(activity_id, input, jobs)` with a `uri` already held by a
  live note returns `Ok(None)` (a replay of the note under a different activity id),
  commits nothing, and enqueues none of `jobs`.
- `create_note` with a taken `uri` returns `PortError::Conflict("DUPLICATE_NOTE_URI")`.
- Deleting a held note releases its `uri` (Group U3). The one exception is a `Delete`
  that finds no note: see Group T below.
- `NoteStore::find_by_uri` is an indexed lookup, never a table scan.
- redb: `NOTES_BY_URI` (uri → note id), written in the note's transaction, backfilled
  at open (`run_uri_index_backfill`; pre-existing duplicate rows resolve to the richest
  row, losers stay in `NOTES` but are unreachable by uri). FDB: a key per uri in the
  note transaction; the read of that key is the conflict range.
- Tests: `u1_same_uri_different_activity_ids`, `u2_concurrent_same_uri`,
  `u3_deleted_note_releases_uri` in `tests/conformance.rs`.

## Remembered deletions (Group T, added 2026-10-17; Draft)

**Why:** bench S6 (2026-10-17). Misskey and Catodon send `Delete` before the `Create`
it races with. Before this, a `Delete` that found no note returned 202 and kept nothing,
so the late `Create` stored the note and it stayed live.

- `record_delete_uri_activity(activity_id, object_uri, jobs) -> Result<bool, PortError>`
  (`ActivityDedup`) is one atomic write:
  - A replayed `activity_id` returns `Ok(false)` and changes nothing.
  - If a live note holds `object_uri` (it may have appeared since the caller's lookup),
    it is deleted with `jobs`, the uri is released as in U3, and the result is `Ok(true)`.
  - Otherwise a tombstone is written for `object_uri` and the result is `Ok(false)`.
    `jobs` are dropped, since no note was deleted.
  - An empty uri returns `Validation`.
- A tombstoned uri refuses later writes of that uri: `record_note_activity` returns
  `Ok(None)` (no row, no jobs), and `create_note` returns `Conflict("TOMBSTONED_NOTE_URI")`.
- Only the `Delete` path writes tombstones. Deleting a held note (`delete_note`,
  `record_delete_note_activity`) does not, so U3 still holds: a re-fetch after deleting a
  held note may recreate it.
- Authority is the caller's job: `handle_delete` tombstones only when the deleter's host
  equals the object URI's host and the object is not local. Federation input can therefore
  grow the tombstone table without bound. Accepted for the MVP, with the same limit as the
  activity-dedup table.
- The race between a concurrent `Create` and `Delete` closes inside the write transaction:
  either the `Create` commits first and the `Delete` deletes it, or the `Delete` commits
  first and the `Create` is refused.
- Known gap: a note that appears between the caller's lookup and the write is deleted
  without a `NoteDeleted` streaming event, because the caller does not yet know the note id.
- redb: `TOMBSTONES_BY_URI` (uri → marker), created at open. FDB: a key per uri, read in the
  create transaction, so the read is the conflict range.
- Tests: `t1_delete_before_create_leaves_tombstone`, `t2_delete_by_uri_removes_and_releases`,
  `t3_delete_replay_and_scope` in `tests/conformance.rs`. Handler level:
  `federation::inbox::delete_tombstone_tests::delete_before_create_does_not_resurrect`.
- Port change: `ActivityDedup::record_delete_uri_activity` is new. `docs/application-ports.md`
  is Frozen and is NOT edited. This addendum and state log `2026-10-17-bench-rerun.md`
  supersede it for this method, pending the owner's sign-off.

## 16. Notification order (Group N, added 2026-10-17; Draft)

- **Contract:** `list_notifications` and `list_notifications_filtered` return a
  notifiee's notifications newest-first **across all kinds**. `sinceId`/`untilId`
  compare on the time-ordered suffix of the id, and `sinceDate`/`untilDate` use that
  same suffix. Ids have the shape `notify:{kind}:{suffix}` (plain ids are their own
  suffix).
- **Why:** the index used to be keyed by the full id. Full ids start with the kind,
  so rows came back grouped by kind (replies, then renotes, reactions, mentions,
  follows). Mentions were buried on the first page of `i/notifications`.
- **Redb:** `notifications_by_user_ordered` maps `(notifiee, suffix) → id`. It is
  built once from `notifications` at open (META `migration:notifications_by_user_ordered:v1`).
- **Test:** `n1_notifications_newest_first_across_kinds` (tests/conformance.rs, Group N).
  It failed on the pre-fix code with grouped output.
- **FDB (Phase 4):** the per-user index must also be keyed by the time-ordered suffix.
  Keying it by the full id reproduces the bug.

## 17. Groups H/I/J — moderation, registration/invites, favorites/emoji (Batch M §3.4; added 2026-10-17; Draft)

Backend-agnostic, same cases both adapters (FDB inherits). Atomicity rule (§2.3 of the guide): a moderation action's state change, its moderation-log row, and its outbound jobs commit in ONE transaction. The cases prove it through the pure port interface by forcing the log append to fail inside the action (invalid row) and asserting nothing else committed.

### Group H — moderation (executed: `tests/conformance.rs` `h1_suspend_atomicity`, `h2_unsuspend_atomicity`, `h3_suspend_unsuspend_race`, `h4_log_never_lost`, 4/4 green 2026-10-17)

- **H1 suspend atomicity**: `set_suspended(true)` commits `is_suspended`, the follow-request teardown in both directions (upstream `postSuspend`), the `suspend` log row, and the outbox job together. With an invalid log row the whole action fails: flag unchanged, no row, no job.
- **H2 unsuspend atomicity**: `set_suspended(false)` commits flag + `unsuspend` row + job together (same fail-closed rule); the removed follow requests stay removed.
- **H3 concurrent suspend and unsuspend**: N threads alternate suspend/unsuspend on one account; every call lands, exactly one log row per call (none lost, none duplicated), account row still valid.
- **H4 log append never lost**: `update_account_logged` commits patch + row + jobs together; an invalid row aborts all three; committed pairs survive reopen.

### Group I — registration and invites (executed: `tests/conformance.rs` `i1_invite_single_use`, `i2_consume_create_atomic`, `i3_registration_policy`, `i4_consume_happy_path`, 4/4 green 2026-10-17; ports: `InviteStore`)

- **I1 single-use invite under N retries**: one code claimed by 5 `consume_and_create_account` calls → exactly one succeeds and creates exactly one account; the others fail `NotFound` and create nothing (the unique-index row goes with the claim).
- **I2 consume + create atomicity**: a failing account insert inside the claim (duplicate handle) aborts the whole transaction — the code stays unused and claimable and no account row appears; a later valid claim then succeeds. No account without a consumed invite, no consumed invite without an account.
- **I3 registration policy persists**: `InstanceSettings.registration` (upstream `disableRegistration`) round-trips through `InstanceSettingsStore` and each write commits with its `updateServerSettings` row. The gate itself is application logic (`AppServices::signup`); its enforcement is asserted by the endpoint test `wp2_signup_policy_gate_and_invite_claim` (closed + no code → plain 400, nothing created).
- **I4 claim happy path + refusals**: a valid unexpired code creates the account, marks `usedBy`/`usedAt`, and drops the index row; re-consuming the same code, an unknown code, and an expired code all fail `NotFound` and create nothing (upstream's signup gate 400s for all three).

### Group J — favorites and emoji (cases land with WP7/WP8)

- **J1 favorite uniqueness** per (account, note) under concurrency.
- **J2 emoji shortcode unique case-insensitively** (account-handle rule, C2).
- **J3 favorite delete leaves the note**.
