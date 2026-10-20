# Application ports

**Status:** Frozen (Phase 0 freeze review 2026-10-14)
**Sources:** `sns-server-proposal.md` §§2, 4, 5; `docs/identity-rules.md`; Misskey `NoteCreateService.ts:170-191` (input shape reference only — our interfaces are clean-room).

## 1. Principles

- The application layer defines the interfaces it needs; adapters (`persistence/redb`, `persistence/foundationdb`, `jobs`, `media`, API/streaming/ActivityPub handlers) implement them. Domain types never appear with I/O attached.
- Ports are **narrow and use-case-shaped**, not a generic database framework (proposal §2): one method per application operation, each stating its atomicity. No generic `query()` / `execute()` escape hatches.
- Every mutating port method commits its **state change and its durable job/outbox records in one storage transaction** (proposal §5). There is no API to enqueue a job outside such a transaction.
- Transactions stay bounded: small payloads, no media bytes, no unbounded fan-out inside the transaction (proposal §§2, 5). Timelines start as fan-out-on-read.
- Both backends implement these exact signatures; the persistence conformance suite (separate deliverable) tests the behavior stated here.

## 2. Shared kernel

- **IDs**: opaque `aidx` strings per `docs/identity-rules.md` §2. Application code never mints IDs inline; it calls `IdGenerator`. Storage columns reserve `varchar(32)`.
- **Clock**: `now() -> timestamp`. Injected so tests control time (ID time-prefixes, `not_before`, leases).
- **Errors**: `Validation` (domain rejection, caller fault), `Conflict` (handle taken, duplicate, stale state), `NotFound`, `StorageUnavailable` (backend fault, safe to retry the whole use case). Adapters map internal errors to these four; no DB-specific errors cross the port boundary.

## 3. Port: `IdGenerator`

- `generate() -> Id` — new unique time-ordered `aidx` ID using the injected clock.
- `parse_time(id) -> timestamp` — extract embedded creation time (for cursors, `published` rendering). Never fails on IDs this instance generated; rejects foreign shapes as `Validation`.

## 4. Port: `AccountStore` (includes the account directory)

Single interface covers single-node and multi-node; the single-node adapter implements ownership trivially (itself).

- `create_account({ handle, … }) -> Account` — atomically: uniqueness check on normalized handle, `UsedUsername` tombstone check, account row, handle→ID mapping. Failure cases: `Conflict(handle_taken)`, `Validation(bad_handle)`.
- `find_by_id(id) -> Account?`
- `find_by_handle(normalized_handle) -> Account?`
- `rename(id, new_handle)` — atomically: checks, mapping update, old handle inserted into `UsedUsername` (never reusable, per identity rules §3). Account ID, actor URL, signing identity unchanged.
- `get_owner(id) -> NodeId` / `set_owner(id, node, migration_proof)` — directory reads/writes. Ownership transfer requires the explicit migration procedure (proposal §3); the port exposes the primitive, the procedure lives in the cluster contract.

## 5. Port: `NoteStore` — exit-box-2 method lives here

`create_note(input, jobs) -> Note` — validates via domain rules, persists the note, enqueues the given durable jobs, **all in one atomic commit**. This is the Phase 0 exit-criterion operation: "a note creation use case can atomically persist both a note and a durable job through the storage interface."

`CreateNote` input (field set grounded in Misskey's `Option`, `NoteCreateService.ts:170-191`; unsupported items recorded, not silently dropped):

| Field | Rule |
|---|---|
| `author_id` | required, must resolve to a local account |
| `text`, `cw` | nullable; at least one of text/attachment/renote required (domain rule) |
| `visibility` | default `public`; allowed set fixed by the visibility-policy contract (pending) |
| `local_only` | default `false` |
| `reply_to`, `renote_of` | optional note IDs; must exist and permit the reply/renote visibility |
| `file_ids` | optional; each must reference committed media metadata owned by the author |
| `poll`, `channel` | **unsupported in Phase 0** — rejected as `Validation(unsupported)` (polls deferred per proposal §7; channels out of scope) |
| `visible_users` | required non-empty iff `visibility = specified` (visibility-policy §6); each entry resolvable |

- `get_note(id) -> Note?`
- `delete_note(id, jobs)` — tombstone + jobs atomically (tombstone URI `{url}/notes/{id}`, per identity rules §4). Remote delete flows reuse this same operation (proposal §4: one business logic, both adapters).

## 6. Port: `JobQueue` (durable outbox)

Jobs are the only cross-transaction communication. Workers (in-process Phase 1, multi-worker Phase 4) consume this port; application code only produces through §5-style atomic methods.

Job envelope:

| Field | Rule |
|---|---|
| `id` | `aidx`, time-ordered (queues drain oldest-first) |
| `kind` | closed enum; initial set: `StreamingPublish`, `Notify`, `FederateDeliver` (`TimelineMaterialize` reserved for the Phase 5 hybrid, not produced yet) |
| `payload` | small structured data (IDs + hints, never media bytes, never follower lists); **cap 64 KiB** (keeps FDB transactions far under the 10 MB limit) |
| `idempotency_key` | required; handlers must tolerate redelivery |
| `not_before` | earliest execution time (backoff without new rows) |
| `attempts`, `lease_expires` | worker bookkeeping |
| `failed`, `fail_reason` | terminal quarantine state: `failed` leaves the claimable set but stays inspectable; `fail_reason` records why (`bad-payload: …`, `permanent-failure: …`, `retry-exhausted`) |

Operations:

- `claim_next(lease_duration) -> Job?` — atomically assigns the oldest due, unleased job. Lease expiry re-makes it claimable: crash-safe, at-least-once redelivery. Quarantined (`failed`) jobs are never returned.
- `complete(job_id)` — terminal success. No-op success on missing jobs.
- `fail(job_id, retry_at)` — increments attempts, sets `not_before`; no-op success on missing jobs and on already-quarantined jobs (terminal state is sticky, never re-queued). When attempts reach `MAX_JOB_ATTEMPTS` (8) the job auto-quarantines with reason `retry-exhausted` instead of rescheduling.
- `fail_permanent(job_id, reason)` — quarantine: the job leaves the claimable set but stays inspectable via `get_job` with `failed == true` and `fail_reason` recorded. Idempotent (keeps the first reason); no-op success on missing jobs.
- `pending_count()` — counts actionable (non-quarantined) jobs only.

> **Superseded (2026-10-15, Phase 2.5 Slice 5):** the `fail` line above replaces the original "`fail(job_id, retry_at)` — increments attempts, sets `not_before`; poison jobs stay visible via `attempts` for later dead-letter policy (dead-letter handling deferred to the delivery contract)." Dead-letter handling is no longer deferred: workers quarantine bad payloads (`bad-payload: …`) and delivery `PermanentFailure`s (`permanent-failure: …`, logged) via `fail_permanent`, and retry exhaustion quarantines with `retry-exhausted` for every job kind (port-level cap `MAX_JOB_ATTEMPTS = 8`, shared with the worker). Quarantined rows are excluded from `claim_next`/`pending_count` but retained for inspection. The legacy-payload no-op branch in the federation outbox is removed: unparseable `FederateDeliver` payloads are `Validation` failures the worker quarantines.

Guarantees: a committed job is never lost (durable before the local operation that created it is acknowledged, proposal §7); delivery is at-least-once with handler-side idempotency, never assumed exactly-once (proposal §5).

## 7. Port: `FollowStore`

- `follow(a, b, jobs)` / `unfollow(a, b, jobs)` — relationship change + jobs atomically. Cross-node local follows are internal operations (proposal §3), never ActivityPub to self.
- `is_following(a, b)`, `followers_of(id, cursor, limit)`, `following_of(id, cursor, limit)` — paginated reads serving fan-out-on-read timelines and delivery fan-out.

## 8. Port: `ActivityDedup`

- `check_and_record(activity_id) -> bool` — atomically records the inbound activity ID and reports whether it was already seen (proposal §7: ID deduplication + transactional state change). Inbox processing calls this inside the same transaction as the resulting state change + jobs.

## 9. Ports: `MediaStore` (metadata, in-DB) + `BlobStore` (bytes, out-of-DB)

- `MediaStore::insert_metadata({ id, owner, hash, mime, size, … })`, `get(id)` — metadata rows commit in the same transaction as the note/operation referencing them; hashes enable dedup and cache validation.
- `BlobStore::put(hash, bytes)`, `get(hash)`, `delete(hash)` — outside either database (proposal §5); local filesystem (single-node) or shared object storage (multi-node). Request-size limits and remote-URL validation live in the federation/media contracts; the port only moves bytes by hash.

## 10. Queries (reads)

Reads never need transactions spanning writes. Initial read surface (fan-out-on-read, proposal §5):

- `get_note`, `find_by_handle`/`find_by_id` (above).
- `notes_by_author(author, cursor, limit)` — cursor is a time-ordered note ID (`sinceId`/`untilId` style); ordering falls out of `aidx`.
- Home-timeline assembly = `following_of` + `notes_by_author` merge at read time. Capped async fan-out arrives only with measured justification (Phase 5); the ports gain a `TimelineMaterialize` producer/consumer pair then, not now.

## 11. Open decisions (must resolve before Frozen)

1. Closed `JobKind` variants for federation retries vs streaming — exact payload schemas per kind (likely a follow-up `jobs` contract or part of the conformance suite).
2. Lease durations and retry schedule numbers (deferred to the delivery contract; conformance tests need at least the backoff shape).
3. ~~`specified`-visibility model (`visible_users`)~~ — resolved by visibility policy §6 (supported, non-empty list required).
4. Cursor/limit defaults and maxima for the paginated reads — belongs to the endpoint manifest (client-visible behavior).

## Superseding note — Phase 2.6 slice 4 (2026-10-15)

`FollowStore` gains `request_follow`, `get_follow_request`, `follow_requests_to`, `accept_follow_request` (request delete + edge + counters + jobs in one txn) and `remove_follow_request` (reject/cancel + jobs in one txn). `ActivityDedup` gains `record_follow_request_activity`. Contract rules: `follow` supersedes a pending request for the same pair; `unfollow` of a pair with only a pending request removes it (and succeeds); `NotFound(NO_FOLLOW_REQUEST)` when accept/remove find none. Conformance cells F1–F5 in `tests/conformance.rs` (FDB inherits them).

## Superseding note — Phase 2.6 slice 8 (2026-10-15)

`InsertMediaInput` gains required `blob_key` (SHA-256 hex of the bytes; the `BlobStore` address). The `hash` input stays the MD5 (packer `md5`, per-owner dedup key). No port *trait* changed: `MediaStore` (`insert_metadata`, `get`, `find_by_owner_and_hash`, `find_by_source_url`, `delete_metadata`) and `BlobStore` (`put`/`get`/`delete`) keep their exact signatures — the length sidecar is internal to `FsBlobStore`. See the `docs/domain-types.md` slice-8 note (wipe) and `state/logs/2026-10-15-phase2-6-slice8-content-addressing.md`.

## Superseding note — Phase 2.6 slice 7 (2026-10-15)

`JobKind` gains `RemoteMediaFetch` with payload `RemoteMediaFetchPayload { note_id, attachments: Vec<RemoteMediaAttachment> }` (contract-stable mirror of the AP attachment shape; ≤16 entries, `note_id` may be `"__PENDING_NOTE_ID__"` at enqueue time and is substituted in-txn). `ActivityDedup` gains read-only `is_recorded` (advisory replay gate before any remote fetch; `record_*_activity` stays the atomic authority). `MediaStore` gains `find_by_source_url` (worker idempotency pre-check). `NoteStore` gains `attach_remote_media(note_id, input, jobs)` — media-row insert + `file_ids` append in one txn, idempotent per (`source_url`, note), `NotFound(NO_SUCH_NOTE)` when the note is missing/deleted. Conformance cells M1–M2 in `tests/conformance.rs` (FDB inherits them). See `state/logs/2026-10-15-phase2-6-slice7-media-safety.md`.

## Superseding note — Phase 2.6 slice 10 (2026-10-15)

`JobQueue` gains `complete_with_followups(job_id, followups)` (job done + follow-up jobs inserted in ONE write transaction), `enqueue_jobs(jobs)` (≤ `MAX_JOBS_PER_TXN` = 500 per call, single transaction) and `defer(job_id, not_before_ms)` (release lease and reschedule WITHOUT incrementing `attempts`; used for destination cooldown). Fan-out jobs complete with one `inbox_override` `FederateDeliver` follow-up per failed/cooldown inbox; fan-outs over 100 inboxes become per-inbox jobs entirely. Idempotency keys are `{job_key}:{inbox}`; receivers dedupe by activity id. Conformance cells `slice10_*` in `tests/conformance.rs` (FDB inherits). See `state/logs/2026-10-15-phase2-6-slice10-delivery-reliability.md`.

## Superseding note — quote posts (2026-10-16, `docs/quote-posts.md`)

`CreateNoteInput` gains `quote: Option<QuoteReference>` (domain-types §2; a renote with own commentary becomes a quote, pure renotes stay `quote: None`). `NoteStore` gains `set_quote_authorization(note_id, state, authorization_uri, verified_at_ms, jobs)` (guarded transition + jobs in one txn; `Approved` requires a stamp; same-state repeats succeed), `find_note_by_quote_request(request_uri)` / `find_note_by_quote_stamp(stamp_uri)` (request/stamp matching; `None` when unknown), `save_quote_stamp(stamp, jobs)` / `get_quote_stamp(stamp_id)` (stamp rows; `revoked` flags stay retained). `ActivityDedup` gains `record_quote_request_accept(activity_id, stamp, jobs)` (stamp + `Accept` job atomically; `None` on replay), `record_quote_request_reject(activity_id, jobs)` (`false` on replay), `record_quote_decision_accept/reject(activity_id, note_id, …)` (our quote `Pending→Approved/Rejected` atomically; `None` on replay) and `record_quote_stamp_delete(activity_id, note_id, jobs)` (`Approved→Revoked` + backing stamp row `revoked=true` in the same txn; `None` on replay). `JobKind` gains `QuoteStampReverify` with `QuoteStampReverifyPayload { accept_activity_id, request_uri, stamp_uri }` (rate-limited stamp re-verification off the inbox request; transient fetch failure retries with worker backoff, invalid content completes quietly, never approval). `FederateDeliver` payloads gain `QuoteRequest` (signed `Create` audience delivery + `QuoteRequest` to the quoted author's inbox), `QuoteResponse` (`Accept`+stamp / `Reject`), and `QuoteStampDeleted` (revocation forwarding). Conformance Group Q in `tests/conformance.rs` (Q1–Q6; FDB inherits them).

## Superseding note — branched replies (2026-10-16, `docs/replies.md`)

`CreateNoteInput` gains `reply_parent_uri: Option<String>` (inbox-only; the local `create_note` path rejects it, and the store rejects input with both `reply_to` and `reply_parent_uri` set). `NoteStore` gains `notes_by_reply(reply_id, cursor)` (newest-first direct children, deleted excluded, cursor bounds applied; `limit` is a candidate scan limit, 0 = backend default 50), `pending_reply_children(parent_uri, limit)` (children waiting on an unresolved parent URI), `link_pending_reply(child, parent, canonical_uri)` (atomic link + re-narrow + parent counter increment + index swap; idempotent `Ok(None)` on replay; `Validation` on URI mismatch or cycle), and `set_pending_reply_parent(child, uri, jobs)` (atomic pending mark + resolver job; idempotent bool). `JobKind` gains `ReplyParentResolve` with `ReplyParentResolvePayload { parent_uri }` (URI-keyed idempotency key `reply-resolve:{uri}`; the worker retries unavailable parents with standard backoff and quarantines past budget with the entry kept; full batches fan out a continuation). Opt-in repair: `repair_unresolved_reply_parents(services, limit)` (clamped 1..=5000) plus the `PINETREE_REPLY_REPAIR_LIMIT` startup env gate (off by default). Conformance Group R in `tests/conformance.rs` (R1–R12; FDB inherits them).

## Superseding note — §0A blocking / muting (2026-10-16, `docs/misskey-parity-plan.md`)

New ports (same atomicity rules as §2: state + jobs in one txn; FDB inherits the signatures):

- `BlockStore`: `block(Blocking, jobs)` (caller builds the row so the federated `Block` activity id `{url}/blocks/{id}` equals the stored row id), `unblock` (`NotFound(NOT_BLOCKING)`), `get_blocking`, `is_blocking`, `blocking_ids_for`, `blocked_by_ids_for`, `list_blockings(blocker, TimelineCursor)`. `block` validates the pair (`BLOCKEE_IS_YOURSELF`), rejects dups (`ALREADY_BLOCKING`), requires both accounts (`NO_SUCH_USER`), removes both follow edges and both pending requests, and enqueues jobs — atomically.
- `MuteStore`: `mute` (`Ok(None)` on past `expiresAt`, `ALREADY_MUTING` on dup), `unmute` / `renote_mute` / `renote_unmute` (`NOT_MUTING` when missing), `get_muting`, `is_muting` (active rows only), `muted_ids_for`, `list_mutings`, `get_renote_muting`, `is_renote_muting`, `renote_muted_ids_for`, `list_renote_mutings`.
- `ActivityDedup` gains `record_block_activity` (replay and re-block converge to `None`) and `record_unblock_activity` (`false` on replay or missing row).
- Follow/react/note-create entry points enforce blocks inside their own transactions (`BLOCKING`/`BLOCKED` on follow both directions, `YOU_HAVE_BEEN_BLOCKED` on react and pure-renote/reply create), so local and inbound paths share one check. Conformance cells G1–G4 in `tests/conformance.rs`.

## Superseding note — §4 polls (2026-10-16, `docs/misskey-parity-plan.md`)

New port (same atomicity rules as §2: state + jobs in one txn; FDB inherits the signatures):

- `PollStore`: `get_poll(note_id)` (`None` when the note carries no poll), `votes_by_user_for_note(user_id, note_id)` (choice-ordered; drives `isVoted` packing and dup detection), `cast_vote(vote, jobs)` (re-checks note-live → `NO_SUCH_NOTE`, poll-present → `NO_POLL`, §0A block → `YOU_HAVE_BEEN_BLOCKED`, expiry → `ALREADY_EXPIRED`, choice range → `INVALID_CHOICE`, dup → `ALREADY_VOTED` — then bumps `votes[choice]` and inserts the vote row atomically), `sync_poll_votes(note_id, counts_by_name, jobs)` (inbound `Question` tally refresh by choice-name match; `NO_POLL` when absent).
- `CreateNoteInput` gains `poll: Option<PollInput>` (`has_poll` must agree; storage validates choices/expiry in-txn) and inbox-only `poll_initial_votes: Option<Vec<u64>>` (origin-reported tallies for cached remote polls; the local path rejects it, storage enforces seed length).
- `JobKind` gains `PollExpiry` with `PollExpiryPayload { note_id }` (`not_before` armed at `expiresAt`; the worker publishes the `pollEnded` streaming event; gone/not-yet-expired polls complete quietly).
- `FederateDeliver` payloads gain `PollVote` (`Create`-vote activity to the poll owner's inbox) and `QuestionUpdated` (`Update(Question)` fan-out to followers + relays; skipped for local-only/deleted/poll-less notes at execution).

## Superseding note — §5 drafts + scheduling (2026-10-16, `docs/misskey-parity-plan.md`)

New application operations (`src/application/commands/drafts.rs`; Misskey API + ActivityPub adapters call these same ops; the worker calls `post_scheduled_draft`):

- `CreateDraftInput` (every `NoteDraft` column except `id`/`user`) + `UpdateDraftPatch` (`None` = absent, `Some(None)` = clear, `Some(Some(_))` = set).
- `create_draft` (draft-limit → scheduled-limit → schedule/poll-expiry validation → referential checks → store + arm in one txn), `update_draft` (partial-shape validation upstream parity; clears every arming then re-arms from the merged row in the same txn), `delete_draft` (clears armings, then deletes), `list_drafts` (cursor + `scheduled` tri-state), `count_drafts`.
- `post_scheduled_draft(draft_id, scheduled_at_ms)`: converges quietly on missing/unscheduled/rescheduled/not-yet-due drafts; posts through the full note-create path, deletes the draft on success and emits `ScheduledNotePosted` (carries the note), or keeps the draft and emits `ScheduledNotePostFailed` (carries the draft) on post failure.
- `NoteStore` gains `create_draft` / `get_draft` / `update_draft` (`NotFound(NO_SUCH_NOTE_DRAFT)`) / `delete_draft` (missing = no-op) / `drafts_by_user(user, cursor, scheduled?)` (newest-first) / `count_drafts` / `count_scheduled_drafts` — each committing the row, its by-user index row, and `jobs` in one transaction (FDB inherits; no second path).
- `JobKind` gains `PostScheduledDraft` with `ScheduledDraftPayload { draft_id, scheduled_at_ms }` (`not_before` armed at `scheduled_at`; bad payloads quarantine, gone/unscheduled/rescheduled drafts complete quietly, only storage faults retry; a failed post is itself a successful run).
- `JobQueue` gains `remove_scheduled_draft_jobs(draft_id)` (`clearSchedule` linear-scan parity: removes every arming including leased ones; the worker converges quietly on removed jobs).
- Limits live on `AppServices` (`note_draft_limit`, `scheduled_note_limit`, wired from `Config`) and surface in `policies` (`noteDraftLimit`, `scheduledNoteLimit`). Conformance Group S in `tests/conformance.rs` (S1–S2; FDB inherits them).

## Superseding note — §7 pinned / featured (2026-10-16, `docs/misskey-parity-plan.md`)

New application operations (`src/application/commands/pins.rs`):

- `pin_note` (ownership → `NO_SUCH_NOTE`; limit → `PIN_LIMIT_EXCEEDED`; duplicate → `ALREADY_PINNED`, enforced atomically by the unique store index; row + federation jobs in one txn), `unpin_note` (same ownership gate; missing pinning rows are no-op successes), `pinned_note_ids` (oldest-first).
- `NoteStore` gains `add_pinning` (`Conflict(ALREADY_PINNED)`), `remove_pinning` (missing = no-op), `pinnings_for_user` (oldest-first) — each with `jobs` in one transaction (FDB inherits; no second path).
- `FederateDeliver` payloads gain `FeaturedChanged { actor_id, note_id, added, inbox_override }` (followers fan-out of `Add`/`Remove` to `{actor}/collections/featured`; relays deferred — no relay infra exists).
- Featured ranking (compatible-simplified, NOT upstream-identical): `rank_featured_global` / `rank_featured_for_user` select by `reactions + renotes` over the trailing window (3d global / 7d per-user, the upstream window lengths) from recent non-sensitive public notes (no CW, no sensitive files — missing rows fail closed; no channels exist), ordered newest-first; `featured_notes_visible` applies `untilId`, visibility, and mute/block post-filters plus the page limit. `FeaturedCache` (3600s TTL, per-`ApiState`) backs both endpoints. Conformance Group P in `tests/conformance.rs` (P1–P2; FDB inherits them).

## Superseding note — §10 notification grouping (2026-10-16, `docs/misskey-parity-plan.md`)

Pure pack-layer change — no port or storage change. `pack_grouped_notifications` (packers) implements the upstream `i/notifications-grouped.ts:106-153` merge over the same flat newest-first page `i/notifications` fetches; the endpoint shares `parse_notification_query` with `i/notifications` and truncates the grouped page to `limit` after packing.

## Superseding note — Batch M moderation + instance settings ports (2026-10-17, `docs/implementer-guide-batch-m.md`)

Status: LANDED (redb adapter green). **All methods below are required — no `Err`/`unimplemented!()` defaults (guide §2 rule 4), so the FDB adapter does not compile until it implements them.** Atomicity is structural like the rest of this file: ports carry no transaction handle, so each compound method commits its state change, its moderation-log row, and its jobs in ONE storage transaction (§2.3; conformance groups H/I).

- `ModerationLogStore` (`src/application/ports.rs`):
  - `set_suspended(account_id, suspended, log, jobs) -> Account` — flag + follow-request teardown in both directions on suspend (upstream `postSuspend`) + log row + jobs, one transaction (conformance H1/H2/H3). The moderator-target refusal rule is application logic (the command), not storage.
  - `set_role(account_id, is_admin, is_moderator, log) -> Account` — the ONLY write path for the built-in role flags (`UpdateAccountPatch` cannot set them); log row in the same transaction.
  - `update_account_logged(account_id, patch, log, jobs) -> Account` — account patch + log row + jobs, one transaction (conformance H4).
  - `append(entry)` — standalone row for log entries that ARE the action (nothing else commits with them). Actions use the compound methods so a row can never be lost against its action.
  - `list_logs(filter, cursor) -> [ModerationLogEntry]` — newest-first, `TimelineCursor` since/until paging, `ModerationLogFilter { action, actor, target, search }` (upstream `admin/show-moderation-logs` params). Bounded scan (F2).
- `InstanceSettingsStore`:
  - `get() -> InstanceSettings` — fresh store = defaults (registration open, no blocked hosts, `maxNoteTextLength` 3000).
  - `update(patch, log) -> InstanceSettings` — patch + moderation-log row in one transaction (§3.3). `blockedHosts` normalize in the store: lowercased, trimmed, deduplicated, empties dropped (upstream `admin/update-meta.ts:260`). Blocked-host matching is the domain rule `domain::instance::host_is_blocked` (upstream `UtilityService.isBlockedHost` suffix match).
- Row types are domain (`domain/moderation.rs`, `domain/instance.rs`): `ModerationLogEntry { id, actor_account?, action, target?, details, created_at_ms }` (`details` is upstream `ModerationLogs.info` JSON), `AbuseReport`, `InstanceSettings`, `RegistrationPolicy`, `InviteCode`.
- `InviteStore` (WP2; upstream `RegistrationTicket` + `SignupApiService` claim):
  - `create_invites(inputs, log?) -> [InviteCode]` — 1..=100 codes in one transaction, optional one `createInvitation` row with the batch (upstream logs only `admin/invite/create`). Codes are generated by the application (upstream charset); ticket ids and `created_at_ms` are store-generated.
  - `find_invite_by_id(id)` / `find_invite_by_code(code)` — the code lookup is the unique index (a row present = claimable); `find_invite_by_code` answers `None` for consumed codes.
  - `list_invites(filter) -> [InviteCode]` — `InviteListFilter { created_by?, filter (all/unused/used/expired), since_id?, until_id?, since_date_ms?, until_date_ms?, sort, limit, offset }`. Bounded scan (F2). `sort` maps upstream's `sort` switch (`+createdAt` id DESC, `-createdAt` id ASC, `+usedAt` usedAt DESC NULLS LAST, `-usedAt` usedAt ASC NULLS FIRST).
  - `delete_invite(id, log?)` — row + unique-index entry removed with the log row (one transaction).
  - `consume_and_create_account(input) -> (Account, InviteCode)` — the signup claim: mark used + drop the unique-index row + create the account (with `password_hash`) in ONE transaction (conformance I1/I2). Missing, used, and expired codes fail `NotFound` and change nothing. `InviteCode` carries a ticket `id` (upstream `RegistrationTicket.id`, an aidx) that the guide §3.3 sketch omits but the packed shape and `invite/delete` require.
  - Application commands (`commands/invites.rs`): `signup` (registration policy + claim), `create_invites`, `list_my_invites` / `list_all_invites`, `delete_invite` (upstream ownership/used rules), `invite_limit` (`null` — the §9.1 default policy has no cap), `create_account_admin` (`admin/accounts/create`, gate bypass + `createAccount` row), `update_instance_settings` (`admin/update-meta` subset, `updateServerSettings` row).
- Coming with later WPs (same rules): `AbuseReportStore` (WP4), favorites/emoji stores (WP7/WP8, conformance J).

## Superseding note — Batch M WP3 suspension (2026-10-18, `docs/implementer-guide-batch-m.md`)

Status: LANDED (redb adapter green, conformance H5 executed). The compound-write side of suspension was already frozen above (`ModerationLogStore::set_suspended`); WP3 adds one REQUIRED read method and two job shapes so the suspend fan-out stays bounded (guide §2 rule 5 / §11):

- `AccountStore::remote_shared_inboxes(after: Option<&str>, max_rows: usize) -> (Vec<String>, Option<String>)` — **required, no default** (guide §2 rule 4: the WP3 sweep fan-out depends on it). Bounded enumeration of every knowable remote `shared_inbox`: scans ≤ `max_rows` account rows strictly after `after` (id order) and returns the non-empty `shared_inbox` values seen plus the next cursor (`None` = table exhausted). Never truncates silently; duplicates can span pages and the caller dedupes (`federation::outbox::collect_known_shared_inboxes`). Upstream gathers the same set from the `followings` rows' `followerSharedInbox`/`followeeSharedInbox` snapshots (`UserSuspendService.postSuspend`/`postUnsuspend`); pinetree reads it from the cached remote accounts (superset — a host known only through a mention still holds a cached actor). Conformance **H5**. FDB must implement the same walk semantics.
- `JobKind::UnfollowAll` + `UnfollowAllPayload { account_id, after }` (upstream `UserSuspendService.unFollowAll`, `silent: true`): ONE sweep job commits in the suspend transaction; the worker expands it at run time — each run removes ≤ `UNFOLLOW_ALL_PAGE` (100) outgoing follow rows, every row deletion in ONE transaction with its own `FederateDeliver` `Undo(Follow)` job (remote followees only), and a full batch returns a continuation carrying the advanced `after` cursor (skipped rows never loop the walk). Crash-safe: a re-run sees removed rows gone; deterministic activity ids (`suspend-unfollow-{follow_id}`) mean a retry mints no second `Undo(Follow)`.
- `StreamingEvent::UserSuspended { account_id }` (`StreamingPublish` job): the stream-close signal; the hub drops sockets whose viewer is the account (`streaming::server`).
- `FederateDeliverPayload::SuspendBroadcast { account_id, activity_id, undo, inbox_override }`: `Delete(Person)` (suspend) / `Undo(Delete(Person))` (unsuspend) to every knowable shared inbox (upstream `postSuspend`/`postUnsuspend`). ONE sweep job per action; the worker resolves the inbox set at run time and reuses the bounded `fan_out` (inline ≤ `FANOUT_INLINE_MAX`, one-inbox jobs beyond, batches ≤ `MAX_JOBS_PER_TXN`). `activity_id` is minted at enqueue so re-runs re-deliver the same activity ids (receiver-side dedupe).

## Superseding note — Batch N stores (2026-10-20, `docs/handoff-batch-n-gaps.md`)

Status: LANDED (redb adapter green, conformance J1–J3 + O1 executed). All ports below are **required, no defaults** (guide §2 rule 4); every mutating method commits state + jobs/log in one transaction (§2.3). The "coming later" line above is superseded: `AbuseReportStore` (WP4), favorites (WS2), emoji (WS1) all landed, plus antennas (WS4).

- `FavoriteStore` (WS2; Misskey `MiNoteFavorite` parity): `favorite_note(favorite, jobs)` (`Conflict(ALREADY_FAVORITED)` on the unique (user, note) index), `unfavorite_note(user, note) -> bool`, `is_favorited(user, note)`, `favorites_by_user(user, cursor)` (newest-first, favorite-id order). redb `favorites` table keyed `(user_id, note_id)`. Conformance Group J (J1 uniqueness, J3 delete-leaves-note).
- `EmojiStore` (WS1 + WP8b; Misskey `MiEmoji` parity): `create_emoji(emoji, log, jobs)` (`Conflict(DUPLICATE_NAME)` on the lowercased-shortcode unique index), `update_emoji(emoji, log, jobs)` (owner-agnostic row replace; shortcode moves re-key the index, `Conflict(SAME_NAME_EMOJI_EXISTS)` on race), `delete_emoji(id, log, jobs) -> bool`, `get_emoji(id)`, `get_emoji_by_name(shortcode)` (case-insensitive), `list_emojis(query?, cursor)` (name/aliases/category substring or exact `:name:`, newest-first), `all_local_emojis()` (pack path), `upsert_remote_emoji(row)` (metadata only; never touches `media_id`), `get_remote_emoji(host, shortcode)`, `all_remote_emojis()` (fetch fan-out source), `set_remote_emoji_media(host, shortcode, media_id)`. redb `emojis` (id → row) + `emojis_by_name` (lower shortcode → id) + `remote_emojis` ((lower host, lower shortcode) → row). Conformance Group J (J2 case-insensitive uniqueness + rename conflict + delete-frees-name + remote upsert/cache rules).
- `MediaStore::list_all_media(cursor)` (WS6 `admin/drive/files` + `show-file`): newest-first rows across all owners, id-ordered, `matches_cursor`-paged, capped at 100/page. The op filters (owner/type/origin/hostname); `show-file` resolves `fileId` or served-`url`/`source_url`.
- `AntennaStore` + `AntennaNoteStore` (WS4; guide WP15): `create_antenna(antenna, jobs)` / `update_antenna(antenna, jobs)` (owner-scoped on write; `NotFound(NO_SUCH_ANTENNA)` on miss), `delete_antenna(owner, id) -> bool` (feed cascade in the same txn), `get_antenna(owner, id)`, `antennas_by_owner(owner)` (newest-first), `all_active_antennas()` (match sweep source), `touch_antenna(owner, id, now)` (read-path reactivation); `append_antenna_note(antenna, note)` (dedupe by key + trim to `ANTENNA_NOTES_CAP` 200 in the same txn), `remove_antenna_note`, `delete_antenna_notes`, `antenna_notes(antenna, cursor)` (newest-first, ≤100/page). redb `antennas` + `antennas_by_owner` + `antenna_notes`. Conformance Group O (O1 trim + cascade).
- `JobKind::AntennaMatch` + `AntennaMatchPayload { note_id }` (WS4): one bounded job per created note (local + federated), enqueued in the note-creation transaction (`__PENDING_NOTE_ID__` substitution); the worker matches active antennas and publishes `StreamingEvent::AntennaNote` post-commit.
- `JobKind::RemoteEmojiFetch` + `RemoteEmojiFetchPayload { host, shortcode }` (WP8b): one bounded job per known uncached remote row on opt-in toggle (≤200); the worker SSRF-fetches (2 MiB cap, `image/*` gates), stores bytes + a drive metadata row, records the local copy.
- `AbuseReportStore::create_flag_report(activity_id, report) -> Option<AbuseReport>` (WS5 inbound `Flag`; upstream `ApInboxService.flag` parity): atomic check-and-record + insert (`None` on replay). Target = first mentioned local user; reporter may be remote.
