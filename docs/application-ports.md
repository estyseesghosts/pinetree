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
