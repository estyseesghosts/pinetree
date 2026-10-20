# Implementation Plan: Branched Replies in Pinetree

**Status:** Draft implementation plan (Slices A+B landed on redb 2026-10-20; C partial; FDB open — see status block)  
**Codebase basis:** Pinetree `master`, reviewed 2026-10-16  
**Scope:** Domain, persistence, Misskey-compatible API, ActivityPub ingestion, tests, and contracts

## Status (2026-10-20 freshness pass)

Verified against `src/persistence/redb/mod.rs`, `src/application/{ports,queries}.rs`, `src/application/commands/inbound/{notes,replies}.rs`, `src/federation/inbox.rs`, `src/domain/{note,visibility}.rs`, `src/activitypub/objects.rs`, `src/jobs/worker.rs`, `src/main.rs`, and `tests/conformance.rs` (Group R). Slices A and B are landed on redb. Slice C is partial. FDB is open.

**Slice A (storage index and query): LANDED on redb**
- Index `NOTES_BY_REPLY` (`src/persistence/redb/mod.rs:211-212`), opened at `:448`. The `(parent, child)` row is removed on delete (`:1856-1858`).
- Backfill: done marker `REPLY_INDEX_DONE_KEY` (`:265`), resume cursor `REPLY_INDEX_PROGRESS_KEY` (`:268`), bounded batches in `run_reply_index_backfill` (`:500-560`). Test R7 at `tests/conformance.rs:3874`.
- Query: `NoteStore::notes_by_reply` (`ports.rs:458`; redb read at `redb/mod.rs:4334-4370`). `AppServices::note_replies` is a visibility-aware bounded scan (`queries.rs:1216-1270`; batch `REPLY_SCAN_BATCH` at `:15`). Tests R2 and R4 (`conformance.rs:3381,3505`).
- Create: the parent's `replies_count` increments with `saturating_add` (`redb/mod.rs:1216-1227`). Test R1 (`conformance.rs:3294`).
- Delete counter (TODOS "replies_count never decremented"): LANDED on redb. `delete_note_in_txn` (`redb/mod.rs:1775-1876`) decrements the parent with `saturating_sub` (`:1865`) under the already-deleted guard (`:1797-1801`). It is the single implementation for every delete path (`:4212-4217`; callers `:8420`, `:8457`; inbound Delete at `src/application/commands/inbound/notes.rs:121-143`). Test R3 (`conformance.rs:3494`).

**Slice B (unresolved parent preservation): LANDED on redb**
- Inbound: a present but unresolved `inReplyTo` is stored as `reply_parent_uri`, never as a root (`src/federation/inbox.rs:2113-2126`). The `ReplyParentResolve` job commits in the same transaction as the note (`inbox.rs:2170-2177`). A parent's arrival enqueues a resolver for its waiting children (`inbox.rs:2184-2197`). TODOS "inbound drops unresolved inReplyTo": LANDED.
- Fail-closed: `src/domain/visibility.rs:71-73`. `notes/show` pending withhold at `queries.rs:456-460`. Tests R8 (`conformance.rs:3998`) and `src/domain/policy.rs:272`.
- Atomic link: `link_pending_reply` (`redb/mod.rs:4415-4560`) clears the pending URI, increments the parent counter (`:4551`), and swaps the index row. Replays return `Ok(None)`. Test R6 (`conformance.rs:3863`).
- Resolver: the `ReplyParentResolve` worker arm (`src/jobs/worker.rs:178-192`) and a bounded resolve with continuation (`src/application/commands/inbound/replies.rs:57-185`). Test R10 (`conformance.rs:4243`).
- Outbound: `inReplyTo` is the resolved parent's canonical URI. Pending parents are withheld (`src/activitypub/objects.rs:569-581`).
- Repair: `repair_unresolved_reply_parents` (`replies.rs:211-256`) re-fetches the origin document and marks only a declared `inReplyTo`. Test R9 (`conformance.rs:4123`). **The opt-in gate is env-only**: `PINETREE_REPLY_REPAIR_LIMIT` at `src/main.rs:307-322`. There is no JSON key (`src/config.rs`). This breaks the AGENTS.md configuration rule. OPEN.

**Slice C (federation and regression hardening): PARTIAL**
- LANDED: R11 (`notes/children` semantics, `conformance.rs:4366`), R12 (`notes/conversation` oldest-first, `:4401`), and outbound direct-parent `inReplyTo` (`objects.rs:569-581`).
- NOT VERIFIED in this pass: the Keystone sweep, live Misskey/Sharkey reply interop, and the Slice C federation tests for private or deleted parents (only the domain test `policy.rs:272` was located).
- OPEN: the §5 counter edge cases (deleting a child whose parent is deleted; a reply that is also a quote or renote). No test was located for them.

**Open items**
1. FDB parity for `notes_by_reply`, the pending link, and the index. `src/persistence/foundationdb/mod.rs` is empty.
2. The repair gate is env-only and startup-only (`main.rs:307`). Move it to `--config`, or record the exception.
3. Index removal on delete (§3.2): redb removes the row. FDB must use the same policy.

## 1. Goal and invariants

Store and expose the immediate parent of every reply reliably, then make direct-child queries efficient enough for a real nested tree in Keystone.

**Keep `Note.reply_id` as the canonical relationship.** A reply to `A` stores `reply_id = A.id`, even when `A` is itself a reply. Do not add `thread_id`, recursive child arrays, or a second authoritative tree representation.

```text
R
├── A        reply_id = R.id
│   ├── A1   reply_id = A.id
│   └── A2   reply_id = A.id
└── B        reply_id = R.id
```

Keep the three existing API contracts distinct:

| Endpoint | Contract to preserve |
|---|---|
| `notes/replies` | Paginated direct replies only (`reply_id == note_id`) |
| `notes/children` | Direct replies plus quote-renotes, matching Misskey behavior |
| `notes/conversation` | Visible ancestor chain, oldest-first, with existing cycle/depth guard |

The server does not need to return a recursive tree. Keystone will recursively call `notes/replies` for each expanded parent.

## 2. Current implementation findings

- `src/domain/note.rs` already stores `reply_id`, `reply_user_id`, and `replies_count`.
- `src/application/queries.rs::note_replies` currently scans `all_notes_bounded()` and filters by `reply_id`. It is bounded to the most recent 10,000 notes, so it is both work proportional to overall note volume and potentially incomplete for large instances.
- `NoteStore` has no indexed direct-child lookup.
- `src/persistence/redb/mod.rs` creates a reply and increments the parent's `replies_count` in one transaction, but has no parent-child index.
- The inspected delete path soft-deletes a note and decrements its author's `notes_count`, but does not decrement the note's parent's `replies_count`. Confirm this across all delete entry points, then fix it once in the store transaction.
- Outbound ActivityPub rendering already sets `inReplyTo` from the note's immediate `reply_id`, resolving the parent's canonical URI where available. Add regression coverage rather than rewriting this working behavior.
- Inbound ActivityPub ingestion explicitly stores replies to unknown remote notes **without threading**: it loses the unresolved `inReplyTo` after `find_note_by_uri` returns no cached parent. This must be fixed to avoid silently flattening remote replies.

## 3. Add an indexed direct-child lookup

### 3.1 Storage contract

Add a narrow `NoteStore` method for fetching direct children by parent ID. A suitable starting signature is:

```rust
fn notes_by_reply(
    &self,
    reply_id: &str,
    cursor: &TimelineCursor,
) -> Result<Vec<Note>, PortError>;
```

Define its contract explicitly: newest-first, direct children only, deleted notes excluded, and cursor bounds applied consistently with the existing endpoint. The `limit` passed to this storage method is a **candidate scan limit**, not necessarily the final number of visible results returned by the API.

Update every `NoteStore` implementation and test double, including FoundationDB-backed storage where implemented, so the interface does not become backend-specific.

### 3.2 redb index

Add an adjacency-list index:

```rust
const NOTES_BY_REPLY: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("notes_by_reply");
```

The key is `(parent_note_id, child_note_id)`. `Note.reply_id` remains authoritative; the index is rebuildable derived state.

- Create/open the table in initialization.
- When creating a note with a resolved `reply_id`, insert `(reply_id, note_id)` in the same write transaction as the note, author index, parent counter update, and durable follow-up jobs.
- Query only the index range for the requested parent; do not call `list_all_notes` from this method.
- Preserve reverse note-ID order and the existing exclusive `sinceId` / `untilId` semantics.
- Treat `limit` as a candidate batch size. If deleted index entries are retained, keep scanning the index until the candidate batch is full or the parent range ends; simply dropping deleted entries from a short raw range can make the caller stop too early.
- Keep index updates and parent-counter updates atomic with the note change.
- Leave index entries for soft-deleted children if convenient, but always filter deleted rows from results. Alternatively remove the index entry on deletion; whichever policy is chosen must be the same for both storage implementations and tests.

### 3.3 Apply visibility before completing a page

The application layer—not the storage adapter—owns `is_visible` and viewer/follow relationships. Do not fetch just one raw page of children and then filter it: if some candidates are private, that can return short pages even when later visible replies exist.

Implement `AppServices::note_replies` as a bounded scan loop:

1. Normalize the requested API page size using the existing endpoint defaults and limits.
2. Fetch a bounded candidate batch from `notes_by_reply`, newest-first, respecting the caller's original cursor bounds.
3. Apply the existing visibility policy and collect visible candidates until the requested visible page is full.
4. If the page is not full and the candidate batch is full, advance the internal exclusive `untilId` to the last scanned child ID and continue. Keep original `sinceId` and date bounds intact.
5. Stop when enough visible replies are collected, no candidates remain, or the original cursor range is exhausted. Ensure each scan advances, so a malformed ID or backend bug cannot loop indefinitely.

This keeps pagination correct without requiring storage adapters to understand visibility policy. Test the scan loop with many hidden/deleted replies preceding visible ones.

## 4. Migrate existing databases safely

Creating an empty index table on startup is insufficient: existing notes would disappear from direct-reply queries. Add a versioned schema migration that backfills `NOTES_BY_REPLY` from stored notes.

The migration must:

- run before serving requests that depend on the index;
- use a schema/migration version marker;
- process notes in bounded batches;
- persist progress so it can resume after a crash;
- be idempotent if a batch is replayed;
- write the completed-version marker only after the full scan and index writes finish;
- preserve all note IDs, parent links, counters, and other stored fields.

Do not run an unbounded all-notes write transaction. Add a migration test with existing multi-level data, forced interruption/restart, and a second run proving idempotence.

## 5. Reply counts and deletion

Keep `replies_count` as a denormalized count of direct replies; it is not a guarantee that every reply is visible to every viewer.

- **Create:** preserve the current atomic increment of the immediate parent's count; add the corresponding child-index insert in the same transaction.
- **Delete:** when a note is first soft-deleted and has a resolved `reply_id`, decrement that parent's counter in the same transaction, using saturating arithmetic. The existing already-deleted guard should make repeated deletion unable to decrement twice.
- **Delete of a parent:** do not delete descendants or silently reparent them. Preserve their `reply_id`. A deleted or invisible parent must not leak its old body through tree queries.
- **Pending-to-resolved link:** use one atomic storage operation to set the child `reply_id`, clear its pending URI, increment the parent counter, and insert the child index entry. Make it idempotent so retrying a job cannot double-count.
- **Counter edge cases:** verify deleting a child whose parent is itself deleted, a remote Delete, and a reply that also has quote/renote metadata.

## 6. Preserve unresolved ActivityPub reply parents

### 6.1 Current gap

`src/federation/inbox.rs::ingest_remote_note` currently resolves `parsed.in_reply_to` only through the locally cached note URI. If no parent is found, it sets `reply_to = None`; after that, the original target URI is not retained. The note can then be mistaken for a root note and cannot be attached later without reconstructing the relationship.

**Existing-data limitation:** already-ingested notes whose unknown `inReplyTo` was discarded cannot be identified from `reply_id = None` alone, because that also means a genuine root. A separate, bounded repair task may re-fetch stored remote notes with a canonical `uri`, inspect their original `inReplyTo`, and queue resolution when a target is found. It must be rate-limited, restartable, and based on the fetched structured field—not guesses from text. Do not run an unbounded remote fetch of every note during startup.

### 6.2 Persist a pending parent URI

Add an optional, backward-compatible internal field such as `reply_parent_uri: Option<String>` to the stored `Note` and the inbound `CreateNoteInput` path. Missing fields in old serialized notes must decode as `None`.

Rules:

- If `inReplyTo` resolves to a cached note, set `reply_id = Some(parent.id)` and leave `reply_parent_uri = None`.
- If `inReplyTo` is present but unresolved, persist it in `reply_parent_uri`; do not invent a root relationship or substitute a different parent.
- If neither field is set, the note is a genuine root note.
- Reject inconsistent internal input that sets both fields to conflicting parents.
- While `reply_parent_uri` is set, every query that excludes replies (for example `withReplies = false`) must treat the note as a reply, not a root. It must not appear in `notes/replies(parent)` until the parent has been resolved to an internal ID.
- **Fail closed on visibility while unresolved.** Because parent-derived visibility restrictions cannot yet be checked, do not expose the body through any ordinary read surface while the parent is unresolved: note detail, profiles, search, timelines, public/global feeds, or ActivityPub object responses. Apply this at a shared read-policy boundary and verify every packer/endpoint; hiding only the timeline row is insufficient. Keep the note and relationship stored for retry. Once linked, apply the same parent-visibility narrowing as a reply whose parent was cached during ingestion. If retries are exhausted, keep it pending and unavailable through ordinary reads until the parent arrives or an explicit repair action is taken. This temporary omission is preferable to misrepresenting the note as a root or potentially exposing content too broadly.
- Do not emit `reply_parent_uri` in the public API: it may disclose a private or otherwise non-displayable target.

### 6.3 Resolve pending links durably

Use the existing safe remote-fetch layer and durable job system; do not perform unbounded remote network I/O inline in the inbox request.

- Add an index/table for pending reply targets keyed by `(parent_uri, child_note_id)`.
- Add a durable resolver job with an idempotent handler for an unresolved parent URI. It should attempt to fetch/cache the parent through existing SSRF-protected code, then link waiting children in bounded batches.
- If a parent URI is still unavailable, retry with backoff. After the current job retry policy is exhausted, keep the unresolved URI/index entry so the relationship is not lost; do not flatten or expose the note as a root. A later parent arrival must be able to enqueue the same resolver again.
- When an inbound note with a canonical URI is stored, schedule resolution for any pending replies targeting that URI. Use one URI-keyed job that processes children in batches rather than enqueuing one huge job per child.
- Link only when the canonical URI matches. Before linking, verify the relationship does not create a cycle. Duplicate activities and resolver retries must not increment counters or insert duplicate index rows.
- Retain existing audience narrowing, visibility filtering, inbox authentication, activity deduplication, and remote-fetch safety limits.

The immediate-parent `inReplyTo` output is already correct for resolved relationships. The ActivityPub object endpoint must also withhold unresolved reply bodies under the fail-closed rule; after linking, existing rendering should emit the resolved parent's canonical URI. Do not expose the pending URI through the public Misskey-compatible JSON packer. Add tests proving replies-to-replies target the immediate parent and remote parents use canonical URIs. Do not introduce FEP-76ea as a requirement for basic threading; `inReplyTo` and the local parent index are sufficient.

## 7. API and contract changes

- Preserve `notes/replies` as a flat `Note[]` page of direct replies. Do not introduce recursive fields into the shared payload.
- Preserve `notes/children` and `notes/conversation` semantics exactly.
- Do not expose `reply_parent_uri` in the Misskey-compatible API. No custom wire field is needed for this slice.
- Update `docs/domain-types.md`, `docs/endpoint-manifest.md`, the storage contract, and the API/profile tests through an explicit superseding decision. These contracts are frozen, so do not silently edit them as if they were never frozen.
- Update current-state/phase documentation and append a dated work log after implementation, per `AGENTS.md`.

## 8. Implementation order

### Slice A — Storage index and query

1. Update the NoteStore contract and all storage implementations/test doubles.
2. Add the redb parent-child index and restart-safe migration.
3. Replace the bounded global scan in `note_replies` with indexed, visibility-aware batched scanning.
4. Add create/delete counter consistency and atomicity tests.

### Slice B — Unresolved remote parent preservation

1. Add the backward-compatible pending-URI field and storage index in each supported storage adapter; update every `CreateNoteInput` constructor and test fixture.
2. Preserve unresolved `inReplyTo` during inbound ingestion.
3. Add durable resolver/link jobs and cycle/idempotency protections.
4. Update the frozen domain, application-port, and persistence contracts; keep the public API note shape unchanged.
5. Add an opt-in, bounded repair path for historical remote notes whose original parent URI was discarded.

### Slice C — Federation and regression hardening

1. Verify outbound `inReplyTo` remains the direct parent's canonical URI.
2. Verify inbound replies whose parent is already cached, arrives later, cannot be fetched, or is private/deleted.
3. Verify `notes/replies`, `notes/children`, and `notes/conversation` do not change semantics.
4. Run persistence conformance, crash recovery, federation interop, Misskey client-profile, streaming, and Keystone sweep suites.

## 9. Acceptance tests

### Storage and API

- `R` has children `A` and `B`; `A` has child `A1`. `notes/replies(R)` returns only `A` and `B`; `notes/replies(A)` returns only `A1`.
- A new reply, its `NOTES_BY_REPLY` entry, the parent's `replies_count`, and durable jobs commit atomically. Injected pre-commit failure leaves none of the changes visible.
- Existing data is indexed across restart; an interrupted migration resumes and does not duplicate entries.
- Cursor paging has no duplicate/missing visible siblings when earlier candidate rows are deleted/private and page boundaries are crossed.
- A deleted reply is absent from results and decrements its parent's counter exactly once.
- Private notes, invisible parents, deleted bodies, and unresolved parent URIs do not leak through branch reads.
- `notes/children` continues to include eligible quote-renotes; `notes/conversation` stays oldest-first and guarded against cycles/depth overflow.

### Federation

- Outbound `inReplyTo` for `A1` targets `A`, not `R`; an unresolved target is persisted and hidden from read surfaces until resolved, then the note is rendered against the canonical parent URI.
- Inbound reply to a cached parent immediately gets the correct `reply_id` and index row.
- Inbound reply to an uncached parent retains its canonical pending URI, is withheld from ordinary reads while unresolved, and is never treated as a root.
- The optional repair task can recover a historical remote reply only when its fetched source object still declares a resolvable `inReplyTo`; it never guesses from text or rewrites genuine roots.
- When the parent later arrives or is successfully fetched, the child links exactly once; its index and counts update atomically.
- Resolver retries and inbox redelivery may enqueue duplicate resolver jobs, but processing remains idempotent: no duplicate notes, counter increments, or child-index entries.
- A permanently unavailable parent remains stored but hidden from ordinary reads, rather than silently becoming a root. A later parent arrival can still trigger linking.
- Cyclic or conflicting pending parent links cannot be committed.

## 10. Relevant code

- [`src/domain/note.rs`](https://github.com/estyseesghosts/pinetree/blob/master/src/domain/note.rs) — canonical note fields and validation.
- [`src/application/ports.rs`](https://github.com/estyseesghosts/pinetree/blob/master/src/application/ports.rs) — `CreateNoteInput`, `NoteStore`, jobs.
- [`src/application/queries.rs`](https://github.com/estyseesghosts/pinetree/blob/master/src/application/queries.rs) — direct-reply query and visibility policy.
- [`src/persistence/redb/mod.rs`](https://github.com/estyseesghosts/pinetree/blob/master/src/persistence/redb/mod.rs) — note creation/deletion, indexes, migrations.
- [`src/federation/inbox.rs`](https://github.com/estyseesghosts/pinetree/blob/master/src/federation/inbox.rs) — unresolved inbound `inReplyTo` behavior.
- [`src/activitypub/objects.rs`](https://github.com/estyseesghosts/pinetree/blob/master/src/activitypub/objects.rs) — outbound `inReplyTo` rendering and inbound parsing.
- [`docs/endpoint-manifest.md`](https://github.com/estyseesghosts/pinetree/blob/master/docs/endpoint-manifest.md) and [`docs/domain-types.md`](https://github.com/estyseesghosts/pinetree/blob/master/docs/domain-types.md) — frozen contracts to supersede explicitly.

## 11. Out of scope

- Returning a server-generated recursive thread tree.
- Replacing `reply_id` with a materialized path or `thread_id`.
- Mastodon REST API support.
- Requiring the optional ActivityPub `thread` extension for ordinary reply trees.
