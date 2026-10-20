# Handoff — fixing the fediverse-bench findings (P1–P6)

> **Status (2026-10-20 freshness pass)** — per-finding status at HEAD (line numbers below are the 2026-10-20 locations; the "re-grep" warning still applies).
> - **P1 duplicate note rows — LANDED:** `NOTES_BY_URI` unique, `DUPLICATE_NOTE_URI`, migration backfill (`src/persistence/redb/mod.rs:580, 1129-1139`); `find_by_uri` O(1) (`:4321`).
> - **P2 `notes/reactions` — LANDED:** `src/api/misskey/endpoints/mod.rs:6278`.
> - **P3 Undo(Like) non-convergence — NOT VERIFIED:** no `docs/TODOS.md` entry; needs a control run (the handoff says so). Propose adding a TODOS line.
> - **P4 `replyId` lost — LANDED:** reply parent pending/resolve in the shared ingest (`src/federation/inbox.rs:2113-2170`); `ap/show` uses that ingest (`src/api/misskey/endpoints/mod.rs:8213`).
> - **P5 remote media on `ap/show` — LANDED for `ap/show`:** `RemoteMediaFetch` queued in-txn (`src/federation/inbox.rs:2081-2153`). **OPEN for the quote path** (bounded fetch, TODOS "Bench forensics").
> - **P6 poll-vote drops — PARTIAL:** per-reason labels (`src/application/commands/polls.rs:181-193`); counters and fixtures OPEN.

Status: Draft. Written 2026-10-17 after a source investigation (no code changed).
Inputs: `docs/fediverse-bench-2026-10-16.md`, `state/logs/2026-10-16-fediverse-bench.md`.
Line numbers are as of master @ f4e1f30; re-grep before trusting them.

## Headline: P1, P4 and P5 share one root cause (confirmed in source)

Notes are deduped **only by activity id** (`ActivityDedup::record_note_activity`,
`src/persistence/redb/mod.rs:4960`: checks `ACTIVITY_DEDUP`, then
`create_note_in_txn`). There is **no uniqueness on the note's AP `uri`**, and
`NoteStore::find_by_uri` (`redb/mod.rs:2861`) is a **full scan of the NOTES table**
that returns the first match.

Several code paths cache a remote note using the note URI as the "activity id",
while the real inbox `Create` arrives later with the `Create` activity's id.
Those are different dedup keys, so the second insert succeeds and creates a
second row for the same `uri`:

| Path | Where | What it skips |
|---|---|---|
| `ap/show` remote lookup | `src/api/misskey/endpoints/mod.rs:~4040-4124` | passes `reply_to: None`, `reply_parent_uri: None`, empty `file_ids`, **no `RemoteMediaFetch` job**, no mentions |
| quote-target caching | `src/federation/inbox.rs:1511`, `:1536` | `fetch_media=false`, uri-as-activity-id |
| `handle_create` | `inbox.rs:725-788` | correct path (reply link, pending-parent job, media job) |

The bench force-dereferenced every probe via `ap/show`, so the bare copy was created
first and the full inbox copy came later as a duplicate. `find_by_uri` then returns
whichever row the scan hits first (the older, bare one).
`ap/show` also returns the bare row, which would explain P4 (no `replyId`) and P5
(`files: []`) in the same runs.

Fix direction (one change fixes P1, and most of P4/P5):
1. **Storage-enforced uniqueness on note `uri`**: add a `NOTES_BY_URI` index
   (uri → note_id) written in `create_note_in_txn` and checked in the same write
   transaction. If the uri is already present, return the existing note
   (`Ok(None)`/existing) instead of inserting. This also turns `find_by_uri` into
   O(1). Put the requirement in the persistence contract (`docs/`) so the Phase 4 FDB
   backend must satisfy it too (FDB: conflict range on the uri key).
2. **Make `ap/show` call the same ingest as the inbox**
   (`ingest_remote_note_inner` with `fetch_media=true`) instead of hand-building
   `CreateNoteInput`. AGENTS.md rule: adapters must not duplicate business logic.
   Delete the duplicate construction at `endpoints/mod.rs:~4040-4124`.
3. **Upgrade-on-second-sight**: if the cached row for a `uri` lacks a parent link or
   attachments and a full `Create` arrives, fill them in (or have the first ingest
   always do the full work, which makes this unnecessary).
4. Migration: a one-off dedup of existing duplicate-uri rows (keep the richest row,
   repoint replies/reactions). Decide whether this is in scope or documented as a
   known state for already-run data.

Unmentioned side finding: `find_by_uri` full scans sit on the hot path of every
`handle_create`, `handle_like`, `handle_undo`, vote, and reply resolution, and the
redb write txn is single-writer. That makes everything slower and more
race-prone as the table grows. This is probably why bugs appear "under load". The
`NOTES_BY_URI` index fixes it. Check `accounts.find_by_uri` for the same pattern.

## Notes per finding

### P1 — duplicate note rows
- Cause: confirmed above (no uri uniqueness; uri-as-activity-id paths).
- Failing test to write first: ingest the same note via `handle_inbound_create_note(uri, ..)`
  (the `ap/show` shape) and then via a `Create` with a different activity id; assert exactly
  one row for that `uri`. Also a two-thread variant. Put it in the redb conformance suite so
  FDB must pass later.
- Verify: re-run the 900-post storm (`scripts/fediverse-bench/`) with probes via `ap/show`
  and re-sweep the home timelines for duplicate `uri`s. Expect 0.
- Also check `Announce` (`inbox.rs:~800`): it stores the renote with `uri = activity_id`,
  which is fine, but `resolve_note_target_id` caches targets and must go through the same
  unique path.

### P2 — `notes/reactions` (list reactors) missing
- Cheap and well-defined. `ReactionStore::reactions_for_note(note_id)` already exists
  (`ports.rs:1121`, `redb/mod.rs:4733`; used by `outbox.rs:349`). No endpoint arm in
  `endpoints/mod.rs` (reaction arms are at `:3321`/`:3343`).
- Implement `notes/reactions` against upstream Misskey's shape: params `noteId`, optional
  `type`, `limit` (default 10, max 100), `offset`, `sinceId`/`untilId`; returns
  `NoteReaction[]` `{id, createdAt, user, type}`. Check `misskey/packages/backend/src/server/api/endpoints/notes/reactions.ts`.
- Respect visibility of the note for the viewer and hidden-reactions rules.
- Note: `reactions_for_note` is unpaged; add a paged/limited store method if the
  note can have many reactions (FDB txn limits).
- Remove it from the "non-goals" lists in `docs/audit-2026-10-16-phase0-to-now.md` and
  `docs/wiki/Current-State.md` (do not rewrite past logs; add a new state log).
- Verify: real `misskey-js` call plus the bench harness.

### P3 — Undo(Like) non-convergence
- Pinetree's outbound path looks right: `UndoLike` job built in
  `application/commands/notes.rs:~656`, rendered and delivered in `outbox.rs:656-697`.
  Inbound `Undo(Like)` handling (`inbox.rs:~668`) resolves the note with the
  full-scan `find_note_by_uri` and calls `handle_inbound_unlike`.
- The bench shows stock Misskey↔Catodon failing too, so do **not** assume it is
  pinetree's bug. Run the isolated control first (Misskey↔Catodon only, no pinetree,
  low load). If that also fails, record it as upstream and stop.
- Pinetree-side things to check regardless: (a) `handle_undo` treats a **bare string**
  `object` as an Announce id only (`inbox.rs:~620`), so an `Undo` whose `object` is the
  `Like` id as a string is silently ignored (Akkoma/Mastodon forms vary); (b) the
  inbound `Undo(Like)` where the Like was recorded under a different `activity_id`;
  (c) whether duplicate rows (P1) make `get_reaction(user, note)` target the wrong
  note id. Fix P1 first, then re-measure P3.
- No fix until the control run and a captured raw `Undo` payload exist.

### P4 — `replyId` lost
- Cause (confirmed for the `ap/show`-first ordering): `ap/show` ingests with
  `reply_to: None, reply_parent_uri: None` (`endpoints/mod.rs:~4040`), so the row is
  a root note with no pending parent, and no `ReplyParentResolve` job is queued.
  The later inbox `Create` makes a second (full) row (P1).
- The pending-parent machinery exists and is sound (`inbox.rs:1218-1290`,
  `NOTES_BY_REPLY_PENDING`, `ReplyParentResolve` job in `jobs/worker.rs:178`), so the fix
  is to route `ap/show` through it (see headline).
- A second, independent path to check: a reply to a parent that arrives *after* the
  reply. The code handles it via `pending_reply_children` probe (`inbox.rs:~1286`).
  Add a test for the reverse arrival order.
- Verify: `pt-ks→pt-mk` pair in `scripts/fediverse-bench/features.json`
  (`R=asf8rkwreqkybsmf`).

### P5 — remote media linking slow / absent
- Cause (confirmed): `ap/show` and the quote-target cache path enqueue **no**
  `RemoteMediaFetch` job (`fetch_media=false` / hand-built input). Notes created that
  way never get attachments. On the normal inbox path the job is durable and
  off-request (`inbox.rs:~1268`, `jobs/worker.rs:147`, `handle_remote_media_fetch`
  `worker.rs:450`), so latency there is queue/worker throughput.
- Fix: the headline change. Then separately measure worker queue depth/latency under
  the storm; log job age. If attachments are still >90 s, look at worker concurrency
  and the single redb writer.
- Quote-target cache path (`inbox.rs:1511/1536`): decide whether quoted notes should
  get media fetched (probably yes, bounded by the existing depth guard).

### P6 — poll votes delayed/absent at pinetree origin
- Not root-caused; I found hypotheses only. Inbound vote path: `handle_create`
  (`inbox.rs:~750-780`) → `handle_inbound_vote` (`commands/polls.rs:173`).
- Every failure branch is a **silent drop** (`Ok(false)`/`accepted()`): poll note not found
  by uri, choice name mismatch, expired, blocked, already voted, `fetch_remote_note`
  failure for a string `object`. Nothing is logged, so "0 votes after 2 min" cannot be
  told apart from "never processed".
- Suspects, in order: (1) `find_note_by_uri` full scan plus single-writer contention
  under load (fixed by the index); (2) a vote arriving for a poll note that is a P1
  duplicate, so `get_poll(target.id)` is on the other row; (3) votes whose `Create`
  has `text`/`cw` set (Akkoma/Catodon variants) so the vote short-circuit
  (`parsed.text.is_none() && parsed.cw.is_none()`) is skipped and it is ingested as a
  note; (4) the idle-time `QuestionUpdated` job fan-out slowing things down.
- First step: add counters/log lines for each drop reason, then re-run the vote storm and
  see which branch fires. Also capture raw vote payloads from each software
  (Misskey, Catodon, Akkoma) into a fixture test.

## Suggested order
1. P1 failing test, then the `NOTES_BY_URI` index plus uniqueness (also fixes performance).
2. Route `ap/show` through `ingest_remote_note_inner` (fixes P4, P5 at the source).
3. Re-run the bench; re-measure P3, P5, P6 before touching them.
4. P2 endpoint.
5. P6 instrumentation, then fix what it shows.
6. P3 control run; fix only what is pinetree's.

## Ground rules (from AGENTS.md)
- Verify by execution; paste real output into a new dated `state/logs/` entry. Do not
  rewrite past logs.
- Both storage backends must pass the same contract tests; put the uniqueness rule in
  `docs/` and the conformance suite.
- Gate before claiming done: `bash scripts/smoke-all.sh`.
- Bench harness: `scripts/fediverse-bench/`. The rig may still be running; see
  `state/logs/2026-10-16-fediverse-bench.md`.

## Copy-paste prompt for the next agent

> Read `docs/handoff-fediverse-bench-fixes.md`, `docs/fediverse-bench-2026-10-16.md`
> and AGENTS.md. Work in the order given under "Suggested order". Start with a failing
> redb conformance test for P1 (same note `uri`, two different activity ids, plus a
> concurrent variant), then implement a storage-enforced `NOTES_BY_URI` index and
> route `ap/show` through the inbox ingest path. Re-verify with the bench harness
> and `bash scripts/smoke-all.sh`, update the persistence-contract docs, and append a
> dated state log with real output. Don't claim P3 or P6 are fixed without the control
> run and instrumentation described in the handoff. Ask before changing any frozen contract.
