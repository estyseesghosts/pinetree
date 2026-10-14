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
