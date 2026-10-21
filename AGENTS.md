# AGENTS.md — pinetree (modular Misskey-compatible SNS server)

Source of truth: `sns-server-proposal.md`. If this file and the proposal disagree, the proposal wins; update this file.

## Layout

- `sns-server-proposal.md` — product/architecture proposal and phased plan.
- `docs/` — frozen and draft contracts (endpoint manifest, domain types, identity rules, visibility policy, ports, conformance tests). See `docs/README.md`.
- `state/logs/` — dated append-only work logs. See `state/README.md`.
- `foundationdb/`, `redb/` — upstream reference checkouts (own git repos, git-ignored). Do not edit; read only.
- `misskey`, `sharkey` — symlinks to reference checkouts outside this repo (git-ignored). Read only; used for API/streaming contract checks.

Contracts are frozen (see `docs/README.md`); Phase 1 may scaffold the Rust crate from them. The throwaway conformance harness lives outside the repo at `/tmp/opencode/phase0-harness` — do not move it into `src/`.

## Current phase: Phase 4 — FoundationDB multi-node (Phase 3 exited 2026-10-16; exit review `docs/phase-3-exit-criteria.md`)

Phase 0 exit: all boxes checked in `docs/phase-0-exit-criteria.md` — contracts frozen, note+job atomicity proven on an in-memory harness (15/15 green), FDB client verified live (7.3.77 cluster, crate 0.11.0/`fdb-7_3`, atomic pair + latencies + failure surface; see `docs/fdb-verification.md` §6).

Phase 1 exit: all boxes checked in `docs/phase-1-exit-criteria.md` — `pinetree` single-node server built and verified (`cargo test`: 78 lib + 26 redb conformance + 2 crash-recovery + 1 live `misskey-js` 2026.10.1-alpha.0 client smoke test = 107/107 green; see `state/logs/2026-10-14-phase-1-complete.md`).

Phase 2 exit: all boxes checked in `docs/phase-2-exit-criteria.md` — WebFinger/NodeInfo, actors + keys, HTTP signatures, shared/personal inboxes, core activities, atomic dedup, durable delivery with retry, remote media; live four-server interop (real Misskey `2026.10.1-alpha.0`, real Sharkey `2025.4.7`, 2× pinetree) green via `scripts/smoke-phase2.sh` (see `state/logs/2026-10-14-phase-2-complete.md`; how to run each server: `docs/phase-2-interop-howto.md`).

Phase 3 scope (proposal §9): client compatibility and streaming. Gaps carried into Phase 3 — `users/notes`, `notes/renotes`, the Misskey `users/followers` endpoint, the streaming route — were all closed by the Phase 3 exit review (`docs/phase-3-exit-criteria.md`).

## State maintenance (required every work session)

1. Before starting: read `sns-server-proposal.md` §9 for your phase, `docs/README.md` for contract status, and the latest entry in `state/logs/`.
2. While working: update the relevant `docs/` contract in place; mark status `Draft` or `Frozen` at the top.
3. When finishing: append one dated log to `state/logs/YYYY-MM-DD-<topic>.md` recording what changed, what was verified, and open questions. Never rewrite past logs; supersede by reference.
4. Keep `docs/phase-0-exit-criteria.md` (or the current phase's checklist) checked off accurately. Do not claim exit criteria met without executed evidence.

## Architecture rules (proposal §§2–5)

- Modular monolith first; new workspace crate only with a concrete reason (reuse, test isolation, compile time, dependency control).
- Dependency direction: `domain` ← `application` ← adapters. Domain must not import HTTP, Misskey/ActivityPub shapes, or any DB crate.
- Misskey API and ActivityPub adapters call the same application operations. No duplicate business logic per adapter.
- One storage transaction commits state change + durable outbox/job record atomically. Workers do fan-out/delivery; jobs are idempotent, retryable, crash-safe.
- FDB limits are hard constraints: ~5s transaction lifetime, ~10 MB transaction size. No media blobs or unbounded fan-out in transactions.
- Public IDs/URLs must never expose node hostnames. Actor URL form `https://{domain}/users/{account_id}`; renames/moves preserve account ID.
- Federation input is untrusted: enforce size limits, timeouts, redirect limits, SSRF protections, signature verification. MVP, not polish.
- All new code must follow the style preferences as specified in rust_style_guide.md. 

## Verification

- On a Windows host, always prefer native Windows tools (PowerShell, native `cargo`) and never WSL: WSL is slow.
- Verify by execution: run tests/prototypes and paste or reference real output in the state log. Do not mark work done on reasoning alone. The full gate is `bash scripts/smoke-all.sh` (see `docs/testing.md`); use it before claiming phase-level completion.
- A live process is not proof of progress. When a compile or test run seems slow, investigate instead of asserting it is fine: identify which target is building/running, compare artifact mtimes against the clock, check per-process CPU, and stream per-test output (never pipe a diagnostic run through `tail` — it hides everything until exit). A test binary burning no CPU for many minutes is hung until proven otherwise; find the stuck test (re-run that target alone, look for `>60s` lines and deadlocks such as double-`Mutex::lock` in one statement), fix or kill it, and report what it was. Added 2026-10-21 after a deadlocked test passed itself off as "still compiling" for hours.
- A green run is not proof the code under test is fresh. Live-gate scripts resolve the server binary via `cargo metadata` (ext4 target dir); a stale binary passes while the tree is broken — always compare the binary mtime against the last source edit before trusting `SMOKE_OK`. Added 2026-10-21 after phase-1 passed on a pre-storage binary while every `/api` call in the fresh build stack-overflowed.
- Dev-profile stack is a real budget: the sync `/api` dispatch match needs just over 2 MiB, overflowing tokio's default 2 MiB threads (`thread 'tokio-rt-worker' has overflowed its stack` on every call, `healthz` unaffected). Diagnose with a `RUST_MIN_STACK` gradient (2M aborts / 4M serves), fix with an explicit `thread_stack_size` on the runtime builder, and pin it with a bounded-stack test (`dispatch_fits_bounded_stack`). Added 2026-10-21 with the storage-overhaul fix.
- Both storage backends must pass the same persistence contract tests (`docs/` defines them; redb adapter comes in Phase 1, FDB in Phase 4 — Phase 0 needs only the interface + conformance test suite + prototype proof).
- Compatibility claims require tests against real Misskey/Sharkey checkouts or target client versions, not internal mocks.

## Configuration

- Every new setting must be addressable through the JSON config file (`--config path/to/pinetree.json`): add a serde field whose JSON key matches the field name, keep it optional with a sane default (empty object must parse), cover it in `validate()`, and add a round-trip test in `src/config.rs`. Unknown keys are hard errors (typo safety) — so document every key you add. CLI flags override file values; `validate()` runs once over the merged result. No settings settable only via env vars or CLI flags.

## Subagent delegation (hard rule)

- Subagents share one worktree: concurrent subagents read and write the same files. The delegating agent must partition work so they cannot collide.
- Assign disjoint file sets; shared files (`docs/*` contracts, schema/migrations, router/ports, shared test files) are owned by the parent for merging — subagents propose patch snippets, they do not edit shared files directly.
- Subagents use uniquely-named test files, leave changes uncommitted, and never `git commit`, `git stash`, `git reset`, or `git checkout --` anything.
- Check `git status --short` before starting; respect uncommitted work by others; stage/commit only your own files when explicitly told to.
