# Phase 1 — exit criteria checklist

> **Status at HEAD (2026-10-20 freshness pass)** — boxes left as recorded (2026-10-14); **no boxes changed** and no tests re-run in this pass.
> - Paths cited in the boxes exist at HEAD: `tests/conformance.rs`, `tests/crash_recovery.rs`, `tests/client_smoke.rs`, `scripts/smoke-phase1.sh`, and the `misskey` symlink used by the json-schema parity test.
> - **Superseded counts:** the "78 lib + 26 redb conformance + 2 crash + 1 client = 107/107" figure is the 2026-10-14 run. `docs/README.md` now reports the conformance suite at **118/118** on redb. Do not quote 107 as current.
> - The `src/persistence/foundationdb/` adapter (Phase 4) is still empty at HEAD (`docs/TODOS.md` P0); this is expected, not a Phase 1 regression.

Source: `sns-server-proposal.md` §9, Phase 1. Plan: `docs/phase-1-plan.md`
(Frozen 2026-10-14; pinned against Misskey `2026.10.1-alpha.0`).
Target clients/versions: Misskey 2026.10 (`2026.10.1-alpha.0` checkout at
`./misskey/`, `packages/misskey-js` 2026.10.1-alpha.0 executed under `bun`).

**Exit (proposal §9 Phase 1):**
- [x] Core flows survive restart and crash-recovery tests — `tests/crash_recovery.rs`
  (2 passed: `c4_kill_mid_traffic_and_recover`, 0 lost acknowledged notes,
  queue drains, Follow/Mention/Reply/Reaction notifications delivered;
  `c4_restart_preserves_accounts_follows_reactions_and_timelines`) plus
  `scripts/smoke-phase1.sh` SIGKILL + restart durability check
  (accounts, tokens, notes, follows, reactions, notifications, media bytes
  all verified post-restart via `misskey-js`).
- [x] Chosen Misskey clients can register/authenticate, post, read timelines,
  follow, and react — `scripts/smoke-phase1.sh` + `tests/client_smoke.rs`
  drive the real `misskey-js` 2026.10.1-alpha.0 `APIClient` end to end:
  `meta`, `username/available`, `POST /signup`, MiAuth web flow
  (`GET/POST /miauth/:session`, one-shot `/api/miauth/:session/check`),
  `i`, `i/update`, `drive/files/create` (+ `GET /files/:id` bytes),
  `notes/create|show`, `following/create`, home/local/hybrid timelines
  (+ `sinceId`/`untilId`), `notes/reactions/create` (204).

## A — Skeleton

- [x] A1 Scaffold — `Cargo.toml`, module tree, `config.rs`, axum `main.rs`
  with `/healthz` → 200 (`state/logs/2026-10-14-phase-1-slices-a-b1-b4.md`).
- [x] A2 Kernel — `aidx` `IdGenerator`, `Clock` trait, 4-variant port error;
  regex/sort-order/`parse_time` tests (same log).
- [x] A3 Domain types — Account, Note, Follow, Reaction, MediaMeta validation
  bounds unit-tested (same log).
- [x] A4 Visibility — single predicate per `docs/visibility-policy.md`; E1–E4
  matrix as unit tests (same log).

## B — Ports + redb

- [x] B1 Ports — `application/ports.rs` traits exactly as in
  `docs/application-ports.md` (slices-a-b1-b4 log).
- [x] B2 Conformance suite — `tests/conformance.rs`, generic over a store
  factory, covering every case in `docs/persistence-conformance.md`
  (`state/logs/2026-10-14-phase-1-slices-b2-b3-c-d1-d2-d4.md`).
- [x] B3 redb adapter — all tables (accounts, handle index, used usernames,
  notes, notes-by-author, follows both directions, reactions, jobs + due
  index, dedup, media meta); **full suite green on redb: 26 passed, 0 failed**
  (`tests/conformance.rs`: `a1`–`a5`, `b1`–`b6`, `c1`–`c4`, `d1`–`d3`,
  `e1`–`e4`, `f1`–`f3`, `conformance_exit_box_2`); crash cases run as a
  subprocess killed with `kill -9` (same log).
- [x] B4 BlobStore (fs) — content-addressed paths, temp-file + rename;
  put/get/delete + partial-write invisibility tests (slices-a-b1-b4 log).

## C — Application + jobs

- [x] C1 Commands — create_account, create/delete note, follow/unfollow,
  react/unreact, update_profile, upload_media; each commit carries its jobs
  (slices-b2-b3-c-d1-d2-d4 log).
- [x] C2 Queries — notes_show, users_show, home/local/hybrid timelines
  (fan-out-on-read, `sinceId`/`untilId`) (same log).
- [x] C3 Worker — in-process tokio worker: claim/complete/fail with backoff;
  `Notify` writes notifications; `StreamingPublish` to a no-op sink;
  no `FederateDeliver` until Phase 2 (same log).
- [x] C4 Crash recovery — `tests/crash_recovery.rs`, **2 passed, 0 failed**:
  mid-traffic `SIGKILL` with 0 lost notes and a drained queue, and restart
  preserving accounts/follows/notes/reactions/media/timelines
  (`state/logs/2026-10-14-phase-1-slices-c4-d3.md`).

## D — Misskey API

- [x] D1 API core — `POST /api/{endpoint}` router; `i`/Bearer extraction;
  permission checks; `{id,code,message}` errors; unknown param →
  `UNSUPPORTED_FIELD`; unknown endpoint → 404; fixed-window rate limits
  (slices-b2-b3-c-d1-d2-d4 log).
- [x] D2 Tokens + MiAuth — hashed tokens with permission sets;
  server-rendered login + approve page; `miauth/gen-token`,
  `/miauth/:session/check`, legacy `auth/session/*` (same log).
- [x] D3 Endpoints — `meta`, `i`, `i/update`, `users/show`, `username/available`,
  `notes/create|show|delete`, `following/create|delete`, 3 timelines,
  reactions, signup, `drive/files/create`; CLI
  `pinetree create-account --username= --password=`; **16 endpoint tests**
  (`state/logs/2026-10-14-phase-1-slices-c4-d3.md`).
- [x] D4 Packers — Note, UserLite/Detailed/MeDetailed, DriveFile; **6 snapshot
  tests** parsing `misskey/packages/backend/src/models/json-schema/*.ts`
  from the 2026.10.1-alpha.0 checkout (slices-b2-b3-c-d1-d2-d4 log).

## E — Exit

- [x] E1 Client smoke — `scripts/smoke-phase1.sh` (shell + `bun` + real
  `misskey-js` 2026.10.1-alpha.0 flow per the slice spec, ending in
  `SMOKE_OK`) and `tests/client_smoke.rs` (cargo integration test running
  the script) (`state/logs/2026-10-14-phase-1-slices-e1-e2.md`).
- [x] E2 Checklist — this file, every box backed by evidence logged in
  `state/logs/` (same log).

## Pre-D gaps settled (plan §Gaps; `docs/endpoint-manifest.md` + `state/logs/2026-10-14-phase-1-gaps-settled.md`)

- [x] 1. Reaction endpoints (`notes/reactions/create|delete`) with
  `write:reactions`, 204s, and `reactionAcceptance` fallback coercion.
- [x] 2. Account creation via `POST /signup` (+ `/api/signup`,
  `/api/admin/accounts/create`), CLI `create-account`, `POST /signin`, and
  web MiAuth login/approval at `GET/POST /miauth/:session`.
- [x] 3. `drive/files/create` (multipart + JSON, `write:drive`) with bytes at
  `GET /files/:id` (`/files/:id/:name` → 301 canonical).
- [x] 4. Target pinned: Misskey 2026.10 (`2026.10.1-alpha.0` checkout +
  `packages/misskey-js` under `bun`).

## Totals

- `cargo test --lib`: **78 passed, 0 failed** (incl. 6 Misskey JSON-schema
  snapshot tests and 16 endpoint tests).
- `cargo test --test conformance`: **26 passed, 0 failed** (first real-backend
  green run on redb).
- `cargo test --test crash_recovery`: **2 passed, 0 failed** (`kill -9`
  subprocess cases).
- `cargo test --test client_smoke`: **1 passed** (`bash
  scripts/smoke-phase1.sh` → `SMOKE_OK`: live binary + `misskey-js`
  2026.10.1-alpha.0, incl. `--mode=cluster` refusal, `create-account` CLI,
  and SIGKILL restart durability; executed evidence in
  `state/logs/2026-10-14-phase-1-complete.md`).

Phase 1 is exited. Phase 2 proceeds on the federation track; Phase 4 is unblocked.
