# 2026-10-14 — Slices E1 & E2 (client smoke + Phase 1 exit checklist)

Goal: Implement Slices E1 (`scripts/smoke-phase1.sh`, `tests/client_smoke.rs`)
and E2 (`docs/phase-1-exit-criteria.md`, `docs/README.md`) per `docs/phase-1-plan.md`.

What changed:
- **E1 shell + misskey-js smoke (`scripts/smoke-phase1.sh`)**: Builds
  `target/debug/pinetree` when absent; checks `--mode=cluster` exits non-zero
  with `"not implemented (Phase 4)"`; runs `create-account
  --username=cli_admin --password=cli_pass`; starts the server on a temp
  `--data-dir` with an ephemeral `--listen=127.0.0.1:<port>` and matching
  `--public-url`. Then two `bun` programs import the real
  `./misskey/packages/misskey-js/src/api.ts` (`APIClient`, `isAPIError`,
  2026.10.1-alpha.0) and execute the full flow: `meta` (version
  `2026.10.1-alpha.0`, `features.miauth`), `username/available`,
  `POST /signup` (alice, bob), MiAuth web flow for both users
  (`GET /miauth/:session`, form `POST /miauth/:session` carrying
  `username`/`password`/`permission` so the minted token carries the granted
  permission set, one-shot `POST /api/miauth/:session/check` with a spent
  second check), `i`, `i/update` (+ `UNSUPPORTED_FIELD` on an unknown field),
  `drive/files/create` multipart PNG via `File` (+ byte-identical
  `GET /files/:id`), `notes/create` (`Hello @bob #pinetree`, 1 file, `tags`
  includes `pinetree`), `notes/show`, `following/create` (`UserLite`),
  home/local/hybrid timelines (+ `sinceId`/`untilId` exclusion), and
  `notes/reactions/create` (204 → `reactions['👍'] === 1`,
  `myReaction === '👍'`). Between the two halves the script `kill -9`s the
  server and restarts it on the same `--data-dir`; part 2 re-verifies
  accounts, tokens, notes, follows (`users/show` `isFollowing`),
  reactions, notifications (`mention` for bob, `follow` + `reaction` for
  alice), and media bytes. Prints `SMOKE_OK`; trap cleans up pid + temp dir.
- **E1 cargo test (`tests/client_smoke.rs`)**: single integration test
  running `bash scripts/smoke-phase1.sh` and asserting exit success.
- **E2 checklist (`docs/phase-1-exit-criteria.md`)**: every Phase 1 exit box
  and slice deliverable (`A1`–`A4`, `B1`–`B4`, `C1`–`C4`, `D1`–`D4`, `E1`–`E2`,
  plus the 4 pre-D gaps) checked with exact test files, counts
  (lib 78, conformance 26, crash_recovery 2, client_smoke 1), and log
  references; `docs/README.md` now lists `phase-1-plan.md` (Frozen) and
  `phase-1-exit-criteria.md` (Tracking checklist — all boxes checked).

Verification:
- Not executed this session: no shell access (all commands denied), so
  neither `bash scripts/smoke-phase1.sh` nor `cargo test` could run here.
  Static checks done by reading: router routes (`/healthz`, `/signup`,
  `/miauth/:session`, `/api/miauth/:session/check`, `/files/:id`,
  `/api/{endpoint}` dispatcher), `APIClient` multipart handling
  (`drive/files/create` is `multipart/form-data` in
  `misskey-js/src/autogen/endpoint.ts:1115`), exclusive cursor semantics
  (`sinceId`/`untilId` in `application/queries.rs:57-64`), and one-shot
  `consume_miauth_session` all match the script's assertions. `bun` resolves
  the `.js`-suffixed autogen imports to the sibling `.ts` files.
- Remaining step: run `bash scripts/smoke-phase1.sh` (or
  `cargo test --test client_smoke`) with `cargo`, `curl`, and `bun`
  available; the exit-criteria doc records this as pending executed evidence
  per AGENTS.md rule 4.

Open questions:
- None for Phase 1 scope. `src/*` untouched per assignment.
