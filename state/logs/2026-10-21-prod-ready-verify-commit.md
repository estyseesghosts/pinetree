# 2026-10-21 (late) — prod-ready verification round + commit

Follow-up to `state/logs/2026-10-21-prod-ready-hardening.md` (which landed
uncommitted). This session re-ran the batch verification round after the
parent's final lint/robustness edits, confirmed the concurrent-backup flake
fix, and committed the full Batches A–J tree. `docs/prod-ready.md` remains
untracked (input guide, not output).

## Re-verification on this box (native Windows, WinLibs GCC)

- `cargo check --offline`: clean, 10 pre-existing lib warnings only
  (unused imports `HeaderValue`, `SigningKey`, `TranscodeConfig`, `NewJob`;
  unused `scheme`, `pid`; dead `checked_ms`, `validate_antenna_import`,
  `PreparedRemoteMedia::{mime,hash}`).
- Batch suites (all green):
  - `config_docker_b2` 11/11
  - `federation_sig_mode_e5` 21/21
  - `storage_paging_c1` 19/19
  - `rate_limit_bounded_d4` 8/8
  - `ingress_limits_g1` 15/15
  - `admin_diagnostics_h1` 10/10
  - `backup_snapshot_f1` 15/15 × 4 consecutive runs (flake check, see below)
  - `streaming` 16/16 (incl. parent `unreadNotification` tolerance)
  - storage suites: `storage_two_pools` 3/3, `storage_cache_evict` 3/3,
    `storage_cache_budget` 3/3, `storage_durable_quota` 5/5,
    `storage_eviction_heal` 4/4, `storage_reconcile` 4/4
  - `scripts/tests/test-smoke-runner.sh` 11/11
- Concurrent-backup drift flake: previously ~1 in 6 runs (kept-zero durable
  rows counted as drift). After removing zero-durable-row writes + verify
  tolerance in `redb/mod.rs`, 4 consecutive full `backup_snapshot_f1` runs
  passed with no mismatch.
- `cargo fmt --check`: FAILS on pre-existing files untouched by this work
  (e.g. `src/activitypub/actor.rs` has zero diff vs HEAD yet fails
  `cargo fmt --check`; same class of drift across `tests/streaming.rs`,
  storage suites). Not reformatted: a repo-wide `cargo fmt` would be an
  unrelated broad rewrite out of scope. New batch files carry only minor
  rustfmt-version drift of the same kind.
- `cargo clippy --all-targets -- -D warnings`: cannot pass — ~629 warnings,
  ~600 pre-existing across 93 files (documented in prior session). This
  session's mechanical lints in batch test files were addressed; no broad
  refactor attempted.
- Full `cargo test --offline --no-fail-fast` (2026-10-21 late, this box):
  lib 626/626, conformance 119/119, crash 2/2, every batch/storage/gaps
  suite green. Only failures: `client_smoke` + `client_profile` (both
  `SMOKE_FAIL: bun not found` in the prerequisite check, before any changed
  code runs; bun absent on this box, docker absent; misskey checkout present
  via `misskey/` junction). Non-regressions: same prerequisite failure occurs
  independent of this work (script exits at bun check, lines 67-74 of
  `smoke-phase1.sh`, before server/client code).
- `scripts/smoke-all.sh` (developer mode, Git Bash on PATH): `SMOKE_ALL_FAIL`,
  exit 1, 62s. Per-phase table: cargo-test FAIL (exit 101, the two bun
  failures above), smoke-phase1 FAIL (bun absent, 1s), smoke-phase3 FAIL (bun
  absent, 0s), keystone-sweep PASS 60/60 (8s), smoke-phase2 SKIP (no docker).
  No `SMOKE_ALL_OK` printed — the gate correctly refuses a false green.
  (Prior expectation of `SMOKE_ALL_PARTIAL`/exit 2 assumed bun present; with
  bun absent the core phases FAIL rather than SKIP, which is the honest
  result for this box.)

## Commit

- Single commit for Batches A–J (code + tests, all previously uncommitted):
  32 modified + 14 new files (new `src/backup/` 3 files, 7 batch test
  files, 2 `scripts/tests/` harnesses, 1 Docker example config, 1 state
  log). `docs/prod-ready.md` explicitly excluded (stays untracked);
  `misskey/` junction untouched (git-ignored env setup).
- Pre-commit secret scan of backup/config/observability diff: no passwords,
  private keys, or tokens.

## Still needs a provisioned host (unchanged)

Docker builds (`runtime` + `runtime-ffmpeg` 4-scenario matrix), live
four-server federation gate, `SMOKE_ALL_REQUIRED=1` full pass, and the
Docker backup/restore drill. Release stays **unqualified**.
