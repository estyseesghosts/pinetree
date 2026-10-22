# 2026-10-22 prod-ready fixes complete (parent integration)

Source of truth: `sns-server-proposal.md` wins on disagreement. This log supersedes
`2026-10-21-prod-ready-hardening.md` / `2026-10-21-prod-ready-verify-commit.md`
for the residual items below; prior logs remain history.

## Delegation

Per `AGENTS.md` hard rule + user requirement, all implementation was delegated
to subagents with disjoint file ownership (shared hotspots
`src/main.rs`, `src/api/misskey/endpoints/mod.rs`, `src/federation/inbox.rs`,
`docs/*` contracts, `src/application/ports/*` owned by parent for merge):

- Audit wave (read-only, 5 agents): Batches A, B, C+F, D+E+G, H+I+J.
- Fix wave (6 agents): smoke gate, Docker health, config+rate-limits,
  storage C+F, RFC9421 overflow, ops admin H.
- Parent: contract merges (`main.rs` bounds wiring, `ports/media_store.rs`
  paging doc), docs reconciliation, release gates.

Windows native toolchain only (`cargo 1.99.1`, WinLibs `mingw64` + llvm-mingw
on PATH, `CARGO_TARGET_DIR=C:\root\pinetree-target`, `$env:CC=gcc`); no WSL.
Google Rust Style Guide: `cargo fmt --check` clean, no glob imports, `expect`
over `unwrap`, context on asserts.

## Fixes landed (worktree, uncommitted pending final gate)

- Batch A: per-step `SMOKE_ALL_LOGDIR` slug logs, cargo `bash -o pipefail -c`
  + `tee cargo-full.log | tail -30`, strict stale-binary fail vs dev rebuild,
  `PINETREE_BIN` explicit opt-in, new `.github/workflows/smoke.yml` (fast +
  strict with pinned Misskey `2026.10.1-alpha.0` + Sharkey `2025.4.7`,
  `upload-artifact smoke-logs`), `print_metadata` logdir + `docker images` +
  misskey/sharkey revs + interop work log. Shell gates 11/11 + 22/22 green.
- Batch B/H1: `Dockerfile` both targets probe `/ready` (bounded; `/healthz`
  liveness-only); B2 script ASSUMPTIONs resolved to real field shapes
  (`i`/`token`, `fileId`, multipart `i`+`file`); `docker/*.json` verified.
  Live Docker matrix NOT-RUN (no daemon on this box).
- Batch D: `Config{rate_limit,uploads}` JSON sections (defaults
  32768/32768/60000 + 2/16/4096, `validate()>0`, round-trip + typo tests),
  limiter/slot bounds plumb (`RateLimitBounds`, `UploadSlotBounds`,
  `with_config`/`from_bounds`), `observability` bare `ip`/`key` word-boundary
  redaction + remote-body-never-logged test, `main.rs` wiring. No env-only knobs.
- Batch C/F: `bundle.rs` short-page break removed (empty-page termination),
  `MAX_CONCURRENT_BACKUPS==1` compile assert + gate enforcement, `FsBlobStore::heal_sidecar`
  (stat + sidecar write), sweep unchecked restored as always-available
  test-convenience wrapper (integration tests need it; production uses
  `_checked`), docs-only quiet-window for job-path backfill (at-open is source
  of truth), `TranscodeConfig::default().enabled=false` aligned.
- Batch E: `rfc9421.rs`/`legacy.rs` checked arithmetic (`checked_sub/abs/add/mul`)
  fail-closed on `i64::MIN/MAX`, far-future `Date`; mode matrix unchanged.
- Batch H: `/ready` verified bounded, gate ordering verified (anon 401,
  non-admin 403 incl. moderator, flag-off 403), `webSocket:{connections,onlineAccounts}`
  filled from hub (O(1)), `STATUS_DETAIL_CAP=32`, 9 remaining nulls stay honest
  with port-getter snippets proposed. `admin_diagnostics_h-fix.rs` 4/4.
- Batch I: `testing.md` (per-step logs, stale gate, CI), `Operations.md`
  (ready probe, quiet-window backfill, backup gate, Known Limitations),
  `Administration.md` (ready probe), `Current-State.md` / `Feature-Matrix.md` /
  `Intentional-Differences.md` / `TODOS.md` reconciled to landed truth
  (RFC9421 single-node, ready/status, backup, drive/folders/hashtags/
  announcements/roles-partial/2FA+webhooks).
- Parent contract: `ports/media_store.rs` paging doc (newest-first, exclusive
  `until_id`, empty-page termination).

## Verification (native, `--offline`)

- `cargo fmt --check`: clean.
- `cargo test --offline --lib config::tests`: 29/29.
- `cargo test --offline --no-fail-fast`: lib 630/630; all integration targets
  green except 2 env-only failures: `client_profile` + `client_smoke`
  (`SMOKE_FAIL: bun not found`, pre-existing env gap, same as prior commit).
  Includes conformance 119/119, `storage_paging_c1` 19/19, `backup_snapshot_f1`
  15/15, `federation_sig_mode_e5` 21/21, new fix files
  (`storage_paging_c-fix` 5/5, `backup_snapshot_f-fix` 3/3,
  `federation_sig_mode_e-fix` 5/5, `rate_limit_config_d-fix` 5/5,
  `admin_diagnostics_h-fix` 4/4).
- Shell: `test-smoke-runner.sh` 11/11, `test-smoke-gate-a-fix.sh` 22/22.
- `cargo clippy --all-targets`: running; pre-existing ~75 warnings noted, no new
  gate claimed until clean run reviewed.
- Live gates NOT-RUN here: Docker matrix, provisioned `SMOKE_ALL_REQUIRED=1`
  full gate, live federation vs pinned references, backup/restore drill on
  throwaway volume. Release status stays **unqualified**, not green.

## Open risks

- Sweep wrapper is compiler-visible (integration-test requirement); guard is
  docs + review, not a cfg gate.
- Backfill job path is quiet-window docs-only (convergent repair); at-open is
  the barrier truth.
- 9 admin nulls need parent-owned ports (job stats, delivery aggregates, ws
  already live, redb bytes, free disk) — tracked in Operations Known Limitations.
- Clippy pre-existing drift (~629 historically) left alone per no-broad-rewrite rule.
- Workflow YAML eyeball-checked only; needs a real Actions run.
- `Expires=i64::MAX` now rejects fail-closed (correct; no real sender does this).

## Files changed (parent + agents, uncommitted)

`Dockerfile`, `scripts/smoke-*.sh`, `keystone-sweep.sh`,
`scripts/tests/test-docker-ffmpeg-b2.sh`, new
`scripts/tests/test-smoke-gate-a-fix.sh` + `.github/workflows/smoke.yml`,
`src/config.rs`, `src/config/tests.rs`, `src/main.rs`,
`src/api/misskey/response/*`, `src/observability.rs`,
`src/activitypub/signatures/rfc9421.rs|legacy.rs`,
`src/federation/fetch_gate.rs` (audit only),
`src/media/reconcile/*`, `src/media/storage.rs`, `src/media/transcode/config.rs`,
`src/persistence/redb/*` (media/store/backfill only), `src/backup/*`,
`src/runtime.rs`, `src/jobs/worker/storage_jobs.rs`,
`src/api/misskey/endpoints/admin_status.rs`,
`src/application/ports/media_store.rs`,
`docs/testing.md`, `docs/wiki/{Operations,Administration,Current-State,Feature-Matrix,Intentional-Differences}.md`,
`docs/TODOS.md`, new `tests/{admin_diagnostics_h,backup_snapshot_f,federation_sig_mode_e,rate_limit_config_d,storage_paging_c}-fix.rs`.
