# 2026-10-21 — prod-ready hardening (docs/prod-ready.md Batches A–J)

Parent-orchestrated, subagent-per-batch with disjoint file ownership (shared
files parent-merged). All fix batches landed uncommitted; parent applied
merge items, docs, and final verification.

## What changed (by batch)

- **A (smoke gate):** `bash -o pipefail` cargo step + full log, `SMOKE_ALL_REQUIRED=1`
  strict mode, `SMOKE_ALL_PARTIAL` (exit 2) for dev skips, guarded steps so the
  table + sentinel always print, fresh-build binary resolution, run-metadata
  header. Harness `scripts/tests/test-smoke-runner.sh` 11/11 green (parent
  re-ran post-H: still green, `bash -n` clean on all scripts).
- **B (config/Docker):** `transcode.enabled=false` default (incl. section-default
  flip by H), `runtime` + `runtime-ffmpeg` Docker targets, `public_url`
  placeholder `https://REPLACE-ME.invalid`, `unsafe.allow_local_http_for_testing`
  JSON opt-in (loopback/localhost/RFC1918/`.test` only + startup warning),
  `--allow-local-http-for-testing` CLI flag, throwaway flags in smoke scripts.
- **C (paging/backfill/reconcile):** `MAX_MEDIA_PAGE_ROWS=500`, newest-first
  exclusive cursors, empty-page termination; at-open migration; paged
  reconcile + completeness-gated sweep; boot `ReconcileStorage` enqueue +
  `storage-backfill` CLI. New `tests/storage_paging_c1.rs` (19 tests, green
  at agent time). Parent applied the `queries.rs` empty-page fix.
- **D (rate limits):** bounded buckets/failures/slot-IP maps (32768/32768/4096),
  expiry pruning, race-safe upload slots. `tests/rate_limit_bounded_d4.rs` 8/8
  green (parent re-ran).
- **E (RFC 9421):** full verifier (`rsa-v1_5-sha256`, covered components,
  `content-digest` integrity, created/expires ±300s), `apply_sig_mode`,
  inbox + fetch-gate enforcement, `set_rfc9421_mode` wired in `main.rs`.
  First pass left two files with unclosed delimiters; parent-verified via
  rustfmt oracle, repair agent added the two missing `}`. New
  `tests/federation_sig_mode_e5.rs` (21 tests, rustfmt clean).
- **F (backup/restore):** `src/backup/` (versioned ZIP, 103-table typed
  registry, shared media gate, trigger/status API, offline restore),
  `pub mod backup` in lib.rs, worker drain arm. Parent fixed after landing:
  14 compile errors were already fixed by a follow-up agent (macro `/`
  separator, dup import, private const, missing `Write` import, `Debug`
  bound); parent then fixed an inverted `check_and_record` test assert, a
  global-gate test-serialization gap, bundle/restore full-table-set mismatch
  (empty tables now materialize as zero-row files), backup-start contention
  with in-flight media ops (bounded reader drain in `acquire_backup_gate`),
  and a tuple-validator bug (lengths are a header block, not interleaved).
- **G (ingress):** per-route 64 KiB inbox cap (`DefaultBodyLimit::max`, 413 at
  extraction), Misskey caps unchanged. New `tests/ingress_limits_g1.rs`.
- **H (diagnostics):** structured logging + redaction, `diagnostics` +
  `backup` config sections, `/healthz` vs `/ready`, admin-only
  status/backup routes, maintenance CLI ops, reconciler gate line. New
  `tests/admin_diagnostics_h1.rs`.
- **I (docs):** testing.md (gate vocabulary), Security/Federation (RFC 9421),
  Operations (health/backup/Docker), Administration (schema), Architecture
  (milestone + unqualified status), database.md + remote-media-cache-plan.md
  addenda.

## Verification status (honest)

- Green on this box (parent-verified 2026-10-21/22, WinLibs GCC 16.2.0 +
  `CC`/`CC_x86_64-pc-windows-gnu` — LLVM-mingw's gcc is NOT sufficient):
  smoke-runner harness 11/11; `rate_limit_bounded_d4` 8/8;
  `backup_snapshot_f1` 15/15 (3 consecutive full runs);
  `config_docker_b2` 11/11; `federation_sig_mode_e5` 21/21;
  `ingress_limits_g1` 15/15; `admin_diagnostics_h1` 10/10;
  `storage_paging_c1` 19/19 + all pre-existing storage suites green.
- Parent-side fixes after agent landing (all verified by the runs above):
  F-test `check_and_record` inverted assert; backup-test global-gate
  serialization; bundle/restore full-table-set mismatch (empty tables now
  zero-row files); backup-start reader contention (bounded drain in
  `acquire_backup_gate`); tuple validator read-lengths-first (redb emits a
  length header block); restore `objects/` path level; follow-vs-block seed
  assertions; `get_job`-by-id vs `pending_count`; B-test stale
  transcode-default; h1 `None::<T>` annotations; g1 raw-socket harness
  (split halves, framed reads, FIN only for truncation, no `Connection:
  close`, comment-aware structural asserts).
- Full `cargo test --offline` running (background); clippy + smoke-all.sh +
  Docker + live federation still to go. Release status: **unqualified**.
- Full-gate triage (2026-10-22, `--no-fail-fast`): lib 626/626, conformance
  119/119, crash 2/2, all gaps*/federation*/batch_q/emoji suites green.
  Two failures, both non-regressions: `client_smoke` (shells to
  `smoke-phase1.sh`, needs `bun` — absent on this box, fails identically on
  HEAD) and `streaming::main_channel_gets_follow_followed_and_notification`
  (HEAD pushes Misskey-parity `unreadNotification`, HEAD's test allowlist
  predates it — verified via `git show HEAD`; parent tolerated the event in
  the test, 16/16 green after).

## Open risks

- New suites E/G/H + full lib/integration tests need a provisioned host run.
- Admin-status federation/quarantine/oldest-pending fields are explicit nulls
  until the ports expose aggregates.
- Bundle encryption at rest is perms-only; free-space probing is operator-side.
