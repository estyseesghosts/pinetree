# 2026-10-21 storage overhaul S3 (finish pass) — DONE, gate green

## What S3 added (on top of S1 `5b623ae` + S2 `53106b8`)
- `JobKind::CacheEvict|ReconcileStorage|BackfillDurableAccounting` + payloads
  (`ports.rs`) + worker dispatch arms (`jobs/worker.rs`: evict via
  `ensure_cache_budget`, reconcile via `reconcile_media_with_cap`, backfill via
  `backfill_durable_accounting`). Payloads ride `NewJob::new_json` (<64 KiB cap
  enforced; test asserts oversize rejected).
- `backfill_durable_accounting` (`media/reconciler.rs`): paged scan (500/page),
  in-memory sums, `set_durable_bytes`/`set_blob_refcount` overwrite (new trait
  methods, redb overrides). Open-time marker backfill already existed; the job
  is the drift-repair path.
- `durable_dir`/`cache_dir` overrides (`config.rs`, default
  `<data_dir>/blobs_{durable,cache}`), `validate()` distinct-dirs + reserved
  class-sum guard, `--durable-dir/--cache-dir` CLI flags, round-trip test.
- `tests/storage_two_pools.rs` (3), `tests/storage_cache_evict.rs` (3),
  `tests/storage_reconcile.rs` (4): pool isolation, rename migration,
  LRU/stale-first eviction with metadata survival, worker dispatch of all
  three jobs, payload cap, orphan sweep, missing-bytes heal, legacy flag,
  backfill restore + idempotence. 10/10 green.
- Full storage set: `storage_durable_quota` (5) + `storage_cache_budget` (3) +
  `storage_eviction_heal` (4) still green (22 storage tests total).

## Stack-overflow incident (the big find)
- Symptom: every `POST /api/*` aborted with `thread 'tokio-rt-worker' has
  overflowed its stack`; `healthz` fine. Dedicated routes (`/api/signup`)
  survived — dispatcher-only.
- Red herring burned: phase-1/3's first green runs used a STALE pre-storage
  binary (`/root/pinetree-target` hadn't rebuilt), so they validated nothing.
- Root cause: the sync `api_dispatch_blocking` match needs just over 2 MiB
  of stack in dev builds; tokio's default 2 MiB worker/blocking threads
  overflowed. Proof: `RUST_MIN_STACK=4M` serves 200s, `2M` aborts; asm shows
  no single >1 MiB frame (deep chain, not one hog); lib tests pass (8 MiB
  test threads); `client_smoke` on native Windows fails on path mangling.
- Fix: explicit `.thread_stack_size(8 << 20)` on the runtime builder in
  `main.rs` (covers workers + blocking pool, which share the builder) +
  `dispatch_fits_bounded_stack` guardrail test (representative dispatches on
  a 4 MiB thread/runtime; fails if need ever exceeds half the prod setting).
- Lesson recorded in `AGENTS.md`: verify the binary under test is actually
  fresh (mtime check), and dev-profile stack need is real — don't dismiss
  overflow as "can't happen".

## Gate verdict (executed 2026-10-21, WSL, `./misskey` → `../kissme/kissme-fe`)
- `bash scripts/smoke-all.sh`: `SMOKE_ALL_OK` — cargo test PASS (96s),
  smoke-phase1 `SMOKE_OK`, smoke-phase3 `SMOKE3_OK`, keystone-sweep
  `60 passed, 0 failed`, phase2 SKIP (needs Node 24 on PATH — environmental,
  documented SKIP path).
- `cargo test --offline --lib`: 536/536 (incl. new guardrail + config tests).
- `cargo test --test client_smoke --test client_profile` (WSL): green.
  Native-Windows `cargo test --test client_smoke` still fails (WSL-bash gets
  a `C:\…` path) — pre-existing harness issue, tracked in TODOS P3 gate item.

## Open / follow-ups (not this batch)
- `PINETREE_BIN` default (`$ROOT/../pinetree-target`) doesn't match this
  machine's ext4 target dir (`/root/pinetree-target`); worked around with a
  sibling symlink (outside repo). Script fix tracked in TODOS P3.
- Transport still buffers bodies in `SafeHttpClient`; disk-stage + cap
  enforcement are real, end-to-end socket-to-disk is a follow-up.
- `Backup/snapshot/restore`, `drive/files/find|move-bulk|…`, FDB parity:
  still open in TODOS.
- Untracked `docs/database.md`/`docs/gaps.md` left alone (not contracts).
