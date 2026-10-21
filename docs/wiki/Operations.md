# Operations

Status: living page, 2026-10-21. Sources: `Dockerfile`, `src/main.rs`, `src/observability.rs`, `src/shutdown.rs`, `scripts/smoke-all.sh`, `docs/testing.md`, `state/logs/2026-10-21-storage-overhaul-s3.md`.

## 1. Run it

Native (Windows host: prefer native `cargo`, never WSL — WSL is slow): `cargo run -- --config pinetree.json --data-dir ./data --listen 127.0.0.1:3000 --public-url https://example.com`. Docker: multi-stage Debian + `tini`, user `pinetree`, `700 /data`. Full gate: `bash scripts/smoke-all.sh` (cargo + phase1 + phase3 + keystone; phase2 SKIP without docker/Node24/checkouts but still prints `SMOKE_ALL_OK` — do not trust blindly; `pipefail` missing, `tail -30` hides output, keystone count 38 header vs 41 doc vs 60 run).

Stale-binary rule: always compare binary mtime vs last source edit before trusting `SMOKE_OK` (phase-1 once passed on a pre-storage binary while fresh tree stack-overflowed). Live-gate resolves via `cargo metadata` ext4 target dir.

## 2. Health, logs, metrics

- `GET /healthz` → static `ok` (liveness only). `GET /ready` → real redb read (readiness). Dockerfile probes only `/healthz` — add `/ready` yourself.
- Logging: `observability::init()` is a no-op; everything is `eprintln!` + `PINETREE_DEBUG_FEDERATION` gate. No levels, no Prometheus/tracing counters. Federation drops log loudly (`outbox.rs:1844`).
- Shutdown: SIGTERM drains `JobWorker` + `DestinationTracker`; in-flight leases reclaim on restart.

## 3. Storage + backups (open)

Single `pinetree.redb` + `blobs_durable/` + `blobs_cache/`. No snapshot/restore tooling yet; crash-between-row-and-blob orphans need the reconciler sweep (`ReconcileStorage`, `BackfillDurableAccounting`). Back up all three together while stopped. No S3, no shared storage (Phase 4). Remote cache eviction is bytes-only LRU+TTL; no metrics for hits/evictions yet.

## 4. Known ops gaps

Missing `ffmpeg` in image (boot refusal with default transcode on); `panic!` on malformed WS/actor input (should be close-frame/error); per-process rate-limiter/upload-slots (needs shared limiter for multi-node); transport buffers bodies (socket-to-disk follow-up open); env-only knobs (see Administration); `healthz` static. See `docs/TODOS.md` P3.

Read more: [Architecture](Architecture.md), [Administration](Administration.md), [Security](Security.md).
