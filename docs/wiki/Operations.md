# Operations

Status: living page, 2026-10-21. Sources: `Dockerfile`, `src/main.rs`, `src/observability.rs`, `src/shutdown.rs`, `scripts/smoke-all.sh`, `docs/testing.md`, `state/logs/2026-10-21-storage-overhaul-s3.md`.

## 1. Run it

Native (Windows host: prefer native `cargo`, never WSL — WSL is slow): `cargo run -- --config pinetree.json --data-dir ./data --listen 127.0.0.1:3000 --public-url https://example.com`. Docker: two targets from a shared base — `runtime` (no FFmpeg) and `runtime-ffmpeg` (Debian `ffmpeg` + build-time probe); bare `docker build` yields the minimal image. Full gate: `bash scripts/smoke-all.sh` (`SMOKE_ALL_OK` = all required phases ran+passed; `SMOKE_ALL_PARTIAL` = dev skip, not a release pass; `SMOKE_ALL_FAIL` = failure or strict-mode skip; `SMOKE_ALL_REQUIRED=1` for CI). Docker transcode matrix + local-HTTP opt-in are covered by `scripts/tests/test-docker-ffmpeg-b2.sh` (needs a Docker host).

Stale-binary rule: always compare binary mtime vs last source edit before trusting `SMOKE_OK` (phase-1 once passed on a pre-storage binary while fresh tree stack-overflowed). Live-gate resolves via `cargo metadata` ext4 target dir.

## 2. Health, logs, metrics

- `GET /healthz` → cheap liveness only. `GET /ready` → bounded readiness (real redb read + storage-root checks; non-2xx when a critical dep is down). Dockerfile `HEALTHCHECK` still probes `/healthz`; switch it to `/ready` once the readiness contract is accepted for traffic.
- Logging: structured levels + stable event names (`[pinetree] level= event= key={:?}`), request correlation IDs (`x-request-id`), job/delivery/media/backup ID correlation; `Authorization`/cookies/tokens/passwords/SMTP creds/private keys/bodies/PII redacted (values truncated at 500 chars); opt-in `[fed-debug]` workflow retained. `diagnostics.structured_logging` (default true) toggles it.
- Admin-only status: `admin/status` (basic fields always; `detailed` arm requires `diagnostics.detailed_admin_status=true`, still admin-authed, still bounded — expensive diagnostics are enqueued, never run inline). Backups: `admin/backup` (202+job id), `admin/backups`, `admin/backup-status`. Unknown/unavailable values are explicit nulls until the ports expose aggregates.
- Shutdown: SIGTERM drains `JobWorker` + `DestinationTracker`; in-flight leases reclaim on restart.

## 3. Storage + backups

Single `pinetree.redb` + `blobs_durable/` + `blobs_cache/`. Media paging contract: `MAX_MEDIA_PAGE_ROWS = 500`, newest-first exclusive cursors, empty-page termination (a short page is never end-of-scan). At-open one-time accounting migration (progress/done META keys, restartable); boot-time bounded `ReconcileStorage` enqueue + operator `storage-backfill` CLI op; orphan sweep refuses incomplete key sets (never deletes live media on a partial walk); durable/cache pools strictly separate. See `docs/database.md` (backup registry + paging contract).

Online backup (admin-only): `POST admin/backup` → job id; `admin/backups`, `admin/backup-status`. One backup at a time behind the shared media gate (media ops may wait — `gate_wait_ms` in status — while other requests serve). Bundles are versioned ZIPs (`manifest.json` + `tables/*.tbl` + both pools + sidecars) in `<data_dir>/backups` (0o700/0o600); `.partial-*` staging outputs are never final and are cleaned at startup. Never run `sweep_orphan_blobs` concurrently with a backup (gate guard now in place; still avoid it operationally).

Restore is offline into a fresh dir: stop the server, `pinetree restore <bundle>.zip <fresh-data-dir>` (validates versions → checksums → schema → counts → integrity → accounting → sidecars → bytes before writing `RESTORE_READY.json`; corrupt/incompatible bundles fail closed), `df -h` the target (free space is operator-checked; bundle cap 64 GiB), cut over `data_dir`, keep the old dir until post-restore validation passes. No S3, no shared storage (Phase 4). Remote cache eviction is bytes-only LRU+TTL.

## 4. Known ops gaps

`panic!` on malformed WS/actor input (should be close-frame/error); per-process rate-limiter/upload-slots are now bounded under churn (Batch D) but still per-process (needs shared limiter for multi-node); env-only knobs (see Administration); federation/quarantine/oldest-pending aggregates are explicit nulls in admin status until the ports expose them. See `docs/TODOS.md` P3.

Read more: [Architecture](Architecture.md), [Administration](Administration.md), [Security](Security.md).
