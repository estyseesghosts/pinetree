# Operations

Status: living page, 2026-10-21. Sources: `Dockerfile`, `src/main.rs`, `src/observability.rs`, `src/shutdown.rs`, `scripts/smoke-all.sh`, `docs/testing.md`, `state/logs/2026-10-21-storage-overhaul-s3.md`.

## 1. Run it

Native (Windows host: prefer native `cargo`, never WSL — WSL is slow): `cargo run -- --config pinetree.json --data-dir ./data --listen 127.0.0.1:3000 --public-url https://example.com`. Docker: two targets from a shared base — `runtime` (no FFmpeg) and `runtime-ffmpeg` (Debian `ffmpeg` + build-time probe); bare `docker build` yields the minimal image. Full gate: `bash scripts/smoke-all.sh` (`SMOKE_ALL_OK` = all required phases ran+passed; `SMOKE_ALL_PARTIAL` = dev skip, not a release pass; `SMOKE_ALL_FAIL` = failure or strict-mode skip; `SMOKE_ALL_REQUIRED=1` for CI). Docker transcode matrix + local-HTTP opt-in are covered by `scripts/tests/test-docker-ffmpeg-b2.sh` (needs a Docker host).

Stale-binary rule: always compare binary mtime vs last source edit before trusting `SMOKE_OK` (phase-1 once passed on a pre-storage binary while fresh tree stack-overflowed). Live-gate resolves via `cargo metadata` ext4 target dir.

## 2. Health, logs, metrics

- `GET /healthz` → cheap liveness only. `GET /ready` → bounded readiness (real redb read + storage-root checks; non-2xx when a critical dep is down). Dockerfile `HEALTHCHECK` probes `/ready` (bounded; `/healthz` remains liveness-only for supervisors).
- Logging: structured levels + stable event names (`[pinetree] level= event= key={:?}`), request correlation IDs (`x-request-id`), job/delivery/media/backup ID correlation; `Authorization`/cookies/tokens/passwords/SMTP creds/private keys/bodies/PII redacted (values truncated at 500 chars); opt-in `[fed-debug]` workflow retained. `diagnostics.structured_logging` (default true) toggles it.
- Admin-only status: `admin/status` (basic fields always; `detailed` arm requires `diagnostics.detailed_admin_status=true`, still admin-authed, still bounded — expensive diagnostics are enqueued, never run inline). Backups: `admin/backup` (202+job id), `admin/backups`, `admin/backup-status`. Unknown/unavailable values are explicit nulls until the ports expose aggregates.
- Shutdown: SIGTERM drains `JobWorker` + `DestinationTracker`; in-flight leases reclaim on restart.

## 3. Storage + backups

Single `pinetree.redb` + `blobs_durable/` + `blobs_cache/`. Media paging contract: `MAX_MEDIA_PAGE_ROWS = 500`, newest-first exclusive cursors, empty-page termination (a short page is never end-of-scan). At-open one-time accounting migration is the source of truth (progress/done META keys, restartable, runs before serve/worker); the `storage-backfill` job path is best-effort convergent repair for quiet maintenance windows only (no uploads/deletes/evictions; re-run to converge). Boot-time bounded `ReconcileStorage` enqueue + operator `storage-backfill` CLI op; orphan sweep refuses incomplete key sets (never deletes live media on a partial walk); durable/cache pools strictly separate. See `docs/database.md` (backup registry + paging contract).

Online backup (admin-only): `POST admin/backup` → job id; `admin/backups`, `admin/backup-status`. One backup at a time behind the shared media gate (`MAX_CONCURRENT_BACKUPS == 1`, compile-time assert + exclusive gate `Busy`; media ops may wait — `gate_wait_ms` in status — while other requests serve). Table rows and the media reference inventory are collected inside one redb read transaction, and any non-evicted reference missing its object+sidecar in both pools fails creation (never published as `Completed`; staging is removed synchronously). Bundles are versioned ZIPs (`manifest.json` + `tables/*.tbl` + both pools + sidecars) in `<data_dir>/backups` (0o700/0o600); `.partial-*` staging outputs are never final and are cleaned at startup. One explicit policy governs size: configured `backup.max_bundle_bytes` (default 64 GiB) within the immutable 64 GiB hard ceiling, enforced at preflight, every streamed byte, final size, and restore input. Retention is configured (`backup.retention_max_count` / `backup.retention_max_bytes`, both unset = keep all): only older completed bundles are pruned, newest completed is never deleted, failed/running records are untouched. Free disk space stays an operator preflight (`df -h`); the in-memory status registry is bounded (`MAX_STATUS_REGISTRY_ENTRIES = 1024`, durable files remain the source of truth). Never run `sweep_orphan_blobs` concurrently with a backup (gate guard now in place; still avoid it operationally).

Restore is offline into a fresh dir: stop the server, `pinetree restore <bundle>.zip <fresh-data-dir>` (validates versions → checksums → schema → counts → integrity → accounting → sidecars → bytes before writing `RESTORE_READY.json`; corrupt/incompatible bundles fail closed), `df -h` the target (free space is operator-checked; size cap is the configured `backup.max_bundle_bytes` within the 64 GiB hard ceiling), cut over `data_dir`, keep the old dir until post-restore validation passes. No S3, no shared storage (Phase 4). Remote cache eviction is bytes-only LRU+TTL.

Federation DNS is bounded: at most `federation.dns_max_in_flight` (default 32, valid `1..=256`) concurrent resolver threads process-wide; a caller timeout abandons only the wait, never the slot, and saturation fails fast with a transient resource-busy error instead of spawning another thread. SSRF/IP validation is unchanged (literals never touch DNS; every resolved address re-checked, including across redirects).

## 4. Known ops gaps

Malformed WS/actor/inbox input maps to typed errors/close frames (no panic paths found in the current WS, inbox, or actor parsers; fuzz/property coverage remains a gap); per-process rate-limiter/upload-slots are now bounded under churn (Batch D, JSON-configurable `rate_limit.*` + `uploads.*`, plus registered `auth/2fa` + `i/2fa/*` buckets) but still per-process (needs shared limiter for multi-node); federation/quarantine/oldest-pending aggregates are explicit nulls in admin status until the ports expose them (websocket `connections`/`onlineAccounts` live, and backup `lastBackupFailureMs`/`lastBackupDurationMs`/`lastBackupBytesWritten` are now live; `STATUS_DETAIL_CAP=32` bounds detailed `recentErrors`). See `docs/TODOS.md` P3.

## 5. Known Limitations (single-node)

- Deliberate Misskey divergences: roles (storage-only, not an authorization boundary — see Administration), most admin screens, passkeys/WebAuthn, webhooks/OAuth, announcements are out of scope for this milestone (see Administration). TOTP enrollment + two-step sign-in are implemented and enforced.
- Live federation, Docker image behavior, and any `SMOKE_ALL_OK` on a box without Docker/toolchain stay unqualified until a provisioned run with pinned Misskey `2026.10.1-alpha.0` + Sharkey `2025.4.7` passes.
- Admin status honest nulls (need port aggregates): `durableUsedBytes`, `freeDiskBytes`, `accountingDrift`, `lastBackfill`, `lastReconcile`, `jobBreakdown`, `oldestPendingMs`, `quarantine`, `federationDelivery`, redb file bytes.
- `storage-backfill` job path requires a quiet window; at-open migration is the source of truth.

Read more: [Architecture](Architecture.md), [Administration](Administration.md), [Security](Security.md).
