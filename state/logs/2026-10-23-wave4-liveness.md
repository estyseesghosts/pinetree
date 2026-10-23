# 2026-10-23 Wave 4 — liveness under load (parent commit)

Three subagents (backup, fan-out, scans/counters) plus a parent-applied
snippet. `src/main.rs` in this commit also carries Wave 5 config-wave CLI
hunks (env shims, extra-CA gate, reply-repair/debug wiring); they are
verified by the config tests and the full gate, and the Wave 5 log owns
their description.

## Changed
- Backup (`src/main.rs`, `src/backup/{bundle,mod,restore}.rs`): drain runs
  on `spawn_blocking` (no `runner_lock` across `.await`, `drain_join_failed`
  warn arm); `MAX_TABLE_ROW_BYTES` 64 MiB → 2 MiB with chunked streaming
  reads and temp-file drain for oversized values; corrupt `*.status.json`
  skips logged (`backup.status_skip`) in both list paths; O(cap) scan and
  `(requested_ms, job_id)` tie-break preserved; `list_backup_statuses`
  still never called from the bounded path.
- Fan-out (`federation/outbox/{payload,mod}.rs`,
  `dispatch/fanout.rs`, `jobs/worker/run.rs`): `FANOUT_INLINE_MAX` 100 →
  16, `FANOUT_INLINE_CONCURRENCY = 4`, `FANOUT_JOB_DEADLINE_MS = 30_000`;
  inline path runs ≤4-wide waves with a wall-clock deadline, converting
  unattempted inboxes to one-inbox follow-ups on expiry; worker-side
  wall-clock guard keeps `Notify`/`RemoteMediaFetch`/`PollExpiry` live.
- Sweep resume cursor (parent-applied from the scans agent's snippet, which
  could not touch `payload.rs`): `SuspendBroadcast.after: Option<String>`
  (`#[serde(default)]`); `execute.rs` threads it through;
  `deliver_suspend_broadcast` fans out one 500-row
  `collect_shared_inbox_page` and returns a same-payload continuation
  when pages remain; seed sites (`moderation.rs`, `accounts.rs`,
  `wave4_fanout_split.rs`) set `after: None`.
- Counters/indexes (scans agent, full detail in
  `state/logs/2026-10-23-wave4-scans-counters.md`): maintained `counters`
  table (notes/accounts/jobs) + `NOTES_BY_RENOTE` index with crash-resumable
  backfills; `TimelineCursor::MAX_LIST_LIMIT = 1000` clamped at the port;
  `collect_follower_id_page` / `collect_shared_inbox_page` (500-row) sweep
  paging with `INTERACTED_AUDIENCE_CAP = 2000`; `reactor_ids_for_note`
  bounded; NodeInfo from counters + 60 s payload cache;
  `NodeinfoProbeGate` (singleflight, 8 in-flight, 60/min budget, advisory
  docs); `remote_shared_inboxes` + `notes_by_renote` ports.
- New tests: `wave4_backup_liveness` (7), `wave4_fanout_split` (2),
  `wave4_scans_bound` (8). Shared `backup_status_bound` untouched and green.

## Verified (parent, native cargo, shared target)
- `--test wave4_backup_liveness --test backup_status_bound` → 11/11
  (agent run; plus 35/35 backup round-trip regressions).
- `--test wave4_fanout_split --test wave4_scans_bound
  --test wave6_fanout_cursor --test wave2_evict_scan` → 2 + 8 + 2 + 5,
  exit 0 (fresh run on this tree, exes newer than sources).
- Full gate `cargo test --locked --offline --no-fail-fast` → all targets
  green except `client_profile`/`client_smoke` (environmental: `bun`
  missing; same as the adversarial-2 gate) — see per-wave notes for the
  contract updates this gate required (now committed separately).
- `rustfmt --edition 2024 --check` on all touched files → clean.
