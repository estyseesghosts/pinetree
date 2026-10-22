# 2026-10-22 — prod-ready-2 implementation (parent merge + verification)

Baseline: `ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e`. All work in
`C:\Users\julie\Desktop\lnx-tmp\2026-10-20-2313\pinetree`, native Windows
cargo (`CARGO_TARGET_DIR=<repo>\target`; tracked `.cargo/config.toml` still
points at WSL `/root/pinetree-target` — left untouched to avoid a full
rebuild; native runs must keep overriding the target dir). No WSL toolchain
used. Style: `cargo fmt` clean, `cargo clippy --all-targets` zero errors
(remaining warnings pre-existing tree-wide noise, e.g. unused imports in
untouched federation/streaming modules).

## Audit (5 parallel read-only subagents; full report in baseline log)

- P0 media races / orphan sweep: CONFIRMED.
- P0 TOTP bypass (+ unthrottled 2FA buckets): CONFIRMED.
- P1 backup snapshot skew / unwired cap / unbounded retention+registry: CONFIRMED.
- P1 unbounded DNS resolver threads: CONFIRMED (SSRF intact).
- P2 diagnostics nulls: CONFIRMED (honest). Panic-on-input: REFUTED as code
  defect (stale doc note; parsers return typed errors; fuzz gap remains).
  Worker infinite-reclaim: REFUTED (catch_unwind + quarantine + budgets).
  Readiness bounded: CONFIRMED implemented. Role-policy storage-only:
  CONFIRMED but documented.

## Implementation (5 parallel workstream subagents, disjoint files)

- Phase 1 media lifecycle: shared gate held across whole logical transitions,
  per-blob-key serialization, deferred-GC recheck (`blob_refcount == 0`
  under the same serialization), `_under_guard` split APIs, exclusive
  barrier across live-key collection + both pool sweeps, incomplete walks
  delete nothing.
- Phase 2 TOTP: password-only sign-in rejected for verified-TOTP accounts
  on all paths; 5-min one-time account-bound challenge consumed on success;
  step-counter replay guard; packer flags from storage; suspended/deleted
  gated at both factors. (Auth/session `meta.rs` correctly untouched: no
  password path there.)
- Phase 3 backup: rows + media inventory from one redb read txn; missing
  required blob fails creation (never `Completed`); `BackupPolicy` (config
  value clamped to 64 GiB hard ceiling) at preflight/stream/final/restore;
  sync→verify→rename→dir-sync publication with synchronous staging cleanup.
- Phase 4 DNS: `BoundedDnsResolver` (default 32 slots, permit travels with
  the worker so caller timeout never frees the slot); injectable resolver;
  `DestinationTracker` bounded (4096 + 1 h stale reclaim).
- Phases 5/6 diagnostics/roles: actionable `lastBackupFailureMs`,
  `lastBackupDurationMs`, `lastBackupBytesWritten`; honest-null docs naming
  the missing port getter; storage-only role contract docs; `expect` on
  header construction removed.

## Parent merge (shared files; subagents proposed snippets only)

- `rate_limiter.rs`: registered `auth/2fa`, `auth/2fa-authenticate`
  (10/account + 30/IP/min) and `i/2fa/*` ordinary buckets. This turned two
  auth tests red (new progressive delay on rapid fail→retry); fixed by
  advancing the manual clock past the backoff, matching the throttle-test
  pattern. Also migrated `gaps_api_auth.rs` (6 sites) and
  `gaps_final_wiring/two_factor_apps.rs` to the challenge-bound API
  (bare `userId + token` no longer mints sessions) with real-clock backoff
  pauses.
- `config.rs`: `federation.dns_max_in_flight` (default 32, `1..=256`,
  field-level serde default so explicit sections without the key keep 32);
  `backup.retention_max_count` / `retention_max_bytes` (`None` = keep,
  `Some(0)` rejected); `max_bundle_bytes` validate tightened to the hard
  ceiling; summary extended. Round-trip + guard tests extended.
- `backup/mod.rs` + `bundle.rs`: `BackupContext` carries
  `max_bundle_bytes` + retention bounds; `policy_for_context` resolves them
  (invalid → ceiling); in-memory registry bounded (1024, terminal-first
  eviction, live jobs never evicted); `prune_retained_backups` keeps newest
  completed always, prunes oldest-first past count/bytes, never touches
  failed/queued/running; hooked into both completion paths (best-effort).
- `main.rs`/`runtime.rs`: context wired from validated config; DNS pool
  published process-wide at startup (`configure_dns_pool`, no-op after
  first lookup); offline `restore` uses the configured policy.
- `delivery`: `ResourceBusy` kind (transient) for saturation/shutdown/
  spawn failure; canonical re-export in `delivery/mod.rs` (retry bridge
  kept for existing importers).
- `docs/wiki/Operations.md`: backup atomicity/cap/retention, restore cap,
  DNS bound + SSRF note, panic-note correction, 2FA/role divergence notes.
- `docs/prod-ready.md`: stamped SUPERSEDED by `prod-ready-2.md`.
- New parent tests `tests/prod_ready_merge.rs` (5): policy honors cap /
  fails closed on invalid, 1-byte cap fails without publishing, count
  retention keeps newest-two, single-bundle over budget survives + unset
  retention prunes nothing, DNS configure entry point.

## Verification (native, `--locked --offline`, `CARGO_TARGET_DIR=target`)

- `cargo check --lib`: clean. `cargo fmt --check`: clean.
  `cargo clippy --all-targets`: zero errors.
- `--lib`: 647/647 (incl. 17 new TOTP tests).
- Targeted suites: `dns_bounded_p1` 9/9, `backup_consistency_p1` 10/10,
  `prod_ready_merge` 5/5, `media_lifecycle_p0` 9/9,
  `diagnostics_roles_p2` 8/8, `backup_snapshot_f1` 15/15,
  `backup_snapshot_f-fix` 10/10, `admin_diagnostics_h1` 8/8 + h-fix 4/4,
  `rate_limit_bounded_d4` 5/5 + d-fix 3/3, storage_* 46 total,
  `conformance` 119/119, `crash_recovery` 2/2, sig-mode 26/26,
  gaps_* 24 suites green (incl. migrated `gaps_api_auth`,
  `gaps_final_wiring`), `federation_interop`/`federation_elixir`
  self-skip pass, `config_docker_b2` 11/11.
- Environmental (pre-existing, unrelated): `client_smoke` fails fast
  (`bun not found`), `client_profile` fails (`smoke-phase3.sh` needs the
  provisioned rig). No bun/node/Docker on this box.
- Phase 7 strict gate NOT run: no Docker, no pinned Misskey/Sharkey
  instances, no external ActivityPub peer. Release verdict on this SHA:
  **UNQUALIFIED** until provisioned-environment runs (strict smoke, both
  Docker targets, live pinned federation, live backup/restore drill).

## Remaining risks / follow-ups

- Full logical-race serialization depends on every media writer holding the
  shared gate (now true in-tree); future writers must use the
  `_under_guard` APIs and document lock order.
- No real disk-full (`ENOSPC`) fault injection; only tiny-cap refusal +
  staging cleanup. No age-based retention (count/bytes only, documented).
- In-memory registry bound is per-process; durable files are the source of
  truth. DestinationTracker bound is count-based; no byte-based resolver
  queue (queue depth is zero by design: fail fast).
- `FEDERATION.md` (new, untracked FEP-67ff record from a workstream) kept
  and committed; `docs/adversarial.md`, `docs/prod-repair.md`,
  `docs/prod-ready-2.md` were pre-existing untracked inputs and stay
  untracked.
