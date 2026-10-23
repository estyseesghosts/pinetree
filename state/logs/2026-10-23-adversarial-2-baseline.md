# 2026-10-23 adversarial-2 baseline (parent)

Verified-good prior repair (gate `2026-10-22-adversarial-2-repair-gate.md`:
lib 656 + all integration green except environmental `bun` skips).
This commit snapshots the remaining uncommitted slice work the
adversarial-3 waves built on; no new behavior is introduced here.

## Included
- `.cargo/config.toml`, `Cargo.toml` (`http-body-util` + `channel`,
  needed by the Wave 7 streaming proxy), `Cargo.lock`,
  `docs/testing.md` (shared-target `CARGO_TARGET_DIR` notes).
- `admin_status.rs` (AudioVideo class-rule comment), `packers/user.rs`
  (passwordless `false` pin), `ports/blob_store.rs` (`open_reader`
  default), `ports/media_store.rs` (`generation`, class rule,
  charged/tombstone/dangling pages, `remove_if`, per-key-class debit
  docs), `media/reconcile/support.rs` (fake dangling page + CAS).
- Slice regression tests + pins: `cache_evict_transition`,
  `media_lifecycle_p0`, `storage_cache_evict`, `storage_reconcile`,
  `two_factor_apps`; new `backup_status_bound`,
  `cache_class_accounting`, `cache_evict_starvation`,
  `cache_reservation_accounting`, `files_http_cache_hit`,
  `reconcile_dangling_cas`.
- Task records: `docs/adversarial-2.md`, `docs/adversarial-3.md`,
  `docs/audit-results/`, `state/logs/2026-10-22-*` (9 slice logs + gate
  + follow-ups), `2026-10-23-cache-class-accounting.md`.
