# 2026-10-22 audit fix follow-up

Wave A (parent): added `RemoteCacheRecord.bytes_absent` (`#[serde(default)]`),
`CacheEvictionOutcome`, `MediaStore::commit_cache_eviction` +
`cache_entries_for_blob_key` (redb one-txn impl, bounded heap evict
candidates), `RegistryStore::consume_totp_login` (redb one-txn impl); stub
impls in `memory_mock_extra.rs`, `packers/tests_support.rs`,
`gaps_transcode.rs`, `reconcile/support.rs`; `mod.rs` re-export;
`bytes_absent:false` at all 9 construction sites. `cargo check
--locked --offline` EXIT 0 (old pinetree-target).

Wave B (4 parallel subagents, disjoint files, no cargo/git):
- B1 cache: `evict.rs` (per-class TTL order, `try_evict_snapshot`
  re-validation), `fetch.rs` (single-charge commit, same-id update),
  `scan.rs` (commit path, live-set skips evicted/empty), new
  `tests/cache_evict_transition.rs` (8 tests).
- B2 totp: `auth_2fa.rs` success path now single `consume_totp_login`,
  new `tests/totp_challenge_race.rs` (5 tests, redb backend).
- B3 backup: `backup/mod.rs` (`newest_backup_statuses`, fail-closed prune),
  `backup/bundle.rs` (strict `media_meta` + sidecar agreement),
  `admin_status.rs` + `invites_meta.rs` newest-tail, new
  `tests/backup_retention_and_diagnostics.rs` (7 tests).
- B4 hints: `packers/user.rs` propagates `totp_flags` error (`?`),
  `FEDERATION.md` baseline + DNS pool paragraph.

Wave C (parent, native Windows, `CARGO_TARGET_DIR=C:\Users\julie\.cargo\shared-target`):
- `cargo fmt --check` EXIT 0 (after one `cargo fmt` pass).
- New targets: `cargo test --locked --offline --test
  cache_evict_transition --test totp_challenge_race --test
  backup_retention_and_diagnostics` EXIT 0 — 7 + 8 + 5 = 20 passed.
  Parent fixes applied before green: `sweep_orphan_pools(&services, ...)`
  arg in `cache_evict_transition.rs:437`; `race-*` usernames to
  `racealice/racebob/racecarol/racedave/raceerin` (hyphens invalid).
- Lib filters: `--lib -- media::remote_cache` 7 pass; `--lib --
  media::reconcile` 4 pass; `--lib -- backup
  application::commands::auth_2fa` 1 pass (config round-trip; auth_2fa has
  0 lib tests).
- Full `cargo test --locked --offline --no-fail-fast` EXIT 101:
  55 suites ok, 1159 passed, 0 failed in-suite; 2 failing suites, both
  environment-only: `client_profile` + `client_smoke` (`SMOKE_FAIL: bun not
  found`, bash smoke scripts — bun not installed on this box). No code
  failures. During the run one real regression surfaced and was fixed:
  `storage_cache_evict::stale_full_evicts_first_and_metadata_survives`
  failed because the barrier compared shared-key sharers against a
  sibling snapshot; fixed to compare each row against its own listed
  version (`try_evict_snapshot(services, listed, snapshot)`); re-ran to
  3/3 green, then full suite shows `storage_cache_evict` ok.
- `scripts/smoke-all.sh` NOT run (bash-only gate; user did not ask). No
  `SMOKE_ALL_OK` claimed. No Docker / live federation claimed.
- Disk: `C:` hit 421MB free during first full run (`ld: final link failed:
  No space left on device`); removed ~15k stale `%TEMP%\pinetree-*`
  scratch dirs, now ~8.8GB free. New setup: both trees use
  `C:\Users\julie\.cargo\shared-target`, `debug = "line-tables-only"`,
  `incremental = false`; build one tree at a time (cargo locks the dir).

Done status: WP1–WP3 behaviors hold with new tests green (20/20) plus
`storage_cache_evict` 3/3 and lib suites green. WP4 landed (no snippet
fallback needed; `pack_me_detailed` already returns `Result`).
Follow-ups still open (not this plan): streaming `MeUpdated` `.ok()`
drop on TOTP store fault (B4 note, fail-closed, needs parent confirm);
production qualification (strict smoke, both Docker targets, pinned
Misskey+Sharkey, backup-after-eviction restore drill).
