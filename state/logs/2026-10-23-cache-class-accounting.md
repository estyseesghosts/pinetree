# 2026-10-23 cache class accounting (adversarial-2 slice 5)

Covers finding 6. Not a production-readiness claim.

## Rule chosen

`AudioVideo` has **no class counter anywhere**: it is governed by
`total_bytes` alone, in apply and evict alike. This is the "keep
AudioVideo out of every class counter" option from the plan, and it is
the rule three existing sites already used — the fix aligns the two
that did not:

- `store_backfill.rs` (`MediaClass::AudioVideo => {}` — total only),
- `FakeMedia` in `src/media/reconcile/support.rs` (AV moves no class
  counter),
- `class_bytes_in` in `src/media/remote_cache/evict.rs` (AV reads 0),

now matched by `RedbStore::cache_accounting_apply` and
`RedbStore::commit_cache_eviction`, which previously folded AudioVideo
into `full_bytes` (`AudioVideo => &mut stats.full_bytes` in both).
Budget checks (`ensure_cache_budget` + the post-sniff re-check in
`fetch.rs`) and admin stats needed no value changes: with the store
fixed, AV bytes pressure the global cap only and `fullBytes` counts
`Full` only. No `CacheStats` schema change, no migration.

## Changed

- `src/persistence/redb/media_store.rs`
  - New `CACHE_BLOB_CLASS` table (`blob_key` → `MediaClass::as_str()`):
    one authoritative accounting class per physical blob key.
  - `remote_cache_insert` records it in the same write transaction as
    the index row: first charge wins (no prior aliases); a restore
    charge (every prior alias `bytes_absent`/zero) overwrites with the
    restoring row's class; live sharers keep the first charge; a
    rewrite of a still-live same-key row records nothing (that path
    accompanies no charge); a pre-migration shared key with live
    aliases and no record is deliberately left unrecorded — attributing
    it to any one alias would be a guess. A row that abandons its
    previous key drops that key's record once its last alias is gone.
  - `commit_cache_eviction` debits the recorded class when present and
    falls back to the caller-passed class (max-alias) for unrecorded
    pre-migration keys; `AudioVideo` (recorded or passed) moves only
    `total_bytes`.
  - `cache_accounting_apply` ignores the class delta for `AudioVideo`
    (total still moves).
  - `remote_cache_remove` / `remote_cache_remove_if` drop the record
    once the last alias row for the key is gone (torn index entries
    whose row points elsewhere do not retain it); helper
    `blob_key_still_indexed`. A later charge re-records.
- `src/persistence/redb/store.rs`: `CACHE_BLOB_CLASS` created in
  `init_tables`.
- `src/persistence/redb/backup.rs`: registry gains `cache_blob_class`
  (STR/STR, same shape as `handle_index`), so export/import and the F1
  registry-completeness test cover it.
- `src/application/ports/media_store.rs`: doc-only — the AV rule on
  `CacheStats`, authoritative-resolution contract on
  `commit_cache_eviction`, AV total-only note on
  `cache_accounting_apply`. No signature changes, so all test doubles
  compile untouched.
- `src/media/remote_cache/evict.rs`: doc-only — `class_bytes_in`
  states the one rule; the max-alias comment now notes the passed
  class is the pre-migration fallback the store overrides.
- `src/api/misskey/endpoints/admin_status.rs`: comment-only — AV bytes
  live in `usedBytes`/total, never `fullBytes`.
- `tests/cache_class_accounting.rs` (new, 5 tests; Redb-backed):
  AV charge/evict ignore `full_bytes`; AV bytes do not consume the
  Full class budget but still pressure the global cap (end to end via
  `ensure_cache_budget`); Preview-first and Full-first shared blobs
  debit the first-charge class even when eviction is asked with the
  other alias's class; a restore charge replaces the recorded class.

## Historical `full_bytes` inflation: documented, not rebuilt

Stored snapshots that predate this slice may hold AudioVideo charges
inside `full_bytes`. They are left alone per the plan ("must not be
repaired by guessing"): a rebuild would have to attribute old shared
blobs without a recorded class, under the exclusive media barrier,
with per-key dedup — not small. Unrecorded keys are fixed at the next
(restore) charge, which records the restoring class. No rebuild added.

## Verified (executed, native PowerShell + native cargo)

Toolchain: `C:\Users\julie\.cargo\bin\cargo.exe` with
`CARGO_TARGET_DIR=C:/Users/julie/.cargo/shared-target` (warm cache)
plus the WinLibs UCRT `mingw64\bin` first on PATH. No commit/stash/
reset/checkout. No `cargo clean`, no target deletions.

- `cargo test --locked --offline --test cache_class_accounting` →
  exit 0, `5 passed; 0 failed` (0.17 s).
  Binary-freshness check: test exe mtime 2026-10-22 20:14:40 is newer
  than the newest source (`media_store.rs` 20:13:30, test file
  20:07:12, `evict.rs` 20:04:15).
- Fail-on-old-code proof (disposable detached-HEAD worktree at
  `7aaaf77` in `%TEMP%\opencode\slice5-old`, private target dir so the
  shared cache was never poisoned, main tree untouched; probe is the
  new test file minus the uncommitted `generation: 0` inits; worktree
  and private target removed after, `git status` verified identical):
  `0 passed; 5 failed`, exit 101, as predicted —
  - `audiovideo_charge_ignores_full_counter`: `full_bytes` left 100,
    right 0 ("AudioVideo must not count as Full"),
  - `audiovideo_bytes_do_not_consume_full_budget`: full-class pressure
    evicted the AV key (evictions 1 vs 0),
  - `shared_blob_debits_first_charge_class` /
    `shared_blob_first_charge_full_debited`: first-charge class left
    40, right 0 ("must debit the first-charge class"),
  - `restore_charge_replaces_accounting_class`: stale Preview debit
    left the re-charged class behind.
- Slice-5 regression gate (same invocation shape, all exit 0):
  `storage_cache_budget` 3/3, `storage_cache_evict` 3/3,
  `storage_eviction_heal` 4/4.
- Adjacent suites, all exit 0: `cache_reservation_accounting` 7/7,
  `cache_evict_starvation` 5/5, `cache_evict_transition` 8/8,
  `reconcile_dangling_cas` 6/6, `storage_reconcile` 4/4,
  `media_lifecycle_p0` 8/8, `backup_snapshot_f1` 15/15 (covers the new
  table's backup registration), lib `remote_cache` 8/8, lib
  `reconcile` 4/4.
- `cargo check --locked --offline --lib`: exit 0 (only pre-existing
  warnings). `rustfmt --edition 2024 --check` on all 7 touched/new
  files: clean.

## Left alone (per slice scope)

- `fetch.rs` / `scan.rs` accounting callers: no edits needed — AV
  deltas are neutralized store-side in apply/commit, and the missing-
  bytes path goes through `commit_cache_eviction`, which resolves the
  authoritative class itself. The dangling path still debits the
  removed row's own class (one row, one debit — unchanged semantics).
- `FakeMedia` and the other test doubles: already total-only for AV;
  they inherit the unchanged trait (no authoritative map — production
  RedbStore owns it; noted as a test-double divergence).
- `store_backfill.rs`: already total-only for AV; not re-seeded with
  class records (a rerun rebuilds index rows whose keys stay
  unrecorded until their next charge — the documented fallback).
- `docs/database.md` "103-table" registry count is now 104 with
  `cache_blob_class`; the doc is parent-owned, flagged here instead of
  edited.
- No full-workspace `cargo test --no-fail-fast` / `cargo fmt --check`;
  other slices have the tree mid-edit.

## Open

- A brand-new index row over already-pooled, never-charged
  crash-orphan bytes records a class with no matching credit (the
  store cannot see the pool); the orphan sweep heals the bytes and any
  drift is bounded by one saturating charge. Same-URL same-key live
  rewrites are exempt from recording for the symmetric reason.
- `remote_cache_charged_page` still scans store-side per page (slice 2
  known item); unrelated to this slice.
