# 2026-10-23 Wave 1 — cache accounting (adversarial-3)

Single agent (files overlap). Parent commit after verification.
`store_backfill.rs` also carries the Wave 4 scans/counters backfill hunks
(scans agent, finished); both verified together below. All other Wave 1
files are Wave 1-only.

## Changed
- `src/media/reconcile/scan.rs` (H1) — dangling path debits only when the
  last charged alias is gone (remaining `!bytes_absent && size>0` entry
  skips the debit; side-effect-free size-0 `commit_cache_eviction` probe;
  byte delete still gated on `blob_refcount==0`).
- `src/persistence/redb/store_backfill.rs` (H2 + Wave 4 counters) — rebuild
  per physical key (skip evicted/`size==0`/key-less/alias-less; charge once;
  writes `CACHE_BLOB_CLASS` first-charge rule; persisted class doubles as
  charged marker). Also carries scans-agent renote + counter backfills.
- `src/media/remote_cache/fetch.rs` (M3/M4/L1) — `(new, old)` stripe locks
  via `acquire_blob_keys_ordered` (smallest-stripe-first, ≤5 stabilization
  retries, fail-closed); orphan fix (`charge = was_absent ||
  previously_debited || entries.is_empty()`); class slack
  `max - total - in_flight` (fail closed at 0).
- `src/media/remote_cache/evict.rs` (L1) — slack includes `in_flight`.
- `src/persistence/redb/media_store.rs` (L3) — `reserve_cache_bytes`
  `checked_add`; `release_cache_reservation` returns `Err` on underflow,
  counter untouched. No trait changes; `heal.rs` reset path untouched.
- `src/media/storage.rs` (M3 helper) — `acquire_blob_keys_ordered`.
- New tests: `wave1_h1_debit`, `wave1_h2_backfill`, `wave1_m3_abandon`,
  `wave1_m4_orphan`, `wave1_l1_slack`, `wave1_l3_release` (agent suite
  740 green: lib 656 + all listed integration targets, 0 failed).

## Verified (parent re-run on commit tree, native cargo, shared target)
- `cargo test --locked --offline --test wave1_h2_backfill
  --test wave4_scans_bound` → 1 + 8 passed (fresh build 1m16s; only
  warnings, no errors). Exe mtimes newer than sources at run time.
- `rustfmt --edition 2024 --check` on all Wave 1 files except
  `store_backfill.rs` → clean. (`store_backfill.rs` carries the second
  agent's hunks; full-file check deferred to the Wave 4 commit.)

## Old-code failure notes (agent-proven)
- H1: old debit-on-`remove_if` zeroed total with a live sibling.
- H2: old summed per row (2×16+8, counted evicted), never wrote class.
- M3: single-key lock restored → `byte_len(old_key)=None` FAIL; fixed → pass.
- M4: orphan `charge=false` → total 0 vs 16.
- L1: `max-total` slack admitted 200 B over class cap.
- L3: `saturating_sub` returned Ok on double-release; now Err.
