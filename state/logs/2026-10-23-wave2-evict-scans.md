# 2026-10-23 Wave 2 — eviction scans and tombstone retention

Takeover agent (the first Wave 2 agent was cancelled mid-refactor leaving
`media_store.rs` unparsable; the takeover repaired and completed the
scope). Parent applied no source changes to these files.

## Changed
- `src/persistence/redb/media_store.rs` — M1 cursor design
  (`REMOTE_CACHE_CHARGED_LRU` / `REMOTE_CACHE_TOMB_LRU`, inline
  maintenance in insert/touch/remove/remove_if, heal-on-divergence +
  stale-rebuild page walks, alias-anchored commit with generation bump
  and no fake freshness); L2 `cache_eviction_read_view` read-txn snapshot
  (refcount point lookup, alias-anchored index targets, `has_durable` +
  `mark_media_ids` scanned on the read txn) so read-only
  `AlreadyAbsent` / `SkippedDurableRef` outcomes return before any write
  txn opens; write phase re-validates every id by point lookup + refcount
  CAS, failing closed on races.
- `src/media/remote_cache/evict.rs` — M2 `retain_expired_tombstones`
  skip-counts unremovable heads (unexpired TTL / live media / lost CAS)
  and pages forward in the same pass, bounded by `MAX_RETENTION_PAGES = 8`
  (512 rows max). Removed rows don't advance the offset.
- `tests/wave2_evict_scan.rs` (new) — 5 tests: charged-window skips
  400-tombstone + 128-charged prefix and evicts tail (single debit,
  generation 2, no fake freshness); shared-key evicts once +
  idempotent `AlreadyAbsent`; touch reorders charged cursor; retention
  pages past 64 heads and removes the expired tail with zero accounting
  movement; read-only outcomes + durable skip. Parent ran `rustfmt` on the
  test (import order + assert shape only).

## Verified (parent re-run on commit tree, native cargo, shared target)
- `--test wave2_evict_scan` → 5 passed (fresh exe newer than sources).
- Neighbors in the same run: `wave4_fanout_split` 2 passed,
  `wave4_scans_bound` 8 passed, `wave6_fanout_cursor` 2 passed.
- `rustfmt --edition 2024 --check` on all 3 files → clean.

## Old-code failure notes (takeover agent)
- M2: old `tombstone_page(0, 64)` + `continue` never visited rows ≥64;
  the 65th-row expired tail provably survived one pass.
- M1: old per-round full-table collect + JSON-decode + sort and double
  `MEDIA_META` full scans under the write lock — O(64·N) per ensure;
  new tests pin cursor-backed paging and the alias-anchored
  empty-`index_keys` path.
