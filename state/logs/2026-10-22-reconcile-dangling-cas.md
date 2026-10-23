# 2026-10-22 dangling-index reconcile CAS (adversarial-2 slice 4)

Covers finding 4 and finding E. Reuses Slice 2's `remote_cache_remove_if`
CAS — no duplicate conditional-remove path added.
Not a production-readiness claim.

## Changed

- `src/application/ports/media_store.rs` (additive only)
  - New `remote_cache_dangling_page(offset, limit)` port method: index
    rows whose media row is gone, in `(source_url, variant)` key order,
    never LRU order. Default impl returns empty (test doubles).
  - `remote_cache_remove_if` untouched (Slice 2); the dangling path
    reuses it.
- `src/persistence/redb/media_store.rs` (additive only)
  - `remote_cache_dangling_page` impl: one read transaction, key-ordered
    `REMOTE_CACHE` walk, one `MEDIA_META` point lookup per row for the
    media-absence check, `offset`/`limit` applied store-side.
- `src/media/reconcile/support.rs` (`#[cfg(test)]` fake only)
  - `FakeMedia::remote_cache_dangling_page` (key order, media-absent
    filter) and `FakeMedia::remote_cache_remove_if` (identity CAS),
    mirroring the production backend so the scan unit tests exercise the
    new selection + removal path.
- `src/media/reconcile/scan.rs` (dangling block only)
  - Candidates no longer come from `remote_cache_evict_candidates`
    (oldest N — newer dangling rows starved behind tombstones/live old
    rows). Snapshot-then-sweep: `remote_cache_dangling_page` is paged to
    at most `cap` rows before any delete.
  - Under the lifecycle gate + per-key lock, the exact
    `(source_url, variant)` row is re-read and must still match the
    candidate's `media_id` + `blob_key`; a concurrent refetch that
    installed `K_new` mismatches and is skipped, leaving the new index,
    metadata, and bytes intact. A live media row under the same id also
    skips.
  - Bytes are deleted only for a still-charged row (`!bytes_absent &&
    size_bytes > 0`) with `refcount == 0`.
  - Removal goes through `remote_cache_remove_if` (CAS); a lost race
    (`false`) skips the debit.
  - Debit uses the size/class still charged on the row actually removed,
    never the stale candidate's `size_bytes`; already-absent/zero rows
    are removed with no accounting move. One debit per physical key per
    pass, as before.
- `tests/reconcile_dangling_cas.rs` (new, 6 tests, real redb backend)

## Verified (executed, native PowerShell + native cargo)

Toolchain: `C:\Users\julie\.cargo\bin\cargo.exe` with
`CARGO_TARGET_DIR=C:\Users\julie\.cargo\shared-target` (warm cache).
No `cargo clean`, no target-dir deletions, no commit/stash/reset.

- `cargo test --locked --offline --test reconcile_dangling_cas`
  exit 0, `6 passed; 0 failed` (0.24 s final run):
  - `dangling_selection_ignores_lru_order` — 10 old live rows + 1 newer
    dangling row: `remote_cache_dangling_page` returns exactly the
    dangling row while `remote_cache_evict_candidates(8)` never surfaces
    it (fail-today proof for finding E).
  - `conditional_remove_rejects_stale_candidate` — stale
    (`mid_old`, `K_old`) CAS returns `false` after a refetch installs
    (`mid_new`, `K_new`); fresh row/bytes/media intact; fresh-pair CAS
    returns `true` (fail-today proof for finding 4 at the store layer).
  - `reconcile_skips_row_replaced_by_concurrent_refetch` — a `RacyMedia`
    wrapper installs a full refetch (bytes + media + index + charge) at
    the reconciler's locked re-read point: new row/media/bytes intact,
    old bytes untouched (skipped before any delete), totals `9 + 7`
    (no debit of either key), `evictions == 0`,
    `orphans_removed == 0`. Would delete the new row under the old
    unconditional `remote_cache_remove`.
  - `reconcile_sweeps_plain_dangling_row_and_debits_once` — no race:
    index gone, bytes gone, `total 9 -> 0`, `evictions == 1`,
    `orphans_removed == 1`.
  - `reconcile_debits_shared_key_once` — two dangling aliases, one key:
    both rows removed, bytes gone, exactly one 9-byte debit
    (`evictions == 1`); second byte delete is an idempotent no-op
    counted per row (`orphans_removed == 2`, same per-row counting as
    before).
  - `reconcile_removes_dangling_tombstone_without_debit` — dangling
    `bytes_absent` zero-size row (fresh, so Slice 2 retention keeps it):
    index removed, totals and evictions unchanged,
    `orphans_removed == 0`.
- Binary-freshness check: test exe
  `reconcile_dangling_cas-6e6476040c64909b.exe` mtime 2026-10-22
  20:00:44 is newer than `scan.rs` / `support.rs` / test file
  (19:59:15) and both `media_store.rs` files (19:55:11/12).
- Regression suites, same invocation shape, all exit 0:
  `reconcile` lib filter 4/4 (includes pre-existing
  `reconcile_sweeps_dangling_index_rows`, now passing through the new
  fake dangling page + CAS), `storage_reconcile` 4/4,
  `cache_evict_starvation` 5/5 (Slice 2 intact),
  `storage_cache_evict` 3/3, `cache_evict_transition` 8/8,
  `media_lifecycle_p0` 8/8.
- `rustfmt --edition 2024 --check` on the four source files plus the new
  test: clean (three hunks reflowed to satisfy it; no logic change).

## Left alone (per slice scope)

- Slice 1 `fetch.rs` guard / `evict.rs` predicate, Slice 2 eviction
  pagination / charged paging / blob index / generation / retention /
  `remote_cache_remove_if` semantics, Slice 3 serving paths, Slice 6/7/8/9
  files: untouched. The `git diff` bulk in `redb/media_store.rs` and
  `ports/media_store.rs` is Slice 2's pre-existing uncommitted work;
  this slice's hunks are additive except the `scan.rs` dangling block
  (the slice target). `remote_cache_evict_candidates` kept as-is (now
  unused by the reconciler; still part of the port).
- Pre-existing `rustc` warnings elsewhere (unused imports in
  `federation/...`) untouched.

## Open questions

- Dangling-page cost is one full index scan per page (up to `cap / 500`
  scans per run). The reconciler job is infrequent, so this matches the
  existing scan economics; a media-id secondary index could bound it if
  the index grows large.
- Fresh dangling tombstones are removed by this pass (no debit) rather
  than left for Slice 2 retention — same coverage as the old code, only
  the mechanism changed. If retention should own all tombstones, that is
  a follow-up coverage call, not a CAS fix.
