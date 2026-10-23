# 2026-10-22 cache eviction starvation (adversarial-2 slice 2)

Covers finding 2, the tombstone half of finding 9, and the alias-window
half of finding 6's eviction path (class debit rule itself is slice 5's).
Not a production-readiness claim.

## Changed

- `src/application/ports/media_store.rs`
  - `RemoteCacheRecord` gains `generation: u64` (`#[serde(default)]`, old
    rows decode as 0). Bumped by the store on every row mutation (insert,
    touch, eviction transition); eviction compares it before timestamps, so
    a same-millisecond touch counts as newer (finding 5 guard half).
  - Three new trait methods with no-op defaults so existing doubles
    compile untouched: `remote_cache_charged_page(offset, limit)`,
    `remote_cache_tombstone_page(offset, limit)`, `remote_cache_remove_if`
    (conditional `(source_url, variant)` remove on `(media_id, blob_key)`
    match — slice 4's dangling path can reuse it).
- `src/media/remote_cache/evict.rs`
  - `ensure_cache_budget` no longer reads "8 oldest raw rows". It pages
    `remote_cache_charged_page` (32/round, `size_bytes > 0 &&
    !bytes_absent`, tombstones filtered store-side) with oldest-first
    order, advancing `offset` past a fully-tried unproductive window.
    Keys are marked tried only when the eviction actually runs for them
    (an early success in a round does not poison the untried remainder —
    caught during testing, see below). `MAX_EVICTION_ROUNDS` (64) still
    fails closed at the reservation.
  - Per key, `snapshot_key_candidate` fingerprints the COMPLETE alias set
    via the blob-key index at listing time. `try_evict_snapshot` then does
    an alias-set CAS under the lifecycle gate + key lock: same alias set,
    same `(media_id, blob_key)` identity, same generation and timestamps
    per alias — else skip. Aliases never need to appear in the window.
  - Debit rule unchanged (max row size, one debit per key); slice 5 owns it.
  - `retain_expired_tombstones`: one bounded page (64) per over-budget
    call; removes only zero-byte rows older than their OWN class TTL whose
    media row is already gone, via the conditional remove. Live-anchored
    tombstones are explicitly skipped; no bytes deleted here.
  - Unit tests rewritten to the fingerprint form, both pinning same-ms
    semantics (touch at the same ms skips; fresh baseline evicts once;
    alias added after listing skips, fresh full baseline evicts once).
- `src/persistence/redb/media_store.rs`
  - New `REMOTE_CACHE_BY_BLOB` table `(blob_key, source_url, variant) →
    u8`. `cache_entries_for_blob_key` is one prefix range + point lookups
    (no full-table scan); torn index entries heal by re-checking the row's
    `blob_key`.
  - `remote_cache_insert` store-assigns `generation` (new rows start at 1,
    rewrites advance past stored and incoming) and maintains the alias
    index inline (including dropping the entry when a row's `blob_key`
    changes). `remote_cache_touch` bumps generation with the clock.
    `commit_cache_eviction` bumps generation on transitioned rows.
    `remote_cache_remove` / new `remote_cache_remove_if` maintain the
    index in the same transaction.
  - New `remote_cache_charged_page` / `remote_cache_tombstone_page`
    (LRU, offset/limit). `remote_cache_evict_candidates` left exactly as
   -is for slice 4's dangling work.
- `src/persistence/redb/store.rs`, `store_backfill.rs`, `backup.rs`
  (index lifecycle): table registered in `init_tables`; one-shot
  `run_remote_cache_blob_index_backfill` at open behind
  `migration:remote_cache_by_blob:v1` (runs after the storage-accounting
  backfill, which also maintains/clears the index inline per batch);
  backup registry gains `remote_cache_by_blob` (STR3/U8) so the F1
  registry-completeness test still holds.
- Mechanical `generation: 0` inits only (no logic change) in struct
  literals in `src/media/remote_cache/fetch.rs` (2),
  `src/media/reconcile/scan.rs` test literals (2),
  `tests/cache_evict_transition.rs`, `tests/cache_reservation_accounting.rs`,
  `tests/media_lifecycle_p0.rs`, `tests/storage_cache_evict.rs`,
  `tests/storage_eviction_heal.rs`, `tests/storage_paging_c1.rs`,
  `tests/storage_reconcile.rs` (1 each). Required for compilation; the
  store overwrites the value on insert. Flagged for parent review since
  several are shared test files.
- `tests/cache_evict_starvation.rs` (new, 5 tests).

## Verified (executed, native PowerShell + native cargo)

Toolchain: `C:\Users\julie\.cargo\bin\cargo.exe` with
`CARGO_TARGET_DIR=C:/Users/julie/.cargo/shared-target` (warm cache) plus
the WinLibs UCRT `mingw64\bin` first on PATH for `gcc`/`dlltool`. No
`cargo clean`, no target deletions. No commit/stash/reset/checkout.

- `cargo test --locked --offline --test cache_evict_starvation` → exit 0,
  `5 passed; 0 failed` (0.83 s final run).
  Binary-freshness check: test exe mtime 2026-10-22 19:42:11 is newer than
  the newest source (`evict.rs` 19:41:03, test file 19:38:28).
- Fail-on-old-code proof (disposable detached-HEAD worktree in
  `%TEMP%\opencode\slice2-old`, main tree untouched; worktree removed
  after, `git status` verified identical): 4 adapted old-API probes assert
  the FIXED outcomes —
  - `probe_tombstones_starve_live_rows`: old total 110 vs 30 (old code even
    starves evacuating its own tombstones: 20 of 100 bytes stuck behind the
    8-window, then live rows untouched).
  - `probe_many_evictions_with_tombstone_head`: old total 120 vs 0.
  - `probe_stuck_window_never_pages`: old total 370 vs 330.
  - `probe_shared_blob_ten_aliases`: old total 40 vs 0.
  - `probe_same_ms_touch_is_invisible`: passes on old code (informational —
    documents the timestamp-only blind spot the generation closes).
  → `1 passed; 4 failed` on HEAD, exit 101, as predicted. An early draft
  of the many-evictions test (12 live rows, no tombstone head) was caught
  by this analysis as vacuous on old code (old loop evicts one/round and
  would finish in 12 rounds) and reshaped with an 8-tombstone head.
- Regression suites, same invocation shape, all exit 0:
  `cache_evict_transition` 8/8, `cache_reservation_accounting` 7/7,
  `storage_cache_evict` 3/3, `storage_eviction_heal` 4/4,
  `media_lifecycle_p0` 8/8, `storage_reconcile` 4/4,
  `storage_paging_c1` 19/19, `backup_snapshot_f1` 15/15 (covers the new
  table's backup registration), lib `remote_cache` 8/8 (incl. the 2 new
  evict unit tests), lib `reconcile` 4/4.
- `rustfmt --edition 2024 --check` on all 16 touched/new files: clean
  (after reformatting `evict.rs`, redb `media_store.rs`, new test).
- `cargo check --locked --offline --lib`: exit 0; only pre-existing
  warnings (repo-wide `ReadableTableMetadata` unused-import pattern,
  pre-slice `PreparedRemoteMedia::{mime, hash}` notes).

## Caught during testing

- Visited-at-listing bug: the first loop draft marked keys tried when
  snapshotted, so one success per round poisoned the untried remainder
  (12-eviction case stopped at 1). Fixed by marking tried only when
  `try_evict_snapshot` actually runs; round-local dedupe added for
  repeated keys within a page.
- Shared-target staleness: building the HEAD worktree into the shared
  target dir left a newer-than-source `libpinetree*.rlib` fingerprint, and
  the next main-tree build silently reused the OLD rlib
  (`no field generation`). Touched one source file to force the rebuild
  and confirmed exe-newer-than-source before trusting green. Same hazard
  class as the AGENTS.md stale-binary note.

## Left alone (per slice scope)

Slice 1 reservation accounting (`reset_in_flight`, guard, predicates)
untouched — `cache_reservation_accounting` still 7/7. Reconcile dangling
path untouched for slice 4 (it gains `remote_cache_remove_if` to reuse).
Class attribution rule untouched for slice 5. No passwordless, cargo
config, backup-status, or streaming changes. Full workspace
`cargo test --no-fail-fast` and workspace `cargo fmt --check`
intentionally not run; other slices have the tree mid-edit.

## Open

- `remote_cache_charged_page` / `tombstone_page` still scan the table per
  page store-side (bounded output, eviction/pressure paths only). A
  `last_access` secondary index would remove the O(table) CPU; the
  finding-9 scan that mattered for correctness (alias resolution) is
  indexed.
- `FakeMedia` and the other test doubles inherit the new methods'
  no-op defaults (empty pages): `ensure_cache_budget` against a fake sees
  no candidates. Production RedbStore implements them; no current test
  runs eviction against a fake.
