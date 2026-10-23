# 2026-10-22 cache reservation accounting (adversarial-2 slice 1)

Covers findings 1, A, B, C. `reset_in_flight` untouched (crash heal only).
Not a production-readiness claim.

## Changed

- `src/media/remote_cache/fetch.rs`
  - Post-`reserve_cache_bytes` body runs inside one `move` closure that
    never releases; the outer `match` releases the FULL `max_bytes`
    reservation exactly once on every exit. All post-reserve `?` paths
    (`acquire_media_op`, `acquire_blob_key`, lookup, `byte_len`, touch,
    staging, commit) propagate to that release.
  - Success releases the full reservation and relies on the exactly-once
    `actual` add for the charge owner (`was_absent || previously_debited`);
    shared-blob hits (`charge == false`) add nothing and release everything.
    The old `release(max - actual)` true-up (which left `actual` in
    `in_flight_bytes` per success) is gone.
  - No `let _ =` on any `release_cache_reservation`; a failed release
    returns `PortError` (success becomes `Err` rather than hiding the leak).
  - After MIME sniff, the REAL class budget is re-checked before commit
    (`class_bytes + actual > cap + global_slack` fails closed with
    `CACHE_CAPACITY_EXCEEDED` plus a miss tick). A Full/AudioVideo object no
    longer commits on Preview room.
  - New `StagedFileGuard` removes the staged temp file on every exit; it
    guards disk bytes only, never the reservation.
- `src/media/remote_cache/evict.rs` (budget predicate only)
  - `ensure_cache_budget` global pressure is now
    `total_bytes + in_flight_bytes + need_bytes` (was `total + need`).
  - `class_cap_bytes` / `class_bytes_in` widened to `pub(crate)` for the
    second class check; no other evict logic touched.
- `tests/cache_reservation_accounting.rs` (new, 7 tests; shared test files
  untouched).

## Verified (executed, native PowerShell + native cargo)

Toolchain note: this shell's default PATH lacks cargo and the MinGW linker
the `x86_64-pc-windows-gnu` target needs. Builds used the full cargo path
with `CARGO_TARGET_DIR=C:/Users/julie/.cargo/shared-target` (warm cache;
slice 7 removed the tracked `target-dir`, so `./target` would rebuild from
scratch) plus the owner's WinLibs UCRT `mingw64\bin` (from HKCU PATH) first
on PATH for `gcc`/`dlltool`. No `cargo clean`, no target-dir deletions.

- `cargo test --locked --offline --test cache_reservation_accounting`
  → exit 0, `7 passed; 0 failed` (0.48 s on the final run).
  Binary-freshness check: test exe mtime 2026-10-22 19:24:59 is newer than
  `fetch.rs` (19:20:02), `evict.rs` (19:19:53), and the test file
  (19:07:51).
- Fail-on-old-code run (sources temporarily at HEAD, test file unchanged):
  exit 101, `2 passed; 5 failed`, matching the predicted defects exactly:
  - `three_successful_fetches`: `in_flight` 16 vs 0 after first fetch
    (success path released only `max - actual`).
  - `shared_blob_second_url`: `in_flight` 16 vs 0 (phantom reservation on
    the `charge == false` path).
  - `lookup_failure_...`: `in_flight` 4096 vs 0 (post-reserve `?` leaked
    the full reservation).
  - `failed_release_surfaces_as_error`: old code returned `Ok` (`let _ =`).
  - `sniffed_class_mismatch`: old code returned `Ok(MediaMeta … text/plain
    69 bytes …)` with no second class check.
  - `insert_failure_...` and `stage_import_failure_...` pass on old code
    too (old `stored`-closure `Err` branch already released); they are
    regression guards, not fail-today proofs.
- Regression suites exercising `ensure_cache_budget`, same invocation
  shape, exit 0: `cache_evict_transition` 8/8, `media_lifecycle_p0` 8/8,
  `storage_cache_evict` 3/3.
- `rustfmt --edition 2024 --check` on the two source files plus the new
  test: clean.

## Left alone (per slice scope)

TOTP-transaction, backup-snapshot/retention, and DNS-bound code untouched.
`reset_in_flight` unchanged. `media_store.rs` counter math unchanged. The
`SafeHttpConfig` / `PreparedRemoteMedia::{mime, hash}` warnings in
`fetch.rs` pre-date this slice. Other worktree modifications observed via
`git status` (slices 6/7/8/9 files) were not read or edited beyond the
shared-target-dir note above.

## Open

- `cargo fmt` (workspace) was run scoped to the three touched files only.
- Full `cargo test --locked --offline --no-fail-fast` and `cargo fmt
  --check` (workspace) intentionally not run here; other slices have the
  tree mid-edit.
