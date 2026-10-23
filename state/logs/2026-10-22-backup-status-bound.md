# 2026-10-22 backup status bound (slice 8)

## Change

- `src/backup/mod.rs` (`newest_backup_statuses` only): no longer calls
  `list_backup_statuses` + full sort + truncate. The function now scans
  the status dir once and retains at most `cap` records in an ascending
  tail (binary-search insert, evict index 0 when a newer record arrives),
  then sorts the tail newest-first. Retained memory is `O(cap)` no matter
  how large the history grows; each file is still parsed once to read its
  `(requested_ms, job_id)` key. Tie order unchanged: descending
  `requested_ms`, ties by descending `job_id`.
- `list_backup_statuses` untouched (still oldest-first for the worker
  drain in `run_next_backup`; runtime CLI listing unchanged). Both
  diagnostics callers (`admin_status.rs`, `invites_meta.rs`) already use
  the bounded function — no caller change needed.
- No status files deleted; retention of ZIPs untouched.
- New test `tests/backup_status_bound.rs` (4 tests, uniquely named; the
  shared `backup_retention_and_diagnostics.rs` was not edited):
  - 50 files / cap 20 returns exactly the newest 20, newest-first, oldest
    excluded, file count unchanged afterwards.
  - `requested_ms` ties with cap smaller than the tie group keep the
    largest `job_id`s in descending order.
  - cap 0 / cap > total / missing-dir edges, plus `list_backup_statuses`
    still oldest-first.
  - structural check: `newest_backup_statuses` body does not reference
    `list_backup_statuses` (fails on the old load-everything code).

## Verification (native Windows, `--locked --offline`)

Toolchain note: this box has no `gcc`/`dlltool` on PATH (rustup gnu
toolchain only), so `cc-rs` build scripts fail on a cold target dir.
Reused the staged zig-based toolchain read-only (copied
`birchtree-agent-rsa-tools\mybin` to `Temp\opencode\pinetree-slice8-bin`,
added `gcc.exe`/`cc.exe` copies of `zig-cc.exe`, prepended to PATH) with
`CARGO_TARGET_DIR=C:\Users\julie\.cargo\shared-target` per the Wave C
setup. No repo config touched, no `cargo clean`.

- `cargo test --locked --offline --test backup_status_bound` EXIT 0 —
  4 passed, 0 failed. Test binary
  `shared-target\debug\deps\backup_status_bound-dfcec12dc43df3e7.exe`
  mtime 7:14 PM newer than `src/backup/mod.rs` (7:05 PM) and the test
  (7:06 PM): fresh build, not stale.
- `cargo test --locked --offline --test backup_retention_and_diagnostics`
  EXIT 0 — 7 passed, 0 failed (slice re-run requirement).
- `cargo fmt --check -- src/backup/mod.rs tests/backup_status_bound.rs`
  EXIT 0.

## Open / not claimed

- Per-file parse work still scales with history size (unavoidable without
  a recent-status index — filenames carry no timestamp); only retained
  memory is bounded. An index was not added: no writer owns it yet.
- Full `cargo test --no-fail-fast` and `smoke-all.sh` not run (parent
  gate). Bun/frontend suites not attempted here.
