# 2026-10-23 Wave 6 — dedup mark on conflict (adversarial-3)

Subagent work, parent commit. Other Wave 6 items (fan-out cursor,
actor upsert, authority matrix) land separately.

## Changed
- `src/persistence/redb/activity_dedup.rs` — `record_note_activity`:
  `DUPLICATE_NOTE_URI` / `TOMBSTONED_NOTE_URI` arm now
  `commit_write(write_txn)` instead of `write_txn.abort()`, so the new
  activity's dedup mark survives the conflict and the next delivery is
  `already_seen`. Safe: `create_note_in_txn` performs only reads before
  the URI check (first mutation ~line 477), so the mark is the txn's only
  mutation.
- `tests/wave6_dedup_mark.rs` (new) — 2 tests: duplicate-URI and
  tombstoned-URI replay floods assert `is_recorded(second_activity_id)`.
  Parent ran `rustfmt --edition 2024` (import order only).

## Verified (subagent, native cargo, shared target dir)
- `cargo --locked --offline test --test wave6_dedup_mark` → 2 passed.
- `--test conformance activity_dedup` (d1, d1-forget, d2, d3, d4,
  d4-atomic, f4) → 7 passed.

## mtime evidence
- `wave6_dedup_mark-aee46a906274fd74.exe` 10:21:34 PM >
  `activity_dedup.rs` 10:18:44 PM, test 10:13:12 PM (parent fmt touched
  import order after green; re-check clean).

## Old-code failure note
- Proven empirically: restored `abort()`, both new tests FAILED at the
  `is_recorded(...)` assertions; re-applied fix → green. Abort discarded
  the mark, so replays re-ran the full insert path.
