# 2026-10-23 Wave 6 — fan-out idempotency (atomicity)

Subagent work; parent fixed the stale trait doc and formatted the test.
(The dedup-mark, actor-upsert, and authority-matrix items landed in
`62a14ed` and `44c19b8`.)

## Changed
- `src/persistence/redb/job_queue.rs` — `filter_new_jobs_in_txn`
  (same-txn scan: drops jobs whose key already has a live/non-quarantined
  row, plus within-batch repeats keeping the first) wired into both
  `enqueue_jobs` and `complete_with_followups`; quarantined rows do NOT
  suppress a same-key re-enqueue. Also fixed the latent double-count with
  the Wave 4 counters work (`complete_with_followups` no longer adds its
  own `+inserted` on top of the helper's credit).
- `src/jobs/worker/dispatch.rs` — comments only: the `Fanout` arm documents
  that each batch enqueue is idempotent, so crash-after-batch-K + lease
  reclaims converge on one row per key.
- `src/application/ports/job_queue.rs` (parent) — `enqueue_jobs` doc no
  longer says "No de-duplication on idempotency key"; documents the
  skip-live / keep-first / quarantine-retry rule.
- `tests/wave6_fanout_cursor.rs` (new) — 1200 follow-ups → batches
  [500,500] + tail 200; commit batch 1, pending 501, replay batch 1,
  pending still 501; batch 2 + atomic tail → 1200, drain yields each key
  once, pending 0; within-batch repeats collapse; quarantined rows don't
  suppress retry. Parent ran `rustfmt` (layout only).

## Verified (parent, native cargo, shared target)
- `--test wave6_fanout_cursor` → 2 passed (fresh exe newer than sources).
- `--test storage_paging_c1` → 19 passed (the `maintenance_triggers`
  row now pins pending 2 / drain 2 per the new contract).
- `--test conformance job_queue` → 18 passed (agent run).
- `rustfmt --edition 2024 --check` on all 4 files → clean.

## Old-code failure note
At HEAD `enqueue_jobs_in_txn` inserted unconditionally: replaying batch 1
gave pending 1001 (not 501) and final 1700 (not 1200).
