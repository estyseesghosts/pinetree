# 2026-10-15 — Phase 2.5 Slice 5: Job failure handling

- `JobQueue` port: `Job` gains `failed: bool` + `fail_reason: Option<String>`
  (both `#[serde(default)]`, old rows still decode); new `fail_permanent(job_id,
  reason)` with a no-op default so in-memory test doubles keep compiling;
  port-level `MAX_JOB_ATTEMPTS: u32 = 8` (lives in `ports.rs`; `retry.rs` was out
  of scope and the cap is port-contract surface).
- redb: `fail_permanent` quarantines (row retained, due-index entries removed,
  lease cleared, reason truncated to 512 chars, idempotent keeping first reason,
  no-op on missing); `fail` auto-quarantines with `retry-exhausted` at the cap
  and is no-op-success on missing/quarantined (was `NotFound`);
  `claim_next` skips `failed` rows; `pending_count` counts non-quarantined only
  (keeps `drain_until_empty` loops terminating).
- Worker (`Disposition` enum): Notify/StreamingPublish `Validation` errors and
  federate-parse failures → `fail_permanent(bad-payload: …)` + logged;
  `PermanentFailure(reason)` → `fail_permanent(permanent-failure: …)` + logged
  (was silent complete); exhaustion quarantines for ALL kinds (was federate-only
  silent complete). Storage-`Err` propagation unchanged.
- Legacy no-op branch (`outbox.rs`) removed: unparseable payloads are now
  `Validation("federate payload invalid: …")` the worker quarantines. Only one
  fixture relied on it (`conformance.rs` `{"op":"follow"}`) — replaced with a
  minimal valid `FederateDeliverPayload::Follow` (grep confirmed no others).
- Conformance: `b7_quarantine_never_reclaimed`, `b8_terminal_ops_on_missing`,
  `b9_retry_exhaustion_quarantines` (generic, FDB inherits) + redb/worker cells
  `b10_worker_quarantines_bad_notify_payload`,
  `b11_worker_quarantines_permanent_federate_failure`,
  `b12_worker_retry_exhaustion_quarantines`. b1–b5 untouched and green.
- `docs/application-ports.md` §6 (frozen) updated with the mandated superseding
  note (quarantine semantics, cap, no-op removal).
- Verified: full `cargo test` green — 108 lib + 35 conformance (+1 client-smoke,
  2 crash-recovery, 2 federation, 1 interop, 1 minrepro), 0 failed.
