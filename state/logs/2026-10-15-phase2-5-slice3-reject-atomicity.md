# 2026-10-15 — Phase 2.5 Slice 3: Reject-follow atomicity

- New `ActivityDedup::record_reject_follow_activity` (required, no default):
  single redb txn does `check_and_mark_dedup_in_txn` + `unfollow_in_txn` + commit.
  Seen id → `Ok(false)`; `NOT_FOLLOWING` swallowed in-txn → `Ok(true)` with the
  Reject marked seen (matches prior handler semantics).
- `handle_inbound_reject_follow` (`commands/inbound/follow.rs`) rewritten onto the
  atomic path (`vec![]` jobs); `federation/inbox.rs:335` caller untouched (contract
  unchanged). "Best-effort" doc admission replaced with atomicity description.
- All silent `record_*`/`forget_activity` trait defaults now fail loudly
  (`unimplemented!`). Audited: `NoopActivityDedup` never constructed; the only
  `forget_activity` callers (`inbox.rs`) run against the real store; webfinger/packer
  `Mem` doubles never touch follow/reject paths. `Noop` + both `Mem` doubles gained
  explicit `record_reject_follow_activity` (`Ok(false)`, old behavior preserved).
  `foundationdb/mod.rs` is empty — nothing to update.
- Conformance (`tests/conformance.rs`, generic so FDB inherits): `d4_reject_follow`
  (first `Ok(true)` removes edge, redelivery `Ok(false)`, missing-edge `Ok(true)`)
  and `d4_reject_follow_atomicity` (pre-commit abort → `StorageUnavailable`, edge
  intact, 0 jobs; retry `Ok(true)` removes edge; redelivery `Ok(false)`).
- Verified: full `cargo test` green — 114 lib + 29 conformance (+1 client-smoke,
  2 crash-recovery, 2 federation, 1 interop, 1 minrepro), 0 failed.
