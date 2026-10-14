# 2026-10-14 — application ports draft

Goal: draft `docs/application-ports.md` per proposal §4 (Phase 0 deliverable; exit box 2 depends on its `create_note` + job atomicity).

What changed:
- Added `docs/application-ports.md` (**Draft**): port principles (use-case-shaped methods, no generic DB framework); shared kernel (opaque `aidx` IDs, injected clock, four-error taxonomy); `IdGenerator`, `AccountStore` (+directory/ownership), `NoteStore` (`create_note` atomic with jobs — the exit-box-2 operation), `JobQueue` envelope + claim/complete/fail with at-least-once lease semantics, `FollowStore`, `ActivityDedup.check_and_record`, `MediaStore`/`BlobStore` split, fan-out-on-read queries; four open decisions (job payload schemas, lease/retry numbers, specified-visibility, pagination defaults) assigned to their owning follow-up contracts.
- `CreateNote` field set grounded in Misskey `NoteCreateService.ts:170-191` (read-only reference): text/cw/visibility-default-public/localOnly-default-false/reply/renote/files adopted; poll/channel rejected as unsupported (proposal §7 defers polls; channels out of scope); `visible_users` deferred to visibility-policy contract.
- Updated `docs/README.md` index row for application ports → Draft.

Evidence: proposal §§2/4/5 re-read this session (atomic state+job commit, bounded TXs, no media bytes/unbounded fan-out in TX, FDB ~5s/10MB limits reflected as 64 KiB job cap); Misskey `Option` type lines 170-191 and defaults (:466-467, from prior grep) cited. No code written or executed — contract-only phase; nothing to run yet.

Open questions / next step: persistence conformance suite (tests the behaviors stated here) and visibility policy (blocks `specified` visibility + timeline rules). Exit checklist left unchecked.
