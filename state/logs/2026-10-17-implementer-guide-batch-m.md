# 2026-10-17 — Batch M implementer guide

## Changed

- Added `docs/implementer-guide-batch-m.md` (Draft). It is the handoff for the next implementer: admin and moderation, registration policy and invite-only signup, suspension, abuse reports, instance blocks, the daily-use list (favorites, custom emoji listing, account self-service, follower removal, `url-preview`, drive extras, self-delete), the documentation updates, the smoke-gate fix, and the owner decisions with their defaults.
- Added the guide to the `docs/README.md` index.

## Verified (by reading code; nothing executed)

- Current-state claims in the guide's §1.2 table were checked against source with grep and reads. Citations are `file:line` at HEAD `384c8df`.
- Upstream Misskey behavior was read directly for: `admin/suspend-user.ts`, `users/report-abuse.ts`, `admin/resolve-abuse-user-report.ts`, `invite/create.ts`, `core/UserSuspendService.ts`, `core/AbuseReportService.ts`. Other upstream references in §1.3 are cited by file and not yet read in full. The implementer must read them before implementing.
- Keystone's call list was read from `/root/keystone/docs/api-surface.md`. Its Pinetree column is stale in two places (`drive/files`, streaming). Reported in the guide §10; the Keystone repo was not edited.

## Found while writing (live defects, recorded for the next implementer)

- `admin/accounts/create` is an alias of public `signup` with no admin check (`src/api/misskey/endpoints/mod.rs:2273`).
- `admin/reject-quotes` checks only a scope, not a moderator role, and writes no moderation log (`mod.rs:2834–2855`). The parity log already admits this.
- `scripts/smoke-all.sh:53` pipes `cargo test` through `tail` inside `bash -c`, so step 1 cannot fail. The Phase 2 SKIP path (`:77–79`) still ends in `SMOKE_ALL_OK`.
- `i/export-favorites` writes an empty file (parity plan §13), and Keystone's import/export uses it.

## Not verified / open

- No tests were run in this session. The gate and the test counts are unverified, as the guide's §7 requires.
- Upstream files not read in full: `admin/update-meta.ts` beyond lines 240–290, the `i/change-password`, `i/regenerate-token`, `i/delete-account`, `i/apps`, and `i/authorized-apps` family, `admin/show-moderation-logs.ts`, `admin/update-user-note.ts`, and `ap/get.ts`. The guide tells the implementer to read them before implementing.
- Owner decisions §9.1–9.9 are open. Defaults in the guide apply until they are changed.
- Pinetree has no outbound `Flag` renderer (none found by grep), so forwarding abuse reports needs new code.
- The guide's `url-preview` section depends on decision §9.3 (credential required). The default is set; it is not yet confirmed.

## Next

- Owner answers §9. Then the implementer starts at guide §12, step 1.
