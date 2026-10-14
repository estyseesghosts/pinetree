# 2026-10-14 — visibility, conformance, FDB plan

Goal: draft visibility policy + persistence conformance suite; define (not execute) the FDB verification.

What changed:
- Added `docs/visibility-policy.md` (**Draft**): four levels (public/home/followers/specified) + orthogonal `local_only`; creation-time narrowing tables (renote/reply/instance-downgrade/blocking); single read predicate `is_visible` with the followers remote-remote fallback; timeline/delivery eligibility matrix. Grounded in Misskey `NoteCreateService.ts:465-571`, `NoteEntityService.ts:277-328`, `stream/channel.ts:68-103` (all re-read this session). Resolves application-ports open decision #3 (`specified` fully supported with non-empty `visible_user_ids`; ports doc to drop that row at freeze).
- Added `docs/persistence-conformance.md` (**Draft**): 7 test groups (atomicity, queue semantics, handles, dedup, visibility agreement, bounds) + explicit exit-box-2 acceptance test. Honest §9: no harness exists; execution awaits first adapter (Phase 1 redb) or a freeze-review fake.
- Added `docs/fdb-verification.md` (**Plan, blocked**): prototype scope/steps, pins, operational checklist, unblock sequence. Verified unexecutable here: no `cargo`, no `libfdb_c`, no cluster (`rustc`-only system package insufficient). Background cargo re-check running; doc will be amended if it finds anything.
- Launched three background reference surveys (core API endpoints, streaming channels, domain entity fields) for the remaining domain-types + manifest contracts.
- Updated `docs/README.md` index for all three docs.

Evidence: reference line citations re-read directly (not probe-recycled); environment checks executed (`which cargo`, lib globs, `fdbcli`). No code written.

Open questions / next step: on survey completion, write domain types + endpoint/streaming manifest, then freeze review. Exit checklist unchecked throughout.
