# 2026-10-15 — Phase 2.5 Slice 0: Baseline

- Ran `cargo test` on the pre-fix tree (commit `6423fc8` + staged Slice 1 edits not yet committed; source identical).
- Result: green, but counts exceed `docs/phase2-5.md:9` (expected 107: 78 lib + 26 conformance + 2 crash-recovery + 1 live `misskey-js`):
  - `src/lib.rs` unittests: 113 passed
  - `src/main.rs` unittests: 0 passed
  - `tests/client_smoke.rs`: 1 passed
  - `tests/conformance.rs`: 27 passed (not 26)
  - `tests/crash_recovery.rs`: 2 passed
  - `tests/federation.rs`: 2 passed
  - `tests/federation_interop.rs`: 1 passed
  - `tests/minrepro.rs`: 1 passed
  - Total: 147 passed, 0 failed.
- Delta vs doc is additive (more tests, no failures): lib 78→113 and conformance 26→27 reflect post-Phase-2 test growth on this tree; the extra `federation`/`interop`/`minrepro` binaries are not counted in the doc's 107. Treating the doc's 107 as stale, not a regression — proceeding, and later slices use this tree's counts as the regression gate.
- No code changes in this slice.
