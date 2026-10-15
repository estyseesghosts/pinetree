# 2026-10-15 — Phase 2.5 Slice 9: Docs and exit gate

- `AGENTS.md:23`: `users/followers` gap corrected — only the Misskey API endpoint
  is missing (`FollowStore::followers_of` + AP `get_actor_followers` exist).
- `docs/README.md:20`: Sharkey pin corrected 2025.4.4 → 2025.4.7 (matches
  AGENTS.md, phase-2 exit record, and the live interop).
- `AGENTS.md` phase header: notes Phase 2.5 slices 0–8 landed, gate verifying.
- `docs/phase2-5.md` (the cleanup plan itself) tracked in this commit.
- Per-slice logs: slice0-baseline, slice1-reproducibility,
  slice1-fixup-snapshot-skip, slice2-split-commands, slice3-reject-atomicity,
  slice4-tls-ssrf, slice5-job-failures, slice6-rsa-crate, slice7-decouple-ap,
  slice8-visibility-decision, slice9 (this file). One commit per slice
  (plus two fixups: snapshot-skip under slice 1, commit re-splits are history
  hygiene, not content changes).
- Cleanup checklist (evidence):
  - [x] Slice 1 reproducibility: `.gitignore` anchored, nine source files tracked,
    `redb = "4.4.0"` crates.io (`git status --ignored` shows no source files).
  - [x] Slice 2 pure move: 28 methods byte-identical, 147 green at split time.
  - [x] Slice 3 atomic Reject: `d4_reject_follow` + `d4_reject_follow_atomicity`.
  - [x] Slice 4 no `-k`: `curl_args_never_disable_tls_and_keep_ssrf_pins`.
  - [x] Slice 5 quarantine: b7–b12 cells; frozen `application-ports.md` updated
    with superseding note.
  - [x] Slice 6 maintained RSA: custom bigint/DER/PEM deleted; fixture interop kept.
  - [x] Slice 7 decoupling: grep shows zero `api::misskey` outside `src/api`
    (one allowed doc comment).
  - [x] Slice 8 visibility: decision logged, no code change.
- Phase 3 entry gate: RUNNING at commit time (clean-clone `cargo test` +
  `scripts/smoke-phase2.sh`); results appended in the gate-evidence commit.
