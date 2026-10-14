# 2026-10-14 — Wiki added; Phase 3 handoff prepared

- Added `docs/wiki/` (`Home`, `About`, `Current-State`, `Roadmap`, `Feature-Matrix`) and linked it from `docs/README.md`. Facts were taken from the code and executed results in `2026-10-14-phase-2-complete.md`; Misskey/Sharkey matrix cells reflect endpoint/module presence in the `./misskey` and `./sharkey` checkouts (not a behaviour audit).
- First git commit of the docs (`docs/`, `AGENTS.md`, `state/`). Source, tests and scripts remain uncommitted/untracked at the owner's discretion.
- Phase 3 handoff prompt delivered in chat (not stored in the repo). Key facts it relies on: `src/streaming/*` are empty stubs, `StreamingSink` is a no-op, no `/streaming` route, ~28 endpoints implemented, compatibility profile not yet declared.
- Open: the owner must choose the Phase 3 target client versions (profile) or confirm the default proposed in the handoff.
