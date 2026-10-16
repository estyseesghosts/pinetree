# 2026-10-16 — README rewrite + docs refresh

Docs-only session (no `src/`/`tests/` changes). The root `README.md` still
described Phase 2 (pre-streaming, "Phase 3 next", 251-test counts); the wiki
and several guides had the same rot.

## What changed

- `README.md` rewritten: Phase 3 + quotes + replies status, feature list
  (streaming, quotes, threaded replies, ~50 endpoints, FDB Phase 4 stub),
  updated layout/tests/scripts inventory, full gate commands with current
  markers (`SMOKE_ALL_OK`, `INTEROP_OK`, `ELIXIR_QUOTE_FEDERATION_OK`,
  `STRESS_OK items=28`), `PINETREE_REPLY_REPAIR_LIMIT` quickstart note.
- `docs/wiki/`: `Home` (date/status), `Current-State` (quote+replies phase rows,
  351-test breakdown, quote/reply API notes, elixir rig results, gaps trimmed of
  shipped items), `Roadmap` (Phase 3 + quote-posts + branched-replies ✅ done;
  quote federation removed from deferred), `Feature-Matrix` (streaming/global
  timeline/quotes/follow-requests/drive-listing/search/quote-federation cells
  flipped, ~50 endpoints, reading note reworded), `About` (today = Phase 3 +
  quotes + replies; streaming layout note fixed).
- `docs/testing.md`: 351 count, measured step times (phase1/phase3/sweep are
  seconds, not minutes).
- `AGENTS.md`: the Phase 3 "known gaps" sentence was stale — all four items
  (`users/notes`, `notes/renotes`, `users/followers`, streaming route) were
  closed by the Phase 3 exit review; now says so with pointer.
- Left untouched deliberately: past `state/logs/` entries and phase exit
  criteria (historical records — old counts there are correct *for their date*),
  `sns-server-proposal.md` (plan unchanged), frozen contracts (already current
  from the replies session).

## Verification

- `git diff --check`: clean.
- `cargo test --offline`: **351 passed, 0 failed** (matches the number quoted
  in the rewritten docs).
