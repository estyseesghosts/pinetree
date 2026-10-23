# 2026-10-23 Wave 8 — storage error redaction + streaming drop counters

Subagent work, parent commit. API/auth remainder lands separately after the
tree is green (Wave 2 takeover in progress).

## Changed
- `src/api/misskey/errors/error.rs` — `StorageUnavailable(_)` maps to
  generic `internal()` with no `info` (was `.with_info(...)` leaking the
  DB string). Sentinel substrings never reach the HTTP body (tested).
- `src/streaming/server/connection/support.rs` — `log_stream_skip` /
  `log_stream_join_skip` (error-class-only warn + bounded ledger, same
  shape as `log_me_updated_skip`), `load_for_fanout`, `load_or_skip`,
  `join_fanout`, `count_send_drop`; reaction-emoji media load and
  note-updated capture gate log-then-skip (invisible stays silent); +4 unit
  tests. Baseline `me_updated_body` untouched.
- `src/streaming/server/connection/dispatch.rs` — all fan-out arms
  (timeline, userList, hashtag, roleTimeline, drive, admin, noteUpdated,
  antenna, userListMember) log `error_class` then skip on store/pack
  faults; join failures diagnosed; gone/deleted/invisible silent; no retry,
  no partial packs; fire-and-forget sends counted.
- `src/streaming/hub.rs:169` — drop counted (`streaming.hub_dropped`);
  at-most-once policy unchanged.
- `src/streaming/server/subscribe.rs` — 5 `let _ = send` sites counted
  (`streaming.send_dropped`); policy unchanged.
- `src/streaming/server/connection/write.rs` — lagged resubscribe counted
  (`streaming.lagged`); policy unchanged.
- `tests/wave8_stream_err.rs` (new) — 3 regression tests.

## Verified (subagent, native cargo, shared target dir)
- `--test wave8_stream_err` → 3 passed (exe 10:50:21 PM > sources).
- `--lib` → 662 passed (incl. 4 new fan-out unit tests).
- `--lib -- streaming:: api::misskey::errors::` → 19 passed.
- Parent: `rustfmt --edition 2024 --check` on all 7 files → clean.

## Old-code failure notes
- Redaction test fails on old `error.rs:188` (sentinel in `info` → body).
- No-receiver hub test fails on old `let _ = send` (counter never moves).
- Delivered-broadcast test passes old and new (no-regression pin).
- MeUpdated path untouched (verified-good list preserved).

## Open
- Committed while `media_store.rs` (Wave 2, cancelled agent) is red; these
  files are disjoint and were verified on the last green tree. Full-suite
  gate deferred until Wave 2 takeover lands.
