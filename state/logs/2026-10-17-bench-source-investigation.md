# 2026-10-17 — Source investigation of fediverse-bench findings P1–P6

Read-only investigation; no product code changed. Output: `docs/handoff-fediverse-bench-fixes.md`
(per-finding notes + next-agent prompt). Supersedes nothing; additive to `2026-10-16-fediverse-bench.md`.

## Findings
- P1/P4/P5 share a root cause confirmed by reading source (not by execution): note dedup is by
  activity id only (`redb/mod.rs:4960`); no note-`uri` uniqueness; `find_by_uri` is a full table
  scan (`redb/mod.rs:2861`). `ap/show` (`endpoints/mod.rs:~4040-4124`) and the quote-target cache
  (`inbox.rs:1511/1536`) cache notes with uri-as-activity-id, with no reply link and no media job,
  so the later real `Create` inserts a second, full row.
- P2: store method `reactions_for_note` exists; the endpoint does not.
- P3: pinetree's outbound UndoLike path looks correct; stock Misskey↔Catodon also fails, so a
  control run is needed before fixing. Noted bare-string `Undo` object is treated as Announce only.
- P6: not root-caused; all vote-drop branches are silent. Instrumentation proposed.

## Not verified
Nothing was executed. All causes are from reading code; the first fix step is a failing test.
