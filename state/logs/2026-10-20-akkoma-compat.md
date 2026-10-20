# 2026-10-20 — Akkoma/Pleroma poll + emoji compat, notes/reactions hardening

Handoff: `HANDOFF_akkoma-compat.md`. Ran subagents A+B+C in parallel, then synthesized.

## Decisions (all feasible unless noted)

- Polls: FEASIBLE — one-line fix. Akkoma's `QuestionOptionsValidator`
  (`question_options_validator.ex:27-28`) requires `options[].type == "Note"`
  (`validate_inclusion` + `validate_required`); pinetree omitted `type`.
  Pleroma's validator is identical on paper but lenient via schema defaults;
  Akkoma's schema has no defaults, so Akkoma fails hard. Misskey/Catodon
  `renderNote.asPoll` already emit `type: "Note"`; Mastodon `PollParser`
  ignores option `type`. Fix makes pinetree byte-identical to Misskey's
  federated shape. Bench: the only `pt→akkoma` poll failure in run5
  (`features.out:220`, poll unresolvable on dst) is explained exactly.
- Reactions inbound: FEASIBLE, LANDED — `EmojiReact` dispatch arm reusing the
  `Like` path (`content`, else `Emoji`-tag `name` fallback);
  `Undo(EmojiReact)` unreacts. Additive; existing bare-`Like` and
  `Like`+`_misskey_reaction` paths untouched. Run5's 3 `akkoma→pt` FAILs are
  the acceptance test.
- Reactions outbound: DEFERRED — akkoma/pleroma stay on the bare-`Like`
  deny-list (favourite-only) until a live Akkoma round-trip verifies an
  `EmojiReact` emit shape (no registry for custom-emoji `Emoji` tags).
- `notes/reactions`: already landed (2026-10-17); this session adds the
  per-note index + contract amendments (see below).

## Changed

- `src/activitypub/objects.rs`: `render_question` options now carry
  `"type": "Note"` (covers both the standalone `Question` doc and the
  Note-embedded `oneOf`/`anyOf` spread in `render_note_object`, which copies
  from `render_question`); shape test asserts `oneOf[i].type == "Note"`.
  `extract_like_reaction` doc + `Emoji`-tag `name` fallback for
  content-less `EmojiReact`.
- `src/federation/inbox.rs`: `dispatch` routes `"EmojiReact"` to
  `handle_like`; `handle_undo` matches `"Like" | "EmojiReact"`; 3 new tests
  (`emoji_react_with_unicode_content_stores_emoji`,
  `emoji_react_with_tag_falls_back_to_emoji_name`,
  `undo_emoji_react_removes_the_reaction`).
- `src/persistence/redb/mod.rs`: new `REACTIONS_BY_NOTE` table
  `(note_id, reaction_id)` → row, maintained in all 4 write paths
  (`react`/`react_in_txn`/`unreact`/`unreact_in_txn`, upsert drops the stale
  index row); `reactions_for_note` is now a bounded per-note range scan
  (was a full-table scan); `run_reactions_by_note_backfill` with META marker
  at open for older DBs.
- `docs/endpoint-manifest.md`: `notes/reactions` row amended (index landed,
  `sinceId`-only newest-first DIVERGENCE, gate = visibility + mute/block/
  suspend via `notes/show` — stricter than Misskey, different axis from
  Sharkey); superseding note records inbound `EmojiReact` + outbound
  favourite-only status.
- `docs/misskey-parity-plan.md` §6: out-of-scope note updated (inbound half
  landed 2026-10-20, outbound still pending live test).

## Verified

- `cargo test --offline`: 424 lib + 100 conformance + integration suites,
  all green (targeted 7/7 incl. 3 new EmojiReact tests + poll shape +
  `notes_reactions_lists_reactors`).
- `bash scripts/smoke-all.sh`: SMOKE_ALL_OK (18s; cargo test + phase1 +
  phase3 + keystone-sweep 41/0 pass; phase2 four-server federation SKIP —
  needs `SHARKEY_NODE_BIN` with Node 22, environmental).
- Live Akkoma round-trip: NOT DONE (no live Akkoma in this session);
  bench-harness `pt→akkoma poll+vote` re-run is the acceptance check for
  the poll fix.

## Open

- Outbound `EmojiReact` emit for akkoma/pleroma (unicode-only behind
  `host_caps`) gated on a live round-trip test.
- `sinceId`-only ASC-vs-DESC: kept newest-first as documented DIVERGENCE;
  revisit only if a client trips on it.
- Foreign custom-emoji shortcodes store verbatim with no rendering
  (no emoji registry) — same standing limitation as Misskey-`Like` customs.
