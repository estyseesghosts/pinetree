# 2026-10-17 — bench fixes: P2 `notes/reactions`, P3 closed as harness bug

Follows `2026-10-17-bench-fixes-p1.md`.

## P2 — implemented
- `AppServices::note_reactions` (`src/application/queries.rs`): visibility via `notes_show`
  (`NO_SUCH_NOTE` when missing/hidden), optional `type` filter (`:name@.:` → `:name:`),
  id/date cursor, newest first, skips reactors whose account is gone.
- `pack_note_reaction` (`packers.rs`), `notes/reactions` arm (`endpoints/mod.rs`).
- Test `notes_reactions_lists_reactors` (shape, order, type filter, paging, unknown note,
  followers-only note hidden from a stranger). `cargo test`: lib 391, conformance 86, all green.
- Docs: `docs/endpoint-manifest.md` row + removed from "explicitly out"; `docs/wiki/Current-State.md`.
- Caveat: `ReactionStore::reactions_for_note` scans (no per-note reaction index); fine for
  redb, needs a per-note key range before it is used on hot FDB paths.
- Not verified live: the running rig pinetrees still run the old binary (not restarted).

## P3 — not a product bug
- Evidence: every misskey-family `unreaction` failure in `features.json` has `secs=0.0` and
  `out=remaining={...}`. In `phase-features.py` `unreact_seen` returned the truthy string
  `"remaining=…"` while the reaction was still present; `wait_until` stops on the first truthy
  value, so the check failed on its first poll, before the Undo could be delivered. Akkoma passed
  only because its unfavourite is synchronous.
- Fix: `unreact_seen` now returns `None` until the count is 0 (`scripts/fediverse-bench/phase-features.py`).
- Control runs (`scripts/fediverse-bench/p3-control.py`, sequential, 3 cycles/pair, rig as running):
  misskey>catodon, catodon>misskey, pt-ks>misskey, misskey>pt-ks, pt-ks>pt-mk, pt-mk>catodon,
  catodon>pt-ks — every cycle `react_s=1.0 undo_s=1.0 remaining={}`.
- Therefore `docs/fediverse-bench-2026-10-16.md` §P3 ("fails to converge", "upstream regression?")
  is superseded by this entry. No pinetree code changed for P3. The bare-string `Undo` object
  concern in the handoff has no evidence behind it and was left alone.
- The full feature matrix was not re-run end to end.

## Rig note
Catodon had died overnight (log ends 23:44 UTC Oct 16). Restarted with
`cd /root/fedifedifedi/catodon && MK_WARNED_ABOUT_CONFIG=true NODE_ENV=development pnpm --filter backend start`
(must be `development`: production rejects `http://` fetches).

## Open
P6 instrumentation, bench re-run for P1/P4/P5, `scripts/smoke-all.sh`.
