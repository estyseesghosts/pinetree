# 2026-10-17 — fediverse bench rerun (run 2 finished), unboost / S2 / S6 / probe analysis, two pinetree findings

Follows `2026-10-17-fed-debug-logging.md`. Uncommitted (per request). Scratch harness only in
`/tmp/opencode/bench-rerun/`; repo harness copies untouched. Rig: pt-ks (`:3000`) and pt-mk (`:3103`)
restarted by PID at 20:2x UTC onto the current build (PIDs 4176255 / 4176256, both `--debug-federation`).

## Summary

1. **Unboost failures (run 2): not pinetree bugs.** Two harness defects, both verified.
   - Akkoma destinations (4 of 7 unboost FAILs): the harness undoes an Akkoma boost with
     `DELETE /statuses/{wrapper}`, which Akkoma answers `404 Record not found`; the result is discarded.
     Akkoma's `POST /statuses/{orig}/unreblog` returns 200 and pinetree's renoteCount goes 2 → 1.
   - Misskey origins (3 of 7; 4 with misskey→akkoma): this Misskey checkout never decrements
     `renoteCount` on delete (verified on Misskey's own API, no federation). The renote row is removed.
2. **Real pinetree bug fixed: `Undo(Like)` named the wrong Like.** Pinetree's outbound Undo used the
   reaction row id; the Like had been federated under a different id. Akkoma drops an Undo whose object
   it cannot find. Fixed (store row id = federated Like id), regression test green, live A/B on Akkoma.
3. **Real pinetree bug found, NOT fixed (open):** a `Delete` that is handled while its `Create` is still
   in flight is dropped (no tombstone), and the late `Create` resurrects the note. Reproduced live and
   with a deterministic test (`#[ignore]`d; the fix is a storage-contract change, see Open decisions).
4. **S2 residue** (19 favourites on Akkoma-authored targets): all from pinetree reactors (18 `pt-mk`,
   1 `pt-ks`). Pinetree's local state is clean; the favourites are orphaned on Akkoma (produced by the
   pre-fix bug). Not re-reproduced in a 79-reaction re-run.
5. **S6 residue**: every residual note is a Misskey-family origin note (misskey mk1, catodon cd1/cd2)
   seen on other nodes. Pinetree- and Akkoma-origin churn notes never remained. Mechanism = item 3.
6. **Probe matrix 0 failures**: Akkoma `/search` returns 404; `note_by_uri` falls back to a home-timeline
   scan, so the 0-failure result is the fallback. `lib.py` is byte-identical to the run that showed
   UNRESOLVED; what changed is the rig's timeline state, not the code. Not verified further.
7. **Gate**: `SMOKE_ALL_SKIP_PHASE2=1 bash scripts/smoke-all.sh` → `SMOKE_ALL_OK`. Step 5 NOT run (see below).

## Changed (product code)

- `src/application/commands/notes.rs`: `react` generates `reaction_id` once; the same id is the stored
  row id and the federated Like id. `unreact` (unchanged) therefore builds `UndoLike` from the id that
  was delivered. Test module `federation_tests::undo_like_names_the_like_that_was_delivered` (new).
- `src/application/ports.rs` (`ReactionStore::react`): new `reaction_id: &str` parameter (doc comment).
  Implementors updated: `src/persistence/redb/mod.rs`, `src/activitypub/webfinger.rs` (test double),
  `src/api/misskey/packers.rs` (test double). `tests/conformance.rs` call site updated.
  `docs/application-ports.md` (Frozen) does not document the `react` signature; logged here as a
  signature change, not a contract change.
- `src/federation/inbox.rs`: test module `delete_tombstone_tests` (new):
  - `misskey_tombstone_delete_removes_the_remote_note` — passes (shape check).
  - `delete_before_create_does_not_resurrect` — `#[ignore]` (open defect), fails when run with `--ignored`.
- No `Reaction` field added; no migration.

## Verified (executed)

Undo(Like) regression, red then green:
```
test application::commands::notes::federation_tests::undo_like_names_the_like_that_was_delivered ... FAILED
  left: "adtd0qo03mz0oikk"   right: "adtd0qo03mz0oikj"     (before fix: Undo named the row id)
test application::commands::notes::federation_tests::undo_like_names_the_like_that_was_delivered ... ok   (after fix)
```
Final full suite (`cargo test --offline`, final tree, `logs/cargo-test-final.log`):
```
test result: ok. 398 passed; 0 failed; 1 ignored   (lib)
conformance 87 · client_profile 1 · client_smoke 1 · crash_recovery 2 · federation 2
federation_elixir 2 · federation_interop 1 · minrepro 1 · quote_fixtures 5 · streaming 13 · EXIT=0
```
Ignored test, run with `--ignored` (expected to fail, the open defect):
```
test federation::inbox::delete_tombstone_tests::delete_before_create_does_not_resurrect ... FAILED
late Create must not resurrect a note whose Delete already arrived
```
Live A/B on Akkoma (`logs/heart-probe.log` before, `logs/fresh-probe-after-fix.log` after):
```
before, pm1 on BBVixiip:  [❤] akkoma pm1 gone after undo: False favs=[..., 'pm1@pt-mk.test', ...]
                          [🎉] akkoma pm1 gone after undo: False
after, fresh note:        [heart] akkoma lists pm1 as favourite: True favs=['pm1@pt-mk.test']
                          [heart] undo status=204
                          [heart] akkoma pm1 gone after undo: True favs=[]
                          [party] akkoma 🎉 count rose: True now=1
                          [party] akkoma 🎉 count back to 0: True now=0
```
Akkoma's activities table (before fix) held the Like `…/likes/asge9udshnq1ifiu` and no Undo; Akkoma's
`UndoValidator` (`/opt/akkoma/lib/pleroma/web/activity_pub/object_validators/undo_validator.ex`)
requires the Undo's object to be an existing activity, otherwise the Undo is dropped with no log line.

Unboost on Akkoma (`logs/unboost-probe-akkoma-run2.log`):
```
[DELETE wrapper] status=404 body={'error': 'Record not found'}
[wait new booster ak1 on pt-ks] seen=True
[after-reblog] pt-ks renoteCount=2 notes/renotes=[('ak1',…),('ak6',…)]
[POST unreblog] status=200
[after-unreblog+30s] pt-ks renoteCount=1 notes/renotes=[('ak6',…)]
```
Unboost on Misskey (local, no federation):
```
origin renoteCount 0
renote by mk2 status 200 ... after renote renoteCount 1
notes/delete renote status 204 ... after delete renoteCount 1
notes/renotes after delete 200 []
```
Source: `NoteDeleteService.ts` decrements only `repliesCount` and instance `notesCount`; the only
`renoteCount` write in the checkout is the `+ 1` in `NoteCreateService.ts:970`.

Run 2 feature matrix (`logs/run2-phase-features.log`, exit 0, 19:32:23→19:51:38 UTC): **234/260**
(run 1: 205/260). Non-Akkoma pairs 12/12 for post, cw, reaction-type checks, quote, delete, followers-only.
FAIL classification (26):
- unboost ×7: pt-ks→akkoma, pt-mk→akkoma, catodon→akkoma, misskey→akkoma (harness DELETE, above);
  misskey→pt-ks, misskey→pt-mk, misskey→catodon (Misskey counter, above).
- poll+vote ×5 (pt-ks→akkoma, pt-mk→akkoma, catodon→akkoma, misskey→akkoma: "poll unresolvable on dst";
  catodon→misskey: `choices=None`). NOT investigated.
- media ×4 to Akkoma ("no attachment on dst copy"). Same lookup path; NOT investigated.
- reply akkoma→X ×4: pinetree replies ARE on Akkoma (`GET /api/v1/statuses/{A}/context` shows
  `pk1@pt-ks.test` and `pm2@pt-mk.test` with the reply text). Harness false negative (search 404).
- reaction akkoma→X ×4: pinetree 🎉 lands as an Akkoma emoji reaction (`pleroma.emoji_reactions`
  `🎉` count 1, `favourited_by` []). Harness counts favourites only. False negative.
- mention-notif akkoma→X ×2: NOT investigated.

Earlier partial-run claim corrected: the first 152 checks had **6 unboost + 2 poll+vote** FAILs (the handoff
said all 8 were unboost).

S4 (`logs/run2-s4-fixed.log`): 48/48 verified excluding Akkoma destinations; Akkoma destinations 0/12
(`no-attachment-60s`). `s4_fixed.py` JSON write failed (`../logs` path relative to `bench-rerun/`), so the
stdout log is the record. Path bug is in the scratch script only.

S2 per origin (current state, `logs/…` via `GET /api/v1/statuses/{id}/favourited_by` on 6 residue targets):
```
pt-mk.test: 18 (pm1 ×4 targets… pm3 on 6/6), pt-ks.test: 1 (pk6), misskey/catodon: 0
total 19 = stress.json residue_after_undo sum (3+3+3+4+3+3)
```
Pinetree-side `myReaction` / `reactions` on the local copies of two of those targets: `{}` (clean).
S2 re-run (`logs/s2-rerun.json`, 8 targets, debug logs on): attempted 106, resolved 79, shortfall 0,
residue 0. Not reproduced; whether any of the 8 targets were Akkoma-authored was not recorded.

Heart-vs-🎉 probe (pm1, before fix): Akkoma status shows the Like as favourite for ❤ and as EmojiReact for
🎉; pinetree sent Undo(Like) for both (`status=200 Delivered`) and both survived.

S6 original (tag S260332): residue pt-ks 1, pt-mk 1, misskey 0, catodon 1, akkoma 3; all `misskey|mk1`
and `catodon|cd1/cd2`. Re-run (tag S6R268816, debug logs on), churn markers of the NEW tag only:
```
pt-ks    3  origins: catodon×2 (cd1,cd2), misskey×1 (mk1)
pt-mk    3  origins: catodon×2, misskey×1
misskey  2  origins: catodon×2
catodon  1  origins: misskey×1
akkoma   3  origins: catodon×2, misskey×1
```
Pinetree- and Akkoma-origin churn: 0 residue. Note: `phase-stress.py`'s S6 counter matches any
`PERF|churn|` marker, so its raw `residue` numbers include the old tag (`{pt-ks:4,…}`); the table above
is the new tag only.

Race evidence (pt-ks debug log, timestamps in ms):
```
…816785 in recv bytes=1448 (Delete)   …816785 in recv bytes=1874 (Create)
…816786 in applied type=Create uri=http://misskey.test/notes/asgew30qfcp903q3
…816815 in done type=Delete id=http://misskey.test/793fb4d9-…  status=202   (note still live)
```
Fast-delete probe (`logs/fast-delete-probe.log`, 5 notes, create+delete with no pause, 40 s settle):
```
asgf8gkwfcp903ry pt-ks:RESIDUE(200) pt-mk:RESIDUE(200)
asgf8gmxfcp903rz pt-ks:gone(404)     pt-mk:gone(404)
asgf8go7fcp903s0 pt-ks:gone(404)     pt-mk:gone(404)
asgf8gpcfcp903s1 pt-ks:gone(404)     pt-mk:gone(404)
asgf8gr1fcp903s2 pt-ks:RESIDUE(200)  pt-mk:gone(404)
```
Every one of the 5 Deletes was answered 202 on pt-ks. Single-note case with the Create already cached
(`logs/live-delete-probe.log`): Delete removes the note on both nodes (`400`, text None).

Deterministic repro: `delete_before_create_does_not_resurrect` (`#[ignore]`d) fails with
`late Create must not resurrect a note whose Delete already arrived`. `find_by_uri` filters deleted rows,
and the Create path only checks the URI index, so nothing remembers the Delete.

Gate on the final tree: `logs/smoke-all-final.log`, `SMOKE_ALL_SKIP_PHASE2=1 bash scripts/smoke-all.sh`
```
[smoke-all] PASS  cargo test (unit + contract + integration) (23s)
[smoke-all] PASS  smoke-phase1 (core flows, misskey-js) (1s)
[smoke-all] PASS  smoke-phase3 (profile REST + streaming, misskey-js) (1s)
[smoke-all] PASS  keystone-sweep (Keystone calls, 38 checks) (2s)
[smoke-all] SKIP  smoke-phase2 (four-server federation) — SMOKE_ALL_SKIP_PHASE2=1
[smoke-all] SMOKE_ALL_OK (27s total)
```
This is NOT the full gate: step 5 is skipped (see Not run).

## Not run / not verified

- **smoke-all step 5 (four-server federation) NOT run.** `scripts/smoke-phase2.sh` binds 3101–3104, which the
  live rig uses (Misskey on 3101, pt-mk on 3103). Its `stop_all` kills listeners on those ports and runs
  `docker compose down -v`, which would remove the Misskey Postgres volume (`pinetree-interop-postgres-1`).
  Not run without your confirmation. Needs the rig stopped first.
- Poll+vote (5), media-to-Akkoma (4), mention-notif-from-Akkoma (2): not investigated.
- The 26-FAIL run-2 matrix is NOT re-run with corrected harness checks; the 7 unboost verdicts and the
  Akkoma-origin reply/reaction verdicts are harness-invalid, as explained above.
- Misskey `renoteCount` behavior: verified in this checkout only, not against upstream Misskey.
- Stuck favourites on Akkoma from the pre-fix bug (19 on the S2 targets, and the pt-mk stale ❤/🎉 for pm1 on
  BBVixiip) are not cleared; pinetree has no row for them. Needs Akkoma-side cleanup.
- Misskey↔Catodon Undo(Like) (the 2026-10-16 P3 leg): not reproduced in run 2 (unreaction 20/20); not
  addressed by this change.
- Probe matrix: explanation is by code and endpoint behavior; the historic UNRESOLVED run was not re-executed.
- Pinetree debug log has no line for Undo/Delete handler outcomes; the race was inferred from timestamps,
  and the handler's silent return path was not instrumented.
- The 2026-10-16 P3 hypothesis (b) "Like recorded under a different activity_id" is what the Undo fix covers.

## Open decisions (need your call)

1. **Delete-before-Create tombstone.** Fix = remember a deleted URI (or a tombstone note row) so a late Create
   is dropped. That needs a new `NoteStore` port method, a redb implementation, a conformance test for both
   backends (AGENTS: both backends), and a logged decision because `docs/application-ports.md` is Frozen.
   I have not made that change; the test is `#[ignore]`d until you decide.
2. **smoke-all step 5** needs the live rig stopped (or a separate port set). Your call.

## Reproduce

- Run-2 features: `bash /tmp/opencode/bench-rerun/run-2.sh` (phase-features + s4_fixed; s4 JSON path bug).
- Probes: `/tmp/opencode/bench-rerun/{unboost_probe,heart_probe,fresh_probe,control_probe,live_delete_probe,fast_delete_probe}.py`,
  S2/S6 re-runs: `s2_rerun.py`, `s6_rerun.py` (scratch; read `/tmp/opencode/bench` artifacts).
