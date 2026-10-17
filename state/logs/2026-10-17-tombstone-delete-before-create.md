# 2026-10-17 — Delete-before-Create tombstone (bench S6), and the smoke-all gate

Follows `2026-10-17-bench-rerun.md`, which left the tombstone race open pending the owner's
decision. The owner approved it ("go ahead with the tombstone for pinetree; you can stop the
live rig to run step 5"). Uncommitted, per the standing rule.

## What was wrong

A `Delete` whose note this node does not yet hold returned 202 and recorded nothing
(`handle_delete` fell through to `accepted()`). A late `Create` for the same URI then stored the
note, so it stayed live. Misskey and Catodon send `Delete` before the `Create` they race with
(bench S6). Reproduced earlier by `delete_before_create_does_not_resurrect` (red, as recorded in
`2026-10-17-bench-rerun.md`).

## What changed

Storage (`src/persistence/redb/mod.rs`):

- New table `TOMBSTONES_BY_URI` (uri → marker), created at open.
- `record_delete_uri_activity` (new): one write transaction. Replayed activity id → `Ok(false)`.
  A live note holding the URI is deleted (`Ok(true)`). Otherwise a tombstone is written
  (`Ok(false)`), and the jobs are dropped because no note was deleted. Empty URI → `Validation`.
- `create_note_in_txn`: a tombstoned URI → `Conflict("TOMBSTONED_NOTE_URI")`.
  `record_note_activity` maps it to `Ok(None)`, like `DUPLICATE_NOTE_URI`.
- Deleting a held note does NOT write a tombstone. This keeps Group U3
  (`u3_deleted_note_releases_uri`): a re-fetch after deleting a held note may recreate it.
  The first version of this change did tombstone on every delete. It broke U3, which caught it;
  the fix is the narrower rule above.

Port (`src/application/ports.rs`): `ActivityDedup::record_delete_uri_activity`, with a default
`unimplemented!` the same as `record_delete_note_activity`. Test doubles are untouched.

Application (`src/application/commands/inbound/notes.rs`): `handle_inbound_tombstone_uri`.
Maps `NotFound` → `Ok(false)`, like `handle_inbound_delete_note`.

Federation (`src/federation/inbox.rs`, `handle_delete`), **Path 4**: at the early return where no
quote stamp matches the object. The tombstone is written only when the actor's host equals the
object URI's host and the object's host is not our own `public_url` host. Path 1 (known note) and
Paths 2–3 (quote stamps) are unchanged. Path 4 only runs when no stamp matches, so a stamp
revocation cannot reach it.

Tests:

- `delete_before_create_does_not_resurrect` (`federation::inbox::delete_tombstone_tests`):
  `#[ignore]` removed, now passing.
- Conformance Group T (`tests/conformance.rs`): `t1_delete_before_create_leaves_tombstone`,
  `t2_delete_by_uri_removes_and_releases`, `t3_delete_replay_and_scope`.

Docs: `docs/persistence-conformance.md` gets a Draft "Group T" section, and the Group U bullet
on releasing URIs gets an exception note pointing to it.

## Verified (executed)

Targeted, after the final edit:

```
cargo test --offline --lib delete_before_create
  test federation::inbox::delete_tombstone_tests::delete_before_create_does_not_resurrect ... ok
  test result: ok. 1 passed; 0 failed; 0 ignored; 398 filtered out

cargo test --offline --test conformance -- t1_ t2_ t3_
  test t3_delete_replay_and_scope ... ok
  test t1_delete_before_create_leaves_tombstone ... ok
  test t2_delete_by_uri_removes_and_releases ... ok
  test result: ok. 3 passed; 0 failed; 0 ignored; 87 filtered out
```

Full `cargo test --offline` (log `/tmp/opencode/bench-rerun/logs/` → `/tmp/opencode/tombstone-full-test.log`),
EXIT=0. Two passes; the first caught the U3 regression (above) and the second is clean:

```
lib:            399 passed; 0 failed; 0 ignored   (was 398 passed + 1 ignored)
conformance:     90 passed; 0 failed              (was 87; +T1, T2, T3)
other suites:   all "ok", 0 failed
```

The U3 test (`u3_deleted_note_releases_uri`) and the U1/U2 tests pass again.

Not verified:

- T1 was not run red at the conformance level. The red evidence is the handler-level test,
  recorded red before the fix in `2026-10-17-bench-rerun.md`. T2 and T3 were written after the fix.
- No live re-run of the bench S6 probe (`fast_delete_probe.py`) against the patched build. The rig
  is stopped (see below).

## Live rig and smoke-all

Before stopping the rig I dumped the Misskey database as insurance, because step 5's
`docker compose down -v` deletes the `pinetree-interop` Postgres volume:

- `/tmp/opencode/rig-backup/misskey-2026-10-17.sql.gz` (321 KB, `pg_dump -d misskey`, 117 `COPY` blocks)

Stopped by PID (TERM): pt-ks 4176255 (`:3000`), pt-mk 4176256 (`:3103`), Misskey master
1963857 (`:3101`). Ports 3000/3101/3103 confirmed free. Their redb data dirs
`/tmp/pinetree-bench/pt-ks` and `/tmp/pinetree-bench/pt-mk` are on disk and untouched.
Akkoma, Pleroma, and Catodon containers are separate compose projects and were left running.

**Run 1, no `SHARKEY_NODE_BIN`:** `/tmp/opencode/bench-rerun/logs/smoke-all-tombstone.log`
printed `SMOKE_ALL_OK`, but step 5 was **SKIPPED**
(`set SHARKEY_NODE_BIN=<dir with a Node 22 node>`). Only steps 1–4 ran:

```
PASS  cargo test (unit + contract + integration) (15s)
PASS  smoke-phase1 (core flows, misskey-js) (1s)
PASS  smoke-phase3 (profile REST + streaming, misskey-js) (1s)
PASS  keystone-sweep (Keystone calls, 38 checks) (2s)   -> KEYSWEEP: 41 passed, 0 failed
SKIP  smoke-phase2 (four-server federation)
```

So run 1 does not verify step 5, and the `SMOKE_ALL_OK` verdict covers steps 1–4 only. The
rig stop was done for step 5, which did not run.

**Run 2:** only Node 24 was installed. Node 22 was installed via nvm (`v22.23.3`, checksum
verified) and run with `SHARKEY_NODE_BIN=/root/.nvm/versions/node/v22.23.3/bin`. Log:
`/tmp/opencode/bench-rerun/logs/smoke-all-tombstone-step5.log`.

```
DONE  cargo test (unit + contract + integration) (14s)
DONE  smoke-phase1 (core flows, misskey-js) (1s)
DONE  smoke-phase3 (profile REST + streaming, misskey-js) (2s)   -> SMOKE3_OK
DONE  keystone-sweep (Keystone calls, 38 checks) (2s)            -> KEYSWEEP: 41 passed, 0 failed
FAIL  smoke-phase2 (four-server federation) (931s, exit 1)       -> SMOKE_FAIL: pinetree-a did not come up
```

**Step 5 is NOT verified. It failed on an environment port conflict, not on pinetree code.**

- `/tmp/pinetree-interop/proxy.log`: the interop proxy never started.
  `Error: listen EADDRINUSE: address already in use 127.0.1.1:80`.
- Cause: the `local-federation-proxy-1` container (nginx:alpine, compose project
  `local-federation`, Akkoma/Pleroma frontend) publishes `0.0.0.0:80` and `0.0.0.0:8099`. The
  phase-2 script binds `127.0.1.1–4:80`. Because the proxy never bound, `wait_http
  http://127.0.1.3` reached that nginx instead of pinetree-a. Its `/api/meta` never answered
  `version`, so the script timed out. `pt3.log` and `pt4.log` are empty because the pinetree
  processes produced no output; `pt3.redb` was created, so pt-a did start.
- I did NOT stop `local-federation-proxy-1`. It was serving a live browser session on
  `127.0.0.1:8099` (requests logged at 21:08 UTC, the check ran at 21:09). It is a different
  compose project from the rig, and the phase-2 script has no way to avoid port 80.
- The script's cleanup ran `docker compose -f scripts/interop/compose.yml down -v`. The
  `pinetree-interop` containers and volumes are **gone** (`docker volume ls` shows no interop
  volume). The Misskey data now exists only in
  `/tmp/opencode/rig-backup/misskey-2026-10-17.sql.gz` (321 KB, verified by `zcat`).
- No stray processes remain from the run. pt-ks and pt-mk are stopped too, but their redb data
  dirs (`/tmp/pinetree-bench/pt-ks`, `pt-mk`) are intact.

**Run 3 — step 5 passes, via a network namespace (no repo change).** Port 80 can't be moved:
`scripts/interop/proxy.mjs` binds `ip:80`, and Misskey's WebFinger URL builder drops the port,
so every node must be on port 80. `local-federation-proxy-1` holds `0.0.0.0:80`, so the
`127.0.1.x:80` binds fail on the host. The fix runs the unmodified script in a fresh network
namespace, where port 80 is free:

- `unshare -n` namespace; veth pair `vh0` (host `10.200.0.1`) ↔ `vn0` (namespace `10.200.0.2`).
- Scratch TCP forwarders `/tmp/opencode/netns/fwd.mjs` bridge Postgres `15432` and Redis `16379`
  across the veth, because the script hard-codes `127.0.0.1:15432/16379` and Docker publishes
  those on host loopback only.
- `nsenter -t <pid> -n env SHARKEY_NODE_BIN=/root/.nvm/versions/node/v22.23.3/bin bash scripts/smoke-all.sh`
  (log `/tmp/opencode/netns/smoke-all-netns.log`).

Result, executed:

```
PASS  cargo test (unit + contract + integration) (15s)
PASS  smoke-phase1 (core flows, misskey-js) (1s)
PASS  smoke-phase3 (profile REST + streaming, misskey-js) (2s)
PASS  keystone-sweep (Keystone calls, 38 checks)   -> KEYSWEEP: 41 passed, 0 failed
PASS  smoke-phase2 (four-server federation) (45s)  -> INTEROP_OK, federation_interop_four_servers ok (14.90s)
SMOKE_ALL_OK (64s total)   SMOKE_EXIT=0
```

Cleanup afterwards: forwarders stopped, veth removed, namespace process killed. The browser proxy
`local-federation-proxy-1` was never touched and still returns 200 on `:8099`. The script's own
`compose down -v` removed the interop Postgres/Redis volumes, as it does every run. The Misskey
dump at `/tmp/opencode/rig-backup/misskey-2026-10-17.sql.gz` is the only copy of the bench Misskey
data.

## Summary of verification for this change

Verified (executed): targeted tests; full `cargo test --offline` (EXIT=0); the complete
`smoke-all.sh` gate, all five steps, `SMOKE_ALL_OK` (run 3, in the namespace).

## Live S6 probe on the patched build (run 4)

The rig was brought back up on the current build (`cargo build --offline` at 17:42):

- Postgres/Redis: `docker compose -f scripts/interop/compose.yml up -d`, `CREATE DATABASE misskey`,
  then the dump restored (`ON_ERROR_STOP=0`, 0 errors, 32 users).
- Misskey: `url: http://misskey.test/`, port 3101, from `probes/misskey-bench.yml`, with
  `MISSKEY_CONFIG_YML=... MISSKEY_WEBFINGER_USE_HTTP=true NODE_ENV=development sh -c
  'node scripts/compile_config.js && exec node built/entry.js'` (the phase-2 start).
- pt-ks / pt-mk: restarted with the handoff's commands (debug switches on). Health: all 200.
- Bench tokens checked before the probe: `mk1@misskey`, `pk1@pt-ks`, `pm1@pt-mk` all `200`.

Results (`state/bench-results/2026-10-17/`):

```
before fix (5 cycles):  pt-ks RESIDUE 2 of 5, pt-mk RESIDUE 1 of 5
after fix  (5 cycles):  0 residue on both nodes   logs/fast-delete-probe-fixed.log
after fix (20 cycles):  0 residue on both nodes   logs/fast-delete-probe-fixed-n20.log
```

**Caveat, stated plainly:** in the post-fix run every Create was applied on pt-ks before its
Delete arrived (about 1.3 s apart; `debug/pt-ks-fed-debug-s6-window.log`). The live run did not
exercise the Delete-before-Create race, so the zero residue shows no regression but does not prove
the tombstone path fired. The race is covered by the deterministic handler test and the T1–T3
conformance cases. Making the live race happen on demand would need a delivery-delay control this
rig does not have.

Left running afterwards: Misskey `:3101`, pt-ks `:3000`, pt-mk `:3103`, and the interop
Postgres/Redis containers. `smoke-phase2.sh` kills listeners on 3101–3104 and runs `down -v`,
so it will stop them again.

## Still not verified

- The Delete-before-Create race under live federation (see caveat above).
- The run-2 FAILs listed in `2026-10-17-bench-rerun.md` (poll+vote, media-to-Akkoma, mention-notif).

## Open questions for the owner

1. **Frozen contract.** `docs/application-ports.md` is Frozen and I did NOT edit it. The new
   `ActivityDedup::record_delete_uri_activity` is recorded only in the Group T addendum and this
   log. Approve that as the superseding record, or ask for a frozen-doc edit.
2. **Unbounded tombstones.** Any remote actor on a host can grow `TOMBSTONES_BY_URI` by sending
   `Delete` for arbitrary URIs on its own host (bounded only by inbox body size). Same class as
   `ACTIVITY_DEDUP`. Options: an age-based sweep, or a per-host cap. MVP accepts it.
3. **Streaming gap.** If a note appears between the handler's lookup and the write, it is deleted
   without a `NoteDeleted` event. Narrow window; accepted and documented, not engineered around.
4. **Replayed Create after a held-note delete** still re-creates the note (U3 by design). Only a
   Delete that found no note tombstones. If the owner wants a replayed Create after a delete
   refused too, that conflicts with U3 and needs a contract decision.
5. Still open from the bench, unchanged here: the unexplained run-2 FAILs (poll+vote ×5,
   media-to-Akkoma ×4, mention-notif-from-Akkoma ×2); the harness repairs listed in
   `2026-10-17-bench-rerun.md`; the Misskey↔Catodon Undo(Like) item from 2026-10-16 P3.
