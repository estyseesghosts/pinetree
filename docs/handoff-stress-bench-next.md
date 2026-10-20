# Handoff — continue the fediverse stress bench (pinetree vs Misskey / Catodon / Akkoma)

> **Status (2026-10-20 freshness pass)** — superseded in practice by the federation-bench run3–5 reports (`docs/federation-bench-2026-10-19-phase7-run{3,4,5}.md`). The pinetree-side findings it asks for are classified in `docs/fediverse-bench-2026-10-16.md` (status block) and `docs/handoff-fediverse-bench-fixes.md`.
> - **LANDED / OPEN / HARNESS-ONLY:** as in those docs. Stress-run numbers were not re-run.

Status: Draft. Written 2026-10-17 after `state/logs/2026-10-17-fed-debug-logging.md`.
Goal: finish the 5-node stress and feature bench against the current pinetree build,
decide which open findings are real pinetree bugs, fix only those (with a failing test
first), then run the gate and write a dated state log.

## 0. Read first (in this order)

1. `AGENTS.md` (rules, verification gate, state-log requirements).
2. `state/logs/2026-10-17-fed-debug-logging.md` (what changed today, live evidence, open items).
3. `docs/fediverse-bench-2026-10-16.md` (topology, methodology, original P1–P6 findings).
4. `state/logs/2026-10-17-bench-fixes-p6-and-reverify.md` (P1/P4/P5/P6 verification history).
5. `docs/persistence-conformance.md` §16 (notification order contract).

## 1. Rig and processes

Nodes: pt-ks (`:3000`, http://pt-ks.test), pt-mk (`:3103`, http://pt-mk.test), misskey,
catodon, akkoma (`*.test`). 30 accounts (`pk1..6`, `pm1..6`, `mk1..6`, `cd1..6`, `ak1..6`),
password `password123`. Health: `curl -s -o /dev/null -w '%{http_code}' http://<host>/healthz`
(pinetree) or `/` (others). Everything returned 200 at the end of the last session.

- Catodon died once overnight. Restart with:
  `cd /root/fedifedifedi/catodon && MK_WARNED_ABOUT_CONFIG=true NODE_ENV=development setsid nohup pnpm --filter backend start >> /tmp/pinetree-bench/logs/catodon.log 2>&1 &`
  It MUST be `NODE_ENV=development`; production rejects `http://` federation fetches.
- pinetree (current build = uncommitted working tree on master, HEAD `737c397`).
  Restart by PID only. The last session's `pgrep -f` restart killed its own shell:
  ```
  ps -eo pid,args | grep -E "^ *[0-9]+ /root/pinetree-target/debug/pinetree --public-url=http://pt-(ks|mk)" | awk '{print $1}'   # then kill <pid> each
  cd /tmp/pinetree-bench
  PINETREE_ALLOW_PRIVATE_IPS=1 setsid nohup /root/pinetree-target/debug/pinetree --debug-federation --public-url=http://pt-ks.test --data-dir=/tmp/pinetree-bench/pt-ks --listen=0.0.0.0:3000 >> /tmp/pinetree-bench/logs/pt-ks.log 2>&1 < /dev/null &
  PINETREE_DEBUG_FEDERATION=1 PINETREE_ALLOW_PRIVATE_IPS=1 setsid nohup /root/pinetree-target/debug/pinetree --public-url=http://pt-mk.test --data-dir=/tmp/pinetree-bench/pt-mk --listen=0.0.0.0:3103 >> /tmp/pinetree-bench/logs/pt-mk.log 2>&1 < /dev/null &
  ```
  Keep both debug switches on for the bench: the `[fed-debug]` lines are the point.
  Rebuild first with `cd /root/pinetree && cargo build --offline`; the binary is
  `/root/pinetree-target/debug/pinetree`.
- Data dirs `/tmp/pinetree-bench/pt-ks`, `/tmp/pinetree-bench/pt-mk` (kept across restarts).
  They already carry the notification-index migration. Pre-migration copies are in
  `/tmp/opencode/bench-rerun/redb-backup-pre-notif-order/`.

## 2. Reading the debug logs (new this session)

Logs are `/tmp/pinetree-bench/logs/pt-ks.log` and `pt-mk.log`. Filter with `grep '\[fed-debug\]'`.

- Inbound: `in  recv` → `in  done type=… status=…` (accepted) or `in  reject reason=…`
  (refused, with the reason). `in  drop reason=…` and `in  ignore reason=unsupported-type`
  are silent-drop cases that used to leave no trace.
- Outbound: `out fanout type=… recipients=N` → `out send type=… inbox=… status=… ms=… outcome=…`,
  with `out defer … reason=destination-cooldown` and `out skip … reason=…` as the other cases.
  `out job key=… outcome=…` is what the worker does with the job (Delivered / Retry / Quarantine).
- Known gaps: Follow/Accept/Like/Announce/Update/Delete handlers return 202 without a
  reason line, so a silent no-op there shows only `in  done … status=202`. Add a debug
  line in those handlers if you need to split blame for them.

## 3. Current state of the bench

Harness: repo copies in `scripts/fediverse-bench/` still have the known bugs. Do NOT run
them as-is. Use the patched scratch copies in `/tmp/opencode/bench-rerun/` and run from there:

- `scripts/probe-matrix.py` (unpatched copy).
- `scripts/phase-features.py` (patched: boost, unboost and reply waits no longer stop on
  their first poll; see `patch_features.py`).
- `scripts/phase-stress.py` (patched by `patch_stress.py`: S2/S3 accept 204, S1 dupes
  counted by `createdAt` after restart, S3 per-origin tallies, S4 eventual check).
- `s4_fixed.py` (replaces the S4 media check; the S4 in phase-stress still has the
  dict-as-id bug and a constant destination).
- `analyze.py` (summarises `/tmp/opencode/bench/{features,probes,stress}.json`).
- `run-all.sh` and `run-2.sh`: sequential runners (never run phases concurrently).
- `mention_latency.py`: time-to-visible for a remote mention on pinetree.

Outputs: `/tmp/opencode/bench/` (overwritten each run). Backups:
`/tmp/opencode/bench-run1-2026-10-17/` (run 1, complete) and
`/tmp/opencode/bench-prev-2026-10-17/` (the run before that).

Run 1 (complete, analysed in `/tmp/opencode/bench-rerun/logs/analyze.txt`):
- S1 post storm: 900 created, 0 failures, 720/720 markers on each node by +60 s, 0 dupes.
- S3: pt-ks 19/19, pt-mk 17/17, misskey 20/20 tallied exactly; catodon 11/19 (upstream).
- S5 replies 60/60 linked; S7 big text 5/5; reaction RTT p50 1.02 s.

Run 2 (partial, stopped on request): feature matrix 152/260 done, 144 PASS and 8 FAIL.
All 8 are `unboost` with `out=None`. S4 not run.

## 4. Open items, in priority order

1. **Unboost failures (run 2).** 8 `unboost` checks time out with the count never dropping.
   Pairs include pt-ks→akkoma, pt-mk→akkoma, misskey→pt-ks, misskey→pt-mk. Run 1 passed
   these. Split blame with the debug logs: find the Undo of the boost, check for
   `out send type=Undo` (or Announce-delete) to the destination inbox, check the status,
   and check `in  reject` on the receiver. Then check the harness: `unboost_seen`
   requires count ≤ before, and `before` is read once.
2. **S2 reaction residue.** After undo, 6 targets still have 3–4 reactions, and 3 targets
   are short by 1. Break it down per reacting origin (Akkoma reactions are excluded from
   pass criteria). Use `out send type=Like`/`Undo` and `in  reject` lines on the receiver.
3. **S6 churn residue.** One residual deleted note on every node, pinetree included,
   after a 120 s wait. Check `out send type=Delete` outcomes and whether the delete
   reached each inbox. Akkoma keeping deleted notes is a known upstream issue (U4).
4. **Probe matrix shows 0 failures** (earlier runs showed 24 Akkoma-search failures).
   Confirm whether Akkoma search now resolves, or whether the harness changed. Do not
   assume either.
5. **Finish run 2**: the full feature matrix (260 checks) with the patched copy, then
   `python3 s4_fixed.py` (writes `/tmp/opencode/bench-rerun/logs/s4-fixed.json`).
   Then, if still needed, the stress phases with the patched `phase-stress.py`.
6. **Gate**: `bash scripts/smoke-all.sh` (see `docs/testing.md`). Not run yet. Run it
   sequentially, after the bench, never concurrently. It is slow (~25 min or more).
   Step 5 may SKIP without docker or built Misskey/Sharkey.
7. **Write** `state/logs/YYYY-MM-DD-bench-rerun.md`: build/commit tested, per-phase
   numbers, anything regressed, which counters you distrusted and why, and an honest
   "not verified" list. Quote real output, not summaries.

## 5. Rules for this work

- Product code changes only when a run shows a real pinetree bug. Write the failing test
  first (`tests/conformance.rs`, Group N is the template). For storage changes, both
  backends must satisfy the contract (`docs/persistence-conformance.md`).
- Compatibility claims need real Misskey/Catodon/Akkoma nodes, not mocks.
- Exclude Akkoma from pass verdicts where the rig's missing search backend affects the
  check, and say so in the report.
- Harness pitfalls, all seen this session: a `wait_until` predicate must return `None`
  while pending (a truthy string stops the wait at once). `note()` takes an id, not the dict
  from `note_by_uri`. 204 is success on reactions and votes. Notification `createdAt`
  on pinetree is the read time, not the arrival time. Use `mention_latency.py` for real
  timing. `CLAUDE_JOB_DIR` is unset here; use `/tmp/opencode/bench-rerun/logs/`.
- Do not commit unless asked. Do not `pkill -f` (see §1).
- Verify by execution and paste real output into the state log.
