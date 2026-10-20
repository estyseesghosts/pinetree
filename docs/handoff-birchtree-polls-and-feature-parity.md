# Handoff: make birchtree federate polls, then get the feature matrix to zero failures

> **Status (2026-10-20 freshness pass)** — a birchtree (separate repo, `/root/birchtree`) workstream; pinetree-side items are classified below.
> - **LANDED (pinetree):** poll `type: "Note"` options (`src/activitypub/objects.rs:1070`); `notes/reactions` (`src/api/misskey/endpoints/mod.rs:6278`); poll-vote per-reason drop labels (`src/application/commands/polls.rs:181-193`).
> - **OPEN (pinetree):** poll-vote counters/fixtures; outbound `EmojiReact` degrade (`docs/TODOS.md` "Bench forensics").
> - **HARNESS-ONLY:** birchtree poll federation, feature-matrix failures, and harness bugs (items 24–28). Paths `tests/phase6_federation.rs`, `docs/phase-6-exit-criteria.md`, `docs/compatibility-profile.md` cited here do **not** exist in pinetree (checked 2026-10-20).

You are continuing the 2026-10-18 federation benchmark work. Read these first, in this order:

1. `/root/pinetree/docs/federation-bench-2026-10-18-session2-problems.md`: every problem hit so far (38
   numbered items). Items 24-28 explain harness bugs you must not rediscover; items 31-37 are open.
2. `/root/pinetree/docs/federation-bench-2026-10-18-postmortem.md`: the earlier postmortem and its open items.
3. `/root/birchtree/docs/compatibility-profile.md`, row **D29** (polls are deliberately not federated), and
   `docs/phase-6-exit-criteria.md` line 47. You will reverse D29, so update both.

## Goal

When you finish, a fresh run of the **features** suite (`phase-features.py`) must show **0 FAIL** for these
five checks, on every ordered node pair that the suite generates (10 nodes, 90 pairs):

- `renote/boost`, `unboost`, `reply`, `media`, and `poll+vote`.

Do not reach 0 by weakening a check, skipping a pair, or marking a failure as not implemented. A check may be
corrected only if it is wrong; say why in the problems log. Everything else the suite already passes (post, cw,
reaction, unreaction, quote, delete, followers-only, mention-notif: 55-59 of 59 each) must still pass.
Then run the whole bench (`run-suites.sh`) and report; the other suites were never validly run.

## Rules

- Edit only `/root/pinetree` and `/root/birchtree`. Do not edit `/root/fedifedifedi` (the Akkoma proxy config and the
  Misskey/Catodon/Akkoma sources) or `/root/parity/lab`. Ask before changing anything outside the two repos.
- Do not commit unless the user asks. Both repos have large uncommitted trees. Backups of the earlier state are in
  `/root/handoff-backup/`.
- **Nothing in `/tmp`.** Every run lives in `/root/bench/<run-id>`; `BENCH_DIR` is required by every script.
- Kill processes by PID, never `pkill -f`. Do not stop `fdbserver` (FoundationDB) without asking. Leave the Claude
  Code daemon and its `bg-spare`/`bg-pty-host` processes alone.
- **Never build while a run is in progress**; the binary on disk then no longer matches what runs. Build first,
  then bring the bench up. Take the build ID from `/proc/<pid>/exe` (`bench-up.sh` prints it).
- Check `df -h /root` before each build. Targets: `CARGO_TARGET_DIR=/root/pinetree-target` for pinetree;
  `/root/birchtree/target` for birchtree.
- Report only what you verified. A miss you have not classified is "unclassified", not a pass or a failure.
  Every number carries a run ID and build IDs for pinetree and birchtree. Never reuse Oct 16/17/18 numbers.
- Final message opens with what is stuck. Then: run ID and `BENCH_DIR`; pinetree and birchtree build IDs; per-suite
  exit codes and durations (`runs/<ts>/summary.txt`); what changed in each repo and what is uncommitted; which
  numbers are unclassified; anything left running.

## How to run the bench

All scripts are in `/root/pinetree/scripts/fediverse-bench/` and take `BENCH_DIR=/root/bench/<run-id>`.

```
cargo build --release   # in /root/birchtree; pinetree: CARGO_TARGET_DIR=/root/pinetree-target cargo build --release
export BENCH_DIR=/root/bench/run3
bash bench-up.sh        # ~10 min. hosts, containers, wipe, start 10 nodes, bootstrap, probe (90/90 must resolve)
BENCH_DIR=$BENCH_DIR setsid -f nohup bash run-suites.sh > $BENCH_DIR/run-suites.out 2>&1   # or one suite: run-suites.sh features
bash bench-down.sh      # stop everything and verify with ss/docker ps
```

Run `phase-features.py` alone with `python3 -u phase-features.py` (needs `BENCH_DIR` and `BENCH_TRACE` for per-call
timing). `follows` must have completed on the same network first (it also warms the RSA keys). The `follows` suite
takes about 23 minutes (Akkoma dominates), `features` over an hour. Do not poll; use a background `until` loop on
`run-suites.out`. Do not pipe the start scripts into `tail` (they hang). Details of the topology are in the header of
`bench-up.sh`; `lab-up.sh` and `lab-net.sh` explain the container fixes (the TLS proxy exists because Mastodon only
does WebFinger over HTTPS and drops ports).

Topology: pinetree pt-a/b/c (:3000/:3103/:3105, Misskey API), birchtree bt-a/b/c (:8080/:8081/:8082, Mastodon API,
identity `http://bt-X.lab` with no port), Misskey :3101, Catodon :3210, Akkoma (docker), Mastodon 4.7.3 (docker).
Ten accounts `u0..u9` per node.

## Starting facts (from the last, partial run; harness fixes below were applied after it)

Failures in the aborted `features` run (`/root/bench/run1/runs/20261018T231448Z/features.out`, pinetree
`f58be005c558f650`, birchtree `481ad5d047821623`), 587 PASS / 160 FAIL. **Do not trust the first four rows**:

| check | fail | cause |
|---|---|---|
| renote/boost | 59 of 59 | harness: `wait_until` bug (fixed). Direct probes showed boosts reach the origin in ~3 s. |
| unboost | 8 | same harness bug (fixed). |
| reply | 27 (all birchtree origins) | harness read only `replyId` (fixed: now also `in_reply_to_id`). |
| media | 26 (all Mastodon-API destinations: bt-*, Akkoma, Mastodon) | harness read only `files`/`attachments` (fixed: `media_attachments`). May still fail: see below. |
| poll+vote | 41 | **real**; see below. |

So the real work is polls. Re-run `features` after these harness fixes to get the true remaining set of failures
before changing any product code.

### Polls (the real gap)

- Birchtree refuses polls by design: D29. Inbound `Question` and `Update` of a `Question` get 422
  (`unsupported: update of Some("Question") is not in the profile`, `src/activitypub/error.rs`); no poll is ever
  sent. Failures seen:
  - all 27 polls **posted on birchtree** (bt-a/b/c to every other node): votes never show up at the origin
    (`choices=None`);
  - pinetree polls are **unresolvable** on bt-a/b/c (9) and on Akkoma (3); pinetree to Mastodon resolves but the
    vote never reaches the origin (3);
  - Misskey to bt-a/b/c polls unresolvable (3).
- What to build: Mastodon-compatible poll federation. A poll is a `Question` object (`oneOf`/`anyOf`, `endTime` or
  `closed`, `votersCount`; each option is a `Note` with `name` and `replies.totalItems`). A vote is a `Create` of a
  `Note` with `name` = the option text and `inReplyTo` = the Question, addressed to the poll's author; the origin
  counts it and sends `Update` of the `Question` to followers with new totals. Check the exact shapes against what
  Mastodon, Misskey, Akkoma and pinetree emit and accept (capture real payloads with
  `--debug-federation` / `BENCH_TRACE`; pinetree's side is in `/root/pinetree/src/federation/`).
- Inbound: parse `Question`; create the status with its poll; accept `Update` of a `Question` (new totals, expiry);
  accept votes (`Create` `Note` with `name`) and tally them. Outbound: send `Create` of the `Question`, deliver votes
  from local voters to the origin, and send `Update` of the `Question` as votes arrive and at expiry.
- Harness: `phase-stress.py` has `POLLS_NOT_FEDERATED` (bt-* reported as "not implemented"). Remove it when polls
  work, and update the S3 numbers. The vote check in `phase-features.py` reads `S.poll(P)` (`lib.Node.poll`); for
  Mastodon-API origins it maps `poll.options[].votes_count`.
- Related birchtree failures to look at while you are there (they are not poll-specific, see problems log 32-34):
  - `Failed to parse incoming activity ... data did not match any variant of untagged enum Activity` for some
    pinetree activities (a `QuoteRequest` from pt-a; some `Create` activities from pt-c). Find which field or type
    is not accepted; it may also be a pinetree-side shape bug.
  - 403 `URL failed verification: Domains do not match`: 85 on bt-a, 80 on bt-b. Find which senders and activity
    types. It may be a legitimate rejection of an activity whose id is on another domain (Akkoma, Mastodon), in
    which case Mastodon accepts it for a reason, or a real bug.
  - `job quarantined: announce without a boost row` (6 per birchtree node): outbound boost delivery runs after the
    boost was removed.

### Media, to verify first

After the harness fix, media may still fail for Mastodon-API destinations if the attachment URL is HTTPS: `lib.fetch_bytes`
cannot fetch `https://mastodon.lab/...` without the lab CA. If so, fix `fetch_bytes` to use the lab CA
(`/root/parity/lab/certs/lab-ca.crt`); that is a harness fix. If the attachment is missing on the destination, that is a
product bug: find out whether the activity carries the `attachment` and whether the destination fetched it. Earlier
handoff item 2 (S4 `no-attachment`, 0 of 166 verified) may be the same harness bug in `phase-stress.py`; check.

### Other facts you need

- Mastodon returns 422 on a first-contact Follow from a Misskey user, about once per 9180 edges (a race; Misskey does
  not retry a permanent 4xx). `follows` therefore exits 1 with one missing edge. Classify it (Mastodon log, Misskey
  log) rather than ignoring it, and say so in the report. Do not hide the edge.
- Pinetree key races (problems 18-19) and the birchtree stale-key refetch (20) are fixed and tested; do not redo them.
- Tests: pinetree `cargo test --lib` (420 passed; integration tests were not run) and birchtree `cargo test --release`
  (all passed, including the stale-key test in `tests/phase6_federation.rs`). The vendored crate's own tests cannot run
  offline. Add tests for each poll behaviour you add.
- Handoff items 2, 3 and 4 of the postmortem remain open: S4 media, S2 miss classification (every miss is delivery lag,
  follow gap, API gap or not implemented, using `BENCH_TRACE`), and `Update` of `Question` (this task).

## Deliverables

1. Birchtree (and pinetree, if the bug is on its side) code and tests for poll federation, with D29 and the Phase 6
   exit-criteria line updated and `vendor/README.md` updated for any change to the vendored crate.
2. A fresh bench run with `features` at 0 FAIL for renote/boost, unboost, reply, media and poll+vote, and a full
   `run-suites.sh` run with every number's run ID and build IDs.
3. The Phase 7 report in `/root/pinetree/docs/` from the fresh run only, with unclassified misses listed as
   unclassified.
4. An updated `docs/federation-bench-2026-10-18-session2-problems.md` (append; do not rewrite) for any new problem.
5. Teardown with `bench-down.sh`, and a verified-clean `ss -ltnp` and `docker ps`.
