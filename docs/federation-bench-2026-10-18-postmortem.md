# Federation bench 2026-10-18: postmortem

> **Status (2026-10-20 freshness pass)** — the "Phase 7 deliverables were not produced" status below is **superseded**: Phase 7 reports exist (`docs/federation-bench-2026-10-19-phase7-run{3,4,5}.md`).
> - **LANDED (pinetree):** `NOTES_BY_URI` uniqueness (`src/persistence/redb/mod.rs:1129-1139`); `notes/reactions` (`src/api/misskey/endpoints/mod.rs:6278`); poll `type: "Note"` (`src/activitypub/objects.rs:1070`); Delete tombstone (`src/federation/inbox.rs:1740-1748`).
> - **OPEN:** quote/reply debt and outbound `EmojiReact` (`docs/TODOS.md` "Bench forensics").
> - **HARNESS-ONLY:** birchtree defects and harness defects listed in the body.

**Status:** The rebench in `/root/PLAN_LOCAL_FEDERATION_FIX_AND_REBENCH.md` is not complete. Phases 1–5 were
worked, with several failures still open. Phase 6 produced partial results from a harness with known defects.
Phase 7 deliverables were not produced. `/root/FEDERATION_BENCHMARK_REPORT.md` is still the first-run report
(last modified 06:03 UTC).

This document records what was done, what was verified, what went wrong, and what remains open. Times are UTC
unless noted.

## 1. Outcome by phase

| Phase | Item | Result | Evidence |
|---|---|---|---|
| 1.1 | Pinetree note URIs | Pass | `uri = http://pt-a.test/notes/ash8gcgreukjayu4` (was `127.0.0.1`); unit tests pass |
| 1.2 | pinetree → pinetree | Pass, after port-80 routing | pt-b `/api/ap/show` resolved a pt-a note |
| 1.2 | pinetree → birchtree | Not verified | birchtree accounts were missing at the time (§2.2) |
| 1.2 | pinetree → misskey | Fail | misskey `ap/show` returns `REQUEST_FAILED` (§3) |
| 2.1 | birchtree ↔ birchtree | Pass | alice@bt-a resolved on bt-b; follow, accept, and timeline delivery |
| 2.2 | birchtree rate-limit flag | Pass | 1600/1600 OK with the flag; 101 × 429 without |
| 2.3 | mastodon proxy | Not needed | mastodon.lab already served on 443; the plan's premise was wrong |
| 3.1 | mastodon reachable from host | Pass | `GET https://mastodon.lab/api/v1/instance` → 200 |
| 3.2 | Misskey accounts u0–u9 | Pass | tokens in `/tmp/bench/accounts.json` |
| 4.1 | pinetree rate-limit flag | Pass | `--debug-weak-rate-limits` (×10); verified on throwaway instances |
| 4.2 | pinetree → misskey | Fail | §3 |
| 5.1 | pt-a → pt-b | Pass | resolved on the first attempt |
| 5.2 | pinetree ↔ mastodon | Fail | §3 |
| 6 | Full benchmark suite | Partial, provisional | §4 |
| 7 | Deliverables | Not produced | — |

## 2. What went wrong

### 2.1 Process

- **Serial suite runs were slow.** The suite runner executed phases one after another, and reruns after failures
  repeated whole phases. `phase-follows` alone took about 7450 s (13:43 → 15:45 UTC) in its last run.
- **Subagent sessions outlived their work.** The first Phase 6 subagent kept running its suites after the operator
  asked for a fresh start, and its processes had to be stopped by hand. The fresh subagent started an S2 run before
  it was cancelled, and that run also had to be stopped by hand (§6). Subagent sessions cannot be killed with the
  tools available here, so only their processes could be stopped.
- **Intermediate reports overstated results.** The Phase 5 report's birchtree and mastodon results rested on stale
  state or on the harness bug in §2.2. The Phase 1 report was accurate, but its "1.2 blocked" conclusion depended on
  a routing problem that was then fixed. Reports from subagents should be re-verified before they are cited.
- **Shared processes were restarted several times.** pinetree pt-a/pt-b/pt-c and birchtree bt-a/bt-b/bt-c were
  restarted during the work, and some results may have come from an older binary or from nodes without the
  federation flags. This was not tracked per run, so the first Phase 6 results cannot be attributed to one build.

### 2.2 Harness defects (`/root/pinetree/scripts/fediverse-bench/`)

- **S2 misses were silently dropped.** The check `out[0][0] in ("unresolved", "exc")` tested the first character of
  the string `"unresolved"`, so misses were printed as `S2 react fail` and never counted. Fixed: misses are now
  counted per node pair in `unresolved`.
- **S2 misses were slow.** `lib.Node.note_by_uri` falls back to scanning every local user's home timeline (up to 10
  pages each) on each miss. One akkoma miss took 7.85 s when measured. The old run had 183 misses, which at that
  cost accounts for most of its 1614 s wall time (an estimate; per-call timing was not logged). Fixed: `build_index`
  builds a per-node `uri → note` index once. Build times: bt-c 0.30 s, mastodon 0.92 s, akkoma 7.27 s.
- **The follow retry pass re-ran deterministic failures.** All 2044 first-pass failures were retried, and 1864 still
  failed. That retry cost roughly an hour. Not yet fixed.
- **S2 has fixed sleeps.** The 90 s verify wait and 60 s post-undo wait add 150 s regardless of the result.
- **Stale state was recorded as federation failure.** Birchtree's `u0..u9` on bt-b and bt-c did not exist when the
  first follow run started. Lookups returned 404 `Record not found`, and the harness recorded them as follow failures.
- **No per-call timing.** The suite logs have no request timestamps, so the time spent in each phase could only be
  inferred from the log's start and end times.

### 2.3 Changes outside the pinetree repo

- `/root/fedifedifedi/local-federation/proxy/nginx.conf` (the Akkoma proxy container) gained port-80 server blocks
  for `pt-a.test`, `pt-b.test`, and `pt-c.test`, and the container was reloaded.
- The root user's token was read from the bench Postgres (`127.0.0.1:15432`) to create Misskey accounts, because
  registration was closed. The operator authorized this for the bench.
- An OAuth app and an access token for `alice` were created in `bt-mastodon`'s database.

## 3. Federation findings

### Confirmed working
- pinetree → pinetree note delivery and resolution (after the port-80 routing fix).
- birchtree → birchtree follow, accept, and timeline delivery (with the vendored crate patch).
- misskey → pinetree inbound delivery (`/tmp/pt-a.log` shows Update activities returning 202).

### Confirmed broken or unverified
- **birchtree rejects inbound `Update` for `Question` objects** with 403 ("not in the profile"; `/tmp/bt-a.log`,
  17:35 UTC). This is a likely cause of poll-vote gaps.
- **Media does not reach the remote copy.** In the Oct 18 S4 run, 0 of 166 uploads were verified (`no-attachment`,
  pt-a → pt-b). The Oct 16 run showed the same failure (0 of 60, pt-ks → pt-mk). The cause is not diagnosed.
- **pinetree → misskey fails on signed fetches.** Misskey returns `REQUEST_FAILED` for pinetree objects. The cause is
  not confirmed; signed-GET handling and URL or host mismatch are the candidates.
- **pinetree ↔ mastodon does not resolve.** `bt-mastodon` cannot resolve `pt-a.test`, and pinetree does not trust the
  lab CA that signed `mastodon.lab`. Both must be fixed before this pair can be measured.
- **The follow graph does not converge.** At the end of the last follow run, 24 edges were still missing (pt-b to the
  akkoma and mastodon accounts).
- **Votes are incomplete.** In the Oct 18 S3 run, 133 of 376 remote votes were confirmed after a 120 s wait, with 15
  shortfalls. Some are delivery lag; the rest are unclassified.

## 4. Phase 6 results (provisional)

| Suite | Last result | Notes |
|---|---|---|
| phase-features | exit 0, Oct 18 09:29 | per-feature results not analysed here |
| phase-follows | 2044 first-pass failures; 1864 after retry; 24 missing edges | retry pass unfixed (§2.2) |
| phase-stress S2 | 83 targets; 685 attempted; 502 resolved; 183 unresolved; 1614 s | misses unclassified; harness fixed after this run |
| phase-stress S3 | 50 polls; 133 of 376 votes | §3 |
| phase-stress S4 | 166 uploads; 0 verified | §3 |
| phase-stress S5 | Oct 18: 487 replies; 60 sampled, 60 OK | — |
| phase-concurrent | exit 1 | not analysed |
| phase6-interop-3node | exit 0 | not analysed |
| phase-crossfamily | exit 0 | not analysed |

S6 (churn) and S7 (big text) have only been run on Oct 16, not on Oct 18. The Oct 16 S6 run left residue
(`pt-ks` 2, `pt-mk` 2, `misskey` 1, `akkoma` 3), so the zero-residue requirement was not met then. These numbers come from
an older harness and build and should not be treated as current.

## 5. Misses: federation or not implemented?

The S2 misses were never classified. What the evidence shows so far:

- **Not implemented.** pinetree has no `notes/reactions` endpoint (404 `UNKNOWN_API_ENDPOINT`, per the comment in
  `lib.py`), so its reactions are count-only. Birchtree and akkoma search do not fetch remote notes by URI: a fresh
  pt-b note returned empty from `/api/v2/search?resolve=true` on bt-c, bt-a, and akkoma. Akkoma's search has no
  backend, per `lib.py`.
- **Delivery lag.** A note created on pt-b was found on bt-c in 0.01 s once it had arrived, so a miss at S2 time can
  simply mean the note had not arrived yet.
- **Follow gap.** A reacting node only sees a remote note in its home timeline if it follows the author. Missing
  follow edges produce misses that look like delivery failures.

Until each category is counted, a miss should be reported as "unclassified", not as a pass or a federation failure.

## 6. Current state

- **Stopped in this session:** the old Phase 6 suite runner and its `phase-concurrent.py` child; the first S2 run; and the
  S2 run started by the fresh subagent (`SKIP_S1=1 ONLY=s2`). That run's partial output is in
  `/tmp/bench/phase6/phase-stress-s2.out`: 79 targets, first pass 259 resolved and 201 unresolved, stopped during the
  settle wait.
- **Still running:** pinetree pt-a (3000), pt-b (3103), and pt-c (3105) on `/tmp/pinetree-target/release/pinetree`, with
  `--debug-federation --debug-weak-rate-limits`; birchtree bt-a (8080), bt-b (8081), and bt-c (8082) with the federation
  flags; the docker infra; and misskey on 3101.
- **Uncommitted:** the pinetree working tree (Phase 1 URI fix, rate-limit flag, S2 harness changes) and the birchtree
  working tree (federation flags, rate-limit flag, and the `vendor/activitypub_federation` patch). Nothing is committed.

## 7. Open items, in priority order

1. Change `phase-follows.py` to skip deterministic failures and report them separately.
2. Diagnose media `no-attachment` on both the send and receive sides.
3. Classify every S2 miss as delivery lag, follow gap, API gap, or not implemented.
4. Fix or document birchtree's `Question` Update rejection (403).
5. Fix pinetree ↔ mastodon: add `--add-host` entries for pt-a/pt-b/pt-c on `bt-mastodon` and `bt-sidekiq`, and give
   pinetree a bench-only way to trust the lab CA.
6. Explain pinetree → misskey `REQUEST_FAILED` on signed fetches.
7. Re-run Phase 6 cleanly, with per-call timing and a build ID for each run, and produce the Phase 7 deliverables.
8. Review and commit or discard the uncommitted changes.
