# Phase 7 report: federation bench run3 (2026-10-19, fresh run only)

> **Status (2026-10-20 freshness pass)** — historical bench record; figures are not re-run.
> - **LANDED (pinetree, verified at HEAD):** poll options carry `type: "Note"` (`src/activitypub/objects.rs:1070`); inbound `EmojiReact` routed to the Like handler (`src/federation/inbox.rs:346`); `NOTES_BY_URI` unique with `DUPLICATE_NOTE_URI` (`src/persistence/redb/mod.rs:1129-1139`); Delete-beats-Create tombstone (`src/federation/inbox.rs:1740-1748`).
> - **OPEN (`docs/TODOS.md` "Bench forensics"):** outbound `EmojiReact` degrades to Like; quote stamp/reply debt; poll-vote drop counters.
> - **HARNESS-ONLY:** the birchtree poll-federation and reblog fixes (birchtree repo, `/root/birchtree`), and harness tooling under `scripts/fediverse-bench/` and `/root/bench`.

Run: `/root/bench/run3`, suite dir `runs/20261019T021448Z`.
Builds (from `/proc/<pid>/exe` at `bench-up`): pinetree `f58be005c558f650`,
birchtree `cc4421edd6130686` (poll federation). Post-run, bt-a/b/c were
restarted on `781d5b6c189f6382` (two product fixes) for live verification
only; every number below was measured on `cc4421...`.

| Suite | Exit | Seconds |
|---|---|---|
| follows | 0 | 785 |
| features | 1 | 3215 |
| stress | 1 | 3084 |
| concurrent | 0 | 37 |
| crossfamily | 1 | 379 |
| interop | 1 | 403 |

## follows

9080/9080... 9180/9180 edges, 0 missing (exit 0). First run in this series
with no 422-race miss. The previous Mastodon-first-contact 422 race did not
reproduce.

## features: 1069 PASS / 101 FAIL

Goal rows: `reply` 0 FAIL. Not met: `renote/boost` 15, `unboost` 14,
`media` 35, `poll+vote` 11.

- `renote/boost * -> bt-*` (15): booster on birchtree never counted on a bt,
  misskey, catodon or akkoma origin (pt/mastodon origins fine). Root cause,
  proven live post-run: the reblog wrapper URI was minted from the target's
  URI, so the Announce id sat on the origin's domain with a foreign actor;
  domain-checking receivers refuse it. Fixed (`{actor}/statuses/{id}`),
  regression-tested, verified live (bt-b boost counted on bt-a in 6 s).
- `unboost`: `* -> akkoma` (9, every origin) and `misskey -> *` (5, every
  non-bt booster), stuck at count 1 hours later. Not lag. Still open: (a)
  akkoma-side Undo handling, (b) misskey-side Undo application. The
  `unboost ... -> bt` PASSes are vacuous (boost never arrived).
- `media * -> bt-*` (27): destination copy had no attachment at check time,
  but S4 hash-verified the same pairs during the run and every re-checkable
  copy shows its attachment hours later. Verdict: async-fetch lag vs a 90 s
  no-deadline check window, not a product bug. (pt->bt copies could not be
  re-found by the author scan, but S4 verified them; same code path.)
- `media <-> mastodon` (18 both directions): attachments present on the
  mastodon side; the FAIL is the harness fetching `https://...` without the
  lab CA (`fetch_bytes` has no CA), plus mastodon re-encoding bytes
  (200 with different sha). Not a federation failure. Harness fix pending.
- `poll+vote` (11): 3 pt->akkoma (akkoma has no search backend: known gap);
  3 akkoma->bt + 5 catodon->bt (origin tallies zero). The bt-side cause is
  the `closed`-means-ended bug below (fixed, verified live). Whether catodon
  additionally drops valid remote votes is open (needs catodon logs).
- `reaction akkoma -> *` (5), `mention-notif akkoma -> *` (6): akkoma-outbound
  interactions never land. Open, same family as the akkoma-Undo lead.
- `delete` (8, scattered incl. identical-software splits): all destination
  copies gone hours later. Lag. Case closed.
- Parse 422s (~31/bt node, pinetree `Create` + `QuoteRequest`) and 403
  domain mismatches (~130-140/bt node): senders unclassified (items 32/33
  carry over). Quotes pass only via link-fallback, so the QuoteRequest parse
  failure is real; prime suspect is the new untagged `Instrument`/`Question`
  paths. Open.
- `announce without a boost row`: 0 everywhere (item 34 not reproducing).

## stress

- S1 post storm: 3030 posts, 0 create failures; pt/bt nodes at 960/960
  markers by +180 s; misskey/catodon/akkoma converge by +180 s; mastodon lags
  (585/775 at +180 s — mastodon-side slowness).
- S2 reaction storm: 613 resolved / 237 unresolved_final, all
  `edge_present_not_visible` (follow graph complete). Pair table in
  `stress.out` (bt-c->misskey/catodon 33 each, akkoma->misskey 31,
  mastodon->akkoma 27, ...). Classification per handoff item 3 not reached;
  capability probes show search-resolve itself works (all 200s). Open.
- S3 poll storm: 50 polls, 427/455 votes ok, shortfall 5 — all
  catodon-origin. `votes_not_implemented: 0` (D29 reversal effective).
  The 28 failed vote calls need per-call classification (trace).
- S4 media burst: 168/202 verified. All 34 unverified involve mastodon as
  src or dst (https fetch + byte transformation). No bt-destination failure
  in S4 — supports the features-media lag verdict. `arrival_after_settle`
  max 12.1 s.
- S5 reply storm: 500 replies, 60/60 sampled ok.
- S6 churn: residue bt-a 5, bt-b 4, bt-c 4, catodon 1, akkoma 4. Zero-residue
  not met. Overlaps the delete-lag finding; per-pair attribution open.
- S7 big text: bt-a/b/c + mastodon correctly 422 on 2900 chars (500 limit).
  The suite expectation is wrong for Mastodon-compatible servers — harness
  bug (cap S7 at 500 or exempt limit-enforcing servers).

## crossfamily: 162 checks, 18 FAIL

9 `media pt -> bt` (same lag verdict as features). 9 `reply bt -> pt`:
proven harness bug — the check reads only Misskey-shape `replyId`, but the
bt-side copy correctly carries `in_reply_to_id` (verified live on
`asib2afq...`). Fixed post-run (same correction as handoff item 25).
Everything else (actor-resolve, follow, note-federation, like, boost,
delete, mention) PASS both directions.

## interop (bt <-> real Mastodon 4.7.3): 22/23

Only FAIL: `bt-a home has alice follow` (mastodon->bt-a follow not visible
in home timeline). Notes/edits/deletes/replies/thread/likes/boosts/quotes/
flags/media all pass both directions. The follow-visibility miss is open
(timeline rule vs delivery).

## concurrent: exit 0

C1-C4 all 50/50 (posts, reactions, media uploads, replies under parallel
load), p50 post latency 237 ms.

## Harness corrections applied before this run (checks were wrong)

- `lib.poll_vote` (Mastodon-family): singular `choice` -> plural `choices`
  (Mastodon 422s the singular; verified live).
- `phase-crossfamily.py` reply check: added `in_reply_to_id` (verified live).
- `POLLS_NOT_FEDERATED` emptied (D29 reversed).
- Pending: `fetch_bytes` lab CA; S7 500-char cap; features media check
  settle-until-deadline (S4 pattern).

## Product fixes from this run (birchtree, rebuilt `781d5b6c189f6382`)

1. Reblog wrapper URI on the booster's domain (was: target's domain).
   Tests: `a_reblog_wrapper_is_addressed_on_the_booster_domain`.
   Verified live both directions of counting.
2. `Question.closed_flag(now_ms)`: a `closed` time only means ended when
   past (Akkoma/Sharkey always send `closed` as the end time).
   Tests: `a_question_with_a_future_closed_time_is_not_expired`.
   Verified live: bt vote on akkoma poll 200, akkoma tallied in 15 s.
3. `Instrument::Question` (poll as quoting status in QuoteRequest).

## Unclassified (honest list)

S2's 237 misses by pair; S3's 28 failed vote calls; akkoma-outbound
reactions/mentions/undos; misskey-origin undo application; catodon remote-
vote tallying; S6 residue attribution; interop follow-visibility miss;
422/403 sender attribution; the single `unboost misskey -> mastodon` and
`delete` scatter (likely lag, unproven individually).

## State left behind

- Bench torn down with `bench-down.sh`: no bench listeners, no containers,
  no hosts lines (verified `ss`/`docker ps`). FoundationDB untouched.
- nginx still running for the two Keystone UIs (`:3120` pt-a, `:8180` bt-a;
  conf `/etc/nginx/conf.d/keystone-bench.conf`; Fedora default `:80` server
  commented out in `/etc/nginx/nginx.conf`). Stop with `systemctl stop nginx`
  (or `nginx -s stop`) when the UIs are no longer needed.
- Nothing committed (both repos have large uncommitted trees, as found).
  Backups predate this session in `/root/handoff-backup/`.
- Live-verification residue on run3 nodes: 2 akkoma polls + votes, 1 bt-a
  post + boost, keystone-probe OAuth app (bt-a), 1 pt-a signin token.
