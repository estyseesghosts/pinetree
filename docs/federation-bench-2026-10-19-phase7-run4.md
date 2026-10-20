# Phase 7 report: federation bench run4 (2026-10-19, fresh run only)

Run: `/root/bench/run4`, suite dir `runs/20261019T144547Z`.
Builds (from `/proc/<pid>/exe` at `bench-up`): pinetree `f58be005c558f650`
(unchanged), birchtree `ee6bfe94122a12c7` (configurable status text limit;
poll federation + reblog-URI + `closed` fixes from run3 included).
Scope rule (operator): only bt/pt-involved pairs matter; third-party pairs
are tallied but out of scope.

| Suite | Exit | Seconds |
|---|---|---|
| follows | 0 | 797 |
| features | 1 | 3709 |
| stress | 1 | 3045 |
| concurrent | 0 | 37 |
| crossfamily | 1 | 211 |
| interop | 1 | 399 |

## follows

9180/9180 edges, 0 missing (exit 0). Second consecutive clean run; the
Mastodon-first-contact 422 race has not reproduced since run2.

## features: 1101 PASS / 69 FAIL (run3: 1069/101)

In-scope (bt/pt-involved): 51. Out-of-scope: 18 (same known third-party
causes, no action).

- `renote/boost` 0 FAIL (was 15): the reblog-wrapper-URI fix confirmed
  across the whole matrix. `reply` 0 FAIL.
- `media * -> bt-*` 0 FAIL (was 27): the deferred settle+deadline check
  (settle 30 s, deadline 240 s, same sha-match bar) proves arrival.
- `media <-> mastodon` (18, all mastodon-involved, all classified, none
  ours): `pt/bt -> mastodon` 6x no-attachment after the full window
  (mastodon never fetched/resolved); `misskey/catodon/akkoma -> mastodon`
  3x `mismatch:404` (mastodon-side fetch failure); `mastodon -> *` 9x
  `mismatch:200` (re-encode; the lab-CA fix turned fetch `exc`s into 200s,
  confirming bytes present but transformed). Recommendation stands: keep
  the bar red with this documented cause rather than weakening it.
- `poll+vote` (8, was 11): `akkoma -> bt-*` fixed by the `closed` fix
  (live: bt vote 200, akkoma tallied). Remaining: 3 pt->akkoma (akkoma has
  no search backend: known gap); 5 catodon-origin (origin tallies zero for
  bt/misskey/mastodon voters but not pt/akkoma voters — catodon-side,
  needs catodon logs).
- `unboost` (26, was 14): fully attributed (problems-log item 53).
  9 bt-booster rows (`bt -> bt` 6, `akkoma -> bt` 3) are the new product
  bug in item 52 (DELETE of a reblog sends `Delete`, never
  `Undo(Announce)`; proven live against real Mastodon 4.7.3 behavior).
  9 `* -> akkoma` rows are akkoma-side (never sends/applies Undo).
  `misskey -> pt/catodon/mastodon` are misskey-side (never applies Undo;
  pt provably sends it). `misskey -> bt-*` is joint. Previously-masked
  rows surfaced because boosts now arrive — a sign of progress, not
  regresssion.
- `reaction akkoma -> pt-*` (3), `mention-notif akkoma -> bt-*` (3):
  akkoma-outbound interactions never land (same family as akkoma-Undo).
  Open, akkoma-side.
- `delete` (6, all bt-dest): all destination copies gone hours later.
  Lag. Case closed.
- Parse 422s / 403 domain mismatches: senders unclassified (items 32/33
  carry over).

## stress

- S1 post storm: 3030 posts, 0 create failures; pt/bt 960/960 by +180 s;
  misskey 882, catodon/akkoma 960 by +180 s; mastodon 746/776
  (mastodon-side slowness).
- S2 reaction storm: 635 resolved / 215 unresolved_final (run3: 613/237),
  all `edge_present_not_visible`, recovered_after_settle=0; capability
  probes all 200. `shortfall_n=2`, `residue_after_undo=1`. Item 3
  classification still open.
- S3 poll storm: 50 polls, 442/455 votes ok (run3: 427/455), shortfall 5 —
  all catodon-origin. `votes_not_implemented: 0`. 13 failed vote calls
  need per-call classification.
- S4 media burst: 159/202 verified (run3: 168/202). All 43 unverified are
  mastodon-involved (21x `mismatch:404` dst=mastodon, 22x `mismatch:200`
  src=mastodon). Zero non-mastodon failures. `arrival_after_settle` max
  12.1 s.
- S5 reply storm: 500 replies, 60/60 sampled ok.
- S6 churn: residue bt-a 4, bt-b 6, bt-c 6, catodon 1, akkoma 2 (run3:
  5/4/4/1/4). Zero-residue not met; per-pair attribution open.
- S7 big text (new split harness): federation half (450 chars) 10/10 ok;
  over-limit half (2900) 10/10 ok — bt-a/b/c + mastodon correctly 422,
  all others accept. Limit enforcement documented, not judged as failure.

## crossfamily: 162 checks, 9 FAIL fresh (run3: 162/18)

`reply bt -> pt` now PASSES (item 41 fix verified). All 9 fresh FAILs are
`media pt -> bt` — proven harness bug (item 51: check never read
`media_attachments`; copies verified live with attachments). Fixed
post-run; re-run results in `crossfamily.rerun*.out` (fresh record kept in
`crossfamily.out.fresh` / `crossfamily.json.fresh`). Everything else
(actor-resolve, follow, note-federation, like, boost, delete, mention)
PASSES both directions. With the corrected check (re-run 2): 162 checks,
2 failures, both `delete`, both converged on re-check (lag) — media 9/9
PASS. Fresh record kept in `crossfamily.out.fresh` /
`crossfamily.json.fresh`; re-runs in `crossfamily.rerun*.out`.

## interop (bt <-> real Mastodon 4.7.3): 22/23

Only FAIL, identical to run3: `bt-a home has alice follow`. Open
(timeline rule vs delivery).

## concurrent: exit 0

C1-C4 all 50/50 (posts p50 118 ms, reactions 70 ms, media uploads 479 ms,
replies 139 ms).

## Product change in this run (birchtree, built `ee6bfe94122a12c7`)

Configurable status text limit: `--max-status-chars=` (default 500,
Mastodon parity; max 3000, Misskey parity), validated, advertised in the
v1/v2 instance configuration, schedule-time validation added (was:
silently dropped at publish). Tests: 4 new + full `cargo test --release`
347 passed / 0 failed.

## Harness corrections in this run (checks were wrong)

- S7 split (federation 450 + over-limit 2900 probe, 500-enforcer 422
  asserted, the rest informational).
- `fetch_bytes` lab CA (`/root/parity/lab/certs/lab-ca.crt`).
- Features media deferred settle+deadline verify (same bar, honest window).
- Crossfamily media: `media_attachments` + fetchable-URL requirement
  (item 51; found by this run).

## Unclassified (honest list)

S2's 215 misses by pair; S3's 13 failed vote calls; akkoma-outbound
reactions/mentions/undos; misskey-origin undo application (mechanism);
catodon remote-vote tallying (mechanism); S6 residue attribution; interop
follow-visibility miss; 422/403 sender attribution. Item 52 (bt unboost)
is root-caused but the fix is not implemented or measured.

## State left behind

- Bench torn down with `bench-down.sh` (see below); FoundationDB untouched.
- nginx still running for the two Keystone UIs (`:3120` pt-a, `:8180` bt-a).
- Nothing committed (both repos have large uncommitted trees, as found).
- Live-probe residue on run4 nodes: 2 bt-a posts + boosts/unboosts,
  1 bt-a post boosted from mastodon, crossfamily re-run notes.
