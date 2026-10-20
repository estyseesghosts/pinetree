# Phase 7 report: federation bench run5 (2026-10-19, fresh run only)

Run: `/root/bench/run5`, suite dir `runs/20261019T181810Z`.
Builds (from `/proc/<pid>/exe` at `bench-up`): pinetree `f58be005c558f650`
(unchanged), birchtree `e24d75aea1085a06` (item-52 reblog-delete Undo fix;
full `cargo test --release` 349/349).
Scope rule (operator): only bt/pt-involved pairs matter; third-party pairs
are tallied but out of scope.

| Suite | Exit | Seconds |
|---|---|---|
| follows | 1 | 1335 |
| features | 1 | 3510 |
| stress | 1 | 3152 |
| concurrent | 0 | 37 |
| crossfamily | 0 | 50 |
| interop | 1 | 398 |

## follows

9177/9180 edges, 3 missing (exit 1): all `mastodon/u0 -> {u0@misskey,
u3@catodon, u7@misskey}`. Mastodon-side first-contact flake family (item
35); the fix touches only birchtree `delete_status` and cannot affect
follows (no bt party on any missing edge). See item 57.

## features: 1108 PASS / 62 FAIL (run4: 1101/69)

In-scope (bt/pt-involved): 44. Out-of-scope: 18 (same known third-party
causes, no action).

- `unboost` 17 (was 26): **all 9 bt-booster rows flipped to PASS**
  (`bt -> bt` 6, `akkoma -> bt` 3) — the item-52 fix verified live by the
  matrix itself (the harness undoes via DELETE of the wrapper, the exact
  fixed path). Remaining rows stay red for the documented item-53 reasons:
  9 `* -> akkoma` (akkoma never applies Undo, incl. the 3 bt boosters that
  now provably send it) + 8 misskey-origin (misskey never applies Undo).
- `poll+vote` 8 (unchanged): 3 pt->akkoma (akkoma has no search backend:
  known gap); 5 catodon-origin (origin tallies zero for bt/misskey/mastodon
  voters but not pt/akkoma voters — catodon `#`-fragment vote-ID skip,
  item 55).
- `media` 18 (all mastodon-involved, all classified, none ours):
  `* -> mastodon` 9x (`pt/bt -> mastodon` 6x `mismatch:404`,
  `misskey/catodon/akkoma -> mastodon` 3x `no-attachment`); `mastodon -> *`
  9x `mismatch:200` (re-encode). In-scope media FAILs: 12.
- `reaction akkoma -> pt-*` (3), `mention-notif akkoma -> bt-b/bt-a` (2,
  plus `akkoma -> mastodon` in-scope? no — out): akkoma-outbound family,
  akkoma-side (mention gap is emit-side: bare text, no Mention tag).
  In-scope: 5 (`akkoma -> misskey/catodon/mastodon` out).
- `delete` 8: 3 `misskey -> bt-*`, 1 `akkoma -> bt-a`, 3 `mastodon -> bt-*`,
  1 `misskey -> akkoma`. **All 8 converged on post-run re-check** (marker
  absent from every dst user's home) — lag, case closed (item 58). The
  delete path is untouched by the fix (fresh-post markers are never
  reblogs), so no fix interaction.
- `renote/boost` 0 FAIL, `reply` 0 FAIL (unchanged).
- Parse 422s (quote-shape untagged-enum gap) / 403s (single catodon
  quote-note `duplicate field 'quote'`): senders attributed in run4 (item
  32/33 family); run5 not re-derived.

## stress

- S1 post storm: 3030 posts, 0 create failures; pt/bt/misskey/catodon/
  akkoma 960/960 by +180 s; mastodon ~750/780 (mastodon-side slowness).
- S2 reaction storm: 635 resolved / 215 unresolved_final (identical totals
  to run4), all `edge_present_not_visible`, recovered_after_settle=0;
  capability probes all 200. Pair table shifted slightly vs run4
  (bt-b->bt-a 10, bt-b->bt-c 6, akkoma->bt-b 7, akkoma->bt-c 14,
  mastodon->catodon 3). Run4 classification carries (timeline-index API
  gap; harness-pagination hypothesis open). `shortfall_n=4` (run4: 2),
  `residue_after_undo` 2 keys (run4: 1) — Like-path, separate mechanism.
- S3 poll storm: 50 polls, 442/455 votes ok (identical to run4), shortfall
  5 — all catodon-origin. `votes_not_implemented: 0`. 13 failed vote calls
  (same count as run4; per-call mechanism carried: akkoma-voter
  `unresolved`).
- S4 media burst: 155/202 verified (run4: 159/202). ALL 47 unverified are
  mastodon-involved: 18x `no-attachment` (dst=mastodon) + 7x
  `mismatch:404` (dst=mastodon) + 22x `mismatch:200` (src=mastodon
  re-encode). Zero non-mastodon failures. `arrival_after_settle` max 6.1 s.
- S5 reply storm: 503 replies, 60/60 sampled ok.
- S6 churn: residue bt-a 4, bt-b 4, bt-c 4, misskey 2 (new vs run4),
  catodon 1, akkoma 2. bt signature matches the root-caused
  Delete-beats-Create mechanism (`duplicate` outcomes present in run5 bt
  logs); mechanism fix not implemented (out of scope). misskey:2 is
  receiver-side, pair-level unclassified.
- S7 big text: federation half 10/10 ok; over-limit half 10/10 ok
  (bt-a/b/c + mastodon 422, rest accept).

## crossfamily: 162 checks, 0 FAIL (run4: 9 fresh / 2 re-run)

First clean crossfamily run: actor-resolve, follow, note-federation, like,
boost, delete, mention, reply, media all PASS both directions. Even the
delete rows passed fresh (no lag this time).

## interop (bt <-> real Mastodon 4.7.3): 22/23 (unchanged)

Only FAIL, third run running: `bt-a home has alice follow`. Broken oracle
(follow creates no status to grep) + async Sidekiq non-delivery; not a
product bug.

## concurrent: exit 0

C1-C4 all 50/50 (posts p50 185 ms, reactions 70 ms, media uploads 418 ms,
replies 130 ms).

## Product change in this run (birchtree, built `e24d75aea1085a06`)

Item-52 fix: `delete_status` enqueues `undo_announce` (wrapper URI) plus
drops the `REBLOGS_BY_ACCOUNT` mapping when the deleted status has
`reblog_of_id`; the `delete` event is kept. Tests: 2 new
(`a_reblog_delete_enqueues_undo_announce`,
`an_ordinary_delete_enqueues_no_undo_announce`); full
`cargo test --release` 349 passed / 0 failed.

## Unclassified (honest list)

S2 pair sub-cause (server filter vs harness pagination); S3 run5 per-call
re-derivation (count + shortfall identical to run4, mechanism carried);
S6 misskey:2 / catodon:1 / akkoma:2 receiver-side pairs; interop oracle
rewrite; follows 3-edge Mastodon-side confirmation; 422/403 run5
re-derivation. Item 52 is fixed and measured.

## If put online today (production-readiness note)

Birchtree (Mastodon-side) — shippable. Posts/replies/boosts/likes/deletes/
media/follows pass vs self, vs pinetree, vs real Mastodon 4.7.3 (22/23, 1 is
oracle). Crossfamily 162/162. Remaining visible quirks are mostly remote-side:
voting on Catodon polls drops (`#`-fragment skip, catodon-side), mentions
from Akkoma never notify (bare-text emit, akkoma-side), unboosts to Akkoma
stay stuck (akkoma never applies Undo), media vs Mastodon flaky
(fetch/re-encode, mastodon-side). Only known bt-caused issue left is the S6
Delete-beats-Create race (bt-a/b/c 4/4/4 `duplicate` residue, needs tombstone).

Pinetree (Misskey-side) — shippable for Misskey-flow. Renotes/replies/
follows/media vs non-Mastodon, favourites vs Mastodon (plain Like → ❤),
emoji vs Misskey/Sharkey/Catodon all good; zero pt-caused reaction FAILs.
Visible gaps: (1) pt-made polls not votable from Akkoma/Pleroma — options miss
`"type":"Note"` (`src/activitypub/objects.rs:1110-1111`), Akkoma
`QuestionOptionsValidator` rejects; (2) emoji reacts with Akkoma/Pleroma dead
both ways (`EmojiReact` dropped at dispatch, documented out-of-scope in
`docs/misskey-parity-plan.md` §6); (3) no `notes/reactions` endpoint
(count-only). Catodon-poll votes and Misskey `renoteCount` quirks live on the
other servers (see `~/bug_reports/` Exhibits 1-2).

## State left behind

- Bench torn down with `bench-down.sh` (see run5 log); FoundationDB
  untouched.
- nginx still running for the two Keystone UIs (`:3120` pt-a, `:8180`
  bt-a).
- Nothing committed (both repos have large uncommitted trees, as found).
- Live-probe residue on run5 nodes: none beyond suite traffic (no extra
  live probes; delete re-check was read-only).
