# Federation bench run4 log (2026-10-19): S7 + media harness tweaks, configurable status limit

> **Status (2026-10-20 freshness pass)** — historical log; not rewritten.
> - **LANDED (pinetree):** `ap/show` media + reply ingest through the shared pipeline (`src/api/misskey/endpoints/mod.rs:8213`, `src/federation/inbox.rs:2081-2170`); `notes/reactions` list present (`endpoints/mod.rs:6278`).
> - **OPEN:** quote-path media fetch (bounded, `docs/TODOS.md` "Bench forensics"); reply debt.
> - **HARNESS-ONLY:** S7 500-char cap and exemption; media settle-until-deadline; birchtree configurable status limit. The cited `src/api/mastodon/mod.rs`, `src/application/statuses.rs`, `src/application/scheduled_statuses.rs`, and `tests/phase3_status_limit.rs` are **not** pinetree paths (no `src/api/mastodon` at HEAD); they are birchtree code.

**Status: all suites finished.** `follows` 0 (797 s); `features` 1 (3709 s,
1101/69); `stress` 1 (3045 s); `concurrent` 0 (37 s); `crossfamily` 1 (211 s,
162 checks, 9 FAIL fresh); `interop` 1 (399 s, 22/23).

- Run: `/root/bench/run4`, suite dir `runs/20261019T144547Z` (verify at teardown).
- Builds (from `/proc/<pid>/exe`, `bench-up.sh`): pinetree
  `f58be005c558f650` (unchanged, no pinetree Rust changes), birchtree
  `ee6bfe94122a12c7` (configurable status limit, see below).
- Bench network: 10 nodes (3 pt, 3 bt, misskey, catodon, akkoma, mastodon);
  90/90 probes resolved on a pristine `bench-up`.
- Scope rule (operator): only pairs involving birchtree or pinetree matter;
  third-party<->third-party FAILs are out of scope (tallied, not investigated).

## What changed before this run

### Birchtree product change: `--max-status-chars=` (default 500, max 3000)

- `src/config.rs`: `max_status_chars` (default 500); validate rejects 0 and
  >3000; unit tests (default-is-500, reject-0, reject-3001/accept-3000).
- `src/main.rs`: `--max-status-chars=` flag (integer 1..=3000, else exit 2).
- `src/api/mastodon/mod.rs`: `ServerInfo.max_status_chars` from config.
- `src/application/statuses.rs`: `create_status()` takes `max_chars`;
  `DEFAULT_MAX_STATUS_LENGTH=500`, `ABS_MAX_STATUS_LENGTH=3000`;
  over-long message interpolates the limit (identical to Mastodon's at default).
- `src/application/scheduled_statuses.rs`: `schedule_status()` validates
  length at schedule time (same message; previously accepted then silently
  dropped at publish); the limit travels in the row params, `publish_scheduled_job`
  replays with the stored limit (old rows fall back to 500).
- `src/api/mastodon/dto.rs`: v1/v2 instance `max_characters` advertise the
  configured value (v1 took a new `&ServerInfo` param).
- `README.md` flags table + `docs/compatibility-profile.md` open-decision
  line updated (status limits decided; media limits still open).
- Tests: new `tests/phase3_status_limit.rs` (4: default rejects 2900 with
  Mastodon message, raised-3000 accepts 2900, 3001 rejected); full
  `cargo test --release` 347 passed / 0 failed (1 ignored measurement).
- Bench nodes run the default (500), so S7's over-limit probe still expects
  422 on bt-*.

### Harness corrections (checks that were wrong, not weakened)

- S7 (`phase-stress.py`): split in two. Federation half posts 450 chars
  (under every default) and is judged everywhere; over-limit half posts 2900
  chars — `bt-*`/mastodon must 422 (judged: a `bad:` only when a 500-enforcer
  wrongly accepts), all other outcomes informational. `S7_FED_CHARS` /
  `S7_OVER_CHARS` env-overridable. `failures()` judges `S7_big_text` plus
  `S7-over-limit` `bad:` entries only.
- `lib.fetch_bytes`: https now trusts `/root/parity/lab/certs/lab-ca.crt`.
  Mastodon-served copies compare against the source-side original (as before);
  re-encoded bytes land as `mismatch:200`, not a fetch error.
- Features media (`phase-features.py`): upload+post inline, verification
  deferred to `media_verify_all()` — global settle (`MEDIA_SETTLE_S=30`) +
  poll-to-deadline (`MEDIA_DEADLINE_S=240`, 60 s cap for unresolvable), same
  sha-match bar, window recorded in every detail. Env-overridable like S4.

## follows: exit 0 (790 s)

9180/9180 edges, 0 missing. Second consecutive clean run; the
Mastodon-first-contact 422 race did not reproduce again.

## features: exit 1 (1101 PASS / 69 FAIL, run3: 1069/101)

In-scope (bt/pt-involved): 51. Out-of-scope (third-party pairs): 18.

- `renote/boost` 0 FAIL (was 15): wrapper-URI fix confirmed across matrix.
  `reply` 0 FAIL.
- `media * -> bt-*` 0 FAIL: deadline loop proves arrival. Remaining 18 media
  FAILs all mastodon-involved, all classified: `pt/bt -> mastodon` 6x
  `no-attachment` (mastodon never fetched/resolved, honest settle30+dl240
  window); `misskey/catodon/akkoma -> mastodon` 3x `mismatch:404`
  (mastodon-side fetch failure); `mastodon -> *` 9x `mismatch:200`
  (re-encode; CA fix turned `exc` into 200s). In-scope media FAILs: 12.
- `poll+vote` 8 (was 11): `akkoma -> bt-*` fixed by the `closed` fix.
  Remaining: `pt -> akkoma` 3 (known akkoma search-backend gap) +
  `catodon -> {bt-a,bt-b,bt-c,misskey,mastodon}` 5, origin tallies zero.
  Pattern: catodon tallies pt/akkoma-voter votes but not bt/misskey/mastodon-
  voter votes -> catodon-side (drops valid remote votes or malformed poll for
  non-misskey voters). Needs catodon logs. In-scope: 6.
- `unboost` 26 (was 14): `* -> akkoma` 9 (akkoma never applies Undo, any
  booster); `misskey -> *` 9 incl. bt (misskey Undo never applies anywhere;
  bt rows newly exposed now boosts arrive); **`bt -> bt` 6 NEW** — boosts
  land but Undo-Announce never decrements the bt origin (previously masked:
  vacuous passes). Suspect birchtree inbound Undo(Announce) gap. Needs trace
  + bt origin logs. `akkoma -> bt-*` 3 ambiguous (akkoma never sends vs bt
  not applying; note `akkoma -> pt` unboost passes, pointing bt-side).
  In-scope: 21 (`misskey -> catodon/mastodon`, `catodon -> akkoma`,
  `mastodon -> akkoma` out of scope).
- `delete` 6 (was 8, scattered): all bt-dest (`misskey -> bt-b/c`,
  `catodon -> bt-b/c`, `mastodon -> bt-a/c`). Narrower than run3's scatter;
  re-check post-run for lag-vs-loss.
- `reaction akkoma -> {pt-a,pt-b,pt-c}` 3 + `mention-notif akkoma -> {bt-a,
  bt-b,bt-c}` 3: akkoma-outbound interactions never land (same family as
  akkoma-Undo). In-scope: 6 (`akkoma -> misskey/catodon/mastodon` out).
- Out-of-scope 18: unboost 5, media 6, poll 2 (`catodon -> misskey/mastodon`),
  reaction 2, mention 3. Same known causes, no action.

## TODO when the run finishes

- stress / concurrent / crossfamily / interop numbers. DONE (below).
- Traces + foreign logs: bt->bt unboost (product suspect), akkoma->bt unboost
  attribution, catodon remote-vote tallying, akkoma-outbound family,
  misskey Undo delivery confirmation. DONE (below).
- Re-checks: delete->bt (6); confirm still-red rows unchanged hours later.
  DONE (delete converged; see below).
- Phase 7 report from fresh-run numbers only; problems appended at 51;
  `bench-down.sh` + `ss`/`docker ps` verify; commits decision (none yet).

## stress (exit 1, 3045 s)

- S1 post storm: 3030 posts, 0 create failures. pt/bt nodes 960/960 by
  +180 s; misskey 882, catodon 960, akkoma 960 by +180 s; mastodon 746/776
  (mastodon-side slowness, same as run3).
- S2 reaction storm: 635 resolved / 215 unresolved_final (run3: 613/237),
  all `edge_present_not_visible`, recovered_after_settle=0, capability
  probes all 200. Pair table in `stress.out` (bt-c->misskey/catodon 33 each,
  akkoma->misskey 31, bt-a->pt-b 24, ...). Item 3 classification still open.
  `shortfall_n=2`, `residue_after_undo=1`.
- S3 poll storm: 50 polls, 442/455 votes ok (run3: 427/455), shortfall 5 —
  all catodon-origin (u1@catodon, `hasPoll: True` live), same as run3.
  `votes_not_implemented: 0`. 13 failed vote calls (`votes_failed=13`,
  run3: 28) still need per-call classification.
- S4 media burst: 159/202 verified (run3: 168/202). ALL 43 unverified are
  mastodon-involved: 21x `mismatch:404` (dst=mastodon, mastodon-side fetch
  failure) + 22x `mismatch:200` (src=mastodon re-encode). Zero non-mastodon
  failures — no bt-destination failure. `arrival_after_settle` max 12.1 s.
- S5 reply storm: 500 replies, 60/60 sampled ok.
- S6 churn: residue bt-a 4, bt-b 6, bt-c 6, catodon 1, akkoma 2 (run3:
  5/4/4/1/4). Zero-residue not met; per-pair attribution open.
- S7 big text: federation half (450 chars) 10/10 ok. Over-limit half (2900):
  bt-a/b/c + mastodon correctly 422, all others accept — 10/10 ok. The
  split-harness design works as intended.

## concurrent: exit 0 (37 s)

C1-C4 all 50/50 (posts p50 118 ms, reactions p50 70 ms, media uploads p50
479 ms, replies p50 139 ms).

## crossfamily: exit 1 (211 s, 162 checks, 9 FAIL fresh)

All 9: `media pt -> bt-*` "no attachment on dst". `reply bt -> pt` now
PASSES (item 41 fix verified). The 9 media FAILs are a HARNESS BUG, not
lag (problem 51): the check reads only Misskey-shape `files`/`attachments`,
but bt-side copies carry `media_attachments` — verified live (3/9 copies
dumped, all with 1 attachment; the check could never pass). Fixed post-run
(same correction as items 25/26); re-run output in
`crossfamily.rerun.out` (fresh record preserved in
`crossfamily.out.fresh` / `crossfamily.json.fresh`). Follow-on found by
re-run 1: the fixed check fired on placeholder rows (attachment row
present, `url` null until the async fetch completes) — `media_seen` now
requires a fetchable URL. Re-run 2 (fixed check): 162 checks, 2 failures,
both `delete` (pt-a->bt-a, pt-b->bt-b), both converged on re-check = lag.
Media 9/9 PASS with the corrected check — the federation was real in the
fresh run; only the check was wrong.

## interop (bt <-> real Mastodon 4.7.3): exit 1 (399 s, 22/23)

Only FAIL, identical to run3: `bt-a home has alice follow`
(mastodon->bt-a follow not visible in home timeline). Everything else
passes both directions.

## Post-run forensics (nodes up, suites complete)

- `delete` (6, all bt-dest): all markers gone from dst timelines hours
  later. Lag, not loss. Case closed.
- S3 shortfall: all 5 polls catodon-origin (u1). Same as run3: catodon
  drops valid remote votes (needs catodon logs).
- `unboost` full attribution (origin=src, booster=dst; count read on origin):
  - OUR BUG (problem 52): bt booster never sends Undo on wrapper delete —
    `delete_status` enqueues only a `delete` event, never `undo_announce`
    (unlike `unreblog_status`, which does). Explains `bt -> bt` (6).
    Proven live: bt-b boost counted on bt-a in 6 s, `/unreblog` decremented
    in 6 s; real Mastodon 4.7.3 DELETE-of-reblog decrements a bt origin in
    6 s (Undo sent), bt DELETE-of-reblog does not. Mastodon parity.
  - akkoma-side (never sends Undo on delete): all 9 `* -> akkoma` rows
    (boosters pt/bt/misskey/catodon/mastodon all fail; mastodon proven to
    send Undo, so the drop is akkoma's).
  - misskey-side (never applies incoming Undo): `misskey -> pt-*` (pt sends
    Undo(Announce) on renote delete — pinetree `delete_note`, pure-renote
    branch), `misskey -> catodon/mastodon`.
  - joint (either side suffices to keep red): `misskey -> bt-*` and
    `akkoma -> bt-*` (booster bt sends nothing AND the origin wouldn't
    apply it).
  - bt applies inbound Undo(Announce) from every sender (log: pt, misskey
    `notes/.../activity/undo`, catodon, akkoma `activities/uuid`,
    mastodon `#announces/.../undo`, all `outcome=applied`).
  - Pinetree sends Undo(Announce) on pure-renote delete (`was_announce_of_uri`
    branch) — which is why `akkoma -> pt-*` unboost passes while every
    `* -> akkoma` row fails.
- S2's 215 misses, S3's 13 failed vote calls, akkoma-outbound
  reactions/mentions, catodon tallying, S6 attribution, interop follow
  miss, 422/403 senders: unchanged/open (items 32/33, 45-50 carry over).
