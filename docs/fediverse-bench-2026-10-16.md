# Fediverse compatibility bench — 2026-10-16

**Scope**: pinetree vs Misskey, Catodon, and Akkoma in a five-node local matrix,
stress-tested with 30 accounts and ~4 500 federated objects. Run by the bench
session described in `state/logs/2026-10-16-fediverse-bench.md`. Harness:
`scripts/fediverse-bench/` (copies of the exact scripts; original working dir
`/tmp/opencode/bench/`, raw data `accounts.json`, `features.json`, `stress*.json`,
`probes.json` + run logs).

## 1. Topology and versions

| Node | Software | Backend | You browse to | Federation URL |
|---|---|---|---|---|
| pt-ks | pinetree (master @ e72c946, debug build) | `:3000` | Keystone vite `localhost:5173` | `http://pt-ks.test` |
| pt-mk | pinetree (same build) | `:3103` | Misskey UI via mk-proxy `localhost:8182` | `http://pt-mk.test` |
| misskey | Misskey `2026.10.1-alpha.0` (stock) | `:3101` | Keystone vite `localhost:5176` | `http://misskey.test` |
| catodon | Catodon `26.10.1-dev.0` (Misskey fork) | `:3210` | own UI `catodon.test` | `http://catodon.test` |
| akkoma | Akkoma `3.20.1-454-ga69f0b9e-develop` | docker `:4000` | pleroma-fe `localhost:8099` | `http://akkoma.test` |

All federation on port 80 via Host-routing nginx (docker `local-federation`
rig, extended with the four bench hostnames). Windows hosts entries point the
`.test` names at the WSL guest so browser-side media resolves. Akkoma's
client-facing rate limits were lifted (`config/prod.secret.exs` bench
override); Misskey/Catodon/pinetree defaults were kept. Accounts: 6 per node,
all `password123` (`pk1..6`, `pm1..6`, `mk1..6` (mk1/cd1 admin), `cd1..6`,
`ak1..6@akkoma.test`).

## 2. Methodology

1. **Mesh**: 30 accounts × 29 = 870 directed follows, every edge verified via
   follower lists on the followee's home server (`FOLLOWS_OK total=720 errors=0`
   on the idempotent re-run; 870 edges total verified).
2. **Probe matrix**: 30 probe posts, each force-dereferenced on all 4 remote
   instances (120 checks) + home-feed census.
3. **Feature matrix**: 11 features × 20 ordered node pairs = 260 checks,
   sequential (a first run overlapped the probe matrix and produced
   timeline-flood artifacts; discarded and re-run clean).
4. **Stress**: 3 independent 900-post storms (one per run) with convergence
   sweeps at +15/+60/+180s and latency sampling; 205-reaction cross-node storm
   with counts; 15 polls + 72 remote votes; 85 media uploads byte-verified;
   240-reply storm with parent-linkage sampling; post/delete churn; reaction
   RTT timing; 2 900-char posts from every node.

**Caveats**: single machine; pinetree is a debug build; Misskey-family notes
creation limit 300/h/user was respected by rotating 30 accounts; several
apparent failures were traced to harness bugs (§7) — findings in §4 are what
survived raw-wire diagnostics.

## 3. Headline metrics

- **Post storm**: 3 × 900 posts, **0 create failures**, wall 32–37 s each
  (~26 posts/s across the matrix). Origin create latency p50 180–250 ms,
  p95 ~1.0–1.2 s, p99 2.3–3.5 s.
- **Convergence** (batches 2 and 3): all five nodes held **720/720 remote
  markers within +15 s** (identical at +60/+180 s). Batch 1 (cold start):
  Misskey lagged badly on inbound (catodon 0/180 at +15 s, still 0 at +180 s in
  the window, later confirmed delivered; window is trimmed by its smaller
  home-timeline cache) — lag, not loss.
- **Reactions**: 205/205 cross-node reactions landed (S2r; per-target residue
  counts 2–8 match attempts exactly).
- **Polls/votes**: tallies after the vote storm — pt-mk 6, misskey 6, akkoma 8,
  catodon 1; pt-ks's own poll read 0 votes at +2 min (see P6).
- **Media**: S4b 25/25 uploads byte-verified on remote nodes
  (sha256-16 match), attachment present ≈ immediately at low load.
- **Replies**: 240 posted; 60/60 sampled carried correct parent linkage
  verified at a foreign node.
- **Reaction RTT** (pt-mk user → pt-ks note): p50 **1.02 s**, max 3.02 s
  for react-or-undo → visible at origin.
- **Big text**: 5/5 nodes received 2 900-char posts intact.
- **Reachability**: 870/870 follow edges; probes 96/120 (all 24 failures are
  `→ akkoma` *search-API lookups* — this Akkoma build has no search backend,
  `/api/v1/search` = 404 "Not implemented"; home-feed census proves delivery
  into Akkoma works: every node's feed carried all four remote sources).

## 4. Pinetree bugs and gaps

Ordered by user-visible impact. All are reproducible with the harness in
`scripts/fediverse-bench/`.

### P1 — Duplicate note rows on fan-in (same AP `uri`, two local ids)
Under multi-source bursts (the 900-post storm), pinetree stores the same
remote note twice. Evidence: pt-ks home sweep found 11 duplicated keys
(pt-mk: 9, akkoma: 3), e.g. uri `http://akkoma.test/objects/8f428057-…`
stored as local ids `asf927pcignk9ann` + `asf927oqignk9ani` (text
`FTR|media|akkoma|pt-ks|198058`), and `http://pt-mk.test/notes/asf8u3nyeqkybsx0`
twice. Users see the same post twice in their timeline; reaction/undo counts
can double-count. Symptom persisted across independent notes; batch-2/3
sweeps showed 0 dupes but those windows were filled by fresh single-source
batches — P1 fires under concurrent multi-source fan-in. **Likely cause**:
inbox dedup keyed by local id / race between the on-demand `ap/show` insert
and the inbox insert, not keyed by AP `uri`.

### P2 — `notes/reactions` (list reactors) not implemented
`POST notes/reactions` on both pinetree nodes answers
`404 UNKNOWN_API_ENDPOINT` (verbatim: `{"code":"UNKNOWN_API_ENDPOINT","id":"2ca3b769-b6d7-4b65-96c9-302f3620a591",…}`).
Documented as an intentional profile gap (`docs/wiki/Current-State.md` §API
subset) but it is Misskey-API-visible: any Misskey client's reaction-list UI
(e.g. Misskey's own note modal "reaction list" tab) 404s against pinetree.
Reaction *creation* and the per-note `reactions` map work.

### P3 — Reaction undo (`Undo(Like)`) fails to converge under load
Feature matrix: every misskey-family→any pair left the reaction standing at
origin for ≥120 s after `notes/reactions/delete` — **including stock
Misskey↔Catodon in both directions** (so this is not purely a pinetree bug;
either an upstream 26.10-era regression pinetree inherits, or a shared
load-sensitivity). Counter-example: at low load, pinetree↔pinetree undo
converges p50 1.02 s (S6, 20 cycles). Akkoma unfavourites undo correctly in
all 4 directions. At pinetree origins the residue may be compounded by P1
(a duplicate row survives removing one). Needs an isolated
upstream-Misskey↔upstream-Catodon control run to split blame.

### P4 — `replyId` lost at pinetree origin for remote replies (load-sensitive)
Feature matrix: 8 pairs where the parent lives on pinetree (or when replies
are authored by Misskey/Akkoma toward a pinetree parent) showed the reply
arriving *without* parent linkage (`rid=None` at `ap/show` after 120 s).
Contradicting data: identical flows passed in isolated diagnostics (replyId
present at t+15 s) and under the 240-reply storm (60/60 correct). Pattern
fits a race between pinetree's on-demand `ap/show` render (returns the note
before the inbox has linked its parent, possibly caching that snapshot) and
inbox parent-linking. Repro: pair `pt-ks→pt-mk` in `features.json`
(`R=asf8rkwreqkybsmf`), diag3 §2 for the green path.

### P5 — Remote-media attachment linking is slow under load
Receiving copies expose `files: []` for >90 s under concurrent load (features
`media` rows: all `D ∈ {pt-ks, pt-mk}` "no attachment on dst copy"), while
isolated checks show attachments instantly (S4b 25/25, p50 ≈ 0 s, byte-equal).
Eventually-consistent, but a client rendering a note in the first minutes
after delivery shows no image. Same symptom class on Akkoma's copies
(upstream side).

### P6 — Poll-vote tallying at pinetree origin is delayed/absent under load
During the vote storm, pt-ks's own poll read 0 votes at +2 min while
pt-mk/misskey/akkoma/catodon tallied correctly; the feature matrix (run 2)
had votes toward pt-ks landing within 150 s. So remote votes reach pinetree
eventually or under light load — the tally is not durable within minutes
under load. Worth checking the inbound `Vote`/`Update(Question)` processing
queue path.

## 5. Cross-software findings (not pinetree bugs, recorded for the lab)

- **U1** — Akkoma favourites of remote notes never appear at Misskey-family
  origins (4/4 pairs, incl. stock Misskey). Akkoma↔Akkoma favs work (rig's
  own test). Either Akkoma outbound `Like` to Misskey-family or their inbound
  acceptance — bisection needed.
- **U2** — Akkoma-authored mentions produce **no notifications** on any
  Misskey-family server (4/4); the reverse direction works everywhere.
- **U3** — Misskey's inbound pipeline lags badly under a cold 900-post burst
  (catodon 0/180 in the +15…+180 s window, delivered later; its home-timeline
  window is also smaller than the others').
- **U4** — Akkoma keeps deleted notes visible in home timelines
  (status fetch returns `deleted: true` but the item stays listed).
- **U5** (rig) — this Akkoma build has no search backend: `/api/v1/search` =
  404 "Not implemented"; Akkoma note-URI lookups must use timeline scans.
- **U6** (rig) — Catodon gates *signin and signup* behind ALTCHA captcha by
  default (`meta.enableAltcha=true`); Misskey 2026 ships with closed
  registration (`disableRegistration: true`).

## 6. Verified green (all 20 ordered pairs unless noted)

Posts (2 700+ across storms, 0 losses after lag windows), content warnings,
followers-only visibility + no leak into global/public timelines, delete
propagation (direct-fetch gone on all receivers), quote posts — native
Misskey renote-quotes **and** Akkoma link-quotes, mention notifications
between all Misskey-family servers, full 870-edge follow mesh, media bytes
(S4b 25/25 sha-verified; features media green to misskey/catodon),
reply linkage under storm load (60/60), 2 900-char posts (5/5), pinetree
outbound `inReplyTo` canonical (D2a), pinetree mentions carry proper AP
`Mention` tags, boost/renote convergence ≤10 s (misskey↔pinetree, both
directions), poll creation shape parity (pinetree = Misskey = Catodon).

## 7. Appendix — failures dismissed as harness bugs

For future readers (each was confirmed against raw wire data before
dismissal): reading Misskey 2026 reactions via `reaction` instead of `type`;
calling `notifications` instead of `i/notifications`; assuming a top-level
`poll.votes` array (it's `choices[].votes`; Akkoma votes need the poll's own
id, not the status id); pinetree answering `204` (not `200`) on
`notes/reactions/create` and `notes/polls/vote`; searching own posts in the
own home timeline (use `users/notes`); own-posts absence from local timelines
during target discovery; running the probe matrix concurrently with the
feature matrix (timeline floods + rate limits produced phantom
followers-only/reply/renote failures); the S2 tag bookkeeping (SKIP_S1
rewrote `stress.tag`); Akkoma HTML-wrapped content vs exact-match checks;
media single-shot checks racing eventual linking; `users/followers`
relation-shaped packs (that matches upstream Misskey).
