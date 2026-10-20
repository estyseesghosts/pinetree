# Federation bench run3 log (2026-10-19): poll federation + full matrix

> **Status (2026-10-20 freshness pass)** — historical log; not rewritten.
> - **LANDED (pinetree):** poll `type: "Note"` options (`src/activitypub/objects.rs:1070`); inbound `EmojiReact` → Like handler (`src/federation/inbox.rs:346`); poll-vote drops labelled per reason (`src/application/commands/polls.rs:181-193`).
> - **OPEN:** poll-vote drop counters/fixtures; outbound `EmojiReact` degrade (`docs/TODOS.md` "Bench forensics").
> - **HARNESS-ONLY:** the two birchtree poll-federation bugs (fixed in birchtree). File paths cited here as `src/activitypub/{outgoing,wire,activities}.rs`, `src/federation/events.rs`, `src/application/federation_inbound.rs`, `src/persistence/redb/polls.rs` are **not** in pinetree at HEAD (checked 2026-10-20); they refer to birchtree-side code.

**Status: all suites finished.** `follows` 0 (785 s); `features` 1 (3215 s,
1069/101); `stress` 1 (3084 s); `concurrent` 0 (37 s); `crossfamily` 1
(379 s; 162 checks, 18 FAIL); `interop` 1 (403 s; 22/23 PASS).
Two birchtree product bugs were root-caused live after the run (both fixed,
tests added, rebuild pending). Cross-suite numbers below are final; `reply`
crossfamily FAILs are a proven harness bug (fixed post-run).

- Run: `/root/bench/run3`, suite dir `runs/20261019T021448Z`.
- Builds (from `/proc/<pid>/exe`, `bench-up.sh`): pinetree
  `f58be005c558f650` (unchanged, no pinetree Rust changes), birchtree
  `cc4421edd6130686` (poll federation, see below).
- Bench network: 10 nodes (3 pt, 3 bt, misskey, catodon, akkoma, mastodon);
  90/90 probes resolved on a pristine `bench-up` (a first `bench-up` + a
  tri-node poll check ran before it; the network was wiped and brought up
  again so the measured run has no probe residue).

## What changed before this run

### Birchtree poll federation (D29 reversed, unit-tested, needs lab proof)

- `src/activitypub/wire.rs`: `Question`/`QuestionOption`/`QuestionReplies`
  wire types (liberal `closed`/`endTime`/`replies` parsing), `name` on `Note`
  (vote detection), `Instrument::Question`, `NoteOrQuestion` fetch type.
- `src/activitypub/activities.rs`: `CreateObject::Question`; `Create` ingests
  a `Question` with its poll or tallies a vote-shaped `Note`
  (`name` + `inReplyTo`, empty content); `Update` handles `Some("Question")`
  (unknown status falls back to ingest, mirroring Mastodon).
- `src/application/federation_inbound.rs`: `RemotePoll`; ingest seeds origin
  tallies (`sync_remote_poll`); `vote` tallies only known **local** polls
  (exact option-name match, block + expiry + duplicate gates);
  `sync_question` overwrites tallies by title, ignores shape changes.
- `src/persistence/redb/polls.rs`: `sync_remote_poll` impl; `vote_poll` txn
  now enqueues `poll_vote` (voter-local only) + `question_update`
  (author-local only) federation events.
- `src/activitypub/outgoing.rs`: poll statuses render as `Question`
  (`oneOf`/`anyOf`, `endTime`/`closed`, `votersCount`); vote `Create(Note))`
  per Mastodon's `VoteSerializer`; `question_update` with a per-send id
  (`#updates/{now_ms}`) so redeliveries do not collapse in origin dedup.
- `src/federation/events.rs`: `poll_vote` (one `Create` per ballot choice, to
  the poll author's inbox) + `question_update` (to followers, skipped when
  `hide_totals`) branches; `PollExpired` handler redistributes `closed`.
- Tests: 10 new in `tests/phase6_content.rs`; full
  `cargo test --release` 338 passed / 0 failed; `cargo check` zero warnings.
- Docs: D29 reversed, D34 + Phase-6 exit line updated. No vendor changes.

### Harness corrections (checks that were wrong, not weakened)

- `lib.py` `poll_vote` for Mastodon-family sent singular `choice`; real
  Mastodon 4.7.3 answers that 400 (`param is missing ... : choices`,
  verified live against the bench mastodon). Now sends `{"choices": [c]}`.
- `phase-stress.py` `POLLS_NOT_FEDERATED` emptied (was `{bt-*}`).

### Tri-node pre-check (same binaries, wiped before the measured run)

`poll-check.py`: bt-a poll voted by pt-a + misskey users tallied `[1, 1]`;
pt-a poll voted by bt-a tallied `[0, 0, 1]`. ALL PASS both directions.

### Keystone UIs (outside the bench, read-only use recommended)

- nginx (dnf-installed; Fedora default `:80` server disabled — host :80
  belongs to the Akkoma proxy). Conf: `/etc/nginx/conf.d/keystone-bench.conf`.
- `http://127.0.0.1:3120/` → pt-a, `http://127.0.0.1:8180/` → bt-a, each with
  its own `dist/` + first-party `keystone.config.json`
  (`/root/bench/run3/keystone-{pt-a,bt-a}/`).
- The shipped `dist/` was a stale Misskey-only build (no OAuth/`api/v1`
  strings); rebuilt from current source (`vite build`) and redeployed.
  bt-a origin detects as Mastodon family via probe-shape (NodeInfo name is
  `birchtree`, `/api/meta` 404s, `/api/v2/instance` is Mastodon-shaped).
  Login on bt-a needs a hard refresh if the old bundle is cached.
- Residue: one pt-a signin token (u0), one `keystone-probe` OAuth app on bt-a.
  No posts/follows/votes touched.

## Final FAIL inventory (`features`, 101 of 1170 unique checks)

Goal rows: `reply` is 0 FAIL. `renote/boost` (15), `unboost` (14), `media`
(35) and `poll+vote` (11) are not. Grouping by suspected cause; all
unclassified until trace/log forensics post-run.

1. **`renote/boost * -> bt-*` (15: 6 bt->bt, 3 misskey->bt, 3 catodon->bt,
   3 akkoma->bt; `count=0`).** Pattern: boosts created on birchtree register
   on pt origins (all PASS) but never on a bt, misskey, catodon or akkoma
   origin. bt-a's log shows **zero inbound Announces from bt-b/bt-c** out of
   ~2800 inbound activities, and bt-b's log shows **no Announce delivery**
   to bt-a's inbox (only Create/Update/Delete). The announce event is not
   producing deliveries; ~140 quarantined jobs per bt node is the lead.
   Quarantine reason not yet extracted (worker log lines carry no reason).
   TODO: map quarantined job payloads, BENCH_TRACE timing, receipt side on
   misskey/catodon/akkoma.
2. **`media * -> bt-*` (27, all 9 sources x 3 bt dsts; `url=None`).**
   Destination note has no attachment at all. bt outbound media passes
   everywhere, so this points at birchtree's inbound remote-media fetch.
   TODO: check FetchRemoteMedia job outcomes + destination media rows.
3. **`media * -> mastodon` (9) and `media mastodon -> *` (9).** Attachment
   URLs are present on the mastodon side; the FAIL is the harness fetching
   `https://...` without the lab CA. Harness fix pending: lab CA in
   `fetch_bytes`. Not a federation failure (verify post-fix).
4. **`unboost * -> akkoma` (9, every origin) and `unboost misskey -> *`
   (5, every non-bt booster; stuck at `count=1`).** Two leads: (a) akkoma
   never sends (or nobody honors) its Undo-Announce; (b) a misskey origin
   never applies Undo-Announce (5 booster implementations). The
   `unboost ... -> bt` PASSes are vacuous (boost never arrived, count stayed
   0). TODO: trace + misskey/akkoma logs.
5. **`poll+vote`: akkoma-involved (3 pt->akkoma "unresolvable on dst",
   3 akkoma->bt-*) and catodon-origin (5).** pt->akkoma is the known search-
   backend gap. The akkoma->bt and catodon->bt cases: destination resolved,
   but the origin tallied zero hours later. CONFIRMED PRODUCT BUG
   (birchtree): `Question::closed_flag()` treated any string `closed` as
   "already ended", but Akkoma (and Sharkey-family catodon) always send
   `closed` carrying the end *time*. Every akkoma/catodon poll ingested as
   sticky-expired, so local votes 422'd and no vote was ever delivered back.
   Live proof post-run: fresh akkoma poll voted from bt-a answered 422
   `The poll has already ended` minutes after creation. Fix: `closed`
   means ended only when its time is past (`closed_flag(now_ms)`);
   regression test added
   (`a_question_with_a_future_closed_time_is_not_expired`).
6. **`reaction akkoma -> *` (5: pt-a/b/c, misskey, catodon) and
   `mention-notif akkoma -> *` (6: +bt-a/b/c, mastodon).** Akkoma-outbound
   interactions never land. Same family as 4(a); TODO post-run.
7. **`delete` (8: misskey->bt-a/bt-b/akkoma, mastodon->bt-a/b/c,
   akkoma->mastodon/bt-c).** Scattered; the same op passes on identical
   software (e.g. misskey->bt-c PASS vs bt-a/b FAIL) -- smells like lag.
   TODO: re-check counts post-run, trace timing.
8. **Parse 422s (~31/node, pinetree `Create` + `QuoteRequest`) and 403
   domain-mismatches (~130-140/node).** Problems-log items 32/33, still
   unclassified. Quotes pass only via link-fallback
   (`origin_sees_relation=False`), so the QuoteRequest parse failure is real;
   prime suspect is the new untagged `Instrument`/`Question` paths vs
   pinetree's exact shape. TODO: reproduce post-run with captured bytes.
9. **`announce without a boost row`: 0 on all bt nodes** (item 34 not
   reproducing).

## Mastodon / misskey cross-checks (from the same partial run)

- Mastodon as destination: everything PASS except media (see 3) and one
  `unboost misskey -> mastodon` (see 4). Mastodon as source: not yet run.
- Misskey as destination: 100% PASS (all sources, incl. media + poll+vote).
  Misskey as source: failures are exactly groups 1, 2, 4, 5, 7 above.

## TODO when the run finishes

- Classify every group with BENCH_TRACE + inbox/delivery logs; extract
  quarantine reasons (store-level if needed).
- Reproduce the QuoteRequest/Announce-vs-bt receipt bugs out of band.
- `fetch_bytes` lab-CA fix; catodon poll-param check.
- Phase 7 report from the fresh run only; append new problems to
  `docs/federation-bench-2026-10-18-session2-problems.md`; `bench-down.sh`
  + `ss`/`docker ps` verify; stop nginx; decide on commits (none yet).

## Post-run live verification (nodes still up, suites complete)

Both fixes verified end to end on the run3 network after restarting bt-a/b/c
on the fixed binary (build `781d5b6c189f6382`; pt nodes untouched):

- Akkoma poll vote: fresh akkoma poll resolved on bt-a, REST vote 200 with
  local tally 1, akkoma origin tallied `x: 1` within 15 s. (Previously 422
  `already ended` minutes after creation.)
- bt boost: fresh bt-b boost of a bt-a post counted on the bt-a origin
  within 6 s. bt-a logged the Announce (`activity=http://bt-b.lab/...`,
  `outcome=applied`); the activity id is now booster-domain.

## Re-checks of the 101 features FAILs (hours later, read-only)

- `delete` (8): all destination copies GONE. Lag, not loss. Case closed.
- `media * -> bt-*`: bt→bt, misskey→bt, catodon→bt, akkoma→bt, mastodon→bt
  copies all show attachments now (each 1). pt→bt copies could not be
  re-found (author-timeline scan window too small), but S4 verified pt→bt
  pairs with hash match during the run. Verdict: async-fetch lag, not a
  product bug; the features 90 s no-deadline check is the wrong window
  (S4's settle-until-deadline pattern is the honest one).
- `renote/boost` (booster on bt) and `unboost` (misskey/akkoma origins):
  origin counts unchanged hours later (still 0 / still stuck). Real loss,
  not lag. The renote half is the wrapper-URI bug above (now fixed); the
  unboost half (Undo not applied on misskey/akkoma origins) is still open.
- `poll+vote` catodon/akkoma-origin: origins still show all-zero tallies.
  bt-side cause (sticky-expiry 422, no delivery) confirmed and fixed; whether
  catodon also drops valid remote votes is still open (needs catodon logs).
