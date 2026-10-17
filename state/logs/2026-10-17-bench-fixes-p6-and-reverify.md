# 2026-10-17 — P6 poll votes + live re-verification of P1/P4/P5 fixes

Follows `2026-10-17-bench-fixes-p1.md` and `-p2-p3.md`.

## P6 — root cause found (partly) and fixed
- Instrumentation: every silent drop in the inbound vote path now logs
  `[inbound-vote] dropped reason=… activity=… note=…` (`vote_dropped` in
  `application/commands/polls.rs`); `handle_create` logs `applied`, `not-a-poll`,
  `parent-not-cached`, `answer-without-cached-poll`.
- Real gap found while reading payload shapes: Pleroma/Akkoma send a vote as
  `Create(Answer{name,inReplyTo})`; `parse_remote_note` rejected type `Answer`, so the vote
  vanished silently. Now accepted (`activitypub/objects.rs`); an Answer that does not resolve
  to a cached poll is dropped, never stored as a note (`is_answer_object`, `inbox.rs`).
- Tests (`inbound_vote_tests` in `inbox.rs`): Note-shaped vote tallied; Answer-shaped vote
  tallied (fails with `[0,0]` when the parser change is reverted); Answer for unknown parent
  stores nothing.
- The original "0 votes at +2 min under load" did NOT reproduce on the new build (below), so
  the load-sensitive cause is attributed to the P1/full-scan fix by evidence of absence only.
  Not proven. The instrumentation will show the branch if it recurs.

## Live re-verification (rig; pt-ks/pt-mk restarted on the new binary, data kept)
Full `phase-stress.py` (tag S254685), 900-post storm included:
- P1: 960/960 convergence at +60 s; home timeline of pt-ks/pt-mk (1350 items) has duplicate
  uris, but all 14 predate the restart (latest 16:19Z < 16:24:56Z). Zero created post-fix.
- P6 votes: tallies per origin vs attempts (Akkoma voters excluded — rig search backend cannot
  resolve their notes): pt-ks 19/19, pt-mk 17/17, misskey 20/20, catodon 11/19 (upstream).
  Sequential/concurrent vote rounds (`scripts/fediverse-bench/p6-votes.py`): 37/37 within ~1 s,
  no drop lines in either pinetree log.
- P5: eventual media linking 12/12 in each of 4 directions; sample latency from resolve to files
  0.5 s ×4. Harness S4 "no-attachment" is its immediate check racing the async job.
- P4/S5: replies 60/60 parent linkage under 266-reply storm.
- Harness issues seen (not product): S2 counts 204 as failure; S3 counts only 200 (votes_ok=0
  though tallies are correct); S4 checks attachments instantly.
- `cargo test`: lib 394, conformance 86, all suites green. `scripts/smoke-all.sh` NOT run.

## Open
- Catodon voter tally shortfall (11/19) and Akkoma-unresolvable rig issue: upstream/rig.
- Existing duplicate-uri rows from earlier runs remain (shadowed in the uri index).
- Fix the three harness counters above before the next bench.
