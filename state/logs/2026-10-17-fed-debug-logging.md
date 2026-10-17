# 2026-10-17 — federation debug logging, notification order fix, bench paused

Follows `2026-10-17-bench-fixes-p6-and-reverify.md`. Uncommitted (per request).

## Why
The fediverse stress bench could not show whether pinetree accepted, rejected, or
delivered a given activity. The only inbound log was `[inbound-vote]`, and outbound
delivery logged nothing. Debug logging for accept/reject and delivery was added so
the next bench can read those outcomes from the server logs directly.

## Changed

### Debug logging (new)
- `src/observability.rs`: process-wide switch (`AtomicBool`, off by default), the
  `fed_debug!` macro (`#[macro_export]`, arguments not evaluated when off), and
  `env_enables_federation_debug` (`1|true|yes|on`, case-insensitive). Lines look like
  `[fed-debug] ts=<unix-ms> <in|out> <event> key=value…`. Untrusted strings use `{:?}`.
- `src/main.rs`: `--debug-federation` flag or `PINETREE_DEBUG_FEDERATION=1` env var
  (either enables). Prints `federation debug logging on (source: …)` at startup.
- `src/federation/inbox.rs` (inbound):
  - `in recv` (path, bytes) on every POST to `/inbox` and `/users/{id}/inbox`
  - reject reasons: `payload-too-large`, `bad-json`, `missing-type`, `missing-id`,
    `personal-inbox-not-local`, `no-signature-header`, `signature-header-unparseable`,
    `key-unresolvable`, `signature-invalid` (after one key refresh), `missing-actor`,
    `actor-not-signer`, `object-authority`, `author-not-signer` (Create)
  - `signature-retry` (cached key failed, refreshing)
  - Create outcomes: `applied note=…`, `drop reason=already-cached-uri`,
    `replay-already-recorded`, `empty-object-uri`, `object-fetch-failed err=…`,
    `object-unparseable`, `object-missing`
  - `in ignore reason=unsupported-type`
  - `in done type id actor status=…` for every dispatched activity (status 202 means
    the handler accepted it; it does not prove it changed state)
- `src/federation/outbox.rs` (outbound):
  - `out fanout type recipients split`
  - `out send type inbox status ms outcome` (outcome = `Delivered`, `TransientFailure`,
    `PermanentFailure`, with the reason in `outcome=`), plus `error=transient|permanent`
    with the send error kind and message
  - `out defer … reason=destination-cooldown`
  - `out skip note reason=note-missing|not-eligible`
- `src/jobs/worker.rs`: `out job key outcome=…` (the disposition the worker acts on) and
  `out job … quarantine reason=bad-payload`.

Not covered yet: drops inside other handlers (Follow/Accept/Like/Announce/Update/Delete
return 202 without a reason line), and the per-note dedup branch of `ingest_remote_note`
(only the `Ok(None)` result is logged). See the handoff.

### Notification order bug (real pinetree bug, found by the bench)
- `i/notifications` returned rows grouped by kind (reply, renote, reaction, mention,
  follow), newest-first only within a kind. Cause: the per-user index was keyed by the
  full notification id `notify:{kind}:{aidx}`, which starts with the kind.
- Fix (`src/persistence/redb/mod.rs`): index `notifications_by_user_ordered` keyed by
  `(notifiee, aidx suffix)`. `sinceId`/`untilId` compare on the suffix. Existing rows are
  backfilled once at open (META `migration:notifications_by_user_ordered:v1`).
- Test `n1_notifications_newest_first_across_kinds` (tests/conformance.rs, Group N).
  It failed on the pre-fix code with grouped output (`reply, reply, reaction, mention,
  mention, follow`) and passes after.
- Contract recorded in `docs/persistence-conformance.md` §16 (Draft).

## Verified (executed)

`cargo build --offline`: clean (pre-existing unused-import warnings only).

`cargo test --offline`: all green. lib 396 (includes `env_value_parsing`,
`switch_gates_the_macro`), conformance 87 (includes `n1_…`), client_profile 1,
client_smoke 1, crash_recovery 2, federation 2, federation_elixir 2,
federation_interop 1, minrepro 1, quote_fixtures 5, streaming 13.

Live, with pt-ks started with `--debug-federation` and pt-mk with
`PINETREE_DEBUG_FEDERATION=1` (restart at 19:11:53Z):

```
federation debug logging on (source: --debug-federation)            # pt-ks
federation debug logging on (source: PINETREE_DEBUG_FEDERATION)     # pt-mk
[fed-debug] ts=1792264321799 in  recv path=/inbox bytes=1862
[fed-debug] ts=1792264321800 in  applied type=Create id="http://misskey.test/notes/asgc7qp1fcp903j2/activity" uri="http://misskey.test/notes/asgc7qp1fcp903j2" note=asgc7qq089m520ue
[fed-debug] ts=1792264321800 in  done type=Create id="http://misskey.test/notes/asgc7qp1fcp903j2/activity" actor="http://misskey.test/users/asf64vjyfcp90003" status=202
[fed-debug] ts=1792264321801 out fanout type=Create recipients=4 split=false
[fed-debug] ts=1792264321805 out send type=Create inbox="http://pt-mk.test/inbox" status=202 ms=3 outcome=Delivered
[fed-debug] ts=1792264321809 out send type=Create inbox="http://misskey.test/inbox" status=202 ms=3 outcome=Delivered
[fed-debug] ts=1792264321814 out send type=Create inbox="http://catodon.test/inbox" status=202 ms=4 outcome=Delivered
[fed-debug] ts=1792264321830 out send type=Create inbox="http://akkoma.test/inbox" status=200 ms=16 outcome=Delivered
[fed-debug] ts=1792264321830 out job key="federate:note:asf64jxtignk96d8:asgc7qpb89m520u9" outcome=Delivered
[fed-debug] ts=1792264325803 in  recv path=/inbox bytes=216
[fed-debug] ts=1792264325805 in  reject reason=key-unresolvable keyId="http://nowhere.invalid/users/x#main-key"
[fed-debug] ts=1792264325805 in  reject reason=signature type=Create id="http://nowhere.invalid/a1"
[fed-debug] ts=1792264325821 in  recv path=/inbox bytes=1820
[fed-debug] ts=1792264325822 in  done type=Like id="http://pt-mk.test/likes/asgc7tsnhnq1idzh" actor="http://pt-mk.test/users/asf64q7eeqkybp4c" status=202
```
(pt-mk's log shows the same Create as `applied` from its side, plus an outbound
`out send type=Like inbox="http://pt-ks.test/inbox" status=202 ms=3 outcome=Delivered`.)

Default-off check: a throwaway instance (no flag, `env -u PINETREE_DEBUG_FEDERATION`,
port 3999) answered the same bad-signature POST with `401`, wrote 0 `[fed-debug]`
lines, and printed no startup line.

Notification order, live (pt-ks, user pk1, restarted on the new binary):
- Before the fix: all replies, then renotes, reactions, mentions, follows.
- After the migration: 82 notifications kept (count unchanged); order
  `MMMrxxMMbbxrxxxx…`, newest-first by suffix (`ids sorted by suffix desc? True`).
- Fresh mentions to pt-ks visible after 3.1 s on every source
  (`state/logs/` file: `/tmp/opencode/bench-rerun/logs/mention-latency-after-fix.txt`).
  Before the fix the same mentions never appeared within 240 s
  (`…/mention-latency.txt`).

## Bench state (stopped by request)

- Run 1 (complete): `/tmp/opencode/bench-rerun/logs/analyze.txt`; raw outputs backed up
  at `/tmp/opencode/bench-run1-2026-10-17/`. Earlier, pre-restart results are in
  `/tmp/opencode/bench-prev-2026-10-17/`.
- Run 2 (partial, killed at ~19:0x UTC): feature matrix stopped at 152 of 260 checks
  (144 PASS, 8 FAIL, all `unboost`). S4 media did not run.
- Harness fixes live only in scratch copies (`/tmp/opencode/bench-rerun/scripts/`):
  - `phase-features.py`: boost, unboost and reply waits no longer stop on their first
    poll (same bug as P3).
  - `s4_fixed.py`: dict-vs-id bug in `note()`, constant destination selection, and a
    60 s eventual attachment check.
  - `phase-stress.py` (`patch_stress.py`): S2 and S3 accept 204, S1 dupes counted by
    `createdAt` after restart, S3 per-origin tallies.
  - The repo copies under `scripts/fediverse-bench/` are unchanged.

## Not verified / open

- 8 `unboost` failures in run 2 (`out=None`, i.e. the count never dropped in 120 s;
  pairs pt-ks→akkoma, misskey→pt-ks, pt-mk→akkoma, misskey→pt-mk among them). Not
  investigated. Run 1 passed these pairs.
- S2 reactions: 3 targets short by 1, and 6 targets keep 3–4 reactions after undo.
  Not investigated.
- S6 churn: one residual deleted note on each node, pinetree included (120 s wait).
  Not investigated.
- Probe matrix: 0 failures this run, where it used to show 24 Akkoma-search failures.
  Not explained.
- Feature matrix run 1 had 55 failing pairs. Most were harness bugs (boost, reply) or
  Akkoma. Mentions into pinetree were the real pinetree bug (fixed above).
- `scripts/smoke-all.sh`: NOT run.
- Logging limits: no request correlation ID; log volume scales with traffic; the
  per-note dedup branch in `ingest_remote_note` only logs `already-cached-uri`.
- The notification index fix must be carried into the Phase 4 FDB index (see
  `docs/persistence-conformance.md` §16).
- Operational note: the first `pgrep -f` restart attempt matched the shell running it
  and killed it (pinetree was left down until restarted by PID). Use an anchored
  `ps -eo pid,args | grep -E "^ *[0-9]+ /root/…"` match.
