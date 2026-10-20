# Federation bench, session 2 (2026-10-18): every problem hit

> **Status (2026-10-20 freshness pass)** — problem log, not rewritten. Items below are classified against HEAD.
> - **LANDED (pinetree):** `notes/reactions` list (item on "missing reactions list"; `src/api/misskey/endpoints/mod.rs:6278`); poll `type: "Note"` (`src/activitypub/objects.rs:1070`); `NOTES_BY_URI` uniqueness (`src/persistence/redb/mod.rs:1129-1139`); Delete tombstone (`src/federation/inbox.rs:1740-1748`); inbound `EmojiReact` (`src/federation/inbox.rs:346`).
> - **OPEN:** quote/reply debt; outbound `EmojiReact` degrade; poll-vote drop counters (`docs/TODOS.md` "Bench forensics").
> - **HARNESS-ONLY:** birchtree product bugs and harness bugs (items 24–28 are harness bugs; items 31–37 were open in birchtree). `src/persistence/redb/statuses.rs` and `tests/phase6_*.rs` cited here are not pinetree paths; `docs/compatibility-profile.md` and `docs/phase-6-exit-criteria.md` cited here do not exist in `docs/`.

This is the log of what went wrong while building a clean, fresh Phase 6 run from
`/root/handoff-federation-bench-fresh-run.md`. It records problems, not results. No number in
this file is a benchmark result. Run directory: `/root/bench/run1` (suites aborted before
completion; see "Run state"). Nothing is committed.

## Run state when this was written

- Two full attempts were aborted and archived (`run-suites.aborted-run{0..3}.out`, `runs.aborted-run{0..2}`).
- The last attempt (`/root/bench/run1`, started 23:14:48 by the runner's clock) ran `follows` to completion
  (exit 1, 1405 s, 1 of 9180 edges missing) and was stopped part-way through `features` to fix the harness.
  Its `features.out` (587 PASS, 160 FAIL) predates the harness fixes below and is **not valid** for the
  boost, unboost, reply and media rows.
- Build IDs of that attempt (sha256 prefix of the running executable): pinetree `f58be005c558f650`,
  birchtree `481ad5d047821623`. Earlier attempts used pinetree `a4c11ad8aabd3da7`, `6f2d552c47397fa3`;
  birchtree `97113e482459a6a7`, `8133109b683bfb30`, `fad381130b6d6ddd`. `fad381130b6d6ddd` was running while the
  binary on disk was rebuilt to `481ad5d047821623` (see problem 27).

## A. Environment and process problems

1. **Pid-file confusion.** `kill $(cat run-suites.pid)` hit the wrong PID, and killing a child made the
   runner advance to the next suite. Kill the runner (SIGKILL) first, then its children. `run-suites.sh`
   now writes its own PID; `bench-down.sh` stops it by that.
2. **A hand-started bt-b (PID 312671) survived two resets and held :8081.** The new bt-b failed to bind, and
   the probe and follow checks ran against stale data for a while. `start-nodes.sh` now refuses to start
   over an occupied port and checks that each listener PID equals its pid file.
3. **`sed`-based path substitution broke helper scripts** (`bootstrap.py`, `stress-fixup.py`: parentheses,
   imports). Fixed by hand; those scripts are obsolete (see 23) and moved to `legacy/`.
4. **`openssl` is not installed on the host.** The bench certificate is signed with the Mastodon image's openssl.
5. **A `docker run` against 172.28.0.10 from the default bridge hung.** The lab network is not reachable from
   there. Use `docker exec` and timeouts.
6. **`start-misskey-family.sh start | tail` hung for 10 minutes.** The backgrounded node inherited the pipe.
   The script now detaches stdin/stdout. Never pipe the start scripts into `tail`.
7. **Foreground `sleep` is blocked in this harness**; waits use `until` loops in background commands.
8. **`docker exec` needs `-i` for stdin.** The first CA append into Akkoma wrote nothing because of this.
9. **`/etc/hosts` inside containers is a bind mount** (no `sed -i`), and `--add-host` cannot change on a running
   container. `lab-net.sh` rewrites the file in place.
10. **The Misskey/Catodon `url` and the harness `HOSTS` must agree** (`http://misskey.test`, no port).

## B. Network and TLS problems (all were environment gaps, not federation bugs)

11. **Mastodon does WebFinger over HTTPS only and drops the remote domain's port.** `host:port` identities for
    birchtree (`bt-a.lab:8080`) can never work. Birchtree nodes now use standard-port identities
    (`http://bt-X.lab`), and `bt-proxy-pt` (nginx, 172.28.0.30) answers TLS and plain HTTP for every bench
    name, signed by the lab CA.
12. **Misskey/Catodon ignore `/etc/hosts`.** Their `CacheableLookup` runs with `lookup: false`, so
    resolution failed with `ENOTFOUND` / `REQUEST_FAILED`. Fixed with `node-hosts-preload.cjs`, which patches
    `dns.promises.Resolver#resolve4/resolve6`. It reports a 1-hour TTL, so a process that cached a stale
    entry must be restarted after an identity change. (Handoff item 6, "pinetree to Misskey REQUEST_FAILED",
    was this, not pinetree.)
13. **Akkoma rejected the lab CA ("Unknown CA").** Elixir loads CA certificates at boot
    (`:public_key.cacerts_load`), so the CA must be appended to the three bundles and Akkoma restarted. A restart
    resets the container's `/etc/hosts`, so `lab-net.sh` re-adds it every time.
14. **Akkoma was not on the lab network** (`birchtree-lab`). `lab-net.sh` attaches it at 172.28.0.40.
15. **Mastodon containers did not resolve the pinetree/misskey/catodon/akkoma names.** They were recreated by
    hand with `--add-host`; `lab-up.sh` now creates them reproducibly.
16. **`/etc/hosts` on the host had to be edited** (outside the "edit only pinetree and birchtree" rule; the
    user approved). `hosts.sh` writes and removes exactly the marked lines.

## C. Real bugs found and fixed

17. **Birchtree returned 500 for every Akkoma-signed activity.** Akkoma publishes `publicKeyPem` ending in
    `\n\n`; the vendored `activitypub_federation` crate's strict PEM parse failed. The key is now trimmed and given
    one newline, and an unparseable key is a signature failure (401), not a 500. 646 stuck Akkoma deliveries
    completed on retry after the fix. (My first guess, a PKCS#1 key, was wrong.)
18. **Pinetree generated two keys for one actor** when two requests raced (`ensure_actor_keypair`: last write
    wins), so remote servers cached a key that was then replaced. This produced the `u9@pt-b` to bt-b signature
    failures. Fixed with an atomic `save_actor_keypair_if_absent` (redb write transactions are serialised).
    Test `concurrent_ensure_actor_keypair_yields_one_key` failed 3 of 3 runs on the old code.
19. **Pinetree `ensure_system_actor` failed with "handle system_actor is already taken"** when two requests
    created it at once (first-contact `NO_SUCH_USER`). Now a `Conflict` re-reads the existing account.
    Test `concurrent_ensure_system_actor_all_succeed` failed about half the time on the old code.
20. **Birchtree never refetched a stale cached remote key.** After a signature failure it kept returning 401
    until the 24 h refresh interval. The vendored inbox now refetches the actor once (60 s cooldown) and checks
    again, as Mastodon does. Test: `a_signature_that_fails_against_a_stale_cached_key_is_checked_against_a_refetched_one`.
21. **Misskey's first lookup of a remote server races** (duplicate-key insert on two concurrent first lookups).
    Not fixed in Misskey (not ours); `lib.resolve` retries once after 2 s.

## D. Harness bugs found and fixed

22. **The follow-convergence check reported ~830 false "missing edges".** Causes: (a) no pagination
    (Mastodon-API followers cap at 80 per page; each user has about 91 followers); (b) birchtree's `Link` header
    is relative; (c) Mastodon's `Link` says `https://` while the harness uses `http://` and does not trust the
    lab CA. Fixed in `lib.Node.followers_accts` (rebuilds the next URL from the node's base). I also misread the
    matrix direction once (the list is the **followers** a node's users lack; follower node = row, followee
    node = column). `verify-follows.py` prints the directional matrix.
23. **Obsolete 5-node helpers** (`bootstrap.py`, `bootstrap-fix.py`, `p3-control.py`, `p6-votes.py`,
    `stress-fixup.py`) targeted the old pt-ks/pt-mk/misskey/catodon/akkoma matrix with 6 users and `/tmp`
    paths. Moved to `scripts/fediverse-bench/legacy/`. They are not part of the run.
24. **`phase-features.py` `wait_until` bug, boost and unboost.** The check lambdas returned a
    `"count=0,before=0"` string when the condition was not yet true. `wait_until` stops on the first truthy
    value, so the check ended on its first poll, before the Announce could be delivered. This made renote/boost
    59 of 59 FAIL and 8 unboost FAIL. A direct test (pt-a to pt-b, bt-a to pt-b, pt-a to bt-b) showed the origin
    count going 0 to 1 within about 3 s. The same bug had already been fixed for unreaction. Fixed.
25. **`phase-features.py` reply check read only `replyId`** (Misskey shape). Mastodon-API nodes report
    `in_reply_to_id`, so all 27 replies from birchtree sources failed with `rid=None`. Fixed.
26. **`phase-features.py` media check read only `files`/`attachments`.** Mastodon-API nodes use
    `media_attachments`; all 26 media failures had a Mastodon-API destination. Field added. **Unverified:**
    Mastodon's media URLs may be HTTPS, which `lib.fetch_bytes` cannot fetch without the lab CA, so some
    media checks may still fail for that reason.
27. **I rebuilt the birchtree binary while a run was in progress** (`cargo test --release` and `cargo build
    --release` replace `target/release/birchtree`). The running nodes kept the old inode, so the build ID on disk
    no longer matched what was running. Always read the build ID from `/proc/<pid>/exe`
    (`bench-up.sh` does) and never build during a run.
28. **User indices were hard-coded for 6 users** (`% 6`) in `phase-features.py` and `phase-stress.py`; the
    topology has 10 per node. Now `len(users)`.
29. **Stale result files sit in the repo's scripts directory** (`features.json`, `follows.json`, `probes.json`,
    `stress*.json`, `stress-run*.log`, `s1-storm-summary.txt`). They are old Oct 16-18 outputs from before
    `BENCH_DIR` was required, some tracked and modified. Do not cite them. Left in place (not mine to delete).
30. Other scripts still writing to `/tmp` and not used by the bench: `birchtree/scripts/smoke-phase4.sh`,
    `smoke-phase7.sh`, `pinetree/scripts/smoke-phase{1,2,3}.sh`, `smoke-all.sh`, `keystone-sweep.sh`.

## E. Open findings (not fixed; the next implementer owns these)

31. **Birchtree does not federate polls on purpose** (`docs/compatibility-profile.md` D29): inbound `Question`
    and `Update` of one are refused 422 (`unsupported: update of Some("Question") is not in the profile`); none are
    sent. The features suite counts this as 41 poll failures. See the handoff for the full list.
32. **Birchtree rejects some pinetree activities with 422** "data did not match any variant of untagged enum
    Activity" (column ~2684 / 3139 / 4701), including `QuoteRequest` from pt-a.
33. **Birchtree 403 "URL failed verification: Domains do not match"**: 85 on bt-a, 80 on bt-b in one run. Sender
    and activity types not yet identified.
34. **Birchtree job quarantined: "announce without a boost row"** (6 per node). A boost's delivery job runs
    when the boost row is gone. Not investigated.
35. **Mastodon returns 422 on a first-contact Follow from a Misskey user** (1 of 9180 edges per run; `u0`, then
    `u7`, so it is a race, not a fixed pair). Mastodon's log shows only `status=422` on the inbox request, with
    no exception. Misskey does not retry a permanent 4xx. Unclassified.
36. **Media is not verified for Mastodon-API destinations** (item 26) and the earlier handoff item 2 ("media
    no-attachment, 0 of 166 verified in S4") may be the same harness bug in `phase-stress.py`. Not checked.
37. **Handoff item 3 (classify every S2 miss: delivery lag / follow gap / API gap / not implemented)** was not
    reached. No S2, stress, concurrent, crossfamily or interop result exists from a valid run.
38. The probe notes I posted on the live nodes while diagnosing boosts (3 notes and 3 boosts as `u8`) are in the
    aborted run's data.

## F. Run3 findings (2026-10-19; run `/root/bench/run3`, builds pinetree
`f58be005c558f650`, birchtree `cc4421edd6130686`; full report in
`docs/federation-bench-2026-10-19-phase7-run3.md`)

39. **Birchtree minted reblog-wrapper URIs on the target's domain.**
    `reblog_links` swapped the target URI's trailing id, so a boost of a
    remote post produced an Announce id on the origin's host with a local
    actor. Domain-checking receivers (birchtree itself, and evidently
    misskey/catodon/akkoma) refuse it; pt/mastodon tolerate foreign-id
    activities. Effect: every bt-created boost uncounted on bt/misskey/
    catodon/akkoma origins (features 15 FAIL), ~140 quarantined deliveries
    per bt node, zero inbound Announces bt->bt in 2800 activities. Fixed:
    wrapper links live on the booster's actor URI; test
    `a_reblog_wrapper_is_addressed_on_the_booster_domain`; verified live
    (bt-b boost counted on bt-a in 6 s).
40. **Birchtree read any string `closed` as "poll already ended".**
    Akkoma (and Sharkey-family catodon) always send `closed` carrying the end
    *time*. Every akkoma/catodon poll ingested sticky-expired, so local votes
    422'd and no vote was ever delivered back (features akkoma/catodon-origin
    polls tallied 0; S3 shortfall 5, all catodon-origin). Fixed:
    `closed_flag(now_ms)` (past time or literal true only); test
    `a_question_with_a_future_closed_time_is_not_expired`; verified live
    (bt vote 200, akkoma tallied in 15 s).
41. **Crossfamily reply check repeated handoff item 25.**
    `phase-crossfamily.py` accepted only Misskey-shape `replyId`; bt-side
    copies carry `in_reply_to_id`. All 9 `reply bt -> pt` FAILs are the check,
    not federation (linkage verified live on the copied note). Fixed the same
    way. (The features twin was fixed before run3.)
42. **Features `media` failures into bt are lag, not loss (S4 proves it).**
    S4 hash-verified bt-destination copies for every source with settle-until-
    deadline polling; every re-checkable features copy shows its attachment
    hours later. The features 90 s no-deadline check measures the async fetch
    queue. Recommended: backport S4's settle-until-deadline pattern to the
    features media check (same bar, honest window).
43. **S7's 2900-char post is invalid for Mastodon-compatible servers.**
    bt-a/b/c and real mastodon all correctly 422 (500 limit). The suite
    expectation needs a 500-char cap or an exemption, not a product change.
44. **Harness `fetch_bytes` still has no lab CA.** All 18 mastodon-involved
    S4/media failures are `https://` fetches (attachments demonstrably
    present) plus mastodon byte transformation (200 with different sha).
    Fix: use `/root/parity/lab/certs/lab-ca.crt`; compare mastodon bytes
    against the source-side original, not the upload.
45. **`delete` scatter (8 features FAILs) converged post-run.** All
    destination copies gone hours later: lag. The S6 residue (bt 13,
    catodon 1, akkoma 4) still needs per-pair attribution.
46. **Akkoma-outbound interactions never land (new family, 11 features
    FAILs).** `reaction akkoma -> *` (5), `mention-notif akkoma -> *` (6),
    `unboost * -> akkoma` (9). Votes TO akkoma arrive from non-bt voters
    (S3), but nothing bt sends reaches akkoma (zero vote deliveries in bt
    logs though general delivery to akkoma's inbox works). Open: event-side
    vs receipt-side, per activity type, with trace.
47. **Undo-Announce never applies on misskey origins (5 features FAILs,
    stuck at count 1 hours later).** Five booster implementations, one
    common receiver. Not lag. Open: misskey-side handling with trace.
48. **Whether catodon tallies valid remote votes is still open.** bt-side
    cause (item 40) confirmed and fixed, but S3 partial tallies vs features
    zeros vs recheck zeros are mutually inconsistent; catodon's own
    `notes/show` read as poll-less during features. Needs catodon logs.
49. **Interop 22/23; only `bt-a home has alice follow` FAILs.**
    Mastodon->bt-a follow not visible in home timeline. Open (timeline rule
    vs delivery); everything else (notes/edits/deletes/replies/likes/boosts/
    quotes/flags/media) passes both directions.
50. **S2's 237 misses are all `edge_present_not_visible`** (follow graph
    complete; capability probes all 200). Pair table in `stress.out`.
    Handoff item 3 (classify every miss) still not reached.

## G. Run4 findings (2026-10-19; run `/root/bench/run4`,
`runs/20261019T144547Z`; builds pinetree `f58be005c558f650`, birchtree
`ee6bfe94122a12c7`; full report in
`docs/federation-bench-2026-10-19-phase7-run4.md`, live log in
`docs/federation-bench-2026-10-19-run4-log.md`)

51. **Crossfamily `media pt -> bt` repeated handoff item 26.**
    `phase-crossfamily.py` read only Misskey-shape `files`/`attachments`;
    bt-side copies carry `media_attachments`. All 9 fresh-run FAILs are the
    check, not federation (copies dumped live, each with 1 attachment).
    Fixed the same way. Follow-on: the fixed check first re-ran into
    placeholder rows (attachment row present, `url` still null while the
    async fetch completes) — `media_seen` now requires a fetchable URL.
    (Features' deferred check never saw this: its deadline loop re-reads
    the note each poll.)
52. **Birchtree DELETE of a reblog sends `Delete`, never `Undo(Announce)`.**
    `delete_status` (`src/persistence/redb/statuses.rs:835`) enqueues only
    a `delete` event; `unreblog_status` (`:1424`) enqueues `undo_announce`.
    The origin counts reblogs from its own index, so a `Delete` of the
    booster-side wrapper changes nothing there: the count sticks at 1.
    Proven live: bt-b boost counted on bt-a in 6 s, `/unreblog` decremented
    in 6 s; real Mastodon 4.7.3 DELETE-of-reblog decrements a bt origin in
    6 s. Mastodon-parity product bug. Explains 9 run4 unboost rows where bt
    is the booster (`bt -> bt` 6, `akkoma -> bt` 3 — previously masked:
    boosts never arrived so these passed vacuously). Fix sketch: when the
    deleted status has `reblog_of_id`, additionally enqueue `undo_announce`
    with the wrapper URI (keep the `delete` for followers' timelines).
    Not implemented in run4; the run4 numbers were measured with the bug.
53. **`unboost` fully attributed (origin=src, booster=dst; 26 rows).**
    Item 52 covers the 9 bt-booster rows. The 9 `* -> akkoma` rows (every
    booster incl. mastodon, which provably sends Undo) are akkoma-side:
    akkoma never sends Undo on delete (its outbound family from item 46)
    and never applies the ones it gets. `misskey -> pt-*` is misskey-side
    (pt provably sends Undo(Announce) on pure-renote delete —
    `delete_note`, `was_announce_of_uri` branch — which is also why
    `akkoma -> pt-*` unboost passes while every `* -> akkoma` row fails).
    `misskey -> catodon/mastodon` likewise misskey-side. `misskey -> bt-*`
    is joint (booster sends nothing AND the origin wouldn't apply it).
    bt applies inbound Undo(Announce) from every sender (log:
    `outcome=applied` for pt, misskey `notes/.../activity/undo`, catodon,
    akkoma `activities/<uuid>`, mastodon `#announces/.../undo`).

## H. Run5 findings (2026-10-19; run `/root/bench/run5`,
`runs/20261019T181810Z`; builds pinetree `f58be005c558f650`, birchtree
`e24d75aea1085a06` (item-52 fix); full report in
`docs/federation-bench-2026-10-19-phase7-run5.md`, live log in
`docs/federation-bench-2026-10-19-run5-log.md`)

54. **Item 52 fixed and measured.** `delete_status`
    (`src/persistence/redb/statuses.rs:835`) now enqueues `undo_announce`
    (wrapper URI, target id) and drops the `REBLOGS_BY_ACCOUNT` mapping
    when the deleted status has `reblog_of_id`; the `delete` is kept.
    Regression tests: `a_reblog_delete_enqueues_undo_announce`,
    `an_ordinary_delete_enqueues_no_undo_announce`; full
    `cargo test --release` 349/349. Run5 matrix proves it live: the
    harness undoes via DELETE of the wrapper
    (`phase-features.py:165-166`), and all 9 bt-booster unboost rows
    (`bt -> bt` 6, `akkoma -> bt` 3) flipped to PASS. The 17 remaining
    rows match item 53 exactly (9 akkoma-side + 8 misskey-side).
55. **S2 run5: same 635/215 signature; run4 classification carries.**
    Totals identical to run4; pairs shifted slightly (bt-b->bt-a 10,
    bt-b->bt-c 6, akkoma->bt-b 7, akkoma->bt-c 14, mastodon->catodon 3;
    rest as run4). All `edge_present_not_visible`, probes all 200 — the
    timeline-index API-gap verdict stands; the server-vs-harness sub-cause
    stays unclassified (needs the deep-pagination live check). Note:
    `shortfall_n=4` (was 2), `residue_after_undo` 2 keys (was 1) —
    Like-path, separate mechanism, unclassified.
56. **S3 run5: identical to run4; run4 mechanism carries.** 442/455,
    shortfall 5 (all catodon-origin), 13 failed vote calls. Per-call
    re-derivation not repeated; the `#`-fragment skip (bt/misskey/
    mastodon vote IDs contain `#`, pt/akkoma do not) and the akkoma-voter
    `unresolved` verdicts stand on run4 trace evidence.
57. **follows run5: 3 missing, all `mastodon/u0 -> misskey/catodon`.**
    Mastodon-side first-contact flake family (item 35). Not the fix: no
    bt party on any missing edge, and the fix touches only the
    reblog-wrapper delete path. Mastodon-log 422 confirmation still open.
58. **Delete re-check run5: 8/8 converged (lag).**
    Read-only home scan (`/root/bench/run5/delete-recheck.py`): every
    `FTR|del|` marker absent from all dst users' homes. The delete path
    is untouched by the fix (fresh-post markers are never reblogs).
59. **Crossfamily first clean run: 162/162, exit 0.** No `.fresh` needed.
60. **S6 run5: bt 4/4/4 (same mechanism), misskey:2 new.** `duplicate`
    delete outcomes present in all three run5 bt logs (139/184/140
    lines) — the Delete-beats-Create swallow from the run4 forensics.
    Mechanism fix not implemented (outside this handoff). misskey:2 is
    receiver-side, pair-level unclassified (as catodon:1/akkoma:2 were).
61. **Interop unchanged (22/23).** Same `bt-a home has alice follow`
    FAIL, third run running: broken oracle (follow creates no status) +
    async Sidekiq non-delivery.
