# Handoff: fix birchtree reblog-delete Undo + classify S2/S3/S6/interop residue

You are continuing the 2026-10-19 federation benchmark. Read these first, in this order:

1. `/root/pinetree/docs/federation-bench-2026-10-19-phase7-run4.md` — the run4 report (all numbers, all classifications).
2. `/root/pinetree/docs/federation-bench-2026-10-19-run4-log.md` — run4 log with the live proofs.
3. `/root/pinetree/docs/federation-bench-2026-10-18-session2-problems.md` items 51–53 — the run4 findings (51 = crossfamily media check bug, fixed; 52 = the Undo bug below; 53 = full unboost attribution).
4. `/root/pinetree/docs/handoff-birchtree-polls-and-feature-parity.md` — the older handoff (its "Rules" and "How to run the bench" still apply verbatim).

## Starting state (verified, do not re-derive)

- Run4 (`/root/bench/run4`, `runs/20261019T144547Z`) is complete: follows 0, features 1 (1101/69), stress 1, concurrent 0, crossfamily 1 (162/9 fresh; 162/2 on the fixed-check re-run, both deletes converged as lag), interop 1 (22/23). Bench is torn down (`bench-down.sh` clean). FoundationDB untouched. Nothing committed in either repo.
- Builds measured in run4: pinetree `f58be005c558f650`, birchtree `ee6bfe94122a12c7` (configurable `--max-status-chars`, default 500, max 3000; full `cargo test --release` was 347/347).
- Proven by re-check, not by matrix: `delete` scatter = lag (all gone); crossfamily `media pt -> bt` = check bug (fixed, re-run 9/9 media PASS); features `media * -> bt` = async-fetch lag (deadline loop proves arrival); all mastodon-involved media FAILs = mastodon-side fetch failure / re-encode (lab-CA fix confirmed 200s).
- Keystone UIs are up and in use: nginx `:3120` (pt-a) and `:8180` (bt-a). Do not disturb them without asking the operator; they are not part of the bench.

## Task 1 (product bug, root-caused — implement + test + verify live)

Birchtree DELETE of a reblog wrapper enqueues only a `delete` event, never `undo_announce` (`src/persistence/redb/statuses.rs:835` vs `unreblog_status` at `:1424`, which does). The origin counts reblogs from its own index, so the count sticks at 1. Proven live on run4: bt boost counted in 6 s, `/unreblog` decremented in 6 s, real Mastodon 4.7.3 DELETE-of-reblog decrements a bt origin in 6 s. Fix sketch in problem 52: when the deleted status has `reblog_of_id`, additionally enqueue `undo_announce` with the wrapper URI (keep the `delete` for followers' timelines). Add regression tests (delete-of-reblog enqueues Undo; ordinary delete does not). This should flip the 9 bt-booster unboost rows (`bt -> bt` 6, `akkoma -> bt` 3). Do NOT touch the `* -> akkoma` rows (akkoma never sends/applies Undo — not ours) or the misskey-origin rows (misskey never applies Undo — not ours).

## Task 2 (forensics, trace-backed classification or stays "unclassified" — never a guess)

- **S2** (`stress.out`, run4: 635 resolved / 215 unresolved_final, all `edge_present_not_visible`, pair table in the JSON): handoff item 3 — classify every miss as delivery lag / follow gap / API gap / not implemented, using `BENCH_TRACE` + inbox logs. Capability probes all return 200, so resolve itself works.
- **S3** (442/455 votes ok, shortfall 5 all catodon-origin u1, 13 failed vote calls): per-call classification from trace; then catodon-side: why valid remote votes from bt/misskey/mastodon voters never tally while pt/akkoma-voter votes do (catodon logs needed; bt-side cause already exonerated by the akkoma contrast).
- **S6** (residue bt-a 4, bt-b 6, bt-c 6, catodon 1, akkoma 2): per-pair attribution; overlaps the delete-lag finding.
- **Interop** (22/23, only `bt-a home has alice follow` FAILs two runs running): timeline rule vs delivery — check bt-a receipt vs home-visibility logic.
- **Carry-overs**: 422/403 sender attribution (problems 32/33); akkoma-outbound reactions/mentions/undos (akkoma-side, one trace to confirm nothing hits pt/bt inboxes).

## Rules (carry over, plus)

- Edit only `/root/pinetree` and `/root/birchtree` (plus `/etc/nginx` only if the operator asks). No commits unless asked. No builds during a run; `df -h /root` before building (`df` over-reports here — true free space is ~86 GB).
- Every run lives in `/root/bench/run5`; `BENCH_DIR` required. Kill by PID, never `pkill -f`; leave fdbserver and the Claudeulm daemon alone.
- Count features FAILs as `grep -c "^\[FAIL\]"` — the file appends a re-list summary at the end, so a naive `grep -c FAIL` double-counts.
- Do not reach 0 by weakening a check. The crossfamily re-run precedent applies: keep fresh-run records (`*.fresh`) when re-running with a fixed check, and say which numbers are fresh vs re-run.
- Final message opens with what is stuck, then: run ID + `BENCH_DIR`; both build IDs; per-suite exits/durations; what changed + what is uncommitted; which numbers are unclassified; anything left running.

## Deliverables

1. Item-52 fix + regression tests, full `cargo test --release` green, rebuilt binaries, pristine `bench-up` to `/root/bench/run5`, full `run-suites.sh` (detached `setsid`/`nohup`, background `until` waiter — never poll). Expected (not guaranteed): bt-booster unboost rows flip to 0 FAIL; akkoma/misskey/catodon rows stay red for the documented reasons in item 53.
2. Updated Phase 7 report (fresh-run numbers only) + run5 log + problems appended (continue numbering at 54).
3. `bench-down.sh` + verified-clean `ss`/`docker ps`; say explicitly whether nginx/Keystone was left running.
