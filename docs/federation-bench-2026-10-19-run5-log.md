# Federation bench run5 log (2026-10-19): item-52 fix verification

**Status: all suites finished.** `follows` 1 (1335 s, 3 missing);
`features` 1 (3510 s, 1108/62); `stress` 1 (3152 s); `concurrent` 0
(37 s); `crossfamily` 0 (50 s, 162/162); `interop` 1 (398 s, 22/23).

- Run: `/root/bench/run5`, suite dir `runs/20261019T181810Z`.
- Builds (from `/proc/<pid>/exe`, `bench-up.sh`): pinetree
  `f58be005c558f650` (unchanged), birchtree `e24d75aea1085a06` (item-52
  fix; disk sha `e24d75aea1085a06...`, `cargo test --release` 349/349).
- Bench network: 10 nodes; 90/90 probes resolved on a pristine `bench-up`.
- Pre-run: `df -h /root` 855G avail; no builds during the run.

## What changed before this run (birchtree only)

`src/persistence/redb/statuses.rs` `delete_status`: when the deleted
status has `reblog_of_id`, additionally enqueue `undo_announce` with
`status_id = target`, `announce_uri = wrapper uri`, and drop the
`REBLOGS_BY_ACCOUNT` mapping (same semantics as `unreblog_status`);
the `delete` event is kept for followers' timelines. Regression tests in
`tests/phase6_content.rs` (reblog-delete enqueues Undo; ordinary delete
does not). No pinetree change; no harness change.

## Suite notes

- follows: 9177/9180; missing all `mastodon/u0 -> {u0@misskey,
  u3@catodon, u7@misskey}` (item 57). Akkoma leg 676 s as usual.
- features: the harness undoes via DELETE of the wrapper
  (`phase-features.py:165-166`) — the exact fixed path — so the 9 flipped
  rows are live proof, not vacuous. `grep -c "^\[FAIL\]"` = 62 (the file
  appends a re-list summary; naive `grep -c FAIL` double-counts).
- stress: S2 635/215 (same totals as run4); S3 442/455 shortfall 5 +
  votes_failed=13 (same as run4); S4 155/202 all-mastodon residue;
  S6 bt 4/4/4 + misskey 2 + catodon 1 + akkoma 2.
- crossfamily: first 0-FAIL run (no `.fresh`/re-run needed this time).
- interop: same single FAIL as runs 3-4.

## Post-run forensics (nodes up, suites complete)

- `delete` (8): all markers absent from every dst user's home
  (`/root/bench/run5/delete-recheck.py`, read-only scan) — lag. Case
  closed (item 58).
- `unboost`: 17 remaining rows match item-53 attribution exactly
  (9 akkoma-side + 8 misskey-side); zero bt-booster rows.
- S6 bt signature: `duplicate` delete outcomes present in all three run5
  bt logs (139/184/140 lines), same Delete-beats-Create shape as run4.

## TODO

- Phase 7 run5 report (done), problems appended at 54 (done),
  `bench-down.sh` + `ss`/`docker ps` verify (done below).

## Follow-up analysis (post-teardown, code-verified, unmeasured on the bench)

- **Pinetree reactions / Mastodon favourites.** Local model: per-(user,note)
  `REACTIONS` rows + per-emoji counters; `coerce_reaction` collapses to ❤
  on like-only notes; no `notes/reactions` endpoint (count-only over API).
  Outbound `Like` is per-recipient-host (`host_caps.rs`, cached NodeInfo,
  ~7-day TTL): emoji-aware hosts get `Like`+`content`+`_misskey_reaction`,
  the plain set (mastodon, akkoma, …) gets a bare `Like`. Inbound
  (`inbox.rs:983`, `objects.rs:1666`, `inbound/notes.rs:244`):
  `_misskey_reaction`, else `content`, else ❤ — so a plain Mastodon
  favourite stores as ❤ (test-pinned). Zero pt-caused reaction FAILs in
  run5.
- **Akkoma emoji reactions: no interop today, either direction.**
  Akkoma sends emoji reacts as `EmojiReact` (plain favorites as bare
  `Like`); pinetree ignores `EmojiReact` at dispatch (accepted, dropped),
  so akkoma→pt emoji never lands. Pt→akkoma emoji degrades to ❤ by the
  plain-`Like` deny-list. Documented as out-of-scope/nice-to-have in
  `docs/misskey-parity-plan.md` (§6 follow-up note, 2026-10-19).
- **Birchtree interop by counterpart (run5 fresh matrix).** vs itself:
  clean (`bt→bt` 6/6 fixed, everything else green, crossfamily 162/162).
  vs pinetree: clean (no `bt↔pt` FAIL anywhere; both agree on
  `Undo(Announce)`). vs real Mastodon 4.7.3: 22/23, all activity families
  pass both ways; single FAIL is the broken follow-grep oracle + Sidekiq
  async (not federation); media rows are mastodon-side fetch/re-encode.
  vs misskey: boosts count, but `misskey→bt` unboosts (never applies Undo)
  and deletes (lag, converged) stay red — all misskey-side. Remaining
  bt-involved red rows: `bt→akkoma` unboost (akkoma never applies),
  `catodon→bt` polls (`#`-fragment skip), `akkoma→bt` mentions (bare-text
  emits), `mastodon→bt` media (re-encode). No bt-caused failure in the
  matrix; the genuine bt bug left is S6 Delete-beats-Create residue.
