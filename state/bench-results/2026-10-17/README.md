# Fediverse bench results — 2026-10-17

Raw evidence for `state/logs/2026-10-17-bench-rerun.md` (run 2, S2/S6, unboost) and
`state/logs/2026-10-17-tombstone-delete-before-create.md` (S6 fix and live probe). Nothing here
is a compatibility claim by itself; the state logs say what is verified.

## Layout

- `logs/` — probe, feature-matrix, and smoke output, copied verbatim from
  `/tmp/opencode/bench-rerun/logs/` plus the netns smoke runs and the Misskey restart log.
- `probes/` — the scratch harness that produced the logs (patched copies, not the repo copies in
  `scripts/fediverse-bench/`, which still have the known bugs). `misskey-bench.yml` is the Misskey
  config used for the post-restart runs (dev credentials, loopback only).
- `debug/` — `[fed-debug]` lines from `pt-ks.log` / `pt-mk.log` for the post-fix S6 probe window
  only (the full logs are 85 MB and stay out of git).

## S6 Delete-before-Create probe (post-fix)

Probe: `probes/fast_delete_probe.py` — Misskey (`mk1`) creates then deletes with no pause; after a
40 s settle, `ap/show` on pt-ks and pt-mk for each note URI. `RESIDUE` = still live.

| run | notes | pt-ks residue | pt-mk residue | source |
|---|---|---|---|---|
| before fix | 5 | 2 | 1 | `logs/fast-delete-probe.log` |
| after fix | 5 | 0 | 0 | `logs/fast-delete-probe-fixed.log` |
| after fix | 20 | 0 | 0 | `logs/fast-delete-probe-fixed-n20.log` |

**Caveat (read before citing the after-fix rows):** in the post-fix window, every `Create` was
applied on pt-ks before its `Delete` arrived, about 1.3 s apart (`debug/pt-ks-fed-debug-s6-window.log`).
So this live run did not exercise the Delete-before-Create ordering, and the zero residue does not
prove the tombstone path fired. The race is covered by the deterministic handler test
`federation::inbox::delete_tombstone_tests::delete_before_create_does_not_resurrect` and the
conformance cases T1–T3. The live probe shows no regression, and nothing more.

## Excluded from git (on purpose)

- **Misskey database dump** (`/tmp/opencode/rig-backup/misskey-2026-10-17.sql.gz`): contains the
  `access_token`, `auth_session`, and `app` tables, which hold live bench credentials. It is kept
  outside the repo. Restoring it into a fresh `pinetree-interop` Postgres reproduces the bench
  Misskey state.
- `/tmp/opencode/bench/accounts.json`: the 30 bench account tokens. Not committed. A scan of every
  committed file found none of the 30 token values.
- Full `pt-ks.log` / `pt-mk.log` (85 MB debug logs), `redb` data directories, and the
  `redb-backup-*` and `src-before-fed-debug` snapshots in `/tmp/opencode/bench-rerun/`.

## Not verified here

- The S6 race path under live federation (see caveat above).
- Run-2 FAILs that are still unexplained: poll+vote ×5, media-to-Akkoma ×4, mention-notif-from-Akkoma ×2.
- Misskey↔Catodon Undo(Like) (2026-10-16 P3).
- Probe-matrix 0-failure result: the difference from the earlier run is rig state, not yet confirmed.
