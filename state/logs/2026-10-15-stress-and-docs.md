# 2026-10-15 — Federation stress battery + docs refresh

## Stress (`/tmp/stress-fed.py`, live KEEP stack, all left running)
- 24 accounts (6/node: `stmk*`, `stsk*`, `stp3s*`, `stp4s*`), full resolve
  mesh, **144 follows, 0 fails**.
- Battery per user: 6× public, home, followers-only, specified (local
  buddy), localOnly, CW, self-reply, pure renote, quote (text+renoteId),
  tiny+600KB image attachments → **~408 notes, 0 create/upload fails**
  (p50 28 ms, p95 315 ms under federation load).
- Cross-node replies + 2-emoji reactions per remote anchor; unreact sample.
- **Anchor receipt 12/12**; negatives clean (no followers/specified/
  localOnly leaks); trash-subset deletes 4xx everywhere.
- 68 logged fails, all script artifacts: 66 timeline-window misses
  (scanner seeks oldest anchor in newest-100 after 400 notes buried it;
  12/12 receipt proves delivery), 2 `ALREADY_REACTED` (script tried 2nd
  emoji where Misskey allows one per user — correct rejection).
- Two script bugs fixed along the way: resolve needed poll-retry (hit
  Sharkey mid-reboot), and `PUBH` used `127.0.0.x` instead of `127.0.1.x`.
- Mesh + content left intact for manual testing.

## Docs refresh (repo)
- `scripts/smoke-phase1.sh`, `scripts/smoke-phase2.sh`: binary resolved via
  `cargo metadata` (honours `.cargo/config.toml` target-dir), `PINETREE_BIN`
  override; phase-2 also hard-checks Node 24 on PATH (Misskey `re2`).
- `README.md`: nvm toolchain, `<target>` binary paths, 251-test count,
  bench subsection (`scripts/bench-local.py` + numbers).
- `docs/phase-2-interop-howto.md`: ext4 note, fixed numbering, Sharkey-own-UI
  section (pre-builds, vite :5174, `VITE_PORT` restart).
- `docs/wiki/Current-State.md`: 150→251, 2.6 row, layout notes;
  `docs/wiki/About.md`: `<target>` paths + ext4 note.

## Still up (KEEP stack, ext4 checkouts)
- Misskey :3101 / UI :8181 (`mk1`), Sharkey :3102 serves own UI (`sk1`),
  pinetree A :3103 / UI :8182 (`pt3`), pinetree B :3104 (`pt4`).
  `pt3` unlocked, `mk1` accepted follower. Stop: `scripts/smoke-phase2.sh stop`.
