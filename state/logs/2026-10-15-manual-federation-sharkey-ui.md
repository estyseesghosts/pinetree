# 2026-10-15 — Manual federation proof + Sharkey web UI wired

## Federation scare resolved (live proof, not a bug)
- User could not see mk1↔pt3 notes federate. Root cause, verified step by
  step: the interop test's final section leaves `pt3` **locked**, so the
  manual `mk1 → pt3` follow sat in `following/requests/list` as pending
  (unlocking does not auto-accept). `followersCount` stayed 0, so no
  delivery — correct behavior.
- After explicit `following/requests/accept` (204): pt3 followers 0→1,
  fresh pt3 note arrived in mk1's `users/notes`. Reverse direction
  (mk1 note → pt3 home timeline) already green earlier. Both directions
  proven on the live KEEP stack (`/tmp/fed-check{,2,3,4}.py`).
- `pt3` left unlocked with `mk1` as accepted follower for manual testing.

## Sharkey web UI (new)
- Sharkey serves its own client: backend renders pug shell at `/` and
  reverse-proxies `/vite → localhost:$VITE_PORT` in no-manifest dev mode
  (`ClientServerService.ts`, `config.ts` dev fallback
  `{'src/_boot_.ts': ...}`).
- Pre-built under Node 22: `frontend-shared`, `misskey-bubble-game`
  (`misskey-js`, `misskey-reversi` dists already present).
- `vite --port 5174 --strictPort` (Node 22) running; log `/tmp/sharkey-vite.log`.
- Restarted Sharkey backend with `VITE_PORT=5174 EMBED_VITE_PORT=5175`
  (same config/env otherwise); pgid appended to `/tmp/pinetree-interop/pids`
  so `smoke-phase2.sh stop` still cleans everything. Verified: `/` shell
  references `/vite/src/_boot_.ts` → 200 through the backend.
- Proxy lesson repeated: `pkill -f` matches the invoker's own cmdline
  (howto gotcha); used pid-only kills. Removed the interim `:8183` proxy —
  unnecessary, since the backend serves its own UI.
- `/tmp/opencode/mk-proxy.mjs` gained a rig-only fallback: Sharkey keeps
  locales at `<root>/locales/*.yml` (48 langs detected).

## Where to click (Windows browser → `localhost`)
- Sharkey UI (own frontend): `http://localhost:3102` (`sk1`/`password123`).
- pinetree A with Misskey UI: `http://localhost:8182` (`/login` helper,
  `pt3`/`password123` — pt3 now unlocked).
- Real Misskey UI: `http://localhost:8181` (`mk1`/`password123`).
- Inside WSL: `http://127.0.1.2`, `http://127.0.1.3`, `http://127.0.1.1`.

## Open
- Proposed moving `misskey`/`sharkey` checkouts (and `CARGO_TARGET_DIR`)
  onto ext4 — 9P `/mnt/c` is the boot/vite bottleneck. Awaiting go-ahead.
