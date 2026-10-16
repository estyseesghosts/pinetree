# 2026-10-15 — Full interop rig for browser testing (pinetree + Keystone + Misskey + Sharkey UIs)

## Why
Owner clarified: Keystone replaces the Misskey-frontend-on-pinetree rig, but
Misskey-with-own-UI and Sharkey-with-own-UI must still run for interop
testing. Everything must be reachable from a Windows browser (WSL2 forwards
`127.0.0.1`/`localhost`, not `127.0.1.x`).

## What runs (all ext4, verified no `/mnt/c` in symlinks, procs, logs, cfgs)
- `KEEP=1 SHARKEY_NODE_BIN=/usr/sbin scripts/smoke-phase2.sh` → SMOKE_OK
  (`tests/federation_interop.rs` INTEROP_OK). Fresh pg/redis, mk1/sk1/sk2 +
  pt3/pt4, federation `all`. Work: `/tmp/pinetree-interop`.
- Keystone's backend = 5th node pt5: `:3000` pinetree restarted as
  `--public-url=http://127.0.1.5 --data-dir=/tmp/pinetree-interop/pt5`
  (`PINETREE_ALLOW_PRIVATE_IPS=1`), proxy `127.0.1.5=3000`, user
  `ks1`/`password123`, RSA warmed. Keystone vite `:5173` proxy untouched.
- Misskey UI: `packages/frontend` vite `--host 0.0.0.0 --port 5181`
  (Node 24; needed `packages/i18n` build + `misskey-bubble-game` build;
  `frontend-shared` resolves from source). Backend restarted with
  `VITE_PORT=5181 EMBED_VITE_PORT=5182` → UI at `http://localhost:3101`.
- Sharkey UI: `frontend` vite `--host 0.0.0.0 --port 5174` (Node 22;
  needed `frontend-shared` + `misskey-bubble-game` builds → `js-built/` +
  `built/`). Backend restarted with `VITE_PORT=5174 EMBED_VITE_PORT=5175`
  → UI at `http://localhost:3102`.
- Embed vites not run (only affects /embed pages); EMBED ports set to
  unused values so backends don't cross-talk.
- pt3 unlocked (`i/update isLocked=false`; the automated test leaves it
  locked, which stalls follows to it).
- `/tmp/pinetree-interop/pids` rewritten with TRUE pgids (see lesson).

## Lessons (process management)
- `scripts/smoke-phase2.sh` records `$!` of the setsid chain, but `setsid`
  forks when its parent is a group leader, so the real masters
  (442532/442551) lived in a different pgid than the pid file. My first
  `VITE_PORT` restart killed the wrapper pgids, hit nothing, and the
  replacements died on EADDRINUSE — leaving the OLD backends serving with
  default `VITE_PORT=5173` (Misskey: `/vite` ECONNREFUSED → stuck spinner;
  Sharkey: wrong assets → boot rejection). Fixed by killing listener PIDs
  from `ss` and verifying `VITE_PORT` in `/proc/<pid>/environ` plus real
  asset URLs (200), not `/`.
- My restart script's `wait_http` passed via the survivors — liveness
  checks must assert the NEW pid, not just any 200.

## Interop matrix (API-verified, all green at handoff)
- Automated: INTEROP_OK (mk/sk/pt3/pt4).
- Manual pt5: pt5→mk ✓, mk→pt5 ✓, pt5→sk ✓, sk→pt5 ✓ (post-restart; see
  Sharkey note).
- Sharkey finding: sk→pt5 notes silently never fanned out until backend
  restart. Root cause: Sharkey delivers via in-memory `userFollowersCache`
  (30-min TTL); it was populated by the first post before the follow accept
  landed, and the accept path never invalidated it (accepted Follow was a
  retry → `insertFollowingDoc` `alreadyFollowed` early-return skips the
  `follow` emit). After restart (cold cache) delivery works in ~6s. Genuine
  upstream Sharkey quirk (same family as the sk2-buddy workaround in the
  smoke script), NOT a pinetree bug. Misskey has no such cache and worked
  throughout.
- Incidental: pinetree caches remote actors under fresh local ids keyed by
  origin `uri` (pt3's view of ks1 ≠ pt5's id) — by design, not a bug.

## Windows browser URLs (all 200 at handoff)
- Keystone (ks1): http://localhost:5173
- Misskey UI (mk1, admin): http://localhost:3101
- Sharkey UI (sk1, admin): http://localhost:3102
- All passwords `password123`. pt3/pt4 API-only (:3103/:3104).
- Known limits: media/avatar URLs embed `127.0.1.x` (federation identity),
  so remote thumbnails won't render in Windows; text interop is unaffected.
  `127.0.1.x` itself is unreachable from Windows — expected.

## Open
- Sharkey staleness quarantines to its cache; if sk→pt delivery stalls
  again after follow churn, restart sk backend (or prod: upstream fix).
- Stop everything: `scripts/smoke-phase2.sh stop` (pids file now correct)
  plus: pt5 pinetree, pt5 proxy, 3 vite servers, keystone vite (see keystone log).
