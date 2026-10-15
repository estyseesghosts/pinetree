# 2026-10-15 — Lab teardown + push to master

## Teardown (executed)
- `scripts/smoke-phase2.sh stop` (backends, proxy, compose down), plus
  pid-only kills of the Misskey/Sharkey vite devs (:5173/:5174) and the
  mk-proxy rigs (:8181/:8182). Verified: no listeners on
  3101–3104/5173–5174/8181–8183/15432/16379; `docker ps` empty.
- Old 14G `/mnt/c/.../pinetree/target` deleted earlier; `/mnt/c`
  `fedifedifedi` checkouts intentionally left alone.

## Push (executed)
- Committed everything outstanding (7 modified + 7 new: `.cargo/`,
  `.gitignore`, README, howto, 2 wiki pages, both smoke scripts,
  `scripts/bench-local.py`, 5 state logs) as `37a38f0` on master.
- `git push origin master`: `91f0338..37a38f0 master -> master` green.
- Working tree clean afterwards (only git-ignored paths remain).
