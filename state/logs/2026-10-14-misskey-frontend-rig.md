# 2026-10-14 — Temporary Misskey-frontend rig (throwaway, outside repo)

Goal: let the user browse pinetree with the real Misskey web UI without
changing the repo. Misskey's frontend calls `${origin}/api/...`
(same-origin) and pinetree sends no CORS headers, so both servers are merged
onto one origin with a reverse proxy.

What changed (all in `/tmp/opencode/`, nothing in the repo):
- **`mk-proxy.mjs`** (new, `bun` no-deps): listens on `PROXY_LISTEN`
  (default `127.0.0.1:8080`); routes `/api|/miauth|/signup|/signin|/files|/identicon|/healthz|/.well-known`
  to pinetree (`PINETREE_UPSTREAM`, default `127.0.0.1:3000`), everything
  else to vite dev (`VITE_UPSTREAM`, default `127.0.0.1:5173`). Upstream
  responses pass through untouched; unreachable upstreams get a 502 naming
  the expected server. No WebSocket upgrade forwarding (HMR won't hot-reload
  through it; plain loads work).
- **`mk-frontend.sh`** (new): one-shot boot — builds `target/debug/pinetree`
  if absent, picks free ports, starts pinetree with `--public-url` set to the
  proxy origin (so packed avatar/file URLs match the browser origin), starts
  the proxy, waits on `/healthz` for both, warns when vite is absent, traps
  cleanup. `DATA_DIR` defaults to a fresh mktemp dir (removed on exit).

What was verified (live, pinetree :3101 + proxy :8181 + python stub as vite :5179):
- `GET /healthz` via proxy → `ok`; `POST /api/meta` → Misskey meta JSON.
- `POST /signup` via proxy → token; packed `avatarUrl` correctly used the
  proxy `--public-url` (`http://127.0.0.1:8181/identicon/...`).
- `GET /miauth/testsession?...` → server-rendered approve HTML with app name.
- `GET /` and `GET /vite/@vite-client` → served by the vite branch (stub
  marker / stub 404, not pinetree JSON).
- Authenticated round-trip via proxy: `i` → `notes/create` → `notes/show`
  (text intact); multipart PNG `drive/files/create` + `GET /files/:id`
  → `cmp` byte-identical.
- Vite down: `GET /` → 502 naming vite + the `pnpm --filter frontend watch`
  hint, while `POST /api/meta` still 200.
- Cleanup verified: no stray processes; test data/stub/logs removed.

Open questions / limits:
- Real vite dev never ran here (no node/pnpm in this env, `pnpm install`
  pulls GBs). The vite branch is proven only against a stub; Misskey's vite
  also requires `.config/default.yml` with `url: http://127.0.0.1:<proxy>/`.
- Even with the real UI, expect partial function: Phase 1 endpoint subset
  only, no `/streaming` WebSocket route (live timelines stay empty), many
  pages 404/`UNSUPPORTED_FIELD`. Not compat work — demo rig only.
- `pkill -f "http.server 5179"` matched the invoking shell's own cmdline and
  killed it; prefer exact-pid kills (lesson, no harm: rig survived).
