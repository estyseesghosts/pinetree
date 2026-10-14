# 2026-10-14 — Misskey frontend live against pinetree (follow-up to frontend-rig log)

Goal: set up the real Misskey web UI (2026.10.1-alpha.0 checkout at
`./misskey/`) and run it against pinetree via the `/tmp/opencode` proxy rig.

## Environment setup (new in this session)

- node v24.18.0 + pnpm present. Installed repo-pinned pnpm 12.9.1
  (`npm install -g pnpm@12.9.1`); backend engines allow
  `^22.22.2 || ^24.17.0 || ^26.4.0`, so node 24 passes.
- Created `.config/default.yml` (git-ignored, `url: http://127.0.0.1:8181/`;
  vite dev reads it for `allowedHosts`). No source files touched.
- `pnpm install --no-frozen-lockfile` in `misskey/`: done in 9m 8.9s.
- Built workspace pre-deps the frontend needs (all have `built/` mains):
  `pnpm --filter i18n build`, `pnpm --filter misskey-js build`,
  `pnpm --filter icons-subsetter build` (its `built/*.css` is imported by
  `src/_boot_.ts`).
- `dnf install`ed Fedora system libs for headless chromium
  (nspr/nss/atk/cups/libdrm/libX*/mesa-gbm/alsa/pango), then
  `npx playwright install chromium` (browsers were absent).

## Proxy changes (`/tmp/opencode/mk-proxy.mjs`)

- Serves a generated dev HTML shell for app routes (Misskey renders its
  shell in the backend; vite dev 302s `/` → `/vite/` which 404s). The shell
  mirrors `views/base.tsx` dev mode: HMR client, `VERSION`/`CLIENT_ENTRY`/
  `LANGS` (version + 28 langs read from the checkout), live pinetree
  `meta` embedded in `#misskey_meta`, `/vite/loader/boot.js` +
  `/vite/loader/style.css`, splash div.
- Serves backend static prefixes like ClientServerService:
  `/static-assets/*` → `packages/backend/assets`,
  `/client-assets/*` → `packages/frontend/assets`,
  `/assets/*` → `built/_frontend_dist_` (where the i18n build wrote
  `locales/*.json`), plus `/favicon.ico`, `/apple-touch-icon.png`.
  Fixed a traversal-guard bug (trailing-slash base + `sep` never matched).

## Debugging detour (recorded so nobody repeats it)

- First vite boot failed: `Failed to resolve entry for package "i18n"`
  → fixed by building workspace deps (above).
- Then `Failed to resolve import "icons-subsetter/built/..."` persisted
  across vite restarts. Probed oxc-resolver directly with vite's exact
  alias/extensions/conditions/mainFields/tsconfig — all resolved fine.
  Root cause was a stale on-disk `packages/frontend/node_modules/.vite/deps`
  optimizer cache written during the failed first run:
  `rm -rf` it → boot module HTTP 200. (Also noted: `pkill -f` once matched
  the invoking shell's own cmdline; use exact pids.)

## Verified by execution (headless chromium via playwright)

- Visitor UI renders through the proxy: title/instance name, splash gone,
  "Sign up / Sign In / timeline / DEV BUILD".
- Note created via pinetree API renders in the live UI timeline
  ("alice @alice … hello from pinetree backend #pinetree").
- API round-trips through the proxy re-verified (signup/i/notes/media
  bytes identical).

## Two real backend gaps found (not rig bugs — UI cannot work around them)

1. **UI signup always 400s**: the web client POSTs `/signup` with null
   captcha keys (`hcaptcha/m-captcha/g-recaptcha/turnstile/testcaptcha-response`).
   Pinetree's `SIGNUP_FIELDS` allowlist (`endpoints/mod.rs`) answers
   `UNSUPPORTED_FIELD` (reproduced via curl). Frozen contract says unknown
   fields → 400, but the first-party client always sends these nulls —
   contract likely needs a null-tolerance carve-out (Phase 2/3 call).
2. **UI password sign-in uses `signin-flow`** (multi-step state machine),
   which pinetree does not implement (`UNKNOWN_API_ENDPOINT` in the
   dialog; manifest covers only single-shot `POST /signin`). So browser
   login is currently impossible; API-token auth works.
- Expected noise, benign: `/streaming` WS fails (no route until Phase 2/3),
  `/sw.js` 404, `GET /api/emojis` 405, `POST /api/stats` +
  `/api/charts/active-users` 404s.

## Running now (left up for the user)

- pinetree :3101 (data `/tmp/pinetree-rig-LW8kDo`, ephemeral),
  proxy :8181, vite :5173. UI at `http://127.0.0.1:8181/`.
  Login `alice` / `alicepw123` (API-level; UI sign-in blocked per gap #2).
- Screenshots: `/tmp/opencode/mk-ui.png`, `/tmp/opencode/mk-timeline.png`,
  `/tmp/opencode/mk-signedup.png` (error dialog), `/tmp/opencode/mk-loggedin.png` (n/a).
- Headless scripts kept in `/tmp/opencode`: `mk-proxy.mjs`,
  `mk-frontend.sh`, `mk-verify.mjs`.
