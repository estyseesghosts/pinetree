# 2026-10-14 — Browser sign-in helper + registry shim (rig only)

Goal: let a human sign in on the real Misskey UI without `signin-flow`
(which pinetree does not implement), temp-rig only. All changes live in
`/tmp/opencode/mk-proxy.mjs`; nothing in the repo or the checkouts.

## What was added to the proxy

- **`GET/POST /login` helper**: plain username/password form. POST calls
  pinetree's single-shot `POST /api/signin`; on success serves a seed page
  that writes the exact session key the app boots from
  (`localStorage['account'] = {...MeDetailed, token}`, cf.
  `packages/frontend/src/i.ts`) and redirects to `/`. Bad credentials →
  re-rendered form with the backend error (401). Fixed one self-inflicted
  bug along the way: the payload was embedded as an object literal, so
  `setItem` stored `"[object Object]"` and boot died in `JSON.parse`
  (`i.ts`); now wrapped in `JSON.stringify`.
- **In-memory `i/registry/*` shim** (`get-all`, `keys`, `get`, `set`,
  `remove`; session-scoped `Map`, single namespace, correct shapes incl.
  204 for void writes and a `NO_SUCH_KEY` 404 envelope). Reason: logged-in
  boot unconditionally calls `i/registry/get-all` (pizzax store load, no
  `.catch`), so its 404 killed the whole app in boot.js
  (`SOMETHING_HAPPENED_IN_PROMISE`). Verified: `{}` → set 204 →
  `{"k":42}` → missing key 404.
- Shell now carries a small "Sign in (temporary rig helper)" link.

## Verified by execution (headless chromium)

- `/login` form → submits as alice → lands on `/` with the logged-in home
  UI (nav, composer entry, alice's timeline with her API-posted note).
  Screenshot: `/tmp/opencode/mk-loggedin.png`.
- Stock UI **sign-up** still 400s (null captcha keys → `UNSUPPORTED_FIELD`,
  logged previously) and stock **Sign In** still fails (`signin-flow`
  unimplemented) — both genuine backend gaps, unchanged.

## Not verified: posting from the UI composer

- Repeated headless attempts stall on Misskey's first-run setup wizard
  (rules agree + confirm modal, profile setup, push notifications,
  tutorial-done) covering the composer button. Wizard mechanics mapped
  along the way: MkSwitch root has no handler (input/XButton/label-span
  do; span+root clicks double-toggle), `updateAgreeNote` opens an
  `os.confirm`, `user-setup-continue` can resolve to 2 elements, several
  wizard buttons are text links without button roles. A human clicks
  through this in seconds; headless timing under vite dev proved too
  fragile to be worth more turns. The write path itself (`notes/create`)
  is proven via API + smoke suite.
