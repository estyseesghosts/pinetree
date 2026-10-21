# HANDOFF — pinetree auth/signin must match Misskey exactly

## Why
Kissme (Misskey frontend, `split-project`) cannot sign in against pinetree:
its password form calls `signin-flow`, which pinetree does not implement
(`UNKNOWN_API_ENDPOINT`). A console one-liner using `signin` + `i` works,
so the gap is specifically the session/auth flow surface, not accounts or
tokens. Goal: wire pinetree against Misskey's frontend with zero client-side
workarounds — that means pinetree's auth system must be behavior-identical
to Misskey's.

## What to investigate (Misskey reference)
Primary reference: `/root/kissme/kissme-be` (Misskey fork, same lineage as
the frontend we test against). Fallback: upstream Misskey source/docs.

1. **Endpoint inventory** — every `auth/*`, `signin*`, `miauth/*`, `oauth/*`
   route Misskey serves: names, params, response shapes, error codes.
   Known pinetree gaps: `signin-flow`. Known present: `signin`,
   `auth/session/show` (verify each matches upstream field-for-field).
2. **The signin-flow state machine** — request/response for each step
   (password, captcha, 2FA/totp, passkey), how `next` hints work, how the
   flow token converts into an app token. Kissme's `MkSignin.*` components
   (`MkSignin.vue`, `.password`, `.totp`, `.passkey`, `.input`) are the
   client contract — each server call they make must succeed unchanged.
3. **Token semantics** — what `signin` returns vs what `signin-flow` mints
   (scope, format, how `i` identifies the caller), MiAuth and OAuth grant
   support, session expiry/revocation.
4. **CORS** — browsers call `/api/*` cross-origin in split deployments;
   pinetree currently sends no `Access-Control-Allow-Origin` (observed:
   preflight to `/api/meta` fails). Check what Misskey sends and match it.

## Definition of done
- Kissme password login (u0/password123 against the rig's pt-a) works with
  no console workaround, including any second-factor UI the server demands.
- `cargo test` + `bash scripts/smoke-all.sh` still green; new contract
  tests pin each auth endpoint's params/responses against the reference.
- Record findings in `docs/` + a dated `state/logs/` entry per AGENTS.md.

## Context
- Rig: `/root/rig` (8 nodes; pt-a `http://127.10.0.11:3000`, accounts
  u0..u7/password123). Kissme dev at `:5173` proxied to pt-a.
- Working bypass (proves the rest of the stack is fine):
  `POST /api/signin` → `POST /api/i` → store as `account` → reload.
- Do not change login behavior for existing `signin` callers; add, don't alter.
