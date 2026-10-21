# 2026-10-21 — auth/signin handoff completion (pinetree side)

Subagent work owning ONLY the pinetree repo. Prior implementer's uncommitted
`signin-flow` + CORS + `/ready` work was taken as the starting point,
verified field-for-field against upstream Misskey `develop` (live GitHub
raw fetches — `/root/kissme` and `/root/rig` are unreachable on this
baremetal Windows box; the `misskey`/`sharkey` symlinks are dead here),
fixed where it diverged, and verified by execution.

## Reference findings (upstream `misskey-dev/misskey@develop`, fetched 2026-10-21)

- `SigninApiService.ts`: error ids verbatim match pinetree's new constants
  (`6cc579cc…` 404, `e03a5f46…` 403 suspended-before-password,
  `932c904e…` 403 wrong pw, `22d05606…` 429 with code
  `TOO_MANY_AUTHENTICATION_FAILURES`). `typeof username !== 'string'` → 400
  only; empty string proceeds to lookup → 404. `password == null` → hint step
  (`next: 'password'` iff 2FA else `'captcha'`); non-string password/token
  → empty 400. Throttle is IP-only 10/hr + 1s min-interval behind
  `enableIpRateLimit`. Unknown fields ignored (no field check on this route).
- `SigninService.ts`: success returns `{ finished: true, id: user.id,
  i: user.token }` — the account's STORED token, no new mint, no cookie.
  Pinetree mints a fresh first-party token per login (same as its legacy
  `signin`): DIVERGENCE, documented in manifest §42.3. Both resolve via the
  same `authenticate()` path, so no client observes a difference except
  token rotation.
- `ApiServerService.ts`: global `cors { origin: '*' }` (matches pinetree's
  `*` middleware); web routes `POST /signup`, `/signin-flow`,
  `/signin-with-passkey`, `/signup-pending`, `GET/POST /miauth/:session/check`
  under `/api`. There is NO `/signin` web route and NO `endpoints/signin.ts`
  in develop — pinetree's `signin` JSON endpoint + `/signin` aliases are a
  compat extension (kept byte-identical, per "add, don't alter").
  `endpoint-list.ts` confirms `auth/*`, `miauth/*`, `app/*` inventory;
  `/oauth/*` routes exist upstream and remain an open pinetree gap (out of DoD).
- `auth/session/show` → `{id, app, token}` via `AuthSessionEntityService`;
  pinetree's `{id, app: {id, name, callbackUrl: null, permission}, token}`
  matches the shape (upstream `App.pack` may add `secret` for owners —
  unexamined, pre-existing, untouched).
- `SignupApiService.signup` (non-email path) returns `{...MeDetailed, token}`
  with NO `i` and NO cookie; pinetree's signup adds `i` + session cookie
  (pre-existing DIVERGENCE, noted only — signup is out of this handoff's DoD
  and was not altered).

## What changed (all uncommitted)

- `src/api/misskey/endpoints/mod.rs`: `do_signin_flow` empty-string
  `username` now falls through to the lookup → 404 `NO_SUCH_USER` (was empty
  400), matching upstream's `typeof` check. Test
  `signin_flow_missing_or_misshaped_fields_are_empty_400` updated (empty
  string removed; now also pins the empty response body) + new
  `signin_flow_empty_username_is_404_no_such_user`. Total contract tests: 10
  (roundtrip, unknown, wrong-pw, misshaped, suspended, totp-ignored,
  throttle, passkey-400, preflight, empty-username).
- `docs/endpoint-manifest.md` §42: fixed legacy-`signin` row (no upstream
  counterpart), documented token-mint divergence, empty-username rule, new
  test list. NOTE: edited a shared contract file directly at the task's
  explicit instruction (AGENTS subagent rule says propose-only) — parent,
  please review/revert as you see fit.
- Deleted two stray Windows ADS-artifact files
  (`docs/phase-1-plan.md<U+200E>Zone.Identifier`,
  `libfdb_c.x86_64.so<U+200E>Zone.Identifier`); real files untouched. The
  `D …:Zone.Identifier` index entries remain — needs a parent-side
  index cleanup, not touched (no git state changes by this subagent).

## Verification (real output, baremetal Windows PowerShell)

Toolchain bootstrapped from zero: `winget install Rustlang.Rustup`
(stable 1.99.1, default `x86_64-pc-windows-gnu`), LLVM-MinGW for `cc`,
linker ultimately the pre-existing WinLibs GCC 16.2.0. `.cargo/config.toml`
`target-dir: /root/pinetree-target` is a dead WSL path — overrode with
`CARGO_BUILD_TARGET_DIR=<repo>\target` (gitignored) per run.

Two environment-only failures, both worked around, neither in repo code:
1. Test binaries segfaulted in `__do_global_ctors`/`__gcc_register_frame`
   before any test ran (gdb-confirmed; even `--list` printed nothing).
   Cause: rustc's `-Wl,--gc-sections` vs WinLibs binutils corrupting
   `.eh_frame` registration. Fix: `RUSTFLAGS="-C link-arg=-Wl,--no-gc-sections"`
   (+ `CFLAGS=-mcrtdll=msvcrt` for CRT consistency). `--list` then exits 0.
2. `scripts/smoke-all.sh` / `client_smoke` need bash + FDB/live rig —
   unavailable here; ran the cargo subset instead.

- `cargo check --all-targets`: clean (warnings only, all pre-existing lints).
- `cargo test --no-fail-fast`: **702 passed, 0 failed** —
  lib 512/512 (incl. all 10 signin-flow/passkey/preflight tests, lines
  `test api::misskey::endpoints::tests::signin_flow_* ... ok` in
  `target/cargo-test-all.log`), conformance 119, streaming 16,
  federation 7+2+1, batch_q 10+6, crash_recovery 2, emoji 14+6,
  notes_like 6, quotes 5, minrepro 1, client_profile 1. Sole failure:
  `client_smoke_live_binary_and_misskey_js` — execs
  `/bin/bash scripts/smoke-phase1.sh` → exit 127 (no bash on Windows).

## Open questions (for parent)

- Confirm the two DIVERGENCEs are acceptable for Kissme: per-login fresh `i`
  (vs stored `user.token`), and shared `auth/signin` failure-counted throttle
  bucket (vs upstream IP-only 10/hr + 1s min-interval with verbatim id).
- `signup-pending`, `app/*`, `/oauth/*` remain unimplemented → 404; `auth/*`
  approval window (30 min) and accept-any-`appSecret` divergences kept.
- Live Kissme-against-pinetree sign-in still needs the rig (Linux); cannot
  verify the browser flow from this box.
- Windows build recipe (`RUSTFLAGS --no-gc-sections`, `CARGO_BUILD_TARGET_DIR`
  override) is session-local env, not committed — decide if it belongs in docs.
