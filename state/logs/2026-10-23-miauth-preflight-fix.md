# 2026-10-23 — MiAuth CORS preflight fix (follow-up)

Supersedes/completes `2026-10-23-miauth-web-flow-fix.md`: after the
page/redirect fix, the browser approved and followed the 302 to
Keystone's `/auth/callback`, but the callback screen showed
"Could not reach the instance (NetworkError...)". Nodeinfo loaded fine
in the same browser, so the host was reachable — the failing call was
Keystone's completion poll `POST /api/miauth/{session}/check`.

## Root cause (pinetree)
CORS preflights were answered only on routes carrying
`.options(preflight)`. The three MiAuth routes (and
`/api/drive/files/create`) had none, so Axum's default 405 — without
`allow-methods`/`allow-headers` — failed the browser preflight and
`fetch` died with NetworkError. The old server's own log showed it:
`POST /miauth/… → 302` then `OPTIONS /api/miauth/…/check → 405`.

## Fix (`src/api/misskey/endpoints/mod.rs`)
- `.options(preflight)` on `/miauth/{session}`,
  `/miauth/{session}/check`, `/api/miauth/{session}/check`,
  `/api/drive/files/create` (drive upload would have broken the same
  way from the Keystone UI next).
- No handler logic touched; matches the convention every neighboring
  route already follows.

## Verification
- Live: `OPTIONS /api/miauth/{session}/check` → `204`,
  `allow-origin: *`, `allow-methods: GET, POST, OPTIONS`,
  `allow-headers: Content-Type, Authorization`.
- `cargo test --release --offline --lib miauth` → 7 passed.
- `cargo test --release --offline --lib preflight` → 1 passed.
- `rustfmt --edition 2024 --check` on both touched files → clean
  (applied its line-wrap, re-ran tests after).
- Server restarted on the fresh binary (same config/DB); Metro UI untouched.

## Open
- Owner retry of the full browser login.
- Both fixes uncommitted (`auth.rs`, `endpoints/mod.rs` + two state logs).
