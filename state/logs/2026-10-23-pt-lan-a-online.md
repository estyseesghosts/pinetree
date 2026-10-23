# 2026-10-23 — pt-lan-a online with Keystone frontend (ops)

Goal: put a pinetree online on the LAN with the new Keystone web UI pointed at it.

## Did
- Built pinetree release native (`cargo build --release --offline`, 2m02s,
  warnings only; `target/release/pinetree.exe` fresh).
- New instance `instances/pt-lan-a/` (fresh DB; left `instances/pt/` smoke-test
  residue untouched): port 3002 (3000/3001 stay with birchtree A/B),
  `listen 0.0.0.0:3002`, `public_url http://10.88.111.34:3002`,
  `federation.allow_private_ips=true` (LAN federation capable),
  `unsafe.allow_local_http_for_testing=true` (private-LAN http opt-in),
  `transcode.enabled=false`. Unknown-key typo safety verified implicitly
  (server booted; only the expected unsafe-HTTP warning).
- `create-account --username=birch --password=password123` (`admin` is a
  preserved username), then `set-role --username=birch --admin`.
- Server up with `RUST_LOG=debug` (background shell).
- Verified live: `POST /api/meta` (misskey shape), `POST /api/signin` as
  birch (token ok), `POST /api/admin/update-meta {disableRegistration:true}`
  then confirmed `disableRegistration":true` in meta (registration closed,
  mirroring bt-lan-a), `POST /api/notes/create` + `local-timeline` round-trip,
  `POST /api/auth/session/generate` returns token + approval URL under the
  LAN `public_url` (Keystone MiAuth login path viable).
- CORS already present (`access-control-allow-origin: *` on `/api/*`
  preflight) — the HANDOFF_auth-signin gap is closed; no UI workaround
  needed.
- Keystone UI: flipped untracked `public/keystone.config.json` homeServer to
  `http://10.88.111.34:3002`, Metro on `:8081` (200 + correct config).
  Demo login: birch / password123 (MiAuth approve at the pinetree URL).

## Harness notes (mine, not server bugs)
- PowerShell 5.1 + curl: `[System.Text.Encoding]::UTF8` writes a BOM, which
  pinetree correctly rejects ("request body must be a JSON object"); use
  `(New-Object System.Text.UTF8Encoding $false)`. Same for `Set-Content
  -Encoding UTF8`. Also avoid inline `--data` with embedded quotes; use
  `--data @file`.

## Next
- Owner: open `http://10.88.111.34:8081/`, sign in as birch, verify timeline.
- Optional: demo accounts, follow-mesh stress (mirrors bt-lan-a 24-account
  setup), federation test vs bt-lan-a/b when those are back up.
- Uncommitted by design: `instances/pt-lan-a/` (local deployment),
  `keystone/public/keystone.config.json` (local wiring). No repo source
  touched.
