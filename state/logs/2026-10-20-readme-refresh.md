# 2026-10-20 — README refresh (capabilities as-built)

## What changed

- Rewrote `README.md` (was a 3-line stub in the working tree) into a detailed,
  as-built account: what the single-node server does today (~95 routed API
  endpoints across accounts/auth/registration, notes+polls+edits+quotes+threads,
  reactions/follows/blocks/mutes, notifications/drafts/import-export, streaming,
  drive, federation, durability), what it deliberately does not do (no cluster,
  no frontend, no Mastodon API, no emoji subsystem, no antennas/clips/channels/
  pages/folders/lists/chat, no OAuth, WP7–WP14 pending), and known rough edges
  (loopback-only interop, substring search, simplified featured ranking, lazy
  RSA keygen, client-side word mutes, stderr-only observability).
- Two findings recorded while gathering facts (both now stated in the README):
  - Batch M WP4 wired the `AbuseReport` store + app ops + outbound `Flag`
    rendering, but `users/report-abuse` / `admin/abuse-user-reports` are NOT
    routed in the dispatcher and inbound `Flag` has no inbox arm
    (`forward_abuse_report` has no callers). User-facing abuse reporting is
    therefore listed under "does not do".
  - `users/followers|following` + private reactions + polls + edits + drafts +
    blocks/mutes + imports/exports + suspension/invites/instance-blocks all
    confirmed routed (dispatcher grep); counts and claims cross-checked against
    `docs/endpoint-manifest.md` §§10–26 and `docs/misskey-parity-plan.md`.

## Verified

- `cargo test --offline` on the current (dirty) tree: 424 lib + 100
  conformance + 29 integration (1 client_profile + 1 client_smoke + 2
  crash_recovery + 2 federation + 2 federation_elixir + 1 federation_interop +
  1 minrepro + 5 quote_fixtures + 14 streaming) = **553 green, 0 failed**.
  README states these numbers.
- `scripts/smoke-all.sh` last full run per `state/logs/2026-10-20-akkoma-compat.md`:
  SMOKE_ALL_OK (phase2 four-server SKIP — no Node 22 in that env);
  `scripts/keystone-sweep.sh` 41/41 there. Not re-run in this session (README-only
  change; no product code touched).

## Open

- Nothing product-side. The working tree still holds unrelated uncommitted
  changes (bench scripts, `src/activitypub/signatures.rs`,
  `src/api/misskey/*`, `src/application/commands/*`, `src/federation/delivery.rs`,
  `src/main.rs`, bench docs/logs). This commit covers `README.md` + this log
  only, deliberately.
