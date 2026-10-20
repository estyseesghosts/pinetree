# Phase 3 exit criteria — Client compatibility and streaming

> **Status at HEAD (2026-10-20 freshness pass)** — boxes left as recorded (Phase 3 exit); **no boxes changed**, tests not re-run.
> - Cited files exist at HEAD: `tests/streaming.rs`, `tests/client_profile.rs`, `scripts/smoke-phase3.sh`, `src/streaming/hub.rs`, `src/streaming/server.rs`.
> - **Still true:** `i/revoke-token` is fully implemented (`src/api/misskey/endpoints/mod.rs:6770`), so the older "no-op" note in `docs/phase-3-profile.md` §7 is stale.
> - **Stale claims elsewhere (not edited here):** streaming `updated` is emitted (`src/streaming/server.rs`, Sharkey parity) while `docs/endpoint-manifest.md` §9 still says it is not; `users/followers|following` moderator bypass is wired but always false until roles land. See `docs/TODOS.md` "Manifest/profile stale lines".
> - **Out of Phase 3 scope, still open:** `hashtag`, `channel`, `drive`, `clip`, `admin`, and the other missing streaming channels (`docs/TODOS.md` "streaming holes").

**Status:** Tracking checklist (proposal §9 Phase 3).

**Exit:** All endpoints in the declared compatibility profile
(`docs/phase-3-profile.md`, frozen 2026-10-16) pass contract tests against
the target client versions.

## Checklist

- [x] Compatibility profile declared (target clients, in-scope
  endpoints/channels/fields, explicit non-goals): `docs/phase-3-profile.md`;
  manifest extended (`docs/endpoint-manifest.md` §9).
- [x] Streaming: authenticated WS `/streaming`, connect/disconnect,
  `main`/`homeTimeline`/`localTimeline`/`hybridTimeline`/`globalTimeline`,
  note capture (`subNote`/`unsubNote` + aliases, `noteUpdated`
  reacted/unreacted/deleted), notification events, fed from
  `StreamingSink` jobs (`src/streaming/hub.rs`, `src/streaming/server.rs`).
- [x] Endpoint coverage per profile §3: `users/notes`, `notes/replies`,
  `notes/renotes`, `notes/children`, `notes/conversation`,
  `notes/unrenote`, `users/followers`, `users/following`, `drive/files`,
  `drive/files/show|update|delete`, extended `i/notifications`,
  `notifications/mark-all-as-read`, `i/revoke-token`, `notes/search`,
  `notes/search-by-tag`.
- [x] Contract tests vs the real clients (no mocks):
  `tests/streaming.rs` (8 live-WS tests), `scripts/smoke-phase3.sh` +
  `tests/client_profile.rs` (real `misskey-js` `APIClient` + `Stream`).
- [x] Full `cargo test` green (lib + conformance + crash_recovery +
  client_smoke + client_profile + federation + federation_interop +
  minrepro + streaming): 283 green, executed 2026-10-16.
- [x] Live four-server topology re-verified (`scripts/smoke-phase2.sh`
  → `SMOKE_OK` + `INTEROP_OK` on Phase 3 binaries 2026-10-16; rig left
  running with KEEP=1; pinetree+keystone via pt5 :3000 + vite :5173,
  misskey+keystone via :3101 + vite :5176, sharkey via Keystone :5177 and
  Sharkey-native UI on :3102 with its own vite :5174).
- [x] Keystone sweep green against the profile (`scripts/keystone-sweep.sh`
  → `KEYSWEEP_OK` 41/41, 2026-10-16; `users/followers|following` shape
  follow-up logged for the keystone repo).

## Out of scope (proposal §9 non-goals, profile §5)

Mastodon API; antennas/clips/pages/channels/chat/Reversi and legacy
features; polls/`notes/polls/vote` and quote-metadata federation;
RFC 9421 signatures; OAuth; multi-node/FDB (Phase 4); scale tuning
(Phase 5).
