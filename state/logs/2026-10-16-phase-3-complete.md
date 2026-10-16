# 2026-10-16 — Phase 3 complete (exit review green)

## Exit criterion (proposal §9)

Every endpoint in the declared compatibility profile
(`docs/phase-3-profile.md`, frozen) passes contract tests against the
target client versions (Misskey web + `misskey-js` 2026.10.1-alpha.0,
Keystone HEAD pinetree-target, Sharkey 2025.4.7 interop parity).
All boxes in `docs/phase-3-exit-criteria.md` checked with executed evidence.

## What was built

- **Profile** (`docs/phase-3-profile.md` + manifest §9): in-scope streaming
  channels/events + 17 REST endpoints + explicit non-goals, after
  exhaustive usage surveys of all three clients (subagent recon).
- **Streaming** (`src/streaming/hub.rs`, `server.rs`): broadcast hub fed by
  durable jobs, `GET /streaming`, 5 channels, capture, notifications;
  `MeUpdated` on profile updates (both commit paths — needed an
  `AccountStore::update_account` jobs parameter; redb deduped onto
  `update_account_in_txn`), `Unfollowed` worker expansion, `Notify` → main
  fan-out on fresh insert only; `pack_notification` moved to `packers.rs`;
  `NotificationStore::mark_all_read`. b12 worker-retry regression caught by
  conformance mid-work and fixed (pass-through for opaque payloads).
- **REST** (background implementer, `state/logs/2026-10-16-phase-3-endpoints.md`):
  queries + commands + ports + redb + dispatch arms + 11 tests. Accepted
  deviations logged there (tokenId no-op, conversation order,
  search/drive error codes, shared rate buckets).

## Verification (all executed)

- `cargo test --offline`: **283 green** (211 lib incl. 10 `phase3_*` + 6
  streaming unit, 51 conformance, 2 crash, 1 client_smoke, 1
  client_profile, 2 federation, 1 interop env-gated, 1 minrepro, 13
  streaming). No new warnings attributable to Phase 3 (repo has
  pre-existing unused-import warnings; no clippy/rustfmt installed here).
- `scripts/smoke-phase3.sh` → `SMOKE3_OK` (real `misskey-js`
  `APIClient` + `Stream` under bun; race fixes: notification polling,
  subNote settle wait, misskey-js error-envelope assertions).
- `scripts/keystone-sweep.sh` → `KEYSWEEP_OK` **41/41** (Keystone's exact
  calls incl. multipart upload, untilId pagination, honest 404s; one
  script-side 429 from the shared delete bucket fixed with spacing).
- `KEEP=1 SHARKEY_NODE_BIN=/usr/sbin bash scripts/smoke-phase2.sh` →
  `SMOKE_OK` + `INTEROP_OK` on Phase 3 binaries (fresh DBs, all four
  servers). Rig left running.
- Topology browsable: Keystone→pinetree `:5173` (ks1), →Misskey `:5176`
  (mk1), →Sharkey `:5177` (sk1); Sharkey-native UI wired on `:3102` +
  its vite `:5174` (prebuilds, backend restarted with `VITE_PORT=5174`,
  sk1 signin + WebFinger verified). Guest `172.28.54.228`; all passwords
  `password123`. Owner fixed a crossed Windows portproxy (local 5173 →
  guest 5177) during the session.

## Open / carry to Phase 4

- Keystone `FollowList` expects `UserLite[]`; profile fixes Misskey
  `Following[]` — one-line map in the keystone repo (its file:line +
  mapping in the profile §6).
- Live-untested by design: 32/1536 caps, 2-min idle reap, suspended-token
  WS 403, remote notes over streaming, search host/userId filters.
- `smoke-phase2.sh` completion pipe wedges when `start_bg` subshells
  linger (two runs showed post-`INTEROP_OK` hangs; workaround: kill the
  lingering `bash scripts/smoke-phase2.sh` processes — backends survive).
  Worth a robustness look before Phase 4 leans on it.
- `src/streaming/{connection,channels,subscriptions}.rs` are still empty
  Phase-0 stubs (all logic lives in `hub.rs`/`server.rs`); harmless but
  should be removed or documented in Phase 4 cleanup.
- Deps added: axum `ws`, `futures-util` (main); `tokio-tungstenite`,
  `reqwest` (dev).
