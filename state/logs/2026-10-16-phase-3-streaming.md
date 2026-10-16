# 2026-10-16 — Phase 3 streaming workstream (in progress)

## Profile declared

`docs/phase-3-profile.md` (frozen): target clients are Misskey web +
`misskey-js` `2026.10.1-alpha.0`, Keystone (`pinetree` target), Sharkey
`2025.4.7` (interop parity only). Streaming §2 + REST §3 in scope; profile
§5 out (antennas, polls/vote, drive folders, clips/channels/chat/pages/
Reversi/lists, grouped/featured/mentions timelines, `signin-flow`, OAuth,
Mastodon API). `docs/endpoint-manifest.md` extended with a Phase 3
superseding §9 pointing at the profile (unknown channel → explicit
`UNSUPPORTED_CHANNEL` error frame; auth-refused credential channels stay
silently ignored per Misskey parity).

Keystone recon (exhaustive grep of `/root/keystone/frontend/src`): REST
only, zero streaming usage. Needs `users/notes`, `users/followers|following`
(typed `UserLite[]` — CONFLICT with Misskey `Following[]`; profile fixes
the Misskey shape, Keystone maps `x.follower!/x.followee!` in its own repo
— logged as cross-repo follow-up, not fixed here), `notes/replies`,
`notes/search`, `notes/search-by-tag`, `notes/unrenote`,
`notifications/mark-all-as-read`, `drive/files` + `delete` + `update`,
`i/revoke-token` (best-effort). Misskey web needs streaming + `users/notes`,
`notes/children|renotes|conversation`, `users/followers|following`,
`drive/files|folders`, `notes/search`.

## Streaming implementation (mine; endpoint agent works REST in parallel)

- `src/streaming/hub.rs` (new): `Hub` (tokio broadcast, at-most-once,
  lag → resubscribe), `HubMessage` (note/react/unreact/delete/main),
  `BroadcastStreamingSink` (parses `StreamingEvent` payloads; `push_main`
  for packed main events). Unit tests: 3/3 green.
- `src/streaming/server.rs` (new): `GET /streaming` upgrade (`?i=` or
  Bearer; anon OK; bad token → 401; suspended → 403; token without
  `read:account` → 401). Protocol: connect/disconnect, `channel`/`ch`
  (only `readNotification` interpreted), top-level `readNotification`,
  `subNote`+`s`+`sr` / `unsubNote`+`un` (refcounted, 1536 LRU cap),
  32-channel cap, 2-min idle reap + 60-s pings, `pong:true` → `connected`.
  Timeline filtering mirrors REST eligibility
  (`note_eligible_for_channel` in `queries.rs`, additive); capture uses
  the Misskey `Connection` light gate (`capture_gate`, additive).
  Unit tests: 3/3 green.
- Worker (`jobs/worker.rs`): `Notify` jobs expand to `main` events
  (`notification` always; `mention`/`reply`/`renote` packed notes;
  `follow`+`followed`; `receiveFollowRequest`) only on fresh insert
  (replay-after-crash skips hints — at-most-once, documented).
  `StreamingPublish` passes through untouched except `Unfollowed`, which
  the worker packs (followee for the follower's `main`). Pass-through
  preserves the b12 retry-exhaustion behavior for opaque payloads
  (caught by conformance during development, fixed before landing).
- Commands: `update_profile` enqueues `MeUpdated` on both commit paths
  (federated + local) — required an `AccountStore::update_account`
  `jobs` parameter; redb now delegates to `update_account_in_txn`
  (deleted ~50 duplicated lines); 2 test `Mem`s + 1 federation caller
  updated. `unfollow` enqueues `Unfollowed`.
- `pack_notification` moved `endpoints/mod.rs` → `packers.rs` (shared by
  REST + worker). `NotificationStore::mark_all_read` added (ports + redb
  + 2 test Mems) for `readNotification` / `mark-all-as-read`.
- `ApiState.hub`, `GET /streaming` route, `main.rs` `hub_and_sink(1024)`;
  `tests/federation.rs` + `tests/conformance.rs` harnesses updated.
- Deps: axum `ws` feature, `futures-util` (main), `tokio-tungstenite` +
  `reqwest` (dev).

## Verification (executed)

- `cargo test --test streaming`: **8/8 green** (live server + real WS
  clients): anon global streams public notes; 401s at upgrade;
  anon-home silent + `UNSUPPORTED_CHANNEL` frame; main
  follow/followed/notification (order-robust); capture
  reacted/unreacted/deleted; home only followed authors; followers-only
  notes skip non-followers; meUpdated on `i/update` + `readNotification`
  → `readAllNotifications` + `isRead` flip.
- Full `cargo test`: lib 200+phase3-agent-tests / conformance 50-51 green
  except 3 agent-owned `phase3_*` endpoint tests that were failing when
  observed mid-edit (agent still working; not mine — my suites green).
- b12 regression caught + fixed (pass-through); conformance green after.

## Open / next

- Endpoint agent (background): profile §3 REST + its contract tests.
- Then: misskey-js `Stream` bun contract test (real client lib), Keystone
  REST sweep, four-server topology re-run, Current-State/wiki refresh,
  Phase 3 exit review. Suspended-token WS 403 has no setter path to test
  (no suspend API) — noted, untested.
