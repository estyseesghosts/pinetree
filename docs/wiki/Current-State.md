# Current state (2026-10-16 — Phase 3 complete, quote posts + branched replies green)

## Phase status

| Phase | Status | Evidence |
|---|---|---|
| 0 Contracts | Done | `docs/phase-0-exit-criteria.md`; FDB client verified live (`docs/fdb-verification.md`) |
| 1 Single-node server | Done | `docs/phase-1-exit-criteria.md`; `state/logs/2026-10-14-phase-1-complete.md` |
| 2 Minimal federation | **Done** | `docs/phase-2-exit-criteria.md`; `state/logs/2026-10-14-phase-2-complete.md` |
| 2.5 Cleanup | **Done** | Slices 0–9, one commit each: `state/logs/2026-10-15-phase2-5-*`; `cargo test` 251 green; `scripts/smoke-phase2.sh` → `SMOKE_OK` |
| 2.6 Federation hardening | **Done** | Slices 1–12 (`state/logs/2026-10-15-phase2-6-*`): key ownership, inbox URLs, follow requests, reply visibility, request bodies, media safety, content addressing, passwords/throttling, delivery reliability, AP collections, RSA→`ring` signer |
| 3 Client compatibility + streaming | **Done** | `docs/phase-3-exit-criteria.md` (all boxes checked); `state/logs/2026-10-16-phase-3-*.md`; `cargo test` 283 green; `scripts/smoke-phase3.sh` → `SMOKE3_OK`; `scripts/keystone-sweep.sh` → `KEYSWEEP_OK` 41/41; `scripts/smoke-phase2.sh` → `SMOKE_OK` on Phase 3 binaries |
| Quote posts (FEP-044f) | **Done** | `docs/quote-posts.md`; `state/logs/2026-10-16-quote-posts.md`; conformance Group Q; 4-server + elixir interop green |
| Branched replies | **Done** | `docs/replies.md` Slices A–C; `state/logs/2026-10-16-replies.md`; conformance Group R; reply-burst stress `STRESS_OK items=28` |
| 4 FoundationDB multi-node | Planned | — |
| 5 Scale and hardening | Planned | — |

## Repository layout

```
sns-server-proposal.md   product/architecture proposal (source of truth)
AGENTS.md                rules for agents; current phase
docs/                    frozen/draft contracts, phase plans, exit criteria, specs, this wiki
state/logs/              dated append-only work logs (real command output)
src/                     the crate (see About → How it is built)
tests/                   conformance (+ Groups Q/R), crash_recovery, client_smoke,
                         client_profile, federation, federation_interop,
                         federation_elixir, quote_fixtures, streaming, minrepro
scripts/                 smoke-all.sh, smoke-phase1/2/3.sh, keystone-sweep.sh,
                         bench-local.py, interop/ (compose + loopback proxy)
foundationdb/, redb/     upstream reference checkouts (git-ignored, read-only)
misskey, sharkey         symlinks to reference checkouts (git-ignored, read-only;
                          keep the targets on the native Linux fs under WSL)
.cargo/config.toml       git-ignored machine-local settings (cargo target-dir on ext4)
```

## Implemented Misskey API surface

JSON POST at `/api/{endpoint}`, credentials via `i` body field or `Authorization: Bearer`.

`meta`, `signup`, `signin`, `admin/accounts/create`, `miauth/gen-token`, `auth/session/generate|show|userkey`, `auth/accept`, `GET|POST /miauth/{session}[/check]`, `i`, `i/update` (full profile store + `rejectQuotes`/`defaultReaction`/`showRemoteEmojis`), `i/notifications` (cursor, markAsRead, type filters) + `i/notifications-grouped`, `notifications/mark-all-as-read`, `i/revoke-token`, `i/pin|unpin`, `users/show`, `users/search`, `users/notes`, `users/reactions`, `users/followers|following` (`Following[]`), `username/available`, `users/report-abuse`, `notes/create`, `notes/show`, `notes/delete`, `notes/unrenote`, `notes/replies|renotes|children|conversation`, `notes/search|search-by-tag`, `notes/timeline`, `notes/local-timeline`, `notes/hybrid-timeline`, `notes/global-timeline`, `notes/reactions` (list), `notes/reactions/create|delete`, `notes/polls/vote`, `notes/drafts/create|list|update|delete|count`, `notes/schedule/create|list|delete` (Sharkey shim over drafts), `notes/edit`, `notes/favorites/create|delete`, `i/favorites`, `notes/state`, `following/create|delete`, `following/requests/list|accept|reject|cancel`, `blocking/*`, `mute/*`, `renote-mute/*`, `users/relation`, `emojis`, `emoji`, `admin/emoji/add|update|delete|list`, `antennas/list|show|create|update|delete|notes|remove-note`, `notifications/mark-all-as-read`, `drive/files` (+`|show|update|delete`, `drive/files/create` multipart/JSON, `GET /files/{id}`), `ap/show`, admin: `show-user|show-users`, `update-user-note`, `delete-account` + `i/delete-account`, `suspend-user|unsuspend-user`, `meta` + `update-meta`, `invite/*` + `admin/invite/*`, `abuse-user-reports` + `resolve|update|forward-abuse-user-report`, `unset-user-avatar|banner`, `delete-all-files-of-a-user`, `drive/files|show-file`.

Streaming: authenticated WebSocket `/streaming` (`?i=` or Bearer) with `main`, `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline`, `antenna` channels, note capture (`subNote`/`unsubNote` + aliases, `reacted`/`unreacted`/`deleted` with the visibility gate), `readNotification`, `UNSUPPORTED_CHANNEL` frame for unknown channels. Word mutes are server-enforced (hard drops, soft `isSoftMuted` flag; `docs/word-mutes.md`).

Quotes pack onto notes as Pinetree extensions (`quoteAuthorizationStatus`, `quoteTargetUri`, `quoteTargetUnavailable`; `docs/quote-posts.md`): present on quotes only, never on pure renotes, always subject to visibility. Replies are served from an indexed direct-child lookup (`docs/replies.md`); unresolved remote parents stay pending and fail-closed until the resolver links them.

Anything else answers 404 `UNKNOWN_API_ENDPOINT`; unknown fields on listed endpoints answer 400 `UNSUPPORTED_FIELD`. The exact contract is `docs/endpoint-manifest.md` as extended by the frozen profile `docs/phase-3-profile.md`.

## Federation surface

- Discovery: `/.well-known/webfinger`, `/.well-known/host-meta[.json]`, `/.well-known/nodeinfo`, `/nodeinfo/2.0|2.1`.
- Objects: `/users/{id}` (+ `/publickey`, `/outbox`, `/followers`, `/following`, `/featured`), `/@{acct}`, `/notes/{id}` (tombstone `410` for deleted).
- Inbound: `POST /inbox`, `POST /users/{id}/inbox` — Digest + HTTP signature required, 64 KiB body cap, ±5 min `Date` skew, actor/object authority checks, atomic dedup.
- Outbound: signed delivery from durable `FederateDeliver` jobs, retries with exponential backoff (`MAX_DELIVERY_ATTEMPTS`), shared-inbox dedup. Quote authorization travels as FEP-044f `QuoteRequest`/`Accept`(`Reject`)/stamp-`Delete` on the pinetree↔pinetree path; legacy-only peers stay `unapproved` by design.
- Interop-learned behaviours: Misskey reuses Follow/Undo ids per follower/followee pair, so dedup forgets them after `Undo(Follow)`; legacy HTTP Signatures only; Pleroma silently absorbs remote Announces of local posts (inbox 200, never displayed) while Akkoma renders them — Announce visibility is peer-defined and never gates our suites.

## Tests (all executed 2026-10-16, quotes + replies)

`cargo test --offline`: 351 green — lib 254, conformance 69 (redb, incl. Groups Q/R), crash_recovery 2, client_smoke 1 (real `misskey-js`), client_profile 1 (real `misskey-js` `APIClient` + `Stream`), federation 2 (two in-process nodes), minrepro 1, quote_fixtures 5, streaming 13 (live WS); federation_elixir 2 + federation_interop 1 skip-gated (need rigs). Clean-clone `cargo test` needs `./misskey`/`./sharkey` re-linked first (packer snapshot + client_profile tests skip without them). Full gate `scripts/smoke-all.sh` → `SMOKE_ALL_OK`; live four-server interop: `scripts/smoke-phase2.sh` → `SMOKE_OK` + `INTEROP_OK` (Misskey 2026.10.1-alpha.0, Sharkey 2025.4.7, 2× pinetree). Profile suites: `scripts/smoke-phase3.sh` → `SMOKE3_OK`, `scripts/keystone-sweep.sh` → `KEYSWEEP_OK` 41/41. Elixir triangle (`local-federation` rig + `pinetree.test`): `ELIXIR_QUOTE_FEDERATION_OK`, extended stress `STRESS_OK items=28 nodes=3 duplicates=0` (15 posts + 8 quotes + 5 replies with follow churn). Single-node bench: `scripts/bench-local.py` (~870 notes-create/s, ~430 timeline/s, ~1200 users-show/s on a debug build).

## Known gaps / limits

- Streaming channels: `userList`, `hashtag`, `roleTimeline`, `channel`, `drive`, `chat*`, `reversi*`, `clip`, `admin`, `serverStats`, `queueStats` answer `UNSUPPORTED_CHANNEL` (`antenna` shipped Batch N); connection caps (32/1536) and the 2 min idle reap are implemented but not live-tested.
- API profile subset only (see above): no channels, drive folders, featured timelines, 2FA, webhooks, OAuth, Mastodon API. Local clips + pages shipped (11 `clips/*` + 7 `pages/*` endpoints + `clips/follow|unfollow|followers`, real export + pinetree-only `i/import-clips`); federation is pinetree-first (`OrderedCollection` clips, `Article` pages) behind default-OFF switches — Phase 2 adds page read gates, clip sync (`Update`/`Delete` + tombstones), clip follows, and `clipUpdated`/`pageLiked` notifications. User lists + pull timelines + `userList` stream, url-preview, self-service auth, follower removal, drive extras (Batch O), and email identity + move + registry + mentions + notification utilities + discovery + versions + translate (Batch P; email/translate transports dormant, `pinned-users` empty, hashtag timeline out — upstream has none). `users/followers|following` return Misskey `Following[]` — Keystone's `FollowList` needs a one-line map in its own repo (logged follow-up).
- `i/update` supports a subset of profile fields (no profile `fields[]`, decorations, mutes, …).
- Federation MVP scope: no `_misskey_content` (never emitted), no Move activity handling, no RFC 9421 message signatures or LD signatures, no public-internet validation (interop was loopback-only).
- Storage is single-node `redb`; FoundationDB adapter and `src/cluster/*` are Phase 4. `--mode=cluster` is refused.
- Actor RSA keys are generated lazily on first use (slow in debug builds; peers may time out on the first fetch). Generating at signup is a known follow-up.
- Remote-user and note lookups, and inbox processing, perform blocking I/O on the tokio blocking pool; there is no per-destination concurrency tuning yet (Phase 5).
- Observability is minimal (stderr diagnostics only).
