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

`meta`, `signup`, `signin`, `admin/accounts/create`, `miauth/gen-token`, `auth/session/generate|show|userkey`, `auth/accept`, `GET|POST /miauth/{session}[/check]`, `i`, `i/update` (subset), `i/notifications` (cursor, markAsRead, type filters), `i/revoke-token`, `users/show`, `users/search`, `users/notes`, `users/followers|following` (`Following[]`), `username/available`, `notes/create`, `notes/show`, `notes/delete`, `notes/unrenote`, `notes/replies|renotes|children|conversation`, `notes/search|search-by-tag`, `notes/timeline`, `notes/local-timeline`, `notes/hybrid-timeline`, `notes/global-timeline`, `notes/reactions` (list), `notes/reactions/create|delete`, `following/create|delete`, `following/requests/list|accept|reject|cancel`, `notifications/mark-all-as-read`, `drive/files` (+`|show|update|delete`, `drive/files/create` multipart/JSON, `GET /files/{id}`), `ap/show`.

Streaming: authenticated WebSocket `/streaming` (`?i=` or Bearer) with `main`, `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline` channels, note capture (`subNote`/`unsubNote` + aliases, `reacted`/`unreacted`/`deleted` with the visibility gate), `readNotification`, `UNSUPPORTED_CHANNEL` frame for unknown channels.

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

- Streaming profile subset only: `userList`, `hashtag`, `antenna`, `roleTimeline`, `channel`, `drive`, `chat*`, `reversi*`, `clip`, `admin`, `serverStats`, `queueStats` channels answer `UNSUPPORTED_CHANNEL`; `pollVoted` never fires (no polls); connection caps (32/1536) and the 2 min idle reap are implemented but not live-tested; suspended-token 403 has no setter path to test.
- API profile subset only (see above): no antennas, clips, channels, pages, drive folders, lists, featured/mentions/user-list timelines, `notifications-grouped|flush`, mute/block, 2FA, webhooks, polls/vote, custom emoji endpoints, OAuth, Mastodon API. `users/followers|following` return Misskey `Following[]` — Keystone's `FollowList` needs a one-line map in its own repo (logged follow-up).
- `i/update` supports a subset of profile fields (no profile `fields[]`, decorations, mutes, …).
- Federation MVP scope: no polls/`_misskey_votes`, no `_misskey_content` (never emitted), no Move/Block/Flag, no RFC 9421 message signatures or LD signatures, no public-internet validation (interop was loopback-only).
- Storage is single-node `redb`; FoundationDB adapter and `src/cluster/*` are Phase 4. `--mode=cluster` is refused.
- Actor RSA keys are generated lazily on first use (slow in debug builds; peers may time out on the first fetch). Generating at signup is a known follow-up.
- Remote-user and note lookups, and inbox processing, perform blocking I/O on the tokio blocking pool; there is no per-destination concurrency tuning yet (Phase 5).
- Observability is minimal (stderr diagnostics only).
