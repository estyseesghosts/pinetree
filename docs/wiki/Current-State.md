# Current state (2026-10-15, Phase 2.5 cleanup complete; Phase 3 entry gate green)

## Phase status

| Phase | Status | Evidence |
|---|---|---|
| 0 Contracts | Done | `docs/phase-0-exit-criteria.md`; FDB client verified live (`docs/fdb-verification.md`) |
| 1 Single-node server | Done | `docs/phase-1-exit-criteria.md`; `state/logs/2026-10-14-phase-1-complete.md` |
| 2 Minimal federation | **Done** | `docs/phase-2-exit-criteria.md`; `state/logs/2026-10-14-phase-2-complete.md` |
| 2.5 Cleanup | **Done** | Slices 0–9, one commit each: `state/logs/2026-10-15-phase2-5-*`; `cargo test` 251 green; `scripts/smoke-phase2.sh` → `SMOKE_OK` |
| 2.6 Federation hardening | **Done** | Slices 1–12 (`state/logs/2026-10-15-phase2-6-*`): key ownership, inbox URLs, follow requests, reply visibility, request bodies, media safety, content addressing, passwords/throttling, delivery reliability, AP collections, RSA→`ring` signer |
| 3 Client compatibility + streaming | **Next — entry gate green** | — |
| 4 FoundationDB multi-node | Planned | — |
| 5 Scale and hardening | Planned | — |

## Repository layout

```
sns-server-proposal.md   product/architecture proposal (source of truth)
AGENTS.md                rules for agents; current phase
docs/                    frozen/draft contracts, phase plans, exit criteria, this wiki
state/logs/              dated append-only work logs (real command output)
src/                     the crate (see About → How it is built)
tests/                   conformance, crash_recovery, client_smoke, federation, federation_interop, minrepro
scripts/                 smoke-phase1.sh, smoke-phase2.sh, interop/ (compose + loopback proxy)
foundationdb/, redb/     upstream reference checkouts (git-ignored, read-only)
misskey, sharkey         symlinks to reference checkouts (git-ignored, read-only;
                          keep the targets on the native Linux fs under WSL)
.cargo/config.toml       git-ignored machine-local settings (cargo target-dir on ext4)
```

## Implemented Misskey API surface

JSON POST at `/api/{endpoint}`, credentials via `i` body field or `Authorization: Bearer`.

`meta`, `signup`, `signin`, `admin/accounts/create`, `miauth/gen-token`, `auth/session/generate|show|userkey`, `auth/accept`, `GET|POST /miauth/{session}[/check]`, `i`, `i/update` (subset), `i/notifications`, `users/show`, `users/search`, `username/available`, `notes/create`, `notes/show`, `notes/delete`, `notes/timeline`, `notes/local-timeline`, `notes/hybrid-timeline`, `notes/reactions/create|delete`, `following/create|delete`, `drive/files/create` (+ `GET /files/{id}`), `ap/show`.

Anything else answers 404 `UNKNOWN_API_ENDPOINT`; unknown fields on listed endpoints answer 400 `UNSUPPORTED_FIELD`. The exact contract is `docs/endpoint-manifest.md` (frozen).

## Federation surface

- Discovery: `/.well-known/webfinger`, `/.well-known/host-meta[.json]`, `/.well-known/nodeinfo`, `/nodeinfo/2.0|2.1`.
- Objects: `/users/{id}` (+ `/publickey`, `/outbox`, `/followers`, `/following`, `/featured`), `/@{acct}`, `/notes/{id}` (tombstone `410` for deleted).
- Inbound: `POST /inbox`, `POST /users/{id}/inbox` — Digest + HTTP signature required, 64 KiB body cap, ±5 min `Date` skew, actor/object authority checks, atomic dedup.
- Outbound: signed delivery from durable `FederateDeliver` jobs, retries with exponential backoff (`MAX_DELIVERY_ATTEMPTS`), shared-inbox dedup.
- Interop-learned behaviours: Misskey reuses Follow/Undo ids per follower/followee pair, so dedup forgets them after `Undo(Follow)`; legacy HTTP Signatures only.

## Tests (all executed 2026-10-15, after slices through 2.6)

`cargo test`: lib 193, conformance 51 (redb), crash_recovery 2, client_smoke 1 (real `misskey-js`), federation 2 (two in-process nodes), minrepro 1, federation_interop 1 (env-gated) — 251 green. Clean-clone `cargo test` needs `./misskey`/`./sharkey` re-linked first (packer snapshot tests skip without them). Live four-server interop: `scripts/smoke-phase2.sh` → `SMOKE_OK` (Misskey 2026.10.1-alpha.0, Sharkey 2025.4.7, 2× pinetree). Single-node bench: `scripts/bench-local.py` (~870 notes-create/s, ~430 timeline/s, ~1200 users-show/s on a debug build).

## Known gaps / limits

- **No streaming.** `src/streaming/*` are empty stubs; `StreamingSink` is a no-op, so there is no `/streaming` route, no note capture, and live UI timelines stay empty.
- API subset only (see above): no `users/notes`, `users/followers`, `users/following`, `notes/renotes`, `notes/replies`, `notes/global-timeline`, `notes/search`, lists, antennas, clips, channels, pages, drive folders/listing, mute/block, 2FA, webhooks, polls, quotes, custom emoji endpoints. The `users/followers` gap is endpoint-only — the `followers_of` store and the ActivityPub followers collection exist.
- `i/update` supports a subset of profile fields (no profile `fields[]`, decorations, mutes, …).
- Federation MVP scope: no polls/`_misskey_votes`, no quote metadata, no `_misskey_content` (never emitted), no Move/Block/Flag, no RFC 9421 message signatures or LD signatures, no public-internet validation (interop was loopback-only).
- Storage is single-node `redb`; FoundationDB adapter and `src/cluster/*` are Phase 4. `--mode=cluster` is refused.
- Actor RSA keys are generated lazily on first use (slow in debug builds; peers may time out on the first fetch). Generating at signup is a known follow-up.
- Remote-user and note lookups, and inbox processing, perform blocking I/O on the tokio blocking pool; there is no per-destination concurrency tuning yet (Phase 5).
- Observability is minimal (stderr diagnostics only).
