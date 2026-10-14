# Phase 1 plan — single-node server

**Status:** Frozen (2026-10-14; pinned against Misskey 2026.10.1-alpha.0)
**Scope:** proposal §9 Phase 1. Builds on the frozen contracts in `docs/`.

## Ground rules

- **Storage depends on deployment mode, with no exceptions.** `mode = single` → redb. `mode = cluster` → FoundationDB. Phase 1 builds only the redb path. At startup, `cluster` mode exits with "not implemented (Phase 4)". redb is never used across nodes, and FDB is never used for single-node.
- The `foundationdb` crate sits behind a cargo feature (`fdb`), off by default. Single-node builds don't need `libfdb_c`.
- One crate, with modules as laid out in proposal §4. Dependencies point domain ← application ← adapters.
- Cheap multi-node prep: every use case takes an `account_id`, and the `AccountStore::get_owner` call stays on the path (it returns the local node). `public_url` is set separately from the listen address, and no host name ever appears in a URL. That leaves a place to add Phase 4 routing without changing call sites.
- A slice is done when `cargo test` is green and its state log entry is written.

## A — Skeleton

| Slice | Deliverable |
|---|---|
| A1 Scaffold | `Cargo.toml`, module tree, `config.rs` (`public_url`, `mode`, `data_dir`, `listen`), axum `main.rs` with `/healthz` returning 200 |
| A2 Kernel | `aidx` `IdGenerator`, `Clock` trait, the 4-variant port error. Tests: regex, sort order, `parse_time` |
| A3 Domain types | Account, Note, Follow, Reaction, MediaMeta, with validation bounds from `domain-types.md`. Unit tests on the bounds |
| A4 Visibility | One predicate (`visibility-policy.md`). The E1–E4 matrix runs as unit tests |

## B — Ports + redb

| Slice | Deliverable |
|---|---|
| B1 Ports | `application/ports.rs` traits, exactly as in `application-ports.md` |
| B2 Conformance suite | `tests/conformance/`, generic over a store factory, covering every case in `persistence-conformance.md`. Re-implemented here; the `/tmp` harness is not copied in |
| B3 redb adapter | Tables: accounts, handle index, used usernames, notes, notes-by-author, follows (both directions), reactions, jobs + due index, dedup, media meta. **Full suite green on redb**, with crash cases run as a subprocess killed with `kill -9` |
| B4 BlobStore (fs) | Content-addressed paths keyed by hash. Writes go to a temp file, then rename. Tests: put/get/delete, and a partial write is never visible |

## C — Application + jobs

| Slice | Deliverable |
|---|---|
| C1 Commands | create_account, create/delete note, follow/unfollow, react/unreact, update_profile, upload_media. Each commit carries its jobs |
| C2 Queries | notes_show, users_show, and home/local/hybrid timelines (fan-out-on-read, `sinceId`/`untilId`) |
| C3 Worker | In-process tokio worker: claim / complete / fail with backoff. `Notify` handler writes notifications. `StreamingPublish` goes to a no-op sink until Phase 3. `FederateDeliver` isn't produced until Phase 2 |
| C4 Crash recovery | Kill the server mid-traffic, restart, assert no lost notes or jobs, and the queue drains |

## D — Misskey API

| Slice | Deliverable |
|---|---|
| D1 API core | `POST /api/{endpoint}` router; `i` or Bearer extraction; permission check; `{id,code,message}` errors; unknown param → `UNSUPPORTED_FIELD`; unknown endpoint → 404; fixed-window rate limits |
| D2 Tokens + MiAuth | Tokens stored hashed, with permission sets. Minimal server-rendered login + approve page; `miauth/gen-token`, `/miauth/:session/check`, legacy `auth/session/*` |
| D3 Endpoints | `meta`, `i`, `i/update`, `users/show`, `username/available`, `notes/create\|show\|delete`, `following/create\|delete`, the 3 timelines, reactions, signup, `drive/files/create` |
| D4 Packers | Note, UserLite/Detailed/MeDetailed, DriveFile. Snapshot tests check field sets and nullability against the Misskey checkout |

## E — Exit

| Slice | Deliverable |
|---|---|
| E1 Client smoke | Script + checklist against the target client(s): register → auth → post → timeline → follow → react |
| E2 Checklist | `docs/phase-1-exit-criteria.md`, every box backed by evidence logged in `state/logs/` |

## Gaps settled before D (recorded in `docs/endpoint-manifest.md` and `state/logs/2026-10-14-phase-1-gaps-settled.md`)

1. **Reaction endpoints (`notes/reactions/create|delete`)**: Added to `docs/endpoint-manifest.md` §4 (`write:reactions`, `204 No Content`, `NO_SUCH_NOTE`/`ALREADY_REACTED`/`NOT_REACTED`/`CANNOT_REACT_TO_RENOTE`, `notes/reactions/delete` 60/h + 3s min interval, `reactionAcceptance` fallback coercion per Misskey `ReactionService.ts:127,149`).
2. **Account creation**: Both `POST /signup` (`/api/signup` + `/api/admin/accounts/create`, returning `MeDetailed & { token }`) and CLI (`pinetree create-account`) are supported, plus `POST /signin` (`/api/signin`) and web MiAuth login/approval at `GET/POST /miauth/:session`.
3. **`drive/files/create` and media URL path**: Added to `docs/endpoint-manifest.md` §4 (`write:drive`, 120/h, multipart/form-data + JSON fallback, `MAX_FILE_SIZE_EXCEEDED` 413, returns `DriveFile`; public URL served at `GET /files/:id` and `/files/:id/:name` → 301 canonical per Misskey `FileServerService.ts:93-99`).
4. **Target clients and versions**: Pinned against **Misskey 2026.10** (`2026.10.1-alpha.0` checkout at `./misskey/` and `packages/misskey-js` 2026.10.1-alpha.0 executed under `bun`).
