# Architecture

Status: living page, 2026-10-21. Authoritative sources: `sns-server-proposal.md` §§2-5, `docs/application-ports.md`, `docs/database.md`, `docs/persistence-conformance.md`, `src/`. If this page and the contracts disagree, the contracts win.

Pinetree is a modular monolith in one Rust crate (`src/lib.rs`, `src/main.rs`). One program, one data file, no Redis/Postgres required. Every mutating operation commits state + background jobs in a single transaction; workers do fan-out later.

## 1. Big picture (node graph)

```
                    ┌─────────────┐
                    │ Misskey app │  kissme-fe, Keystone, misskey-js
                    │ (REST + WS) │
                    └──────┬──────┘
                           │ POST /api/{endpoint}, GET /streaming
                           ▼
                    ┌─────────────┐
                    │  axum router │  src/api/misskey/endpoints/mod.rs
                    │  + CORS/*   │  2 MiB body cap, dispatcher 1 MiB JSON cap
                    │  + nosniff  │
                    └──────┬──────┘
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
       ┌──────────┐ ┌──────────┐ ┌──────────┐
       │ Misskey  │ │ Activity │ │  WS hub  │  streaming/hub.rs + server.rs
       │ API pack │ │ Pub docs │ │  (1024   │
       │ + auth   │ │ + fetch  │ │  slot)   │
       └────┬─────┘ └────┬─────┘ └────┬─────┘
            │            │            │
            └────────────┼────────────┘
                         ▼
              ┌─────────────────────┐
              │   AppServices       │  application/commands/*
              │   (use-cases over   │  same ops for API + AP
              │    ports)           │
              └─────────┬───────────┘
                        │ ports.rs (domain <- application <- adapters)
           ┌────────────┼────────────┐
           ▼            ▼            ▼
     ┌──────────┐ ┌──────────┐ ┌──────────┐
     │  redb    │ │ FsBlob   │ │ JobWorker│  jobs/worker.rs (50 ms poll)
     │ single   │ │ durable/ │ │ + Notify │
     │ .redb    │ │ cache    │ │ + Deliver│
     └──────────┘ └──────────┘ └──────────┘
           │            │            │
           └────────────┼────────────┘
                        ▼
              ┌─────────────────────┐
              │  Fediverse (AP)     │  federation/* + activitypub/*
              │  in/out, signed     │
              └─────────────────────┘
```

Dependency rule: `domain <- application <- adapters`. Domain is pure data + validation (no HTTP, no DB). Application holds `AppServices { stores, clock, ids, federation_http, public_url, limits }`. Adapters (REST packers, AP renderers, redb, jobs, media) implement ports. Misskey API and ActivityPub call the same `AppServices` ops.

```
domain/      account, note, follow, blocking, muting, poll, reaction, quote,
             visibility, policy, notification, antenna, clip, page, drive,
             emoji, webhook, instance, user_list, pinning, draft ...
application/ ports.rs, queries.rs, events.rs, commands/{notes,follows,blocks,
             media,polls,antennas,drafts,clips,pages,webhooks,exports,...}
             + inbound/{follow,actors,quotes,notes,replies}
api/         misskey/{auth,endpoints,errors,packers,request,response}
activitypub/ actor, objects, webfinger, signatures, quotes, federated_collections
federation/  inbox, outbox, delivery, retry, host_caps, fetch_gate
streaming/   hub (broadcast), server (WS)
persistence/ redb/ (live), foundationdb/ (empty stub, Phase 4)
jobs/        worker, transcode      media/ storage, ingest, remote_cache ...
cluster/     empty stubs (Phase 4)  config.rs  observability.rs  shutdown.rs
```

## 2. Request path (REST)

`build_router()` (`endpoints/mod.rs:126-222`): `GET /healthz` (static `ok`), `/ready` (real redb read probe), `/files/{id}`, `/proxy`, `/miauth/*`, `/signup|/signin|/signin-flow`, `POST /api/drive/files/create` (multipart/JSON, own streaming cap), `POST /api/{*endpoint} → api_dispatch`, `GET /streaming`, federation routes (`/.well-known/*`, `/users/{id}[...]`, `/notes/{id}[/activity]`, `/clips/{id}`, `/pages/{id}`, `/inbox`, `/users/{id}/inbox`).

`api_dispatch → api_dispatch_blocking` is a giant endpoint-string match (~9k lines). Helpers: `parse_json_body`, `resolve_caller/require_caller`, `rate_key_for`, `client_ip` (X-Forwarded-For only from `trusted_proxies`), `get_limit(1-100,d10)`, `check_allowed_fields` (unknown field ⇒ 400 `UNSUPPORTED_FIELD`), `map_*_error` (per-endpoint UUID-preserving), static `GALLERY_STORE/ANNOUNCEMENT_STORE` mem fallbacks.

Envelope: `{error:{message,code,id,kind,info?}}`, kind→HTTP (401 auth, 403 permission, 404 unknown, 413 body, 429+Retry-After, 400 client, 500 internal), one-UUID-per-code, void ⇒ 204. Auth order: `Authorization: Bearer` → JSON `i` → `pinetree_session` cookie. Tokens SHA-256 hex; passwords Argon2id + legacy `salt:hash` verify-and-upgrade.

Tokio stack: sync dispatcher needs >2 MiB in dev; `main.rs` sets `thread_stack_size(8<<20)` with `dispatch_fits_bounded_stack` guard.

## 3. Internal mechanisms

### 3.1 Atomic state + jobs

Rule (`application-ports.md:13-16`): every mutating port commits state + `Job[]` in one txn. No out-of-txn enqueue. Example: `NoteStore::create_note(input,jobs)` writes note + dedup + `StreamingPublish/Notify/FederateDeliver/RemoteMediaFetch` jobs atomically.

`Job {id, kind, payload ≤64 KiB, idempotency_key!, not_before, attempts, lease, failed}`. `MAX_JOB_ATTEMPTS=8`, `MAX_JOBS_PER_TXN=500`. Kinds: `StreamingPublish, Notify, FederateDeliver, RemoteMediaFetch, QuoteStampReverify, ReplyParentResolve, PollExpiry, PostScheduledDraft, Export/Import, AntennaMatch, CacheEvict, ReconcileStorage, BackfillDurableAccounting, TranscodeMedia, WebhookDeliver, EmailSend, TimelineMaterialize(reserved)`.

`JobWorker` (`jobs/worker.rs:20-130`): `claim_next(30s lease)` → `catch_unwind(dispatch)` → Done / Retry(quadratic + Retry-After ≤1h) / Split(batched follow-ups) / Defer / Quarantine. Lease expiry counts as attempt; 8 ⇒ quarantine `retry-exhausted`. Spawned every 50 ms; SIGTERM drains.

### 3.2 Streaming

Producer contract: commands commit `StreamingPublish/Notify` in-txn; worker translates to `HubMessage`. `Hub {broadcast 1024, presence 10 min}` is at-most-once, lag-drop. `BroadcastStreamingSink` maps `StreamingEvent` → wire. WS `/streaming?i=` upgrade; 32 channels/conn, 1536 captures LRU, 2-min reap + 60 s ping. Channels: `main, home/local/hybrid/globalTimeline, antenna, userList, hashtag, roleTimeline(silent), drive, serverStats/queueStats(snapshot), admin, clip`. See [Federation](Federation.md) for delivery side, [Client API](Client-API.md) for wire.

### 3.3 Media

Bytes live outside the DB (`FsBlobStore {objects/xx/hash + tmp/, atomic rename, .len sidecar}`). Two pools (`main.rs:33-51`): `blobs_durable/` (per-user 1 GiB quota, refcounted, reconciler sweep) + `blobs_cache/` (512 MiB global, 128/256/128 class, LRU+TTL bytes-only). Legacy `blobs/` renamed once at boot. `reset_in_flight` heals crashes. Remote media: `validate_remote_media_url` + `download_and_sniff` (ignores declared type, HTML/SVG→octet-stream), 16/note, 64 MiB/note, `RemoteMediaFetch` job + fetch-on-first-request, `Disabled` fails closed.

## 4. Storage / database

### 4.1 redb (live, single-node)

`persistence/redb/mod.rs` (~10k lines): one file `pinetree.redb`, one writer, ACID. Tables: `ACCOUNTS, HANDLE_INDEX, USED_USERNAMES, ACCOUNT_OWNERS, NOTES, NOTES_BY_AUTHOR, NOTES_BY_URI, FOLLOWS_FORWARD/REVERSE, BLOCKINGS_*, MUTINGS_*, FOLLOW_REQ_*, REACTIONS(+BY_NOTE), DRAFTS, PINNINGS, FAVORITES, EMOJIS(+BY_NAME), REMOTE_EMOJIS, ANTENNAS, pages/clips/lists/signins/registry/media/remote_cache/accounting`, `CLIP_TOMBSTONES`, job queue, dedup + tombstones, migration markers. Media bytes never in txn (FDB 10 MB/5 s discipline by design).

Conformance `tests/conformance.rs` 119 green on redb; `tests/crash_recovery.rs` 2/2 kill -9; storage quota/cache/evict/reconcile 22 green. TimelineMaterialize reserved (timelines built on read from stored notes — simpler, fine at small scale; Phase 5 optimizes).

Prod-readiness (2026-10-21, single-node milestone): media paging is a real
contract (`MAX_MEDIA_PAGE_ROWS = 500`, newest-first exclusive cursors,
empty-page termination); accounting backfill is an at-open migration;
reconciliation is paged with a completeness-gated sweep; online backup
(versioned ZIP + both pools, single consistent read txn) and offline restore
into a fresh dir are implemented (`src/backup/`); inbound signatures honor
`federation.rfc9421_mode`; inbox ingress is per-route capped (64 KiB, 413 at
extraction); rate-limit/upload-slot maps are bounded; health/readiness/status
are split (`/healthz` liveness, `/ready` bounded readiness, admin-only
status + detailed diagnostics). Explicitly still out of scope for this
milestone: FoundationDB/cluster routing/multi-node ownership, shared rate
limiting, cross-node WebSocket delivery, S3/object storage, account movement
between nodes, multi-node HA. Live `cargo test` + Docker + federation
verification of the new suites is pending a provisioned host (this dev box
has no C toolchain: `ring` cannot rebuild), so release status stays
**unqualified**, not green.

### 4.2 FoundationDB (planned, Phase 4)

`persistence/foundationdb/mod.rs` is 0 bytes; `cluster/*` (account_directory, routing, internal_protocol) empty; `jobs/queue.rs` empty (real queue lives in redb facet). `Config::Cluster` is hard-rejected. Open per `docs/database.md` order: FDB adapter → account directory/routing/internal protocol → shared limiter/tracker → migration tool. No cross-node transport today; `Hub` is in-process, `DestinationTracker` per-process.

## 5. Configuration

JSON file `--config` + CLI overrides + `validate()` once. Every key optional with default, `deny_unknown_fields`. See [Administration](Administration.md) for the full schema. Known exceptions (env-only, tracked): `PINETREE_ALLOW_PRIVATE_IPS`, `PINETREE_DEBUG_FEDERATION`, `PINETREE_REPLY_REPAIR_LIMIT`; lease/retry numbers hardcoded.

## 6. What this buys you / costs you

- Buy: one binary + one file to back up; predictable layering; crash-safe jobs; FDB-safe bounds from day one.
- Cost: no multi-node yet; in-process streaming only; bounded-scan pagination (search/hashtags 10k window, list timelines ≤50×100); structured logging + admin status exist but federation/quarantine/oldest-pending aggregates are explicit nulls until the ports expose them.

Read more: [Federation](Federation.md), [Client API](Client-API.md), [Administration](Administration.md), [Operations](Operations.md), [Security](Security.md), [Developer Guide](Developer-Guide.md).
