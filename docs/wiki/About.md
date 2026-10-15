# About pinetree

pinetree is a lightweight, modular **Misskey-compatible social network server** written in Rust. Existing Misskey clients talk to it through a deliberately chosen, contract-tested subset of the Misskey API, and it federates over ActivityPub with Misskey, Sharkey and other servers.

## Goals (proposal §1)

- Small, well-defined modules that can be extended without rewriting unrelated systems.
- Runs as a simple **single-node** instance (embedded `redb` database, no Postgres/Redis/broker) *or*, in the planned multi-node mode, as one logical instance spread over several servers on **FoundationDB**.
- Supports Misskey clients via a tested API subset, including streaming (streaming is Phase 3).
- Federates in a Misskey-compatible way.

## Explicit non-goals

- Reimplementing every legacy Misskey feature or extension.
- The Mastodon *client* REST API (ActivityPub federation with Mastodon servers stays in scope; Sharkey, by contrast, ships a Mastodon API).
- Microservices from day one; requiring SQL, Redis or a message broker.

## What it does today (Phase 2.5 cleanup complete, 2026-10-15)

- **Accounts and auth:** signup/signin, MiAuth (web flow and token checks), legacy `auth/session/*`, per-token permissions, rate limits with Misskey-style `429` + `retryAfter`.
- **Notes:** create (text, CW, visibility `public/home/followers/specified`, reply, renote, files, `localOnly`), show, delete; reactions create/delete; follows create/delete; home/local/hybrid timelines with cursor pagination; notifications list; remote lookup by `username`+`host` and `ap/show`.
- **Media:** multipart/JSON upload, local filesystem blob store, public file serving; remote attachment validation and caching (scheme, SSRF, size limit, MIME sniff).
- **Federation (ActivityPub):** WebFinger (JRD + XRD), host-meta, NodeInfo 2.0/2.1, actor documents with RSA keys, outbox/followers/following/featured collections, note documents, shared and personal inboxes, legacy HTTP Signatures (`rsa-sha256`, Digest) both ways, Follow/Accept/Reject/Undo/Create(Note)/Update/Delete/Like(`_misskey_reaction`)/Announce, atomic activity-id deduplication (including Reject), durable outbound delivery with retry and backoff. RSA comes from the maintained `rsa` crate (the hand-rolled bigint/DER/PEM code was removed in Phase 2.5); TLS verification is never disabled (the SSRF test exemption no longer touches it).
- **Durability:** every local state change commits together with its outbox/job record in one storage transaction; workers deliver, notify and fan out; jobs are idempotent and crash-safe (crash-recovery tests). Terminally failed jobs are quarantined with a logged reason (`fail_permanent`: bad payloads, delivery `PermanentFailure`, retry exhaustion) instead of retrying forever.
- **Security posture:** federation input is untrusted: size limits, timeouts, redirect limits, SSRF protection, signature and authority checks, HTML/MFM sanitization.

## How it is built

Modular monolith; dependency direction `domain` ← `application` ← adapters.

- `src/domain` — pure types and rules (accounts, notes, visibility policy, IDs). No HTTP, no DB crates.
- `src/application` — use cases (`commands`, `queries`, ports/traits). The Misskey API and ActivityPub adapters call the *same* operations; no duplicated business logic.
- `src/persistence/redb` — the single-node storage adapter (the FoundationDB adapter is Phase 4). Both adapters must pass the same conformance tests.
- `src/api/misskey` — HTTP API adapter (axum). `src/activitypub`, `src/federation` — protocol and delivery adapters; they depend only on `Arc<AppServices>`, never on the Misskey adapter.
- `src/jobs`, `src/media` — durable job queue/worker and blob storage.
- `src/streaming`, `src/cluster` — placeholders for Phase 3 and Phase 4.

Identity is stable and independent of placement: actor URLs are `https://{domain}/users/{account_id}`; renames and moves preserve the account ID; public IDs and URLs never expose node hostnames. IDs are Misskey-style `aidx`.

## Running it

```bash
cargo build --offline
<target>/debug/pinetree --public-url=http://127.0.0.1:3000 --data-dir=./data --listen=127.0.0.1:3000
<target>/debug/pinetree create-account --username=alice --password=secret --data-dir=./data
PINETREE_ALLOW_PRIVATE_IPS=1 ...   # only for loopback federation tests; default blocks private addresses
```

(`<target>` is `./target`, or wherever `.cargo/config.toml` points it —
this machine uses `/root/pinetree-target`. Keep checkouts and the target dir
on the native Linux filesystem under WSL; 9P is several times slower.)

Cluster mode (`--mode=cluster`) deliberately exits with "not implemented (Phase 4)". For a full local federation lab (real Misskey, real Sharkey, two pinetrees) see `docs/phase-2-interop-howto.md` and `scripts/smoke-phase2.sh`.

A fresh clone also needs the reference checkouts linked as `./misskey` and `./sharkey` (see `AGENTS.md` layout) for the client-smoke test and packer snapshot tests; `redb` itself comes from crates.io, no extra step. The repo-root `README.md` has the full quickstart.

## Verification culture

Compatibility claims are backed by executed tests against real software: `tests/client_smoke.rs` drives the actual `misskey-js` client; `tests/federation_interop.rs` federates with a real Misskey and a real Sharkey checkout. Work logs with real output live in `state/logs/`.
