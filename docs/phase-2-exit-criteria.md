# Phase 2 — exit criteria checklist

> **Status at HEAD (2026-10-20 freshness pass)** — boxes left as recorded (2026-10-14); **no boxes changed**, the four-server interop was **not** re-run.
> - `scripts/smoke-phase2.sh` exists and supports `KEEP=1` and `stop`; `misskey` and `sharkey` checkouts resolve (symlinks outside the repo). See `docs/phase-2-interop-howto.md` (status block).
> - The header pins Sharkey `2025.4.4`; `docs/README.md` pins `2025.4.7`. Unreconciled — the Phase 2 record is the 2025.4.4 run.
> - `smoke-all.sh` step 5 prints SKIP when Node 22 / docker / built checkouts are absent, and then still prints `SMOKE_ALL_OK` (`docs/testing.md` "Known gaps"). A green `SMOKE_ALL_OK` on a machine without the federation rig does **not** re-prove these boxes.

Source: `sns-server-proposal.md` §7 and §9, Phase 2. Plan: `docs/phase-2-plan.md`
(Frozen 2026-10-14; pinned against Misskey `2026.10.1-alpha.0` at `./misskey/`
and Sharkey `2025.4.4` at `./sharkey/`).

**Exit (proposal §9 Phase 2):**
- [x] Local and remote follow/post/react flows work end-to-end across `pinetree`, Misskey, and Sharkey.
- [x] Transient delivery failures retry without duplicating visible state; remote failure never undoes a committed local operation.

## Scope checklist (proposal §7 + §9 Phase 2)

- [x] **WebFinger (`A1`)**: `GET /.well-known/webfinger?resource=acct:user@xyz` (plus bare `user@xyz`, `{url}/users/{id}`, `{url}/@{username}`) resolves to canonical actor URL `https://{domain}/users/{account_id}` (`application/jrd+json` and XRD), plus `/.well-known/host-meta` and `/.well-known/host-meta.json`.
- [x] **NodeInfo + instance metadata (`A1`)**: `GET /.well-known/nodeinfo`, `GET /nodeinfo/2.0`, and `GET /nodeinfo/2.1` for ecosystem discovery.
- [x] **Actor documents & keys (`A2`, `B1`)**: stable actor document (`GET /users/{id}`, `/@{acct}`), public key (`#main-key` and `/users/{id}/publickey`), inbox/outbox/followers/following/featured collection references, RSA key generation, rotation, and persistent storage in `redb`.
- [x] **HTTP signatures (`A3`)**: verify inbound + sign outbound requests using the legacy HTTP Signatures format (`rsa-sha256`, `(request-target) host date` for GET, `(request-target) host date digest` for POST) and `Digest: SHA-256=...` body validation.
- [x] **Inboxes — actor + shared (`C1`, `D2`)**: `POST /inbox` and `POST /users/{id}/inbox` with signature/digest auth, request-size limits, timeouts, redirect limits, SSRF/private-IP protections, and audience/permission checks.
- [x] **Core activities on shared application operations (`B1`, `D1`, `D2`, `D3`)**: `Follow`, `Accept`, `Reject`, `Undo`, `Create`/`Note`, `Update`, `Delete`, `Like` (including `_misskey_reaction`), and `Announce` mapped onto the same application operations used by the Misskey API (no duplicate business logic; no `_misskey_content`; quotes/polls/`_misskey_votes` deferred).
- [x] **Activity-ID deduplication + transactional state changes (`D1`, `D2`)**: inbound activity IDs deduplicated atomically inside the same storage transaction as state changes and follow-up jobs.
- [x] **Durable outbound delivery (`D3`)**: `JobKind::FederateDeliver` committed atomically with local state changes; worker delivers with retries, exponential backoff, per-destination limits/failure handling, and shared-inbox deduplication; remote failure never undoes a committed local post.
- [x] **Remote media validation & caching (`C2`)**: remote attachment URLs validated (scheme, SSRF, size limit, MIME sniff) before fetching/caching; metadata stored in `MediaStore` (`redb`), bytes stored in `BlobStore` (filesystem) outside the database.
- [x] **MFM/HTML sanitization (`B2`)**: inbound remote HTML/MFM sanitized against XSS/script injection; outbound notes render safe HTML + `source` (`text/x.misskeymarkdown`) without `_misskey_content`.
- [x] **Verification & live interop (`E1`, `E2`)**: unit + integration tests (`tests/federation.rs`) and live interop tests (`tests/federation_interop.rs` / `scripts/smoke-phase2.sh`) running two `pinetree` instances, `./misskey`, and `./sharkey` federating with each other, with executed output recorded in `state/logs/`.

## Evidence (2026-10-14; see `state/logs/2026-10-14-phase-2-complete.md`)

- **Live four-server interop** — `scripts/smoke-phase2.sh` (fresh postgres/redis, Misskey `2026.10.1-alpha.0`, Sharkey `2025.4.7`, two pinetree instances on `127.0.1.1–4`) printed `SMOKE_OK`; `tests/federation_interop.rs` passed on the fresh stack and twice more on the same stack (re-follow/re-unfollow path). Flow: 12-way WebFinger resolve, 12 follows + Accepts, public notes to all peers, `_misskey_reaction` (`👍`) reactions, renotes, undo reaction, undo renote, delete, unfollow.
- **Transient failures retry without duplicating visible state; remote failure never undoes a local commit** — retry/backoff phase of `tests/federation.rs::federation_two_node_end_to_end` (jobs are enqueued in the same transaction as the local change; `MAX_DELIVERY_ATTEMPTS` backoff in `JobWorker`).
- **Dedup / atomicity** — `tests/conformance.rs` 27/27 (incl. `d1_forget_activity_allows_reprocessing`), two-node replay phase.
- **Remote media** — `federation_remote_media_cache` ok. **Sanitization / signatures** — lib suite 112/112 (+ `sign_verify_with_q_larger_than_p`).
- Not claimed: public-internet federation, polls/quotes/`_misskey_votes`, Mastodon API (all out of Phase 2 scope).
