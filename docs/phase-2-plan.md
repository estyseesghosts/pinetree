> **Status (2026-10-20 freshness pass):** PLAN HISTORY. Phase 2 exit: `docs/phase-2-exit-criteria.md` has 13 boxes checked and 0 open (grep). Pointers: `state/logs/2026-10-14-phase-2-complete.md`; `phase-2-interop-howto.md`. Header says Sharkey `2025.4.4`; README and exit docs say `2025.4.7`. Use the latter.
> Tags: `[LANDED — evidence]` verified in code at HEAD; `[OPEN]` not in code; `[SUPERSEDED by …]`; `[DECISION — still binding]`; `[PLAN HISTORY]`. Original text is unchanged; tag blocks are additions. Short paths are under `src/`; `mod.rs` = `src/api/misskey/endpoints/mod.rs`.

# Phase 2 plan — minimal federation

**Status:** Frozen (2026-10-14; pinned against Misskey `2026.10.1-alpha.0` and Sharkey `2025.4.4`)
**Scope:** `sns-server-proposal.md` §7 and §9 Phase 2. Builds on the frozen contracts in `docs/`.

## Ground rules

> **[DECISION — still binding]** Invariants still hold. Polls and quotes are NOT deferred any more: `[SUPERSEDED by docs/quote-posts.md and inventory §1.5]`.


- **Architecture invariants (`sns-server-proposal.md` §§2–5, 7):**
  - Dependency direction: `domain` ← `application` ← adapters (`api/misskey`, `activitypub`, `federation`, `persistence/redb`, `media`, `jobs`). Domain never imports HTTP, ActivityPub JSON shapes, or database crates.
  - The Misskey API and ActivityPub adapters call the **same application operations** (`AppServices`). No duplicate business logic for note creation, deletion, following, or reactions.
  - One storage transaction commits the state change and its durable outbox/job records (`JobKind::FederateDeliver`, `Notify`, `StreamingPublish`) atomically.
  - FDB transaction limits (~5s, ~10 MB, 64 KiB job payload cap) are respected: no media blobs or unbounded follower fan-out inside a transaction. Outbox jobs carry compact structured payloads and workers resolve inbox recipients and deliver with at-least-once retries + deduplication.
  - Public IDs and URLs never expose physical node hostnames (`https://{domain}/users/{account_id}`, `https://{domain}/notes/{note_id}`).
  - Federation input is untrusted: enforce request-size limits, timeouts, redirect limits, SSRF/private-IP protections (with explicit config allowlist for local federation testing), HTTP signature + `Digest` verification, audience/permission checks, remote media URL validation before fetch/cache, and HTML/MFM sanitization.
  - Reaction metadata uses `_misskey_reaction`; deprecated `_misskey_content` is never produced. Polls, `_misskey_votes`, and quotes remain deferred per proposal §7.
- A slice is done when `cargo test` is green and its state log entry is written in `state/logs/`.

## A — Discovery, Keys & HTTP Signatures

> **[PLAN HISTORY]** Landed (webfinger.rs, signatures.rs).


| Slice | Deliverable |
|---|---|
| A1 Discovery (WebFinger, host-meta, NodeInfo) | `GET /.well-known/webfinger?resource=...` resolving `acct:user@host`, bare `user@host`, `{url}/users/{id}`, and `{url}/@{username}` to canonical actor URL `{url}/users/{id}` (`application/jrd+json` + XRD fallback); `GET /.well-known/host-meta` (XRD) + `/.well-known/host-meta.json`; `GET /.well-known/nodeinfo` + `GET /nodeinfo/2.0` + `GET /nodeinfo/2.1` |
| A2 Actor Keys & Key Store | RSA-2048 keypair generation, rotation, and storage (`ActorKeyStore` port + redb tables for local account keypairs and cached remote actor public keys; system instance actor `system.actor` for signed GETs). Unit + persistence tests |
| A3 HTTP Signatures & Digest | `src/activitypub/signatures.rs`: legacy HTTP Signatures (`rsa-sha256`, `Signature` / `Authorization: Signature` header, `(request-target) host date` for GET and `(request-target) host date digest` for POST) + `Digest: SHA-256=...` computation and verification, clock-skew check, keyId resolution (`{actor}#main-key` and `{actor}/publickey`). Unit tests |

## B — Actor Documents, Objects & Content Sanitization

> **[PLAN HISTORY]** Landed (actor.rs, objects.rs).


| Slice | Deliverable |
|---|---|
| B1 Actor & Note Documents | `src/activitypub/actor.rs` and `src/activitypub/objects.rs`: JSON-LD `@context` (ActivityStreams + Security v1 + Misskey/toot extensions without `_misskey_content`), `Person`/`Service`/`Application` rendering and parsing, `Note`/`Create`/`Announce`/`Like`/`Follow`/`Accept`/`Reject`/`Undo`/`Delete`/`Tombstone`/`Update` rendering and parsing, `to`/`cc` ↔ `Visibility` (`public`, `home`, `followers`, `specified`) audience mapping (`ApAudienceService` rules), `GET /users/{id}`, `/users/{id}/publickey`, `/users/{id}/outbox`, `/users/{id}/followers`, `/users/{id}/following`, `/users/{id}/collections/featured`, `/@{acct}`, `/notes/{id}`, `/notes/{id}/activity` with AP content negotiation (`application/activity+json`, `application/ld+json`, HTML redirect on `/users/{id}`) |
| B2 HTML ↔ MFM & Sanitization | `src/activitypub/objects.rs`: outbound MFM/text → safe HTML (`<p>`, `<br>`, `<a class="u-url mention">`, `#hashtag` links) + `source: { content, mediaType: "text/x.misskeymarkdown" }` (never `_misskey_content`); inbound HTML/MFM sanitization (strip `<script>`, `<style>`, `<iframe>`, `<object>`, event-handler attributes `on*`, `javascript:`/`data:`/`vbscript:` URLs; convert clean HTML to MFM text when `source` is absent). Unit tests |

## C — Federation Transport, SSRF Protection & Remote Media Cache

> **[PLAN HISTORY]** Landed for transport and SSRF. Remote-media lifecycle is `[OPEN]` (see `remote-media-cache-plan.md`).


| Slice | Deliverable |
|---|---|
| C1 Safe HTTP Client & Remote Resolver | `src/federation/delivery.rs`: untrusted-peer HTTP client enforcing request/response size limits (64 KiB AP JSON, configurable media cap), timeouts (10s connect/read), redirect limit (≤3 hops, no `https` → `http` downgrade), and SSRF IP validation (reject loopback, RFC1918, link-local, CGNAT, multicast, unspecified IPs unless matched by configured `allowed_private_networks` CIDR allowlist for local federation rigs). WebFinger + signed-GET actor/note resolver that caches remote accounts and public keys in storage. Unit tests for SSRF, redirect, and size-limit rejection |
| C2 Remote Media Validation & Cache | `src/media/remote_cache.rs` + `src/media/ingest.rs`: validate remote attachment URLs (scheme `http`/`https`, SSRF check, size limit, MIME sniff) before fetching/caching; store metadata in `MediaStore` (`owner_id = None`, remote source `uri`/`url`) and bytes in `BlobStore` outside the DB. Unit + integration tests |

## D — Transactional Inbound & Outbound Federation

> **[PLAN HISTORY]** Landed (inbox.rs, outbox.rs, retry.rs).


| Slice | Deliverable |
|---|---|
| D1 Transactional Dedup & Shared Application Operations | Extend storage ports + `RedbStore` and `AppServices` so inbound activities (`Follow`, `Accept`, `Reject`, `Undo`, `Create/Note`, `Update`, `Delete`, `Like` incl. `_misskey_reaction`, `Announce`) and local Misskey API commands share the exact same domain/application operations and commit state change + `ActivityDedup` record + durable `FederateDeliver`/`Notify`/`StreamingPublish` jobs in one storage transaction. Add `ap/show` and remote `users/show` (`username` + `host`) resolution so Misskey/Sharkey clients can resolve and follow remote actors |
| D2 Inbox Processing (Actor + Shared) | `src/federation/inbox.rs`: `POST /inbox` and `POST /users/{id}/inbox` with 64 KiB body limit, `Digest` + HTTP signature verification (fetching/refreshing remote signer key on miss), actor/object origin authority check, activity-ID deduplication, audience/permission checks, and dispatch to shared application commands. Unit + integration tests |
| D3 Durable Outbox & Retry Worker | `src/federation/outbox.rs` and `src/federation/retry.rs`: produce `JobKind::FederateDeliver` atomically on local `follow`, `unfollow`, `Accept(Follow)`, `create_note` (`Create(Note)` or `Announce`), `delete_note` (`Delete`), `react` (`Like` with `_misskey_reaction`), `unreact` (`Undo(Like)`), `update_profile` (`Update(Person)`); shared-inbox deduplication per destination host; worker delivery with exponential backoff, per-destination concurrency/failure tracking, permanent (4xx except 408/429) vs transient (5xx/network/408/429) classification; local post remains committed even when remote delivery fails and succeeds on retry without duplicating visible state |

## E — Verification & Live Interop (Two Pinetrees + Misskey + Sharkey)

> **[PLAN HISTORY]** Landed per `phase-2-exit-criteria.md`; live scripts `scripts/smoke-phase2.sh`.


| Slice | Deliverable |
|---|---|
| E1 Pinetree ↔ Pinetree Federation Suite | `tests/federation.rs`: two live `pinetree` instances federating over HTTP — WebFinger, NodeInfo, Actor fetch, HTTP signatures, Follow + Accept, Undo Follow, Create Note (public, home, followers, specified, reply, CW, mention, hashtag, remote media attachment), Announce (renote) + Undo Announce, Like (`_misskey_reaction`) + Undo Like, Update Person, Delete Note, activity-ID deduplication, SSRF/size/signature rejection, and transient delivery failure + retry recovery without duplicate state |
| E2 Real Misskey + Sharkey + Two Pinetrees Interop | Live 4-server interop test (`scripts/smoke-phase2.sh` + `tests/federation_interop.rs`): spin up real `./misskey`, real `./sharkey`, and two `pinetree` instances (`pinetree-a`, `pinetree-b`) and verify end-to-end federation across all four servers (WebFinger discovery, remote follow/accept, cross-instance note creation/reply/renote, `_misskey_reaction` reactions, undo/delete). Check off `docs/phase-2-exit-criteria.md` with executed output in `state/logs/` |
