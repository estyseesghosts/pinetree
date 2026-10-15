Status: Draft (2026-10-14). Pre-Phase-3 security/correctness cleanup, driven by the external audit of `6b58dcb` (`pine2.md`).

# Phase 2.6 — Security and federation-correctness cleanup

## Audit verification summary

Every claim in the audit was checked against the tree at `6b58dcb` (read-only, file:line evidence). Nothing was executed; "confirmed" means traced in code. Each slice below must add a failing regression test **first**, then the fix.

| # | Audit claim | Verdict | Notes / corrections |
|---|---|---|---|
| H1 | Unbounded request bodies | **Confirmed** | `DefaultBodyLimit::disable()` at `src/api/misskey/endpoints/mod.rs:126` covers every Misskey route (incl. `/api/{*endpoint}`, signin, miauth), not just uploads. `to_bytes(body, usize::MAX)` at `mod.rs:1098` runs before auth (`:1191`), the rate limit (`:1195`), and the 250 MiB check (`application/commands/media.rs:19`). AP inbox routes are merged *after* the layer and are bounded (64 KiB, `federation/inbox.rs:81`). |
| H2 | Remote media served as active content | **Confirmed, and worse** | Declared `mediaType` beats the response header (`media/remote_cache.rs:68-73`). It is also checked *before* the UTF-8→`text/plain` step (`media/ingest.rs:36-44`), so ASCII HTML/SVG is stored as declared. `serve_file` (`mod.rs:415-441`) sets only `Content-Type`: no `nosniff`, CSP or `Content-Disposition`, and no global header layer. **New:** local multipart uploads of non-UTF-8 bytes keep the caller's `Content-Type` (`commands/media.rs:84-87`), so a local user can store `text/html`. |
| H3 | Fast password hashing | **Confirmed** | `salt_hex:sha256_hex`, single round, unversioned (`api/misskey/auth.rs:26-61`). No KDF crate is in `Cargo.lock`. |
| H4 | No auth rate limiting | **Confirmed, and worse** | `/signin`, `/api/signin`, MiAuth submit/check, and MiAuth `signup=1` never call the limiter (`mod.rs:650-960`). **New:** `client_ip` (`mod.rs:141-158`) trusts `X-Forwarded-For` unconditionally and falls back to the shared key `"unknown"`. `ConnectInfo` is never used. |
| H5 | `sharedInbox` unvalidated | **Confirmed** | `actor.rs:140-147` only trims the value. The personal inbox check (`actor.rs:134`) compares host only, not scheme, so an http inbox for an https actor passes. Delivery-time SSRF guards apply but do not stop cross-origin delivery to a public host. |
| M6 | RSA / RUSTSEC-2023-0071 | **Confirmed, needs a decision** | `rsa 0.9.10`. Signing via `SigningKey::<Sha256>::sign`, which uses no blinding (`DummyRng`). No decrypt/OAEP anywhere. Private-key ops at `signatures.rs:127-135` from `signed_get_json` and `signed_post_activity`. The slice-6 log already flagged the open advisory check. |
| M7 | AP collections single page | **Confirmed, and worse** | Followers/following are capped at 200 with `unwrap_or_default()` (`actor.rs:621-622`, `:652+`). `totalItems` comes from counters, so it disagrees with `orderedItems`. The outbox is capped at 50 and its `totalItems` is the filtered page size. **New:** outbound fan-out silently truncates at 10,000 followers (`federation/outbox.rs:148`). No contract covers AP pagination. |
| C1 | Public-key cache poisoning | **Confirmed — critical** | `publicKey.id` is never tied to the actor (`actor.rs:153-166`; `owner` is checked only if present). `save_remote_public_key` overwrites by key id (`persistence/redb/mod.rs:2443`). `authenticate` discards the cached owner (`inbox.rs:174`). The signer is derived from the keyId *string* (`signatures.rs:315`, `inbox.rs:116-123`). Full impersonation path traced; no test covers it. |
| C2 | Locked accounts leak followers-only | **Confirmed** | No pending state (`domain/follow.rs:11-18`). Inbound follow is stored unconditionally (`inbound/follow.rs:72-79`). Local `following/create` ignores `is_locked`. Visibility and fan-out both trust any stored edge. **New:** locked accounts also get a "followed you" notification for unapproved requests. |
| C3 | Media fetched before dedup | **Confirmed** | Dedup happens only in `record_note_activity`, after `ingest_remote_note` has fetched every attachment inline in the inbox request (`inbox.rs:721-755`). No count or total-bytes cap: about 2,500 attachments fit in a 64 KiB body. **New:** replays leave orphan `MediaMeta` rows (`remote_cache.rs:111`), and failures are swallowed with 202. |
| R4 | curl chunked double-decode | **Confirmed** | No `--raw`, so `read_http_response` re-dechunks curl's output (`delivery.rs:575-618`). Every body-bearing HTTPS fetch from a chunked peer gets an empty body or an error (actor, WebFinger, note, media). Inbox POST outcomes are unaffected (status only). No chunked test exists. |
| R5 | Whole-batch retry; sync drain | **Confirmed** | One job per activity, sequential fan-out, any transient failure re-sends to everyone (`outbox.rs:398-424`). The `outbox.rs:165-172` doc comment claims a per-inbox retry that does not exist. `DestinationTracker::health()` has zero callers. `drain(64)` runs in seven API write handlers (`mod.rs:1520-1935`) and processes *any* job kind; drain errors surface after the write has committed. **New:** SSRF/DNS errors are retried as transient; `MAX_DELIVERY_ATTEMPTS` is dead. |
| D6 | MD5 blob keys | **Confirmed** | Global blob namespace keyed by MD5 (`commands/media.rs:24`, `remote_cache.rs:103`). `FsBlobStore::put` skips the write if the key exists (`media/storage.rs:52-56`). No SHA-256 anywhere for media. `md5` is exposed to clients (`packers.rs:340`), so it must stay as a field. |
| D7 | `decode_chunked` overflow panic | **Confirmed, and worse** | `chunk_size + 2` unchecked, direct slicing (`delivery.rs:658-662`). Reachable over plain HTTP and, through R4, over HTTPS. **New:** the worker swallows the panic (`jobs/worker.rs:165`). `claim_next` reclaims expired leases without incrementing `attempts` (`redb/mod.rs:1435-1440`), so a panicking job loops forever. |
| D8 | Reply ignores parent visibility | **Confirmed** | `create_note` checks existence only (`commands/notes.rs:18-35`). Contrast `react`, which checks (`:206-214`). Replying to a `Specified` note inherits its `visible_user_ids`, so a non-recipient can address the original audience. Renotes are already rejected for followers-only and specified targets by `policy.rs:81-91`. |

Further gaps found during verification, folded into slices: the TCP path has no total deadline, and DNS lookup has no timeout (R slice). Remote `Account.validate()` is not run on actor upsert (H5 slice).

## Slices

Same rules as Phase 2.5: one commit per slice, a dated log per slice, test-first, and both backends' conformance suite stays green. The order is by severity × blast radius, with tiny crash fixes first.

### Slice 0: Baseline
- Record clean-clone `cargo test` (expect 150/150) and `scripts/smoke-phase2.sh` (`SMOKE_OK`) on `6b58dcb`. No code changes.

### Slice 1: HTTP response framing (R4 + D7)
- Pass `--raw` to curl, so `read_http_response` sees real framing on both paths. The alternative is to skip dechunking on the curl path; pick one and document it.
- `decode_chunked`: `checked_add`, reject any chunk larger than the remaining budget, and use `get()` slicing. Also reject chunk-extension garbage.
- Handle (or explicitly reject) extra header blocks: `100 Continue`, proxy CONNECT. Add `-q` so `.curlrc` and proxy env are ignored, unless proxies are a supported config.
- Panic containment: `claim_next` increments `attempts` when it reclaims an expired lease (port change, with a conformance cell). Add `catch_unwind` (or `JoinError` → quarantine) in the worker and in `drain`.
- **Tests:** a chunked response over a local HTTPS fixture (or a curl-args test plus a parser test on raw framing); a fuzz-style `decode_chunked` test with `ffffffffffffffff`; a conformance cell showing lease expiry counts as an attempt.

### Slice 2: Key ownership (C1) — highest severity
- At parse: `publicKey.id` must equal the actor id or be a fragment or path under it. In all cases it must be on the same origin as the actor. `owner`, when present, must equal the actor id. Reject otherwise.
- At cache write: refuse to overwrite a key id whose stored `actor_uri` differs. Drop the speculative `{uri}/publickey` alias, or store it only when it is the real key id.
- At inbox auth: `authenticate` returns the key's owner (from cache or the fresh fetch). `activity.actor` must equal that owner, not a string derived from the keyId. Keep `force_refresh`, but re-apply the same checks after the refetch.
- **Tests:** a malicious actor doc advertising a victim's key id (with and without `owner`), then a forged victim activity, must be rejected. The pre-cached real key must not be overwritten. Honest Misskey/Sharkey key shapes (`#main-key`, `/publickey`) must still pass.

### Slice 3: Inbox URL validation (H5)
- Parse `endpoints.sharedInbox` with the same URL parser. It must be same-host as the actor, and if the actor is https it must not be http. Apply the same scheme rule to `inbox`. On failure, drop the shared inbox and fall back to the personal inbox; don't reject the actor.
- Run `Account::validate()` on the remote-actor upsert path.
- **Tests:** a third-party `sharedInbox`, an https→http downgrade, and an overlong URL.

### Slice 4: Follow request state (C2)
- Domain: `Follow` gains `state: Pending | Accepted`, or a separate `FollowRequest` record (**decision D-a**). Only accepted edges feed `follows_author`, `followers_of` fan-out, counters, and AP collections.
- Inbound Follow to a locked local account becomes pending: no auto-Accept, and a `receiveFollowRequest` notification instead of `follow`. Local `following/create` to a locked account becomes pending too.
- Accept/Reject path for local owners: add the minimal Misskey endpoints `following/requests/list`, `/accept`, `/reject`, `/cancel`. Accept emits an AP `Accept` job atomically with the state flip. Without these endpoints a locked account could never gain followers. They are pulled forward from Phase 3 deliberately (**decision D-b**).
- Pack `hasPendingFollowRequestFromYou/ToYou` from real state instead of hard-coded `false`.
- Outbound: following a remote locked account is pending until the inbound `Accept`. Verify against Misskey's behaviour.
- Port, conformance and contract updates to frozen `docs/domain-types.md`, `docs/application-ports.md`, and `docs/visibility-policy.md`, each with a logged superseding note. Add conformance cells, so the FDB backend inherits them.
- **Tests:** a pending follower receives no followers-only delivery and can't read via the API. Accept grants both. Reject and cancel clean up. Live: a Misskey user follows a locked pinetree user and is accepted (extend the smoke script).

### Slice 5: Reply visibility (D8)
- `create_note`: if `reply_id` is set, require `is_visible(target, author_viewer)`. Otherwise return Misskey's `NO_SUCH_NOTE`, so existence is not leaked. Apply the same check to the inbound remote-reply path only where it would grant audience. Remote replies to unseen notes are still stored, but must not widen the audience.
- Re-check renote/quote against `is_visible` for completeness. The policy already covers it, so this is a test only.
- **Tests:** replies to followers-only (non-follower) and specified (non-recipient) notes are rejected. The allowed cases still work.

### Slice 6: Request bodies (H1)
- Remove the router-wide `DefaultBodyLimit::disable()`. Set a sane default (Misskey's API JSON limit, or 1 MiB) on the Misskey routes.
- Give `drive/files/create` its own layer, capped at `max_file_size` + multipart overhead. Read the body as a stream with a running cap (`http_body_util::Limited`) and abort with 413 as soon as the cap is crossed.
- Auth ordering: when the token is in the `Authorization` header, authenticate and rate-limit **before** reading the body. When the token is the multipart `i` field (as misskey-js sends it), the bounded streaming read is the guard. Also add a per-IP concurrent-upload limit (**decision D-c**: how strict).
- **Tests:** an oversized upload is rejected without buffering (assert via a capped body source). An oversized JSON body to `/api/notes/create` returns 413. The misskey-js client smoke test still uploads.

### Slice 7: Media safety (H2 + C3)
- MIME allowlist: images (png/jpeg/gif/webp/avif), video (mp4/webm), audio (mpeg/ogg/wav), and `application/octet-stream`. The stored MIME comes from sniffing **only**. Declared or remote types may refine within the allowlist but never introduce a type. Explicitly never `text/html`, `image/svg+xml`, `application/xhtml+xml` or `text/xml`. `text/plain` only if a sniff confirms UTF-8. Apply to local and remote ingest via one shared `sniff_mime`; delete the duplicate in `commands/media.rs`.
- `serve_file`: `X-Content-Type-Options: nosniff`, `Content-Security-Policy: default-src 'none'; sandbox`, and `Content-Disposition: attachment` for anything not in the inline-safe set. Add a global `nosniff` layer too. A separate cookieless media origin goes on the Phase 4 shared-media list, not here.
- Inbound Create: check activity dedup (read-only `is_recorded`) **before** any fetch. The final `record_note_activity` stays the atomic authority, so the race is benign. Cap attachments per note (Misskey uses 16) and total bytes per activity. Make attachment `MediaMeta` insert idempotent per (remote URL, note), or fold it into the dedup transaction, so replays don't orphan rows.
- Move remote media fetching off the inbox request into a durable job (**decision D-d**: now, or as Phase 3 "attachment behavior"). The recommendation is now: the inbox path otherwise holds a request open for up to N×10 s.
- **Tests:** remote `text/html` and SVG attachments are stored as octet-stream or rejected. A local non-UTF-8 upload with `text/html` is not served as HTML. Response headers are present. Replaying the same Create triggers zero fetches. Too many attachments are truncated or rejected.

### Slice 8: Content addressing (D6)
- Blob key is SHA-256. `MediaMeta` keeps `md5` as a separate field for the packer. `FsBlobStore::put` may skip on an existing key (SHA-256 is safe), but `get` verifies length.
- Existing data migration (**decision D-e**): there are no production instances, so the recommendation is a one-shot rekey on startup or simply documenting a data wipe.
- **Tests:** two distinct blobs never share storage. Upload dedup per owner still works. The packer's `md5` is unchanged for the same bytes.

### Slice 9: Passwords and auth throttling (H3 + H4)
- Argon2id (`argon2` crate, PHC string `$argon2id$...`). Verify the legacy `salt:hex` format and rehash on successful login. New hashes are always Argon2id. Pick parameters and log them (target ~50–100 ms).
- Run hashing in `spawn_blocking`, never on the async executor.
- Rate limit signin, MiAuth submit and check, and MiAuth signup per account **and** per IP: fixed window plus a progressive delay, with no hard lockout so a victim can't be locked out by an attacker. Return Misskey-shaped `RATE_LIMIT_EXCEEDED`.
- Client IP from `ConnectInfo<SocketAddr>`. Honor `X-Forwarded-For` only when the peer is in a new `trusted_proxies` config list. Remove the shared `"unknown"` key.
- **Tests:** a legacy hash verifies and is upgraded. Brute force trips the limiter. A spoofed XFF from an untrusted peer is ignored.

### Slice 10: Delivery reliability (R5 + M7 fan-out cap)
- On partial fan-out failure, mark the activity job done and enqueue one `Federate` job per **failed** inbox, using the existing `inbox_override` field (this implements the doc comment at `outbox.rs:165`). Each job is enqueued in one transaction.
- Classify errors: SSRF block, invalid URL and DNS NXDOMAIN are permanent; timeouts, 5xx and 429 are transient. Honor `Retry-After`.
- Consult `DestinationTracker` before sending. A host in cooldown is deferred, not counted as an attempt. Delete dead `MAX_DELIVERY_ATTEMPTS`.
- Paginate `followers_of` in the fan-out (no 10k truncation). Large fan-outs split into batched jobs to respect future FDB transaction limits.
- Remove the synchronous `drain` from API handlers and rely on the worker (50 ms poll). Tests that need determinism call `drain` explicitly. Check that the client smoke test and `smoke-phase2.sh` don't depend on same-request delivery; add polling where they do.
- Add an overall deadline to the plain-HTTP path and a DNS timeout (resolve on a bounded thread or with a timeout wrapper).
- **Tests:** one of three inboxes failing means only that inbox is retried. A cooldown host is skipped. API latency is independent of peer latency (a slow-peer fixture).

### Slice 11: AP collections (M7)
- Write a short contract section (`docs/identity-rules.md` or a new `docs/activitypub-collections.md`) covering `OrderedCollection` plus `OrderedCollectionPage`, `first`/`next`/`prev`, page size 20–50, opaque cursor, and `totalItems` from counters. Match what Misskey and Sharkey emit and accept.
- Followers, following and outbox use cursor paging with `next` links. Storage errors return 500. `find_by_id` misses are logged, not silently dropped. Outbox `totalItems` is the visible-to-public count, or omitted.
- Respect the Misskey "hide followers/following" settings if `Account` carries them; otherwise note it as a Phase 3 gap.
- **Tests:** 250 followers page through completely. A storage error returns 500. Live: Misskey can fetch the collections.

### Slice 12: RSA advisory decision (M6)
- Check the current RUSTSEC-2023-0071 status and whether `rsa 0.10` (still RC in the local registry) fixes it.
- **Decision D-f:**
  - (a) Sign with `aws-lc-rs` or `ring`, which are constant-time, and keep `rsa` for verify and SPKI parsing.
  - (b) Accept the risk with a written rationale: signing only, no decryption oracle, remote timing observation is impractical.
  - Recommendation: (a), if a spike shows stored PKCS#1/PKCS#8 keys load unchanged. Signing sits behind the `signatures.rs` surface, so it's a small change.
- **Verification:** a fixture sign/verify round-trip, existing keys still load, and `smoke-phase2.sh` against live Misskey and Sharkey.

### Slice 13: Docs and exit gate
- A log per slice. Frozen-contract superseding notes for slices 1, 4 and 11. Mark this file's checklist with evidence. Update the `AGENTS.md` phase header.
- **Phase 3 entry gate:**
  - clean-clone `cargo test` is green, with all new regression tests listed;
  - `scripts/smoke-phase2.sh` is green;
  - the extended smoke test, where Misskey follows a locked pinetree user, is green;
  - `cargo audit` output is recorded, with remaining advisories justified.

## Ordering

- Slices 1, 2 and 3 are small and independent; land them first. Slice 2 is the most urgent.
- Slice 4 is the largest and touches frozen contracts; land it before 5 and 10, which read follow state.
- Slices 6 and 7 both touch `endpoints/mod.rs` upload and serve code; do them sequentially.
- Slice 8 follows 7, because both touch media ingest.
- Slices 9, 11 and 12 are independent and can land any time after 0.

## Open decisions

- **D-a** — Pending follows: a state field on `Follow`, or a separate `FollowRequest` store. Recommendation: a separate store, which mirrors Misskey's `FollowRequest` and keeps `followers_of` and counters untouched.
- **D-b** — Pull the `following/requests/*` endpoints into 2.6? Recommendation: yes. Fixing C2 without them makes locked accounts unusable.
- **D-c** — Strictness of upload concurrency and size limits per IP or account.
- **D-d** — Move remote media fetch into jobs now, or in Phase 3? Recommendation: now.
- **D-e** — Blob rekey migration, or a documented wipe. Recommendation: wipe, since there is no production data.
- **D-f** — RSA signing backend. Recommendation: `aws-lc-rs` or `ring`, after a spike.

## Out of scope (kept for Phase 3+)

- Interop beyond Misskey and Sharkey (Mastodon, Pixelfed).
- Quotes, polls, and distributed deployment.
- A separate cookieless media origin (Phase 4 shared media).
- Revisiting the remote-remote followers fallback. Still recorded in the slice-8 decision log; Phase 3 streaming must revisit it.
