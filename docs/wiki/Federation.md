# Federation

Status: living page, 2026-10-21. Sources: `docs/phase-2-exit-criteria.md`, `docs/phase-2-interop-howto.md`, `scripts/smoke-phase2.sh`, `src/activitypub/*`, `src/federation/*`, `docs/identity-rules.md`, `docs/activitypub-collections.md`, `docs/federated-clips.md`, `docs/federated-pages.md`, `HANDOFF_akkoma-compat.md`. Contracts win on disagreement.

Pinetree speaks ActivityPub with HTTP Signatures. Proven live 4-server (Misskey `2026.10.1-alpha.0`, Sharkey `2025.4.7`, 2×pinetree): WebFinger, follows+Accepts, notes/replies/renotes/likes/Undo/delete/unfollow. Public-internet soak and Mastodon/Pixie/Akkoma round-trips are open.

## 1. How pinetree finds others (and is found)

| Path | Handler |
|---|---|
| `GET /.well-known/webfinger?resource=` | `activitypub/webfinger.rs` — accepts `acct:user@host`, bare `user@host`, `{base}/users/{id}`, `{base}/@{user}`; foreign `@{other}` ⇒ 422; unknown/suspended/deleted ⇒ 404 (actor layer 410 for deleted) |
| `GET /.well-known/host-meta(.json)` | XRD/JRD |
| `GET /.well-known/nodeinfo, /nodeinfo/2.0\|2.1` | `software.name: misskey`, `version: 2026.10.1-alpha.0` (intentional spoof for compat probing) |

JRD: `subject acct:`, `aliases[/@user, actor]`, `self activity+json → actor`, `profile-page → /@user`, `subscribe → /authorize-follow`. `Cache-Control: public,max-age=180`. Outbound `webfinger_discover_actor_uri` tries https→http, requires `rel=self + activity+json`. Lab needs port-80 proxy (`scripts/interop/proxy.mjs`) because Misskey drops ports.

## 2. Actors, keys, signatures

Canonical `https://{domain}/users/{id}` (`domain/account.rs`), never node hostnames. Doc (`activitypub/actor.rs`): Misskey/Sharkey contexts minus `_misskey_content` + FEP-044f quote terms; `Person/Service(bot)/Application(system)`; `inbox/outbox/followers/following/featured`; `sharedInbox`; `url /@{user}`; `publicKey #main-key`; `_misskey_summary/isCat/vcard/manuallyApproves/discoverable`; optional `alsoKnownAs/movedTo`; pinetree-first `clips` iff `federate_clips` on.

Keys: lazy RSA-2048 `ensure_actor_keypair` (first-wins), `rotate`, `ensure_system_actor` signs outbound GETs. Remote `cache_remote_public_key` refuses keyId→different-actor rebinding. Parse enforces `publicKey.id` under actor origin + `owner==id`, sharedInbox same-host else personal fallback. Stricter than upstream.

Signatures (`signatures.rs`): GET signs `(request-target) date host`; POST + `Digest: SHA-256=...` (byte-identical to Misskey `ApRequestService`). Verify requires target+host+date (+Digest), Digest-match, ±300 s skew, `rsa-sha256|hs2019`, `(created)/(expires)` supported. Inbox: cached-key → refresh-once → owner URI; enforces `actor==signer`. Fetch-gate: same but anon-fallback with Serve/403/404 map. RFC 9421 verified per `config.federation.rfc9421_mode` (`permissive` default accepts either valid scheme with owner agreement; `enforce` requires valid RFC 9421, rejects legacy-only); `rsa-v1_5-sha256` only, required components and freshness enforced, exercised by `tests/federation_sig_mode_e5.rs`.

Collections: `outbox|followers|following|featured` paged `OrderedCollection` 40/page opaque cursor, no `prev`, outbox omits `totalItems`, followers/following 403 unless Public (no follower-exemption yet — see doubts).

## 3. Inboxes (what we accept)

`POST /inbox`, `POST /users/{id}/inbox` → per-route 64 KiB extraction cap → 413 before handler buffering (framework `BytesRejection`; status unchanged, body text is the framework's); bad JSON/type/id → 400; personal target must exist local else 404; signature 401 (legacy-only 401s under `enforce`); `actor==signer` 401; object-authority gate (Create object same-host, Update(Person) self-only). Dispatch: `Follow, Accept, Reject, Undo, Block, Create, Announce, Like, EmojiReact→Like, Delete, Update, Add/Remove, Move, QuoteRequest, Flag`; else 202 ignore (relays refused).

All funnel to the same `AppServices::handle_inbound_*` as the REST API. Dedup: `ActivityDedup check_and_record` in-state-txn; replays ⇒ 202. Tombstones + `NOTES_BY_URI` uniqueness. Local commit never undone on remote failure.

Behaviors: Follow→pending if locked; Accept(sees followee==signer) seeds `ActivityIdIndex` for `Undo(Accept)`-by-ID (process-local, restart-loses); Undo/Announce/Like/Delete/Update(Person|Note|Question|Collection)/Move(forged 401 + local migration)/Quote(FEP-044f stamp verify)/Flag→abuse/Add/Remove clip+featured pin sync/Block.

## 4. Outboxes (what we send)

Recipients paged 1000, remote-only, per-dest dedup, `sharedInbox||inbox`; ≤100 inline else Split to per-inbox jobs. `deliver_single`: cooldown → `signed_post_activity` → `classify_inbox_status` (2xx done; 408/429/5xx transient + Retry-After ≤1h; 3xx/other-4xx permanent; SSRF/NXDOMAIN permanent). Per-host `1s*2^n` backoff (`DestinationTracker`); panic→quarantine. Adds vs Misskey: explicit cooldown + Retry-After + 100-split (FDB-safe).

Remote media never inline: `RemoteMediaFetch` job + fetch-on-first-request; `Disabled` fails closed. SSRF/size/timeouts: http/https only, literal+resolved IP vs loopback/RFC1918/link-local/CGNAT/multicast/ULA/doc/mapped, `allow_private_ips` exempts IP only (TLS always), 10 s/20 s/5 s DNS, ≤3 GET redirects no downgrade, 64 KiB AP/WebFinger, 32 KiB headers.

## 5. Pinetree ↔ pinetree (full fidelity)

Everything above plus first-class extras:

- Quotes FEP-044f with stamp authority exact-host + negative cache, reply coalescing, `Update(Note)` string-URI fetch-and-apply, `Undo(Accept)` index.
- Clips as `OrderedCollection` at `/clips/{id}` (note URIs only, per-item gating, 200/page) + index `/users/{id}/clips` + `Add/Remove` + `Update/Delete(OrderedCollection)` + tombstone 410 + `Follow` collection. See `docs/federated-clips.md`. Both admin `federateClips` + per-clip flag must be ON (default OFF).
- Pages as `Article` at `/pages/{id}` (`name`=title, `content`=sanitized HTML, `source` blocks+variables, script never serialized) + `Create/Update/Delete(Article)` + page-likes back as `Like`. See `docs/federated-pages.md`. Per-page defaults ON but admin `federatePages` defaults OFF.
- Reaction 3-way emit is actually cross-software (below), not pinetree-only.

## 6. Pinetree ↔ Misskey / Sharkey (expected)

Shipped and lab-proven. Misskey `validPost` includes `Note|Question|Article|...`; our `Create(Article)` for pages becomes a note (title ignored, summary→CW, content→body). Sharkey `Update(Note)` string-URI and `Question.options[].type=="Note"` tolerated. Followers/following ordered by ID not time (documented divergence; treated as sets). `totalItems` omitted on outbox. Reaction split via NodeInfo probe (`host_caps`): Misskey-family `Like+_misskey_reaction`, Akkoma/Pleroma `EmojiReact`, Mastodon-family bare `Like`; unknown→MisskeyLike. SuspendBroadcast enumerates known shared inboxes (vs upstream followings snapshot).

What Misskey/Sharkey ignore gracefully: `clips` field, clip collections, page `source` extension, `Update/Delete(OrderedCollection)` for clips (unknown collection URIs dropped).

## 7. Pinetree ↔ Mastodon / generic ActivityPub

Mastodon `SUPPORTED=[Note,Question]`, `CONVERTED=[Image,Audio,Video,Article,Page,Event]`: our `Article` pages convert to a toot (content→text or name fallback, summary→CW, url appended). No extra work needed for basic readability; no rich pages UI elsewhere. Clips invisible (no collection-follow semantics) — correct to ignore.

Generic AP: implement `Create|Update|Delete(Note|Article|Page|Question)`, `Follow|Accept|Reject|Undo`, `Like|UndoLike`, `Announce|UndoAnnounce`, `Block|UndoBlock`, `Flag`, `Move`, `Add/Remove` (clips/featured), FEP-044f `QuoteRequest/Accept/Reject`. Sign with `(request-target) date host (+digest)`, serve fetch-gated objects with the Serve/403/404 mapping (anonymous→404). Respect `Digest`, 300 s skew, 64 KiB caps, no https→http downgrade.

## 8. Open / non-goals

Mastodon/Pixelfed/Akkoma/Pleroma live round-trips, public-internet soak, bench 0-fail, `D-c/D-e` items. No relay forward, no reversi/channels/chat streaming, no C2S outbox. Ephemeral `ActivityIdIndex/ReplyCoalescer/StampNegativeCache` lost on restart (best-effort). `host_caps` HTTPS-only probe fails for `http://` labs (benign fallback). See `docs/TODOS.md` P2/P3.
