# FEDERATION.md — pinetree

**Status:** accurate against `master` at `ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e`
(`2026-10-22`, `docs: align single-node ops and release gates`).
Upstream `origin/master` verified equal to `HEAD` at audit time
(`git fetch` + `rev-parse`; 0 ahead / 0 behind). The working tree contained
uncommitted work by another implementer (media lifecycle, TOTP, backup,
DNS, diagnostics — see final report); all federation claims below are based
on the checked-out `HEAD` revision, not on uncommitted changes.

This document is the FEP-67ff record for pinetree. It describes what a
remote server needs to interoperate with pinetree, grounded in
`src/activitypub/`, `src/federation/`, `src/domain/`,
`src/application/commands/` and live-interop records. It does not describe
planned work as operational.

Spec for this file format:

- [FEP-67ff: FEDERATION.md](https://codeberg.org/fediverse/fep/src/branch/main/fep/67ff/fep-67ff.md)

## Federation overview

pinetree is a single-binary, single-node, Misskey-compatible SNS server
(Rust, embedded `redb`, local file storage, no frontend). Its role in the
Fediverse is a Misskey-family leaf server: local users (`https://{domain}/users/{account_id}`)
publish notes, polls, reactions, renotes, quotes, pins, clips/pages
(pinetree-first, opt-in), follows, blocks, moves and abuse reports; remote
Misskey, Sharkey, Akkoma/Pleroma-family, Mastodon-family and Catodon
servers federate with it over ActivityPub server-to-server.

What federates:

- People (local `Person`, system `Application` actor for signed fetches).
- Notes (`Note`), articles/pages (`Article`/`Page`), polls (`Question`),
  reactions (`Like` / `EmojiReact`), reposts (`Announce`), quotes
  (FEP-044f + legacy aliases), edits (`Update`), deletes (`Delete` /
  `Tombstone`), follows, blocks, moves, pins/featured membership,
  clips/pages collections (pinetree-only, default OFF), abuse reports
  (`Flag`).

Client APIs (Misskey API, streaming WebSocket) are out of scope here except
where they shape wire behavior. There is no Mastodon client REST API and no
ActivityPub client-to-server API.

Live verification to date is loopback/docker labs only (two pinetrees + real
Misskey `2026.10.1-alpha.0` + real Sharkey `2025.4.7` via
`scripts/smoke-phase2.sh`; multi-family bench harness covering
Akkoma/Pleroma, Mastodon, Catodon). Never verified against the live public
internet. SSRF protection blocks private IPs by default; loopback labs need
`PINETREE_ALLOW_PRIVATE_IPS=1`.

## Supported federation protocols and standards

| Protocol | Role | Spec | pinetree support |
|---|---|---|---|
| ActivityPub server-to-server | inbox delivery, actor/object fetch, addressing | [W3C ActivityPub](https://www.w3.org/TR/activitypub/) | Implemented (S2S only; no C2S). Shared inbox `POST /inbox` + personal `POST /users/{id}/inbox`. Actor, note, outbox, followers, following, featured served. |
| WebFinger | handle → actor resolution | [RFC 7033](https://datatracker.ietf.org/doc/html/rfc7033) | Implemented. `GET /.well-known/webfinger?resource=…` (JRD default, XRD on `Accept`), plus `host-meta` XRD/JRD. |
| HTTP Signatures (legacy) | request authentication | [draft-cavanaugh-http-signatures](https://datatracker.ietf.org/doc/html/draft-cavanaugh-http-signatures) | Implemented inbound + outbound. `rsa-sha256`, `(request-target) host date [digest]`, `Digest: SHA-256=…`, ±300 s `Date` skew, 64 KiB body cap. Outbound POST/GET are legacy-signed. |
| HTTP Signatures (RFC 9421) | request authentication | [RFC 9421](https://www.rfc-editor.org/rfc/rfc9421.html) | Inbound verified (`permissive` default accepts either scheme; `enforce` requires RFC 9421). Outbound RFC 9421 signing exists in code but production delivery sends legacy only — do not require RFC 9421 from pinetree. |
| NodeInfo 2.0 / 2.1 | capability discovery | [NodeInfo](http://nodeinfo.diaspora.software/) | Implemented. `GET /.well-known/nodeinfo`, `GET /nodeinfo/2.0`, `GET /nodeinfo/2.1`. `software.name` reports `misskey` for compatibility; counts cover local accounts/notes only; 300 s cache. Used at runtime only for per-host reaction-style classification. |
| RSA keys | actor identity | [RFC 8017 PKCS#1](https://datatracker.ietf.org/doc/html/rfc8017) (via `ring`/`rsa`) | Implemented. 2048-bit RSA, `#main-key` fragment plus `GET /users/{id}/publickey`, SPKI public / PKCS#8-or-PKCS#1 private PEM. Lazy generation on first use. |

Only the rows above are claimed. In particular: no Linked Data
Signatures, no RFC 9421-only operation, no OAuth, no Mastodon API.

## Supported FEPs

| FEP | Spec | Support | Notes |
|---|---|---|---|
| [FEP-044f](https://codeberg.org/fediverse/fep/src/branch/main/fep/044f/fep-044f.md) | [FEP-044f](https://codeberg.org/fediverse/fep/src/branch/main/fep/044f/fep-044f.md) | **Partial** (operational pinetree↔pinetree; legacy-compatible outward) | Canonical `quote` + `QuoteRequest` / `Accept`+`result` / `Reject` / stamp `QuoteAuthorization` / stamp `Delete` revocation implemented. Auto-approve for `public`/`home` (+ self/mention exceptions); `manualApproval` never advertised (no review queue). Remote strip-to-plain-note fallback for non-FEP peers. Intentional divergence: Keystone renders unapproved/rejected quotes; FEP-044f non-normative guidance suggests hiding them. |
| [FEP-e232](https://codeberg.org/fediverse/fep/src/branch/main/fep/e232/fep-e232.md) | [FEP-e232](https://codeberg.org/fediverse/fep/src/branch/main/fep/e232/fep-e232.md) | **Partial / minimal** | Exactly one compat `Link` tag emitted for a quote target; inbound extracts the first Misskey-relation `Link`. Outbound `Note.tag` otherwise carries only `Mention`/`Hashtag`/quote-`Link` (no `Emoji` tags on notes; `Emoji` tags appear only on `EmojiReact`). Not a general object-links implementation. |
| [FEP-67ff](https://codeberg.org/fediverse/fep/src/branch/main/fep/67ff/fep-67ff.md) | [FEP-67ff](https://codeberg.org/fediverse/fep/src/branch/main/fep/67ff/fep-67ff.md) | **Supported** | This file. |

No other FEP is claimed. `FEP-8a8e` (events), relays, channels, thread
muting and any quote `manualApproval` workflow are planned/not built and
are omitted here.

## Actor discovery and addressing

- **Canonical actor ID:** `https://{public_url_host}/users/{account_id}`
  (`src/domain/account.rs:620-628`). Never exposes node hostnames. Renames
  preserve account ID; moves preserve it via alias (`also_known_as`,
  `moved_to_uri`).
- **WebFinger** (`src/activitypub/webfinger/finger.rs`): accepts
  `{base}/users/{id}`, `{base}/@{username}`, `acct:user@host`,
  `user@host`, bare `user` (case-insensitive); `system.actor` /
  `instance.actor` map to the system actor. Only local, non-suspended,
  non-deleted accounts resolve; otherwise `404` (`422` for foreign host,
  `400` for missing `resource`). JRD by default; XRD only when `Accept`
  prefers `application/xrd+xml`. `Cache: public,max-age=180`.
  `subject: acct:user@host`, `aliases: [@user, actor]`,
  `links: [self→actor, profile-page, subscribe]`.
- **host-meta:** `GET /.well-known/host-meta` (XRD) and
  `/.well-known/host-meta.json` (JRD), both pointing at the WebFinger
  template. No auth.
- **Actor document** (`GET /users/{id}`, `GET /@{acct}`):
  `Person` (regular users), `Service` (bots), `Application` (system actor).
  Exposes `inbox` (personal), `outbox`, `followers`, `following`,
  `featured` (`{actor}/collections/featured`), `sharedInbox` (`{base}/inbox`
  plus `endpoints.sharedInbox`), `url` (`{base}/@{username}`),
  `publicKey ({actor}#main-key, owner, PEM)`. Non-ActivityPub `Accept` on
  `/users/{id}` redirects `302` to `/@user`; `/@acct` currently serves AP
  JSON in both branches (HTML placeholder dead — do not rely on content
  negotiation there). `Cache: public,max-age=180`.
- **Public key:** also at `GET /users/{id}/publickey`. Rotation supported
  (`rotate_actor_keypair`); remote-key cache binds `keyId → first actor URI`
  and conflicts on rebind (anti-poison).
- **Inbox discovery:** send to `preferred_inbox`: the actor's `sharedInbox`
  when present and non-empty, else its personal `inbox`
  (`src/federation/outbox/recipients.rs`). Local actors are never selected.
  Dedup is by inbox URL string; followers are paged (no truncation; tested
  to 10k+).
- **Collections:** `followers` / `following` are `OrderedCollection` with
  cursor pages (40/page); non-public follower/following lists return `403`.
  `featured` route exists but always renders empty in the audited revision
  (pin data is enforced locally; remote `Add`/`Remove` for featured is
  still processed — see below). Outbox index omits `totalItems` and scans
  bounded batches; only `!localOnly && (Public|Home) && is_visible` notes
  appear.
- **Notes:** `GET /notes/{id}` serves public/`home` local notes;
  authenticated fetch via the shared fetch gate (failures collapse to
  anonymous → `404` instead of `401` for existence-hiding resources);
  deleted notes return `410 Gone` with a `Tombstone`
  (`src/activitypub/objects/object_handlers.rs`, `tombstone.rs`).

## ActivityPub interoperability

### Inbound (remote → pinetree)

Transport: `POST /inbox` (shared) and `POST /users/{id}/inbox`
(personal), 64 KiB cap (router 413 + handler backstop), JSON required.
`type` + `id` required (`400` otherwise). Personal inbox must be local
(`404` otherwise). Authentication: legacy or RFC 9421 signature must verify
(`401` otherwise); `activity.actor` must equal the key owner; object-authority
gate (`src/federation/inbox/mod.rs:385-435`): `Create` requires
`object.host == signer.host`; `Update(Person)` requires `object == signer`;
note/article/page/question/clip updates and string-URI objects defer finer
checks to dispatch. `Accept/Reject/Undo/Announce/Like/Delete/Block/QuoteRequest/Add/Remove`
pass the gate and are re-validated at dispatch. Unknown activity types return
`202 Accepted` and are ignored (no state change).

Dedup: read-only `is_recorded(activity_id)` gate before any fetch, then
atomic `check_and_record` commits activity-ID + state + follow-up jobs in one
storage transaction. `Undo(Follow)` forgets both `Follow.id` and `Undo.id`
(Misskey ID reuse). `Accept(Follow)` seeds a process-local by-ID index for
bare-IRI `Undo(Accept)` (restart loses it; worst case converges without
effect). Tombstone-before-`Create` prevents resurrection. Replays are `202`
without duplication.

| Activity | Accepted objects | Practical behavior |
|---|---|---|
| `Follow` | local user (or local `/clips/{id}` for clip follows) | Remote follower resolved (fetch+upsert); blocked-host senders dropped pre-write (`202`); must target a local account (`202` otherwise); commits `handle_inbound_follow`. Locked accounts follow the same path; approval emits `Accept`/`Reject`. |
| `Accept` | embedded `Follow`, or `QuoteRequest` shape | `Follow` shape must have `actor` local, `object == signer`, else `401`/`202`; seeds by-ID index. `QuoteRequest` shape routes to quote-accept (stamp IRI in `result` required). Missing `object` → `202`. |
| `Reject` | embedded `Follow`, or `QuoteRequest` shape | Same routing as `Accept`; quote-reject flips matching request to `Rejected`. |
| `Undo` | `Follow`, `Like`/`EmojiReact`, `Block`, `Announce`, `Accept` (embedded or bare-IRI) | `Follow` → unfollow; `Like` → unlike; `Block` → unblock; `Announce` → delete the actor's renote of the target (by activity ID or by lookup); bare-IRI `Accept` → unfollow only if `Accept.actor == signer` and the pair was previously recorded. Anything else → `202`. |
| `Block` | local user | Remote blocker resolved; blocked-host senders dropped; blockee must be local (`202` otherwise). Records Misskey-parity block (follow teardown happens on the application path, bounded). |
| `Create` | `Note`, `Article` (→ page path), `Page`, `Question` (poll), vote-shaped `Create` (content-less + `name` + `inReplyTo`), `Answer` | String-URI object is fetched first (fail → `202`); embedded object must parse (`note_parser.rs`) or `202`; author must equal signer (`401` otherwise); blocked hosts dropped; unknown local targets / unfetchable remotes / suspended authors → `202`. `Article` on the public path is ingested as a page when applicable. Vote-shaped `Create` targeting a cached poll records a vote and never creates a note; `Answer` without a cached poll is dropped. Otherwise ingests a note: reply link or fail-closed pending parent + coalesced `ReplyParentResolve` job, mentions/visible-users best-effort, media → durable `RemoteMediaFetch` job, quote resolution (unresolved target kept as URI + `unapproved`), poll 2–10 options × 1–50 chars else poll-less. |
| `Announce` | note URI (local or origin-checked remote) | Announcer resolved; target resolved locally first, else origin-checked fetch-and-cache (fail → `202`); empty `to`/`cc` coerced to followers; stored as a renote note keyed by `activity_id`. |
| `Like` / `EmojiReact` | note URI, or `/pages/{id}` / remote page URI | No object → `202`. Page likes notify the page owner (replay-safe) and stop. Note likes resolve the target (fail → `202`) and record via the acceptance-coerced path. `_misskey_reaction` preferred, then `content`, then `Emoji` tag `name`, else `❤` fallback. Suspended/blocked and self-reactions are filtered on the application path. |
| `Delete` | note, `Tombstone`, `Person`, page, clip (`OrderedCollection` / `/clips/*`), quote stamp | Replay-gated. Note: author (or same-origin host for Tombstone race) must match; third-party deletes → `401`/`202`. `Delete(Person)` for a known remote URI deletes the cached actor (local URIs and `object != actor` → `202`). Stamp `Delete` authority is the quoted-post author; third-party forwards ignored (`202`). Unknown note URIs: same-host Tombstone race records a tombstone so a late `Create` is refused; otherwise `202`. |
| `Update` | actor document, `Note`/`Article`/`Page`, `Question` (tally), `OrderedCollection` (clip sync) | String-object `Update` for a note fetches and requires `attributedTo == signer`. Actor `Update` requires `id == signer`, then patches display fields. Note edit requires authority, re-resolves quotes/polls, maps storage `Validation\|NotFound\|Conflict` to `202` (storage faults propagate → sender retries). Clip `Update` requires `fetch_collection_owner == signer`. `Update(Question)` overwrites the tally idempotently; local polls are never overwritten by remotes. |
| `Add` / `Remove` | `{actor}/collections/featured` (pins), clip membership | Replay-gated. Non-`featured` `/clips/`-local targets are never remote-mutated (`202`). Featured: resolves/fetches the note, requires `author == signer`, then add/remove pinning (`Conflict` → success). Other collections: owner must equal signer or the update is dropped and the cache evicted. |
| `Move` | `Move{object, target}` | `object != signer` → forged `401`. Mover resolved local-first → cached → fetch; blocked hosts dropped; target resolved the same way (unresolvable → `202`, retryable, nothing recorded). Records `moved_to_uri`/`also_known_as`, then migrates local followers (unfollow-old + follow-new, 100/edge, one txn per edge). |
| `QuoteRequest` (+ full-FEP IRI form) | `object` = quoted local note, `instrument` = quoter's note | Requires quoted URI + instrument; instrument fetched/parsed (fail → `202`); `instrument.attributedTo == signer` (`401` otherwise); instrument must quote exactly `object` (`202` otherwise); quoted post must be a live local note (visibility-enforced). Policy: auto-approve public/unlisted + self/mention exceptions; tighter targets get `Reject` only when the requester is entitled, else silent ignore (no review queue). |
| `Accept` / `Reject` (quote) | `object` = request IRI, `Accept.result` = stamp IRI | Quote path only when the shapes match. Decider must be the quoted-post author (`401` otherwise). `Accept` with an unfetchable stamp enqueues a retry job (stays pending, never auto-approved). `Reject` flips only the matching request. |
| `Flag` | object URI(s) / embedded object(s) / arrays | Reporter must resolve (failure propagates → sender retries). First local user in the flattened object list is the target; remote-only reports quietly skip. Blocked-host reporters dropped pre-write. Atomic check-and-record + insert; replays deduped. Surfaces in `admin/abuse-user-reports` (admins poll; no mail/webhook). |
| Anything else (`QuoteAuthorization`, `Tombstone`-as-activity, `View`, `Relay`, `Join`, …) | — | `202 Accepted`, logged as `unsupported-type`, no state change. |

Inbound object types: `Note`, `Article`, `Page`, `Question`, `Answer`
(vote-shaped only), `Tombstone` (delete path), `QuoteAuthorization`
(stamp fetch only, never dispatched as an activity). Required: non-empty
`id`, `attributedTo` (string or `{id}`). `content` prefers
`source.content` for `text/x.misskeymarkdown`/`text/markdown`, else sanitized
HTML→MFM; `summary` → CW; `inReplyTo` string-or-`{id}`; `to`/`cc`
string/object/array; `tag[]` honors `Mention{href}`, `Hashtag{name}`,
`Emoji{name,icon}` (capped at 32); `sensitive: bool`;
`attachment` single/array of `Document|Image|Video|Audio`
(`url|href`, `mediaType`, `name`, `sensitive`, `blurhash`, `width/height`);
polls via `oneOf|anyOf` (+ `replies.totalItems` / `_misskey_votes`,
`endTime`/`closed`). Malformed or policy-dropped payloads are `202`
without storage; storage faults propagate as `5xx` so senders retry.

### Outbound (pinetree → remote)

Local actions enqueue `JobKind::FederateDeliver` in the same storage
transaction as the state change (`__PENDING_NOTE_ID__` substituted in-txn;
`!localOnly` gating at enqueue). Workers deliver; remote failure never
undoes the commit. `inbox_override` variants go direct; `None` resolves
recipients then fans out. Fan-out ≤100 inboxes sends inline, larger splits
into one follow-up job per inbox (`{job_key}:{inbox}` idempotency,
`MAX_JOB_ATTEMPTS=8`, `base 1s × 2^attempts` capped by `Retry-After`/1 h,
terminal failures quarantined with a logged reason).

| Local action | Outbound activity | Recipients |
|---|---|---|
| Note created (non-`localOnly`) | `Create(Note)`, or `Announce` for pure renotes (no text/CW/files) | Followers (unless `specified`) + mentions + visible-users + reply/renote authors. |
| Note edited | `Update(Note)` | Author followers + interacted set (reply author, renote-target author, all repliers, bounded 10k renoters/reactors). |
| Note deleted | `Delete(Note)` (+ `Tombstone` serving at the note URI: `410`) | Author followers only. |
| Poll voted (local) | vote delivery (`PollVote`) | Poll author inbox. Poll tally changes fan out as `Update(Question)`-equivalent to followers (`to: Public` or followers). |
| Reaction created/deleted | `Like` / `Undo(Like)` | Note author's inbox, single-target, per-host wire split (below). |
| Follow / unfollow / accept / reject / cancel | `Follow` / `Undo(Follow)` / `Accept` / `Reject` | Counterparty inbox (followee/follower). `following/invalidate` emits followee-authored `Reject` to remote followers. |
| Block / unblock | `Block` / `Undo(Block)` | Blockee inbox. |
| Move (`i/move`) | `Move(old → new)` | Old account's remote followers. |
| Quote created | `Create(quote-Note)` + `QuoteRequest{object, instrument}` | Note audience + quoted author's inbox. |
| Quote decision (auto) | `Accept{request, result: stamp}` / `Reject{request}` | Quoter inbox. Stamp served at `/users/{id}/stamps/{stamp}` (visibility-gated). |
| Stamp revoked | `Delete(stamp)` | Quote author followers + quote extras. |
| Profile updated | `Update(Person)` (`to: Public`) | Author followers. |
| Pins changed | featured-membership change (`Add`/`Remove`-equivalent `FeaturedChanged`) | Author followers + extras. |
| Clip / page created / updated / deleted (only when `federateClips` / `federatePages` ON, default OFF) | `Create` / `Update(OrderedCollection)` / `Delete(OrderedCollection)`; pages as `Article` | Owner followers + clip followers + extras. |
| Page liked/unliked | page-`Like` / `Undo` | Page owner inbox. |
| Abuse report filed | `Flag` | Reported host's inbox. |
| Suspension | broadcast to known shared inboxes | All previously seen remote shared inboxes. |

Transport (`src/federation/delivery/`): signed `POST` (legacy headers
`Host/Date/Digest/Signature`, `Content-Type: application/activity+json`),
signed `GET` via the system actor for actor/note/stamp fetches. `POST`
never follows redirects (3xx → permanent failure at the retry layer);
`GET` follows ≤3 with `https→http` downgrade refused and every hop
re-resolved/re-validated. DNS is performed per send with SSRF validation
(all resolved addresses checked; loopback/RFC 1918/link-local/CGNAT/
multicast/IPv6 ULA/doc/mapped-v4 rejected) unless `allow_private_ips` /
`allowed_hosts`; TLS always verified (never `-k`); timeouts 10 s op /
20 s total, 5 s DNS, 64 KiB response cap. Blocked-host inboxes are skipped
quietly (`Delivered`) at dispatch.

Delivery converges quietly (`Delivered`) when the author/target is gone,
remote, mismatched, deleted, `localOnly`, or blocked — delivery workers do
not error callers on these paths.

### Visibility and addressing

Outbound audience (`render_note_audience`, Misskey `ApAudienceService`
parity) and inbound parsing (`parse_note_audience`) are inverses:

| pinetree visibility | `to` | `cc` |
|---|---|---|
| `public` | `Public` | followers + mentions |
| `home` (unlisted) | followers | `Public` + mentions |
| `followers` | followers | mentions only |
| `specified` (direct) | visible-user IDs + mentions | — |

`localOnly` (any visibility) is never federated (enqueue skip + delivery
`Delivered` short-circuit). Pending (parent-unresolved) replies are withheld
from AP rendering and from `notes/show`/timelines (fail-closed) until the
resolver links them. Inbound `Announce` with empty `to`/`cc` is coerced to
followers. Outbound `inReplyTo` is the resolved direct parent's canonical URI
only; pending parents emit no `inReplyTo`. Outbound `Update(Question)` uses
`to: Public` or followers. `Update(Person)` uses `to: Public`.
`_misskey_content` is never emitted; `source{mediaType:
text/x.misskeymarkdown}` is always the MFM source of truth.

Local vs remote representation: local actor/note URIs are built from
`public_url` (`/users/{id}`, `/notes/{id}`, `/notes/{id}/activity`); remote
objects keep their stored `uri`. Pure renotes render as `Announce`; quotes
render as notes with quote fields (below), never as bare `Announce`.

## Content and extensions

- **Replies / threading:** direct-parent `inReplyTo` only (no recursive
  trees on the wire). Inbound unresolved `inReplyTo` is preserved as
  `reply_parent_uri` with a coalesced `ReplyParentResolve` job
  (N-waiters → 1 job, TTL-guarded); late parents link children atomically
  and bump `replies_count`. `notes/replies` (direct), `notes/children`
  (replies + quote-renotes), `notes/conversation` (visible ancestor chain,
  oldest-first). Deleted parents decrement counters; deletes are tombstoned
  so late `Create`s are refused.
- **Mentions / hashtags:** outbound `Mention{href, name: @user@host}` per
  resolved mention + `Hashtag{href: {base}/tags/{lower}, name: #tag}`;
  inbound honors the same shapes plus audience URIs as recipient hints.
- **Attachments / remote media:** outbound `attachment[]` of
  `Document|Image|Video|Audio` with `url`, `mediaType`, `name`,
  `sensitive`, `blurhash`, dimensions. Inbound URLs validated (http/https,
  SSRF, size, MIME sniff) before a durable `RemoteMediaFetch` job caches
  bytes to the local `BlobStore` (served at `GET /files/{id}`); metadata in
  `MediaStore`. `folderId` must be null (no folders).
- **Custom emoji:** instance-curated registry (case-insensitive uniqueness;
  `admin/emoji/*`). Inbound `Emoji{name, icon}` tags (≤32/note) ingest
  metadata; remote-emoji rendering is opt-in per user (`showRemoteEmojis`,
  lazy fetch). Foreign `:shortcode:` reactions are stored verbatim. Outbound
  unicode reactions carry no fabricated tag; custom reactions to
  Akkoma/Pleroma hosts include an `Emoji` tag. Foreign custom emoji never
  renders locally (documented).
- **Reactions:** inbound `Like`/`EmojiReact` normalize via
  `_misskey_reaction` → `content` → `Emoji` tag `name` → `❤`. Outbound is
  per-recipient-host via a 7-day NodeInfo probe (`host_caps.rs`): Misskey
  family (incl. Sharkey, Catodon, Firefish, pinetree, unknown/failed probe)
  gets `Like + _misskey_reaction (+ content)`; Akkoma/Pleroma gets
  `EmojiReact{content (+ Emoji tag for :shortcode:)}`; Mastodon family
  (Mastodon, Hometown, GlitchSoc, Pixelfed, Lemmy, GoToSocial, Friendica)
  gets a bare `Like`. **Caveat:** the README reports outbound emoji reactions
  to Akkoma-family hosts as degraded to favorites pending a live round-trip
  test; the `EmojiReact` emit path exists in code but has not passed that
  gate, so treat Akkoma emoji interop as **unverified**.
- **Polls:** outbound `Question` (`oneOf`/`anyOf`, `endTime`/`closed`);
  inbound requires ≥2 named options (2–10 options × 1–50 chars in the
  audited path, else poll-less), votes from `replies.totalItems` or
  `_misskey_votes`. Local votes, `notes/polls/vote`, expiry and federated
  `Update(Question)` tallies are implemented; `pollVoted` streaming events
  fire locally.
- **Reposts:** pure renotes ↔ `Announce` both directions (with `Undo`);
  quotes (notes with commentary) never collapse into pure renotes.
- **Quotes:** five inbound carriers collapse to one `QuoteReference`:
  FEP-044f `quote`, `_misskey_quote` (both slash variants),
  `quoteUrl`, `quoteUri`, one FEP-e232 `Link`. Conflicting canonical-vs-legacy
  targets prefer canonical and record the conflict. Outbound emits canonical
  `quote` + all legacy aliases + one e232 `Link` + `interactionPolicy.canQuote`
  (GoToSocial namespace; `automaticApproval` only — `manualApproval` never
  advertised) + `quoteAuthorization` only when a valid stamp exists + a single
  textual `RE: <URL>` fallback when safe (never duplicated, never added for
  private targets). Stamp verification requires exact
  `interactingObject`/`interactionTarget`/`attributedTo` match via the
  target author's trusted fetch path plus FEP self/mention exceptions
  (structured identity, never substrings). Rejected/revoked quotes are retained
  with their status for the API (`quoteAuthorizationStatus`,
  `quoteTargetUri`/`quoteTargetUnavailable`) and never re-labeled approved.
- **Content warnings / formatting:** `summary` ↔ CW; `sensitive` if CW or any
  sensitive file; outbound HTML is MFM-rendered sanitized HTML, inbound HTML
  sanitized to MFM; `_misskey_content` never emitted or required.
- **Pins / featured:** local pins (`i/pin`, limit 5) federate membership
  changes; remote `Add`/`Remove` for `{actor}/collections/featured` add/remove
  pins (author must equal signer). The served `featured` collection in this
  revision renders empty — remote fetchers must not treat that as
  authoritative for pin state.
- **Clips / pages (pinetree-first divergence, default OFF):** `clips/*` and
  `pages/*` federate as `OrderedCollection` (clips) / `Article` (pages) with
  owner-follower + clip-follower fan-out, `Add`/`Remove` membership,
  clip follows (`Follow` on `/clips/{id}`), page likes and tombstones, gated by
  `federateClips`/`federatePages` and per-resource authenticated-fetch
  callbacks. Misskey has no AP representation for these; other servers
  gracefully ignore them. `Article` senders that are not pages fall back to
  the note pipeline; article-derived notes are not auto-converted to pages
  (recorded divergence).
- **Moves:** `i/move` + inbound `Move` (alias handshake, follower migration,
  replay/forgery covered). No `Move` for clips/pages.
- **Reports:** `users/report-abuse` + `Flag` forwarding; `admin/abuse-user-reports`
  family for review. No mail/webhook notifications.

## Security and validation

Externally relevant, no secrets disclosed:

- Verify inbound (legacy + RFC 9421, `permissive` default; `enforce` mode
  requires valid RFC 9421 — `src/config.rs: federaton.rfc9421_mode`,
  `validate()` + round-trip test). Reject unauthenticated posts (`401`),
  actor≠signer (`401`), authority failures (`401`), oversize bodies (`413`),
  bad JSON/missing `type`/`id` (`400`), unknown personal inbox (`404`).
  Everything else unprocessable is `202` (no state, no retry storm);
  storage faults are `5xx` so honest senders retry.
- Require `(request-target)+host+date` (+`digest` when a body is present)
  for legacy; `@method+@authority+( @path|@target-uri)` + `created` (±skew) +
  covered `content-digest|digest` for RFC 9421. `Digest`/`Content-Digest`
  SHA-256 verified (multi-entry tolerant). `expires` enforced, `nonce`
  ignored, only `rsa-v1_5-sha256`/`rsa-sha256` accepted.
- Fetch gate never emits `401`: signature failures collapse to anonymous;
  `Forbidden+identified → 403`, `Forbidden+anon → 404`, `Hidden → 404`.
- SSRF: http/https only, no `user@`, explicit ports allowed; every DNS
  answer checked against private/reserved ranges (loopback, RFC 1918,
  link-local, CGNAT, multicast, `0/8`, docs, IPv6 ULA/link-local/mapped-v4)
  unless `allow_private_ips`/`allowed_hosts`; redirects ≤3 (GET only),
  `https→http` refused, each hop re-validated; TLS always verified.
- Instance blocks (`blockedHosts`, subdomain-aware, lowercased/deduped):
  enforced inbound before any state write/dedup and at delivery time
  (skipped as `Delivered`); also applied on read (`suspended_or_host_blocked`).
- Federation input treated as untrusted: size/timeout/redirect caps,
  HTML/MFM sanitization, no media blobs in transactions, remote media
  MIME-sniffed and bounded, quote-stamp re-verification rate-limited with a
  negative cache (60 s × failures, ≤1 h) that still discovers revocations.

## Known limitations

Verified (code + docs), not speculation:

- **Loopback-only interop.** No public-internet federation has been run.
  `PINETREE_ALLOW_PRIVATE_IPS=1` is required for local labs; production
  wants the block on.
- **Single-node storage.** One `redb` writer, no read replicas, no live
  migration to multi-node (`--mode=cluster` refuses to start; FDB adapter is
  an empty stub). Export/import covers accounts/follows/blocks/mutes/notes,
  not a transparent upgrade.
- **Outbound `EmojiReact` unverified.** The per-host split exists, but the
  project reports Akkoma-family emoji reactions as degraded to favorites
  until a live round-trip passes. Treat emoji reactions with Akkoma/Pleroma
  as experimental.
- **Foreign custom emoji never renders** locally (documented, not a bug
  from the fetcher's view).
- **`featured` collection serves empty** in this revision; pins still
  federate via `Add`/`Remove` processing and local enforcement.
- **Ephemeral federation caches are process-local:** `Accept`-by-ID index,
  reply-parent coalescer, stamp negative cache and `DestinationTracker`
  (4096 entries, 1 h staleness) are lost on restart. Worst case is an extra
  delivery or a missed bare-IRI `Undo(Accept)` until the next embedded
  `Accept` re-seeds — never a wrong unfollow.
- **Lazy RSA generation** on first use is slow in debug builds; a peer's
  first fetch of a new account can time out.
- **`software.name: misskey`** in NodeInfo is intentional masquerade for
  compatibility; `metadata` carries `nodeName: Pinetree`. Do not fingerprint
  pinetree by `software.name` alone.
- **No `manualApproval` quote workflow,** no review queue, no `Move` for
  lists/clips, no relays/channels/events, no thread muting, no Linked Data
  Signatures, no `_misskey_content`, no Mastodon API. Requests for these get
  `404`/`UNSUPPORTED_*`, never a half-implementation.
- **Clips/pages federation is pinetree-only and default-OFF;**
  `Article`-to-page auto-conversion is an open divergence.
- **Enforce-mode interop unproven:** `rfc9421_mode=enforce` is implemented
  and unit-tested but its effect on major peers has not been measured in a
  live lab in this audit.
- **Search is a bounded substring scan** (no index); `notes/reactions`
  pages newest-first on `sinceId`-only cursors (documented Misskey
  divergence); featured ranking is simplified; timelines filter
  blocked/muted/suspended/instance-blocked on every read.

Uncertainty (honestly marked): real Misskey/Sharkey quote/poll/edit
round-trips were not re-run in this audit (Phase 2/bench records are
historical); DNS resolve→connect TOCTOU, curl `--resolve` IPv6 coverage,
punycode vs subdomain-block edge cases, and `GET /@acct` HTML behavior were
not proven here and are omitted from claims above.

## Additional documentation

All links exist in this repository at the audited revision:

- `README.md` — run, configure, test, project layout.
- `sns-server-proposal.md` — product/architecture source of truth.
- `AGENTS.md` — contributor rules (config via JSON file, style, verification).
- `docs/phase-2-interop-howto.md` — how to run each server in the live lab.
- `docs/testing.md` — full gate (`bash scripts/smoke-all.sh`), suites, topologies.
- `docs/phase-2-exit-criteria.md`, `docs/phase-3-exit-criteria.md` — what the labs proved.
- `docs/identity-rules.md` — handle/account/URL rules.
- `docs/visibility-policy.md` — visibility matrix.
- `docs/activitypub-collections.md` — outbox/followers/following paging.
- `docs/quote-posts.md` — FEP-044f design, wire shapes, conformance boundary.
- `docs/replies.md` — branched replies, pending parents, resolver.
- `docs/federated-collections.md` — clips/pages wire + fetch gate.
- `docs/word-mutes.md` — server-side word-mute enforcement.
- `docs/remote-media-cache-plan.md` — remote media plan/status.
- `state/logs/` — dated work logs with executed output (read the latest entry first).
