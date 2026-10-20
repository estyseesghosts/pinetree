# Federated collections: clips + pages (pinetree↔pinetree)

Status: Draft (Phase 1 local + wire + fan-out landed 2026-10-20; Phase 2
sync/follow/gated-pages + Phase 3 fidelity landed here as built).

Clips and pages are server-local in Misskey (no AP representation), so
everything below is a pinetree-first DIVERGENCE — pinetree↔pinetree only,
gracefully ignored by everyone else. This doc is the contract a remote
implementer (or the private-community track,
`docs/community-planning.md`) codes against.

## 1. Authenticated-fetch gate (shared infrastructure)

Location: `src/federation/fetch_gate.rs`. The gate does two things and
nothing else:

1. **Authenticate the fetcher** (`authenticate_fetch_viewer`): verify the
   HTTP Signature on an inbound GET (same key-lookup discipline as the
   inbox: cached key, fetch-on-miss, one refresh on rotation; requires
   signed `(request-target)`, `host`, `date`; 300 s skew). Any failure —
   missing/unparsable header, unresolvable key, bad signature — collapses
   to an **anonymous viewer**, never a 401/403. Rationale: existence-hiding
   resources must 404 strangers without confirming the object exists.
2. **Map (viewer, resource decision) to serve/403/404**
   (`decide_fetch`):
   - `Serve` → 200 with the object.
   - `Forbidden` + identified viewer → 403.
   - `Forbidden` + anonymous viewer → 404 (existence unconfirmed).
   - `Hidden` → 404 always.

Resource authorization lives in per-resource callbacks **above** the gate
(e.g. `AppServices::authorize_clip_fetch`,
`AppServices::authorize_page_fetch`: pure functions over the resource row
+ switches + `Option<actor_uri>`). **No object-type logic lives below the
callback** — the gate never inspects what kind of object is fetched.

### Reuse contract (community track)

Private-community fetch (`docs/community-planning.md`: "private fetch
requires follower-instance signature", "reader checks + signed
`is_follower` probe") calls the same two functions:

```rust
let viewer = authenticate_fetch_viewer(&services, &original_target, &headers);
let decision: FetchDecision = /* community membership check */;
decide_fetch(&viewer, decision)?; // Ok(()) = serve, Err(status) = answer it
```

Rules for new callbacks: return `Serve` only for explicitly authorized
fetchers; `Forbidden` for identified-but-unauthorized; `Hidden` for
local-only/deleted/unknown. Never distinguish "forbidden" from "missing"
yourself — the gate's anonymous→404 collapse does that generically.

Signed-outbound fetching (`signed_get_json` in
`src/federation/delivery.rs`, system-actor key) is the counterpart for
server-to-server pulls.

## 2. Privacy tiers (clips + pages, one model)

`Private` (self-only, never leaves) → `LocalApproved` (remote 403) →
`Unlisted` (capability URI, served to bearer, never advertised) →
`Listed` (advertised, collection servable, items gated per-item) →
`RemoteApproved` (signed fetch, listed approver actors only).

Upstream `isPublic` maps onto the model (`true` ⇒ listed-global, `false`
⇒ self-only). Clips stay private by default: `isPublic` defaults false
and the per-clip `federate` flag defaults OFF. Pages are public by
default: no visibility gate (upstream parity) and the per-page
`federate` flag defaults ON (an absent `federate` param means true;
explicit `false` opts out). Either the admin switch
(`federateClips`/`federatePages`, default OFF, `admin/update-meta`) or
the per-object flag off ⇒ local-only behavior, zero protocol footprint
(routes 404, actor `clips` reference omitted) — so pages federate only
once the instance opts in.

Remote-approved carries the standing trust caveat: we control who can
fetch, not what the remote server does next (same as federated DMs).
High-stakes content stays local-only.

## 3. Clip wire shape

- Collection: `OrderedCollection` at `{base}/clips/{id}` with
  `attributedTo`, `name`, `summary`, `totalItems`, paged **note URIs only**
  (never embedded notes; page size 200 = the member cap; single full-state
  page + `first` link). No new activity types: collection state is the
  protocol.
- Membership sync: full-state paging with 10-minute TTL revalidation
  (`resolve_remote_clip`) + inbound `Add`/`Remove` for promptness
  (idempotent cache eviction, authority = collection owner verified by
  fetching the collection and comparing `attributedTo`).
- Item privacy composes: every served note URI resolves through the
  existing note path on the fetching side; the serving side only includes
  notes visible to the verified fetcher (per-item follow edges). A
  non-visible note in a visible collection resolves to nothing — by
  construction, never by special-casing.
- Advertisement: listed clips in the actor's `clips` collection reference
  (`{actor}/clips` → `OrderedCollection` of listed + federate-enabled
  clip URIs); unlisted never advertised; nothing in NodeInfo, search, or
  public timelines.
- Outbound promptness: adding/removing a **remote-authored** note
  delivers `Add`/`Remove` (`target` = collection URI) to the note
  author's inbox. Local-authored members need no delivery (co-located).
- Membership sync completion (Phase 2a): the collection document carries
  the standard ActivityStreams **`updated`** stamp, compared by
  `resolve_remote_clip` before full pulls (matching stamp ⇒ reuse the
  cached membership without walking `first`). Metadata edits fan out
  **`Update(OrderedCollection)`** carrying the embedded full collection
  document (one round trip, no refetch); deletes — and tier flips to a
  more restrictive tier — fan out **`Delete(OrderedCollection)`**.
  Authority is always the owner (verified by fetching the collection and
  comparing `attributedTo`, same as `Add`/`Remove`); receivers
  refresh-or-evict, replays are idempotent. Fan-out fires only for
  `Listed` + federate-flag + switch (unlisted-with-flag loses broadcast —
  documented behavior change vs Phase 1); recipients are note authors
  (direct, existing) + owner followers (new, mirrors pinned-note delivery)
  + clip followers (new).
- Clip tombstones: deletes write a `CLIP_TOMBSTONES` row (`uri` →
  `deleted_at_ms`, tiny rows, kept forever in v1). Serving a tombstoned
  URI returns `Tombstone` (`formerType: OrderedCollection`) instead of
  404, so receivers distinguish "deleted, drop your copy" from "hidden
  from you". The switch off still forces 404 (zero protocol footprint).
- Clip follows (pinetree-first — Misskey has no clip-follow; pinetree-fixed
  error UUIDs, documented as DIVERGENCE): `ClipFollowStore`
  (`ClipFollow {id, user_id, clip_id, created_at_at_ms}`, unique pair;
  duplicate converges, unfollow idempotent, delete cascades, no cap in v1).
  Endpoints `clips/follow|unfollow` (permission `write:clip-favorite`,
  recorded choice), `isFollowing` in the clip pack for authed viewers,
  `clips/followers` owner-only (others get 403 and read self-status via
  `isFollowing` — following a clip must not advertise the follower set).
  Anyone who can read the clip may follow (possession of the URI + read
  access is the gate). Federation: `Follow{actor, object: collection URI}`
  → clip owner's personal inbox, `Undo(Follow)` retracts; authority =
  signer == follower; deduped (Misskey id-reuse: both ids forgotten on
  Undo so re-follow converges); remote rows pruned when the clip goes
  non-listed/deleted. No new stream channel — membership events surface
  as notifications only (recorded decision).

## 4. Page wire shape

- Document: `Article` at `{base}/pages/{id}` with `name` = title,
  `content` = server-rendered sanitized HTML, `summary`, `url` = page
  URL, `attributedTo`, `published`/`updated` (origin timestamps when the
  row was materialized remotely, row times otherwise), `attachment` =
  eye-catch image (+ origin attachments verbatim for remote pages),
  `tag` = [], `source` = `{content` blocks, `variables`, `presentation`
  `{font, alignCenter, hideTitleWhenPinned}`, `slug}` for
  pinetree↔pinetree fidelity (no new top-level fields). Audience mirrors
  tier + visibility (audited, Phase 3):

  | row state | `to` | `cc` |
  |---|---|---|
  | `Listed` + Public (or Home — pages have no timelines) | public collection | followers collection |
  | `Listed` + Followers | followers collection | — |
  | `Listed` + Specified | addressed actors (local ids resolved, remote kept verbatim) | — |
  | any other tier | — | — |

  The fetch gate, not this claim, enforces who reads.
- Read gate (Phase 1 — lands before any non-public `Article` becomes a
  page): `Page.visibility` reuses the note enum (`Public` default, `Home`
  collapses to Public) + `visible_user_ids`; old rows decode as public.
  Unauthorized ⇒ `NO_SUCH_PAGE`-hiding on `pages/show` (id and name forms),
  AP `GET /pages/{id}`, `pages/like` (liker must satisfy visibility),
  `pages/featured` (public only), and the pack path (pack only what show/AP
  already authorized). Owner always passes; moderators bypass (consistent
  with `pages/delete`). A local viewer passes a remote author's
  followers-only page iff they follow that author; a *remote* viewer of a
  cached copy cannot be verified ⇒ denied (fail closed, same posture as
  federated DMs). Serving re-checks per request, so stale replicas self-heal
  on refetch; the update path carries visibility forward.
- Ingest mapping (`to`/`cc` → visibility, documented table): Public-in-`to`
  ⇒ Public; Public-in-`cc` ⇒ Public (explicit, documented collapse);
  followers collection in `to`/`cc` ⇒ Followers; otherwise Specified with
  best-effort-resolved user ids (unresolvable recipients dropped,
  fail-closed, same as note ingest).
- **Inbound `Article`s materialize as pages, not notes** (any well-formed
  object with `type == "Article"` and non-empty `id`; only malformed objects
  keep the old note path). Existing Article-derived *notes* stay notes — no
  retro-conversion (recorded DIVERGENCE). Identity is the Article `id`
  (stored as the row's canonical `uri`; updates/deletes/likes correlate by
  URI, never slug). Content prefers `source.content` blocks (validated),
  else one synthesized text block from sanitized HTML; `presentation` +
  `slug` round-trip font/align/hide + the author's slug (unknown values
  fall back to local defaults); `variables` pass through.
  **`script` is NEVER stored from remote input** (remote rows keep
  `script: ""`) and NEVER serialized into any federated field,
  structurally. Slugs derive from the title (or remote `slug` when valid)
  with per-owner numeric disambiguation. Owner is the remote author's
  account row; remote pages never re-federate (`Update`/`Delete` inbound
  apply to the row; outbound `Create`/`Update` fan-out is local-author
  only; Specified pages never fan out).
- **`script` is NEVER serialized into any federated field,
  structurally**: the serializer has no code path that reads
  `page.script`. Block rendering escapes all text and only emits a fixed
  tag whitelist (`p/h2/section/figure/img/blockquote/a`); image URLs come
  from local drive rows (server-constructed), quote links only for
  `http(s)` URLs or resolvable local notes; unknown blocks are dropped.
- Updates ride the existing `Update(Article)` delivery path (inbound
  `Update(Article)` refreshes the materialized page when the signer owns
  it; otherwise the note pipeline). The refresh carries visibility forward.
- Deletes: inbound owner-signed `Delete(Article-URI)` retracts the
  materialized page (likes cascade); local page deletes fan out
  `Delete(Article)` to the Create/Update recipients (inbound side already
  retracted — pure omission fix).
- Page likes: stored + counted; likes on materialized remote pages
  federate back to the origin as `Like` with the page-URI object
  (pinetree↔pinetree; co-located likes need no delivery; degrades to
  nothing elsewhere). Remote likes notify the page owner (`pageLiked` —
  closes the silent-likes gap). Local notifications for clip activity:
  note added to / removed from a clip → notify the note's author; clip
  metadata updated → notify clip followers; clip deleted or restricted →
  notify followers + all member-note authors (membership edits do NOT spam
  followers — quieter rule, recorded default).

## 5. Route table

| Route | Gate | Serves |
|---|---|---|
| `GET /clips/{id}[?page=true]` | §1 + clip tier callback | `OrderedCollection` / page |
| `GET /users/{id}/clips[?page=true]` | switch only (index reveals nothing unlisted) | advertised clips index |
| `GET /pages/{id}` | §1 + page tier callback + visibility gate (§4) | `Article` |
| Actor doc `clips` field | switch only | collection reference |

Response codes: unknown/ungated ⇒ 404; deleted clips ⇒ 410 `Tombstone`
(`formerType: OrderedCollection`) while the switch is on; known signer
without authorization ⇒ 403; unknown/unsigned ⇒ 404 (existence unconfirmed).

## 6. Non-goals (recorded)

Channels (Lemmy covers that later). New activity types beyond
`Add`/`Remove` handling **except the Phase 2 clip-sync pair
(`Update`/`Delete` of `OrderedCollection`) and the page-delete
`Delete(Article)`** — both reuse existing delivery/fan-out machinery.
Executable content federation, ever.
Public-directory/indexing of unlisted objects. `pages/featured` stays a
pure query (upstream parity — no admin-curation surface exists upstream).
No new object type: everything shippable fits in `Article` + the existing
`source` extension point (reaffirmed — a custom type is revisited only if
a second implementation wants block-level interop). No version history for
pages/clips; no retro-conversion of old Article-derived notes. Clip-subscribe
beyond follow (follow IS the subscribe). Fetch logs / "who pulled my clip"
tracking (privacy liability). SMTP, Mastodon API, OAuth, relays, custom roles
(standing non-goals, unchanged).
