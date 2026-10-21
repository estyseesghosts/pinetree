# Federated Clips — client + federation implementer guide

**Status:** Draft (2026-10-21; supersedes `docs/federated-collections.md` §3, `docs/handoff-federated-clips-pages.md` Phase 1–2, `docs/endpoint-manifest.md` §38 Phase 1–2, `docs/keystone-clip-page-surface.md` clip parts)

**Sources:** `src/domain/clip.rs:1-57`, `src/domain/collection.rs` (tiers), `src/application/ports.rs:2639-2732` (`ClipStore`), `src/application/commands/clips.rs`, `src/api/misskey/endpoints/mod.rs:4213-4667`, `src/api/misskey/errors.rs:212-230,1003-1015`, `src/api/misskey/packers.rs:747-...` (`pack_clip`), `src/api/misskey/response.rs:106-110`, `src/activitypub/federated_collections.rs:1-120,318-...`, `src/activitypub/actor.rs:553-559` (actor `clips` reference), `src/federation/fetch_gate.rs:81-152`, `src/federation/inbox.rs:376-444,521,573`, `src/federation/outbox.rs` (Add/Remove/Update/Delete Collection), `src/persistence/redb/mod.rs:145,6636-6648` (tombstones)

Clips are named note collections. Upstream Misskey shape is preserved verbatim on REST. Federation is pinetree-first DIVERGENCE: Misskey has no ActivityPub representation for clips. Pinetree federates clips pinetree↔pinetree as `OrderedCollection`; everyone else ignores them gracefully.

## 1. Data model (what the server stores)

`Clip { id (aidx), owner_id, name (1–100 chars), description (nullable, ≤2048, ''→null), is_public (default false), tier (CollectionTier, default from is_public), federate (default false), approved_actor_uris, created_at_ms, last_clipped_at_ms (touched on add-note only, never cleared), updated_at_ms (bumped on every member/metadata change, served as AP `updated`) }`

- `ClipNote { clip_id, note_id }` unique pair. Cap 200 notes/clip (`CLIP_NOTES_PER_CLIP`).
- `ClipFavorite { user_id, clip_id }` unique pair.
- `ClipFollow { id, user_id, clip_id, created_at_ms }` unique pair, no cap in v1. Pinetree-first; Misskey has no clip-follow.
- `CLIP_TOMBSTONES { uri → deleted_at_ms }` kept forever in v1, tiny rows.
- Caps: 10 clips/owner (`CLIP_LIMIT_PER_OWNER`, upstream `clipLimit:10`), 200 notes/clip (upstream `noteEachClipsLimit:200`).

`notesCount` is derived at read time from the index; there is no `clippedCount` counter (DIVERGENCE).

## 2. Privacy tiers (one model for clips + pages)

`Private` (self-only, never leaves) → `LocalApproved` (remote 403) → `Unlisted` (capability URI, served to bearer, never advertised) → `Listed` (advertised, servable, items gated per-item) → `RemoteApproved` (signed fetch, listed approver actors only).

Upstream `isPublic` maps: `true` ⇒ listed-global, `false` ⇒ self-only. Per-clip `federate` flag defaults OFF. Admin switch `federateClips` defaults OFF (`src/domain/instance.rs`, settable via `admin/update-meta {federateClips}`).

Either switch OFF ⇒ local-only behavior, zero protocol footprint: AP routes 404, actor `clips` field omitted, no fan-out.

`RemoteApproved` caveat: we control who can fetch, not what the remote does next (same as DMs). High-stakes content stays local.

## 3. REST API (Misskey parity + pinetree follow extensions)

Base: `POST /api/{endpoint}` JSON `{i, ...params}` (`i` = token; also `Authorization: Bearer`). Auth required unless noted. Unknown field ⇒ `400 UNSUPPORTED_FIELD`. Success: packed objects ⇒ 200, mutations ⇒ 204 void.

Pack `Clip`: `{id, createdAt, lastClippedAt|null, name, description, isPublic, notesCount (owner-only, else hidden), isFollowing (authed only), isFavorited (authed only)}`. Federation columns (`tier`, `federate`, `approved_actor_uris`) are never packed.

| Endpoint | Params | Auth / rate | Returns | Notes |
|---|---|---|---|---|
| `clips/create` | `name! (1–100), isPublic=false, description? (≤2048)` + pinetree `federate?=false, visibility? (tier), approvedActorUris?` | `write:clips`, no dedicated bucket | 200 `Clip` | `TOO_MANY_CLIPS` at 10/owner. `federate` opt-in. |
| `clips/update` | `clipId!, name?, isPublic?, description?, federate?, visibility?, approvedActorUris?` | owner only, no bucket | 204 | Partial merge. Metadata edit fans out `Update(OrderedCollection)` if Listed+flag+switch. |
| `clips/delete` | `clipId!` | owner (or moderator via `admin` path writes log) | 204 | Cascades members + favorites + follows. Writes tombstone, fans out `Delete(OrderedCollection)`. |
| `clips/list` | `limit? (1–100 d10), sinceId/untilId?` | authed, own clips newest-first | 200 `Clip[]` | |
| `clips/show` | `clipId!` | authed optional; private hides as `NO_SUCH_CLIP` unless owner | 200 `Clip` | Adds `isFollowing` when authed. |
| `clips/notes` | `clipId!, limit?, sinceId/untilId?, ...search?` | same visibility as show | 200 `Note[]` | Visibility/mute/block-filtered, per-word text/cw search. Old name `notes/clips` is alias target — call `clips/notes`. Frontend `pages/note.vue` old `notes/clips` 404s; use `clips/notes`. |
| `clips/add-note` | `clipId!, noteId!` | owner, **20/h** (`response.rs:108`) | 204 | `ALREADY_CLIPPED` dup, `TOO_MANY_CLIP_NOTES` at 200. Bumps `updated_at_ms` + `lastClippedAt`. Remote-authored member ⇒ `Add` to author inbox. |
| `clips/remove-note` | `clipId!, noteId!` | owner | 204 | Idempotent. Bumps `updated_at_ms`. Remote-authored ⇒ `Remove`. |
| `clips/favorite` / `unfavorite` | `clipId!` | `write:clip-favorite` | 204 | Private hides as `NO_SUCH_CLIP`; `unfavorite` skips visibility check (upstream parity). `ALREADY_FAVORITED` / `NOT_FAVORITED`. |
| `clips/my-favorites` | paging | authed | 200 `Clip[]` (unpacked rows) | |
| `clips/follow` / `unfollow` | `clipId!` | `write:clip-favorite`, anyone who can read the clip | 204 | Pinetree-first DIVERGENCE. Idempotent, unfollow of unknown ⇒ 204. URI possession + read access is the gate. |
| `clips/followers` | `clipId!` | owner-only | 200 `UserLite[]` | Non-owner ⇒ 403 `ACCESS_DENIED` (pinetree-fixed UUID `errors.rs:954-957`). Others read self-status via `isFollowing`. |
| `i/export-clips` | — | `read:drive`, **1/h** | 200 export job/file (upstream `serializeClip` shape; invisible/gone notes skipped) | |
| `i/import-clips` | `fileId!` | `write:drive`, **1/h** | 204 | Pinetree-only (no upstream endpoint). Find-or-create by name, attach visible notes. |

`notes/show-partial-bulk`, `notes/clips` (old name), `drive` bare routes do not exist — do not call them.

## 4. Federation wire (pinetree↔pinetree only)

### 4.1 Serving

| Route | Gate | Serves |
|---|---|---|
| `GET /clips/{id}[?page=true]` | fetch gate §6 + clip tier callback | `OrderedCollection` / single full-state page |
| `GET /users/{id}/clips[?page=true]` | switch only (reveals nothing unlisted) | advertised index (listed + federate-enabled URIs only) |
| Actor doc `clips` field | switch only | collection reference, omitted when switch off |

- Collection doc: `{@context, id:{base}/clips/{id}, type:OrderedCollection, attributedTo:owner actor, name, summary (=description), totalItems, first:{id}?page=true, updated}` + page `{type:OrderedCollectionPage, partOf, orderedItems:[note URIs only, ≤200], next?}`. Never embedded notes. `prev` never emitted.
- Page size = member cap (200); one full-state page + `first` link is the normal case.
- Per-item gating: only notes visible to the verified fetcher are included (per-item follow edges). Non-visible member resolves to nothing by construction.
- Tombstone: deleted URI ⇒ `410 {type:Tombstone, formerType:OrderedCollection}` while switch on; 404 when switch off.
- Response codes: unknown/ungated ⇒ 404; known signer unauthorized ⇒ 403; unknown/unsigned ⇒ 404 (never confirm existence to strangers).

### 4.2 Fetch gate (shared with pages/communities)

`federation/fetch_gate.rs:81,140`: `authenticate_fetch_viewer` (verify HTTP Signature on GET: cached key, fetch-on-miss, one rotation, `(request-target) host date`, 300 s skew; any failure ⇒ anonymous, never 401) + `decide_fetch` (`Serve→200`, `Forbidden`+identified→403, `Forbidden`+anon→404, `Hidden`→404). Resource callbacks (`authorize_clip_fetch`) sit above the gate.

Outbound pulls use `signed_get_json` (system-actor key).

### 4.3 Sync protocol (no new activity types except Collection Update/Delete)

- Full-state paging with 10-min TTL (`resolve_remote_clip`): compare `updated` stamp before full pulls; matching stamp ⇒ reuse cache without walking `first`.
- Prompt `Add`/`Remove` (`target` = collection URI) for membership changes involving a remote-authored note; outbound to note author's inbox, inbound verifies owner by fetching the collection and comparing `attributedTo`, then evicts cache (never mutates local clips). Idempotent, forged owner-mismatch ignored.
- Metadata edits ⇒ `Update(OrderedCollection)` with embedded full doc (one round trip). Deletes and tier-restriction flips ⇒ `Delete(OrderedCollection)` (receivers evict + prune follows; replays idempotent).
- Fan-out only when `Listed` + per-clip flag + admin switch (unlisted-with-flag gets capability fetch but no broadcast — documented change). Recipients: note authors + owner followers (mirrors pinned-note delivery) + clip followers. One activity id per event, deduped.
- Clip follows: `Follow{actor, object:collection URI}` → owner personal inbox, `Undo(Follow)` retracts. Authority = signer == follower; readable-clip gate; Misskey id-reuse (both ids forgotten on Undo so re-follow converges). Remote rows pruned on non-listed/deleted.
- Article-derived notes are not clips; relays/channels out of scope.

Misskey/Mastodon behavior: they have no clip objects; `Follow` on a collection URI fails validation and is dropped. No action needed — graceful ignore is correct.

## 5. What a client implementer must do

1. CRUD via §3; `name` 1–100, `description` ≤2048, `isPublic` default false. Expose `federate` toggle separately from `isPublic` (federate = allow AP serving; isPublic = local visibility). Default federate OFF; warn that ON still needs admin `federateClips`.
2. Member add/remove: call `clips/add-note|remove-note`; expect 20/h throttle on add (`429` + `Retry-After`). Handle `ALREADY_CLIPPED`, `TOO_MANY_CLIP_NOTES` (200), `NO_SUCH_CLIP/NOTE` with field-accurate `info.param`.
3. Render lists via `clips/list|show|notes`; never construct note lists from AP yourself for local clips. Use `isFollowing` (not `followers`) to gate follow buttons; `followers` is owner-only by design.
4. Follows: `clips/follow|unfollow` (idempotent 204), read `isFollowing`. Expect 403 `ACCESS_DENIED` on `followers` as non-owner — fall back to `isFollowing`.
5. Favorites: `favorite|unfavorite|my-favorites` with `write:clip-favorite`.
6. NO_STREAMING: there is no `clip` federation stream for membership; Keystone is REST-only. Membership events arrive as `main:notification` `clipUpdated` (`{clipId, clip}` on metadata edit/delete/restrict) — poll `i/notifications`. Do not open `useChannel('channel')` for clips.
7. Tombstones: on `clipUpdated` naming a deleted clip, drop cached copy (AP serves `Tombstone`, not the collection).
8. Export/import: `i/export-clips` → file, `i/import-clips {fileId}` (pinetree-only) find-or-create by name.
9. Errors: every failure is `{error:{message,code,id,kind,info?}}`; private clips hide as `NO_SUCH_CLIP` (404) for non-owners; unknown endpoint `UNKNOWN_API_ENDPOINT` 404; unknown field `UNSUPPORTED_FIELD` 400 naming `info.param`; void mutations 204 with empty body.
10. Do not call `notes/clips` (old alias), `channels/*`, `chat/*` for clips. Do not expect `notesCount` for non-owners, `isFavorited/isFollowing` for anonymous.

## 6. Rate limits + error catalogue

- `clips/add-note` 20/h per user; `i/export-clips`, `i/import-clips` 1/h each (pinetree documents 1/h where upstream uses 1/day for clips — looser, intentional). Others in §3 have no dedicated bucket (global JSON 1 MiB cap → 413, per-family buckets apply where noted in manifest §1).
- Throttle ⇒ `429 RATE_LIMIT_EXCEEDED` + `Retry-After`.
- Verbatim upstream UUIDs (`errors.rs` `CLIP_*`): `create TOO_MANY_CLIPS 920f7c2d-...`, `update/delete/show/notes/add/remove/favorite/unfavorite/follow NO_SUCH_CLIP*` per-endpoint ids, `add-note NO_SUCH_NOTE fc8c0b49-...`, `ALREADY_CLIPPED 734806c4-...`, `TOO_MANY_CLIP_NOTES f0dba960-...`, `ALREADY_FAVORITED 92658936-...`, `NOT_FAVORITED 90c3a9e8-...`, `followers ACCESS_DENIED 3edf8e54-...` (pinetree-fixed). One-UUID-per-code convention (update reuses create UUID where upstream mints a second).
- Moderator refusals: `400 INVALID_PARAM` + message (upstream would throw untyped 500).

## 7. Non-goals / open

No version history; no retro-conversion; no clip-subscribe beyond follow; no fetch-log tracking; no public-directory indexing of unlisted; no executable content; no relays/channels. Remote-approved trust caveat applies. `Update/ Delete(OrderedCollection)` fan-out and tombstone 410 are the only new AP behaviors; everything else reuses delivery/dedup/retry machinery.
