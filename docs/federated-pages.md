# Federated Pages — client + federation implementer guide

**Status:** Draft (2026-10-21; supersedes `docs/federated-collections.md` §4, `docs/handoff-federated-clips-pages.md` Phase 3, `docs/endpoint-manifest.md` §38 Phase 3, `docs/keystone-clip-page-surface.md` page parts)

**Sources:** `src/domain/page.rs:1-80`, `src/domain/collection.rs`, `src/application/ports.rs:2739-...` (`PageStore`), `src/application/commands/pages.rs:1-130,242-304`, `src/api/misskey/endpoints/mod.rs:4669-4847`, `src/api/misskey/errors.rs:233-246,1229-1265`, `src/api/misskey/packers.rs:842-...` (`pack_page`), `src/api/misskey/response.rs:106-110`, `src/activitypub/federated_collections.rs:15-33,112-316,589-632,759-...`, `src/federation/fetch_gate.rs:81-152`, `src/federation/inbox.rs:1618-...`, `src/federation/outbox.rs:225,1337,1605-1627`, `src/persistence/redb/mod.rs:158,3884-3885`, `src/domain/instance.rs:112,129`

Pages are long-form documents. Upstream Misskey shape is preserved verbatim on REST. Federation is pinetree-first DIVERGENCE: Misskey pages have zero AP surface. Pinetree federates pages pinetree↔pinetree as `Article`; vanilla Misskey and Mastodon read them as plain notes/posts (graceful degrade), never as pages.

## 1. Data model (what the server stores)

`Page { id (aidx), owner_id, title (≤256, required), name slug (1–256, `pageNameSchema`, unique per owner, reserved chars `:/?#[]@!$&'()*+,;=%` + whitespace/control rejected), summary (nullable ≤256), content (block array), variables (array), script (AiScript ≤16384, local-only), eye_catching_image_id (nullable, must be caller's own file), font (serif|sandbox-sans, default sans-serif), align_center, hide_title_when_pinned (default false), tier (default Listed), page_visibility (Public|Home|Followers|Specified, default Public), visible_user_ids (≤100, Specified only), federate (default true — absent param means true, explicit false opts out), approved_actor_uris, likedCount, remote fields (uri, remote_published_ms, remote_updated_ms, remote_attachments), created/updated_at_ms }`

- `PageLike { user_id, page_id }` unique pair.
- Remote pages: same row with `uri` = origin Article `id`, owner = remote author account row, `script: ""` always, origin attachments verbatim.
- Slugs disambiguated per-owner numeric (`hello`, `hello-1`, ...). Updates are partial-merge with null-clears.
- `script` executes client-side upstream; pinetree has no AiScript engine — stored, never executed server-side, never federated.

## 2. Privacy tiers + read gate (pages have both)

Tier model (same as clips): `Private → LocalApproved → Unlisted → Listed → RemoteApproved`. Local pages default `Listed`. Upstream legacy `visibility` param keeps meaning tier; new pinetree params `pageVisibility` + `visibleUserIds` mean read gate (DIVERGENCE).

`pageVisibility`: `public|home|followers|specified` (default `public`; `home` collapses to Public — pages have no timelines). `visibleUserIds` ≤100 ids, only meaningful with `specified`.

Unauthorized ⇒ `NO_SUCH_PAGE`-hiding on `pages/show` (id and `name+username` forms), AP `GET /pages/{id}`, `pages/like`, `pages/featured` (public only), and pack path. Owner always passes; moderators bypass (consistent with `pages/delete`). Local viewer passes a remote author's followers-only page iff they follow that author; remote viewer of a cached copy fails closed (same posture as DMs). Serving re-checks per request so stale replicas self-heal; updates carry visibility forward.

Admin switch `federatePages` defaults OFF (settable via `admin/update-meta {federatePages}`). Per-page `federate` defaults ON. Either off ⇒ 404 + no fan-out (pages federate only once the instance opts in, even though pages default to federate=true).

## 3. REST API (Misskey parity + pageVisibility/federate extensions)

Base `POST /api/{endpoint}` JSON. Unknown field ⇒ `400 UNSUPPORTED_FIELD`. Packed `Page` ⇒ 200, mutations ⇒ 204 void (create returns 200 packed).

Pack `Page` (`packedPageSchema` parity): `{id, createdAt, updatedAt, title, name, summary, content, variables, script (local reads), font, alignCenter, hideTitleWhenPinned, eyeCatchingImage, attachedFiles (from image blocks), likedCount, isLiked (authed only), user (packed owner), visibility (tier), pageVisibility, visibleUserIds?}`.

| Endpoint | Params | Auth / rate | Returns | Notes |
|---|---|---|---|---|
| `pages/create` | `title!, name! (slug), summary?, content! (blocks), variables?, script?, eyeCatchingImageId?, font?, alignCenter?, hideTitleWhenPinned?, visibility? (tier), pageVisibility?=public, visibleUserIds?, federate?=true` | authed, **10/h** | 200 `Page` | Full body required. `NAME_ALREADY_EXISTS` on slug clash. `NO_SUCH_FILE` if eye-catch not own file. |
| `pages/update` | `pageId!, ...same partial` | owner only, **300/h** | 204 | Partial merge, null clears. `NO_SUCH_PAGE` / `ACCESS_DENIED` / `NAME_ALREADY_EXISTS` / `NO_SUCH_FILE`. |
| `pages/delete` | `pageId!` | owner or moderator (moderator writes `deletePage` log) | 204 | Fans out `Delete(Article)` if federated. Likes cascade. |
| `pages/show` | `{pageId!} \| {name!, username!}` (local users only) | optional; gated pages hide as `NO_SUCH_PAGE` | 200 `Page` | `isLiked` only when authed. |
| `pages/like` / `unlike` | `pageId!` | authed, liker must satisfy visibility | 204 | `YOUR_PAGE` on own? (`PAGE_YOUR_PAGE`), `ALREADY_LIKED` / `NOT_LIKED`. Remote-page likes federate back as `Like`. |
| `pages/featured` | — | optional, public only | 200 `Page[]` top-10 by `likedCount` | Implemented as upstream pure query (no admin curation surface exists upstream — evidence-backed override of old 404 plan). |
| `users/pages` | `userId!` + paging | visibility-gated | 200 `Page[]` | |
| `admin` delete path | via `pages/delete` moderator | moderator | 204 | |

Eye-catch must be caller's own drive file or `NO_SUCH_FILE`. Slug rules enforced (`INVALID_PARAM` on reserved chars/length).

## 4. Federation wire (Article)

### 4.1 Document

`GET /pages/{id}` serves `{@context, id:{base}/pages/{id}, type:Article, attributedTo:owner actor, to/cc (audience mirror below), name:title, summary, content:server-rendered sanitized HTML, url:page URL (=id), published/updated (origin stamps for remote rows, row times otherwise), attachment:[eye-catch Image|Document + origin attachments verbatim], tag:[], source:{content blocks, variables, presentation{font,alignCenter,hideTitleWhenPinned}, slug, mediaType:application/json}}`.

Audience mirror:

| row state | `to` | `cc` |
|---|---|---|
| Listed + Public/Home | public collection | followers collection |
| Listed + Followers | followers collection | — |
| Listed + Specified | addressed actors (local ids→actor URIs, remote kept verbatim) | — |
| any other tier | — | — |

Fetch gate enforces; the claim never over-grants. Specified pages never fan out beyond addressed inboxes.

Block renderer: escapes all text, fixed tag whitelist (`p/h2/section/figure/img/blockquote/a`), image URLs from local drive rows server-constructed, quote links only for `http(s)` or resolvable local notes, unknown blocks dropped. **`script` has no code path to any federated field — structurally excluded, not a flag.**

### 4.2 Activities

- Local create/update (Listed + flag + switch) ⇒ `Create(Article)` / `Update(Article)` (`render_page_activity`, audience copied from Article so Create/Update never over-claim) to followers (+ Specified addressing). Deletes ⇒ `Delete(Article)` to Create/Update recipients.
- Likes on materialized remote pages ⇒ `Like {object:page URI, content:❤}` back to origin (co-located likes need no delivery).
- Inbound: well-formed `type==Article` + non-empty `id` ⇒ materialize as remote page (canonical `uri`; content prefers validated `source.content` blocks, else one synthesized text block from sanitized HTML; `presentation`+`slug` round-trip with fallback defaults; `variables` passthrough; `script` never stored — remote rows keep `""`; slug derived + per-owner disambiguation; owner = remote author row; remote pages never re-federate). Only malformed objects keep the note pipeline. Existing Article-derived notes stay notes (no retro-conversion — DIVERGENCE). Inbound `Update(Article)` refreshes the row when signer owns it (carries visibility forward); inbound owner-signed `Delete(Article-URI)` retracts it (likes cascade). `to/cc` ⇒ visibility ingest mapping: Public-in-`to`⇒Public; Public-in-`cc`⇒Public (explicit collapse); followers collection⇒Followers; else Specified with best-effort ids (unresolvable dropped, fail-closed).
- Tombstones: none for pages (delete = `Delete(Article)` + row removal; receivers drop by URI).

### 4.3 How others read us (no work needed)

- Vanilla Misskey `validPost` includes `Article, Page, ...`; `isPost` true ⇒ `Create(Article)` creates a note (`ApInboxService:427`, `ApNoteService:87`). Title in `name` is ignored for notes; `summary`→CW, `content`→body. Your page appears as a post, not a page.
- Mastodon `SUPPORTED_TYPES=[Note,Question]`, `CONVERTED_TYPES=[Image,Audio,Video,Article,Page,Event]`; docs: "uses content if available, or name if not ... url appended, summary as CW". Your page appears as a toot.
- Clips are invisible to them (no collection-follow semantics). Graceful ignore is correct.

### 4.4 Fetch gate (same as clips)

`authenticate_fetch_viewer` + `decide_fetch` (§6 in clips doc): anonymous→404, known-unauthorized→403, `Hidden`→404. Callbacks: `authorize_page_fetch` (tier + `pageVisibility`). Outbound pulls via `signed_get_json` (system actor).

## 5. What a client implementer must do

1. Create/update with full block JSON; validate slug client-side (1–256, no `:/?#[]@!$&'()*+,;=%` or whitespace/control) to avoid `INVALID_PARAM`. Expose `pageVisibility` (`public|home|followers|specified`) + `visibleUserIds` (≤100) separately from legacy `visibility` (tier). Expose `federate` toggle default ON; warn admin switch still gates federation.
2. Eye-catch: upload via `drive/files/create` first, then pass id; handle `NO_SUCH_FILE`.
3. Show: support both `pageId` and `name+username` forms; treat `NO_SUCH_PAGE` as hidden/not-found (do not distinguish). Render `content` blocks natively when you have them; fall back to `content` HTML string for remote pages. Never execute `script` from a federated payload — there is none, but do not add one.
4. Likes: `pages/like|unlike`, render `isLiked` only when authed, `likedCount` always. Handle `ALREADY_LIKED/NOT_LIKED/YOUR_PAGE`.
5. Featured: `pages/featured` is a pure top-10 query — render as list, no curation UI.
6. Notifications: remote `Like` on your page arrives as `pageLiked` (`{pageId, page}`) via `main:notification` / `i/notifications` (include/exclude `pageLiked`). No new stream channel. Poll `i/notifications` (Keystone REST-only).
7. Export: pages have no export endpoints (only clips do). Persist via `pages/show`.
8. Federation awareness: federated pages never carry `script`; do not expect block-level fidelity on non-pinetree servers (they see HTML post). Pinetree↔pinetree `source` round-trips blocks/variables/presentation/slug.
9. Rate limits: `pages/create` 10/h, `pages/update` 300/h — surface 429 + `Retry-After` with backoff; queue edits client-side.
10. Do not call `page-push`, `i/page-likes`, `i/pages`, `users/flashs` for pages; they 404. Do not expect streaming `clip`/`channel` events for pages.

## 6. Rate limits + error catalogue

- `pages/create` 10/h, `pages/update` 300/h per user (`response.rs:109-110`). Throttle ⇒ `429 RATE_LIMIT_EXCEEDED` + `Retry-After`.
- Global 1 MiB JSON cap ⇒ 413; `UNSUPPORTED_FIELD` 400 on unknown params; `UNKNOWN_API_ENDPOINT` 404 only for truly-unknown.
- Verbatim upstream UUIDs (`errors.rs` `PAGE_*`): `create NO_SUCH_FILE b7b97489-...`, `NAME_ALREADY_EXISTS 4650348e-...`, `update NO_SUCH_PAGE 21149b9e-...`, `ACCESS_DENIED 3c15cd52-...`, `NO_SUCH_FILE cfc23c7c-...`, `NAME_ALREADY_EXISTS 2298a392-...`, `delete NO_SUCH_PAGE eb0c6e1d-...`, `ACCESS_DENIED 8b741b3e-...`, `show NO_SUCH_PAGE 222120c0-...`, `like NO_SUCH_PAGE cc98a8a2-...`, `YOUR_PAGE 28800466-...`, `ALREADY_LIKED d4c1edbe-...`, `unlike NO_SUCH_PAGE a0d41e20-...`, `NOT_LIKED f5e586b0-...`. Gated pages always hide as `NO_SUCH_PAGE` (never 403) on show/like/featured/AP/pack.
- Moderator delete writes `deletePage` moderation log; `deleteAccount` tombstones preserve pages per account-deletion rules.

## 7. Non-goals / open

No version history; no retro-conversion of old Article-notes; no public-directory indexing of unlisted; no executable federation ever; no custom type (fits in `Article` + `source` extension; revisited only if a second implementation wants block-level interop); no `pages/featured` admin curation (upstream has none); relays/channels out. Remote-approved trust caveat applies.
