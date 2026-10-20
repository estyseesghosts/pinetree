# Federated clips + pages, phase 2: sync, follow-push, gated pages, Article fidelity

Status: plan (build after Batch Q lands).

## Why

Phase 1 (local parity + wire + delivery fan-out for clips/pages) is committed (`5f5d416`, `7f0c965`). Two gaps remain, both raised by the owner:

1. **Clips need real sync + notification.** Membership promptness exists (Add/Remove to note authors), but metadata edits, deletes/restrictions, and staleness detection don't propagate, and nobody is *told* — there is no user-facing signal and no way to follow a clip.
2. **Pages need their loss list closed within `Article`, or a new type.** Owner decision, recorded here: **no new object type** — everything shippable fits in `Article` + the existing `source` extension point. A custom type is only revisited if a second implementation wants block-level interop (criteria in §5).

A third owner decision, also recorded here: **gated (non-public) `Article`s materialize as pages, not notes.** That requires page-level read gating first (Phase 2), since today's pages are world-readable.

## Starting state (HEAD at time of writing — re-verify at build start)

- `Clip {id, owner, name≤100, description≤2048, is_public, tier, federate (default off), approved_actor_uris, created_at, lastClippedAt}`; tier derived from `isPublic` unless overridden; clips default private + unfederated.
- `Page {…, tier, federate (default ON), approved_actor_uris, uri (canonical remote URI), remote_attachments, liked_count}`; **no `visibility` field** — show/AP/like/featured are ungated.
- Fetch gate generic (`fetch_gate.rs`): signature verify → viewer; `decide_fetch` maps resource decisions to serve/403/404. Contract: `docs/federated-collections.md` §1.
- Wire: clips as `OrderedCollection` (note URIs only, **no `updated` field**); pages as `Article` (`to`/`cc` from tier: Listed ⇒ Public+followers, else empty).
- Outbox: `ClipMembershipChanged` (Add/Remove, note-author inboxes only, no tier check), `PageCreated`/`PageUpdated` (followers fan-out, Listed-only), `PageLiked` (to origin). No `Update`/`Delete` for collections; no outbound `Delete(Article)`.
- Inbound: Add/Remove ⇒ cache eviction with owner verification (never mutates local clips); `Like(Page)` counted; embedded `Update(Article)` refreshes the materialized page else falls to the note pipeline; `Delete(Article-URI)` retracts the page; public Articles materialize as remote pages (`script` never stored); **non-public Articles fall back to notes** (to be removed in Phase 2).
- Tombstones: none for clips. Deleted clip URIs 404 like never-existed ones.
- Follow model for clips: none. Notification kinds: no clip/page kinds; `Notification` has `note_id`/`fileId`/`draftId` nullable refs, nothing clip/page-shaped.
- Config rule (AGENTS.md §Configuration): any new setting needs a serde field, default, `validate()` coverage, round-trip test, and docs. **This plan needs no new settings** (timeouts/TTLs stay constants, rate buckets reuse the existing table); if one becomes necessary it follows the rule.
- Test harness: per-node Tokio runtimes (`tests/federation.rs` `FedNode`).

## Delegation rule (AGENTS.md hard rule)

- `git status --short` first; respect uncommitted work (Batch Q may still be dirty); stage/commit only your own files, only when told.
- Partition subagent work into **disjoint file sets**. Shared files (dispatcher `api_dispatch`, `ports.rs`, `errors.rs`/`packers.rs`, `docs/*`, existing test files, routes) are parent-owned — **subagents propose patch snippets, never edit shared files directly**.
- Subagents use uniquely-named test files, leave changes uncommitted, never `git commit`/`stash`/`reset`/`checkout --` anything.

Suggested split (parallel investigation ok, serial build):

- Subagent A (clip sync wire): `Update(Collection)`/`Delete(Collection)` shapes, `updated` field, Tombstone serving precedent (`objects.rs` note path), dedup/idempotency patterns for replays.
- Subagent B (follow primitive): Misskey has no clip-follow; survey how user-list favorites + `Follow`/`Undo` inbox handling + fan-out recipient collection (`collect_recipient_inboxes_for_author`) work, and propose the `ClipFollow` row shape + authority rules.
- Subagent C (notifications): `NotificationKind` wire types, `notificationRecieveConfig` registration points, pack path for non-note entities, and where Keystone would render the new kinds (report only).

## Config rule

No new `Config` keys in this plan. (If fan-out ever needs a knob — batch window, tombstone retention — it follows AGENTS.md §Configuration: serde field, default, `validate()`, round-trip test, documented key list. None is proposed.)

## Phase 0 — reconcile with Batch Q (read-only first, no code)

Batch Q Phase 0/1 covers adjacent federation ground (featured Add/Remove ingest, `Undo(Accept)`, `Delete(Person)` marking; possibly the nine-fixes pass touches the same inbox dispatch arms). Before building:

1. Re-verify at HEAD which of Q's federation items landed and where; the clip `Add`/`Remove` eviction path and the new `Update`/`Delete(Collection)` arms must not double-handle or contradict them. Shared dispatch arms stay parent-owned; conflicts resolve by editing one side, never both in parallel.
2. Confirm the starting-state list above (especially: no `visibility` on `Page`, no clip tombstones, no `updated` on collections, no follow store).
3. Output: a one-page delta note appended to the batch log (what Q already covered vs what this plan still owns).

## Phase 1 — pages: visibility, then gated ingest (leak-risk work goes first)

Page-level read gating must land *before* any non-public Article becomes a page; otherwise restricted content becomes world-readable.

1. **Domain**: `Page.visibility: Visibility` (reuse the note enum) + `Page.visible_user_ids: Vec<String>`, both `#[serde(default)]` (old rows decode as public). Local creates default `Public` — no local behavior change (upstream parity).
2. **Enforcement points** (unauthorized ⇒ `NO_SUCH_CLIP`-style hiding as `NO_SUCH_PAGE`, never a distinguishing error):
   - `pages/show` (id and name+username forms),
   - AP `GET /pages/{id}` (gate callback checks visibility *and* tier),
   - `pages/like` (liker must satisfy visibility; keep `YOUR_PAGE`/`ALREADY_LIKED` semantics),
   - `pages/featured` (public only),
   - pack path (pack only what show/AP already authorized; no independent reads).
   Owner always passes; moderators bypass (consistent with `pages/delete`).
3. **Ingest mapping** (`to`/`cc` → visibility, documented table): Public-in-`to` ⇒ Public; Public-in-`cc` ⇒ Public (pages have no timelines to exclude from — explicit, documented collapse); followers collection in `to`/`cc` ⇒ Followers; otherwise Specified with best-effort-resolved user ids (unresolvable recipients dropped, fail-closed, same as note ingest).
4. **Kill the note fallback** for well-formed Articles (object, `type == "Article"`, non-empty `id`). Only malformed objects keep the old path. Existing Article-derived *notes* stay notes — no retro-conversion (recorded DIVERGENCE).
5. **Remote-followers rule, stated plainly**: a local viewer passes a remote author's followers-only page iff they follow that author (local edge, checkable). A *remote* viewer of our cached copy cannot be verified ⇒ denied (fail closed, same posture as federated DMs).
6. Update path carries visibility forward (refresh from the new doc); serving re-checks per request, so stale replicas self-heal on refetch.

## Phase 2 — clips: sync completion + follow + follower-wide push

### 2a. Sync protocol (silent convergence)

1. **`updated` on the collection document** (standard ActivityStreams field, served + compared by `resolve_remote_clip` before full pulls).
2. **Clip tombstones**: new `PAGES_BY_URI`-style index (`CLIP_TOMBSTONES`: uri → deleted_at). Deletes write one; serving a tombstoned URI returns `Tombstone` (`formerType: OrderedCollection`) instead of 404, so receivers distinguish "deleted, drop your copy" from "hidden from you". Tiny rows, no prune in v1 (documented).
3. **`Update(Collection)`** for metadata edits (rename/description/tier/flag), carrying the **embedded full collection document** (one round trip, no refetch). Authority = owner (verified like Add/Remove); apply = refresh-or-evict; replays idempotent.
4. **`Delete(Collection)`** on delete **and** on tier flips to a more restrictive tier (listed→anything else). Same authority/idempotency; receivers evict + prune dependent state (see 2b/2c).
5. **Fan-out rule change (small, aligns with pages): fan-out fires only for `Listed` + federate-flag + switch.** Unlisted/approved clips keep capability/direct fetch with zero broadcast (today Add/Remove check flag+switch but not tier — tighten it; document the behavior change: unlisted-with-flag loses promptness).
6. **Recipients: note authors (direct, existing) + owner followers (new fan-out, mirrors pinned-note delivery) + clip followers (new, §2b).** Reuse `collect_recipient_inboxes_for_author` + per-inbox jobs; the existing idempotency-key scheme carries over.

### 2b. Clip follow primitive (IN SCOPE — follower-wide push needs it)

Misskey has no clip-follow, so this is pinetree-first surface (pinetree-fixed error UUIDs, documented as such):

1. **`ClipFollowStore` port** (`ClipFollow {id, user_id, clip_id, created_at}`; unique pair): `follow_clip` (idempotent bool), `unfollow_clip` (idempotent bool), `is_following`, `followers_of_clip` (newest-first, bounded), `followed_clips_of` (for timelines/lookup). redb tables + new conformance **Group Z** (duplicate converges; unfollow idempotent; delete cascades; cap? — no cap v1, document why: follows are cheap rows, no fan-out amplification at rest).
2. **Endpoints** `clips/follow|unfollow` (credential + `write:clip-favorite`-adjacent permission — reuse `write:account`? decide at build: needs *some* kind; propose `write:clip-favorite` for symmetry with clip favorites, record the choice) + `isFollowing` in the clip pack for authed viewers + `clips/followers` (owner-only list; others get only self-status — following an unlisted clip must not advertise the follower set).
3. **Who may follow**: anyone who can read the clip (`visible_clip` rule reuse). No separate capability needed — possession of the URI plus read access is the gate.
4. **Federation**: `Follow{actor, object: collection URI}` → clip owner's inbox (personal, like `PageLiked`'s addressing); `Undo(Follow)` retracts; authority = signer == follower; deduped; remote rows pruned when the clip goes non-listed/deleted. Remote followers join the §2a fan-out set.
5. **Keystone contract**: endpoint shapes + `isFollowing` field + which main-channel/stream (if any) carries membership events — propose notifications only, no new stream channel (record the decision).

### 2c. User-facing notifications (the "notify" half)

1. New kinds `ClipUpdated` (wire `clipUpdated`) + `PageLiked` (wire `pageLiked`), following the `ExportCompleted` precedent for pinetree-only wire types (DIVERGENCE, Keystone-only consumers).
2. New nullable `Notification.clip_id` + `Notification.page_id` (`#[serde(default)]`, mirrors `fileId`/`draftId` columns).
3. Emit points (all through the existing durable `Notify` path):
   - note added to / removed from a clip → notify the note's author;
   - clip metadata updated → notify clip followers;
   - clip deleted or restricted → notify followers + all member-note authors;
   - remote `Like` on a page → notify the page owner (closes the silent-likes gap).
4. Register the wire types wherever notification types are enumerated (receive-config validation, include/exclude filters); pack embedded clip/page like notes are embedded for reactions.
5. Keystone rendering is Keystone-side (reported, not implemented — same pattern as page likes).

## Phase 3 — Article fidelity small wins (all inside `Article`, no new type)

1. **Origin timestamps**: store `remote_published_ms`/`remote_updated_ms` at ingest (`#[serde(default)] None` = pre-existing rows), serve those instead of row times (fall back to row times when absent).
2. **Outbound `Delete(Article)`** on local page delete (same recipients as Create/Update). Pure omission fix; inbound side already handles it.
3. **`source` presentation extension**: document (in `docs/federated-collections.md` §4) the extended `source` schema — content blocks + variables + `presentation {font, alignCenter, hideTitleWhenPinned}` + slug. No new top-level fields.
4. **Audience audit**: confirm emitted `to`/`cc` matches the new visibility (Public ⇒ public collection; Followers ⇒ followers collection; Specified ⇒ addressed actors, no public claim). Fix gaps found.

## Phase 4 — contracts, docs, gates

- `docs/federated-collections.md`: §§2/4 updates (defaults, `updated`+Tombstone, `source` schema, audience table, follow handshake, no-new-type criteria reaffirmed).
- Manifest superseding note (§37), parity-plan §14 line, profile §5 (clips/follow + gated pages move in-scope), wiki + README can/can't lists, API-surface doc for Keystone (new endpoints, `isFollowing`, notification wire types, no new stream channel).
- Conformance: Group Z (clip follows + tombstones) + Group Y extension (visibility columns, URI index already Y3); bump counts + `docs/README.md` count line.
- Keystone sweep: cover `clips/follow|unfollow|followers`, gated `pages/show`, new notification kinds (update the 48/0 baseline where calls stop 404ing).
- State log per AGENTS.md with executed evidence.

## Gates (exit deliverables)

1. `cargo test --offline` green (counts recorded: lib + conformance + integration).
2. `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK`; `bash scripts/keystone-sweep.sh` → all green (sweep updated).
3. **Fetch-gate matrix re-green** (tiers × visibility × signer) after the Page visibility addition.
4. **Two-local-pinetree clip tests**: subscribe handshake (`Follow`/`Undo` converge, forged follow ignored), `Update`/`Delete` converge incl. tier-restriction pruning, fan-out receipt at followers, author + follower notifications recorded, TTL + `updated` revalidation, tombstone served at deleted URI.
5. **Two-local-pinetree page tests**: gated `Article` → remote page row (not a note) for followers audience; stranger gets 403/404 with zero content bytes on *every* surface (show/AP/like/featured/export); `Update`/`Delete`/`Like` converge; origin timestamps survive the round trip; non-public content never appears in logs/packs of unauthorized readers.
6. State logs per AGENTS.md with real command output.

## Non-goals (explicit)

- Fetch logs / "who pulled my clip" tracking (privacy liability; TTL + fan-out converge without it).
- A new object type (criteria for revisit in §5 of the prior discussion: a second implementation wanting block-level interop).
- Version history for pages/clips; retro-conversion of old Article-derived notes.
- Clip-subscribe *beyond* follow (no separate "watch without following" mode; follow IS the subscribe).
- SMTP, Mastodon API, OAuth, relays, custom roles (standing non-goals, unchanged).
- Anything in Batch Q's punch list not reconciled in Phase 0 — Q owns it.

## Open questions for the owner (answer before build, or bless the defaults)

1. **Followers-list visibility**: owner-only list + self-status for others (proposed). Alternative: public list on listed clips. Default?
2. **Notification noise**: followers notified on *every* membership edit, or only on delete/restrict + metadata (membership edits → note authors only)? Proposed: the latter (quieter).
3. **`clips/follow` permission kind**: reuse `write:clip-favorite` (proposed) vs mint `write:clip-follows`.
4. **Tombstone retention**: keep forever (proposed; rows are tiny) vs time-bounded prune (needs a retention setting → Config rule churn).
