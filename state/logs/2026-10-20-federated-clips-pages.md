# 2026-10-20 — Federated clips + pages (handoff, Phases 0–3 local + wire)

Implements `docs/handoff-federated-clips-pages.md` with three plan adjustments
landed during the session:

1. **Phase 0 paused, then reinstated generic.** Fetch-gate work paused per
   owner direction ("we're going to change how we do authenticated
   fetch"), then reinstated with a second customer
   (`docs/community-planning.md` private-community fetch): the gate is
   generic (signature verification + serve/403/404 decision interface),
   with clip/page/community authorization as callbacks above it. No
   object-type logic below the callback.
2. **`pages/featured` implemented, not 404.** The handoff assumed an
   admin-curation surface; upstream `featured.ts` is a pure top-10-by-
   `likedCount` query. Evidence-backed override, recorded in the manifest.
3. **Inbound `Article`s materialize as pages, not notes** (owner
   direction mid-session, superseding the handoff's note-pipeline line
   for public Articles). Public inbound `Article` → remote page row
   (canonical `uri` + URI index, content from `source` blocks or
   sanitized synthesis, `script` never stored, slug derived +
   disambiguated, owner = remote author row, never re-federated).
   Non-public Articles keep the note pipeline (visibility enforcement).
   Updates/deletes/likes correlate by URI. Page likes on remote pages
   federate back to the origin as `Like`.
5. **Pages federate by default; clips stay private** (owner decision
   2026-10-20). `PageBody.federate` absent ⇒ true (explicit `false`
   opts out); clips unchanged (`isPublic` false, per-clip flag false).
   The admin switches still gate everything (default OFF), so there is
   no protocol footprint until the instance opts in. Documented in the
   handoff §Phase 3, manifest §36, `federated-collections.md` §2, and
   pinned by a store-read assertion in `pages_crud_likes_and_featured`.
4. **Test harness: per-node Tokio runtimes.** A clips gate test hung
   deterministically on the shared test runtime (B stopped accepting
   while all workers idled, timers dead, EAGAIN on reads). Giving each
   `FedNode` its own runtime (closer to prod, where nodes are separate
   processes) fixed it; the gate then passes in ~2s. Pre-existing tests
   migrated to the same harness (`boot_node` is sync via a raw spawn
   thread; `Drop` shuts runtimes down without blocking).

Solo build (no subagents for edits — parent-owned shared files per
AGENTS.md). Three read-only investigation subagents (clips upstream,
pages upstream, auth-fetch infra) fed the build; two of their UUIDs were
mistyped in the reports and corrected against the upstream checkout
(`ALREADY_FAVORITED`, `TOO_MANY_CLIP_NOTES` — all 30 clip/page UUIDs
re-verified verbatim by script afterwards).

## What landed

- **Phase 0** (`src/federation/fetch_gate.rs`): `authenticate_fetch_viewer`
  (GET signature verify + key lookup; every failure → anonymous viewer) +
  `decide_fetch` (`Serve`→200, `Forbidden`+identified→403,
  `Forbidden`+anonymous→404, `Hidden`→404). Matrix unit test green.
  Contract: `docs/federated-collections.md` §1.
- **Phase 1** (local clips): `domain::{collection,clip}`, `ClipStore` +
  redb tables, `commands/clips.rs` ops, 11 `clips/*` endpoints (verbatim
  params/errors incl. per-endpoint `NO_SUCH_CLIP` UUIDs), `pack_clip`,
  `add-note` 20/h bucket, real `export-clips` + pinetree-only
  `i/import-clips`, conformance Group X, endpoint + round-trip tests.
- **Phase 3 local** (pages): `domain/page.rs`, `PageStore` + redb tables
  (+ `(owner,name)` index), `commands/pages.rs` ops (owner-or-moderator
  delete with `deletePage` log), 7 `pages/*` endpoints (verbatim UUIDs),
  `pack_page`, `create` 10/h + `update` 300/h buckets, conformance Group Y,
  endpoint tests. `admin/update-meta` gains `federateClips`/
  `federatePages` (default OFF); `admin/meta` reports them.
- **Wire + delivery** (Phases 2/3): `OrderedCollection` clips
  (`/clips/{id}`, `/users/{id}/clips`, actor `clips` ref) with per-item
  visibility gating; `Article` pages (`/pages/{id}`, script structurally
  excluded, sanitized render); `ClipMembershipChanged`/`PageCreated`/
  `PageUpdated` delivery payloads + execute arms; inbound `Add`/`Remove`
  (owner-verified cache eviction, never local mutation) + inbound
  `Like(Page)` (remote-attributed count); TTL resolver
  (`resolve_remote_clip` + `_with_ttl` seam, bounded process cache);
  inbound `Article` stays on the note pipeline.

## Executed evidence (the gate)

- `cargo test --offline`: 489 lib (incl. 6 new config-schema tests) +
  115 conformance (Groups X+Y, incl. Y3 URI index) + 34 integration
  (1+1+2+5+2+1+1+5+16) = **638/638 green**.
- `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK` (phase2 SKIP — no Node 22
  in this environment, same environmental note as Batches O/P).
- `bash scripts/keystone-sweep.sh` → `KEYSWEEP_OK`, **48 passed / 0
  failed** (7 new clip/page calls; the sweep's stale "honest 404s for
  antennas" header comment predates WS4 — untouched).
- Gate 3 (fetch-gate matrix serve/403/404): green.
- Gate 4 (clip CRUD + caps + favorites + real export/import round-trip):
  green.
- Gate 5 (two-local-pinetree clips): green —
  `federation_clips_listed_unlisted_approved_and_sync` (listed per-item
  gating incl. a private note correctly absent, unlisted by capability +
  unadvertised, remote-approved 403/404 matrix, TTL revalidation + Add/
  Remove converge, switches/flags off ⇒ 404 + no advertisement + no
  resolution).
- Gate 6 (two-local-pinetree pages): green —
  `federation_pages_article_round_trip_update_and_likes` (Article
  round-trip title/summary/content/eye-catch, `Update(Article)`
  converges, script-assertion over every federated byte, page-like
  round-trip + replay convergence, hostile-`Article` fixture ingests
  sanitized as a page with no shadow note).
- Keystone follow-ups: none (no Keystone edits).

## Open / deferred

- Remote-page materialization (inbound Article → note, Misskey parity).
- Page-like outbound delivery (co-located likes need none; documented).
- Follower-server promptness for clips (TTL convergence only; documented).
- Featured stays a pure query (upstream parity).
- New `ModerationAction::DeletePage` variant (`deletePage` log parity).
