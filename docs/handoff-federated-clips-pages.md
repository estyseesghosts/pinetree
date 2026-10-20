# Implementation plan: federated clips + pages (pinetree↔pinetree, closed-by-default)

## Why

Clips and pages are server-local in Misskey (no AP representation), so federating them is a pinetree-first DIVERGENCE — pinetree↔pinetree only for now, gracefully ignored by everyone else. This plan ships local parity first (Keystone's clip UI exists and gets 404s today; `i/export-clips` currently writes empty files), then federation. Channels are OUT (Lemmy communities cover that need later).

## Delegation rule (AGENTS.md hard rule)

- Check `git status --short` first; respect uncommitted work (Batch O may still be dirty); stage/commit only your own files, only when told to.
- Partition subagent work into **disjoint file sets**. Shared files (dispatcher, `ports.rs`, `errors.rs`/`packers.rs`, `docs/*`, existing test files) are parent-owned — **subagents propose patch snippets, never edit shared files directly**.
- Subagents use uniquely-named test files, leave changes uncommitted, never `git commit`/`stash`/`reset`/`checkout --` anything.

Suggested split (parallel investigation, then serial build):

- Subagent A (clips upstream + fetch): `clips/create|update|delete|list|show|notes|add-note|remove-note|favorite|unfavorite|my-favorites.ts` (params, error ids incl. `alreadyClipped`/`tooManyClipNotes` caps, `ClipService` rules), `Clip` model (`name` ≤100, `isPublic`, `description` ≤2048). How Misskey packs clips + clip notes. Output: endpoint/error table + store shape + cap numbers.
- Subagent B (pages upstream + Article): `pages/create|update|delete|show|like|unlike|featured.ts`, `Page` model (`title`, `name` slug unique per user, `summary`, `content` block array, `variables`, `script`, `eyeCatchingImageId`, font/align flags). What `featured` requires (admin curation — expect skip). Mastodon/Misskey `Article` handling (Misskey `validPost` already includes `Article`; pinetree inbox already accepts `Article` in Create/Update dispatch). Output: field table (federate vs local-only per field) + like-shape + featured verdict.
- Subagent C (authenticated fetch): how Mastodon does authorized fetch (signed GET, which actors sign, what gets 401/403/404), what our `SafeHttpClient` + inbox signature verification already cover, and the minimal addition: verify HTTP Signatures on inbound GETs for non-public objects. Output: design for signed-GET verification + key lookup + response-code mapping (unknown signer → 404, known-but-unauthorized → 403 — never confirm existence to strangers).

## Phase 0 — authenticated fetch (prerequisite, no shortcuts)

- Verify HTTP Signatures on inbound GETs for designated non-public object routes (clip collections, unlisted/listed-but-gated pages, remote-approved items). Known signer + authorized → serve; known signer + not authorized → 403; unknown/unsigned → 404 (existence unconfirmed).
- This is shared infrastructure: build it once, use for both types. Tests: matrix of {public, unlisted-capability, approved-remote, stranger, unsigned} × {serve, 403, 404}.

## Phase 1 — local clips (Misskey parity)

- Store: `Clip {id, owner, name ≤100, description ≤2048, visibility-tier}` + `clip_notes(clip, note)` (unique, capped per upstream `tooManyClipNotes` number from subagent A). Map upstream `isPublic` onto the tier model (see privacy tiers below; `isPublic=true` ⇒ listed-global + federate flag still default off).
- Endpoints (verbatim params/errors): `create|update|delete|list|show|notes`, `add-note {clipId!, noteId!}` (`noSuchClip`/`noSuchNote`/`alreadyClipped`/`tooManyClipNotes`), `remove-note`, `favorite|unfavorite|my-favorites`.
- Fix `i/export-clips` to write real data (today: empty file) and `i/import-clips` to consume it (empty-file import path already exists — extend, don't fork).
- Conformance group for clip CRUD + caps + favorites.

## Phase 2 — federated clips

- Wire shape: `OrderedCollection` at `/clips/{id}` carrying `attributedTo`, `name`, `summary`, `totalItems`, paged **note URIs only** (never embedded notes; cap items at 200, antennas precedent). No new activity types required: collection state is the protocol.
- Membership sync: full-state paging with TTL revalidation + inbound `Add`/`Remove` for promptness (idempotent apply, authority = collection owner, dedup like everything else).
- Item privacy composes: each note resolves through the existing inbound note path (dedup, authority, Block/host-drop, visibility gate at read). A non-visible note in a visible collection resolves to nothing — by construction, never by special-casing.
- Advertisement: listed clips appear in the actor's `clips` collection reference; unlisted never do; neither appears in NodeInfo, search, or public timelines.
- Privacy gate matrix (enforced at fetch): self-only ⇒ no remote serve at all (no collection URI advertised); local-approved ⇒ remote fetch 403; unlisted ⇒ serve to bearer of capability URI; listed ⇒ serve collection, items still gated per-item; remote-approved ⇒ serve only to signed requests from listed approver actors (Phase 0).
- Admin: `federateClips` instance switch (default OFF) + per-clip federate flag (default OFF). Either off ⇒ local-only behavior, zero protocol footprint.

## Phase 3 — local + federated pages (Article, script rule)

- Store the full Misskey shape (`title`, per-user-unique `name` slug, `summary`, `content` blocks, `variables`, **`script`**, `eyeCatchImageId`, font/align flags): local parity for Keystone.
- Endpoints: `create|update|delete|show` (show by name+username), `like|unlike`. `featured`: SKIP (admin-curation surface; record 404 + reason).
- Federate as **`Article`** (existing type — Misskey `validPost` includes it, our inbox already dispatches it): `name`=title, `content`=server-rendered sanitized HTML, `summary`, `url`=page URL, `attributedTo`, `published`/`updated`, `attachment`=[eye-catch image]. `source` carries content blocks + `variables` for pinetree↔pinetree fidelity — **`script` is NEVER serialized into any federated field, structurally** (not a flag: the serializer has no code path that emits it). Updates ride the existing `Update(Article)` delivery path.
- Inbound `Article`: resolve through the note pipeline (type already accepted; work is storage + long-form render handoff to Keystone). Render ONLY from sanitized `content`, or re-sanitize `source` server-side on ingest — never execute, never trust remote `source`. Same four privacy tiers + same advertisement rules + `federatePages` switch (default OFF) + per-page flag (default OFF) as clips.
- Page likes: store + count; federate as `Like(Page-target)` only pinetree↔pinetree (degrades to nothing elsewhere — fine).

## Privacy tiers (both types, one model)

`self-only` (never leaves) → `local-approved` (local membership list, remote 403) → `unlisted` (capability URI, served to bearer, never advertised) → `listed` (advertised, collection servable, items individually gated) → `remote-approved` (Phase-0 signed fetch, listed approver actors only). Remote-approved carries the standing trust caveat (we control who can fetch, not what the remote server does next — same as federated DMs; the UI states this). High-stakes content stays local-only; that guidance goes in `docs/` and the UI copy (Keystone-side, reported not implemented).

## Cross-cutting rules

- `domain` ← `application` ← adapters; one txn commits state + outbox/job; idempotent/retryable/crash-safe; FDB 5s/10MB discipline; upstream ids verbatim where Misskey shapes are mirrored; every intentional deviation marked DIVERGENCE (federation itself is the big one — record it once, reference it).
- New stores → `ports.rs` → redb → conformance groups (clips, clip-notes, pages, page-likes). `docs/persistence-conformance.md` counts.
- Contract updates: manifest superseding note per phase, profile §5, parity-plan §14 line, `docs/wiki/*`, README can/can't lists. New doc: `docs/federated-collections.md` (wire shapes, privacy matrix, script rule, advertisement rules) — the spec remote implementers would need.
- Federation input untrusted throughout (collection fetch caps, item-count caps, HTML sanitization on ingest AND render, SSRF-guarded resolution).

## Gates (exit deliverables per phase — nothing merges without these)

1. `cargo test --offline` green (counts recorded: lib + conformance + integration).
2. `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK`; `bash scripts/keystone-sweep.sh` → 41/0 (update sweep where clip/page calls stop 404ing).
3. **Phase 0:** fetch-gate matrix test (serve/403/404 across signer × tier) green.
4. **Phase 1:** clip CRUD + caps + favorites + real export/import round-trip green.
5. **Phase 2:** two-local-pinetree clip federation: listed collection resolves with per-item gating (incl. a private note correctly absent), unlisted works by capability URI and stays unadvertised, remote-approved refuses strangers (403) and strangers get 404 on unknown signers, TTL revalidation + Add/Remove converge, either admin/object flag off ⇒ zero remote footprint (assert no outbound fetch attempts).
6. **Phase 3:** two-local-pinetree page federation: `Article` round-trip (title/summary/content/eye-catch), `Update(Article)` converges, scripted page federates as static-only (assert: no `script` bytes in ANY federated payload — test serializes every federated field and greps), page-like round-trip, inbound `Article` from a fixtures file renders sanitized.
7. State logs per `AGENTS.md` with real command output; Keystone follow-ups reported, not implemented.

## Non-goals

- Channels (Lemmy covers this later). Antennas `src` semantics untouched. No new activity types beyond `Add`/`Remove` handling. No executable content federation, ever. No public-directory/indexing of unlisted objects. No Keystone edits. No email/SMTP, no Mastodon API, no custom roles. `featured` pages stay 404 until an admin-curation design exists.
