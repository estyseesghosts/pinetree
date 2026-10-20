> **Status (2026-10-20 freshness pass):** Phase A1 (Batch O remainder) landed except `create-from-public`. Everything from A2 onward is OPEN, and no code exists for it. Baseline note: 'done through Batch N (`2b38ed7`)' is historical; HEAD is `7fa40e0`.
> Tags: `[LANDED — evidence]` verified in code at HEAD; `[OPEN]` not in code; `[SUPERSEDED by …]`; `[DECISION — still binding]`; `[PLAN HISTORY]`. Original text is unchanged; tag blocks are additions. Short paths are under `src/`; `mod.rs` = `src/api/misskey/endpoints/mod.rs`.

# Pinetree extension plan — beyond a basic Misskey clone (2026-10-20)

Sources of truth:
- `docs/pinetree-decisions.md` (fork-diff synthesis + second-pass calls)
- `docs/community-planning.md` (native communities: optional extension + minimal core seal)
- Baseline: single-node done through Batch N (`2b38ed7`); Batch O (WP9/10/12/13/16) is the only parity tail left.

Cross-cutting rules (every slice):

> **[DECISION — still binding]** Rules for every slice (domain/application/adapters, FDB 5s/10MB, state log, gates). `scripts/smoke-all.sh:53` still pipes cargo test to `tail -30`, so the gate-masking issue in `TODOS.md` is open.

- `domain <- application <- adapters`. No business logic duplicated per adapter.
- One storage txn commits state + outbox/job atomically; workers do fan-out/delivery; jobs idempotent, retryable, crash-safe. FDB 5s/10MB discipline (no blobs or unbounded work in txns).
- Upstream error codes + ids verbatim; read the upstream file before inventing anything. Mark intentional deviations `DIVERGENCE` with reason in `docs/endpoint-manifest.md`.
- New stores: `ports.rs` trait -> redb impl (FDB inherits) -> conformance group in `tests/conformance.rs` (+ `docs/persistence-conformance.md` count).
- Contract updates per slice: manifest superseding note, `phase-3-profile.md` §5 if out-of-scope changes, `misskey-parity-plan.md` §14 status, `wiki/*` if user-visible.
- Federation input untrusted: SSRF/size/timeout/redirect/signature/authority discipline on every new inbound path.
- Gates per slice: `cargo test --offline` green (record lib + conformance + integration counts), `bash scripts/smoke-all.sh` -> `SMOKE_ALL_OK`, `bash scripts/keystone-sweep.sh` -> 41/0, state log per `AGENTS.md` (append dated log, never rewrite past logs).

---

## Phase A — Parity tail + core seal (prereq)

> **[OPEN]** A1 landed; A2 (core seal) is not started.


### A1. Batch O remainder

> **[LANDED — mod.rs:6036 (following/invalidate), :6527 (i/change-password), :6580 (i/regenerate-token), :6622 (i/apps), :8017 (drive/files/upload-from-url), :8064 (find-by-hash), :8371 (url-preview), :3152 (users/lists/create)]** WP9/10/12/13/16 surfaces are in the dispatcher. `users/lists/create-from-public` is still deferred (`[OPEN]`, `TODOS.md`).

Why: don't build extensions on a moving baseline.
Instructions: implement per `docs/handoff-batch-o.md` in order WP10 (follower removal + lookup) -> WP9 (self-service: change-password, regenerate-token, apps/authorized-apps, revoke-token real, signin-history store) -> WP13+WP12 (upload-from-url, find-by-hash with md5 DIVERGENCE decision, url-preview with credential-required + in-memory cache) -> WP16 (user lists, unblocks antennas `src=list`) last. Serial build: domain -> ports -> redb -> op -> route + packer -> federation -> conformance -> manifest -> smoke.
Deliverables: WP9/WP10/WP13/WP12/WP16 -> LANDED, manifest notes, README capability lists accurate.

### A2. Minimal core seal (`restrictedTo`, ~50 lines)

> **[OPEN]** No `restrictedTo`, no `note.community_id` (grep of `src/` empty).

Why: `community-planning.md` Shape — core owns exactly one thing.
Instructions: add nullable `note.community_id FK SET NULL`; add `restrictedTo` seal-check in `FeedFilter` + all read paths (timelines, threads, search, users/notes, preview, relay, direct fetch, streaming capture gates). Rule: tagged post invisible by default unless extension verifies membership proof. No extension or error = hidden (safe direction). No groups/mods/UI in core. Community-tagged posts degrade to plain addressed notes on core-only servers. Generalizes later to followers-only, invites, DMs.
Deliverables: conformance cases (sealed-by-default, owner/self paths, no-leak on error), `visibility-policy.md` + community-planning status update.

---

## Phase B — Moderation, join queue, instance plumbing

> **[OPEN]** B2 is partial: `admin/reject-quotes` now requires moderator (`mod.rs:4535-4539`). The rest is not started.

Source: TransFem yes / extras no + second-pass calls. Skip Mastodon API server, ListenBrainz.

### B1. Join queue

> **[OPEN]** No `approvalRequiredForSignup` or `approvalRequiredEmailDomains` in `src/`.

Instructions: `approvalRequiredForSignup` + `approvalRequiredEmailDomains` in meta, approve/decline endpoints, approvals page data path, join reasons stored + packed. Touch `application/commands/accounts.rs`, `invites.rs`, `ports.rs` + redb + conformance.
Deliverable: closed registrations operable without CLI; negative tests (duplicate approval, bad domain).

### B2. Day-to-day user ops

> **[OPEN]** Silence, one-click set, spam-log, CSAM quarantine, and purge-blocked are not found by grep. `reject-quotes` only.

Instructions: copy TransFem one-click set (cw-note/user/instance, nsfw on/off, silence on/off, rejectQuotes, suspend, delete) in `moderation.rs` + DVD extensions (spam-log search/restore, CSAM quarantine/resolve, purge-blocked).
Deliverable: admin one-click mod actions with modlog rows + txn atomicity tests.

### B3. Instance plumbing

> **[OPEN]** `set-root`, `restart-migration`, `gen-vapid-keys`, shared-access grants, and purge-user-content: none found in `src/`.

Instructions: copy set-root, restart-migration, roles clone + annotate-condition, gen-vapid-keys, shared-access grants + dialog, purge-user-content script semantics.
Deliverable: each op logged, manifest-noted, smoke-covered.

---

## Phase C — Protocol corrections (small, federate-safe)

> **[OPEN]** C1–C4 not started (see per-item).


### C1. Like vs favorite split

> **[OPEN]** Favorites are local-only and already work (`notes/favorites/create` `mod.rs:6350`; `i/export-favorites` now enqueues a real export `mod.rs:5813-5822`). The federated lightweight `Like` that is separate from reactions is not found.

Instructions: `like` = Sharkey `like` = Mastodon `favorite` — federated lightweight endorsement via `Like` activity, SEPARATE from emoji reactions. Misskey `favorites` = local-only bookmarks (already implemented) — never leaves instance. Do NOT conflate. Touch `domain/favorite.rs`, `activitypub/objects.rs`, `host_caps.rs` (bare Like to Mastodon, Like+content+`_misskey_reaction` to emoji-aware hosts).
Deliverable: federated like round-trip (pinetree<->Sharkey/Mastodon) + favorites stay local (no outbound activity).

### C2. Messages hidden from timelines (Catodon #647)

> **[OPEN]** No specified-hiding exemption found in feed filters (grep for `exempt` returns only word-mute and self-note matchers). Not exhaustively audited.

Instructions: specified-visibility notes that aren't events stay out of home/local/global/custom feeds, profile notes, live streams. Owner sees own on profile via exempt. Invite-only event posts are NOT messages and still list.
Deliverable: timeline/profile/stream conformance cases + exempt test.

### C3. Cat mode split

> **[OPEN]** Only `isCat` exists (`api/misskey/packers.rs:76`, `activitypub/actor.rs:306,540`). No `speakAsCat`.

Instructions: `be a cat` (`isCat`, cosmetic/nyaize baseline) independent from `speak as cat` (`speakAsCat`, Sharkey-only speech transform). Two toggles, independent. Touch profile store + packer + transform gate.
Deliverable: toggle matrix test (4 combos).

### C4. RFC 9421

> **[OPEN]** No `Signature-Input` or `Content-Digest` in `src/`. Legacy signatures only (`activitypub/signatures.rs`).

Instructions: verify-if-present, permissive-first with logging, then tighten. Covers `Signature-Input` + `Content-Digest` alongside legacy in `activitypub/signatures.rs`.
Deliverable: accept legacy-only, RFC9421-valid, and dual-signed; log-then-reject on bad digest; bench peers unaffected.

Note-edit `Update(Note)` federation is DECIDED (Sharkey/Catodon send + accept with `NoteEdit` rows + `notes/edit` + `notes/versions`; upstream-Misskey revert rejected) — verify existing path, don't rebuild.

> **[DECISION — still binding]** Implementation is present: `notes/edit` (`api/misskey/endpoints/mod.rs:4302`) and `notes/versions` (`:7920`). Inbound `Update(Note)` is embedded-only per the inventory (§2.3).


---

## Phase D — Events (steal Catodon FEP-8a8e outright)

> **[OPEN]** No `NoteEvent`, `EventRsvp`, or `EventInvitation` anywhere in `src/` (grep for `NoteEvent|EventRsvp|rsvp` is empty).


### D1. Model + RSVP

> **[OPEN]**

Instructions: `NoteEvent` sidecar (1:1 CASCADE), `EventRsvp` unique-pair, `EventInvitation` PK-pair, dual-dialect RSVP (Join/Leave + Accept-family), `catodon:inviteTo` DM fallback. Ports -> redb -> conformance.
Deliverable: RSVP idempotency + unique-pair enforcement + dual-dialect accept tests.

### D2. API + UX + invites

> **[OPEN]**

Instructions: `events/{upcoming,rsvp,unrsvp,attendees,invitees,invite}` shape, `MkEventEditor`/`SkNoteEvent` UX contract, public-invite semantics (≤20/call, once-per-person, local-only never federates).
Deliverable: endpoint errors verbatim, invite caps enforced, no federation from public-invite path.

### D3. ICS + visibility

> **[OPEN]**

Instructions: visitor-gated `.ics` (account feed + single note); invite-only event posts exempt from C2 hiding (still list).
Deliverable: ICS format check + gate tests (visitor vs non-visitor).

---

## Phase E — Imports + language (modified flow, heuristic-first)

> **[OPEN]** `i/import-*` exists for pinetree/Misskey exports. Pixelfed, Catodon, and Mastodon parsers are not found.


### E1. Imports

> **[OPEN]** Per-source parsers for Pixelfed, Catodon, Mastodon, and Sharkey are not found (grep for `Pixelfed|Catodon` in `src/` is empty).

Instructions: keep Sharkey job-family + policy-gate shape, write/adjust per-source parsers. Must support Misskey, Pixelfed, Mastodon, Sharkey, Catodon now; Lemmy later. Pixelfed + Catodon + Sharkey + Lemmy are new vs Sharkey's six. Akkoma/Firefish explicitly out unless demanded.
Deliverable: round-trip fixtures per source + policy-gate tests + job idempotency.

### E2. Langdetect + translate composition (open question -> decide here)

> **[OPEN]** A `translate` endpoint exists per the inventory, but the provider and LLM-gate composition is undecided and undocumented.

Instructions: single langdetect (lingua); provider picker + LLM quality gate composed, not stacked. Heuristic path works with zero external deps; LLM quality gate behind flag only. Never 500 on provider failure — degrade with log.
Deliverable: composition doc (which provider when, when LLM gate runs), fallback tests (provider down -> heuristic -> 200).

---

## Phase F — Intelligence: heuristics first, LLM optional

> **[OPEN]** Not started.

Source: DVD steal features, not dependencies.

### F1. Rec MVP + spam/CSAM heuristics (no new infra)

> **[OPEN]**

Instructions: quality scoring, interest vectors, pgvector + heuristics first. No Milvus, no vLLM/Qwen, no Python sidecar. Bounded work per read/write (FDB-safe).
Deliverable: rec MVP scope doc + heuristic spam/CSAM actions (search/restore, quarantine/resolve already in B2 — wire scoring here) working on default single-binary install.

### F2. LLM upgrade layer (flagged, degradable)

> **[OPEN]**

Instructions: DVD recommendations, `LlmQuality` translation gate, LLM spam/CSAM as upgrades over F1 baselines. External deps (Milvus, vLLM/Qwen, sidecar) behind flags, degradable, never 500s. Flag-off = F1 behavior exactly.
Deliverable: flag matrix test (on/off/unreachable sidecar -> 200 + logged degradation), no default-path dependency.

---

## Phase G — Native communities extension (planning done, DDL + policy TBD)

> **[OPEN]** See `community-planning.md` (all sections OPEN).

Catodon forum support is reference only (markers, unwraps, `/api/v3` shim, gates). Build natively per `community-planning.md`, on top of A2 seal.

### G1. DDL + CommunityPolicyService

> **[OPEN]**

Instructions: `community(id, actor_uri UNIQUE, webfinger_lower UNIQUE, host, software, visibility, posting_policy, listed, nsfw, languages[], counts JSONB, also_known_as[])` — stable id across rename/move. `community_member(community_id FK CASCADE, user_id FK CASCADE, role member|mod|admin, state active|pending|banned, banned_by, reason, expires_at, PK(community_id,user_id), IDX(user_id))` — one lookup for all checks; explicit owner (not row-order seniority). Single `CommunityPolicyService.canPost/canView/canModerate`; no scattered isGroup checks. Fanout `communityTimeline:{id}` + home fill on write; adoption = same path. Counts/catalog materialized + invalidated on write (not 5-min cache). Modlog per community. Notification prefs first-class.
Deliverable: exact DDL + policy code + conformance (post/view/moderate matrix) + FDB-safety argument (bounded fanout).

### G2. Federation wire (Lemmy-compatible)

> **[OPEN]**

Instructions: Group actor (`!name@host`, inbox/followers/outbox/moderators/featured, sharedInbox, keys, languages, `manuallyApprovesFollowers`=private, `discoverable`=!unlisted). `audience`=community on every community activity. Public must contain Public in to/cc; private must not. Post Create(Page) + Announce(Create) + compat Announce(Page); comments Create(Note) w/ inReplyTo chain; votes bare Like/Dislike + audience; Delete (self, no summary) vs Remove (summary + withReplies); Block(+removeData), Add(target moderators|featured), Lock, Update(Group/Page), Flag/Resolve/Warn. Inbox accepts wrapped + raw; re-announces local mail. Outbox newest ~20 Announce(Create(Page)). Backfill: paged resumable outbox walk (not 10-cap). Lenient parsing (one-or-many, skip bad tags). Deterministic Create ids, UUID announces. RFC 9421 + legacy verify-if-present permissive-first.
Deliverable: Lemmy interop bench (post/comment/vote/mod actions both directions) + backfill resume test.

### G3. Visibility / roles / moderation

> **[OPEN]**

Instructions: visibility public | unlisted (hidden unless subscribed) | local_only_public (unfederated, anon OK) | local_only_private (unfederated + login) | private (follow -> ApprovalRequired -> Accepted, enforced reads+writes). Private full enforcement only extension-to-extension (reader checks + signed instance `is_follower` probe); vs core-only degrades to unlisted-ish (addressable, not advertised, not gate-kept); true secrecy = local-only. Flip to non-federating: tombstone then update. LocalOnly HTTP 404; private fetch requires follower-instance signature. Roles: create any non-banned (or admins-only if flag); edit mod/admin; delete top-mod/owner; remove/purge site admin. Add-mod needs seniority; transfer = atomic owner update (federates). Ban/unban + optional wipe; approvals queue (approve/deny); posting_restricted_to_mods announcements mode; pins/locks/distinguish/warn; reports; shared modlog. Day-to-day + plumbing per Phase B.
Deliverable: visibility matrix tests + role/seniority tests + private-federation probe tests.

### G4. Migration + shim (deferred)

> **[OPEN]**

Instructions: (1) community row per isGroup user (uri->actor_uri, username/host->webfinger, forum*->new cols); (2) `note.community_id` dual-write with forumId then batch copy; (3) members from follows of groups, bans from forum_ban; (4) cut reads to community_id, drop forumId/forumScoped + user.forum* after verify. Lemmy `/api/v3` shim = extension endpoint, v1 or later (TBD, not in G1-G3).
Deliverable: migration dry-run log + cutover checklist + shim decision recorded.

---

## Build order (recommended)

> **[OPEN]** Only A1 is landed. Everything from A2 onward is open.

A1 -> A2 -> B1 -> B2 -> B3 -> C1 -> C2 -> C3 -> C4 -> D1 -> D2 -> D3 -> E1 -> E2 -> F1 -> F2 -> G1 -> G2 -> G3 -> G4.

Start extensions (B-D) only after A1/A2 seal the baseline. F2 only with infra. G needs its DDL + policy implementation pass (G1) before any wire work.
