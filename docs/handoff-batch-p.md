# Handoff: Batch P — email identity, account move, registry, small reads, timelines, notifications, discovery, versions, translate

> **Status (2026-10-20 freshness pass)** — spot-checked (endpoint strings + dispatch arms present); behaviour not re-tested.
> - **LANDED:** `i/update-email` (`endpoints/mod.rs:~7193`); `i/move` (`endpoints/mod.rs:~7385`, inbound Move in `src/federation/inbox.rs`); `i/registry/*` (`endpoints/mod.rs:~7521-7662`); `notifications/test-notification` (`endpoints/mod.rs:~7831`); `users/recommendation` + `notes/recommendations` (`endpoints/mod.rs:~7877-7897`); `notes/versions` (`endpoints/mod.rs:~7920`); `notes/translate` (`endpoints/mod.rs:~7974`); `notifications/mark-all-as-read` present.
> - **OPEN:** `i/move` root = oldest admin is documented divergence (inventory §2), not a gap; account migration preserving keys/content is a Phase 4 item (TODOS P0).

## Why

Close the remaining small-but-real Misskey gaps in dependency order: the email identity loop, account move/migration (the only item here with federation consequences), `i/registry`, small read endpoints, mentions timeline, notification utilities, discovery, edit-history viewing, and translation. Upstream pins: Misskey `2026.10.1-alpha.0`, Sharkey `2025.4.7` (read-only).

## Delegation rule (AGENTS.md hard rule — follow it exactly)

- Check `git status --short` before starting. Batch O (§34) may still be uncommitted in the working tree — respect it: do not touch, restage, or rewrite others' files. Stage/commit only your own files, only when told to.
- Partition subagent work into **disjoint file sets**. Shared files (dispatcher `src/api/misskey/endpoints/mod.rs`, `ports.rs`, `errors.rs`/`packers.rs`, `docs/*` contracts, conformance + existing test files) are owned by the parent for merging — **subagents propose patch snippets, they do not edit shared files directly**.
- Subagents use **uniquely-named test files**, leave changes uncommitted, and never `git commit`/`stash`/`reset`/`checkout --` anything.

Suggested split (parallel investigation, disjoint code):

- Subagent A (email + translate transports): upstream `i/update-email.ts` (`password!`, `email?`, `token?`), `request-reset-password.ts` (`username!`, `email!`), `reset-password.ts` (`token!`, `password!`), `verify-email.ts` (`code!`), password-reset/email-verification token lifecycle + TTLs in the handlers; `notes/translate.ts` (DeepL call: `target_lang` truncated at `-`, auth header, `CANNOT_TRANSLATE_INVISIBLE_NOTE`). Pinetree: Config shape, no SMTP anywhere, no translation backend. Output: token/code lifecycle table + `EmailService`/`TranslateService` port proposals + config keys.
- Subagent B (move + registry): upstream `i/move.ts` (param `moveToAccount!`; root/already-moved guards; `alsoKnownAs` alias check; `AccountMoveService.moveFromLocal`: `renderMove` delivery, followers unfollow-then-refollow, `postMoveProcess`), `Move` rendering + inbound handling (`ApInboxService` Move arm — verify), `movedToUri`/`alsoKnownAs` fields (note: `alsoKnownAs` storage already exists from §0B — verify); registry family (`i/registry/get|set|get-all|get-detail|keys|keys-with-type|remove|scopes-with-domain`) incl. the scope regex `^[a-zA-Z0-9_]+$`. Output: move state machine + inbound/outbound plan + registry store shape.
- Subagent C (reads + timelines + notifications): upstream `get-online-users-count.ts` (how is "online" counted — verify: Redis heartbeat?), `pinned-users.ts`, `fetch-external-resources.ts` (`url!`+`hash!` — compare with our url-preview fetcher for dedup), `server-info.ts`, `stats.ts`, `meta.ts` extras worth mirroring; `notes/mentions.ts` (`following` default false, visibility filter — read the handler, not just params), hashtag endpoints dir (list/search/show/trend/users — confirm there is NO tag timeline upstream; `notes/search-by-tag` already covers it); `notifications/flush.ts` (`flushAllNotifications` — read what it actually does) + `test-notification.ts` (type `test` + `{}` body). Output: per-endpoint mirror/skip table with handler-level notes.
- Subagent D (discovery + versions): upstream `users/recommendation.ts` (limit/offset — read the scoring, likely mutuals/friends-of-friends); **no `notes/versions` in this Misskey pin** (verified absent) — check Sharkey's `notes/versions.ts` and mirror THAT shape if present, else define from our `note_edits` rows and mark first-party. Notes/recommendations: no upstream endpoint in either pin (verify) — define as thin alias over the users/recommendation scoring, documented first-party. Output: scoring plan (documented-simplified, featured-ranking precedent) + versions shape proposal.

## Workstreams (build in this order)

### 1. Email identity loop (needs A's transport plan; owner decision required)

Endpoints: `i/update-email {password!, email?, token?}`, `request-reset-password {username!, email!}`, `reset-password {token!, password!}`, `verify-email {code!}` — params, flows, and error ids mirrored from the handlers subagent A reads (token/code TTLs, what "wrong/expired" answers, whether reset emails enumerate accounts — mirror the information-leak posture exactly).
- Add an `EmailService` port. Default transport: **no-op with a documented `EMAIL_NOT_CONFIGURED` error** on the send paths (config keys reserved: host/port/auth/from). Rationale: a single-node server without SMTP must fail loudly, never silently drop a password reset. If the owner has SMTP details mid-batch, wire them; otherwise the loop is code-complete and dormant. Record the choice.
- 2FA `token` on update-email: accepted-and-ignored (no 2FA exists — same treatment as change-password).

### 2. Account move (needs B's state machine; the federation-sensitive one)

- `i/move {moveToAccount!}`: root-user refusal, already-moved refusal, `acct` parse, remote resolution, `alsoKnownAs` alias check (destination must list the old account URI), destination-moved check — then `moveFromLocal`: persist `movedToUri`, deliver `Move` to old followers, followers unfollow-old + follow-new.
- Inbound `Move`: verify actor authority (the mover's old account), update `movedToUri`, migrate follow edges per `postMoveProcess`. Same drop-before-dedup discipline as blocked hosts. Tests must cover replay (re-delivered Move converges) and forged Move (third-party actor rejected).
- `alsoKnownAs` editing already exists (§0B) — the alias handshake needs no new params; confirm and cite.

### 3. `i/registry` (needs B's store shape)

Full family: `get|set|get-all|get-detail|keys|keys-with-type|remove|scopes-with-domain`. New `RegistryStore` (scope-validated `^[a-zA-Z0-9_]+$`, per-user namespacing, domain scoping as upstream). Straight key-value — no federation, no jobs.

### 4. Small reads + meta extras (needs C's table)

- `get-online-users-count`: define "online" from OUR streaming hub presence (not Redis — we have none); document the definition.
- `pinned-users`, `fetch-external-resources` (dedupe against the url-preview fetcher — one code path), `fetch-rss` (only if a client needs it; otherwise 404 + record), `server-info`/`stats` (counts from our stores), cheap `meta` extras. Skip-and-record anything needing infrastructure we don't have.
- Each: credential posture mirrored from upstream `meta`, rate bucket per existing conventions.

### 5. Timelines: mentions (+ tag confirmation)

- `notes/mentions {following=false, limit, sinceId/untilId/sinceDate/untilDate, visibility?}`: mirror the handler's following/visibility filtering (read it — don't guess which direction `following` filters).
- Hashtag timeline: confirm C's finding that upstream has none; if so, NO new endpoint — `notes/search-by-tag` is the answer, documented in one manifest line.

### 6. Notification utilities (needs C's finding)

- `notifications/test-notification`: emit type `test` through the normal `Notify` path (must survive type filters + never break grouped rendering — test it).
- `notifications/flush`: mirror `flushAllNotifications` semantics exactly (read it; if it's a BullMQ-drain concept with no pinetree equivalent, implement the closest honest equivalent — likely a no-op acknowledged 204 — and document why).

### 7. Discovery (needs D's scoring)

- `users/recommendation {limit, offset}`: upstream shape, documented-simplified scoring (mutuals/friends-of-friends over local graph — featured-ranking precedent, mark DIVERGENCE).
- `notes/recommendations`: first-party thin alias over the same scoring (no upstream endpoint in either pin — verified absent from Misskey; confirm Sharkey too). If no client consumes it, ship it anyway (one arm, same scorer) and note it.

### 8. Versions + translate (needs D + A)

- `notes/versions`: shape mirrored from Sharkey's `versions.ts` if present there, else defined from `note_edits` rows (oldText/newText/cw/dates) as a first-party endpoint. Read-only; author-or-public visibility.
- `notes/translate {noteId!, targetLang!}`: `TranslateService` port; invisible-note → `CANNOT_TRANSLATE_INVISIBLE_NOTE`; `targetLang` truncated at `-` (upstream parity). No backend configured → same `TRANSLATE_NOT_CONFIGURED` loud-failure posture as email, config keys reserved (DeepL-compatible). Never proxy arbitrary fetch — translate path only.

## Cross-cutting rules

- `domain` ← `application` ← adapters; one txn commits state + outbox/job; idempotent/retryable/crash-safe jobs; FDB 5s/10MB discipline; upstream ids verbatim; DIVERGENCEs with reasons (recommender scoring, email/translate dormant transports, online-definition, flush equivalent, versions shape if first-party).
- New stores → `ports.rs` trait → redb → conformance group (`RegistryStore`, email/translate token rows if stored, move state on accounts). Counts in `docs/persistence-conformance.md`.
- Contract updates: manifest superseding note per workstream, profile §5, parity-plan §14 line, wiki + README capability lists where user-visible.
- Federation input untrusted (Move sender validation, SSRF-guarded fetches). Password/email endpoints get auth-failure throttling like signin.

## Gates

1. `cargo test --offline` green (counts recorded). 2. `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK`. 3. `bash scripts/keystone-sweep.sh` → 41/0. 4. Live-ish verification where it matters: `i/move` end-to-end between two local pinetrees (followers refollow, Move delivered, replay converges, forgery rejected). 5. State logs per `AGENTS.md` with real output.

## Deliverables

Code + tests + contract updates for all eight items (or feasibility refusal with code evidence). README/wiki touch-ups where behavior changed.

## Non-goals

- No SMTP credentials committed anywhere; no real email sent from dev/test (dormant transport + error posture instead). No translation backend keys in repo. No Keystone edits. No `create-from-public`, no folders, no thread-muting, no Mastodon API/OAuth, no custom roles, no relays management. No algorithmic-feed infrastructure beyond the documented-simplified recommender.
