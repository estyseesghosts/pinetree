# Handoff: Batch N — emoji, favorites, notifications, antennas, abuse reports, admin remainder, MiAuth, server-side word mutes

> **Status (2026-10-20 freshness pass)** — spot-checked (endpoint strings + dispatch arms present); behaviour not re-tested.
> - **LANDED:** `notes/favorites/create` + `notes/state` (`src/api/misskey/endpoints/mod.rs`, `src/application/commands/favorites.rs`); `antennas/create` (`endpoints/mod.rs:~5293`); `admin/emoji/add` (`endpoints/mod.rs:~5109`); `followRequestAccepted` notification (`src/application/commands/follows.rs:117`); server-side word mute domain (`src/domain/word_mute.rs`); abuse/Flag (`src/application/commands/abuse_reports.rs`); MiAuth hardening (manifest §23).
> - **OPEN:** emoji bulk/remote/copy/import-zip admin families (TODOS "Admin families"); `notes/thread-muting/*` is declared-out (manifest §11).

## Context (read first)

- Product/architecture source of truth: `sns-server-proposal.md` (§§1, 6–7, 9).
- Agent rules + state maintenance: `AGENTS.md` (read the whole file; dated log required when finishing).
- Contract index: `docs/README.md`. Compatibility profile (frozen): `docs/phase-3-profile.md`.
- API contract (frozen + dated superseding notes §§10–26): `docs/endpoint-manifest.md`.
- Parity plan (rev.3, §§0–13 landed; §14 WP1–WP6 landed, WP7–WP16 pending): `docs/misskey-parity-plan.md`.
- Batch M guide (the spec for WP7–WP16, out-of-batch list §8, owner decisions §9, upstream file refs throughout): `docs/implementer-guide-batch-m.md` — **§§5 (WP7–WP16), 8, 9 are required reading.**
- Current state (what/why): `docs/wiki/Current-State.md`, `docs/wiki/Feature-Matrix.md`, `state/logs/2026-10-20-readme-refresh.md` (how the README claims were verified).
- Upstream pins (read-only): Misskey `2026.10.1-alpha.0` (`./misskey`), Sharkey `2025.4.7` (`./sharkey`).
- Keystone (external repo, do NOT edit): `/root/keystone` — `frontend/src/api/*`, `docs/api-surface.md` (cites exact pinetree gaps: emoji picker §73, url-preview §99, antennas 404 §96/§129, streaming §134).

## Objective

Close the user-visible gaps a real client trips over, in Keystone's priority order:

1. Custom emoji (listing, admin management, packing, remote opt-in).
2. Favorites + note state.
3. Notifications (`followRequestAccepted` + audit of the unemitted list).
4. Antennas (full WP15 stack: CRUD, matching job, feed, stream, import/export).
5. Abuse reports (the WP4 remainder: wire the HTTP endpoints + inbound `Flag`).
6. Moderation/admin remainder (audit `admin/*` vs upstream, close what's cheap, log what stays out).
7. MiAuth audit + hardening.
8. Server-side word mutes (soft + hard) — a pinetree DIVERGENCE, owner-approved. Build it (no separate spec phase).

## Instructions (use subagents)

You MUST use subagents (`general` or `explore`) for parallel investigation. Suggested split — run in parallel, then synthesize before writing any code:

- Subagent A (emoji): upstream `endpoints/emojis.ts` (public `{emojis: [EmojiSimple]}`, `cacheSec: 3600`, `host: IsNull()` filter), `endpoints/emoji.ts`, `admin/emoji/{add,update,delete,list}.ts` (+ bulk/`copy`/`import-zip`/`list-remote` to rule out), `export-custom-emojis.ts`, `EmojiEntityService`; pinetree `src/api/misskey/packers.rs:75,965,1058` (the `{}` maps), blob store, `i/update` store. Output: exact endpoint/param/error table + store shape + packing key format (bare shortcode → URL) + WP8b opt-in plan.
- Subagent B (favorites + notifications): upstream `notes/favorites/create|delete.ts` (error ids incl. `ALREADY_FAVORITED`), `i/favorites.ts`, `notes/state.ts`; upstream notification types vs pinetree `mod.rs:952` unemitted list; pinetree `Notify` creation path + `includeTypes/excludeTypes` filters. Output: endpoint table + `notes/state` shape + which notification types can be emitted cheaply vs which stay out (with reasons).
- Subagent C (antennas): upstream `AntennaService.ts:97–209` (match order), antenna endpoints, `antennas/list|show|create|update|delete|notes|remove-note`, `antenna` stream channel, import/export processors; guide WP15 (both DIVERGENCEs, no-cache architecture, `antenna_match` job, `antenna_notes` capped at 200, `antennaLimit` 5). Output: implementation plan in guide order + conformance cases + FDB-safety argument (bounded job, range-scan read).
- Subagent D (abuse + admin): WHY the WP4 endpoints are missing — `src/application/commands/abuse_reports.rs` (ops exist, zero callers outside the file), `src/federation/outbox.rs:213-215,1187-1209` (`Flag` render, never enqueued), no `Flag` arm in `src/federation/inbox.rs`, manifest §24 (documents endpoints that don't route — fix the manifest if scope changes). Upstream: `admin/abuse-user-reports.ts`, `admin/resolve-abuse-user-report.ts`, `admin/update-abuse-user-report.ts`, `admin/forward-abuse-user-report.ts`, `admin/abuse-report/notification-recipient/`, user-facing report endpoint (verify exact path in tree). Then audit `admin/*`: diff `./misskey/.../endpoints/admin/` against pinetree's routed list and classify each as implement / out (with reason) / needs-owner-decision. Output: wire-up plan for reports + admin gap table.
- Subagent E (MiAuth + word mutes): MiAuth — upstream `miauth/gen-token.ts`, web session routes, permission scoping, session expiry/cleanup; pinetree `mod.rs:143-144` routes, `miauth/gen-token`, `auth/session/*` arms; find divergences and harden (expiry, permission enforcement, rate limits, tests). Word mutes — upstream `frontend/src/utility/check-word-mute.ts` (haystack `cw + \n + text`, string AND-groups, `/regex/flags` form, self-exempt, empty→false) and backend non-enforcement (`hardMutedWords` only packed, `UserEntityService.ts:616`); pinetree stores + packs, never filters. Output: MiAuth gap list + build plan for workstream 8 (Misskey-exact matcher over note+reply+renote; hard = `FeedFilter` drop; soft = packed viewer flag when matched and no own CW, mirroring Keystone's badge rule).

Do not skip the subagent step. Synthesize A–E into one build order, then implement serially per workstream (each: domain → ports → redb → application op → route + packer → federation if any → conformance → manifest update → smoke).

## Workstreams

### 1. Custom emoji (guide WP8 + WP8b)

Per guide: `CustomEmoji` store (shortcode unique case-insensitively, media in blob store), `emojis` (public) + `emoji {name}`, `admin/emoji/add|update|delete|list` (admin, logged), fill `reactionEmojis`/`emojis` maps keyed bare-shortcode → URL, fix `i/export-favorites`-style empty-file problem for `export-custom-emojis` if claimed. Remote emoji per WP8b: ingest metadata only (no fetch), per-account `showRemoteEmojis` default false via `i/update` (DIVERGENCE, upstream has no such field), lazy fetch job on opt-in, off = local-only packs + `:name@host:` text rendering. Out of scope: bulk ops, `import-zip`, `copy`, `list-remote` (guide §8). Acceptance: Keystone picker Custom section renders (Keystone re-verifies on its side).

### 2. Favorites + note state (guide WP7)

`notes/favorites/create|delete` (upstream error ids verbatim, incl. `ALREADY_FAVORITED` + `NO_SUCH_NOTE`), unique per (account, note), `i/favorites` cursor list, `notes/state` (`isFavorited` + whatever else is cheap), `isFavorited` on packed notes for authenticated callers, and fix `i/export-favorites` to write real data (Keystone import/export UI uses it).

### 3. Notifications (guide WP11 + audit)

Emit `followRequestAccepted` (packing + type-filter coverage). Audit every other unemitted type against upstream and either wire it or record why it stays out (guide §8 already outs `app`, `achievementEarned`, `login`, `roleAssigned` — confirm, don't re-litigate without new evidence).

### 4. Antennas (guide WP15)

Full stack per guide: CRUD + `notes` + `remove-note`, `antenna_match` job enqueued in the note-creation transaction, `antenna_notes` table capped at newest 200, read-time visibility/mute/block filtering, `antenna` stream event post-commit, real export/import, both DIVERGENCEs recorded (`home` = own + followed notes; no 7-day auto-deactivation, instance cap instead). `src=list` needs the WP16 membership index — either build WP16 first or ship `src=list` refused with a documented error until lists land (decide in synthesis, record it).

### 5. Abuse reports (WP4 remainder)

Wire `users/report-abuse` (credential, `write:report-abuse`) + `admin/abuse-user-reports` + resolve/update/forward admin endpoints with the upstream error ids in manifest §24 (correct the manifest where it over-claims). Route inbound `Flag` in the inbox dispatcher (validate, store/forward per the app op, no state write before checks — same drop-before-dedup discipline as blocked hosts). Enqueue `Flag` delivery from `forward_abuse_report` so the outbox render actually fires. Keep the documented DIVERGENCEs (no admin stream/mail/webhook notifications — admins poll; reporter host always null). Tests: endpoint errors, resolve/set-note/mark-forwarded txn atomicity, inbound `Flag` validation, delivery enqueue.

### 6. Moderation/admin remainder

From subagent D's audit: close what's cheap and in-policy (likely candidates: `admin/unset-user-avatar|banner`, `admin/delete-all-files-of-a-user` — verify against tree), and explicitly classify the rest (roles/policies, `send-email`, relays beyond `blockedHosts`, `server-info`/stats, `get-user-ips`, reset-password/unset-mfa → all need email or policy decisions first). Update manifest + profile §5 for anything newly out.

### 7. MiAuth audit + hardening

Compare pinetree's flow (`miauth/gen-token`, `GET|POST /miauth/{session}`, `/check`, legacy `auth/session/*`) against upstream end to end: permission arrays honored, session expiry + cleanup, one-time use, rate limits, suspended-user handling. Close gaps found; add negative tests (expired session, bad token, permission downgrade). `signin-flow` stays out (guide §8).

### 8. Server-side word mutes (BUILD — owner-approved DIVERGENCE from upstream)

Upstream never enforces word mutes server-side (`hardMutedWords` is only packed, `UserEntityService.ts:616`); pinetree will. Mirror the exact matching semantics both clients already implement — Misskey `frontend/src/utility/check-word-mute.ts` and Keystone `frontend/src/misskey/softMute.ts` (Misskey-exact, verified against each other):

- Haystack is `cw + '\n' + text`, trimmed (CW first — `^` anchors at the CW).
- Array filters are case-sensitive substring AND-groups (empties dropped; group left empty never matches); bare-string filters are `/pattern/flags` regexes (invalid/non-matching shape never matches; reset `lastIndex` per test).
- A user's own notes never match. Empty text never matches.
- A note matches if itself, its reply, or its renote matches (first match wins — `checkNoteWordMute`).

Enforcement (new behavior, record as DIVERGENCE in the manifest):

- **Hard mute** (`hardMutedWords`): drop the note in `FeedFilter` on every timeline/note read (timelines, threads, search, `users/notes`, streaming capture gates) — the server-side equivalent of Keystone's `isExcluded` hard-word rule. Self-notes exempt per the matcher.
- **Soft mute** (`mutedWords`): do NOT drop. Pack a viewer-specific flag on the note (propose the name in the manifest note; e.g. `isSoftMuted`) when the note matches AND has no CW of its own — the server-side equivalent of Keystone's `softIds` badge rule (a note with its own CW needs no badge). Any client can then badge/hide-with-reveal; Keystone migrates its badge source from local computation to this flag (cross-repo follow-up — report it, don't implement it).
- Notifications: do NOT gate creation on word mutes (neither client does; keep creation-time semantics unchanged). Record this.
- No migration needed: `mutedWords` (soft) and `hardMutedWords` (hard) are already separate stored + packed fields; they just gain enforcement.
- Perf: compile per-user filter sets once per read path (cache compiled regexes, bounded count), never per-note regex compile at write time; FDB-safe (no unbounded transaction work). New conformance cases: hard-drop on timelines/threads/search, soft-flag packing incl. CW-exemption and self-exemption, reply/renote-transitive match, regex/AND-group/empty edge cases.
- Contract: new `docs/word-mutes.md` (Draft ok) describing semantics + DIVERGENCE rationale, manifest superseding note, profile §5 tweak (word mutes move from "client-side" to server-enforced).

## Cross-cutting rules (non-negotiable)

- `domain` ← `application` ← adapters. No business logic duplicated per adapter.
- One storage transaction commits state + durable outbox/job atomically; workers do fan-out/delivery; jobs idempotent, retryable, crash-safe. No media blobs or unbounded work in transactions (5s / 10MB FDB limits always in mind).
- Upstream error codes + ids verbatim; read the upstream file before inventing anything. Mark intentional deviations `DIVERGENCE` with a reason in the manifest.
- New stores: `ports.rs` trait → redb impl (FDB inherits) → conformance group in `tests/conformance.rs` (+ `docs/persistence-conformance.md` count).
- Update `docs/endpoint-manifest.md` (new superseding note per workstream), `docs/phase-3-profile.md` §5 if out-scope changes, `docs/misskey-parity-plan.md` §14 status lines, `docs/wiki/*` if user-visible behavior changes.
- Federation input untrusted: existing SSRF/size/timeout/redirect/signature/authority discipline applies to every new inbound path (`Flag`, remote emoji tags, upload-from-URL is WP13 — NOT this batch, do not sneak it in).

## Gates (run before claiming done)

1. `cargo test --offline` green (record the three numbers: lib + conformance + integration).
2. `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK` (note phase2 SKIP if no Node 22 — environmental, record it).
3. `bash scripts/keystone-sweep.sh` → 41/0 (re-check Keystone's `api-surface.md` claims you invalidated: emoji §73, antennas §129, streaming §134).
4. State maintenance per `AGENTS.md`: read proposal §9 + `docs/README.md` + latest `state/logs/` first; append dated logs (`state/logs/YYYY-MM-DD-<topic>.md`), never rewrite past logs; paste real command output.

## Deliverables

- Code + tests + contract updates for workstreams 1–8 (or a feasibility refusal with code evidence for any item that turns out infeasible).
- README + wiki touch-ups only where user-visible behavior actually changed.

## Non-goals

- Do not touch Keystone (`/root/keystone`) — report cross-repo follow-ups, don't implement them.
- Do not build WP9–WP10, WP12–WP14, WP16 (separate batches), Babysit scope: if antennas need `src=list`, refuse it with a documented error (or pull WP16 forward only with owner approval).
- Do not "fix" upstream Misskey/Sharkey/Akkoma. Do not re-litigate guide §8/§9 decisions without new code evidence.
- No bulk emoji ops, no `import-zip`, no relays, no email/SMTP, no custom roles, no thread-muting, no Mastodon API, no OAuth.
