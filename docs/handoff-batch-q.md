# Batch Q plan: fix the nine audit findings + implement the remainder

> **Status (2026-10-20 freshness pass)** — the nine audit findings were **not** re-audited in this pass; no finding is marked fixed.
> - **LANDED-unverified:** the audit's `following/requests/list` surface exists (`endpoints/mod.rs`, 3 hits for `following/requests/list`).
> - **OPEN (confirmed absent, 0 endpoint hits):** `i/webhooks/*`, `i/2fa/*`, `my/apps`, `app/*`, `drive/folders/*`, `announcements`, `i/read-announcement`, `following/update`, `following/requests/sent`, `hashtags/*`; `rateLimitFactor` is a fixed `1` in the packer (`src/api/misskey/packers.rs:1212`), not a limiter knob.
> - **OPEN (see `docs/TODOS.md` "Handoff batches N–Q"):** the nine findings (re-audit before closing each), webhooks in scope, media pipeline (thumbnails/blurhash/Range), analytics/retention/charts, rate-limit remainder.

## Why

The 2026-10-20 code audit (four read-only subagents + head-verification) left nine concrete broken/divergent items and a list of openly unbuilt features. Since then Batch P, federated clips/pages, and a JSON config file have committed — so some items may already be fixed. This batch: verify each finding at HEAD, fix what's still broken, then implement the remaining missing features except `notes/translate` (no backend), achievements (out), and passkeys (out).

## Delegation rule (AGENTS.md hard rule)

- `git status --short` first; respect uncommitted work; stage/commit only your own files when told.
- Disjoint file sets per subagent. Shared files (dispatcher, `ports.rs`, `errors.rs`/`packers.rs`, `docs/*`, existing test files) are parent-owned — subagents propose patch snippets, never edit shared files directly.
- Uniquely-named test files, no commits/stash/reset/checkout by subagents.

## Config rule (new AGENTS.md §Configuration — applies to everything below)

Every new setting gets a serde field on `Config` (JSON key = field name, optional with default, covered by `validate()`, round-trip test in `src/config.rs`, documented key list). CLI flags override file values. No env-only or CLI-only settings. This already covers e.g. `federateClips`/`federatePages`-style toggles, captcha keys, translation keys, retention windows.

## Phase 0 — verify at HEAD (read-only subagent first, no code)

For each of the nine, confirm broken-vs-fixed at HEAD with file:line, because the tree moved after the audit:

1. `following/requests/list` permission kind (`mod.rs:~5800` requires `read:account`; upstream `list.ts:18` kind is `read:following`). Audit-time status: STILL BROKEN.
2. `notes/delete` moderator delete: manifest §4 promises non-owner/non-moderator → ACCESS_DENIED; `delete_note` (`commands/notes.rs:~310`) rejects every non-author. Determine which side is wrong (read upstream `delete.ts:57` + decide; owner decision lives here, not in Phase 1).
3. `users/show` suspension 404 + `notes/show` `CONTENT_RESTRICTED_BY_USER` gate. Unverified at HEAD.
4. `i/notifications` missing rate bucket (upstream 30/30s) + mark-read-on-empty-filter vs upstream early return.
5. `following/create` `withReplies` default true vs upstream false.
6. `notes/create` fileIds/mediaIds merge — audit-time note says FIXED (`fileIds` wins, comment ~line 703). Confirm + test exists.
7. `notes/timeline` include-flags — audit-time note says FIXED (threaded at `queries.rs:~896`). Confirm + test exists.
8. Federation trio: featured Add/Remove ingest + `Undo(Accept)` + `Delete(Person)`. Check `fetch_gate`/clips work didn't already cover pieces.
9. `meta.mediaProxy` advertises `/proxy` with no route — confirm route still absent (or meta fixed).

Second deliverable of Phase 0: enumerate the remaining missing-vs-upstream list at HEAD (Batch P/O/N landed a lot). Skips: `notes/translate`, achievements, passkeys, and standing non-goals (Mastodon API, OAuth, clips/channels/pages-local, thread-muting, relays, LD-signatures, webhooks? — NO, webhooks are in scope below). Output: punch list with file:line + upstream path per item, grouped by the Phase 2 workstreams. If an item from the old "missing" list already landed, record where (don't re-implement).

## Phase 1 — the nine fixes (only items Phase 0 confirms still open)

- Fix code (preferred) or correct the manifest, never both drifting: each fix updates code + manifest + tests together.
- New tests per item (endpoint behavior + federation behavior where applicable): wrong-kind token on requests/list; moderator delete (whichever side wins); suspended-user show; anonymous `requireSigninToViewContents` note; notif bucket + empty-filter ordering; `withReplies` default; fileIds/mediaIds precedence; timeline flags honored; featured pin sync / Undo(Accept) unfollow / Delete(Person) marking; meta proxy truthfulness.
- DIVERGENCE entries where upstream behavior is intentionally not matched (with reasons).

## Phase 2 — implement the remainder (in dependency order; skip translate/achievements/passkeys)

Workstreams (Phase 0 finalizes this list; expected shape):

1. **Auth hardening**: app registry (`NO_SUCH_APP` reachable — any-secret mints sessions today), 2FA/TOTP + security-key branches on signin (captcha: config-gated keys per Config rule; absent keys ⇒ documented skip, never silent failure), `my/*` app-management side.
2. **Webhooks**: subscription store + delivery jobs (durable, retryable, crash-safe like federation delivery) + `webhooks/*` endpoints.
3. **Media pipeline**: thumbnails, blurhash/dimensions computation at ingest (packers stop emitting null), EXIF stripping, Range + conditional-GET serving. Backfill story for existing files (open-time job, bounded) or document-as-null-until-reupload — decide in Phase 0, record it.
4. **Reads + discovery remainder**: whatever Phase 0 finds still missing (`fetch-external-resources`/`fetch-rss`, `get-avatar-decorations`, `pinned-users`, `page-push`, `my/*` if not covered in WS1, `server-info`/`stats` from stores, announcements incl. `read-announcement`).
5. **Analytics (scoped)**: retention job + minimal charts from existing stores. NOT a full Misskey-charts port — instance-level counts the admin UI needs, documented-simplified. If a chart has no consumer, it stays out (record per-chart).
6. **Rate-limit + role remainder**: `rateLimitFactor` (or documented single-table decision), bucket the unbucketed expensive reads the audit flagged (`notes/search`, `users/notes|followers|following`, `notes/show` family) with values + rationale.

## Cross-cutting rules

- `domain` ← `application` ← adapters; one txn state+job; idempotent/retryable/crash-safe jobs; FDB 5s/10MB discipline; federation input untrusted.
- Upstream error codes + ids verbatim; DIVERGENCEs with reasons.
- New stores → `ports.rs` → redb → conformance group + counts. New settings → Config rule above.
- Contract updates: manifest superseding note per workstream, profile §5, parity-plan §14, wiki + README can/can't lists.

## Gates

1. `cargo test --offline` green (counts recorded). 2. `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK`. 3. `bash scripts/keystone-sweep.sh` → all passing (update sweep where newly-implemented endpoints un-404). 4. Live two-server proof for the federation fixes (featured sync, Undo(Accept), Delete(Person)). 5. State logs per AGENTS.md with real output.

## Deliverables

Phase 0 punch list (committed as part of the batch log even if items turn out fixed — negative results are results). Phase 1 fixes + tests. Phase 2 features + contracts. README/wiki where behavior changed.

## Non-goals

`notes/translate` (no backend), achievements, passkeys, Mastodon API, OAuth, local clips/channels/pages (federated variants have their own plan), thread-muting, relays, LD-signatures, email sending (dormant transport stays unless owner wires SMTP), custom roles.
