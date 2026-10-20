# Handoff: Batch O — self-service, follower removal & lookup, drive extras, url-preview, user lists

> **Status (2026-10-20 freshness pass)** — spot-checked (endpoint strings + dispatch arms present); behaviour not re-tested.
> - **LANDED:** `following/invalidate` (`src/application/commands/follows.rs`, `endpoints/mod.rs`); `users/search-by-username-and-host` (`endpoints/mod.rs`); `drive/files/upload-from-url` (`endpoints/mod.rs:~8017`, `src/application/commands/media.rs`); `drive/files/find-by-hash` (`endpoints/mod.rs:~8064`); `url-preview` (`endpoints/mod.rs:~8371`); `i/revoke-token` (`endpoints/mod.rs:~6770`); user lists (`src/application/commands/user_lists.rs`).
> - **OPEN:** `users/lists/create-from-public` (absent, 0 hits; TODOS "Manifest"); remote list members via system actor (owner decision pending); `drive/files/find|move-bulk|check-existence` (TODOS P1).

## Why

The remaining client-visible gaps, in dependency order: self-service auth (WP9), follower removal + remote lookup (WP10), drive extras (WP13) + `url-preview` (WP12), and user lists (WP16 — the biggest item; it also unblocks antennas `src=list`, currently refused). Guide spec: `docs/implementer-guide-batch-m.md` §§5 (WP9–WP13, WP16), 8, 9. Upstream pins: Misskey `2026.10.1-alpha.0`, Sharkey `2025.4.7` (read-only).

## Instructions (use subagents)

You MUST use subagents (`general` or `explore`) for parallel investigation, then synthesize into one build order and implement serially (domain → ports → redb → op → route + packer → federation → conformance → manifest → smoke). Do NOT skip the subagent step.

- Subagent A (self-service + tokens): map pinetree's token model (`src/application/ports.rs:1888` `ApiTokenRecord`, `app_secret`) — native token vs app grants, what `i/revoke-token {tokenId}` must revoke to make the no-op real. Upstream: `i/change-password.ts` (`currentPassword`, `newPassword` min 1, optional `token`), `i/regenerate-token.ts` (`password`), `i/apps.ts` (sort `+createdAt|-createdAt|+lastUsedAt|-lastUsedAt`), `i/authorized-apps.ts` (limit/offset/sort), `i/signin-history.ts` (limit/sinceId/…). Read each handler fully: does change-password revoke other sessions? What does regenerate-token return? What distinguishes apps from authorized-apps? Output: endpoint/param/error table + token-model mapping + signin-history recording points.
- Subagent B (follows/lookup): upstream `following/invalidate.ts` (kind `write:following`; errors `NO_SUCH_USER b77e6ae6-…`, `FOLLOWER_IS_YOURSELF 07dc03b9-…`, `NOT_FOLLOWING 918faac3-…`; unfollows via `userFollowingService.unfollow(follower, followee)`; returns packed follower `UserLite`), `users/search-by-username-and-host.ts` (username?/host? alias over user search), `ap/get.ts` (params `uri?/userId?/noteId?/expandCollectionItems/limit/allowAnonymous`; read its `meta` — credential? — and its fetch path). Pinetree: existing unfollow op + federation path (`following/delete` flow), `users/show` by id/username+host, `ap/show`. Output: invalidate plan (incl. remote Reject/Undo reuse), alias plan, `ap/get` risk assessment (arbitrary-URI resolution — SSRF guard + auth mandatory; recommend scope).
- Subagent C (drive + preview): upstream `drive/files/upload-from-url.ts` (`url!`, `folderId?` nullable, `isSensitive`, `comment`, `marker?`, `force`) — read the handler: what is `marker` (opaque passthrough or semantics? accept-and-ignore only if opaque); `drive/files/find-by-hash.ts` (`md5!`). Pinetree: `src/media/ingest.rs`, §2-rule-6 fetcher, SHA-256 content keys (`src/media/storage.rs`), drive-file duplicate logic (`force`). Keystone: `url-preview` shape `{title?, description?, siteName?, image?, icon?}`. Output: ingest-reuse plan, md5 decision (see below), preview fetch/parse/cache plan.
- Subagent D (user lists): guide WP16 in full (model, limits, visibility, pull-not-push timeline, reply rule, remote members + system actor, `userList` stream, notification list-config, CSV export/import, errors, acceptance). Upstream: `users/lists/*` endpoints, `notes/user-list-timeline.ts:148–250` (the pure-database read shape that justifies pull), `UserListService.ts:97–121` (remote-member Follow). Pinetree: `packers.rs:633` advertises 5 (change to enforced value), `notificationRecieveConfig` list-drop at `application/commands/notifications.rs:64–66`, antennas `src=list` refusal + membership index. Output: implementation plan in guide order + conformance cases + what enabling `src=list` touches.

## Workstreams (build in this order)

### 1. WP10 — follower removal + lookup (first; smallest, unblocks nothing but closes fast)

- `following/invalidate {userId!}`: self → `FOLLOWER_IS_YOURSELF`; unknown → `NO_SUCH_USER` (pinetree's id); not-a-follower → `NOT_FOLLOWING` (`918faac3-…`); else remove the follow edge (same txn discipline as unfollow) and emit Reject/Undo to a remote follower via the existing unfollow federation path. Return packed follower `UserLite`.
- `users/search-by-username-and-host {username?, host?}`: thin alias over `users/show` resolution. `ap/get`: implement per subagent B's recommendation; minimum bar is credential-required + SSRF-guarded + bounded; if the upstream semantics can't be mirrored safely, ship the safe subset and document the rest as `UNSUPPORTED_FIELD`, don't widen the fetch surface.

### 2. WP9 — self-service (needs subagent A's token map)

- `i/change-password {currentPassword!, newPassword! (min 1), token?}`: verify current password first (wrong-password error mirrored from upstream); mirror upstream on session revocation (read the handler — do what it does); new password through the same hasher/policy as signup.
- `i/regenerate-token {password!}`: verify password; rotate the **native** token only; app grants keep working (owner decision §9.7 default — confirm, don't reinterpret).
- `i/apps` (sort param) + `i/authorized-apps` (limit/offset/sort): list the caller's grants per the upstream distinction subagent A documents.
- `i/revoke-token {tokenId}`: make it real (revoke the named grant); keep the no-param form (revoke caller token).
- **New store required**: `i/signin-history` has no backing — nothing records signins today. Add a `SigninRecord` store + hooks in ALL signin paths (password signin, MiAuth approve, `auth/session/userkey`), record from deploy forward (no backfill possible — document it), cursor-paged list. Keep rows bounded (cap per user, trim oldest in the same txn).
- `i/update-email`: DEFERRED (needs the §9.2 email decision). Do not build.

### 3. WP13 + WP12 — drive extras + url-preview

- `drive/files/upload-from-url {url!, folderId?, isSensitive?, comment?, marker?, force?}`: reuse `src/media/ingest.rs` + the §2-rule-6 fetcher (SSRF, timeouts, redirect cap, size cap, `image/*`-style content checks per existing ingest rules). `folderId`: no folders exist — require null (same rule as `drive/files`), don't invent folder semantics. `marker`: per subagent C's finding. `force` follows existing drive dedup semantics.
- `drive/files/find-by-hash {md5!}`: upstream keys on md5; pinetree content-keys on SHA-256. Decision (implementer picks, records as DIVERGENCE either way): **store md5 alongside SHA-256 at ingest + bounded backfill at open** (precedent: `REACTIONS_BY_NOTE` backfill) so old files resolve too — preferred; fallback is new-ingests-only with old files documented as unresolvable until re-uploaded. Do NOT match the `md5` param against SHA-256 keys silently.
- `url-preview {url!}` (first-party, Keystone shape — NOT a Misskey endpoint): credential REQUIRED (anonymous link-preview is an open proxy); SSRF guard + timeouts + redirect cap + size cap; parse OpenGraph + `<title>` only; no arbitrary content types. Cache in process memory with size + TTL caps — explicitly NOT in the store (no FDB impact). Return `{title?, description?, siteName?, image?, icon?}`.

### 4. WP16 — user lists (last; biggest; unblocks antennas `src=list`)

Per guide WP16, no shortcuts: `users/lists/create|update|delete|list|show|push|pull|get-memberships|update-membership|favorite|unfavorite`, `notes/user-list-timeline` (pull: merge members' newest-first with bounded member count — no write amplification), `userList` stream (credential + owner check up front, membership re-read per event, `userAdded`/`userRemoved`), CSV export/import, list-based notification receive-config (replace the creation-time drop), `userListLimit` 10 enforced AND advertised (fix `packers.rs:633`), per-list member cap 50 (`TOO_MANY_USERS` — confirm name against `RoleService.ts`), visibility (public readable, private owner-only, `list` refuses remote users), `create-from-public` DEFERRED (guide §8). Remote members: system actor + queued Follow per upstream (owner default §9.12 — confirm with owner if this fires during implementation, it creates outbound follows the user didn't directly request). Enabling antennas `src=list` rides here (membership index shared) — flip the refusal to real matching + test.

## Cross-cutting rules

- `domain` ← `application` ← adapters; one txn commits state + outbox/job; jobs idempotent/retryable/crash-safe; FDB 5s/10MB discipline; upstream error codes + ids verbatim, DIVERGENCEs recorded with reasons.
- New stores → `ports.rs` trait → redb → conformance group (`SigninRecord`, list memberships; `docs/persistence-conformance.md` counts).
- Contract updates: manifest superseding note per workstream, profile §5, parity-plan §14 WP9/WP10/WP13/WP12/WP16 lines → LANDED, `docs/wiki/*` where user-visible, README capability lists (favorites/emoji/antennas precedent: keep the README's can/can't lists accurate).
- Federation input untrusted (§2-rule-6 fetcher for url-preview/upload-from-url; `ap/get` per subagent B). Password endpoints get the auth-failure rate-limit treatment like the signin paths.

## Gates

1. `cargo test --offline` green (lib + conformance + integration counts recorded).
2. `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK` (phase2 SKIP only if environmental).
3. `bash scripts/keystone-sweep.sh` → 41/0.
4. Keystone `api-surface.md` re-check for anything newly unblocked (list source, notification list picker, preview fetcher swap — report, don't implement).
5. State logs per `AGENTS.md` with real command output; never rewrite past logs.

## Deliverables

Code + tests + contract updates for all five items (or feasibility refusal with code evidence). README/wiki touch-ups where user-visible behavior changed.

## Non-goals

- No Keystone edits. No `i/update-email`, no `create-from-public`, no drive folders (still out — `folderId` stays null-only), no thread-muting, no Mastodon API/OAuth, no email/SMTP, no custom roles, no relays management.
- `ap/get` scope is capped by subagent B's safety assessment — a documented safe subset beats an upstream-exact open fetch.
