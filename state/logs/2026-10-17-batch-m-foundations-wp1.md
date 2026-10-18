# 2026-10-17 — Batch M foundations + WP1: roles, moderation log, instance settings, reject-quotes fix

Session scope: `docs/implementer-guide-batch-m.md` §3 foundations + WP1 (suggested order §12 items 1–2). Implementation commit **e331d4f**; this log + `docs/misskey-parity-plan.md` §14 land after it.

## What changed

**Domain** (pure; no I/O)
- `domain/account.rs`: `Account.is_admin` / `Account.is_moderator` (`#[serde(default)]`, so pre-Batch-M rows decode to `false`), `has_moderator_powers()` (admin implies moderator).
- `domain/moderation.rs` (new): `ModerationLogEntry { id, actor_account?, action, target?, details, created_at_ms }` (`details` = upstream `ModerationLogs.info` JSON object), `ModerationAction` (string-serialized, upstream type names where they exist), `AbuseReport` + `AbuseReportResolution` (WP4 domain, landed early).
- `domain/instance.rs` (new): `InstanceSettings` (registration, blocked_hosts, name, description, max_note_text_length; defaults = today's behavior: open / none / 3000), `RegistrationPolicy` (open|closed ↔ `disableRegistration`), `InviteCode` (WP2 domain), `InstanceSettings::normalize_blocked_hosts`, `host_is_blocked` — upstream `UtilityService.isBlockedHost` semantics (equality or `host.ends_with("." + blocked)`).

**Ports** (`application/ports.rs`) — `ModerationLogStore` + `InstanceSettingsStore`, **required methods only** (guide §2 rule 4: no `unimplemented!()`/`Err` defaults, so the FDB adapter must implement them). Atomicity is structural like the rest of the file: the compound methods (`set_suspended`, `set_role`, `update_account_logged`, `InstanceSettingsStore::update`) commit state + moderation-log row + jobs in one storage transaction; `append` is only for rows that are the whole action. `set_suspended` also deletes follow requests in both directions on suspend (upstream `postSuspend`).

**redb**: `moderation_logs` (id-keyed, reverse range = newest first) and `instance_settings` (singleton) tables; `impl ModerationLogStore/InstanceSettingsStore for RedbStore`; shared `append_log_in_txn`, `delete_follow_requests_involving_in_txn`, `set_account_flags_in_txn`.

**API / application**
- `api/misskey/auth.rs`: `require_moderator(caller)` / `require_admin(caller)` (role checks in the handler — scopes alone are not authorization).
- `packers.rs`: `isModerator`/`isAdmin` now the real flags (`isModerator` true for admins, upstream `RoleService.isModerator` parity); new `pack_moderation_log` (upstream `ModerationLogEntityService.pack` shape).
- `application/queries.rs`: `viewer_is_moderator` reads the real role (was hardcoded `false`); returns `Result<bool, _>` now.
- `application/commands/accounts.rs`: `set_reject_quotes(moderator_id, account_id, reject)` — WP3 refusal rule (never act on `has_moderator_powers()` targets), Sharkey-shaped log row, flag + log + `MeUpdated` job in one transaction. No-change calls stay no-ops (no log row), Sharkey parity.
- `application/commands/moderation.rs` (new): `set_role` (only write path for the role flags; `UpdateAccountPatch` cannot set them) and `list_moderation_logs`.
- `api/misskey/endpoints/mod.rs`:
  - `admin/reject-quotes` — **live authorization bug closed** (any first-party credential could flag any account; parity log 2026-10-16:114–116 admitted it). Now `require_permission` + `require_moderator`, refusal on moderator/admin targets, logged.
  - `admin/show-moderation-logs` (new) — credential + `read:admin:show-moderation-log` + moderator; `limit`/`sinceId`/`untilId`/`sinceDate`/`untilDate`/`type`/`userId`/`search` (upstream params); unknown `type` answers `[]` like upstream's `log.type = :type`.
- `main.rs`: CLI `set-role --username=<name> (--admin|--moderator)[=false]` (flags not named keep their values; operator path for the first admin).

**Wiring**: `AppServices` gained `moderation` + `instance_settings` handles in all 21 construction sites (19 redb-backed, 2 test-double `Mem`s got stub impls in the house `unavailable(...)` style).

**Docs (same change set)**: `docs/endpoint-manifest.md` §21, `docs/application-ports.md` superseding note (new ports, no defaults), `docs/persistence-conformance.md` §17 (Groups H/I/J; H executed, I/J skeletons), `docs/misskey-parity-plan.md` §14 (per-WP status).

## What was verified (executed output)

- `cargo test --offline` — **532 passed, 0 failed** across all targets:
  - lib: `test result: ok. 410 passed; 0 failed` (was 408 pre-session; +2 domain/endpoint tests, reject-quotes + new `admin_show_moderation_logs_moderator_only`)
  - conformance: `test result: ok. 94 passed; 0 failed` (was 90; +4 Group H cases: `h1_suspend_atomicity`, `h2_unsuspend_atomicity`, `h3_suspend_unsuspend_race`, `h4_log_never_lost`)
  - crash_recovery 2, federation 2, federation_elixir 2, federation_interop 1, client_profile 1, client_smoke 1, minrepro 1, quote_fixtures 5, streaming 13.
- Conformance atomicity proof is fault-injection through the pure port interface: passing an invalid log row (`details` not an object) into `set_suspended`/`update_account_logged` fails the whole action — flag/patch unchanged, no log row, no job — proving state + row + jobs commit together or not at all (H1/H2/H4). H3 ran 8 threads × 20 alternating suspend/unsuspend: exactly 160 rows, ids unique, account row valid. H4 also asserts the pair survives store reopen.
- Endpoint tests assert: plain user → `403 PERMISSION_DENIED` and **no state change**; moderator succeeds and the log row is readable in the same request's aftermath; moderator-on-moderator refused with no state change; log packing keys `{id, createdAt, type, info, userId, user}`; `userId`/`type` filters; `untilId` paging.

## Divergences recorded (do not "fix" back)

1. **Log action names**: match upstream where an upstream type exists — `updateServerSettings` (guide §3.2 lists `updateMeta`), `createInvitation` (guide lists `createInvite`), and Sharkey's `acceptQuotesUser`/`rejectQuotesUser` for the quote toggle (guide lists `updateUserRejectQuotes`; `admin/reject-quotes` is Sharkey parity and Sharkey `types.ts:140-141` names these two). Client moderation-log UIs label rows by exact string, so upstream strings win. Pinetree-only names: `setRole`, `deleteInvite`, `createAccount`.
2. **Moderator-target refusals** answer `400 INVALID_PARAM` carrying upstream's `cannot moderate moderator account` message. Upstream throws an untyped `Error` and its API layer answers `500 INTERNAL_ERROR` with a generic message (`ApiCallService.ts:#onExecError`); there is no upstream error code/id to copy (guide §1.3 rule). Clients see a failure either way; the message matches upstream.
3. **CLI `setRole` rows** carry `user`/`userId` = `null` (operator action, no acting account); upstream rows always have a user.
4. **`admin/show-moderation-logs` is moderator-only** (guide §3.2); upstream requires admin (`requireAdmin`).
5. **Role flags are not in `UpdateAccountPatch`** — `ModerationLogStore::set_role` is the only write path (narrower attack surface than patching).

## Open questions / next steps

- **WP3 undo-follow fan-out**: guide WP3 asks for an Undo-Follow job per outgoing follow inside the suspend transaction; that is unbounded against `MAX_JOBS_PER_TXN = 500` and the FDB txn cap (AGENTS hard constraint). Plan: one sweep job the worker expands into per-recipient deliveries (§11 "hand bulk work to jobs"). Flagged in `misskey-parity-plan.md` §14.
- **WP2 next** (registration policy, invites, the `admin/accounts/create` hole — treat as the first WP2 item per guide §11): `InviteStore` port lands there with conformance Group I cases.
- Deferred to batch end (§6): `docs/phase-3-profile.md` §5 non-goal refresh, `docs/testing.md` + §7 smoke-gate fix, `docs/wiki/*`, AGENTS.md phase + test counts.
- Pre-existing untracked files from the guide session (`docs/implementer-guide-batch-m.md`, `state/logs/2026-10-17-implementer-guide-batch-m.md`, `state/logs/2026-10-17-batch-m-scope-update.md`, `docs/README.md` row) are included in the follow-up commit so the batch record is in git.
