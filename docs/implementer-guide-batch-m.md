> **Status (2026-10-20 freshness pass):** WP1–WP16 are largely landed in the dispatcher. Open: WP8 admin emoji (partially), WP16 `create-from-public` and remote-member system actor, §6 doc updates, §7 smoke-gate fix (still `tail -30` at `scripts/smoke-all.sh:53`), and §9 owner decisions. Line refs in §1.2 are stale; use `docs/inventory-pinetree-vs-misskey-2026-10-20.md` §1.
> Tags: `[LANDED — evidence]` verified in code at HEAD; `[OPEN]` not in code; `[SUPERSEDED by …]`; `[DECISION — still binding]`; `[PLAN HISTORY]`. Original text is unchanged; tag blocks are additions. Short paths are under `src/`; `mod.rs` = `src/api/misskey/endpoints/mod.rs`.

# Implementer guide — Batch M: moderation, registration policy, and daily-use parity (pre-Phase 4)

**Status:** Draft (2026-10-17). Owner decisions in §9 are open; the defaults stated there apply until changed.
**Sources:** `AGENTS.md`; `sns-server-proposal.md` §§2–5, §9; `docs/endpoint-manifest.md`; `docs/phase-3-profile.md` §§3–5; `docs/application-ports.md`; `docs/persistence-conformance.md`; `docs/misskey-parity-plan.md`; Keystone `docs/api-surface.md` (in `/root/keystone`); upstream Misskey `2026.10.1-alpha.0` under `misskey/packages/backend/src/`.

---

## 0. Why this batch exists and what "done" means

> **[PLAN HISTORY]** Pre-Phase-4 batch; scope executed per work package below.


Pinetree is a headless backend. Keystone is the primary UI and the target for every item here. The Misskey web UI is an optional frontend path. Pinetree does **not** serve any UI bundle, and that is not a gap.

This batch closes the gaps that keep pinetree from being usable as a real instance backend. It must land **before Phase 4 (FoundationDB)**, so that the new ports are designed once and then implemented by both backends. A new port that the FDB adapter has to rediscover later is a defect of this batch.

Scope:

- **In:** admin and moderator identity, moderation log, registration policy (open / invite-only / closed), user suspension, abuse reports, instance blocks, the admin read/write surface those need, the daily-use list in §5, **antennas and user lists** (WP15, WP16), and **remote custom emoji as a per-user opt-in, off by default** (WP8b).
- **Out:** web push (`sw/*`, per owner instruction) and the items in §8.
- **Last:** the smoke gate fix in §7, run after implementation and before the final test run.

Done means all of the following hold:

1. Every work package (§4–§5) has endpoint tests and conformance cases, and the redb adapter passes them.
2. `cargo test --offline` passes. Report the executed count from the run, not the count in any doc.
3. Keystone's call list (`/root/keystone/docs/api-surface.md`) passes against pinetree via `scripts/keystone-sweep.sh`, updated for new calls.
4. `scripts/smoke-phase1.sh` and `scripts/smoke-phase3.sh` still pass.
5. `bash scripts/smoke-all.sh` runs green, with the §7 fixes in place. Phase 2 either runs or is explicitly opted out, and the log says which.
6. Docs, logs, and the parity plan are updated (§6).

---

## 1. Read before writing code

> **[PLAN HISTORY]** Line refs in §1.2 point at pre-batch code (e.g. `queries.rs:124–130`, now `queries.rs:135`).


1. `AGENTS.md` — layout, state-maintenance rules, architecture rules, verification rules.
2. `docs/README.md` — contract status. Frozen contracts change only through a logged superseding decision.
3. `docs/application-ports.md` and `src/application/ports.rs` — the port pattern. This batch adds ports; read the existing ones first.
4. `docs/persistence-conformance.md` — case groups A–F. This batch adds groups H–J (§3.4).
5. `docs/phase-3-profile.md` §§3–5 and `docs/endpoint-manifest.md` §8 — the default for unknown endpoints is 404, and §5 is the list of non-goals.
6. `docs/misskey-parity-plan.md` — the house pattern for a plan: each section carries a status line and points at the commit and log that landed it. Follow that pattern in §6.
7. `/root/keystone/docs/api-surface.md` — the calls the primary UI makes. See §1.1 for known staleness.

### 1.1 Corrections to the earlier gap analysis

> **[PLAN HISTORY]**


- **Serving the UI is not a gap.** Pinetree is paired with a UI, not serving one.
- **Keystone's Pinetree column is stale in places.** Its table says `drive/files` returns 404 (`api-surface.md:128`). Pinetree implements `drive/files` (`src/api/misskey/endpoints/mod.rs:3757`). Its table also says streaming is "not implemented" (`:134`), but Phase 3 shipped streaming. Report both to the Keystone side (§10); do not edit the Keystone repo from here.
- **Keystone still calls `url-preview`, `emojis`, `antennas/*`.** It records `url-preview` as a pinetree workstream (`api-surface.md:99`). `emojis` is a gap (`:73`, `:133`). `antennas/*` is an honest 404 today and becomes real under WP15.
- **`signin-flow` is only needed for the optional Misskey web UI path.** Keystone uses `signin` on pinetree (`api-surface.md:37`). It is not in this batch (§8).

### 1.2 Verified current state (cite these when you start)

> **[PLAN HISTORY]** Superseded by `inventory-pinetree-vs-misskey-2026-10-20.md` §1.


| Fact | Where |
|---|---|
| `isModerator` and `isAdmin` are hardcoded `false` | `src/api/misskey/packers.rs:678–679` |
| `disableRegistration` hardcoded `false`; `enableEmail` hardcoded `false` | `src/api/misskey/packers.rs:1537`, `:1563` |
| `admin/accounts/create` is an alias of public `signup`, with no admin check | `src/api/misskey/endpoints/mod.rs:2273` |
| `admin/reject-quotes` checks only `write:admin:reject-quotes`; no moderator check; no moderation log | `mod.rs:2834–2855`; TODO at `:2828–2830`; permission check at `:2837` |
| `blocked_hosts()` always returns empty; comment says there is no config | `src/application/queries.rs:124–130` |
| Read-path filtering already checks suspended authors and blocked hosts | `queries.rs:247–264` |
| `is_suspended` is read everywhere; **no production setter exists** | `queries.rs:1436` is a test double; checks at `auth.rs:253`, `actor.rs:534`, `webfinger.rs:145`, `inbound/notes.rs:193`, `redb/mod.rs:658,2851` |
| No moderation-log store | `src/application/commands/accounts.rs:44` |
| No `Flag` activity rendering or delivery found by grep | `src/` |
| `create-account` CLI exists | `src/main.rs:65`, `:160` |
| `notifications` list includes `followRequestAccepted` as not emitted | `mod.rs:952` |
| Smoke step 1 pipes `cargo test` through `tail`; the step's exit code is `tail`'s | `scripts/smoke-all.sh:53` |
| Phase 2 SKIP still ends with `SMOKE_ALL_OK` | `scripts/smoke-all.sh:77–79`, `:92` |

### 1.3 Upstream references

> **[PLAN HISTORY]**


Read each upstream file before implementing its feature. Paths are under `misskey/packages/backend/src/`.

| Feature | Upstream |
|---|---|
| Suspend / unsuspend | `server/api/endpoints/admin/suspend-user.ts`, `unsuspend-user.ts`; `core/UserSuspendService.ts` |
| Report | `server/api/endpoints/users/report-abuse.ts`; `core/AbuseReportService.ts` |
| Resolve report | `server/api/endpoints/admin/resolve-abuse-user-report.ts` |
| Forward report | `server/api/endpoints/admin/forward-abuse-user-report.ts` (`AbuseReportService.forward`) |
| List reports | `server/api/endpoints/admin/abuse-user-reports.ts` |
| Moderation log | `server/api/endpoints/admin/show-moderation-logs.ts`; `core/ModerationLogService.ts` |
| Invites | `server/api/endpoints/invite/create.ts`, `invite/list.ts`, `invite/delete.ts`, `invite/limit.ts`; `admin/invite/create.ts`, `admin/invite/list.ts` |
| Signup gate | `server/api/SignupApiService.ts` (the `disableRegistration` check is around line 136; read the invite check that follows it) |
| Meta read / write | `server/api/endpoints/admin/meta.ts`, `admin/update-meta.ts` (`blockedHosts` at `:259–260`, lowercased) |
| Instance block semantics | `core/UtilityService.ts:52–54` (`isBlockedHost` is a suffix match: host ends with `.` + blocked host); `core/QueryService.ts:340`; `core/FanoutTimelineEndpointService.ts:145–148` |
| Admin user views | `server/api/endpoints/admin/show-user.ts`, `show-users.ts`, `update-user-note.ts`, `delete-account.ts` |
| Favorites | `server/api/endpoints/notes/favorites/create.ts`, `delete.ts`; `i/favorites.ts`; `notes/state.ts` |
| Follower removal | `server/api/endpoints/following/invalidate.ts` |
| Custom emoji | `server/api/endpoints/emojis.ts`, `emoji.ts`, `admin/emoji/add.ts`, `update.ts`, `delete.ts`, `list.ts` |
| Account self-service | `server/api/endpoints/i/change-password.ts`, `i/regenerate-token.ts`, `i/delete-account.ts`, `i/apps.ts`, `i/authorized-apps.ts` |
| Remote lookup | `server/api/endpoints/ap/get.ts`; `users/search-by-username-and-host.ts` |
| Drive extras | `server/api/endpoints/drive/files/upload-from-url.ts`, `find-by-hash.ts` |

Misskey error codes you will need, with the upstream ids (copy these exactly):

- `NO_SUCH_USER` `1acefcb5-0959-43fd-9685-b48305736cb5`
- `CANNOT_REPORT_YOURSELF` `1e13149e-b1e8-43cf-902e-c01dbfcb202f`
- `CANNOT_REPORT_THE_ADMIN` `35e166f5-05fb-4f87-a2d5-adb42676d48f`
- `NO_SUCH_ABUSE_REPORT` `ac3794dd-2ce4-d878-e546-73c60c06b398` (404)
- `EXCEEDED_LIMIT_OF_CREATE_INVITE_CODE` `8b165dd3-6f37-4557-8db1-73175d63c641`

For any other error code, take the id from the upstream file. Do not invent one.

---

## 2. Cross-cutting rules for every work package

> **[DECISION — still binding]**


These come from `AGENTS.md`. They are restated here because the rules are easy to break in a feature this size.

1. **Domain stays pure.** Domain types have no HTTP, Misskey, ActivityPub, or DB imports.
2. **One application operation, two adapters.** The Misskey API and ActivityPub adapters call the same command. Do not put a moderation rule in an endpoint arm.
3. **Atomic state + outbox.** A moderation action commits its state change, its moderation-log row, and its outbound jobs (Delete, Undo, Flag, Undo-Follow) in one storage transaction. Group A of the conformance suite defines this. Never write the state and then enqueue separately.
4. **Ports have no `unimplemented!()` defaults.** `ports.rs` already has 20 `unimplemented!()` defaults and 39 "not implemented" `Err` defaults (`ports.rs:388–1369`). Any backend that omits one panics at runtime. **New ports must have required methods**, so the FDB adapter does not compile until it implements them. Do not add to the existing defaults.
5. **Bounded transactions.** No media bytes, no unbounded fan-out, and no outbound HTTP inside a transaction. FDB limits are ~5 s and ~10 MB (proposal §§2, 5).
6. **Untrusted input.** Anything that fetches a remote URL (`url-preview`, `drive/files/upload-from-url`, `forward` delivery) uses the same SSRF guard, timeouts, redirect limit, and size limit as federation delivery (`src/federation/delivery.rs`: `is_private_or_reserved_ip`, plus redirect-target re-validation). Do not write a second fetcher. Never let `PINETREE_ALLOW_PRIVATE_IPS` reach a user-facing fetch; it is for local federation tests only.
7. **Public IDs never expose node hostnames.** Account URLs stay `https://{domain}/users/{account_id}`.
8. **Every new endpoint gets an entry in `docs/endpoint-manifest.md`**, and every change to the non-goal list gets a superseding note in `docs/phase-3-profile.md` §5 (§6 below).
9. **Errors match upstream.** Use the upstream code and id for each error. The `{ error: { message, code, id } }` shape is already implemented in `src/api/misskey/errors.rs`.
10. **Permissions.** Admin and moderator endpoints require the role in the handler, not only a scope. Scopes alone are not authorization (see WP1).

---

## 3. Shared foundations (do these first; WP1–WP3 depend on them)

> **[LANDED — domain/instance.rs:99 (blocked_hosts); domain/moderation.rs:74,100 (moderation log actions); api/misskey/auth.rs:243 (require_moderator)]** §3.4 conformance groups H–Z are present in `tests/conformance.rs`; see `persistence-conformance.md` §17.


### 3.1 Roles

> **[LANDED — api/misskey/auth.rs:243 require_moderator; require_admin in the same module]** `role_ids` stored but not enforced (`TODOS.md` domain gaps).


- Add two booleans on `Account` in `src/domain/account.rs`: `is_admin`, `is_moderator`. Use fixed built-in roles only. Custom roles and policies are out (§8).
- Persist in redb. Check how the existing schema is versioned in `src/persistence/redb/mod.rs` and follow that pattern. Do not invent a second migration mechanism.
- `packers.rs:678–679` reads the real flags.
- Add a CLI subcommand next to `create-account` in `src/main.rs`: `set-role --username=<name> (--admin|--moderator)[=false]`. This is how the first admin is created.
- Add `require_moderator(caller)` and `require_admin(caller)` helpers in `src/api/misskey/auth.rs`, next to the existing checks.
- An admin implies moderator. Moderators cannot act on admins or other moderators (the refusal rule in WP3).

### 3.2 Moderation log

> **[LANDED — domain/moderation.rs:74,100; admin/show-moderation-logs mod.rs:4868]**


- Domain type `ModerationLogEntry { id, actor_account, action, target, details, created_at }`. `action` is an enum: `suspend`, `unsuspend`, `resolveAbuseReport`, `forwardAbuseReport`, `updateAbuseReportNote`, `updateUserRejectQuotes`, `setRole`, `updateMeta`, `createInvite`, `deleteInvite`, and so on. Match upstream action names where they exist (see `ModerationLogService.ts`).
- Port `ModerationLogStore`: `append(entry)` (called inside the same transaction as the action, §2.3) and `list(filter, cursor, limit)`.
- Endpoint: `admin/show-moderation-logs` (upstream file of the same name), cursor paging `sinceId`/`untilId`/`limit`, moderator only.
- This closes the TODO at `mod.rs:2830` (moderation log).

### 3.3 Instance settings

> **[LANDED — domain/instance.rs:99 blocked_hosts; admin/update-meta mod.rs:5642]**


- Domain type `InstanceSettings { registration: RegistrationPolicy, blocked_hosts: Vec<String>, name, description, max_note_text_length, … }`. Keep only what this batch uses; do not build a generic settings bag.
- Port `InstanceSettingsStore`: `get()` and `update(patch)`. `update` runs in a transaction with the moderation-log append.
- Defaults reproduce today's behavior: registration open, no blocked hosts, `maxNoteTextLength` 3000.
- `packers.rs:1537` and `:1560` read from this store (not hardcoded).

### 3.4 Conformance groups to add (`docs/persistence-conformance.md`)

> **[LANDED — tests/conformance.rs (118 #[test] fns by grep)]** Redb only; FDB not implemented.


Add these to the suite; both adapters must pass them. Give each case an id following the existing style.

- **H (moderation):** H1 suspend atomicity (state + log + Delete job together); H2 unsuspend atomicity; H3 concurrent suspend and unsuspend race (no lost update); H4 log append is never lost when the action commits.
- **I (registration and invites):** I1 single-use invite consumed by N concurrent signups → exactly one succeeds; I2 consume + account create is atomic (no account without a consumed invite, and no consumed invite without an account); I3 expired invite rejected; I4 `disableRegistration` with no invite → rejected.
- **J (favorites and emoji):** J1 favorite is unique per (account, note) under concurrency; J2 emoji shortcode unique case-insensitively, matching the account handle rule (group C2); J3 delete of a favorite leaves the note.

---

## 4. Admin and moderation (WP1–WP6)

> **[LANDED — per-WP below]**


Do WP1–WP3 before anything else in this section. WP4–WP6 depend on them.

### WP1 — Roles, guards, moderation log, and the reject-quotes fix

> **[LANDED — mod.rs:4535-4539 (reject-quotes calls require_moderator); mod.rs:4868 (show-moderation-logs)]** The `:2824–2832` TODO removal was not re-verified.


**Deliverables**

- §3.1 roles, §3.2 log, CLI `set-role`.
- Guards on every admin and moderator endpoint in this batch.
- `admin/reject-quotes` (`mod.rs:2834–2855`): add `require_moderator` (and refuse to act on admins/moderators, per the WP3 refusal rule). Write a moderation-log entry. Remove the TODO comments at `:2824–2832`.
- `admin/show-moderation-logs`.

**Why this is first.** Right now any first-party credential can set `rejectQuotes` on any account. The parity log admits this (`state/logs/2026-10-16-misskey-parity-complete.md:114–116`). This is a live authorization bug, not a design choice. Fix it before anything else in this batch.

**Acceptance**

- A plain user calling `admin/reject-quotes` gets 403 and no state change.
- A moderator succeeds and gets a log row in the same commit.
- Test: the existing case in `mod.rs` (the reject-quotes test near `:10895`) is updated to assert the moderator requirement.

### WP2 — Registration policy and invite-only signup

> **[LANDED — mod.rs:2784-2788 (admin/accounts/create requires admin); mod.rs:5441-5512 (invite/create|list|delete|limit); accounts.rs:27 (registration gate bypass); mod.rs:2074 (signup enforces policy)]** `admin/invite/create|list` not individually located.


**Model (recommended).** Use Misskey's model, which is one switch. `registration` open or closed. When closed, `signup` requires a valid invite code. Closed with no codes issued is a fully closed instance. Read the invite check in `SignupApiService.ts` around line 136 before implementing, so the error shape matches upstream.

**Deliverables**

- `InstanceSettings.registration` (§3.3). `meta.disableRegistration` reads it (`packers.rs:1537`).
- `do_signup` (`mod.rs:1621`) enforces the policy. The consume-invite step and the account create are **one transaction** (conformance I2).
- **Fix `admin/accounts/create`** (`mod.rs:2273`). It is currently a public signup alias. Make it require an admin, bypass the registration gate (that is its purpose), and log the action. Keep `signup` as the public path.
- Invites (upstream `invite/*` and `admin/invite/*`):
  - Store: `InviteCode { code, created_by, created_at, expires_at, used_by, used_at }`.
  - `invite/create` (returns the code), `invite/list`, `invite/delete`, `invite/limit`.
  - `admin/invite/create` and `admin/invite/list` (admin view).
  - Creation rights: see §9.1.
- `admin/update-meta` subset: `disableRegistration` (maps to `registration`), `blockedHosts` (WP6). Read it with `admin/meta`.

**Acceptance**

- With registration closed and no invite: `signup` is rejected.
- With a valid invite: exactly one of N concurrent signups succeeds (conformance I1).
- `admin/accounts/create` by a non-admin is 403.
- `meta.disableRegistration` reflects the setting.

### WP3 — Suspension

> **[LANDED — mod.rs:4935-4938 (admin/suspend-user, unsuspend-user); streaming/hub.rs:221-224 (UserSuspended closes sockets); commands/moderation.rs:164,176 (sweep jobs)]**


**Deliverables**

- `admin/suspend-user` `{userId}` and `admin/unsuspend-user` `{userId}`, moderator only. Refuse to suspend an admin or moderator (upstream: `cannot suspend moderator account`, `suspend-user.ts:45–47`).
- Suspend is one transaction (conformance H1):
  1. set `is_suspended = true`;
  2. delete follow requests in both directions (upstream `postSuspend`);
  3. append the moderation-log row;
  4. enqueue jobs: for a **local** user, a Delete(Person) to every known shared inbox (upstream `postSuspend`); and an Undo-Follow job, marked silent, per outgoing follow (upstream `unFollowAll`, `silent: true`).
- Unsuspend (H2): flip the flag, log, and for a local user enqueue Undo(Delete) to the same inboxes (upstream `postUnsuspend`).
- **Close live streams.** Upstream publishes `userChangeSuspendedState` so open sockets are dropped. Pinetree must disconnect that account's open `/streaming` sockets on suspend. Check `src/streaming/hub.rs`; nothing in it does this today.

**Enforcement already exists** (`queries.rs:260`, `auth.rs:253`, `actor.rs:534`, `webfinger.rs:145`, `inbound/notes.rs:193`). Your job is to add the **setter** and the stream close. Do not rewrite the read-path checks.

**Acceptance**

- Suspended local user: sign-in refused; existing token gets 403; open stream closed; AP actor fetch not served; a Delete is queued to remote inboxes.
- Unsuspend restores all of the above except the removed follow requests, which stay removed.
- Suspending a moderator or admin returns an error and changes nothing.
- Moderation log has one row per action, written in the same commit.

### WP4 — Abuse reports

> **[LANDED — mod.rs:4983 (abuse-user-reports), :5023 (resolve), :5059 (forward); abuse_reports.rs:213 (Flag job); activitypub/objects.rs:860 (Flag renderer)]** Admin abuse-report note-update endpoint not individually located.


**Deliverables**

- `users/report-abuse` `{userId, comment}` (`comment` 1–2048 chars). Errors: `NO_SUCH_USER`, `CANNOT_REPORT_YOURSELF`, `CANNOT_REPORT_THE_ADMIN` (upstream: admins are not reportable, `report-abuse.ts:69–71`). Reporter host is always null (local reports only).
- Store `AbuseReport { id, target_account, reporter, comment, resolved, resolved_as, assignee, moderation_note, forwarded, created_at }`. Port `AbuseReportStore`, with a required `list`, `get`, `resolve`, `set_note`, `mark_forwarded`.
- `admin/abuse-user-reports` (list; moderator). Read upstream for the filter params.
- `admin/resolve-abuse-user-report` `{reportId, resolvedAs: accept|reject|null}`. Error `NO_SUCH_ABUSE_REPORT`. Logged.
- The note-update endpoint from the upstream `admin/abuse-report/*` family (read the file list under `admin/abuse-report/` first). Logged (`updateAbuseReportNote`).
- `admin/forward-abuse-user-report` `{reportId}` — **last in this WP** (§9.5). Only for a remote target (`targetUserHost != null`), only once (upstream: `already been forwarded`). Sends a Flag activity to the target's inbox. Pinetree has no Flag renderer; add one in `src/activitypub/` and a delivery job. Logged (`forwardAbuseReport`).
- Notifications: pinetree does not notify admins by mail or webhook in this batch (§8). Admins poll `admin/abuse-user-reports`.

**Acceptance**

- Reporting yourself, or an admin, fails with the upstream code.
- A report appears in the admin list; resolving it writes a log row.
- Forward on a local target is rejected; forward on a remote target sends exactly one Flag.

### WP5 — Instance blocks

> **[LANDED — application/queries.rs:135-137 (blocked_hosts reads the store, not an empty vec); inbound_host_blocked (queries.rs:137+); admin/update-meta mod.rs:5642]** Delivery-time outbound skip not re-verified. The TODOS line that says `blocked_hosts()` is empty is stale.


**Semantics (copy upstream exactly).** Blocking a host also blocks its subdomains. Upstream `isBlockedHost` (`UtilityService.ts:52–54`) matches a host that ends with `.` + the blocked host. Entries are stored lowercased (`admin/update-meta.ts:260`).

**Deliverables**

- `InstanceSettings.blocked_hosts` (§3.3). `admin/update-meta` `blockedHosts` replaces the list (lowercased, dedup, empty removed). `admin/meta` returns it.
- Replace `queries.rs:124–130` so `blocked_hosts()` reads the store, not an empty vec. Wire the store into whatever struct owns that method. Do not leave a second copy.
- **Inbound:** drop activities from a blocked host **before** any state write or dedup record. Match the suspended-author path (`inbound/notes.rs:193`), and make sure a rejected activity writes no dedup row (or the blocked host's activities would be recorded and then rejected forever, which is wrong after unblock).
- **Outbound:** check the blocked list **at delivery time**, not at enqueue time. The list can change between enqueue and send. Skip deliveries to blocked hosts without failing the job.
- **Read path:** notes whose author, renote author, or reply author is on a blocked host are hidden. This is already implemented as `FeedFilter` for suspended and blocked-host filtering (`queries.rs:247–264`); confirm it covers all three author slots against upstream `FanoutTimelineEndpointService.ts:145–148`.
- `federation/instances` (upstream file of the same name) is optional here. If you skip it, say so in the log.

**Acceptance**

- Blocking `example.com` hides notes from `example.com` and `sub.example.com`, and stops inbound activities from both.
- Unblocking restores visibility of existing notes, with no data loss.
- No dedup record is written for a dropped activity (verified by a test that re-sends the activity after unblock and sees it processed).

### WP6 — Admin read and user views

> **[LANDED — mod.rs:4562 (admin/show-user), :4657 (update-user-note), :4672 (admin/delete-account)]** `admin/show-users` not located.


- `admin/show-user` `{userId}` and `admin/show-users` (upstream params; read the file). Moderator only. Include `isSuspended`, `isAdmin`, `isModerator`, and report counts.
- `admin/update-user-note` `{userId, text}`: moderator note on an account (upstream `update-user-note.ts`). Logged.
- `admin/delete-account` `{userId}`: admin only. Shares the machinery of `i/delete-account` (§5.9). Build it once; do not write two deletes.
- Remove the TODO at `mod.rs:2828–2830` and `accounts.rs:45–108` only as each item lands. Leave any TODO that is still open, and list it in the log.

---

## 5. Daily-use parity (WP7–WP16)

> **[OPEN]** Per-WP status below; WP8 and WP16 have open items.


Items are in priority order for Keystone. Each item is independent unless a dependency is noted.

### WP7 — Favorites and note state

> **[LANDED — mod.rs:6350 (notes/favorites/create); mod.rs:5813-5822 (i/export-favorites enqueues a real export, not an empty file)]** `i/favorites` and `notes/state` not individually re-verified.


- `notes/favorites/create` `{noteId}` and `notes/favorites/delete` `{noteId}`. Unique per (account, note) (conformance J1). Upstream errors: check `create.ts` for `ALREADY_FAVORITED` and `NO_SUCH_NOTE` and copy the ids.
- `i/favorites` (list, `sinceId`/`untilId`/`limit`).
- `notes/state` returns `{isFavorited, …}` for the caller.
- Packed notes carry `isFavorited` when the caller is authenticated (match upstream packing).
- **Also fix `i/export-favorites`**, which currently writes an empty file (parity plan §13). Keystone's import/export UI uses it (`/root/keystone/frontend/src/api/importExport.ts:47, 65`).

### WP8 — Custom emoji (listing and admin management)

> **[OPEN — emojis list mod.rs:5080; admin/emoji/add mod.rs:5109, update :5155 landed]** `admin/emoji/delete|list` and the packing into `reactionEmojis`/`emojis` not re-verified. Check before closing.


Keystone calls `emojis` and the picker drops its Custom section when the call 404s (`api-surface.md:73`). Keystone's verified shape is `{emojis: [{name, category, url, aliases}]}` from `{}` (public, no auth).

- Store `CustomEmoji { id, shortcode, category, url_media_id, aliases, created_at }`. Shortcode unique case-insensitively (conformance J2). Media goes in the blob store, not the emoji record.
- `emojis` (public listing), `emoji` `{name}` (single).
- `admin/emoji/add`, `admin/emoji/update`, `admin/emoji/delete`, `admin/emoji/list` (admin only, logged). Keep the bulk operations out (§8).
- Packing: fill `reactionEmojis` and `emojis` maps (currently `{}` at `packers.rs:75`, `:965`, `:1058`) from the store, keyed by bare shortcode → URL. Keystone expects bare-inner keys with URLs (`api-surface.md:72`).
- **Remote custom emoji is opt-in per user, default off** (§9.4). Details in WP8b below. With the setting off, remote custom emoji render as their `:name@host:` text, and no remote emoji URLs are packed.

### WP8b — Remote custom emoji (per-user opt-in, default off)

> **[LANDED — account.rs:212,477 (show_remote_emojis, default false); domain/emoji.rs:75 (RemoteEmoji)]** Toggle-enqueues-fetch and default-off packing not re-verified.


Decided in §9.4. The user chooses; there is no instance-wide override in this batch.

- **Ingest metadata only.** When a remote note or actor arrives with AP `Emoji` tags, store `RemoteEmoji { host, shortcode, remote_url, first_seen }`, unique on (host, lowercased shortcode). Cap the tags accepted per object (pick a number, state it in the log). **Fetch no image at ingest.**
- **Setting.** Per-account `showRemoteEmojis`, default `false`, set through `i/update`. Upstream has no such field, so mark it `DIVERGENCE` with the reason: the owner wants the user to choose. Read it back on `i` and `i/update`.
- **Off.** Packed `emojis` and `reactionEmojis` contain local emoji only. A remote custom reaction renders as its `:name@host:` text. No remote emoji URL is packed.
- **On.** Packed maps include the remote emoji keyed `name@host`, as Keystone expects (`/root/keystone/docs/api-surface.md:72`).
- **Image fetching is lazy and a job.** When the user turns the setting on, enqueue one fetch job per known remote emoji. Each job uses the SSRF-guarded fetcher, a size cap, and a `image/*` content-type check, then stores the bytes in the blob store. The toggle transaction stays bounded.
- **Privacy note for the owner.** Until the cached copy exists, the packed URL points at the remote host, so the user's client loads it directly. This is the trade-off for not proxying; record it in the log.
- **Turning it off** hides the emoji without deleting metadata or cached bytes.

**Acceptance.** Default off: no remote emoji URL in any packed map, and remote reactions render as text. Turning it on packs them. Turning it off removes them with no data loss. No image is fetched before the setting is turned on. Shortcodes are unique per host, case-insensitively.

### WP9 — Account self-service

> **[LANDED — mod.rs:6527 (i/change-password), :6580 (i/regenerate-token), :6622 (i/apps), :6677 (i/authorized-apps)]** `i/update-email` correctly deferred.


- `i/change-password` `{password, newPassword}`. Read upstream for the wrong-password error and whether it revokes other sessions. Mirror upstream.
- `i/regenerate-token` `{password}`. Rotates the **native** token. Read upstream for the response. Scoped app tokens are separate (§9.7).
- `i/apps` and `i/authorized-apps`. List app grants for the caller, cursor-paged.
- `i/revoke-token`: today `tokenId` is a no-op (`api-surface.md` and the audit). Make `tokenId` revoke the named token. Keep the no-param form Keystone uses (revoke the caller's token).
- `i/update-email`: **deferred** (depends on §9.2). Do not implement without an email decision.

### WP10 — Follower removal and remote lookup

> **[LANDED — mod.rs:6036 (following/invalidate), :3062 (users/search-by-username-and-host), :8213 (ap/get|ap/show)]**


- `following/invalidate` `{userId}`: remove a follower (upstream `following/invalidate.ts`). Emit a Reject/Undo to the remote follower if the follower is remote (match the existing unfollow federation path).
- `users/search-by-username-and-host` `{username, host?}`: a thin alias over `users/show`.
- `ap/get` `{uri}`: returns the AP object for a URI. Lower priority; implement if time allows, and list it as deferred otherwise.

### WP11 — Notifications: follow-request accepted

> **[LANDED — commands/follows.rs:98-125 (accept emits FollowRequestAccepted); inbound/follow.rs:177]**


- Emit `followRequestAccepted` when a locked account accepts a request (`mod.rs:952` lists it as not emitted). Test the packing and the `includeTypes`/`excludeTypes` filter.
- Do not implement the other unemitted types (`app`, `achievementEarned`, `login`, `roleAssigned`). They are out (§8).

### WP12 — Link preview: `url-preview` (Keystone-requested, first-party)

> **[LANDED — mod.rs:8371 (url-preview, rate-limited)]** SSRF guard, auth-required, and cache caps not re-verified here.


Keystone asks for this explicitly (`api-surface.md:99`). It is not a Misskey endpoint.

- Input `{url}`. Output `{title?, description?, siteName?, image?, icon?}` (Keystone's shape, `api-surface.md:99`).
- Fetch with the §2 rule 6 SSRF guard, timeouts, redirect limit, and size cap. Parse OpenGraph and `<title>` only. Do not follow arbitrary content types.
- **Auth required** (§9.3). Anonymous link-preview endpoints are an open proxy.
- Cache results in process memory with a size and TTL cap. The cache is not state and must not go in the store, so it does not affect the FDB design.
- The Keystone side swaps its client fetcher to this endpoint (§10). Do not change Keystone from here.

**Acceptance:** private, loopback, and link-local addresses rejected; oversized and slow responses cut off; redirects capped; an unauthenticated call rejected.

### WP13 — Drive extras

> **[LANDED — mod.rs:8017 (upload-from-url), :8064 (find-by-hash)]** `folderId` still accepted and ignored (`[OPEN]` per §9.8).


- `drive/files/upload-from-url` `{url, folderId?, force?, isSensitive?, comment?, name?}`. Reuse `src/media/ingest.rs` and the §2 rule 6 fetcher. Enforce the existing media size limit. Do not add a second ingest path.
- `drive/files/find-by-hash` (upstream param name; read the file). Uses the existing SHA-256 keys in `src/media/storage.rs`.
- Drive folders are pending the owner decision in §9.8 (recommended in scope, §13.4). Until then, `folderId` is accepted and ignored, as today.

### WP14 — Self-delete

> **[LANDED — mod.rs:4685 (i/delete-account)]**


- `i/delete-account` (upstream: read `i/delete-account.ts` for the password requirement and error ids). Shares the machinery with `admin/delete-account` (§4 WP6).
- One transaction: tombstone the account (the handle stays reserved, per conformance C3), remove follows and requests, append the log (for admin actions), and enqueue a Delete(Person) to remote inboxes. Do not delete note bodies in-line; hand that to a job so the transaction stays bounded.
- Last in the batch because it touches the most subsystems.

---

### WP15 — Antennas

> **[LANDED — mod.rs:5268-5293 (antennas/list|show|create…); commands/antennas.rs:337 (AntennaMatch job); persistence/redb/mod.rs:114 (antenna_notes); mod.rs:5789-5796 (i/export-antennas enqueues a real export)]** `home` label and the no-auto-deactivation divergence not re-verified.


Antennas are rules that collect matching notes into a personal feed. Keystone already has the UI, and it gets an honest 404 today (`/root/keystone/docs/api-surface.md:96, 129`).

**Scope.** `antennas/list`, `show`, `create`, `update`, `delete`, `notes`, `remove-note`; real `i/export-antennas` and `i/import-antennas` (today they write `[]` and apply zero rows); and the `antenna` streaming channel.

**Model.** `Antenna { id, owner, name (≤100), src ∈ {home, all, users, users_blacklist, list}, user_list_id (for src=list), keywords: [[string]], exclude_keywords: [[string]], users: [acct], case_sensitive, with_replies, with_file, local_only, exclude_bots, exclude_notes_in_sensitive_channel, is_active, last_used_at }`. `exclude_notes_in_sensitive_channel` is accepted and stored, but has no effect while channels are out (§8).

**Matching (copy upstream, `misskey/packages/backend/src/core/AntennaService.ts:97–209`).** The checks run in this order:

1. Drop if `exclude_bots` and the author is a bot, or if `exclude_notes_in_sensitive_channel` and the note is in a sensitive channel (no channels, so this is a no-op).
2. Drop remote authors if `local_only`.
3. Drop every reply if `with_replies` is false, including self-replies.
4. Visibility: `specified` requires the owner to be the author or in `visibleUserIds`. `followers` requires the owner to follow the author.
5. Source: `all` matches everything; `users` requires the author's acct in `users`; `users_blacklist` requires it not to be; `list` requires membership in `user_list_id`.
6. `keywords`: the outer array is OR, the inner array is AND. Empty strings are stripped. Haystack is `text + "\n" + cw`. Case-insensitive unless `case_sensitive`, with substring matching.
7. `exclude_keywords` uses the same logic. Any matching group drops the note.
8. `with_file`: drop if there are no attachments.

**Divergence 1: `home`.** Upstream `home` is a `TODO` and matches everything (`AntennaService.ts:137–139`). Implement the label instead: the owner's own notes plus notes from accounts the owner follows. Mark `DIVERGENCE`. Owner decision §9.11 (default: follows).

**Divergence 2: no auto-deactivation in v1.** Upstream marks antennas inactive after 7 days unused (`CleanProcessorService.ts:51–56`). That update publishes no event, so in-memory copies keep matching until restart. Pinetree v1 does not deactivate. It caps the number of antennas per instance instead, and logs the cap.

**Architecture (no in-process cache, no Redis).** Upstream keeps active antennas in a per-node array and matches synchronously on note creation. That does not work across nodes in Phase 4. Instead:

- Note creation enqueues one bounded `antenna_match` job carrying the note id, in the same transaction as the note (§2 rule 3).
- The worker loads the active antennas, matches, and writes matches to an `antenna_notes` table: `(antenna, note id)`, ordered by id. In the same transaction, trim the feed to the newest 200 entries.
- Publish the `antenna` stream event after commit, through the `StreamingSink` port.
- The feed read is a range scan on `antenna_notes`. Visibility, mute, and block filters run at read time (`FeedFilter`, §0A of the parity plan).
- The `list` source checks membership through the membership index (WP16). It does not join memberships on every note.

**Limits.** `antennaLimit` defaults to 5 (`RoleService.ts:99`). `packers.rs:628` already advertises 5; make the enforced limit and the advertised one the same value.

**Errors.** `NO_SUCH_USER_LIST`, `TOO_MANY_ANTENNAS`, `EMPTY_KEYWORD`. Copy the ids from `endpoints/antennas/create.ts:96–105` (read it before implementing).

**Import and export.** Export writes the caller's antennas as JSON. Import validates each row with the same rules as `create`, skips empty-keyword rows the way upstream does, and is rate-limited 1 per hour (`ImportAntennasProcessorService.ts:21–109`). Read the upstream files before implementing.

**Acceptance.** `src=all` with no keywords matches every local note. Keyword AND and OR tests cover the examples in upstream's behavior. `with_replies=false` drops replies. `local_only` drops remote notes. `specified` and `followers` visibility respected. `src=list` matches members only. The feed is newest first and capped at 200. The `antenna` stream requires the owner. Deleting an antenna stops its matches.

### WP16 — User lists

> **[LANDED — mod.rs:3152-3488 (users/lists/*), notes/user-list-timeline; userList streaming]** `create-from-public` `[OPEN]`. Remote-member system-actor Follow `[OPEN]` (§9.12). The notification `list` config is not re-verified.


Lists are named sets of accounts with their own timeline. Keystone's antenna list source depends on them (`api-surface.md:95–96, 115–117`).

**Scope.** `users/lists/create`, `update`, `delete`, `list`, `show`, `push`, `pull`, `get-memberships`, `update-membership`, `favorite`, `unfavorite`; `notes/user-list-timeline`; `i/export-user-lists` and `i/import-user-lists` (CSV); the `userList` streaming channel; list-based notification receive config. `create-from-public` is deferred (§8).

**Model.** `UserList { id, owner, name (≤128), is_public }`. `UserListMembership { list, member, with_replies }`, unique on (list, member). Read `UserList.ts` and `UserListMembership.ts` before implementing.

**Limits.** `userListLimit` defaults to 10 per user (`RoleService.ts:104`). Pinetree currently advertises 5 at `packers.rs:633`; change it to the enforced value. The per-list member cap defaults to 50 (`RoleService.ts:105`, `TOO_MANY_USERS`). Confirm which limit that name refers to by reading `RoleService.ts` before implementing.

**Visibility.** A public list is readable by anyone. A private list is readable only by its owner. `users/lists/list` refuses remote users.

**Divergence 3: timeline is read-time (pull), not write-time (push).** Upstream fans a note out to each list containing the author at write time (`NoteCreateService.ts:1139–1156`), and adding a member does not backfill (`UserListService.ts:97–121`). Pinetree reads at request time: merge the members' notes ordered by id, with a bounded member count. Reasons: no write amplification, adding a member backfills their history, and it avoids unbounded fan-out in FDB transactions. Upstream has a pure database path with the same shape (`notes/user-list-timeline.ts:148–250`), so this is a known mode, not new behavior. Mark `DIVERGENCE`.

**Reply rule.** Apply each membership's `with_replies` the way upstream does. Read the exact rule at `NoteCreateService.ts:1139–1156` before implementing. Apply the same mute, block, and visibility filters as the home timeline.

**Remote members.** Adding a remote member enqueues a Follow from a system actor (`UserListService.ts:116–120`). Without that Follow, remote members' notes never reach the instance, so the list silently shows nothing for them. Pinetree has no system actor yet. Owner decision §9.12; the default is to create one. Read upstream's removal path before deciding what happens to the Follow when the member leaves the last list.

**Streaming.** The `userList` channel requires a credential and an owner match. Upstream's channel uses `this.user!.id` without a check (`stream/channels/user-list.ts:58`), so an anonymous connect likely throws. Pinetree refuses it up front. Membership is re-read on each event, not polled every 5 s. Events are `userAdded` and `userRemoved`.

**Notification receive config.** Upstream's `notificationRecieveConfig` accepts `{type: 'list', userListId}`. Pinetree drops list configs at creation (`application/commands/notifications.rs:64–66`, parity §11). Implement them now that lists exist. Pinetree already validates the list shape (`domain/account.rs:336–368`).

**Acceptance.** Push and pull update membership and the list timeline. Limits enforced. Public and private visibility respected. The timeline includes members' notes with the reply rule applied, and mutes and blocks applied. A remote member's Follow is queued. Deleting a list removes its memberships and stops its antennas (WP15). The stream requires an owner. A list-based notification config delivers notifications from members only.

---

## 6. Documentation updates (required, same change set)

> **[OPEN]** Not audited in this pass. `phase-3-profile.md` §5 is still stale per `TODOS.md`.


1. **`docs/endpoint-manifest.md`** — a row or superseding note for every endpoint added or changed in §4–§5. Mark each status.
2. **`docs/phase-3-profile.md` §5** — the non-goal list is stale. It still lists `notes/reactions` (list), `notes/featured`, `users/featured-notes`, and `i/notifications-grouped`, all shipped. Add a superseding note, and move every item this batch implements out of the non-goal list. Keep items §8 keeps.
3. **`docs/application-ports.md`** — the new ports (`ModerationLogStore`, `InstanceSettingsStore`, `AbuseReportStore`, `InviteStore`, `EmojiStore`, favorites, and any others). Note that no defaults exist.
4. **`docs/persistence-conformance.md`** — groups H, I, J (§3.4), with the status line updated.
5. **`docs/misskey-parity-plan.md`** — add a §14 "Moderation and daily-use batch" with a per-WP status line, in the same style as §§0–13. Each line cites the commit and log.
6. **`docs/testing.md`** — the smoke-gate changes (§7).
7. **`docs/wiki/`** — `Feature-Matrix.md`, `Current-State.md`, `Roadmap.md`, `About.md`. These are stale against the parity plan. Fix them in this batch, since they will mislead the next reader.
8. **`AGENTS.md`** — "Current phase" and the test counts. The "107/107" in AGENTS.md is stale.
9. **`state/logs/YYYY-MM-DD-batch-m-<topic>.md`** — one dated log per session. Never rewrite past logs; supersede by reference. Record executed output, not expectations.

---

## 7. Smoke gate fix (do this after implementation, before the final test run)

> **[OPEN]** `scripts/smoke-all.sh:53` still reads `… cargo test --offline 2>&1 | tail -30`. The masking issue is open.


The smoke gate currently reports green when tests fail, so it cannot be trusted until this is fixed. Do this **after** the work above and **before** the final run.

1. **Step 1 exit code (`scripts/smoke-all.sh:53`).** The step runs `bash -c "cd … && cargo test --offline 2>&1 | tail -30"`. The step's status is the status of `tail`, which is 0 even when `cargo test` fails. Fix: remove the `| tail -30` from inside `bash -c`. The outer `step()` already tees the output and reads `PIPESTATUS[0]`, so it keeps the full log and the right exit code. Keep a `tail` for display if you want one, but let the exit status come from `cargo test`.
   - **Verify:** temporarily add a failing test, run `bash scripts/smoke-all.sh`, confirm `SMOKE_ALL_FAIL` and a non-zero exit, then remove the test and confirm the test is gone before the final run.
2. **Phase 2 skip (`scripts/smoke-all.sh:77–79`, `:92`).** A missing prerequisite currently prints `SKIP` and the gate still ends with `SMOKE_ALL_OK`. Change this so that a skip is a failure by default, with the reason printed. The explicit opt-out `SMOKE_ALL_SKIP_PHASE2=1` (already in the header at `:17–19`) prints `SMOKE_ALL_OK (phase2 skipped by request)`.
   - If the environment cannot run Phase 2 (no docker, no Node 24, no built Misskey or Sharkey), the implementer must say so in the log and set the opt-out explicitly. Do not leave the gate silently green.
3. **Update the header comment** at `scripts/smoke-all.sh:9–19` to match the new behavior.
4. **Extend `scripts/keystone-sweep.sh`** with the Keystone calls this batch adds (`url-preview`, `emojis`, `i/favorites`, `notes/state`, `users/search-by-username-and-host`). Update its check count (the header says 38) to match.
5. **Run the full gate:** `bash scripts/smoke-all.sh`. Paste the `RESULTS` block into the log.

---

## 8. Explicitly out of this batch

> **[DECISION — still binding]**


Each item stays a 404 or a documented non-goal. Record it in the manifest and profile §5 (§6).

- **Web push:** `sw/*`, `/sw.js`. Owner instruction.
- **Clips, channels, pages, drive folders, Reversi, chat, hashtags, `roleTimeline`, `serverStats`, `queueStats`.** Profile §5 non-goals for now. Keystone renders honest 404s. Each is pending an owner decision (§9.8), with a plain-language explanation of what it does in `docs/implementer-guide-batch-m.md` §13 (added after the owner reviews it). **Antennas and lists are no longer out of scope; see WP15 and WP16.**
- **`signin-flow` and the optional Misskey web UI path.** Needed only if the Misskey UI is exercised (§1.1).
- **Email:** SMTP, `enableEmail`, `i/update-email`, password reset, admin mail notifications (§9.2).
- **2FA, passkeys, webhooks, `i/claim-achievement`, achievements, flash, gallery, charts, stats, retention, promo, announcements.** No decision recorded; treated as non-goals.
- **Custom roles and policies** (`admin/roles/*`, `roles/*`, `canInvite`-style policies). Two built-in roles only (§3.1).
- **Translation** (`notes/translate`, Feature-Matrix ⛔).
- **Emoji bulk operations** (`admin/emoji/*-bulk`, `import-zip`, `copy`, `list-remote`). Remote custom emoji is in scope as an opt-in (WP8b).
- **Federation admin tools:** `admin/federation/*` beyond `blockedHosts`, `federation/*` stats, relays.
- **Unemitted notification types** other than `followRequestAccepted`.
- **Admin mail and webhook notifications** for reports.
- **Thread-muting** (parity plan §0A).

---

## 9. Owner decisions (defaults apply until changed)

> **[OPEN]** Owner decisions pending. The defaults in each WP apply.


1. **Who may create invites?** Default: admins and moderators only, through `admin/invite/create`. Option: allow all signed-in users within a cap (upstream `invite/create` with `canInvite`). The cap option needs a per-user limit store.
2. **Email.** Default: none in this batch. `enableEmail` stays `false`; password reset and email change are out. Option: implement an outbound email port with a no-op transport, so password reset can be added later without changing the API surface.
3. **`url-preview` authentication.** Default: credential required. Keystone calls it from a signed-in UI, so this costs nothing and avoids an open proxy.
4. **Remote custom emoji.** Decided: in scope, **off by default, and the user chooses to enable it** (per-account setting, WP8b). There is no instance-wide override in this batch.
5. **Forwarding abuse reports to remote instances (Flag).** Default: included as the last item of WP4. Drop it if it slows the batch.
6. **Self-delete (`i/delete-account`).** Default: included as WP14. It is the most involved item.
7. **Scoped app tokens vs. native token rotation.** `i/regenerate-token` rotates the native token only. Default: app grants keep working. Confirm before changing that.
8. **Clips, channels, pages, folders.** Default: stay non-goals until the owner decides, after reading the explanation in §13. **Antennas and lists** are decided in scope (WP15, WP16) and need no further decision.
9. **Admin creation.** Default: the CLI `set-role` is the only way to make the first admin. Option: a bootstrap token. Keep the CLI.
10. **Drive folder name limit.** Upstream accepts 200 characters on create and update, but the column is `varchar(128)` (§13.4). Default: 128, enforced in the domain, with a validation error above it. Decide before implementing folders, and only if §9.8 puts folders in scope.
11. **Antenna `home` source.** Upstream's `home` matches everything, which is a TODO (`AntennaService.ts:137–139`). Default: the owner's own notes plus notes from accounts the owner follows, which is what the label promises (WP15, divergence 1).
12. **Remote list members.** Default: create a system actor, and queue a Follow when a remote account is added to a list, as upstream does (WP16). The owner should confirm this, since it adds an outbound follow that a user did not directly request.

---

## 10. Cross-repo follow-ups (not pinetree's to make)

> **[OPEN]** Owned by Keystone and other repos, not audited here.


Report these to the Keystone side. Do not edit the Keystone repo from here.

1. `api-surface.md:128` says pinetree `drive/files` is 404. It is implemented (`mod.rs:3757`). Update the Pinetree column.
2. `api-surface.md:134` says pinetree streaming is "not implemented". Phase 3 shipped it. Update the column, and re-verify the Keystone streaming client against pinetree.
3. After WP12 lands, Keystone swaps its link-preview fetcher to `url-preview`. Until then, its client-side OpenGraph fallback still works.
4. After WP8 lands, the Keystone picker's Custom section should render on pinetree. Keystone should re-verify.
5. `api-surface.md:78` says `notes/search` is policy-dependent on these servers. Pinetree's bounded full-scan search (`queries.rs:948–1001`) is a known limitation to document, not a reason to hide it.
6. **Notification config is a Keystone bug on both backends.** `Notifications.tsx:166–199` sends `notificationRecieveConfig` as a flat string map. Upstream requires objects (`{type}` or `{type: 'list', userListId}`), and pinetree rejects the flat form with `INVALID_PARAM` (`api/misskey/endpoints/mod.rs:5124–5127`). The save fails today. Keystone must send the object form. Its list option (`Notifications.tsx:335`) also needs a list picker once WP16 lands.
7. **Keystone's list source stays hidden until WP16 lands,** then unhides the `list` option in `Antennas.tsx` and the notification list picker. Update `api-surface.md:95–96, 115–117` at the same time.
8. **Antennas return real data after WP15.** Keystone's honest-404 state (`api-surface.md:129`) becomes the normal path. The `antenna` channel is then real on pinetree too, so its streaming note (`api-surface.md:134`) also needs updating.

---

## 11. Known risks the implementer should watch

> **[PLAN HISTORY]**


- **Two places can enforce the same rule.** Suspension and blocked-host checks are already in several read and inbound paths. Add the setter and the new check in the existing place. Do not add a third copy.
- **`admin/accounts/create` is a live hole** until WP2 lands. Treat it as the first thing to fix in WP2, even though it appears later in the batch.
- **Dedup rows for dropped activities.** A blocked-host or suspended-author activity must not leave a dedup record. Otherwise unblocking does not let the activity through, and the record is the thing that forever suppresses it.
- **Delivery-time checks, not enqueue-time.** Blocked hosts and suspended authors must be checked when the job runs.
- **Upstream code and id are the contract.** If you cannot find an upstream error code, read the upstream file again before inventing one.
- **Transaction size.** Account delete and any "delete all of X" path must hand bulk work to jobs. Do not write it inline.
- **Antenna matching cost.** Matching runs against every active antenna for every new note. Cap the number of antennas per instance, and record the cap in the log. Phase 4 will need an index on `src` and `owner` before the cap is raised.
- **Remote list members create outbound follows.** This is the one list feature that touches federation. Test it against a real remote before calling WP16 done.
- **Two antenna behaviors differ from upstream on purpose** (`home` and no auto-deactivation). Record both as `DIVERGENCE` in the manifest so the next reader does not "fix" them back.

---

## 12. Suggested order of work

> **[PLAN HISTORY]**


1. §3 foundations (roles, log, settings, conformance skeletons for H/I/J).
2. WP1 — roles, guards, log, and the reject-quotes fix. It is a live bug, so it goes first.
3. WP2 — registration policy, invites, and the `admin/accounts/create` hole.
4. WP3 — suspension (with the stream close).
5. WP4 — reports.
6. WP5 — instance blocks.
7. WP6 — admin read views.
8. WP7–WP12 — daily-use items in Keystone's priority order.
9. WP13–WP14 — drive extras and self-delete.
10. WP15 — antennas (after WP7–WP14, since antennas and lists share the membership index and the feed machinery).
11. WP16 — lists. Lands before the list-based notification config and the Keystone list source.
12. §6 documentation.
13. §7 smoke gate fix, then the full `bash scripts/smoke-all.sh` run.
14. Log the session (`state/logs/`).

---

## 13. Remaining Misskey features: what they do, and the scope question

> **[OPEN]** Scope question still owner-pending.


Source: upstream Misskey `2026.10.1-alpha.0` (`misskey/packages/backend/src/`, read-only). Complexity is the rough backend cost for a Rust re-implementation. None of these four is federated: no ActivityPub object or activity exists for any of them, so none can break federation compatibility if left out.

### 13.1 Clips — "bookmark collections for notes"

> **[LANDED — application/commands/clips.rs; mod.rs:3877-3920]**


**What a user does.** Creates named collections of notes ("clips"). Adds a note to a clip from the note menu. Clips are private by default. A public clip appears on the owner's profile (Clips tab) and at `/clips/:id`. A note's page lists the public clips that contain it.

**Rules.** Name up to 100 characters; description up to 2048. Limits per role: 10 clips per user, 200 notes per clip. Adding a note bumps a counter on the note. Clips have a favorites list. There is a streaming channel that pushes `updated` and `deleted` events, not note content.

**Federation.** None. **Notifications.** None.

**Cost.** Three tables and a handful of endpoints (14 endpoint files, including an async export). **Complexity: low.** No dependency on timelines or fan-out.

**Keystone.** Not called (not in `api-surface.md`).

**Recommendation.** In scope, after Batch M, as a small self-contained batch. It is cheap, and a curation feature many users expect. It is not daily use, so it does not block the current batch.

### 13.2 Channels — "community timelines"

> **[DECISION — still binding]** Declared out (`endpoint-manifest.md` §5/§9). `channels/*` still 404 (`mod.rs:9122`).


**What a user does.** Creates a channel with a name, description, banner, color, and sensitive flag. Posts to it. Followers see channel posts in their home timeline. Channels have their own timeline page, can be favorited and muted (with optional expiry), and can pin notes.

**Rules that shape the design.**
- Posts into a channel are forced to `public` and `localOnly` (`core/NoteCreateService.ts:468–470`). Channel notes never federate.
- Channel posts are excluded from the local and global timelines (`endpoints/notes/local-timeline.ts:153`, `global-timeline.ts:74`).
- Channel creation is limited to 10 per hour per user, and archived channels reject posts.
- Follower fan-out is done in-process over all followers (`NoteCreateService.ts:1078–1089`). That is a problem for FDB's transaction limits. The implementer would have to design it as a bounded job, which is the reason the complexity is rated medium-high.

**Dependencies.** Drive files (banner), notes (pins, renotes), the home timeline (fan-out), and mute. Search and the hybrid timeline also change.

**Cost.** Four tables, three services, two streaming channels, plus changes to note creation, timelines, and search. **Complexity: medium-high.** The CRUD alone is medium; the timeline integration is what makes it expensive.

**Keystone.** Not called.

**Recommendation.** Out for now. Revisit only if channels are wanted as a product feature. The cost is in the timeline integration, and it lands on the FDB design that Phase 4 is about to start.

### 13.3 Pages — "long-form documents with embedded scripts"

> **[LANDED — application/commands/pages.rs; mod.rs:3944-4010]** Scripts are not a pinetree feature.


**What a user does.** Writes a long-form document from blocks (text, section, image, note embed, inputs) and can attach a small client-side script that makes the page interactive. Pages are viewed at `/@user/pages/<name>`. There is a Featured tab, a My tab, and a Liked tab. Pages can be liked and appear on the profile.

**Rules.** Title up to 256 characters; name unique per user. The visibility enum exists, but the code hardcodes `public` and no endpoint sets anything else. The script field is up to 16 KB. Creation is limited to 10 per hour. There is no per-user page count cap that I found.

**Federation.** None. Misskey Pages are not ActivityPub objects. (An inbound AP `Page` type does exist, but it is an unrelated note-like type.)

**Cost.** Backend: two tables and about 11 endpoint files; the backend mostly stores and returns JSON. **Complexity: low-medium on the backend.** The real effort is the frontend: the block editor and the script interpreter. The script runs client-side, so a backend that does not render pages gets no benefit from the feature.

**Keystone.** Not called. Keystone has no page editor or renderer.

**Recommendation.** Out. Storing pages without the editor and renderer produces data that no UI can show. Revisit only with a Keystone page editor in scope.

### 13.4 Drive folders — "folders inside Drive"

> **[OPEN]** `folderId` rejected or ignored; folders not implemented.


**What a user does.** Creates folders inside their Drive, renames, moves, and deletes them. Deleting a folder is refused unless it is empty. Folders nest without a depth limit, with a cycle check on move. Files have an optional folder. There is a default upload folder setting. Folders are private to their owner.

**Rules.** Create is limited to 10 per hour per user.

**Data bug to fix if this is built.** The create and update endpoints accept names up to 200 characters (`endpoints/drive/folders/create.ts:46`, `update.ts:52`), but the database column is `varchar(128)` (`models/DriveFolder.ts:15–18`). A longer name would likely fail at the database. I did not test this. Whichever backend ships folders must pick one limit and enforce it in the domain.

**Federation.** None.

**Streaming.** One `drive` channel that forwards `folderCreated`, `folderUpdated`, and `folderDeleted`. Pinetree already has a `drive` streaming decision, so this is one more event type on the same channel.

**Cost.** One table, a few endpoints, and an optional `folderId` on the file endpoints. **Complexity: low.**

**Keystone.** Its `api-surface.md` lists drive folder navigation as deferred, and its `drive/files` call sends `folderId` as optional (`api-surface.md:92`). Keystone is ready for folders whenever pinetree provides them.

**Recommendation.** In scope as a low-cost addition, in the same batch as the drive extras (WP13). It is the basic way to organize files, and Keystone already accepts the parameter. Decide the name limit before implementing (§9.10).

### 13.5 Summary for the owner

> **[OPEN]** Owner summary; still pending.


| Feature | Federated | Cost | Keystone needs it | Recommendation |
|---|---|---|---|---|
| Clips | No | Low | No | In scope, after Batch M |
| Channels | No | Medium-high (timeline integration) | No | Out for now |
| Pages | No | Low backend, high frontend | No | Out |
| Drive folders | No | Low | Deferred, and the parameter is already accepted | In scope with WP13 |
