# 2026-10-20 — Batch N gaps (WS1–WS8) + Sharkey scheduling shim

Implements `docs/handoff-batch-n-gaps.md` workstreams 1–8 and the follow-up
`docs/handoff-sharkey-scheduling-shim.md`. All five investigation subagents
(A–E) ran first; findings synthesized into one build order, then implemented
serially per the handoff (domain → ports → redb → op → route + packer →
federation → conformance → manifest → smoke).

## Executed evidence (the gate)

- `cargo test --offline`: 449 lib + 104 conformance + 29 integration
  (1+1+2+2+2+1+1+5+15) = 582/582 green (recorded counts, not doc counts).
- `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK` (phase2 SKIP — no Node 22
  in this environment, recorded as environmental; the opt-out was NOT
  set, the skip prints explicitly).
- `bash scripts/keystone-sweep.sh` → `KEYSWEEP_OK`, 41 passed / 0 failed
  (includes the updated `antennas/list 200 empty` assertion; `userList`
  stays the `UNSUPPORTED_CHANNEL` probe).

## What landed (per workstream)

- **WS1 custom emoji + WP8b remote opt-in** (manifest §28): `emojis`
  (public, `{emojis}`), `emoji {name}`, `admin/emoji/add|update|delete|list`
  (moderator + scope, verbatim upstream ids incl. add-UUID `NO_SUCH_FILE`,
  rename-via-id+name `doNameUpdate` parity, exact-`:name:` + substring
  query). `EmojiStore` + redb `emojis`/`emojis_by_name`/`remote_emojis`;
  note maps keyed bare-shortcode → URL (+ `name@host` remotes when the
  viewer opted in). `showRemoteEmojis` via `i/update`, packed on
  `MeDetailed` (DIVERGENCE). Ingest = AP `Emoji`-tag metadata only
  (≤32/object, http(s) only); opt-in enqueues ≤200 `RemoteEmojiFetch` jobs
  (SSRF-guarded, 2 MiB, `image/*` gates, drive row cached). Conformance J2.
- **WS2 favorites + note state** (manifest §27): `notes/favorites/create|
  delete` (verbatim `ALREADY_FAVORITED`/`NOT_FAVORITED` UUIDs, create-gates-
  visibility asymmetry kept), `i/favorites`, `notes/state`
  (`{isFavorited, isMutedThread: false}`), `isFavorited` on packed notes,
  real `i/export-favorites` (upstream serialize shape). `FavoriteStore`
  + redb `favorites`. Conformance J1/J3.
- **WS3 notifications**: `followRequestAccepted` emitted on local accept +
  inbound `Accept(Follow)`; first-class filter value, plain `notification`
  stream hint. Remaining unemitted list recorded in §27 (`note`,
  `pollEnded`, `roleAssigned`, `chatRoomInvitationReceived`,
  `achievementEarned`, `login`, `createToken`, `app`, `test` — no
  subsystem to emit them).
- **WS4 antennas** (manifest §29): full stack — CRUD + `notes` +
  `remove-note`, `AntennaMatch` job enqueued in the note-creation txn
  (local + federated), worker match + `antenna_notes` capped at newest 200,
  post-commit `antenna` stream event + `antenna` channel (owner-scoped,
  read-time visibility/mute/block filters), real export/import (verbatim
  `exportedAntennaSchema` rules, empty-keyword skip, `src=list`+accts
  rewrite; import pre-check `count + batch >= limit` →
  `TOO_MANY_ANTENNAS` `600917d4-…`). DIVERGENCEs: `home` = own + followed
  notes; no auto-deactivation (cap 5 enforced = advertised); `src=list`
  refused (`NO_SUCH_USER_LIST`, WP16 pending). Conformance Group O (O1
  trim + cascade). `antennas/list` is now 200 (keystone sweep updated;
  `userList` stays the `UNSUPPORTED_CHANNEL` probe).
- **WS5 abuse reports** (manifest §24): the five routes wired
  (`report-abuse` answers the packed report; admin arms answer 204) +
  `Flag` delivery on forward. **Correction mid-session**: the
  investigation claimed upstream has no inbound Flag handler — wrong
  (verified `ApInboxService.flag` + `isFlag` dispatch live in the pin).
  So inbound `Flag` IS routed: first mentioned LOCAL user becomes the
  target, remote actor the reporter, `content` + JSON URIs the comment;
  remote-only/empty targets skip (upstream `skip`); atomic dedup + insert
  (`create_flag_report`, replays converge); blocked-host drop before any
  write; overlong comments truncate to 2048 (DIVERGENCE). `report_abuse`
  refusal narrowed to admin-only (upstream parity). Endpoint + inbox
  tests green.
- **WS6 admin remainder** (manifest §30 audit table):
  `admin/unset-user-avatar|banner` (moderator + refusal rule, log rows,
  no-op when unset), `admin/delete-all-files-of-a-user` (`requireAdmin`,
  pinetree-named log row with count), `admin/drive/files|show-file`
  (moderator, read-only; new required `MediaStore::list_all_media` port).
  Everything else classified out / needs-owner-decision in §30.
- **WS7 MiAuth** (manifest §33): 30-min approval window
  (`expires_at_ms`, deleted-on-read, re-armed on re-approve), GET never
  persists, submit refuses suspended approvers, `gen-token` requires
  `session` + `permission`. Kept lax (documented): no permission
  allowlist, no app registry (`NO_SUCH_APP` unemittable, verified late at
  `userkey`), dead legacy approve URL, no dedicated `gen-token` bucket.
- **WS8 server-side word mutes** (owner-approved DIVERGENCE;
  `docs/word-mutes.md`, manifest §31): Misskey-exact matcher
  (`src/domain/word_mute.rs`; haystack order, self/empty exemptions,
  case-sensitive AND-groups, `/pattern/flags` with `i/m/s/g/d/u/y`
  handling, transitive self→reply→renote); hard drops in `FeedFilter` on
  every read (timelines, threads, search, `users/notes`, `notes/show`,
  `users/reactions`, `antennas/notes`, streaming eligibility + capture);
  soft packs viewer-specific `isSoftMuted` (matched AND no own CW —
  Keystone badge rule); notifications never gated.
- **Sharkey scheduling shim** (manifest §32): `notes/schedule/create|
  list|delete` as a thin alias over drafts (no new storage/jobs/ports).
  Verified ms-epoch `scheduledAt`; `fileIds ?? mediaIds`; poll
  `expiredAfter` converts against the scheduled time, `expiresAt` wins
  when present (DIVERGENCE from the drafts wins rule); up-front
  pure-renote/visibility/poll-vs-schedule/cap checks with verbatim
  Sharkey ids (incl. per-endpoint UUIDs via dedicated constructors);
  packed schedule-note shape; delete clears the publish job exactly once;
  end-to-end publish verified.

## DIVERGENCE / behavior notes (all recorded in the manifest)

- `UserLite.emojis` stays `{}` (profile-emoji attachment not modeled;
  registry served via `emojis`/`notes` maps).
- `notes/create` `CANNOT_REPLY_TO_A_PURE_RENOTE` now carries the verbatim
  Sharkey UUID (`3ac74a84-…`) — the code previously emitted a generated
  UUID (improvement, same code string).
- `notes/schedule` reuses the drafts 300/hr bucket (no second rate-kind).
- Inbox emoji ingest runs after drop checks; actor upserts for blocked
  hosts pre-date this batch (noted, unchanged).

## Open / pending (not this batch)

- WP9 (self-service), WP10 (follower removal/remote lookup), WP13 drive
  extras (`upload-from-url`, `find-by-hash`), WP16 user lists
  (`src=list` refused until then), url-preview, signin-flow/OAuth/roles/
  email/2FA/webhooks (guide §8 outs).
- §7 smoke-gate fix + remaining doc counts (phase-3-profile §5 note,
  testing.md, AGENTS counts) still pending batch end — the gate above
  ran on the current scripts (phase2 SKIP is environmental, printed).
- Keystone cross-repo follow-ups: none new (soft `isSoftMuted` flag is
  additive; Keystone migrates its badge source at will — reported, not
  implemented here).
