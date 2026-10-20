# Handoff: Sharkey `notes/schedule/*` shim (slip in before Batch N closes)

## Why

Pinetree's scheduling copies Misskey's model (draft + `scheduledAt`/`isActuallyScheduled` → delayed publish job). Sharkey instead has a dedicated `notes/schedule/create|delete|list` family over a `note_schedule` row. Same capability, different shape — so Sharkey clients calling `notes/schedule/*` get a 404 today. This handoff adds the Sharkey family as a **thin alias over the existing draft machinery**: no new storage, no new tables, no new job kinds, no new ports. Do this after your current workstream, before the final smoke — it must not disturb in-flight work.

## What exists (do not rebuild)

- Drafts: `notes/drafts/create|list|update|delete|count` (`src/api/misskey/endpoints/mod.rs:5748+`, `parse_draft_create` ~line 681, `src/application/commands/drafts.rs`). `scheduled_at_ms` is ms-epoch; `is_actually_scheduled=true` + future `scheduledAt` enqueues `JobKind::PostScheduledDraft` with not-before; update/delete clear and reschedule (`clearSchedule` scan). Cap: `scheduled_note_limit` (`count_scheduled_drafts`); errors `TOO_MANY_SCHEDULED_NOTES` / `SCHEDULED_AT_REQUIRED` / `SCHEDULED_AT_MUST_BE_IN_FUTURE`.
- Upstream reference: `sharkey/packages/backend/src/server/api/endpoints/notes/schedule/{create,delete,list}.ts`, model `sharkey/packages/backend/src/models/NoteSchedule.ts`. Read all three files fully before coding — the mapping below is a guide, the upstream file wins on any detail.

## Build (three endpoint arms, all thin)

### 1. `notes/schedule/create` → actually-scheduled draft

Accept Sharkey's params: `visibility` (default `public`), `visibleUserIds?`, `cw?`, `reactionAcceptance?`, `noExtractMentions?`/`noExtractHashtags?`/`noExtractEmojis?`, `replyId?`, `renoteId?`, `text?`, `fileIds?`/`mediaIds?` (accept both, same 1–16 ids), `poll?` (`choices[2..10]`, `multiple`, `expiresAt`/`expiredAfter`), `scheduleNote: {scheduledAt!}` (required). Enforce the `anyOf` content rule (one of text/renoteId/fileIds/mediaIds/poll).

- Translate to `CreateDraftInput` with `is_actually_scheduled=true`, `scheduled_at_ms = scheduleNote.scheduledAt`. **Confirm the unit** (ms epoch) against the tail of upstream `create.ts` and one client call site — do not guess; if the tree is ambiguous, say so in the log.
- Cap: count the caller's actually-scheduled drafts against `scheduled_note_limit`; on overflow return Sharkey's `SCHEDULE_NOTE_MAX` (`168707c3-e7da-4031-989e-f42aa3a274b2`) instead of our `TOO_MANY_SCHEDULED_NOTES`.
- Up-front validation (Sharkey validates at schedule time; our drafts validate at publish — the shim must do both, returning **Sharkey's error ids verbatim**): `NO_SUCH_RENOTE_TARGET` (`b5c90186-…`), `CANNOT_RENOTE_TO_A_PURE_RENOTE` (`fd4cc33e-…`), `CANNOT_RENOTE_DUE_TO_VISIBILITY` (`be9529e9-…`), `NO_SUCH_REPLY_TARGET` (`749ee0f6-…`), `CANNOT_REPLY_TO_A_PURE_RENOTE` (`3ac74a84-…`), `CANNOT_CREATE_ALREADY_EXPIRED_POLL` (`04da457d-…`), `CANNOT_CREATE_ALREADY_EXPIRED_SCHEDULE` (`8a9bfb90-…`, scheduledAt must be in the future), `NO_SUCH_FILE` (`b6992544-…`), `YOU_HAVE_BEEN_BLOCKED` (`b390d7e1-…`). No channels exist: there is no `channelId` in the upstream paramDef, so nothing to reject — but keep the `CANNOT_RENOTE_OUTSIDE_OF_CHANNEL` id out of scope and note why.
- `fileIds`+`mediaIds` both present: accept (upstream allows both keys); prefer `fileIds`, fall back to `mediaIds`. Record the choice.
- Response: mirror the tail of upstream `create.ts` (packed schedule-note shape) built from the created draft. Auth/permission/rate-limit: credential required; reuse the `notes/drafts/create` rate bucket and document it (upstream kind is `write:notes-schedule`; we do not mint a separate kind — one line in the manifest).

### 2. `notes/schedule/list` → scheduled drafts, Sharkey shape

Params `sinceId?`/`untilId?`/`limit` (1–100, default 10). Query the caller's drafts filtered to `is_actually_scheduled`, newest-first, cursor-paged. Pack each row into the upstream shape: `{id, userId, scheduledAt: ISO, note: {text (default `''`), cw, fileIds (+ packed `files`), visibility (default `public`), visibleUsers (packed UserLite), reactionAcceptance (default null), user (packed caller), createdAt: scheduledAt-ISO, isSchedule: true, id, renote/reply (packed or null), renoteId/replyId, poll?}}`. Reuse existing packers; resolve renote/reply targets the same way `notes/show` does (missing target → null, never an error).

### 3. `notes/schedule/delete` → delete the scheduled draft

Param `noteId!` (the schedule entry id = our draft id). Not found → Sharkey's `NO_SUCH_NOTE` (`a58056ba-…` — note: different id from our drafts `NO_SUCH_NOTE`; use theirs here). Found but another user's → `PERMISSION_DENIED` (`c0da2fed-…`, HTTP 403). Delete via the existing draft-delete path so the publish job is cleared/rescheduled exactly once (no double-publish, no orphan job). Return void.

## Rules

- No new tables, no new `JobKind`, no new ports. If you find yourself adding storage, stop — the mapping is wrong.
- Publish path untouched: when the time comes the existing `PostScheduledDraft` job fires and the note is created through the normal path (mentions/hashtags/emoji extraction flags honored at publish as drafts already do).
- Upstream error ids verbatim (listed above); read the file if any id here looks off.
- Contract updates: manifest superseding note (§-next, "Sharkey scheduling shim"), `docs/phase-3-profile.md` §5 tweak if it claims scheduling is Misskey-only, parity-plan §14 line. Mark the `write:notes-schedule` → shared-bucket decision as DIVERGENCE with the one-line reason (no second rate-kind for an alias).
- Keystone: none of this is Keystone-facing (it uses drafts). No cross-repo note needed.

## Gate (small, same discipline)

- `cargo test --offline` green; add endpoint tests: create-mapping (incl. each up-front error id + `SCHEDULE_NOTE_MAX` + expired-schedule + both file-key forms), list shape (ISO dates, defaults, `isSchedule: true`), delete (unknown id, foreign id → 403, job cleared — schedule then delete then advance clock, no note appears), publish still fires for a shim-created entry end to end.
- Conformance: no new store, so no new conformance group — assert that in the log, don't invent one.
- Re-run `bash scripts/keystone-sweep.sh` (41/0) as a no-regression check since the dispatcher gains arms.
- State log: append to the Batch N log stream (`state/logs/`), don't rewrite anything.

## Non-goals

- No `note_schedule` table, no `ScheduleNotePost` job, no channel support, no `CANNOT_RENOTE_OUTSIDE_OF_CHANNEL` enforcement, no separate rate-kind, no `notes/schedule/update` (upstream has none — edits go through `notes/drafts/update` on the underlying draft; document that one line in the manifest).
