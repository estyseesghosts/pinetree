# 2026-10-15 — Global (federated) timeline

## What changed

Pinetree had home (`notes/timeline`), local (`notes/local-timeline`) and
hybrid timelines but no federated timeline. Added `notes/global-timeline`,
following Misskey's `notes/global-timeline.ts` (`misskey` checkout,
`packages/backend/src/server/api/endpoints/notes/global-timeline.ts`):

- `src/application/queries.rs`: new `AppServices::global_timeline`
  (optional viewer, cursor, `with_files`, `with_renotes`). Eligibility reuses
  the existing `is_eligible_for_federated_timeline` domain predicate
  (non-deleted, non-`local_only`, `visibility == public` — local and remote
  notes), then the shared `is_visible` read check. No reply filter:
  Misskey's global paramDef has no `withReplies`, so replies are included.
- `src/api/misskey/endpoints/mod.rs`: new `"notes/global-timeline"` arm.
  No auth required (authenticated callers still get viewer-aware
  visibility packing). Allowed params mirror Misskey's paramDef
  (`withFiles`, `withRenotes`, `limit`, `sinceId`, `untilId`, `sinceDate`,
  `untilDate`) plus `allowPartial` accepted-and-ignored like the other
  timeline endpoints. `withReplies` → 400 `UNSUPPORTED_FIELD` per the
  frozen §8 convention.
- Docs: `docs/endpoint-manifest.md` timelines table + superseding note
  (frozen file, changed via this logged decision);
  `docs/wiki/Current-State.md` endpoint list and known-gaps list.

Out of scope (unchanged): streaming is still unimplemented
(`src/streaming/*` are empty stubs, no `/streaming` route), so there is no
`globalTimeline` streaming channel yet — same as the other timeline
channels. No role-policy gate (`gtlAvailable`): pinetree has no role
policies; the endpoint is always available.

## Verification (no multi-server spin-up)

New test
`api::misskey::endpoints::tests::global_timeline_carries_remote_public_notes`
covers, in one in-process redb instance:

- local public / home / local-only / followers notes + a self-reply via
  `notes/create`; a remote public and a remote followers note via
  `handle_inbound_create_note` (the same op the inbox uses) for a cached
  remote actor;
- anonymous `notes/global-timeline` returns exactly
  `[remote-public, self-reply, local-public]` newest-first (home,
  local-only, followers excluded on both sides);
- `withReplies` → 400 `UNSUPPORTED_FIELD`; authenticated read matches the
  anonymous surface; `notes/local-timeline` still excludes the remote note;
- pure renote included by default, excluded with `withRenotes=false`;
  `withFiles=true` → empty (no attachments); `limit`/`untilId` pagination.

`cargo test`: 194 lib + 51 conformance + 2 crash_recovery + 1 client_smoke
+ 2 federation + 1 minrepro + 1 federation_interop(env-gated) — all green.

## Open questions

- None for the endpoint. The `globalTimeline` streaming channel arrives
  with the rest of Phase 3 streaming (needs a real `/streaming` route
  first).
