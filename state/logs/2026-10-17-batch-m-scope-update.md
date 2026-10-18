# 2026-10-17 — Batch M scope update (antennas, lists, remote emoji, §13 explanations)

## Changed

- `docs/implementer-guide-batch-m.md` (still Draft; supersedes the scope in `state/logs/2026-10-17-implementer-guide-batch-m.md`):
  - **Antennas (WP15) and user lists (WP16) are in scope.** Both were profile §5 non-goals. The profile and endpoint manifest change when the work lands (§6 item 2).
  - **Remote custom emoji (WP8b) is in scope as a per-user opt-in, default off.** Metadata is ingested; images are fetched only after the user enables it.
  - **New §13** explains clips, channels, pages, and drive folders for the owner, with a recommendation for each. Clips and folders lean in scope; channels and pages lean out. Owner decision still open (§9.8).
  - **New owner decisions §9.10–9.12:** folder name limit (128 default), antenna `home` semantics (follows default), remote list members create a system actor and a Follow (default yes).
  - Keystone follow-ups (§10): the notification-config flat-map bug, the list source unhide, and the antenna 404 state.
  - Work order (§12) places antennas and lists after the daily-use items.

## Verified

- Upstream antenna and list behavior was read by the research explorers (`misskey/packages/backend/src/core/AntennaService.ts`, `core/UserListService.ts`, `endpoints/antennas/*`, `endpoints/users/lists/*`, `endpoints/notes/user-list-timeline.ts`). Citations in the guide come from that research. They were not re-read by the author of this log.
- Pinetree has no antenna or user-list dispatch arms (research grep of `src/`). `i/export-antennas` writes `[]`. `i/import-antennas` applies zero rows.
- Keystone's notification config bug was checked against `Notifications.tsx:166–199` and pinetree's `INVALID_PARAM` at `api/misskey/endpoints/mod.rs:5124–5127` by the research explorer.
- Clips, channels, pages, and folders behavior comes from an upstream read by an explorer (`misskey/packages/backend/src/`). Endpoint counts were taken from directory listings.

## Not verified / open

- No code changed and no tests run. This session edited docs only.
- Several upstream details were cited by the research but not re-read before writing; the guide tells the implementer to read them first (`RoleService.ts:104–105` for the list member cap, the notification-config code path, `ImportAntennasProcessorService.ts`, `UserListService.ts` removal path, `NoteCreateService.ts:1139–1156` reply rule).
- Two of the explorer's findings were not reproduced live: that the Keystone notification save fails on pinetree today, and that an anonymous `userList` connect throws upstream.
- The guide's §0 and §8 wording still says "Draft"; the owner decisions in §9.8 and §9.10–9.12 are open.

## Next

- Owner answers §9.8 (clips, channels, pages, folders), §9.10–9.12.
- Implementer starts at guide §12 step 1 once the owner signs off.
