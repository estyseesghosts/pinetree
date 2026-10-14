# 2026-10-14 — Phase 1 gaps settled & manifest pinned to Misskey 2026.10

Goal: Resolve the 4 pre-Phase-1D gaps listed in `docs/phase-1-plan.md` against the local Misskey `2026.10.1-alpha.0` checkout (`./misskey/`) via `finder` inspection before scaffolding and implementing slices A–E.

What changed:
- Verified against `misskey/` (2026.10.1-alpha.0) via `finder`:
  1. `notes/reactions/create.ts:19-74` and `notes/reactions/delete.ts:18-46`: `write:reactions` permission, returns `204 No Content`, errors `NO_SUCH_NOTE`, `ALREADY_REACTED`, `NOT_REACTED`, `YOU_HAVE_BEEN_BLOCKED`, `CANNOT_REACT_TO_RENOTE`; `delete` rate limit 60/h with 3s min interval; `reactionAcceptance` mismatch coerces to fallback `❤️` (`ReactionService.ts:127,149`).
  2. Account creation: `SignupApiService.ts:249-257` (`POST /signup`) and `admin/accounts/create.ts:103-116` return `MeDetailed` (with secrets) + `{ token }`. Added `POST /signup`, `POST /api/signup`, `POST /api/admin/accounts/create`, `POST /signin`, `POST /api/signin`, `GET/POST /miauth/:session` web approval flow, and CLI subcommand support.
  3. `drive/files/create.ts:17-123` and `FileServerService.ts:93-99`: `write:drive` permission, 120/h rate limit, multipart/form-data (`file`, `name`, `comment`, `isSensitive`, `force`, `folderId`, `i`), returns `DriveFile` (`self: true`), errors `INVALID_FILE_NAME`, `MAX_FILE_SIZE_EXCEEDED` (413), `UNALLOWED_FILE_TYPE`. Public URL served at `{public_url}/files/{id}` (`GET /files/:id`, `/files/:id/:name` → 301).
  4. Target version pinned to Misskey 2026.10 (`2026.10.1-alpha.0` checkout + `packages/misskey-js` 2026.10.1-alpha.0).
  5. Also verified exact `ApiCallService.ts:306-325` error envelope `{ error: { message, code, id, kind, info? } }` and `/miauth/:session/check` response `{ ok: true, token, user }` (`ApiServerService.ts:157-177`).
- Updated `docs/endpoint-manifest.md` and `docs/phase-1-plan.md` (status → Frozen) with these superseding additions.

Verification:
- Direct file:line inspection of `./misskey/packages/backend/src/` and `./misskey/packages/frontend/src/pages/miauth.vue` via `finder`.

Open questions:
- None. Proceeding with Slices A1–A4.
