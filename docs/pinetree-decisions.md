# Pinetree decisions (2026-10-20, from fork-diff synthesis)

## DVD — steal features, not dependencies
- DVD ML/LLM (recommendations, LlmQuality translation, lingua langdetect, spam/CSAM) is cool and worth stealing.
- AI models must NOT be default-or-only path. Heuristic/structural fallbacks stay first-class (quality scoring, translate providers, langdetect, interest vectors). External deps (Milvus, vLLM/Qwen, Python sidecar) behind flags, degradable, never 500s.

## TransFem — moderation yes, extras no
- Take: signup approvals, silence/nsfw/cw/reject-quotes ops, edit/versions/schedule (federate Updates — decided second pass), bubble/following timelines, noindex, shared-access, imports pipeline shape (modified source list — decided).
- Skip: Mastodon API server (second product, defer), ListenBrainz (no demand).
- Likes: pinetree `like` = sharkey `like` = mastodon `favorite` — a federated lightweight endorsement, SEPARATE from emoji reactions.
  - Misskey `favorites` = bookmarks (local-only, already implemented in pinetree). Do NOT conflate. Keep both: `like` federates via Like activity; `favorite` never leaves the instance.

## Lemmy — native, not bolted-on
- DECIDED 2026-10-20: communities = optional extension + minimal core seal. Core: nullable community link + `restrictedTo` deny-by-default (no timeline/search/preview/relay/fetch without proof). Extension: community/member tables, policy service, Group federation, backfill, API/UI. Full plan: community-planning.md.
- Catodon forum support is reference only (markers, unwraps, /api/v3 shim, gates). Pinetree designs natively per community-planning.md.
- Needs implementation pass for exact DDL + policy code (planning done, code TBD).

## Events — steal Catodon FEP-8a8e outright
- Take `NoteEvent` sidecar (1:1 CASCADE), `EventRsvp` unique-pair, `EventInvitation` PK-pair, dual-dialect RSVP (Join/Leave + Accept-family), `catodon:inviteTo` DM fallback, visitor-gated `.ics` (account feed + single note), `events/{upcoming,rsvp,unrsvp,attendees,invitees,invite}` shape, `MkEventEditor`/`SkNoteEvent` UX.
- Keep public-invite (≤20/call, once-per-person, local-only never federates) semantics.

## New calls (2026-10-20, second pass)
- Messages hidden from timelines: follow Catodon (#647). Specified-visibility notes that aren't events stay out of home/local/global/custom feeds, profile notes, and live streams (owner sees own on profile via exempt). Invite-only event posts are NOT messages and still list.
- Note edits + history federated: adopt Sharkey/Catodon `Update(Note)` send + accept (with `NoteEdit` rows + `notes/edit` + `notes/versions`), NOT upstream-Misskey revert. Resolves prior open question.
- Cat mode copied, split in two: `be a cat` (isCat, cosmetic/nyaize baseline) separate from `speak as cat` (speakAsCat, Sharkey-only speech transform). Two toggles, independent.
- RFC 9421: implement (verify-if-present, permissive-first with logging, then tighten). Covers `Signature-Input` + `Content-Digest` alongside legacy.
- Imports (modified flow, NOT Sharkey as-is): must support Misskey, Pixelfed, Mastodon, Sharkey, Catodon now; Lemmy later. Sharkey job-family + policy-gate shape kept, parsers per source written/adjusted (Pixelfed + Catodon + Sharkey + Lemmy are new vs Sharkey's six; Akkoma/Firefish explicitly out unless demanded).
- Join queue: add `approvalRequiredForSignup` + `approvalRequiredEmailDomains` + approve/decline endpoints + approvals page + join reasons.
- Day-to-day user ops: copy TransFem one-click set (cw-note/user/instance, nsfw on/off, silence on/off, rejectQuotes, suspend, delete) + DVD extensions (spam-log search/restore, CSAM quarantine/resolve, purge-blocked).
- Instance plumbing: copy too (set-root, restart-migration, roles clone + annotate-condition, gen-vapid-keys, shared-access grants + dialog, purge-user-content script semantics).

## Open questions
- [x] Native community model design: PLANNED (see community-planning.md; extension + core seal decided). Implementation (DDL + policy code) TBD.
- [x] Note-edit `Update` federation: DECIDED — federate (see second pass). Upstream-Misskey revert rejected.
- [ ] Translate pipeline composition (provider picker + LLM quality gate composed, not stacked) + single langdetect (lingua) decision.
- [ ] Rec engine MVP scope (pgvector + heuristics first, Milvus/LLM flagged later).
