# 2026-10-16 Misskey parity plan

What changed:
- Added `docs/misskey-parity-plan.md` (Status: Draft, backend only, no frontend).
- Covers: full `i/update` (29 missing fields), private followers/following, private reactions, Sharkey `rejectQuotes`, polls, drafts+scheduling, default emoji, pins/featured, edit vs delete-and-redraft, follower paging, notification grouping/filtering, timeline reply/block/mute rules + server CW, import/export. Build order §§0-13.
- Updated `docs/README.md` index with the new plan file.

Verified:
- `git diff --check` clean; `docs/misskey-parity-plan.md` starts with `Status: Draft`.
- No `src/`/`tests/` touched; `./misskey` `./sharkey` read-only.
- Rule recorded: match upstream Misskey params/errors/packs by default; Sharkey only for `rejectQuotes`/`notes/edit`; divergences marked.

Open questions:
- Confirm literal "replies from followed only" in home vs Misskey parity (self + replies-to-you).
- Confirm per-user default reaction emoji as extension vs per-note `reactionAcceptance` only.
- Confirm server-injected CW for soft-mutes (upstream is client-side).
- Confirm Sharkey `notes/edit` scope if real editing is wanted (needs federated `Update(Note)`).
