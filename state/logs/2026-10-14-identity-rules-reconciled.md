# 2026-10-14 — identity rules reconciled with references

Goal: resolve the four TODOs in `docs/identity-rules.md` using the Misskey/Sharkey reference probes.

What changed:
- Rewrote `docs/identity-rules.md` (still **Draft**, pending freeze review): fixed ID scheme as `aidx` with full six-method reference table; handle rules (`^\w{1,20}$`, `usernameLower` uniqueness, `UsedUsername` no-reuse); full actor/note/WebFinger URL shapes; migration preserves tombstones; §7 Sharkey extras marked out of scope.
- Updated `docs/README.md` index row accordingly.

Evidence (all verified by local re-check, not just probe output):
- `grep ^id:` example configs → both `aidx` (misskey `.config/example.yml:296`, sharkey `:286`); `ls` confirms identical six-module id/ dirs in both checkouts.
- aid/aidx regexes, `localUsernameSchema` lines, `genLocalUserUri` (`{url}/users/{id}`), `genLocalNoteUri` (`{url}/notes/{id}`) all re-grepped and matching probe citations.
- Full line-level citations for routes, Person fields, WebFinger, signup enforcement recorded in the doc itself.

Open questions / next step: freeze review of identity rules (Phase 0 exit box 1 also needs note/media/inbox URL rules — media paths deferred to media contract). Exit checklist left unchecked; next contract candidate is application ports (storage interface), which exit box 2 depends on.
