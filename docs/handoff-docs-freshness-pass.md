# Handoff — docs freshness pass + human-readable wiki

Date: 2026-10-20. Parent owns this handoff; workers execute workstreams below.

## Goal

1. Every doc in `docs/` is fresh to the latest codebase: no stale claims, landed work marked landed, open work marked open with pointers.
2. A human-readable wiki in `docs/wiki/` explains all of this in simple, jargon-free words.

## Hard rules (AGENTS.md)

- **Docs only. Do NOT edit code.** No changes under `src/`, `tests/`, `scripts/`, `Cargo.*`. Read code freely to verify claims; cite `file:line`.
- Check `git status --short` before starting; respect uncommitted work; never `commit`/`stash`/`reset`/`checkout --`.
- Subagents share one worktree: disjoint file sets (assigned below). Shared files (`docs/TODOS.md`, `docs/README.md`, `docs/wiki/Home.md`) are owned by the PARENT for merging — workers PROPOSE patch snippets for shared files, they do not edit them.
- Frozen docs stay frozen: `domain-types.md`, `identity-rules.md`, `visibility-policy.md`, `application-ports.md`, `endpoint-manifest.md` (see `docs/README.md`). Correct them via dated addendum sections or superseding notes, never silent rewrites. Record superseding decisions in the state log.
- Never rewrite past state logs; append new dated logs to `state/logs/`. Mark doc `Status:` lines accurately (Draft/Frozen).
- Verify by execution where a doc makes a testable claim (run the test/suite, paste output in the log). Do not mark fresh on reasoning alone.

## Inputs (read first)

- `docs/inventory-pinetree-vs-misskey-2026-10-20.md` (code truth as of 2026-10-20)
- `docs/TODOS.md` (gap list, incl. "verify-then-close" items — those are your first targets)
- `state/logs/2026-10-20-todos-docs-sweep.md`, `state/logs/2026-10-20-todos-from-inventory.md`
- `docs/README.md` (contract index + status)

## Workstreams (disjoint file sets)

1. **API contracts** — files: `endpoint-manifest.md`, `phase-3-profile.md`, `misskey-parity-plan.md`, `sharkey-parity.md`, `catodon.md`. Verify every endpoint/channel/event claim against `src/api/**`, `src/streaming/**`. Known-stale (fix first): `updated`-not-emitted, `tokenId`-revoke no-op, grouped/featured/reactions listed out, `notes/search` offset/range, `server-info`, `reset-password`, timeline `withReplies`, moderator bypass, `Update(Note)` TODO arm, `create-from-public`. Propose TODOS triage for the rest.
2. **Domain/federation contracts** — files: `domain-types.md`, `application-ports.md`, `visibility-policy.md`, `identity-rules.md`, `activitypub-collections.md`, `quote-posts.md`, `replies.md`, `federated-collections.md`, `word-mutes.md`. Verify against `src/domain/**`, `src/application/**`, `src/activitypub/**`, `src/federation/**`. Mark slices A–C (replies), manual-approval gate (quotes), hide-followers fields, pending-URI machinery as landed or open. Frozen docs: addenda only.
3. **Plans + decisions** — files: `extension-plan.md`, `community-planning.md`, `pinetree-decisions.md`, `remote-media-cache-plan.md`, `implementer-guide-batch-m.md`, `keystone-clip-page-surface.md`, `persistence-conformance.md`, `phase-1-plan.md`, `phase-2-plan.md`, `phase2-5.md`, `phase2-6.md`. Add honest `Status:` + "still open / landed / superseded" headers per section. Do not rewrite history; annotate.
4. **Archive + index** — files: `audit-2026-10-16-phase0-to-now.md`, `fediverse-bench-*.md`, `federation-bench-*.md`, `handoff-*.md`, `phase-*-exit-criteria.md`, `testing.md`, `phase-2-interop-howto.md`, `fdb-verification.md`, `README.md` (propose snippet to parent). Mark each bench/handoff item landed/open/harness-only. Update exit-criteria and testing docs only where code proves a change.
5. **Wiki (human-readable)** — files: `docs/wiki/*` (may create new pages). Rewrite so a non-engineer can understand: what pinetree is, what works today, what's intentionally different from Misskey, what's missing and when it's planned, glossary of every jargon term used (federation, timeline, relay, etc. in plain words). No implementation code, no `file:line` dumps — link to the contract docs for detail. Keep Home/Current-State/Roadmap/Feature-Matrix accurate to workstreams 1–4 output.

## Exit criteria

- `docs/TODOS.md` "verify-then-close" section is empty (every item verified-closed or promoted to a real TODO).
- Every doc touched has an accurate `Status:` line and no claim contradicted by current code.
- Wiki reads clean for a non-technical reader (test: no unexplained jargon; each page links to detail docs).
- One dated state log appended describing what changed, what was verified (with command output), and open questions.
- `git status --short` shows only intended docs/wiki edits.
