# 2026-10-16 Audit Phase0-to-Now

Full report: `docs/audit-2026-10-16-phase0-to-now.md` (8/8 sections filed).

What changed:
- Ran 8 parallel explore subagents: internals, API endpoints, federation inbound, federation outbound, storage, phase history, pinetree-vs-Misskey 2026.10.1-alpha.0, pinetree-vs-Sharkey 2025.4.7.
- Filed each result into docs audit with file:line evidence; flipped report Status Draft -> Complete.

Verified:
- `docs/audit-2026-10-16-phase0-to-now.md` contains all 8 Status: Filled markers (checked via read).
- No source code touched; reference checkouts ./misskey ./sharkey read-only.

Open questions:
- Phase 4 FDB adapter + cluster/* still 0-byte stubs, --mode=cluster refused; conformance Q/R redb-only.
- Known deviations kept: strict UNSUPPORTED_FIELD, public follow lists, tokenId revoke no-op, no blocks/mutes/polls/channels/folders/OAuth, opaque collection cursors, /proxy unimplemented.
