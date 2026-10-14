# Phase 0 — exit criteria checklist

Source: `sns-server-proposal.md` §9, Phase 0. Freeze review: 2026-10-14.

**Exit:**
- [x] IDs and public URI rules are fixed and frozen in `docs/` (frozen: `docs/identity-rules.md` — `aidx` IDs, handle rules, actor/note/WebFinger URL shapes).
- [x] A note-creation use case atomically persists both a note and a durable job through the storage interface (frozen: `docs/application-ports.md` §5 + `docs/persistence-conformance.md` §8; green run: 15/15 in-memory harness, first real-backend run = Phase 1 redb).

**Deliverables:**
- [x] Initial Misskey endpoint/streaming manifest (frozen: `docs/endpoint-manifest.md`).
- [x] Core domain types (frozen: `docs/domain-types.md`).
- [x] Identity rules (frozen: `docs/identity-rules.md`).
- [x] Visibility policy (frozen: `docs/visibility-policy.md`).
- [x] Application ports (frozen: `docs/application-ports.md`).
- [x] Persistence conformance tests (frozen: `docs/persistence-conformance.md`; green on harness, redb run pending Phase 1).
- [x] FDB Rust client + operational requirements verified via small vertical-slice prototype — **executed 2026-10-14** (throwaway `/tmp/opencode/fdb-probe` against live 7.3.77: open/transact/read, atomic note+job pair incl. cross-process kill demo, p50 ~3–6 ms, value-limit 2103 boundary, lazy-handle/1515/2008 failure surface; `docs/fdb-verification.md` §6). Mid-run loss/reconnect posture deferred to Phase 4 adapter tests.

Phase 0 is fully exited. Phase 1 proceeds on the redb track; Phase 4 is unblocked.
