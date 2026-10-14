# 2026-10-14 — Phase 0 freeze (domain types, manifest, harness, freeze)

Goal: finish Phase 0 — domain types, endpoint/streaming manifest, executed proof, freeze review.

What changed:
- Added `docs/domain-types.md` (now **Frozen**): Account/Note/Follow/Reaction/Media must-carry field sets with bounds, grounded in Misskey `models/*.ts` (all key claims re-verified: `id()` varchar(32), `(usernameLower,host)` + `(followerId,followeeId)` + `(userId,noteId)` unique indexes, `noteVisibilities`/`noteReactionAcceptances` enums, note uri/url null-when-local). Open decisions resolved during the session: text API-max 3000 (`const.ts:6`; storage carries full), media cap = instance config (`MAX_FILE_SIZE_EXCEEDED` shape), profile subset (description/location/birthday join Account; `fields[]`/decorations deferred to Phase 3).
- Added `docs/endpoint-manifest.md` (now **Frozen**): initial manifest — transport/auth conventions, MiAuth + legacy auth endpoints (`ApiServerService.ts:157` check route verified), account/note/follow/timeline endpoints with params/limits/errors (notes/create paramDef re-read directly, lines 129-214), fixed response-shape sets, streaming subset (5 channels + note capture + 32/1536 limits), rate-limit values, documented `UNSUPPORTED_*` responses for everything else.
- Fixed `docs/application-ports.md` residue: `visible_users` row + open decision #3 now reflect the visibility-policy resolution.
- Built throwaway harness `/tmp/opencode/phase0-harness` (std-only, not in repo): ports over in-memory maps with commit-point fault injection. `cargo test`: **15/15 green** (A1–A4, B1/B3/B4/B6, C1/C3 incl. 8-thread handle race, D1/D3 incl. 8-thread dedup race, E1 three-call-site agreement matrix, E3, exit-box-2 acceptance). One test bug of mine (blanket anon assertion) caught and fixed by the suite itself — recorded as evidence the suite bites.
- `docs/persistence-conformance.md` §9 rewritten with the green run + honest scope (interface viability, not a backend; redb = first real run, Phase 1).
- `docs/fdb-verification.md` §5 updated: cargo 1.98.1 present, but still no `libfdb_c`, no server/cluster, and no network to fetch them — live probe stays blocked with unblock sequence.
- Froze all six contract docs; `docs/README.md` index → Frozen; `docs/phase-0-exit-criteria.md` — both exit boxes + six deliverable boxes checked, FDB box explicitly unchecked; `AGENTS.md` rolled to Phase 1 (single-node track unblocked, Phase 4 gated on FDB probe).

Evidence: `cargo test --offline` output (15 passed, 0 failed) this session; `cargo --version` 1.98.1; lib/cluster/network absence verified by direct checks. Reference line citations re-read, not probe-recycled (probes: entities, endpoints, streaming surveys + local verification).

Open questions / next step: Phase 1 — scaffold the Rust crate per frozen contracts, redb adapter first (conformance green run against real storage). FDB probe the moment infra exists.
