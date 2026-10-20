# docs/ — contract documents

Each contract file carries a `Status:` line at the top: `Draft` or `Frozen`.
`Frozen` means the Phase 0 exit review accepted it; frozen files change only via a logged superseding decision. Frozen files are corrected with dated addenda, never silent rewrites.

Status reconciled against code on 2026-10-20 (freshness pass). Each file has a `Status (2026-10-20 freshness pass)` block with LANDED / OPEN evidence. Open work is tracked in `docs/TODOS.md`. Plain-language overview: `docs/wiki/Home.md`.

## Index

| File | Status | Proposal section |
|---|---|---|
| `phase-0-exit-criteria.md` | Tracking checklist | §9 Phase 0 |
| Endpoint + streaming manifest | Frozen (`docs/endpoint-manifest.md`; 2026-10-20 addendum §39 supersedes stale rows at §§6, 9, 13, 14, 15, 19, 30) | §6 |
| Core domain types | Frozen (`docs/domain-types.md`; 2026-10-20 addendum on deferred list) | §4 |
| Identity rules | Frozen (`docs/identity-rules.md`; 2026-10-20 addendum) | §3 |
| ActivityPub collections (outbox, followers, following paging) | Frozen (`docs/activitypub-collections.md`; Phase 2.6 slice 11; 2026-10-20 addendum: hide-followers fields landed) | §7 |
| Visibility policy | Frozen (`docs/visibility-policy.md`; 2026-10-20 addendum: `blocked_hosts()` store-backed) | §4 (`policy.rs`) |
| Application ports | Frozen (`docs/application-ports.md`; 2026-10-20 addendum: 20 `unimplemented!()` trait defaults still present) | §4 (`ports.rs`) |
| Persistence conformance tests | Frozen, redb green (`docs/persistence-conformance.md`; 118 `#[test]` fns in `tests/conformance.rs`, redb only; FDB not implemented) | §2 |
| FDB client verification note | Executed 2026-10-14 (`docs/fdb-verification.md`; Q1–Q3 + startup-Q4 answered live; probe scripts under `/tmp` no longer exist, so not reproducible) | §9 Phase 0 |
| Phase 1 plan | PLAN HISTORY (`docs/phase-1-plan.md`; pinned against Misskey 2026.10.1-alpha.0) | §9 Phase 1 |
| Phase 1 exit criteria | Tracking checklist — all boxes checked (`docs/phase-1-exit-criteria.md`) | §9 Phase 1 |
| Phase 2 plan | PLAN HISTORY (`docs/phase-2-plan.md`; Misskey 2026.10.1-alpha.0 & Sharkey header says `2025.4.4`, README and exit docs say `2025.4.7`; reconciliation open in TODOS) | §§7, 9 Phase 2 |
| Phase 2 exit criteria | Tracking checklist — all boxes checked (`docs/phase-2-exit-criteria.md`) | §§7, 9 Phase 2 |
| Phase 2 interop how-to | Operational guide (`docs/phase-2-interop-howto.md`) | §9 Phase 2 |
| Phase 2.5 / 2.6 plans | PLAN HISTORY (`docs/phase2-5.md`, `docs/phase2-6.md`; most slices landed, slice 7 (phase2-5) and slice 13 (phase2-6) open) | §9 Phase 2 |
| Phase 3 compatibility profile | Frozen (`docs/phase-3-profile.md`; 2026-10-20 addendum §8 supersedes §§5, 7) | §9 Phase 3 |
| Phase 3 exit criteria | Tracking checklist — all boxes checked (`docs/phase-3-exit-criteria.md`) | §9 Phase 3 |
| Testing guide (all suites + browser topologies) | How-to (`docs/testing.md`; entry point: `scripts/smoke-all.sh`; "Known gaps" section lists the `tail -30` gate issue) | §9 all phases |
| Quote posts + FEP-044f | Implementation guide (`docs/quote-posts.md`; reviewed 2026-10-16; manual approval not built and not advertised) | §9 |
| Branched replies | Implementation guide (`docs/replies.md`; slices A+B landed on redb 2026-10-20; C partial; FDB open) | §9 |
| Word mutes | Implemented (`docs/word-mutes.md`; server-side enforcement) | §9 |
| Federated collections (clips + pages) | Spec (`docs/federated-collections.md`; fetch-gate contract + wire shapes; built in `7fa40e0`) | §9 |
| Misskey parity plan (backend only) | Draft (`docs/misskey-parity-plan.md`; 2026-10-20 status block; §11 list-config divergence corrected) | §6 |
| Sharkey parity | Descriptive, not a contract (`docs/sharkey-parity.md`; 2026-10-20 cross-reference table; Sharkey claims not re-verified) | §6 |
| Catodon plan | Forward plan with status table (`docs/catodon.md`; nothing built in `src/`) | §9 |
| Remote-media cache plan | Draft (`docs/remote-media-cache-plan.md`; all 8 build steps OPEN at 2026-10-20) | §9 |
| Batch M implementer guide (moderation, registration policy, daily-use parity, pre-Phase 4) | Draft (`docs/implementer-guide-batch-m.md`; WP1–WP16 mostly landed with per-WP evidence; WP8 and WP16 open) | §§6, 9 |
| Extension plan / community planning / pinetree decisions | Forward plans (`docs/extension-plan.md`, `docs/community-planning.md`, `docs/pinetree-decisions.md`; status tags per section; communities, events, RFC 9421 open) | §9 |
| Keystone clip/page surface | Design record (`docs/keystone-clip-page-surface.md`; status per section) | §9 |
| Gap list | Living (`docs/TODOS.md`; verify-then-close section resolved 2026-10-20) | all |
| Code inventory (2026-10-20) | Snapshot (`docs/inventory-pinetree-vs-misskey-2026-10-20.md`; the code truth the freshness pass was checked against) | all |
| Wiki (plain-language overview) | Living docs (`docs/wiki/Home.md`; About, Current state, Feature matrix, Roadmap, Intentional differences, Glossary, How to read the docs) | all |

## Archive index (status as of 2026-10-20)

Archive docs (audits, bench logs, handoffs, exit records) each carry a `Status (2026-10-20 freshness pass)` block. Read it first; the body is historical and is not rewritten.

- **Mostly landed:** `federation-bench-2026-10-18-postmortem`, `federation-bench-2026-10-18-session2-problems`, `federation-bench-2026-10-19-phase7-run{3,4,5}` and `-run{3,4,5}-log`, `fediverse-bench-2026-10-16`, `handoff-batch-{n-gaps,o,p}`, `handoff-federated-clips-pages{,-phase2}`, `handoff-sharkey-scheduling-shim`.
- **Open** (see `docs/TODOS.md`): `handoff-batch-q`, `handoff-fediverse-bench-fixes` (P3 Undo(Like), P5 quote-path fetch, P6 counters), `handoff-stress-bench-next`, `audit-2026-10-16-phase0-to-now`.
- **Harness-only** (birchtree or lab, not pinetree code): `handoff-birchtree-polls-and-feature-parity`, `handoff-birchtree-unboost-and-stress-forensics`.
- **Live how-tos:** `testing.md` (see "Known gaps"), `phase-2-interop-howto.md`, `fdb-verification.md` (probe not reproducible).
- **Phase exit records:** `phase-{0,1,2,3}-exit-criteria.md`. Boxes are as recorded; none were re-executed in the freshness pass.

Conventions: concrete over abstract (methods, fields, nullability, error shapes); record unsupported behavior explicitly; no implementation code in contracts.
