# docs/ — contract documents

Each contract file carries a `Status:` line at the top: `Draft` or `Frozen`.
`Frozen` means the Phase 0 exit review accepted it; frozen files change only via a logged superseding decision.

## Index

| File | Status | Proposal section |
|---|---|---|
| `phase-0-exit-criteria.md` | Tracking checklist | §9 Phase 0 |
| Endpoint + streaming manifest | Frozen (`docs/endpoint-manifest.md`) | §6 |
| Core domain types | Frozen (`docs/domain-types.md`) | §4 |
| Identity rules | Frozen (`docs/identity-rules.md`) | §3 |
| ActivityPub collections (outbox, followers, following paging) | Frozen (`docs/activitypub-collections.md`; Phase 2.6 slice 11) | §7 |
| Visibility policy | Frozen (`docs/visibility-policy.md`) | §4 (`policy.rs`) |
| Application ports | Frozen (`docs/application-ports.md`) | §4 (`ports.rs`) |
| Persistence conformance tests | Frozen, redb green (`docs/persistence-conformance.md`; 115/115 green on redb in `tests/conformance.rs` — incl. Group V V1–V4 2026-10-20 + Group W W1–W2 2026-10-20 + Group X X1–X2 + Group Y Y1–Y3 2026-10-20) | §2 |
| FDB client verification note | Executed 2026-10-14 (`docs/fdb-verification.md`; Q1–Q3 + startup-Q4 answered live) | §9 Phase 0 |
| Phase 1 plan | Frozen (`docs/phase-1-plan.md`; pinned against Misskey 2026.10.1-alpha.0) | §9 Phase 1 |
| Phase 1 exit criteria | Tracking checklist — all boxes checked (`docs/phase-1-exit-criteria.md`) | §9 Phase 1 |
| Phase 2 plan | Frozen (`docs/phase-2-plan.md`; pinned against Misskey 2026.10.1-alpha.0 & Sharkey 2025.4.7) | §§7, 9 Phase 2 |
| Phase 2 exit criteria | Tracking checklist — all boxes checked (`docs/phase-2-exit-criteria.md`) | §§7, 9 Phase 2 |
| Phase 2 interop how-to | Operational guide (`docs/phase-2-interop-howto.md`) | §9 Phase 2 |
| Phase 3 compatibility profile | Frozen (`docs/phase-3-profile.md`; target clients + in-scope surface + non-goals) | §9 Phase 3 |
| Phase 3 exit criteria | Tracking checklist — all boxes checked (`docs/phase-3-exit-criteria.md`) | §9 Phase 3 |
| Testing guide (all suites + browser topologies) | How-to (`docs/testing.md`; entry point: `scripts/smoke-all.sh`) | §9 all phases |
| Quote posts + FEP-044f | Implementation guide (`docs/quote-posts.md`; reviewed 2026-10-16) | §9 |
| Branched replies | Implementation guide (`docs/replies.md`; reviewed 2026-10-16) | §9 |
| Federated collections (clips + pages) | Spec (`docs/federated-collections.md`; fetch-gate contract + wire shapes, 2026-10-20) | §9 |
| Misskey parity plan (backend only) | Draft (`docs/misskey-parity-plan.md`; 2026-10-16) | §6 |
| Remote-media cache plan | Draft (`docs/remote-media-cache-plan.md`; from `storage-cache.md` review) | §9 |
| Batch M implementer guide (moderation, registration policy, daily-use parity, pre-Phase 4) | Draft (`docs/implementer-guide-batch-m.md`; 2026-10-17) | §§6, 9 |
| Wiki (overview, current state, roadmap, feature matrix) | Living docs (`docs/wiki/Home.md`) | all |

Conventions: concrete over abstract (methods, fields, nullability, error shapes); record unsupported behavior explicitly; no implementation code in contracts.
