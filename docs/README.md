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
| Visibility policy | Frozen (`docs/visibility-policy.md`) | §4 (`policy.rs`) |
| Application ports | Frozen (`docs/application-ports.md`) | §4 (`ports.rs`) |
| Persistence conformance tests | Frozen, redb green (`docs/persistence-conformance.md`; 26/26 green on redb in `tests/conformance.rs`) | §2 |
| FDB client verification note | Executed 2026-10-14 (`docs/fdb-verification.md`; Q1–Q3 + startup-Q4 answered live) | §9 Phase 0 |
| Phase 1 plan | Frozen (`docs/phase-1-plan.md`; pinned against Misskey 2026.10.1-alpha.0) | §9 Phase 1 |
| Phase 1 exit criteria | Tracking checklist — all boxes checked (`docs/phase-1-exit-criteria.md`) | §9 Phase 1 |
| Phase 2 plan | Frozen (`docs/phase-2-plan.md`; pinned against Misskey 2026.10.1-alpha.0 & Sharkey 2025.4.4) | §§7, 9 Phase 2 |
| Phase 2 exit criteria | Tracking checklist — all boxes checked (`docs/phase-2-exit-criteria.md`) | §§7, 9 Phase 2 |
| Phase 2 interop how-to | Operational guide (`docs/phase-2-interop-howto.md`) | §9 Phase 2 |
| Wiki (overview, current state, roadmap, feature matrix) | Living docs (`docs/wiki/Home.md`) | all |

Conventions: concrete over abstract (methods, fields, nullability, error shapes); record unsupported behavior explicitly; no implementation code in contracts.
