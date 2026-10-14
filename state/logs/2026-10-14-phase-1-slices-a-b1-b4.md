# 2026-10-14 — Slices A1–A4, B1, B4 (scaffold, kernel, domain types, visibility policy, ports, fs blob store)

Goal: Implement Phase 1 Slices A1–A4, B1, and B4 per `docs/phase-1-plan.md`.

What changed:
- **A1 Scaffold**: Created `Cargo.toml` (single crate `pinetree`, `redb = { path = "redb" }`, optional `fdb` feature off by default), `src/config.rs` (`Config`, `DeploymentMode::Single | Cluster`, `cluster` mode returns `"cluster mode is not implemented (Phase 4)"`), `src/lib.rs`, `src/main.rs` (`GET /healthz` → 200 `"ok"`), and module tree matching `sns-server-proposal.md` §4.
- **A2 Kernel**: Implemented `src/domain/id.rs` (`AidxGenerator`, `IdGenerator`, `Clock`, `SystemClock`, `ManualClock`, `DomainError` / `PortError`).
- **A3 Domain types**: Implemented `src/domain/{account,note,follow,reaction,media,notification}.rs` with all frozen validation bounds from `docs/domain-types.md`.
- **A4 Visibility policy**: Implemented `src/domain/{visibility,policy}.rs` with the single `is_visible` predicate, timeline/federation eligibility helpers, and `narrow_on_create` rules from `docs/visibility-policy.md`.
- **B1 Ports**: Implemented `src/application/ports.rs` and `src/application/events.rs` covering `AccountStore`, `NoteStore`, `FollowStore`, `ReactionStore`, `JobQueue`, `ActivityDedup`, `MediaStore`, `BlobStore`, `NotificationStore`, `AuthStore`, and `StreamingSink`.
- **B4 BlobStore (fs)**: Implemented `src/media/storage.rs` (`FsBlobStore`) using content-addressed paths (`objects/{prefix}/{hash}`), write-to-temp (`tmp/{hash}.{pid}.{seq}.tmp`) + `sync_all` + atomic `rename`.

Verification:
- Executed `cargo test --lib`: **42 passed; 0 failed** (aidx regex/sort/parse_time, account/note/follow/reaction/media bounds, E1–E4 visibility matrix + narrowing rules, `FsBlobStore` put/get/delete + partial-write invisibility + path traversal rejection).

Open questions:
- Proceeding to Slices B2 & B3 (redb adapter + full conformance suite including `kill -9` subprocess crash tests) and Slices C1–C3 (application commands, queries, and background worker).
