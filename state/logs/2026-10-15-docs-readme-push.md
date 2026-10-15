# 2026-10-15 — Docs refresh, README, push

- Updated the wiki for Phase 2.5: `Home.md` (status date + root README link),
  `About.md` (Phase 2.5 capabilities: `rsa`-crate keys, job quarantine, Reject
  atomicity, TLS always-on, `Arc<AppServices>` decoupling; clean-clone symlink
  prerequisites), `Current-State.md` (2.5 row, 150-test breakdown, corrected
  `users/followers` endpoint-only gap), `Roadmap.md` (2.5 row, Phase 3 entry
  gate green), `Feature-Matrix.md` (version line, quarantine + Reject cells).
- Added root `README.md` (was missing): features, layout, prerequisites
  (Rust/crates.io redb, bun + `./misskey`, docker + Node 24/Node 22,
  `SHARKEY_NODE_BIN`), quickstart, test matrix, clean-clone procedure, docs
  index. License section states the fact: no `LICENSE` file declared yet
  (verified: no LICENSE*/COPYING*, no `license` in Cargo.toml).
- Verified: `git status --ignored` shows no source files ignored; working tree
  was clean before this commit; `cargo test` 150 green and `smoke-phase2.sh`
  SMOKE_OK from the Phase 2.5 gate still stand (no code touched in this session
  — docs only).
- Open: choosing a license; declaring the Phase 3 compatibility profile
  (Roadmap §Phase-3 item 1); `cargo audit`/`deny` in CI (Slice 6 follow-up).
