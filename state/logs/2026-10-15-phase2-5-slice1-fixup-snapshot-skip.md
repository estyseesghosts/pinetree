# 2026-10-15 — Phase 2.5 Slice 1 fixup: snapshot tests skip without checkout

- Clean-clone `cargo test` (Slice 1 verification) failed 6 lib tests:
  `api::misskey::packers::tests::snapshot_*` read schemas from the git-ignored
  `./misskey` symlink (`packers.rs: schema_path`), which doesn't exist in a fresh
  clone. Working tree was green only because the symlink is present locally.
- Fix: `load_props` returns `Option`; the six snapshot tests early-return with a
  `SKIP snapshot: ...` stderr note when the checkout is absent. Local runs with
  the checkout still verify the snapshots; clean clones stay green.
- Checked no other tests depend on git-ignored paths (`misskey`/`sharkey`/
  `foundationdb`/`redb`): remaining `CARGO_MANIFEST_DIR` uses are tracked files
  (`scripts/smoke-phase1.sh`) or tempdir ledgers. `federation_interop.rs` is a
  0.00s canary, unaffected.
- Verified: full `cargo test` 147 green locally (6 snapshots pass with checkout);
  clean-clone re-run deferred to the Slice 9 exit gate.
