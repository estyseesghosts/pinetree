# 2026-10-22 cargo target dir untracked (adversarial-2 slice 7, finding 8)

## Changed
- `.cargo/config.toml`: removed the tracked `[build] target-dir =
  "C:/Users/julie/.cargo/shared-target"` absolute path. The file now holds
  only a comment: local birchtree target-dir sharing is `CARGO_TARGET_DIR`
  in the environment or a gitignored user config, not a committed path.
- `docs/testing.md` (`cargo test` notes): replaced the stale
  "`target-dir` is redirected to ext4 via `.cargo/config.toml`
  (git-ignored, machine-local)" line — the file is tracked, so that claim
  was wrong — with the `CARGO_TARGET_DIR` sentence above (plus the
  one-tree-at-a-time lock note). No new README section; this doc already
  mentioned the shared target.
- Left untouched per instructions: `.gitignore` (already lists `/.cargo/`;
  not used as the fix), `C:/Users/julie/.cargo/shared-target` (local build
  cache, still present), and all Slice 1/3/6/8/9-owned paths.

## Verified (executed, native PowerShell + native cargo)
- `C:\Users\julie\.cargo\bin\cargo.exe metadata --no-deps --format-version 1 --offline`
  → exit 0. Resolves `pinetree 0.1.0` (lib + bin + full test-target list);
  `target_directory` now defaults to `...\pinetree\target`.
- `git status --short`: only `M .cargo/config.toml`, `M docs/testing.md`,
  plus pre-existing untracked `docs/adversarial-2.md` (not mine). No clean
  rebuild run; no commit/stash/reset performed.

## Open
- Whoever builds locally and still wants the shared birchtree target dir
  must export `CARGO_TARGET_DIR=C:/Users/julie/.cargo/shared-target`
  (or equivalent user config); otherwise cargo uses `./target`.
