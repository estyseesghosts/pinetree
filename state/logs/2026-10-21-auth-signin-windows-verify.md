# 2026-10-21 — auth/signin verification + commit (parent, baremetal Windows)

Supersedes by reference: `2026-10-21-auth-signin.md` (implementer session).
That log is complete and accurate for the code work; this entry records
parent-side verification, one correction to its environment theory, and the
commit. Nothing in the implementer's log was rewritten.

## What the implementer delivered (taken as-is, reviewed)

- `signin-flow` state machine (`do_signin_flow`), `/signin-flow` +
  `/api/signin-flow` routes + dispatcher arm, `signin-with-passkey` 400 stub,
  CORS `*` middleware + preflight, `GET /ready`, four `ApiError`
  constructors, 10 contract tests, `docs/endpoint-manifest.md` §42.
- Parent review: empty-username→404 matches upstream `typeof` semantics;
  error ids verbatim; legacy `signin` untouched; divergences (fresh `i` per
  login, shared throttle bucket, passkey stub, no `/oauth/*`) all documented
  in §42. `src/shutdown.rs` + container files are the pre-existing rig work.
  No changes were needed to any of it.

## Correction: the Windows segfault was stale build artifacts, not `--gc-sections`

The implementer log (§"Verification", item 1) attributes the
`__do_global_ctors` / `__gcc_register_frame` startup crash to
`-Wl,--gc-sections` vs WinLibs binutils and prescribes session-local
`RUSTFLAGS="-C link-arg=-Wl,--no-gc-sections"` (+ `CFLAGS=-mcrtdll=msvcrt`).

Parent re-investigation (gdb, objdump, linker map) showed every "smoking
gun" evaporating on comparison with the working birchtree binary: the
`.ctors` entry is GCC's own legitimate `register_frame_ctor`, the IAT
prefill pattern is identical in the working binary, same `ring` 0.17.14,
same `opt-level = 1`. Decisive experiment: full `cargo clean` (removed
**12.6 GiB / 13142 files** — the `target/` dir came over from the WSL copy
carrying Linux objects) + pristine rebuild with **no RUSTFLAGS** → every
binary runs. The `RUSTFLAGS` workaround is therefore *not* needed and is
*not* committed; it likely only worked by forcing a relink. If the crash
ever recurs, `cargo clean` is the first step, not linker flags.

## Windows build recipe (this machine, persistent)

- Toolchain: `stable-x86_64-pc-windows-gnu` (rustc 1.99.1), default.
- `WinLibs gcc 16.2.0` on PATH per command (for `cc-rs`/ring).
- `CC_x86_64-pc-windows-gnu` → LLVM-MinGW `x86_64-w64-mingw32-gcc.exe,
  set as a **persistent user env var** (kept: ring+clang is well-supported;
  harmless to birchtree). Re-verify with stock gcc was deliberately skipped
  — no reason to churn a green tree.
- `CARGO_TARGET_DIR=<repo>\target` per command (overrides the tracked
  `.cargo/config.toml` WSL path `/root/pinetree-target`; the tracked file
  was intentionally left untouched).
- `git config core.fileMode` left at default here (no mode noise in pinetree).

## Verification (real output, baremetal Windows PowerShell)

- `cargo check --all-targets --locked` → exit 0 (warnings only, pre-existing).
- `cargo test --locked --offline --no-fail-fast` on the pristine tree:
  **lib 512/512** (incl. all 10 `signin_flow_*`/passkey/preflight tests),
  conformance 119, batch_q 10+6, streaming 16, federation 7, elixir 2,
  interop 1, crash_recovery 2, emoji 14+6, notes_like 6, quotes 5,
  minrepro 1, client_profile 1. **0 failed.**
- Sole red: `client_smoke_live_binary_and_misskey_js` — the test execs
  `bash` with a backslash Windows path, which Git Bash mangles
  (`C:Usersjulie…`, exit 127). Even past that, the script needs the
  git-ignored `./misskey` checkout (absent — the same test is the one
  documented failure on the Linux baseline) plus `bun`/SIGKILL semantics.
  Environmental, pre-existing, unrelated to this handoff. Deliberately NOT
  papered over with a path shim: the suite would still fail on the missing
  checkout, and the failure message already says exactly what is missing.

## Commit

Staged everything (`git add -A`), including the two `D …Zone.Identifier`
ADS-artifact deletions and all untracked container/handoff/log files.
`docs/endpoint-manifest.md` byte-checked: valid UTF-8, 0 U+FFFD (the `???`
in console diffs is a font artifact, not file damage).
