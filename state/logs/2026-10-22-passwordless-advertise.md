# 2026-10-22 stop advertising passwordless login (adversarial-2 slice 6, finding 7)

## Changed
- `src/api/misskey/packers/user.rs` (`pack_me_detailed`): `usePasswordLessLogin`
  packs `false` always. The stored `TotpState::passwordless` bit is no longer
  surfaced; `totp_flags` keeps returning it (second tuple value) for other
  readers, but the packer ignores it. Store faults still propagate (`?`), so a
  failed read never reports `twoFactorEnabled: false`. Divergence documented
  inline.
- `src/application/commands/auth_2fa.rs` (`set_2fa_passwordless`): `value: true`
  is rejected with `INVALID_PARAM` (400) and persists nothing; `value: false`
  clears a legacy stored `true` (no-op when already clear). `totp_flags` doc
  updated: second value is legacy stored data, not a client-visible capability.
- `src/api/misskey/endpoints/dispatch_auth/twofa.rs` (`i_2fa_password_less`):
  doc-only divergence note (refuses enable, persists nothing on `true`).
- `src/api/misskey/endpoints/auth_http/signin_flow.rs` (`do_signin_flow` doc)
  and `auth_http/signup.rs` (`do_signin` comment): divergence notes that the
  password step stays mandatory even for legacy stored `passwordless: true`
  rows. No behavior change in either file: both already gate on
  `totp_flags(...).0` only.
- `src/api/misskey/endpoints/tests_auth_totp_p0.rs`:
  `passwordless_does_not_downgrade` rewritten — setter `value: true` now
  asserts 400 `INVALID_PARAM`, pack asserts `usePasswordLessLogin == false`,
  plus a `value: false` no-op-success call; password-still-required (`signin`
  rejected) and challenge-flow-completes checks kept intact, not weakened.
  New `legacy_passwordless_true_still_requires_password`: flips the stored
  bit directly via the registry (the setter no longer permits it), then
  asserts it still packs `false`, still rejects password-only `signin`, and
  still completes password + TOTP via `signin-flow` + `i/2fa/authenticate`.
- Not implemented (per plan): passwordless sign-in. No new workspace crate.
  Untouched per constraints: `src/media/remote_cache/{fetch,evict}.rs`,
  `.cargo/config.toml`, `src/backup`, `src/streaming`, `src/media/reconcile`.

## Verified (executed, native PowerShell + native cargo)
- `cargo test --locked --offline --lib tests_auth_totp_p0` → exit 0:
  `test result: ok. 18 passed; 0 failed` (incl. rewritten
  `passwordless_does_not_downgrade` and new
  `legacy_passwordless_true_still_requires_password`), 636 filtered out.
- No compiler warnings reference any of the six edited files.
- Env notes: `CARGO_TARGET_DIR=C:\Users\julie\.cargo\shared-target` (as parent
  uses) plus WinLibs GCC
  (`...WinGet\Packages\BrechtSanders.WinLibs.POSIX.UCRT_...\mingw64\bin`)
  prepended to `PATH` — the default shell PATH has no `cc`/`dlltool`, so the
  stock gnu toolchain fails at `getrandom`'s build script without it
  (llvm-mingw alone is insufficient: no `libgcc`, `lld: unable to find
  library -lgcc_eh`). Two earlier attempts failed environmentally, not from
  this slice: one hit `dlltool.exe: program not found` (pre-PATH-fix), two
  more read a half-written `src/media/remote_cache/evict.rs` while the Slice 1
  agent was rewriting it (`couldn't read ... stream did not contain valid
  UTF-8`); re-ran after that file settled. That file was not touched.

## Open
- Pre-existing stored `passwordless: true` rows are honored as data but never
  advertised; no migration clears them (clearing happens only if the user
  calls the setter with `value: false`). Upstream clients that gate UI on
  `usePasswordLessLogin: true` will now always see `false` — intended.
- Full `cargo test --locked --offline --no-fail-fast` not run here (slice
  scope is the TOTP auth tests; sibling slices are mid-flight in the same
  tree). `cargo fmt --check` not run (another agent is actively editing
  shared files; formatting the tree now would race them).
