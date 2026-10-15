# 2026-10-15 — Phase 2.5 Slice 2: Split `commands.rs`

- `src/application/commands.rs` (1236 lines) → `src/application/commands/` module:
  `mod.rs` (`AppServices` + submodule decls), `helpers.rs`, `accounts.rs`,
  `notes.rs`, `follows.rs`, `media.rs`, `keys.rs`, `inbound/mod.rs`,
  `inbound/follow.rs`, `inbound/notes.rs`, `inbound/actors.rs`, `tests.rs`
  (shared `test_services` as `pub(crate)` + existing keypair test verbatim).
- Pure move: actual `handle_inbound_*` names kept (doc's `_unfollow`/`_accept_follow`/`_reject_follow`
  shorthands NOT renamed). `crate::application::commands::AppServices` path unchanged.
- Visibility bumps (compiler-required only): `now_ms`, `notify_job`, `streaming_job`
  private → `pub(super)`. Dead imports from the per-file `use` split trimmed
  (trait imports unneeded for `dyn` dispatch).
- Layout note: implementer session lacked file-delete, so the root first landed at
  `commands.rs` with `#[path]` attrs; converted to the literal `commands/mod.rs`
  layout (`git mv` + plain `pub mod` decls).
- Diff check: all 28 methods + `md5_hex`/`sniff_mime` present exactly once in their
  assigned files; `handle_inbound_reject_follow` body byte-identical;
  `grep api::misskey` in `commands/` clean.
- Verification: full `cargo test` 147 green (113 lib + 1 client-smoke + 27 conformance
  + 2 crash-recovery + 2 federation + 1 interop + 1 minrepro); `cargo build` has no
  `commands/` warnings (remaining warnings pre-date the split).
