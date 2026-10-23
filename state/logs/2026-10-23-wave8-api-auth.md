# 2026-10-23 Wave 8 — API/auth residuals (layering, oracles, 2FA)

Subagent work. Parent fixed three test failures (progressive-backoff
sleeps, TOTP-step advance), swapped two gate orders (deleted before
suspended), and blessed the flagged `scheduled_draft.rs` worker hunk;
no other behavior changes.

## Changed
- Layer inversion fixed (password verify + pack inputs moved down to
  application/domain; adapters keep HTTP shapes):
  - `src/domain/password.rs` (new): Argon2id PHC + legacy `salt:hash`
    verify, `needs_rehash` — moved verbatim from `api::auth`.
    `api::misskey::auth.rs` keeps thin delegates (same signatures).
  - `application/commands/emoji.rs` (new `emoji_served_url` /
    `emoji_simple_value` / `emoji_detailed_value`, logic moved verbatim);
    `packers/emoji.rs` delegates; `emoji_transfer.rs` uses the app fn.
  - `application/commands/helpers.rs` (new `note_interaction_jobs` +
    `NoteSideEffectMode::{Local,Inbound}`); `notes/create.rs` and
    `inbound/notes.rs` both call it (strict/best-effort, job suffixes
    `reply` vs `inbound-reply`, error codes preserved).
  - `pins.rs` / `polls.rs` in-file tests assert application outputs
    (`pinned_note_ids`, `get_poll` tallies) instead of packed shapes.
- `drafts/scheduled.rs` returns `(PostScheduledOutcome, Option<…>)`
  (no pack, no push); `jobs/worker/scheduled_draft.rs` packs the
  `main`-channel body worker-side (parent-blessed: adapter-side packing
  mirroring `push_notify_mains`, single `notification` hint only).
- 2FA contract (`auth_2fa.rs`): pending secret under `KEY_TOTP_PENDING`
  (upstream `twoFactorTempSecret` parity; old secret works until
  `done`/`key-done` promotes; legacy in-place rows still confirm);
  deleted checked before suspended in `authenticate_2fa`
  (`NO_SUCH_2FA`, never 403); `set_2fa_passwordless(false)` without a
  verified secret is a no-op success; prune failure logged, still expired;
  `register`→`update-key` no longer clobbers the verified row.
- Sign-in oracle (both postures intentional + pinned, `wave8_api_auth`):
  flow keeps the upstream oracle (404/403 before password check) —
  deleted now reads 404 (parent: deleted-before-suspended swap, since
  storage marks deleted rows suspended); legacy keeps password-first
  hardening (deleted + correct password → 401; parent: same swap; deleted
  branch records no failure).
- Unwraps → `let Some` / `@`-binding / `let-else`: `dispatch_lists.rs`
  (×2), `dispatch_i/account.rs:124`, `dispatch_auth/password.rs:222`.
- Anonymous list fallback: Misskey parity confirmed for anonymous +
  `forPublic=false` (public branch kept); matrix pinned either way.
- `passwordless` packer stays `false`; `user.rs:578` untouched (per audit).
- `tests/wave8_api_auth.rs` (new, 12 tests). Parent: 1.1 s + 2.2 s sleeps
  around failing legacy calls (progressive per-IP backoff 1 s → 2 s is
  intended throttle, not posture), TOTP-step advance after `done_2fa`
  (confirmation consumes the step's code), deleted-before-suspended in
  both handlers.

## Verified (native cargo, shared target)
- `--test wave8_api_auth` → 12 passed (fresh exe newer than sources).
- Full gate: `wave8_api_auth` green; lib + all integration green except
  environmental `client_profile`/`client_smoke` (`bun` missing).
- `rustfmt --edition 2024 --check` on all 22 files → clean.
