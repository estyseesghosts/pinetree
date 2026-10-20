# 2026-10-20 — `notes/like` (Sharkey/Catodon parity)

Scope: backend only. Closes TODOS "Like-vs-favorite split" (extension-plan C1,
decisions `:10-11`).

## Reference findings (read, not assumed)

- Sharkey `notes/like.ts` and Catodon `notes/like.ts` are thin wrappers over
  `ReactionService.create` with `override ?? serverSettings.defaultLike`
  (kind `write:reactions`, 2 calls/sec). Neither has a separate like table,
  a separate activity shape, or a `notes/unlike` endpoint. Unlike is
  `notes/reactions/delete` in both. The decisions doc's "SEPARATE from emoji
  reactions" phrasing is corrected in manifest §41: a like IS a reaction row;
  "Sharkey like = Mastodon favourite" is emergent from the existing bare-`Like`
  federation split (outbound plain `Like` to Mastodon-class hosts, inbound
  bare `Like` → ❤ — parity plan §6, untouched).
- Toggle difference: Sharkey catches an already-reacted error id and deletes
  (toggle); Catodon documents same→no-op, different→replace. That error id is
  never thrown by current Sharkey `create` (same→no-op, different→replace in
  the service), so the toggle catch is dead upstream. Owner decision:
  implement the advertised toggle — same-emoji re-press unlikes, different
  emoji replaces.
- Default source decision (owner): new `default_like` JSON config key +
  `--default-like` CLI flag (AGENTS.md-compliant: optional serde field, sane
  default, `validate()` 1..=260 chars, round-trip + bounds tests in
  `src/config.rs`); precedence `override ??` user `defaultReaction ??`
  instance `default_like ??` ❤. Custom `:name:` spellings allowed in the
  setting, gated at use time.

## Changes

- `src/config.rs`: `default_like: Option<String>`, validate, CLI handled in
  `main.rs`, wired through `AppServices::default_like` (+ ~70 struct-literal
  updates across `src/`/`tests/`, verified exhaustive by script).
- `src/application/commands/notes.rs`: `resolve_like_emoji`,
  `resolve_reaction_for_note` (extracted from `react`, shared so both paths
  store byte-identical strings), `like_note` (toggle/replace, never surfaces
  `ALREADY_REACTED`); `react_with_default` now uses the same chain.
- `mod.rs`: `notes/like` arm (`write:reactions`, 204) + endpoint tests;
  2/sec bucket in `response.rs`.
- `tests/notes_like_contract.rs`: 6 op-level tests (default chain ×3 levels,
  toggle, replace, unknown-custom fallback, missing note).
- Docs: manifest §41, parity-plan §6 chain note, TODOS checked.

## Verification

- New tests: `notes_like_contract` 6/6, `notes_like_*` endpoint tests 2/2.
- Full `cargo test --offline`: all 18 targets green, 0 failed (501 lib incl.
  the 3 new tests, 119 conformance incl. J3, all integration files).
- `bash scripts/smoke-all.sh` → `EXIT=0`, `SMOKE_ALL_OK (29s)`: phase3 PASS,
  keystone-sweep PASS (38 checks), phase2 SKIP (needs `SHARKEY_NODE_BIN`,
  pre-existing environmental skip).
