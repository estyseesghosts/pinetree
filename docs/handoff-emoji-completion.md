# Handoff — complete custom emoji support

Status: Draft (2026-10-20). Owner: next implementer. Pinetree backend only.

## Goal

Close the custom-emoji gaps so Misskey-compatible clients can manage and receive custom emojis the way they do against Misskey. Done means: every item in "Scope" is either implemented and tested, or explicitly moved to "Documented out" with a dated decision.

## Starting point (verified 2026-10-20, read from code)

Already working:
- Admin add, update, delete, list of local emojis: `src/api/misskey/endpoints/mod.rs:5109-5235` (`admin/emoji/add|update|delete|list`).
- Application ops: `create_emoji`, `update_emoji`, `delete_emoji`, `ingest_remote_emoji_tags`, `fetch_remote_emoji`, `enqueue_remote_emoji_fetches`: `src/application/commands/emoji.rs:57-291`.
- Remote emoji ingest from inbound posts and background fetch, gated by `show_remote_emojis` (opt-in).

Gaps (from `docs/TODOS.md`, P1 "client-visible API holes" and "streaming holes", plus `docs/misskey-parity-plan.md:168`):
1. Admin bulk add, remote copy, and import-from-zip (`admin/emoji/*` family).
2. Custom emoji export (`export-custom-emojis`).
3. Streaming broadcasts `emojiAdded`, `emojiUpdated`, `emojiDeleted` (currently only `UserSuspended` and `HubMessage` are broadcast: `src/streaming/server.rs:211-231`).
4. Custom `:name:` in posts is passed through with no registry or role check (documented divergence; decide whether to enforce).
5. Anything else found while verifying the above — record it, don't silently expand scope.

Check each claim again before you build on it. The inventory is a snapshot; the code wins.

## Rules you must follow (from `AGENTS.md`)

- Read `sns-server-proposal.md` §9 (current phase) and `docs/README.md` first. Read the latest entry in `state/logs/` for context.
- Frozen contracts (`endpoint-manifest.md`, `domain-types.md`, `application-ports.md`, etc.) change only by dated addendum. If the change needs a new port or a new job kind, record it.
- Domain must not import HTTP or Misskey shapes. Misskey API and ActivityPub adapters call the same application ops; no duplicated business logic.
- Every setting you add goes in the JSON config file (`--config`): optional serde field, sane default, covered in `validate()`, round-trip test in `src/config.rs`, documented key. No env-only or CLI-only settings.
- State change and any outbox/job record commit in one storage transaction.
- Federation input is untrusted. Remote emoji fetch must keep its size limits, timeouts, redirect limits, and SSRF checks.

## Subagent plan (required: disjoint files)

Subagents share one worktree. Partition so they cannot collide:

- **Subagent A — admin bulk/copy/zip:** new application op(s) in `src/application/commands/emoji.rs` (or a new `emoji_import.rs`), plus new `admin/emoji/*` arms in `src/api/misskey/endpoints/mod.rs`. Owns the endpoint dispatcher only for its new arms.
- **Subagent B — export + streaming events:** `export-custom-emojis` op and endpoint arm; `emojiAdded|Updated|Deleted` broadcast in `src/streaming/server.rs` and `events.rs`.
- **Subagent C — tests:** unique test files only, e.g. `tests/emoji_bulk_contract.rs`, `tests/emoji_streaming_contract.rs`. No edits to shared test files.
- **Parent (you) — shared files:** `docs/endpoint-manifest.md` addendum, `docs/TODOS.md`, `docs/README.md`, `src/config.rs` (if any new key), router and ports. Merge subagent output yourself. Subagents propose snippets for these; they do not edit them.

Subagents must leave their changes **uncommitted**, and must not run `git commit`, `git stash`, `git reset`, or `git checkout --`. Check `git status --short` before starting and respect others' uncommitted work.

## Testing (required, executed)

Run and paste real output into the state log. Do not mark done on reasoning alone.

1. Unit tests for each new application op (validation, duplicate shortcode, permission, bounded input).
2. Contract tests for each new endpoint: happy path, bad input, unknown field (`UNSUPPORTED_FIELD`), permission denied, pagination for `list`.
3. Streaming tests: a subscribed client receives `emojiAdded`, `emojiUpdated`, `emojiDeleted` with the correct payload; a non-subscribed client does not.
4. Zip import: reject zip-bombs and oversized entries, reject non-image MIME, reject path traversal in filenames, and cap entry count.
5. Persistence: run the conformance suite (`cargo test --test conformance`) — the same contract must pass on redb. Add conformance cases for any new store method.
6. Full gate: `bash scripts/smoke-all.sh`. Note: the script currently pipes cargo through `tail -30`, which masks failures, and prints `SMOKE_ALL_OK` on SKIP. Check the exit status yourself (`echo $?`) and do not trust the final line alone.
7. Compatibility: where a claim depends on Misskey behaviour, test against the real checkout at `./misskey` (read-only), not against mocks alone.

## Done when

- [ ] Every item in "Gaps" is implemented+tested, or moved to a documented out-of-scope decision in `docs/misskey-parity-plan.md`.
- [ ] `docs/endpoint-manifest.md` has a dated addendum listing the new arms with `file:line` evidence.
- [ ] `docs/TODOS.md` items checked off with evidence; no new unchecked item left unexplained.
- [ ] `state/logs/YYYY-MM-DD-emoji-completion.md` appended: changes, verification output (real commands and results), open questions.
- [ ] `git status --short` shows only the intended files.
- [ ] **Committed.** The parent commits after the tests pass, with a message that names the feature and lists the verification run. Do not leave work uncommitted at the end of the session, and do not push without the owner's go-ahead.

## Prompt to give the next implementer

> Read `docs/handoff-emoji-completion.md` and follow it exactly. Start with `git status --short`, then read the files it names. Partition the work across subagents as the doc says, with disjoint files. Run the tests it lists and paste the real output into a new dated log under `state/logs/`. When everything passes, commit your work with a message that names the feature and the verification run. Don't push. Report what's left open.
