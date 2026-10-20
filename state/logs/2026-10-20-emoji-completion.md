# 2026-10-20 — custom-emoji completion (handoff `docs/handoff-emoji-completion.md`)

Scope: pinetree backend only. Closes all five handoff gaps: (1) admin bulk/remote-copy/import-zip,
(2) `export-custom-emojis`, (3) `emojiAdded/Updated/Deleted` streaming broadcasts,
(4) custom-`:name:` enforcement decision, (5) nothing else found (recorded below).
Nothing was already implemented beyond the handoff's starting-point inventory — verified each
claim against code before building.

## Workstreams (disjoint file sets, one worktree)

- **Parent groundwork:** `StreamingEvent::EmojiBroadcast{op, emojis: Vec<CustomEmoji>}` + `EmojiBroadcastOp`
  (`events.rs`); `HubMessage::EmojiBroadcast{event, body}` + sink arm + `StreamingSink::push_broadcast`
  (`hub.rs`, `ports.rs`); `EmojiStore::list_remote_emojis` port + redb impl; `JobKind::ExportCustomEmojis|
  ImportCustomEmojis` + `CustomEmojiTransferPayload`; `zip = 2.2 (deflate)` dependency; server.rs
  broadcast dispatch arm (`{type, body}` to every connection, no channel filter).
- **Subagent A** (`emoji.rs`, `packers.rs` append, `tests/emoji_admin_bulk.rs`): bulk ops
  (add/remove/set-aliases, set-category/license with upstream null-reset, delete-bulk, 100-id cap),
  `copy_remote_emoji{host,name}`, `list_remote_emojis` op, `pack_remote_emoji_detailed`,
  broadcast enqueue in create/update/delete + bulks.
- **Subagent B** (`emoji_transfer.rs`, `worker.rs`, `tests/emoji_streaming_transfer.rs`): export/import
  job handlers, `handle_emoji_broadcast` worker interception, 6 streaming+transfer tests.
- **Parent merge:** endpoint arms for all ten endpoints (`mod.rs:5264-5445`, `:6005-6018`), `enqueue_*
  helpers in `emoji_transfer.rs`, 1/hr `export-custom-emojis` bucket (`response.rs`), Gap-4 reaction
  fallback (`notes.rs:resolve_custom_emoji_reaction`), conformance J3, docs (manifest §40, parity-plan
  §6/§13 corrections, TODOS), this log, commit.

## Design decisions (all upstream-verified against `./misskey` pin `2026.10.1-alpha.0`)

- Events carry domain snapshots; the worker packs to `EmojiDetailed` (application never imports API
  wire shapes; the sink has no `AppServices`). Delete snapshots before the row is gone.
- `admin/emoji/copy` takes `{host, name}`, not upstream `{emojiId}` — remote rows have no global ids.
  New `CANNOT_COPY_EMOJI` 400 for unusable remotes.
- Bulk alias/category/license writes per-row `UpdateCustomEmoji` log rows (upstream logs nothing;
  the port requires one log per `update_emoji` call).
- `list-remote` `sinceId`/`untilId` accepted-but-ignored (sort is by `first_seen_ms`); remote
  `EmojiDetailed.id` is `{host}:{shortcode}` composite.
- Export zip in-memory + content-deduped (`force=false`); import rows attribute the importer as
  moderator; `meta.json` capped 16 MiB (upstream 64 MiB); otherwise upstream-verbatim guards.
- Gap 4 implemented, not documented-out: unknown names, role-gated rows (permission unprovable —
  decided DIVERGENCE), `localOnly` on remote notes, sensitive on `nonSensitive*` notes all fall back
  to ❤, never an error (`ReactionService.create` parity). `:name@other-host:` stays verbatim.
- Parity-plan §13 "customEmoji has no dedicated endpoint in this pin" was factually wrong
  (`export-custom-emojis.ts` exists in the pin) — corrected with a dated note.

## Verification (executed, real output)

- `cargo test --offline` full suite: 498 lib + 118+1 conformance (new J3 green) + 14
  `emoji_admin_bulk` + 6 `emoji_streaming_transfer` + 16 `streaming` + rest = **688 passed, 0 failed**
  (17 `test result: ok` lines, zero `FAILED`/failures lines).
- `bash scripts/smoke-all.sh` → `EXIT=0`, `SMOKE_ALL_OK (28s)`: phase3 PASS, keystone-sweep PASS
  (38 checks), phase2 SKIP (needs `SHARKEY_NODE_BIN`, pre-existing environmental skip).
- `cargo check --offline` clean after every merge step.

## Open questions

- `admin/emoji/import-zip` has no upstream rate bucket; it rides the default open bucket like the
  other `admin/emoji/*` arms — tighten if abuse surface matters.
- Manifest §28 body + TODOS line 181 (custom-emoji passthrough verify item) still describe the old
  divergence; the §40 addendum supersedes them but the body lines were left frozen per convention.
