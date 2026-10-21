# 2026-10-21 gaps final wiring 3/5: redb tables + impls

Subagent `ses_e9fb3b40cffeSx2TFt71C2xdlI` (redb only); parent reviewed,
style-audited, fixed one warning, and commits. Worker/endpoints agents
still running.

## What landed (all additive: +1357/−39)

Tables (15, wired in `init_tables`):
- Drive (5): `drive_folders`, `drive_folders_by_owner_parent`
  (`""` = root), `drive_folders_by_name` (name-bucket id-list),
  `drive_file_folders`, `drive_files_in_folder`.
- Webhooks (6): `system_webhooks` + `_by_trigger`, `user_webhooks` +
  `_by_owner` + `_by_owner_trigger`, `abuse_report_recipients`.
- Federation (4): `ACCOUNTS_BY_URI`, `ACCOUNTS_BY_HANDLE`
  (full-handle alias), `NOTES_BY_QUOTE_REQUEST`,
  `NOTES_BY_QUOTE_STAMP`.

Impls (against the committed `ports.rs` paths):
- `DriveFolderStore`: full CRUD + child/name/count + file↔folder
  mapping; row + indexes + jobs in ONE txn (`MediaStore` discipline).
- `SystemWebhookStore` / `UserWebhookStore` /
  `AbuseReportRecipientStore`: insert/get/list/put/delete/active-for
  (trigger prefix range + `is_active` filter + truncate).
- Admin: NO new tables — `registry_handle()` exposes the existing
  `RegistryStore` for `~instance`-scoped admin state.
- Index maintenance in the same txn as the row (create/rename/edit/
  delete paths); scan bodies replaced at `find_by_uri` (~4245),
  `find_note_by_quote_request` (~4775), `find_note_by_quote_stamp`
  (~4800). Backfills `run_account_uri_index_backfill` +
  `run_quote_index_backfill` with `META` markers, called from `open()`
  (file's existing migration convention). No channel/chat/gallery/
  announcement tables per SKIP.
- Decisions: `HANDLE_INDEX` untouched (primary); quote collisions
  last-writer-wins; `find_*` filters deleted notes (scan parity).

## Parent fixes in this slice

- Removed stray untracked file `" | head -20"` (my own first-session
  shell artifact).
- Normalized CRLF→LF on two agent-touched files at merge (inbox.rs,
  objects.rs in the WP5 slice) — real diffs only.
- `inbox.rs`: dropped unused `Clock`/`IdGenerator` imports (warning
  cleanup in a committed file; no running agent owns it).

## Verification

- `cargo check --offline --lib`: green (worker agent's arms have
  landed in-tree since the ports slice).
- Added lines: 0 width violations, 0 `unwrap`, one-module-per-`use`,
  `format!` messages, `///` docs on new pubs.
- Remaining lib warnings are pre-existing or other agents':
  `signatures.rs` imports, `errors.rs:1277` unreachable pattern
  (endpoints agent's file — theirs to clear), `note.rs` unused_mut,
  `exports.rs` dead fn (both pre-existing).

## Style compliance (per `rust_style_guide.html`)

Manual 100-col wrapping (no `cargo fmt` in container); abbreviations
limited to the file's own local conventions (`by_*`, `ids`);
`expect`-only error paths, no `unwrap` in added code.
