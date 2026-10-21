# 2026-10-21 gaps final wiring 5/5 (drive folders + memo/mute + webhooks + send-email)

Parent merge of the two unverified agent sessions (worker/main/config,
endpoints/errors) plus the parent-written missing arms. All
`cargo check --offline --tests` green (native Windows cargo, ~1 min);
`cargo test --offline --test gaps_final_wiring` green.

## What landed

- Dispatcher arms (17 new + 2 extended legs), all per manifest §43:
  `drive/folders/{create,show,find,update,delete}`,
  `drive/files/{find,move-bulk}`, folder-aware `drive/files`
  (replaces the "folders unsupported" reject) + `drive/files/update`
  folder leg, `notes/thread-muting/{create,delete}`,
  `users/update-memo`, `system-webhook/*` ×5, `webhooks/*` ×5,
  `admin/send-email` (durable `EmailSend` enqueue; 403/400 gates).
- New ops: `mute_note_thread` / `unmute_note_thread` (`notes.rs`),
  `update_memo` (`accounts.rs`).
- New packers: `pack_drive_folder`, `pack_system_webhook`,
  `pack_user_webhook`; `pack_drive_file` now resolves the real
  `folderId` (fail-open null).
- New error constructors with per-endpoint UUIDs (19 minted v4,
  `TODO(final): verify against misskey`): folder ×9, webhook ×6+1,
  memo ×1, thread-mute ×2. Mappers `map_drive_folder_error`,
  `map_webhook_error`.
- `AppServices` gains 6 durable fields (`folders`,
  `system_webhooks`, `user_webhooks`, `abuse_recipients`, `memos`,
  `thread_mutes`); `ActivityIndex` deliberately NOT added (deferred per
  docs, zero call sites). 46 literals patched by script (44
  `store.clone()`, 2 mem doubles via new fail-closed
  `UnavailableStores` stub in `commands::tests`).
- Trait unification: local `DriveFolderStore` / `SystemWebhookStore` /
  `UserWebhookStore` / `AbuseReportRecipientStore` defs in
  `drive_folders.rs` / `webhooks.rs` deleted (byte-identical to ports);
  everything uses `ports::*`. Test fakes re-pointed.
- `main.rs` publishes the SMTP sender to the dispatcher slot when
  configured (email hookup); transcode gate moved below the
  `create-account`/`set-role` early-returns.
- `Config::validate` enforces transcode ranges only
  (`validate_ranges`); the ffmpeg probe runs once at startup. Smoke
  scripts pass `--no-transcode` (lab ffmpeg lacks h264/hevc decoders).
- `sanitize_mfm` fix: comma-form `data:` URLs now neutralize
  (`blocked:,x`); exposed by the parent's sanitizer hardening.
- CRLF phantom diffs normalized to LF (11 files); line-ending policy
  unchanged (autocrlf handles checkout).

## Verification (executed 2026-10-21, native Windows cargo + WSL gates)

- `cargo check --offline --tests` (native): 0 errors.
- `cargo test --offline --test gaps_final_wiring` (native): 1 passed
  (covers every new arm: CRUD, error codes + UUID pins, guards,
  cap, job enqueue).
- Full gate `bash scripts/smoke-all.sh` (WSL): `SMOKE_ALL_OK`
  (420s total) — `cargo test` PASS (414s: lib 601 + all
  integration targets incl. `gaps_final_wiring`, `gaps_api_drive`,
  `gaps_webhooks`), `smoke-phase1` PASS, `smoke-phase3` PASS,
  `keystone-sweep` PASS (60 checks). `smoke-phase2`
  (four-server federation) SKIP: needs Node 24 on PATH (environment
  limitation, same as before this change).
- Two flakes seen and cleared during the run (both load/timing, both
  pass in isolation and in the final gate): `gaps_streaming`
  `userlist_channel_applies_with_files_and_with_renotes` (5s
  stream-frame timeout under parallel load), `client_smoke`
  (parallel-load failure; manual `smoke-phase1.sh` run: `SMOKE_OK`).
- Pre-existing test updated, not weakened:
  `phase3_drive_files` now asserts a missing/foreign `folderId`
  lists `[]` with 200 (was: 400 reject).

## Divergences recorded (new)

- Webhook endpoint params use `webhookId` in both families (upstream
  `i/webhooks/*` parity; system-webhook has no upstream shape —
  consistent within pinetree, documented in arms).
- `webhooks/create` over-cap is pinetree-defined `TOO_MANY_WEBHOOKS`
  (upstream has no limit error); `pack_meta.webhookLimit` stays 0.
- `drive/files` without `folderId` lists everything (unchanged);
  with `folderId` the page may shrink under `limit` (documented —
  clients page with `untilId`).
- Abuse-report → email fan-out producer still deferred (planners exist;
  §43.8 worker-side producers land separately). `EmailSend` jobs today
  come only from `admin/send-email`.

## Still open (not in this commit)

- 7 harder endpoints need new query ports (notes partial-bulk, polls
  recommendation, clips alias, flashs, achievements, birthday,
  frequently-replied) — second project, listed in gaps.md.
- `scripts/smoke-all.sh` full gate + keystone sweep + live interop
  (internet-gated parts stay open per §43.10).
