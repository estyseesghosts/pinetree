# 2026-10-14 — Slices B2, B3, C1–C3, D1, D2, D4 (redb conformance suite, application commands/queries/worker, Misskey packers)

Goal: Implement Slices B2, B3, C1–C3, D1, D2, and D4 of `docs/phase-1-plan.md` and run the full persistence conformance suite against `redb` (`4.4.0`).

What changed:
- **B3 redb adapter (`src/persistence/redb/mod.rs`)**: Implemented `RedbStore` with all 19 tables (`accounts`, `handle_index`, `used_usernames`, `account_owners`, `notes`, `notes_by_author`, `follows_forward`, `follows_reverse`, `reactions`, `jobs`, `jobs_due_index`, `activity_dedup`, `media_meta`, `notifications`, `notifications_by_user`, `credentials`, `api_tokens`, `miauth_sessions`, `legacy_sessions`), committing state changes and durable outbox jobs atomically in one `WriteTransaction`.
- **B2 Conformance suite (`tests/conformance.rs`)**: Implemented all 26 cases from `docs/persistence-conformance.md` (`a1`–`a5`, `b1`–`b6`, `c1`–`c4`, `d1`–`d3`, `e1`–`e4`, `f1`–`f3`, and `conformance_exit_box_2`), generic over `StoreFactory`/`ConformanceStore` and executed against `RedbFactory`. Crash cases (`a1`–`a5`, `b5`, `conformance_exit_box_2`) spawn a child subprocess and kill it with `SIGKILL` (`kill -9`) via `child.kill()`.
- **C1–C3 Application + Worker (`src/application/commands.rs`, `src/application/queries.rs`, `src/jobs/worker.rs`)**: Implemented `AppServices` commands (`create_account`, `create_note`, `delete_note`, `follow`, `unfollow`, `react`, `unreact`, `update_profile`, `upload_media`), fan-out-on-read queries (`notes_show`, `users_show_by_id`/`by_username`, `home_timeline`, `local_timeline`, `hybrid_timeline`), and `JobWorker` (`process_one`, `drain`, `spawn` with exponential backoff and idempotent `Notify` insertion).
- **D1, D2, D4 Misskey API Core, Auth, Packers (`src/api/misskey/{errors,request,response,auth,packers}.rs`)**: Implemented `ApiError` envelope, `check_allowed_fields`, fixed-window `RateLimiter`, SHA-256 token & salted password hashing, MiAuth HTML approval renderer, and entity packers (`Note`, `UserLite`, `UserDetailedNotMe`, `MeDetailed`, `DriveFile`, `Meta`) with snapshot tests parsing `misskey/packages/backend/src/models/json-schema/*.ts` directly from the Misskey 2026.10.1-alpha.0 checkout.

Verification:
- `cargo test --lib`: **63 passed; 0 failed** (including all 6 Misskey 2026.10 JSON schema snapshot tests).
- `cargo test --test conformance`: **26 passed; 0 failed** in 1.00s (first real-backend conformance green run on redb).

Open questions:
- Proceeding to Slice D3 (`src/api/misskey/endpoints/`, `src/api/misskey/mod.rs`, `src/main.rs`), Slice C4 (`tests/crash_recovery.rs`), and Slices E1–E2 (client smoke test + `docs/phase-1-exit-criteria.md`).
