# 2026-10-14 — Slices C4 and D3 (Misskey 2026.10 endpoints, router, CLI, and mid-traffic crash recovery)

Goal: Complete Slice C4 (`tests/crash_recovery.rs`) and Slice D3 (`src/api/misskey/endpoints/mod.rs`, `src/api/misskey/mod.rs`, `src/main.rs`).

What changed:
- **D3 Misskey Endpoints & Router (`src/api/misskey/endpoints/mod.rs`, `src/api/misskey/mod.rs`, `src/main.rs`)**:
  - Wired all Phase 1 routes: `GET /healthz`, `GET /files/{id}`, `GET /files/{id}/{name}` (301 redirect), `GET /identicon/{id}` (SVG), `GET/POST /miauth/{session}` (server-rendered approval page + callback redirect with unsafe scheme blocklist), one-shot `POST /miauth/{session}/check` and `POST /api/miauth/{session}/check`, `POST /signup` (`/api/signup`, `/api/admin/accounts/create`), `POST /signin` (`/api/signin`), `POST /api/drive/files/create` (multipart/form-data + JSON), and the `POST /api/{*endpoint}` dispatcher (`meta`, `miauth/gen-token`, `auth/session/{generate,show,userkey}`, `auth/accept`, `i`, `i/update`, `users/show`, `users/search`, `username/available`, `notes/{create,show,delete}`, `following/{create,delete}`, `notes/{timeline,local-timeline,hybrid-timeline}`, `notes/reactions/{create,delete}`, `i/notifications`).
  - Added CLI subcommand `pinetree create-account --username=<name> --password=<pw>` and wired `main.rs` startup with `RedbStore`, `FsBlobStore`, `JobWorker`, and `RateLimiter::with_misskey_defaults`.
  - Added 16 endpoint unit/integration tests in `src/api/misskey/endpoints/mod.rs`.
- **C4 Crash Recovery (`tests/crash_recovery.rs`)**:
  - `c4_kill_mid_traffic_and_recover`: Spawns a child generating live account/follow/note/reply/renote/mention/reaction traffic while logging acknowledged note IDs to `acknowledged_notes.log` (`sync_all` per note), sends `SIGKILL` (`child.kill()`) mid-traffic after ≥15 notes, reopens `RedbStore` on the same directory, verifies 0 lost acknowledged notes, drains the `JobWorker` queue to `pending_count() == 0`, and verifies `Follow`, `Mention`, `Reply`, and `Reaction` notifications were delivered.
  - `c4_restart_preserves_accounts_follows_reactions_and_timelines`: Verifies accounts, follows, notes, reactions, media blobs, and home/local/hybrid timelines survive `SIGKILL` + restart.

Verification:
- `cargo test --lib`: **78 passed; 0 failed** in 0.10s.
- `cargo test --test crash_recovery`: **2 passed; 0 failed** in 1.63s.

Open questions:
- Proceeding to Slices E1 & E2 (`scripts/smoke-phase1.sh` using `misskey-js` 2026.10.1-alpha.0 under `bun`, `tests/client_smoke.rs`, and `docs/phase-1-exit-criteria.md`).
