# 2026-10-15 — Smoke test: pinetree + Keystone (fresh data dir)

## Why
Owner moved the repo to ext4 and redirected client testing from Misskey's
frontend to Keystone (`~/keystone` → `/root/keystone`). Asked for a
pinetree+Keystone smoke test with no leftover Misskey/Sharkey/pinetree
processes, leaving instances browser-accessible afterwards.

## Preconditions (executed)
- `ss -tlnp` + `ps`: no pinetree/vite/Misskey/Sharkey listeners or procs
  (only fdbserver :4500, opencode, system services). Docker: no containers.
- `./misskey`, `./sharkey` were 0-byte regular files (broken by the ext4
  move); replaced with symlinks to `/root/fedifedifedi/{misskey,sharkey}`
  (both version-verified: Misskey `2026.10.1-alpha.0`, Sharkey `2025.4.7`).
  Both are git-ignored; `git status` otherwise unchanged.
- Worktree has uncommitted global-timeline changes (`endpoint-manifest.md`,
  `Current-State.md`, `endpoints/mod.rs`, `queries.rs`), so rebuilt:
  `cargo build --offline` green in 14.5s (4 pre-existing warnings only),
  binary `/root/pinetree-target/debug/pinetree` (101M).

## What ran
- Fresh data dir `/tmp/opencode/pinetree-smoke/data` (existing `./data`
  untouched). Accounts created before start, same dir:
  `alice`/`alice-dev-password`, `bob`/`bob-dev-password`.
- Server: `pinetree --public-url=http://127.0.0.1:3000
  --data-dir=/tmp/opencode/pinetree-smoke/data --listen=127.0.0.1:3000`
  (PID 440249, log `/tmp/opencode/pinetree-smoke/logs/pinetree.log` —
  no errors/panics).
- API smoke `/tmp/opencode/keystone-smoke.py`: **30/30 green** on re-read
  (29/30 on first pass — see note). Covers: meta, signin ×2, bad-password
  401, `i` restore, bad-token 401, create ×2, local-timeline, home timeline
  after follow, note show, reply, react 204 + `myReaction`, unreact 204,
  quote (text+renoteId embeds renote), follow notifs, users/show by userId,
  users/search, cursor pagination, unknown-endpoint 404
  `UNKNOWN_API_ENDPOINT`, unsupported-field 400 `UNSUPPORTED_FIELD`,
  missing-note error, delete-own 204 ×2, deleted-note error, unfollow.
- New since Keystone's last 24/24 round-trip: `notes/global-timeline`
  returns 200 with both public notes (was 404 `UNKNOWN_API_ENDPOINT`);
  `withReplies` → 400 `UNSUPPORTED_FIELD` per frozen §8.
- Notification timing note: first immediate read showed 0 notifs for bob;
  re-read ~3s later showed follow+reaction+renote (bob) and reply (alice).
  Same async-worker race as the earlier Keystone `build.md` finding —
  backend correct, test artifact. No code change.

## Left running (browser-accessible)
- Pinetree: http://127.0.0.1:3000 (fresh smoke data, alice+bob as above)
- Keystone (vite dev): http://127.0.0.1:5173 (proxy `/api`+`/files` → :3000;
  signin through the proxy verified OK)
- Misskey/Sharkey servers: NOT started — testing moved to Keystone per
  owner; reference checkouts remain at `/root/fedifedifedi/`.

## Open
- Browser-visual pass (desktop + mobile, console errors) still needs a
  human or browser tooling — not available in this environment.
- Uncommitted global-timeline changes still uncommitted (owner's call).
