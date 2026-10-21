# 2026-10-21 gaps final wiring 2/5: docs (manifest + TODOS + ports addenda)

Subagent `ses_e9fb3b408ffeqnIxtyU89pIK7J` (docs only); parent corrected
two inaccuracies against HEAD and commits. No `src/`/`tests/` touched.

## What landed

- `docs/endpoint-manifest.md`: `## §43` addendum (43.1–43.10) —
  drive/folders/stream/extras, following/thread-mute/memo/pages/
  create-from-public/partial-bulk/polls-recommendation/clips-alias,
  2FA/apps, misc, admin/federation/queue/recipients/send-email,
  gallery/flash/hashtags/announcements, webhooks/SMTP, streaming table
  + 4 DIVERGENCEs, transcode, federation deltas. Supersedes §39.2
  still-404 rows + §11/§30/§34 out-lines for shipped families.
- `docs/TODOS.md`: P1 items flipped to landed per family; `## OPEN`
  added (live interop + bench); attached-chat-messages, achievements/
  flashs/birthday/frequently-replied, notifications/create, page-push,
  producer-side debt left open.
- `docs/application-ports.md`: addendum listing what wiring 1/5
  actually landed vs proposed-only.

## Parent corrections (against the subagent draft)

- Channels/chat: draft said "built in-tree but unwired" — the code
  was REMOVED (2026-10-21 removal commit). Manifest §43 intro, TODOS
  entries fixed to REMOVED.
- Ports addendum: draft implied `RoleStore`/`RelayStore`/`AdStore`/
  `FederationInstanceStore`, `enumerate_jobs`/`clear_queue`, poll/
  reaction aggregates landed — they did NOT (admin persists via
  existing `RegistryStore`; the rest are proposed-only). Rewrote the
  section to landed-vs-proposed. `GalleryStore`/`AnnouncementStore`
  noted as command-module-only (not in ports); `ChannelStore`/
  `ChatStore` noted removed.
- Reverted environmental CRLF flip on
  `docs/remote-media-cache-plan.md` (0 content change; agent never
  touched it).

## Verification

Docs-only: no frozen text rewritten (manifest/ports pure appends;
TODOS checkbox flips per that file's conventions). No `cargo` impact.
