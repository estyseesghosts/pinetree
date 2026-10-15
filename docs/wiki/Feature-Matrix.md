# Feature matrix: pinetree vs Misskey vs Sharkey

Compared versions: **pinetree** after the Phase 2.5 cleanup (2026-10-15); **Misskey** `2026.10.1-alpha.0` and **Sharkey** `2025.4.7` (the read-only checkouts `./misskey`, `./sharkey`). Misskey/Sharkey cells reflect what exists in those checkouts (endpoint and module presence was checked by listing the source tree; behaviour depth was not audited). pinetree cells reflect executed tests only.

Legend: ✅ supported · 🟡 partial/subset · ❌ not supported (yet) · ⛔ deliberate non-goal · 🗓 planned (phase)

## Platform

| | pinetree | Misskey | Sharkey |
|---|---|---|---|
| Language / runtime | Rust, single binary | TypeScript / Node (NestJS, Fastify) | TypeScript / Node (Misskey fork) |
| Database | embedded `redb` ✅; FoundationDB multi-node 🗓 4 | PostgreSQL | PostgreSQL |
| Redis / broker required | no | yes (Redis) | yes (Redis) |
| Single-user / small-instance footprint | designed for it | heavy (Postgres + Redis) | heavy |
| Multi-node as one logical instance | 🗓 4 (FoundationDB) | via external DB/Redis | via external DB/Redis |
| Built-in web client | ❌ (uses existing Misskey clients) | ✅ | ✅ |
| Public API size | ~28 endpoints | ~440 endpoint modules | ~460 endpoint modules |

## Client API

| | pinetree | Misskey | Sharkey |
|---|---|---|---|
| Misskey JSON API (`/api/*`, `i`/Bearer) | 🟡 core subset, contract-tested | ✅ | ✅ |
| MiAuth | ✅ | ✅ | ✅ |
| Legacy `auth/session/*` | ✅ | ✅ | ✅ |
| OAuth / IndieAuth | ❌ (🗓 only if a target client needs it) | ✅ | ✅ |
| Mastodon client API | ⛔ | ❌ | ✅ |
| Streaming (`/streaming`, channels, note capture) | ❌ 🗓 3 | ✅ | ✅ |
| Timelines home / local / hybrid | ✅ | ✅ | ✅ |
| Global timeline | ❌ 🗓 3 | ✅ | ✅ |
| Other timelines (user lists, hashtag, antenna, role, channel, bubble) | ❌ | ✅ (no bubble) | ✅ (incl. bubble) |
| Notes: text, CW, visibility, reply, renote, files, localOnly | ✅ | ✅ | ✅ |
| Quote notes | ❌ 🗓 3+ (only if in profile) | ✅ | ✅ |
| Polls | ❌ (deferred) | ✅ | ✅ |
| Note editing | ❌ | ❌ | ✅ |
| Reactions (unicode) | ✅ | ✅ | ✅ |
| Custom-emoji reactions / emoji management | ❌ | ✅ | ✅ |
| Notifications list | ✅ (list) | ✅ | ✅ |
| Follow / unfollow | ✅ | ✅ | ✅ |
| Follow requests (locked accounts) | 🟡 locked flag respected; request endpoints ❌ | ✅ | ✅ |
| Mute / block / renote-mute | ❌ | ✅ | ✅ |
| User lists, antennas, clips, pages, flash, gallery, channels | ❌ | ✅ | ✅ |
| Drive: upload | ✅ | ✅ | ✅ |
| Drive: folders, listing, management | ❌ 🗓 3 as needed | ✅ | ✅ |
| Search (notes/users) | 🟡 `users/search` only | ✅ | ✅ |
| Profile edit | 🟡 subset (`name`, `description`, `location`, `birthday`, avatar/banner, flags) | ✅ | ✅ |
| 2FA / passkeys / webhooks | ❌ | ✅ | ✅ |
| Roles / moderation / admin API | ❌ (admin account creation only) | ✅ | ✅ |
| Chat, Reversi, translation, etc. | ⛔ legacy/extra | ✅ | ✅ |
| Rate limiting (Misskey-style 429 + retryAfter) | ✅ | ✅ | ✅ |

## ActivityPub federation

| | pinetree | Misskey | Sharkey |
|---|---|---|---|
| WebFinger (JRD + XRD), host-meta | ✅ | ✅ | ✅ |
| NodeInfo | ✅ 2.0 / 2.1 | ✅ | ✅ |
| Actor docs, public key, collections | ✅ | ✅ | ✅ |
| Shared + personal inbox | ✅ | ✅ | ✅ |
| Legacy HTTP Signatures (cavage) + Digest | ✅ | ✅ | ✅ |
| RFC 9421 message signatures | ❌ (deferred) | 🟡 | 🟡 |
| Linked-Data signatures | ❌ | 🟡 | 🟡 |
| Follow / Accept / Reject / Undo | ✅ (live-tested vs both) | ✅ | ✅ |
| Create(Note), Update, Delete (Tombstone) | ✅ | ✅ | ✅ |
| Like with `_misskey_reaction` | ✅ | ✅ | ✅ |
| Announce (renote) + Undo | ✅ | ✅ | ✅ |
| Quote (`_misskey_quote` / `quoteUrl`) | ❌ (follow-up) | ✅ | ✅ |
| Polls (`Question`, `_misskey_votes`) | ❌ (deferred) | ✅ | ✅ |
| `_misskey_content` | never emitted | legacy | legacy |
| Move / Block / Flag | ❌ | ✅ | ✅ |
| Atomic activity-id dedup | ✅ (same transaction as the state change, including Reject) | job-level | job-level |
| Durable outbound delivery + backoff | ✅ (outbox in the same transaction as the post; terminal quarantine + retry caps) | ✅ (BullMQ) | ✅ (BullMQ) |
| SSRF / size / redirect / timeout limits | ✅ | ✅ | ✅ |
| Remote media validation + cache | ✅ | ✅ | ✅ |
| Verified interop | ✅ vs real Misskey + Sharkey (loopback) | — | — |
| Public-internet federation tested | ❌ | ✅ | ✅ |

## Reading the matrix

pinetree intentionally implements a *subset*: the goal is "existing Misskey clients work against a small, fast, easy-to-run server" rather than feature parity. Phase 3 widens the client API and adds streaming for a declared client profile; Phase 4 adds the multi-node FoundationDB mode that neither Misskey nor Sharkey offer natively; Phase 5 measures and hardens. See [Roadmap](Roadmap.md).
