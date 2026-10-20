# pinetree

pinetree is a lightweight, single-binary **Misskey-compatible social network
server** written in Rust. Existing Misskey clients talk to it through a
deliberately chosen, contract-tested subset of the Misskey API (plus
streaming), and it federates over ActivityPub with Misskey, Sharkey, and other
fediverse software.

It is deliberately **small to run**: no Postgres, no Redis, no message broker.
One binary, one embedded database file (`redb`), local file storage, done.

pinetree does not come with a frontend. Point any Misskey-compatible web
client at it — for example [Keystone](https://github.com/estyseesghosts/keystone)
(which targets pinetree as a backend) or [KissMe](https://github.com/estyseesghosts/kissme).

> **Status (2026-10-20):** the single-node server is done through client
> compatibility, federation, streaming, quote posts, branched replies, polls,
> post editing, drafts/scheduling, blocking/muting, private lists, pins,
> notification grouping/filtering, profile import/export, and a first batch of
> moderation + registration tooling. Multi-node clustering (FoundationDB) is
> still planned, not built. Details below — including what is *not* wired up
> yet.

---

## Table of contents

- [What it does today](#what-it-does-today)
- [What it does not do](#what-it-does-not-do)
- [Known gaps and rough edges](#known-gaps-and-rough-edges)
- [Where things stand](#where-things-stand)
- [Run your own server](#run-your-own-server)
  - [Prerequisites](#prerequisites)
  - [Build it](#build-it)
  - [Start it](#start-it)
  - [Create your first account](#create-your-first-account)
  - [Log in and post](#log-in-and-post)
- [Use it day-to-day](#use-it-day-to-day)
- [What it talks to](#what-it-talks-to)
- [Project layout](#project-layout)
- [Testing it yourself](#testing-it-yourself)
- [Contributing & reading more](#contributing--reading-more)
- [License](#license)

---

## What it does today

Everything in this section runs on the single-node server and is covered by
executed tests (see [Testing it yourself](#testing-it-yourself)). Roughly 95
JSON API endpoints answer at `POST /api/{endpoint}`; anything not on the list
returns `404 UNKNOWN_API_ENDPOINT`, and unknown fields on listed endpoints
return `400 UNSUPPORTED_FIELD`. The exact contract is
`docs/endpoint-manifest.md`.

**Accounts, auth, and registration:**

- Signup / signin with per-token permissions, rate limits with Misskey-style
  `429` + `retryAfter`.
- MiAuth web flow (`miauth/gen-token`, `GET|POST /miauth/{session}[/check]`)
  and legacy `auth/session/generate|show|userkey` + `auth/accept`.
- Registration policy: open signup, invite-only (`invite/create|list|delete|limit`,
  `admin/invite/create|list`), or admin-managed (`admin/accounts/create`,
  `disableRegistration` via `admin/meta` / `admin/update-meta`).
- Roles: admin and moderator (`set-role` CLI), `isAdmin` / `isModerator`
  packed on users, a moderation log (`admin/show-moderation-logs`).
- Suspension (`admin/suspend-user|unsuspend-user`): sign-in refused with
  `USER_SUSPENDED`, open streams closed, follows torn down in bounded sweeps.
- Full profile store: `i/update` accepts all 39 upstream profile params
  (name, description, location, birthday, avatar/banner, followedMessage,
  languages, muted words/instances, notification receive config, and more),
  plus pinetree's own `rejectQuotes` and `defaultReaction` toggles.
- Self-delete (`i/delete-account`) and admin delete (`admin/delete-account`),
  admin user views (`admin/show-user(s)`, `admin/update-user-note`).

**Notes and timelines:**

- Create (text, content warning, `public` / `home` / `followers` /
  `specified` visibility, replies, renotes, file attachments, `localOnly`),
  show, delete, unrenote.
- **Polls** with voting (`notes/polls/vote`), expiry, and federation as
  `Question` (+ `pollVoted` events).
- **Post editing** in the Sharkey `notes/edit` shape (reply immutable,
  visibility forced back, poll add/replace resets votes, version history),
  plus `edited` notifications and federated `Update(Note)`.
- **Quote posts** (FEP-044f canonical + legacy compat + request/approval
  workflow; quoter-side `rejectQuotes` enforcement, remote strip-to-plain-note
  fallback) and **branched reply threads** that stay intact when the parent
  arrives late (pending parents fail closed until the resolver links them).
- Home / local / hybrid / global timelines with cursor pagination,
  `users/notes`, `notes/replies|renotes|children|conversation`,
  `notes/search|search-by-tag` (substring scan over local notes),
  `notes/featured`, `users/featured-notes`.
- Read-time enforcement everywhere: blocked/muted/suspended authors,
  instance blocks, and renote-mutes are filtered on every timeline and note
  read.

**Reactions, follows, and social graph:**

- Reactions create/delete with Misskey normalization (bare-`❤` fallback,
  `likeOnly`, legacy mapping); per-user `defaultReaction` so a bare heart-tap
  resolves server-side; full reaction list via `notes/reactions`.
- Follow / unfollow, follow requests to locked accounts
  (`following/requests/list|accept|reject|cancel`).
- Blocking, muting, renote-muting (`blocking/*`, `mute/*`, `renote-mute/*`,
  `users/relation`), enforced on follow/react/vote/create and on every read.
- Private followers/following lists (self / follower / public gating with
  `FORBIDDEN` + count masking) and private reactions (`users/reactions`).
- Pins (`i/pin|unpin`, limit 5, federated) and a documented “compatible,
  simplified” featured-notes ranking (not upstream-identical).

**Notifications, drafts, import/export:**

- `i/notifications` (cursor, `markAsRead`, include/exclude type filters,
  creation-time receive-config gates, muting/self drops),
  `i/notifications-grouped` (reaction/renote grouping, pure pack-layer),
  `notifications/mark-all-as-read`.
- Drafts + scheduled posts (`notes/drafts/create|list|update|delete|count`;
  scheduling = `scheduledAt` on a draft, published by a durable job with
  `scheduledNotePosted/Failed` notifications).
- Profile import/export: 8 exports (`following`, `blocking`, `mute`, `notes`,
  `antennas`, `clips`, `favorites`, `user-lists`) + 5 imports with upstream
  error codes and an `exportCompleted` notification. (Entities pinetree has no
  model for — user lists, clips — export empty valid files and import zero
  rows; antennas and favorites export/import real rows. That is documented,
  not silent.)

**Streaming:**

- Authenticated WebSocket `/streaming` (`?i=` or Bearer): `main`,
  `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline`,
  `antenna` (owner-scoped `params.antennaId`, `note` frames for matches).
- Note capture (`subNote`/`unsubNote` + aliases, `reacted`/`unreacted`/`deleted`
  with the visibility gate), `readNotification`, `pollVoted`, `edited` note
  updates, unknown channels answered with `UNSUPPORTED_CHANNEL`.
- Suspended users' sockets are closed server-side.

**Media:**

- Multipart/JSON upload (`drive/files/create`), listing/show/update/delete,
  public serving at `GET /files/{id}`, local filesystem blob store.
- Remote attachments are validated and cached (scheme, SSRF, size limit,
  MIME sniff). No folders: `folderId` must be null.

**Federation (ActivityPub):**

- WebFinger (JRD + XRD), host-meta, NodeInfo 2.0/2.1, stable actor documents
  with RSA keys, outbox/followers/following/featured collections, note
  documents (tombstone `410` for deleted).
- Shared + personal inboxes. Digest + legacy HTTP Signatures (`rsa-sha256`)
  verified inbound and emitted outbound; 64 KiB body cap, ±5 min `Date` skew,
  actor/object authority checks, atomic activity-ID deduplication (in the same
  transaction as the state change).
- Activities: Follow / Accept / Reject / Undo, Create(Note), Update (edits and
  poll votes), Delete, Like with `_misskey_reaction`, Announce, `Question`
  (polls), FEP-044f quote authorization on the pinetree↔pinetree path.
  Inbound `EmojiReact` (Akkoma/Pleroma-family) is accepted through the Like
  path.
- Durable outbound delivery with exponential backoff, per-destination limits,
  shared-inbox dedup; terminally failed jobs are quarantined with a logged
  reason instead of retrying forever.
- Instance blocks (`blockedHosts`): subdomain-aware, enforced on inbound
  (drop before state write) and at delivery time outbound.
- Federation input is treated as untrusted: size/timeout/redirect limits,
  SSRF protection (private IPs blocked by default), HTML/MFM sanitization.

**Durability:**

- Every local state change commits together with its outbox/job record in one
  storage transaction; workers do notifications, streaming fan-out, and
  delivery. Jobs are idempotent, deduplicated, retryable, and crash-safe
  (crash-recovery tests).

---

## What it does not do

Deliberate non-goals (proposal §1) and deferred work — asking for these
returns `404 UNKNOWN_API_ENDPOINT` (API) or `UNSUPPORTED_CHANNEL` (streaming)
rather than a broken half-implementation:

- **No multi-node mode.** `--mode=cluster` refuses to start
  (`cluster mode is not implemented (Phase 4)`). One process, one embedded
  `redb` file, no FoundationDB adapter, no account directory, no cross-node
  routing. That is Phase 4, not started.
- **No bundled web client.** Use Keystone, KissMe, or any Misskey-compatible
  client.
- **No Mastodon client REST API.** ActivityPub federation *with*
  Mastodon-family servers stays in scope; the Mastodon API itself is a
  non-goal (Sharkey ships one; pinetree never will unless the proposal
  changes).
- **Custom emoji are instance-curated.** Public `emojis`/`emoji` listing,
  `admin/emoji/add|update|delete|list` (moderator), shortcode registry with
  case-insensitive uniqueness, remote-emoji opt-in per user
  (`showRemoteEmojis`, metadata ingest + lazy fetch). Foreign `:shortcode:`
  reactions store verbatim. Outbound reactions to Akkoma/Pleroma-family hosts
  degrade to a plain `Like` (❤ favorite) until an `EmojiReact` emit shape
  passes a live round-trip test.
- **User lists shipped (Batch O WP16).** `users/lists/*` (12 endpoints),
  `notes/user-list-timeline` (owner-only pull read), `userList` streaming
  channel (`userAdded`/`userRemoved`/notes), list notification
  receive-configs, CSV export/import, and antenna `src=list` now resolves
  through the membership index. Limits enforced + advertised
  (`userListLimit 10`, `userEachUserListsLimit 50`). Remote members queue a
  system-actor Follow. `create-from-public` stays 404.
- **No clips, channels, pages, drive folders, chat,
  Reversi, or search-by-hashtag.** No featured timelines,
  no `drive` streaming channel,
  no `signin-flow`, no 2FA/passkeys/webhooks.
  Antennas, favorites, custom emoji, user lists, mentions timeline,
  notification utilities, discovery, versions, and translate stubs shipped
  (Batch N + O + P).
- **No OAuth.** MiAuth + API tokens only (OAuth lands only if a target client
  requires it).
- **No thread muting, no RFC 9421 or Linked-Data
  signatures,** no `_misskey_content` (never emitted, by decision), no polls
  over deleted/expired edge beyond documented errors. Account move (`i/move`
  + inbound `Move`) ships in Batch P — verified live between two pinetrees.
- **Self-service, follower-removal, remote lookup, drive extras, and
  url-preview shipped (Batch O WP9/WP10/WP12/WP13).**
  `i/change-password`, `i/regenerate-token` (native rotation; app grants
  survive), `i/apps`, `i/authorized-apps`, real `i/revoke-token{tokenId}`,
  `i/signin-history` (deploy-forward rows, capped at 100/user),
  `following/invalidate` (followee-authored Reject to remote followers),
  `users/search-by-username-and-host`, `ap/get` (credentialed safe subset
  of `ap/show`), `drive/files/upload-from-url`,
  `drive/files/find-by-hash`, first-party `url-preview`.
- **Email identity + discovery batch shipped (Batch P).**
  `i/update-email`, `request-reset-password`, `reset-password`,
  `verify-email` (code-complete, dormant: no SMTP configured — sends fail
  loudly, never silently), `i/move` + inbound `Move` (alias handshake,
  follower migration, replay/forgery covered), `i/registry/*` (8 endpoints),
  `get-online-users-count` (streaming-presence definition), `pinned-users`
  (`[]`), `fetch-external-resources`, `fetch-rss` (documented subset),
  `server-info`, `stats` (real counts), `notes/mentions`,
  `notifications/test-notification`, `notifications/flush` (mark-read +
  event), `users/recommendation`, `notes/recommendations` (simplified
  scoring), `notes/versions` (Sharkey shape), `notes/translate`
  (dormant: no backend — loud failure).
- **Abuse reporting is wired up** (`users/report-abuse` + the
  `admin/abuse-user-reports` family with `Flag` forwarding); admins poll the
  list (no mail/webhook notifications).

---

## Known gaps and rough edges

Things that work but with documented limits you may trip over:

- **Loopback-tested only.** Interop is verified against real servers in local
  labs (see below), never against the live public internet. SSRF protection
  blocks private IPs by default; loopback federation needs
  `PINETREE_ALLOW_PRIVATE_IPS=1`.
- **Single-node storage.** One writer (`redb`), no read replicas, no live
  migration path to multi-node yet (export/import covers accounts, follows,
  blocks, mutes, and notes — not a transparent upgrade).
- **Search is a bounded substring scan** over cached notes, not an index.
  `notes/reactions` pages newest-first on `sinceId`-only cursors (documented
  divergence from Misskey's ascending order).
- **Featured notes use a simplified ranking** (recent non-sensitive public
  notes scored by reactions + renotes) — compatible in shape, not identical to
  upstream.
- **Actor RSA keys generate lazily on first use** — slow in debug builds, so
  the first remote fetch of a new account can time out for peers.
- **Word mutes are server-enforced** (owner-approved divergence from
  upstream, which enforces them client-side only): `hardMutedWords` drop
  notes on every read; `mutedWords` pack a viewer-specific `isSoftMuted`
  flag (true only when matched and the note has no CW of its own). See
  `docs/word-mutes.md`.
- **Private follow lists have no per-user visibility knobs yet** — the profile
  stores the fields, lists gate on the documented rule (self/follower/public).
- **No per-destination delivery tuning, no timeline indexes beyond the
  reactions-by-note index, minimal observability** (stderr diagnostics only).
  That is Phase 5 work and starts only with measurements.
- **Benchmarks are debug-build, single-node numbers** (~870 notes-create/s,
  ~430 timeline/s, ~1200 users-show/s) — a rough guide, not a promise.

---

## Where things stand

| Phase | What it was | Status |
|---|---|---|
| 0 | Freeze the contracts (API manifest, identity rules, storage tests) | Done |
| 1 | Single-node server (accounts, notes, follows, reactions, drive, jobs) | Done |
| 2 | Federation (WebFinger, actors, signatures, inboxes, delivery, dedup) | Done |
| 3 | Client compatibility & streaming (live WebSocket, richer API, real-client contract tests) | Done |
| — | Quote posts (FEP-044f) and branched replies on top | Done |
| — | Misskey parity batch: polls, editing, drafts/scheduling, blocks/mutes, private lists, pins, notification grouping/filtering, import/export | Done |
| — | Moderation batch (Batch M, pre-Phase 4): roles, registration/invites, suspension, instance blocks, admin views; abuse-report plumbing below the API layer | Mostly done — WP1–WP6 landed, WP7+ pending |
| — | Daily-use batch (Batch N): favorites + note state, custom emoji (+ remote opt-in), `followRequestAccepted`, antennas, abuse-report routes, admin remainder, word mutes (server-side), MiAuth hardening, Sharkey schedule shim | Done (this session) — `url-preview`, self-service, follower removal, user lists still pending |
| 4 | Multi-node on FoundationDB | Planned — not started |
| 5 | Scale & hardening | Planned |

The single-node server is **ready to run today**. Multi-node mode refuses
to start until Phase 4 lands (that is intentional).

---

## Run your own server

### Prerequisites

1. **Rust** (recent stable). Grab it from [rustup](https://rustup.rs/) if you
   don't have it.
2. That's it for running the server itself.

> If you want to run the full test suite or the interop labs later, you'll
> also want `bun`, Docker (with the compose plugin), Node 24, and Node 22.
> But to *run a pinetree*, you only need Rust.

### Build it

```bash
cargo build --release
```

This produces a single binary at `target/release/pinetree`.

> **If you're on WSL:** keep your checkout on the native Linux filesystem
> (e.g. `/home/you/pinetree`, not `/mnt/c/...`). The 9P filesystem is several
> times slower for builds and for the database. You can also set a custom
> target dir in `.cargo/config.toml` (git-ignored) if you want the build
> artifacts on a fast drive.

### Start it

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

- `--public-url` is the URL the world (and other servers) will see. It must
  start with `http://` or `https://` and include a host. Actor URLs, note URLs,
  and WebFinger will all be built from it.
- `--data-dir` is where the embedded database (`pinetree.redb`) and any
  uploaded files live. Point it at a directory you can back up.
- `--listen` is the address the HTTP server binds to. Use `127.0.0.1:3000` to
  keep it local-only, or `0.0.0.0:3000` to expose it on your network.

Every one of these has a JSON-file equivalent. Point `--config` at a file
and it becomes the base configuration; CLI flags override the file, and
`validate()` runs once over the merged result. Every key is optional —
absent keys take the compiled defaults — and unknown keys are rejected
(typo safety):

```json
{
  "public_url": "https://social.example.com",
  "data_dir": "./data",
  "listen": "127.0.0.1:3000",
  "max_file_size": 262144000,
  "trusted_proxies": ["10.0.0.1"],
  "note_draft_limit": 10,
  "scheduled_note_limit": 1,
  "pin_limit": 5
}
```

```bash
target/release/pinetree --config=/etc/pinetree.json --listen=0.0.0.0:3000
```

The remaining flags (`--mode`, `--trusted-proxies`, `--max-file-size`,
`--preserved-usernames`, `--prohibited-words`, `--sensitive-words`,
`--note-draft-limit`, `--scheduled-note-limit`, `--pin-limit`,
`--debug-federation`, `--debug-weak-rate-limits`,
`--federation-extra-ca`) override the same keys.

If you're just trying it out locally, this works too:

```bash
target/release/pinetree \
  --public-url=http://127.0.0.1:3000 \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

> By default, federation requests to private/internal IPs are blocked (SSRF
> protection). For local-only federation testing between servers on your
> machine, you can opt in with `PINETREE_ALLOW_PRIVATE_IPS=1`. In production
> you want the block on.

### Create your first account

You can create accounts from the command line:

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  create-account --username=alice --password=a-good-password
```

(Yes, the `create-account` subcommand goes after the server flags — it exits
after creating the account instead of starting the server. Registration policy
— open, invite-only, or admin-only — is managed afterwards via
`admin/update-meta` and the invite endpoints.)

To make yourself an admin (or moderator):

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  set-role --username=alice --admin
```

(`--moderator`, and `=false` to revoke, work the same way.)

### Log in and post

pinetree supports a few ways to authenticate:

**Via the API (signin):**

```bash
curl -s -X POST http://127.0.0.1:3000/api/signin \
  -H 'content-type: application/json' \
  -d '{"username":"alice","password":"a-good-password"}'
```

This returns `{"i": "your-api-token"}`. Use that token as `i` in any API
request, or as `Authorization: Bearer <token>`.

**Via the web (MiAuth):** open `http://127.0.0.1:3000/miauth/<session>` in a
browser after generating a token with `POST /api/miauth/gen-token`.

**Via a Misskey client:** point your favourite Misskey-compatible client at
your server's URL and log in with the username/password above.

Then post a note:

```bash
curl -s -X POST http://127.0.0.1:3000/api/notes/create \
  -H 'content-type: application/json' \
  -H 'Authorization: Bearer your-api-token' \
  -d '{"text":"hello from pinetree","visibility":"public"}'
```

---

## Use it day-to-day

Once it's running, you interact with pinetree the same way you'd interact
with any Misskey instance:

- **Post:** `POST /api/notes/create` with `text`, optional `visibility`,
  `cw` (content warning), `replyId`, `renoteId`, `fileIds`, `localOnly`,
  `poll`. Edit with `notes/edit`, vote with `notes/polls/vote`.
- **Timelines:** `POST /api/notes/timeline`, `notes/local-timeline`,
  `notes/hybrid-timeline`, `notes/global-timeline`; thread views via
  `notes/replies|renotes|children|conversation`; `notes/search`,
  `notes/search-by-tag`.
- **Reactions:** `POST /api/notes/reactions/create` with `noteId` + `reaction`
  (e.g. `"👍"`); omit `reaction` to use your `defaultReaction`.
- **Follows:** `POST /api/following/create` with `userId`; locked accounts go
  through `following/requests/*`. Block/mute via `blocking/*`, `mute/*`,
  `renote-mute/*`.
- **Drafts:** `notes/drafts/create|list|update|delete|count`; add `scheduledAt`
  to publish later.
- **Portability:** `i/export-*` / `i/import-*` for follows, blocks, mutes,
  notes, and more.
- **Streaming:** connect a WebSocket to `ws://<host>/streaming?i=<token>` (or
  with a Bearer header). Channels include `main`, `homeTimeline`,
  `localTimeline`, `hybridTimeline`, and `globalTimeline`.
- **Drive:** `POST /api/drive/files/create` (multipart) to upload; files are
  served at `GET /files/<id>`.

The full API surface is documented in `docs/endpoint-manifest.md` (the frozen
contract plus dated superseding notes) and `docs/phase-3-profile.md` (the
compatibility profile). Anything not in that list returns
`404 UNKNOWN_API_ENDPOINT`.

---

## What it talks to

pinetree federates over ActivityPub and has been tested against real software
in local (loopback/docker) labs — never against the public internet so far:

- **Misskey** (2026.10.1-alpha.0) and **Sharkey** (2025.4.7): follow, post,
  react, reply, quote, and delivery flows in a live four-server interop lab
  (two pinetrees + real Misskey + real Sharkey).
- **Akkoma / Pleroma-family, Mastodon, Catodon, and a reference implementation:**
  broader multi-family bench harness (`scripts/fediverse-bench/`) covering
  follows, feature interop (notes, polls, reactions, quotes), and stress runs.
  Known standing limits from those runs: outbound emoji reactions to
  Akkoma-family hosts degrade to favorites; foreign custom emoji never
  renders.

It speaks the core activity types (Follow, Accept, Reject, Undo, Create,
Update, Delete, Like, Announce, Question), WebFinger, NodeInfo, HTTP
Signatures, and FEP-044f quote authorization (full workflow between
pinetrees; legacy-compatible with Misskey/Sharkey).

---

## Project layout

```
.
├── sns-server-proposal.md    the product & architecture plan (source of truth)
├── AGENTS.md                 rules for anyone (human or AI) working in this repo
├── docs/                     frozen contracts, phase plans, exit criteria, wiki
│   ├── endpoint-manifest.md  the Misskey API contract (+ superseding notes)
│   ├── phase-3-profile.md    the compatibility profile (target clients, scope)
│   ├── misskey-parity-plan.md  the parity batch (§§0–13 done; Batch M status)
│   ├── implementer-guide-batch-m.md  moderation/daily-use batch (WP1–WP6 done)
│   ├── testing.md            how to run every test suite
│   └── wiki/                 living overview docs (About, Current State, Roadmap)
├── src/                      the Rust crate
│   ├── domain/               pure types & rules (no HTTP, no DB)
│   ├── application/          use cases (commands, queries, ports)
│   ├── api/misskey/          HTTP API adapter (endpoints, packers, errors)
│   ├── activitypub/          ActivityPub protocol adapter
│   ├── federation/           inbox, outbox, delivery, retry
│   ├── persistence/redb/     the embedded database adapter
│   ├── streaming/            WebSocket server & channel hub
│   ├── jobs/                 durable job queue & worker
│   ├── media/                file blob storage
│   └── cluster/              Phase 4 multi-node (stub for now)
├── tests/                    conformance (100 cases incl. polls, edits,
│                             blocks, imports), federation, streaming,
│                             crash-recovery, real-client smoke tests
├── scripts/                  smoke-all.sh and friends (one-command test gates)
├── scripts/fediverse-bench/  multi-family federation bench harness
├── state/logs/               dated work logs with real command output
└── misskey, sharkey          symlinks to reference checkouts (git-ignored;
                              only needed for tests, not for running a server)
```

The code is a **modular monolith**: one crate, clear dependency direction
(`domain` ← `application` ← adapters). The Misskey API and the ActivityPub
adapters call the *same* application operations — no duplicated business
logic.

---

## Testing it yourself

The full verification gate is:

```bash
bash scripts/smoke-all.sh
```

That runs (in order):

1. `cargo test --offline` — the full Rust test suite. Currently **553 green**:
   424 unit tests, 100 storage-conformance cases on redb, and 29 integration
   tests (streaming over live WebSockets, federation between in-process nodes,
   crash recovery, and real-`misskey-js` client smoke tests).
2. `scripts/smoke-phase1.sh` — core flows driven by the real `misskey-js`
   client library.
3. `scripts/smoke-phase3.sh` — the compatibility profile endpoints and
   streaming via `misskey-js`.
4. `scripts/keystone-sweep.sh` — Keystone frontend's exact API calls (41
   checks, all passing).
5. `scripts/smoke-phase2.sh` — live four-server federation interop (real
   Misskey + real Sharkey + two pinetree instances). Needs Node 22 for the
   Sharkey side; it skips cleanly without it.

You can also run pieces individually:

```bash
cargo test --offline                          # Rust tests only
PINETREE_BIN=target/release/pinetree bash scripts/smoke-phase1.sh
```

See `docs/testing.md` for the full guide, including how to run the interop
rig and the multi-family bench harness by hand.

---

## Contributing & reading more

- `sns-server-proposal.md` — the product and architecture proposal. Wins on
  any disagreement with this README.
- `AGENTS.md` — the rules of the road for contributors (human or AI).
- `docs/` — the frozen contracts and the living wiki.
- `state/logs/` — dated work logs with real output. Read the latest entry to
  get up to speed.

If you want to help, the most useful things are: trying it out, running the
test suite, and filing issues when something doesn't work. The Batch M
remainder (WP7+: favorites, emoji, self-service, follower removal, URL
preview, drive extras) and then Phase 4 (multi-node) are the open frontiers.

---

## License

Not yet declared — there is no `LICENSE` file yet. Ask before assuming
anything.
