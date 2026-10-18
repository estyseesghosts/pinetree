# pinetree

A small, friendly **social network server** written in Rust that speaks the
same language as [Misskey](https://misskey-hub.net/) — both its client API and
its federation protocol (ActivityPub). Post notes, follow people, react, quote,
reply in threads, stream updates live, and chat with other servers across the
fediverse.

It is deliberately **lightweight**: no Postgres, no Redis, no message broker.
One binary, one embedded database file, done.

> **Status:** Actively in development. Phases 0–3 are complete and
> green-tested (single-node server, federation with real Misskey/Sharkey,
> streaming, quote posts, branched replies). The next phase adds multi-node
> clustering on FoundationDB. See [Where things stand](#where-things-stand).

---

## Table of contents

- [What it does](#what-it-does)
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

## What it does

pinetree is a fediverse server that Misskey clients can talk to. Concretely,
that means:

**For people using it (via a Misskey client, the API, or MiAuth web login):**

- Sign up, sign in, get an API token.
- Post notes — text, content warnings, visibility (`public` / `home` /
  `followers` / `specified`), replies, renotes, **quote posts**, and file
  attachments.
- Branched reply threads that stay intact even when the original parent
  arrives late from a remote server.
- React with emoji, follow people, send follow requests to locked accounts.
- Browse home / local / hybrid / global timelines.
- Get notified about mentions, replies, reactions, and follows.
- **Stream** live updates over a WebSocket (new notes, reactions, etc.).
- Upload and manage drive files.

**For other servers (federation / ActivityPub):**

- WebFinger, NodeInfo, actor documents with RSA keys.
- Shared and personal inboxes with HTTP Signatures.
- Follow / Accept / Reject / Undo, Create / Update / Delete / Like / Announce.
- Durable delivery with retry and backoff — a remote hiccup never loses a post.
- Quote-post authorization (FEP-044f) between pinetree instances.
- Verified against real Misskey, Sharkey, Pleroma, and Akkoma in local interop
  tests.

---

## Where things stand

| Phase | What it was | Status |
|---|---|---|
| 0 | Freeze the contracts (API manifest, identity rules, storage tests) | Done |
| 1 | Single-node server (accounts, notes, follows, reactions, drive, jobs) | Done |
| 2 | Federation (WebFinger, actors, signatures, inboxes, delivery, dedup) | Done |
| 3 | Client compatibility & streaming (live WebSocket, richer API, real-client contract tests) | Done |
| — | Quote posts (FEP-044f) and branched replies on top | Done |
| 4 | Multi-node on FoundationDB | Planned — next up |
| 5 | Scale & hardening | Planned |

The single-node server is **ready to run today**. Multi-node mode will refuse
to start until Phase 4 lands (that is intentional).

---

## Run your own server

### Prerequisites

1. **Rust** (recent stable). Grab it from [rustup](https://rustup.rs/) if you
   don't have it.
2. That's it for running the server itself.

> If you want to run the full test suite or the interop labs later, you'll
> also want `bun`, Docker (with the compose plugin), Node 24, and Node 22.
> But to just *run a pinetree*, you only need Rust.

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
after creating the account instead of starting the server.)

To make yourself an admin:

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  set-role --username=alice --admin=true
```

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
  `cw` (content warning), `replyId`, `renoteId`, `fileIds`, `localOnly`.
- **Timelines:** `POST /api/notes/timeline`, `notes/local-timeline`,
  `notes/hybrid-timeline`, `notes/global-timeline`.
- **Reactions:** `POST /api/notes/reactions/create` with `noteId` + `reaction`
  (e.g. `"👍"`).
- **Follows:** `POST /api/following/create` with `userId`.
- **Streaming:** connect a WebSocket to `ws://<host>/streaming?i=<token>` (or
  with a Bearer header). Channels include `main`, `homeTimeline`,
  `localTimeline`, `hybridTimeline`, and `globalTimeline`.
- **Drive:** `POST /api/drive/files/create` (multipart) to upload; files are
  served at `GET /files/<id>`.

The full API surface is documented in `docs/endpoint-manifest.md` (the frozen
contract) and `docs/phase-3-profile.md` (the compatibility profile). Anything
not in that list returns `404 UNKNOWN_API_ENDPOINT`.

---

## What it talks to

pinetree federates over ActivityPub and has been tested against real:

- **Misskey** (2026.10.1-alpha.0)
- **Sharkey** (2025.4.7)
- **Pleroma / Akkoma** (via the local-federation test rig)

It supports the core activity types (Follow, Accept, Reject, Undo, Create,
Update, Delete, Like, Announce), WebFinger, NodeInfo, HTTP Signatures, and
FEP-044f quote authorization between pinetree instances.

---

## Project layout

```
.
├── sns-server-proposal.md    the product & architecture plan (source of truth)
├── AGENTS.md                 rules for anyone (human or AI) working in this repo
├── docs/                     frozen contracts, phase plans, exit criteria, wiki
│   ├── endpoint-manifest.md  the Misskey API contract
│   ├── phase-3-profile.md    the compatibility profile (target clients, scope)
│   ├── testing.md            how to run every test suite
│   └── wiki/                 living overview docs (About, Current State, Roadmap)
├── src/                      the Rust crate
│   ├── domain/               pure types & rules (no HTTP, no DB)
│   ├── application/          use cases (commands, queries, ports)
│   ├── api/misskey/          HTTP API adapter
│   ├── activitypub/          ActivityPub protocol adapter
│   ├── federation/           delivery, retry, inbound processing
│   ├── persistence/redb/     the embedded database adapter
│   ├── streaming/            WebSocket server & channel hub
│   ├── jobs/                 durable job queue & worker
│   ├── media/                file blob storage
│   └── cluster/              Phase 4 multi-node (stub for now)
├── tests/                    conformance, federation, streaming, crash-recovery
├── scripts/                  smoke-all.sh and friends (one-command test gates)
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

1. `cargo test --offline` — the full Rust test suite (unit tests, storage
   conformance, federation, streaming, crash recovery, real-client smoke
   tests). Currently **351 tests green**.
2. `scripts/smoke-phase1.sh` — core flows driven by the real `misskey-js`
   client library.
3. `scripts/smoke-phase3.sh` — the compatibility profile endpoints and
   streaming via `misskey-js`.
4. `scripts/keystone-sweep.sh` — Keystone frontend's exact API calls (41
   checks).
5. `scripts/smoke-phase2.sh` — live four-server federation interop (real
   Misskey + real Sharkey + two pinetree instances).

You can also run pieces individually:

```bash
cargo test --offline                          # Rust tests only
PINETREE_BIN=target/release/pinetree bash scripts/smoke-phase1.sh
```

See `docs/testing.md` for the full guide, including how to run the interop
rig by hand.

---

## Contributing & reading more

- `sns-server-proposal.md` — the product and architecture proposal. Wins on
  any disagreement with this README.
- `AGENTS.md` — the rules of the road for contributors (human or AI).
- `docs/` — the frozen contracts and the living wiki.
- `state/logs/` — dated work logs with real output. Read the latest entry to
  get up to speed.

If you want to help, the most useful things are: trying it out, running the
test suite, and filing issues when something doesn't work. Phase 4
(multi-node) is next if you want to dig into distributed systems work.

---

## License

Not yet declared — there is no `LICENSE` file yet. Ask before assuming
anything.
