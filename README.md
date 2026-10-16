# pinetree

A lightweight, modular **Misskey-compatible social network server** written in Rust.
Existing Misskey clients talk to it through a deliberately chosen, contract-tested
subset of the Misskey API (including streaming), and it federates over ActivityPub
with Misskey, Sharkey, Pleroma/Akkoma and other servers. Quote posts (FEP-044f)
and branched reply threads work both locally and across federation.

Status: **Phase 3 (client compatibility + streaming) exited 2026-10-16**, plus
quote posts and branched replies on top — all gate-green. Current phase is
**Phase 4 (FoundationDB multi-node)**. See [`docs/wiki/Current-State.md`](docs/wiki/Current-State.md) and `AGENTS.md`.

## Features

- Accounts and auth: signup/signin, MiAuth web flow, legacy `auth/session/*`,
  per-token permissions, Misskey-style rate limits.
- Notes (~50 Misskey API endpoints): create/show/delete, replies, renotes,
  **quote posts** (canonical FEP-044f `quote` + legacy `_misskey_quote`/`quoteUrl`/
  `quoteUri` compat + authorization workflow), reactions, follows + follow
  requests (locked accounts), home / local / hybrid / global timelines,
  `notes/replies|renotes|children|conversation`, search, notifications, drive
  upload + listing + file serving.
- **Streaming**: authenticated WebSocket `/streaming` with `main`, `homeTimeline`,
  `localTimeline`, `hybridTimeline`, `globalTimeline` channels and note capture
  (`subNote`/`unsubNote`, reacted/unreacted/deleted events).
- **Replies as real threads**: indexed direct-child lookup (no global scans),
  cursor paging that stays full past hidden rows, exact `replies_count`
  accounting through delete, and remote threads that survive unknown parents
  (unresolved `inReplyTo` is kept pending and fail-closed — never flattened
  into a root — then linked by a durable resolver when the parent arrives).
- Federation: WebFinger, NodeInfo, actor documents with RSA keys, shared and
  personal inboxes, legacy HTTP Signatures, Follow/Accept/Reject/Undo,
  Create/Update/Delete/Like/Announce, atomic activity-id deduplication, durable
  outbound delivery with retry, backoff and terminal quarantine.
- Durability: every state change commits together with its outbox/job record in
  one storage transaction (embedded `redb`, single node). FoundationDB
  multi-node mode is Phase 4 (adapter still a stub; the persistence contract
  suite is backend-generic so it inherits every case).
- No SQL, Redis, or message broker. One Rust binary.

## Layout

```
sns-server-proposal.md   product/architecture proposal (source of truth)
README.md                this file
AGENTS.md                rules for agents working in this repo
docs/                    frozen contracts, phase plans, exit criteria, specs, wiki
docs/quote-posts.md      quote-posts implementation guide (FEP-044f)
docs/replies.md          branched-replies implementation guide
state/logs/              dated append-only work logs (real command output)
src/                     the crate: domain <- application <- adapters
tests/                   conformance (+ Groups Q/R), crash_recovery, client_smoke,
                         client_profile, federation, federation_interop,
                         federation_elixir, quote_fixtures, streaming, minrepro
scripts/                 smoke-all.sh, smoke-phase1/2/3.sh, keystone-sweep.sh,
                         bench-local.py, interop/ helpers
misskey, sharkey         symlinks to read-only reference checkouts (recreate
                         these yourself; see Prerequisites)
foundationdb/, redb/     upstream reference checkouts (read-only, git-ignored)
```

## Prerequisites

- Recent stable Rust (`cargo build` / `cargo test`; `redb` comes from crates.io,
  no extra step).
- For the test suites: `bun` plus the `./misskey` checkout (live `misskey-js`
  client smoke test and packer snapshot tests), and for the interop rigs:
  `docker` with the compose plugin, Node 24 (Misskey) **and** Node 22 (Sharkey
  rejects Node 24 via its `engines` field — pass
  `SHARKEY_NODE_BIN=<dir containing a Node 22 binary>`). `nvm` handles this:
  `nvm install 24` (default) plus system Node 22, then
  `SHARKEY_NODE_BIN=/usr/sbin scripts/smoke-phase2.sh`.
- The Pleroma/Akkoma triangle (`tests/federation_elixir.rs`) needs the
  `local-federation` rig plus a `pinetree.test` node; see `docs/testing.md`.
- The pinetree binary lands in the cargo target dir (default `./target`,
  this machine `/root/pinetree-target` via git-ignored `.cargo/config.toml`).
  `scripts/smoke-*.sh` resolve it via `cargo metadata` (`PINETREE_BIN`
  overrides).
- Reference checkouts are git-ignored: after cloning, symlink (or clone) them as
  `./misskey` and `./sharkey` next to the repo, matching Misskey
  `2026.10.1-alpha.0` and Sharkey `2025.4.7`.
- Under WSL, keep heavy trees off `/mnt/c` (9P is several times slower for
  Node/Rust workloads): put the checkouts on the Linux filesystem
  (e.g. `/root/fedifedifedi/...`) and symlink to them, and build artifacts
  go to ext4 via the git-ignored `.cargo/config.toml` (`target-dir`;
  adjust the path if your checkout lives elsewhere).

## Quickstart

```bash
cargo build
<target>/debug/pinetree --public-url=http://127.0.0.1:3000 --data-dir=./data --listen=127.0.0.1:3000
<target>/debug/pinetree create-account --username=alice --password=secret --data-dir=./data
```

(`<target>` is `./target` by default, `/root/pinetree-target` on this
machine — see Prerequisites.)

`--mode=cluster` deliberately refuses ("not implemented (Phase 4)").
`PINETREE_ALLOW_PRIVATE_IPS=1` is an SSRF *testing* exemption for loopback
federation only; the default blocks private addresses, and TLS verification is
always on. `PINETREE_REPLY_REPAIR_LIMIT=<1..=5000>` opts into a bounded
re-fetch of historical unresolved reply parents at startup (off by default).

## Testing

```bash
cargo test --offline                             # full suite (351 green)
bash scripts/smoke-all.sh                        # all suites incl. live binaries (SMOKE_ALL_OK)
SHARKEY_NODE_BIN=<node22/bin> scripts/smoke-phase2.sh   # four-server interop: Misskey + Sharkey + 2x pinetree
PINETREE_ELIXIR_INTEROP=1 cargo test --offline --test federation_elixir elixir_quote_federation
PINETREE_ELIXIR_STRESS=1 cargo test --offline --test federation_elixir elixir_quote_stress
```

Gates, in order: `cargo test --offline` (254 lib + 69 persistence-conformance
incl. Groups Q/R + client/streaming/federation suites) → `smoke-all.sh`
(phase1 core flows, phase3 profile + streaming, keystone-sweep 41/41, phase2
four-server `INTEROP_OK`) → elixir triangle `ELIXIR_QUOTE_FEDERATION_OK` →
extended stress `STRESS_OK items=28 nodes=3 duplicates=0` (15 posts + 8 quotes
+ 5 replies across pinetree/Pleroma/Akkoma with follow churn).

Benching a single node (stays under the per-account rate limits by rotating
users; numbers are debug-build lower bounds):

```bash
PINETREE_ALLOW_PRIVATE_IPS=1 <target>/debug/pinetree --public-url=http://127.0.0.1:3210 \
  --data-dir=/tmp/pinetree-bench --listen=127.0.0.1:3210 &
python3 scripts/bench-local.py --base http://127.0.0.1:3210 --creates 400 --reads 1000
# notes/create ~870/s p50 ~1ms, notes/timeline ~430/s, users/show ~1200/s
```

Clean-clone procedure: clone, re-link `./misskey`/`./sharkey`, run `cargo test`.
For the manual interop lab see `docs/phase-2-interop-howto.md`; for every suite
and rig see `docs/testing.md`.

## Docs

- `sns-server-proposal.md` — architecture and phased plan (wins on disagreement).
- `docs/` — frozen contracts (endpoint manifest, domain types, identity rules,
  visibility policy, application ports, persistence conformance) plus the
  [`wiki/`](docs/wiki/Home.md) overview, phase plans, and the `quote-posts.md` /
  `replies.md` implementation guides.
- `state/logs/` — dated work logs with real output; read the latest entry before
  starting work, append one when finishing (see `AGENTS.md`).
- `AGENTS.md` — contributor/agent rules: modular-monolith structure, dependency
  direction, atomic state+job transactions, verification by execution.

## License

Not yet declared — there is no `LICENSE` file in this repo.
