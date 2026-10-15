# pinetree

A lightweight, modular **Misskey-compatible social network server** written in Rust.
Existing Misskey clients talk to it through a deliberately chosen, contract-tested
subset of the Misskey API, and it federates over ActivityPub with Misskey, Sharkey
and other servers.

Status: Phase 2 (minimal federation) plus the Phase 2.5 cleanup are complete and
gate-green; Phase 3 (client compatibility + streaming) is next. See
[`docs/wiki/Current-State.md`](docs/wiki/Current-State.md) and `AGENTS.md`.

## Features

- Accounts and auth: signup/signin, MiAuth web flow, legacy `auth/session/*`,
  per-token permissions, Misskey-style rate limits.
- Notes: create/show/delete, replies, renotes, reactions, follows, home / local /
  hybrid timelines, notifications, drive upload + file serving.
- Federation: WebFinger, NodeInfo, actor documents with RSA keys, shared and
  personal inboxes, legacy HTTP Signatures, Follow/Accept/Reject/Undo,
  Create/Update/Delete/Like/Announce, atomic activity-id deduplication, durable
  outbound delivery with retry, backoff and terminal quarantine.
- Durability: every state change commits together with its outbox/job record in
  one storage transaction (embedded `redb`, single node). FoundationDB
  multi-node mode is Phase 4.
- No streaming yet (`src/streaming` is a stub) — that is Phase 3.

## Layout

```
sns-server-proposal.md   product/architecture proposal (source of truth)
README.md                this file
AGENTS.md                rules for agents working in this repo
docs/                    frozen contracts, phase plans, exit criteria, wiki
state/logs/              dated append-only work logs (real command output)
src/                     the crate: domain <- application <- adapters
tests/                   conformance, crash_recovery, client_smoke, federation,
                         federation_interop, minrepro
scripts/                 smoke-phase1.sh, smoke-phase2.sh, interop/ helpers
misskey, sharkey         symlinks to read-only reference checkouts (recreate
                         these yourself; see Prerequisites)
foundationdb/, redb/     upstream reference checkouts (read-only, git-ignored)
```

## Prerequisites

- Recent stable Rust (`cargo build` / `cargo test`; `redb` comes from crates.io,
  no extra step).
- For the test suites: `bun` plus the `./misskey` checkout (live `misskey-js`
  client smoke test and packer snapshot tests), and for the interop rig:
  `docker` with the compose plugin, Node 24 (Misskey) **and** Node 22 (Sharkey
  rejects Node 24 via its `engines` field — pass
  `SHARKEY_NODE_BIN=<dir containing a Node 22 binary>`). `nvm` handles this:
  `nvm install 24` (default) plus system Node 22, then
  `SHARKEY_NODE_BIN=/usr/sbin scripts/smoke-phase2.sh`.
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
always on.

## Testing

```bash
cargo test                                        # full suite (251 green: lib 193 + conformance 51 + the rest)
scripts/smoke-phase1.sh                           # live binary + real misskey-js client (needs bun + ./misskey)
SHARKEY_NODE_BIN=<node22/bin> scripts/smoke-phase2.sh   # four-server interop: Misskey + Sharkey + 2x pinetree
```

Benching a single node (stays under the per-account rate limits by rotating
users; numbers are debug-build lower bounds):

```bash
PINETREE_ALLOW_PRIVATE_IPS=1 <target>/debug/pinetree --public-url=http://127.0.0.1:3210 \
  --data-dir=/tmp/pinetree-bench --listen=127.0.0.1:3210 &
python3 scripts/bench-local.py --base http://127.0.0.1:3210 --creates 400 --reads 1000
# notes/create ~870/s p50 ~1ms, notes/timeline ~430/s, users/show ~1200/s
```

Clean-clone procedure: clone, re-link `./misskey`/`./sharkey`, run `cargo test`.
For the manual interop lab see `docs/phase-2-interop-howto.md`.

## Docs

- `sns-server-proposal.md` — architecture and phased plan (wins on disagreement).
- `docs/` — frozen contracts (endpoint manifest, domain types, identity rules,
  visibility policy, application ports, persistence conformance) plus the
  [`wiki/`](docs/wiki/Home.md) overview and phase plans.
- `state/logs/` — dated work logs with real output; read the latest entry before
  starting work, append one when finishing (see `AGENTS.md`).
- `AGENTS.md` — contributor/agent rules: modular-monolith structure, dependency
  direction, atomic state+job transactions, verification by execution.

## License

Not yet declared — there is no `LICENSE` file in this repo.
