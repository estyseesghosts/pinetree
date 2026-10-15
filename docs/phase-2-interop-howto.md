# Phase 2 interop — how to run each server yourself

Status: Draft (2026-10-14). Companion to `scripts/smoke-phase2.sh` and
`tests/federation_interop.rs`. Everything binds to loopback only; nothing talks
to the public internet.

## Topology

| Node | Public URL (what peers see) | Backend port | Login |
|---|---|---|---|
| Misskey `2026.10.1-alpha.0` (`./misskey`) | `http://127.0.1.1` | 3101 | `mk1` / `password123` (admin) |
| Sharkey `2025.4.7` (`./sharkey`) | `http://127.0.1.2` | 3102 | `sk1` / `password123` (admin) |
| pinetree A | `http://127.0.1.3` | 3103 | `pt3` / `password123` |
| pinetree B | `http://127.0.1.4` | 3104 | `pt4` / `password123` |
| postgres (db `misskey`, `sharkey`) | — | 15432 | user/pass `interop` |
| redis (db 1 = Misskey, 2 = Sharkey) | — | 16379 | — |

Why these odd addresses: Misskey's WebFinger URL builder drops the port from
URL queries, so every node must sit on port 80 of its own loopback IP.
`scripts/interop/proxy.mjs` does that mapping (run as root, or any user allowed
to bind port 80 on 127.0.1.x).

## One command

```bash
SHARKEY_NODE_BIN=/path/to/node22/bin scripts/smoke-phase2.sh   # prints SMOKE_OK
KEEP=1 SHARKEY_NODE_BIN=... scripts/smoke-phase2.sh            # leave it running
scripts/smoke-phase2.sh stop                                   # tear everything down
```

Work dir (logs, pids, configs, pinetree data): `/tmp/pinetree-interop`
(`INTEROP_WORK` overrides). Databases are recreated from scratch each run.
Cold start is slow when the repo lives on `/mnt/c` (Sharkey can take 5+ min to
print its first line; it is not hung).

## One-time prerequisites

1. docker + compose plugin (images `postgres:16-alpine`, `redis:7-alpine`).
2. Node 24 (Misskey) **and** Node 22 (Sharkey): `nvm install 24` as default,
   system Node 22 via `SHARKEY_NODE_BIN=/usr/sbin`. The script hard-checks
   both majors (Misskey's `re2` is compiled for 24, Sharkey's tree for 22).
3. Misskey backend built: `cd misskey && pnpm build-pre && pnpm --filter "backend..." build` (Node 24 is fine).
4. Sharkey needs **Node 22** (its `engines` field rejects 24). Get one, e.g.
   `curl -L https://nodejs.org/dist/latest-v22.x/node-v22.23.3-linux-x64.tar.xz | tar xJ`,
   then with it first on `PATH`:
   `cd sharkey && pnpm install --frozen-lockfile && pnpm build-pre && pnpm --filter "backend..." build`.
5. `cargo build --offline` (the pinetree binary lands in the cargo target dir
   — `./target` by default, `/root/pinetree-target` here via `.cargo/config.toml`;
   `scripts/smoke-phase2.sh` resolves it via `cargo metadata`).

Filesystem note (WSL): this repo may live on `/mnt/c`, but keep the
`./misskey` / `./sharkey` checkout targets **and** the cargo target dir on
the native Linux filesystem — 9P makes boots, migrations, vite and
compiles several times slower. This machine uses `/root/fedifedifedi/`
(checkouts; symlinked as `./misskey`, `./sharkey`) and `/root/pinetree-target`
(via git-ignored `.cargo/config.toml`). `scripts/smoke-phase2.sh` resolves
everything through the symlinks, so no script changes are needed.

## Starting the pieces by hand

Set `W=/tmp/pinetree-interop-manual` and `mkdir -p $W`.

**Backing services**
```bash
docker compose -f scripts/interop/compose.yml up -d
docker exec pinetree-interop-postgres-1 psql -U interop -d postgres \
  -c 'create database misskey' -c 'create database sharkey'
```

**Configs** (`$W/misskey.yml`; Sharkey is the same with port 3102,
url `http://127.0.1.2/`, db `sharkey`, redis db 2):
```yaml
url: http://127.0.1.1/
port: 3101
db: {host: 127.0.0.1, port: 15432, db: misskey, user: interop, pass: interop}
dbReplications: false
redis: {host: 127.0.0.1, port: 16379, db: 1}
fulltextSearch: {provider: sqlLike}
id: 'aidx'
allowedPrivateNetworks: ['127.0.0.0/8']
```

**Misskey**
```bash
cd misskey/packages/backend
MISSKEY_CONFIG_YML=$W/misskey.yml pnpm migrate
MISSKEY_CONFIG_YML=$W/misskey.yml MISSKEY_WEBFINGER_USE_HTTP=true NODE_ENV=development \
  sh -c 'node scripts/compile_config.js && node built/entry.js'
```

**Sharkey** (Node 22 first on PATH; single process — in cluster mode its queue
worker did not start here and Follow/Accept stayed queued)
```bash
cd sharkey/packages/backend
MISSKEY_CONFIG_YML=$W/sharkey.yml pnpm migrate
MISSKEY_CONFIG_YML=$W/sharkey.yml MISSKEY_WEBFINGER_USE_HTTP=true NODE_ENV=development \
  MK_DISABLE_CLUSTERING=1 node built/boot/entry.js
```

Why the env vars: `NODE_ENV=development` because both refuse `http://` URLs
in production; `MISSKEY_WEBFINGER_USE_HTTP=true` makes them WebFinger over
http; `allowedPrivateNetworks` lets them fetch from loopback.

**pinetree A and B** (`<target>` = cargo target dir, `./target` by default)
```bash
for n in 3 4; do
  PINETREE_ALLOW_PRIVATE_IPS=1 <target>/debug/pinetree --public-url=http://127.0.1.$n \
    --data-dir=$W/pt$n --listen=127.0.0.1:310$n &
done
```

**Proxy** (puts each node on port 80 of its loopback IP)
```bash
node scripts/interop/proxy.mjs 127.0.1.1=3101 127.0.1.2=3102 127.0.1.3=3103 127.0.1.4=3104 &
```

**Accounts** (first admin account on Misskey/Sharkey needs no auth; Misskey and
Sharkey default to `federation: none`, which must be switched to `all`)
```bash
for pair in "127.0.1.1 mk1" "127.0.1.2 sk1"; do set -- $pair
  curl -s -XPOST http://$1/api/admin/accounts/create -H 'content-type: application/json' \
    -d "{\"username\":\"$2\",\"password\":\"password123\"}" >/dev/null
  TOK=$(curl -s -XPOST http://$1/api/signin-flow -H 'content-type: application/json' \
    -d "{\"username\":\"$2\",\"password\":\"password123\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["i"])')
  curl -s -XPOST http://$1/api/admin/update-meta -H "authorization: Bearer $TOK" \
    -H 'content-type: application/json' -d '{"federation":"all"}'
done
for n in 3 4; do curl -s -XPOST http://127.0.1.$n/api/signup -H 'content-type: application/json' \
  -d "{\"username\":\"pt$n\",\"password\":\"password123\"}" >/dev/null; done
```
Then warm each pinetree's lazily generated RSA key once (slow in a dev build;
otherwise a peer's first fetch can time out):
`curl -H 'accept: application/activity+json' http://127.0.1.3/users/<id-from-webfinger>`.

## Poking at it

* Remote lookup from any node: `POST /api/users/show {"username":"pt3","host":"127.0.1.3"}`
  (Bearer token from `signin-flow` on Misskey/Sharkey, `signin` on pinetree).
* Follow: `POST /api/following/create {"userId": <the remote user's local id>}`.
* Post: `POST /api/notes/create {"text":"hi","visibility":"public"}`; reactions
  `notes/reactions/create`, renotes `notes/create {"renoteId": ...}`.
* The automated version of all of this is `tests/federation_interop.rs`:
  `PINETREE_INTEROP=1 cargo test --offline --test federation_interop -- --nocapture`
  against a running stack.

## Looking at it from a Windows browser (WSL2)

WSL2 forwards `localhost` / `127.0.0.1` listeners to Windows, but **not**
`127.0.1.x`. The backends on `127.0.0.1:3101-3104` are reachable from Windows
as `http://localhost:310x`. Misskey/Sharkey have no front-end built here; to
get a Misskey UI on a pinetree node use the temporary rig
`/tmp/opencode/mk-proxy.mjs` (outside the repo):
`PROXY_LISTEN=127.0.0.1:8182 PINETREE_UPSTREAM=127.0.0.1:3103 bun /tmp/opencode/mk-proxy.mjs`
then open `http://localhost:8182` (needs the Misskey vite dev server on 5173).
Links and avatars will still point at the `127.0.1.x` public URL.

## Sharkey's own web UI (vite dev behind its backend)

Unlike pinetree, Sharkey serves its own client: the backend renders the app
shell at `/` and reverse-proxies `/vite` to a vite dev server when no
production frontend manifest exists (`VITE_PORT`, default 5173). To get it:

```bash
# one-time pre-builds (Node 22), then vite on 5174 (5173 is Misskey's)
cd sharkey && pnpm --filter frontend-shared build && pnpm --filter misskey-bubble-game build
cd packages/frontend && PATH=<node22-bin>:$PATH ./node_modules/.bin/vite --port 5174 --strictPort &
# point the running backend at it (restart with the env var; same DB)
VITE_PORT=5174 EMBED_VITE_PORT=5175 <same backend command as above> &
```

Then open `http://localhost:3102` (`sk1` / `password123`). If `/` serves the
shell but assets 404, the backend's `/vite` proxy is aimed at the wrong port
— check `VITE_PORT`. First vite boot optimizes deps and is slow on `/mnt/c`.

## Known gotchas found while bringing this up

* Misskey/Sharkey `federation` defaults to `none` — nothing federates until
  `admin/update-meta {"federation":"all"}`.
* Misskey/Sharkey reject `http://` actor/note/url fields when `NODE_ENV=production`.
* Sharkey needs Node 22; cluster mode left the queue worker down (use `MK_DISABLE_CLUSTERING=1`).
* Misskey's `re2` native module is compiled for its build-time Node major:
  run Misskey migrate/serve under the same Node 24 it was built with
  (`NODE_MODULE_VERSION 137`), Sharkey under Node 22 (127) via
  `SHARKEY_NODE_BIN`. A `was compiled against a different Node.js version`
  `ERR_DLOPEN_FAILED` means the majors got swapped.
* Don't `pkill -f built/entry.js` from the same shell command line: it matches itself.
* A stale previous Misskey on the port makes a new one silently fail to bind.
