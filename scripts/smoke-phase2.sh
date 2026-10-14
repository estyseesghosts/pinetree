#!/usr/bin/env bash
# Phase 2 Slice E2 — four-server federation smoke: real Misskey (./misskey),
# real Sharkey (./sharkey) and two pinetree instances federate over loopback.
#
# Brings up from scratch (fresh databases each run):
#   postgres + redis      docker compose (scripts/interop/compose.yml)
#   Misskey               127.0.1.1:80  (backend :3101)  user mk1
#   Sharkey               127.0.1.2:80  (backend :3102)  user sk1
#   pinetree A / B        127.0.1.3:80 / 127.0.1.4:80 (backends :3103/:3104)
#                         users pt3 / pt4
# A tiny node proxy (scripts/interop/proxy.mjs) puts every node on port 80 of
# its own loopback IP, because Misskey's WebFinger URL builder drops ports.
# All passwords are `password123`. Then runs
#   PINETREE_INTEROP=1 cargo test --offline --test federation_interop
# and prints INTEROP_OK on success.
#
# Prerequisites (see docs/phase-2-interop-howto.md):
#   * docker with the compose plugin, node (>=24 for Misskey), pnpm, python3
#   * ./misskey built:  (cd misskey && pnpm build-pre && pnpm --filter "backend..." build)
#   * ./sharkey installed + built with Node 22:
#       (cd sharkey && pnpm install --frozen-lockfile && pnpm build-pre && pnpm --filter "backend..." build)
#   * SHARKEY_NODE_BIN=<dir containing a Node 22 `node`> unless `node` is already 22.x
#   * target/debug/pinetree (built here if missing)
#
# Env: KEEP=1 leaves everything running (stop with scripts/smoke-phase2.sh stop).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${INTEROP_WORK:-/tmp/pinetree-interop}"
COMPOSE=(docker compose -f "$ROOT/scripts/interop/compose.yml")
PIDS="$WORK/pids"
PW=password123

stop_all() {
  if [[ -f "$PIDS" ]]; then
    while read -r pgid; do
      kill -TERM -- "-$pgid" 2>/dev/null || true
    done <"$PIDS"
    sleep 2
    while read -r pgid; do
      kill -KILL -- "-$pgid" 2>/dev/null || true
    done <"$PIDS"
    rm -f "$PIDS"
  fi
  # Belt and braces: anything still listening on the interop ports (stale
  # servers keep in-memory state that breaks a fresh run) is killed too.
  local stale
  stale="$(ss -Hltnp 2>/dev/null | grep -E ':(3101|3102|3103|3104) |127\.0\.1\.[1-4]:80 ' | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u || true)"
  for pid in $stale; do
    pkill -KILL -P "$pid" 2>/dev/null || true
    kill -KILL "$pid" 2>/dev/null || true
  done
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}

if [[ "${1:-}" == "stop" ]]; then
  stop_all
  echo "[interop] stopped"
  exit 0
fi

if [[ "${KEEP:-0}" != "1" ]]; then
  trap stop_all EXIT
fi

mkdir -p "$WORK"
stop_all # clear leftovers from a previous run
: >"$PIDS"

start_bg() { # name, cwd, env-assignments..., -- command...
  local name="$1" cwd="$2"; shift 2
  local envs=()
  while [[ "$1" != "--" ]]; do envs+=("$1"); shift; done
  shift
  (cd "$cwd" && env "${envs[@]}" setsid "$@" >"$WORK/$name.log" 2>&1 & echo $! >"$WORK/$name.pid")
  sleep 0.2
  # setsid makes the child its own process-group leader: pgid == pid.
  cat "$WORK/$name.pid" >>"$PIDS"
}

api() { # base endpoint json [token]
  local auth=()
  [[ -n "${4:-}" ]] && auth=(-H "authorization: Bearer $4")
  curl -sS -m 90 -X POST "$1/api/$2" -H 'content-type: application/json' "${auth[@]}" -d "$3"
}

wait_http() { # url what
  local i
  for i in $(seq 1 450); do
    if curl -s -m 3 -X POST "$1/api/meta" -H 'content-type: application/json' -d '{}' | grep -q version; then
      return 0
    fi
    sleep 2
  done
  echo "SMOKE_FAIL: $2 did not come up (see $WORK/*.log)" >&2
  return 1
}

# --- prerequisites ---------------------------------------------------------
[[ -f "$ROOT/misskey/packages/backend/built/entry.js" ]] || { echo "SMOKE_FAIL: ./misskey backend not built" >&2; exit 1; }
[[ -f "$ROOT/sharkey/packages/backend/built/boot/entry.js" ]] || { echo "SMOKE_FAIL: ./sharkey backend not built" >&2; exit 1; }
SK_PATH="${SHARKEY_NODE_BIN:+$SHARKEY_NODE_BIN:}$PATH"
[[ "$(PATH="$SK_PATH" node --version)" == v22.* ]] || { echo "SMOKE_FAIL: Sharkey needs Node 22 (set SHARKEY_NODE_BIN)" >&2; exit 1; }
[[ -x "$ROOT/target/debug/pinetree" ]] || (cd "$ROOT" && cargo build --offline)

# --- backing services ------------------------------------------------------
"${COMPOSE[@]}" up -d >/dev/null
for i in $(seq 1 60); do
  docker exec pinetree-interop-postgres-1 pg_isready -U interop >/dev/null 2>&1 && break
  sleep 1
done
docker exec pinetree-interop-postgres-1 psql -U interop -d postgres -c 'create database misskey' -c 'create database sharkey' >/dev/null

write_cfg() { # file port ip db redisdb
  cat >"$1" <<EOF
url: http://$3/
port: $2
db: {host: 127.0.0.1, port: 15432, db: $4, user: interop, pass: interop}
dbReplications: false
redis: {host: 127.0.0.1, port: 16379, db: $5}
fulltextSearch: {provider: sqlLike}
id: 'aidx'
allowedPrivateNetworks: ['127.0.0.0/8']
EOF
}
write_cfg "$WORK/misskey.yml" 3101 127.0.1.1 misskey 1
write_cfg "$WORK/sharkey.yml" 3102 127.0.1.2 sharkey 2

echo "[interop] migrating misskey + sharkey..."
(cd "$ROOT/misskey/packages/backend" && MISSKEY_CONFIG_YML="$WORK/misskey.yml" pnpm migrate >"$WORK/misskey-migrate.log" 2>&1)
(cd "$ROOT/sharkey/packages/backend" && PATH="$SK_PATH" MISSKEY_CONFIG_YML="$WORK/sharkey.yml" pnpm migrate >"$WORK/sharkey-migrate.log" 2>&1)

# --- servers ---------------------------------------------------------------
# NODE_ENV=development: Misskey/Sharkey reject http:// URLs in production.
# MISSKEY_WEBFINGER_USE_HTTP: WebFinger over http for these loopback hosts.
# MK_DISABLE_CLUSTERING (Sharkey): in cluster mode its queue worker never
# started here and outbound activities (Follow, ...) stayed queued.
start_bg misskey "$ROOT/misskey/packages/backend" \
  MISSKEY_CONFIG_YML="$WORK/misskey.yml" MISSKEY_WEBFINGER_USE_HTTP=true NODE_ENV=development \
  -- sh -c 'node scripts/compile_config.js >/dev/null && exec node built/entry.js'
start_bg sharkey "$ROOT/sharkey/packages/backend" \
  PATH="$SK_PATH" MISSKEY_CONFIG_YML="$WORK/sharkey.yml" MISSKEY_WEBFINGER_USE_HTTP=true \
  NODE_ENV=development MK_DISABLE_CLUSTERING=1 -- node built/boot/entry.js
for n in 3 4; do
  rm -rf "$WORK/pt$n"; mkdir -p "$WORK/pt$n"
  start_bg "pt$n" "$ROOT" PINETREE_ALLOW_PRIVATE_IPS=1 -- \
    "$ROOT/target/debug/pinetree" --public-url="http://127.0.1.$n" --data-dir="$WORK/pt$n" --listen="127.0.0.1:310$n"
done
start_bg proxy "$ROOT" -- node scripts/interop/proxy.mjs 127.0.1.1=3101 127.0.1.2=3102 127.0.1.3=3103 127.0.1.4=3104

echo "[interop] waiting for servers (Sharkey/Misskey are slow to boot from /mnt/c)..."
wait_http http://127.0.1.3 pinetree-a
wait_http http://127.0.1.4 pinetree-b
wait_http http://127.0.0.1:3101 misskey
wait_http http://127.0.0.1:3102 sharkey

# --- accounts --------------------------------------------------------------
echo "[interop] creating accounts..."
for spec in "http://127.0.1.1 mk1" "http://127.0.1.2 sk1"; do
  set -- $spec
  api "$1" admin/accounts/create "{\"username\":\"$2\",\"password\":\"$PW\"}" >/dev/null
  tok="$(api "$1" signin-flow "{\"username\":\"$2\",\"password\":\"$PW\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["i"])')"
  api "$1" admin/update-meta '{"federation":"all"}' "$tok" >/dev/null # default is "none"
  if [[ "$2" == sk1 ]]; then
    # Local buddy used by the test to invalidate Sharkey's followings cache.
    api "$1" admin/accounts/create "{\"username\":\"sk2\",\"password\":\"$PW\"}" "$tok" >/dev/null
  fi
done
for n in 3 4; do
  api "http://127.0.1.$n" signup "{\"username\":\"pt$n\",\"password\":\"$PW\"}" >/dev/null
  # Warm the lazily generated RSA key (slow in a dev build) so the first
  # remote fetch does not hit the peer's request timeout.
  actor="$(curl -sS -m 90 "http://127.0.1.$n/.well-known/webfinger?resource=acct:pt$n@127.0.1.$n" | python3 -c 'import json,sys;print(json.load(sys.stdin)["links"][0]["href"])')"
  curl -sS -m 90 -H 'accept: application/activity+json' "$actor" >/dev/null
done

# --- the interop test ------------------------------------------------------
echo "[interop] running tests/federation_interop.rs..."
cd "$ROOT"
PINETREE_INTEROP=1 cargo test --offline --test federation_interop -- --nocapture 2>&1 | tee "$WORK/test.log" | grep -v '^warning\|^ *|\|^ *=\|^$\|-->' || true
if grep -q INTEROP_OK "$WORK/test.log"; then
  echo "SMOKE_OK"
else
  echo "SMOKE_FAIL: see $WORK/test.log and $WORK/*.log" >&2
  exit 1
fi
