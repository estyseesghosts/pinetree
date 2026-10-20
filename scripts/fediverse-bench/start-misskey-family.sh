#!/usr/bin/env bash
# (Re)start the bench Misskey (:3101) and Catodon (:3210) with the /etc/hosts DNS shim and lab CA.
#   BENCH_DIR=/root/bench/<run-id> start-misskey-family.sh [start|stop]
# Needs the Misskey config at $BENCH_DIR/misskey/misskey.yml; Catodon uses its own .config/default.yml.
set -euo pipefail
BENCH_DIR="${BENCH_DIR:?set BENCH_DIR=/root/bench/<run-id>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
LAB="${LAB:-/root/parity/lab}"
export NODE_OPTIONS="--require $HERE/node-hosts-preload.cjs"
export NODE_EXTRA_CA_CERTS="$LAB/certs/lab-ca.crt"
export NODE_ENV=development MISSKEY_WEBFINGER_USE_HTTP=true

stop_port() { # stop whatever listens on the port, by PID
  for pid in $(ss -ltnpH "sport = :$1" | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u); do kill "$pid"; done
  for _ in $(seq 20); do ss -ltnH "sport = :$1" | grep -q . || return 0; sleep 1; done
  echo "port $1 still busy" >&2; return 1
}
case "${1:-start}" in
  stop) stop_port 3101; stop_port 3210 ;;
  start)
    mkdir -p "$BENCH_DIR/misskey" "$BENCH_DIR/catodon"
    ( cd /root/fedifedifedi/misskey/packages/backend && \
      MISSKEY_CONFIG_YML="$BENCH_DIR/misskey/misskey.yml" setsid nohup sh -c 'node scripts/compile_config.js && exec node built/entry.js' \
        >"$BENCH_DIR/misskey/misskey.log" 2>&1 </dev/null & ) >/dev/null 2>&1
    ( cd /root/fedifedifedi/catodon/packages/backend && \
      setsid nohup node built/boot/entry.js >"$BENCH_DIR/catodon/catodon.log" 2>&1 </dev/null & ) >/dev/null 2>&1
    ;;
  *) echo "usage: start-misskey-family.sh [start|stop]" >&2; exit 2 ;;
esac
