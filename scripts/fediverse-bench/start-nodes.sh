#!/usr/bin/env bash
# Start the pinetree (pt-a/b/c) and birchtree (bt-a/b/c) bench nodes for one run.
#
#   BENCH_DIR=/root/bench/<run-id> start-nodes.sh [start|stop]
#
# Data, logs and pids live under $BENCH_DIR/nodes. Lab-only flags (plain HTTP federation, private
# ranges, the lab CA, weak rate limits) are on; never use this for anything but the bench.
# Stops by PID file, never by pattern.
set -euo pipefail
BENCH_DIR="${BENCH_DIR:?set BENCH_DIR=/root/bench/<run-id>}"
PT_BIN="${PT_BIN:-/root/pinetree-target/release/pinetree}"
BT_BIN="${BT_BIN:-/root/birchtree/target/release/birchtree}"
LAB="${LAB:-/root/parity/lab}"
CA="$LAB/certs/lab-ca.crt"
N="$BENCH_DIR/nodes"
mkdir -p "$N"

stop_all() {
  for f in "$N"/*.pid; do
    [ -e "$f" ] || continue
    pid=$(cat "$f")
    if kill -0 "$pid" 2>/dev/null; then kill "$pid"; fi
    rm -f "$f"
  done
}

N_USERS="${N_USERS:-10}"
# The stores are single-process (redb lock), so accounts u0..u<N-1> are created before first start.
seed_pt() { # name url
  [ -d "$N/$1-data" ] && return 0
  for i in $(seq 0 $((N_USERS - 1))); do
    "$PT_BIN" --public-url="$2" --data-dir="$N/$1-data" create-account --username="u$i" --password=password123 >>"$N/$1.seed.log" 2>&1
  done
}
seed_bt() { # name url
  [ -d "$N/$1-data" ] && return 0
  for i in $(seq 0 $((N_USERS - 1))); do
    "$BT_BIN" --public-url="$2" --data-dir="$N/$1-data" create-account --username="u$i" --password=password123 \
      --email="u$i@bench.invalid" >>"$N/$1.seed.log" 2>&1
  done
}

start_pt() { # name url port
  seed_pt "$1" "$2"
  PINETREE_ALLOW_PRIVATE_IPS=1 "$PT_BIN" --public-url="$2" --data-dir="$N/$1-data" --listen=0.0.0.0:"$3" \
    --debug-federation --debug-weak-rate-limits --federation-extra-ca="$CA" >"$N/$1.log" 2>&1 &
  echo $! >"$N/$1.pid"
}
start_bt() { # name url port
  seed_bt "$1" "$2"
  "$BT_BIN" --public-url="$2" --data-dir="$N/$1-data" --listen=0.0.0.0:"$3" \
    --federation-allow-http --federation-allow-private=127.0.0.0/8,172.28.0.0/24 \
    --federation-extra-ca="$CA" --debug-weak-rate-limits >"$N/$1.log" 2>&1 &
  echo $! >"$N/$1.pid"
}

# A node that fails to bind (an old process still holds the port) would otherwise be silently replaced
# in the run by that stale process, with its old data.
check_ports_free() {
  local busy=0
  for p in 3000 3103 3105 8080 8081 8082; do
    if ss -ltnH "sport = :$p" | grep -q .; then echo "port $p is already in use: stop the old node first" >&2; busy=1; fi
  done
  return $busy
}

case "${1:-start}" in
  stop) stop_all ;;
  start)
    check_ports_free
    start_pt pt-a http://pt-a.test 3000
    start_pt pt-b http://pt-b.test 3103
    start_pt pt-c http://pt-c.test 3105
    start_bt bt-a http://bt-a.lab 8080
    start_bt bt-b http://bt-b.lab 8081
    start_bt bt-c http://bt-c.lab 8082
    sleep 3
    for spec in pt-a:3000 pt-b:3103 pt-c:3105 bt-a:8080 bt-b:8081 bt-c:8082; do
      n="${spec%%:*}"; p="${spec##*:}"
      ss -ltnpH "sport = :$p" | grep -q "pid=$(cat "$N/$n.pid")," || { echo "$n did not bind :$p (see $N/$n.log)" >&2; exit 1; }
    done
    ;;
  *) echo "usage: start-nodes.sh [start|stop]" >&2; exit 2 ;;
esac
