#!/usr/bin/env bash
# Take the whole bench down and verify it. Stops by PID file / listening port / container name,
# never by pattern. FoundationDB (fdbserver) and the Claude Code daemon are not part of the bench
# and are left alone.
#
#   BENCH_DIR=/root/bench/<run-id> bench-down.sh
set -uo pipefail
BENCH_DIR="${BENCH_DIR:?set BENCH_DIR=/root/bench/<run-id>}"
HERE="$(cd "$(dirname "$0")" && pwd)"

# suite runner first (its children die with it), by the PID recorded at launch
if [ -s "$BENCH_DIR/run-suites.pid" ]; then
  for pid in $(cat "$BENCH_DIR/run-suites.pid"); do
    if kill -0 "$pid" 2>/dev/null && tr '\0' ' ' </proc/"$pid"/cmdline | grep -q run-suites.sh; then
      pgid=$(ps -o pgid= -p "$pid" | tr -d ' '); kill -9 "$pid"; kill -- -"$pgid" 2>/dev/null || true
    fi
  done
fi
bash "$HERE/start-nodes.sh" stop
bash "$HERE/start-misskey-family.sh" stop

docker network disconnect birchtree-lab local-federation-akkoma-1 2>/dev/null || true
docker rm -f bt-proxy-pt >/dev/null 2>&1 || true
docker stop bt-mastodon bt-sidekiq bt-proxy-mastodon bt-proxy-birchtree bt-redis bt-pg \
  local-federation-akkoma-1 local-federation-db-akkoma-1 local-federation-proxy-1 \
  pinetree-interop-postgres-1 pinetree-interop-redis-1 catodon-db-1 catodon-valkey-1 \
  local-federation-pleroma-1 local-federation-db-pleroma-1 >/dev/null 2>&1
bash "$HERE/hosts.sh" remove

echo "--- listeners on bench ports (expect none):"
ss -ltnp | grep -E ":(3000|3103|3105|3101|3210|8080|8081|8082|4000|4001|5432|6379|8099|80) " || echo none
echo "--- running containers (expect none):"
docker ps --format '{{.Names}}' | grep . || echo none
echo "--- bench hosts lines (expect none):"
bash "$HERE/hosts.sh" show | grep . || echo none
