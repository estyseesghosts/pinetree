#!/usr/bin/env bash
# Reset every bench node to an empty database so a run starts with no accounts, follows or cached
# remote actors. DESTROYS bench data: nodes' stores, the Misskey/Catodon/Akkoma/Mastodon databases.
#
#   BENCH_DIR=/root/bench/<run-id> reset-lab.sh
#
# Afterwards: start-nodes.sh start; Misskey/Catodon via start-misskey-family.sh; then bootstrap-run.py,
# bootstrap-lab.py, lab-net.sh, probe-resolve.py. Containers must already exist (lab up.sh + compose).
set -euo pipefail
BENCH_DIR="${BENCH_DIR:?set BENCH_DIR=/root/bench/<run-id>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
LAB="${LAB:-/root/parity/lab}"
MK_PG=pinetree-interop-postgres-1

"$HERE/start-nodes.sh" stop
"$HERE/start-misskey-family.sh" stop
rm -rf "$BENCH_DIR"/nodes/*-data "$BENCH_DIR"/nodes/*.seed.log "$BENCH_DIR/accounts.json"

# Misskey (db 1 in the interop redis) and Catodon
docker exec "$MK_PG" psql -U interop -d postgres -c 'drop database if exists misskey with (force)' -c 'create database misskey' >/dev/null
docker exec pinetree-interop-redis-1 redis-cli -n 1 flushdb >/dev/null
(cd /root/fedifedifedi/misskey/packages/backend && MISSKEY_CONFIG_YML="$BENCH_DIR/misskey/misskey.yml" pnpm migrate >"$BENCH_DIR/misskey/migrate.log" 2>&1)
docker exec catodon-db-1 psql -U catodon -d postgres -c 'drop database if exists catodon with (force)' -c 'create database catodon owner catodon' >/dev/null
docker exec catodon-valkey-1 valkey-cli flushall >/dev/null
(cd /root/fedifedifedi/catodon/packages/backend && pnpm migrate >"$BENCH_DIR/catodon/migrate.log" 2>&1)

# Akkoma: empty schema, then a restart runs its migrations (a restart resets /etc/hosts: run lab-net.sh after)
docker exec local-federation-db-akkoma-1 psql -U akkoma -d akkoma -c 'drop schema public cascade' -c 'create schema public' >/dev/null
docker restart local-federation-akkoma-1 >/dev/null

# Mastodon: stop web and sidekiq, empty schema + redis, db:prepare, start again
docker stop bt-mastodon bt-sidekiq >/dev/null
docker exec bt-pg psql -U postgres -d mastodon_production -c 'drop schema public cascade' -c 'create schema public' >/dev/null
docker exec bt-redis redis-cli flushall >/dev/null
docker run --rm --network birchtree-lab --env-file "$LAB/mastodon.env" -e SSL_CERT_FILE=/certs/ca-bundle.pem -v "$LAB/certs:/certs:ro" \
  birchtree-parity/mastodon:v4.7.3 bin/rails db:prepare >"$BENCH_DIR/mastodon-db-prepare.log" 2>&1
docker start bt-mastodon bt-sidekiq >/dev/null
echo "reset-lab done"
