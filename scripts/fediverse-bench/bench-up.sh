#!/usr/bin/env bash
# Bring the whole 10-node bench up on EMPTY databases and prove the network resolves.
# DESTROYS all bench data (see reset-lab.sh). Does not start the suites.
#
#   BENCH_DIR=/root/bench/<run-id> bench-up.sh
#   BENCH_DIR=/root/bench/<run-id> setsid -f nohup bash run-suites.sh > $BENCH_DIR/run-suites.out 2>&1
#   BENCH_DIR=/root/bench/<run-id> bench-down.sh
#
# Topology (10 nodes, 10 accounts u0..u9 each; Mastodon has one extra, `alice`):
#   pinetree  pt-a :3000, pt-b :3103, pt-c :3105        (Misskey API; http://pt-X.test)
#   birchtree bt-a :8080, bt-b :8081, bt-c :8082        (Mastodon API; http://bt-X.lab, no port in the identity)
#   misskey :3101, catodon :3210, akkoma (docker, 127.0.0.1:4000), mastodon (docker, https://mastodon.lab)
# Builds are the binaries named by PT_BIN / BT_BIN (defaults: /root/pinetree-target/release/pinetree,
# /root/birchtree/target/release/birchtree). Build first; this script never builds.
set -euo pipefail
BENCH_DIR="${BENCH_DIR:?set BENCH_DIR=/root/bench/<run-id>}"
case "$BENCH_DIR" in /tmp/*|/tmp) echo "refusing to run under /tmp" >&2; exit 1 ;; esac
export BENCH_DIR
HERE="$(cd "$(dirname "$0")" && pwd)"
PT_BIN="${PT_BIN:-/root/pinetree-target/release/pinetree}"
BT_BIN="${BT_BIN:-/root/birchtree/target/release/birchtree}"
mkdir -p "$BENCH_DIR"/{nodes,misskey,catodon}
[ "$(df --output=avail -BG /root | tail -1 | tr -dc 0-9)" -ge 20 ] || { echo "less than 20 GB free on /root" >&2; exit 1; }
[ -x "$PT_BIN" ] && [ -x "$BT_BIN" ] || { echo "build both binaries first ($PT_BIN, $BT_BIN)" >&2; exit 1; }

bash "$HERE/hosts.sh" add
bash "$HERE/lab-up.sh"

# Misskey's config (the file is per run: it carries this run's ports). The DB password is the lab postgres's own.
if [ ! -s "$BENCH_DIR/misskey/misskey.yml" ]; then
  PW=$(docker inspect pinetree-interop-postgres-1 --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^POSTGRES_PASSWORD=//p')
  cat >"$BENCH_DIR/misskey/misskey.yml" <<EOT
url: http://misskey.test/
port: 3101
db: {host: 127.0.0.1, port: 15432, db: misskey, user: interop, pass: $PW}
dbReplications: false
redis: {host: 127.0.0.1, port: 16379, db: 1}
fulltextSearch: {provider: sqlLike}
id: 'aidx'
allowedPrivateNetworks: ['127.0.0.0/8', '172.16.0.0/12']
EOT
  chmod 600 "$BENCH_DIR/misskey/misskey.yml"
fi

bash "$HERE/reset-lab.sh"            # also stops any node still running
bash "$HERE/start-nodes.sh" start
bash "$HERE/start-misskey-family.sh" start
for p in 3101 3210; do
  for _ in $(seq 90); do ss -ltnH "sport = :$p" | grep -q . && break; sleep 2; done
  ss -ltnH "sport = :$p" | grep -q . || { echo "nothing listens on :$p (see $BENCH_DIR/{misskey,catodon})" >&2; exit 1; }
done
sleep 5

python3 "$HERE/bootstrap-run.py"
python3 "$HERE/bootstrap-lab.py"
bash "$HERE/lab-net.sh"
python3 "$HERE/probe-resolve.py"      # exits 1 unless all 90 ordered pairs resolve

echo "--- build IDs (sha256 of the running executables)"
for n in pt-a pt-b pt-c bt-a bt-b bt-c; do
  printf '%s %s\n' "$n" "$(sha256sum /proc/"$(cat "$BENCH_DIR/nodes/$n.pid")"/exe | cut -c1-16)"
done
echo "bench-up ok: $BENCH_DIR"
