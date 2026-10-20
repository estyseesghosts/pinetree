#!/usr/bin/env bash
# Add or remove the bench names in the host's /etc/hosts. Every line carries the marker below, so
# `remove` deletes exactly what `add` wrote and nothing else. Needs root.
#   hosts.sh add | remove | show
#
# Why these names: the Misskey family, Akkoma and the harness resolve them on the host; the lab
# addresses (172.28.0.0/24) are the birchtree-lab docker network:
#   .10 mastodon.lab (bt-proxy-mastodon)   .20 birchtree.lab   .30 bt-proxy-pt (TLS + plain proxy)
set -euo pipefail
MARK="# bench-2026-10-18"
case "${1:-show}" in
  add)
    "$0" remove
    cat >>/etc/hosts <<EOT
127.0.0.1 pt-a.test pt-b.test pt-c.test akkoma.test misskey.test catodon.test $MARK
172.28.0.10 mastodon.lab $MARK
172.28.0.20 birchtree.lab $MARK
172.28.0.30 bt-a.lab bt-b.lab bt-c.lab $MARK
EOT
    ;;
  remove) grep -vF "$MARK" /etc/hosts >/etc/hosts.bench.new; cat /etc/hosts.bench.new >/etc/hosts; rm -f /etc/hosts.bench.new ;;
  show) grep -F "$MARK" /etc/hosts || true ;;
  *) echo "usage: hosts.sh add|remove|show" >&2; exit 2 ;;
esac
