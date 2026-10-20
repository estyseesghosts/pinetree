#!/usr/bin/env bash
# Bench-only container networking fixes that the lab compose files cannot express (idempotent):
#  - Akkoma: join the lab network, resolve the pinetree/birchtree/mastodon names, trust the lab CA.
#    The CA is appended to the bundles Akkoma reads at boot (:public_key.cacerts_load), so Akkoma is
#    restarted when the CA is first added. A restart resets /etc/hosts, so this script re-adds it.
#  - Mastodon (bt-mastodon, bt-sidekiq) and Akkoma: bt-a/b/c.lab resolve to the bench proxy, which
#    answers TLS and plain HTTP on the birchtree ports (a `host:port` handle makes them WebFinger over HTTPS).
# Mastodon's pt-*/misskey/catodon/akkoma names go to the bench TLS proxy through --add-host at
# `docker run` time (see the run report); Mastodon only does WebFinger over HTTPS.
set -euo pipefail
LAB="${LAB:-/root/parity/lab}"
A=local-federation-akkoma-1
docker network inspect birchtree-lab -f '{{range .Containers}}{{.Name}} {{end}}' | grep -qw "$A" \
  || docker network connect --ip 172.28.0.40 birchtree-lab "$A"
added=0
for f in /etc/ssl/certs/ca-certificates.crt /opt/akkoma/deps/certifi/priv/cacerts.pem /opt/akkoma/deps/castore/priv/cacerts.pem; do
  if ! docker exec "$A" grep -q '^# bench-lab-ca$' "$f"; then
    docker exec -i -u 0 "$A" sh -c "echo '# bench-lab-ca' >> $f; cat >> $f" < "$LAB/certs/lab-ca.crt"; added=1
  fi
done
if [ "$added" = 1 ]; then
  docker restart "$A" >/dev/null
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "$A")" = healthy ]; do sleep 3; done
fi
# Rewrite /etc/hosts entries (it is a bind mount, so no sed -i): drop our old lines and docker's
# add-host lines for the bt-*.lab names, then append the current ones.
PROXY=172.28.0.30   # bench TLS/plain proxy (bt-proxy-pt); fronts pt-*, misskey, catodon, akkoma, bt-*.lab
rehost() { # container entries...
  local c="$1"; shift
  docker exec -u 0 "$c" sh -c 'grep -vE "# bench-2026-10-18|[[:space:]]bt-[abc]\.lab([[:space:]]|$)" /etc/hosts > /etc/hosts.new; cat /etc/hosts.new > /etc/hosts; rm /etc/hosts.new'
  for e in "$@"; do docker exec -u 0 "$c" sh -c "echo '$e # bench-2026-10-18' >> /etc/hosts"; done
}
rehost "$A" "172.17.0.1 pt-a.test pt-b.test pt-c.test" "$PROXY bt-a.lab bt-b.lab bt-c.lab" "172.28.0.10 mastodon.lab"
for c in bt-mastodon bt-sidekiq; do rehost "$c" "$PROXY bt-a.lab bt-b.lab bt-c.lab"; done
echo "lab-net ok"
