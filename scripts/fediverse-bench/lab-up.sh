#!/usr/bin/env bash
# Bring up the containers the bench needs (idempotent). Starts the lab infrastructure that already
# exists, builds the bench TLS proxy, and creates the two Mastodon app containers if they are missing.
#
#   BENCH_DIR=/root/bench/<run-id> lab-up.sh
#
# Containers (docker network birchtree-lab, 172.28.0.0/24):
#   bt-pg, bt-redis                 Mastodon's postgres (.2) and redis (.3)
#   bt-mastodon (.11), bt-sidekiq (.12)  Mastodon 4.7.3 web and worker (created here, env from $LAB/mastodon.env)
#   bt-proxy-mastodon (.10)         TLS front for mastodon.lab
#   bt-proxy-pt (.30)               TLS and plain-HTTP front for pt-*.test, misskey/catodon/akkoma.test, bt-*.lab
#                                   (Mastodon federates over HTTPS only and drops ports; see the run report)
#   local-federation-{akkoma,db-akkoma,proxy}-1   Akkoma, its postgres, the nginx that fronts Misskey/Akkoma/Catodon on :80
#   pinetree-interop-{postgres,redis}-1           Misskey's postgres and redis
#   catodon-{db,valkey}-1                         Catodon's postgres and valkey
# Pleroma (local-federation-pleroma-1) is not part of the 10-node topology and is left alone.
# bt-proxy-birchtree (.20, birchtree.lab) is started because the lab hosts file maps the name, but no bench node uses it.
set -euo pipefail
BENCH_DIR="${BENCH_DIR:?set BENCH_DIR=/root/bench/<run-id>}"
LAB="${LAB:-/root/parity/lab}"
IMG=birchtree-parity/mastodon:v4.7.3
NET=birchtree-lab
TLS="$BENCH_DIR/pt-tls"
PROXY_IP=172.28.0.30

docker network inspect "$NET" >/dev/null || { echo "docker network $NET is missing: bring the lab up first ($LAB)" >&2; exit 1; }
docker start bt-pg bt-redis bt-proxy-mastodon bt-proxy-birchtree \
  pinetree-interop-postgres-1 pinetree-interop-redis-1 \
  local-federation-db-akkoma-1 local-federation-akkoma-1 local-federation-proxy-1 \
  catodon-db-1 catodon-valkey-1 >/dev/null

# --- bench TLS proxy: cert for every name, signed by the lab CA (openssl is not on the host; the Mastodon image has it)
mkdir -p "$TLS"
NAMES="pt-a.test pt-b.test pt-c.test misskey.test catodon.test akkoma.test bt-a.lab bt-b.lab bt-c.lab bt-d.lab bt-e.lab mast-b.lab"
{ printf 'subjectAltName='; first=1; for n in $NAMES; do [ $first = 1 ] || printf ','; printf 'DNS:%s' "$n"; first=0; done; echo
  echo 'extendedKeyUsage=serverAuth'; } >"$TLS/pt.ext"
# upstream per name: the host's docker-bridge address (172.28.0.1) and the node's port
declare -A PORT=([pt-a.test]=3000 [pt-b.test]=3103 [pt-c.test]=3105 [misskey.test]=80 [catodon.test]=80 [akkoma.test]=80
                 [bt-a.lab]=8080 [bt-b.lab]=8081 [bt-c.lab]=8082 [bt-d.lab]=8083 [bt-e.lab]=8084 [mast-b.lab]=8085)
{ echo 'events {}'; echo 'http {'; echo '  client_max_body_size 64m;'
  for n in $NAMES; do
    echo "  server { listen 80; listen 443 ssl; server_name $n; ssl_certificate /pt/pt.crt; ssl_certificate_key /pt/pt.key;"
    echo "    location / { proxy_pass http://172.28.0.1:${PORT[$n]}; proxy_set_header Host \$host; proxy_set_header X-Forwarded-Proto \$scheme; proxy_set_header X-Forwarded-For \$remote_addr; proxy_http_version 1.1; proxy_set_header Upgrade \$http_upgrade; proxy_set_header Connection \"upgrade\"; } }"
  done; echo '}'; } >"$TLS/nginx.conf"
# mast-b.lab is a second Mastodon: like bt-proxy-mastodon, always claim https
# (Mastodon redirects plain-http upstreams to https, breaking harness http).
python3 - "$TLS/nginx.conf" <<'EOF'
import sys
p = sys.argv[1]
t = open(p).read()
i = t.index("server_name mast-b.lab")
j = t.index("} }", i)
seg = t[i:j].replace("X-Forwarded-Proto $scheme", "X-Forwarded-Proto https")
open(p, "w").write(t[:i] + seg + t[j:])
EOF
if [ ! -s "$TLS/pt.crt" ]; then
  cat >"$TLS/sign.sh" <<'EOS'
set -e
cd /pt
openssl req -new -newkey rsa:2048 -nodes -keyout pt.key -subj "/CN=pt-a.test" -out pt.csr 2>/dev/null
openssl x509 -req -in pt.csr -CA /ca/lab-ca.crt -CAkey /ca/lab-ca.key -CAcreateserial -CAserial /pt/ca.srl -out pt.crt -days 30 -extfile pt.ext
chmod 644 pt.key
EOS
  docker run --rm --user 0 --entrypoint sh -v "$LAB/certs:/ca:ro" -v "$TLS:/pt" "$IMG" /pt/sign.sh
fi
docker rm -f bt-proxy-pt >/dev/null 2>&1 || true
docker run -d --name bt-proxy-pt --network "$NET" --ip "$PROXY_IP" \
  -v "$TLS:/pt:ro" -v "$TLS/nginx.conf:/etc/nginx/nginx.conf:ro" nginx:alpine >/dev/null

# --- Mastodon web + sidekiq. Names resolve to the bench proxy (it answers TLS), mastodon.lab to its own proxy.
HOSTARGS=()
for n in $NAMES; do HOSTARGS+=(--add-host "$n:$PROXY_IP"); done
HOSTARGS+=(--add-host mastodon.lab:172.28.0.10 --add-host birchtree.lab:172.28.0.20)
mk() { # name ip cmd...
  local name="$1" ip="$2"; shift 2
  if docker inspect "$name" >/dev/null 2>&1; then docker start "$name" >/dev/null; return; fi
  docker run -d --name "$name" --network "$NET" --ip "$ip" --env-file "$LAB/mastodon.env" \
    -e SSL_CERT_FILE=/certs/ca-bundle.pem -v "$LAB/certs:/certs:ro" "${HOSTARGS[@]}" "$IMG" "$@" >/dev/null
}
mk bt-mastodon 172.28.0.11 bin/rails server -b 0.0.0.0 -p 3000
mk bt-sidekiq 172.28.0.12 bundle exec sidekiq
echo "lab-up ok"
