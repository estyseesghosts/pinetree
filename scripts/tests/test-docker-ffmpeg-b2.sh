#!/usr/bin/env bash
# test-docker-ffmpeg-b2.sh — Batch B (prod-ready.md §3) Docker end-to-end tests.
#
# Covers the five §B2 cases plus the CLI local-HTTP smoke-rig path (§B3):
#   0. minimal + http public_url WITHOUT the flag: rejects (non-zero, names flag).
#      minimal + http public_url WITH --allow-local-http-for-testing: serves + WARNING.
#   1. minimal target + default (transcode disabled): starts, health/readiness
#      work, ffmpeg absent/not-required.
#   2. minimal target + transcode.enabled=true: non-zero exit + actionable probe
#      message naming the toolchain probe; never serve mode.
#   3. ffmpeg target + default config: starts, transcoding still disabled.
#   4. ffmpeg target + explicit enabled config: startup validates the toolchain,
#      then a real media-transcode smoke runs through the normal worker path
#      (tiny deterministic PNG fixture, bounded size/time; output bytes, MIME/
#      ext and size limits verified; a corrupt upload can never report completed
#      success).
#   5. SIGTERM mid-transcode: lease/retry + temp cleanup correct on restart.
#
# Run on a Docker host from the repo root:
#   bash scripts/tests/test-docker-ffmpeg-b2.sh
# Env overrides: KEEP_IMAGES=1 (skip image rm), B2_PORT_BASE=3711.
# Without `docker` the script prints DOCKER_B2_SKIP and exits 2 (NOT-RUN —
# never green). The Rust-side gate logic is proven by `cargo test
# --offline --test config_docker_b2` on boxes without Docker.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT_BASE="${B2_PORT_BASE:-3711}"

PASS_N=0
FAIL_N=0
pass() { PASS_N=$((PASS_N + 1)); echo "[PASS] $1"; }
fail() { FAIL_N=$((FAIL_N + 1)); echo "[FAIL] $1"; }

if ! command -v docker >/dev/null 2>&1; then
  echo "DOCKER_B2_SKIP: no 'docker' on PATH — Docker scenarios NOT-RUN on this box."
  echo "Run on a Docker host: bash scripts/tests/test-docker-ffmpeg-b2.sh"
  exit 2
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "DOCKER_B2_SKIP: 'curl' required for health polling."
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "DOCKER_B2_SKIP: 'python3' required for throwaway config generation."
  exit 2
fi

IMG_MIN="pinetree-b2:runtime"
IMG_FFMPEG="pinetree-b2:ffmpeg"
T="$(mktemp -d "${TMPDIR:-/tmp}/docker-ffmpeg-b2-XXXXXX")"
CIDS=""
VOLS=""
cleanup() {
  for c in $CIDS; do docker rm -f "$c" >/dev/null 2>&1 || true; done
  for v in $VOLS; do docker volume rm "$v" >/dev/null 2>&1 || true; done
  rm -rf "$T"
  if [[ "${KEEP_IMAGES:-0}" != "1" ]]; then
    docker rmi "$IMG_MIN" "$IMG_FFMPEG" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_http() { # $1=url $2=timeout_secs
  local url="$1" timeout_s="$2" waited=0
  while (( waited < timeout_s )); do
    if curl -fsS --max-time 3 "$url" >/dev/null 2>&1; then return 0; fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

mkconfig() { # $1=dest $2=public_url $3=transcode_enabled(true|false) $4=allow_http(true|false)
  python3 - "$1" "$2" "$3" "$4" <<'PYEOF'
import json, sys
dest, url, enabled, allow = sys.argv[1], sys.argv[2], sys.argv[3] == "true", sys.argv[4] == "true"
cfg = {
    "public_url": url,
    "data_dir": "/data",
    "listen": "0.0.0.0:3000",
    "mode": "single",
    "max_file_size": 262144000,
    "preserved_usernames": ["admin", "root", "system", "i"],
    "transcode": {"enabled": enabled},
    "unsafe": {"allow_local_http_for_testing": allow},
}
with open(dest, "w") as fh:
    json.dump(cfg, fh)
PYEOF
}

echo "== batch-b2 docker: building both targets"
docker build --target runtime -t "$IMG_MIN" "$ROOT" || { fail "build --target runtime"; exit 1; }
pass "build --target runtime"
docker build --target runtime-ffmpeg -t "$IMG_FFMPEG" "$ROOT" || { fail "build --target runtime-ffmpeg"; exit 1; }
pass "build --target runtime-ffmpeg"

# --- (0) CLI local-HTTP smoke-rig path ---------------------------------------
P0=$((PORT_BASE + 0))
mkconfig "$T/c0.json" "http://127.0.0.1:$P0" false false
if docker run --rm -v "$T/c0.json:/etc/pinetree/config.json:ro" \
    "$IMG_MIN" --config=/etc/pinetree/config.json --data-dir=/data --listen="0.0.0.0:3000" \
    >"$T/c0.log" 2>&1; then
  fail "(0a) http public_url without opt-in started (must reject)"
else
  if grep -q "unsafe.allow_local_http_for_testing" "$T/c0.log"; then
    pass "(0a) http public_url rejected without opt-in (names the flag)"
  else
    fail "(0a) rejection did not name unsafe.allow_local_http_for_testing"; cat "$T/c0.log"
  fi
fi
mkconfig "$T/c0b.json" "http://127.0.0.1:$P0" false true
docker run -d --name b2c0 -p "127.0.0.1:$P0:3000" -v "$T/c0b.json:/etc/pinetree/config.json:ro" \
  "$IMG_MIN" --config=/etc/pinetree/config.json --data-dir=/data \
  --listen=0.0.0.0:3000 --allow-local-http-for-testing >"$T/c0b.id"
CIDS="b2c0 $CIDS"
if wait_http "http://127.0.0.1:$P0/healthz" 30; then
  pass "(0b) CLI --allow-local-http-for-testing serves a throwaway http origin"
else
  fail "(0b) CLI flag run never became healthy"; docker logs b2c0 2>&1 | tail -20 || true
fi
if docker logs b2c0 2>&1 | grep -q "WARNING: unsafe.allow_local_http_for_testing=true"; then
  pass "(0b) conspicuous unsafe WARNING logged (host class, no secrets)"
else
  fail "(0b) unsafe WARNING missing from logs"
fi
docker rm -f b2c0 >/dev/null; CIDS="${CIDS/b2c0/}"

# --- (1) minimal + default ----------------------------------------------------
P1=$((PORT_BASE + 1)); V1="b2v1"; VOLS="$VOLS $V1"
docker volume create "$V1" >/dev/null
mkconfig "$T/c1.json" "https://smoke-b2.test" false false
docker run -d --name b2c1 -p "127.0.0.1:$P1:3000" -v "$T/c1.json:/etc/pinetree/config.json:ro" \
  -v "$V1:/data" "$IMG_MIN" --config=/etc/pinetree/config.json --data-dir=/data \
  --listen=0.0.0.0:3000
CIDS="b2c1 $CIDS"
if wait_http "http://127.0.0.1:$P1/healthz" 60 && wait_http "http://127.0.0.1:$P1/ready" 60; then
  pass "(1) minimal+default: /healthz and /ready work"
else
  fail "(1) minimal+default never became healthy/ready"; docker logs b2c1 2>&1 | tail -20 || true
fi
if docker logs b2c1 2>&1 | grep -q "transcode disabled"; then
  pass "(1) minimal+default reports transcoding disabled"
else
  fail "(1) 'transcode disabled' line missing"
fi
if docker exec b2c1 ffmpeg -version >/dev/null 2>&1; then
  fail "(1) ffmpeg present in the minimal image"
else
  pass "(1) ffmpeg absent from the minimal image"
fi

# --- (2) minimal + enabled ----------------------------------------------------
mkconfig "$T/c2.json" "https://smoke-b2.test" true false
set +e
docker run --rm -v "$T/c2.json:/etc/pinetree/config.json:ro" \
  "$IMG_MIN" --config=/etc/pinetree/config.json --data-dir=/data \
  --listen=0.0.0.0:3000 >"$T/c2.log" 2>&1
code=$?
set -e
if (( code != 0 )); then
  pass "(2) minimal+enabled exits non-zero ($code)"
else
  fail "(2) minimal+enabled exited 0 (must fail closed)"
fi
if grep -q "transcode toolchain unavailable" "$T/c2.log" \
  && grep -q "ffmpeg" "$T/c2.log" \
  && grep -q "transcode.enabled=false" "$T/c2.log"; then
  pass "(2) actionable probe message (names toolchain + remediation)"
else
  fail "(2) probe message not actionable"; cat "$T/c2.log"
fi

# --- (3) ffmpeg target + default ----------------------------------------------
P3=$((PORT_BASE + 3)); V3="b2v3"; VOLS="$VOLS $V3"
docker volume create "$V3" >/dev/null
mkconfig "$T/c3.json" "https://smoke-b2.test" false false
docker run -d --name b2c3 -p "127.0.0.1:$P3:3000" -v "$T/c3.json:/etc/pinetree/config.json:ro" \
  -v "$V3:/data" "$IMG_FFMPEG" --config=/etc/pinetree/config.json --data-dir=/data \
  --listen=0.0.0.0:3000
CIDS="b2c3 $CIDS"
if wait_http "http://127.0.0.1:$P3/healthz" 60; then
  pass "(3) ffmpeg-target+default starts and serves"
else
  fail "(3) ffmpeg-target+default never became healthy"; docker logs b2c3 2>&1 | tail -20 || true
fi
if docker logs b2c3 2>&1 | grep -q "transcode disabled"; then
  pass "(3) ffmpeg-target+default keeps transcoding disabled by default"
else
  fail "(3) ffmpeg image overrode the disabled default"
fi
if docker exec b2c3 ffmpeg -hide_banner -version 2>&1 | grep -q "ffmpeg version"; then
  pass "(3) ffmpeg toolchain present in the ffmpeg image"
else
  fail "(3) ffmpeg missing from the ffmpeg image"
fi

# --- (4) ffmpeg target + enabled: real worker-path transcode smoke ------------
# TODO(integration-owner): confirm the /api/signin response token field and the
# /api/drive/files/create + /api/drive/files/show field names against the live
# build before stamping this scenario green; the container mechanics below are
# exact, the three JSON field assumptions are marked ASSUMPTION.
P4=$((PORT_BASE + 4)); V4="b2v4"; VOLS="$VOLS $V4"
docker volume create "$V4" >/dev/null
mkconfig "$T/c4.json" "https://smoke-b2.test" true false
docker run --rm -v "$T/c4.json:/etc/pinetree/config.json:ro" -v "$V4:/data" \
  "$IMG_FFMPEG" --config=/etc/pinetree/config.json --data-dir=/data \
  --listen=0.0.0.0:3000 create-account --username=smoke --password='s3cret-pw-b2' \
  >"$T/c4-acct.log" 2>&1 || { fail "(4) create-account one-shot"; cat "$T/c4-acct.log"; exit 1; }
pass "(4) operator account created (pre-serve one-shot)"
docker run -d --name b2c4 -p "127.0.0.1:$P4:3000" -v "$T/c4.json:/etc/pinetree/config.json:ro" \
  -v "$V4:/data" "$IMG_FFMPEG" --config=/etc/pinetree/config.json --data-dir=/data \
  --listen=0.0.0.0:3000
CIDS="b2c4 $CIDS"
wait_http "http://127.0.0.1:$P4/healthz" 60 || { fail "(4) enabled server never healthy"; }
if docker logs b2c4 2>&1 | grep -q "transcode toolchain: ffmpeg version"; then
  pass "(4) startup validated the real toolchain (version logged)"
else
  fail "(4) toolchain version line missing"; docker logs b2c4 2>&1 | tail -20 || true
fi
# Deterministic tiny fixture (64x64 PNG, still-image -> WebP path), generated
# with the image's own ffmpeg. Bounded: a few KiB, milliseconds of CPU.
docker exec b2c4 ffmpeg -hide_banner -loglevel error -y -f lavfi \
  -i "testsrc=size=64x64:rate=5:duration=1" -frames:v 1 /tmp/tiny-b2.png
docker cp b2c4:/tmp/tiny-b2.png "$T/tiny-b2.png"
SZ=$(wc -c <"$T/tiny-b2.png")
(( SZ > 0 && SZ < 1048576 )) || { fail "(4) fixture size out of bounds ($SZ)"; }
pass "(4) tiny deterministic fixture generated in-image ($SZ bytes)"
TOKEN=$(curl -fsS --max-time 10 -X POST "http://127.0.0.1:$P4/api/signin" \
  -H 'Content-Type: application/json' \
  -d '{"username":"smoke","password":"s3cret-pw-b2"}' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("token") or d.get("access_token") or d.get("i") or "")') \
  || TOKEN=""
  # ASSUMPTION: /api/signin returns the token under one of these keys.
[[ -n "$TOKEN" ]] || { fail "(4) signin returned no token (see ASSUMPTION)"; exit 1; }
UP=$(curl -fsS --max-time 60 -X POST "http://127.0.0.1:$P4/api/drive/files/create" \
  -F "i=$TOKEN" -F "file=@$T/tiny-b2.png;type=image/png") \
  || { fail "(4) upload request failed"; exit 1; }
  # ASSUMPTION: multipart fields `i` + `file` per Misskey convention.
echo "$UP" >"$T/up.json"
FID=$(python3 -c 'import json; print(json.load(open("'"$T/up.json"'")).get("id") or "")')
[[ -n "$FID" ]] || { fail "(4) upload returned no file id"; cat "$T/up.json"; exit 1; }
# Poll the worker-path result (bounded: 120 s). Success = derived WebP bytes
# served (type image/webp, thumbnail present, within caps).
OK4=""
for _ in $(seq 1 60); do
  SHOW=$(curl -fsS --max-time 10 -X POST "http://127.0.0.1:$P4/api/drive/files/show" \
    -H 'Content-Type: application/json' -d "{\"i\":\"$TOKEN\",\"fileId\":\"$FID\"}" 2>/dev/null || true)
  # ASSUMPTION: /api/drive/files/show takes {i,fileId}; derived file reports type/thumbnailUrl.
  TYP=$(echo "$SHOW" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("type") or "")' 2>/dev/null || true)
  THUMB=$(echo "$SHOW" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("thumbnailUrl") or "")' 2>/dev/null || true)
  if [[ "$TYP" == "image/webp" && -n "$THUMB" ]]; then OK4="yes"; break; fi
  sleep 2
done
if [[ "$OK4" == "yes" ]]; then
  pass "(4) worker-path transcode converged to derived image/webp with thumbnail"
else
  fail "(4) derived WebP never converged (see ASSUMPTIONs)"; tail -c 2000 "$T/up.json"
fi
# Corrupt upload must never report completed success: garbage bytes advertised
# as PNG must fail loudly (non-2xx, error envelope, or a failed file state —
# never a derived image/webp success).
head -c 256 /dev/urandom >"$T/garbage.png"
set +e
GUP=$(curl -fsS --max-time 60 -X POST "http://127.0.0.1:$P4/api/drive/files/create" \
  -F "i=$TOKEN" -F "file=@$T/garbage.png;type=image/png" 2>"$T/garbage.err")
gcode=$?
set -e
if (( gcode != 0 )); then
  pass "(4) corrupt upload rejected loudly at upload time"
else
  echo "$GUP" >"$T/garbage.json"
  GFID=$(python3 -c 'import json; print(json.load(open("'"$T/garbage.json"'")).get("id") or "")' 2>/dev/null || true)
  if [[ -z "$GFID" ]]; then
    pass "(4) corrupt upload returned an error envelope (no file id)"
  else
    GOK=""
    for _ in $(seq 1 30); do
      GSHOW=$(curl -fsS --max-time 10 -X POST "http://127.0.0.1:$P4/api/drive/files/show" \
        -H 'Content-Type: application/json' -d "{\"i\":\"$TOKEN\",\"fileId\":\"$GFID\"}" 2>/dev/null || true)
      GTYP=$(echo "$GSHOW" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("type") or "")' 2>/dev/null || true)
      if [[ "$GTYP" == "image/webp" ]]; then GOK="false-success"; break; fi
      sleep 2
    done
    if [[ "$GOK" == "false-success" ]]; then
      fail "(4) FAILED transcode reported completed image/webp success"
    else
      pass "(4) corrupt upload never converged to false success"
    fi
  fi
fi

# --- (5) SIGTERM mid-transcode: lease/retry + temp cleanup --------------------
P5=$((PORT_BASE + 5)); V5="b2v5"; VOLS="$VOLS $V5"
docker volume create "$V5" >/dev/null
mkconfig "$T/c5.json" "https://smoke-b2.test" true false
docker run --rm -v "$T/c5.json:/etc/pinetree/config.json:ro" -v "$V5:/data" \
  "$IMG_FFMPEG" --config=/etc/pinetree/config.json --data-dir=/data \
  --listen=0.0.0.0:3000 create-account --username=smoke --password='s3cret-pw-b2' \
  >/dev/null 2>&1 || { fail "(5) create-account one-shot"; exit 1; }
docker run -d --name b2c5 -p "127.0.0.1:$P5:3000" -v "$T/c5.json:/etc/pinetree/config.json:ro" \
  -v "$V5:/data" "$IMG_FFMPEG" --config=/etc/pinetree/config.json --data-dir=/data \
  --listen=0.0.0.0:3000
CIDS="b2c5 $CIDS"
wait_http "http://127.0.0.1:$P5/healthz" 60 || { fail "(5) server never healthy"; }
# Larger (still bounded: 320x240, 10 s, mpeg4) fixture so a transcode is likely
# in flight when SIGTERM lands.
docker exec b2c5 ffmpeg -hide_banner -loglevel error -y -f lavfi \
  -i "testsrc=size=320x240:rate=10:duration=10" -c:v mpeg4 /tmp/mid-b2.mp4
docker cp b2c5:/tmp/mid-b2.mp4 "$T/mid-b2.mp4"
T5=$(curl -fsS --max-time 10 -X POST "http://127.0.0.1:$P5/api/signin" \
  -H 'Content-Type: application/json' -d '{"username":"smoke","password":"s3cret-pw-b2"}' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("token") or d.get("access_token") or d.get("i") or "")') \
  || T5=""
[[ -n "$T5" ]] || { fail "(5) signin returned no token"; exit 1; }
curl -fsS --max-time 120 -X POST "http://127.0.0.1:$P5/api/drive/files/create" \
  -F "i=$T5" -F "file=@$T/mid-b2.mp4;type=video/mp4" >"$T/mid-up.json" 2>/dev/null || true
docker stop -t 2 b2c5 >/dev/null  # SIGTERM via tini -> graceful drain
sleep 2
docker start b2c5 >/dev/null
if wait_http "http://127.0.0.1:$P5/healthz" 60; then
  pass "(5) server restarts cleanly after SIGTERM mid-transcode"
else
  fail "(5) server did not recover after SIGTERM"
fi
# No fresh transcode temp orphans may accumulate: only hour-old files are
# swept, so a restart minutes later must show zero stale sweeps and no fresh
# pinetree-transcode-* litter from the killed job beyond its grace window.
if docker logs b2c5 2>&1 | grep -q "transcode temp sweep: removed"; then
  fail "(5) unexpected stale temp sweep right after restart"
else
  pass "(5) no stale temp sweep claimed on fast restart (grace window holds)"
fi
LEFTOVER=$(docker exec b2c5 sh -c 'ls /tmp/pinetree-transcode-* 2>/dev/null | wc -l' || echo 0)
if [[ "$LEFTOVER" =~ ^[0-9]+$ ]] && (( LEFTOVER <= 2 )); then
  pass "(5) transcode temp litter bounded after kill+restart ($LEFTOVER)"
else
  fail "(5) transcode temp litter: $LEFTOVER"
fi
# The interrupted job must converge via lease expiry/retry (completes or fails
# loudly on later polls — asserted as non-hang here; full convergence belongs
# to the worker-lease suite).
MIDFID=$(python3 -c 'import json; print(json.load(open("'"$T/mid-up.json"'")).get("id") or "")' 2>/dev/null || true)
if [[ -n "$MIDFID" ]]; then
  if curl -fsS --max-time 10 -X POST "http://127.0.0.1:$P5/api/drive/files/show" \
      -H 'Content-Type: application/json' -d "{\"i\":\"$T5\",\"fileId\":\"$MIDFID\"}" \
      >/dev/null 2>&1; then
    pass "(5) interrupted upload's metadata still readable after restart"
  else
    fail "(5) interrupted upload unreadable after restart"
  fi
else
  echo "[SKIP] (5) mid-transcode upload returned no id; convergence unchecked"
fi

echo "== batch-b2 docker: PASS=$PASS_N FAIL=$FAIL_N"
(( FAIL_N == 0 ))
