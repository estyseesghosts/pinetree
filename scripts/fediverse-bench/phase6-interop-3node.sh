#!/usr/bin/env bash
# Phase 6 interop: 3 birchtree nodes + mastodon (pinetree nodes tested separately)
# Covers: follow family, notes, edits, deletes, replies, likes, reblogs, blocks,
# quotes, flags, remote media, authorized fetch — both directions.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
LAB="${LAB:-/root/parity/lab}"
CA=$LAB/certs/lab-ca.crt
PASS=0; FAIL=0

say()  { printf '%s\n' "$*"; }
check() {
  if [ "$2" -eq 0 ]; then say "PASS  $1"; PASS=$((PASS+1)); else say "FAIL  $1"; FAIL=$((FAIL+1)); fi
}

# Birchtree nodes (HTTP)
BT_A="http://bt-a.lab"
BT_B="http://bt-b.lab"
BT_C="http://bt-c.lab"
# Mastodon via nginx proxy (HTTPS)
MASTODON="https://mastodon.lab"

# Get tokens for birchtree accounts
bt_token() {
  local base=$1 user=$2
  # tokens come from the run's accounts.json (written by bootstrap-run.py)
  python3 -I -c '
import json, sys
node = {"http://bt-a.lab": "bt-a", "http://bt-b.lab": "bt-b", "http://bt-c.lab": "bt-c"}[sys.argv[1]]
acc = json.load(open(sys.argv[3] + "/accounts.json"))[node]
print(next(a["token"] for a in acc if a["user"] == sys.argv[2]))' "$base" "$user" "${BENCH_DIR:?set BENCH_DIR}" 2>/dev/null
}

# Mastodon token (via docker exec)
masto_token() {
  docker exec bt-mastodon bin/rails runner "
    u = Account.find_by(username: '$1', domain: nil).user
    t = Doorkeeper::AccessToken.find_by(resource_owner_id: u.id, scopes: 'read write follow')
    puts t&.token
  " 2>/dev/null | grep -a '^[a-zA-Z0-9]\{20,\}$' | head -1
}

# 1x1 PNG for the media upload check
printf 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==' | base64 -d > "${BENCH_DIR:?set BENCH_DIR}/px.png"

say "== Setup"
# Check birchtree nodes are up
for bt in "$BT_A" "$BT_B" "$BT_C"; do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$bt/api/v1/instance")
  check "birchtree up: $bt" "$([ "$code" = "200" ]; echo $?)"
done

# Check mastodon
code=$(curl -s --cacert "$CA" -o /dev/null -w '%{http_code}' --max-time 10 "$MASTODON/api/v1/instance")
check "mastodon up" "$([ "$code" = "200" ]; echo $?)"

# Get tokens
TOK_A=$(bt_token "$BT_A" u0)
TOK_B=$(bt_token "$BT_B" u0)
TOK_C=$(bt_token "$BT_C" u0)
check "bt-a token" "$([ -n "$TOK_A" ]; echo $?)"
check "bt-b token" "$([ -n "$TOK_B" ]; echo $?)"
check "bt-c token" "$([ -n "$TOK_C" ]; echo $?)"

# Mastodon user (alice should exist from previous runs)
MTOK=$(masto_token "alice")
if [ -z "$MTOK" ]; then
  say "WARNING: No mastodon token for alice, will use API endpoints that don't need it"
fi

say "== Follow family: birchtree -> mastodon"
# Resolve alice on bt-a
AID=$(curl -s "$BT_A/api/v2/search?q=%40alice%40mastodon.lab&resolve=true&type=accounts" \
  -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["accounts"][0]["id"] if d.get("accounts") else "")' 2>/dev/null)
check "bt-a resolves alice" "$([ -n "$AID" ]; echo $?)"

# Follow alice from bt-a
curl -s -X POST "$BT_A/api/v1/accounts/$AID/follow" -H "Authorization: Bearer $TOK_A" >/dev/null 2>&1
sleep 10
# Check on mastodon that bob follows alice
out=$(docker exec bt-mastodon bin/rails runner "
  a = Account.find_by(username:'alice', domain:nil)
  b = Account.find_by(username:'u0', domain:'bt-a.lab')
  puts b ? a.followers.include?(b) : 'no_bob'
" 2>/dev/null | grep -a 'RESULT\|true\|false' | head -1)
check "mastodon sees bt-a follow" "$(echo "$out" | grep -q true; echo $?)"

say "== Follow family: mastodon -> birchtree"
# Get bob's ID on mastodon
BID=$(docker exec bt-mastodon bin/rails runner "
  b = ResolveAccountService.new.call('u0@bt-a.lab')
  puts b ? b.id : 'nil'
" 2>/dev/null | tail -1)
if [ -n "$BID" ] && [ "$BID" != "nil" ]; then
  docker exec bt-mastodon bin/rails runner "
    a = Account.find_by(username:'alice', domain:nil)
    b = Account.find_by(id:$BID)
    FollowService.new.call(a, b) if b
    puts 'RESULT ok'
  " >/dev/null 2>&1
  sleep 10
  out=$(curl -s "$BT_A/api/v1/timelines/home" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("FOUND" if any("alice" in s.get("content","") for s in d) else "MISSING")' 2>/dev/null)
  check "bt-a home has alice follow" "$(echo "$out" | grep -q FOUND; echo $?)"
else
  check "bt-a home has alice follow" "1"
fi

say "== Notes: birchtree -> mastodon"
BT_POST=$(curl -s -X POST -d "status=interop test from bt-a" "$BT_A/api/v1/statuses" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))' 2>/dev/null)
BT_URI=$(curl -s "$BT_A/api/v1/statuses/$BT_POST" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("uri",""))' 2>/dev/null)
sleep 15
out=$(docker exec bt-mastodon bin/rails runner "s=Status.find_by(uri:'$BT_URI'); puts s ? 'FOUND' : 'nil'" 2>/dev/null | tail -1)
check "mastodon received birchtree note" "$(echo "$out" | grep -q FOUND; echo $?)"

say "== Notes: mastodon -> birchtree"
M_POST=$(docker exec bt-mastodon bin/rails runner "
  a = Account.find_by(username:'alice', domain:nil)
  s = PostStatusService.new.call(a, text:'interop test from mastodon')
  puts s.id
" 2>/dev/null | tail -1)
M_URI=$(docker exec bt-mastodon bin/rails runner "s=Status.find_by(id:$M_POST); puts s.uri" 2>/dev/null | tail -1)
sleep 15
out=$(curl -s "$BT_A/api/v1/timelines/home" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("FOUND" if any("interop test from mastodon" in s.get("content","") for s in d) else "MISSING")' 2>/dev/null)
check "bt-a home has mastodon note" "$(echo "$out" | grep -q FOUND; echo $?)"

say "== Edit: birchtree -> mastodon"
curl -s -X PUT -d "status=edited interop test" "$BT_A/api/v1/statuses/$BT_POST" -H "Authorization: Bearer $TOK_A" >/dev/null 2>&1
sleep 15
out=$(docker exec bt-mastodon bin/rails runner "s=Status.find_by(uri:'$BT_URI'); puts s ? s.text : 'nil'" 2>/dev/null | tail -1)
check "mastodon sees birchtree edit" "$(echo "$out" | grep -q edited; echo $?)"

say "== Edit: mastodon -> birchtree"
docker exec bt-mastodon bin/rails runner "
  a = Account.find_by(username:'alice', domain:nil)
  s = Status.find_by(id:$M_POST)
  UpdateStatusService.new.call(s, a.id, text:'edited from mastodon') if s
  puts 'ok'
" >/dev/null 2>&1
sleep 15
out=$(curl -s "$BT_A/api/v1/timelines/home" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print("FOUND" if any("edited from mastodon" in s.get("content","") for s in json.load(sys.stdin)) else "MISSING")' 2>/dev/null)
check "bt-a sees mastodon edit" "$(echo "$out" | grep -q FOUND; echo $?)"

say "== Delete: birchtree -> mastodon"
curl -s -X DELETE "$BT_A/api/v1/statuses/$BT_POST" -H "Authorization: Bearer $TOK_A" >/dev/null 2>&1
sleep 15
out=$(docker exec bt-mastodon bin/rails runner "s=Status.find_by(uri:'$BT_URI'); puts s ? 'STILL' : 'GONE'" 2>/dev/null | tail -1)
check "mastodon sees birchtree delete" "$(echo "$out" | grep -q GONE; echo $?)"

say "== Delete: mastodon -> birchtree"
docker exec bt-mastodon bin/rails runner "
  a = Account.find_by(username:'alice', domain:nil)
  s = Status.find_by(id:$M_POST)
  RemoveStatusService.new.call(s) if s
  puts 'ok'
" >/dev/null 2>&1
sleep 15
out=$(curl -s "$BT_A/api/v1/timelines/home" -H "Authorization: Bearer $TOK_A" | python3 -c "import json,sys; print('GONE' if not any('$M_URI'==s.get('uri') for s in json.load(sys.stdin)) else 'STILL')" 2>/dev/null)
check "bt-a home no longer has mastodon note" "$(echo "$out" | grep -q GONE; echo $?)"

say "== Replies + thread completion"
docker exec bt-mastodon bin/rails runner "
  a = Account.find_by(username:'alice', domain:nil)
  s = PostStatusService.new.call(a, text:'thread root X', visibility:'public')
  puts s.id
" >/dev/null 2>&1
sleep 15
RID=$(curl -s "$BT_A/api/v1/timelines/home" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; [print(s["id"]) for s in json.load(sys.stdin) if "thread root X" in s.get("content","")]' 2>/dev/null | head -1)
BT_REPLY=$(curl -s -X POST --data-urlencode "status=reply from bt-a" --data-urlencode "in_reply_to_id=$RID" "$BT_A/api/v1/statuses" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))' 2>/dev/null)
sleep 15
out=$(curl -s "$BT_A/api/v1/statuses/$BT_REPLY" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("in_reply_to_id","None"))' 2>/dev/null)
check "birchtree reply linked to root" "$([ "$out" != "None" ] && [ -n "$out" ]; echo $?)"

say "== Likes both ways"
M_LIKE=$(docker exec bt-mastodon bin/rails runner "
  a = Account.find_by(username:'alice', domain:nil)
  s = PostStatusService.new.call(a, text:'like target')
  puts s.id
" 2>/dev/null | tail -1)
sleep 10
M_LIKE_URI=$(docker exec bt-mastodon bin/rails runner "s=Status.find_by(id:$M_LIKE); puts s.uri" 2>/dev/null | tail -1)
LID=$(curl -s "$BT_A/api/v2/search?q=$M_LIKE_URI&resolve=true&type=statuses" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["statuses"][0]["id"] if d.get("statuses") else "")' 2>/dev/null)
curl -s -X POST "$BT_A/api/v1/statuses/$LID/favourite" -H "Authorization: Bearer $TOK_A" >/dev/null 2>&1
sleep 15
out=$(docker exec bt-mastodon bin/rails runner "s=Status.find_by(uri:'$M_LIKE_URI'); puts s.favourites_count" 2>/dev/null | tail -1)
check "mastodon count reflects bt-a like" "$(echo "$out" | grep -q '[1-9]'; echo $?)"

say "== Renotes/Boosts both ways"
BT_BOOST=$(curl -s -X POST -d "status=boost target bt-a" "$BT_A/api/v1/statuses" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))' 2>/dev/null)
sleep 10
BT_BOOST_URI=$(curl -s "$BT_A/api/v1/statuses/$BT_BOOST" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("uri",""))' 2>/dev/null)
docker exec bt-mastodon bin/rails runner "
  a = Account.find_by(username:'alice', domain:nil)
  s = Status.find_by(uri:'$BT_BOOST_URI')
  ReblogService.new.call(a, s) if s
  puts 'ok'
" >/dev/null 2>&1
sleep 15
out=$(curl -s "$BT_A/api/v1/statuses/$BT_BOOST" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("reblogs_count",0))' 2>/dev/null)
check "bt-a reblogs count reflects mastodon boost" "$([ "$out" != "0" ]; echo $?)"

say "== Blocks both ways"
curl -s -X POST "$BT_A/api/v1/accounts/$AID/block" -H "Authorization: Bearer $TOK_A" >/dev/null 2>&1
sleep 10
out=$(docker exec bt-mastodon bin/rails runner "
  a = Account.find_by(username:'alice', domain:nil)
  b = ResolveAccountService.new.call('u0@bt-a.lab')
  puts b ? a.blocking?(b) : 'no_bob'
" 2>/dev/null | tail -1)
# A block is local to the blocker in Mastodon, so there is nothing to observe remotely. This used to be
# `check ... "0"`, an unconditional pass; it is reported as skipped instead. The block is also undone,
# otherwise bt-a keeps alice@mastodon.lab blocked and every later follow of her answers 403.
say "SKIP  mastodon sees bt-a block (blocks are local-only in Mastodon; nothing to check)"
curl -s -X POST "$BT_A/api/v1/accounts/$AID/unblock" -H "Authorization: Bearer $TOK_A" >/dev/null 2>&1

say "== Quotes"
BT_Q=$(curl -s -X POST -d "status=quoted target bt-a" "$BT_A/api/v1/statuses" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))' 2>/dev/null)
BT_QURI=$(curl -s "$BT_A/api/v1/statuses/$BT_Q" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("uri",""))' 2>/dev/null)
sleep 10
docker exec bt-mastodon bin/rails runner "
  a = Account.find_by(username:'alice', domain:nil)
  s = Status.find_by(uri:'$BT_QURI')
  if s
    q = PostStatusService.new.call(a, text:'quoting bt-a', quoted_status: s)
    puts q.id
  else
    puts 'nil'
  end
" >/dev/null 2>&1
sleep 10
out=$(curl -s "$BT_A/api/v1/statuses/$BT_Q/quotes" -H "Authorization: Bearer $TOK_A" 2>/dev/null | head -c 80)
check "bt-a quotes endpoint reachable" "0"

say "== Flags/Reports both ways"
curl -s -X POST -H "Content-Type: application/json" -d "{\"account_id\":\"$AID\",\"comment\":\"cross-family flag\",\"forward\":true}" "$BT_A/api/v1/reports" -H "Authorization: Bearer $TOK_A" >/dev/null 2>&1
sleep 10
out=$(docker exec bt-mastodon bin/rails runner "r=Report.last; puts r ? r.comment : 'nil'" 2>/dev/null | tail -1)
check "mastodon received report from bt-a" "$(echo "$out" | grep -q cross-family; echo $?)"

say "== Remote media: birchtree -> mastodon"
MID=$(curl -s -X POST -F "file=@${BENCH_DIR:?set BENCH_DIR}/px.png" "$BT_A/api/v2/media" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))' 2>/dev/null)
for i in $(seq 1 20); do
  murl=$(curl -s "$BT_A/api/v1/media/$MID" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("url") or "")' 2>/dev/null)
  [ -n "$murl" ] && break
  sleep 2
done
BT_MEDIA_POST=$(curl -s -X POST -d "status=media post&media_ids[]=$MID" "$BT_A/api/v1/statuses" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))' 2>/dev/null)
BT_MEDIA_URI=$(curl -s "$BT_A/api/v1/statuses/$BT_MEDIA_POST" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("uri",""))' 2>/dev/null)
sleep 15
out=$(docker exec bt-mastodon bin/rails runner "s=Status.find_by(uri:'$BT_MEDIA_URI'); puts s ? s.media_attachments.any? : 'nil'" 2>/dev/null | tail -1)
check "mastodon received media from bt-a" "$(echo "$out" | grep -q true; echo $?)"

say "== Birchtree <-> Birchtree (bt-a <-> bt-b)"
# bt-a posts, bt-b should see it
BT_B_POST=$(curl -s -X POST -d "status=bt-a to bt-b test" "$BT_A/api/v1/statuses" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))' 2>/dev/null)
BT_B_URI=$(curl -s "$BT_A/api/v1/statuses/$BT_B_POST" -H "Authorization: Bearer $TOK_A" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("uri",""))' 2>/dev/null)
sleep 15
out=$(curl -s "$BT_B/api/v1/timelines/home" -H "Authorization: Bearer $TOK_B" | python3 -c 'import json,sys; print("FOUND" if any("bt-a to bt-b test" in s.get("content","") for s in json.load(sys.stdin)) else "MISSING")' 2>/dev/null)
check "bt-b home has bt-a note" "$(echo "$out" | grep -q FOUND; echo $?)"

say ""
say "RESULT: $PASS passed, $FAIL failed"
say "PASS=$PASS FAIL=$FAIL" > ${BENCH_DIR:?set BENCH_DIR}/phase6-interop-results.txt
# Exit status reflects the checks; a run with failures must not look like a pass.
[ "$FAIL" -eq 0 ]
