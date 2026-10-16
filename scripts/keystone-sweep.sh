#!/usr/bin/env bash
# Phase 3 — Keystone sweep: Keystone's exact pinetree-target calls
# (reconstructed from /root/keystone/frontend/src) against a THROWAWAY
# pinetree server. Never touches the play stack.
#
# Asserts 200s + shapes for: signin, i, i/update, users/show|search|notes,
# users/followers|following (Following[] — see note), notes/* (create, show,
# delete, replies, timelines, search, search-by-tag, reactions, unrenote),
# i/notifications + mark-all-as-read, drive/files (+multipart upload, update,
# delete), following/* (+requests/cancel on a locked account),
# i/revoke-token, meta, and honest 404s for antennas/* (out of profile).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${PINETREE_BIN:-$ROOT/../pinetree-target/debug/pinetree}"
if [[ ! -x "$BIN" ]]; then
  BIN="$ROOT/target/debug/pinetree"
fi
TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/pinetree-keysweep-XXXXXX")"
DATA_DIR="$TMPDIR/data"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

if [[ ! -x "$BIN" ]]; then
  echo "[keysweep] building pinetree binary..." >&2
  (cd "$ROOT" && cargo build --offline)
fi
[[ -x "$BIN" ]] || { echo "KEYSWEEP_FAIL: binary missing at $BIN" >&2; exit 1; }

pick_port() {
  for _ in $(seq 1 50); do
    local p=$((20000 + RANDOM % 40000))
    if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
      exec 3>&- || true
    else
      echo "$p"; return 0
    fi
  done
  return 1
}
PORT="$(pick_port)"
ORIGIN="http://127.0.0.1:$PORT"
"$BIN" --data-dir="$DATA_DIR" --listen="127.0.0.1:$PORT" --public-url="$ORIGIN" &
SERVER_PID=$!
for _ in $(seq 1 100); do
  if curl -fs -o /dev/null -m 2 "$ORIGIN/healthz" 2>/dev/null; then break; fi
  sleep 0.2
done
curl -fs -o /dev/null -m 2 "$ORIGIN/healthz" || {
  echo "KEYSWEEP_FAIL: server unhealthy at $ORIGIN" >&2; exit 1
}

PINETREE_ORIGIN="$ORIGIN" python3 - <<'PY_EOF'
import json, os, time, urllib.request, urllib.error

BASE = os.environ["PINETREE_ORIGIN"]
PASS, FAIL = [], []
def api(endpoint, params=None, token=None, raw_body=None):
    body = dict(params or {})
    if token:
        body["i"] = token
    data = raw_body if raw_body is not None else json.dumps(body).encode()
    req = urllib.request.Request(f"{BASE}/api/{endpoint}", data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, (json.loads(raw) if raw else None)
        except Exception:
            return e.code, {"_raw": raw}

def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""), flush=True)

def signup(username):
    req = urllib.request.Request(f"{BASE}/signup",
        data=json.dumps({"username": username, "password": "password123"}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())

def wait_for(name, fn, timeout=8):
    start = time.time()
    while time.time() - start < timeout:
        v = fn()
        if v:
            check(name, True)
            return v
        time.sleep(0.15)
    check(name, False, "timed out")
    return None

# meta (Composer.tsx:23, public)
s, meta = api("meta")
check("meta public", s == 200 and meta.get("name") == "Pinetree", f"s={s}")

# signin + i (auth.tsx)
a, b = signup("alice"), signup("bob")
TA, TB, IDA, IDB = a["token"], b["token"], a["id"], b["id"]
s, me = api("i", {}, TA)
check("i returns me", s == 200 and me["username"] == "alice", f"s={s}")
s, bad = api("i", {}, "nope")
check("bad token 401", s == 401, f"s={s}")

# i/update (Profile.tsx:117)
s, upd = api("i/update", {"name": "Alice K"}, TA)
check("i/update name", s == 200 and upd["name"] == "Alice K", f"s={s}")

# users/show + search (Profile.tsx, MfmRenderer, Composer, Search.tsx)
s, u = api("users/show", {"userId": IDB})
check("users/show by id", s == 200 and u["username"] == "bob", f"s={s}")
s, u = api("users/show", {"username": "bob"})
check("users/show by username", s == 200 and u["id"] == IDB, f"s={s}")
s, hits = api("users/search", {"query": "ali", "limit": 20})
check("users/search", s == 200 and any(x["username"] == "alice" for x in hits), f"s={s}")

# notes/create trio: post, reply, repost (Composer, TimelineFeed, Note.tsx)
s, c1 = api("notes/create", {"text": "keystone hello #sweep"}, TA)
N1 = c1["createdNote"]["id"]
check("notes/create post", s == 200 and bool(N1), f"s={s}")
s, c2 = api("notes/create", {"text": "replying", "replyId": N1}, TB)
R1 = c2["createdNote"]["id"]
check("notes/create reply", s == 200 and bool(R1), f"s={s}")
s, c3 = api("notes/create", {"renoteId": N1}, TB)
P1 = c3["createdNote"]["id"]
check("notes/create repost", s == 200, f"s={s}")

# notes/show + users/notes + notes/replies (Note.tsx, Profile.tsx)
s, shown = api("notes/show", {"noteId": N1})
check("notes/show public", s == 200 and shown["id"] == N1, f"s={s}")
s, un = api("users/notes", {"userId": IDA, "limit": 10, "withReplies": False}, TA)
check("users/notes", s == 200 and any(n["id"] == N1 for n in un), f"s={s}")
s, un2 = api("users/notes", {"userId": IDA, "limit": 1, "untilId": N1}, TA)
check("users/notes untilId", s == 200, f"s={s} n={len(un2) if isinstance(un2, list) else un2}")
s, reps = api("notes/replies", {"noteId": N1, "limit": 10})
check("notes/replies", s == 200 and any(n["id"] == R1 for n in reps), f"s={s}")

# reactions (TimelineFeed, Note.tsx)
s, _ = api("notes/reactions/create", {"noteId": N1, "reaction": "\u2764"}, TB)
check("reactions/create 204", s == 204, f"s={s}")
s, shown = api("notes/show", {"noteId": N1}, TB)
check("myReaction reflected", s == 200 and shown.get("myReaction") == "\u2764", f"s={s}")
s, _ = api("notes/reactions/delete", {"noteId": N1}, TB)
check("reactions/delete 204", s == 204, f"s={s}")

# timelines (Home/Local/Global routes)
s, home = api("notes/timeline", {"limit": 10}, TA)
check("notes/timeline", s == 200 and isinstance(home, list), f"s={s}")
s, local = api("notes/local-timeline", {"limit": 10})
check("notes/local-timeline", s == 200 and any(n["id"] == N1 for n in local), f"s={s}")
s, glob = api("notes/global-timeline", {"limit": 10})
check("notes/global-timeline", s == 200 and any(n["id"] == N1 for n in glob), f"s={s}")

# search (Search.tsx)
s, sr = api("notes/search", {"query": "keystone", "limit": 20}, TA)
check("notes/search", s == 200 and any(n["id"] == N1 for n in sr), f"s={s}")
s, tg = api("notes/search-by-tag", {"tag": "sweep", "limit": 20}, TA)
check("notes/search-by-tag", s == 200 and any(n["id"] == N1 for n in tg), f"s={s}")

# follow + followers/following + cancel-request on locked account
s, _ = api("following/create", {"userId": IDB}, TA)
check("following/create", s == 200, f"s={s}")
s, fwer = api("users/followers", {"userId": IDB, "limit": 10})
shape_ok = (s == 200 and len(fwer) == 1 and fwer[0].get("follower", {}).get("username") == "alice"
    and fwer[0].get("followeeId") == IDB and isinstance(fwer[0].get("createdAt"), str))
check("users/followers Following[] shape", shape_ok, f"s={s} body={json.dumps(fwer)[:200]}")
print("       NOTE: Keystone Profile.tsx types this as UserLite[]; the profile fixes the Misskey", flush=True)
print("       Following[] shape — Keystone must map x.follower!/x.followee! (own-repo follow-up).", flush=True)
s, fing = api("users/following", {"userId": IDA, "limit": 10})
check("users/following", s == 200 and len(fing) == 1 and fing[0].get("followee", {}).get("username") == "bob", f"s={s}")
c = signup("carol")
s, _ = api("i/update", {"isLocked": True}, c["token"])
s, _ = api("following/create", {"userId": c["id"]}, TB)
s, _ = api("following/requests/cancel", {"userId": c["id"]}, TB)
check("following/requests/cancel", s in (200, 204), f"s={s}")
s, _ = api("following/delete", {"userId": IDB}, TA)
check("following/delete", s == 200, f"s={s}")

# notifications + mark-all-as-read (Notifications.tsx)
s, _ = api("following/create", {"userId": IDB}, TA)
found = wait_for("i/notifications has follow", lambda: any(
    n.get("type") == "follow" for n in (api("i/notifications", {"limit": 20}, TB)[1] or [])))
# Keystone paginates with untilId (checked again below once two exist).
s, page1 = api("i/notifications", {"limit": 1}, TB)
check("i/notifications newest-first", s == 200 and len(page1) == 1, f"s={s}")
# Second notification (mention) so untilId pagination crosses a page boundary.
s, _ = api("notes/create", {"text": "ping @bob, two pages"}, TA)
found = wait_for("mention notification lands", lambda: any(
    n.get("type") == "mention" for n in (api("i/notifications", {"limit": 20, "markAsRead": False}, TB)[1] or [])))
s, p1 = api("i/notifications", {"limit": 1, "markAsRead": False}, TB)
s, p2 = api("i/notifications", {"limit": 10, "markAsRead": False, "untilId": p1[0]["id"]}, TB)
check("i/notifications untilId pagination", s == 200 and len(p1) == 1 and len(p2) >= 1 and all(
    n["id"] != p1[0]["id"] for n in p2), f"s={s}")
s, _ = api("notifications/mark-all-as-read", {}, TB)
s, after = api("i/notifications", {"limit": 20}, TB)
check("mark-all-as-read", s == 200 and all(n.get("isRead") for n in after), f"s={s}")

# drive: list + multipart upload + update + delete (DriveBrowser.tsx)
s, files = api("drive/files", {"limit": 10}, TA)
check("drive/files list", s == 200 and isinstance(files, list), f"s={s}")
boundary = "----keysweep1234"
png = bytes.fromhex("89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000d49444154789c626001000000ffff030000060005727601820000000049454e44ae426082")
parts = []
def field(name, value):
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n".encode())
field("i", TA)
parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="s.png"\r\nContent-Type: image/png\r\n\r\n'.encode() + png + b"\r\n")
parts.append(f"--{boundary}--\r\n".encode())
mreq = urllib.request.Request(f"{BASE}/api/drive/files/create", data=b"".join(parts),
    headers={"Content-Type": f"multipart/form-data; boundary={boundary}"}, method="POST")
with urllib.request.urlopen(mreq, timeout=15) as r:
    up = json.loads(r.read().decode())
    check("drive multipart upload", r.status == 200 and up.get("name") == "s.png", f"s={r.status}")
s, files = api("drive/files", {"limit": 10, "type": "image/*"}, TA)
check("drive/files type filter", s == 200 and any(f["id"] == up["id"] for f in files), f"s={s}")
s, du = api("drive/files/update", {"fileId": up["id"], "isSensitive": True}, TA)
check("drive/files/update", s == 200 and du.get("isSensitive") is True, f"s={s}")
s, _ = api("drive/files/delete", {"fileId": up["id"]}, TA)
check("drive/files/delete", s == 204, f"s={s}")

# unrepost + delete (TimelineFeed, Note.tsx). notes/delete enforces a 1 s
# minimum interval (shared with unrenote), so space them out.
s, _ = api("notes/unrenote", {"noteId": N1}, TB)
check("notes/unrenote", s == 204, f"s={s}")
time.sleep(1.2)
s, _ = api("notes/delete", {"noteId": R1}, TB)
check("notes/delete own", s == 204, f"s={s}")

# honest 404 for out-of-profile antennas (Keystone renders "not available")
s, ae = api("antennas/list", {}, TA)
check("antennas/list 404", s == 404 and (ae.get("error") or {}).get("code") == "UNKNOWN_API_ENDPOINT", f"s={s}")

# revoke last (kills the token)
s, second = api("signin", {"username": "bob", "password": "password123"})
T2 = second["token"]
s, _ = api("i/revoke-token", {}, T2)
s2, _ = api("i", {}, T2)
s1, _ = api("i", {}, TB)
check("i/revoke-token kills caller only", s2 == 401 and s1 == 200, f"revoked={s2} sibling={s1}")

print(f"\nKEYSWEEP: {len(PASS)} passed, {len(FAIL)} failed", flush=True)
if FAIL:
    print("FAILURES:", FAIL, flush=True)
    raise SystemExit(1)
print("KEYSWEEP_OK", flush=True)
PY_EOF
