#!/usr/bin/env python3
"""Phase A: full-mesh follows across all 30 accounts (30x29=870 edges) + verify.

Warms pinetree RSA keys first (lazy generation is slow in dev builds and can
time out a peer's first signed fetch). Verifies each user's follower set.

Retry policy: only transient failures are retried after a settle delay
(connection/timeout exceptions, HTTP 5xx, HTTP 429). Deterministic failures
(HTTP 4xx such as 404 "account not found" or "Not implemented") are never
retried; they are recorded separately in follows.json with their status and
body so they can be inspected.
"""
import json, sys, time, urllib.request
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib
from lib import load_nodes, HOSTS, fetch_bytes, BENCH_DIR

OUT_PATHS = [os.path.join(BENCH_DIR, "follows.json"), os.path.join(os.path.dirname(os.path.abspath(__file__)), "follows.json")]
RETRY_SETTLE_S = 30

nodes = load_nodes()
nodes = {k: v for k, v in nodes.items() if len(v.users) > 0}
__import__('lib').run_header('follows', nodes)
assert len(nodes) >= 5, f"Expected at least 5 nodes with accounts, got {len(nodes)}: {nodes.keys()}"

# Fast-fail handle resolution for the mesh. lib.Node.resolve retries 5 times with sleeps
# up to 20s, so every handle a Mastodon-family node has never received activity from costs
# about 60s. Here it gets 2 attempts; the retry pass below gives late-arriving accounts a
# second chance. Same lookups as lib.Node.resolve (lookup, then search with resolve=true).
def _resolve_fast(self, acct, user):
    if acct in self._ids: return self._ids[acct]
    if self.family:
        return _orig_resolve(self, acct, user)
    for attempt in range(2):
        s, v = self.mapi("GET", "/accounts/lookup?" + urllib.parse.urlencode({"acct": acct}), user=user)
        if s == 200 and isinstance(v, dict) and v.get("id"):
            self._ids[acct] = v["id"]; return v["id"]
        s, v = self.mapi2("GET", "/search?" + urllib.parse.urlencode(
            {"q": acct, "resolve": "true", "type": "accounts"}), user=user)
        if s == 200 and isinstance(v, dict) and v.get("accounts"):
            self._ids[acct] = v["accounts"][0]["id"]; return v["accounts"][0]["id"]
        time.sleep(1)
    raise AssertionError((self.node, "resolve", acct, s, v))
_orig_resolve = lib.Node.resolve
lib.Node.resolve = _resolve_fast

def is_transient(status):
    """Only these are worth one retry: transport errors, 5xx, and 429 rate limits."""
    return status in (None, "exc", 429) or (isinstance(status, int) and status >= 500)

# 1. warm RSA keys on both pinetree nodes (AP fetch of every local actor)
# Every pinetree node in the topology (pt-a/pt-b/pt-c; the first run used pt-ks/pt-mk).
for n in [k for k in nodes if k.startswith("pt-")]:
    node = nodes[n]
    for u in node.users:
        uid = node.local_id(u)
        st, body = fetch_bytes(f"http://{HOSTS[n]}/users/{uid}", timeout=60)
        print(f"warm {n}/{u.name}: {st}", flush=True)

# 2. full mesh follows
t0 = time.time()
total = 0
def try_follow(node, u, pu):
    """Returns (status, detail). status is an int HTTP code, or 'exc' for a transport error."""
    try:
        return node.follow(u, pu.acct)
    except AssertionError as e:
        # lib.Node.resolve raises (node, "resolve", acct, status, body)
        a = e.args[0] if e.args and isinstance(e.args[0], tuple) else ()
        status = a[3] if len(a) >= 5 else "exc"
        body = a[4] if len(a) >= 5 else e.args
        return status, str(body)[:300]

def describe(node, u, pu):
    return f"{node.node}/{u.name} -> {pu.acct}"

first_failed = []  # dicts: edge, status, detail, transient
for src, node in nodes.items():
    for u in node.users:
        for dst, peer in nodes.items():
            if dst == src: continue
            for pu in peer.users:
                s, v = try_follow(node, u, pu)
                total += 1
                if s not in (200, 409):  # 409-ish = already following
                    rec = {"edge": describe(node, u, pu), "status": s, "detail": v,
                           "transient": is_transient(s), "_job": (node, u, pu)}
                    first_failed.append(rec)
                    print(f"FOLLOW_FAIL {src}/{u.name} -> {pu.acct}: {s} {v}", flush=True)
    print(f"{src}: follows done ({time.time()-t0:.0f}s elapsed, {len(first_failed)} failed so far)", flush=True)
first_pass_fails = len(first_failed)
transient = [r for r in first_failed if r["transient"]]
deterministic = [r for r in first_failed if not r["transient"]]
print(f"first pass: {first_pass_fails} failed; {len(transient)} transient, {len(deterministic)} deterministic", flush=True)

# 2b. retry transient failures only, after a settle delay
retried = []
if transient:
    print(f"settling {RETRY_SETTLE_S}s before retrying {len(transient)} transient failures", flush=True)
    time.sleep(RETRY_SETTLE_S)
for rec in transient:
    node, u, pu = rec["_job"]
    s, v = try_follow(node, u, pu)
    ok = s in (200, 409)
    retried.append({"edge": rec["edge"], "first_status": rec["status"], "retry_status": s,
                    "recovered": ok, "detail": None if ok else v})
    print(f"FOLLOW_RETRY {rec['edge']}: first={rec['status']} retry={s} recovered={ok}", flush=True)
recovered = sum(1 for r in retried if r["recovered"])
still_transient = [r for r in retried if not r["recovered"]]
for r in deterministic:
    print(f"FOLLOW_FAIL_DETERMINISTIC {r['edge']}: {r['status']} {r['detail']}", flush=True)

def strip(rs): return [{k: v for k, v in r.items() if k != "_job"} for r in rs]
by_status = {}
for r in deterministic:
    key = str(r["status"])
    by_status[key] = by_status.get(key, 0) + 1
errs = len(still_transient) + len(deterministic)
print(f"after retry: transient retried={len(retried)} recovered={recovered} "
      f"still_failing={len(still_transient)}; deterministic={len(deterministic)} {by_status}", flush=True)

# 3. verify convergence: every user's followers == everyone else (29)
deadline = time.time() + 600
missing = {}
while True:
    missing = {}
    for src, node in nodes.items():
        for u in node.users:
            uid = node.local_id(u)
            followers = node.followers_accts(uid, node.users[0])
            want = {pu.acct for dn, peer in nodes.items() if dn != src for pu in peer.users}
            lack = want - followers
            if lack: missing[f"{src}/{u.name}"] = sorted(lack)
    if not missing or time.time() > deadline:
        break
    print(f"...{sum(len(v) for v in missing.values())} missing edges; waiting 20s", flush=True)
    time.sleep(20)

result = {
    "total_follows": total,
    "first_pass_failures": first_pass_fails,
    "transient_first_pass": len(transient),
    "transient_retried": retried,
    "transient_recovered": recovered,
    "transient_still_failing": len(still_transient),
    "deterministic_failures_n": len(deterministic),
    "deterministic_failures_by_status": by_status,
    "deterministic_failures": strip(deterministic),
    "errors": errs,
    "missing": missing,
}
for p in OUT_PATHS:
    json.dump(result, open(p, "w"), indent=1)
if missing:
    print("MISSING EDGES:")
    for k, v in list(missing.items())[:20]: print(" ", k, "->", v)
    sys.exit(1)
print(f"FOLLOWS_OK total={total} errors={errs} elapsed={time.time()-t0:.0f}s")
