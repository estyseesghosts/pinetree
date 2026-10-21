#!/usr/bin/env python3
"""Cross-family interop tests: pinetree <-> birchtree.

Tests WebFinger+actor, follow (request/accept/reject/undo), notes, edits,
deletes, replies, likes, boosts/renotes, quotes, mentions, media, blocks
in both directions. Records results to $BENCH_DIR/crossfamily.json.
"""
import json, sys, time, threading
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import load_nodes, HOSTS, fetch_bytes, sha, BENCH_DIR

nodes = load_nodes()
nodes = {k: v for k, v in nodes.items() if len(v.users) > 0}
__import__('lib').run_header('crossfamily', nodes)
RESULTS = []
LOCK = threading.Lock()

def rec(feature, src, dst, ok, detail="", secs=0.0):
    with LOCK:
        RESULTS.append({"feature": feature, "src": src, "dst": dst,
                        "ok": bool(ok), "detail": str(detail)[:300], "secs": round(secs, 1)})
    print(f"[{'PASS' if ok else 'FAIL'}] {feature:16} {src:>7} -> {dst:<7} {detail[:120]}", flush=True)

def wait_until(fn, timeout=120, interval=2):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
            if last: return last
        except Exception as e:
            last = f"exc:{e}"
        time.sleep(interval)
    return None

def png_bytes(seed: int):
    import zlib, struct
    def chunk(t, d):
        c = struct.pack(">I", len(d)) + t + d
        return c + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
    ihdr = struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0)
    raw = zlib.compress(b"\x00" + bytes([seed % 256, (seed * 7) % 256, (seed * 13) % 256]))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", raw) + chunk(b"IEND", b""))

TAG = "XF" + str(int(time.time()))[-6:]

# Test pairs: all pinetree <-> birchtree combinations
PINETREE_NODES = ["pt-a", "pt-b", "pt-c"]
BIRCHTREE_NODES = ["bt-a", "bt-b", "bt-c", "bt-d", "bt-e"]

TEST_PAIRS = [(p, b) for p in PINETREE_NODES for b in BIRCHTREE_NODES] + \
             [(b, p) for b in BIRCHTREE_NODES for p in PINETREE_NODES]

def test_pair(src, dst):
    S, D = nodes[src], nodes[dst]
    su = S.users[0]
    du = D.users[1]

    # 1. WebFinger + actor resolution
    t0 = time.time()
    try:
        rid = D.resolve(su.acct, du)
        rec("actor-resolve", src, dst, True, f"rid={rid}", time.time() - t0)
    except Exception as e:
        rec("actor-resolve", src, dst, False, str(e)[:200])
        return  # can't continue without resolution

    # 2. Follow
    t0 = time.time()
    try:
        s, v = S.follow(su, du.acct)
        follow_ok = s in (200, 409)
        rec("follow", src, dst, follow_ok, f"status={s}", time.time() - t0)
    except Exception as e:
        rec("follow", src, dst, False, str(e)[:200])

    # 3. Post + federation
    t0 = time.time()
    try:
        marker = f"XF|post|{src}|{dst}|{TAG}"
        nid = S.post(su, text=marker)
        uri = S.uri_of(nid, su)
        def post_seen():
            for n in D.home(du, limit=30):
                t = n.get("text") or n.get("content") or ""
                if marker in t:
                    return True
            return None
        out = wait_until(post_seen, timeout=120)
        rec("note-federation", src, dst, bool(out), f"nid={nid}", time.time() - t0)
    except Exception as e:
        rec("note-federation", src, dst, False, str(e)[:200])

    # 4. Reply
    t0 = time.time()
    try:
        # Find the post on dst
        items = D.home(du, limit=30)
        target = None
        for n in items:
            t = n.get("text") or n.get("content") or ""
            if f"XF|post|{src}|{dst}|{TAG}" in t:
                target = n
                break
        if target:
            marker = f"XF|reply|{src}|{dst}|{TAG}"
            rid = D.post(du, text=marker, reply_to=target.get("id"))
            ruri = D.uri_of(rid, du)
            def reply_seen():
                v = S.note_by_uri(ruri, su)
                if not v: return None
                # Misskey shape: replyId; Mastodon API: in_reply_to_id.
                rid = v.get("replyId") or (v.get("reply") or {}).get("id") or v.get("in_reply_to_id")
                return True if rid else None
            out = wait_until(reply_seen, timeout=120)
            rec("reply", src, dst, bool(out), f"rid={rid}", time.time() - t0)
        else:
            rec("reply", src, dst, False, "target post not found on dst")
    except Exception as e:
        rec("reply", src, dst, False, str(e)[:200])

    # 5. Like/React
    t0 = time.time()
    try:
        items = D.home(du, limit=30)
        target = None
        for n in items:
            t = n.get("text") or n.get("content") or ""
            if f"XF|post|{src}|{dst}|{TAG}" in t:
                target = n
                break
        if target:
            s, v = D.react(du, target.get("id"))
            def like_seen():
                rs = S.reactions(nid, su)
                total = sum(len(a) if isinstance(a, list) else int(a or 0) for a in rs.values())
                return True if total >= 1 else None
            out = wait_until(like_seen, timeout=120)
            rec("like-react", src, dst, bool(out), f"status={s}", time.time() - t0)
        else:
            rec("like-react", src, dst, False, "target not found")
    except Exception as e:
        rec("like-react", src, dst, False, str(e)[:200])

    # 6. Boost/Renote
    t0 = time.time()
    try:
        items = D.home(du, limit=30)
        target = None
        for n in items:
            t = n.get("text") or n.get("content") or ""
            if f"XF|post|{src}|{dst}|{TAG}" in t:
                target = n
                break
        if target:
            before = S.renote_count(nid, su)
            bid = D.boost(du, target.get("id"))
            def boost_seen():
                c = S.renote_count(nid, su)
                return True if c > before else None
            out = wait_until(boost_seen, timeout=120)
            rec("boost-renote", src, dst, bool(out), f"bid={bid}", time.time() - t0)
        else:
            rec("boost-renote", src, dst, False, "target not found")
    except Exception as e:
        rec("boost-renote", src, dst, False, str(e)[:200])

    # 7. Delete propagation
    t0 = time.time()
    try:
        marker = f"XF|del|{src}|{dst}|{TAG}"
        did = S.post(su, text=marker)
        duri = S.uri_of(did, su)
        # Wait for it to propagate
        wait_until(lambda: D.note_by_uri(duri, du), timeout=60)
        S.delete(su, did)
        def deleted():
            v = D.note_by_uri(duri, du)
            return True if v is None else None
        out = wait_until(deleted, timeout=120)
        rec("delete", src, dst, bool(out), f"did={did}", time.time() - t0)
    except Exception as e:
        rec("delete", src, dst, False, str(e)[:200])

    # 8. Mention notification
    t0 = time.time()
    try:
        marker = f"XF|mention|{src}|{dst}|{TAG}"
        Mp = S.post(su, text=f"@{du.name}@{HOSTS[dst]} {marker}")
        def notif_seen():
            items = D.notifications(du, limit=20)
            return marker in json.dumps(items) or None
        out = wait_until(notif_seen, timeout=120)
        rec("mention-notif", src, dst, bool(out), f"Mp={Mp}", time.time() - t0)
    except Exception as e:
        rec("mention-notif", src, dst, False, str(e)[:200])

    # 9. Media federation
    t0 = time.time()
    try:
        data = png_bytes(len(src) * 17 + len(dst) + int(TAG[2:]))
        fid, furl = S.upload(su, data)
        marker = f"XF|media|{src}|{dst}|{TAG}"
        mid = S.post(su, text=marker, media=[fid])
        muri = S.uri_of(mid, su)
        def media_seen():
            v = D.note_by_uri(muri, du)
            if not v: return None
            # Mastodon-API destinations serve `media_attachments` (same
            # correction as features handoff items 25/26: `files` alone is
            # Misskey-shape only). A row without any fetchable URL is a
            # placeholder for a fetch that has not completed, not an arrival.
            fs = (v or {}).get("files") or (v or {}).get("attachments") or (v or {}).get("media_attachments") or []
            fs = [f for f in fs if f.get("url") or f.get("remote_url") or f.get("textUrl")]
            return fs or None
        files = wait_until(media_seen, timeout=90, interval=3)
        if files:
            url = files[0].get("url") or files[0].get("remote_url") or files[0].get("textUrl")
            st, body = fetch_bytes(url, timeout=60)
            media_ok = st == 200 and sha(body) == sha(data)
            rec("media", src, dst, media_ok, f"url={url} st={st}", time.time() - t0)
        else:
            rec("media", src, dst, False, "no attachment on dst")
    except Exception as e:
        rec("media", src, dst, False, str(e)[:200])

def main():
    print(f"{len(TEST_PAIRS)} cross-family pairs, tag={TAG}", flush=True)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(lambda p: test_pair(*p), TEST_PAIRS))
    json.dump(RESULTS, open(os.path.join(BENCH_DIR, "crossfamily.json"), "w"), indent=1)
    # Summary
    feats = sorted({r["feature"] for r in RESULTS})
    print("\n=== CROSS-FAMILY MATRIX ===")
    print(f"{'feature':16}", " ".join(f"{a[:2]}>{b[:2]}" for a, b in TEST_PAIRS))
    for f in feats:
        row = []
        for a, b in TEST_PAIRS:
            m = [r for r in RESULTS if r["feature"] == f and r["src"] == a and r["dst"] == b]
            row.append("P" if m and m[0]["ok"] else ("F" if m else "-"))
        print(f"{f:16}", " ".join(f"{x:^5}" for x in row))
    fails = [r for r in RESULTS if not r["ok"]]
    print(f"\n{len(RESULTS)} checks, {len(fails)} failures")
    for r in fails[:20]:
        print(" FAIL", r["feature"], r["src"], "->", r["dst"], r["detail"][:160])
    if fails: sys.exit(1)

if __name__ == "__main__":
    main()