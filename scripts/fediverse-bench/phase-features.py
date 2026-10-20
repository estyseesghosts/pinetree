#!/usr/bin/env python3
"""Phase B: feature matrix. Every feature x every ordered node pair (20 pairs).

Features: post, cw, reply, reaction(+undo), renote/boost(+undo), quote,
poll(+remote vote), media federation, followers-only, delete, mention.
Records per-check results into $BENCH_DIR/features.json + prints a matrix.
"""
import json, sys, time, threading
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import load_nodes, HOSTS, fetch_bytes, sha, BENCH_DIR

nodes = load_nodes()
nodes = {k: v for k, v in nodes.items() if len(v.users) > 0}
__import__('lib').run_header('features', nodes)
PAIRS = [(a, b) for a in nodes for b in nodes if a != b]
RESULTS = []            # {feature, src, dst, ok, detail, secs}
LOCK = threading.Lock()
MEDIA_JOBS = []         # stashed media uploads, verified after all pairs (S4 pattern)
# Honest window for the deferred media check (same bar as before: attachment
# present + sha match against the source-side original). Env-overridable like
# S4's S4_SETTLE_S/S4_DEADLINE_S; the values are printed in every detail.
MEDIA_SETTLE_S = int(os.environ.get("MEDIA_SETTLE_S", 30))
MEDIA_DEADLINE_S = int(os.environ.get("MEDIA_DEADLINE_S", 240))

def rec(feature, src, dst, ok, detail="", secs=0.0):
    with LOCK:
        RESULTS.append({"feature": feature, "src": src, "dst": dst,
                        "ok": bool(ok), "detail": str(detail)[:300], "secs": round(secs, 1)})
    print(f"[{'PASS' if ok else 'FAIL'}] {feature:12} {src:>7} -> {dst:<7} {detail[:120]}", flush=True)

def wait_until(fn, timeout=120, interval=2, desc=""):
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

def remote_note(node, user, uri):
    """Resolve uri to a local note id on node (with retries)."""
    def go():
        v = node.note_by_uri(uri, user)
        return (v or {}).get("id") if v else None
    return wait_until(go, timeout=90, interval=3)

def png_bytes(seed: int):
    """Tiny valid PNGs, distinct bytes per seed (varying pixel content)."""
    import zlib, struct
    def chunk(t, d):
        c = struct.pack(">I", len(d)) + t + d
        return c + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
    ihdr = struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0)
    raw = zlib.compress(b"\x00" + bytes([seed % 256, (seed * 7) % 256, (seed * 13) % 256]))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", raw) + chunk(b"IEND", b""))

TAG = str(int(time.time()))[-6:]

def pair_worker(pi, src, dst):
    S, D = nodes[src], nodes[dst]
    su = S.users[pi % len(S.users)]
    du = D.users[(pi + 2) % len(D.users)]
    t_pair = time.time()
    try:
        # 1. plain post
        t0 = time.time()
        A = S.post(su, text=f"FTR|post|{src}|{dst}|{TAG}")
        A_uri = S.uri_of(A, su)
        got = wait_until(lambda: any(FTR_marker(n, f"FTR|post|{src}|{dst}|{TAG}")
                                     for n in D.home(D.users[(pi+1) % len(D.users)], limit=30)),
                         timeout=120)
        rec("post", src, dst, bool(got), f"A={A} uri={A_uri}", time.time() - t0)
        if not got: return

        # 2. cw post
        t0 = time.time()
        marker = f"FTR|cw|{src}|{dst}|{TAG}"
        C = S.post(su, text="hidden body", cw=marker)
        C_uri = S.uri_of(C, su)
        def cw_seen():
            for n in D.home(D.users[(pi+1) % len(D.users)], limit=30):
                if "hidden body" in (text_of(D, n) or "") and marker in (cw_of(D, n) or ""):
                    return True
            return None
        got = wait_until(cw_seen, timeout=120)
        rec("cw", src, dst, bool(got), f"C={C}", time.time() - t0)

        # 3. reply
        t0 = time.time()
        marker = f"FTR|reply|{src}|{dst}|{TAG}"
        A_local_on_d = remote_note(D, du, A_uri)
        reply_ok = reply_detail = None
        if A_local_on_d:
            R = D.post(du, text=marker, reply_to=A_local_on_d)
            R_uri = D.uri_of(R, du)
            def reply_seen():
                v = S.note_by_uri(R_uri, su)
                if not v: return None
                # Misskey family: replyId; Mastodon API: in_reply_to_id
                rid = v.get("replyId") or (v.get("reply") or {}).get("id") or v.get("in_reply_to_id")
                seen["rid"] = rid
                return rid == A or None   # only a settled state is truthy (see unreact_seen)
            seen = {}
            out = wait_until(reply_seen, timeout=120)
            reply_ok = out is True
            reply_detail = f"R={R} out={out if reply_ok else 'rid=%s,A=%s' % (seen.get('rid'), A)}"
        else:
            reply_detail = "could not resolve A on dst"
        rec("reply", src, dst, reply_ok, reply_detail, time.time() - t0)

        # 4. reaction (dst -> A) + undo
        t0 = time.time()
        A_local_on_d = A_local_on_d or remote_note(D, du, A_uri)
        react_ok = undo_ok = None
        if A_local_on_d:
            s, v = D.react(du, A_local_on_d)
            per_user = None
            def react_seen():
                nonlocal per_user
                rs = S.reactions(A, su)
                # count-based first (works with pinetree's count-only fallback)
                total = sum(len(a) if isinstance(a, list) else int(a or 0) for a in rs.values())
                if total >= 1:
                    # prefer confirming the specific reactor when identities exist
                    for k, accts in rs.items():
                        if isinstance(accts, list) and du.acct in accts:
                            per_user = f"{k}:{accts}"
                            return True
                    return True if per_user is None or total >= 1 else None
                return None
            out = wait_until(react_seen, timeout=120)
            react_ok = bool(out); t_react = time.time() - t0
            rec("reaction", src, dst, react_ok, f"A={A} out={out} per_user={per_user}", t_react)
            # undo
            t0 = time.time()
            D.unreac(du, A_local_on_d)
            def unreact_seen():
                rs = S.reactions(A, su)
                total = sum(len(a) if isinstance(a, list) else int(a or 0) for a in rs.values())
                # Only a settled state is truthy: wait_until stops on the first
                # truthy value, so returning "remaining=..." here made the check
                # fail instantly (0.0 s) before the Undo could be delivered.
                return True if total == 0 else None
            out = wait_until(unreact_seen, timeout=120)
            undo_ok = out is True
            rec("unreaction", src, dst, undo_ok, f"out={out}", time.time() - t0)
        else:
            rec("reaction", src, dst, False, "A unresolvable on dst")

        # 5. boost + undo
        t0 = time.time()
        before = S.renote_count(A, su)
        boost_ok = undo_ok = None
        if A_local_on_d:
            B = D.boost(du, A_local_on_d)
            out = wait_until(lambda: (S.renote_count(A, su) or 0) > before or None, timeout=120)
            boost_ok = out is True
            rec("renote/boost", src, dst, boost_ok,
                f"B={B} out={out or 'count=%s,before=%s' % (S.renote_count(A, su), before)}", time.time() - t0)
            t0 = time.time()
            # undo via delete of the boost
            D.delete(du, B)
            out = wait_until(lambda: (S.renote_count(A, su) or 0) <= before or None, timeout=120)
            undo_ok = out is True
            rec("unboost", src, dst, undo_ok,
                f"out={out or 'count=%s,before=%s' % (S.renote_count(A, su), before)}", time.time() - t0)
        else:
            rec("renote/boost", src, dst, False, "A unresolvable on dst")

        # 6. quote
        t0 = time.time()
        marker = f"FTR|quote|{src}|{dst}|{TAG}"
        quote_ok = quote_detail = None
        A_local_on_d = A_local_on_d or remote_note(D, du, A_uri)
        if A_local_on_d:
            if D.family:
                Q = D.post(du, text=marker, quote_of=A_local_on_d)
                Q_uri = D.uri_of(Q, du)
                def quote_seen():
                    v = D.note(Q, du)
                    if v and (v.get("renoteId") == A_local_on_d or
                              (v.get("renote") or {}).get("id") == A_local_on_d):
                        return True
                    return None
                out = wait_until(quote_seen, timeout=60)
                quote_ok = bool(out)
                quote_detail = f"Q={Q} local_renote_ok={quote_ok}"
            else:
                Q = D.post(du, text=f"{marker} {A_uri}")
                Q_uri = D.uri_of(Q, du)
                # verify the link-quote arrives at src with the URI preserved
                def lq_seen():
                    for n in S.home(su, limit=30):
                        if marker in (text_of(S, n) or "") and A_uri in (text_of(S, n) or ""):
                            return True
                    return None
                out = wait_until(lq_seen, timeout=120)
                quote_ok = bool(out); quote_detail = f"Q={Q} link-quote arrived at src={bool(out)}"
            # verify origin sees the quote relation (misskey-family src)
            if quote_ok and S.family and Q_uri:
                def origin_quote():
                    v = S.note_by_uri(Q_uri, su)
                    if not v: return None
                    return v.get("renoteId") == A or (v.get("renote") or {}).get("id") == A
                oq = wait_until(origin_quote, timeout=90)
                quote_detail += f" origin_sees_relation={bool(oq)}"
        else:
            quote_detail = "A unresolvable on dst"
        rec("quote", src, dst, quote_ok, quote_detail, time.time() - t0)

        # 7. poll + remote vote
        t0 = time.time()
        marker = f"FTR|poll|{src}|{dst}|{TAG}"
        P = S.post(su, text=marker, poll={"choices": ["alpha", "beta", "gamma"], "expires_in": 3600})
        P_uri = S.uri_of(P, su)
        P_local_on_d = remote_note(D, du, P_uri)
        poll_ok = None
        if P_local_on_d:
            D.poll_vote(du, P_local_on_d, 1)
            def vote_seen():
                p = S.poll(P, su)
                if p and len(p["choices"]) >= 2 and p["choices"][1]["votes"] >= 1:
                    return p
                return None
            out = wait_until(vote_seen, timeout=150)
            poll_ok = bool(out)
            rec("poll+vote", src, dst, poll_ok, f"P={P} choices={out}", time.time() - t0)
        else:
            rec("poll+vote", src, dst, False, "poll unresolvable on dst", time.time() - t0)

        # 8. media federation: upload+post inline, verified later by
        # media_verify_all() (global settle + poll-to-deadline, the S4
        # pattern: remote media arrives via an async fetch job, so verifying
        # immediately measures the fetch queue, not federation).
        t0 = time.time()
        data = png_bytes(pi * 31 + len(src) + len(dst))
        fid, furl = S.upload(su, data)
        M = S.post(su, text=f"FTR|media|{src}|{dst}|{TAG}", media=[fid])
        M_uri = S.uri_of(M, su)
        with LOCK:
            MEDIA_JOBS.append({"src": src, "dst": dst, "uri": M_uri,
                               "want": sha(data), "t0": t0})

        # 9. followers-only visibility
        t0 = time.time()
        marker = f"FTR|fo|{src}|{dst}|{TAG}"
        F = S.post(su, text=marker, visibility="followers")
        F_uri = S.uri_of(F, su)
        fo_home = wait_until(lambda: any(marker in (text_of(D, n) or "")
                                         for n in D.home(du, limit=30)) or None, timeout=120)
        # absent from a third node's public/global timeline
        third = [n for n in nodes if n not in (src, dst)][0]
        TN = nodes[third]
        time.sleep(10)
        if TN.family:
            s, gl = TN.api("notes/global-timeline", {"limit": 20}, TN.users[0])
        else:
            s, gl = TN.mapi("GET", "/timelines/public?limit=20", user=TN.users[0])
        leaked = any(marker in json.dumps(gl or {}) for _ in [0])
        rec("followers-only", src, dst, bool(fo_home) and not leaked,
            f"on_follower={bool(fo_home)} leaked_to_global={leaked}", time.time() - t0)

        # 10. delete propagation
        t0 = time.time()
        marker = f"FTR|del|{src}|{dst}|{TAG}"
        Dl = S.post(su, text=marker)
        Dl_uri = S.uri_of(Dl, su)
        S.delete(su, Dl)
        def deleted():
            v = D.note_by_uri(Dl_uri, du)
            if v is None: return True
            # some servers keep a tombstoned copy
            return None
        out = wait_until(deleted, timeout=150)
        rec("delete", src, dst, bool(out), f"Dl={Dl}", time.time() - t0)

        # 11. mention notification
        t0 = time.time()
        marker = f"FTR|mention|{src}|{dst}|{TAG}"
        Mp = S.post(su, text=f"@{du.name}@{HOSTS[dst]} {marker}")
        def notif_seen():
            items = D.notifications(du, limit=20)
            return marker in json.dumps(items) or None
        out = wait_until(notif_seen, timeout=120)
        rec("mention-notif", src, dst, bool(out), f"Mp={Mp}", time.time() - t0)
    except Exception as e:
        import traceback
        rec("pair-crash", src, dst, False, f"{e}", time.time() - t_pair)
        traceback.print_exc()

def text_of(node, n):
    if node.family: return n.get("text")
    return n.get("content")  # akkoma: raw html; markers still findable

def cw_of(node, n):
    if node.family: return n.get("cw")
    return n.get("spoiler_text")

def FTR_marker(n, marker):
    node_is_family = True  # helper used with both; check both fields
    t = n.get("text") or n.get("content") or ""
    return marker in t

def follows_warning():
    """Feature posts only reach a node whose users follow the author, so a run before the follow
    graph converges reports setup, not federation. Warn loudly rather than refuse to run."""
    try:
        f = json.load(open(os.path.join(BENCH_DIR, "follows.json")))
    except Exception:
        return "no follows.json: run phase-follows first"
    n = sum(len(v) for v in (f.get("missing") or {}).values())
    return f"follows.json lists {n} missing edges" if n else None

def media_verify_all():
    """Verify every stashed media upload after a global settle, polling each
    pair to a deadline (the S4 pattern). A copy that shows up late is lag,
    not loss; the bar is unchanged (attachment present + sha match)."""
    print(f"media: {len(MEDIA_JOBS)} posted; settling {MEDIA_SETTLE_S}s for async media fetch", flush=True)
    time.sleep(MEDIA_SETTLE_S)
    def verify_once(job):
        D = nodes[job["dst"]]
        du = D.users[0]
        local = D.note_by_uri(job["uri"], du)
        if not local:
            return ("unresolvable", None, None)
        lid = local.get("id") if isinstance(local, dict) else local
        v = D.note(lid, du)
        files = (v or {}).get("files") or (v or {}).get("attachments") or (v or {}).get("media_attachments") or []
        if not files:
            return ("no-attachment", None, None)
        url = files[0].get("url") or files[0].get("remote_url") or files[0].get("textUrl")
        if not url:
            return ("no-url", None, None)
        st, body = fetch_bytes(url, timeout=60)
        if st == 200 and sha(body) == job["want"]:
            return ("ok", url, st)
        # mastodon re-encodes served bytes (200 with a different sha): the
        # comparison is against the source-side original, so that case lands
        # here as mismatch:200, not as a fetch failure.
        return (f"mismatch:{st}", url, st)
    def verify(job):
        t1 = time.time()
        while True:
            cat, url, st = verify_once(job)
            if cat == "ok":
                return (job, True, f"url={url} status={st} sha_match=True window=settle{MEDIA_SETTLE_S}+dl{MEDIA_DEADLINE_S}")
            if time.time() - t1 > MEDIA_DEADLINE_S:
                return (job, False, f"{cat} url={url} status={st} window=settle{MEDIA_SETTLE_S}+dl{MEDIA_DEADLINE_S}")
            # A note that never reached the node must not hold a worker for
            # the whole deadline; give it one minute for plain delivery lag.
            if cat == "unresolvable" and time.time() - t1 > 60:
                return (job, False, f"{cat} window=settle{MEDIA_SETTLE_S}+60s")
            time.sleep(5)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=6) as ex:
        for job, ok, detail in ex.map(verify, list(MEDIA_JOBS)):
            rec("media", job["src"], job["dst"], ok, detail, time.time() - job["t0"])

def main():
    w = follows_warning()
    if w: print(f"WARNING: {w}; post/reaction results below may reflect the follow graph, not federation", flush=True)
    print(f"{len(PAIRS)} pairs, tag={TAG}", flush=True)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(lambda p: pair_worker(PAIRS.index(p), *p), PAIRS))
    media_verify_all()
    json.dump(RESULTS, open(os.path.join(BENCH_DIR, "features.json"), "w"), indent=1)
    # matrix summary
    feats = sorted({r["feature"] for r in RESULTS})
    print("\n=== FEATURE MATRIX (rows=feature, cols=src->dst) ===")
    print(f"{'feature':14}", " ".join(f"{a[:2]}>{b[:2]}" for a, b in PAIRS))
    for f in feats:
        row = []
        for a, b in PAIRS:
            m = [r for r in RESULTS if r["feature"] == f and r["src"] == a and r["dst"] == b]
            row.append("P" if m and m[0]["ok"] else ("F" if m else "-"))
        print(f"{f:14}", " ".join(f"{x:^5}" for x in row))
    fails = [r for r in RESULTS if not r["ok"]]
    print(f"\n{len(RESULTS)} checks, {len(fails)} failures")
    for r in fails[:40]:
        print(" FAIL", r["feature"], r["src"], "->", r["dst"], r["detail"][:160])
    if fails: sys.exit(1)

if __name__ == "__main__":
    main()
