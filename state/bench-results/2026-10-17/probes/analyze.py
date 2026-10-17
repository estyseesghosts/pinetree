"""Summarise a bench rerun from /tmp/opencode/bench/{features,probes,stress}.json.

Usage: python3 -I analyze.py [bench_dir]   (default /tmp/opencode/bench)
Prints plain text; the report log quotes this output verbatim.
Akkoma exclusion: rig Akkoma has no search backend, so Akkoma-authored voters/reactors and
Akkoma-as-destination lookups are reported separately and excluded from the pass verdicts.
"""
import json, sys, os, collections

B = sys.argv[1] if len(sys.argv) > 1 else "/tmp/opencode/bench"
AK = "akkoma"

def load(name):
    p = os.path.join(B, name)
    return json.load(open(p)) if os.path.exists(p) else None

print("== FEATURE MATRIX (features.json)")
feat = load("features.json") or []
by = collections.OrderedDict()
for r in feat:
    by.setdefault(r["feature"], []).append(r)
tot_ok = tot = 0
ex_ok = ex_n = 0           # excluding any pair that touches akkoma
for f, rows in by.items():
    ok = sum(1 for r in rows if r["ok"]); tot_ok += ok; tot += len(rows)
    nak = [r for r in rows if AK not in (r["src"], r["dst"])]
    nok = sum(1 for r in nak if r["ok"]); ex_ok += nok; ex_n += len(nak)
    print(f"  {f:16s} all {ok:2d}/{len(rows):2d}   excl-akkoma {nok:2d}/{len(nak):2d}")
print(f"  TOTAL all {tot_ok}/{tot}   excl-akkoma {ex_ok}/{ex_n}")
fails = [r for r in feat if not r["ok"]]
print(f"  failing pairs: {len(fails)}")
for r in fails[:60]:
    print(f"    FAIL {r['feature']:14s} {r['src']:>8s} -> {r['dst']:<8s} {str(r.get('detail'))[:110]}")

print("\n== PROBE MATRIX (probes.json)")
pr = load("probes.json")
if pr:
    pf = pr.get("probe_fails")
    print(f"  probe_fails={pf if isinstance(pf, int) else len(pf)}")
    if isinstance(pf, list):
        dsts = collections.Counter(x.get("dst") if isinstance(x, dict) else str(x)[:40] for x in pf)
        print(f"  fails by dst: {dict(dsts)}")

st = load("stress.json")
if not st:
    print("\nno stress.json"); sys.exit(0)
P = st["phases"]
print(f"\n== STRESS (stress.json) tag={st.get('tag')} wall_total_s={st.get('wall_total_s')}")

s1 = P.get("S1_post_storm", {})
print("== S1 post storm")
print(f"  created={s1.get('posts')} create_failures={s1.get('create_failures')} wall_s={s1.get('wall_s')} "
      f"p50/p95/p99 ms={s1.get('create_p50_ms')}/{s1.get('create_p95_ms')}/{s1.get('create_p99_ms')}")
print(f"  fed latency p50/p95/max s = {s1.get('fed_latency_p50_s')}/{s1.get('fed_latency_p95_s')}/{s1.get('fed_latency_max_s')}")
for w, snap in (s1.get("sweeps") or {}).items():
    print(f"  +{w}s:")
    for dst, d in snap.items():
        print(f"    {dst:8s} markers={d['total_markers']:4d}/720  dupes_legacy={d['dupes']:3d}  "
              f"dupes_post_restart={d.get('dupes_post_restart','n/a')}  dupes_this_run={d.get('dupes_this_run','n/a')}  items={d['items']}")

s2 = P.get("S2_reaction_storm", {})
print("== S2 reactions")
print("  " + json.dumps({k: v for k, v in s2.items() if k != "shortfall"})[:600])
print(f"  shortfall_n={s2.get('shortfall_n')} {json.dumps(s2.get('shortfall'))[:300]}")

s3 = P.get("S3_poll_storm", {})
print("== S3 polls")
print(f"  polls={s3.get('polls')} attempted={s3.get('votes_attempted')} ok(200|204)={s3.get('votes_ok')} shortfall_n={s3.get('shortfall_n')}")
for src, d in (s3.get("by_origin") or {}).items():
    verdict = "EXACT" if d["tallied"] == d["ok"] else f"short by {d['ok'] - d['tallied']}"
    print(f"    origin {src:8s} attempts(non-akkoma voters)={d['attempts_non_akkoma']:3d} ok={d['ok']:3d} tallied={d['tallied']:3d}  {verdict}")

s4 = P.get("S4_media_burst", {})
print("== S4 media")
print(f"  uploads={s4.get('uploads')} verified_ok={s4.get('verified_ok')} bad={s4.get('bad')}")
print(f"  by_dst={s4.get('by_dst')}")
print(f"  attach_wait_s={s4.get('attach_wait_s')}")

s5 = P.get("S5_reply_storm", {})
print("== S5 replies")
print(f"  replies={s5.get('replies')} sampled={s5.get('sampled')} ok={s5.get('ok')} bad={s5.get('bad')}")

s6 = P.get("S6_churn", {})
print("== S6 churn")
print(f"  deleted={s6.get('deleted')} residue={s6.get('residue')} reaction_rtt_s={s6.get('reaction_rtt_s')}")

print("== S7 big text")
print(f"  {P.get('S7_big_text')}")
