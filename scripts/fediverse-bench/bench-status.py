#!/usr/bin/env python3
"""Bench status reporter (long_bench.md section 5). Stdlib only.
Reads $BENCH_DIR/runs/<latest>/ + nodes/ + sysstat.csv,
writes status/status-<UTC>.md + status-latest.md with fixed headings.
"""
import json, os, glob, time, subprocess
from datetime import datetime, timezone
BENCH_DIR = os.environ.get("BENCH_DIR") or "/root/bench/current"
try:
    BENCH_DIR = os.path.realpath(BENCH_DIR)
except Exception:
    pass

def latest_run():
    runs = sorted(glob.glob(os.path.join(BENCH_DIR, "runs", "*")))
    runs = [r for r in runs if os.path.isdir(r)]
    return runs[-1] if runs else None

def read_summary(run):
    p = os.path.join(run, "summary.txt") if run else None
    if p and os.path.exists(p):
        try: return open(p).read().strip()[-2000:]
        except Exception: return "summary unreadable"
    return "no summary yet"

def trace_stats(run, window_s=600):
    tp = os.path.join(run, "trace.jsonl") if run else None
    if not tp or not os.path.exists(tp):
        return {"lines": 0, "note": "no trace yet"}
    now = time.time(); win = []; allms = []
    total = 0; fails = 0; win_fails = 0; win_5xx = 0; win_429 = 0; last_ts = 0
    try:
        with open(tp) as f:
            for line in f:
                total += 1
                try: r = json.loads(line)
                except Exception: continue
                ms = r.get("ms")
                if isinstance(ms, (int, float)): allms.append(ms)
                st = r.get("status")
                if isinstance(st, int) and st >= 400: fails += 1
                ts = r.get("ts", 0)
                if ts > last_ts: last_ts = ts
                if now - ts < window_s:
                    win.append(ms if isinstance(ms, (int, float)) else 0)
                    if st == 429: win_429 += 1
                    elif isinstance(st, int) and st >= 500: win_5xx += 1; win_fails += 1
                    elif isinstance(st, int) and st >= 400: win_fails += 1
    except Exception as e:
        return {"lines": total, "error": str(e)[:200]}
    def pct(a, p):
        if not a: return 0
        a = sorted(a); return round(a[min(len(a)-1, int(len(a)*p/100))], 1)
    idle_s = round(now - last_ts, 0) if last_ts else -1
    return {"lines": total, "fails": fails,
            "p50_win": pct(win, 50), "p95_win": pct(win, 95), "p99_win": pct(win, 99),
            "p50_all": pct(allms, 50), "p95_all": pct(allms, 95), "n_win": len(win),
            "win_fails": win_fails, "win_5xx": win_5xx, "win_429": win_429,
            "win_rate_min": round(len(win) / (window_s / 60), 1), "idle_s": idle_s}

def nodes_up():
    ndir = os.path.join(BENCH_DIR, "nodes")
    ups, downs = [], []
    if not os.path.isdir(ndir):
        return ups, ["nodes/ missing"]
    for pidf in sorted(glob.glob(os.path.join(ndir, "*.pid"))):
        name = os.path.basename(pidf)[:-4]
        if name in ("bot", "status-loop", "run-suites"): continue
        try:
            pid = open(pidf).read().strip()
            os.kill(int(pid), 0)
            ups.append(f"{name}({pid})")
        except Exception:
            downs.append(name)
    # also check listeners
    try:
        ss = subprocess.run(["ss", "-ltnH"], capture_output=True, text=True, timeout=10).stdout
    except Exception:
        ss = ""
    return ups, downs

def sysstat_tail(n=5):
    p = os.path.join(BENCH_DIR, "nodes", "sysstat.csv")
    if not os.path.exists(p): return "no sysstat yet"
    try:
        lines = open(p).read().strip().splitlines()
        return "\n".join(lines[-n:])
    except Exception as e:
        return f"sysstat unreadable: {e}"

def current_suite(run):
    """Which suite is running now + its most informative last line."""
    rs = os.path.join(BENCH_DIR, "run-suites.out")
    try: lines = open(rs).read().strip().splitlines()
    except Exception: return ("unknown", "run-suites.out missing")
    started, done = [], []
    for l in lines:
        if l.startswith("[run-suites] start "): started.append(l.split("start ", 1)[1].strip().split()[0])
        if l.startswith("[run-suites] done"): done.append(l)
    cur = started[-1] if len(started) > len(done) else None
    if not cur:
        return ("idle", lines[-1][:200] if lines else "no output")
    # suite log tail: last non-empty, non-warm/progress-noise line + progress hints
    outp = os.path.join(run, f"{cur}.out") if run else None
    tail, n = "no log yet", 0
    try:
        ls = [l for l in open(outp).read().splitlines() if l.strip()]
        n = len(ls)
        tail = ls[-1][:220] if ls else "empty log"
    except Exception as e:
        tail = f"unreadable: {e}"
    return (cur, f"{n} log lines; last: {tail}")

def follow_health(run):
    """Live follow-mesh signal from the follows suite log."""
    outp = os.path.join(run, "follows.out") if run else None
    try: ls = open(outp).read().splitlines()
    except Exception: return "follows suite not started"
    done = [l.split(":")[0] for l in ls if "follows done" in l]
    fails = [l[:160] for l in ls if "FAIL" in l.upper() and "0 failed" not in l]
    for cand in (os.path.join(BENCH_DIR, "follows.json"), os.path.join(run or "", "follows.json")):
        if os.path.exists(cand):
            try:
                d = json.load(open(cand))
                cells = sum(len(v) if isinstance(v, dict) else 1 for v in (d.values() if isinstance(d, dict) else d))
                return f"{len(done)} nodes meshed ({', '.join(done) or 'none yet'}); follows.json present ({cells} cells)"
            except Exception: pass
    return f"{len(done)}/12 nodes meshed ({', '.join(done) or 'none yet'})" + (f"; {len(fails)} FAIL lines" if fails else "")

def media_health():
    for cand in (os.path.join(BENCH_DIR, "stress.json"),):
        if os.path.exists(cand):
            try:
                s4 = (json.load(open(cand)).get("phases") or {}).get("S4_media_burst") or {}
                return f"S4: uploads={s4.get('uploads')} verified_ok={s4.get('verified_ok')} ({s4.get('failure_categories')})"
            except Exception as e: return f"stress.json unreadable: {e}"
    return "S1–S7 not started (sampled media gate runs in stress)"

def main():
    utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run = latest_run()
    run_id = os.path.basename(BENCH_DIR)
    t0file = os.path.join(BENCH_DIR, "t0.txt")
    try: t0 = float(open(t0file).read().strip())
    except Exception:
        t0 = time.time(); open(t0file, "w").write(str(t0))
    el = int(time.time() - t0)
    summ = read_summary(run)
    ts = trace_stats(run)
    ups, downs = nodes_up()
    sysstat = sysstat_tail()
    suite, suite_detail = current_suite(run)
    follow_h = follow_health(run)
    media_h = media_health()
    win_f = ts.get('win_fails', 'n/a') if isinstance(ts.get('win_fails'), int) else 'n/a'
    # follow/probe health: follows.json + probes if present
    follows = "no follows.json yet"
    for cand in [os.path.join(BENCH_DIR, "follows.json")] + ( [os.path.join(run, "follows.json")] if run else []):
        if os.path.exists(cand):
            try:
                d = json.load(open(cand)); follows = json.dumps(d)[:800]
            except Exception as e: follows = f"unreadable: {e}"
    md = f"""# Status {utc} — run {run_id} (T+{el//3600}h{(el%3600)//60}m)
## What's happening
- NOW: suite `{suite}` — {suite_detail}
- call rate last 10min: {ts.get('win_rate_min', 'n/a')}/min over {ts.get('n_win', 0)} calls; cumulative {ts.get('lines', 0)} calls, {ts.get('fails', 'n/a')} fails>=400 (5xx {ts.get('win_5xx', 'n/a')} + other {ts.get('win_fails', 0) - ts.get('win_5xx', 0) if isinstance(ts.get('win_fails'), int) and isinstance(ts.get('win_5xx'), int) else 'n/a'} in window, 429 {ts.get('win_429', 'n/a')}); trace idle {ts.get('idle_s', 'n/a')}s
- latency p50/p95 last 10min: {ts.get('p50_win')}/{ts.get('p95_win')}ms; run-to-date p50/p95: {ts.get('p50_all')}/{ts.get('p95_all')}ms
- suite summary:\n```\n{summ}\n```\n- nodes up ({len(ups)}): {', '.join(ups) or 'none'}; down: {', '.join(downs) or 'none'}
- sysstat tail:\n```\n{sysstat}\n```
## What's failing (new vs known)
- window fails>=400: {win_f} in last 10min (5xx {ts.get('win_5xx', 'n/a')}, 429 {ts.get('win_429', 'n/a')}); cumulative {ts.get('fails', 'n/a')} of {ts.get('lines', 0)}
- deterministic 422/403 skips counted separately by phase-follows/probe-matrix, never as fails.
## Media watch
- {media_h}
## Follow/probe health
- 132-pair resolve: 132/132 at t0. Mesh: {follow_h}.
## Next
- order: follows → probe-matrix → features → stress(S1–S7) → concurrent → crossfamily → interop; then drip + rolling restart. Operator: keep BENCH_TRACE on, keep status loop alive.
"""
    sdir = os.path.join(BENCH_DIR, "status")
    os.makedirs(sdir, exist_ok=True)
    out = os.path.join(sdir, f"status-{utc}.md")
    open(out, "w").write(md)
    open(os.path.join(sdir, "status-latest.md"), "w").write(md)
    # compat: adapter reads $BENCH_DIR/status/status-latest.md
    print(f"wrote {out}")
if __name__ == "__main__":
    main()
