#!/usr/bin/env python3
"""summarize a raftload csv per cell: run outcomes, achieved rate, commit latency
percentiles, election/term activity, failed proposals, memory, and whether the
injected delay was reached. means with 95% t-intervals over ok runs; counts over all.

  python3 research/analyze_raftload.py research/results/raftload_stage3_leader_delay.csv"""
import csv, collections, math, statistics as st, sys

T = {1: 0, 2: 12.706, 3: 4.303, 4: 3.182, 5: 2.776, 6: 2.571, 7: 2.447, 8: 2.365, 9: 2.306,
     10: 2.262, 11: 2.228, 12: 2.201}

def ci(v):
    if not v: return (float("nan"), float("nan"))
    m = st.mean(v)
    if len(v) < 2: return (m, float("nan"))
    return (m, T.get(len(v), 2.0) * st.stdev(v) / math.sqrt(len(v)))

def f(r, k):
    try: return float(r[k])
    except (KeyError, ValueError): return float("nan")

rows = list(csv.DictReader(open(sys.argv[1])))
cells = collections.OrderedDict()
for r in rows: cells.setdefault(r["label"], []).append(r)
print(f"{sys.argv[1]}: {len(rows)} runs   (unfin = total writes never committed, abort = ok runs stopped early by the 3000 MB heap guard, heapMB = max peak heap)\n")
print(f"{'cell':20s} {'ok/total':8s} {'rate/s':>8s} {'p50ms':>14s} {'p99ms':>14s} {'elec':>9s} {'termΔ':>6s} {'leadΔ':>6s} {'fail':>6s} {'unfin':>6s} {'abort':>5s} {'heapMB':>7s} {'Lsync_max':>9s} {'ex/weak/ok':>10s}")
for lab, rs in cells.items():
    ok = [r for r in rs if r["status"] == "ok"]
    def col(k): return [f(r, k) for r in ok if f(r, k) == f(r, k)]
    p50, p99 = ci(col("commit_p50_ms")), ci(col("commit_p99_ms"))
    elec = col("elections"); tc = [f(r, "term_end") - f(r, "term_start") for r in ok]
    # injected delay reached: for each role with a delay, injected_ms >= 0.9 * syncs * delay
    reached = "n/a"
    d = rs[0]["delays"]
    if d not in ("-", ""):
        okc = weak = 0
        for r in ok:
            good = wk = True
            for part in d.split(","):
                role, dur = part.split("="); ms = float(dur.rstrip("ms"))
                syncs, inj = f(r, role + "_syncs"), f(r, role + "_injected_ms")
                if not (syncs > 0 and inj >= 0.9 * ms * syncs - 1 and inj <= ms * syncs + 1): good = False
                # weak: some delay was injected and a sync at least as long as the delay was seen
                if not (inj > 0 and f(r, role + "_sync_max_ms") >= 0.9 * ms): wk = False
            okc += good; weak += wk
        reached = f"{okc}/{weak}/{len(ok)}"
    print(f"{lab:20s} {len(ok):>3d}/{len(rs):<4d} {st.mean(col('achieved_per_s')) if ok else float('nan'):8.0f} "
          f"{p50[0]:7.1f}±{p50[1]:5.1f} {p99[0]:7.1f}±{p99[1]:5.1f} "
          f"{(st.mean(elec) if elec else float('nan')):5.1f}({int(sum(elec)) if elec else 0:>3d}) "
          f"{(st.mean(tc) if tc else float('nan')):6.1f} {(st.mean(col('leader_changes')) if ok else float('nan')):6.1f} "
          f"{(st.mean(col('failed')) if ok else float('nan')):6.1f} {int(sum(col('unfinished'))):6d} {sum(1 for r in ok if r.get('mem_abort')=='1'):5d} "
          f"{(max(col('peak_heap_mb')) if col('peak_heap_mb') else float('nan')):7.0f} "
          f"{(max(col('L_sync_max_ms')) if col('L_sync_max_ms') else float('nan')):9.0f} {reached:>10s}")
bad = [(r["label"], r["status"]) for r in rows if r["status"] != "ok"]
if bad:
    c = collections.Counter(bad)
    print("\nnon-ok runs:", ", ".join(f"{l}:{s} x{n}" for (l, s), n in sorted(c.items())))
