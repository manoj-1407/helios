#!/usr/bin/env python3
"""paired analysis for research/LOCK_SCOPE_PREREG.md.
  python3 research/analyze_lockscope.py research/results/lockscope_primary.csv
primary: fraction of runs with term_end > term_start (disrupted), exact McNemar per cell pair.
mem_abort counts as disrupted. runs with status != ok are listed and their pair dropped."""
import csv, sys, math, random, collections

rows = list(csv.DictReader(open(sys.argv[1])))
cells = collections.defaultdict(dict)   # cell -> rep -> {arm: row}
missing = []
for r in rows:
    lab = r["label"]; cell, arm = lab.rsplit("_", 1)
    if r["status"] != "ok":
        missing.append((lab, r["rep"], r["status"])); continue
    cells[cell].setdefault(int(r["rep"]), {})[arm] = r

def disrupted(r):
    return int(r["term_end"]) > int(r["term_start"]) or r["mem_abort"] == "1"

def mcnemar(b, c):  # b: ao0 disrupted only, c: ao1 disrupted only; exact two-sided
    n = b + c
    if n == 0: return 1.0
    k = min(b, c)
    p = sum(math.comb(n, i) for i in range(k + 1)) / 2 ** n * 2
    return min(1.0, p)

def boot(d, B=4000, seed=1):
    if not d: return (float("nan"),) * 3
    rng = random.Random(seed); n = len(d)
    ms = sorted(sum(rng.choice(d) for _ in range(n)) / n for _ in range(B))
    return sum(d) / n, ms[int(.025 * B)], ms[int(.975 * B)]

def f(r, k): return float(r[k])
order = ["ctl", "r050", "r090", "r100", "r120"]
print("cell  pairs  disrupted_ao0  disrupted_ao1  only0 only1  mcnemar_p   elections_ao0 elections_ao1")
for cell in [c for c in order if c in cells]:
    pairs = [v for v in cells[cell].values() if "ao0" in v and "ao1" in v]
    n = len(pairs)
    d0 = sum(disrupted(p["ao0"]) for p in pairs); d1 = sum(disrupted(p["ao1"]) for p in pairs)
    b = sum(disrupted(p["ao0"]) and not disrupted(p["ao1"]) for p in pairs)
    c = sum(disrupted(p["ao1"]) and not disrupted(p["ao0"]) for p in pairs)
    e0 = sum(int(p["ao0"]["elections"]) for p in pairs); e1 = sum(int(p["ao1"]["elections"]) for p in pairs)
    lost = sum(int(p[a]["lost"]) for p in pairs for a in ("ao0", "ao1"))
    print(f"{cell:5} {n:4}  {d0:4} ({100*d0/max(n,1):4.0f}%)  {d1:4} ({100*d1/max(n,1):4.0f}%)  {b:4} {c:4}  {mcnemar(b,c):9.4f}  {e0:8} {e1:8}   lost_acked={lost}")
print("\nsecondary, paired difference ao1 - ao0 (mean, 95% bootstrap interval), runs where both ok")
keys = [("commit_p50_ms", 1), ("commit_p99_ms", 1), ("achieved_per_s", 1), ("failed", 1), ("peak_heap_mb", 1),
        ("peak_goroutines", 1), ("L_propose_hold_max_ms", 1), ("L_propose_wait_max_ms", 1), ("L_sendgap_max_ms", 1)]
for cell in [c for c in order if c in cells]:
    pairs = [v for v in cells[cell].values() if "ao0" in v and "ao1" in v]
    out = []
    for k, _ in keys:
        try:
            d = [f(p["ao1"], k) - f(p["ao0"], k) for p in pairs]
        except (KeyError, ValueError):
            continue
        m, lo, hi = boot(d)
        out.append(f"{k} {m:+.1f} [{lo:+.1f},{hi:+.1f}]")
    print(cell, "|", "; ".join(out))
print("\nmissing/failed runs:", missing if missing else "none")
