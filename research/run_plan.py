#!/usr/bin/env python3
"""run a raftload plan with one process per run, so an out-of-memory kill or a crash
costs one run and is recorded as a result instead of ending the whole batch.

  go build -o raftload ./research/cmd/raftload
  python3 research/run_plan.py ./raftload research/plans/stage1_load_delay.cells out.csv \
      --reps 10 --dur 20 --warmup 4 --seed 11

every rep runs every cell once in a seeded random order (a randomized block).
the output csv is the driver's columns with a leading 'status' column: 'ok', or
'crashed(<returncode>)' / 'timeout' with only the identifying columns filled in."""
import argparse, random, subprocess, sys, time

ap = argparse.ArgumentParser()
ap.add_argument("binary"); ap.add_argument("cells"); ap.add_argument("out")
ap.add_argument("--reps", type=int, default=10)
ap.add_argument("--dur", type=float, default=20)
ap.add_argument("--warmup", type=float, default=4)
ap.add_argument("--seed", type=int, default=1)
a = ap.parse_args()

cells = []
for line in open(a.cells):
    line = line.strip()
    if line and not line.startswith("#"):
        cells.append(line.split())
hdr = subprocess.run([a.binary, "-cells", a.cells, "-header"], capture_output=True, text=True).stdout.strip()
ncols = len(hdr.split(","))
out = open(a.out, "w")
out.write("status," + hdr + "\n"); out.flush()
for rep in range(1, a.reps + 1):
    order = list(range(len(cells)))
    random.Random(a.seed * 100003 + rep).shuffle(order)
    for pos, idx in enumerate(order, 1):
        label, rate, enforce, delays, stalls = cells[idx]
        cmd = [a.binary, "-cells", a.cells, "-single", str(idx), "-rep", str(rep), "-pos", str(pos),
               "-dur", f"{a.dur}s", "-warmup", f"{a.warmup}s", "-seed", str(a.seed)]
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=a.dur + 120)
            row = p.stdout.strip().splitlines()[-1] if p.stdout.strip() else ""
            if p.returncode == 0 and row.count(",") == ncols - 1:
                status = "ok"
            else:
                status = f"crashed({p.returncode})"
                row = ""
        except subprocess.TimeoutExpired:
            status, row = "timeout", ""
        if not row:
            fields = [str(rep), str(pos), label, rate, enforce, delays, stalls, f"{a.dur:.0f}"]
            row = ",".join(fields + [""] * (ncols - len(fields)))
        out.write(f"{status},{row}\n"); out.flush()
        print(f"[run_plan] rep {rep} pos {pos} {label} {status}", file=sys.stderr, flush=True)
        time.sleep(0.5)
