# Benchmark Results

Run data is written here by `cmd/bench`. To collect the thesis comparison:

```bash
# Build the JVM jar first (requires Java 21 + Maven)
cd helios-kv && mvn -q package -DskipTests && cp target/helios-kv-*.jar ../helios-kv/helios-kv.jar && cd ..

# Run all three compaction configs (adaptive / mismatched / static-leveled)
go run ./cmd/bench -ops 50000 -wr 0.9 -out bench/results/thesis.json
```

`thesis.json` is the canonical result file committed to the repository.
Each run overwrites it. The file records the git commit hash so it can
be traced back to the exact code version that produced it.

## What the numbers mean

| metric | what it shows |
|---|---|
| `ops_per_sec` | throughput — higher is better |
| `p99_apply_lag_ns` | tail latency for a follower to apply a committed entry |
| `p99_lag_after_compaction_ns` | same, measured in the 500 ms window after a compaction strategy switch |
| `write_amplification` | physical / logical bytes ratio — 1.0 is perfect, higher costs disk bandwidth |
| `compaction_switches` | how many times ADAPTIVE changed strategy during the run |

### Expected thesis outcome (write-heavy 90% workload)

- **adaptive_both**: lowest p99 lag and lowest p99_lag_after_compaction — the classifier correctly switches to a write-optimised strategy and the follower keeps up
- **mismatched** (leader=LEVELED, follower=TIERED): highest p99 lag — TIERED on a write-heavy follower causes excessive space amplification and slower apply throughput
- **static_leveled**: moderate lag but highest write_amplification — LEVELED is suboptimal for pure-write loads because it rewrites too aggressively

### Observed deviations (thesis.json run)

**p99 lag ordering** differed from the prediction: `mismatched` produced the lowest p99 (~68ms) rather than the highest, and `static_leveled` was worst (~108ms). Likely cause: with only 200 unique keys and a 90% write ratio, the workload is effectively an update-heavy scan over a tiny key space. TIERED compaction on the follower does less write I/O in this regime because overwrites dominate; LEVELED's eager merging caused more background I/O that competed with apply threads, raising lag. A wider key space (e.g. 10k–100k keys) would produce the expected ordering.

**write_amplification < 1.0** for all configs (0.37–0.41). This is correct given the workload: the 200-key space means many overwrites accumulate in the memtable before each flush, so the flushed SSTable is far smaller than the sum of all bytes put through `db.put()`. The ratio approaches 1/updates-per-key, not the >1.0 compaction amplification the thesis assumed. A fresh-insert workload (monotonic keys) would produce write_amp > 1.0.

**compaction_switches = 0** — no full compaction ran during any config's ~90s window. Flushes happened (evidenced by non-zero write_amp) but the compaction manager did not accumulate enough SSTables to trigger a merge. Longer runs or smaller ops would surface compaction events.
