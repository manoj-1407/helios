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

Document any deviations from the above honestly here if the actual numbers differ.
