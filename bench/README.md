# bench

Runs the three-config thesis benchmark. Each config starts 3 helios-kv
JVM processes, wires them into a 3-node Raft cluster via HeliosKVBridge,
drives 50,000 ops at 90% write ratio, and records apply latency and
compaction events via BridgeMetrics.

## Run

```bash
go run ./cmd/bench -ops 50000
```

Takes 10-20 minutes. Produces the THESIS SUMMARY table.

## Configs

| config | leader mode | follower mode |
|---|---|---|
| adaptive_both | ADAPTIVE | ADAPTIVE |
| mismatched | FORCE_LEVELED | FORCE_TIERED |
| static_leveled | FORCE_LEVELED | FORCE_LEVELED |

## What to look at

- `p99_lag_ns`: tail latency under write-heavy load
- `max_lag`: where compaction spikes are most visible
- `cmp_events`: how many times the adaptive classifier switched strategies

For a stronger compaction signal, reduce key space from 10000 to 500
in replication_bench.go and run with `-ops 200000`.
