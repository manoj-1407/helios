# bench — Benchmark Suite

The bench package provides tools for measuring Helios performance under controlled conditions. Use it to characterize throughput, latency, and write amplification before and after configuration changes.

---

## Benchmarks available

### `bench/replication_bench.go` — Replication benchmark

Measures end-to-end write latency and throughput for a multi-node cluster under varying load levels. Specifically designed to measure the effect of the replication-aware compaction feedback loop.

**What it measures:**
- Write throughput (ops/sec) at 1k, 5k, 10k, 20k ops/sec target rates
- P50, P95, P99, P999 write latency at each rate
- Max follower lag at each rate
- Leader CPU and disk I/O (sampled every second)
- Compaction events during the run

**How to run:**

```bash
# start a 3-node cluster first (see quickstart in main README)

go run bench/replication_bench.go \
  -addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 \
  -duration 60s \
  -rate 5000 \
  -key-size 32 \
  -value-size 256
```

Flags:
- `-addrs`: comma-separated helios node addresses
- `-duration`: how long to run (default 60s)
- `-rate`: target writes per second (default 1000)
- `-key-size`: key size in bytes (default 32)
- `-value-size`: value size in bytes (default 256)
- `-random-keys`: use random keys (write-heavy). Default: sequential keys (more representative of time-series)
- `-read-fraction`: fraction of operations that are reads (default 0.0 — pure writes)

**Sample output:**
```
=== Helios replication benchmark ===
nodes: 3  duration: 60s  rate: 5000 ops/sec
key: 32B  value: 256B  reads: 0%

--- throughput ---
achieved: 4987 ops/sec  (target: 5000)

--- latency ---
P50:    0.8ms
P95:    2.1ms
P99:    4.7ms
P999:  12.3ms

--- replication ---
max follower lag (peak):    23 entries
max follower lag (average):  4 entries
pressure level (peak):      0.31
pressure events HIGH+:      0

--- compaction ---
compaction cycles:    8
write amplification:  17.4x
strategy switches:    0
```

### `bench/cluster.go` — Cluster stress benchmark

Spins up an in-process simulated cluster (no real disk I/O, no real network) and measures the throughput ceiling of the Raft consensus layer itself. Useful for isolating consensus overhead from storage overhead.

```bash
go run bench/cluster.go -nodes 3 -ops 100000
```

---

## Interpreting results

### Write amplification

`write amplification = bytes written by compaction / bytes written by user`

- **1x**: no compaction (FIFO mode, or compaction not yet triggered)
- **3–10x**: normal for TIERED compaction
- **10–30x**: normal for LEVELED compaction
- **>50x**: heavy compaction load — may indicate write-heavy workload with small keys, or pressure loop deferring compaction and causing a large backlog

### Follower lag

Follower lag is measured in log entries. Each write = 1 log entry.

- **0–10 entries**: healthy. Normal network jitter.
- **10–50 entries**: mild lag. Watch for trend.
- **50–200 entries**: significant lag. Pressure loop should have activated.
- **>200 entries**: follower is struggling. Check disk I/O, CPU, and compaction state on that node.

### Pressure events

`pressure events HIGH+` counts how many times during the benchmark run the pressure level exceeded 0.60 (HIGH or CRITICAL state). Ideally this is 0 for a well-sized cluster. If it's nonzero but follower lag stayed bounded (< 200 entries), the pressure loop is working correctly.

### P999 latency

The P999 (99.9th percentile) is the single most important latency number for production readiness. One write in 1000 takes this long. Spikes in P999 indicate garbage collection pauses (JVM GC), disk I/O stalls, or Raft leader elections.

---

## Comparing with and without replication pressure feedback

To measure the impact of the pressure loop, run two benchmarks:

**Test 1: with pressure feedback (default)**
```bash
go run bench/replication_bench.go -rate 8000 -duration 120s
```

**Test 2: without pressure feedback**  
Temporarily comment out the `CompactionPressureLoop.Start()` call in `cmd/helios/main.go`, rebuild, run the same benchmark.

Compare:
- Max follower lag (should be lower with pressure feedback)
- P99/P999 write latency (should be lower or more stable)
- Number of snapshot transfers (should be lower or zero with pressure feedback)

---

## Running redis-benchmark against the RESP server

`redis-benchmark` is a standard Redis benchmarking tool that can target any RESP-compatible server:

```bash
# single node
redis-benchmark -p 6380 -t set,get -n 100000 -q

# with pipelining (higher throughput, less representative of real usage)
redis-benchmark -p 6380 -t set,get -n 100000 -P 10 -q

# mixed workload: 80% reads, 20% writes
redis-benchmark -p 6380 -t set,get -n 100000 --ratio 20:80 -q
```

Note: `redis-benchmark` always talks to a single node. For multi-node throughput, use `bench/replication_bench.go` which is Helios-aware and routes writes to the leader.
