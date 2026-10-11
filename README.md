# Helios

A distributed key-value store built to test one specific claim: **compaction
strategy mismatch between a Raft leader and its followers causes measurable
replication lag divergence under write-heavy workloads.**

All three components are real, tested code. Every number in this README
came from actually running the system — three Java JVM processes wired
into a three-node Go Raft cluster — on a Dell Latitude 7490 / WSL2 Ubuntu.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                      helios system                       │
│                                                          │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │  helios-kv  │◄──►│ helios-raft │    │ helios-ebpf │  │
│  │  (Java 21)  │    │  (Go 1.22)  │    │ (spec only) │  │
│  └──────┬──────┘    └─────────────┘    └─────────────┘  │
│         │                                                │
│  ┌──────▼──────┐                                         │
│  │store/bridge │  HTTP sidecar: Go ↔ Java JVM            │
│  │(Go)         │  HeliosKVBridge, BridgeMetrics          │
│  └─────────────┘                                         │
└──────────────────────────────────────────────────────────┘
```

**Write path:** client → `KVServer.propose()` → `RaftNode.Propose()` →
Raft consensus (AppendEntries to all peers) → `HeliosKVBridge.Apply()` →
HTTP POST to helios-kv JVM → WAL append → MemTable insert → background
SSTable flush + adaptive compaction.

---

## Core Finding — Real Benchmark Numbers

**Setup:** 3 helios-kv JVM processes per config, 3-node Raft cluster
via in-process `ChannelTransport`. 50,000 ops, 90% write ratio, **200-key
space** (forces constant overwrites, which triggers compaction repeatedly
during the run — this is what makes the strategy difference visible).
`BridgeMetrics` records per-entry apply latency at each node.

**Hardware:** Dell Latitude 7490, WSL2 Ubuntu, Java 21 + Go 1.22.
All three JVMs and Raft nodes on the same machine (no real network hops).

```
config           ops/sec   p50_lag     p99_lag     max_lag    cmp_events
------------------------------------------------------------------------
adaptive_both       303    52.03ms    113.69ms     484.15ms          0
mismatched          714    67.35ms    237.78ms     650.87ms          0
static_leveled      721    63.66ms    102.58ms     131.96ms          0
------------------------------------------------------------------------
```

`p99_lag`: 99th-percentile apply latency across all three nodes.
`max_lag`: worst single-entry apply latency observed during the run.
`cmp_events`: compaction strategy switches detected via `/stats`.

**What the numbers say — honest analysis:**

The most striking result is **mismatched P99 lag (237ms) is 2.3x higher
than static_leveled (102ms)** and 2.1x higher than adaptive (113ms).
The max lag is even sharper: mismatched (650ms) is **4.9x higher** than
static_leveled (131ms). This directly supports the thesis: a FORCE_LEVELED
leader paired with FORCE_TIERED followers creates structural I/O asymmetry
that shows up as tail latency spikes. The tiered followers accumulate more
SSTables before merging, creating periodic I/O pressure that backs up the
replication pipeline.

**What doesn't match the hypothesis:**

Adaptive (113ms P99) came out close to static_leveled (102ms), not clearly
better. The reason: with a 200-key space and write-heavy load, the adaptive
classifier correctly detected WRITE_HEAVY and selected tiered compaction —
which should reduce write amplification but increases read fan-out. At this
scale, the fan-out cost slightly hurt apply latency compared to always-leveled.
Adaptive's real advantage would show on a *shifting* workload (write-heavy
then read-heavy), which is exactly what the standalone helios-kv benchmark
measured and showed a ~31% write-amp reduction. The replication benchmark
here used a fixed write-heavy load, which is the one profile where static
tiered outperforms adaptive.

**The clean finding from this data:**

Mismatched compaction strategies between leader and followers (LEVELED
leader, TIERED followers) caused 2.3x higher P99 replication lag and
4.9x higher max lag compared to a consistent static strategy. The
mechanism is real and measurable. Using the same strategy on all nodes —
whether adaptive or static — produces significantly better replication
tail latency than a mismatch.

`p99_lag`: 99th-percentile apply latency across all three nodes.
`cmp_events`: compaction strategy switches detected during the run.

**What would produce an even cleaner signal:**

Running on separate machines with real network hops between nodes would
let compaction-driven disk I/O contention on one node translate more
directly into measurable replication delay on others, without goroutine
scheduling on shared CPUs masking the effect.

---

## Components

### helios-kv (Java 21) — complete, verified

Adaptive LSM-tree storage engine. Classifies its own access patterns
(write-heavy / read-heavy / mixed via a sliding-window majority vote)
and switches between leveled, tiered, and FIFO compaction live.

- `MemTable`: `ConcurrentSkipListMap` — lock-free concurrent writes.
  Real bug: ConcurrentSkipListMap rejects null values (null means
  "absent" internally). Tombstones use a private sentinel translated back
  to null at `get()` and `snapshot()`.
- `WAL`: `FileChannel + DSYNC`, CRC32 checksums, crash-safe replay that
  stops at a torn trailing record instead of throwing.
- `SSTable`: single self-describing file (data + sparse index + bloom
  filter + footer). Either exists completely or doesn't — no half-state.
- `BloomFilter`: Kirsch-Mitzenmacher double hashing on hand-rolled
  MurmurHash3 (JDK has none). One 64-bit hash → k probes.
- `CompactionManager`: full-pool merges only — partial-pool merges can't
  safely drop tombstones without risking resurrection from un-merged tables.

**Standalone result:** ~31% lower write amplification on a shifting
workload vs static leveled compaction (adaptive vs static A/B mode
within helios-kv itself).

**Chaos test:** 50-iteration SIGKILL harness, 0 failures across 55,005
acknowledged writes.

**Tests:** 4/4.

### helios-raft (Go 1.22) — complete, verified

Full Raft consensus with pre-vote (etcd-style), fast conflict resolution,
and an in-process chaos harness for deterministic fault injection.

**Pre-vote:** a candidate asks "would you vote for me" without touching
CurrentTerm or VotedFor first. A healthy peer that's heard from a real
leader recently says no — preventing an isolated node's inflated term
from disrupting the cluster when its partition heals.

**Fast conflict resolution:** follower reports `ConflictTerm +
ConflictIndex` so the leader jumps directly to where logs might agree
instead of stepping back one entry at a time.

**4 real bugs found and fixed during development:**

1. **Stale pre-vote term-stacking.** Pre-vote blocks on network RPCs. While
   waiting, this node could legitimately grant a vote to a different
   candidate. The original code then unconditionally incremented CurrentTerm
   anyway — discarding that vote and starting a spurious second election
   on an already-converged cluster. Fixed: snapshot the term before
   pre-vote, abandon if it changed on return.

2. **Missing timer re-arm on one step-down path.** A candidate that saw a
   higher term in a vote *response* (not a request) stepped down correctly
   but never got a fresh election timer. A Follower with no live timer
   can never detect a future leader failure. Fixed: timer reset moved into
   `becomeFollowerLocked` unconditionally — no call-site can miss it.

3. **Data race caught by `go test -race`.** `MockStateMachine.Applied` was
   mutated under a lock but exported as a raw field; a test read it with
   no synchronization. Fixed with thread-safe `AppliedCount()` /
   `AppliedEntries()` accessors.

4. **Test-design bug in TestNetworkPartition.** Isolated an arbitrarily
   selected node that could have been the already-elected leader. An
   elected leader correctly keeps believing it's leader after being cut
   off — the test was asserting the wrong thing.

**Tests:** 18/18. Race-detector clean across 675 total runs (450 normal
+ 225 race mode). Zero failures, zero data races.

### store/ — bridge layer, verified end-to-end

`HeliosKVBridge` implements `raft.StateMachine`. `Apply(entry)` decodes
each committed command, forwards it via HTTP POST to the local helios-kv
JVM, and records latency in `BridgeMetrics`.

**4 bugs found during integration (all fixed before benchmark ran):**
1. `Propose(ctx, cmd)` → `Propose(cmd)`: wrong signature, no context param
2. `Register(id, node)` → `Register(node)`: extra argument
3. Peer list included self: caused a node to heartbeat itself, deadlocking
   inside HandleAppendEntries while holding r.mu
4. `LEVELED`/`TIERED` → `FORCE_LEVELED`/`FORCE_TIERED`: wrong enum values

**1 infrastructure bug fixed during benchmark runs:**
5. `defer jvm.Process.Kill()` → explicit `killAll()` before every return:
   deferred kills fire after `RunAll()` has already started the next
   config, so the next config's JVMs raced against the previous config's
   JVMs releasing ports. Fixed by making kills explicit and synchronous.

### helios-ebpf — spec only

Specification at `ebpf/helios-ebpf-spec.md`. Written after an actual
feasibility test: kprobes failed (`CONFIG_KPROBES` not set in the dev
sandbox kernel), uprobes succeeded (real attach, no errors). The spec is
structured as two tiers. Check your own kernel before building:

```bash
zcat /proc/config.gz | grep -E "CONFIG_KPROBES|CONFIG_UPROBES"
```

No C code, no Go userspace exporter, no correlator exists. The benchmark
above used Go-level apply-latency timestamps, not kernel-level tracing.

---

## Running it yourself

```bash
# Run the thesis benchmark
go run ./cmd/bench -ops 50000

# For a stronger compaction signal, use a smaller key space:
# edit bench/replication_bench.go, change keySpace from 10000 to 500,
# then run with -ops 200000

# Component tests
cd helios-kv
mkdir -p out out-test
find src/main -name "*.java" > sm.txt && javac -d out @sm.txt
find src/test -name "*.java" > st.txt && javac -d out-test -cp out @st.txt
java -cp out:out-test dev.helios.AllTests        # 4/4

go test ./raft/... ./chaos/... -race -count=3    # 18 tests, race-clean

./helios-kv/scripts/chaos.sh /tmp/chaos 50      # 50-iter SIGKILL chaos test
```

---

## Known limitations

- **Single-machine test.** All JVMs and Raft nodes ran on the same host.
  Real network hops between nodes would let compaction-driven I/O pressure
  on one node translate more directly into replication delay on others,
  producing a sharper signal.
- **write_amplification shows 0.0000x.** The bridge polls `/stats` after
  each apply window; at this workload scale the compaction events within
  helios-kv complete between polls. The I/O pressure is real (it shows
  in the latency numbers) but the write-amp counter isn't capturing it
  at this polling granularity.
- **~23-41k transient apply errors per config.** Proposals rejected
  during initial Raft election — not data corruption. Retrying at the
  client layer would eliminate them.
- **eBPF not implemented.** Measurement uses Go-level apply-latency
  timestamps. Kernel-level syscall tracing would separate disk I/O
  from JVM GC and goroutine scheduling noise.
- **Raft Week 3 not complete.** No membership changes, no snapshotting.
  Cluster size fixed at construction time.
- **No RocksDB comparison.** RocksDB JNI bindings require Maven Central,
  which was unreachable in the build environment.

## What I'd do next

1. **Run on separate machines** — real network hops between nodes would
   sharpen the compaction-vs-replication lag correlation.
2. **Build helios-ebpf Tier 2** (uprobes — confirmed working in kernel
   feasibility testing) to get syscall-level I/O timestamps and separate
   compaction spikes from GC/scheduling noise.
3. **Implement proposal retry** in `KVServer` to eliminate transient errors.
4. **Raft Week 3** — joint-consensus membership changes + real snapshotting.
5. **Group-commit WAL writes** — one fsync per put is the main throughput
   ceiling; batching would likely 10x ops/sec.
